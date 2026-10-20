// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

mod blob_file_list;
mod diff;
pub mod edit;
pub mod edit_log;
// `framing` uses `std::io::{Read, Write}`. The whole `version`
// module (recovery, persist, super_version, this file's
// `Version::encode_into`) is also std-bound today and consumes
// the framing helpers unconditionally. Gating only `framing`
// behind `#[cfg(feature = "std")]` would NOT help the
// no-std-check job at all — the callers (recovery, persist,
// super_version) would then fail to resolve `framing` under
// `--no-default-features --features alloc`, producing a
// strictly-worse compile-error count than leaving it ungated
// (one missing-module error per call site × N call sites vs the
// existing std-only call sites failing to compile on their own
// merits). The `no_std-check` job's metric is "error count must
// not increase", and adding a feature gate here increases it. So
// gating `framing` only pays off once its callers no longer need
// `std::io` either.
mod framing;
mod optimize;
mod persist;
pub mod recovery;
pub mod run;
mod super_version;
#[cfg(test)]
mod tests;

pub use blob_file_list::BlobFileList;
pub use persist::{SNAPSHOT_TEMPORARY_SUFFIX, persist_version};
pub use run::Run;
pub use super_version::RetentionEffect;
pub use super_version::SnapshotRef;
pub use super_version::{SuperVersion, SuperVersions};

use crate::TreeType;
use crate::blob_tree::{FragmentationEntry, FragmentationMap};
use crate::checksum::ChecksumType;
use crate::coding::Encode;
use crate::compaction::state::hidden_set::HiddenSet;
use crate::version::recovery::Recovery;
use crate::{
    HashSet, KeyRange, Table, TableId,
    comparator::UserComparator,
    vlog::{BlobFile, BlobFileId},
};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::ops::Deref;

#[doc(hidden)]
pub use optimize::optimize_key_ranges;
use optimize::{optimize_runs, order_by_age};

/// Lays out the runs of level `level_idx`: L0 from its tables' ages, since its
/// runs overlap and their order is what recency means there; a deeper level
/// by fusing its disjoint tables.
fn place_runs(
    level_idx: usize,
    runs: Vec<Run<Table>>,
    cmp: &dyn crate::comparator::UserComparator,
) -> Vec<Run<Table>> {
    if level_idx == 0 {
        order_by_age(
            runs.into_iter()
                .flat_map(|mut run| core::mem::take(run.inner_mut())),
            cmp,
        )
    } else {
        optimize_runs(runs, cmp)
    }
}
use run::Ranged;

/// Whether `level` holds a table `named` picks: a level that holds none is
/// left as it stands by a change, its layout neither rebuilt nor copied.
fn level_holds_any(level: &Level, named: impl Fn(TableId) -> bool) -> bool {
    level
        .iter()
        .flat_map(|run| run.iter())
        .any(|table| named(table.id()))
}

/// Context threaded through [`Version`] transformation methods.
///
/// Bundles the user comparator required for maintaining correct table ordering
/// across level mutations. Passed to [`Version::with_new_l0_run`],
/// [`Version::with_merge`], [`Version::with_moved`], and
/// [`Version::with_dropped`].
pub struct TransformContext<'a> {
    comparator: &'a dyn UserComparator,
}

impl<'a> TransformContext<'a> {
    /// Creates a new context with the given user comparator.
    pub fn new(comparator: &'a dyn UserComparator) -> Self {
        Self { comparator }
    }

    /// Returns the user comparator.
    pub fn comparator(&self) -> &'a dyn UserComparator {
        self.comparator
    }
}

pub const DEFAULT_LEVEL_COUNT: u8 = 7;

/// ID of a version: each install takes the one above the last, and an open
/// of a tree whose id reached [`RENUMBER_FROM`] starts the count again at `0`.
pub type VersionId = u64;

/// The version id from which an open reinstalls the tree under id `0`: half
/// the range, so the installs of one session can never reach the top.
pub const RENUMBER_FROM: VersionId = 1 << 63;

impl Ranged for Table {
    fn key_range(&self) -> &KeyRange {
        &self.metadata.key_range
    }
}

pub struct GenericLevel<T: Ranged> {
    runs: Vec<Arc<Run<T>>>,
}

impl<T: Ranged> core::ops::Deref for GenericLevel<T> {
    type Target = [Arc<Run<T>>];

    fn deref(&self) -> &Self::Target {
        &self.runs
    }
}

impl<T: Ranged> GenericLevel<T> {
    pub fn new(runs: Vec<Arc<Run<T>>>) -> Self {
        Self { runs }
    }

    pub fn table_count(&self) -> usize {
        self.iter().map(|x| x.len()).sum()
    }

    pub fn run_count(&self) -> usize {
        self.runs.len()
    }

    pub fn is_disjoint(&self) -> bool {
        self.run_count() == 1
    }

    pub fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &Arc<Run<T>>> {
        self.runs.iter()
    }
}

#[derive(Clone)]
pub struct Level(Arc<GenericLevel<Table>>);

impl core::ops::Deref for Level {
    type Target = GenericLevel<Table>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Level {
    pub fn empty() -> Self {
        Self::from_runs(vec![])
    }

    pub fn from_runs(runs: Vec<Arc<Run<Table>>>) -> Self {
        Self(Arc::new(GenericLevel { runs }))
    }

    pub fn list_ids(&self) -> HashSet<TableId> {
        self.iter()
            .flat_map(|run| run.iter())
            .map(Table::id)
            .collect()
    }

    pub fn first_run(&self) -> Option<&Arc<Run<Table>>> {
        self.runs.first()
    }

    /// Returns the on-disk size of the level.
    pub fn size(&self) -> u64 {
        self.0
            .iter()
            .flat_map(|x| x.iter())
            .map(Table::file_size)
            .sum()
    }

    pub fn aggregate_key_range(&self) -> KeyRange {
        self.aggregate_key_range_cmp(&crate::comparator::DefaultUserComparator)
    }

    /// Like [`Self::aggregate_key_range`], but uses a custom comparator for key ordering.
    ///
    /// Per-run aggregation via [`Run::aggregate_key_range`] is comparator-correct
    /// because runs are sorted in comparator order (ensured by `push_cmp`), so
    /// `first().min()` and `last().max()` yield the true extremes under the
    /// configured comparator. The cross-run aggregation then uses `aggregate_cmp`
    /// to find the global min/max.
    pub fn aggregate_key_range_cmp<C: crate::comparator::UserComparator + ?Sized>(
        &self,
        cmp: &C,
    ) -> KeyRange {
        if self.run_count() == 1 {
            #[expect(
                clippy::expect_used,
                reason = "we check for run_count, so the first run must exist"
            )]
            self.runs
                .first()
                .expect("should exist")
                .aggregate_key_range()
        } else {
            let key_ranges = self
                .iter()
                .map(|x| Run::aggregate_key_range(x))
                .collect::<Vec<_>>();

            KeyRange::aggregate_cmp(key_ranges.iter(), cmp)
        }
    }
}

#[derive(Clone)]
pub struct VersionInner {
    /// The version's ID
    id: VersionId,

    tree_type: TreeType,

    /// The individual LSM-tree levels which consist of runs of tables
    levels: Vec<Level>,

    // NOTE: We purposefully use Arc<_> to avoid deep cloning the blob files again and again
    //
    // Changing the value log tends to happen way less often than other modifications to the
    // LSM-tree
    //
    /// Blob files for large values (value log)
    #[doc(hidden)]
    pub blob_files: Arc<BlobFileList>,

    /// Blob file fragmentation
    gc_stats: Arc<FragmentationMap>,

    /// Highest snapshot seqno this version can no longer serve: data a
    /// snapshot at or below it saw may have been discarded (a GC compaction
    /// dropped versions below its watermark, a `clear` / table drop removed
    /// whole tables). Monotone across versions and persisted with the
    /// manifest, so the tree refuses those snapshots, live and after a reopen,
    /// instead of answering from data they never saw. `0` until the first such
    /// install.
    retention_floor: crate::SeqNo,

    /// Ids of the compression dictionaries this version's tables may reference,
    /// ascending. The bytes live in `dicts/`; this is what says which of them
    /// the tree still owes a reader.
    ///
    /// Versioned rather than tree-global for the same reason tables are: the
    /// file can be removed once the current version no longer references it
    /// (a reader of an older version holds tables that pinned it on open), and
    /// a recovery that lands on an older version still finds what that
    /// version's tables were written against.
    ///
    /// Not gated on the compression features. A build without them cannot
    /// resolve these ids, but it must carry the list forward across its own
    /// installs, or reopening with zstd again would find tables whose
    /// dictionary the tree has forgotten.
    dicts: Arc<[crate::file::DictId]>,

    /// The encoded report of a repair the tree still owes its caller, written
    /// into every snapshot of this version and those derived from it, until a
    /// repairing open hands it out and installs a version without it. Living
    /// in the snapshot, it is there exactly when the snapshot is installed.
    repair_report: Option<Arc<[u8]>>,

    /// The queries the tree served since it opened, by kind, shared by every
    /// version derived from this one: the read mix a compaction plans for.
    /// Not persisted.
    read_mix: Arc<ReadMix>,
}

/// The queries a tree served since it opened: point lookups, each key of a
/// batch one, and scans, each one however many runs it reaches.
///
/// Every read bumps it, from every thread: the counts are spread over
/// stripes a cache line apart, so readers on different cores do not take one
/// line from each other on every lookup.
#[derive(Default, Debug)]
pub struct ReadMix {
    stripes: [ReadMixStripe; READ_MIX_STRIPES],
}

const READ_MIX_STRIPES: usize = 16;
// `ReadMix::stripe` takes the stripe from the top four bits of a product.
const _: () = assert!(READ_MIX_STRIPES == 1 << 4);

/// One stripe of a [`ReadMix`], a cache line of its own on every target
/// that reads it from several cores.
#[derive(Default, Debug)]
#[repr(align(128))]
struct ReadMixStripe {
    points: portable_atomic::AtomicU64,
    scans: portable_atomic::AtomicU64,
}

impl ReadMix {
    /// The stripe the calling thread counts into. Threads run on stacks of
    /// their own, megabytes apart, so a stack address tells them apart at
    /// no cost and with no thread-local state. The stacks differ in their
    /// high bits and agree in the low ones at one call depth, so the address
    /// is mixed by a Fibonacci multiply and the stripe taken from the top of
    /// the product, which every bit of the address reaches. Two threads
    /// sharing a stripe only share its line.
    fn stripe(&self) -> &ReadMixStripe {
        let marker = 0u8;
        let at = core::ptr::from_ref(&marker) as usize as u64;
        // The top four bits of the product: below sixteen, the stripe count.
        let mixed = usize::try_from(at.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 60).unwrap_or(0);
        #[expect(
            clippy::indexing_slicing,
            reason = "the top four bits of the product index sixteen stripes"
        )]
        &self.stripes[mixed]
    }

    /// Counts `keys` point lookups.
    pub(crate) fn note_points(&self, keys: u64) {
        self.stripe()
            .points
            .fetch_add(keys, portable_atomic::Ordering::Relaxed);
    }

    /// Counts a scan.
    pub(crate) fn note_scan(&self) {
        self.stripe()
            .scans
            .fetch_add(1, portable_atomic::Ordering::Relaxed);
    }

    /// The point lookups and the scans counted so far.
    pub(crate) fn counts(&self) -> (u64, u64) {
        self.stripes.iter().fold((0, 0), |(points, scans), stripe| {
            // Bounded by the reads a tree serves.
            (
                points + stripe.points.load(portable_atomic::Ordering::Relaxed),
                scans + stripe.scans.load(portable_atomic::Ordering::Relaxed),
            )
        })
    }
}

/// A version is an immutable, point-in-time view of a tree's structure
///
/// Any time a table is created or deleted, a new version is created.
#[derive(Clone)]
pub struct Version {
    inner: Arc<VersionInner>,
}

impl core::ops::Deref for Version {
    type Target = VersionInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

// TODO: impl using generics so we can easily unit test Version transformation functions
impl Version {
    /// Returns the initial tree type.
    pub fn tree_type(&self) -> TreeType {
        self.tree_type
    }

    /// Returns the version ID.
    pub fn id(&self) -> VersionId {
        self.id
    }

    pub fn gc_stats(&self) -> &FragmentationMap {
        &self.gc_stats
    }

    /// The queries the tree served since it opened, by kind.
    pub(crate) fn read_mix(&self) -> &ReadMix {
        &self.read_mix
    }

    /// Highest snapshot seqno this version can no longer serve after a
    /// reopen; see [`VersionInner::retention_floor`].
    #[must_use]
    pub fn retention_floor(&self) -> crate::SeqNo {
        self.retention_floor
    }

    /// Raises the retention floor to `floor` (never lowers it: a later
    /// install cannot un-discard what an earlier one dropped). Returns this
    /// version unchanged when `floor` is not above the current one.
    #[must_use]
    pub fn with_retention_floor(&self, floor: crate::SeqNo) -> Self {
        if floor <= self.retention_floor {
            return self.clone();
        }
        Self {
            inner: Arc::new(VersionInner {
                id: self.id,
                tree_type: self.tree_type,
                levels: self.levels.clone(),
                blob_files: self.blob_files.clone(),
                gc_stats: self.gc_stats.clone(),
                retention_floor: floor,
                dicts: self.dicts.clone(),
                repair_report: self.repair_report.clone(),
                read_mix: Arc::clone(&self.read_mix),
            }),
        }
    }

    /// Ids of the compression dictionaries this version's tables may reference,
    /// ascending. See [`VersionInner::dicts`].
    #[must_use]
    pub fn dicts(&self) -> &[crate::file::DictId] {
        &self.dicts
    }

    /// The dictionary ids this version's files actually reference, ascending.
    ///
    /// The REGISTERED set ([`Self::dicts`]) is what the tree owes a reader; this
    /// is what it still needs. They diverge once a compaction rewrites the last
    /// file that used a dictionary, which is exactly when it becomes
    /// collectable.
    ///
    /// Blob files count as much as tables: a KV-separated tree stores its values
    /// there, under a dictionary of their own, and a blob file whose dictionary
    /// was collected cannot resolve a single indirection.
    #[must_use]
    pub fn referenced_dicts(&self) -> Vec<crate::file::DictId> {
        fn dict_of(compression: crate::CompressionType) -> Option<crate::file::DictId> {
            match compression {
                #[cfg(zstd_any)]
                crate::CompressionType::ZstdDict { dict_id, .. } => Some(dict_id),
                _ => None,
            }
        }

        // The data block codec only. An index block never names a dictionary:
        // the writer downgrades a `ZstdDict` index policy to plain zstd before
        // it records `index_block_compression`, so that field never holds one.
        let mut ids: Vec<crate::file::DictId> = self
            .iter_tables()
            .filter_map(|table| dict_of(table.metadata.data_block_compression))
            .chain(
                self.blob_files
                    .iter()
                    .filter_map(|bf| dict_of(bf.compression())),
            )
            .collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// The next version, registering `id` as well.
    ///
    /// Idempotent: an id already present returns this version unchanged, since
    /// the id is derived from the dictionary's own bytes and cannot name two
    /// different dictionaries. That is not a transition, so it is not installed.
    #[must_use]
    pub fn with_dict(&self, id: crate::file::DictId) -> Self {
        if self.dicts.contains(&id) {
            return self.clone();
        }
        let mut dicts = self.dicts.to_vec();
        // Ascending, so the persisted order does not depend on registration
        // order and two trees holding the same dictionaries encode alike.
        let at = dicts.partition_point(|held| *held < id);
        dicts.insert(at, id);
        self.with_id_and_dicts(self.id + 1, dicts)
    }

    /// The next version, no longer registering any of `ids`. None of them
    /// held returns this version unchanged, which is not installed either.
    ///
    /// Dropping an id is what makes its file collectable; the bytes are
    /// removed separately, once this drop is installed as the current version.
    /// One transition for the whole set, however many ids it drops.
    #[must_use]
    pub fn without_dicts(&self, ids: &[crate::file::DictId]) -> Self {
        if !self.dicts.iter().any(|held| ids.contains(held)) {
            return self.clone();
        }
        let dicts = self
            .dicts
            .iter()
            .copied()
            .filter(|held| !ids.contains(held))
            .collect::<Vec<_>>();
        self.with_id_and_dicts(self.id + 1, dicts)
    }

    /// The next version with every table and blob file gone, still registering
    /// the ids in `still_written` it held.
    ///
    /// Nothing is left to read, so the only dictionaries still owed are the
    /// ones the next write compresses against. Dropping those as well would
    /// leave the tables written after this naming an id the version does not
    /// register, and a checkpoint, which carries only what is registered, would
    /// copy them without it.
    #[must_use]
    pub(crate) fn cleared(&self, still_written: &[crate::file::DictId]) -> Self {
        let dicts = self
            .dicts
            .iter()
            .copied()
            .filter(|id| still_written.contains(id))
            .collect();
        Self::new(self.id + 1, self.tree_type)
            .with_dicts(dicts)
            .with_repair_report(self.repair_report.clone())
    }

    /// The encoded report of a repair the tree still owes, if any. See
    /// [`VersionInner::repair_report`].
    #[must_use]
    pub(crate) fn repair_report(&self) -> Option<&[u8]> {
        self.repair_report.as_deref()
    }

    /// This version owing `report` instead of its own, under the SAME id: for
    /// a version a repair or a recovery is building, not one being installed.
    #[must_use]
    pub(crate) fn with_repair_report(&self, report: Option<Arc<[u8]>>) -> Self {
        Self {
            inner: Arc::new(VersionInner {
                repair_report: report,
                ..VersionInner::clone(&self.inner)
            }),
        }
    }

    /// The next version, owing no repair report: what an install that hands
    /// the report out writes. A snapshot holds the report, so the install
    /// rotates.
    #[cfg(feature = "std")]
    #[must_use]
    pub(crate) fn without_repair_report(&self) -> Self {
        Self {
            inner: Arc::new(VersionInner {
                id: self.id + 1,
                repair_report: None,
                ..VersionInner::clone(&self.inner)
            }),
        }
    }

    /// This version with `dicts` in place of its own list, under the SAME id.
    ///
    /// For a version that is being rebuilt rather than installed: manifest
    /// repair and recovery set the list this way, since the ids have to be
    /// derived from the recovered files. An install goes through
    /// [`Self::with_dict`] / [`Self::without_dicts`], which advance the id like
    /// every other transition: a rotation writes the installed version as a
    /// fresh `v{id}` snapshot, and one under the prior id would collide with
    /// the snapshot that id already names.
    #[must_use]
    pub(crate) fn with_dicts(&self, dicts: Vec<crate::file::DictId>) -> Self {
        self.with_id_and_dicts(self.id, dicts)
    }

    /// This version under the id an open installs it with, unchanged
    /// otherwise: the next id, so the snapshot it writes never replaces the
    /// one it was read from, or id `0` once this one is at or past
    /// [`RENUMBER_FROM`]. See [`Self::is_renumbered_on_open`].
    #[must_use]
    pub(crate) fn reinstalled(&self) -> Self {
        let next = if self.is_renumbered_on_open() {
            0
        } else {
            // Below `RENUMBER_FROM`, so one more stays in range.
            self.id + 1
        };
        self.with_id_and_dicts(next, self.dicts.to_vec())
    }

    /// Whether an open reinstalls this version under id `0` rather than the
    /// next id. Every install takes one id, an open included, so a tree whose
    /// ids reach the top of the range (in practice only one written or edited
    /// by other means) would run out; restarting the count keeps it openable,
    /// and a session that starts below [`RENUMBER_FROM`] cannot reach the top
    /// by its own installs.
    #[must_use]
    pub(crate) fn is_renumbered_on_open(&self) -> bool {
        self.id >= RENUMBER_FROM
    }

    fn with_id_and_dicts(&self, id: VersionId, dicts: Vec<crate::file::DictId>) -> Self {
        Self {
            inner: Arc::new(VersionInner {
                id,
                tree_type: self.tree_type,
                levels: self.levels.clone(),
                blob_files: self.blob_files.clone(),
                gc_stats: self.gc_stats.clone(),
                retention_floor: self.retention_floor,
                dicts: Arc::from(dicts),
                repair_report: self.repair_report.clone(),
                read_mix: Arc::clone(&self.read_mix),
            }),
        }
    }

    /// Same rule as [`Self::with_retention_floor`], in place: reuses the
    /// allocation when this handle is the sole owner.
    ///
    /// The install path always is, because the version it raises the floor on
    /// was just built by the edit's own mutator, and rebuilding it there would
    /// throw that fresh allocation away to change one integer. A handle still
    /// shared with the prior version falls back to the rebuild, which is what
    /// the shared case has to do regardless.
    pub(crate) fn set_retention_floor(&mut self, floor: crate::SeqNo) {
        if floor <= self.retention_floor {
            return;
        }
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.retention_floor = floor;
        } else {
            *self = self.with_retention_floor(floor);
        }
    }

    pub fn l0(&self) -> &Level {
        #[expect(clippy::expect_used)]
        self.levels.first().expect("L0 should exist")
    }

    #[must_use]
    pub fn level_is_busy(&self, idx: usize, hidden_set: &HiddenSet) -> bool {
        self.level(idx).is_some_and(|level| {
            level
                .iter()
                .flat_map(|run| run.iter())
                .any(|table| hidden_set.is_hidden(table.id()))
        })
    }

    /// Creates a new empty version.
    pub fn new(id: VersionId, tree_type: TreeType) -> Self {
        let levels = (0..DEFAULT_LEVEL_COUNT).map(|_| Level::empty()).collect();

        Self {
            inner: Arc::new(VersionInner {
                id,
                tree_type,
                levels,
                blob_files: Arc::default(),
                gc_stats: Arc::default(),
                retention_floor: 0,
                dicts: Arc::from([]),
                repair_report: None,
                read_mix: Arc::default(),
            }),
        }
    }

    pub(crate) fn from_recovery(
        recovery: Recovery,
        tables: &[Table],
        blob_files: &[BlobFile],
        comparator: &dyn crate::comparator::UserComparator,
    ) -> crate::Result<Self> {
        let version_levels = recovery
            .table_ids
            .iter()
            .enumerate()
            .map(|(level_idx, level)| {
                let level_runs = level
                    .iter()
                    .map(|run| {
                        let run_tables = run
                            .iter()
                            .map(|table| {
                                let opened = tables
                                    .iter()
                                    .find(|x| x.id() == table.id)
                                    .cloned()
                                    .ok_or(crate::Error::Unrecoverable)?;
                                // Rebuild the tight-space restricted view: the
                                // data below the bound was punched out, so reads
                                // must clamp to it (its index still references the
                                // punched prefix).
                                Ok(match recovery.restrictions.get(&table.id) {
                                    Some(bound) => opened.with_restriction(bound.clone()),
                                    None => opened,
                                })
                            })
                            .collect::<crate::Result<Vec<_>>>()?;

                        // Tables are in persisted order, which preserves the
                        // comparator-sorted order from when the run was written.
                        // No re-sort needed — the manifest faithfully round-trips
                        // the run's table sequence.
                        Ok(Arc::new(
                            #[expect(
                                clippy::expect_used,
                                reason = "empty runs should not exist, so there should not be any empty persisted runs"
                            )]
                            Run::new(run_tables).expect("persisted runs should not be empty"),
                        ))
                    })
                    .collect::<crate::Result<Vec<_>>>()?;

                // A persisted L0 is kept as written when it is in recency order
                // (repair, for one, writes a run per table), and laid out again
                // from its tables' ages when it is not: a manifest an earlier
                // placement wrote may hold a newer table behind an older one
                // it overlaps.
                if level_idx == 0 {
                    let persisted: Vec<&Run<Table>> =
                        level_runs.iter().map(Arc::as_ref).collect();
                    if !optimize::in_recency_order(&persisted, comparator) {
                        let tables = level_runs.iter().flat_map(|run| run.iter().cloned());
                        let level_runs = order_by_age(tables, comparator)
                            .into_iter()
                            .map(Arc::new)
                            .collect();
                        return Ok(Level::from_runs(level_runs));
                    }
                }
                Ok(Level::from_runs(level_runs))
            })
            .collect::<crate::Result<Vec<_>>>()?;

        let recovered = Self::from_levels(
            recovery.curr_version_id,
            recovery.tree_type,
            version_levels,
            BlobFileList::new(blob_files.iter().cloned().map(|bf| (bf.id(), bf)).collect()),
            recovery.gc_stats,
        )
        .with_retention_floor(recovery.retention_floor);

        // What the manifest recorded, plus every id the recovered files name.
        // The two agree once every registration has landed; they part on a
        // manifest written before the tree stored its dictionaries, and after a
        // crash lost the edit registering a file already published. The files
        // are what a reader and a checkpoint need, and every id they name
        // resolved at the open or the table would have refused it.
        //
        // The other direction, a registered id whose file is gone, is settled
        // by the open, which holds the dictionary set this function does not:
        // it drops every registered id the tree no longer holds.
        let mut dicts = recovery.dicts;
        dicts.extend(recovered.referenced_dicts());
        dicts.sort_unstable();
        dicts.dedup();
        Ok(recovered
            .with_dicts(dicts)
            .with_repair_report(recovery.repair_report.map(Arc::from)))
    }

    /// Creates a new pre-populated version.
    pub fn from_levels(
        id: VersionId,
        tree_type: TreeType,
        levels: Vec<Level>,
        blob_files: BlobFileList,
        gc_stats: FragmentationMap,
    ) -> Self {
        Self {
            inner: Arc::new(VersionInner {
                id,
                tree_type,
                levels,
                blob_files: Arc::new(blob_files),
                gc_stats: Arc::new(gc_stats),
                retention_floor: 0,
                dicts: Arc::from([]),
                repair_report: None,
                read_mix: Arc::default(),
            }),
        }
    }

    /// Returns the number of levels.
    pub fn level_count(&self) -> usize {
        self.levels.len()
    }

    /// Returns an iterator through all levels.
    pub fn iter_levels(&self) -> impl Iterator<Item = &Level> {
        self.levels.iter()
    }

    /// Returns the number of tables in all levels.
    pub fn table_count(&self) -> usize {
        self.iter_levels().map(|x| x.table_count()).sum()
    }

    pub fn blob_file_count(&self) -> usize {
        self.blob_files.len()
    }

    /// Returns an iterator over all tables.
    pub fn iter_tables(&self) -> impl Iterator<Item = &Table> {
        self.levels
            .iter()
            .flat_map(|x| x.iter())
            .flat_map(|x| x.iter())
    }

    pub(crate) fn get_table(&self, id: TableId) -> Option<&Table> {
        self.iter_tables().find(|x| x.metadata.id == id)
    }

    /// Gets the n-th level.
    pub fn level(&self, n: usize) -> Option<&Level> {
        self.levels.get(n)
    }

    /// Creates a new version with the additional run added to the "top" of L0.
    pub fn with_new_l0_run(
        &self,
        run: &[Table],
        blob_files: Option<&[BlobFile]>,
        diff: Option<FragmentationMap>,
        ctx: &TransformContext<'_>,
    ) -> Self {
        let comparator = ctx.comparator;
        let id = self.id + 1;

        let mut levels = vec![];

        // L0
        levels.push({
            // Copy-on-write the first level with new run at top

            #[expect(clippy::expect_used, reason = "L0 always exists")]
            let l0 = self.levels.first().expect("L0 should always exist");

            let prev_runs = l0
                .runs
                .iter()
                .map(|run| {
                    let run: Run<_> = run.deref().clone();
                    run
                })
                .collect::<Vec<_>>();

            let mut runs = Vec::with_capacity(prev_runs.len() + run.len());

            // Start each freshly flushed table as its own run. `optimize_runs`
            // will fuse them back together when their key ranges stay truly
            // disjoint, but RT-bearing flush tables may intentionally widen
            // their persisted key_range and must not be forced into a single
            // run where `Run::get_for_key` assumes non-overlap.
            runs.extend(run.iter().cloned().map(|table| {
                let Some(run) = Run::new(vec![table]) else {
                    unreachable!("single-table run should never be empty");
                };

                run
            }));

            runs.extend(prev_runs);

            let runs = place_runs(0, runs, comparator);

            Level::from_runs(runs.into_iter().map(Arc::new).collect())
        });

        // L1+
        levels.extend(self.levels.iter().skip(1).cloned());

        // Value log
        let value_log = if let Some(blob_files) = blob_files {
            let mut copy = self.blob_files.deref().clone();
            copy.extend(blob_files.iter().cloned().map(|bf| (bf.id(), bf)));
            copy.into()
        } else {
            self.blob_files.clone()
        };

        let gc_stats = if let Some(diff) = diff {
            let mut copy = self.gc_stats.deref().clone();
            diff.merge_into(&mut copy);
            copy.prune(&value_log);
            Arc::new(copy)
        } else {
            self.gc_stats.clone()
        };

        Self {
            inner: Arc::new(VersionInner {
                id,
                tree_type: self.tree_type,
                levels,
                blob_files: value_log,
                gc_stats,
                retention_floor: self.retention_floor,
                dicts: self.dicts.clone(),
                repair_report: self.repair_report.clone(),
                read_mix: Arc::clone(&self.read_mix),
            }),
        }
    }

    /// Returns a new version with a list of tables removed.
    ///
    /// The table files are not immediately deleted, this is handled by the version system's free list.
    ///
    /// `memtable_refs` reports whether a memtable row references a blob file,
    /// which keeps that file whatever its counts say.
    pub fn with_dropped(
        &self,
        ids: &[TableId],
        dropped_blob_files: &mut Vec<BlobFile>,
        memtable_refs: &dyn Fn(BlobFileId) -> bool,
        ctx: &TransformContext<'_>,
    ) -> crate::Result<Self> {
        let comparator = ctx.comparator;
        let id = self.id + 1;

        let mut levels = vec![];

        let mut dropped_tables: Vec<Table> = vec![];

        for (level_idx, level) in self.levels.iter().enumerate() {
            // A level the drop takes nothing from keeps its layout as it is.
            if !level_holds_any(level, |id| ids.contains(&id)) {
                levels.push(level.clone());
                continue;
            }
            let runs = level
                .runs
                .iter()
                .map(|run| {
                    let mut run: Run<_> = run.deref().clone();

                    let removed_tables = run
                        .inner_mut()
                        .extract_if(.., |x| ids.contains(&x.metadata.id));

                    dropped_tables.extend(removed_tables);

                    run
                })
                .filter(|x| !x.is_empty())
                .collect::<Vec<_>>();

            let runs = place_runs(level_idx, runs, comparator);

            levels.push(Level::from_runs(runs.into_iter().map(Arc::new).collect()));
        }

        let gc_stats = if dropped_tables.is_empty() {
            self.gc_stats.clone()
        } else {
            let mut copy = self.gc_stats.deref().clone();

            for table in &dropped_tables {
                let linked_blob_files = table.list_blob_file_references()?.unwrap_or_default();

                for blob_file in linked_blob_files {
                    copy.entry(blob_file.blob_file_id)
                        .and_modify(|counter| {
                            counter.bytes += blob_file.bytes;
                            counter.len += blob_file.len;
                        })
                        .or_insert_with(|| {
                            FragmentationEntry::new(
                                blob_file.len,
                                blob_file.bytes,
                                blob_file.on_disk_bytes,
                            )
                        });
                }
            }

            Arc::new(copy)
        };

        let value_log = if dropped_tables.is_empty() {
            self.blob_files.clone()
        } else {
            // A dropped table charges only the objects it owns, so a file can
            // look dead while a kept table or a memtable row still borrows an
            // object in it: such a file stays until that reference goes.
            let candidates: crate::HashSet<BlobFileId> = self
                .blob_files
                .iter()
                .filter(|file| file.is_dead(&gc_stats))
                .map(BlobFile::id)
                .collect();
            let mut linked = crate::HashSet::default();
            if !candidates.is_empty() {
                for table in levels
                    .iter()
                    .flat_map(|level| level.iter())
                    .flat_map(|run| run.iter())
                {
                    for link in table.blob_links()? {
                        if candidates.contains(&link.blob_file_id) {
                            linked.insert(link.blob_file_id);
                        }
                    }
                }
            }
            let mut copy = self.blob_files.deref().clone();
            dropped_blob_files
                .extend(copy.prune_dead(&gc_stats, |id| linked.contains(&id) || memtable_refs(id)));
            Arc::new(copy)
        };

        Ok(Self {
            inner: Arc::new(VersionInner {
                id,
                tree_type: self.tree_type,
                levels,
                blob_files: value_log,
                gc_stats,
                retention_floor: self.retention_floor,
                dicts: self.dicts.clone(),
                repair_report: self.repair_report.clone(),
                read_mix: Arc::clone(&self.read_mix),
            }),
        })
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "merge requires blob/GC params alongside context; further bundling planned"
    )]
    pub fn with_merge(
        &self,
        old_ids: &[TableId],
        new_tables: &[Table],
        dest_level: usize,
        diff: Option<FragmentationMap>,
        new_blob_files: Vec<BlobFile>,
        blob_files_to_drop: &HashSet<BlobFileId>,
        ctx: &TransformContext<'_>,
    ) -> Self {
        let comparator = ctx.comparator;
        let id = self.id + 1;

        let mut levels = vec![];

        for (level_idx, level) in self.levels.iter().enumerate() {
            // A level the merge neither takes from nor writes to keeps its
            // layout as it is.
            if level_idx != dest_level && !level_holds_any(level, |id| old_ids.contains(&id)) {
                levels.push(level.clone());
                continue;
            }
            let mut runs = level
                .runs
                .iter()
                .map(|run| {
                    let mut run: Run<_> = run.deref().clone();
                    run.retain(|x| !old_ids.contains(&x.metadata.id));
                    run
                })
                .filter(|x| !x.is_empty())
                .collect::<Vec<_>>();

            if level_idx == dest_level
                && let Some(run) = Run::new(new_tables.to_vec())
            {
                if dest_level == 0 {
                    // Intra-L0 compaction (flushes use with_new_l0_run). Where
                    // the output goes is decided by its tables' ages below, so
                    // a run flushed while it ran stays in front of it.
                    runs.push(run);
                } else {
                    runs.insert(0, run);
                }
            }

            let runs = place_runs(level_idx, runs, comparator);

            levels.push(Level::from_runs(runs.into_iter().map(Arc::new).collect()));
        }

        let has_diff = diff.is_some();

        let value_log = if has_diff || !new_blob_files.is_empty() || !blob_files_to_drop.is_empty()
        {
            let mut copy = self.blob_files.deref().clone();

            for blob_file in new_blob_files {
                copy.insert(blob_file.id(), blob_file);
            }

            for &id in blob_files_to_drop {
                copy.remove(id);
            }

            Arc::new(copy)
        } else {
            self.blob_files.clone()
        };

        let gc_stats = if has_diff || !blob_files_to_drop.is_empty() {
            let mut copy = self.gc_stats.deref().clone();

            if let Some(diff) = diff {
                diff.merge_into(&mut copy);
            }

            copy.prune(&value_log);

            Arc::new(copy)
        } else {
            self.gc_stats.clone()
        };

        Self {
            inner: Arc::new(VersionInner {
                id,
                tree_type: self.tree_type,
                levels,
                blob_files: value_log,
                gc_stats,
                retention_floor: self.retention_floor,
                dicts: self.dicts.clone(),
                repair_report: self.repair_report.clone(),
                read_mix: Arc::clone(&self.read_mix),
            }),
        }
    }

    pub fn with_moved(
        &self,
        ids: &[TableId],
        dest_level: usize,
        ctx: &TransformContext<'_>,
    ) -> Self {
        let comparator = ctx.comparator;
        let id = self.id + 1;

        // The moved tables of each source run, in the walk's order: levels top
        // down and each level's runs newest first, so every group is newer
        // than the groups after it. Overlapping runs moved together keep that
        // order in the destination; one group per run, they are not mixed.
        let mut moved_runs: Vec<Vec<Table>> = Vec::new();
        for run in self.levels.iter().flat_map(|level| level.iter()) {
            let mut moved: Vec<Table> = run
                .iter()
                .filter(|table| ids.contains(&table.id()))
                .cloned()
                .collect();
            if moved.is_empty() {
                continue;
            }
            // A run's tables are in key order already, but one taken from L0
            // is a set of tables ordered by age: a run is searched by binary
            // search over minimum keys, so it is sorted into comparator order.
            moved.sort_by(|a, b| comparator.compare(a.key_range().min(), b.key_range().min()));
            moved_runs.push(moved);
        }

        assert_eq!(
            moved_runs.iter().map(Vec::len).sum::<usize>(),
            ids.len(),
            "invalid table IDs"
        );

        // Tables from several source runs that do not overlap each other go
        // into the destination as one run, as before: sorted into comparator
        // order, since `optimize_runs` below re-sorts only when it has to
        // split the level into several runs.
        let disjoint = moved_runs.len() <= 1 || {
            let mut all: Vec<&Table> = moved_runs.iter().flatten().collect();
            all.sort_by(|a, b| comparator.compare(a.key_range().min(), b.key_range().min()));
            all.iter().zip(all.iter().skip(1)).all(|(a, b)| {
                comparator
                    .compare(a.key_range().max(), b.key_range().min())
                    .is_lt()
            })
        };
        if disjoint && moved_runs.len() > 1 {
            let mut all: Vec<Table> = core::mem::take(&mut moved_runs)
                .into_iter()
                .flatten()
                .collect();
            all.sort_by(|a, b| comparator.compare(a.key_range().min(), b.key_range().min()));
            moved_runs.push(all);
        }

        let mut levels = vec![];

        for (level_idx, level) in self.levels.iter().enumerate() {
            // A level the move neither takes from nor moves to keeps its
            // layout as it is.
            if level_idx != dest_level && !level_holds_any(level, |id| ids.contains(&id)) {
                levels.push(level.clone());
                continue;
            }
            let mut runs = level
                .runs
                .iter()
                .map(|run| {
                    let mut run: Run<_> = run.deref().clone();
                    run.retain(|x| !ids.contains(&x.metadata.id));
                    run
                })
                .filter(|x| !x.is_empty())
                .collect::<Vec<_>>();

            if level_idx == dest_level {
                // Newest first, ahead of the destination's runs: whatever was
                // moved down sits above what was already there.
                runs.splice(
                    0..0,
                    moved_runs
                        .iter()
                        .filter_map(|tables| Run::new(tables.clone())),
                );
            }

            let runs = place_runs(level_idx, runs, comparator);

            levels.push(Level::from_runs(runs.into_iter().map(Arc::new).collect()));
        }

        Self {
            inner: Arc::new(VersionInner {
                id,
                tree_type: self.tree_type,
                levels,
                blob_files: self.blob_files.clone(),
                gc_stats: self.gc_stats.clone(),
                retention_floor: self.retention_floor,
                dicts: self.dicts.clone(),
                repair_report: self.repair_report.clone(),
                read_mix: Arc::clone(&self.read_mix),
            }),
        }
    }

    /// Returns a version identical to this one except that the table with
    /// `table_id` reports `checksum` as its full-file digest, or `None` when
    /// the table is no longer part of this version (compacted away — nothing
    /// to refresh). Used after an in-place heal rewrites a table's bytes: the
    /// healed file's digest no longer matches the one captured at recovery,
    /// and installing this version persists the refreshed digest to the
    /// manifest via the version diff. Level / run structure is untouched
    /// (same table, same key range — only the digest changes).
    #[must_use]
    pub(crate) fn with_refreshed_table_checksum(
        &self,
        table_id: TableId,
        checksum: crate::checksum::Checksum,
    ) -> Option<Self> {
        if !self.iter_tables().any(|t| t.id() == table_id) {
            return None;
        }

        let id = self.id + 1;

        let mut levels = vec![];

        for level in &self.levels {
            let runs = level
                .runs
                .iter()
                .map(|run| {
                    if run.iter().any(|t| t.id() == table_id) {
                        let mut run: Run<_> = run.deref().clone();
                        for t in run.inner_mut().iter_mut() {
                            if t.id() == table_id {
                                *t = t.with_refreshed_checksum(checksum);
                            }
                        }
                        Arc::new(run)
                    } else {
                        run.clone()
                    }
                })
                .collect::<Vec<_>>();

            levels.push(Level::from_runs(runs));
        }

        Some(Self {
            inner: Arc::new(VersionInner {
                id,
                tree_type: self.tree_type,
                levels,
                blob_files: self.blob_files.clone(),
                gc_stats: self.gc_stats.clone(),
                retention_floor: self.retention_floor,
                dicts: self.dicts.clone(),
                repair_report: self.repair_report.clone(),
                read_mix: Arc::clone(&self.read_mix),
            }),
        })
    }

    /// Tight-space slice install for one or more inputs: replaces each
    /// `(id, restricted view)` in `restricted` with its clamped view, drops the
    /// fully-consumed inputs in `removed_ids`, and adds the slice `outputs` as a
    /// new run in `dest_level`. The restriction rides on the [`Table`] wrapper,
    /// so `diff` / `encode_into` persist it; a replaced / removed prior view is
    /// released by the version swap and (for a restricted view) punches its
    /// consumed prefix once its readers drain.
    ///
    /// For KV-separated trees the slice's `gc_diff` (newly dead blob entries)
    /// and any `new_blob_files` it produced are folded in, so the running GC
    /// stats stay accurate; globally-dead blob files are not dropped here (an
    /// unprocessed slice may still reference them) — that happens at the final
    /// removal.
    #[expect(
        clippy::too_many_arguments,
        reason = "a slice install carries the SST swaps/removes/outputs plus the \
                  KV-separation blob delta (new files + GC diff); bundling would \
                  just move the argument list"
    )]
    pub fn with_tight_slice(
        &self,
        restricted: &[(TableId, Table)],
        removed_ids: &[TableId],
        outputs: &[Table],
        new_blob_files: Vec<BlobFile>,
        gc_diff: Option<FragmentationMap>,
        dest_level: usize,
        ctx: &TransformContext<'_>,
    ) -> Self {
        let comparator = ctx.comparator;
        let id = self.id + 1;

        let mut levels = vec![];

        for (level_idx, level) in self.levels.iter().enumerate() {
            // A level the slice neither consumes, restricts nor writes to
            // keeps its layout as it is.
            if level_idx != dest_level
                && !level_holds_any(level, |id| {
                    removed_ids.contains(&id) || restricted.iter().any(|(rid, _)| *rid == id)
                })
            {
                levels.push(level.clone());
                continue;
            }
            let mut runs = level
                .runs
                .iter()
                .map(|run| {
                    let mut run: Run<_> = run.deref().clone();
                    // Drop fully-consumed inputs.
                    run.retain(|t| !removed_ids.contains(&t.id()));
                    // Swap each restricted input for its clamped view (same id).
                    for t in run.inner_mut().iter_mut() {
                        if let Some((_, view)) = restricted.iter().find(|(rid, _)| *rid == t.id()) {
                            *t = view.clone();
                        }
                    }
                    run
                })
                .filter(|run| !run.is_empty())
                .collect::<Vec<_>>();

            if level_idx == dest_level
                && let Some(run) = Run::new(outputs.to_vec())
            {
                if dest_level == 0 {
                    runs.push(run);
                } else {
                    runs.insert(0, run);
                }
            }

            let runs = place_runs(level_idx, runs, comparator);

            levels.push(Level::from_runs(runs.into_iter().map(Arc::new).collect()));
        }

        // KV-separation blob delta: add any newly written blob files and fold in
        // this slice's GC diff. Dead blob files are NOT pruned here — a later
        // slice may still reference them; the final removal does the drop.
        let value_log = if gc_diff.is_some() || !new_blob_files.is_empty() {
            let mut copy = self.blob_files.deref().clone();
            for blob_file in new_blob_files {
                copy.insert(blob_file.id(), blob_file);
            }
            Arc::new(copy)
        } else {
            self.blob_files.clone()
        };
        let gc_stats = if let Some(diff) = gc_diff {
            let mut copy = self.gc_stats.deref().clone();
            diff.merge_into(&mut copy);
            Arc::new(copy)
        } else {
            self.gc_stats.clone()
        };

        Self {
            inner: Arc::new(VersionInner {
                id,
                tree_type: self.tree_type,
                levels,
                blob_files: value_log,
                gc_stats,
                retention_floor: self.retention_floor,
                dicts: self.dicts.clone(),
                repair_report: self.repair_report.clone(),
                read_mix: Arc::clone(&self.read_mix),
            }),
        }
    }
}

impl Version {
    pub(crate) fn encode_into(
        &self,
        writer: &mut crate::manifest_blocks::writer::ManifestArchiveWriter,
        comparator_name: &str,
    ) -> Result<(), crate::Error> {
        use crate::FormatVersion;
        #[cfg(not(feature = "std"))]
        use crate::io::Write;
        use crate::io::{LittleEndian, WriteBytesExt};
        #[cfg(feature = "std")]
        use std::io::Write;

        //
        // Manifest
        //

        writer.start("format_version")?;
        writer.write_u8(FormatVersion::V6.into())?;

        // The snapshot names its own version: recovery reads a snapshot by
        // the id in its file name, and a file holding another version under
        // that name (a copy, a rename gone wrong) must not pass for it.
        writer.start("version_id")?;
        writer.write_u64::<LittleEndian>(self.id())?;

        writer.start("crate_version")?;
        writer.write_all(env!("CARGO_PKG_VERSION").as_bytes())?;

        writer.start("tree_type")?;
        writer.write_u8(self.tree_type.into())?;

        writer.start("level_count")?;
        writer.write_u8(
            u8::try_from(self.level_count())
                .map_err(|_| crate::Error::InvalidHeader("level_count exceeds u8"))?,
        )?;

        writer.start("filter_hash_type")?;
        writer.write_u8(u8::from(ChecksumType::Xxh3))?;

        writer.start("comparator_name")?;
        writer.write_all(comparator_name.as_bytes())?;

        //
        // Levels
        //

        writer.start("tables")?;

        // Shared scratch buffer for per-record framing payloads.
        // Reused across every table + blob record in this version,
        // so the framing helper grows the buffer once and reuses it
        // — no per-record heap allocation. The records are all
        // <= 33 bytes today; pre-sized to cover the larger of the
        // two so the first iteration doesn't trigger a realloc.
        let mut framing_scratch: Vec<u8> = Vec::with_capacity(64);

        // The number of levels the layout spans, then one framed record per
        // table, in level, run and position order, each naming its own level
        // and run; the section holds no run or table counts (see
        // `recovery::TABLE_ENTRY_PAYLOAD_LEN`).
        writer.write_u8(
            u8::try_from(self.level_count())
                .map_err(|_| crate::Error::InvalidHeader("level_count exceeds u8"))?,
        )?;
        for (level_idx, level) in self.iter_levels().enumerate() {
            let level_no = u8::try_from(level_idx)
                .map_err(|_| crate::Error::InvalidHeader("tables: level index exceeds u8"))?;
            for (run_idx, run) in level.iter().enumerate() {
                let place = recovery::TablePlace {
                    level: level_no,
                    run: u32::try_from(run_idx).map_err(|_| {
                        crate::Error::InvalidHeader("tables: run index exceeds u32")
                    })?,
                };
                for table in run.iter() {
                    framing::write_framed_record(writer, &mut framing_scratch, |payload| {
                        recovery::encode_table_entry_payload(
                            payload,
                            place,
                            table.id(),
                            table.checksum(),
                            table.global_seqno(),
                            table.l0_recency(),
                        )
                    })?;
                }
            }
        }

        writer.start("blob_files")?;

        // Blob file count
        #[expect(
            clippy::cast_possible_truncation,
            reason = "there are always less than 4 billion blob files"
        )]
        writer.write_u32::<LittleEndian>(self.blob_files.len() as u32)?;

        for file in self.blob_files.iter() {
            // Per-blob record framed, same rationale as tables
            // above; same scratch buffer keeps allocations at zero
            // across the section boundary.
            framing::write_framed_record(writer, &mut framing_scratch, |payload| {
                payload.write_u64::<LittleEndian>(file.id())?;
                payload.write_u8(0)?; // Checksum type, 0 = XXH3
                payload.write_u128::<LittleEndian>(file.0.checksum.into_u128())?;
                Ok(())
            })?;
        }

        writer.start("blob_gc_stats")?;

        self.gc_stats.encode_into(writer)?;

        // Tight-space restrictions: per-table key-range lower bounds for tables
        // whose prefix has been punched out and superseded by a merged output.
        // Empty (count 0) on versions that never ran tight-space reclaim, so the
        // section is one zero count in the common case. Each entry is the table
        // id then a length-prefixed key (variable length, hence its own section
        // rather than the fixed-length per-table `tables` record).
        writer.start("restrictions")?;
        let restricted: Vec<(TableId, crate::UserKey)> = self
            .iter_tables()
            .filter_map(|t| t.restrict_lower_bound().map(|b| (t.id(), b.clone())))
            .collect();
        writer.write_u32::<LittleEndian>(
            u32::try_from(restricted.len()).map_err(|_| crate::Error::Unrecoverable)?,
        )?;
        for (id, key) in &restricted {
            writer.write_u64::<LittleEndian>(*id)?;
            writer.write_u32::<LittleEndian>(
                u32::try_from(key.len()).map_err(|_| crate::Error::Unrecoverable)?,
            )?;
            writer.write_all(key)?;
        }

        // The blob analogue: per-blob-file live-data frontiers for files whose
        // consumed prefix was reclaimed in place. Fixed-width entries (id then
        // offset) — a blob frontier is a byte position, not a key. Empty on
        // versions that never reclaimed a blob prefix.
        //
        // No format bump for this (or the `restrictions`) section: a snapshot
        // carries a non-empty entry only once tight-space reclaim has RUN, and
        // opening such a tree with a binary that predates the feature is a
        // DOWNGRADE — outside the support contract (consumers pin the exact
        // crate version and move in lockstep with the engine). Trees that
        // never reclaimed write an empty section and stay fully readable
        // either way, matching the append-only edit-log treatment of the same
        // data.
        writer.start("blob_restrictions")?;
        let blob_restricted: Vec<(crate::vlog::BlobFileId, u64)> = self
            .blob_files
            .iter()
            .filter(|bf| bf.live_data_start() > 0)
            .map(|bf| (bf.id(), bf.live_data_start()))
            .collect();
        writer.write_u32::<LittleEndian>(
            u32::try_from(blob_restricted.len()).map_err(|_| crate::Error::Unrecoverable)?,
        )?;
        for (id, frontier) in &blob_restricted {
            writer.write_u64::<LittleEndian>(*id)?;
            writer.write_u64::<LittleEndian>(*frontier)?;
        }

        // The retention floor: the highest snapshot a reopen may no longer
        // serve (see `VersionInner::retention_floor`). Its own section so a
        // reader that predates it simply does not find it (and serves every
        // snapshot, as before), and a snapshot written before it existed
        // recovers as floor 0: the same optional-section treatment as the
        // restrictions above.
        writer.start("retention_floor")?;
        writer.write_u64::<LittleEndian>(self.retention_floor)?;

        // The dictionary ids this version's tables may reference. Written only
        // when the tree holds any, so a tree that compresses against none adds
        // no section and encodes exactly as it did before; the same optional
        // treatment as the two sections above. Ids only: the bytes are in
        // `dicts/`, keyed by these.
        if !self.dicts.is_empty() {
            writer.start("dicts")?;
            writer.write_u32::<LittleEndian>(
                u32::try_from(self.dicts.len()).map_err(|_| crate::Error::Unrecoverable)?,
            )?;
            for id in self.dicts.iter() {
                writer.write_u32::<LittleEndian>(*id)?;
            }
        }

        // The report of a repair the tree still owes, written only while it
        // owes one: the snapshot holds it, so it is there exactly when this
        // snapshot is installed.
        if let Some(report) = &self.repair_report {
            writer.start("repair_report")?;
            writer.write_all(report)?;
        }

        Ok(())
    }
}

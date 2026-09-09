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
pub use persist::persist_version;
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

use optimize::optimize_runs;
use run::Ranged;

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

/// Monotonically increasing ID of a version.
pub type VersionId = u64;

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
    /// manifest, so a reopened tree refuses those snapshots instead of
    /// answering from data they never saw. `0` until the first such install.
    retention_floor: crate::SeqNo,

    /// Ids of the compression dictionaries this version's tables may reference,
    /// ascending. The bytes live in `dicts/`; this is what says which of them
    /// the tree still owes a reader.
    ///
    /// Versioned rather than tree-global for the same reason tables are: a
    /// dictionary is reachable exactly while some retained version references
    /// it, so the file can be removed once no version does, and a recovery that
    /// lands on an older version still finds what that version's tables were
    /// written against.
    ///
    /// Not gated on the compression features. A build without them cannot
    /// resolve these ids, but it must carry the list forward across its own
    /// installs, or reopening with zstd again would find tables whose
    /// dictionary the tree has forgotten.
    dicts: Arc<[crate::file::DictId]>,
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

    /// Registers `id` with this version, returning the extended version.
    ///
    /// Idempotent: an id already present returns this version unchanged, since
    /// the id is derived from the dictionary's own bytes and cannot name two
    /// different dictionaries.
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
        self.with_dicts(dicts)
    }

    /// Drops `id` from this version, returning the reduced version. An id this
    /// version does not hold returns it unchanged.
    ///
    /// Dropping the id is what makes the file collectable; the bytes are
    /// removed separately, once no retained version holds the id any more.
    #[must_use]
    pub fn without_dict(&self, id: crate::file::DictId) -> Self {
        if !self.dicts.contains(&id) {
            return self.clone();
        }
        let dicts = self
            .dicts
            .iter()
            .copied()
            .filter(|held| *held != id)
            .collect::<Vec<_>>();
        self.with_dicts(dicts)
    }

    /// This version with `dicts` in place of its own list.
    ///
    /// A REBUILT version (manifest repair) also sets the list this way, since
    /// it starts empty and the ids have to be derived from the recovered files.
    #[must_use]
    pub(crate) fn with_dicts(&self, dicts: Vec<crate::file::DictId>) -> Self {
        Self {
            inner: Arc::new(VersionInner {
                id: self.id,
                tree_type: self.tree_type,
                levels: self.levels.clone(),
                blob_files: self.blob_files.clone(),
                gc_stats: self.gc_stats.clone(),
                retention_floor: self.retention_floor,
                dicts: Arc::from(dicts),
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
            }),
        }
    }

    pub(crate) fn from_recovery(
        recovery: Recovery,
        tables: &[Table],
        blob_files: &[BlobFile],
    ) -> crate::Result<Self> {
        let version_levels = recovery
            .table_ids
            .iter()
            .map(|level| {
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

        // The recovered set is what the manifest recorded, so it is adopted
        // whole rather than folded in one id at a time.
        Ok(recovered.with_dicts(recovery.dicts))
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

            let runs = optimize_runs(runs, comparator);

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
            }),
        }
    }

    /// Returns a new version with a list of tables removed.
    ///
    /// The table files are not immediately deleted, this is handled by the version system's free list.
    pub fn with_dropped(
        &self,
        ids: &[TableId],
        dropped_blob_files: &mut Vec<BlobFile>,
        ctx: &TransformContext<'_>,
    ) -> crate::Result<Self> {
        let comparator = ctx.comparator;
        let id = self.id + 1;

        let mut levels = vec![];

        let mut dropped_tables: Vec<Table> = vec![];

        for level in &self.levels {
            let runs = level
                .runs
                .iter()
                .map(|run| {
                    // TODO: don't clone Arc inner if we don't need to modify
                    let mut run: Run<_> = run.deref().clone();

                    let removed_tables = run
                        .inner_mut()
                        .extract_if(.., |x| ids.contains(&x.metadata.id));

                    dropped_tables.extend(removed_tables);

                    run
                })
                .filter(|x| !x.is_empty())
                .collect::<Vec<_>>();

            let runs = optimize_runs(runs, comparator);

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
            let mut copy = self.blob_files.deref().clone();
            dropped_blob_files.extend(copy.prune_dead(&gc_stats));
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
            let mut runs = level
                .runs
                .iter()
                .map(|run| {
                    // TODO: don't clone Arc inner if we don't need to modify
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
                    // NOTE: dest_level == 0 in with_merge only occurs for intra-L0
                    // compaction (memtable flushes use with_new_l0_run, not with_merge).
                    // Append the merged (older) run so that any concurrently flushed
                    // (newer) runs remain at the front and are searched first during
                    // point reads.
                    runs.push(run);
                } else {
                    runs.insert(0, run);
                }
            }

            let runs = optimize_runs(runs, comparator);

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

        let affected_tables = self
            .iter_tables()
            .filter(|x| ids.contains(&x.id()))
            .cloned()
            .collect::<Vec<_>>();

        assert_eq!(affected_tables.len(), ids.len(), "invalid table IDs");

        let mut levels = vec![];

        for (level_idx, level) in self.levels.iter().enumerate() {
            let mut runs = level
                .runs
                .iter()
                .map(|run| {
                    // TODO: don't clone Arc inner if we don't need to modify
                    let mut run: Run<_> = run.deref().clone();
                    run.retain(|x| !ids.contains(&x.metadata.id));
                    run
                })
                .filter(|x| !x.is_empty())
                .collect::<Vec<_>>();

            if level_idx == dest_level
                && let Some(run) = Run::new(affected_tables.clone())
            {
                runs.insert(0, run);
            }

            let runs = optimize_runs(runs, comparator);

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

            let runs = optimize_runs(runs, comparator);

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
        writer.write_u8(FormatVersion::V5.into())?;

        writer.start("crate_version")?;
        writer.write_all(env!("CARGO_PKG_VERSION").as_bytes())?;

        writer.start("tree_type")?;
        writer.write_u8(self.tree_type.into())?;

        writer.start("level_count")?;
        #[expect(
            clippy::cast_possible_truncation,
            reason = "level count is bounded by 255"
        )]
        writer.write_u8(self.level_count() as u8)?;

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

        // Per-record framing details live in src/version/framing.rs.
        // Top-level shape inside the `tables` section after framing:
        //   level_count: u8
        //   for each level:
        //     run_count: u8
        //     for each run:
        //       table_count: u32 LE
        //       for each table:
        //         FRAMED(table_record_payload)   // 12-byte header + payload
        //
        // The level / run / table_count counters stay unframed
        // because they ARE the section's own structural shape — the
        // pre-framing readers used them to walk the section and the
        // framing-aware readers continue to use them the same way.
        // Only the per-table record bytes (the 33-byte
        // id+checksum_type+checksum+global_seqno payload) become
        // framed so PointInTimeRecovery / SkipAnyCorruptedRecords
        // have exact byte boundaries to skip on.

        // Level count
        #[expect(
            clippy::cast_possible_truncation,
            reason = "there are always less than 256 levels"
        )]
        writer.write_u8(self.level_count() as u8)?;

        for level in self.iter_levels() {
            // Run count
            #[expect(
                clippy::cast_possible_truncation,
                reason = "there are always less than 256 runs"
            )]
            writer.write_u8(level.len() as u8)?;

            for run in level.iter() {
                // Table count
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "there are always less than 4 billion tables in a run"
                )]
                writer.write_u32::<LittleEndian>(run.len() as u32)?;

                // Tables — each one framed.
                for table in run.iter() {
                    framing::write_framed_record(writer, &mut framing_scratch, |payload| {
                        payload.write_u64::<LittleEndian>(table.id())?;
                        payload.write_u8(0)?; // Checksum type, 0 = XXH3
                        payload.write_u128::<LittleEndian>(table.checksum().into_u128())?;
                        payload.write_u64::<LittleEndian>(table.global_seqno())?;
                        Ok(())
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

        Ok(())
    }
}

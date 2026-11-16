// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use crate::{
    MAX_SEQNO, SeqNo, SharedSequenceNumberGenerator,
    comparator::SharedComparator,
    fs::{Fs, SyncMode},
    memtable::Memtable,
    tree::sealed::SealedMemtables,
    version::{Version, VersionId, edit_log, persist_version},
};

/// Removes `path`, treating an already-absent file as success — a prior crash
/// (or a racing rotation) may have removed it already.
fn remove_if_present(fs: &dyn Fs, path: &Path) -> crate::Result<()> {
    match fs.remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == crate::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Syncs, once each, the directories holding the tables `next` adds over
/// `prior`: a new table's name is durable only once its directory is, and the
/// manifest edit that names it must not outlive it. The table writers sync
/// only their files, so a transition writing N tables to one directory syncs
/// that directory once, not N times. A directory whose new tables were all
/// written into files whose entries were durable already (pooled files) is
/// not synced for them.
///
/// Under [`SyncMode::Barrier`] a folder on another device than the manifest
/// at `tree_path` is synced in full, pooled entries or not: it is the last
/// sync of the transition on that device, and a barrier orders writes on its
/// own device only, so the manifest edit on the other one could otherwise
/// outlive the tables it names.
fn sync_new_table_directories(
    prior: &Version,
    next: &Version,
    tree_path: &Path,
    manifest_fs: &dyn Fs,
    sync_mode: SyncMode,
) -> crate::Result<()> {
    let known: crate::HashSet<crate::TableId> = prior.iter_tables().map(crate::Table::id).collect();
    // Each folder holding a new table, with whether it needs a sync.
    let mut folders: Vec<(&Arc<dyn Fs>, &Path, bool)> = Vec::new();
    for table in next.iter_tables() {
        if known.contains(&table.id()) {
            continue;
        }
        let Some(folder) = table.path.parent() else {
            continue;
        };
        let needs_sync = !table.entry_durable();
        if let Some(seen) = folders
            .iter_mut()
            .find(|(fs, at, _)| Arc::ptr_eq(fs, &table.fs) && *at == folder)
        {
            seen.2 |= needs_sync;
        } else {
            folders.push((&table.fs, folder, needs_sync));
        }
    }
    for (fs, folder, needs_sync) in folders {
        let flushes_device =
            sync_mode == SyncMode::Barrier && !same_device(&**fs, folder, manifest_fs, tree_path);
        if flushes_device {
            crate::file::fsync_directory(folder, &**fs, SyncMode::Full)?;
        } else if needs_sync {
            crate::file::fsync_directory(folder, &**fs, sync_mode)?;
        }
    }
    Ok(())
}

/// Whether `path` on `fs` and `other` on `other_fs` provably lie on one
/// device; a backend that cannot tell answers no.
fn same_device(fs: &dyn Fs, path: &Path, other_fs: &dyn Fs, other: &Path) -> bool {
    matches!(
        (fs.volume_id(path), other_fs.volume_id(other)),
        (Some(a), Some(b)) if a == b
    )
}

/// The sync mode the manifest edit from `prior` to `next` is persisted with.
/// Under [`SyncMode::Barrier`] an edit that drops a table or a blob file on
/// another device than the manifest at `tree_path`, or narrows one there (a
/// table's restriction, a blob file's live data start), is synced in full: the
/// file is removed or its consumed prefix punched once the edit is installed,
/// and a barrier on the manifest's device would not keep that change from
/// reaching the file's own device first. The same holds for `kept_outputs`,
/// outputs of an install of unknown outcome that the rotation this edit makes
/// removes.
fn manifest_sync_mode(
    prior: &Version,
    next: &Version,
    kept_outputs: &[crate::compaction::output_ledger::KeptOutputs],
    tree_path: &Path,
    manifest_fs: &dyn Fs,
    sync_mode: SyncMode,
) -> SyncMode {
    if sync_mode != SyncMode::Barrier {
        return sync_mode;
    }
    let elsewhere = |fs: &dyn Fs, path: &Path| {
        path.parent()
            .is_none_or(|folder| !same_device(fs, folder, manifest_fs, tree_path))
    };
    let kept: crate::HashMap<crate::TableId, Option<&crate::UserKey>> = next
        .iter_tables()
        .map(|table| (table.id(), table.restrict_lower_bound()))
        .collect();
    let mutates_table_elsewhere = prior.iter_tables().any(|table| {
        kept.get(&table.id())
            .is_none_or(|&bound| bound != table.restrict_lower_bound())
            && elsewhere(&*table.fs, &table.path)
    });
    let mutates_blob_file_elsewhere = || {
        prior.blob_files.iter().any(|blob_file| {
            next.blob_files
                .get(blob_file.id())
                .is_none_or(|kept| kept.live_data_start() != blob_file.live_data_start())
                && elsewhere(&*blob_file.0.fs, &blob_file.0.path)
        })
    };
    let removes_kept_elsewhere = || {
        kept_outputs
            .iter()
            .flat_map(crate::compaction::output_ledger::KeptOutputs::files)
            .any(|(file_fs, path)| elsewhere(file_fs, path))
    };
    if mutates_table_elsewhere || mutates_blob_file_elsewhere() || removes_kept_elsewhere() {
        SyncMode::Full
    } else {
        sync_mode
    }
}

use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
#[cfg(feature = "std")]
use arc_swap::ArcSwap;

use crate::path::Path;

/// A file a deferred flush wrote and no sync has made durable yet: the
/// install that persists the version naming it syncs it first. Held only
/// until that install, which is also the one that drops a file a compaction
/// consumed, so holding it keeps no file on disk longer.
#[derive(Clone)]
pub(crate) enum UnsyncedFile {
    /// A table.
    Table(crate::Table),
    /// A blob file.
    Blob(crate::vlog::BlobFile),
}

impl UnsyncedFile {
    /// The file's backend and path.
    fn location(&self) -> (&Arc<dyn Fs>, &Path) {
        match self {
            Self::Table(table) => (&table.fs, &table.path),
            Self::Blob(blob_file) => (&blob_file.0.fs, &blob_file.0.path),
        }
    }

    /// Syncs the file's data and size, all a read of it needs.
    fn sync(&self, sync_mode: SyncMode) -> crate::Result<()> {
        let origin = match self {
            Self::Table(_) => crate::fs::WriteOrigin::FlushTable,
            Self::Blob(_) => crate::fs::WriteOrigin::FlushBlob,
        };
        let (fs, path) = self.location();
        let file = fs.open(
            path,
            &crate::fs::FsOpenOptions::new()
                .write(true)
                .write_origin(origin),
        )?;
        crate::fs::FsFile::sync_data_with(&*file, sync_mode)?;
        Ok(())
    }

    /// The file as an edit names it among the files synced alongside it, with
    /// the length it was written to; `None` for a table whose folder is no
    /// UTF-8 path, which the edit cannot name.
    #[cfg(feature = "std")]
    fn alongside(&self) -> crate::Result<Option<super::edit::AlongsideFile>> {
        let (fs, path) = self.location();
        let (kind, id) = match self {
            Self::Table(table) => {
                let Some(folder) = path.parent().and_then(Path::to_str) else {
                    return Ok(None);
                };
                (
                    super::edit::AlongsideKind::Table {
                        folder: folder.into(),
                    },
                    table.id(),
                )
            }
            Self::Blob(blob_file) => (super::edit::AlongsideKind::Blob, blob_file.id()),
        };
        Ok(Some(super::edit::AlongsideFile {
            kind,
            id,
            len: fs.metadata(path)?.len,
        }))
    }

    /// Whether `other` is the same file.
    fn same(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Table(a), Self::Table(b)) => a.id() == b.id(),
            (Self::Blob(a), Self::Blob(b)) => a.id() == b.id(),
            _ => false,
        }
    }

    /// Whether a version naming the tables `tables` and its blob files still
    /// names the file.
    fn named_by(&self, tables: &crate::HashSet<crate::TableId>, version: &Version) -> bool {
        match self {
            Self::Table(table) => tables.contains(&table.id()),
            Self::Blob(blob_file) => version.blob_files.get(blob_file.id()).is_some(),
        }
    }
}

/// Files an install syncs at the same time as the edit naming them.
struct Alongside<'a> {
    /// The files, as the edit names them.
    #[cfg_attr(
        not(feature = "std"),
        expect(
            dead_code,
            reason = "without threads no edit is written beside its files"
        )
    )]
    files: Vec<super::edit::AlongsideFile>,
    /// Syncs them, and the folders of the new tables.
    sync: &'a (dyn Fn() -> crate::Result<()> + Sync),
}

/// Who syncs the files an install adds.
pub(crate) enum NewFiles {
    /// Their writers did, before the install.
    Synced,
    /// None yet: the version is installed in memory only, and the next
    /// install that persists, or [`SuperVersions::persist_current`], syncs
    /// them and writes it to the manifest (a flush under
    /// [`FlushDurability::Deferred`](crate::config::FlushDurability::Deferred)).
    Deferred(Vec<UnsyncedFile>),
    /// The install, at the same time as the manifest edit naming them: the
    /// edit says so, and recovery checks the files before it trusts the edit.
    Alongside(Vec<UnsyncedFile>),
}

/// A super version is a point-in-time snapshot of memtables and a [`Version`] (list of disk files)
#[derive(Clone)]
pub struct SuperVersion {
    /// Active memtable that is being written to
    #[doc(hidden)]
    pub active_memtable: Arc<Memtable>,

    /// Frozen memtables that are being flushed
    pub(crate) sealed_memtables: Arc<SealedMemtables>,

    /// Current tree version
    pub(crate) version: Version,

    pub(crate) seqno: SeqNo,
}

/// The [`SuperVersion`] one point read runs against.
///
/// A point read does not need its own copy of the snapshot: every snapshot
/// above the retention floor is served by the current version, and holding the
/// mirror's `arc-swap` load guard keeps it alive without the history lock or a
/// clone (three `Arc` bumps per call).
///
/// The guard is short-lived by design: held across a long scan it would delay
/// the mirror's writers, so iterators keep cloning, and so does a read a
/// caller suspends for as long as it likes. The mirror needs `arc-swap`, so
/// under no-std a read always clones the current version out of the locked
/// history.
pub(crate) enum SnapshotRef {
    /// The current snapshot, pinned by the mirror's load guard.
    #[cfg(feature = "std")]
    Latest(arc_swap::Guard<Arc<SuperVersion>>),
    /// A snapshot of the read's own.
    Owned(SuperVersion),
}

impl core::ops::Deref for SnapshotRef {
    type Target = SuperVersion;

    fn deref(&self) -> &SuperVersion {
        match self {
            #[cfg(feature = "std")]
            Self::Latest(guard) => guard,
            Self::Owned(version) => version,
        }
    }
}

/// What an installed version does to the snapshots older than it, which is
/// what decides whether the install raises the persisted retention floor
/// (see [`Version::retention_floor`]).
///
/// The floor is the read boundary, live and after a reopen alike: every
/// snapshot above it is answered from the latest version, so an install that
/// loses what an older snapshot read has to raise it, or that snapshot would be
/// answered from data that is gone.
#[derive(Clone, Copy, Debug)]
pub(crate) enum RetentionEffect {
    /// Every snapshot the prior version served stays servable after a
    /// reopen: the install only adds or moves data (flush, ingest, trivial
    /// move, checksum refresh, blob relocation).
    Keep,
    /// The install ran MVCC garbage collection below this watermark: versions
    /// a snapshot below it depended on may have been dropped, so after a
    /// reopen every snapshot below the watermark (capped at the install's own
    /// seqno) is unservable. `0` means no GC ran and is the same as
    /// [`Self::Keep`].
    GcBelow(SeqNo),
    /// The install physically removes or rewrites user-visible data
    /// regardless of any watermark (a `clear`, a table drop, a compaction
    /// whose filter transformed rows): every snapshot at or below the
    /// install's own seqno is unservable after a reopen.
    DropsData,
}

impl RetentionEffect {
    /// What an install owes older snapshots, from what its run actually did.
    ///
    /// Every install that runs a merge stream derives its effect here rather
    /// than restating the rule: a flush and a compaction disagreeing about it
    /// is how a floor came to promise data the output no longer held.
    ///
    /// `collected_below_watermark` says the run lost or rewrote a version
    /// because of `watermark`. Deriving the effect from the watermark at all
    /// rests on an invariant the callers uphold: everything they report
    /// happens STRICTLY BELOW the watermark. (The floor this yields is safe,
    /// not tight: the report is one boolean with no seqno in it, so a run that
    /// collected at seqno 10 under a watermark of 100 still refuses up to the
    /// cap. Refusing more than was lost costs an error; refusing less would
    /// answer from data that is gone.) The fold, merge
    /// resolution, weak-delete annihilation and bottom-level eviction are each
    /// gated on the head's own seqno; applied range tombstones are strictly
    /// below it and so are the entries they cover; bottommost seqno zeroing
    /// rewrites only below it. A run that reports a loss at or above the
    /// watermark would break that and needs `DropsData`, not this.
    ///
    /// A user compaction filter is the one thing that acts regardless of any
    /// watermark, since a removal at watermark 0 still drops the row, so its
    /// verdict outranks everything else.
    pub(crate) fn of_run(
        filter_transformed: bool,
        collected_below_watermark: bool,
        watermark: SeqNo,
    ) -> Self {
        if filter_transformed {
            Self::DropsData
        } else if collected_below_watermark {
            Self::GcBelow(watermark)
        } else {
            Self::Keep
        }
    }
}

/// The tree's current [`SuperVersion`], and the manifest persistence that
/// installs its successors.
///
/// Only the latest version is kept. Every snapshot above the retention floor
/// is answered from it, since the compaction folds keep every key version such
/// a snapshot reads and every loss they report raises the floor; an older
/// version is needed only by a reader that already resolved it, and that
/// reader holds its own clone, which keeps its tables on disk until it drops.
pub struct SuperVersions {
    current: SuperVersion,

    /// The version the manifest on disk holds: `current`'s, unless deferred
    /// flushes installed versions after it in memory only. Every install
    /// that persists writes the change from this one.
    persisted: Version,

    /// The files deferred flushes wrote since the last persist and no sync
    /// has made durable: the persist that next names them syncs them first.
    unsynced: Vec<UnsyncedFile>,

    /// Stable comparator identity persisted in every version file.
    comparator_name: Arc<str>,

    /// Durability level (`Config::sync_mode`) applied to every manifest /
    /// version persist this history performs. Immutable for the tree's life.
    sync_mode: SyncMode,

    /// Whether the backend syncs files one at a time
    /// ([`FsCapabilities::serial_syncs`](crate::fs::FsCapabilities::serial_syncs)),
    /// asked at the first persist: the backend does not change under a tree.
    serial_syncs: Option<bool>,

    /// Version id of the installed on-disk snapshot. Each version upgrade
    /// appends a [`VersionEdit`](crate::version::edit::VersionEdit) to the log
    /// `edits-{snapshot_id}` instead of rewriting the whole manifest; once that
    /// log grows past [`Self::log_rotate_bytes`] the next upgrade rotates —
    /// writes and installs a fresh snapshot with an empty log — and this id
    /// advances to the new snapshot's.
    snapshot_id: VersionId,

    /// Edit-log size (bytes) past which the next upgrade rotates instead of
    /// appending (`Config::manifest_log_rotate_bytes`, default 1 MiB). Immutable
    /// for the tree's life.
    log_rotate_bytes: u64,

    /// Cached size of the current `edits-{snapshot_id}` log in bytes. `None`
    /// until first measured (a recovered log may be non-empty) or after an
    /// append error left the on-disk size uncertain. Kept exact by adding each
    /// appended record's size: every install runs under the version write
    /// lock, so this history is the log's only writer. Saves an `open` +
    /// `seek` syscall pair on every flush / compaction install.
    log_bytes: Option<u64>,

    /// Whether an install failed since the last rotation that succeeded. A
    /// failed rotation may still have installed its snapshot on disk (its
    /// rename made before the failure can survive), and recovery would then
    /// replay that snapshot's log, not the current one: an edit appended to
    /// the current log would be lost. A failed append may have left its record
    /// in the log, which the next append's sync would make durable. So the
    /// next persist rotates, writing a snapshot of the whole state that
    /// supersedes either outcome.
    rotation_pending: bool,

    /// Outputs installs of unknown outcome left on disk. Holding one sets
    /// [`Self::rotation_pending`]: the next rotation that succeeds supersedes
    /// every edit that may name them, and removes them.
    kept_outputs: Vec<crate::compaction::output_ledger::KeptOutputs>,

    /// Reusable payload-assembly buffer for edit appends — the scratch
    /// `edit_log::append_edit` documents as reusable; allocating it per
    /// install defeated that.
    edit_scratch: Vec<u8>,

    /// Lock-free mirror of the current `SuperVersion`, shared with the `Tree`
    /// so a read can load it without taking the history `RwLock`. Kept in
    /// sync under the same write lock at every site that changes the current
    /// version (construction, [`append_version`](Self::append_version),
    /// [`replace_latest_version`](Self::replace_latest_version)). Recent
    /// inserts remain visible through it because they mutate the shared
    /// `active_memtable` behind a stable `Arc`; the current version only
    /// changes on rotation, flush and compaction.
    ///
    /// `std`-only: `arc-swap` is not `#![no_std]`. A no-std build (where
    /// `SuperVersions` is already std-bound for other reasons) simply does
    /// without the lock-free mirror.
    #[cfg(feature = "std")]
    latest: Arc<ArcSwap<SuperVersion>>,

    /// The current version's L0 run count, written at every publish: a writer
    /// pacing itself against L0 reads it with one load, without the history
    /// lock or a version clone, whose cache lines the readers share.
    l0_runs: Arc<crate::memtable::own_line::OwnLine<core::sync::atomic::AtomicUsize>>,

    /// What inserts hold instead of the history lock, and where they get the
    /// active memtable from: the gate's own copy, not [`Self::latest`]. Only
    /// [`Self::publish`] replaces it, through a closed gate, so no insert lands
    /// in a memtable once it is sealed; `latest` is the readers' mirror.
    #[cfg(feature = "std")]
    insert_gate: Arc<super::insert_gate::InsertGate>,

    /// The tree's filter budget, when it allocates filter memory by probe
    /// load: every version this history publishes updates the filter bytes
    /// it holds.
    filter_budget: Option<Arc<crate::filter_budget::FilterBudget>>,

    /// The blob objects installs released, for the references reads handed
    /// out before them. Reads register under this history's read lock and
    /// installs record under its write lock.
    released: Arc<crate::blob_tree::released::ReleasedObjects>,

    /// Fired by an install after its seqno is taken and before the version is
    /// persisted, so a test can act inside that window.
    #[cfg(test)]
    pub(crate) before_persist: Option<Box<dyn Fn() + Send + Sync>>,
}

/// The runs `version` holds in L0.
fn l0_run_count(version: &Version) -> usize {
    version.level(0).map_or_default(|level| level.run_count())
}

impl SuperVersions {
    /// Builds the in-memory version history. `snapshot_id` is the version id of
    /// the installed on-disk snapshot, whose log the persist that installed it
    /// made durable by name: `version.id()`, since a create and an open both
    /// install the version they start from.
    ///
    /// The seed version's seqno is the recovered
    /// [`retention_floor`](Version::retention_floor), below which the data a
    /// snapshot saw may be gone (`0` on a fresh create).
    pub fn new(
        version: Version,
        comparator: &SharedComparator,
        sync_mode: SyncMode,
        snapshot_id: VersionId,
        log_rotate_bytes: u64,
    ) -> Self {
        let comparator_name: Arc<str> = comparator.name().into();

        let initial = SuperVersion {
            active_memtable: Arc::new(Memtable::new(0, comparator.clone())),
            sealed_memtables: Arc::default(),
            seqno: version.retention_floor(),
            version: version.clone(),
        };

        Self {
            l0_runs: Arc::new(crate::memtable::own_line::OwnLine(
                core::sync::atomic::AtomicUsize::new(l0_run_count(&initial.version)),
            )),
            #[cfg(feature = "std")]
            latest: Arc::new(ArcSwap::from_pointee(initial.clone())),
            #[cfg(feature = "std")]
            insert_gate: Arc::new(super::insert_gate::InsertGate::new(Arc::clone(
                &initial.active_memtable,
            ))),
            current: initial,
            persisted: version,
            unsynced: Vec::new(),
            comparator_name,
            sync_mode,
            serial_syncs: None,
            snapshot_id,
            log_rotate_bytes,
            log_bytes: None,
            rotation_pending: false,
            kept_outputs: Vec::new(),
            edit_scratch: Vec::new(),
            filter_budget: None,
            released: Arc::default(),
            #[cfg(test)]
            before_persist: None,
        }
    }

    /// The blob objects installs released, for the references reads handed
    /// out before them (see [`crate::blob_tree::released`]).
    pub(crate) fn released(&self) -> &Arc<crate::blob_tree::released::ReleasedObjects> {
        &self.released
    }

    /// Holds `kept`, the outputs of a flush or compaction whose install had an
    /// unknown outcome, until a rotation removes them, whichever install
    /// makes it.
    ///
    /// The next install rotates even when another one rotated since the
    /// failure: only a rotation that counts these files syncs its snapshot in
    /// full when they lie on another device than the manifest, so their
    /// removal cannot reach that device before the snapshot superseding the
    /// failed edit is durable.
    ///
    /// The removal runs under the history's lock. It is rare, after a failed
    /// install only, and what it takes (the descriptor cache, the space pool)
    /// never waits on the history.
    pub(crate) fn keep_outputs(&mut self, kept: crate::compaction::output_ledger::KeptOutputs) {
        self.rotation_pending = true;
        self.kept_outputs.push(kept);
    }

    /// Makes every version this history publishes, the current one first,
    /// update `budget`'s held filter bytes.
    #[must_use]
    pub(crate) fn with_filter_budget(
        mut self,
        budget: Arc<crate::filter_budget::FilterBudget>,
    ) -> Self {
        budget.publish(&self.current.version);
        self.filter_budget = Some(budget);
        self
    }

    /// Modifies the level manifest atomically.
    ///
    /// The function accepts a transition function that receives the current version
    /// and returns a new version.
    ///
    /// The function takes care of persisting the version changes on disk.
    /// `retention` names what the install does to older snapshots; a GC
    /// compaction or a data drop raises the persisted retention floor with
    /// the same version edit (see [`RetentionEffect`]).
    // Takes &SharedSequenceNumberGenerator (not &dyn SequenceNumberGenerator)
    // because Config stores Arc<dyn ...> and all callers already have that type.
    #[expect(
        clippy::too_many_arguments,
        reason = "version upgrade threads tree_path, mutator closure, two seqno gens, fs, \
                  runtime, encryption, retention effect: every parameter is load-bearing \
                  per the manifest-persist contract"
    )]
    pub(crate) fn upgrade_version<F: FnOnce(&SuperVersion) -> crate::Result<SuperVersion>>(
        &mut self,
        tree_path: &Path,
        f: F,
        seqno: &SharedSequenceNumberGenerator,
        visible_seqno: &SharedSequenceNumberGenerator,
        fs: &dyn Fs,
        runtime: Arc<crate::runtime_config::RuntimeConfig>,
        encryption: Option<Arc<dyn crate::encryption::EncryptionProvider>>,
        retention: RetentionEffect,
    ) -> crate::Result<()> {
        self.install(
            tree_path,
            f,
            || seqno.next(),
            visible_seqno,
            fs,
            runtime,
            encryption,
            retention,
            NewFiles::Synced,
        )
    }

    /// Like `upgrade_version`, but takes an already-allocated sequence number.
    ///
    /// This is useful when the seqno must be coordinated with other operations
    /// (e.g., bulk ingestion where tables are recovered with the same seqno).
    #[expect(
        clippy::too_many_arguments,
        reason = "version upgrade with pre-allocated seqno: tree_path, mutator, seqno, \
                  visible_seqno, fs, runtime, encryption, retention effect; same \
                  load-bearing surface as the auto-allocating sibling above"
    )]
    pub(crate) fn upgrade_version_with_seqno<
        F: FnOnce(&SuperVersion) -> crate::Result<SuperVersion>,
    >(
        &mut self,
        tree_path: &Path,
        f: F,
        seqno: SeqNo,
        visible_seqno: &SharedSequenceNumberGenerator,
        fs: &dyn Fs,
        runtime: Arc<crate::runtime_config::RuntimeConfig>,
        encryption: Option<Arc<dyn crate::encryption::EncryptionProvider>>,
        retention: RetentionEffect,
    ) -> crate::Result<()> {
        self.install(
            tree_path,
            f,
            || seqno,
            visible_seqno,
            fs,
            runtime,
            encryption,
            retention,
            NewFiles::Synced,
        )
    }

    /// Like `upgrade_version`, with `files` saying who syncs the files the
    /// new version adds (see [`NewFiles`]).
    #[expect(
        clippy::too_many_arguments,
        reason = "the same load-bearing surface as `upgrade_version`, plus the new files"
    )]
    pub(crate) fn upgrade_version_adding<
        F: FnOnce(&SuperVersion) -> crate::Result<SuperVersion>,
    >(
        &mut self,
        tree_path: &Path,
        f: F,
        seqno: &SharedSequenceNumberGenerator,
        visible_seqno: &SharedSequenceNumberGenerator,
        fs: &dyn Fs,
        runtime: Arc<crate::runtime_config::RuntimeConfig>,
        encryption: Option<Arc<dyn crate::encryption::EncryptionProvider>>,
        retention: RetentionEffect,
        files: NewFiles,
    ) -> crate::Result<()> {
        self.install(
            tree_path,
            f,
            || seqno.next(),
            visible_seqno,
            fs,
            runtime,
            encryption,
            retention,
            files,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the install's own surface: see `upgrade_version_with_seqno`"
    )]
    fn install<F: FnOnce(&SuperVersion) -> crate::Result<SuperVersion>>(
        &mut self,
        tree_path: &Path,
        f: F,
        // Taken only once the next version is known: see the insert gate below.
        seqno: impl FnOnce() -> SeqNo,
        visible_seqno: &SharedSequenceNumberGenerator,
        fs: &dyn Fs,
        runtime: Arc<crate::runtime_config::RuntimeConfig>,
        encryption: Option<Arc<dyn crate::encryption::EncryptionProvider>>,
        retention: RetentionEffect,
        files: NewFiles,
    ) -> crate::Result<()> {
        let prior = self.latest_version();
        let mut next_version = f(&prior)?;
        // A version that replaces the active memtable takes its seqno with the
        // insert gate closed, and keeps it closed until it is published: an
        // insert either took its seqno before (it is in the replaced memtable,
        // covered by this install) or waits and lands in the new one. Open,
        // an insert with a later seqno could land in the replaced memtable
        // and be dropped with it, above the floor a clear raises. The `Arc`
        // clone lets the guard outlive the `&mut self` persist below.
        #[cfg(feature = "std")]
        let gate = (!Arc::ptr_eq(&prior.active_memtable, &next_version.active_memtable))
            .then(|| Arc::clone(&self.insert_gate));
        #[cfg(feature = "std")]
        let closed = gate.as_deref().map(super::insert_gate::InsertGate::close);
        // Install seqnos are non-decreasing. The counter is caller-owned, and a
        // deployment that reopens with it reset would otherwise install below
        // the current version: a drop or a filtering compaction there raises
        // the floor only to that smaller seqno, leaving snapshots above it
        // answered by the current version without the rows it removed. Under
        // the seqno contract (monotone counter) this clamp is a no-op.
        let seqno = seqno().max(prior.seqno);
        // Every install is a transition to a new id. A rotation persists the
        // installed version as a fresh `v{id}` snapshot, created exclusively,
        // so one under the prior id would fail on the snapshot that id names.
        debug_assert!(
            next_version.version.id() > prior.version.id(),
            "an install must advance the version id ({} -> {})",
            prior.version.id(),
            next_version.version.id(),
        );
        // A checkpoint carries only the dictionaries a version registers, so a
        // file naming one the version does not register would travel without it.
        debug_assert!(
            next_version
                .version
                .referenced_dicts()
                .iter()
                .all(|id| next_version.version.dicts().contains(id)),
            "every dictionary a file references is registered ({:?} of {:?})",
            next_version.version.referenced_dicts(),
            next_version.version.dicts(),
        );
        next_version.seqno = seqno;
        log::trace!("Next version seqno={}", next_version.seqno);

        // Raise the persisted retention floor for an install that discards
        // what older snapshots saw; it rides in the same version edit, so a
        // crash cannot separate the data loss from the boundary that records
        // it. A GC watermark `w` invalidates every snapshot below `w`, i.e.
        // the floor (highest unservable snapshot) is `w - 1`; a data drop
        // invalidates everything up to and including the install itself.
        //
        // Neither exceeds the install's own seqno: a snapshot taken after the
        // install (any seqno above it, since the counter is monotone) sees the
        // installed, complete data. A watermark far above the counter, such
        // as `SeqNo::MAX` to collect all existing history, must therefore not
        // push the floor past the install, or every snapshot the counter can
        // still hand out would be refused after a reopen.
        let floor = match retention {
            RetentionEffect::Keep | RetentionEffect::GcBelow(0) => None,
            RetentionEffect::GcBelow(watermark) => Some((watermark - 1).min(seqno)),
            RetentionEffect::DropsData => Some(seqno),
        };
        if let Some(floor) = floor {
            next_version.version.set_retention_floor(floor);
        }

        #[cfg(test)]
        if let Some(hook) = &self.before_persist {
            hook();
        }
        match files {
            // The floor rides in the version too: lost with it in a crash,
            // together with the flush whose collection raised it.
            NewFiles::Deferred(files) => self.unsynced.extend(files),
            NewFiles::Synced => {
                self.persist_version(
                    tree_path,
                    &next_version.version,
                    fs,
                    runtime,
                    encryption,
                    &[],
                )?;
            }
            NewFiles::Alongside(files) => {
                self.persist_version(
                    tree_path,
                    &next_version.version,
                    fs,
                    runtime,
                    encryption,
                    &files,
                )?;
            }
        }
        self.publish(
            next_version,
            #[cfg(feature = "std")]
            closed.as_ref(),
        );
        #[cfg(feature = "std")]
        drop(closed);

        // Clamp to stay below the reserved MSB range.
        let next_visible = seqno.saturating_add(1).min(MAX_SEQNO);
        visible_seqno.fetch_max(next_visible);

        Ok(())
    }

    /// Makes `next` the version on disk: the files deferred flushes left
    /// unsynced that it still names are synced, then the folders of the
    /// tables it adds over the persisted version, then the change from that
    /// version is written. A file a deferred flush wrote and `next` no longer
    /// names (a compaction consumed it) never reaches the manifest and is not
    /// synced.
    ///
    /// `alongside` are files `next` adds that no one has synced yet: they and
    /// the folders of the new tables are synced at the same time as the
    /// change is written, which then says so (see [`Self::persist_change`]).
    fn persist_version(
        &mut self,
        tree_path: &Path,
        next: &Version,
        fs: &dyn Fs,
        runtime: Arc<crate::runtime_config::RuntimeConfig>,
        encryption: Option<Arc<dyn crate::encryption::EncryptionProvider>>,
        alongside: &[UnsyncedFile],
    ) -> crate::Result<()> {
        let persisted = self.persisted.clone();
        let serial = *self
            .serial_syncs
            .get_or_insert_with(|| fs.capabilities(tree_path).serial_syncs);
        if !self.unsynced.is_empty() {
            // Kept until the persist succeeds: a failure leaves them for the
            // next one to sync again.
            Self::sync_files(&self.unsynced_named_by(next), self.sync_mode, serial)?;
        }
        let sync_mode = self.sync_mode;
        let sync_new = || -> crate::Result<()> {
            Self::sync_files(alongside, sync_mode, serial)?;
            sync_new_table_directories(&persisted, next, tree_path, fs, sync_mode)
        };
        // Outputs are kept only while the next persist rotates, which removes
        // them.
        let manifest_mode = manifest_sync_mode(
            &persisted,
            next,
            &self.kept_outputs,
            tree_path,
            fs,
            self.sync_mode,
        );
        // A file the edit cannot name is synced before it, with the rest, and
        // so is every file without threads to sync them beside the edit, or
        // on a backend whose syncs go one at a time.
        #[cfg(feature = "std")]
        let files: Option<Vec<_>> = if serial {
            None
        } else {
            alongside
                .iter()
                .map(UnsyncedFile::alongside)
                .collect::<crate::Result<_>>()?
        };
        #[cfg(not(feature = "std"))]
        let files: Option<Vec<super::edit::AlongsideFile>> = None;
        let alongside_job = match files {
            Some(files) if !files.is_empty() => Some(Alongside {
                files,
                sync: &sync_new,
            }),
            _ => {
                sync_new()?;
                None
            }
        };
        self.persist_change(
            tree_path,
            &persisted,
            next,
            fs,
            runtime,
            encryption,
            manifest_mode,
            alongside_job,
        )?;
        self.unsynced.clear();
        self.persisted = next.clone();
        Ok(())
    }

    /// Syncs `files`, then each folder their blob files lie in, once. A
    /// deferred table's folder is synced by the persist, with the folders of
    /// every table it adds (`sync_new_table_directories`). `serial`: one file
    /// at a time (see [`FsCapabilities::serial_syncs`](crate::fs::FsCapabilities::serial_syncs)).
    pub(crate) fn sync_files(
        files: &[UnsyncedFile],
        sync_mode: SyncMode,
        serial: bool,
    ) -> crate::Result<()> {
        Self::sync_each(files, sync_mode, serial)?;
        let mut blob_folders: Vec<(&Arc<dyn Fs>, &Path)> = Vec::new();
        for file in files {
            let (file_fs, path) = file.location();
            if matches!(file, UnsyncedFile::Blob(_))
                && let Some(folder) = path.parent()
                && !blob_folders
                    .iter()
                    .any(|(fs, at)| Arc::ptr_eq(fs, file_fs) && *at == folder)
            {
                blob_folders.push((file_fs, folder));
            }
        }
        for (fs, folder) in blob_folders {
            let mode = crate::file::folder_sync_mode(&**fs, folder, sync_mode);
            crate::file::fsync_directory(folder, &**fs, mode)?;
        }
        Ok(())
    }

    /// The version the manifest on disk holds: the current one, unless
    /// deferred flushes installed versions after it in memory only.
    pub(crate) fn persisted_version(&self) -> &Version {
        &self.persisted
    }

    /// Syncs every file of `files`, concurrently: syncs of separate files on
    /// one file system are committed together (a journaling file system
    /// folds them into one journal commit), so a barrier over many deferred
    /// flushes waits about as long as for one, where one after another each
    /// would wait its own commit out.
    #[cfg(feature = "std")]
    fn sync_each(files: &[UnsyncedFile], sync_mode: SyncMode, serial: bool) -> crate::Result<()> {
        /// Syncs in flight at once: enough for a journal commit to fold a
        /// barrier's worth of them, few enough to stay a handful of threads.
        const IN_FLIGHT: usize = 16;

        if serial || files.len() == 1 {
            for file in files {
                file.sync(sync_mode)?;
            }
            return Ok(());
        }
        let next = core::sync::atomic::AtomicUsize::new(0);
        let failed: parking_lot::Mutex<Option<crate::Error>> = parking_lot::Mutex::new(None);
        std::thread::scope(|scope| {
            for _ in 0..files.len().min(IN_FLIGHT) {
                scope.spawn(|| {
                    while let Some(file) =
                        files.get(next.fetch_add(1, core::sync::atomic::Ordering::Relaxed))
                    {
                        if let Err(e) = file.sync(sync_mode) {
                            failed.lock().get_or_insert(e);
                            return;
                        }
                    }
                });
            }
        });
        failed.into_inner().map_or(Ok(()), Err)
    }

    /// Syncs every file of `files`, one after another: no threads without
    /// `std`.
    #[cfg(not(feature = "std"))]
    fn sync_each(files: &[UnsyncedFile], sync_mode: SyncMode, _serial: bool) -> crate::Result<()> {
        for file in files {
            file.sync(sync_mode)?;
        }
        Ok(())
    }

    /// The files deferred flushes left unsynced, for a caller to sync before
    /// it takes the write lock to persist (see [`Self::forget_synced`]).
    pub(crate) fn unsynced_files(&self) -> Vec<UnsyncedFile> {
        self.unsynced.clone()
    }

    /// The files deferred flushes left unsynced that `version` names.
    pub(crate) fn unsynced_named_by(&self, version: &Version) -> Vec<UnsyncedFile> {
        if self.unsynced.is_empty() {
            return Vec::new();
        }
        let tables: crate::HashSet<crate::TableId> =
            version.iter_tables().map(crate::Table::id).collect();
        self.unsynced
            .iter()
            .filter(|file| file.named_by(&tables, version))
            .cloned()
            .collect()
    }

    /// Takes `synced`, which [`Self::sync_files`] made durable, off the files
    /// a persist still has to sync.
    pub(crate) fn forget_synced(&mut self, synced: &[UnsyncedFile]) {
        self.unsynced
            .retain(|file| !synced.iter().any(|done| done.same(file)));
    }

    /// Writes the current version to the manifest when deferred flushes
    /// installed it in memory only; a no-op otherwise.
    ///
    /// # Errors
    ///
    /// Propagates a sync or manifest write error; the current version stays
    /// installed in memory and the next persist retries.
    pub(crate) fn persist_current(
        &mut self,
        tree_path: &Path,
        fs: &dyn Fs,
        runtime: Arc<crate::runtime_config::RuntimeConfig>,
        encryption: Option<Arc<dyn crate::encryption::EncryptionProvider>>,
    ) -> crate::Result<()> {
        if self.persisted.id() == self.current.version.id() {
            return Ok(());
        }
        let next = self.current.version.clone();
        self.persist_version(tree_path, &next, fs, runtime, encryption, &[])
    }

    /// Persists the transition from `prior` to `next` to disk, durably, the
    /// incremental way: append one [`VersionEdit`](crate::version::edit::VersionEdit)
    /// to the current snapshot's log (the common, O(changed-levels) path), or
    /// rotate when that log has grown past [`Self::log_rotate_bytes`] or the
    /// edit is too large for one log record.
    ///
    /// Rotation writes a fresh full snapshot for `next` under a temporary name,
    /// syncs it, and installs it by renaming it (all inside [`persist_version`]);
    /// only after the install is durable does it delete the previous snapshot
    /// file and its log. Crash points: before the rename is durable, the old
    /// snapshot is the highest one and its log is intact (recover old +
    /// replay); once it is, the new snapshot is complete and its log is empty
    /// (recover new, no edits). A torn trailing edit
    /// from an interrupted append is dropped on replay — the operation that wrote
    /// it was never acknowledged upward.
    ///
    /// `sync_mode` is the mode this transition is persisted with (see
    /// `manifest_sync_mode`).
    ///
    /// `alongside` syncs files the transition adds that no one synced yet.
    /// An appended edit runs it at the same time as its own append and sync,
    /// and says so ([`VersionEdit::synced_alongside`](super::edit::VersionEdit::synced_alongside)):
    /// a power loss may keep the edit without the files, and recovery then
    /// checks them and takes the edit back. That edit is the log's last, as
    /// the install returns once both are done and nothing appends before. A
    /// rotation runs it first: a snapshot carries no such mark.
    #[expect(
        clippy::too_many_arguments,
        reason = "the transition, where it goes, how it is encoded and how durably: \
                  every parameter is the install's own"
    )]
    fn persist_change(
        &mut self,
        tree_path: &Path,
        prior: &Version,
        next: &Version,
        fs: &dyn Fs,
        runtime: Arc<crate::runtime_config::RuntimeConfig>,
        encryption: Option<Arc<dyn crate::encryption::EncryptionProvider>>,
        sync_mode: SyncMode,
        mut alongside: Option<Alongside<'_>>,
    ) -> crate::Result<()> {
        let log_path = edit_log::log_path(tree_path, self.snapshot_id);

        // Cached log size when available; measure once otherwise (fresh open
        // with a recovered log, or after an append error of unknown extent).
        let log_size = if let Some(n) = self.log_bytes {
            n
        } else {
            let n = edit_log::log_size(fs, &log_path)?;
            self.log_bytes = Some(n);
            n
        };

        // An owed repair report lives in the snapshot alone, so a transition
        // that changes it is written as one.
        if log_size < self.log_rotate_bytes
            && !self.rotation_pending
            && prior.repair_report() == next.repair_report()
        {
            // Common path: append the delta and fsync. No snapshot rewrite.
            let edit = next.diff(prior)?;
            let appended = match alongside.take() {
                #[cfg(feature = "std")]
                Some(Alongside {
                    files,
                    sync: sync_files,
                }) => {
                    let mut edit = edit;
                    edit.synced_alongside = files;
                    // The mark is positional after the floor and the set.
                    edit.retention_floor
                        .get_or_insert_with(|| next.retention_floor());
                    edit.dicts.get_or_insert_with(|| next.dicts().to_vec());
                    let scratch = &mut self.edit_scratch;
                    let (appended, synced) = std::thread::scope(|scope| {
                        let synced = scope.spawn(sync_files);
                        let appended =
                            edit_log::append_edit(fs, &log_path, &edit, scratch, sync_mode);
                        let synced = synced
                            .join()
                            .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
                        (appended, synced)
                    });
                    match (appended, synced) {
                        // The edit is durable and its files may not be: a later
                        // append would make it a middle edit, replayed
                        // unchecked, so the next persist rotates.
                        (Ok(Some(appended)), Err(e)) => {
                            self.log_bytes = Some(log_size + appended);
                            self.rotation_pending = true;
                            return Err(crate::Error::InstallOutcomeUnknown(
                                alloc::boxed::Box::new(e),
                            ));
                        }
                        // Nothing was written.
                        (Ok(None), Err(e)) => return Err(e),
                        (appended, _) => appended,
                    }
                }
                // Without threads `persist_version` builds none.
                _ => edit_log::append_edit(fs, &log_path, &edit, &mut self.edit_scratch, sync_mode),
            };
            match appended {
                Ok(Some(appended)) => {
                    self.log_bytes = Some(log_size + appended);
                    return Ok(());
                }
                // The edit is too large for one record and nothing was
                // written: rotate below, since a snapshot holds a level of
                // any size. The scratch grew to the oversized payload; give
                // that back so the history does not hold a manifest-sized
                // buffer for the rest of the tree's life.
                Ok(None) => {
                    self.edit_scratch.clear();
                    self.edit_scratch
                        .shrink_to(crate::version::framing::MAX_FRAME_PAYLOAD as usize);
                }
                Err(e) => {
                    // A failed append may have written a record, whole or
                    // partial: the on-disk size is unknown, so drop the cache
                    // and re-measure on the next install. A later append
                    // would land after that record and its sync would make
                    // it durable, so the next install rotates instead.
                    self.log_bytes = None;
                    self.rotation_pending = true;
                    return Err(e);
                }
            }
        }

        // Rotation: write `next` as a fresh full snapshot and install it, once
        // the files it adds are synced.
        if let Some(Alongside {
            sync: sync_files, ..
        }) = alongside
        {
            sync_files()?;
        }
        let old_snapshot = self.snapshot_id;
        if let Err(e) = persist_version(
            tree_path,
            next,
            &self.comparator_name,
            fs,
            runtime,
            encryption,
            sync_mode,
        ) {
            self.rotation_pending = true;
            return Err(e);
        }
        self.rotation_pending = false;
        // The snapshot on disk names none of the kept outputs, and no version
        // in memory ever did.
        for kept in self.kept_outputs.drain(..) {
            kept.remove();
        }
        self.snapshot_id = next.id();
        // The new generation starts with the empty log the persist created,
        // its name durable under the persist's directory sync.
        self.log_bytes = Some(0);

        // The durable commit point of a rotation is the install inside
        // `persist_version` above (its log, synced into the directory) — past
        // it, the rotation has SUCCEEDED. Deleting the old generation's log +
        // snapshot is pure garbage collection, so it is best-effort: a failure
        // here must NOT propagate, or the caller (`upgrade_version_with_seqno`)
        // would skip `append_version` / `fetch_max` and keep stale in-memory
        // state while the new snapshot is installed on disk — an on-disk /
        // in-memory divergence. A leaked old generation is never chosen (its id
        // is lower) and is swept by `cleanup_orphaned_version` on the next open.
        if let Err(e) = remove_if_present(fs, &log_path) {
            log::warn!(
                "rotation: failed to remove old edit log {}: {e}",
                log_path.display()
            );
        }
        if old_snapshot != self.snapshot_id {
            let old_path = tree_path.join(format!("v{old_snapshot}"));
            if let Err(e) = remove_if_present(fs, &old_path) {
                log::warn!(
                    "rotation: failed to remove old snapshot {}: {e}",
                    old_path.display()
                );
            }
        }
        Ok(())
    }

    /// Makes `version` the current one. The replaced version stays alive only
    /// in the readers that already hold it.
    pub fn append_version(&mut self, version: SuperVersion) {
        self.publish(
            version,
            #[cfg(feature = "std")]
            None,
        );
    }

    /// [`Self::append_version`], given the insert gate when the caller
    /// already holds it closed.
    fn publish(
        &mut self,
        version: SuperVersion,
        #[cfg(feature = "std")] closed: Option<&super::insert_gate::Closed<'_>>,
    ) {
        if let Some(budget) = &self.filter_budget {
            budget.publish(&version.version);
        }
        self.l0_runs.store(
            l0_run_count(&version.version),
            core::sync::atomic::Ordering::Release,
        );
        // A new active memtable is handed to inserts with the insert gate
        // closed: the inserts already into the old one finish first, and the
        // ones after land in the new one. The gate is closed here unless the
        // caller holds it closed already.
        #[cfg(feature = "std")]
        let own_closed = if Arc::ptr_eq(&self.current.active_memtable, &version.active_memtable) {
            None
        } else if let Some(closed) = closed {
            closed.set_active(Arc::clone(&version.active_memtable));
            None
        } else {
            let own = self.insert_gate.close();
            own.set_active(Arc::clone(&version.active_memtable));
            Some(own)
        };
        // Mirror it into the lock-free pointer so reads see it without taking
        // the history lock.
        #[cfg(feature = "std")]
        self.latest.store(Arc::new(version.clone()));
        self.current = version;
        #[cfg(feature = "std")]
        drop(own_closed);
    }

    /// Replaces the current version with one that changes only its memtables
    /// (a rotation, a cleared active memtable), which the manifest does not
    /// record.
    pub fn replace_latest_version(&mut self, version: SuperVersion) {
        self.append_version(version);
    }

    /// Returns a handle to the lock-free latest-`SuperVersion` mirror.
    ///
    /// The `Tree` stores a clone of this handle and reads it on the point-read
    /// hot path (`get` at `MAX_SEQNO`) to avoid the history `RwLock`. The
    /// handle stays valid for the tree's lifetime; the pointee is swapped by
    /// [`append_version`](Self::append_version) /
    /// [`replace_latest_version`](Self::replace_latest_version) under the
    /// history write lock.
    ///
    /// Crate-internal: exposing the `ArcSwap` publicly would let a downstream
    /// caller `store()` into it without the version-history write lock,
    /// breaking the "mirror only changes at back-changing sites" invariant.
    ///
    /// `std`-only: the mirror exists only when `arc-swap` is available.
    #[cfg(feature = "std")]
    #[must_use]
    pub(crate) fn latest_handle(&self) -> Arc<ArcSwap<SuperVersion>> {
        Arc::clone(&self.latest)
    }

    /// Returns the current version's L0 run count as every publish writes it
    /// (see `l0_runs`).
    #[must_use]
    pub(crate) fn l0_runs_handle(
        &self,
    ) -> Arc<crate::memtable::own_line::OwnLine<core::sync::atomic::AtomicUsize>> {
        Arc::clone(&self.l0_runs)
    }

    /// Returns the gate inserts pass instead of taking the history lock (see
    /// `insert_gate`).
    #[cfg(feature = "std")]
    #[must_use]
    pub(crate) fn insert_gate_handle(&self) -> Arc<super::insert_gate::InsertGate> {
        Arc::clone(&self.insert_gate)
    }

    pub fn latest_version(&self) -> SuperVersion {
        self.latest_version_ref().clone()
    }

    /// Borrows the newest super-version. This is the write hot path's
    /// accessor: a clone bumps and later drops three `Arc`s (active
    /// memtable, sealed memtables, version), which is pure overhead when
    /// the caller only needs one field for the duration of the lock guard.
    pub fn latest_version_ref(&self) -> &SuperVersion {
        &self.current
    }

    /// Every dictionary id the current version registers.
    ///
    /// The collection boundary: a file may be unlinked only once it is absent
    /// from this set. A reader holding a replaced version needs no file: its
    /// tables and blob files pinned their dictionaries when they were opened.
    #[must_use]
    pub fn registered_dicts(&self) -> Vec<crate::file::DictId> {
        self.current.version.dicts().to_vec()
    }

    /// Resolves the super-version that serves a read at snapshot `seqno`: the
    /// current one, for every snapshot above the retention floor.
    ///
    /// The current version answers such a snapshot as the version installed
    /// just below it would have: a fold keeps every key version a snapshot at
    /// or above its watermark reads (the newest below the watermark included),
    /// and an install that loses or rewrites anything a lower snapshot read
    /// raises the floor past it (see [`RetentionEffect`]). What is not covered
    /// by the floor is therefore never gone, and what is gone is refused. A
    /// reopened tree already reads this way, so a live one and a reopened one
    /// draw the same boundary.
    ///
    /// Snapshot `0` is served: no entry has a seqno below `0`, so nothing is
    /// visible at it whatever the file set.
    ///
    /// # Errors
    ///
    /// [`Error::SnapshotBelowRetention`](crate::Error::SnapshotBelowRetention)
    /// when `0 < seqno <= retention_floor`: an install collected what
    /// the snapshot read, and answering from what survived would return data
    /// the snapshot never saw.
    pub fn get_version_for_snapshot(&self, seqno: SeqNo) -> crate::Result<SuperVersion> {
        self.current.check_serves(seqno)?;
        Ok(self.current.clone())
    }
}

impl SuperVersion {
    /// The retention check every snapshot read goes through: the current
    /// version serves `seqno` iff it is `0` or above its retention floor.
    ///
    /// # Errors
    ///
    /// [`Error::SnapshotBelowRetention`](crate::Error::SnapshotBelowRetention)
    /// when `0 < seqno <= floor`.
    #[inline]
    pub(crate) fn check_serves(&self, seqno: SeqNo) -> crate::Result<()> {
        let floor = self.version.retention_floor();
        // The common case first: a read above the floor.
        if seqno > floor || seqno == 0 {
            Ok(())
        } else {
            Err(below_retention(seqno, floor))
        }
    }

    /// Bytes held by the active and the sealed memtables: the memory flushes
    /// have not yet released.
    pub(crate) fn unflushed_bytes(&self) -> u64 {
        // Each memtable's size is bytes in memory, so the sum is bounded by the
        // process's memory and cannot overflow u64.
        self.active_memtable.size()
            + self
                .sealed_memtables
                .iter()
                .map(|memtable| memtable.size())
                .sum::<u64>()
    }

    /// The memtables flushes have yet to write: the sealed ones, and the
    /// active one unless it holds nothing. Its size says so without its
    /// range-tombstone lock, which `is_empty` takes and a tombstone insert
    /// holds across the tree's comparator.
    #[cfg(feature = "std")]
    pub(crate) fn unflushed_memtables(&self) -> u64 {
        self.sealed_memtables.len() as u64 + u64::from(self.active_memtable.size() > 0)
    }

    /// Whether a cell row in the active or a sealed memtable references
    /// `blob_file_id`: such a row links the file from no table until its
    /// flush, so the file must stay.
    pub(crate) fn memtables_reference_blob_file(
        &self,
        blob_file_id: crate::vlog::BlobFileId,
    ) -> bool {
        self.active_memtable.references_blob_file(blob_file_id)
            || self
                .sealed_memtables
                .iter()
                .any(|memtable| memtable.references_blob_file(blob_file_id))
    }

    /// The blob files cell rows in the active and the sealed memtables
    /// reference: each stays when its last table goes.
    pub(crate) fn memtable_blob_files(&self) -> crate::HashSet<crate::vlog::BlobFileId> {
        let mut files = crate::HashSet::default();
        self.active_memtable.collect_blob_files(&mut files);
        for memtable in self.sealed_memtables.iter() {
            memtable.collect_blob_files(&mut files);
        }
        files
    }
}

/// Kept out of line so the check inlined into every read stays a load and two
/// compares.
#[cold]
#[inline(never)]
fn below_retention(requested: SeqNo, floor: SeqNo) -> crate::Error {
    crate::Error::SnapshotBelowRetention {
        requested,
        oldest_retained: floor,
    }
}

#[cfg(test)]
mod tests;

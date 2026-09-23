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
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
#[cfg(feature = "std")]
use arc_swap::ArcSwap;

use crate::path::Path;

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
/// Short-lived by design: a guard held across a long scan would delay the
/// mirror's writers, so iterators keep cloning; this type serves the point
/// lookups. The mirror needs `arc-swap`, so under no-std the read clones the
/// current version out of the locked history instead.
pub enum SnapshotRef {
    /// The current snapshot, pinned by the mirror's load guard.
    #[cfg(feature = "std")]
    Latest(arc_swap::Guard<Arc<SuperVersion>>),
    /// The current snapshot, cloned out of the locked version history.
    #[cfg(not(feature = "std"))]
    Owned(SuperVersion),
}

impl core::ops::Deref for SnapshotRef {
    type Target = SuperVersion;

    fn deref(&self) -> &SuperVersion {
        match self {
            #[cfg(feature = "std")]
            Self::Latest(guard) => guard,
            #[cfg(not(feature = "std"))]
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
pub enum RetentionEffect {
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

    /// Stable comparator identity persisted in every version file.
    comparator_name: Arc<str>,

    /// Durability level (`Config::sync_mode`) applied to every manifest /
    /// version persist this history performs. Immutable for the tree's life.
    sync_mode: SyncMode,

    /// Version id of the on-disk snapshot the `CURRENT` pointer references. Each
    /// version upgrade appends a [`VersionEdit`](crate::version::edit::VersionEdit)
    /// to the log `edits-{snapshot_id}` instead of rewriting the whole manifest;
    /// once that log grows past [`Self::log_rotate_bytes`] the next upgrade
    /// rotates — writes a fresh snapshot, repoints `CURRENT`, starts an empty
    /// log — and this id advances to the new snapshot's.
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
}

impl SuperVersions {
    /// Builds the in-memory version history. `snapshot_id` is the version id of
    /// the on-disk snapshot `CURRENT` points at — `version.id()` on a fresh
    /// create (the first persist writes that snapshot), or the recovered
    /// snapshot id on open (which may be `< version.id()` when edits were
    /// replayed on top of it).
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
            version,
        };

        Self {
            #[cfg(feature = "std")]
            latest: Arc::new(ArcSwap::from_pointee(initial.clone())),
            current: initial,
            comparator_name,
            sync_mode,
            snapshot_id,
            log_rotate_bytes,
            log_bytes: None,
            edit_scratch: Vec::new(),
        }
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
        self.upgrade_version_with_seqno(
            tree_path,
            f,
            seqno.next(),
            visible_seqno,
            fs,
            runtime,
            encryption,
            retention,
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
        let prior = self.latest_version();
        // Install seqnos are non-decreasing. The counter is caller-owned, and a
        // deployment that reopens with it reset would otherwise install below
        // the current version: a drop or a filtering compaction there raises
        // the floor only to that smaller seqno, leaving snapshots above it
        // answered by the current version without the rows it removed. Under
        // the seqno contract (monotone counter) this clamp is a no-op.
        let seqno = seqno.max(prior.seqno);
        let mut next_version = f(&prior)?;
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

        self.persist_change(
            tree_path,
            &prior.version,
            &next_version.version,
            fs,
            runtime,
            encryption,
        )?;
        self.append_version(next_version);

        // Clamp to stay below the reserved MSB range.
        let next_visible = seqno.saturating_add(1).min(MAX_SEQNO);
        visible_seqno.fetch_max(next_visible);

        Ok(())
    }

    /// Persists the transition from `prior` to `next` to disk, durably, the
    /// incremental way: append one [`VersionEdit`](crate::version::edit::VersionEdit)
    /// to the current snapshot's log (the common, O(changed-levels) path), or
    /// rotate when that log has grown past [`Self::log_rotate_bytes`] or the
    /// edit is too large for one log record.
    ///
    /// Rotation writes a fresh full snapshot for `next`, fsyncs it, and atomically
    /// repoints `CURRENT` (all inside [`persist_version`]); only after `CURRENT`
    /// commits does it delete the previous snapshot file and its log. Crash points:
    /// before the `CURRENT` switch, `CURRENT` still names the old snapshot and its
    /// log is intact (recover old + replay); after the switch, the new snapshot is
    /// complete and its log is empty (recover new, no edits). A torn trailing edit
    /// from an interrupted append is dropped on replay — the operation that wrote
    /// it was never acknowledged upward.
    fn persist_change(
        &mut self,
        tree_path: &Path,
        prior: &Version,
        next: &Version,
        fs: &dyn Fs,
        runtime: Arc<crate::runtime_config::RuntimeConfig>,
        encryption: Option<Arc<dyn crate::encryption::EncryptionProvider>>,
    ) -> crate::Result<()> {
        let log_path = tree_path.join(format!("edits-{}", self.snapshot_id));

        // Cached log size when available; measure once otherwise (fresh open
        // with a recovered log, or after an append error of unknown extent).
        let log_size = if let Some(n) = self.log_bytes {
            n
        } else {
            let n = edit_log::log_size(fs, &log_path)?;
            self.log_bytes = Some(n);
            n
        };

        if log_size < self.log_rotate_bytes {
            // Common path: append the delta and fsync. No snapshot rewrite.
            let edit = next.diff(prior)?;
            match edit_log::append_edit(
                fs,
                &log_path,
                &edit,
                &mut self.edit_scratch,
                self.sync_mode,
            ) {
                Ok(Some(appended)) => {
                    self.log_bytes = Some(log_size + appended);
                    return Ok(());
                }
                // The edit is too large for one record and nothing was
                // written: rotate below, since a snapshot holds a level of
                // any size.
                Ok(None) => {}
                Err(e) => {
                    // A failed append may have written a partial record; the
                    // on-disk size is unknown, so drop the cache and re-measure
                    // on the next install.
                    self.log_bytes = None;
                    return Err(e);
                }
            }
        }

        // Rotation: write `next` as a fresh full snapshot and repoint CURRENT.
        let old_snapshot = self.snapshot_id;
        persist_version(
            tree_path,
            next,
            &self.comparator_name,
            fs,
            runtime,
            encryption,
            self.sync_mode,
        )?;
        self.snapshot_id = next.id();
        // The new generation starts with an empty log (created lazily on the
        // first append).
        self.log_bytes = Some(0);

        // The durable commit point of a rotation is the CURRENT repoint inside
        // `persist_version` above — past it, the rotation has SUCCEEDED. Deleting
        // the old generation's log + snapshot is pure garbage collection, so it
        // is best-effort: a failure here must NOT propagate, or the caller
        // (`upgrade_version_with_seqno`) would skip `append_version` /
        // `fetch_max` and keep stale in-memory state while CURRENT already names
        // the new snapshot — an on-disk/in-memory divergence. A leaked old file
        // is harmless and swept by `cleanup_orphaned_version` on the next open.
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
        // Mirror it into the lock-free pointer so reads see it without taking
        // the history lock.
        #[cfg(feature = "std")]
        self.latest.store(Arc::new(version.clone()));
        self.current = version;
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

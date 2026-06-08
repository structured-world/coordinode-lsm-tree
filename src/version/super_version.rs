// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

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
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}
#[cfg(feature = "std")]
use arc_swap::ArcSwap;
use std::{collections::VecDeque, path::Path, sync::Arc};

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

pub struct SuperVersions {
    versions: VecDeque<SuperVersion>,

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

    /// Lock-free mirror of the latest (back) `SuperVersion`, shared with the
    /// `Tree` so a point read at `MAX_SEQNO` can load the current snapshot
    /// without taking the history `RwLock` or cloning the deque entry. Kept
    /// in sync under the same write lock at every site that changes the back
    /// (construction, [`append_version`](Self::append_version),
    /// [`replace_latest_version`](Self::replace_latest_version)). Recent
    /// inserts remain visible through it because they mutate the shared
    /// `active_memtable` behind a stable `Arc` — the back only changes on
    /// flush / compaction.
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
            version,
            seqno: 0,
        };

        Self {
            #[cfg(feature = "std")]
            latest: Arc::new(ArcSwap::from_pointee(initial.clone())),
            versions: vec![initial].into(),
            comparator_name,
            sync_mode,
            snapshot_id,
            log_rotate_bytes,
        }
    }

    pub fn memtable_size_sum(&self) -> u64 {
        let mut set = crate::HashMap::default();

        for super_version in &self.versions {
            set.entry(super_version.active_memtable.id)
                .and_modify(|bytes| *bytes += super_version.active_memtable.size())
                .or_insert_with(|| super_version.active_memtable.size());

            for sealed in super_version.sealed_memtables.iter() {
                set.entry(sealed.id)
                    .and_modify(|bytes| *bytes += sealed.size())
                    .or_insert_with(|| sealed.size());
            }
        }

        set.into_values().sum()
    }

    pub fn len(&self) -> usize {
        self.versions.len()
    }

    pub fn free_list_len(&self) -> usize {
        self.len().saturating_sub(1)
    }

    pub fn maintenance(
        &mut self,
        folder: &Path,
        gc_watermark: SeqNo,
        fs: &dyn Fs,
    ) -> crate::Result<()> {
        if gc_watermark == 0 {
            return Ok(());
        }

        if self.free_list_len() < 1 {
            return Ok(());
        }

        log::trace!("Running manifest GC with watermark={gc_watermark}");

        if let Some(hi_idx) = self.versions.iter().rposition(|x| x.seqno < gc_watermark) {
            for _ in 0..hi_idx {
                let Some(head) = self.versions.front() else {
                    break;
                };

                let evicted_id = head.version.id();
                log::trace!("Removing version #{evicted_id} (seqno={})", head.seqno);

                // Under the incremental manifest only the CURRENT snapshot has a
                // `v{id}` file on disk; intermediate versions live in the edit
                // log and have no file (so removing them is a no-op NotFound).
                // The snapshot file must NOT be removed here even when its
                // in-memory version is evicted from the history — `CURRENT` still
                // points at it and the log layers on top. Its lifecycle belongs
                // to rotation (which writes the next snapshot and deletes the old
                // one only after `CURRENT` is repointed).
                if evicted_id != self.snapshot_id {
                    let path = folder.join(format!("v{evicted_id}"));
                    match fs.remove_file(&path) {
                        Ok(()) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        Err(e) => return Err(e.into()),
                    }
                }

                self.versions.pop_front();
            }
        }

        log::trace!(
            "Manifest GC done, version length now {}",
            self.versions.len()
        );

        Ok(())
    }

    /// Modifies the level manifest atomically.
    ///
    /// The function accepts a transition function that receives the current version
    /// and returns a new version.
    ///
    /// The function takes care of persisting the version changes on disk.
    // Takes &SharedSequenceNumberGenerator (not &dyn SequenceNumberGenerator)
    // because Config stores Arc<dyn ...> and all callers already have that type.
    #[expect(
        clippy::too_many_arguments,
        reason = "version upgrade threads tree_path, mutator closure, two seqno gens, fs, \
                  runtime, encryption — every parameter is load-bearing per the \
                  manifest-persist contract"
    )]
    pub(crate) fn upgrade_version<F: FnOnce(&SuperVersion) -> crate::Result<SuperVersion>>(
        &mut self,
        tree_path: &Path,
        f: F,
        seqno: &SharedSequenceNumberGenerator,
        visible_seqno: &SharedSequenceNumberGenerator,
        fs: &dyn Fs,
        runtime: std::sync::Arc<crate::runtime_config::RuntimeConfig>,
        encryption: Option<std::sync::Arc<dyn crate::encryption::EncryptionProvider>>,
    ) -> crate::Result<()> {
        self.upgrade_version_with_seqno(
            tree_path,
            f,
            seqno.next(),
            visible_seqno,
            fs,
            runtime,
            encryption,
        )
    }

    /// Like `upgrade_version`, but takes an already-allocated sequence number.
    ///
    /// This is useful when the seqno must be coordinated with other operations
    /// (e.g., bulk ingestion where tables are recovered with the same seqno).
    #[expect(
        clippy::too_many_arguments,
        reason = "version upgrade with pre-allocated seqno: tree_path, mutator, seqno, \
                  visible_seqno, fs, runtime, encryption — same load-bearing surface as \
                  the auto-allocating sibling above"
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
        runtime: std::sync::Arc<crate::runtime_config::RuntimeConfig>,
        encryption: Option<std::sync::Arc<dyn crate::encryption::EncryptionProvider>>,
    ) -> crate::Result<()> {
        let prior = self.latest_version();
        let mut next_version = f(&prior)?;
        next_version.seqno = seqno;
        log::trace!("Next version seqno={}", next_version.seqno);

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
    /// rotate when that log has grown past [`Self::log_rotate_bytes`].
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
        runtime: std::sync::Arc<crate::runtime_config::RuntimeConfig>,
        encryption: Option<std::sync::Arc<dyn crate::encryption::EncryptionProvider>>,
    ) -> crate::Result<()> {
        let log_path = tree_path.join(format!("edits-{}", self.snapshot_id));

        if edit_log::log_size(fs, &log_path)? < self.log_rotate_bytes {
            // Common path: append the delta and fsync. No snapshot rewrite.
            let edit = next.diff(prior)?;
            let mut scratch = Vec::new();
            return edit_log::append_edit(fs, &log_path, &edit, &mut scratch, self.sync_mode);
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

    pub fn append_version(&mut self, version: SuperVersion) {
        // Mirror the new back into the lock-free latest pointer so point
        // reads at MAX_SEQNO see it without taking the history lock.
        #[cfg(feature = "std")]
        self.latest.store(Arc::new(version.clone()));
        self.versions.push_back(version);
    }

    pub fn replace_latest_version(&mut self, version: SuperVersion) {
        if self.versions.pop_back().is_some() {
            #[cfg(feature = "std")]
            self.latest.store(Arc::new(version.clone()));
            self.versions.push_back(version);
        }
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
        #[expect(clippy::expect_used, reason = "SuperVersion is expected to exist")]
        self.versions
            .iter()
            .last()
            .cloned()
            .expect("should always have a SuperVersion")
    }

    pub fn get_version_for_snapshot(&self, seqno: SeqNo) -> SuperVersion {
        if seqno == 0 {
            #[expect(clippy::expect_used, reason = "SuperVersion is expected to exist")]
            return self
                .versions
                .front()
                .cloned()
                .expect("should always find a SuperVersion");
        }

        let version = self
            .versions
            .iter()
            .rev()
            .find(|version| version.seqno < seqno)
            .cloned();

        if version.is_none() {
            log::error!("Failed to find a SuperVersion for snapshot with seqno={seqno}");
            log::error!("SuperVersions:");

            for version in self.versions.iter().rev() {
                log::error!("-> {}, seqno={}", version.version.id(), version.seqno);
            }
        }

        #[expect(clippy::expect_used, reason = "SuperVersion is expected to exist")]
        version.expect("should always find a SuperVersion")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::comparator::default_comparator;
    use crate::fs::{Fs, FsOpenOptions, MemFs};
    use test_log::test;

    fn new_memtable(id: u64) -> Memtable {
        Memtable::new(id, default_comparator())
    }

    fn test_super_versions(versions: Vec<SuperVersion>) -> SuperVersions {
        #[cfg(feature = "std")]
        #[expect(
            clippy::expect_used,
            reason = "test helper: every caller passes a non-empty version list"
        )]
        let latest = Arc::new(ArcSwap::from_pointee(
            versions
                .last()
                .cloned()
                .expect("test helper requires at least one version"),
        ));
        SuperVersions {
            versions: versions.into(),
            comparator_name: "default".into(),
            sync_mode: SyncMode::Normal,
            snapshot_id: 0,
            log_rotate_bytes: 1024 * 1024,
            #[cfg(feature = "std")]
            latest,
        }
    }

    /// Seed version files (`v1`, `v2`, ...) into `fs` at `dir` for each
    /// `SuperVersion` in the list. This makes GC tests exercise the real
    /// `Fs::remove_file` path instead of only hitting `NotFound`.
    fn seed_version_files(dir: &Path, versions: &SuperVersions, fs: &dyn Fs) -> crate::Result<()> {
        fs.create_dir_all(dir)?;
        for sv in &versions.versions {
            let path = dir.join(format!("v{}", sv.version.id()));
            fs.open(
                &path,
                &FsOpenOptions::new().write(true).create(true).truncate(true),
            )?;
        }
        Ok(())
    }

    #[test]
    fn super_version_gc_above_watermark() -> crate::Result<()> {
        let fs = MemFs::new();
        let dir = Path::new("/gc/above");
        let mut history = test_super_versions(vec![
            SuperVersion {
                active_memtable: Arc::new(new_memtable(0)),
                sealed_memtables: Arc::default(),
                version: Version::new(1, crate::TreeType::Standard),
                seqno: 0,
            },
            SuperVersion {
                active_memtable: Arc::new(new_memtable(0)),
                sealed_memtables: Arc::default(),
                version: Version::new(2, crate::TreeType::Standard),
                seqno: 1,
            },
            SuperVersion {
                active_memtable: Arc::new(new_memtable(0)),
                sealed_memtables: Arc::default(),
                version: Version::new(3, crate::TreeType::Standard),
                seqno: 2,
            },
        ]);
        seed_version_files(dir, &history, &fs)?;

        // gc_watermark=0 → early return, no GC
        history.maintenance(dir, 0, &fs)?;

        assert_eq!(history.free_list_len(), 2);
        // All version files still present (no GC ran)
        assert!(fs.exists(&dir.join("v1"))?);
        assert!(fs.exists(&dir.join("v2"))?);
        assert!(fs.exists(&dir.join("v3"))?);

        Ok(())
    }

    #[test]
    fn super_version_gc_preserves_current_snapshot_file() -> crate::Result<()> {
        // The CURRENT snapshot file must survive GC even when its in-memory
        // version is evicted from the history — `CURRENT` still points at it and
        // the edit log layers on top. Set snapshot_id to a seeded v{id} that GC
        // will evict, and assert its file stays while a non-snapshot evictee is
        // removed.
        let fs = MemFs::new();
        let dir = Path::new("/gc/snapshot");
        let mut history = test_super_versions(vec![
            SuperVersion {
                active_memtable: Arc::new(new_memtable(0)),
                sealed_memtables: Arc::default(),
                version: Version::new(1, crate::TreeType::Standard),
                seqno: 0,
            },
            SuperVersion {
                active_memtable: Arc::new(new_memtable(0)),
                sealed_memtables: Arc::default(),
                version: Version::new(2, crate::TreeType::Standard),
                seqno: 1,
            },
            SuperVersion {
                active_memtable: Arc::new(new_memtable(0)),
                sealed_memtables: Arc::default(),
                version: Version::new(3, crate::TreeType::Standard),
                seqno: 2,
            },
        ]);
        // CURRENT points at v1 (the snapshot the edit log is layered on).
        history.snapshot_id = 1;
        seed_version_files(dir, &history, &fs)?;

        // Watermark 3 evicts v1 (seqno 0) and v2 (seqno 1) from the history.
        history.maintenance(dir, 3, &fs)?;

        assert!(
            fs.exists(&dir.join("v1"))?,
            "the CURRENT snapshot file must NOT be GC'd even when its version is evicted"
        );
        assert!(
            !fs.exists(&dir.join("v2"))?,
            "a non-snapshot evicted version's file is still removed"
        );
        Ok(())
    }

    #[test]
    fn super_version_gc_below_watermark_simple() -> crate::Result<()> {
        let fs = MemFs::new();
        let dir = Path::new("/gc/simple");
        let mut history = test_super_versions(vec![
            SuperVersion {
                active_memtable: Arc::new(new_memtable(0)),
                sealed_memtables: Arc::default(),
                version: Version::new(1, crate::TreeType::Standard),
                seqno: 0,
            },
            SuperVersion {
                active_memtable: Arc::new(new_memtable(0)),
                sealed_memtables: Arc::default(),
                version: Version::new(2, crate::TreeType::Standard),
                seqno: 1,
            },
            SuperVersion {
                active_memtable: Arc::new(new_memtable(0)),
                sealed_memtables: Arc::default(),
                version: Version::new(3, crate::TreeType::Standard),
                seqno: 2,
            },
        ]);
        seed_version_files(dir, &history, &fs)?;

        history.maintenance(dir, 3, &fs)?;

        assert_eq!(history.len(), 1);
        // v1 and v2 deleted by GC, v3 kept
        assert!(!fs.exists(&dir.join("v1"))?);
        assert!(!fs.exists(&dir.join("v2"))?);
        assert!(fs.exists(&dir.join("v3"))?);

        Ok(())
    }

    #[test]
    fn super_version_gc_below_watermark_simple_2() -> crate::Result<()> {
        let fs = MemFs::new();
        let dir = Path::new("/gc/simple2");
        let mut history = test_super_versions(vec![
            SuperVersion {
                active_memtable: Arc::new(new_memtable(0)),
                sealed_memtables: Arc::default(),
                version: Version::new(1, crate::TreeType::Standard),
                seqno: 0,
            },
            SuperVersion {
                active_memtable: Arc::new(new_memtable(0)),
                sealed_memtables: Arc::default(),
                version: Version::new(2, crate::TreeType::Standard),
                seqno: 1,
            },
            SuperVersion {
                active_memtable: Arc::new(new_memtable(0)),
                sealed_memtables: Arc::default(),
                version: Version::new(3, crate::TreeType::Standard),
                seqno: 2,
            },
            SuperVersion {
                active_memtable: Arc::new(new_memtable(0)),
                sealed_memtables: Arc::default(),
                version: Version::new(4, crate::TreeType::Standard),
                seqno: 8,
            },
        ]);
        seed_version_files(dir, &history, &fs)?;

        history.maintenance(dir, 3, &fs)?;

        assert_eq!(history.len(), 2);
        // v1 and v2 deleted, v3 and v4 kept
        assert!(!fs.exists(&dir.join("v1"))?);
        assert!(!fs.exists(&dir.join("v2"))?);
        assert!(fs.exists(&dir.join("v3"))?);
        assert!(fs.exists(&dir.join("v4"))?);

        Ok(())
    }

    #[test]
    fn super_version_gc_below_watermark_keep() -> crate::Result<()> {
        let fs = MemFs::new();
        let dir = Path::new("/gc/keep");
        let mut history = test_super_versions(vec![
            SuperVersion {
                active_memtable: Arc::new(new_memtable(0)),
                sealed_memtables: Arc::default(),
                version: Version::new(1, crate::TreeType::Standard),
                seqno: 0,
            },
            SuperVersion {
                active_memtable: Arc::new(new_memtable(0)),
                sealed_memtables: Arc::default(),
                version: Version::new(2, crate::TreeType::Standard),
                seqno: 8,
            },
        ]);
        seed_version_files(dir, &history, &fs)?;

        history.maintenance(dir, 3, &fs)?;

        assert_eq!(history.len(), 2);
        // Both kept — no version below watermark has a successor also below watermark
        assert!(fs.exists(&dir.join("v1"))?);
        assert!(fs.exists(&dir.join("v2"))?);

        Ok(())
    }

    #[test]
    fn super_version_gc_below_watermark_shadowed() -> crate::Result<()> {
        let fs = MemFs::new();
        let dir = Path::new("/gc/shadowed");
        let mut history = test_super_versions(vec![
            SuperVersion {
                active_memtable: Arc::new(new_memtable(0)),
                sealed_memtables: Arc::default(),
                version: Version::new(1, crate::TreeType::Standard),
                seqno: 0,
            },
            SuperVersion {
                active_memtable: Arc::new(new_memtable(0)),
                sealed_memtables: Arc::default(),
                version: Version::new(2, crate::TreeType::Standard),
                seqno: 2,
            },
        ]);
        seed_version_files(dir, &history, &fs)?;

        history.maintenance(dir, 3, &fs)?;

        assert_eq!(history.len(), 1);
        // v1 deleted, v2 kept
        assert!(!fs.exists(&dir.join("v1"))?);
        assert!(fs.exists(&dir.join("v2"))?);

        Ok(())
    }
}

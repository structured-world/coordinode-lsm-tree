// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Reference-counted file-deletion gate used by checkpoint-style snapshots.
//!
//! While a [`DeletionPause`] is *active* (refcount ≥ 1), the [`Drop`]
//! implementations on tables and blob files do not call
//! [`Fs::remove_file`](crate::fs::Fs::remove_file) immediately. Instead they
//! enqueue `(fs, path)` for later removal. Compaction may continue producing
//! obsolete files; their physical deletion is just deferred.
//!
//! When the last [`Pause`] guard is dropped, the queue is drained and every
//! queued path is unlinked through the original [`crate::fs::Fs`] backend.
//! This pattern mirrors `RocksDB`'s `DisableFileDeletions` /
//! `EnableFileDeletions` API used by `Checkpoint::CreateCheckpoint`.
//!
//! # Why a queue per pause and not per file?
//!
//! Tables and blob files only know their own path + [`crate::fs::Fs`]; they
//! do not have a back-reference to the tree they belong to. By embedding an
//! [`Arc<DeletionPause>`] (optional, [`None`] by default) into each table /
//! blob-file `Inner`, the [`Drop`] check is O(1) and lock-free in the
//! common case (no checkpoint in progress).

use crate::fs::Fs;
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    },
};

/// Shared state controlling whether file deletions are deferred.
///
/// Cheap to clone: holds an atomic counter plus a `Mutex<Vec<...>>` that is
/// only contended during checkpoint setup/teardown, never on the read path.
#[derive(Default)]
pub struct DeletionPause {
    /// Number of active [`Pause`] guards. `0` means deletions happen
    /// immediately; `>0` means they are queued.
    active: AtomicU32,

    /// Paths queued for removal while at least one pause was active.
    queue: Mutex<Vec<QueuedDeletion>>,
}

struct QueuedDeletion {
    fs: Arc<dyn Fs>,
    path: PathBuf,
}

impl std::fmt::Debug for DeletionPause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeletionPause")
            .field("active", &self.active.load(Ordering::Relaxed))
            .field("queued", &self.queue.lock().map(|q| q.len()).unwrap_or(0))
            .finish()
    }
}

impl DeletionPause {
    /// Creates a new pause controller in the inactive state.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Returns `true` if there is at least one active [`Pause`] guard and
    /// deletions should therefore be queued.
    #[inline]
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire) > 0
    }

    /// Tries to enqueue `(fs, path)` for later removal. Returns `true` if
    /// the deletion was queued (caller must NOT call `remove_file`), or
    /// `false` if the pause is currently inactive (caller proceeds with
    /// the deletion as usual).
    pub fn try_enqueue(&self, fs: Arc<dyn Fs>, path: PathBuf) -> bool {
        if !self.is_active() {
            return false;
        }
        // Lock the queue then re-check under the lock — if the pause was
        // released between the atomic load above and acquiring the lock,
        // the queue would never be drained and the file would leak.
        let Ok(mut queue) = self.queue.lock() else {
            // Mutex poisoning means another thread panicked while holding
            // the queue. We refuse to enqueue (returning false) so the
            // caller falls back to immediate removal — the safer option.
            return false;
        };
        if !self.is_active() {
            return false;
        }
        queue.push(QueuedDeletion { fs, path });
        true
    }

    /// Acquires a pause guard. While at least one guard is alive,
    /// [`try_enqueue`](Self::try_enqueue) defers deletions.
    pub fn acquire(self: &Arc<Self>) -> Pause {
        self.active.fetch_add(1, Ordering::AcqRel);
        Pause {
            inner: Arc::clone(self),
        }
    }
}

/// RAII guard that keeps a [`DeletionPause`] active. Dropping the last
/// guard drains the queue and unlinks every queued file.
#[must_use = "deletion pause is released when this guard is dropped"]
pub struct Pause {
    inner: Arc<DeletionPause>,
}

impl Drop for Pause {
    fn drop(&mut self) {
        // Use AcqRel so the decrement is sequenced with respect to any
        // queued enqueue calls performed by other threads.
        let prev = self.inner.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(prev > 0, "DeletionPause underflow");

        if prev == 1 {
            // We were the last pause holder — drain and execute pending
            // deletions. We deliberately swap out the queue under the
            // lock so newly-queued items (which can only happen if a new
            // pause is acquired concurrently) do not get lost.
            let drained = {
                let Ok(mut queue) = self.inner.queue.lock() else {
                    return;
                };
                std::mem::take(&mut *queue)
            };

            for item in drained {
                if let Err(e) = item.fs.remove_file(&item.path) {
                    // Match the warning style used by Table/BlobFile Drop
                    // impls so log filters keep working.
                    log::warn!(
                        "Failed to remove deferred deletion {}: {e:?}",
                        item.path.display(),
                    );
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;
    use crate::fs::{Fs, MemFs};
    use std::path::Path;

    fn write_file(fs: &MemFs, path: &Path, bytes: &[u8]) {
        use std::io::Write;
        let opts = crate::fs::FsOpenOptions::new().write(true).create(true);
        let mut f = fs.open(path, &opts).unwrap();
        f.write_all(bytes).unwrap();
    }

    #[test]
    fn deletion_pause_defers_then_executes_removal() {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/d")).unwrap();
        let path = Path::new("/d/file.sst").to_path_buf();
        write_file(&fs, &path, b"sst");
        let dyn_fs: Arc<dyn Fs> = Arc::new(fs.clone());

        let pause = DeletionPause::new();
        let guard = pause.acquire();

        assert!(pause.try_enqueue(dyn_fs.clone(), path.clone()));
        assert!(
            fs.exists(&path).unwrap(),
            "file must still exist while paused"
        );

        drop(guard);
        assert!(
            !fs.exists(&path).unwrap(),
            "file must be removed after pause released"
        );
    }

    #[test]
    fn enqueue_returns_false_when_inactive() {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/d")).unwrap();
        let path = Path::new("/d/file.sst").to_path_buf();
        write_file(&fs, &path, b"x");
        let dyn_fs: Arc<dyn Fs> = Arc::new(fs.clone());

        let pause = DeletionPause::new();
        assert!(!pause.try_enqueue(dyn_fs, path.clone()));
        assert!(fs.exists(&path).unwrap());
    }

    #[test]
    fn nested_pauses_only_release_on_last_drop() {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/d")).unwrap();
        let path = Path::new("/d/file.sst").to_path_buf();
        write_file(&fs, &path, b"x");
        let dyn_fs: Arc<dyn Fs> = Arc::new(fs.clone());

        let pause = DeletionPause::new();
        let outer = pause.acquire();
        let inner = pause.acquire();

        assert!(pause.try_enqueue(dyn_fs, path.clone()));

        drop(inner);
        assert!(fs.exists(&path).unwrap(), "still paused by outer guard");

        drop(outer);
        assert!(
            !fs.exists(&path).unwrap(),
            "released after last guard dropped"
        );
    }
}

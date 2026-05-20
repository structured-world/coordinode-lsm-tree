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

// Synchronisation comes from `spin::Mutex` (no_std-compatible) rather
// than `std::sync::Mutex`. The queue is only contended during
// checkpoint setup/teardown — never on the read path — so spin
// contention is irrelevant in practice; the benefit is that this
// module's std footprint is bounded by what the `Fs` trait already
// requires (path types + I/O) with no extra std-only synchronisation
// primitive layered on top. `spin::Mutex` also cannot poison, which
// removes the `PoisonError::into_inner` recovery branches that the
// std variant required.
//
// `PathBuf` has no alloc-only counterpart in the standard library
// (path types live in `std::path`, not `alloc::path`), so it stays
// here — the value is unavoidably std-coupled the moment we call
// `Fs::remove_file(&Path)` anyway.
use crate::fs::Fs;
use alloc::sync::Arc;
use core::sync::atomic::{AtomicU32, Ordering};
use spin::Mutex;
use std::path::PathBuf;

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

impl core::fmt::Debug for DeletionPause {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("DeletionPause")
            .field("active", &self.active.load(Ordering::Relaxed))
            .field("queued", &self.queue.lock().len())
            .finish()
    }
}

impl DeletionPause {
    /// Creates a new pause controller in the inactive state.
    ///
    /// This is the plain constructor — owns the value, no allocation
    /// decision baked in. Use [`Self::new_shared`] when you specifically
    /// want an `Arc`-wrapped controller (the common case for tree
    /// installation where every table / blob file holds a clone).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Convenience: creates a new pause controller and wraps it in an
    /// [`Arc`]. Equivalent to `Arc::new(DeletionPause::new())`.
    #[must_use]
    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self::new())
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
        // `spin::Mutex` cannot poison, so no recovery branch is needed.
        let mut queue = self.queue.lock();
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

        if prev != 1 {
            return;
        }

        // We were the last pause holder — drain and execute pending
        // deletions. Generation race: between the `fetch_sub` above and
        // acquiring the queue lock below, another thread can call
        // `acquire()` and `try_enqueue()`. Items pushed in that new
        // generation belong to the new pause, not to us. Re-check
        // `active` under the lock and bail out if a new pause is now
        // in flight; the new pause's eventual `Drop` will drain those
        // items at the correct generation boundary.
        //
        // `spin::Mutex` cannot poison, so there is no
        // `PoisonError`-recovery branch here — `lock()` always returns
        // a guard.
        let drained = {
            let mut queue = self.inner.queue.lock();
            if self.inner.active.load(Ordering::Acquire) > 0 {
                // A new pause has taken responsibility for the queue.
                // Leave its items alone; its drop will handle them.
                return;
            }
            core::mem::take(&mut *queue)
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

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test code")]
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

        let pause = DeletionPause::new_shared();
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

        let pause = DeletionPause::new_shared();
        assert!(!pause.try_enqueue(dyn_fs, path.clone()));
        assert!(fs.exists(&path).unwrap());
    }

    /// Regression test for the generation race in `Drop for Pause`.
    ///
    /// Scenario the broken code allows:
    ///
    /// 1. Thread A holds the only pause (`active == 1`).
    /// 2. Thread A calls `fetch_sub(1)`, observing `prev == 1` (now `active == 0`).
    /// 3. Before Thread A locks the queue, Thread B calls `acquire()`
    ///    (`active == 1`) and `try_enqueue` queues a fresh deletion.
    /// 4. Thread A finally locks the queue and the original code does
    ///    `mem::take`, *executing* the deletion Thread B was supposed to
    ///    defer. Thread B's file vanishes despite an active pause.
    ///
    /// The deterministic reproducer below uses two channels to pin the
    /// invariant check at the exact moment when Thread B holds an active
    /// pause and the queue contains its enqueued item. Without the fix,
    /// A's drop would have already swept the queue and removed B's file
    /// before B signalled `ready` — the survives-while-B-holds-pause
    /// assertion fires. With the fix, A's drop bails out under the lock
    /// (because B's `acquire` already incremented `active`) and the file
    /// survives until B drops at the end.
    #[test]
    fn drain_does_not_steal_a_new_generation_queue() {
        use std::sync::mpsc;
        use std::thread;

        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/d")).unwrap();
        let path = Path::new("/d/race.sst").to_path_buf();
        write_file(&fs, &path, b"keep-me");
        let dyn_fs: Arc<dyn Fs> = Arc::new(fs.clone());

        let pause = DeletionPause::new_shared();
        let a = pause.acquire();

        // Deterministic handshake: `a_dropped_tx` signals that A's
        // `fetch_sub(1)` has executed (so `active == 0` from A's
        // perspective and A is committed to the drain branch); B then
        // races in with `acquire()`+`try_enqueue()` to exercise the
        // generation-race protection. Spin-wait on `active == 0` instead
        // of using a fixed sleep so the test is robust under any
        // scheduler / CI load.
        let (a_dropped_tx, a_dropped_rx) = mpsc::channel::<()>();
        let (ready_tx, ready_rx) = mpsc::channel::<()>();
        let (release_tx, release_rx) = mpsc::channel::<()>();

        let b_pause = Arc::clone(&pause);
        let b_fs = Arc::clone(&dyn_fs);
        let b_path = path.clone();
        let b_thread = thread::spawn(move || {
            // Block until A has begun its drop. We then spin until
            // A's `fetch_sub` has visibly reduced `active` to 0 —
            // that's the exact window where A is about to lock the
            // queue and the fix MUST notice our subsequent `acquire`
            // under the lock.
            a_dropped_rx.recv().unwrap();
            while b_pause.active.load(Ordering::Acquire) != 0 {
                core::hint::spin_loop();
            }
            let _b = b_pause.acquire();
            assert!(b_pause.try_enqueue(b_fs, b_path));
            // Signal the main thread that B is in the right state and
            // wait for the invariant check to complete before dropping
            // the pause guard.
            ready_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            // Implicit drop here drains the queue.
        });

        // Trigger A's drop now. Send the handshake AFTER drop returns
        // so B observes A's `fetch_sub`-decremented `active` value.
        drop(a);
        a_dropped_tx.send(()).unwrap();

        // Block until B has acquired AND enqueued; this is the exact
        // moment the race manifests. With the fix, the file MUST still
        // exist on disk because B's pause is active and the queued item
        // belongs to B's generation. Without the fix, A's drain has
        // already removed it — assertion fires here, not after join.
        ready_rx.recv().unwrap();
        assert!(
            fs.exists(&path).unwrap(),
            "file must survive while Thread B holds an active pause \
             (a's drain leaked into b's generation)",
        );
        release_tx.send(()).unwrap();

        b_thread.join().unwrap();

        // Sanity: after B drops too, the file is gone (B's drop drained
        // its own generation properly).
        assert!(
            !fs.exists(&path).unwrap(),
            "file should be removed after both pauses dropped",
        );
    }

    #[test]
    fn nested_pauses_only_release_on_last_drop() {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/d")).unwrap();
        let path = Path::new("/d/file.sst").to_path_buf();
        write_file(&fs, &path, b"x");
        let dyn_fs: Arc<dyn Fs> = Arc::new(fs.clone());

        let pause = DeletionPause::new_shared();
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

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Reference-counted file-deletion gate used by checkpoint-style snapshots.
//!
//! While a [`DeletionPause`] is *active* (refcount ≥ 1), the [`Drop`]
//! implementations on tables and blob files do not call
//! [`Fs::remove_file`] immediately. Instead they
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
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::sync::atomic::{AtomicU32, Ordering};

use crate::path::PathBuf;
use spin::Mutex;

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

#[cfg_attr(
    not(feature = "std"),
    allow(
        dead_code,
        reason = "no_std-capable deletion gate; only the std-gated checkpoint consumer acquires a pause, so under no_std nothing is ever queued"
    )
)]
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
    #[cfg_attr(
        not(feature = "std"),
        allow(
            dead_code,
            reason = "no_std-capable; only the std-gated checkpoint consumer acquires a pause"
        )
    )]
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
#[cfg_attr(
    not(feature = "std"),
    allow(
        dead_code,
        reason = "no_std-capable RAII guard; only the std-gated checkpoint consumer constructs one"
    )
)]
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

        // Test-only deterministic interleave point: the
        // `drain_does_not_steal_a_new_generation_queue` regression test
        // exercises the exact window between `fetch_sub(1)` above and
        // the queue lock below. Without a hook the window is microseconds
        // wide and unobservable from outside; the hook lets the test
        // suspend this drop until thread B has run `acquire() + try_enqueue()`.
        // Production builds compile this out (the symbol exists only
        // under `#[cfg(test)]`).
        #[cfg(test)]
        tests::drain_barrier::wait();

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
mod tests;

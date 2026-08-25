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

    /// Reclaims the drain could not perform because the file was still shared
    /// (a checkpoint linked it) or the link count could not be read. They are
    /// RETAINED rather than dropped: unlinking the checkpoint only decrements
    /// the link count, and the live restricted table keeps holding the inode,
    /// so nothing else would ever free the consumed prefix — the space would
    /// stay allocated until an unrelated compaction retires the table, under
    /// exactly the low-space condition that chose the tight-space path.
    /// Retried by [`retry_pending_reclaims`](Self::retry_pending_reclaims).
    pending_reclaims: Mutex<Vec<QueuedDeletion>>,

    /// Excludes a checkpoint's hard-link window from in-place file mutation
    /// (the ECC autoheal): a checkpoint that links an SST between the heal's
    /// link-count probe and its write-back would capture bytes the heal is
    /// about to change, under a digest its immutable manifest already
    /// recorded. Mutators hold the read side (distinct tables heal in
    /// parallel), a checkpoint holds the write side for its whole copy/link
    /// pass. `parking_lot` (not `spin`): both windows are long-lived,
    /// blocking operations, so spinning would burn a core.
    #[cfg(feature = "std")]
    link_gate: parking_lot::RwLock<()>,
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
    action: QueuedAction,
}

/// What the drain does with a queued path.
#[cfg_attr(
    not(feature = "std"),
    allow(
        dead_code,
        reason = "no_std-capable deletion gate; only the std-gated checkpoint consumer acquires a pause, so under no_std nothing is ever queued"
    )
)]
enum QueuedAction {
    /// Unlink the file (an obsolete table or blob file).
    Remove,
    /// Punch the listed `(offset, len)` extents, in the given order: a
    /// tight-space prefix reclaim whose view dropped during the pause. The
    /// extents are ordered top-down by the producer and the drain stops at the
    /// first failure, which keeps the resulting hole pattern classifiable for
    /// a sidecar-less manifest repair.
    Punch(Vec<(u64, u64)>),
}

impl core::fmt::Debug for DeletionPause {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("DeletionPause")
            .field("active", &self.active.load(Ordering::Relaxed))
            .field("queued", &self.queue.lock().len())
            // `link_gate` is omitted: a lock has no meaningful debug state.
            .finish_non_exhaustive()
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
        self.try_enqueue_action(fs, path, QueuedAction::Remove)
    }

    /// Tries to defer a tight-space prefix reclaim of `path` until the pause
    /// releases. Returns `true` if it was queued (the caller must NOT punch),
    /// `false` if the pause is inactive (the caller punches as usual).
    ///
    /// The intent lives in the view that is dropping, so without this the
    /// reclaim would be lost outright — and the space it was reclaiming is
    /// exactly what a tight-space compaction is short of. `extents` are punched
    /// in the order given (top-down) once the checkpoint's window closes, and
    /// only if the file is still exclusively owned then: a checkpoint that DID
    /// link it during its window shares the inode, and punching would zero the
    /// checkpoint's copy too.
    pub(crate) fn try_enqueue_punch(
        &self,
        fs: Arc<dyn Fs>,
        path: PathBuf,
        extents: Vec<(u64, u64)>,
    ) -> bool {
        self.try_enqueue_action(fs, path, QueuedAction::Punch(extents))
    }

    fn try_enqueue_action(&self, fs: Arc<dyn Fs>, path: PathBuf, action: QueuedAction) -> bool {
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
        queue.push(QueuedDeletion { fs, path, action });
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

    /// Enters an in-place-mutation window (blocks while a checkpoint's
    /// link window is open). Mutators of DISTINCT files may hold windows
    /// concurrently; see the `link_gate` field docs.
    #[cfg(feature = "std")]
    pub(crate) fn enter_mutation_window(&self) -> parking_lot::RwLockReadGuard<'_, ()> {
        self.link_gate.read()
    }

    /// Enters a checkpoint link window (blocks while any in-place-mutation
    /// window is open, and excludes new ones until dropped).
    #[cfg(feature = "std")]
    pub(crate) fn enter_link_window(&self) -> parking_lot::RwLockWriteGuard<'_, ()> {
        self.link_gate.write()
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
            match item.action {
                QueuedAction::Remove => {
                    if let Err(e) = item.fs.remove_file(&item.path) {
                        // Match the warning style used by Table/BlobFile Drop
                        // impls so log filters keep working.
                        log::warn!(
                            "Failed to remove deferred deletion {}: {e:?}",
                            item.path.display(),
                        );
                    }
                }
                QueuedAction::Punch(_) => {
                    DeletionPause::reclaim_or_retain(item, &self.inner.pending_reclaims);
                }
            }
        }
    }
}

impl DeletionPause {
    /// Punches `item`'s extents when the file is exclusively ours, or RETAINS
    /// the intent for a later retry when it is not.
    ///
    /// A checkpoint that hard-linked the file during its window shares the
    /// inode, and its captured manifest records the file UNRESTRICTED — punching
    /// would zero the checkpoint's copy. A probe that cannot answer is treated
    /// the same way. Neither case may DISCARD the reclaim: removing the
    /// checkpoint only decrements the link count while the live restricted table
    /// keeps holding the inode, so the consumed prefix would stay allocated with
    /// nothing left to free it.
    fn reclaim_or_retain(item: QueuedDeletion, pending: &Mutex<Vec<QueuedDeletion>>) {
        let QueuedAction::Punch(extents) = &item.action else {
            return;
        };
        match item.fs.hard_link_count(&item.path) {
            Ok(n) if n <= 1 => {
                for &(offset, len) in extents {
                    if let Err(e) = item.fs.punch_hole(&item.path, offset, len) {
                        log::warn!(
                            "Failed to punch deferred tight-space extent at {offset} of {}; \
                             stopping the reclaim to keep the hole pattern classifiable: {e:?}",
                            item.path.display(),
                        );
                        break;
                    }
                }
            }
            // The file is gone (the table was retired while the pause was held),
            // so the space it held is already back: nothing to retain.
            Err(e) if e.kind() == crate::io::ErrorKind::NotFound => {}
            probe => {
                log::debug!(
                    "Deferring the tight-space punch of {} again: the file is hard-linked \
                     (or the link count is unknown: {probe:?})",
                    item.path.display(),
                );
                pending.lock().push(item);
            }
        }
    }

    /// Re-attempts every reclaim a drain had to retain, keeping the ones whose
    /// file is still shared.
    ///
    /// Call it where the reclaimed space is needed — the tight-space compaction
    /// path — and after a checkpoint releases, since that is when its links
    /// usually disappear.
    pub(crate) fn retry_pending_reclaims(&self) {
        let retained = core::mem::take(&mut *self.pending_reclaims.lock());
        for item in retained {
            Self::reclaim_or_retain(item, &self.pending_reclaims);
        }
    }

    /// Whether any reclaim is still waiting for its file to stop being shared.
    #[cfg(test)]
    pub(crate) fn has_pending_reclaims(&self) -> bool {
        !self.pending_reclaims.lock().is_empty()
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test code")]
mod tests;

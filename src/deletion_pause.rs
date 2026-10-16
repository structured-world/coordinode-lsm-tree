// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

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

    /// The files live pins list, and the in-place mutations that keep a pin
    /// from being taken: a pinned file's bytes must not change until its last
    /// pin drops. `parking_lot`: a pin waits on `pins_settled` for in-flight
    /// mutations to finish, which spinning would turn into a busy core.
    #[cfg(feature = "std")]
    pins: parking_lot::Mutex<PinState>,

    /// Signalled when the last in-flight mutation finishes.
    #[cfg(feature = "std")]
    pins_settled: parking_lot::Condvar,
}

/// See [`DeletionPause::pins`].
#[cfg(feature = "std")]
#[derive(Default)]
struct PinState {
    /// Pins being taken: while non-zero no in-place mutation starts, so the
    /// version a pin captures is not changed under it before its files are
    /// registered.
    pending: u32,
    /// In-place mutations under way (a heal, a prefix punch).
    in_flight: u32,
    /// How many live pins list each file.
    files: crate::HashMap<PathBuf, u32>,
    /// Removals of pinned files whose last view dropped, run when the last
    /// pin listing the file drops.
    removals: Vec<(Arc<dyn Fs>, PathBuf)>,
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
                    self.inner
                        .reclaim_or_retain(item, &self.inner.pending_reclaims);
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
    ///
    /// The probe and the punch run inside ONE mutation window. A queued reclaim
    /// holds no live version object, so — unlike a table's or blob file's `Drop`
    /// — nothing else keeps a checkpoint out of the gap between them: a
    /// checkpoint starting after the probe read `1` would hard-link the file and
    /// then watch the punch zero the inode its immutable snapshot shares. The
    /// window excludes every checkpoint generation across the whole sequence.
    fn reclaim_or_retain(&self, item: QueuedDeletion, pending: &Mutex<Vec<QueuedDeletion>>) {
        let QueuedAction::Punch(extents) = &item.action else {
            return;
        };
        #[cfg(feature = "std")]
        let _mutation = self.enter_mutation_window();
        // A pinned file keeps its bytes until its last pin drops, which retries
        // this reclaim.
        #[cfg(feature = "std")]
        let Some(_unpinned) = self.begin_mutation(&item.path) else {
            pending.lock().push(item);
            return;
        };
        match item.fs.hard_link_count(&item.path) {
            Ok(n) if n <= 1 => {
                let mut unreclaimed: Option<Vec<(u64, u64)>> = None;
                for (at, &(offset, len)) in extents.iter().enumerate() {
                    if let Err(e) = item.fs.punch_hole(&item.path, offset, len) {
                        log::warn!(
                            "Failed to punch deferred tight-space extent at {offset} of {}; \
                             stopping the reclaim to keep the hole pattern classifiable, \
                             retaining it and the extents below it for a retry: {e:?}",
                            item.path.display(),
                        );
                        // The pass stops here — punching below an unreclaimed
                        // extent would break the top-down hole pattern a
                        // sidecar-less repair reads. But the failure is often
                        // transient, and dropping what is left would strand the
                        // space with nothing able to free it, so the failed
                        // extent and the untried remainder are retained.
                        unreclaimed = extents.get(at..).map(<[(u64, u64)]>::to_vec);
                        break;
                    }
                }
                if let Some(rest) = unreclaimed {
                    pending.lock().push(QueuedDeletion {
                        fs: item.fs,
                        path: item.path,
                        action: QueuedAction::Punch(rest),
                    });
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

    /// Retains a reclaim that cannot be punched RIGHT NOW — a COMPLETED
    /// checkpoint still hard-links the file, or the link probe cannot answer —
    /// so [`Self::retry_pending_reclaims`] finishes it once the link is gone.
    ///
    /// Deliberately a bare queue push, with no probe and no punch: the caller
    /// is a `Drop` impl, which must not block on the mutation gate (a
    /// checkpoint dropping its captured version holds the gate's write half,
    /// and that very drop can be what releases the caller's last `Arc`). The
    /// retry re-probes under the gate.
    #[cfg(feature = "std")]
    pub(crate) fn retain_reclaim(&self, fs: Arc<dyn Fs>, path: PathBuf, extents: Vec<(u64, u64)>) {
        self.pending_reclaims.lock().push(QueuedDeletion {
            fs,
            path,
            action: QueuedAction::Punch(extents),
        });
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
            self.reclaim_or_retain(item, &self.pending_reclaims);
        }
    }

    /// Whether any reclaim is still waiting for its file to stop being shared.
    #[cfg(test)]
    pub(crate) fn has_pending_reclaims(&self) -> bool {
        !self.pending_reclaims.lock().is_empty()
    }
}

#[cfg(feature = "std")]
impl DeletionPause {
    /// Starts an in-place mutation of `path` (a heal write-back, a prefix
    /// punch), or `None` when a live pin lists the file or a pin is being
    /// taken: the caller then leaves the file as it is and keeps its intent
    /// for a later attempt. Never blocks, so a `Drop` impl may call it.
    pub(crate) fn begin_mutation(&self, path: &crate::path::Path) -> Option<MutationGuard<'_>> {
        let mut state = self.pins.lock();
        if state.pending > 0 || state.files.contains_key(path) {
            return None;
        }
        // Bounded by the number of threads mutating at once.
        state.in_flight += 1;
        drop(state);
        Some(MutationGuard { pause: self })
    }

    /// Whether a live pin lists `path`. Only dictionary collection asks: the
    /// other paths need the answer together with a mutation slot or a
    /// deferred removal.
    #[cfg(zstd_any)]
    pub(crate) fn is_pinned(&self, path: &crate::path::Path) -> bool {
        self.pins.lock().files.contains_key(path)
    }

    /// Takes over the removal of `path` when a live pin lists it, returning
    /// `true`; the removal runs when the last pin listing the file drops.
    /// Never blocks, so a `Drop` impl may call it.
    pub(crate) fn defer_removal_if_pinned(
        &self,
        fs: &Arc<dyn Fs>,
        path: &crate::path::Path,
    ) -> bool {
        let mut state = self.pins.lock();
        if !state.files.contains_key(path) {
            return false;
        }
        state.removals.push((Arc::clone(fs), path.to_path_buf()));
        true
    }

    /// Stops new in-place mutations and waits for the ones under way, so the
    /// version a pin captures next is not changed under it. The returned
    /// guard lets mutations start again once the pin's files are registered.
    ///
    /// Holds no lock while waiting: a heal under way takes the version lock
    /// to record its new digest, and must be able to.
    pub(crate) fn begin_pin(&self) -> PinGuard<'_> {
        let mut state = self.pins.lock();
        state.pending += 1;
        while state.in_flight > 0 {
            self.pins_settled.wait(&mut state);
        }
        PinGuard { pause: self }
    }

    /// Registers the files of a pin.
    pub(crate) fn pin_files(&self, paths: &[PathBuf]) {
        let mut state = self.pins.lock();
        for path in paths {
            // Bounded by the number of live pins.
            *state.files.entry(path.clone()).or_insert(0) += 1;
        }
    }

    /// Releases the files of a pin: runs the removals deferred for the files
    /// no other pin lists, then the reclaims that waited for them.
    pub(crate) fn unpin_files(&self, paths: &[PathBuf]) {
        let removals = {
            let mut state = self.pins.lock();
            let mut released = Vec::new();
            for path in paths {
                if let Some(count) = state.files.get_mut(path.as_path()) {
                    *count -= 1;
                    if *count == 0 {
                        state.files.remove(path.as_path());
                        released.push(path);
                    }
                }
            }
            let (due, kept): (Vec<_>, Vec<_>) = core::mem::take(&mut state.removals)
                .into_iter()
                .partition(|(_, path)| released.contains(&path));
            state.removals = kept;
            due
        };
        for (fs, path) in removals {
            if let Err(e) = fs.remove_file(&path) {
                log::warn!(
                    "Failed to remove {} after its last pin dropped: {e:?}",
                    path.display(),
                );
            }
        }
        self.retry_pending_reclaims();
    }
}

/// An in-place mutation under way; see [`DeletionPause::begin_mutation`].
#[cfg(feature = "std")]
pub struct MutationGuard<'a> {
    pause: &'a DeletionPause,
}

#[cfg(feature = "std")]
impl Drop for MutationGuard<'_> {
    fn drop(&mut self) {
        let mut state = self.pause.pins.lock();
        state.in_flight -= 1;
        if state.in_flight == 0 {
            self.pause.pins_settled.notify_all();
        }
    }
}

/// A pin being taken; see [`DeletionPause::begin_pin`].
#[cfg(feature = "std")]
pub struct PinGuard<'a> {
    pause: &'a DeletionPause,
}

#[cfg(feature = "std")]
impl Drop for PinGuard<'_> {
    fn drop(&mut self) {
        self.pause.pins.lock().pending -= 1;
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test code")]
mod tests;

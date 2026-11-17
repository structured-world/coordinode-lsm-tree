// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! What a flush does on the device under a tree, chosen from the device's I/O
//! scheduler.
//!
//! An I/O scheduler that serves each queue in turn and idles for its next
//! request (BFQ) and one that does not (`mq-deadline`, `none`) reward opposite
//! choices on the flush path: what halves a flush's syncs on one doubles them
//! on the other. Which one a device runs is a property of its physical
//! location, so an [`IoPolicy`] asks it once per location (a backend's volume)
//! and keeps the answer with the trees open on it. A tree takes the strategy
//! into its [`RuntimeConfig`](crate::runtime_config::RuntimeConfig) when it
//! opens, and [`IoPolicy::refresh`] asks the location again and hands every
//! tree open on it the strategy for what it runs now. A flush finds the
//! strategy in the runtime configuration it loads anyway: it never asks.

use crate::fs::Fs;
use crate::path::Path;
use crate::runtime_config::handle::RuntimeConfigHandle;
use alloc::sync::Arc;

/// What a flush does on the device under its tree.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct IoStrategy {
    /// A flush one of whose files started writeback while written syncs its
    /// files one after another rather than side by side.
    ///
    /// BFQ serves the writeback such a file started from its asynchronous
    /// queue and idles between the queues of syncs issued side by side: a
    /// flush of 2.5 MB synced its table and blob file 25% faster one after
    /// the other, one of 18 MB 15% faster. A flush of 256 KB, which started
    /// no writeback, synced them 6% faster side by side, as every flush does
    /// under a scheduler that does not idle.
    serial_after_writeback: bool,
    /// The refill of a table file pool that a write's claim starts waits
    /// until the write's install has synced, rather than run beside it.
    ///
    /// Under BFQ the batch's files and folder sync, issued from another
    /// thread beside a new tree's first flush, took that flush to 105 ms at
    /// p99 against 7 with the batch after it. Under `mq-deadline` the same
    /// refill beside the flush made the flush's table name durable before
    /// its install synced the folder: the first flush of a new tree took
    /// 0.7 ms longer (1.4 ms with a blob file) with the batch held.
    refill_after_install: bool,
    /// Syncs issued side by side go to the backend as one batch
    /// ([`Fs::sync_many`]), which the std backend on Linux submits from one
    /// thread to an `io_uring`, rather than on threads of their own.
    ///
    /// A scheduler that serves each thread's queue in turn idles between the
    /// threads' syncs; one queue it does not. Elsewhere the ring costs: a
    /// flush syncing its table and folder took 0.45-0.5 ms longer through it
    /// than on threads, on ext4 and on Btrfs (`mq-deadline`).
    batch_syncs: bool,
}

/// How a batch of syncs is issued.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SyncSchedule {
    /// One after another, on the calling thread.
    OneAtATime,
    /// Side by side, each file on a thread of its own.
    SideBySide,
    /// Side by side, as one batch to the backend ([`Fs::sync_many`]).
    Batched,
}

impl IoStrategy {
    /// Syncs side by side and refills beside the write: a device whose
    /// scheduler does not idle between queues, or one nothing is known about.
    pub(crate) const NONE: Self = Self {
        serial_after_writeback: false,
        refill_after_install: false,
        batch_syncs: false,
    };

    /// The strategy for a device whose I/O scheduler idles between queues
    /// (`idles`), or does not.
    pub(crate) const fn for_scheduler(idles: bool) -> Self {
        Self {
            serial_after_writeback: idles,
            refill_after_install: idles,
            batch_syncs: idles,
        }
    }

    /// How a batch of syncs is issued, given whether one of its files started
    /// writeback while it was written and whether the backend syncs one file
    /// at a time ([`FsCapabilities::serial_syncs`](crate::fs::FsCapabilities::serial_syncs)).
    pub(crate) const fn sync_schedule(
        self,
        started_writeback: bool,
        backend_serial: bool,
    ) -> SyncSchedule {
        if backend_serial || self.serial_syncs(started_writeback) {
            SyncSchedule::OneAtATime
        } else if self.batch_syncs {
            SyncSchedule::Batched
        } else {
            SyncSchedule::SideBySide
        }
    }

    /// Whether a pool refill a write's claim starts waits for the write's
    /// install.
    pub(crate) const fn refill_after_install(self) -> bool {
        self.refill_after_install
    }

    /// Whether a flush syncs its files one after another, given whether one
    /// of them started writeback while it was written.
    pub(crate) const fn serial_syncs(self, started_writeback: bool) -> bool {
        self.serial_after_writeback && started_writeback
    }
}

/// What the I/O schedulers of the devices trees open on do, asked once per
/// location (a backend's volume), with the trees open on each.
///
/// A tree takes it from its [`Config`](crate::Config)
/// ([`use_io_policy`](crate::Config::use_io_policy)); one shared by the trees
/// a process opens asks each device once for all of them, and
/// [`Self::refresh`] reaches every one of them. A location stays after its
/// last tree closes, holding only its answer (a few dozen bytes per volume):
/// asking a device took up to 0.47 ms, and its answer changes only with its
/// scheduler.
///
/// Without `std` it keeps nothing: there is no scheduler to ask, and every
/// tree runs the configured writeback and syncs.
///
/// # Examples
///
/// ```
/// use lsm_tree::{AbstractTree, Config, IoPolicy, SequenceNumberCounter};
/// use std::sync::Arc;
///
/// let policy = Arc::new(IoPolicy::new());
/// let folder = tempfile::tempdir()?;
/// let tree = Config::new(
///     folder.path().join("a"),
///     SequenceNumberCounter::default(),
///     SequenceNumberCounter::default(),
/// )
/// .use_io_policy(Arc::clone(&policy))
/// .open()?;
/// // After a device's scheduler changed, every tree on it follows.
/// let _idles = policy.refresh(&lsm_tree::fs::StdFs, folder.path());
/// # drop(tree);
/// # Ok::<(), lsm_tree::Error>(())
/// ```
#[derive(Default)]
pub struct IoPolicy {
    /// Every location a tree given this policy opened on. Its lock covers
    /// only finding a location; nothing is asked under it.
    #[cfg(feature = "std")]
    locations: registry::Locations,
}

impl core::fmt::Debug for IoPolicy {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("IoPolicy").finish_non_exhaustive()
    }
}

impl IoPolicy {
    /// A policy that has asked no device yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Takes the strategy for the location of `path` on `fs` into the
    /// runtime configuration `handle` holds, and keeps `handle` with the
    /// location so a later [`Self::refresh`] reaches it. The location is
    /// asked only when no tree has asked about it before.
    pub(crate) fn attach(&self, fs: &dyn Fs, path: &Path, handle: &Arc<RuntimeConfigHandle>) {
        #[cfg(feature = "std")]
        registry::attach(&self.locations, fs, path, handle);
        #[cfg(not(feature = "std"))]
        handle.set_io_strategy(IoStrategy::for_scheduler(fs.idles_between_io_queues(path)));
    }

    /// Switches every tree open with this policy on the location of `path` to
    /// the strategy for the I/O scheduler its device runs now.
    ///
    /// Call it after the scheduler of a device has been changed (for example
    /// through `/sys/block/<disk>/queue/scheduler`): trees opened before keep
    /// the strategy they opened with until then.
    ///
    /// A tree reaches a refresh through the volume [`Fs::volume_id`] names
    /// for its folder. A tree opened on a backend that names none, and every
    /// tree in a build without `std`, keeps the strategy it opened with:
    /// reopen it to take a new one.
    ///
    /// Returns whether the device's scheduler idles between queues (BFQ).
    pub fn refresh(&self, fs: &dyn Fs, path: &Path) -> bool {
        #[cfg(feature = "std")]
        return registry::refresh(&self.locations, fs, path);
        #[cfg(not(feature = "std"))]
        fs.idles_between_io_queues(path)
    }
}

/// The locations a policy asked about, with the trees open on each.
#[cfg(feature = "std")]
mod registry {
    use super::IoStrategy;
    use crate::fs::Fs;
    use crate::path::Path;
    use crate::runtime_config::handle::RuntimeConfigHandle;
    use alloc::sync::{Arc, Weak};
    use alloc::vec::Vec;

    /// The locations of one policy.
    pub(super) type Locations = parking_lot::Mutex<Vec<Arc<Location>>>;

    /// One physical location: a backend's volume, what its scheduler was
    /// found to do, and the runtime configurations of the trees open on it
    /// (a closed one's is let go at the next attach).
    pub(super) struct Location {
        backend: Option<u64>,
        volume: u64,
        /// Its own lock, under which the location is asked and its answer
        /// set in its trees: a slow device holds up only the trees on it.
        state: parking_lot::Mutex<State>,
    }

    struct State {
        /// What the scheduler does; `None` until the first tree's ask.
        idles: Option<bool>,
        trees: Vec<Weak<RuntimeConfigHandle>>,
    }

    /// The location `backend` names `volume`, if a tree was opened on it.
    fn find(locations: &Locations, backend: Option<u64>, volume: u64) -> Option<Arc<Location>> {
        locations
            .lock()
            .iter()
            .find(|at| at.backend == backend && at.volume == volume)
            .cloned()
    }

    /// Sets in `handle` the strategy for the location of `path`, asking it
    /// only when no tree has before, and keeps `handle` with it. A backend
    /// that names no volume for `path` is asked each time and kept nowhere.
    ///
    /// The strategy is set under the location's lock, as a refresh sets its
    /// own: the one a tree runs is always the one its location records.
    pub(super) fn attach(
        locations: &Locations,
        fs: &dyn Fs,
        path: &Path,
        handle: &Arc<RuntimeConfigHandle>,
    ) {
        let Some(volume) = fs.volume_id(path) else {
            handle.set_io_strategy(IoStrategy::for_scheduler(fs.idles_between_io_queues(path)));
            return;
        };
        let backend = fs.backend_id();
        let mut held = locations.lock();
        let known = held
            .iter()
            .find(|at| at.backend == backend && at.volume == volume)
            .cloned();
        let location = if let Some(known) = known {
            drop(held);
            known
        } else {
            let location = Arc::new(Location {
                backend,
                volume,
                state: parking_lot::Mutex::new(State {
                    idles: None,
                    trees: Vec::new(),
                }),
            });
            held.push(Arc::clone(&location));
            drop(held);
            location
        };
        let mut state = location.state.lock();
        state.trees.retain(|tree| tree.strong_count() > 0);
        state.trees.push(Arc::downgrade(handle));
        // Asked under the location's lock: two trees opening on one new
        // location ask it once.
        let idles = *state
            .idles
            .get_or_insert_with(|| fs.idles_between_io_queues(path));
        handle.set_io_strategy(IoStrategy::for_scheduler(idles));
        drop(state);
    }

    /// Asks the location of `path` what it does now and sets the strategy for
    /// it in every tree still open on it, both under the location's lock: of
    /// two refreshes, the one that asked last is the one the trees run.
    /// Returns what the location does.
    pub(super) fn refresh(locations: &Locations, fs: &dyn Fs, path: &Path) -> bool {
        let location = fs
            .volume_id(path)
            .and_then(|volume| find(locations, fs.backend_id(), volume));
        let Some(location) = location else {
            return fs.idles_between_io_queues(path);
        };
        let mut state = location.state.lock();
        let idles = fs.idles_between_io_queues(path);
        state.idles = Some(idles);
        state.trees.retain(|tree| tree.strong_count() > 0);
        let strategy = IoStrategy::for_scheduler(idles);
        for tree in state.trees.iter().filter_map(Weak::upgrade) {
            tree.set_io_strategy(strategy);
        }
        drop(state);
        idles
    }
}

#[cfg(test)]
mod tests;

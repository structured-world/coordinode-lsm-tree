// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! What a flush does on the device under a tree, chosen from the device's I/O
//! scheduler.
//!
//! An I/O scheduler that serves each queue in turn and idles for its next
//! request (BFQ) and one that does not (`mq-deadline`, `none`) reward opposite
//! choices on the flush path: what halves a flush's syncs on one doubles them
//! on the other. Which one a device runs is a property of its physical
//! location, so it is asked once per location (a backend's volume) and kept
//! here with the trees open on it. A tree takes the strategy into its
//! [`RuntimeConfig`](crate::runtime_config::RuntimeConfig) when it opens, and
//! [`refresh_io_policy`] asks the location again and hands every tree open on
//! it the strategy for what it runs now. A flush finds the strategy in the
//! runtime configuration it loads anyway: it never asks.

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
}

impl IoStrategy {
    /// Syncs side by side and refills beside the write: a device whose
    /// scheduler does not idle between queues, or one nothing is known about.
    pub(crate) const NONE: Self = Self {
        serial_after_writeback: false,
        refill_after_install: false,
    };

    /// The strategy for a device whose I/O scheduler idles between queues
    /// (`idles`), or does not.
    pub(crate) const fn for_scheduler(idles: bool) -> Self {
        Self {
            serial_after_writeback: idles,
            refill_after_install: idles,
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

/// Takes the strategy for the location of `path` on `fs` into the runtime
/// configuration `handle` holds, and keeps `handle` with the location so a
/// later [`refresh_io_policy`] reaches it. The location is asked only when
/// no tree has asked about it before.
pub(crate) fn attach(fs: &dyn Fs, path: &Path, handle: &Arc<RuntimeConfigHandle>) {
    registry::attach(fs, path, handle);
}

/// Switches every tree open on the location of `path` to the strategy for
/// the I/O scheduler its device runs now.
///
/// Call it after the scheduler of a device has been changed (for example
/// through `/sys/block/<disk>/queue/scheduler`): trees opened before keep the
/// strategy they opened with until then.
///
/// Returns whether the device's scheduler idles between queues (BFQ).
///
/// # Examples
///
/// ```
/// use lsm_tree::fs::StdFs;
///
/// let folder = tempfile::tempdir()?;
/// // Whatever the host runs, the call is cheap enough for a timer.
/// let _idles = lsm_tree::refresh_io_policy(&StdFs, folder.path());
/// # Ok::<(), std::io::Error>(())
/// ```
pub fn refresh_io_policy(fs: &dyn Fs, path: &Path) -> bool {
    registry::refresh(fs, path)
}

/// The locations asked about, with the trees open on each.
#[cfg(feature = "std")]
mod registry {
    use super::IoStrategy;
    use crate::fs::Fs;
    use crate::path::Path;
    use crate::runtime_config::handle::RuntimeConfigHandle;
    use alloc::sync::{Arc, Weak};
    use alloc::vec::Vec;

    /// One physical location: a backend's volume, what its scheduler was
    /// found to do, and the runtime configurations of the trees open on it.
    struct Location {
        backend: Option<u64>,
        volume: u64,
        idles: bool,
        trees: Vec<Weak<RuntimeConfigHandle>>,
    }

    /// Every location a tree is open on, in this process. A host fact read
    /// from the system, not tree state: what a device's scheduler does is one
    /// answer for every tree on it whatever opened them, so the registry is
    /// the process's, as the device is.
    static LOCATIONS: parking_lot::Mutex<Vec<Location>> = parking_lot::Mutex::new(Vec::new());

    /// Sets in `handle` the strategy for the location of `path`, asking it
    /// only when no tree has before, and keeps `handle` with it. A backend
    /// that names no volume for `path` is asked each time and kept nowhere.
    ///
    /// The strategy is set under the registry's lock, as a refresh sets its
    /// own: the one a tree runs is always the one its location records.
    pub(super) fn attach(fs: &dyn Fs, path: &Path, handle: &Arc<RuntimeConfigHandle>) {
        let Some(volume) = fs.volume_id(path) else {
            handle.set_io_strategy(IoStrategy::for_scheduler(fs.idles_between_io_queues(path)));
            return;
        };
        let backend = fs.backend_id();
        let mut locations = LOCATIONS.lock();
        // A location goes with its last tree: the registry holds the locations
        // trees are open on, not every one the process ever opened a tree on.
        locations.retain(|at| at.trees.iter().any(|tree| tree.strong_count() > 0));
        let known = locations
            .iter_mut()
            .find(|at| at.backend == backend && at.volume == volume);
        let location = if let Some(location) = known {
            location
        } else {
            // Asked under the lock: two trees opening on one new location
            // ask it once.
            let idles = fs.idles_between_io_queues(path);
            locations.push(Location {
                backend,
                volume,
                idles,
                trees: Vec::new(),
            });
            let Some(location) = locations.last_mut() else {
                handle.set_io_strategy(IoStrategy::for_scheduler(idles));
                return;
            };
            location
        };
        location.trees.retain(|tree| tree.strong_count() > 0);
        location.trees.push(Arc::downgrade(handle));
        handle.set_io_strategy(IoStrategy::for_scheduler(location.idles));
        drop(locations);
    }

    /// Asks the location of `path` what it does now and sets the strategy for
    /// it in every tree still open on it, both under the registry's lock: of
    /// two refreshes, the one that asked last is the one the trees run.
    /// Returns what the location does.
    pub(super) fn refresh(fs: &dyn Fs, path: &Path) -> bool {
        let Some(volume) = fs.volume_id(path) else {
            return fs.idles_between_io_queues(path);
        };
        let backend = fs.backend_id();
        let mut locations = LOCATIONS.lock();
        let idles = fs.idles_between_io_queues(path);
        let Some(location) = locations
            .iter_mut()
            .find(|at| at.backend == backend && at.volume == volume)
        else {
            return idles;
        };
        location.idles = idles;
        location.trees.retain(|tree| tree.strong_count() > 0);
        let strategy = IoStrategy::for_scheduler(idles);
        for tree in location.trees.iter().filter_map(Weak::upgrade) {
            tree.set_io_strategy(strategy);
        }
        drop(locations);
        idles
    }
}

/// Without `std` there is no process-wide registry and no scheduler to ask:
/// every tree runs the configured writeback and syncs.
#[cfg(not(feature = "std"))]
mod registry {
    use super::IoStrategy;
    use crate::fs::Fs;
    use crate::path::Path;
    use crate::runtime_config::handle::RuntimeConfigHandle;
    use alloc::sync::Arc;

    pub(super) fn attach(fs: &dyn Fs, path: &Path, handle: &Arc<RuntimeConfigHandle>) {
        handle.set_io_strategy(IoStrategy::for_scheduler(fs.idles_between_io_queues(path)));
    }

    pub(super) fn refresh(fs: &dyn Fs, path: &Path) -> bool {
        fs.idles_between_io_queues(path)
    }
}

#[cfg(test)]
mod tests;

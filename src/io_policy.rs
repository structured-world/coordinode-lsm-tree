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
    /// The blob files a flush writes start no writeback while written.
    blob_writeback_off: bool,
    /// A flush syncs its files one after another rather than side by side.
    serial_syncs: bool,
}

impl IoStrategy {
    /// Writeback and syncs as configured: a device whose scheduler does not
    /// idle between queues, or one nothing is known about.
    pub(crate) const NONE: Self = Self {
        blob_writeback_off: false,
        serial_syncs: false,
    };

    /// The strategy for a device whose I/O scheduler idles between queues
    /// (`idles`), or does not.
    pub(crate) const fn for_scheduler(idles: bool) -> Self {
        Self {
            blob_writeback_off: idles,
            serial_syncs: false,
        }
    }

    /// The bytes the blob files of a flush gather between the writebacks
    /// their writer starts: `configured`, or none.
    pub(crate) const fn blob_writeback_bytes(self, configured: u64) -> u64 {
        if self.blob_writeback_off {
            0
        } else {
            configured
        }
    }

    /// Whether a flush syncs its files one after another.
    pub(crate) const fn serial_syncs(self) -> bool {
        self.serial_syncs
    }
}

/// Takes the strategy for the location of `path` on `fs` into the runtime
/// configuration `handle` holds, and keeps `handle` with the location so a
/// later [`refresh_io_policy`] reaches it. The location is asked only when
/// no tree has asked about it before.
pub(crate) fn attach(fs: &dyn Fs, path: &Path, handle: &Arc<RuntimeConfigHandle>) {
    let strategy = registry::attach(fs, path, handle);
    handle.set_io_strategy(strategy);
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
    let idles = fs.idles_between_io_queues(path);
    let strategy = IoStrategy::for_scheduler(idles);
    for handle in registry::refresh(fs, path, idles) {
        handle.set_io_strategy(strategy);
    }
    idles
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

    /// Every location a tree has opened on, in this process. A host fact
    /// read from the system, not tree state: shared by every tree on it.
    static LOCATIONS: parking_lot::Mutex<Vec<Location>> = parking_lot::Mutex::new(Vec::new());

    /// The strategy for the location of `path`, asking it only when no tree
    /// has before, and `handle` kept with it. A backend that names no volume
    /// for `path` is asked each time and kept nowhere.
    pub(super) fn attach(
        fs: &dyn Fs,
        path: &Path,
        handle: &Arc<RuntimeConfigHandle>,
    ) -> IoStrategy {
        let Some(volume) = fs.volume_id(path) else {
            return IoStrategy::for_scheduler(fs.idles_between_io_queues(path));
        };
        let backend = fs.backend_id();
        let mut locations = LOCATIONS.lock();
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
                return IoStrategy::for_scheduler(idles);
            };
            location
        };
        location.trees.retain(|tree| tree.strong_count() > 0);
        location.trees.push(Arc::downgrade(handle));
        let idles = location.idles;
        drop(locations);
        IoStrategy::for_scheduler(idles)
    }

    /// Records what the location of `path` was found to do now, and returns
    /// the runtime configurations of the trees still open on it.
    pub(super) fn refresh(fs: &dyn Fs, path: &Path, idles: bool) -> Vec<Arc<RuntimeConfigHandle>> {
        let Some(volume) = fs.volume_id(path) else {
            return Vec::new();
        };
        let backend = fs.backend_id();
        let mut locations = LOCATIONS.lock();
        let Some(location) = locations
            .iter_mut()
            .find(|at| at.backend == backend && at.volume == volume)
        else {
            return Vec::new();
        };
        location.idles = idles;
        location.trees.retain(|tree| tree.strong_count() > 0);
        let trees = location.trees.iter().filter_map(Weak::upgrade).collect();
        drop(locations);
        trees
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
    use alloc::vec::Vec;

    pub(super) fn attach(
        fs: &dyn Fs,
        path: &Path,
        _handle: &Arc<RuntimeConfigHandle>,
    ) -> IoStrategy {
        IoStrategy::for_scheduler(fs.idles_between_io_queues(path))
    }

    pub(super) fn refresh(
        _fs: &dyn Fs,
        _path: &Path,
        _idles: bool,
    ) -> Vec<Arc<RuntimeConfigHandle>> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests;

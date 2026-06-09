// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Rate-limited background file deleter.
//!
//! Obsolete on-disk files (SSTs, blob files) are reclaimed in two steps so a
//! caller measuring disk footprint right after a logical delete (`clear`,
//! compaction) sees the drop without the foreground thread blocking on the
//! unlinks:
//!
//! 1. The reclaim site truncates the file to zero length **synchronously**
//!    (an O(1) metadata op that returns its data blocks to the filesystem
//!    immediately, so a `walkdir + sum(len)` scan reflects the reclaim at
//!    once), then
//! 2. enqueues the now-empty path here for the worker thread to `unlink` the
//!    directory entry **off the foreground path**, optionally rate-limited so
//!    a mass deletion (post-compaction, post-`clear` over thousands of files)
//!    does not storm the device (mirrors `RocksDB`'s `DeleteScheduler`).
//!
//! The control this module provides — *when* and *how fast* entries are
//! unlinked — is the part no filesystem primitive offers; the per-file op
//! itself (`truncate` / `unlink`) is a plain `Fs` call.
//!
//! # no-std
//!
//! Background deletion needs a thread, so the whole module is gated behind the
//! `std` feature. A `no_std` build reclaims files synchronously at the Drop
//! site instead (no scheduler installed). The public surface stays the same
//! shape so the call sites do not branch on the feature beyond "is a deleter
//! installed".
// no-std: synchronous reclaim at the Drop site (no background thread)

#![cfg(feature = "std")]

use crate::fs::Fs;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        mpsc::{Receiver, Sender, channel},
    },
    thread::JoinHandle,
    time::Duration,
};

/// A unit of background work: unlink `path` through `fs`.
struct DeleteJob {
    fs: Arc<dyn Fs>,
    path: PathBuf,
}

/// Rate-limited background file deleter.
///
/// Cheap to clone-share via `Arc`. Enqueuing is a non-blocking channel send;
/// the dedicated worker thread performs the unlinks. Dropping the deleter
/// signals the worker to drain every queued job and exit, so no file is leaked
/// on shutdown.
pub struct BackgroundDeleter {
    sender: Option<Sender<DeleteJob>>,
    worker: Option<JoinHandle<()>>,
}

impl core::fmt::Debug for BackgroundDeleter {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BackgroundDeleter").finish_non_exhaustive()
    }
}

impl BackgroundDeleter {
    /// Spawns a background deleter.
    ///
    /// `min_interval` throttles the worker: it waits at least that long
    /// between consecutive unlinks, capping the deletion rate so a mass
    /// reclaim does not contend with foreground I/O. `None` means unlimited
    /// (delete as fast as the queue drains, still off the foreground thread).
    #[must_use]
    pub fn new(min_interval: Option<Duration>) -> Self {
        let (sender, receiver) = channel::<DeleteJob>();
        let worker = std::thread::Builder::new()
            .name("lsm-deleter".into())
            .spawn(move || Self::run(&receiver, min_interval))
            .ok();
        Self {
            sender: Some(sender),
            worker,
        }
    }

    /// Enqueues `path` for background unlink through `fs`.
    ///
    /// Non-blocking. If the worker thread has already exited (shutdown in
    /// progress) the job is dropped — the file is then reclaimed on next
    /// recovery's orphan sweep, never leaked silently into correctness.
    pub fn enqueue(&self, fs: Arc<dyn Fs>, path: PathBuf) {
        if let Some(sender) = &self.sender {
            // A send error means the worker is gone; the path falls to the
            // recovery orphan sweep rather than blocking the caller.
            let _ = sender.send(DeleteJob { fs, path });
        }
    }

    /// Worker loop: unlink each queued path, honoring the rate cap. Exits when
    /// the channel closes (the deleter was dropped) and the queue is drained.
    fn run(receiver: &Receiver<DeleteJob>, min_interval: Option<Duration>) {
        while let Ok(job) = receiver.recv() {
            if let Err(e) = job.fs.remove_file(&job.path) {
                log::warn!(
                    "background deleter failed to unlink {}: {e:?}",
                    job.path.display(),
                );
            }
            if let Some(interval) = min_interval {
                std::thread::sleep(interval);
            }
        }
    }
}

impl Drop for BackgroundDeleter {
    fn drop(&mut self) {
        // Close the channel so the worker's `recv` returns `Err` once the
        // queue is empty, then join it — every already-enqueued unlink runs
        // before we return, so a tree close does not leak obsolete files.
        drop(self.sender.take());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;
    use crate::fs::{Fs, FsOpenOptions, MemFs};
    use std::io::Write;

    #[test]
    fn drains_queued_deletions_on_drop() {
        let fs: Arc<dyn Fs> = Arc::new(MemFs::default());
        let paths: Vec<PathBuf> = (0..16).map(|i| PathBuf::from(format!("/f{i}"))).collect();
        for p in &paths {
            let mut f = fs
                .open(p, &FsOpenOptions::new().write(true).create(true))
                .unwrap();
            f.write_all(b"data").unwrap();
            f.flush().unwrap();
            assert!(fs.open(p, &FsOpenOptions::new().read(true)).is_ok());
        }

        {
            let deleter = BackgroundDeleter::new(None);
            for p in &paths {
                deleter.enqueue(Arc::clone(&fs), p.clone());
            }
            // Drop drains the queue and joins the worker: every enqueued unlink
            // has completed by the time this scope ends.
        }

        for p in &paths {
            assert!(
                fs.open(p, &FsOpenOptions::new().read(true)).is_err(),
                "{} should have been unlinked by the background deleter",
                p.display(),
            );
        }
    }
}

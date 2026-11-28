// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

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
//! # Synchronous truncate is conditional
//!
//! Step 1 only runs when the reclaim site can confirm it owns the **sole** hard
//! link to the inode (link count == 1, via [`Fs::hard_link_count`]). Truncating
//! a shared inode would zero a checkpoint's hard-linked copy too, so when the
//! link is shared — or the count can't be determined (e.g. a backend without a
//! link-count primitive, currently non-Unix `StdFs`) — the truncate is skipped
//! and reclaim is the async unlink (step 2) alone. The reclaim is still correct
//! in that case; the *immediate* footprint drop (step 1) is the part that's
//! conditional on link-count support, so it is effectively a Unix-only fast
//! path today.
//!
//! [`Fs::hard_link_count`]: crate::fs::Fs::hard_link_count
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

/// Jobs the worker takes off its channel, at most, to find the evictions
/// queued behind one: what it holds back in memory while it walks the cache.
const EVICT_LOOKAHEAD: usize = 1_024;

/// A unit of background work.
enum DeleteJob {
    /// Unlink `path` through `fs`.
    Unlink { fs: Arc<dyn Fs>, path: PathBuf },
    /// Take a deleted file's entries out of `cache`: no read reaches them
    /// again, and until evicted they hold capacity live blocks could use.
    Evict {
        cache: Arc<crate::Cache>,
        dead: crate::cache::DeadFile,
    },
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
    /// The liveness of the tree whose files this deleter removes, given up
    /// only once the worker removed the last of them.
    life: Option<Arc<crate::close::Life>>,
    /// The cache the tree's files are read through and the tree's id there,
    /// for a deleter that also evicts what its deleted files left cached.
    owner: Option<(Arc<crate::Cache>, crate::TreeId)>,
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
        Self::spawn(min_interval, None, None)
    }

    /// A tree's deleter, keeping the tree open until its queue is drained,
    /// and evicting from `cache` what the files it deletes left there under
    /// the tree's id.
    pub(crate) fn for_tree(
        min_interval: Option<Duration>,
        life: Arc<crate::close::Life>,
        cache: Arc<crate::Cache>,
        tree: crate::TreeId,
    ) -> Self {
        Self::spawn(min_interval, Some(life), Some((cache, tree)))
    }

    fn spawn(
        min_interval: Option<Duration>,
        life: Option<Arc<crate::close::Life>>,
        owner: Option<(Arc<crate::Cache>, crate::TreeId)>,
    ) -> Self {
        let (sender, receiver) = channel::<DeleteJob>();
        let worker = std::thread::Builder::new()
            .name("lsm-deleter".into())
            .spawn(move || Self::run(&receiver, min_interval))
            .ok();
        Self {
            sender: Some(sender),
            worker,
            life,
            owner,
        }
    }

    /// Queues the eviction of a deleted table's cached blocks, rows and
    /// directories. Called once no read can reach the table again, from
    /// whichever thread dropped its last handle: the walk over the cache runs
    /// on the worker, never on a reader.
    pub(crate) fn evict_table(&self, table: crate::TableId) {
        if let Some((cache, tree)) = &self.owner {
            self.send(DeleteJob::Evict {
                cache: Arc::clone(cache),
                dead: crate::cache::DeadFile::Table { tree: *tree, table },
            });
        }
    }

    /// [`Self::evict_table`] for a deleted blob file's cached frames and
    /// records.
    pub(crate) fn evict_blob_file(&self, file: crate::vlog::BlobFileId) {
        if let Some((cache, tree)) = &self.owner {
            self.send(DeleteJob::Evict {
                cache: Arc::clone(cache),
                dead: crate::cache::DeadFile::BlobFile { tree: *tree, file },
            });
        }
    }

    /// Hands `job` to the worker, or does it here when the worker is gone.
    fn send(&self, job: DeleteJob) {
        match &self.sender {
            // `send` only fails if the receiver (worker) is gone; it hands the
            // job back in the error.
            Some(sender) => {
                if let Err(std::sync::mpsc::SendError(job)) = sender.send(job) {
                    Self::run_now(job);
                }
            }
            None => Self::run_now(job),
        }
    }

    /// Enqueues `path` for background unlink through `fs`.
    ///
    /// Non-blocking on the happy path. If the worker is not running — the
    /// thread failed to spawn in [`Self::new`], or the channel is otherwise
    /// disconnected — the unlink is performed **synchronously** instead of
    /// silently dropped: the caller has typically already truncated the file,
    /// so dropping the job would leave a zero-length directory entry behind.
    pub fn enqueue(&self, fs: Arc<dyn Fs>, path: PathBuf) {
        self.send(DeleteJob::Unlink { fs, path });
    }

    /// Synchronous fallback for when the background worker is gone.
    fn run_now(job: DeleteJob) {
        match job {
            DeleteJob::Unlink { fs, path } => {
                if let Err(e) = fs.remove_file(&path) {
                    log::warn!(
                        "background deleter sync fallback failed to unlink {}: {e:?}",
                        path.display(),
                    );
                }
            }
            // Left to the cache's own eviction: this runs on the thread that
            // dropped the file's last handle, which may be a reader's, and
            // the walk over the cache is not its to pay for.
            DeleteJob::Evict { .. } => {}
        }
    }

    /// Worker loop: unlink each queued path, honoring the rate cap, and evict
    /// what deleted files left cached. Exits when the channel closes (the
    /// deleter was dropped) and the queue is drained.
    fn run(receiver: &Receiver<DeleteJob>, min_interval: Option<Duration>) {
        // Jobs taken off the channel while looking for evictions to batch,
        // in the order they were queued.
        let mut held: std::collections::VecDeque<DeleteJob> = std::collections::VecDeque::new();
        loop {
            let job = match held.pop_front() {
                Some(job) => job,
                None => match receiver.recv() {
                    Ok(job) => job,
                    Err(_) => return,
                },
            };
            match job {
                DeleteJob::Unlink { fs, path } => {
                    if let Err(e) = fs.remove_file(&path) {
                        log::warn!(
                            "background deleter failed to unlink {}: {e:?}",
                            path.display(),
                        );
                    }
                    if let Some(interval) = min_interval {
                        std::thread::sleep(interval);
                    }
                }
                DeleteJob::Evict { cache, dead } => {
                    // The inputs of one compaction are dropped together, each
                    // queueing its eviction and then its unlink: one walk
                    // over the cache takes out every file whose eviction is
                    // queued, the unlinks between them kept in their order.
                    // Stopping at the first unlink walked the whole cache
                    // once per file.
                    let mut files = vec![dead];
                    let mut kept = std::collections::VecDeque::with_capacity(held.len());
                    let queued = core::mem::take(&mut held).into_iter().chain(
                        core::iter::from_fn(|| receiver.try_recv().ok()).take(EVICT_LOOKAHEAD),
                    );
                    for job in queued {
                        match job {
                            DeleteJob::Evict { cache: other, dead }
                                if Arc::ptr_eq(&other, &cache) =>
                            {
                                files.push(dead);
                            }
                            other => kept.push_back(other),
                        }
                    }
                    held = kept;
                    cache.evict_files(&files);
                }
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
        // Only now: the tree may close on this thread once the worker is gone.
        drop(self.life.take());
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test code")]
mod tests;

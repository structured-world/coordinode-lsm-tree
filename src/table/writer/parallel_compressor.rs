// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Parallel block-compression pipeline for table writes.
//!
//! The table writer's CPU-bound per-block work (compress → encrypt → checksum
//! → ecc, [`Block::prepare_with_flags`]) is the single biggest serial cost
//! during compaction. This module farms that work out to worker threads while
//! the writer keeps the file writes (and the byte-offset-dependent index
//! registration) strictly ordered on its own thread.
//!
//! Threads are reached through the [`CompactionSpawner`] seam, not hard-wired
//! to any one pool: the default [`RayonSpawner`] backs onto a shared
//! [`rayon::ThreadPool`] (predictable thread count across many trees), but a
//! caller can inject any executor. The whole module is `std`-only — there are
//! no threads below `std`, so a `no_std` build simply never constructs a
//! pipeline and the writer takes its flat serial path.
//!
//! ## Ordering and backpressure
//!
//! Each submitted block gets a monotonically increasing sequence number.
//! Workers store their finished [`PreparedBlock`] under that number in a shared
//! reorder map; the writer drains strictly in sequence order via
//! [`BlockCompressor::take_next`], so on-disk block order is identical to the
//! serial path regardless of which worker finishes first. The writer caps the
//! number of in-flight blocks (submitted but not yet drained) so a huge SST
//! never buffers its entire compressed output: when the cap is reached it
//! drains (and writes) one block before submitting the next.
//!
//! ## Deadlock freedom (help-first draining)
//!
//! Jobs live in a shared queue; a spawned task is only a TOKEN that claims one
//! queued job. When the writer needs a block that is not ready, it first claims
//! and runs queued jobs on its own thread, and parks only once the queue is
//! empty (every remaining job is then executing on a worker). A writer running
//! ON one of the spawner's own threads — or against a fully saturated or
//! one-worker pool — therefore degrades to the serial path instead of waiting
//! on a token task that can never run.

// `Box` for the (no_std-able) CompactionSpawner trait; under std it's in the
// prelude. Everything below the trait is the std-only parallel pipeline.
#[cfg(feature = "std")]
use crate::{
    CompressionType, TableId,
    table::block::{Block, BlockIdentity, BlockTransform, BlockType, PreparedBlock},
};
#[cfg(not(feature = "std"))]
use alloc::boxed::Box;
#[cfg(feature = "std")]
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Condvar, Mutex, PoisonError},
};

#[cfg(all(feature = "std", zstd_any))]
use crate::compression::ZstdDictionary;

#[cfg(feature = "std")]
use crate::encryption::EncryptionProvider;

/// Caller-injectable execution backend for parallel block compression.
///
/// The pipeline needs exactly one capability: run a `FnOnce` on *some* worker,
/// fire-and-forget, in any order. Implement this to plug a custom thread pool
/// (e.g. an RTOS scheduler on a threaded `no_std` target) in place of the
/// default [`RayonSpawner`]. Result ordering is the pipeline's concern, not the
/// spawner's, so an implementation may run tasks on any thread in any order.
pub trait CompactionSpawner: Send + Sync {
    /// Schedules `task` to run on a worker. Must not block the caller.
    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>);
}

/// Default [`CompactionSpawner`] backed by a [`rayon::ThreadPool`].
///
/// Wrapping the pool in `Arc` lets the same pool be shared across many trees
/// (pass one built pool to several `Config`s) so thread count stays bounded by
/// the pool size rather than by the number of open trees.
#[cfg(feature = "parallel")]
pub struct RayonSpawner {
    pool: Arc<rayon::ThreadPool>,
}

#[cfg(feature = "parallel")]
impl RayonSpawner {
    /// Builds a private pool with `threads` workers.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::Io`] if the OS refuses to start the worker
    /// threads.
    pub fn with_threads(threads: usize) -> crate::Result<Self> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|i| format!("lsm-compress-{i}"))
            .build()
            .map_err(|e| crate::Error::Io(crate::io::Error::other(e.to_string())))?;
        Ok(Self {
            pool: Arc::new(pool),
        })
    }

    /// Wraps an existing pool, sharing it with whoever else holds the `Arc`.
    #[must_use]
    pub fn from_pool(pool: Arc<rayon::ThreadPool>) -> Self {
        Self { pool }
    }
}

#[cfg(feature = "parallel")]
impl CompactionSpawner for RayonSpawner {
    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        self.pool.spawn(task);
    }
}

/// One submitted, not-yet-executed transform job. Lives in the shared queue so
/// that ANY thread — a pool worker via its token task, or the writer itself
/// inside [`BlockCompressor::take_next`] — can claim and run it.
#[cfg(feature = "std")]
struct PendingJob {
    seq: u64,
    encoded: Vec<u8>,
    extra_flags: u8,
}

/// Shared pipeline state: the job queue, the reorder slot for finished blocks
/// (keyed by submission sequence number), and the constant per-SST transform
/// parameters every job needs.
#[cfg(feature = "std")]
struct Shared {
    queue: Mutex<VecDeque<PendingJob>>,
    ready: Mutex<BTreeMap<u64, crate::Result<PreparedBlock<'static>>>>,
    woke: Condvar,

    // Constant transform parameters, read by whichever thread runs a job.
    table_id: TableId,
    compression: CompressionType,
    encryption: Option<Arc<dyn EncryptionProvider>>,
    #[cfg(zstd_any)]
    zstd_dict: Option<Arc<ZstdDictionary>>,
    ecc: Option<crate::table::block::EccParams>,
}

#[cfg(feature = "std")]
impl Shared {
    /// Claims one queued job and runs it to completion, publishing the result
    /// into the reorder slot. Returns `false` when the queue was empty (the
    /// job this token was spawned for was already helped to completion by
    /// another thread — a cheap no-op).
    fn run_one(&self) -> bool {
        let job = {
            let mut queue = self.queue.lock().unwrap_or_else(PoisonError::into_inner);
            queue.pop_front()
        };
        let Some(job) = job else {
            return false;
        };
        let result = prepare_owned(
            &job.encoded,
            self.table_id,
            self.compression,
            self.encryption.as_deref(),
            #[cfg(zstd_any)]
            self.zstd_dict.as_deref(),
            self.ecc,
            job.extra_flags,
        );
        let mut ready = self.ready.lock().unwrap_or_else(PoisonError::into_inner);
        ready.insert(job.seq, result);
        drop(ready);
        self.woke.notify_all();
        true
    }
}

/// Ordered parallel block-preparation pipeline.
///
/// Holds the per-writer transform parameters (constant across the SST) and the
/// shared reorder slot. The writer feeds encoded block buffers in via
/// [`Self::submit`] and pulls finished blocks back out, in submission order,
/// via [`Self::take_next`].
#[cfg(feature = "std")]
pub struct BlockCompressor {
    spawner: Arc<dyn CompactionSpawner>,
    shared: Arc<Shared>,

    next_submit: u64,
    next_drain: u64,
}

#[cfg(feature = "std")]
impl BlockCompressor {
    pub fn new(
        spawner: Arc<dyn CompactionSpawner>,
        table_id: TableId,
        compression: CompressionType,
        encryption: Option<Arc<dyn EncryptionProvider>>,
        #[cfg(zstd_any)] zstd_dict: Option<Arc<ZstdDictionary>>,
        ecc: Option<crate::table::block::EccParams>,
    ) -> Self {
        Self {
            spawner,
            shared: Arc::new(Shared {
                queue: Mutex::new(VecDeque::new()),
                ready: Mutex::new(BTreeMap::new()),
                woke: Condvar::new(),
                table_id,
                compression,
                encryption,
                #[cfg(zstd_any)]
                zstd_dict,
                ecc,
            }),
            next_submit: 0,
            next_drain: 0,
        }
    }

    /// Number of blocks submitted but not yet drained (in flight or buffered).
    pub fn pending(&self) -> usize {
        // next_submit >= next_drain always holds (drain never outruns submit).
        usize::try_from(self.next_submit - self.next_drain).unwrap_or(usize::MAX)
    }

    /// Submits an encoded block buffer for preparation on a worker thread.
    ///
    /// `extra_flags` carries the per-KV checksum-footer bit (the one bit the
    /// transform can't derive), mirroring the serial
    /// [`Block::write_into_with_flags`] contract.
    pub fn submit(&mut self, encoded: Vec<u8>, extra_flags: u8) {
        let seq = self.next_submit;
        self.next_submit += 1;

        // The job goes into the shared queue, not into the spawned closure:
        // the spawned task is only a TOKEN that claims one queued job. Any
        // thread can claim — including the writer itself in `take_next` — so
        // a job is never stranded behind a saturated or re-entrant pool.
        {
            let mut queue = self
                .shared
                .queue
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            queue.push_back(PendingJob {
                seq,
                encoded,
                extra_flags,
            });
        }

        let shared = Arc::clone(&self.shared);
        self.spawner.spawn(Box::new(move || {
            let _ = shared.run_one();
        }));
    }

    /// Returns the next-in-sequence block, running queued transform jobs on
    /// THIS thread while it is not ready ("help-first" draining).
    ///
    /// Helping is what makes the pipeline deadlock-free by construction: if
    /// the spawner's workers are all busy — or the caller itself is running ON
    /// one of the spawner's threads, so its token tasks are queued behind this
    /// very call — the drain claims the pending jobs from the shared queue and
    /// executes them inline instead of parking. A one-worker (or fully
    /// saturated) pool degrades to the serial path, never to a deadlock. The
    /// writer only parks once the queue is empty, which means every remaining
    /// in-flight job is already EXECUTING on some worker and will publish.
    ///
    /// Returns `None` only when nothing is in flight ([`Self::pending`] is 0).
    /// The inner `Result` carries any transform error raised on the worker.
    pub fn take_next(&mut self) -> Option<crate::Result<PreparedBlock<'static>>> {
        if self.next_drain == self.next_submit {
            return None;
        }
        let seq = self.next_drain;
        loop {
            {
                let mut ready = self
                    .shared
                    .ready
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if let Some(result) = ready.remove(&seq) {
                    self.next_drain += 1;
                    return Some(result);
                }
            }

            // Not ready: help. Jobs are queued FIFO and `seq` is the oldest
            // undrained submission, so the first claimed job is `seq` itself
            // unless a worker already claimed it.
            if self.shared.run_one() {
                continue;
            }

            // Queue empty: `seq` is executing on a worker right now (this
            // writer thread is the only submitter, so no new job can appear
            // while it sits here). Park until the worker publishes.
            let mut ready = self
                .shared
                .ready
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            loop {
                if let Some(result) = ready.remove(&seq) {
                    self.next_drain += 1;
                    return Some(result);
                }
                ready = self
                    .shared
                    .woke
                    .wait(ready)
                    .unwrap_or_else(PoisonError::into_inner);
            }
        }
    }
}

/// Worker-side block preparation: rebuild the transform from owned parts, run
/// the pipeline, and detach the result from the borrowed `encoded` buffer.
#[cfg(feature = "std")]
fn prepare_owned(
    encoded: &[u8],
    table_id: TableId,
    compression: CompressionType,
    encryption: Option<&dyn EncryptionProvider>,
    #[cfg(zstd_any)] zstd_dict: Option<&ZstdDictionary>,
    ecc: Option<crate::table::block::EccParams>,
    extra_flags: u8,
) -> crate::Result<PreparedBlock<'static>> {
    let transform = BlockTransform::from_parts(
        compression,
        encryption,
        #[cfg(zstd_any)]
        zstd_dict,
    )?;
    let transform = if let Some(ecc) = ecc {
        transform.with_ecc(ecc)
    } else {
        transform
    };

    let identity = BlockIdentity {
        table_id,
        block_type: BlockType::Data,
        dict_id: compression.dict_id(),
        window_log: 0,
    };

    Ok(Block::prepare_with_flags(encoded, identity, &transform, extra_flags)?.into_owned())
}

#[cfg(test)]
mod tests;

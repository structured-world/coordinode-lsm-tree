// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

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

use crate::{
    CompressionType, TableId,
    table::block::{Block, BlockIdentity, BlockTransform, BlockType, PreparedBlock},
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Condvar, Mutex, PoisonError},
};

#[cfg(zstd_any)]
use crate::compression::ZstdDictionary;

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
            .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;
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

/// Shared reorder slot: finished blocks keyed by submission sequence number.
struct Shared {
    ready: Mutex<BTreeMap<u64, crate::Result<PreparedBlock<'static>>>>,
    woke: Condvar,
}

/// Ordered parallel block-preparation pipeline.
///
/// Holds the per-writer transform parameters (constant across the SST) and the
/// shared reorder slot. The writer feeds encoded block buffers in via
/// [`Self::submit`] and pulls finished blocks back out, in submission order,
/// via [`Self::take_next`].
pub struct BlockCompressor {
    spawner: Arc<dyn CompactionSpawner>,
    shared: Arc<Shared>,

    // Constant transform parameters, cloned into each job closure.
    table_id: TableId,
    compression: CompressionType,
    encryption: Option<Arc<dyn EncryptionProvider>>,
    #[cfg(zstd_any)]
    zstd_dict: Option<Arc<ZstdDictionary>>,
    ecc: Option<crate::table::block::EccParams>,

    next_submit: u64,
    next_drain: u64,
}

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
                ready: Mutex::new(BTreeMap::new()),
                woke: Condvar::new(),
            }),
            table_id,
            compression,
            encryption,
            #[cfg(zstd_any)]
            zstd_dict,
            ecc,
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

        let shared = Arc::clone(&self.shared);
        let table_id = self.table_id;
        let compression = self.compression;
        let encryption = self.encryption.clone();
        #[cfg(zstd_any)]
        let zstd_dict = self.zstd_dict.clone();
        let ecc = self.ecc;

        self.spawner.spawn(Box::new(move || {
            let result = prepare_owned(
                &encoded,
                table_id,
                compression,
                encryption.as_deref(),
                #[cfg(zstd_any)]
                zstd_dict.as_deref(),
                ecc,
                extra_flags,
            );
            let mut ready = shared.ready.lock().unwrap_or_else(PoisonError::into_inner);
            ready.insert(seq, result);
            drop(ready);
            shared.woke.notify_all();
        }));
    }

    /// Blocks until the next-in-sequence block is ready, then returns it.
    ///
    /// Returns `None` only when nothing is in flight ([`Self::pending`] is 0).
    /// The inner `Result` carries any transform error raised on the worker.
    pub fn take_next(&mut self) -> Option<crate::Result<PreparedBlock<'static>>> {
        if self.next_drain == self.next_submit {
            return None;
        }
        let seq = self.next_drain;
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

/// Worker-side block preparation: rebuild the transform from owned parts, run
/// the pipeline, and detach the result from the borrowed `encoded` buffer.
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
        tree_id: 0,
        table_id,
        // Provisional: the real on-disk offset is unknown until the serial
        // write step assigns it. prepare_with_flags does not consume the
        // offset today; if AAD-over-offset ever lands, encryption must move to
        // the (offset-bearing) write phase, since the offset depends on
        // post-compression size and so cannot be known at submit time.
        block_offset: 0,
        block_type: BlockType::Data,
        dict_id: compression.dict_id(),
        window_log: 0,
    };

    Ok(Block::prepare_with_flags(encoded, identity, &transform, extra_flags)?.into_owned())
}

#[cfg(test)]
mod tests {
    #![expect(clippy::expect_used, reason = "test code")]
    use super::*;

    /// Deterministic spawner that runs each task synchronously on submit.
    /// Exercises the full reorder/box machinery without thread timing.
    struct InlineSpawner;
    impl CompactionSpawner for InlineSpawner {
        fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
            task();
        }
    }

    fn encode_plain(payload: &[u8]) -> Vec<u8> {
        payload.to_vec()
    }

    /// Defers tasks and runs them in REVERSE submission order on demand, so the
    /// reorder buffer receives out-of-order completions (the inline spawner only
    /// ever completes in order, leaving the reordering logic untested).
    #[derive(Default)]
    struct ReverseSpawner {
        tasks: Mutex<Vec<Box<dyn FnOnce() + Send + 'static>>>,
    }
    impl ReverseSpawner {
        fn run_all_reverse(&self) {
            let mut tasks =
                std::mem::take(&mut *self.tasks.lock().unwrap_or_else(PoisonError::into_inner));
            tasks.reverse();
            for task in tasks {
                task();
            }
        }
    }
    impl CompactionSpawner for ReverseSpawner {
        fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
            self.tasks
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(task);
        }
    }

    #[test]
    fn take_next_returns_blocks_in_submission_order() {
        let mut c = BlockCompressor::new(
            Arc::new(InlineSpawner),
            7,
            CompressionType::None,
            None,
            #[cfg(zstd_any)]
            None,
            None,
        );
        assert_eq!(c.pending(), 0);
        assert!(c.take_next().is_none());

        c.submit(encode_plain(b"alpha"), 0);
        c.submit(encode_plain(b"beta"), 0);
        c.submit(encode_plain(b"gamma"), 0);
        assert_eq!(c.pending(), 3);

        let mut out = Vec::new();
        while c.pending() > 0 {
            let prepared = c
                .take_next()
                .expect("pending > 0 yields a block")
                .expect("plain block prepares without error");
            let mut buf = Vec::new();
            prepared.write_to(&mut buf).expect("write to vec");
            out.push(buf);
        }
        assert_eq!(out.len(), 3);
        assert!(c.take_next().is_none());
    }

    #[test]
    fn take_next_reorders_out_of_order_completions() {
        let spawner = Arc::new(ReverseSpawner::default());
        let mut c = BlockCompressor::new(
            spawner.clone() as Arc<dyn CompactionSpawner>,
            7,
            CompressionType::None,
            None,
            #[cfg(zstd_any)]
            None,
            None,
        );

        // Distinct uncompressed lengths (1, 2, 3) tag each block by submission
        // order; with no compression `uncompressed_length` == input length.
        c.submit(vec![0u8; 1], 0);
        c.submit(vec![0u8; 2], 0);
        c.submit(vec![0u8; 3], 0);
        assert_eq!(c.pending(), 3);

        // Complete the tasks in REVERSE order — the reorder buffer is filled
        // last-seq-first before any drain.
        spawner.run_all_reverse();

        // Despite reverse completion, take_next must yield submission order.
        for expected_len in [1u32, 2, 3] {
            let prepared = c
                .take_next()
                .expect("pending > 0 yields a block")
                .expect("plain block prepares without error");
            let mut buf = Vec::new();
            let header = prepared.write_to(&mut buf).expect("write to vec");
            assert_eq!(
                header.uncompressed_length, expected_len,
                "blocks must drain in submission order regardless of completion order",
            );
        }
        assert!(c.take_next().is_none());
    }
}

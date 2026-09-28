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
//! ## Ordering, backpressure and deadlock freedom
//!
//! The blocks run on an [`OrderedPipeline`]: the writer takes them back
//! strictly in submission order, so on-disk block order is identical to the
//! serial path whichever worker finishes first, and it helps run queued blocks
//! rather than wait on a pool that cannot reach them. The writer caps the
//! blocks in flight (submitted but not yet written) so a huge SST never
//! buffers its whole compressed output: at the cap it writes one block before
//! submitting the next, and the pipeline's ring is sized to that cap.
//!
//! ## Small blocks stay on the writer
//!
//! A block whose payload is below the inline threshold is prepared on the
//! writer thread as it is submitted: its transform costs less than handing it
//! to a worker and back.
//!
//! [`OrderedPipeline`]: super::ordered_pipeline::OrderedPipeline

// `Box` for the (no_std-able) CompactionSpawner trait; under std it's in the
// prelude. Everything below the trait is the std-only parallel pipeline.
#[cfg(feature = "std")]
use super::ordered_pipeline::{OrderedJob, OrderedPipeline};
#[cfg(feature = "std")]
use crate::{
    CompressionType, TableId,
    table::block::{Block, BlockIdentity, BlockTransform, BlockType, PreparedBlock},
};
#[cfg(not(feature = "std"))]
use alloc::boxed::Box;
#[cfg(feature = "std")]
use std::sync::Arc;

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

/// One block to prepare: its encoded payload and the per-KV checksum-footer
/// bit the transform cannot derive.
#[cfg(feature = "std")]
struct BlockJob {
    encoded: Vec<u8>,
    extra_flags: u8,
}

/// The transform settings of one table, read by whichever thread prepares a
/// block of it.
#[cfg(feature = "std")]
struct BlockSettings {
    table_id: TableId,
    compression: CompressionType,
    encryption: Option<Arc<dyn EncryptionProvider>>,
    #[cfg(zstd_any)]
    zstd_dict: Option<Arc<ZstdDictionary>>,
    #[cfg(zstd_any)]
    two_pass_seed: bool,
    ecc: Option<crate::table::block::EccParams>,
}

#[cfg(feature = "std")]
impl OrderedJob for BlockJob {
    type Context = BlockSettings;
    type Output = crate::Result<PreparedBlock<'static>>;

    fn run(self, settings: &BlockSettings) -> Self::Output {
        prepare_owned(
            &self.encoded,
            settings.table_id,
            TransformParams {
                compression: settings.compression,
                encryption: settings.encryption.as_deref(),
                #[cfg(zstd_any)]
                zstd_dict: settings.zstd_dict.as_deref(),
                #[cfg(zstd_any)]
                two_pass_seed: settings.two_pass_seed,
                ecc: settings.ecc,
            },
            self.extra_flags,
        )
    }
}

/// How a writer runs its block preparation on worker threads.
#[cfg(feature = "std")]
#[derive(Clone)]
pub struct ParallelCompression {
    /// Where the workers run.
    pub spawner: Arc<dyn CompactionSpawner>,
    /// How many workers a writer keeps busy at once.
    pub threads: usize,
    /// Blocks whose payload is below this many bytes are prepared on the
    /// writer thread; `None` derives it from the data block size.
    pub inline_below: Option<u32>,
}

/// Ordered parallel block-preparation pipeline for one table.
///
/// The writer feeds encoded block buffers in via [`Self::submit`] and pulls
/// finished blocks back out, in submission order, via [`Self::take_next`].
#[cfg(feature = "std")]
pub struct BlockCompressor {
    pipeline: OrderedPipeline<BlockJob>,
    /// Payloads shorter than this many bytes are prepared on the writer
    /// thread.
    inline_below: u64,
}

/// The share of a table's block length below which a block's payload is
/// prepared on the writer thread when no threshold is configured.
#[cfg(feature = "std")]
const INLINE_BELOW_BLOCK_DIVISOR: u64 = 8;

/// The inline threshold of a table cutting blocks at `block_len` bytes, when
/// none is configured.
#[cfg(feature = "std")]
pub fn derived_inline_below(block_len: u64) -> u64 {
    block_len / INLINE_BELOW_BLOCK_DIVISOR
}

#[cfg(feature = "std")]
impl BlockCompressor {
    /// A pipeline on `spawner` with up to `threads` workers, for a writer that
    /// keeps at most `in_flight` blocks outstanding, preparing payloads under
    /// `inline_below` bytes on the writer thread.
    #[expect(
        clippy::too_many_arguments,
        reason = "the table's transform settings, each feature-gated, plus the pipeline's shape"
    )]
    pub fn new(
        spawner: Arc<dyn CompactionSpawner>,
        threads: usize,
        in_flight: usize,
        inline_below: u64,
        table_id: TableId,
        compression: CompressionType,
        encryption: Option<Arc<dyn EncryptionProvider>>,
        #[cfg(zstd_any)] zstd_dict: Option<Arc<ZstdDictionary>>,
        #[cfg(zstd_any)] two_pass_seed: bool,
        ecc: Option<crate::table::block::EccParams>,
    ) -> Self {
        let settings = BlockSettings {
            table_id,
            compression,
            encryption,
            #[cfg(zstd_any)]
            zstd_dict,
            #[cfg(zstd_any)]
            two_pass_seed,
            ecc,
        };
        Self {
            pipeline: OrderedPipeline::new(spawner, settings, threads, in_flight),
            inline_below,
        }
    }

    /// Number of blocks submitted but not yet drained (in flight or buffered).
    pub fn pending(&self) -> usize {
        self.pipeline.pending()
    }

    /// Submits an encoded block buffer for preparation, on a worker thread or,
    /// below the inline threshold, right here.
    ///
    /// `extra_flags` carries the per-KV checksum-footer bit (the one bit the
    /// transform can't derive), mirroring the serial
    /// [`Block::write_into_with_flags`] contract.
    pub fn submit(&mut self, encoded: Vec<u8>, extra_flags: u8) {
        let inline = (encoded.len() as u64) < self.inline_below;
        let job = BlockJob {
            encoded,
            extra_flags,
        };
        if inline {
            self.pipeline.submit_inline(job);
        } else {
            self.pipeline.submit(job);
        }
    }

    /// Returns the next block in submission order, preparing queued blocks on
    /// this thread while it is not ready: a saturated or one-worker pool, or a
    /// writer running on one of the pool's own threads, degrades to the
    /// serial path instead of a deadlock.
    ///
    /// Returns `None` only when nothing is in flight ([`Self::pending`] is 0).
    /// The inner `Result` carries any transform error raised on the worker.
    pub fn take_next(&mut self) -> Option<crate::Result<PreparedBlock<'static>>> {
        self.pipeline.take_next()
    }
}

/// The transform settings a block is written under, borrowed from
/// [`BlockSettings`] for the duration of one job. Grouped rather than passed one by one: they are
/// read together, they are constant for a whole table, and every one of them
/// describes the same thing, how this block is encoded.
#[cfg(feature = "std")]
#[derive(Clone, Copy)]
struct TransformParams<'a> {
    compression: CompressionType,
    encryption: Option<&'a dyn EncryptionProvider>,
    #[cfg(zstd_any)]
    zstd_dict: Option<&'a ZstdDictionary>,
    #[cfg(zstd_any)]
    two_pass_seed: bool,
    ecc: Option<crate::table::block::EccParams>,
}

/// Worker-side block preparation: rebuild the transform from owned parts, run
/// the pipeline, and detach the result from the borrowed `encoded` buffer.
#[cfg(feature = "std")]
fn prepare_owned(
    encoded: &[u8],
    table_id: TableId,
    params: TransformParams<'_>,
    extra_flags: u8,
) -> crate::Result<PreparedBlock<'static>> {
    let TransformParams {
        compression,
        encryption,
        #[cfg(zstd_any)]
        zstd_dict,
        #[cfg(zstd_any)]
        two_pass_seed,
        ecc,
    } = params;
    let transform = BlockTransform::from_parts(
        compression,
        encryption,
        #[cfg(zstd_any)]
        zstd_dict,
    )?;
    #[cfg(zstd_any)]
    let transform = transform.with_two_pass_seed(two_pass_seed);
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

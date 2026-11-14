// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A flush's separated values compressed off the flush's thread.
//!
//! Each value is compressed alone, a record or a frame of its own, so a flush
//! that stops to compress each one spends its thread there. The flush hands
//! its values to the workers under the codec the blob file would write each
//! with now and goes on; it takes them back in the order it handed them over
//! and writes each then. A file's codec can change between the two (its own
//! dictionary is built from its first values, a file is cut at its target),
//! so the write takes the compressed bytes only when they were made under the
//! codec it writes with, and compresses the value again otherwise.
//!
//! The values go to the workers a batch at a time. A value of a few hundred
//! bytes compresses in about the time it takes to hand one job to a worker
//! and take its result back, so a job per value spent the flush's thread on
//! the handing over, and kept the pool's threads waking for work the size of
//! a wake; beside a table codec that needs those threads, it slowed the flush
//! instead. A batch holds enough values that the handing over is a small
//! part of it, and few enough that a flush of a thousand values still
//! spreads over the workers: batches of 64 KiB left such a flush three of
//! them, most compressed by the flush itself as it drained, and it wrote a
//! quarter slower than with a job per value.

use crate::{
    UserValue,
    key::InternalKey,
    table::writer::{
        ParallelCompression,
        ordered_pipeline::{OrderedJob, OrderedPipeline},
    },
    vlog::blob_file::writer::{OwnedCodec, PreCompressed},
};
use alloc::collections::VecDeque;

/// The value bytes a batch holds before it goes to a worker: some tens of
/// microseconds of compression, against the microsecond or two of handing a
/// job over. A value at or past it is a batch by itself.
const BATCH_BYTES: usize = 4 * 1_024;

/// The most values a batch holds, whatever their bytes: values of a few bytes
/// do not gather without bound.
const BATCH_VALUES: usize = 32;

/// One value to compress and the codec to compress it under.
type Value = (UserValue, OwnedCodec);

/// A batch of values to compress, each under its own codec.
struct BatchJob(Vec<Value>);

impl OrderedJob for BatchJob {
    type Context = ();
    type Output = Vec<crate::Result<PreCompressed>>;

    fn run(self, (): &()) -> Self::Output {
        self.0
            .into_iter()
            .map(|(value, codec)| codec.precompress(&value))
            .collect()
    }
}

/// A separated value waiting for its compressed bytes.
pub(crate) struct Separated {
    pub(crate) key: InternalKey,
    pub(crate) value: UserValue,
    pub(crate) class: u8,
}

/// Compresses a flush's separated values on the pool, handing them back in
/// the order they went in.
pub(crate) struct ValuesAhead {
    pipeline: OrderedPipeline<BatchJob>,
    /// The most batches with a worker or waiting for one at once.
    batches: usize,
    /// The values handed over and not yet taken back, oldest first.
    waiting: VecDeque<Separated>,
    /// The value bytes of `waiting`.
    queued_bytes: u64,
    /// The batch being gathered, and the value bytes in it.
    batch: Vec<Value>,
    batch_bytes: usize,
    /// What compressing the oldest batch taken back made, oldest first.
    ready: VecDeque<crate::Result<PreCompressed>>,
}

impl ValuesAhead {
    /// The values of a flush that has a pool to compress on. `table_on_pool`
    /// says the flush's table writer prepares its blocks on the same pool:
    /// the values then take one worker fewer than it has, since values on
    /// every thread leave the table's blocks to the flush's own thread
    /// (measured with tables at zstd 22: 7.27 to 6.20 us per write at 10 000
    /// keys). With the table off the pool that worker is the values' (4.26
    /// against 4.74 us at 70 000 keys without it).
    pub(crate) fn for_flush(
        parallel: Option<ParallelCompression>,
        table_on_pool: bool,
    ) -> Option<Self> {
        let parallel = parallel?;
        let threads = parallel
            .threads
            .saturating_sub(usize::from(table_on_pool))
            .max(1);
        // A batch per worker, and three more gathered behind each so no
        // worker waits for the flush.
        let batches = threads * 4;
        Some(Self {
            pipeline: OrderedPipeline::new(parallel.spawner, (), threads, batches),
            batches,
            waiting: VecDeque::new(),
            queued_bytes: 0,
            batch: Vec::new(),
            batch_bytes: 0,
            ready: VecDeque::new(),
        })
    }

    /// Whether a compressed value is at hand for the flush to write before
    /// it submits the next: taking one per value submitted keeps what is held
    /// compressed and unwritten to a batch or two.
    pub(crate) fn has_ready(&self) -> bool {
        !self.ready.is_empty()
    }

    /// The bytes of the values handed over and not yet taken back.
    pub(crate) fn queued_bytes(&self) -> u64 {
        self.queued_bytes
    }

    /// Hands `separated` over to be compressed under `codec`.
    pub(crate) fn submit(&mut self, separated: Separated, codec: OwnedCodec) {
        self.queued_bytes += separated.value.len() as u64;
        self.batch_bytes += separated.value.len();
        self.batch.push((separated.value.clone(), codec));
        self.waiting.push_back(separated);
        if self.batch_bytes >= BATCH_BYTES || self.batch.len() >= BATCH_VALUES {
            // Every batch the workers may hold is out: the oldest comes back
            // first, so the one going in has a place among them. Its values
            // follow the ones already at hand, in the order they went in.
            if self.pipeline.pending() >= self.batches
                && let Some(oldest) = self.pipeline.take_next()
            {
                self.ready.extend(oldest);
            }
            let batch = core::mem::take(&mut self.batch);
            self.batch_bytes = 0;
            self.pipeline.submit(BatchJob(batch));
        }
    }

    /// The oldest value submitted and what compressing it made, once it is
    /// compressed; `None` when nothing is waiting.
    pub(crate) fn take(&mut self) -> Option<crate::Result<(Separated, PreCompressed)>> {
        let separated = self.waiting.pop_front()?;
        self.queued_bytes -= separated.value.len() as u64;
        if self.ready.is_empty() {
            // The oldest values not yet taken are the oldest batch's; with
            // none out they are the batch being gathered, which the flush
            // wants now and so compresses itself.
            if self.pipeline.pending() == 0 {
                let batch = core::mem::take(&mut self.batch);
                self.batch_bytes = 0;
                self.pipeline.submit_inline(BatchJob(batch));
            }
            self.ready = self.pipeline.take_next()?.into();
        }
        let compressed = self.ready.pop_front()?;
        Some(compressed.map(|compressed| (separated, compressed)))
    }
}

// The values are compressed ahead only under a codec.
#[cfg(all(test, any(feature = "lz4", zstd_any)))]
#[expect(
    clippy::expect_used,
    reason = "test code: a fixture that is missing fails the test"
)]
mod tests;

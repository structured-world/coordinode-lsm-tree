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
//! instead. A batch holds enough bytes that the handing over is a small part
//! of it.

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
/// job over.
const BATCH_BYTES: usize = 64 * 1_024;

/// The most values a batch holds, whatever their bytes: values of a few bytes
/// do not gather without bound.
const BATCH_VALUES: usize = 512;

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
    /// The batch being gathered, and the value bytes in it.
    batch: Vec<Value>,
    batch_bytes: usize,
    /// What compressing the oldest batch taken back made, oldest first.
    ready: VecDeque<crate::Result<PreCompressed>>,
}

impl ValuesAhead {
    /// The values of a flush that has a pool to compress on.
    pub(crate) fn for_flush(parallel: Option<ParallelCompression>) -> Option<Self> {
        let parallel = parallel?;
        let threads = parallel.threads.max(1);
        // A batch per worker, and one more gathered behind each so no worker
        // waits for the flush.
        let batches = threads * 2;
        Some(Self {
            pipeline: OrderedPipeline::new(parallel.spawner, (), threads, batches),
            batches,
            waiting: VecDeque::new(),
            batch: Vec::new(),
            batch_bytes: 0,
            ready: VecDeque::new(),
        })
    }

    /// Whether a value has to be taken before the next is submitted: every
    /// batch the workers may hold is out, so the next one to fill would have
    /// nowhere to go.
    pub(crate) fn is_full(&self) -> bool {
        self.pipeline.pending() >= self.batches
    }

    /// Hands `separated` over to be compressed under `codec`.
    pub(crate) fn submit(&mut self, separated: Separated, codec: OwnedCodec) {
        self.batch_bytes += separated.value.len();
        self.batch.push((separated.value.clone(), codec));
        self.waiting.push_back(separated);
        if self.batch_bytes >= BATCH_BYTES || self.batch.len() >= BATCH_VALUES {
            let batch = core::mem::take(&mut self.batch);
            self.batch_bytes = 0;
            self.pipeline.submit(BatchJob(batch));
        }
    }

    /// The oldest value submitted and what compressing it made, once it is
    /// compressed; `None` when nothing is waiting.
    pub(crate) fn take(&mut self) -> Option<crate::Result<(Separated, PreCompressed)>> {
        let separated = self.waiting.pop_front()?;
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

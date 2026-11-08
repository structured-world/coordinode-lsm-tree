// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A flush's separated values compressed off the flush's thread.
//!
//! Each value is compressed alone, a record or a frame of its own, so a flush
//! that stops to compress each one spends its thread there. The flush hands a
//! value to the workers under the codec the blob file would write it with now
//! and goes on; it takes the values back in the order it handed them over and
//! writes each then. A file's codec can change between the two (its own
//! dictionary is built from its first values, a file is cut at its target),
//! so the write takes the compressed bytes only when they were made under the
//! codec it writes with, and compresses the value again otherwise.

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

/// Values in flight: enough that no worker waits for the flush, few enough
/// that a flush holds few compressed values, and that a file's codec changes
/// within them rarely.
const IN_FLIGHT: usize = 64;

struct ValueJob {
    /// The value's bytes, shared with the flush, which writes them too: a
    /// record or frame stored as it is holds the value itself.
    value: UserValue,
    codec: OwnedCodec,
}

impl OrderedJob for ValueJob {
    type Context = ();
    type Output = crate::Result<PreCompressed>;

    fn run(self, (): &()) -> Self::Output {
        self.codec.precompress(&self.value)
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
    pipeline: OrderedPipeline<ValueJob>,
    waiting: VecDeque<Separated>,
}

impl ValuesAhead {
    /// The values of a flush that has a pool to compress on.
    pub(crate) fn for_flush(parallel: Option<ParallelCompression>) -> Option<Self> {
        let parallel = parallel?;
        Some(Self {
            pipeline: OrderedPipeline::new(
                parallel.spawner,
                (),
                parallel.threads.max(1),
                IN_FLIGHT,
            ),
            waiting: VecDeque::with_capacity(IN_FLIGHT),
        })
    }

    /// Whether a value has to be taken before the next is submitted.
    pub(crate) fn is_full(&self) -> bool {
        self.waiting.len() >= IN_FLIGHT
    }

    /// Hands `separated` over to be compressed under `codec`.
    pub(crate) fn submit(&mut self, separated: Separated, codec: OwnedCodec) {
        self.pipeline.submit(ValueJob {
            value: separated.value.clone(),
            codec,
        });
        self.waiting.push_back(separated);
    }

    /// The oldest value submitted and what compressing it made, once it is
    /// compressed; `None` when nothing is waiting.
    pub(crate) fn take(&mut self) -> Option<crate::Result<(Separated, PreCompressed)>> {
        let separated = self.waiting.pop_front()?;
        let compressed = self.pipeline.take_next()?;
        Some(compressed.map(|compressed| (separated, compressed)))
    }
}

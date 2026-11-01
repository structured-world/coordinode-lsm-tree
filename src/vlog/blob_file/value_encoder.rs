// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Zstd for a flush's separated values, behind the flush's own thread.
//!
//! Every value is its own frame, so a flush that stops to compress each one
//! spends its thread there. The flush hands values to one background worker
//! and goes on; parallelism inside a frame is the codec's own. The writer
//! takes values back in the order it submitted them, so what reaches the blob
//! file and the table does not depend on who compressed it.

#[cfg(all(feature = "std", zstd_any))]
use crate::CompressionType;
use crate::{UserValue, key::InternalKey};

/// A separated value on its way to the blob file, without its bytes.
pub(crate) struct Separated {
    pub(crate) key: InternalKey,
    /// The value's decoded length.
    pub(crate) size: u32,
    pub(crate) class: u8,
}

#[cfg(all(feature = "std", zstd_any))]
mod workers {
    use super::{CompressionType, Separated, UserValue};
    use crate::table::writer::{
        ParallelCompression,
        ordered_pipeline::{OrderedJob, OrderedPipeline},
    };
    use alloc::collections::VecDeque;

    /// One worker: the flush runs alongside it, not on many of the pool's
    /// threads.
    const WORKERS: usize = 1;

    /// Values in flight: enough that the worker never waits for the writer, few
    /// enough that a flush does not hold many encoded values.
    const IN_FLIGHT: usize = 64;

    struct ValueJob(UserValue);

    struct ValueCodec {
        compression: CompressionType,
        two_pass_seed: bool,
    }

    impl OrderedJob for ValueJob {
        type Context = ValueCodec;
        type Output = crate::Result<Vec<u8>>;

        fn run(self, codec: &ValueCodec) -> Self::Output {
            crate::vlog::blob_file::writer::encode_value(
                codec.compression,
                None,
                codec.two_pass_seed,
                &self.0,
            )
            .map(alloc::borrow::Cow::into_owned)
        }
    }

    /// Compresses a flush's separated values in the background, handing them
    /// back in the order they went in.
    pub(crate) struct ValueEncoder {
        pipeline: OrderedPipeline<ValueJob>,
        waiting: VecDeque<Separated>,
    }

    impl ValueEncoder {
        /// An encoder for a flush, when it has a pool and a codec a worker can
        /// run: a plain zstd level, since a dictionary follows the key's domain
        /// and the writer settles that as it writes.
        pub(crate) fn for_flush(
            parallel: Option<ParallelCompression>,
            compression: CompressionType,
            two_pass_seed: bool,
        ) -> Option<Self> {
            if !matches!(compression, CompressionType::Zstd(_)) {
                return None;
            }
            Some(Self {
                pipeline: OrderedPipeline::new(
                    parallel?.spawner,
                    ValueCodec {
                        compression,
                        two_pass_seed,
                    },
                    WORKERS,
                    IN_FLIGHT,
                ),
                waiting: VecDeque::with_capacity(IN_FLIGHT),
            })
        }

        /// Whether a value has to be taken before the next is submitted.
        pub(crate) fn is_full(&self) -> bool {
            self.waiting.len() >= IN_FLIGHT
        }

        pub(crate) fn submit(&mut self, separated: Separated, value: UserValue) {
            self.waiting.push_back(separated);
            self.pipeline.submit(ValueJob(value));
        }

        /// The oldest value submitted, once it is encoded; `None` when nothing
        /// is waiting.
        pub(crate) fn take(&mut self) -> Option<crate::Result<(Separated, Vec<u8>)>> {
            let separated = self.waiting.pop_front()?;
            let encoded = self.pipeline.take_next()?;
            Some(encoded.map(|encoded| (separated, encoded)))
        }
    }
}

#[cfg(all(feature = "std", zstd_any))]
pub(crate) use workers::ValueEncoder;

/// Without threads or zstd there is nothing to hand to a worker: never built.
#[cfg(not(all(feature = "std", zstd_any)))]
pub(crate) struct ValueEncoder;

#[cfg(not(all(feature = "std", zstd_any)))]
#[expect(
    clippy::unused_self,
    clippy::needless_pass_by_ref_mut,
    reason = "the signatures of the real encoder"
)]
impl ValueEncoder {
    pub(crate) fn is_full(&self) -> bool {
        false
    }

    pub(crate) fn submit(&mut self, _: Separated, _: UserValue) {}

    pub(crate) fn take(&mut self) -> Option<crate::Result<(Separated, Vec<u8>)>> {
        None
    }
}

// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{BlockOffset, DataBlock, GlobalTableId, data_block::Iter as DataBlockIter};
use crate::{
    Cache, CompressionType, InternalValue, SeqNo, UserKey,
    comparator::SharedComparator,
    encryption::EncryptionProvider,
    file_accessor::FileAccessor,
    table::{
        BlockHandle,
        block::ParsedItem,
        block_index::{BlockIndexIter, BlockIndexIterImpl},
        util::load_block,
    },
};
use self_cell::self_cell;
use std::{path::PathBuf, sync::Arc};

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

type InnerIter<'a> = DataBlockIter<'a>;

pub enum Bound {
    Included(UserKey),
    Excluded(UserKey),
}
type Bounds = (Option<Bound>, Option<Bound>);

self_cell!(
    pub struct OwnedDataBlockIter {
        owner: DataBlock,

        #[covariant]
        dependent: InnerIter,
    }
);

impl OwnedDataBlockIter {
    fn seek_lower_inclusive(&mut self, needle: &[u8], seqno: SeqNo) -> bool {
        self.with_dependent_mut(|_, m| m.seek(needle, seqno))
    }

    fn seek_upper_inclusive(&mut self, needle: &[u8], seqno: SeqNo) -> bool {
        self.with_dependent_mut(|_, m| m.seek_upper(needle, seqno))
    }

    fn seek_lower_exclusive(&mut self, needle: &[u8], seqno: SeqNo) -> bool {
        self.with_dependent_mut(|_, m| m.seek_exclusive(needle, seqno))
    }

    fn seek_upper_exclusive(&mut self, needle: &[u8], seqno: SeqNo) -> bool {
        self.with_dependent_mut(|_, m| m.seek_upper_exclusive(needle, seqno))
    }

    pub fn seek_lower_bound(&mut self, bound: &Bound, seqno: SeqNo) -> bool {
        match bound {
            Bound::Included(key) => self.seek_lower_inclusive(key, seqno),
            Bound::Excluded(key) => self.seek_lower_exclusive(key, seqno),
        }
    }

    pub fn seek_upper_bound(&mut self, bound: &Bound, seqno: SeqNo) -> bool {
        match bound {
            Bound::Included(key) => self.seek_upper_inclusive(key, seqno),
            Bound::Excluded(key) => self.seek_upper_exclusive(key, seqno),
        }
    }
}

impl Iterator for OwnedDataBlockIter {
    type Item = InternalValue;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|block, iter| {
            iter.next().map(|item| item.materialize(&block.inner.data))
        })
    }
}

impl DoubleEndedIterator for OwnedDataBlockIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|block, iter| {
            iter.next_back()
                .map(|item| item.materialize(&block.inner.data))
        })
    }
}

fn create_data_block_reader(
    block: DataBlock,
    comparator: SharedComparator,
) -> crate::Result<OwnedDataBlockIter> {
    OwnedDataBlockIter::try_new(block, |b| b.try_iter(comparator))
}

pub struct Iter {
    table_id: GlobalTableId,
    path: Arc<PathBuf>,

    global_seqno: SeqNo,

    #[expect(clippy::struct_field_names)]
    index_iter: BlockIndexIterImpl,

    file_accessor: FileAccessor,
    cache: Arc<Cache>,
    compression: CompressionType,
    encryption: Option<Arc<dyn EncryptionProvider>>,
    #[cfg(zstd_any)]
    zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,
    comparator: SharedComparator,

    index_initialized: bool,

    lo_offset: BlockOffset,
    lo_data_block: Option<OwnedDataBlockIter>,

    hi_offset: BlockOffset,
    hi_data_block: Option<OwnedDataBlockIter>,

    range: Bounds,

    /// Set on unrecoverable block-init error so subsequent `next()` /
    /// `next_back()` calls return `None` instead of skipping past the
    /// corrupt block.
    poisoned: bool,

    #[cfg(feature = "metrics")]
    metrics: Arc<Metrics>,
}

impl Iter {
    #[expect(
        clippy::too_many_arguments,
        reason = "encryption, comparator and metrics add extra parameters to the constructor"
    )]
    pub fn new(
        table_id: GlobalTableId,
        global_seqno: SeqNo,
        path: Arc<PathBuf>,
        index_iter: BlockIndexIterImpl,
        file_accessor: FileAccessor,
        cache: Arc<Cache>,
        compression: CompressionType,
        encryption: Option<Arc<dyn EncryptionProvider>>,
        #[cfg(zstd_any)] zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,
        comparator: SharedComparator,
        #[cfg(feature = "metrics")] metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            table_id,
            path,

            global_seqno,

            index_iter,
            file_accessor,
            cache,
            compression,
            encryption,
            #[cfg(zstd_any)]
            zstd_dictionary,
            comparator,

            index_initialized: false,

            lo_offset: BlockOffset(0),
            lo_data_block: None,

            hi_offset: BlockOffset(u64::MAX),
            hi_data_block: None,

            range: (None, None),
            poisoned: false,

            #[cfg(feature = "metrics")]
            metrics,
        }
    }

    pub fn set_lower_bound(&mut self, bound: Bound) {
        self.range.0 = Some(bound);
    }

    pub fn set_upper_bound(&mut self, bound: Bound) {
        self.range.1 = Some(bound);
    }
}

impl Iter {
    /// Marks the iterator as permanently failed so subsequent `next()` /
    /// `next_back()` calls return `None` instead of skipping past the error.
    ///
    /// Returns `Some(Err(...))` so callers can `return self.poison(e)` directly
    /// inside `Iterator::next`.
    #[expect(
        clippy::unnecessary_wraps,
        reason = "matches Iterator::next return type"
    )]
    fn poison<E: Into<crate::Error>>(&mut self, err: E) -> Option<crate::Result<InternalValue>> {
        self.poisoned = true;
        Some(Err(err.into()))
    }
}

impl Iterator for Iter {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.poisoned {
            return None;
        }

        // Always try to keep iterating inside the already-materialized low data block first; this
        // lets callers consume multiple entries without touching the index or cache again.
        if let Some(block) = &mut self.lo_data_block
            && let Some(item) = block
                .next()
                .map(|mut v| {
                    v.key.seqno += self.global_seqno;
                    v
                })
                .map(Ok)
        {
            return Some(item);
        }

        if !self.index_initialized {
            // Lazily initialize the index iterator here (not in `new`) so callers can set bounds
            // before we incur any seek or I/O cost. Bounds exclusivity is enforced at the data-
            // block level; index seeks only narrow the span of blocks to touch.
            let mut ok = if let Some(bound) = &self.range.0 {
                // Seek to the first block whose end key is ≥ lower bound.
                // If this fails we can immediately conclude the range is empty.
                let key = match bound {
                    Bound::Included(k) | Bound::Excluded(k) => k,
                };
                self.index_iter.seek_lower(key, u64::MAX)
            } else {
                true
            };

            if ok && let Some(bound) = &self.range.1 {
                // Apply an upper-bound seek to cap the block span, but keep exact high-key
                // handling inside the data block so exclusivity is respected precisely.
                let key = match bound {
                    Bound::Included(k) | Bound::Excluded(k) => k,
                };
                ok = self.index_iter.seek_upper(key, u64::MAX);
            }

            self.index_initialized = true;

            if !ok {
                // No block in the index overlaps the requested window, so we clear state and return
                // EOF without attempting to touch any data blocks.
                self.lo_data_block = None;
                self.hi_data_block = None;
                return None;
            }
        }

        loop {
            let Some(handle) = self.index_iter.next() else {
                // No more block handles coming from the index.  Flush any pending items buffered on
                // the high side (used by reverse iteration) before signalling completion.
                if let Some(block) = &mut self.hi_data_block
                    && let Some(item) = block
                        .next()
                        .map(|mut v| {
                            v.key.seqno += self.global_seqno;
                            v
                        })
                        .map(Ok)
                {
                    return Some(item);
                }

                // Nothing left to serve; drop both buffers so the iterator can be reused safely.
                self.lo_data_block = None;
                self.hi_data_block = None;
                return None;
            };
            let handle = match handle {
                Ok(h) => h,
                Err(e) => return self.poison(e),
            };

            // Load the next data block via `load_block`, which checks the block cache
            // internally and validates block type on both cache-hit and I/O paths.
            let block = match load_block(
                self.table_id,
                &self.path,
                &self.file_accessor,
                &self.cache,
                &BlockHandle::new(handle.offset(), handle.size()),
                crate::table::block::BlockType::Data,
                self.compression,
                self.encryption.as_deref(),
                #[cfg(zstd_any)]
                self.zstd_dictionary.as_deref(),
                #[cfg(feature = "metrics")]
                &self.metrics,
            ) {
                Ok(b) => b,
                Err(e) => return self.poison(e),
            };
            let block = DataBlock::new(block);

            let mut reader = match create_data_block_reader(block, self.comparator.clone()) {
                Ok(r) => r,
                Err(e) => return self.poison(e),
            };

            // Forward path: seek the low side first to avoid returning entries below the lower
            // bound, then clamp the iterator on the high side. This guarantees iteration stays in
            // [low, high] with exact control over inclusivity/exclusivity.
            if let Some(bound) = &self.range.0 {
                reader.seek_lower_bound(bound, SeqNo::MAX);
            }
            if let Some(bound) = &self.range.1 {
                reader.seek_upper_bound(bound, SeqNo::MAX);
            }

            let item = reader.next();

            self.lo_offset = handle.offset();
            self.lo_data_block = Some(reader);

            if let Some(mut item) = item {
                item.key.seqno += self.global_seqno;

                // Serving the first item immediately avoids stashing it in a temporary buffer and
                // keeps block iteration semantics identical to the simple case at the top.
                return Some(Ok(item));
            }
        }
    }
}

impl DoubleEndedIterator for Iter {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.poisoned {
            return None;
        }

        // Mirror the forward iterator: prefer consuming buffered items from the high data block to
        // avoid touching the index once a block has been materialized.
        if let Some(block) = &mut self.hi_data_block
            && let Some(item) = block
                .next_back()
                .map(|mut v| {
                    v.key.seqno += self.global_seqno;
                    v
                })
                .map(Ok)
        {
            return Some(item);
        }

        if !self.index_initialized {
            // Mirror forward iteration: initialize lazily so bounds can be applied up-front. The
            // index only restricts which blocks we consider; tight bound enforcement happens in
            // the data block readers below.
            let mut ok = if let Some(bound) = &self.range.0 {
                let key = match bound {
                    Bound::Included(k) | Bound::Excluded(k) => k,
                };
                self.index_iter.seek_lower(key, u64::MAX)
            } else {
                true
            };

            if ok && let Some(bound) = &self.range.1 {
                let key = match bound {
                    Bound::Included(k) | Bound::Excluded(k) => k,
                };
                ok = self.index_iter.seek_upper(key, u64::MAX);
            }

            self.index_initialized = true;

            if !ok {
                // No index span overlaps the requested window; clear both buffers and finish early.
                self.lo_data_block = None;
                self.hi_data_block = None;
                return None;
            }
        }

        loop {
            let Some(handle) = self.index_iter.next_back() else {
                // Once we exhaust the index in reverse order, flush any items that were buffered on
                // the low side (set when iterating forward first) before signalling completion.
                if let Some(block) = &mut self.lo_data_block
                    && let Some(item) = block
                        .next_back()
                        .map(|mut v| {
                            v.key.seqno += self.global_seqno;
                            v
                        })
                        .map(Ok)
                {
                    return Some(item);
                }

                // Nothing left to produce; reset both buffers to keep the iterator reusable.
                self.lo_data_block = None;
                self.hi_data_block = None;
                return None;
            };
            let handle = match handle {
                Ok(h) => h,
                Err(e) => return self.poison(e),
            };

            // Load the next data block via `load_block`, which checks the block cache
            // internally and validates block type on both cache-hit and I/O paths.
            let block = match load_block(
                self.table_id,
                &self.path,
                &self.file_accessor,
                &self.cache,
                &BlockHandle::new(handle.offset(), handle.size()),
                crate::table::block::BlockType::Data,
                self.compression,
                self.encryption.as_deref(),
                #[cfg(zstd_any)]
                self.zstd_dictionary.as_deref(),
                #[cfg(feature = "metrics")]
                &self.metrics,
            ) {
                Ok(b) => b,
                Err(e) => return self.poison(e),
            };
            let block = DataBlock::new(block);

            let mut reader = match create_data_block_reader(block, self.comparator.clone()) {
                Ok(r) => r,
                Err(e) => return self.poison(e),
            };

            // Reverse path: clamp the high side first so `next_back` never yields an entry above
            // the upper bound, then apply the low-side seek to avoid stepping below the lower
            // bound during reverse traversal.
            if let Some(bound) = &self.range.1 {
                reader.seek_upper_bound(bound, SeqNo::MAX);
            }
            if let Some(bound) = &self.range.0 {
                reader.seek_lower_bound(bound, SeqNo::MAX);
            }

            let item = reader.next_back();

            self.hi_offset = handle.offset();
            self.hi_data_block = Some(reader);

            if let Some(mut item) = item {
                item.key.seqno += self.global_seqno;

                // Emit the first materialized entry immediately to match the forward path and avoid
                // storing it in a temporary buffer.
                return Some(Ok(item));
            }
        }
    }
}

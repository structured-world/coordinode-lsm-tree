// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::KeyedBlockHandle;
use crate::{
    Cache, CompressionType, GlobalTableId, SeqNo, UserKey,
    comparator::SharedComparator,
    encryption::EncryptionProvider,
    file_accessor::FileAccessor,
    table::{
        BlockHandle, IndexBlock,
        block::BlockType,
        block_index::{BlockIndexIter, iter::OwnedIndexBlockIter},
        util::load_block,
    },
};
use std::{path::PathBuf, sync::Arc};

#[cfg(feature = "metrics")]
use crate::Metrics;

/// Index that translates item keys to data block handles
///
/// The index is loaded on demand.
pub struct VolatileBlockIndex {
    pub(crate) table_id: GlobalTableId,
    pub(crate) path: Arc<PathBuf>,
    pub(crate) file_accessor: FileAccessor,
    pub(crate) cache: Arc<Cache>,
    pub(crate) handle: BlockHandle,
    pub(crate) compression: CompressionType,
    pub(crate) encryption: Option<Arc<dyn EncryptionProvider>>,
    pub(crate) comparator: SharedComparator,

    #[cfg(feature = "metrics")]
    pub(crate) metrics: Arc<Metrics>,
}

impl VolatileBlockIndex {
    pub fn forward_reader(&self, needle: &[u8], seqno: SeqNo) -> Iter {
        let mut iter = Iter::new(self);
        iter.seek_lower(needle, seqno);
        iter
    }

    pub fn iter(&self) -> Iter {
        Iter::new(self)
    }
}

pub struct Iter {
    inner: Option<OwnedIndexBlockIter>,
    table_id: GlobalTableId,
    path: Arc<PathBuf>,
    file_accessor: FileAccessor,
    cache: Arc<Cache>,
    handle: BlockHandle,
    compression: CompressionType,
    encryption: Option<Arc<dyn EncryptionProvider>>,
    comparator: SharedComparator,

    lo: Option<(UserKey, SeqNo)>,
    hi: Option<(UserKey, SeqNo)>,

    #[cfg(feature = "metrics")]
    pub(crate) metrics: Arc<Metrics>,

    poisoned: bool,
}

impl Iter {
    fn new(index: &VolatileBlockIndex) -> Self {
        Self {
            inner: None,
            table_id: index.table_id,
            path: index.path.clone(),
            file_accessor: index.file_accessor.clone(),
            cache: index.cache.clone(),
            handle: index.handle,
            compression: index.compression,
            encryption: index.encryption.clone(),
            comparator: index.comparator.clone(),

            lo: None,
            hi: None,

            #[cfg(feature = "metrics")]
            metrics: index.metrics.clone(),
            poisoned: false,
        }
    }

    #[expect(
        clippy::unnecessary_wraps,
        reason = "matches Iterator::next return type"
    )]
    fn poison<E: Into<crate::Error>>(&mut self, err: E) -> Option<crate::Result<KeyedBlockHandle>> {
        self.poisoned = true;
        Some(Err(err.into()))
    }

    /// Lazily loads the index block and initialises the bounded iterator.
    ///
    /// On `Ok(None)` (empty range) the iterator is marked exhausted so
    /// subsequent `next()` / `next_back()` calls return `None` without
    /// re-loading the block from disk.
    fn init_inner(&mut self) -> crate::Result<Option<OwnedIndexBlockIter>> {
        let block = load_block(
            self.table_id,
            &self.path,
            &self.file_accessor,
            &self.cache,
            &self.handle,
            BlockType::Index,
            self.compression,
            self.encryption.as_deref(),
            #[cfg(zstd_any)]
            None,
            #[cfg(feature = "metrics")]
            &self.metrics,
        )?;
        let index_block = IndexBlock::new(block);
        let lo = self.lo.as_ref().map(|(k, s)| (k.as_ref(), *s));
        let hi = self.hi.as_ref().map(|(k, s)| (k.as_ref(), *s));

        let iter = OwnedIndexBlockIter::from_block_with_bounds(
            index_block,
            self.comparator.clone(),
            lo,
            hi,
        )?;

        if iter.is_none() {
            // Empty range: mark exhausted to prevent repeated I/O.
            self.poisoned = true;
        }

        Ok(iter)
    }
}

impl BlockIndexIter for Iter {
    fn seek_lower(&mut self, key: &[u8], seqno: SeqNo) -> bool {
        self.lo = Some((key.into(), seqno));
        true
    }

    fn seek_upper(&mut self, key: &[u8], seqno: SeqNo) -> bool {
        self.hi = Some((key.into(), seqno));
        true
    }
}

impl Iterator for Iter {
    type Item = crate::Result<KeyedBlockHandle>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.poisoned {
            return None;
        }

        if let Some(inner) = &mut self.inner {
            inner.next().map(Ok)
        } else {
            let mut iter = match self.init_inner() {
                Ok(Some(it)) => it,
                Ok(None) => return None,
                Err(e) => return self.poison(e),
            };

            let next_item = iter.next().map(Ok);
            self.inner = Some(iter);
            next_item
        }
    }
}

impl DoubleEndedIterator for Iter {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.poisoned {
            return None;
        }

        if let Some(inner) = &mut self.inner {
            inner.next_back().map(Ok)
        } else {
            let mut iter = match self.init_inner() {
                Ok(Some(it)) => it,
                Ok(None) => return None,
                Err(e) => return self.poison(e),
            };

            let next_item = iter.next_back().map(Ok);
            self.inner = Some(iter);
            next_item
        }
    }
}

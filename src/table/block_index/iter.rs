// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    comparator::SharedComparator,
    table::{block::ParsedItem, index_block::Iter as IndexBlockIter, IndexBlock, KeyedBlockHandle},
    SeqNo,
};
use self_cell::self_cell;

self_cell!(
    pub struct OwnedIndexBlockIter {
        owner: IndexBlock,

        #[covariant]
        dependent: IndexBlockIter,
    }
);

impl OwnedIndexBlockIter {
    /// Creates an owned iterator from a block and a comparator.
    pub fn from_block(block: IndexBlock, comparator: SharedComparator) -> Self {
        Self::new(block, |b| b.iter(comparator))
    }

    /// Creates an owned iterator with optional lower/upper seek bounds.
    ///
    /// Returns `None` if either seek operation indicates that no items
    /// fall within the given bounds.
    pub fn from_block_with_bounds(
        block: IndexBlock,
        comparator: SharedComparator,
        lo: Option<(&[u8], SeqNo)>,
        hi: Option<(&[u8], SeqNo)>,
    ) -> Option<Self> {
        let mut iter = Self::from_block(block, comparator);

        if let Some((key, seqno)) = lo {
            if !iter.seek_lower(key, seqno) {
                return None;
            }
        }
        if let Some((key, seqno)) = hi {
            if !iter.seek_upper(key, seqno) {
                return None;
            }
        }

        Some(iter)
    }

    pub fn seek_lower(&mut self, needle: &[u8], seqno: SeqNo) -> bool {
        self.with_dependent_mut(|_, m| m.seek(needle, seqno))
    }

    pub fn seek_upper(&mut self, needle: &[u8], seqno: SeqNo) -> bool {
        self.with_dependent_mut(|_, m| m.seek_upper(needle, seqno))
    }
}

impl Iterator for OwnedIndexBlockIter {
    type Item = KeyedBlockHandle;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|block, iter| {
            iter.next().map(|item| item.materialize(&block.inner.data))
        })
    }
}

impl DoubleEndedIterator for OwnedIndexBlockIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|block, iter| {
            iter.next_back()
                .map(|item| item.materialize(&block.inner.data))
        })
    }
}

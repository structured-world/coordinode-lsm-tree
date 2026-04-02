// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::SeqNo;
use crate::comparator::SharedComparator;
use crate::table::block::Decoder;
use crate::table::block_index::{BlockIndexIter, iter::OwnedIndexBlockIter};
use crate::table::index_block::IndexBlockParsedItem;
use crate::table::{IndexBlock, KeyedBlockHandle};

/// Index that translates item keys to data block handles
///
/// The index is fully loaded into memory.
pub struct FullBlockIndex {
    block: IndexBlock,
    comparator: SharedComparator,
}

impl FullBlockIndex {
    /// Creates a new full block index.
    ///
    /// Eagerly validates the block trailer so that subsequent `iter()` calls
    /// cannot panic on malformed blocks.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::InvalidTrailer`] if the block trailer is
    /// malformed.
    pub fn new(block: IndexBlock, comparator: SharedComparator) -> crate::Result<Self> {
        // Validate trailer layout once at construction so later iter() calls
        // cannot panic. Block type is the caller's responsibility (checked by
        // read_tli / filter_tli path before reaching here).
        Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::try_new(&block.inner)?;
        Ok(Self { block, comparator })
    }

    pub fn inner(&self) -> &IndexBlock {
        &self.block
    }

    pub fn forward_reader(&self, needle: &[u8], seqno: SeqNo) -> Option<Iter> {
        let mut it = self.iter();
        if it.seek_lower(needle, seqno) {
            Some(it)
        } else {
            None
        }
    }

    pub fn iter(&self) -> Iter {
        // Invariant: trailer was validated in `new`, so `from_block` cannot fail.
        #[expect(clippy::expect_used, reason = "trailer validated at construction")]
        Iter(
            OwnedIndexBlockIter::from_block(self.block.clone(), self.comparator.clone())
                .expect("trailer validated at construction"),
        )
    }
}

pub struct Iter(OwnedIndexBlockIter);

impl BlockIndexIter for Iter {
    fn seek_lower(&mut self, key: &[u8], seqno: SeqNo) -> bool {
        self.0.seek_lower(key, seqno)
    }

    fn seek_upper(&mut self, key: &[u8], seqno: SeqNo) -> bool {
        self.0.seek_upper(key, seqno)
    }
}

impl Iterator for Iter {
    type Item = crate::Result<KeyedBlockHandle>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(Ok)
    }
}

impl DoubleEndedIterator for Iter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back().map(Ok)
    }
}

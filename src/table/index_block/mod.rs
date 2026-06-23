// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

mod block_handle;
mod iter;

pub use block_handle::{BlockHandle, KeyedBlockHandle};
pub use iter::Iter;

use super::{
    Block,
    block::{BlockOffset, Encoder, Trailer},
};
use crate::Slice;
use crate::io::{Error, ErrorKind};
use crate::{
    SeqNo,
    table::{
        block::{Decoder, DecoderMeta, ParsedItem},
        util::{SliceIndexes, compare_prefixed_slice},
    },
};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[derive(Debug)]
pub struct IndexBlockParsedItem {
    pub offset: BlockOffset,
    pub size: u32,
    pub prefix: Option<SliceIndexes>,
    pub end_key: SliceIndexes,
    pub seqno: SeqNo,
}

impl ParsedItem<KeyedBlockHandle> for IndexBlockParsedItem {
    fn compare_key(
        &self,
        needle: &[u8],
        bytes: &[u8],
        cmp: &dyn crate::comparator::UserComparator,
    ) -> core::cmp::Ordering {
        // SAFETY: slice indexes come from the block parser which validates them
        // during decoding. The block format guarantees they are within bounds.
        if let Some(prefix) = &self.prefix {
            let prefix = unsafe { bytes.get_unchecked(prefix.0..prefix.1) };
            let rest_key = unsafe { bytes.get_unchecked(self.end_key.0..self.end_key.1) };
            compare_prefixed_slice(prefix, rest_key, needle, cmp)
        } else {
            // No allocation to avoid for a contiguous key — `compare()` is
            // already optimal for any comparator here. The lex fast path is
            // kept only on the prefix branch above (where
            // `compare_prefixed_slice_lexicographic` avoids the prefix+suffix
            // concatenation) and at the binary-search predicate construction
            // sites in `iter.rs` (where one `is_lexicographic()` is hoisted
            // to amortise across all BS probes). An extra `is_lexicographic()`
            // call per linear-scan step would cost custom comparators a
            // second vtable dispatch without any matching saving on the
            // default path.
            let key = unsafe { bytes.get_unchecked(self.end_key.0..self.end_key.1) };
            cmp.compare(key, needle)
        }
    }

    fn seqno(&self) -> SeqNo {
        self.seqno
    }

    fn key_offset(&self) -> usize {
        self.end_key.0
    }

    fn key_end_offset(&self) -> usize {
        self.end_key.1
    }

    fn materialize(&self, bytes: &Slice) -> KeyedBlockHandle {
        // NOTE: We consider the prefix and key slice indexes to be trustworthy
        #[expect(clippy::indexing_slicing)]
        let key = if let Some(prefix) = &self.prefix {
            let prefix_key = &bytes[prefix.0..prefix.1];
            let rest_key = &bytes[self.end_key.0..self.end_key.1];
            Slice::fused(prefix_key, rest_key)
        } else {
            bytes.slice(self.end_key.0..self.end_key.1)
        };

        KeyedBlockHandle::new(key, self.seqno, BlockHandle::new(self.offset, self.size))
    }
}

/// Block that contains block handles (file offset + size)
#[derive(Clone)]
pub struct IndexBlock {
    pub inner: Block,
}

impl IndexBlock {
    #[must_use]
    pub fn new(inner: Block) -> Self {
        Self { inner }
    }

    /// Accesses the inner raw bytes
    #[must_use]
    pub fn as_slice(&self) -> &Slice {
        &self.inner.data
    }

    /// Returns the number of items in the block.
    #[must_use]
    #[expect(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        Trailer::new(&self.inner).item_count()
    }

    /// Creates a fallible iterator over the index block.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::InvalidTag`] if the block is not an index block,
    /// or [`crate::Error::InvalidTrailer`] if the block trailer is malformed
    /// (e.g. `restart_interval == 0`).
    pub fn try_iter(
        &self,
        comparator: crate::comparator::SharedComparator,
    ) -> crate::Result<Iter<'_>> {
        use crate::table::block::BlockType;

        if self.inner.header.block_type != BlockType::Index {
            return Err(crate::Error::InvalidTag((
                "BlockType",
                self.inner.header.block_type.into(),
            )));
        }
        Ok(Iter::new(
            Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::try_new(&self.inner)?,
            comparator,
        ))
    }

    #[must_use]
    pub fn iter(&self, comparator: crate::comparator::SharedComparator) -> Iter<'_> {
        Iter::new(
            Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::new(&self.inner),
            comparator,
        )
    }

    /// Parses the block trailer once and returns the reusable
    /// [`DecoderMeta`], so repeated lookups can build decoders via
    /// [`Self::iter_with_meta`] without re-parsing.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::InvalidTrailer`] / [`crate::Error::InvalidTag`]
    /// if the block is not a valid index block.
    pub fn decoder_meta(&self) -> crate::Result<DecoderMeta> {
        Ok(Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::try_new(&self.inner)?.meta())
    }

    /// Builds a borrowing iterator from pre-parsed [`DecoderMeta`],
    /// skipping the per-call trailer parse. `meta` must come from
    /// [`Self::decoder_meta`] on this same block.
    #[must_use]
    pub fn iter_with_meta(
        &self,
        comparator: crate::comparator::SharedComparator,
        meta: DecoderMeta,
    ) -> Iter<'_> {
        Iter::new(
            Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::from_meta(&self.inner, meta),
            comparator,
        )
    }

    pub fn encode_into_vec(items: &[KeyedBlockHandle]) -> crate::Result<Vec<u8>> {
        Self::encode_into_vec_with_restart_interval(items, 1)
    }

    /// Builds an index block with the given restart interval into a new `Vec`.
    ///
    /// # Errors
    ///
    /// Returns [`crate::io::ErrorKind::InvalidInput`] when `restart_interval == 0`.
    ///
    /// # Panics
    ///
    /// Panics if `items` is empty.
    pub fn encode_into_vec_with_restart_interval(
        items: &[KeyedBlockHandle],
        restart_interval: u8,
    ) -> crate::Result<Vec<u8>> {
        let mut buf = vec![];

        Self::encode_into_with_restart_interval(&mut buf, items, restart_interval)?;

        Ok(buf)
    }

    /// Builds an index block.
    ///
    /// # Panics
    ///
    /// Panics if the given item array if empty.
    pub fn encode_into(writer: &mut Vec<u8>, items: &[KeyedBlockHandle]) -> crate::Result<()> {
        Self::encode_into_with_restart_interval(writer, items, 1)
    }

    /// Builds an index block using the provided restart interval.
    ///
    /// # Errors
    ///
    /// Returns [`crate::io::ErrorKind::InvalidInput`] when `restart_interval == 0`.
    ///
    /// # Panics
    ///
    /// Panics if `items` is empty.
    pub fn encode_into_with_restart_interval(
        writer: &mut Vec<u8>,
        items: &[KeyedBlockHandle],
        restart_interval: u8,
    ) -> crate::Result<()> {
        if restart_interval == 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "index block restart interval must be greater than zero",
            )
            .into());
        }

        #[expect(clippy::expect_used)]
        let first_key = items.first().expect("chunk should not be empty").end_key();

        let mut serializer = Encoder::<'_, BlockOffset, KeyedBlockHandle>::new(
            writer,
            items.len(),
            restart_interval,
            0.0, // Index blocks do not support hash index
            first_key,
        );

        for item in items {
            serializer.write(item)?;
        }

        serializer.finish()
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::indexing_slicing, reason = "test code")]
mod tests;

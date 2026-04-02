// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod block_handle;
mod iter;

pub use block_handle::{BlockHandle, KeyedBlockHandle};
pub use iter::Iter;

use super::{
    Block,
    block::{BlockOffset, Encoder, Trailer},
};
use crate::Slice;
use crate::{
    SeqNo,
    table::{
        block::{Decoder, ParsedItem},
        util::{SliceIndexes, compare_prefixed_slice},
    },
};
use std::io::{Error, ErrorKind};

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
    ) -> std::cmp::Ordering {
        // SAFETY: slice indexes come from the block parser which validates them
        // during decoding. The block format guarantees they are within bounds.
        if let Some(prefix) = &self.prefix {
            let prefix = unsafe { bytes.get_unchecked(prefix.0..prefix.1) };
            let rest_key = unsafe { bytes.get_unchecked(self.end_key.0..self.end_key.1) };
            compare_prefixed_slice(prefix, rest_key, needle, cmp)
        } else {
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

    pub fn encode_into_vec(items: &[KeyedBlockHandle]) -> crate::Result<Vec<u8>> {
        Self::encode_into_vec_with_restart_interval(items, 1)
    }

    /// Builds an index block with the given restart interval into a new `Vec`.
    ///
    /// # Errors
    ///
    /// Returns [`std::io::ErrorKind::InvalidInput`] when `restart_interval == 0`.
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
    /// Returns [`std::io::ErrorKind::InvalidInput`] when `restart_interval == 0`.
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
mod tests {
    use super::*;
    use crate::table::BlockHandle;

    fn make_shared_prefix_handles(count: usize) -> Vec<KeyedBlockHandle> {
        (0..count)
            .map(|i| {
                let key = format!("adj:out:vertex-0001:edge-{i:04}:target-0001");
                KeyedBlockHandle::new(
                    key.into(),
                    i as u64,
                    BlockHandle::new(BlockOffset((i as u64) * 4096), 4096),
                )
            })
            .collect()
    }

    #[test]
    fn higher_restart_interval_reduces_index_block_size_for_shared_prefix_keys() {
        let handles = make_shared_prefix_handles(256);

        let legacy = IndexBlock::encode_into_vec_with_restart_interval(&handles, 1).unwrap();
        let compressed = IndexBlock::encode_into_vec_with_restart_interval(&handles, 16).unwrap();

        assert!(
            compressed.len() < legacy.len(),
            "compressed={} should be smaller than legacy={}",
            compressed.len(),
            legacy.len(),
        );
    }

    #[test]
    fn zero_restart_interval_is_rejected() {
        let handles = make_shared_prefix_handles(2);
        let Err(err) = IndexBlock::encode_into_vec_with_restart_interval(&handles, 0) else {
            panic!("restart interval of zero must be rejected");
        };
        assert!(matches!(err, crate::Error::Io(e) if e.kind() == ErrorKind::InvalidInput));
    }

    #[test]
    fn try_iter_zero_restart_interval_returns_invalid_trailer() {
        use crate::Checksum;
        use crate::table::block::{BlockType, Header, Trailer};

        let handles = make_shared_prefix_handles(4);
        let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 2).unwrap();

        let block = Block {
            data: bytes.clone().into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        };
        let trailer_offset = Trailer::new(&block).trailer_offset();
        bytes[trailer_offset] = 0;

        let corrupt_index = IndexBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });

        let cmp = crate::comparator::default_comparator();
        assert!(
            matches!(
                corrupt_index.try_iter(cmp),
                Err(crate::Error::InvalidTrailer)
            ),
            "zero restart_interval must return InvalidTrailer",
        );
    }
}

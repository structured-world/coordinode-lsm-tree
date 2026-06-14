// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::comparator::SharedComparator;
use crate::table::block::{BlockType, Decoder, DecoderMeta, ParsedItem};
use crate::table::block_index::{BlockIndexIter, iter::OwnedIndexBlockIter};
use crate::table::index_block::IndexBlockParsedItem;
use crate::table::{IndexBlock, KeyedBlockHandle};
use crate::{SeqNo, Slice};

/// Seqno-aware binary search positioning a freshly-built index decoder at the
/// first block handle whose `end_key >= needle` (or equal with a head seqno at
/// least the snapshot boundary). Borrows the comparator (lex fast path is
/// statically dispatched). Mirrors `index_block::Iter::seek_with_cache_resets`
/// but drives the decoder directly: a fresh decoder has no peek/back-cursor
/// state to reset, so those resets (and the double-ended peeking wrapper) are
/// pure overhead on the point-read path.
fn seek_index_block(
    decoder: &mut Decoder<'_, KeyedBlockHandle, IndexBlockParsedItem>,
    needle: &[u8],
    seqno: SeqNo,
    comparator: &SharedComparator,
) -> bool {
    let landed = if comparator.is_lexicographic() {
        decoder.seek(
            |end_key, s| match end_key.cmp(needle) {
                core::cmp::Ordering::Greater => false,
                core::cmp::Ordering::Less => true,
                core::cmp::Ordering::Equal => s >= seqno,
            },
            true,
        )
    } else {
        decoder.seek(
            |end_key, s| match comparator.compare(end_key, needle) {
                core::cmp::Ordering::Greater => false,
                core::cmp::Ordering::Less => true,
                core::cmp::Ordering::Equal => s >= seqno,
            },
            true,
        )
    };
    if !landed {
        return false;
    }

    // Restart heads only carry every Nth handle's key; scan within the landed
    // interval to the first handle that actually covers the needle.
    if decoder.restart_interval() > 1 {
        decoder.advance_while(|item, bytes| {
            match item.compare_key(needle, bytes, comparator.as_ref()) {
                core::cmp::Ordering::Greater => false,
                core::cmp::Ordering::Less => true,
                core::cmp::Ordering::Equal => item.seqno() >= seqno,
            }
        });
    }

    true
}

/// Index that translates item keys to data block handles
///
/// The index is fully loaded into memory.
pub struct FullBlockIndex {
    block: IndexBlock,
    comparator: SharedComparator,
    /// Trailer metadata parsed once at construction. The point-read fast
    /// path ([`Self::point_read_reader`]) reuses it instead of re-parsing
    /// the trailer on every lookup.
    meta: DecoderMeta,
}

impl FullBlockIndex {
    /// Creates a new full block index.
    ///
    /// Eagerly validates the block trailer so that subsequent `iter()` calls
    /// cannot panic on malformed blocks.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::InvalidTag`] if `block` is not an index block,
    /// or [`crate::Error::InvalidTrailer`] if the block trailer is malformed.
    pub fn new(block: IndexBlock, comparator: SharedComparator) -> crate::Result<Self> {
        if block.inner.header.block_type != BlockType::Index {
            return Err(crate::Error::InvalidTag((
                "BlockType",
                block.inner.header.block_type.into(),
            )));
        }
        // Validate trailer layout once at construction so later iter() calls
        // cannot panic, and keep the parsed metadata so the point-read hot
        // path can build decoders without re-parsing the trailer.
        let meta = Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::try_new(&block.inner)?.meta();
        Ok(Self {
            block,
            comparator,
            meta,
        })
    }

    pub fn inner(&self) -> &IndexBlock {
        &self.block
    }

    /// Borrowing point-read seek: positions a borrowing iterator at the
    /// first block handle at or after `(needle, seqno)`.
    ///
    /// Unlike [`Self::forward_reader`] this does NOT clone the index block
    /// (the returned iterator borrows it) and reuses the trailer metadata
    /// parsed at construction, so a point read pays neither the block
    /// refcount bump nor a trailer re-parse. Used by the table point-read
    /// path; range scans keep the owned [`Self::forward_reader`].
    pub fn point_read_reader(&self, needle: &[u8], seqno: SeqNo) -> Option<PointReadIter<'_>> {
        // from_meta reuses the trailer parsed at construction; the seek closure
        // borrows the comparator (no Arc bump). Driving the decoder directly
        // skips the double-ended peeking wrapper and its no-op cache resets.
        let mut decoder = Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::from_meta(
            &self.block.inner,
            self.meta,
        );
        if seek_index_block(&mut decoder, needle, seqno, &self.comparator) {
            Some(PointReadIter {
                decoder,
                data: &self.block.inner.data,
            })
        } else {
            None
        }
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
        Iter(OwnedIndexBlockIter::from_validated_block(
            self.block.clone(),
            self.comparator.clone(),
        ))
    }
}

/// Borrowing point-read iterator returned by
/// [`FullBlockIndex::point_read_reader`].
///
/// Drives the index-block [`Decoder`] directly (forward-only) and materializes
/// each parsed item into a [`KeyedBlockHandle`] against the borrowed block
/// data. Both fields share an immutable borrow of the index block, so no
/// `Arc`/`Bytes` clone happens per lookup.
pub struct PointReadIter<'a> {
    decoder: Decoder<'a, KeyedBlockHandle, IndexBlockParsedItem>,
    data: &'a Slice,
}

impl Iterator for PointReadIter<'_> {
    type Item = crate::Result<KeyedBlockHandle>;

    fn next(&mut self) -> Option<Self::Item> {
        // materialize is infallible; wrap in Ok to match the owned
        // `forward_reader` iterator's item type so the caller's `?` works.
        self.decoder
            .next()
            .map(|item| Ok(item.materialize(self.data)))
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

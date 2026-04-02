// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    SeqNo,
    comparator::SharedComparator,
    table::{IndexBlock, KeyedBlockHandle, block::ParsedItem, index_block::Iter as IndexBlockIter},
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
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::InvalidTrailer`] if the block trailer is
    /// malformed (e.g. `restart_interval == 0`).
    pub(crate) fn from_block(
        block: IndexBlock,
        comparator: SharedComparator,
    ) -> crate::Result<Self> {
        Self::try_new(block, |b| b.try_iter(comparator))
    }

    /// Creates an owned iterator with optional lower/upper seek bounds.
    ///
    /// The lower bound `lo`, if provided, seeks the forward cursor to the
    /// first entry at or after `(key, seqno)`. Returns `None` if no such
    /// entry exists.
    ///
    /// The upper bound `hi`, if provided, seeds the internal upper-bound
    /// cursor via `seek_upper_bound_cursor`.
    ///
    /// This always positions the back cursor for reverse iteration and may
    /// also cap forward iteration in compressed index blocks
    /// (`restart_interval > 1`) where upper-bound seeking trims the right
    /// edge of the active decoder window.
    ///
    /// Returns `Ok(None)` when the requested range is empty: if `lo > hi`,
    /// if the lower-bound seek finds no entry, or if the upper-bound cursor
    /// seek reports failure.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::InvalidTrailer`] if the block trailer is
    /// malformed.
    pub(crate) fn from_block_with_bounds(
        block: IndexBlock,
        comparator: SharedComparator,
        lo: Option<(&[u8], SeqNo)>,
        hi: Option<(&[u8], SeqNo)>,
    ) -> crate::Result<Option<Self>> {
        // Short-circuit contradictory bounds: lo > hi means an empty range.
        if let (Some((lo_key, _)), Some((hi_key, _))) = (lo, hi)
            && comparator.compare(lo_key, hi_key) == std::cmp::Ordering::Greater
        {
            return Ok(None);
        }

        let mut iter = Self::from_block(block, comparator)?;

        // Use incremental bound-cursor methods: seek_lower_bound_cursor
        // resets front but preserves back; seek_upper_bound_cursor preserves
        // front (the candidate seeded by seek_lower's peek()).
        if let Some((key, seqno)) = lo
            && !iter.with_dependent_mut(|_, m| m.seek_lower_bound_cursor(key, seqno))
        {
            return Ok(None);
        }
        // TODO(#196): seek_upper_bound_cursor returns bool — a poisoned/clamped
        // cursor is indistinguishable from "empty range". Make fallible.
        if let Some((key, seqno)) = hi
            && !iter.with_dependent_mut(|_, m| m.seek_upper_bound_cursor(key, seqno))
        {
            return Ok(None);
        }

        Ok(Some(iter))
    }

    /// Full lower-bound re-seek: resets both front and back caches.
    ///
    /// For incremental bound positioning that preserves the back cache,
    /// `from_block_with_bounds` uses `seek_lower_bound_cursor` internally.
    pub fn seek_lower(&mut self, needle: &[u8], seqno: SeqNo) -> bool {
        self.with_dependent_mut(|_, m| m.seek(needle, seqno))
    }

    /// Upper-bound seek for forward-limit positioning.
    ///
    /// Preserves the current front cursor and re-seeks only the back cursor,
    /// so this tightens the existing forward window instead of performing a
    /// full upper re-seek.
    pub fn seek_upper(&mut self, needle: &[u8], _seqno: SeqNo) -> bool {
        self.with_dependent_mut(|_, m| {
            // reset_front=false: preserve front cache from prior seek_lower
            // reset_back=true: clear stale back state from reverse iteration
            // check_back_cache=false: forward-limit mode, don't require peek_back
            m.seek_upper_impl(needle, false, true, false)
        })
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

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    clippy::doc_markdown,
    clippy::cast_possible_truncation,
    reason = "test code"
)]
mod tests {
    use super::*;
    use crate::{
        Checksum,
        comparator::default_comparator,
        table::BlockHandle,
        table::block::{BlockOffset, BlockType, Header},
    };

    /// Builds an IndexBlock containing entries with the given keys (seqno=0 for all).
    fn make_index_block(keys: &[&[u8]], restart_interval: u8) -> IndexBlock {
        let items: Vec<KeyedBlockHandle> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| {
                KeyedBlockHandle::new(
                    (*k).into(),
                    0,
                    BlockHandle::new(BlockOffset(i as u64 * 100), 100),
                )
            })
            .collect();

        let bytes =
            IndexBlock::encode_into_vec_with_restart_interval(&items, restart_interval).unwrap();
        let data_len = bytes.len() as u32;
        IndexBlock::new(crate::table::block::Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: data_len,
                uncompressed_length: data_len,
            },
        })
    }

    #[test]
    fn from_block_iterates_all_entries() {
        let block = make_index_block(&[b"a", b"b", b"c"], 1);
        let mut iter = OwnedIndexBlockIter::from_block(block, default_comparator()).unwrap();

        let keys: Vec<_> = iter.by_ref().map(|h| h.end_key().to_vec()).collect();
        assert_eq!(keys, vec![b"a", b"b", b"c"]);
    }

    #[test]
    fn from_block_with_bounds_no_bounds_returns_all() {
        let block = make_index_block(&[b"a", b"b", b"c"], 1);
        let iter =
            OwnedIndexBlockIter::from_block_with_bounds(block, default_comparator(), None, None)
                .unwrap();

        assert!(iter.is_some());
        let keys: Vec<_> = iter.unwrap().map(|h| h.end_key().to_vec()).collect();
        assert_eq!(keys, vec![b"a", b"b", b"c"]);
    }

    #[test]
    fn from_block_with_bounds_lo_bound_seeks_forward() {
        let block = make_index_block(&[b"a", b"b", b"c"], 1);
        let iter = OwnedIndexBlockIter::from_block_with_bounds(
            block,
            default_comparator(),
            Some((b"b", SeqNo::MAX)),
            None,
        )
        .unwrap();

        assert!(iter.is_some());
        let keys: Vec<_> = iter.unwrap().map(|h| h.end_key().to_vec()).collect();
        assert_eq!(keys, vec![b"b", b"c"]);
    }

    #[test]
    fn from_block_with_bounds_hi_bound_sets_back_cursor() {
        // For restart_interval=1, seek_upper primarily positions the
        // decoder's back-end cursor.
        let block = make_index_block(&[b"a", b"b", b"c", b"d"], 1);
        let mut iter = OwnedIndexBlockIter::from_block_with_bounds(
            block,
            default_comparator(),
            None,
            Some((b"c", 0)),
        )
        .unwrap()
        .unwrap();

        // Forward iteration still starts from the beginning
        assert_eq!(iter.next().unwrap().end_key().as_ref(), b"a");

        // seek_upper("c", 0) positions the back cursor at the first block
        // with end_key > "c", which is "d". next_back yields from there downward.
        assert_eq!(iter.next_back().unwrap().end_key().as_ref(), b"d");
        assert_eq!(iter.next_back().unwrap().end_key().as_ref(), b"c");
        assert_eq!(iter.next_back().unwrap().end_key().as_ref(), b"b");
        assert!(iter.next_back().is_none());
    }

    #[test]
    fn from_block_with_bounds_both_bounds() {
        // Include trailing key "e" so the hi bound actually clips the sequence;
        // with [a,b,c,d] and hi="c", "d" is already the tail and a broken
        // upper-bound path would still pass.
        let block = make_index_block(&[b"a", b"b", b"c", b"d", b"e"], 1);
        let iter = OwnedIndexBlockIter::from_block_with_bounds(
            block,
            default_comparator(),
            Some((b"b", SeqNo::MAX)),
            Some((b"c", 0)),
        )
        .unwrap()
        .unwrap();

        let keys: Vec<_> = iter.map(|h| h.end_key().to_vec()).collect();
        assert_eq!(keys, vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]);
    }

    #[test]
    fn from_block_with_bounds_compressed_both_bounds() {
        // Exercise the seek_lower_bound_cursor → seek_upper_bound_cursor
        // sequence with restart_interval > 1 to cover the compressed-interval
        // trim_back_to_upper_bound + advance_upper_restart_interval path.
        let block = make_index_block(&[b"a", b"b", b"c", b"d", b"e", b"f", b"g", b"h"], 4);
        let iter = OwnedIndexBlockIter::from_block_with_bounds(
            block,
            default_comparator(),
            Some((b"c", SeqNo::MAX)),
            Some((b"f", 0)),
        )
        .unwrap()
        .unwrap();

        let keys: Vec<_> = iter.map(|h| h.end_key().to_vec()).collect();
        assert_eq!(
            keys,
            vec![b"c".to_vec(), b"d".to_vec(), b"e".to_vec(), b"f".to_vec(),]
        );
    }

    #[test]
    fn from_block_with_bounds_lo_past_end_returns_none() {
        let block = make_index_block(&[b"a", b"b"], 1);
        let iter = OwnedIndexBlockIter::from_block_with_bounds(
            block,
            default_comparator(),
            Some((b"z", SeqNo::MAX)),
            None,
        )
        .unwrap();

        assert!(iter.is_none());
    }

    #[test]
    fn from_block_with_bounds_inverted_bounds_returns_none() {
        let block = make_index_block(&[b"a", b"b", b"c", b"d", b"e"], 1);
        let iter = OwnedIndexBlockIter::from_block_with_bounds(
            block,
            default_comparator(),
            Some((b"d", SeqNo::MAX)),
            Some((b"b", 0)),
        )
        .unwrap();

        assert!(iter.is_none(), "inverted lo > hi must return None");
    }

    #[test]
    fn from_block_with_bounds_restart_interval_gt_one() {
        let block = make_index_block(
            &[
                b"adj:out:vertex-0001:edge-0000",
                b"adj:out:vertex-0001:edge-0001",
                b"adj:out:vertex-0001:edge-0002",
                b"adj:out:vertex-0001:edge-0003",
                b"adj:out:vertex-0001:edge-0004",
                b"adj:out:vertex-0001:edge-0005",
            ],
            4,
        );

        let iter = OwnedIndexBlockIter::from_block_with_bounds(
            block,
            default_comparator(),
            Some((b"adj:out:vertex-0001:edge-0002", SeqNo::MAX)),
            None,
        )
        .unwrap();

        assert!(iter.is_some());
        let keys: Vec<_> = iter.unwrap().map(|h| h.end_key().to_vec()).collect();
        assert_eq!(
            keys,
            vec![
                b"adj:out:vertex-0001:edge-0002".to_vec(),
                b"adj:out:vertex-0001:edge-0003".to_vec(),
                b"adj:out:vertex-0001:edge-0004".to_vec(),
                b"adj:out:vertex-0001:edge-0005".to_vec(),
            ]
        );
    }

    #[test]
    fn from_block_with_upper_bound_restart_interval_gt_one() {
        let block = make_index_block(
            &[
                b"adj:out:vertex-0001:edge-0000",
                b"adj:out:vertex-0001:edge-0001",
                b"adj:out:vertex-0001:edge-0002",
                b"adj:out:vertex-0001:edge-0003",
                b"adj:out:vertex-0001:edge-0004",
                b"adj:out:vertex-0001:edge-0005",
            ],
            4,
        );

        let mut iter = OwnedIndexBlockIter::from_block_with_bounds(
            block,
            default_comparator(),
            None,
            Some((b"adj:out:vertex-0001:edge-0002", 0)),
        )
        .unwrap()
        .unwrap();

        let keys: Vec<_> =
            std::iter::from_fn(|| iter.next_back().map(|h| h.end_key().to_vec())).collect();
        assert_eq!(
            keys,
            vec![
                b"adj:out:vertex-0001:edge-0002".to_vec(),
                b"adj:out:vertex-0001:edge-0001".to_vec(),
                b"adj:out:vertex-0001:edge-0000".to_vec(),
            ]
        );
    }

    #[test]
    fn seek_upper_with_equal_end_keys_keeps_full_forward_limit_span() {
        let block = make_index_block(&[b"k", b"k", b"k", b"k"], 1);
        let mut iter = OwnedIndexBlockIter::from_block(block, default_comparator()).unwrap();

        assert!(iter.seek_upper(b"k", SeqNo::MAX));

        let keys: Vec<_> = iter.map(|h| h.end_key().to_vec()).collect();
        assert_eq!(
            keys,
            vec![b"k".to_vec(), b"k".to_vec(), b"k".to_vec(), b"k".to_vec()]
        );
    }
}

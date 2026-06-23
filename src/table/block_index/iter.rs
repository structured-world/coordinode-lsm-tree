// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

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

    /// Creates an owned iterator from a pre-validated block.
    ///
    /// Uses the infallible [`IndexBlock::iter`] path, which delegates to
    /// [`crate::table::block::Decoder::new`]. The decoder still parses the
    /// trailer bytes (it needs the field values), but the caller's
    /// prior validation guarantees the internal `expect` cannot fire,
    /// making the call effectively infallible and removing `Result`
    /// overhead from the hot path.
    ///
    /// # Safety contract (logical)
    ///
    /// The caller **must** have already validated both:
    ///
    /// - the block trailer (e.g. via
    ///   [`crate::table::block::Decoder::try_new`] or a prior successful
    ///   `from_block` call); and
    /// - that the wrapped block is an index block satisfying the same
    ///   `BlockType::Index` invariant checked by `try_iter`.
    ///
    /// Calling this on a block that violates either invariant may panic
    /// inside the decoder or produce nonsensical iteration results.
    pub(crate) fn from_validated_block(block: IndexBlock, comparator: SharedComparator) -> Self {
        Self::new(block, |b| b.iter(comparator))
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
            && comparator.compare(lo_key, hi_key) == core::cmp::Ordering::Greater
        {
            return Ok(None);
        }

        let mut iter = Self::from_block(block, comparator)?;

        // Use incremental bound-cursor methods: seek_lower_bound_cursor
        // resets front but preserves back; seek_upper_bound_cursor preserves
        // front (the candidate seeded by seek_lower's peek()).
        if let Some((key, seqno)) = lo
            && !iter.with_dependent_mut(|_, m| m.seek_lower_bound_cursor(key, seqno))?
        {
            return Ok(None);
        }
        if let Some((key, seqno)) = hi
            && !iter.with_dependent_mut(|_, m| m.seek_upper_bound_cursor(key, seqno))?
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
            //
            // seek_upper_impl may return Err on a poisoned/clamped cursor;
            // the public bool-returning API treats that as "not found" for
            // backward compatibility — callers that need error propagation
            // should use from_block_with_bounds / seek_upper_bound_cursor.
            m.seek_upper_impl(needle, false, true, false)
                .unwrap_or(false)
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
mod tests;

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use crate::{
    InternalValue, SeqNo,
    comparator::SharedComparator,
    double_ended_peekable::{DoubleEndedPeekable, DoubleEndedPeekableExt, MaybePeeked},
    table::{
        block::{Decoder, ParsedItem},
        data_block::DataBlockParsedItem,
    },
};

/// The data block iterator handles double-ended scans over a data block
pub struct Iter<'a> {
    bytes: &'a [u8],
    decoder:
        DoubleEndedPeekable<DataBlockParsedItem, Decoder<'a, InternalValue, DataBlockParsedItem>>,
    comparator: SharedComparator,
}

impl<'a> Iter<'a> {
    /// Creates a new iterator over a data block.
    #[must_use]
    pub fn new(
        bytes: &'a [u8],
        decoder: Decoder<'a, InternalValue, DataBlockParsedItem>,
        comparator: SharedComparator,
    ) -> Self {
        let decoder = decoder.double_ended_peekable();
        Self {
            bytes,
            decoder,
            comparator,
        }
    }

    /// Seek the iterator to an byte offset.
    ///
    /// This is used when the hash index returns a hit.
    pub fn seek_to_offset(&mut self, offset: usize) -> bool {
        self.decoder.inner_mut().set_lo_offset(offset);
        true
    }

    /// Seeks to the last restart interval whose head key is strictly below the
    /// target `needle`, or equal to it with a seqno that is at least the given
    /// snapshot boundary.
    ///
    /// Here `seqno` is a snapshot boundary: point reads return the first item
    /// with `item.seqno < seqno`. Using the internal key ordering
    /// (`user_key` ASC, `seqno` DESC), this skips restart intervals that can only
    /// contain versions newer than the snapshot, so any visible version for
    /// `needle` will be found within roughly one restart interval of the
    /// resulting position.
    pub fn seek_to_key_seqno(&mut self, needle: &[u8], seqno: SeqNo) -> bool {
        // Lex fast path: `<[u8]>::cmp` is statically dispatched and inlinable
        // — no vtable lookup per probe. The custom-comparator branch retains
        // the existing `dyn UserComparator::compare` call. Each closure has a
        // distinct type, so `Decoder::seek` monomorphizes both shapes and the
        // inner binary-search loop is virtual-call-free on the lex path.
        let cmp = &self.comparator;
        if cmp.is_lexicographic() {
            self.decoder.inner_mut().seek(
                |head_key, head_seqno| match head_key.cmp(needle) {
                    core::cmp::Ordering::Less => true,
                    core::cmp::Ordering::Equal => head_seqno >= seqno,
                    core::cmp::Ordering::Greater => false,
                },
                false,
            )
        } else {
            self.decoder.inner_mut().seek(
                |head_key, head_seqno| match cmp.compare(head_key, needle) {
                    core::cmp::Ordering::Less => true,
                    core::cmp::Ordering::Equal => head_seqno >= seqno,
                    core::cmp::Ordering::Greater => false,
                },
                false,
            )
        }
    }

    pub fn seek(&mut self, needle: &[u8], seqno: SeqNo) -> bool {
        // Reuse the seqno-aware binary search from `seek_to_key_seqno`, then
        // follow up with a linear scan to position at the exact key.
        if !self.seek_to_key_seqno(needle, seqno) {
            return false;
        }

        // TODO: make sure we only linear scan over the current restart interval
        // TODO: if we do more steps, something has gone wrong with the seek probably, maybe...?

        // Linear scan
        loop {
            let Some(item) = self.decoder.peek() else {
                return false;
            };

            match item.compare_key(needle, self.bytes, self.comparator.as_ref()) {
                core::cmp::Ordering::Equal => {
                    return true;
                }
                core::cmp::Ordering::Greater => {
                    return false;
                }
                core::cmp::Ordering::Less => {
                    // Continue

                    #[expect(
                        clippy::expect_used,
                        reason = "we peeked a value successfully, so there must be a next item in the stream"
                    )]
                    self.decoder.next().expect("should exist");
                }
            }
        }
    }

    /// Reverse inclusive seek: position at the last key `<= needle`.
    ///
    /// `seqno` is accepted for API uniformity with the forward seek methods
    /// ([`seek`], [`seek_exclusive`]) but is **intentionally unused** here.
    /// Backward binary search cannot leverage seqno because intervals are
    /// visited from the selected one toward index 0 — a tighter predicate
    /// would skip intervals that may contain the visible version.
    pub fn seek_upper(&mut self, needle: &[u8], _seqno: SeqNo) -> bool {
        let cmp = &self.comparator;
        let landed = if cmp.is_lexicographic() {
            self.decoder
                .inner_mut()
                .seek_upper(|head_key, _| head_key <= needle, false)
        } else {
            self.decoder.inner_mut().seek_upper(
                |head_key, _| cmp.compare(head_key, needle) != core::cmp::Ordering::Greater,
                false,
            )
        };
        if !landed {
            return false;
        }

        // Linear scan
        loop {
            let Some(item) = self.decoder.peek_back() else {
                return false;
            };

            match item.compare_key(needle, self.bytes, self.comparator.as_ref()) {
                core::cmp::Ordering::Equal => {
                    return true;
                }
                core::cmp::Ordering::Less => {
                    return false;
                }
                core::cmp::Ordering::Greater => {
                    // Continue

                    #[expect(
                        clippy::expect_used,
                        reason = "we peeked a value successfully, so there must be a next item in the stream"
                    )]
                    self.decoder.next_back().expect("should exist");
                }
            }
        }
    }

    /// Moves the front past every entry whose key is `needle`, from where it
    /// stands, never back; whether an entry of a later key is left. The entries
    /// passed are compared as parsed, never materialized.
    pub fn skip_past(&mut self, needle: &[u8]) -> bool {
        loop {
            let Some(item) = self.decoder.peek() else {
                return false;
            };
            if item.compare_key(needle, self.bytes, self.comparator.as_ref())
                == core::cmp::Ordering::Greater
            {
                return true;
            }
            self.decoder.next();
        }
    }

    /// Moves the back down to the lowest remaining entry above `(needle,
    /// seqno)` in internal-key order, a later key or a version of `needle`
    /// below `seqno`, dropping the entries after it; whether the next
    /// `next_back` yields such an entry. The entries dropped are compared as
    /// parsed, never materialized, and an entry the back has handed out is
    /// never brought back.
    pub fn trim_back_above(&mut self, needle: &[u8], seqno: SeqNo) -> bool {
        let cmp = self.comparator.as_ref();
        let above =
            |item: &DataBlockParsedItem, bytes: &[u8]| match item.compare_key(needle, bytes, cmp) {
                core::cmp::Ordering::Greater => true,
                core::cmp::Ordering::Equal => item.seqno() < seqno,
                core::cmp::Ordering::Less => false,
            };
        let head_above = |key: &[u8], head_seqno: SeqNo| match cmp.compare(key, needle) {
            core::cmp::Ordering::Greater => true,
            core::cmp::Ordering::Equal => head_seqno < seqno,
            core::cmp::Ordering::Less => false,
        };

        // A peeked back entry is the next one out and sits past everything
        // the decoder still holds.
        match self.decoder.take_back_peeked() {
            MaybePeeked::Unpeeked => self.decoder.inner_mut().trim_back_above(head_above, above),
            MaybePeeked::Peeked(Some(item)) if above(&item, self.bytes) => {
                // It goes only if the decoder still holds a lower entry above.
                if !self.decoder.inner_mut().trim_back_above(head_above, above) {
                    self.decoder
                        .put_back_peeked(MaybePeeked::Peeked(Some(item)));
                }
                true
            }
            // Nothing left, or the next entry is not above, and nothing
            // before it is: nothing is dropped.
            peeked @ MaybePeeked::Peeked(_) => {
                self.decoder.put_back_peeked(peeked);
                false
            }
        }
    }

    pub fn seek_exclusive(&mut self, needle: &[u8], seqno: SeqNo) -> bool {
        // Exclusive lower bound: same seqno-aware binary search, but the linear
        // scan below skips entries equal to `needle`.
        if !self.seek_to_key_seqno(needle, seqno) {
            return false;
        }

        loop {
            let Some(item) = self.decoder.peek() else {
                return false;
            };

            match item.compare_key(needle, self.bytes, self.comparator.as_ref()) {
                core::cmp::Ordering::Greater => {
                    return true;
                }
                core::cmp::Ordering::Equal | core::cmp::Ordering::Less => {
                    #[expect(
                        clippy::expect_used,
                        reason = "we peeked a value successfully, so there must be a next item in the stream"
                    )]
                    self.decoder.next().expect("should exist");
                }
            }
        }
    }

    /// Reverse exclusive seek: position at the last key `< needle`.
    ///
    /// See [`seek_upper`] for why `seqno` is accepted but unused in reverse
    /// seeks.
    pub fn seek_upper_exclusive(&mut self, needle: &[u8], _seqno: SeqNo) -> bool {
        let cmp = &self.comparator;
        let landed = if cmp.is_lexicographic() {
            self.decoder
                .inner_mut()
                .seek_upper(|head_key, _| head_key <= needle, false)
        } else {
            self.decoder.inner_mut().seek_upper(
                |head_key, _| cmp.compare(head_key, needle) != core::cmp::Ordering::Greater,
                false,
            )
        };
        if !landed {
            return false;
        }

        loop {
            let Some(item) = self.decoder.peek_back() else {
                return false;
            };

            match item.compare_key(needle, self.bytes, self.comparator.as_ref()) {
                core::cmp::Ordering::Less => {
                    return true;
                }
                core::cmp::Ordering::Equal | core::cmp::Ordering::Greater => {
                    #[expect(
                        clippy::expect_used,
                        reason = "we peeked a value successfully, so there must be a next item in the stream"
                    )]
                    self.decoder.next_back().expect("should exist");
                }
            }
        }
    }
}

impl Iterator for Iter<'_> {
    type Item = DataBlockParsedItem;

    fn next(&mut self) -> Option<Self::Item> {
        self.decoder.next()
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.decoder.next_back()
    }
}

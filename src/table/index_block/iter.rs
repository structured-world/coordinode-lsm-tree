// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::{
    SeqNo,
    comparator::SharedComparator,
    double_ended_peekable::{DoubleEndedPeekable, DoubleEndedPeekableExt},
    table::{
        KeyedBlockHandle,
        block::{Decoder, ParsedItem},
        index_block::IndexBlockParsedItem,
    },
};

pub struct Iter<'a> {
    decoder: DoubleEndedPeekable<
        IndexBlockParsedItem,
        Decoder<'a, KeyedBlockHandle, IndexBlockParsedItem>,
    >,
    comparator: SharedComparator,
}

impl<'a> Iter<'a> {
    #[must_use]
    pub fn new(
        decoder: Decoder<'a, KeyedBlockHandle, IndexBlockParsedItem>,
        comparator: SharedComparator,
    ) -> Self {
        let decoder = decoder.double_ended_peekable();
        Self {
            decoder,
            comparator,
        }
    }

    fn seek_with_cache_resets(
        &mut self,
        needle: &[u8],
        seqno: SeqNo,
        reset_front: bool,
        reset_back: bool,
    ) -> bool {
        let cmp = &self.comparator;
        if reset_front {
            self.decoder.reset_front_peeked();
        }
        if reset_back {
            self.decoder.reset_back_peeked();
            self.decoder.inner_mut().reset_back_cursor();
        }
        // Lex fast path skips the `dyn UserComparator::compare` vtable in the
        // binary-search probe loop. Each closure is a distinct type, so
        // `Decoder::seek` monomorphizes per-shape and the inner loop is
        // virtual-call-free when the default lexicographic comparator is in use.
        let landed = if cmp.is_lexicographic() {
            self.decoder.inner_mut().seek(
                |end_key, s| match end_key.cmp(needle) {
                    core::cmp::Ordering::Greater => false,
                    core::cmp::Ordering::Less => true,
                    core::cmp::Ordering::Equal => s >= seqno,
                },
                true,
            )
        } else {
            self.decoder.inner_mut().seek(
                |end_key, s| match cmp.compare(end_key, needle) {
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

        if self.decoder.inner_mut().restart_interval() > 1 {
            self.decoder.inner_mut().advance_while(|item, bytes| {
                match item.compare_key(needle, bytes, cmp.as_ref()) {
                    core::cmp::Ordering::Greater => false,
                    core::cmp::Ordering::Less => true,
                    core::cmp::Ordering::Equal => item.seqno() >= seqno,
                }
            });
        }

        self.decoder.peek().is_some()
    }

    pub fn seek(&mut self, needle: &[u8], seqno: SeqNo) -> bool {
        self.seek_with_cache_resets(needle, seqno, true, true)
    }

    /// Full upper-bound re-seek: resets both front and back caches.
    ///
    /// For incremental bound adjustment that preserves a prior `seek_lower`'s
    /// front cache, use `seek_upper_bound_cursor` instead.
    pub fn seek_upper(&mut self, needle: &[u8], _seqno: SeqNo) -> bool {
        // seek_upper_impl may return Err on a poisoned/clamped cursor;
        // the public bool-returning API treats that as "not found" for
        // backward compatibility — callers that need error propagation
        // should use seek_upper_bound_cursor instead.
        self.seek_upper_impl(needle, true, true, true)
            .unwrap_or(false)
    }

    pub(crate) fn seek_upper_impl(
        &mut self,
        needle: &[u8],
        reset_front: bool,
        reset_back: bool,
        check_back_cache: bool,
    ) -> crate::Result<bool> {
        let cmp = &self.comparator;
        if reset_front {
            self.decoder.reset_front_peeked();
        }
        if reset_back {
            self.decoder.reset_back_peeked();
        }
        let restart_interval = self.decoder.inner_mut().restart_interval();
        // Same devirtualization strategy as `seek_with_cache_resets`: split on
        // `is_lexicographic()` so the inner binary-search predicate is a static
        // slice comparison on the lex path. The three predicate shapes (strict <,
        // ≤, ≤) each get their own pair of monomorphizations.
        let lex = cmp.is_lexicographic();
        let found = if restart_interval == 1 {
            if check_back_cache {
                // BACK CURSOR (reverse iteration): find the first block whose
                // end_key ≥ needle.  Using strict-less here together with
                // partition_point_2 lands exactly on that block.
                if lex {
                    self.decoder
                        .inner_mut()
                        .seek_upper(|end_key, _s| end_key < needle, true)
                } else {
                    self.decoder.inner_mut().seek_upper(
                        |end_key, _s| cmp.compare(end_key, needle) == core::cmp::Ordering::Less,
                        true,
                    )
                }
            } else {
                // FORWARD LIMIT (upper-bound for forward scan): we must include
                // *all* blocks whose end_key ≤ needle (they may contain entries
                // at needle) plus the first block with end_key > needle (it may
                // start at a key ≤ needle).  Using ≤ with partition_point_2
                // finds the first block with end_key > needle; that block is
                // included because hi_scanner.offset is placed *after* it.
                // When all blocks share the same end_key == needle (e.g. a
                // pure-merge scenario with 4 000 operands for one user_key),
                // the predicate is true for every entry so partition_point_2
                // returns the last entry — allowing all blocks to be visited.
                if lex {
                    self.decoder
                        .inner_mut()
                        .seek_upper(|end_key, _s| end_key <= needle, true)
                } else {
                    self.decoder.inner_mut().seek_upper(
                        |end_key, _s| cmp.compare(end_key, needle) != core::cmp::Ordering::Greater,
                        true,
                    )
                }
            }
        } else if lex {
            self.decoder
                .inner_mut()
                .seek_upper(|end_key, _s| end_key <= needle, true)
        } else {
            self.decoder.inner_mut().seek_upper(
                |end_key, _s| cmp.compare(end_key, needle) != core::cmp::Ordering::Greater,
                true,
            )
        };
        if !found {
            return Ok(false);
        }

        if restart_interval > 1 {
            self.decoder
                .inner_mut()
                .trim_back_to_upper_bound(|item, bytes| {
                    item.compare_key(needle, bytes, cmp.as_ref())
                });

            while self
                .decoder
                .inner_mut()
                .upper_stack_tail_cmp(|item, bytes| item.compare_key(needle, bytes, cmp.as_ref()))
                == Some(core::cmp::Ordering::Less)
            {
                if !self.decoder.inner_mut().advance_upper_restart_interval() {
                    break;
                }

                self.decoder
                    .inner_mut()
                    .trim_back_to_upper_bound(|item, bytes| {
                        item.compare_key(needle, bytes, cmp.as_ref())
                    });
            }

            // advance_upper_restart_interval may have clamped/poisoned the upper
            // cursor (empty stack after corruption). Propagate as an error so
            // callers do not treat a poisoned cursor as "empty range".
            if self
                .decoder
                .inner_mut()
                .upper_stack_tail_cmp(|item, bytes| item.compare_key(needle, bytes, cmp.as_ref()))
                .is_none()
            {
                return Err(crate::Error::InvalidTrailer);
            }
        }

        if check_back_cache {
            Ok(self.decoder.peek_back().is_some())
        } else {
            Ok(true)
        }
    }

    #[expect(
        clippy::unnecessary_wraps,
        reason = "API plumbing: inner seek_with_cache_resets will become fallible \
                  when Decoder binary-index parsing surfaces errors"
    )]
    pub(crate) fn seek_lower_bound_cursor(
        &mut self,
        needle: &[u8],
        seqno: SeqNo,
    ) -> crate::Result<bool> {
        Ok(self.seek_with_cache_resets(needle, seqno, true, false))
    }

    pub(crate) fn seek_upper_bound_cursor(
        &mut self,
        needle: &[u8],
        _seqno: SeqNo,
    ) -> crate::Result<bool> {
        // Keep the front cache intact: lower-bound cursor seeks intentionally
        // seed the first candidate via `peek()`. Clearing front cache here
        // would skip that candidate because the underlying decoder has already
        // advanced its low cursor past the peeked item.
        //
        // The cached candidate cannot fall outside the upper bound because callers
        // guarantee lo <= hi: seek_lower positions lo at the first block with
        // end_key >= lo_needle, and seek_upper positions hi at the first block with
        // end_key > hi_needle. Since lo_needle <= hi_needle, front_peeked is always
        // within the bounded window.
        self.seek_upper_impl(needle, false, true, false)
    }
}

impl Iterator for Iter<'_> {
    type Item = IndexBlockParsedItem;

    fn next(&mut self) -> Option<Self::Item> {
        self.decoder.next()
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.decoder.next_back()
    }
}

// Unit tests for IndexBlock::Iter seek/seek_upper behavior are covered by
// integration tests in tests/custom_comparator.rs (which exercise the full
// block-index → data-block path with both default and custom comparators)
// and by the existing table-level tests in src/table/tests.rs.

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    reason = "corruption tests intentionally mutate encoded bytes via direct indexing"
)]
mod tests;

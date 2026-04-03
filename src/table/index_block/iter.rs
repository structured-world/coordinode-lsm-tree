// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

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
        if !self.decoder.inner_mut().seek(
            |end_key, s| match cmp.compare(end_key, needle) {
                std::cmp::Ordering::Greater => false,
                std::cmp::Ordering::Less => true,
                std::cmp::Ordering::Equal => s >= seqno,
            },
            true,
        ) {
            return false;
        }

        if self.decoder.inner_mut().restart_interval() > 1 {
            self.decoder.inner_mut().advance_while(|item, bytes| {
                match item.compare_key(needle, bytes, cmp.as_ref()) {
                    std::cmp::Ordering::Greater => false,
                    std::cmp::Ordering::Less => true,
                    std::cmp::Ordering::Equal => item.seqno() >= seqno,
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
        let found = if restart_interval == 1 {
            if check_back_cache {
                // BACK CURSOR (reverse iteration): find the first block whose
                // end_key ≥ needle.  Using strict-less here together with
                // partition_point_2 lands exactly on that block.
                self.decoder.inner_mut().seek_upper(
                    |end_key, _s| cmp.compare(end_key, needle) == std::cmp::Ordering::Less,
                    true,
                )
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
                self.decoder.inner_mut().seek_upper(
                    |end_key, _s| cmp.compare(end_key, needle) != std::cmp::Ordering::Greater,
                    true,
                )
            }
        } else {
            self.decoder.inner_mut().seek_upper(
                |end_key, _s| cmp.compare(end_key, needle) != std::cmp::Ordering::Greater,
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
                == Some(std::cmp::Ordering::Less)
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
mod tests {
    use super::*;
    use crate::{
        Checksum,
        coding::Decode,
        comparator::default_comparator,
        table::{
            Block, BlockHandle, BlockOffset, IndexBlock, KeyedBlockHandle,
            block::{BlockType, Header, ParsedItem, Trailer},
        },
    };
    use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
    use std::io::Cursor;
    use varint_rs::VarintReader;

    fn make_handles(count: usize) -> Vec<KeyedBlockHandle> {
        (0..count)
            .map(|i| {
                let key = format!("adj:out:vertex-0001:edge-{i:04}");
                KeyedBlockHandle::new(
                    key.into(),
                    i as u64,
                    BlockHandle::new(BlockOffset((i as u64) * 4096), 4096),
                )
            })
            .collect()
    }

    fn make_index_block(restart_interval: u8) -> IndexBlock {
        let handles = make_handles(16);
        let bytes =
            IndexBlock::encode_into_vec_with_restart_interval(&handles, restart_interval).unwrap();
        IndexBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        })
    }

    fn make_corrupted_index_block_with_invalid_first_restart_head() -> IndexBlock {
        let handles = make_handles(16);
        let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 8).unwrap();
        let key_len_offset = first_restart_head_key_len_offset(&bytes);
        bytes[key_len_offset] = 0xFF;
        bytes[key_len_offset + 1] = 0xFF;
        bytes[key_len_offset + 2] = 0x03;
        IndexBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        })
    }

    fn first_restart_head_key_len_offset(bytes: &[u8]) -> usize {
        let mut cursor = Cursor::new(bytes);
        let marker = cursor.read_u8().unwrap();
        assert_eq!(marker, 0);
        let _ = BlockHandle::decode_from(&mut cursor).unwrap();
        let _ = cursor.read_u64_varint().unwrap();
        usize::try_from(cursor.position()).unwrap()
    }

    fn make_corrupted_index_block_with_invalid_binary_index_offset() -> IndexBlock {
        let handles = make_handles(16);
        let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 8).unwrap();

        let trailer_probe = IndexBlock::new(Block {
            data: bytes.clone().into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });
        let trailer_offset = Trailer::new(&trailer_probe.inner).trailer_offset();
        let binary_index_offset_pos = trailer_offset + 1 + 1 + std::mem::size_of::<u32>();
        #[expect(
            clippy::cast_possible_truncation,
            reason = "test block sizes stay well below u32::MAX"
        )]
        let invalid_binary_index_offset = (bytes.len() as u32).saturating_add(1);
        LittleEndian::write_u32(
            &mut bytes
                [binary_index_offset_pos..binary_index_offset_pos + std::mem::size_of::<u32>()],
            invalid_binary_index_offset,
        );

        IndexBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        })
    }

    #[test]
    fn seek_clears_stale_front_cache_before_reposition() {
        let index = make_index_block(8);
        let mut iter = index.iter(default_comparator());
        let mut fresh = index.iter(default_comparator());

        assert!(iter.seek(b"adj:out:vertex-0001:edge-0002", 0));
        let seek_after_reposition = iter.seek(b"adj:out:vertex-0001:edge-0011", 0);
        let item_after_reposition = iter.next().map(|item| item.materialize(index.as_slice()));

        let fresh_seek = fresh.seek(b"adj:out:vertex-0001:edge-0011", 0);
        let fresh_item = fresh.next().map(|item| item.materialize(index.as_slice()));

        assert_eq!(seek_after_reposition, fresh_seek);
        assert_eq!(item_after_reposition, fresh_item);
    }

    #[test]
    fn seek_upper_clears_stale_back_cache_before_reposition() {
        let index = make_index_block(8);
        let mut iter = index.iter(default_comparator());
        let mut fresh = index.iter(default_comparator());

        assert!(iter.seek_upper(b"adj:out:vertex-0001:edge-0011", SeqNo::MAX));
        let seek_after_reposition = iter.seek_upper(b"adj:out:vertex-0001:edge-0003", SeqNo::MAX);
        let item_after_reposition = iter
            .next_back()
            .map(|item| item.materialize(index.as_slice()));

        let fresh_seek = fresh.seek_upper(b"adj:out:vertex-0001:edge-0003", SeqNo::MAX);
        let fresh_item = fresh
            .next_back()
            .map(|item| item.materialize(index.as_slice()));

        assert_eq!(seek_after_reposition, fresh_seek);
        assert_eq!(item_after_reposition, fresh_item);
    }

    #[test]
    fn seek_upper_keeps_first_covering_handle_in_compressed_interval() {
        let index = make_index_block(8);
        let mut iter = index.iter(default_comparator());

        assert!(iter.seek_upper(b"adj:out:vertex-0001:edge-0003z", SeqNo::MAX));
        let selected = iter.next_back().unwrap().materialize(index.as_slice());
        assert_eq!(
            selected.end_key().as_ref(),
            b"adj:out:vertex-0001:edge-0004"
        );
    }

    #[test]
    fn seek_upper_keeps_exact_match_without_restoring_next_item() {
        let index = make_index_block(8);
        let mut iter = index.iter(default_comparator());

        assert!(iter.seek_upper(b"adj:out:vertex-0001:edge-0003", SeqNo::MAX));
        let selected = iter.next_back().unwrap().materialize(index.as_slice());
        assert_eq!(
            selected.end_key().as_ref(),
            b"adj:out:vertex-0001:edge-0003"
        );
    }

    #[test]
    fn seek_upper_with_small_needle_restores_first_item_when_interval_trim_empties_stack() {
        let index = make_index_block(8);
        let mut iter = index.iter(default_comparator());

        assert!(iter.seek_upper(b"adj:out:vertex-0001:edge--001", SeqNo::MAX));
        let selected = iter.next_back().unwrap().materialize(index.as_slice());
        assert_eq!(
            selected.end_key().as_ref(),
            b"adj:out:vertex-0001:edge-0000"
        );
    }

    #[test]
    fn seek_upper_exact_match_restart_interval_one_keeps_exact_handle() {
        let index = make_index_block(1);
        let mut iter = index.iter(default_comparator());

        assert!(iter.seek_upper(b"adj:out:vertex-0001:edge-0003", SeqNo::MAX));
        let selected = iter.next_back().unwrap().materialize(index.as_slice());
        assert_eq!(
            selected.end_key().as_ref(),
            b"adj:out:vertex-0001:edge-0003"
        );
    }

    #[test]
    fn seek_upper_with_large_needle_keeps_back_cursor_at_last_item() {
        let index = make_index_block(8);
        let mut iter = index.iter(default_comparator());

        assert!(iter.seek_upper(b"adj:out:vertex-0001:edge-9999", SeqNo::MAX));
        let selected = iter.next_back().unwrap().materialize(index.as_slice());
        assert_eq!(
            selected.end_key().as_ref(),
            b"adj:out:vertex-0001:edge-0015"
        );
    }

    #[test]
    fn seek_upper_between_intervals_keeps_next_restart_head() {
        let index = make_index_block(8);
        let mut iter = index.iter(default_comparator());

        assert!(iter.seek_upper(b"adj:out:vertex-0001:edge-0007z", SeqNo::MAX));
        let selected = iter.next_back().unwrap().materialize(index.as_slice());
        assert_eq!(
            selected.end_key().as_ref(),
            b"adj:out:vertex-0001:edge-0008"
        );
    }

    #[test]
    fn seek_upper_forward_iteration_stops_after_first_covering_item() {
        let index = make_index_block(8);
        let mut iter = index.iter(default_comparator());

        assert!(iter.seek_upper(b"adj:out:vertex-0001:edge-0003z", SeqNo::MAX));

        let keys: Vec<Vec<u8>> = iter
            .map(|item| item.materialize(index.as_slice()).end_key().to_vec())
            .collect();

        assert_eq!(
            keys,
            vec![
                b"adj:out:vertex-0001:edge-0000".to_vec(),
                b"adj:out:vertex-0001:edge-0001".to_vec(),
                b"adj:out:vertex-0001:edge-0002".to_vec(),
                b"adj:out:vertex-0001:edge-0003".to_vec(),
                b"adj:out:vertex-0001:edge-0004".to_vec(),
            ]
        );
    }

    #[test]
    fn seek_upper_bound_cursor_between_intervals_includes_next_restart_head() {
        let index = make_index_block(8);
        let mut iter = index.iter(default_comparator());

        assert!(
            iter.seek_upper_bound_cursor(b"adj:out:vertex-0001:edge-0007z", SeqNo::MAX)
                .unwrap()
        );

        let keys: Vec<Vec<u8>> = iter
            .map(|item| item.materialize(index.as_slice()).end_key().to_vec())
            .collect();

        assert_eq!(
            keys,
            vec![
                b"adj:out:vertex-0001:edge-0000".to_vec(),
                b"adj:out:vertex-0001:edge-0001".to_vec(),
                b"adj:out:vertex-0001:edge-0002".to_vec(),
                b"adj:out:vertex-0001:edge-0003".to_vec(),
                b"adj:out:vertex-0001:edge-0004".to_vec(),
                b"adj:out:vertex-0001:edge-0005".to_vec(),
                b"adj:out:vertex-0001:edge-0006".to_vec(),
                b"adj:out:vertex-0001:edge-0007".to_vec(),
                b"adj:out:vertex-0001:edge-0008".to_vec(),
            ]
        );
    }

    #[test]
    fn seek_reposition_clears_stale_back_cache() {
        let index = make_index_block(8);
        let mut iter = index.iter(default_comparator());
        let mut control = index.iter(default_comparator());

        assert!(iter.seek_upper(b"adj:out:vertex-0001:edge-0011", SeqNo::MAX));
        assert_eq!(
            iter.decoder
                .peek_back()
                .unwrap()
                .materialize(index.as_slice())
                .end_key()
                .as_ref(),
            b"adj:out:vertex-0001:edge-0011"
        );

        assert!(iter.seek(b"adj:out:vertex-0001:edge-0003", 0));
        let item_after_reposition = iter
            .next_back()
            .map(|item| item.materialize(index.as_slice()));

        assert!(control.seek(b"adj:out:vertex-0001:edge-0003", 0));
        let control_item = control
            .next_back()
            .map(|item| item.materialize(index.as_slice()));

        assert_eq!(item_after_reposition, control_item);
        assert_eq!(
            item_after_reposition.unwrap().end_key().as_ref(),
            b"adj:out:vertex-0001:edge-0015"
        );
    }

    #[test]
    fn seek_upper_reposition_clears_stale_front_cache() {
        let index = make_index_block(8);
        let mut iter = index.iter(default_comparator());
        let mut control = index.iter(default_comparator());

        assert!(iter.seek(b"adj:out:vertex-0001:edge-0002", 0));
        let stale_front_key = iter
            .decoder
            .peek()
            .unwrap()
            .materialize(index.as_slice())
            .end_key()
            .to_vec();

        assert!(iter.seek_upper(b"adj:out:vertex-0001:edge-0007", SeqNo::MAX));
        let item_after_reposition = iter
            .next_back()
            .map(|item| item.materialize(index.as_slice()));

        assert!(control.seek(b"adj:out:vertex-0001:edge-0002", 0));
        assert!(control.seek_upper(b"adj:out:vertex-0001:edge-0007", SeqNo::MAX));
        let control_item = control
            .next_back()
            .map(|item| item.materialize(index.as_slice()));

        assert_eq!(item_after_reposition, control_item);
        assert_ne!(
            item_after_reposition.unwrap().end_key().as_ref(),
            stale_front_key.as_slice()
        );
    }

    #[test]
    fn next_stays_none_after_invalid_restart_head_parse() {
        let index = make_corrupted_index_block_with_invalid_first_restart_head();
        let mut iter = index.iter(default_comparator());

        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
    }

    #[test]
    fn try_iter_rejects_invalid_binary_index_offset() {
        let index = make_corrupted_index_block_with_invalid_binary_index_offset();
        // try_iter rejects the block eagerly due to invalid layout metadata
        assert!(
            matches!(
                index.try_iter(default_comparator()),
                Err(crate::Error::InvalidTrailer)
            ),
            "corrupt binary_index_offset must be rejected by try_iter",
        );
    }

    /// Finds the byte offset of the second restart head by reading the binary
    /// index from the block trailer.
    fn second_restart_head_byte_offset(bytes: &[u8]) -> usize {
        let probe = IndexBlock::new(Block {
            data: bytes.to_vec().into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });
        let trailer_offset = Trailer::new(&probe.inner).trailer_offset();
        let trailer = &bytes[trailer_offset..];

        // Trailer layout: restart_interval(u8)[0], step_size(u8)[1],
        //   binary_index_len(u32LE)[2..6], binary_index_offset(u32LE)[6..10], ...
        let step_size = trailer[1] as usize;
        let binary_index_offset = LittleEndian::read_u32(&trailer[6..10]) as usize;

        // Entry 1 in the binary index is the second restart head offset
        let entry_pos = binary_index_offset + step_size;
        if step_size == 2 {
            LittleEndian::read_u16(&bytes[entry_pos..entry_pos + 2]) as usize
        } else {
            LittleEndian::read_u32(&bytes[entry_pos..entry_pos + 4]) as usize
        }
    }

    /// Returns the byte offset of the first truncated (non-head) item in the
    /// restart interval starting at `restart_offset`, after skipping the full
    /// restart head + its key bytes.
    fn first_truncated_item_offset_in_interval(bytes: &[u8], restart_offset: usize) -> usize {
        let mut cursor = Cursor::new(&bytes[restart_offset..]);
        // Restart head: marker(0) + BlockHandle + seqno + key_len + key_bytes
        let marker = cursor.read_u8().unwrap();
        assert_eq!(marker, 0, "expected restart head marker");
        let _ = BlockHandle::decode_from(&mut cursor).unwrap();
        let _ = cursor.read_u64_varint().unwrap();
        let key_len: u64 = cursor.read_u16_varint().unwrap().into();
        cursor.set_position(cursor.position() + key_len);
        // Now at the first truncated item
        let truncated_marker = cursor.read_u8().unwrap();
        assert_eq!(truncated_marker, 1, "second entry should be truncated");
        let _ = cursor.read_u16_varint().unwrap(); // shared prefix len
        // cursor is now at rest_key_len — corrupt THIS to break fill_stack
        restart_offset + usize::try_from(cursor.position()).unwrap()
    }

    fn make_index_block_with_corrupt_second_interval_item() -> IndexBlock {
        let handles = make_handles(16);
        let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 8).unwrap();
        let second_restart = second_restart_head_byte_offset(&bytes);
        let rest_key_len_pos = first_truncated_item_offset_in_interval(&bytes, second_restart);
        // Overwrite rest_key_len with an impossibly large varint
        bytes[rest_key_len_pos] = 0xFF;
        bytes[rest_key_len_pos + 1] = 0xFF;
        bytes[rest_key_len_pos + 2] = 0x03;
        IndexBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        })
    }

    #[test]
    fn seek_upper_bound_cursor_returns_err_on_poisoned_cursor() {
        // Block layout: 16 entries with restart_interval=8 → 2 restart intervals.
        // First interval (entries 0-7, keys edge-0000..edge-0007) is valid.
        // Second interval has a corrupted non-head item (entry 9): its
        // rest_key_len is overwritten so fill_stack poisons the back cursor.
        // The restart head (entry 8) is valid so binary search still works.
        //
        // Needle "edge-0007z" lands in the first interval via binary search.
        // trim_back_to_upper_bound doesn't pop anything (all first-interval
        // items ≤ needle), but the stack tail "edge-0007" < "edge-0007z" so
        // the advance loop fires.
        // advance_upper_restart_interval clears the stack and tries to fill from
        // the corrupt second interval → fill_stack fails → stack empty →
        // seek_upper_bound_cursor must return Err(InvalidTrailer), not Ok(false).
        let index = make_index_block_with_corrupt_second_interval_item();
        let mut iter = index.iter(default_comparator());

        let result = iter.seek_upper_bound_cursor(b"adj:out:vertex-0001:edge-0007z", SeqNo::MAX);
        assert!(
            matches!(result, Err(crate::Error::InvalidTrailer)),
            "poisoned upper cursor must return Err(InvalidTrailer), got {result:?}",
        );
    }
}

// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{binary_index::Reader as BinaryIndexReader, TRAILER_START_MARKER};
use crate::{
    table::{block::Trailer, Block},
    SeqNo, Slice,
};
use byteorder::{LittleEndian, ReadBytesExt};
use std::{io::Cursor, marker::PhantomData};

/// Validates that `restart_interval` and `binary_index_step_size` read from a
/// block trailer are within their allowed ranges.
///
/// Returns `Err(crate::Error::InvalidTrailer)` on malformed values that would
/// otherwise trigger an assertion failure deeper in the decoder.
fn validate_trailer_fields(restart_interval: u8, binary_index_step_size: u8) -> crate::Result<()> {
    if restart_interval == 0 {
        return Err(crate::Error::InvalidTrailer);
    }
    if binary_index_step_size != 2 && binary_index_step_size != 4 {
        return Err(crate::Error::InvalidTrailer);
    }
    Ok(())
}

/// Represents an object that was parsed from a byte array
///
/// Parsed items only hold references to their keys and values, use `materialize` to create an owned value.
pub trait ParsedItem<M> {
    /// Compares this item's key with a needle using the given comparator.
    ///
    /// We can not access the key directly because it may be comprised of prefix + suffix.
    fn compare_key(
        &self,
        needle: &[u8],
        bytes: &[u8],
        cmp: &dyn crate::comparator::UserComparator,
    ) -> std::cmp::Ordering;

    /// Returns the item's seqno.
    fn seqno(&self) -> SeqNo;

    /// Returns the byte offset of the key's start position.
    fn key_offset(&self) -> usize;

    /// Returns one-past-the-end byte offset of the key.
    fn key_end_offset(&self) -> usize;

    /// Converts the parsed representation to an owned value.
    fn materialize(&self, bytes: &Slice) -> M;
}

/// Describes an object that can be parsed from a block, either a full item (restart head), or a truncated item
pub trait Decodable<ParsedItem> {
    /// Parses the key of the next restart head from a reader.
    ///
    /// This is the fast path for binary-search probes: it skips full-item
    /// decode and only extracts `(key, seqno)`. `entries_end` bounds the
    /// key span so malformed lengths cannot leak into trailer/index bytes.
    fn parse_restart_key<'a>(
        reader: &mut Cursor<&[u8]>,
        offset: usize,
        data: &'a [u8],
        entries_end: usize,
    ) -> Option<(&'a [u8], SeqNo)>;

    /// Parses a restart head from a reader.
    ///
    /// `offset` is the position of the item to read in the block's byte slice.
    fn parse_full(
        reader: &mut Cursor<&[u8]>,
        offset: usize,
        entries_end: usize,
    ) -> Option<ParsedItem>;

    /// Parses a (possibly) prefix truncated item from a reader.
    fn parse_truncated(
        reader: &mut Cursor<&[u8]>,
        offset: usize,
        base_key_offset: usize,
        base_key_end: usize,
        entries_end: usize,
    ) -> Option<ParsedItem>;
}

#[derive(Debug)]
struct LoScanner {
    offset: usize,
    remaining_in_interval: usize,
    base_key_offset: Option<usize>,
    base_key_end: Option<usize>,
}

#[derive(Debug)]
struct HiScanner {
    offset: usize,
    ptr_idx: usize,
    stack: Vec<usize>, // TODO: SmallVec?
    base_key_offset: Option<usize>,
    base_key_end: Option<usize>,
}

/// Generic block decoder for RocksDB-style blocks
///
/// Supports prefix truncation and binary search index (through restart intervals).
pub struct Decoder<'a, Item: Decodable<Parsed>, Parsed: ParsedItem<Item>> {
    block: &'a Block,
    phantom: PhantomData<(Item, Parsed)>,

    lo_scanner: LoScanner,
    hi_scanner: HiScanner,

    // Cached metadata
    restart_interval: u8,
    binary_index_step_size: u8,
    binary_index_offset: u32,
    binary_index_len: u32,
    hash_index_len: u32,
    hash_index_offset: u32,
    cached_entries_end: Option<usize>,
}

impl<'a, Item: Decodable<Parsed>, Parsed: ParsedItem<Item>> Decoder<'a, Item, Parsed> {
    #[must_use]
    pub fn restart_interval(&self) -> u8 {
        self.restart_interval
    }

    /// Creates a new block decoder, returning an error on malformed trailer
    /// fields instead of panicking.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::InvalidTrailer`] when:
    /// - the block is too small to contain a trailer,
    /// - `restart_interval` is zero,
    /// - `binary_index_step_size` is not 2 or 4, or
    /// - binary/hash-index layout metadata is inconsistent.
    pub fn try_new(block: &'a Block) -> crate::Result<Self> {
        let trailer = Trailer::try_new(block)?;
        let mut reader = trailer.as_slice();

        let restart_interval = unwrap!(reader.read_u8());
        let binary_index_step_size = unwrap!(reader.read_u8());

        validate_trailer_fields(restart_interval, binary_index_step_size)?;

        let binary_index_len = unwrap!(reader.read_u32::<LittleEndian>());
        let binary_index_offset = unwrap!(reader.read_u32::<LittleEndian>());
        let hash_index_len = unwrap!(reader.read_u32::<LittleEndian>());
        let hash_index_offset = unwrap!(reader.read_u32::<LittleEndian>());

        let mut decoder = Self {
            block,
            phantom: PhantomData,

            lo_scanner: LoScanner {
                offset: 0,
                remaining_in_interval: 0,
                base_key_offset: None,
                base_key_end: None,
            },

            hi_scanner: HiScanner {
                offset: 0,
                ptr_idx: usize::try_from(binary_index_len).unwrap_or(usize::MAX),
                stack: Vec::new(),
                base_key_offset: None,
                base_key_end: None,
            },

            restart_interval,

            binary_index_step_size,
            binary_index_offset,
            binary_index_len,
            hash_index_len,
            hash_index_offset,
            cached_entries_end: None,
        };
        decoder.cached_entries_end = Some(
            decoder
                .compute_entries_end()
                .ok_or(crate::Error::InvalidTrailer)?,
        );
        Ok(decoder)
    }

    /// Creates a new block decoder.
    ///
    /// # Panics
    ///
    /// Panics on any block corruption detected by [`Self::try_new`]:
    /// undersized blocks, invalid trailer fields, or inconsistent
    /// binary/hash-index layout metadata. Prefer `try_new` in I/O paths
    /// where corrupt blocks should produce a structured error.
    #[must_use]
    #[expect(
        clippy::expect_used,
        reason = "infallible wrapper for test/non-I/O paths"
    )]
    pub fn new(block: &'a Block) -> Self {
        Self::try_new(block).expect("valid block trailer")
    }

    fn binary_index_bounds(&self) -> Option<(usize, usize)> {
        let step_size = match self.binary_index_step_size {
            2 | 4 => usize::from(self.binary_index_step_size),
            _ => return None,
        };
        let binary_index_len = usize::try_from(self.binary_index_len).ok()?;
        if binary_index_len == 0 {
            return None;
        }
        let binary_index_bytes = binary_index_len.checked_mul(step_size)?;
        let binary_index_offset = usize::try_from(self.binary_index_offset).ok()?;
        let binary_index_end = binary_index_offset.checked_add(binary_index_bytes)?;
        let trailer_offset = Trailer::new(self.block).trailer_offset();
        let hash_index_len = usize::try_from(self.hash_index_len).ok()?;
        let hash_index_offset = usize::try_from(self.hash_index_offset).ok()?;

        if (hash_index_len == 0) != (hash_index_offset == 0) {
            return None;
        }

        if hash_index_offset > trailer_offset {
            return None;
        }

        if hash_index_offset > 0 {
            let hash_index_end = hash_index_offset.checked_add(hash_index_len)?;
            if hash_index_end != trailer_offset {
                return None;
            }
        }

        let binary_index_limit = if hash_index_offset > 0 {
            hash_index_offset
        } else {
            trailer_offset
        };
        if binary_index_offset == 0 || binary_index_end != binary_index_limit {
            return None;
        }

        Some((binary_index_offset, binary_index_end))
    }

    fn get_binary_index_reader(&self) -> Option<BinaryIndexReader<'_>> {
        let (binary_index_offset, _) = self.binary_index_bounds()?;
        if self.block.data.get(binary_index_offset - 1).copied()? != TRAILER_START_MARKER {
            return None;
        }

        Some(BinaryIndexReader::new(
            &self.block.data,
            self.binary_index_offset,
            self.binary_index_len,
            self.binary_index_step_size,
        ))
    }

    fn reader_at(data: &[u8], offset: usize) -> Option<Cursor<&[u8]>> {
        if offset >= data.len() {
            return None;
        }
        Some(Cursor::new(data.get(offset..)?))
    }

    fn get_key_at(&self, pos: usize, entries_end: usize) -> Option<(&[u8], SeqNo)> {
        if pos >= entries_end {
            return None;
        }
        let bytes = &self.block.data;
        let mut cursor = Self::reader_at(bytes, pos)?;
        Item::parse_restart_key(&mut cursor, pos, bytes, entries_end)
    }

    fn partition_point<F>(&self, pred: F) -> Option<(/* offset */ usize, /* idx */ usize)>
    where
        F: Fn(&[u8], SeqNo) -> bool,
    {
        // The first pass over the binary index emulates `Iterator::partition_point` over the
        // restart heads that are in natural key order.  We keep track of both the byte offset and
        // the restart index because callers need the offset to seed the linear scanner, while the
        // index is sometimes reused (for example by `seek_upper`).
        //
        // In contrast to the usual `partition_point`, we intentionally return the *last* restart
        // entry when the predicate continues to hold for every head key.  Forward scans rely on
        // this behaviour to land on the final restart interval and resume the linear scan there
        // instead of erroneously reporting "not found".
        let binary_index = self.get_binary_index_reader()?;
        let entries_end = self.entries_end()?;

        debug_assert!(
            binary_index.len() >= 1,
            "binary index should never be empty",
        );

        let mut left: usize = 0;
        let mut right = binary_index.len();

        if right == 0 {
            return None;
        }

        while left < right {
            let mid = usize::midpoint(left, right);

            let offset = binary_index.get(mid);

            let (head_key, head_seqno) = self.get_key_at(offset, entries_end)?;

            if pred(head_key, head_seqno) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        if left == 0 {
            return Some((0, 0));
        }

        if left == binary_index.len() {
            let idx = binary_index.len() - 1;
            let offset = binary_index.get(idx);
            return Some((offset, idx));
        }

        let offset = binary_index.get(left - 1);

        Some((offset, left - 1))
    }

    // TODO:
    fn partition_point_2<F>(&self, pred: F) -> Option<(/* offset */ usize, /* idx */ usize)>
    where
        F: Fn(&[u8], SeqNo) -> bool,
    {
        // `partition_point_2` mirrors `partition_point` but keeps the *next* restart entry instead
        // of the previous one. This variant is used exclusively by reverse scans (`seek_upper`)
        // that want the first restart whose head key exceeds the predicate. Returning the raw
        // offset preserves the ability to reuse linear scanning infrastructure without duplicating
        // decoder logic.
        let binary_index = self.get_binary_index_reader()?;
        let entries_end = self.entries_end()?;

        debug_assert!(
            binary_index.len() >= 1,
            "binary index should never be empty",
        );

        let mut left: usize = 0;
        let mut right = binary_index.len();

        if right == 0 {
            return None;
        }

        while left < right {
            let mid = usize::midpoint(left, right);

            let offset = binary_index.get(mid);

            let (head_key, head_seqno) = self.get_key_at(offset, entries_end)?;

            if pred(head_key, head_seqno) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        if left == binary_index.len() {
            let idx = binary_index.len() - 1;
            let offset = binary_index.get(idx);
            return Some((offset, idx));
        }

        let offset = binary_index.get(left);

        Some((offset, left))
    }

    pub fn set_lo_offset(&mut self, offset: usize) {
        self.lo_scanner.offset = offset;
    }

    /// Resets reverse-scan state so the next `next_back()` starts from the
    /// right edge of the block rather than any previously seeded upper bound.
    pub fn reset_back_cursor(&mut self) {
        self.hi_scanner.ptr_idx = usize::try_from(self.binary_index_len).unwrap_or(usize::MAX);
        self.hi_scanner.stack.clear();
        self.hi_scanner.base_key_offset = None;
        self.hi_scanner.base_key_end = None;
    }

    fn poison_back_cursor(&mut self) {
        // Clamp rather than clear: setting base_key_offset = Some(0) keeps
        // the upper bound visible to next(), so forward iteration also stops.
        // Clearing to None would let next() continue past the corrupted
        // interval because it only enforces the ceiling when base_key_offset
        // is Some.
        self.clamp_upper_to_lo();
    }

    fn clamp_upper_to_lo(&mut self) {
        self.hi_scanner.offset = self.lo_scanner.offset;
        self.hi_scanner.ptr_idx = usize::MAX;
        self.hi_scanner.stack.clear();
        self.hi_scanner.base_key_offset = Some(0);
        self.hi_scanner.base_key_end = Some(0);
    }

    fn exhaust(&mut self) {
        let end = self.block.data.len();

        self.lo_scanner.offset = end;
        self.lo_scanner.remaining_in_interval = 0;
        self.lo_scanner.base_key_offset = None;
        self.lo_scanner.base_key_end = None;

        self.hi_scanner.offset = end;
        self.hi_scanner.ptr_idx = usize::MAX;
        self.hi_scanner.stack.clear();
        self.hi_scanner.base_key_offset = Some(0);
        self.hi_scanner.base_key_end = Some(0);
    }

    /// Seeks using the given predicate.
    ///
    /// Returns `false` if the key does not possible exist.
    #[must_use]
    pub fn seek(&mut self, pred: impl Fn(&[u8], SeqNo) -> bool, second_partition: bool) -> bool {
        // Index blocks historically used `second_partition` because with restart_interval=1 each
        // restart head is also an item boundary, so the "next restart head" is the lower bound we
        // want. Once restart intervals can be larger than 1, jumping to the next restart head
        // would skip all items in the current interval. In that case we must keep using the
        // current interval and linearly scan within it.
        let use_next_restart = second_partition && self.restart_interval == 1;

        let result = if use_next_restart {
            self.partition_point_2(&pred)
        } else {
            self.partition_point(&pred)
        };

        // Binary index lookup
        let Some((offset, _)) = result else {
            self.exhaust();
            return false;
        };

        if use_next_restart
            && self
                .entries_end()
                .and_then(|entries_end| self.get_key_at(offset, entries_end))
                .is_some_and(|(key, seqno)| pred(key, seqno))
        {
            // `second_partition == true` means we ran the "look one restart ahead" search used by
            // index blocks. When the predicate is still true at the chosen restart head it means
            // the caller asked us to seek strictly beyond the last entry. In that case we skip any
            // costly parsing and flip both scanners into an "exhausted" state so the outer iterator
            // immediately reports EOF.
            self.exhaust();
            return false;
        }

        self.lo_scanner.offset = offset;
        self.lo_scanner.remaining_in_interval = 0;
        self.lo_scanner.base_key_offset = None;
        self.lo_scanner.base_key_end = None;

        true
    }

    /// Seeks the upper bound using the given predicate.
    ///
    /// Returns `false` if the key does not possible exist.
    #[must_use]
    pub fn seek_upper(
        &mut self,
        pred: impl Fn(&[u8], SeqNo) -> bool,
        second_partition: bool,
    ) -> bool {
        let use_next_restart = second_partition && self.restart_interval == 1;

        let result = if use_next_restart {
            self.partition_point_2(&pred)
        } else {
            self.partition_point(&pred)
        };

        // Binary index lookup
        let Some((offset, idx)) = result else {
            self.exhaust();
            return false;
        };

        self.hi_scanner.offset = offset;
        self.hi_scanner.ptr_idx = idx;
        self.hi_scanner.stack.clear();
        self.hi_scanner.base_key_offset = None;
        self.hi_scanner.base_key_end = None;

        self.fill_stack();
        if self.hi_scanner.stack.is_empty() {
            // `fill_stack` failed to decode the selected upper interval and poisoned the
            // reverse scanner. Fail closed by turning the upper cursor into a zero-width bound
            // at the current forward offset so bounded scans cannot continue past the limit.
            self.clamp_upper_to_lo();
            return false;
        }

        true
    }

    fn parse_current_item(
        reader: &mut Cursor<&[u8]>,
        offset: usize,
        base_key_offset: Option<usize>,
        base_key_end: Option<usize>,
        is_restart: bool,
        entries_end: usize,
    ) -> Option<Parsed> {
        if is_restart {
            Item::parse_full(reader, offset, entries_end)
        } else {
            #[expect(clippy::expect_used, reason = "we trust the is_restart flag")]
            Item::parse_truncated(
                reader,
                offset,
                base_key_offset.expect("should parse truncated item"),
                base_key_end.expect("should parse truncated item"),
                entries_end,
            )
        }
    }

    fn compute_entries_end(&self) -> Option<usize> {
        let (binary_index_offset, _) = self.binary_index_bounds()?;
        if self.block.data.get(binary_index_offset - 1).copied()? != TRAILER_START_MARKER {
            return None;
        }
        Some(binary_index_offset - 1)
    }

    fn entries_end(&self) -> Option<usize> {
        self.cached_entries_end
    }

    fn fill_stack(&mut self) {
        // Always rebuild from clean state: consume_stack_top may have partially
        // drained the stack (e.g. early return on offset < lo_scanner.offset),
        // leaving stale offsets from the previous interval.
        self.hi_scanner.stack.clear();
        self.hi_scanner.base_key_offset = None;
        self.hi_scanner.base_key_end = None;

        let Some(entries_end) = self.entries_end() else {
            self.poison_back_cursor();
            return;
        };
        let Some(binary_index) = self.get_binary_index_reader() else {
            self.poison_back_cursor();
            return;
        };
        if self.hi_scanner.ptr_idx >= binary_index.len() {
            // Stack/base_key already cleared at function entry.
            return;
        }

        {
            self.hi_scanner.offset = binary_index.get(self.hi_scanner.ptr_idx);

            let offset = self.hi_scanner.offset;
            let Some(mut reader) = Self::reader_at(&self.block.data, offset) else {
                self.poison_back_cursor();
                return;
            };

            #[expect(
                clippy::cast_possible_truncation,
                reason = "blocks do not even come close to 4 GiB in size"
            )]
            let parsed_restart =
                Item::parse_full(&mut reader, offset, entries_end).inspect(|item| {
                    self.hi_scanner.offset += reader.position() as usize;
                    self.hi_scanner.base_key_offset = Some(item.key_offset());
                    self.hi_scanner.base_key_end = Some(item.key_end_offset());
                });

            if parsed_restart.is_some() {
                self.hi_scanner.stack.push(offset);
            } else {
                self.poison_back_cursor();
                return;
            }
        }

        for _ in 1..self.restart_interval {
            let offset = self.hi_scanner.offset;
            let Some(mut reader) = Self::reader_at(&self.block.data, offset) else {
                self.poison_back_cursor();
                return;
            };

            #[expect(clippy::expect_used, reason = "base key offset is expected to exist")]
            #[expect(
                clippy::cast_possible_truncation,
                reason = "blocks do not even come close to 4 GiB in size"
            )]
            if Item::parse_truncated(
                &mut reader,
                offset,
                self.hi_scanner.base_key_offset.expect("should exist"),
                self.hi_scanner.base_key_end.expect("should exist"),
                entries_end,
            )
            .inspect(|_| {
                self.hi_scanner.offset += reader.position() as usize;
            })
            .is_some()
            {
                self.hi_scanner.stack.push(offset);
            } else {
                if offset < entries_end {
                    self.poison_back_cursor();
                    return;
                }
                break;
            }
        }
    }

    fn consume_stack_top(&mut self) -> Option<Parsed> {
        let offset = self.hi_scanner.stack.pop()?;
        let entries_end = self.entries_end()?;

        if self.lo_scanner.offset > 0 && offset < self.lo_scanner.offset {
            return None;
        }

        self.hi_scanner.offset = offset;

        let is_restart = self.hi_scanner.stack.is_empty();

        let mut reader = Self::reader_at(&self.block.data, offset)?;

        Self::parse_current_item(
            &mut reader,
            offset,
            self.hi_scanner.base_key_offset,
            self.hi_scanner.base_key_end,
            is_restart,
            entries_end,
        )
    }

    pub fn advance_while(&mut self, pred: impl Fn(&Parsed, &[u8]) -> bool) {
        let Some(entries_end) = self.entries_end() else {
            return;
        };

        loop {
            let hi_offset = if self.hi_scanner.base_key_offset.is_some() {
                Some(self.hi_scanner.offset)
            } else {
                None
            };
            if hi_offset.is_some_and(|hi| self.lo_scanner.offset >= hi) {
                break;
            }

            let is_restart = self.lo_scanner.remaining_in_interval == 0;

            let Some(mut reader) = Self::reader_at(&self.block.data, self.lo_scanner.offset) else {
                break;
            };

            let Some(item) = Self::parse_current_item(
                &mut reader,
                self.lo_scanner.offset,
                self.lo_scanner.base_key_offset,
                self.lo_scanner.base_key_end,
                is_restart,
                entries_end,
            ) else {
                break;
            };

            if !pred(&item, &self.block.data) {
                break;
            }

            #[expect(
                clippy::cast_possible_truncation,
                reason = "blocks do not even come close to 4 GiB in size"
            )]
            {
                let Some(next_offset) = self
                    .lo_scanner
                    .offset
                    .checked_add(reader.position() as usize)
                else {
                    break;
                };
                if hi_offset.is_some_and(|hi| next_offset > hi) {
                    break;
                }
                self.lo_scanner.offset = next_offset;
            }

            if is_restart {
                self.lo_scanner.base_key_offset = Some(item.key_offset());
                self.lo_scanner.base_key_end = Some(item.key_end_offset());
                self.lo_scanner.remaining_in_interval = usize::from(self.restart_interval) - 1;
            } else {
                self.lo_scanner.remaining_in_interval -= 1;
            }
        }
    }

    pub fn trim_back_to_upper_bound(&mut self, cmp: impl Fn(&Parsed, &[u8]) -> std::cmp::Ordering) {
        let Some(entries_end) = self.entries_end() else {
            return;
        };

        let mut last_popped = None;

        loop {
            let Some(&offset) = self.hi_scanner.stack.last() else {
                break;
            };

            let is_restart = self.hi_scanner.stack.len() == 1;

            let Some(mut reader) = Self::reader_at(&self.block.data, offset) else {
                break;
            };

            let Some(item) = Self::parse_current_item(
                &mut reader,
                offset,
                self.hi_scanner.base_key_offset,
                self.hi_scanner.base_key_end,
                is_restart,
                entries_end,
            ) else {
                break;
            };

            if cmp(&item, &self.block.data) != std::cmp::Ordering::Greater {
                break;
            }

            last_popped = Some(offset);
            self.hi_scanner.stack.pop();
        }

        let Some(candidate_offset) = last_popped else {
            return;
        };

        let should_restore = if let Some(&offset) = self.hi_scanner.stack.last() {
            let is_restart = self.hi_scanner.stack.len() == 1;

            let Some(mut reader) = Self::reader_at(&self.block.data, offset) else {
                return;
            };

            let Some(item) = Self::parse_current_item(
                &mut reader,
                offset,
                self.hi_scanner.base_key_offset,
                self.hi_scanner.base_key_end,
                is_restart,
                entries_end,
            ) else {
                return;
            };

            cmp(&item, &self.block.data) == std::cmp::Ordering::Less
        } else {
            true
        };

        if should_restore {
            // `candidate_offset` is the first item with key > needle.
            //
            // For reverse upper seeks we intentionally keep that first-greater item:
            // it is the covering interval boundary that may still contain `needle`.
            // Dropping it would incorrectly move `next_back()` to the previous item
            // (< needle) and skip the covering block.
            self.hi_scanner.stack.push(candidate_offset);
        }

        let Some(&offset) = self.hi_scanner.stack.last() else {
            return;
        };
        let is_restart = self.hi_scanner.stack.len() == 1;

        let Some(mut reader) = Self::reader_at(&self.block.data, offset) else {
            return;
        };

        #[expect(
            clippy::cast_possible_truncation,
            reason = "blocks do not even come close to 4 GiB in size"
        )]
        if Self::parse_current_item(
            &mut reader,
            offset,
            self.hi_scanner.base_key_offset,
            self.hi_scanner.base_key_end,
            is_restart,
            entries_end,
        )
        .is_some()
        {
            self.hi_scanner.offset = offset + reader.position() as usize;
        }
    }

    #[must_use]
    pub fn upper_stack_tail_cmp(
        &self,
        cmp: impl Fn(&Parsed, &[u8]) -> std::cmp::Ordering,
    ) -> Option<std::cmp::Ordering> {
        let &offset = self.hi_scanner.stack.last()?;
        let entries_end = self.entries_end()?;
        let is_restart = self.hi_scanner.stack.len() == 1;

        let mut reader = Self::reader_at(&self.block.data, offset)?;

        let item = Self::parse_current_item(
            &mut reader,
            offset,
            self.hi_scanner.base_key_offset,
            self.hi_scanner.base_key_end,
            is_restart,
            entries_end,
        )?;

        Some(cmp(&item, &self.block.data))
    }

    #[must_use]
    pub fn advance_upper_restart_interval(&mut self) -> bool {
        let Some(next_idx) = self.hi_scanner.ptr_idx.checked_add(1) else {
            return false;
        };
        let Ok(binary_index_len) = usize::try_from(self.binary_index_len) else {
            return false;
        };
        if next_idx >= binary_index_len {
            return false;
        }

        self.hi_scanner.ptr_idx = next_idx;
        self.hi_scanner.stack.clear();
        self.hi_scanner.base_key_offset = None;
        self.hi_scanner.base_key_end = None;
        self.fill_stack();

        if self.hi_scanner.stack.is_empty() {
            self.clamp_upper_to_lo();
            return false;
        }

        true
    }
}

impl<Item: Decodable<Parsed>, Parsed: ParsedItem<Item>> Iterator for Decoder<'_, Item, Parsed> {
    type Item = Parsed;

    fn next(&mut self) -> Option<Self::Item> {
        let entries_end = self.entries_end()?;
        if self.lo_scanner.offset >= self.block.data.len() {
            return None;
        }

        if self.hi_scanner.base_key_offset.is_some()
            && self.lo_scanner.offset >= self.hi_scanner.offset
        {
            return None;
        }

        let is_restart: bool = self.lo_scanner.remaining_in_interval == 0;

        let mut reader = Cursor::new(self.block.data.get(self.lo_scanner.offset..)?);

        #[expect(
            clippy::cast_possible_truncation,
            reason = "blocks do not even come close to 4 GiB in size"
        )]
        let item = Self::parse_current_item(
            &mut reader,
            self.lo_scanner.offset,
            self.lo_scanner.base_key_offset,
            self.lo_scanner.base_key_end,
            is_restart,
            entries_end,
        )
        .inspect(|item| {
            self.lo_scanner.offset += reader.position() as usize;

            if is_restart {
                self.lo_scanner.base_key_offset = Some(item.key_offset());
                self.lo_scanner.base_key_end = Some(item.key_end_offset());
            }
        });

        if item.is_some() {
            if is_restart {
                self.lo_scanner.remaining_in_interval = usize::from(self.restart_interval) - 1;
            } else {
                self.lo_scanner.remaining_in_interval -= 1;
            }
        } else {
            self.lo_scanner.offset = self.block.data.len();
            self.lo_scanner.remaining_in_interval = 0;
            self.lo_scanner.base_key_offset = None;
            self.lo_scanner.base_key_end = None;
        }

        item
    }
}

impl<Item: Decodable<Parsed>, Parsed: ParsedItem<Item>> DoubleEndedIterator
    for Decoder<'_, Item, Parsed>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(top) = self.consume_stack_top() {
            return Some(top);
        }

        // NOTE: If we wrapped, we are at the end
        // This is safe to do, because there cannot be that many restart intervals
        if self.hi_scanner.ptr_idx == usize::MAX {
            return None;
        }

        self.hi_scanner.ptr_idx = self.hi_scanner.ptr_idx.wrapping_sub(1);

        // NOTE: If we wrapped, we are at the end
        // This is safe to do, because there cannot be that many restart intervals
        if self.hi_scanner.ptr_idx == usize::MAX {
            return None;
        }

        self.fill_stack();

        self.consume_stack_top()
    }
}

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    reason = "corruption regression tests intentionally mutate encoded bytes and assert parser rejection paths"
)]
mod tests {
    use super::Decoder;
    use crate::{
        table::{
            block::{BlockType, Header, Trailer},
            data_block::DataBlockParsedItem,
            index_block::IndexBlockParsedItem,
            Block, BlockHandle, BlockOffset, DataBlock, IndexBlock, KeyedBlockHandle,
        },
        Checksum, InternalValue,
    };
    use byteorder::{ByteOrder, LittleEndian};

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

    fn binary_index_offset_field_pos(bytes: &[u8]) -> usize {
        let trailer_probe = IndexBlock::new(Block {
            data: bytes.to_vec().into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });
        let trailer_offset = Trailer::new(&trailer_probe.inner).trailer_offset();
        trailer_offset + 1 + 1 + std::mem::size_of::<u32>()
    }

    fn binary_index_len_field_pos(bytes: &[u8]) -> usize {
        let trailer_probe = IndexBlock::new(Block {
            data: bytes.to_vec().into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });
        let trailer_offset = Trailer::new(&trailer_probe.inner).trailer_offset();
        trailer_offset + 1 + 1
    }

    fn binary_index_step_size_field_pos(bytes: &[u8]) -> usize {
        let trailer_probe = IndexBlock::new(Block {
            data: bytes.to_vec().into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });
        let trailer_offset = Trailer::new(&trailer_probe.inner).trailer_offset();
        trailer_offset + 1
    }

    fn hash_index_len_field_pos(bytes: &[u8]) -> usize {
        let trailer_probe = IndexBlock::new(Block {
            data: bytes.to_vec().into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });
        let trailer_offset = Trailer::new(&trailer_probe.inner).trailer_offset();
        trailer_offset + 1 + 1 + (2 * std::mem::size_of::<u32>())
    }

    fn hash_index_offset_field_pos(bytes: &[u8]) -> usize {
        hash_index_len_field_pos(bytes) + std::mem::size_of::<u32>()
    }

    fn first_restart_key_len_field_pos(bytes: &[u8]) -> usize {
        use crate::coding::Decode;
        use crate::table::BlockHandle;
        use byteorder::ReadBytesExt;
        use std::io::Cursor;
        use varint_rs::VarintReader;

        let mut cursor = Cursor::new(bytes);

        let marker = cursor.read_u8().expect("restart head marker");
        assert_eq!(marker, 0, "first entry in index block must be restart");

        let _ = BlockHandle::decode_from(&mut cursor).expect("block handle");
        let _ = cursor.read_u64_varint().expect("seqno");

        usize::try_from(cursor.position()).expect("position should fit usize")
    }

    fn first_truncated_rest_key_len_field_pos(bytes: &[u8]) -> usize {
        use crate::coding::Decode;
        use crate::table::BlockHandle;
        use byteorder::ReadBytesExt;
        use std::io::Cursor;
        use varint_rs::VarintReader;

        let mut cursor = Cursor::new(bytes);

        let marker = cursor.read_u8().expect("restart head marker");
        assert_eq!(marker, 0, "first entry in index block must be restart");

        let _ = BlockHandle::decode_from(&mut cursor).expect("block handle");
        let _ = cursor.read_u64_varint().expect("seqno");
        let key_len = cursor.read_u16_varint().expect("key len");
        cursor.set_position(cursor.position() + u64::from(key_len));

        let truncated_marker = cursor.read_u8().expect("truncated marker");
        assert_eq!(truncated_marker, 1, "second entry should be truncated");

        let _ = cursor.read_u16_varint().expect("shared prefix len");

        usize::try_from(cursor.position()).expect("position should fit usize")
    }

    fn nth_truncated_rest_key_len_field_pos(bytes: &[u8], ordinal: usize) -> usize {
        use crate::coding::Decode;
        use crate::table::BlockHandle;
        use byteorder::ReadBytesExt;
        use std::io::Cursor;
        use varint_rs::VarintReader;

        let mut cursor = Cursor::new(bytes);
        let mut seen = 0usize;

        while usize::try_from(cursor.position()).expect("position should fit usize") < bytes.len() {
            let marker = cursor.read_u8().expect("entry marker");
            match marker {
                0 => {
                    let _ = BlockHandle::decode_from(&mut cursor).expect("block handle");
                    let _ = cursor.read_u64_varint().expect("seqno");
                    let key_len = cursor.read_u16_varint().expect("key len");
                    cursor.set_position(cursor.position() + u64::from(key_len));
                }
                1 => {
                    let _ = BlockHandle::decode_from(&mut cursor).expect("block handle");
                    let _ = cursor.read_u64_varint().expect("seqno");
                    let _ = cursor.read_u16_varint().expect("shared prefix len");
                    let pos =
                        usize::try_from(cursor.position()).expect("position should fit usize");
                    if seen == ordinal {
                        return pos;
                    }
                    seen += 1;
                    let rest_key_len = cursor.read_u16_varint().expect("rest key len");
                    cursor.set_position(cursor.position() + u64::from(rest_key_len));
                }
                crate::table::block::TRAILER_START_MARKER => break,
                _ => panic!("unexpected entry marker in fixture"),
            }
        }

        panic!("truncated entry ordinal out of range");
    }

    #[test]
    fn entries_end_rejects_binary_index_offset_past_block_end() {
        let handles = make_handles(8);
        let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 8).unwrap();
        let binary_index_offset_pos = binary_index_offset_field_pos(&bytes);
        let invalid_binary_index_offset = (bytes.len() as u32).saturating_add(1);
        LittleEndian::write_u32(
            &mut bytes
                [binary_index_offset_pos..binary_index_offset_pos + std::mem::size_of::<u32>()],
            invalid_binary_index_offset,
        );

        let block = Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        };
        assert!(
            matches!(
                Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::try_new(&block),
                Err(crate::Error::InvalidTrailer)
            ),
            "bogus binary_index_offset must be rejected by try_new",
        );
    }

    #[test]
    fn entries_end_rejects_zero_binary_index_offset() {
        let handles = make_handles(8);
        let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 8).unwrap();
        let binary_index_offset_pos = binary_index_offset_field_pos(&bytes);
        LittleEndian::write_u32(
            &mut bytes
                [binary_index_offset_pos..binary_index_offset_pos + std::mem::size_of::<u32>()],
            0,
        );

        let block = Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        };
        assert!(
            matches!(
                Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::try_new(&block),
                Err(crate::Error::InvalidTrailer)
            ),
            "zero binary_index_offset must be rejected as InvalidTrailer",
        );
    }

    #[test]
    fn entries_end_rejects_when_marker_before_binary_index_is_missing() {
        let handles = make_handles(8);
        let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 8).unwrap();
        let binary_index_offset_pos = binary_index_offset_field_pos(&bytes);
        let binary_index_offset = LittleEndian::read_u32(
            &bytes[binary_index_offset_pos..binary_index_offset_pos + std::mem::size_of::<u32>()],
        ) as usize;
        bytes[binary_index_offset - 1] = 0;

        let block = Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        };
        assert!(
            matches!(
                Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::try_new(&block),
                Err(crate::Error::InvalidTrailer)
            ),
            "missing marker before binary index must be rejected as InvalidTrailer",
        );
    }

    #[test]
    fn seek_rejects_binary_index_slice_past_block_end() {
        let handles = make_handles(8);
        let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 8).unwrap();
        let binary_index_len_pos = binary_index_len_field_pos(&bytes);
        let current_binary_index_len = LittleEndian::read_u32(
            &bytes[binary_index_len_pos..binary_index_len_pos + std::mem::size_of::<u32>()],
        );
        let inflated_binary_index_len = current_binary_index_len.saturating_add(10_000);
        LittleEndian::write_u32(
            &mut bytes[binary_index_len_pos..binary_index_len_pos + std::mem::size_of::<u32>()],
            inflated_binary_index_len,
        );

        let block = Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        };
        assert!(
            matches!(
                Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::try_new(&block),
                Err(crate::Error::InvalidTrailer)
            ),
            "binary index slice past block end must be rejected as InvalidTrailer",
        );
    }

    #[test]
    fn seek_rejects_binary_index_slice_spilling_into_trailer() {
        let handles = make_handles(8);
        let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 8).unwrap();
        let binary_index_len_pos = binary_index_len_field_pos(&bytes);
        let binary_index_offset_pos = binary_index_offset_field_pos(&bytes);
        let binary_index_step_size_pos = binary_index_step_size_field_pos(&bytes);
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

        let binary_index_offset = LittleEndian::read_u32(
            &bytes[binary_index_offset_pos..binary_index_offset_pos + std::mem::size_of::<u32>()],
        ) as usize;
        let step_size = usize::from(bytes[binary_index_step_size_pos]);
        let entries_before_trailer = trailer_offset - binary_index_offset;
        let inflated_binary_index_len = u32::try_from((entries_before_trailer / step_size) + 1)
            .expect("test fixture expects u32 binary index len");
        LittleEndian::write_u32(
            &mut bytes[binary_index_len_pos..binary_index_len_pos + std::mem::size_of::<u32>()],
            inflated_binary_index_len,
        );

        let block = Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        };
        assert!(
            matches!(
                Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::try_new(&block),
                Err(crate::Error::InvalidTrailer)
            ),
            "binary index slice spilling into trailer must be rejected as InvalidTrailer",
        );
    }

    #[test]
    fn seek_rejects_binary_index_slice_shorter_than_metadata_boundary() {
        let handles = make_handles(8);
        let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 8).unwrap();
        let binary_index_len_pos = binary_index_len_field_pos(&bytes);
        let current_binary_index_len = LittleEndian::read_u32(
            &bytes[binary_index_len_pos..binary_index_len_pos + std::mem::size_of::<u32>()],
        );
        let shortened_binary_index_len = current_binary_index_len.saturating_sub(1);
        LittleEndian::write_u32(
            &mut bytes[binary_index_len_pos..binary_index_len_pos + std::mem::size_of::<u32>()],
            shortened_binary_index_len,
        );

        let block = Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        };
        assert!(
            matches!(
                Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::try_new(&block),
                Err(crate::Error::InvalidTrailer)
            ),
            "binary index slice shorter than metadata boundary must be rejected as InvalidTrailer",
        );
    }

    #[test]
    fn seek_rejects_restart_head_key_crossing_entries_end() {
        let handles = make_handles(8);
        let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 8).unwrap();

        let binary_index_offset_pos = binary_index_offset_field_pos(&bytes);
        let binary_index_offset = LittleEndian::read_u32(
            &bytes[binary_index_offset_pos..binary_index_offset_pos + std::mem::size_of::<u32>()],
        ) as usize;
        let entries_end = binary_index_offset - 1;

        let key_len_pos = first_restart_key_len_field_pos(&bytes);
        let key_start = key_len_pos + 1;
        let overlapping_key_len = u8::try_from(entries_end - key_start + 1)
            .expect("test fixture expects one-byte key varint");
        bytes[key_len_pos] = overlapping_key_len;

        let block = Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        };
        let mut decoder = Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::new(&block);

        assert!(!decoder.seek(|_, _| true, false));
    }

    #[test]
    fn advance_while_respects_active_upper_bound() {
        let handles = make_handles(16);
        let bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 4).unwrap();
        let block = Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        };
        let mut decoder = Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::new(&block);
        decoder.hi_scanner.offset = 1;
        decoder.hi_scanner.base_key_offset = Some(0);
        decoder.hi_scanner.base_key_end = Some(0);
        let upper_bound = decoder.hi_scanner.offset;

        decoder.advance_while(|_, _| true);

        assert!(
            decoder.lo_scanner.offset <= upper_bound,
            "advance_while should not move beyond active upper bound"
        );
    }

    #[test]
    fn fill_stack_clears_partial_interval_when_truncated_parse_fails() {
        let handles = make_handles(8);
        let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 8).unwrap();
        let rest_key_len_pos = first_truncated_rest_key_len_field_pos(&bytes);
        bytes[rest_key_len_pos] = 0xFF;
        bytes[rest_key_len_pos + 1] = 0xFF;
        bytes[rest_key_len_pos + 2] = 0x03;

        let block = Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        };
        let mut decoder = Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::new(&block);
        decoder.hi_scanner.ptr_idx = 0;
        decoder.hi_scanner.stack.clear();
        decoder.hi_scanner.base_key_offset = None;
        decoder.hi_scanner.base_key_end = None;

        decoder.fill_stack();

        assert!(
            decoder.hi_scanner.stack.is_empty(),
            "partial reverse interval must be discarded on parse failure"
        );
        // poison_back_cursor delegates to clamp_upper_to_lo which preserves
        // the hard-stop sentinel as Some(0) so next() also stops.
        assert_eq!(decoder.hi_scanner.base_key_offset, Some(0));
        assert_eq!(decoder.hi_scanner.base_key_end, Some(0));
    }

    #[test]
    fn seek_upper_fails_closed_when_selected_interval_is_corrupted() {
        let handles = make_handles(8);
        let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 8).unwrap();
        let rest_key_len_pos = first_truncated_rest_key_len_field_pos(&bytes);
        bytes[rest_key_len_pos] = 0xFF;
        bytes[rest_key_len_pos + 1] = 0xFF;
        bytes[rest_key_len_pos + 2] = 0x03;

        let block = Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        };
        let mut decoder = Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::new(&block);

        assert!(
            !decoder.seek_upper(|_, _| false, false),
            "seek_upper should fail when the selected upper interval is malformed"
        );
        assert_eq!(
            decoder.hi_scanner.offset, decoder.lo_scanner.offset,
            "failed upper seek must clamp the forward bound to current lo offset"
        );
        assert_eq!(decoder.hi_scanner.base_key_offset, Some(0));
        assert_eq!(decoder.hi_scanner.base_key_end, Some(0));
        assert!(
            decoder.next().is_none(),
            "zero-width upper cursor must prevent fail-open forward scans"
        );
    }

    #[test]
    fn seek_exhausts_existing_cursor_when_binary_search_fails() {
        let handles = make_handles(16);
        let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 2).unwrap();
        let binary_index_offset_pos = binary_index_offset_field_pos(&bytes);
        let binary_index_len_pos = binary_index_len_field_pos(&bytes);
        let binary_index_step_size_pos = binary_index_step_size_field_pos(&bytes);
        let binary_index_offset = LittleEndian::read_u32(
            &bytes[binary_index_offset_pos..binary_index_offset_pos + std::mem::size_of::<u32>()],
        ) as usize;
        let binary_index_len = LittleEndian::read_u32(
            &bytes[binary_index_len_pos..binary_index_len_pos + std::mem::size_of::<u32>()],
        ) as usize;
        let binary_index_step_size = usize::from(bytes[binary_index_step_size_pos]);
        let mid = binary_index_len / 2;
        let mid_offset = binary_index_offset + (mid * binary_index_step_size);
        let trailer_start = binary_index_offset - 1;
        #[expect(
            clippy::cast_possible_truncation,
            reason = "test fixture keeps trailer_start within u32"
        )]
        let trailer_start_u32 = trailer_start as u32;
        match binary_index_step_size {
            2 => {
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "u16 step stores test offset"
                )]
                let trailer_start_u16 = trailer_start_u32 as u16;
                LittleEndian::write_u16(
                    &mut bytes[mid_offset..mid_offset + std::mem::size_of::<u16>()],
                    trailer_start_u16,
                );
            }
            4 => {
                LittleEndian::write_u32(
                    &mut bytes[mid_offset..mid_offset + std::mem::size_of::<u32>()],
                    trailer_start_u32,
                );
            }
            _ => panic!("unexpected binary index step size"),
        }

        let block = Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        };
        let mut decoder = Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::new(&block);

        decoder.lo_scanner.offset = 0;
        decoder.lo_scanner.remaining_in_interval = 0;
        decoder.lo_scanner.base_key_offset = None;
        decoder.lo_scanner.base_key_end = None;

        assert!(
            !decoder.seek(|_, _| false, false),
            "seek should fail when binary-index search probes a malformed restart head"
        );
        assert!(
            decoder.next().is_none(),
            "failed seek must exhaust the old forward cursor state"
        );
    }

    #[test]
    fn seek_upper_exhausts_existing_cursor_when_binary_search_fails() {
        let handles = make_handles(16);
        let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 2).unwrap();
        let binary_index_offset_pos = binary_index_offset_field_pos(&bytes);
        let binary_index_len_pos = binary_index_len_field_pos(&bytes);
        let binary_index_step_size_pos = binary_index_step_size_field_pos(&bytes);
        let binary_index_offset = LittleEndian::read_u32(
            &bytes[binary_index_offset_pos..binary_index_offset_pos + std::mem::size_of::<u32>()],
        ) as usize;
        let binary_index_len = LittleEndian::read_u32(
            &bytes[binary_index_len_pos..binary_index_len_pos + std::mem::size_of::<u32>()],
        ) as usize;
        let binary_index_step_size = usize::from(bytes[binary_index_step_size_pos]);
        let mid = binary_index_len / 2;
        let mid_offset = binary_index_offset + (mid * binary_index_step_size);
        let trailer_start = binary_index_offset - 1;
        #[expect(
            clippy::cast_possible_truncation,
            reason = "test fixture keeps trailer_start within u32"
        )]
        let trailer_start_u32 = trailer_start as u32;
        match binary_index_step_size {
            2 => {
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "u16 step stores test offset"
                )]
                let trailer_start_u16 = trailer_start_u32 as u16;
                LittleEndian::write_u16(
                    &mut bytes[mid_offset..mid_offset + std::mem::size_of::<u16>()],
                    trailer_start_u16,
                );
            }
            4 => {
                LittleEndian::write_u32(
                    &mut bytes[mid_offset..mid_offset + std::mem::size_of::<u32>()],
                    trailer_start_u32,
                );
            }
            _ => panic!("unexpected binary index step size"),
        }

        let block = Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        };
        let mut decoder = Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::new(&block);

        decoder.hi_scanner.offset = 0;
        decoder.hi_scanner.ptr_idx = 0;
        decoder.hi_scanner.stack.push(0);
        decoder.hi_scanner.base_key_offset = None;
        decoder.hi_scanner.base_key_end = None;

        assert!(
            !decoder.seek_upper(|_, _| false, false),
            "seek_upper should fail when binary-index search probes a malformed restart head"
        );
        assert!(
            decoder.next_back().is_none(),
            "failed seek_upper must exhaust the old reverse cursor state"
        );
    }

    #[test]
    fn advance_upper_restart_interval_preserves_hard_upper_bound_on_corruption() {
        let handles = make_handles(16);
        let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 2).unwrap();
        let second_interval_rest_key_len_pos = nth_truncated_rest_key_len_field_pos(&bytes, 1);
        bytes[second_interval_rest_key_len_pos] = 0xFF;
        bytes[second_interval_rest_key_len_pos + 1] = 0xFF;
        bytes[second_interval_rest_key_len_pos + 2] = 0x03;

        let block = Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        };
        let mut decoder = Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::new(&block);
        decoder.hi_scanner.ptr_idx = 0;
        decoder.hi_scanner.stack.clear();
        decoder.hi_scanner.base_key_offset = None;
        decoder.hi_scanner.base_key_end = None;
        decoder.fill_stack();
        assert!(
            !decoder.hi_scanner.stack.is_empty(),
            "fixture should seed initial upper interval"
        );

        decoder.lo_scanner.offset = 0;
        decoder.lo_scanner.remaining_in_interval = 0;
        decoder.lo_scanner.base_key_offset = None;
        decoder.lo_scanner.base_key_end = None;

        assert!(
            !decoder.advance_upper_restart_interval(),
            "advancing into a malformed interval must fail"
        );
        assert_eq!(
            decoder.hi_scanner.base_key_offset,
            Some(0),
            "failed advance must keep an active hard upper bound"
        );
        assert_eq!(decoder.hi_scanner.base_key_end, Some(0));
        assert!(
            decoder.next().is_none(),
            "forward scan must not continue past a failed upper-bound advance"
        );
    }

    #[test]
    fn poison_back_cursor_also_stops_forward_next() {
        let handles = make_handles(8);
        let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 4).unwrap();

        // Corrupt the second interval's truncated rest_key_len so fill_stack fails
        let rest_key_pos = first_truncated_rest_key_len_field_pos(&bytes);
        bytes[rest_key_pos] = 0xFF;
        bytes[rest_key_pos + 1] = 0xFF;
        bytes[rest_key_pos + 2] = 0x03;

        let block = Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        };
        let mut decoder = Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::new(&block);

        // Seed the hi_scanner at interval 0 so next_back can try to advance
        decoder.hi_scanner.ptr_idx = 0;
        decoder.hi_scanner.stack.clear();
        decoder.hi_scanner.base_key_offset = None;
        decoder.hi_scanner.base_key_end = None;
        decoder.fill_stack();

        // Consume the stack, then next_back tries the previous interval which
        // is corrupted — fill_stack calls poison_back_cursor
        while decoder.consume_stack_top().is_some() {}
        let back = decoder.next_back();
        assert!(
            back.is_none(),
            "next_back must return None on corrupted interval"
        );

        // The critical check: next() must ALSO stop — poison_back_cursor
        // now clamps the upper bound so forward iteration cannot continue
        assert!(
            decoder.next().is_none(),
            "next() must not yield items after poison_back_cursor clamped the upper bound"
        );
    }

    #[test]
    fn binary_index_bounds_accepts_data_block_with_hash_index() {
        let items = [
            InternalValue::from_components(b"a", b"a", 3, crate::ValueType::Value),
            InternalValue::from_components(b"b", b"b", 2, crate::ValueType::Value),
            InternalValue::from_components(b"c", b"c", 1, crate::ValueType::Value),
            InternalValue::from_components(b"d", b"d", 0, crate::ValueType::Value),
        ];
        let bytes = DataBlock::encode_into_vec(&items, 1, 1.33).expect("encode data block");
        let block = Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        };

        let trailer_offset = Trailer::new(&block).trailer_offset();
        let hash_index_offset_pos =
            trailer_offset + 1 + 1 + (2 * std::mem::size_of::<u32>()) + std::mem::size_of::<u32>();
        let hash_index_offset = LittleEndian::read_u32(
            &block.data[hash_index_offset_pos..hash_index_offset_pos + std::mem::size_of::<u32>()],
        );
        assert!(hash_index_offset > 0, "fixture must encode a hash index");

        let mut decoder = Decoder::<InternalValue, DataBlockParsedItem>::new(&block);
        assert!(decoder.binary_index_bounds().is_some());
        assert!(decoder.seek(|_, _| true, false));
    }

    #[test]
    fn binary_index_bounds_rejects_hash_index_spilling_past_trailer() {
        let items = [
            InternalValue::from_components(b"a", b"a", 3, crate::ValueType::Value),
            InternalValue::from_components(b"b", b"b", 2, crate::ValueType::Value),
            InternalValue::from_components(b"c", b"c", 1, crate::ValueType::Value),
            InternalValue::from_components(b"d", b"d", 0, crate::ValueType::Value),
        ];
        let mut bytes = DataBlock::encode_into_vec(&items, 1, 1.33).expect("encode data block");
        let hash_index_offset_pos = hash_index_offset_field_pos(&bytes);
        let hash_index_offset = LittleEndian::read_u32(
            &bytes[hash_index_offset_pos..hash_index_offset_pos + std::mem::size_of::<u32>()],
        );
        assert!(hash_index_offset > 0, "fixture must encode a hash index");

        let hash_index_len_pos = hash_index_len_field_pos(&bytes);
        LittleEndian::write_u32(
            &mut bytes[hash_index_len_pos..hash_index_len_pos + std::mem::size_of::<u32>()],
            u32::MAX,
        );

        let block = Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        };
        assert!(
            matches!(
                Decoder::<InternalValue, DataBlockParsedItem>::try_new(&block),
                Err(crate::Error::InvalidTrailer)
            ),
            "hash index spilling past trailer must be rejected as InvalidTrailer",
        );
    }

    #[test]
    fn binary_index_bounds_rejects_hash_index_ending_before_trailer() {
        let items = [
            InternalValue::from_components(b"a", b"a", 3, crate::ValueType::Value),
            InternalValue::from_components(b"b", b"b", 2, crate::ValueType::Value),
            InternalValue::from_components(b"c", b"c", 1, crate::ValueType::Value),
            InternalValue::from_components(b"d", b"d", 0, crate::ValueType::Value),
        ];
        let mut bytes = DataBlock::encode_into_vec(&items, 1, 1.33).expect("encode data block");
        let hash_index_len_pos = hash_index_len_field_pos(&bytes);
        let hash_index_len = LittleEndian::read_u32(
            &bytes[hash_index_len_pos..hash_index_len_pos + std::mem::size_of::<u32>()],
        );
        assert!(
            hash_index_len > 0,
            "fixture must encode a non-empty hash index"
        );
        LittleEndian::write_u32(
            &mut bytes[hash_index_len_pos..hash_index_len_pos + std::mem::size_of::<u32>()],
            hash_index_len - 1,
        );

        let block = Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        };
        assert!(
            matches!(
                Decoder::<InternalValue, DataBlockParsedItem>::try_new(&block),
                Err(crate::Error::InvalidTrailer)
            ),
            "hash index ending before trailer must be rejected as InvalidTrailer",
        );
    }

    #[test]
    fn binary_index_bounds_rejects_zero_length() {
        let handles = make_handles(4);
        let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 2).unwrap();

        // Zero out binary_index_len in the trailer
        let bi_len_pos = binary_index_len_field_pos(&bytes);
        LittleEndian::write_u32(
            &mut bytes[bi_len_pos..bi_len_pos + std::mem::size_of::<u32>()],
            0,
        );

        let block = Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        };
        assert!(
            matches!(
                Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::try_new(&block),
                Err(crate::Error::InvalidTrailer)
            ),
            "zero-length binary index must be rejected as InvalidTrailer",
        );
    }

    #[test]
    fn seek_rejects_eof_binary_index_offset() {
        let handles = make_handles(4);
        let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 2).unwrap();

        // Find the binary index region and tamper the first entry to point at data.len()
        let bi_offset_pos = binary_index_offset_field_pos(&bytes);
        let bi_len_pos = binary_index_len_field_pos(&bytes);
        let bi_step_pos = binary_index_step_size_field_pos(&bytes);
        let bi_offset = LittleEndian::read_u32(
            &bytes[bi_offset_pos..bi_offset_pos + std::mem::size_of::<u32>()],
        ) as usize;
        let step = usize::from(bytes[bi_step_pos]);
        let bi_len =
            LittleEndian::read_u32(&bytes[bi_len_pos..bi_len_pos + std::mem::size_of::<u32>()])
                as usize;

        // Write data.len() into every binary index slot so seek lands at EOF
        let data_len = bytes.len();
        for i in 0..bi_len {
            let slot = bi_offset + i * step;
            match step {
                2 => {
                    #[expect(clippy::cast_possible_truncation, reason = "test block is small")]
                    let val = data_len as u16;
                    LittleEndian::write_u16(&mut bytes[slot..slot + 2], val);
                }
                4 => {
                    #[expect(clippy::cast_possible_truncation, reason = "test block is small")]
                    let val = data_len as u32;
                    LittleEndian::write_u32(&mut bytes[slot..slot + 4], val);
                }
                _ => panic!("unexpected step size"),
            }
        }

        let block = Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        };
        let mut decoder = Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::new(&block);

        // Must not panic — reader_at rejects EOF offsets
        let result = decoder.seek(
            |key, _| key >= b"adj:out:vertex-0001:edge-0002".as_slice(),
            false,
        );
        assert!(
            !result,
            "seek must fail when every binary index entry points at data.len()"
        );
    }

    #[test]
    fn try_new_zero_restart_interval_returns_invalid_trailer() {
        let handles = make_handles(4);
        let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 2).unwrap();

        // Locate trailer and zero out restart_interval (first trailer byte).
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

        let corrupt_block = Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        };

        assert!(
            matches!(
                Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::try_new(&corrupt_block),
                Err(crate::Error::InvalidTrailer)
            ),
            "zero restart_interval must return InvalidTrailer",
        );
    }

    #[test]
    fn try_new_invalid_binary_index_step_size_returns_invalid_trailer() {
        let handles = make_handles(4);
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
        // Corrupt binary_index_step_size (second trailer byte) to an invalid value.
        bytes[trailer_offset + 1] = 3;

        let corrupt_block = Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        };

        assert!(
            matches!(
                Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::try_new(&corrupt_block),
                Err(crate::Error::InvalidTrailer)
            ),
            "invalid step size must return InvalidTrailer",
        );
    }

    #[test]
    #[should_panic(expected = "valid block trailer")]
    fn new_panics_on_zero_restart_interval() {
        let handles = make_handles(4);
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

        let corrupt_block = Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        };

        // Must panic, not silently succeed.
        let _ = Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::new(&corrupt_block);
    }
}

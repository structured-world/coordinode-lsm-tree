// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

mod iter;

#[cfg(test)]
mod iter_test;

pub use iter::Iter;

use super::block::{
    Block, Decodable, Decoder, Encodable, Encoder, ParsedItem, TRAILER_START_MARKER, Trailer,
    decoder::read_leb128, hash_index::Reader as HashIndexReader,
};
use crate::io::Cursor;
#[cfg(not(feature = "std"))]
use crate::io::VarintWriter;
use crate::io::WriteBytesExt;
use crate::io::{LittleEndian, ReadBytesExt};
use crate::key::InternalKey;
use crate::table::block::hash_index::{MARKER_CONFLICT, MARKER_FREE};
use crate::table::util::{SliceIndexes, compare_prefixed_slice};
use crate::{InternalValue, SeqNo, Slice, ValueType};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
#[cfg(feature = "std")]
use varint_rs::VarintWriter;

impl Decodable<DataBlockParsedItem> for InternalValue {
    fn parse_restart_key<'a>(
        reader: &mut Cursor<&[u8]>,
        offset: usize,
        data: &'a [u8],
        entries_end: usize,
    ) -> Option<(&'a [u8], SeqNo)> {
        // Slice-based decode (see `parse_full` / `read_leb128`).
        let buf: &[u8] = reader.get_ref();
        let mut pos = usize::try_from(reader.position()).ok()?;

        let value_type = *buf.get(pos)?;
        pos += 1;
        if value_type == TRAILER_START_MARKER {
            return None;
        }

        let (seqno, np) = read_leb128!(buf, pos);
        pos = np;

        let (key_len_raw, np) = read_leb128!(buf, pos);
        pos = np;
        let key_len = usize::try_from(key_len_raw)
            .ok()
            .filter(|&k| u16::try_from(k).is_ok())?;
        let key_start = offset.checked_add(pos)?;
        let key_end = key_start.checked_add(key_len)?;
        if key_end > entries_end {
            return None;
        }
        pos = pos.checked_add(key_len)?;

        let key = data.get(key_start..key_end)?;
        reader.set_position(pos as u64);
        Some((key, seqno))
    }

    fn parse_full(
        reader: &mut Cursor<&[u8]>,
        offset: usize,
        entries_end: usize,
    ) -> Option<DataBlockParsedItem> {
        // Slice-based decode: read from the cursor's buffer through a local
        // index instead of `Cursor::read_u8` per byte; advance the cursor once
        // at the end. See `read_leb128`.
        let buf: &[u8] = reader.get_ref();
        let mut pos = usize::try_from(reader.position()).ok()?;

        let value_type = *buf.get(pos)?;
        pos += 1;
        if value_type == TRAILER_START_MARKER {
            return None;
        }

        let value_type = ValueType::try_from(value_type).ok()?;

        let (seqno, np) = read_leb128!(buf, pos);
        pos = np;

        let (key_len_raw, np) = read_leb128!(buf, pos);
        pos = np;
        // key_len is encoded as a u16 varint on the wire.
        let key_len = usize::try_from(key_len_raw)
            .ok()
            .filter(|&k| u16::try_from(k).is_ok())?;
        let key_start = offset.checked_add(pos)?;
        if key_start > entries_end {
            return None;
        }
        let key_end = key_start.checked_add(key_len)?;
        if key_end > entries_end {
            return None;
        }
        pos = pos.checked_add(key_len)?;

        let is_value = !value_type.is_tombstone();

        let val_len: usize = if is_value {
            let (val_len_raw, np) = read_leb128!(buf, pos);
            pos = np;
            // val_len is encoded as a u32 varint on the wire.
            usize::try_from(val_len_raw)
                .ok()
                .filter(|&v| u32::try_from(v).is_ok())?
        } else {
            0
        };
        let val_offset = offset.checked_add(pos)?;
        if val_offset > entries_end {
            return None;
        }
        let val_end = val_offset.checked_add(val_len)?;
        if val_end > entries_end {
            return None;
        }
        pos = pos.checked_add(val_len)?;
        reader.set_position(pos as u64);

        Some(if is_value {
            DataBlockParsedItem {
                value_type,
                seqno,
                prefix: None,
                key: SliceIndexes(key_start, key_end),
                value: Some(SliceIndexes(val_offset, val_end)),
            }
        } else {
            DataBlockParsedItem {
                value_type,
                seqno,
                prefix: None,
                key: SliceIndexes(key_start, key_end),
                value: None, // TODO: enum value/tombstone, so value is not Option for values
            }
        })
    }

    fn parse_truncated(
        reader: &mut Cursor<&[u8]>,
        offset: usize,
        base_key_offset: usize,
        base_key_end: usize,
        entries_end: usize,
    ) -> Option<DataBlockParsedItem> {
        // Slice-based decode (see `parse_full` / `read_leb128`).
        let buf: &[u8] = reader.get_ref();
        let mut pos = usize::try_from(reader.position()).ok()?;

        let value_type = *buf.get(pos)?;
        pos += 1;
        if value_type == TRAILER_START_MARKER {
            return None;
        }
        let value_type = ValueType::try_from(value_type).ok()?;

        let (seqno, np) = read_leb128!(buf, pos);
        pos = np;

        let (shared_raw, np) = read_leb128!(buf, pos);
        pos = np;
        let shared_prefix_len = usize::try_from(shared_raw)
            .ok()
            .filter(|&k| u16::try_from(k).is_ok())?;
        let (rest_raw, np) = read_leb128!(buf, pos);
        pos = np;
        let rest_key_len = usize::try_from(rest_raw)
            .ok()
            .filter(|&k| u16::try_from(k).is_ok())?;

        if base_key_end < base_key_offset || base_key_end > offset {
            return None;
        }
        // base_key_end is the byte offset where the restart head's key ends.
        // (base_key_end - base_key_offset) == restart_key_len, so this check
        // rejects shared_prefix_len > restart_key_len.
        let prefix_end = base_key_offset.checked_add(shared_prefix_len)?;
        if prefix_end > base_key_end {
            return None;
        }

        let key_offset = offset.checked_add(pos)?;
        if key_offset > entries_end {
            return None;
        }
        let key_end = key_offset.checked_add(rest_key_len)?;
        if key_end > entries_end {
            return None;
        }
        pos = pos.checked_add(rest_key_len)?;

        let is_value = !value_type.is_tombstone();

        let val_len: usize = if is_value {
            let (val_len_raw, np) = read_leb128!(buf, pos);
            pos = np;
            usize::try_from(val_len_raw)
                .ok()
                .filter(|&v| u32::try_from(v).is_ok())?
        } else {
            0
        };
        let val_offset = offset.checked_add(pos)?;
        if val_offset > entries_end {
            return None;
        }
        let val_end = val_offset.checked_add(val_len)?;
        if val_end > entries_end {
            return None;
        }
        pos = pos.checked_add(val_len)?;
        reader.set_position(pos as u64);

        Some(if is_value {
            DataBlockParsedItem {
                value_type,
                seqno,
                prefix: Some(SliceIndexes(base_key_offset, prefix_end)),
                key: SliceIndexes(key_offset, key_end),
                value: Some(SliceIndexes(val_offset, val_end)),
            }
        } else {
            DataBlockParsedItem {
                value_type,
                seqno,
                prefix: Some(SliceIndexes(base_key_offset, prefix_end)),
                key: SliceIndexes(key_offset, key_end),
                value: None,
            }
        })
    }
}

impl Encodable<()> for InternalValue {
    fn encode_full_into<W: crate::io::Write>(
        &self,
        writer: &mut W,
        _state: &mut (),
    ) -> crate::Result<()> {
        // We encode restart markers as:
        // [value type] [seqno] [user key len] [user key] [value len] [value]
        // 1            2       3              4          5?           6?

        writer.write_u8(u8::from(self.key.value_type))?; // 1
        writer.write_u64_varint(self.key.seqno)?; // 2

        #[expect(clippy::cast_possible_truncation, reason = "keys are u16 length max")]
        writer.write_u16_varint(self.key.user_key.len() as u16)?; // 3
        writer.write_all(&self.key.user_key)?; // 4

        // NOTE: Only write value len + value if we are actually a value
        if !self.is_tombstone() {
            #[expect(clippy::cast_possible_truncation, reason = "values are u32 length max")]
            writer.write_u32_varint(self.value.len() as u32)?; // 5
            writer.write_all(&self.value)?; // 6
        }

        Ok(())
    }

    fn encode_truncated_into<W: crate::io::Write>(
        &self,
        writer: &mut W,
        _state: &mut (),
        shared_len: usize,
    ) -> crate::Result<()> {
        // We encode truncated values as:
        // [value type] [seqno] [shared prefix len] [rest key len] [rest key] [value len] [value]
        // 1            2       3                   4              5          6?          7?

        writer.write_u8(u8::from(self.key.value_type))?; // 1
        writer.write_u64_varint(self.key.seqno)?; // 2

        // TODO: maybe we can skip this varint altogether if prefix truncation = false

        #[expect(clippy::cast_possible_truncation, reason = "keys are u16 length max")]
        writer.write_u16_varint(shared_len as u16)?; // 3

        let rest_len = self.key().len() - shared_len;

        #[expect(clippy::cast_possible_truncation, reason = "keys are u16 length max")]
        writer.write_u16_varint(rest_len as u16)?; // 4

        #[expect(
            clippy::expect_used,
            reason = "the shared len should not be greater than key length"
        )]
        let truncated_user_key = self
            .key
            .user_key
            .get(shared_len..)
            .expect("should be in bounds");

        writer.write_all(truncated_user_key)?; // 5

        // NOTE: Only write value len + value if we are actually a value
        if !self.is_tombstone() {
            #[expect(clippy::cast_possible_truncation, reason = "values are u32 length max")]
            writer.write_u32_varint(self.value.len() as u32)?; // 6
            writer.write_all(&self.value)?; // 7
        }

        Ok(())
    }

    fn key(&self) -> &[u8] {
        &self.key.user_key
    }
}

#[derive(Debug)]
pub struct DataBlockParsedItem {
    pub value_type: ValueType,
    pub seqno: SeqNo,
    pub prefix: Option<SliceIndexes>,
    pub key: SliceIndexes,
    pub value: Option<SliceIndexes>,
}

impl ParsedItem<InternalValue> for DataBlockParsedItem {
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
            let rest_key = unsafe { bytes.get_unchecked(self.key.0..self.key.1) };
            compare_prefixed_slice(prefix, rest_key, needle, cmp)
        } else {
            // The no-prefix branch has no allocation to avoid — `compare()` on
            // a contiguous slice is already optimal for any comparator. Adding
            // an `is_lexicographic()` short-circuit here costs custom
            // comparators an extra `dyn` vtable call per linear-scan step
            // without any matching saving on the default path (where `<[u8]>::cmp`
            // and `DefaultUserComparator::compare` lower to the same memcmp).
            // The lex fast path is intentionally kept ONLY on the prefix branch
            // above (where `compare_prefixed_slice_lexicographic` avoids the
            // prefix+suffix concatenation allocation) and at the binary-search
            // predicate construction sites in `iter.rs` (where one
            // `is_lexicographic()` call is hoisted to amortise across all
            // O(log restarts) BS probes).
            let key = unsafe { bytes.get_unchecked(self.key.0..self.key.1) };
            cmp.compare(key, needle)
        }
    }

    fn seqno(&self) -> SeqNo {
        self.seqno
    }

    fn key_offset(&self) -> usize {
        self.key.0
    }

    fn key_end_offset(&self) -> usize {
        self.key.1
    }

    fn materialize(&self, bytes: &Slice) -> InternalValue {
        // NOTE: We consider the prefix and key slice indexes to be trustworthy
        #[expect(clippy::indexing_slicing)]
        let key = if let Some(prefix) = &self.prefix {
            let prefix_key = &bytes[prefix.0..prefix.1];
            let rest_key = &bytes[self.key.0..self.key.1];
            Slice::fused(prefix_key, rest_key)
        } else {
            bytes.slice(self.key.0..self.key.1)
        };

        let key = InternalKey::new(key, self.seqno, self.value_type);

        let value = self
            .value
            .as_ref()
            .map_or_else(Slice::empty, |v| bytes.slice(v.0..v.1));

        InternalValue { key, value }
    }
}

// TODO: allow disabling binary index (for meta block)
// -> saves space in metadata blocks
// -> point reads then need to use iter().find() to find stuff (which is fine)
// see https://github.com/fjall-rs/lsm-tree/issues/185

/// Block that contains key-value pairs (user data)
#[derive(Clone)]
pub struct DataBlock {
    pub inner: Block,
}

/// Seqno-aware binary search positioning the decoder's forward cursor at the
/// last restart interval whose head key is below `needle` (or equal with a
/// seqno at least the snapshot boundary). Borrows the comparator: the lex fast
/// path is statically dispatched and inlinable, the custom-comparator branch
/// keeps the `dyn UserComparator::compare` call. Mirrors
/// [`Iter::seek_to_key_seqno`] but drives the decoder directly.
fn seek_data_block(
    decoder: &mut Decoder<'_, InternalValue, DataBlockParsedItem>,
    needle: &[u8],
    seqno: SeqNo,
    comparator: &crate::comparator::SharedComparator,
) -> bool {
    if comparator.is_lexicographic() {
        decoder.seek(
            |head_key, head_seqno| match head_key.cmp(needle) {
                core::cmp::Ordering::Less => true,
                core::cmp::Ordering::Equal => head_seqno >= seqno,
                core::cmp::Ordering::Greater => false,
            },
            false,
        )
    } else {
        decoder.seek(
            |head_key, head_seqno| match comparator.compare(head_key, needle) {
                core::cmp::Ordering::Less => true,
                core::cmp::Ordering::Equal => head_seqno >= seqno,
                core::cmp::Ordering::Greater => false,
            },
            false,
        )
    }
}

impl DataBlock {
    /// Interprets a block as a data block.
    ///
    /// The caller needs to make sure the block is actually a data block
    /// (e.g. by checking the block type, this is typically done in the `load_block` routine)
    #[must_use]
    pub fn new(inner: Block) -> Self {
        Self { inner }
    }

    /// Builds a `DataBlock` from a freshly loaded block, transparently
    /// stripping the per-KV checksum footer when the SST carries one.
    ///
    /// `has_kv_footer` comes from the per-SST descriptor
    /// (`ParsedMeta::kv_checksum_algo.is_some()`), NOT from a per-block header
    /// flag: data blocks omit the `block_flags` byte (it lives in the
    /// descriptor), so the caller supplies the table-wide footer state. When
    /// `false` this is identical to [`Self::new`]; when `true` it splits the
    /// footer off and stores only the inner standard data-block slice, so
    /// every downstream read path — `try_iter`, `point_read`, seek, trailer —
    /// operates unchanged. The per-KV digests are NOT verified here (that is
    /// the scrub / paranoid path, see [`Self::verify_kv_checked`]); the
    /// block-level checksum already validated the on-disk bytes at load time.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::InvalidTrailer`] if a footer-bearing block's
    /// footer is structurally malformed.
    /// Reconstructs a row-major data block from a loaded columnar (PAX) block:
    /// decode the `ColumnBatch`, rebuild the entries, and re-encode them
    /// row-major in memory so every existing row read path is reused unchanged.
    /// The caller must have loaded `block` with `BlockType::Columnar` (its AAD /
    /// descriptor), so this is shared by both the random-access and scan paths.
    #[cfg(feature = "columnar")]
    pub(crate) fn from_columnar_block(
        block_data: &[u8],
        restart_interval: u8,
    ) -> crate::Result<Self> {
        let batch = crate::table::columnar::ColumnBatch::decode(block_data)?;
        let entries = crate::table::columnar::column_batch_to_entries(&batch)?;
        // The writer never spills an empty block, so a zero-row columnar block is
        // corrupt; reject it before the row encoder, which has a non-empty
        // precondition and would otherwise panic.
        if entries.is_empty() {
            return Err(crate::Error::InvalidHeader(
                "columnar: empty reconstructed data block",
            ));
        }
        Self::encode_entries_to_block(&entries, restart_interval)
    }

    /// As [`Self::from_columnar_block`], but drops rows whose global position is
    /// marked deleted in `deletes`. `block_start_row` is the position of this
    /// block's first row within the segment (block-index order).
    ///
    /// Returns `Ok(None)` when every row in the block is deleted: the row encoder
    /// has a non-empty precondition, so a fully-deleted block is reported as
    /// "nothing to yield" and the caller skips it.
    // Reconstruction-side masking primitive used by the iterator's columnar
    // delete-masking path (see `Iter::load_and_resolve`).
    #[cfg(feature = "columnar")]
    pub(crate) fn from_columnar_block_masked(
        block_data: &[u8],
        restart_interval: u8,
        deletes: &crate::table::delete_bitmap::DeleteBitmap,
        block_start_row: u32,
    ) -> crate::Result<Option<Self>> {
        let batch = crate::table::columnar::ColumnBatch::decode(block_data)?;
        let entries = crate::table::columnar::column_batch_to_entries(&batch)?;
        if entries.is_empty() {
            return Err(crate::Error::InvalidHeader(
                "columnar: empty reconstructed data block",
            ));
        }
        // Each row's position is `block_start_row + index`. The bitmap is
        // u32-positional and `build_position_bitmap` rejects segments past
        // u32::MAX rows at write time, but a corrupt zone-map `block_start_row`
        // could still push the sum over, so fail explicitly rather than wrapping
        // back to 0 (which would mask the wrong rows).
        let mut kept = Vec::with_capacity(entries.len());
        for (index, entry) in entries.into_iter().enumerate() {
            let offset = u32::try_from(index).map_err(|_| {
                crate::Error::InvalidHeader("columnar: block row index exceeds u32::MAX")
            })?;
            let pos = block_start_row
                .checked_add(offset)
                .ok_or(crate::Error::InvalidHeader(
                    "columnar: row position exceeds u32::MAX",
                ))?;
            if !deletes.contains(pos) {
                kept.push(entry);
            }
        }
        if kept.is_empty() {
            return Ok(None);
        }
        Self::encode_entries_to_block(&kept, restart_interval).map(Some)
    }

    /// Re-encodes reconstructed columnar `entries` into a row-major data block.
    /// In-memory, so a fixed restart interval yields a correct iterable block.
    #[cfg(feature = "columnar")]
    fn encode_entries_to_block(
        entries: &[InternalValue],
        restart_interval: u8,
    ) -> crate::Result<Self> {
        let mut buf = Vec::new();
        Self::encode_into(&mut buf, entries, restart_interval, 0.0)?;
        let len = u32::try_from(buf.len())
            .map_err(|_| crate::Error::InvalidHeader("columnar: re-encoded block exceeds u32"))?;
        let header = crate::table::block::Header {
            block_type: crate::table::block::BlockType::Data,
            block_flags: 0,
            checksum: crate::checksum::Checksum::from_raw(crate::hash::hash128(&buf)),
            data_length: len,
            uncompressed_length: len,
        };
        // Columnar blocks carry no per-KV checksum footer.
        Self::from_loaded(
            crate::table::block::Block {
                header,
                data: crate::Slice::from(buf),
            },
            false,
        )
    }

    pub(crate) fn from_loaded(block: Block, has_kv_footer: bool) -> crate::Result<Self> {
        use crate::table::block::kv_checksum;

        if !has_kv_footer {
            return Ok(Self::new(block));
        }

        // Determine the inner-slice length by splitting the footer, then
        // keep a zero-copy sub-slice of the original block bytes.
        let array_start = kv_checksum::split_inner(&block.data)?.len();
        let mut header = block.header;
        // The footer was part of the (decompressed) payload, so shrink
        // `uncompressed_length` to the stripped inner length. Leaving it at
        // the footered length makes the in-memory block inconsistent and
        // makes cache weighing over-count footer bytes. `array_start` is a
        // prefix of the original payload, so it fits the same u32.
        #[expect(
            clippy::cast_possible_truncation,
            reason = "array_start <= original uncompressed_length, which is u32"
        )]
        {
            header.uncompressed_length = array_start as u32;
        }
        // Clear the footer flag so the in-memory block doesn't advertise a
        // footer that has just been stripped. (SST data blocks already decode
        // with block_flags == 0 since they omit the byte, so this is a no-op
        // there; it keeps the stripped block honest if a flag was ever set —
        // matching the same clear in `verify_kv_checked`.)
        header.block_flags &= !crate::table::block::header::block_flags::KV_CHECKSUM_FOOTER;
        Ok(Self::new(Block {
            header,
            data: block.data.slice(..array_start),
        }))
    }

    /// Accesses the inner raw bytes
    #[must_use]
    pub fn as_slice(&self) -> &Slice {
        &self.inner.data
    }

    #[must_use]
    pub fn get_hash_index_reader(&self) -> Option<HashIndexReader<'_>> {
        use core::mem::size_of;

        let trailer = Trailer::new(&self.inner);

        // NOTE: Skip restart interval (u8), binary index step size (u8)
        // and binary stuff (2x u32)
        let offset = size_of::<u8>() + size_of::<u8>() + size_of::<u32>() + size_of::<u32>();

        let mut reader = unwrap!(trailer.as_slice().get(offset..));

        let hash_index_len = unwrap!(reader.read_u32::<LittleEndian>());
        let hash_index_offset = unwrap!(reader.read_u32::<LittleEndian>());

        if hash_index_len == 0 {
            debug_assert_eq!(
                0, hash_index_offset,
                "hash index offset should be 0 if its length is 0"
            );
            None
        } else {
            Some(HashIndexReader::new(
                &self.inner.data,
                hash_index_offset,
                hash_index_len,
            ))
        }
    }

    /// Returns the number of hash buckets.
    #[must_use]
    pub fn hash_bucket_count(&self) -> Option<usize> {
        self.get_hash_index_reader()
            .map(|reader| reader.bucket_count())
    }

    /// Performs a point read for a single key/seqno pair.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::InvalidTag`] if the wrapped block is not a data
    /// or meta block, or [`crate::Error::InvalidTrailer`] if the block trailer
    /// is malformed.
    ///
    /// # Breaking change
    ///
    /// Previously returned `Option<InternalValue>`; now returns
    /// `Result<Option<InternalValue>>` so trailer corruption is surfaced
    /// instead of silently skipped.
    pub fn point_read(
        &self,
        needle: &[u8],
        seqno: SeqNo,
        comparator: &crate::comparator::SharedComparator,
    ) -> crate::Result<Option<InternalValue>> {
        self.point_read_with(needle, seqno, comparator, None, |item, bytes| {
            item.materialize(bytes)
        })
    }

    /// Like [`Self::point_read`], but positioned by a retrieval-ribbon locator
    /// `slot` instead of the in-block binary search.
    ///
    /// `slot` is the restart index (`is_entry == false`) or the exact entry
    /// index (`is_entry == true`, mapped to its restart via the block's
    /// `restart_interval`). The scan jumps straight to that restart head and
    /// proceeds forward, skipping the binary search. If the block has no binary
    /// index or the restart is out of range, it falls back to the seqno-aware
    /// seek, so a stale hint never returns a wrong answer.
    ///
    /// # Errors
    ///
    /// Propagates a trailer/decoder error (same conditions as
    /// [`Self::point_read`]).
    pub fn point_read_at_slot(
        &self,
        slot: u64,
        is_entry: bool,
        needle: &[u8],
        seqno: SeqNo,
        comparator: &crate::comparator::SharedComparator,
    ) -> crate::Result<Option<InternalValue>> {
        self.point_read_with(
            needle,
            seqno,
            comparator,
            Some((slot, is_entry)),
            DataBlockParsedItem::materialize,
        )
    }

    /// Value-only point read: returns `(value_type, seqno, value)` without
    /// reconstructing the entry key.
    ///
    /// On the value-returning `get` path the caller has the needle and never
    /// reads the matched entry's key bytes, so fusing the delta-encoded key (an
    /// allocation + copy per hit) is pure waste. The value is a zero-copy
    /// sub-slice of the cached block, mirroring the `RocksDB` `PinnableSlice`.
    pub fn point_read_value(
        &self,
        needle: &[u8],
        seqno: SeqNo,
        comparator: &crate::comparator::SharedComparator,
    ) -> crate::Result<Option<(ValueType, SeqNo, Slice)>> {
        self.point_read_with(needle, seqno, comparator, None, |item, bytes| {
            let value = item
                .value
                .as_ref()
                .map_or_else(Slice::empty, |v| bytes.slice(v.0..v.1));
            (item.value_type, item.seqno, value)
        })
    }

    /// Value-only counterpart of [`Self::point_read_at_slot`]: positioned by a
    /// retrieval-ribbon locator `slot`, returns `(value_type, seqno, value)`.
    ///
    /// # Errors
    ///
    /// Propagates a trailer/decoder error (same conditions as
    /// [`Self::point_read_value`]).
    pub fn point_read_value_at_slot(
        &self,
        slot: u64,
        is_entry: bool,
        needle: &[u8],
        seqno: SeqNo,
        comparator: &crate::comparator::SharedComparator,
    ) -> crate::Result<Option<(ValueType, SeqNo, Slice)>> {
        self.point_read_with(
            needle,
            seqno,
            comparator,
            Some((slot, is_entry)),
            |item, bytes| {
                let value = item
                    .value
                    .as_ref()
                    .map_or_else(Slice::empty, |v| bytes.slice(v.0..v.1));
                (item.value_type, item.seqno, value)
            },
        )
    }

    /// Shared point-read seek + scan, parameterized over how the matched entry
    /// is turned into a result. `point_read` materializes a full
    /// [`InternalValue`]; `point_read_value` extracts only the value.
    ///
    /// `slot_hint` (`Some((slot, is_entry))`) positions the scan directly at a
    /// retrieval-ribbon locator's restart head instead of binary-searching;
    /// `None` keeps the default hash-index / seqno-seek positioning.
    ///
    /// Drives the [`Decoder`] directly rather than through [`Iter`]: point reads
    /// are forward-only seek + scan, so the double-ended peeking wrapper is pure
    /// overhead here. The hash / binary index readers are built from the
    /// decoder's trailer metadata (parsed once in `try_new`), avoiding the
    /// per-lookup trailer re-parse a fresh reader off the block would incur.
    /// The seqno-aware seek closure borrows the comparator, so no `Arc`
    /// refcount bump happens per lookup.
    #[inline]
    fn point_read_with<R>(
        &self,
        needle: &[u8],
        seqno: SeqNo,
        comparator: &crate::comparator::SharedComparator,
        slot_hint: Option<(u64, bool)>,
        extract: impl FnOnce(&DataBlockParsedItem, &Slice) -> R,
    ) -> crate::Result<Option<R>> {
        use crate::table::block::BlockType;

        // DataBlock is used for both Data and Meta blocks (same encoding).
        if !matches!(
            self.inner.header.block_type,
            BlockType::Data | BlockType::Meta
        ) {
            return Err(crate::Error::InvalidTag((
                "BlockType",
                self.inner.header.block_type.into(),
            )));
        }

        // try_new validates + caches all trailer fields once.
        let mut decoder = Decoder::<InternalValue, DataBlockParsedItem>::try_new(&self.inner)?;

        let positioned = if let Some((slot, is_entry)) = slot_hint {
            // Retrieval-ribbon positioning: jump straight to the restart head the
            // key's newest version lives in, skipping the binary search. For
            // Entry precision the slot is an exact entry index, mapped to its
            // restart via the block's restart_interval.
            let restart_idx = if is_entry {
                let ri = u64::from(decoder.restart_interval());
                if ri == 0 { slot } else { slot / ri }
            } else {
                slot
            };
            let restart_idx = usize::try_from(restart_idx).unwrap_or(usize::MAX);
            match decoder.cached_binary_index_reader() {
                Some(binary_index_reader) if restart_idx < binary_index_reader.len() => {
                    let offset: usize = binary_index_reader.get(restart_idx);
                    decoder.set_lo_offset(offset);
                    true
                }
                // No binary index, or a stale/out-of-range restart (corruption /
                // format drift): fall back to the seqno-aware seek so the hint
                // can never produce a wrong answer.
                _ => seek_data_block(&mut decoder, needle, seqno, comparator),
            }
        } else if let Some(hash_index_reader) = decoder.cached_hash_index_reader() {
            match hash_index_reader.get(needle) {
                MARKER_FREE => return Ok(None),
                // NOTE: Fallback to seqno-aware binary search on a hash conflict.
                MARKER_CONFLICT => seek_data_block(&mut decoder, needle, seqno, comparator),
                idx => match decoder.cached_binary_index_reader() {
                    Some(binary_index_reader) => {
                        let offset: usize = binary_index_reader.get(usize::from(idx));
                        decoder.set_lo_offset(offset);
                        true
                    }
                    None => seek_data_block(&mut decoder, needle, seqno, comparator),
                },
            }
        } else {
            // NOTE: Seqno-aware binary search reduces linear scanning by skipping most
            // restart intervals that contain only versions newer than the target seqno
            seek_data_block(&mut decoder, needle, seqno, comparator)
        };

        if !positioned {
            return Ok(None);
        }

        // Linear scan (forward-only; the decoder is its own Iterator).
        for item in decoder.by_ref() {
            match item.compare_key(needle, &self.inner.data, comparator.as_ref()) {
                core::cmp::Ordering::Greater => {
                    // We are past our searched key
                    return Ok(None);
                }
                core::cmp::Ordering::Equal => {
                    // If key is same as needle, check sequence number
                }
                core::cmp::Ordering::Less => {
                    // We are before our searched key
                    continue;
                }
            }

            if item.seqno >= seqno {
                continue;
            }

            return Ok(Some(extract(&item, &self.inner.data)));
        }

        Ok(None)
    }

    /// Creates a fallible iterator over the data block.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::InvalidTag`] if the block is not a data or meta
    /// block, or [`crate::Error::InvalidTrailer`] if the block trailer is
    /// malformed (e.g. `restart_interval == 0`).
    pub fn try_iter(
        &self,
        comparator: crate::comparator::SharedComparator,
    ) -> crate::Result<Iter<'_>> {
        use crate::table::block::BlockType;

        // DataBlock is used for both Data and Meta blocks (same encoding).
        if !matches!(
            self.inner.header.block_type,
            BlockType::Data | BlockType::Meta
        ) {
            return Err(crate::Error::InvalidTag((
                "BlockType",
                self.inner.header.block_type.into(),
            )));
        }
        Ok(Iter::new(
            &self.inner.data,
            Decoder::<InternalValue, DataBlockParsedItem>::try_new(&self.inner)?,
            comparator,
        ))
    }

    #[must_use]
    pub fn iter(&self, comparator: crate::comparator::SharedComparator) -> Iter<'_> {
        Iter::new(
            &self.inner.data,
            Decoder::<InternalValue, DataBlockParsedItem>::new(&self.inner),
            comparator,
        )
    }

    /// Returns the binary index length (number of pointers).
    ///
    /// The number of pointers is equal to the number of restart intervals.
    #[must_use]
    pub fn binary_index_len(&self) -> u32 {
        use core::mem::size_of;

        let trailer = Trailer::new(&self.inner);

        // NOTE: Skip restart interval (u8) and binary index step size (u8)
        let offset = 2 * size_of::<u8>();
        let mut reader = unwrap!(trailer.as_slice().get(offset..));

        unwrap!(reader.read_u32::<LittleEndian>())
    }

    /// Returns the number of items in the block.
    #[must_use]
    #[expect(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        Trailer::new(&self.inner).item_count()
    }

    pub fn encode_into_vec(
        items: &[InternalValue],
        restart_interval: u8,
        hash_index_ratio: f32,
    ) -> crate::Result<Vec<u8>> {
        let mut buf = vec![];

        Self::encode_into(&mut buf, items, restart_interval, hash_index_ratio)?;

        Ok(buf)
    }

    /// Builds an data block.
    ///
    /// # Panics
    ///
    /// Panics if the given item array if empty.
    pub fn encode_into(
        writer: &mut Vec<u8>,
        items: &[InternalValue],
        restart_interval: u8,
        hash_index_ratio: f32,
    ) -> crate::Result<()> {
        #[expect(clippy::expect_used, reason = "the chunk should not be empty")]
        let first_key = &items
            .first()
            .expect("chunk should not be empty")
            .key
            .user_key;

        let mut serializer = Encoder::<'_, (), InternalValue>::new(
            writer,
            items.len(),
            restart_interval,
            hash_index_ratio,
            first_key,
        );

        for item in items {
            serializer.write(item)?;
        }

        serializer.finish()
    }

    /// Builds a per-KV-checked data block: the standard data-block payload
    /// followed by a per-KV checksum footer. The block role stays `Data`.
    ///
    /// Footer presence is recorded out-of-band in the per-SST meta descriptor
    /// (`descriptor#kv_checksum`), NOT in a per-block header flag: SST data
    /// blocks omit the `block_flags` byte on disk, so `header.block_flags`
    /// decodes as `0` and the read path consults the descriptor (not the
    /// header) to decide whether to strip the footer.
    ///
    /// `digests` MUST be in the same order as `items` (digest `i` belongs to
    /// entry `i` in scan order) and computed via
    /// [`kv_checksum::kv_digest`](super::block::kv_checksum) over each entry's
    /// logical content. The writer computes the digests when it compiles the
    /// block; a `None` for an unavailable algorithm is the caller's
    /// responsibility to reject before reaching this point.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::InvalidTrailer`] if `digests.len()` does not
    /// equal `items.len()`: the footer's `count` field would disagree with
    /// the stored digest array, so the write is rejected rather than
    /// producing structurally-corrupt on-disk data (validated at runtime,
    /// not only via a debug assertion that vanishes in release builds).
    ///
    /// # Panics
    ///
    /// Panics if `items` is empty (same contract as [`Self::encode_into`]).
    pub(crate) fn encode_kv_checked_into(
        writer: &mut Vec<u8>,
        items: &[InternalValue],
        digests: &[u64],
        algo: crate::runtime_config::ChecksumAlgorithm,
        restart_interval: u8,
        hash_index_ratio: f32,
    ) -> crate::Result<()> {
        if items.len() != digests.len() {
            return Err(crate::Error::InvalidTrailer);
        }
        Self::encode_into(writer, items, restart_interval, hash_index_ratio)?;
        super::block::kv_checksum::append_footer(writer, digests, algo);
        Ok(())
    }

    /// Verifies every entry of a footer-bearing data block against its
    /// stored per-KV digest (the scrub / paranoid path).
    ///
    /// `footer_wrapped` is the full on-disk data-block payload INCLUDING the
    /// per-KV checksum footer (i.e. the bytes as loaded, before
    /// [`Self::from_loaded`] strips them). This splits the footer, decodes the
    /// inner standard data block, and for each entry in scan order recomputes
    /// the logical-content digest and compares it to the stored value.
    ///
    /// When `expected_algo` is `Some`, the footer's own algorithm tag must
    /// match it (the per-SST `descriptor#kv_checksum` algorithm): a divergence
    /// means a writer bug or corruption flipped the tag, which would otherwise
    /// let the scrub verify digests under the wrong algorithm and silently
    /// return `Ok`. `None` skips the cross-check (the footer's self-described
    /// algorithm is then authoritative).
    ///
    /// # Errors
    ///
    /// - [`crate::Error::InvalidTrailer`] if the footer is malformed, its
    ///   entry count disagrees with the decoded block, or its algorithm tag
    ///   diverges from `expected_algo`.
    /// - [`crate::Error::FeatureUnsupported`] if the footer's algorithm is not
    ///   compiled into this build.
    /// - [`crate::Error::ChecksumMismatch`] on the first entry whose recomputed
    ///   logical-content digest disagrees with the stored one (corruption of
    ///   the entry bytes or the stored digest).
    #[cfg_attr(
        not(feature = "std"),
        allow(
            dead_code,
            reason = "core+alloc per-KV scrub; the verify/scrub consumer is std-gated, so unused under no_std"
        )
    )]
    pub(crate) fn verify_kv_checked(
        footer_wrapped: &Slice,
        header: super::block::Header,
        comparator: crate::comparator::SharedComparator,
        expected_algo: Option<crate::runtime_config::ChecksumAlgorithm>,
    ) -> crate::Result<()> {
        use super::block::{BlockType, ParsedItem, kv_checksum};

        let split = kv_checksum::split_full(footer_wrapped)?;

        // Cross-check the footer's self-described algorithm against the
        // per-SST descriptor when provided. A mismatch means the tag was
        // flipped (writer bug / corruption); verifying under the wrong
        // algorithm could otherwise pass against forged digests.
        if let Some(expected) = expected_algo
            && split.algo != expected
        {
            return Err(crate::Error::InvalidTrailer);
        }

        // The scrub only verifies Data blocks. The caller loads raw Data
        // blocks, so a non-Data header here is corruption or a caller bug:
        // reject it rather than coerce `block_type = Data`, which would verify
        // a non-Data payload as if it were Data and defeat the scrub.
        if header.block_type != BlockType::Data {
            return Err(crate::Error::InvalidTag((
                "BlockType",
                header.block_type.into(),
            )));
        }

        // Decode the inner slice through the standard data-block path. Reuse
        // the real header (Copy) but present the inner slice as a consistent
        // plain Data block: clear the per-KV footer flag and shrink
        // `uncompressed_length` to the stripped length so the in-memory block
        // matches its bytes (the type gate then passes and no field lies).
        let mut inner_header = header;
        inner_header.block_flags &= !super::block::header::block_flags::KV_CHECKSUM_FOOTER;
        let inner_len = split.inner.len();
        #[expect(
            clippy::cast_possible_truncation,
            reason = "split.inner is a prefix of the footered payload, which fits u32"
        )]
        {
            inner_header.uncompressed_length = inner_len as u32;
        }
        // Zero-copy: the inner payload is a prefix of `footer_wrapped`, so take
        // a refcounted sub-slice of the caller's owning `Slice` rather than
        // `Slice::from(&[u8])` (which would copy ~block_size bytes per block on
        // the scrub path).
        let inner = Self::new(Block {
            header: inner_header,
            data: footer_wrapped.slice(..inner_len),
        });
        let inner_data = inner.inner.data.clone();

        let mut idx = 0usize;
        for parsed in inner.try_iter(comparator)? {
            let item = parsed.materialize(&inner_data);
            // Read the stored digest on demand from the borrowed footer bytes
            // (no owned Vec<u64> materialization — see `SplitFull`).
            let stored = split.digest(idx).ok_or(crate::Error::InvalidTrailer)?;
            let recomputed = kv_checksum::kv_digest(&item, split.algo)
                .ok_or(crate::Error::FeatureUnsupported("kv-checksum-algorithm"))?;
            if recomputed != stored {
                return Err(crate::Error::ChecksumMismatch {
                    got: crate::Checksum::from_raw(u128::from(recomputed)),
                    expected: crate::Checksum::from_raw(u128::from(stored)),
                });
            }
            idx += 1;
        }

        if idx != split.count() {
            // Footer claims a different entry count than the inner block
            // decoded — structural inconsistency, not a content mismatch.
            return Err(crate::Error::InvalidTrailer);
        }
        Ok(())
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::indexing_slicing)]
mod tests;

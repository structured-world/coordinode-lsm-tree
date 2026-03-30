// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{SeqNo, UserKey};
use crate::{
    coding::{Decode, Encode},
    table::{
        block::{BlockOffset, Decodable, Encodable, TRAILER_START_MARKER},
        index_block::IndexBlockParsedItem,
        util::SliceIndexes,
    },
};
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::{Cursor, Seek};
use varint_rs::{VarintReader, VarintWriter};

/// Points to a block on file
#[derive(Copy, Clone, Debug, Default)]
pub struct BlockHandle {
    /// Position of block in file
    offset: BlockOffset,

    /// Size of block in bytes
    size: u32,
}

impl BlockHandle {
    #[must_use]
    pub fn new(offset: BlockOffset, size: u32) -> Self {
        Self { offset, size }
    }

    #[must_use]
    pub fn size(&self) -> u32 {
        self.size
    }

    #[must_use]
    pub fn offset(&self) -> BlockOffset {
        self.offset
    }
}

impl Encode for BlockHandle {
    fn encode_into<W: std::io::Write>(&self, writer: &mut W) -> Result<(), crate::Error> {
        writer.write_u64_varint(*self.offset)?;
        writer.write_u32_varint(self.size)?;
        Ok(())
    }
}

impl Decode for BlockHandle {
    fn decode_from<R: std::io::Read>(reader: &mut R) -> Result<Self, crate::Error>
    where
        Self: Sized,
    {
        let offset = reader.read_u64_varint()?;
        let size = reader.read_u32_varint()?;

        Ok(Self {
            offset: BlockOffset(offset),
            size,
        })
    }
}

/// Points to a block on file
#[derive(Clone, Debug)]
pub struct KeyedBlockHandle {
    /// Key of last item in block
    end_key: UserKey,

    /// Seqno of last item in block
    seqno: SeqNo,

    inner: BlockHandle,
}

impl AsRef<BlockHandle> for KeyedBlockHandle {
    fn as_ref(&self) -> &BlockHandle {
        &self.inner
    }
}

impl KeyedBlockHandle {
    #[must_use]
    pub fn into_inner(self) -> BlockHandle {
        self.inner
    }

    #[must_use]
    pub fn new(end_key: UserKey, seqno: SeqNo, handle: BlockHandle) -> Self {
        Self {
            end_key,
            seqno,
            inner: handle,
        }
    }

    #[must_use]
    pub fn seqno(&self) -> SeqNo {
        self.seqno
    }

    pub fn shift(&mut self, delta: BlockOffset) {
        self.inner.offset += delta;
    }

    #[must_use]
    pub fn size(&self) -> u32 {
        self.inner.size()
    }

    #[must_use]
    pub fn offset(&self) -> BlockOffset {
        self.inner.offset()
    }

    #[must_use]
    pub fn end_key(&self) -> &UserKey {
        &self.end_key
    }
}

#[cfg(test)]
impl PartialEq for KeyedBlockHandle {
    fn eq(&self, other: &Self) -> bool {
        self.offset() == other.offset()
    }
}

impl Encodable<BlockOffset> for KeyedBlockHandle {
    fn encode_full_into<W: std::io::Write>(
        &self,
        writer: &mut W,
        state: &mut BlockOffset,
    ) -> crate::Result<()> {
        // We encode restart markers as:
        // [marker=0] [offset] [size] [seqno] [key len] [end key]
        // 1          2        3      4       5         6

        writer.write_u8(0)?; // 1

        self.inner.encode_into(writer)?; // 2, 3

        unwrap!(writer.write_u64_varint(self.seqno)); // 4

        #[expect(clippy::cast_possible_truncation, reason = "keys are u16 long max")]
        writer.write_u16_varint(self.end_key.len() as u16)?; // 5
        writer.write_all(&self.end_key)?; // 6

        *state = BlockOffset(*self.offset() + u64::from(self.size()));

        Ok(())
    }

    // TODO: see https://github.com/structured-world/coordinode-lsm-tree/issues/184
    fn encode_truncated_into<W: std::io::Write>(
        &self,
        writer: &mut W,
        _state: &mut BlockOffset,
        shared_len: usize,
    ) -> crate::Result<()> {
        // We encode truncated index entries as:
        // [marker=1] [offset] [size] [seqno] [shared prefix len] [rest key len] [rest key]
        // 1          2        3      4       5                   6              7
        writer.write_u8(1)?;

        self.inner.encode_into(writer)?;
        writer.write_u64_varint(self.seqno)?;

        #[expect(clippy::cast_possible_truncation, reason = "keys are u16 long max")]
        writer.write_u16_varint(shared_len as u16)?;

        #[expect(
            clippy::expect_used,
            reason = "the shared len should not be greater than key length"
        )]
        let truncated_end_key = self.end_key.get(shared_len..).expect("should be in bounds");
        let rest_len = truncated_end_key.len();

        #[expect(clippy::cast_possible_truncation, reason = "keys are u16 long max")]
        writer.write_u16_varint(rest_len as u16)?;
        writer.write_all(truncated_end_key)?;

        Ok(())
    }

    fn key(&self) -> &[u8] {
        &self.end_key
    }
}

impl Decodable<IndexBlockParsedItem> for KeyedBlockHandle {
    fn parse_full(
        reader: &mut Cursor<&[u8]>,
        offset: usize,
        entries_end: usize,
    ) -> Option<IndexBlockParsedItem> {
        let marker = unwrap!(reader.read_u8());

        if marker == TRAILER_START_MARKER {
            return None;
        }
        if marker != 0 {
            return None;
        }

        let handle = unwrap!(BlockHandle::decode_from(reader));
        let seqno = unwrap!(reader.read_u64_varint());

        let key_len: usize = unwrap!(reader.read_u16_varint()).into();
        #[expect(
            clippy::cast_possible_truncation,
            reason = "blocks tend to be some megabytes in size at most, so position should fit into usize"
        )]
        let key_start = offset.checked_add(reader.position() as usize)?;

        #[expect(
            clippy::cast_possible_wrap,
            reason = "key_len is bounded by u16::MAX, no wrap expected"
        )]
        let offset_i64 = key_len as i64;
        if key_start > entries_end {
            return None;
        }
        let key_end = key_start.checked_add(key_len)?;
        if key_end > entries_end {
            return None;
        }
        unwrap!(reader.seek_relative(offset_i64));

        Some(IndexBlockParsedItem {
            prefix: None,
            end_key: SliceIndexes(key_start, key_end),
            offset: handle.offset(),
            size: handle.size(),
            seqno,
        })
    }

    fn parse_restart_key<'a>(
        reader: &mut Cursor<&[u8]>,
        offset: usize,
        data: &'a [u8],
        entries_end: usize,
    ) -> Option<(&'a [u8], SeqNo)> {
        let marker = unwrap!(reader.read_u8());

        if marker == TRAILER_START_MARKER {
            return None;
        }
        if marker != 0 {
            return None;
        }

        let _file_offset = unwrap!(reader.read_u64_varint());
        let _size = unwrap!(reader.read_u32_varint());
        let seqno = unwrap!(reader.read_u64_varint());

        let key_len: usize = unwrap!(reader.read_u16_varint()).into();
        #[expect(
            clippy::cast_possible_truncation,
            reason = "blocks tend to be some megabytes in size at most, so position should fit into usize"
        )]
        let key_start = offset.checked_add(reader.position() as usize)?;
        let key_end = key_start.checked_add(key_len)?;
        if key_end > entries_end {
            return None;
        }

        #[expect(
            clippy::cast_possible_wrap,
            reason = "key_len is bounded by u16::MAX, no wrap expected"
        )]
        let key_len_i64 = key_len as i64;
        unwrap!(reader.seek_relative(key_len_i64));

        let key = data.get(key_start..key_end);

        key.map(|k| (k, seqno))
    }

    fn parse_truncated(
        reader: &mut Cursor<&[u8]>,
        offset: usize,
        base_key_offset: usize,
        base_key_end: usize,
        entries_end: usize,
    ) -> Option<IndexBlockParsedItem> {
        let marker = reader.read_u8().ok()?;

        if marker == TRAILER_START_MARKER {
            return None;
        }

        if marker != 1 {
            return None;
        }

        let handle = BlockHandle::decode_from(reader).ok()?;
        let seqno = reader.read_u64_varint().ok()?;

        let shared_prefix_len: usize = reader.read_u16_varint().ok()?.into();
        let rest_key_len: usize = reader.read_u16_varint().ok()?.into();

        #[expect(
            clippy::cast_possible_truncation,
            reason = "blocks tend to be some megabytes in size at most, so position should fit into usize"
        )]
        let key_start = offset.checked_add(reader.position() as usize)?;
        if key_start > entries_end {
            return None;
        }
        let remaining_suffix_bytes = entries_end.checked_sub(key_start)?;
        if rest_key_len > remaining_suffix_bytes {
            return None;
        }

        if base_key_offset > offset {
            return None;
        }
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

        #[expect(
            clippy::cast_possible_wrap,
            reason = "rest_key_len is bounded by u16::MAX, no wrap expected"
        )]
        let rest_key_len_i64 = rest_key_len as i64;
        reader.seek_relative(rest_key_len_i64).ok()?;
        let end_key_end = key_start.checked_add(rest_key_len)?;

        Some(IndexBlockParsedItem {
            prefix: Some(SliceIndexes(base_key_offset, prefix_end)),
            end_key: SliceIndexes(key_start, end_key_end),
            offset: handle.offset(),
            size: handle.size(),
            seqno,
        })
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;
    use crate::table::block::{Decodable, Encodable};

    fn make_truncated_entry(shared_prefix_len: usize) -> Vec<u8> {
        let handle = KeyedBlockHandle::new(
            b"abcdef".to_vec().into(),
            0,
            BlockHandle::new(BlockOffset(0), 1),
        );
        let mut bytes = Vec::new();
        let mut state = BlockOffset(0);
        handle
            .encode_truncated_into(&mut bytes, &mut state, shared_prefix_len)
            .unwrap();
        bytes
    }

    fn make_full_entry() -> Vec<u8> {
        let handle = KeyedBlockHandle::new(
            b"abcdef".to_vec().into(),
            0,
            BlockHandle::new(BlockOffset(0), 1),
        );
        let mut bytes = Vec::new();
        let mut state = BlockOffset(0);
        handle.encode_full_into(&mut bytes, &mut state).unwrap();
        bytes
    }

    fn shared_prefix_len_offset(bytes: &[u8]) -> usize {
        let mut cursor = Cursor::new(bytes);
        let marker = cursor.read_u8().unwrap();
        assert_eq!(marker, 1);
        let _ = BlockHandle::decode_from(&mut cursor).unwrap();
        let _ = cursor.read_u64_varint().unwrap();
        usize::try_from(cursor.position()).unwrap()
    }

    fn rest_key_len_offset(bytes: &[u8]) -> usize {
        let mut cursor = Cursor::new(bytes);
        let marker = cursor.read_u8().unwrap();
        assert_eq!(marker, 1);
        let _ = BlockHandle::decode_from(&mut cursor).unwrap();
        let _ = cursor.read_u64_varint().unwrap();
        let _ = cursor.read_u16_varint().unwrap();
        usize::try_from(cursor.position()).unwrap()
    }

    fn full_key_len_offset(bytes: &[u8]) -> usize {
        let mut cursor = Cursor::new(bytes);
        let marker = cursor.read_u8().unwrap();
        assert_eq!(marker, 0);
        let _ = BlockHandle::decode_from(&mut cursor).unwrap();
        let _ = cursor.read_u64_varint().unwrap();
        usize::try_from(cursor.position()).unwrap()
    }

    #[test]
    fn parse_full_rejects_restart_key_span_overlapping_trailer_region() {
        let mut bytes = make_full_entry();
        let offset = 16;
        let entries_end = offset + bytes.len();
        let key_len_pos = full_key_len_offset(&bytes);
        *bytes.get_mut(key_len_pos).unwrap() = 8;
        bytes.extend_from_slice(&[0u8; 32]);

        let mut cursor = Cursor::new(bytes.as_slice());
        let parsed = <KeyedBlockHandle as Decodable<IndexBlockParsedItem>>::parse_full(
            &mut cursor,
            offset,
            entries_end,
        );
        assert!(parsed.is_none());
    }

    #[test]
    fn parse_truncated_rejects_suffix_len_beyond_remaining_bytes() {
        let mut bytes = make_truncated_entry(2);
        let rest_len_pos = rest_key_len_offset(&bytes);
        *bytes.get_mut(rest_len_pos).unwrap() = 100;
        let offset = 16;
        let entries_end = offset + bytes.len();
        let mut cursor = Cursor::new(bytes.as_slice());
        let parsed = <KeyedBlockHandle as Decodable<IndexBlockParsedItem>>::parse_truncated(
            &mut cursor,
            offset,
            12,
            16,
            entries_end,
        );
        assert!(parsed.is_none());
    }

    #[test]
    fn parse_truncated_rejects_prefix_span_crossing_entry_boundary() {
        let mut bytes = make_truncated_entry(2);
        let shared_len_pos = shared_prefix_len_offset(&bytes);
        *bytes.get_mut(shared_len_pos).unwrap() = 100;
        let offset = 16;
        let entries_end = offset + bytes.len();
        let mut cursor = Cursor::new(bytes.as_slice());
        let parsed = <KeyedBlockHandle as Decodable<IndexBlockParsedItem>>::parse_truncated(
            &mut cursor,
            offset,
            13,
            16,
            entries_end,
        );
        assert!(parsed.is_none());
    }

    #[test]
    fn parse_truncated_rejects_prefix_span_crossing_restart_key_boundary() {
        let mut bytes = make_truncated_entry(2);
        let shared_len_pos = shared_prefix_len_offset(&bytes);
        *bytes.get_mut(shared_len_pos).unwrap() = 7;
        let offset = 16;
        let entries_end = offset + bytes.len();
        let mut cursor = Cursor::new(bytes.as_slice());
        let parsed = <KeyedBlockHandle as Decodable<IndexBlockParsedItem>>::parse_truncated(
            &mut cursor,
            offset,
            8,
            14,
            entries_end,
        );
        assert!(parsed.is_none());
    }

    #[test]
    fn parse_truncated_rejects_base_key_offset_past_entry_start() {
        let bytes = make_truncated_entry(1);
        let offset = 16;
        let entries_end = offset + bytes.len();
        let mut cursor = Cursor::new(bytes.as_slice());
        let parsed = <KeyedBlockHandle as Decodable<IndexBlockParsedItem>>::parse_truncated(
            &mut cursor,
            offset,
            17,
            16,
            entries_end,
        );
        assert!(parsed.is_none());
    }

    #[test]
    fn parse_truncated_rejects_invalid_marker() {
        let mut bytes = make_truncated_entry(1);
        let invalid_marker = match TRAILER_START_MARKER {
            2 => 3,
            _ => 2,
        };
        *bytes.get_mut(0).unwrap() = invalid_marker;
        let offset = 16;
        let entries_end = offset + bytes.len();
        let mut cursor = Cursor::new(bytes.as_slice());
        let parsed = <KeyedBlockHandle as Decodable<IndexBlockParsedItem>>::parse_truncated(
            &mut cursor,
            offset,
            12,
            16,
            entries_end,
        );
        assert!(parsed.is_none());
    }

    #[test]
    fn parse_truncated_rejects_suffix_span_overlapping_trailer_region() {
        let mut bytes = make_truncated_entry(2);
        let offset = 16;
        let entries_end = offset + bytes.len();
        let rest_len_pos = rest_key_len_offset(&bytes);
        *bytes.get_mut(rest_len_pos).unwrap() = 6;
        bytes.extend_from_slice(&[0u8; 32]);

        let mut cursor = Cursor::new(bytes.as_slice());
        let parsed = <KeyedBlockHandle as Decodable<IndexBlockParsedItem>>::parse_truncated(
            &mut cursor,
            offset,
            12,
            16,
            entries_end,
        );
        assert!(parsed.is_none());
    }

    #[test]
    fn parse_restart_key_rejects_truncated_entry_marker() {
        let bytes = make_truncated_entry(1);
        let mut cursor = Cursor::new(bytes.as_slice());
        let parsed = <KeyedBlockHandle as Decodable<IndexBlockParsedItem>>::parse_restart_key(
            &mut cursor,
            0,
            bytes.as_slice(),
            bytes.len(),
        );
        assert!(parsed.is_none());
    }
}

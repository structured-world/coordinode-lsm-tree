// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod iter;

#[cfg(test)]
mod iter_test;

pub use iter::Iter;

use super::block::{
    Block, Decodable, Decoder, Encodable, Encoder, ParsedItem, TRAILER_START_MARKER, Trailer,
    binary_index::Reader as BinaryIndexReader, hash_index::Reader as HashIndexReader,
};
use crate::key::InternalKey;
use crate::table::block::hash_index::{MARKER_CONFLICT, MARKER_FREE};
use crate::table::util::{SliceIndexes, compare_prefixed_slice};
use crate::{InternalValue, SeqNo, Slice, ValueType};
use byteorder::WriteBytesExt;
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Cursor;
use std::io::Seek;
use varint_rs::{VarintReader, VarintWriter};

impl Decodable<DataBlockParsedItem> for InternalValue {
    fn parse_restart_key<'a>(
        reader: &mut Cursor<&[u8]>,
        offset: usize,
        data: &'a [u8],
        entries_end: usize,
    ) -> Option<(&'a [u8], SeqNo)> {
        let value_type = reader.read_u8().ok()?;

        if value_type == TRAILER_START_MARKER {
            return None;
        }

        let seqno = reader.read_u64_varint().ok()?;

        let key_len: usize = reader.read_u16_varint().ok()?.into();
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
        reader.seek_relative(key_len_i64).ok()?;

        let key = data.get(key_start..key_end);

        key.map(|k| (k, seqno))
    }

    fn parse_full(
        reader: &mut Cursor<&[u8]>,
        offset: usize,
        entries_end: usize,
    ) -> Option<DataBlockParsedItem> {
        let value_type = reader.read_u8().ok()?;
        if value_type == TRAILER_START_MARKER {
            return None;
        }

        let value_type = ValueType::try_from(value_type).ok()?;

        let seqno = reader.read_u64_varint().ok()?;

        let key_len: usize = reader.read_u16_varint().ok()?.into();
        #[expect(
            clippy::cast_possible_truncation,
            reason = "blocks tend to be some megabytes in size at most, so position should fit into usize"
        )]
        let key_start = offset.checked_add(reader.position() as usize)?;
        #[expect(
            clippy::cast_possible_wrap,
            reason = "key_len is bounded by u16::MAX, no wrap expected"
        )]
        let key_len_i64 = key_len as i64;
        if key_start > entries_end {
            return None;
        }
        let key_end = key_start.checked_add(key_len)?;
        if key_end > entries_end {
            return None;
        }
        reader.seek_relative(key_len_i64).ok()?;

        let is_value = !value_type.is_tombstone();

        let val_len: usize = if is_value {
            reader.read_u32_varint().ok()? as usize
        } else {
            0
        };
        #[expect(
            clippy::cast_possible_truncation,
            reason = "blocks tend to be some megabytes in size at most, so position should fit into usize"
        )]
        let val_offset = offset.checked_add(reader.position() as usize)?;
        #[expect(
            clippy::cast_possible_wrap,
            reason = "val_len is bounded by u32::MAX, no wrap expected"
        )]
        let val_len_i64 = val_len as i64;
        if val_offset > entries_end {
            return None;
        }
        let val_end = val_offset.checked_add(val_len)?;
        if val_end > entries_end {
            return None;
        }
        reader.seek_relative(val_len_i64).ok()?;

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
        let value_type = reader.read_u8().ok()?;
        if value_type == TRAILER_START_MARKER {
            return None;
        }
        let value_type = ValueType::try_from(value_type).ok()?;

        let seqno = reader.read_u64_varint().ok()?;

        let shared_prefix_len: usize = reader.read_u16_varint().ok()?.into();
        let rest_key_len: usize = reader.read_u16_varint().ok()?.into();
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
            clippy::cast_possible_truncation,
            reason = "truncation is not expected to happen"
        )]
        let key_offset = offset.checked_add(reader.position() as usize)?;
        if key_offset > entries_end {
            return None;
        }
        let key_end = key_offset.checked_add(rest_key_len)?;
        if key_end > entries_end {
            return None;
        }

        #[expect(
            clippy::cast_possible_wrap,
            reason = "rest_key_len is bounded by u16::MAX, no wrap expected"
        )]
        let rest_key_len_i64 = rest_key_len as i64;
        reader.seek_relative(rest_key_len_i64).ok()?;

        let is_value = !value_type.is_tombstone();

        let val_len: usize = if is_value {
            reader.read_u32_varint().ok()? as usize
        } else {
            0
        };
        #[expect(
            clippy::cast_possible_truncation,
            reason = "truncation is not expected to happen"
        )]
        let val_offset = offset.checked_add(reader.position() as usize)?;
        if val_offset > entries_end {
            return None;
        }
        let val_end = val_offset.checked_add(val_len)?;
        if val_end > entries_end {
            return None;
        }
        #[expect(
            clippy::cast_possible_wrap,
            reason = "val_len is bounded by u32::MAX, fits in i64 without wrap"
        )]
        let val_len_i64 = val_len as i64;
        reader.seek_relative(val_len_i64).ok()?;

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
    fn encode_full_into<W: std::io::Write>(
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

    fn encode_truncated_into<W: std::io::Write>(
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
    ) -> std::cmp::Ordering {
        // SAFETY: slice indexes come from the block parser which validates them
        // during decoding. The block format guarantees they are within bounds.
        if let Some(prefix) = &self.prefix {
            let prefix = unsafe { bytes.get_unchecked(prefix.0..prefix.1) };
            let rest_key = unsafe { bytes.get_unchecked(self.key.0..self.key.1) };
            compare_prefixed_slice(prefix, rest_key, needle, cmp)
        } else {
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

impl DataBlock {
    /// Interprets a block as a data block.
    ///
    /// The caller needs to make sure the block is actually a data block
    /// (e.g. by checking the block type, this is typically done in the `load_block` routine)
    #[must_use]
    pub fn new(inner: Block) -> Self {
        Self { inner }
    }

    /// Accesses the inner raw bytes
    #[must_use]
    pub fn as_slice(&self) -> &Slice {
        &self.inner.data
    }

    pub(crate) fn get_binary_index_reader(&self) -> BinaryIndexReader<'_> {
        use std::mem::size_of;

        let trailer = Trailer::new(&self.inner);

        // NOTE: Skip restart interval (u8)
        let offset = size_of::<u8>();

        let mut reader = unwrap!(trailer.as_slice().get(offset..));

        let binary_index_step_size = unwrap!(reader.read_u8());

        debug_assert!(
            binary_index_step_size == 2 || binary_index_step_size == 4,
            "invalid binary index step size",
        );

        let binary_index_len = unwrap!(reader.read_u32::<LittleEndian>());
        let binary_index_offset = unwrap!(reader.read_u32::<LittleEndian>());

        BinaryIndexReader::new(
            &self.inner.data,
            binary_index_offset,
            binary_index_len,
            binary_index_step_size,
        )
    }

    #[must_use]
    pub fn get_hash_index_reader(&self) -> Option<HashIndexReader<'_>> {
        use std::mem::size_of;

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
    /// Returns [`crate::Error::InvalidTrailer`] if the block trailer is
    /// malformed.
    pub fn point_read(
        &self,
        needle: &[u8],
        seqno: SeqNo,
        comparator: &crate::comparator::SharedComparator,
    ) -> crate::Result<Option<InternalValue>> {
        // Validate trailer once via try_iter (which calls Decoder::try_new
        // internally). This must happen before get_hash_index_reader /
        // get_binary_index_reader which read trailer fields without their own
        // validation.
        let mut iter = self.try_iter(comparator.clone())?;

        let iter = if let Some(hash_index_reader) = self.get_hash_index_reader() {
            match hash_index_reader.get(needle) {
                MARKER_FREE => {
                    return Ok(None);
                }
                MARKER_CONFLICT => {
                    // NOTE: Fallback to seqno-aware binary search
                    if !iter.seek_to_key_seqno(needle, seqno) {
                        return Ok(None);
                    }

                    iter
                }
                idx => {
                    let offset: usize = self.get_binary_index_reader().get(usize::from(idx));
                    iter.seek_to_offset(offset);

                    iter
                }
            }
        } else {
            // NOTE: Seqno-aware binary search reduces linear scanning by skipping most
            // restart intervals that contain only versions newer than the target seqno
            if !iter.seek_to_key_seqno(needle, seqno) {
                return Ok(None);
            }

            iter
        };

        // Linear scan
        for item in iter {
            match item.compare_key(needle, &self.inner.data, comparator.as_ref()) {
                std::cmp::Ordering::Greater => {
                    // We are past our searched key
                    return Ok(None);
                }
                std::cmp::Ordering::Equal => {
                    // If key is same as needle, check sequence number
                }
                std::cmp::Ordering::Less => {
                    // We are before our searched key
                    continue;
                }
            }

            if item.seqno >= seqno {
                continue;
            }

            return Ok(Some(item.materialize(&self.inner.data)));
        }

        Ok(None)
    }

    /// Creates a fallible iterator over the data block.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::InvalidTrailer`] if the block trailer is
    /// malformed (e.g. `restart_interval == 0`).
    pub fn try_iter(
        &self,
        comparator: crate::comparator::SharedComparator,
    ) -> crate::Result<Iter<'_>> {
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
        use std::mem::size_of;

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
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::DataBlockParsedItem;
    use crate::comparator::default_comparator;
    use crate::{
        Checksum, InternalValue, SeqNo, Slice,
        ValueType::{Tombstone, Value},
        table::{
            Block, DataBlock,
            block::{BlockType, Decodable, Encodable, Header, ParsedItem},
        },
    };
    use byteorder::ReadBytesExt;
    use std::io::{Cursor, Seek};
    use test_log::test;
    use varint_rs::VarintReader;

    fn make_truncated_data_entry(shared_prefix_len: usize) -> Vec<u8> {
        let value = InternalValue::from_components("abcdef", "payload", 0, Value);
        let mut bytes = Vec::new();
        value
            .encode_truncated_into(&mut bytes, &mut (), shared_prefix_len)
            .expect("encoding test InternalValue into truncated form must succeed");
        bytes
    }

    fn make_full_data_entry() -> Vec<u8> {
        let value = InternalValue::from_components("abcdef", "payload", 0, Value);
        let mut bytes = Vec::new();
        value
            .encode_full_into(&mut bytes, &mut ())
            .expect("encoding full test InternalValue must succeed");
        bytes
    }

    fn make_full_tombstone_entry() -> Vec<u8> {
        let value = InternalValue::from_components("abcdef", "", 0, Tombstone);
        let mut bytes = Vec::new();
        value
            .encode_full_into(&mut bytes, &mut ())
            .expect("encoding full tombstone InternalValue must succeed");
        bytes
    }

    fn make_truncated_tombstone_entry(shared_prefix_len: usize) -> Vec<u8> {
        let value = InternalValue::from_components("abcdef", "", 0, Tombstone);
        let mut bytes = Vec::new();
        value
            .encode_truncated_into(&mut bytes, &mut (), shared_prefix_len)
            .expect("encoding tombstone InternalValue into truncated form must succeed");
        bytes
    }

    fn data_shared_prefix_len_offset(bytes: &[u8]) -> usize {
        let mut cursor = Cursor::new(bytes);
        let _value_type = cursor
            .read_u8()
            .expect("test fixture encoding must contain a value type byte");
        let _seqno = cursor
            .read_u64_varint()
            .expect("test fixture encoding must contain a seqno varint");
        usize::try_from(cursor.position())
            .expect("cursor position must fit into usize in test environment")
    }

    fn data_rest_key_len_offset(bytes: &[u8]) -> usize {
        let mut cursor = Cursor::new(bytes);
        let _value_type = cursor
            .read_u8()
            .expect("test fixture encoding must contain a value type byte");
        let _seqno = cursor
            .read_u64_varint()
            .expect("test fixture encoding must contain a seqno varint");
        let _shared_prefix_len = cursor
            .read_u16_varint()
            .expect("test fixture encoding must contain a shared-prefix varint");
        usize::try_from(cursor.position())
            .expect("cursor position must fit into usize in test environment")
    }

    fn data_value_len_offset(bytes: &[u8]) -> usize {
        let mut cursor = Cursor::new(bytes);
        let _value_type = cursor
            .read_u8()
            .expect("test fixture encoding must contain a value type byte");
        let _seqno = cursor
            .read_u64_varint()
            .expect("test fixture encoding must contain a seqno varint");
        let _shared_prefix_len = cursor
            .read_u16_varint()
            .expect("test fixture encoding must contain a shared-prefix varint");
        let rest_key_len: usize = cursor
            .read_u16_varint()
            .expect("test fixture encoding must contain a rest-key-length varint")
            .into();
        #[expect(
            clippy::cast_possible_wrap,
            reason = "rest_key_len is encoded as u16 in test fixture"
        )]
        cursor
            .seek_relative(rest_key_len as i64)
            .expect("rest key skip in test fixture should succeed");
        usize::try_from(cursor.position())
            .expect("cursor position must fit into usize in test environment")
    }

    fn data_full_key_len_offset(bytes: &[u8]) -> usize {
        let mut cursor = Cursor::new(bytes);
        let _value_type = cursor
            .read_u8()
            .expect("test fixture encoding must contain a value type byte");
        let _seqno = cursor
            .read_u64_varint()
            .expect("test fixture encoding must contain a seqno varint");
        usize::try_from(cursor.position())
            .expect("cursor position must fit into usize in test environment")
    }

    fn data_full_value_len_offset(bytes: &[u8]) -> usize {
        let mut cursor = Cursor::new(bytes);
        let _value_type = cursor
            .read_u8()
            .expect("test fixture encoding must contain a value type byte");
        let _seqno = cursor
            .read_u64_varint()
            .expect("test fixture encoding must contain a seqno varint");
        let key_len: usize = cursor
            .read_u16_varint()
            .expect("test fixture encoding must contain a key-length varint")
            .into();
        #[expect(
            clippy::cast_possible_wrap,
            reason = "key_len is encoded as u16 in test fixture"
        )]
        cursor
            .seek_relative(key_len as i64)
            .expect("key skip in test fixture should succeed");
        usize::try_from(cursor.position())
            .expect("cursor position must fit into usize in test environment")
    }

    #[test]
    fn parse_full_rejects_restart_key_span_overlapping_trailer_region() {
        let mut bytes = make_full_tombstone_entry();
        let offset = 16;
        let entries_end = offset + bytes.len();
        let key_len_pos = data_full_key_len_offset(&bytes);
        *bytes
            .get_mut(key_len_pos)
            .expect("key_len_pos must point to an existing byte in test fixture") = 8;
        bytes.extend_from_slice(&[0u8; 32]);

        let mut cursor = Cursor::new(bytes.as_slice());
        let parsed = <InternalValue as Decodable<DataBlockParsedItem>>::parse_full(
            &mut cursor,
            offset,
            entries_end,
        );
        assert!(parsed.is_none());
    }

    #[test]
    fn parse_full_rejects_restart_value_span_overlapping_trailer_region() {
        let mut bytes = make_full_data_entry();
        let offset = 16;
        let entries_end = offset + bytes.len();
        let val_len_pos = data_full_value_len_offset(&bytes);
        *bytes
            .get_mut(val_len_pos)
            .expect("val_len_pos must point to an existing byte in test fixture") = 8;
        bytes.extend_from_slice(&[0u8; 32]);

        let mut cursor = Cursor::new(bytes.as_slice());
        let parsed = <InternalValue as Decodable<DataBlockParsedItem>>::parse_full(
            &mut cursor,
            offset,
            entries_end,
        );
        assert!(parsed.is_none());
    }

    #[test]
    fn parse_full_returns_none_for_unknown_value_type_byte() {
        let mut bytes = make_full_tombstone_entry();
        let value_type = bytes
            .get_mut(0)
            .expect("full entry fixture must contain value_type byte");
        *value_type = 5;

        let offset = 16;
        let entries_end = offset + bytes.len();
        let mut cursor = Cursor::new(bytes.as_slice());
        let parsed = <InternalValue as Decodable<DataBlockParsedItem>>::parse_full(
            &mut cursor,
            offset,
            entries_end,
        );
        assert!(parsed.is_none());
    }

    #[test]
    fn parse_truncated_rejects_prefix_span_crossing_restart_key_boundary() {
        let mut bytes = make_truncated_data_entry(2);
        let shared_len_pos = data_shared_prefix_len_offset(&bytes);
        *bytes
            .get_mut(shared_len_pos)
            .expect("shared_len_pos must point to an existing byte in test fixture") = 7;

        let offset = 16;
        let entries_end = offset + bytes.len();
        let mut cursor = Cursor::new(bytes.as_slice());
        let parsed = <InternalValue as Decodable<DataBlockParsedItem>>::parse_truncated(
            &mut cursor,
            offset,
            8,
            14,
            entries_end,
        );
        assert!(parsed.is_none());
    }

    #[test]
    fn parse_truncated_returns_none_for_unknown_value_type_byte() {
        let mut bytes = make_truncated_tombstone_entry(2);
        let value_type = bytes
            .get_mut(0)
            .expect("truncated entry fixture must contain value_type byte");
        *value_type = 5;

        let offset = 16;
        let entries_end = offset + bytes.len();
        let mut cursor = Cursor::new(bytes.as_slice());
        let parsed = <InternalValue as Decodable<DataBlockParsedItem>>::parse_truncated(
            &mut cursor,
            offset,
            8,
            14,
            entries_end,
        );
        assert!(parsed.is_none());
    }

    #[test]
    fn parse_truncated_rejects_key_span_crossing_block_boundary() {
        let mut bytes = make_truncated_tombstone_entry(2);
        let rest_len_pos = data_rest_key_len_offset(&bytes);
        *bytes
            .get_mut(rest_len_pos)
            .expect("rest_len_pos must point to an existing byte in test fixture") = 64;

        let offset = 16;
        let entries_end = offset + bytes.len();
        let mut cursor = Cursor::new(bytes.as_slice());
        let parsed = <InternalValue as Decodable<DataBlockParsedItem>>::parse_truncated(
            &mut cursor,
            offset,
            8,
            14,
            entries_end,
        );
        assert!(parsed.is_none());
    }

    #[test]
    fn parse_truncated_rejects_value_span_crossing_block_boundary() {
        let mut bytes = make_truncated_data_entry(2);
        let val_len_pos = data_value_len_offset(&bytes);
        *bytes
            .get_mut(val_len_pos)
            .expect("val_len_pos must point to an existing byte in test fixture") = 127;

        let offset = 16;
        let entries_end = offset + bytes.len();
        let mut cursor = Cursor::new(bytes.as_slice());
        let parsed = <InternalValue as Decodable<DataBlockParsedItem>>::parse_truncated(
            &mut cursor,
            offset,
            8,
            14,
            entries_end,
        );
        assert!(parsed.is_none());
    }

    #[test]
    fn parse_truncated_rejects_key_span_overlapping_trailer_region() {
        let mut bytes = make_truncated_tombstone_entry(2);
        let offset = 16;
        let entries_end = offset + bytes.len();
        let rest_len_pos = data_rest_key_len_offset(&bytes);
        *bytes
            .get_mut(rest_len_pos)
            .expect("rest_len_pos must point to an existing byte in test fixture") = 6;
        bytes.extend_from_slice(&[0u8; 32]);

        let mut cursor = Cursor::new(bytes.as_slice());
        let parsed = <InternalValue as Decodable<DataBlockParsedItem>>::parse_truncated(
            &mut cursor,
            offset,
            8,
            14,
            entries_end,
        );
        assert!(parsed.is_none());
    }

    #[test]
    fn parse_truncated_rejects_value_span_overlapping_trailer_region() {
        let mut bytes = make_truncated_data_entry(2);
        let offset = 16;
        let entries_end = offset + bytes.len();
        let val_len_pos = data_value_len_offset(&bytes);
        *bytes
            .get_mut(val_len_pos)
            .expect("val_len_pos must point to an existing byte in test fixture") = 8;
        bytes.extend_from_slice(&[0u8; 32]);

        let mut cursor = Cursor::new(bytes.as_slice());
        let parsed = <InternalValue as Decodable<DataBlockParsedItem>>::parse_truncated(
            &mut cursor,
            offset,
            8,
            14,
            entries_end,
        );
        assert!(parsed.is_none());
    }

    #[test]
    fn data_block_ping_pong_fuzz_1() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(
                Slice::from([111]),
                Slice::from([119]),
                8_602_264_972_526_186_597,
                Value,
            ),
            InternalValue::from_components(
                Slice::from([121, 120, 99]),
                Slice::from([101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101]),
                11_426_548_769_907,
                Value,
            ),
        ];

        let ping_pong_code = [1, 0];

        let bytes: Vec<u8> = DataBlock::encode_into_vec(&items, 1, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });

        let expected_ping_ponged_items = {
            let mut iter = items.iter();
            let mut v = vec![];

            for &x in &ping_pong_code {
                if x == 0 {
                    v.push(iter.next().cloned().expect("should have item"));
                } else {
                    v.push(iter.next_back().cloned().expect("should have item"));
                }
            }

            v
        };

        let real_ping_ponged_items = {
            let mut iter = data_block
                .iter(default_comparator())
                .map(|x| x.materialize(data_block.as_slice()));

            let mut v = vec![];

            for &x in &ping_pong_code {
                if x == 0 {
                    v.push(iter.next().expect("should have item"));
                } else {
                    v.push(iter.next_back().expect("should have item"));
                }
            }

            v
        };

        assert_eq!(expected_ping_ponged_items, real_ping_ponged_items);

        Ok(())
    }

    #[test]
    fn data_block_point_read_simple() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes: Vec<u8> = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            assert!(
                data_block
                    .point_read(b"a", SeqNo::MAX, &default_comparator())?
                    .is_none(),
                "should return None because a does not exist",
            );

            assert!(
                data_block
                    .point_read(b"b", SeqNo::MAX, &default_comparator())?
                    .is_some(),
                "should return Some because b exists",
            );

            assert!(
                data_block
                    .point_read(b"z", SeqNo::MAX, &default_comparator())?
                    .is_none(),
                "should return Some because z does not exist",
            );
        }

        Ok(())
    }

    #[test]
    fn data_block_point_read_one() -> crate::Result<()> {
        let items = [InternalValue::from_components(
            "pla:earth:fact",
            "eaaaaaaaaarth",
            0,
            crate::ValueType::Value,
        )];

        let bytes = DataBlock::encode_into_vec(&items, 16, 0.0)?;
        let serialized_len = bytes.len();

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert_eq!(data_block.inner.size(), serialized_len);
        assert_eq!(1, data_block.binary_index_len());

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, SeqNo::MAX, &default_comparator())?,
            );
        }

        assert_eq!(
            None,
            data_block.point_read(b"yyy", SeqNo::MAX, &default_comparator())?
        );

        Ok(())
    }

    #[test]
    fn data_block_vhandle() -> crate::Result<()> {
        let items = [InternalValue::from_components(
            "abc",
            "world",
            1,
            crate::ValueType::Indirection,
        )];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;
            let serialized_len = bytes.len();

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            assert_eq!(data_block.len(), items.len());
            assert_eq!(data_block.inner.size(), serialized_len);

            assert_eq!(
                Some(items[0].clone()),
                data_block.point_read(b"abc", 777, &default_comparator())?
            );
            assert!(
                data_block
                    .point_read(b"abc", 1, &default_comparator())?
                    .is_none()
            );
        }

        Ok(())
    }

    #[test]
    fn data_block_mvcc_read_first() -> crate::Result<()> {
        let items = [InternalValue::from_components(
            "hello",
            "world",
            0,
            crate::ValueType::Value,
        )];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;
            let serialized_len = bytes.len();

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            assert_eq!(data_block.len(), items.len());
            assert_eq!(data_block.inner.size(), serialized_len);

            assert_eq!(
                Some(items[0].clone()),
                data_block.point_read(b"hello", 777, &default_comparator())?
            );
        }

        Ok(())
    }

    #[test]
    fn data_block_point_read_fuzz_1() -> crate::Result<()> {
        let items = [
            InternalValue::from_components([0], b"", 23_523_531_241_241_242, Value),
            InternalValue::from_components([0], b"", 0, Value),
        ];

        let bytes = DataBlock::encode_into_vec(&items, 16, 1.33)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(
            data_block
                .hash_bucket_count()
                .expect("should have built hash index")
                > 0,
        );

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(
                    &needle.key.user_key,
                    needle.key.seqno + 1,
                    &default_comparator()
                )?,
            );
        }

        assert_eq!(
            None,
            data_block.point_read(b"yyy", SeqNo::MAX, &default_comparator())?
        );

        Ok(())
    }

    #[test]
    fn data_block_point_read_fuzz_2() -> crate::Result<()> {
        let items = [
            InternalValue::from_components([0], [], 5, Value),
            InternalValue::from_components([0], [], 4, Tombstone),
            InternalValue::from_components([0], [], 3, Value),
            InternalValue::from_components([0], [], 0, Value),
        ];

        let bytes = DataBlock::encode_into_vec(&items, 2, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().is_none());

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(
                    &needle.key.user_key,
                    needle.key.seqno + 1,
                    &default_comparator()
                )?,
            );
        }

        assert_eq!(
            None,
            data_block.point_read(b"yyy", SeqNo::MAX, &default_comparator())?
        );

        Ok(())
    }

    #[test]
    fn data_block_point_read_dense() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(b"a", b"a", 3, Value),
            InternalValue::from_components(b"b", b"b", 2, Value),
            InternalValue::from_components(b"c", b"c", 1, Value),
            InternalValue::from_components(b"d", b"d", 65, Value),
        ];

        let bytes = DataBlock::encode_into_vec(&items, 1, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert_eq!(4, data_block.binary_index_len());

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, SeqNo::MAX, &default_comparator())?,
            );
        }

        assert_eq!(
            None,
            data_block.point_read(b"yyy", SeqNo::MAX, &default_comparator())?
        );

        Ok(())
    }

    #[test]
    fn data_block_point_read_dense_mvcc_with_hash() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(b"a", b"a", 3, Value),
            InternalValue::from_components(b"a", b"a", 2, Value),
            InternalValue::from_components(b"a", b"a", 1, Value),
            InternalValue::from_components(b"b", b"b", 65, Value),
        ];

        let bytes = DataBlock::encode_into_vec(&items, 1, 1.33)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(
            data_block
                .hash_bucket_count()
                .expect("should have built hash index")
                > 0,
        );

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(
                    &needle.key.user_key,
                    needle.key.seqno + 1,
                    &default_comparator()
                )?,
            );
        }

        assert_eq!(
            None,
            data_block.point_read(b"yyy", SeqNo::MAX, &default_comparator())?
        );

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn data_block_point_read_mvcc_latest_fuzz_1() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(Slice::from([0]), Slice::from([]), 0, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 0, Value),
            InternalValue::from_components(
                Slice::from([255, 255, 0]),
                Slice::from([]),
                127_886_946_205_696,
                Tombstone,
            ),
        ];

        let bytes = DataBlock::encode_into_vec(&items, 2, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.get_hash_index_reader().is_none());

        assert_eq!(
            Some(items.get(1).cloned().unwrap()),
            data_block.point_read(&[233, 233], SeqNo::MAX, &default_comparator())?
        );
        assert_eq!(
            None,
            data_block.point_read(b"yyy", SeqNo::MAX, &default_comparator())?
        );

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn data_block_point_read_mvcc_latest_fuzz_2() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(Slice::from([0]), Slice::from([]), 0, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 8, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 7, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 6, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 5, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 4, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 3, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 2, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 1, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 0, Value),
            InternalValue::from_components(
                Slice::from([255, 255, 0]),
                Slice::from([]),
                127_886_946_205_696,
                Tombstone,
            ),
        ];

        let bytes = DataBlock::encode_into_vec(&items, 2, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });

        assert_eq!(data_block.len(), items.len());

        assert_eq!(
            Some(items.get(1).cloned().unwrap()),
            data_block.point_read(&[233, 233], SeqNo::MAX, &default_comparator())?
        );
        assert_eq!(
            Some(items.last().cloned().unwrap()),
            data_block.point_read(&[255, 255, 0], SeqNo::MAX, &default_comparator())?
        );
        assert_eq!(
            None,
            data_block.point_read(b"yyy", SeqNo::MAX, &default_comparator())?
        );

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn data_block_point_read_mvcc_latest_fuzz_3() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(Slice::from([0]), Slice::from([]), 0, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 8, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 7, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 6, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 5, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 4, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 3, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 2, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 1, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 0, Value),
            InternalValue::from_components(
                Slice::from([255, 255, 0]),
                Slice::from([]),
                127_886_946_205_696,
                Tombstone,
            ),
        ];

        let bytes = DataBlock::encode_into_vec(&items, 2, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });

        assert_eq!(data_block.len(), items.len());

        assert_eq!(
            Some(items.get(1).cloned().unwrap()),
            data_block.point_read(&[233, 233], SeqNo::MAX, &default_comparator())?
        );
        assert_eq!(
            Some(items.last().cloned().unwrap()),
            data_block.point_read(&[255, 255, 0], SeqNo::MAX, &default_comparator())?
        );
        assert_eq!(
            None,
            data_block.point_read(b"yyy", SeqNo::MAX, &default_comparator())?
        );

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn data_block_point_read_mvcc_latest_fuzz_3_dense() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(Slice::from([0]), Slice::from([]), 0, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 8, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 7, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 6, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 5, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 4, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 3, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 2, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 1, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 0, Value),
            InternalValue::from_components(
                Slice::from([255, 255, 0]),
                Slice::from([]),
                127_886_946_205_696,
                Tombstone,
            ),
        ];

        let bytes = DataBlock::encode_into_vec(&items, 1, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });

        assert_eq!(data_block.len(), items.len());

        assert_eq!(
            Some(items.get(1).cloned().unwrap()),
            data_block.point_read(&[233, 233], SeqNo::MAX, &default_comparator())?
        );
        assert_eq!(
            Some(items.last().cloned().unwrap()),
            data_block.point_read(&[255, 255, 0], SeqNo::MAX, &default_comparator())?
        );
        assert_eq!(
            None,
            data_block.point_read(b"yyy", SeqNo::MAX, &default_comparator())?
        );

        Ok(())
    }

    #[test]
    fn data_block_point_read_dense_mvcc_no_hash() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(b"a", b"a", 3, Value),
            InternalValue::from_components(b"a", b"a", 2, Value),
            InternalValue::from_components(b"a", b"a", 1, Value),
            InternalValue::from_components(b"b", b"b", 65, Value),
        ];

        let bytes = DataBlock::encode_into_vec(&items, 1, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().is_none());

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(
                    &needle.key.user_key,
                    needle.key.seqno + 1,
                    &default_comparator()
                )?,
            );
        }

        assert_eq!(
            None,
            data_block.point_read(b"yyy", SeqNo::MAX, &default_comparator())?
        );

        Ok(())
    }

    #[test]
    fn data_block_point_read_shadowing() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("pla:saturn:fact", "Saturn is pretty big", 0, Value),
            InternalValue::from_components("pla:saturn:name", "Saturn", 0, Value),
            InternalValue::from_components("pla:venus:fact", "", 1, Tombstone),
            InternalValue::from_components("pla:venus:fact", "Venus exists", 0, Value),
            InternalValue::from_components("pla:venus:name", "Venus", 0, Value),
        ];

        let bytes = DataBlock::encode_into_vec(&items, 16, 1.33)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(
            data_block
                .hash_bucket_count()
                .expect("should have built hash index")
                > 0,
        );

        assert!(
            data_block
                .point_read(b"pla:venus:fact", SeqNo::MAX, &default_comparator())?
                .expect("should exist")
                .is_tombstone()
        );

        Ok(())
    }

    #[test]
    fn data_block_point_read_dense_2() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("pla:earth:fact", "eaaaaaaaaarth", 0, Value),
            InternalValue::from_components("pla:jupiter:fact", "Jupiter is big", 0, Value),
            InternalValue::from_components("pla:jupiter:mass", "Massive", 0, Value),
            InternalValue::from_components("pla:jupiter:name", "Jupiter", 0, Value),
            InternalValue::from_components("pla:jupiter:radius", "Big", 0, Value),
            InternalValue::from_components("pla:saturn:fact", "Saturn is pretty big", 0, Value),
            InternalValue::from_components("pla:saturn:name", "Saturn", 0, Value),
            InternalValue::from_components("pla:venus:fact", "", 1, Tombstone),
            InternalValue::from_components("pla:venus:fact", "Venus exists", 0, Value),
            InternalValue::from_components("pla:venus:name", "Venus", 0, Value),
        ];

        let bytes = DataBlock::encode_into_vec(&items, 1, 1.33)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(
            data_block
                .hash_bucket_count()
                .expect("should have built hash index")
                > 0,
        );

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(
                    &needle.key.user_key,
                    needle.key.seqno + 1,
                    &default_comparator()
                )?,
            );
        }

        assert_eq!(
            None,
            data_block.point_read(b"yyy", SeqNo::MAX, &default_comparator())?
        );

        Ok(())
    }

    #[test]
    fn data_block_point_read_seqno_aware_seek() -> crate::Result<()> {
        // Key "a" with seqno 5,4,3,2,1 — point_read("a", seqno=3)
        // returns the first version with seqno < 3, i.e., v2 ("a2")
        let items = [
            InternalValue::from_components(b"a", b"a5", 5, Value),
            InternalValue::from_components(b"a", b"a4", 4, Value),
            InternalValue::from_components(b"a", b"a3", 3, Value),
            InternalValue::from_components(b"a", b"a2", 2, Value),
            InternalValue::from_components(b"a", b"a1", 1, Value),
        ];

        // Test across various restart intervals: at restart_interval=1 every item
        // is a restart head so binary search lands exactly; at larger intervals it
        // may scan within the restart range but must still return the correct version.
        for restart_interval in 1..=4 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            // seqno=4 → should see version with seqno=3 (first with seqno < 4)
            assert_eq!(
                Some(items[2].clone()),
                data_block.point_read(b"a", 4, &default_comparator())?,
                "restart_interval={restart_interval}: seqno=4 should return v3",
            );

            // seqno=3 → should see version with seqno=2
            assert_eq!(
                Some(items[3].clone()),
                data_block.point_read(b"a", 3, &default_comparator())?,
                "restart_interval={restart_interval}: seqno=3 should return v2",
            );

            // seqno=6 → should see latest version (seqno=5)
            assert_eq!(
                Some(items[0].clone()),
                data_block.point_read(b"a", 6, &default_comparator())?,
                "restart_interval={restart_interval}: seqno=6 should return v5",
            );

            // seqno=1 → no visible version (all seqno >= 1)
            assert!(
                data_block
                    .point_read(b"a", 1, &default_comparator())?
                    .is_none(),
                "restart_interval={restart_interval}: seqno=1 should return None",
            );

            // Non-existent key
            assert!(
                data_block
                    .point_read(b"b", SeqNo::MAX, &default_comparator())?
                    .is_none(),
                "restart_interval={restart_interval}: key 'b' should not exist",
            );
        }

        Ok(())
    }

    #[test]
    fn data_block_point_read_seqno_aware_seek_mixed_keys() -> crate::Result<()> {
        // Multiple keys with multiple versions
        let items = [
            InternalValue::from_components(b"a", b"a3", 3, Value),
            InternalValue::from_components(b"a", b"a2", 2, Value),
            InternalValue::from_components(b"a", b"a1", 1, Value),
            InternalValue::from_components(b"b", b"b5", 5, Value),
            InternalValue::from_components(b"b", b"b4", 4, Value),
            InternalValue::from_components(b"b", b"b3", 3, Value),
            InternalValue::from_components(b"b", b"b2", 2, Value),
            InternalValue::from_components(b"b", b"b1", 1, Value),
            InternalValue::from_components(b"c", b"c1", 1, Value),
        ];

        for restart_interval in 1..=4 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            // Read "b" at seqno=4 → should return version with seqno=3
            assert_eq!(
                Some(items[5].clone()),
                data_block.point_read(b"b", 4, &default_comparator())?,
                "restart_interval={restart_interval}: b@4 should return b3",
            );

            // Read "a" at seqno=2 → should return version with seqno=1
            assert_eq!(
                Some(items[2].clone()),
                data_block.point_read(b"a", 2, &default_comparator())?,
                "restart_interval={restart_interval}: a@2 should return a1",
            );

            // Read "c" at seqno=2 → should return version with seqno=1
            assert_eq!(
                Some(items[8].clone()),
                data_block.point_read(b"c", 2, &default_comparator())?,
                "restart_interval={restart_interval}: c@2 should return c1",
            );
        }

        Ok(())
    }

    #[test]
    fn try_iter_zero_restart_interval_returns_invalid_trailer() -> crate::Result<()> {
        use crate::table::block::Trailer;

        let items = vec![InternalValue::from_components(b"a", b"v", 0, Value)];
        let mut bytes = DataBlock::encode_into_vec(&items, 1, 0.0)?;

        let block = Block {
            data: bytes.clone().into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        };
        let trailer_offset = Trailer::new(&block).trailer_offset();
        bytes[trailer_offset] = 0;

        let corrupt_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });

        assert!(
            matches!(
                corrupt_block.try_iter(default_comparator()),
                Err(crate::Error::InvalidTrailer)
            ),
            "zero restart_interval must return InvalidTrailer",
        );

        Ok(())
    }

    #[test]
    fn point_read_zero_restart_interval_returns_invalid_trailer() -> crate::Result<()> {
        use crate::table::block::Trailer;

        let items = vec![InternalValue::from_components(b"a", b"v", 0, Value)];
        let mut bytes = DataBlock::encode_into_vec(&items, 1, 0.0)?;

        let block = Block {
            data: bytes.clone().into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        };
        let trailer_offset = Trailer::new(&block).trailer_offset();
        bytes[trailer_offset] = 0;

        let corrupt_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });

        assert!(
            matches!(
                corrupt_block.point_read(b"a", SeqNo::MAX, &default_comparator()),
                Err(crate::Error::InvalidTrailer)
            ),
            "point_read on corrupt block must return InvalidTrailer",
        );

        Ok(())
    }

    #[test]
    fn point_read_zero_restart_interval_hash_path_returns_invalid_trailer() -> crate::Result<()> {
        use crate::table::block::Trailer;

        // Use hash_index_ratio > 0 so the hash-index fast path is exercised.
        let items = vec![InternalValue::from_components(b"a", b"v", 0, Value)];
        let mut bytes = DataBlock::encode_into_vec(&items, 1, 1.33)?;

        let original_block = DataBlock::new(Block {
            data: bytes.clone().into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });
        assert!(
            original_block.hash_bucket_count().is_some(),
            "test fixture must build a hash index so point_read takes the hash fast path",
        );
        let trailer_offset = Trailer::new(&original_block.inner).trailer_offset();
        bytes[trailer_offset] = 0;

        let corrupt_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });

        // The upfront Decoder::try_new guard must reject the block before
        // get_hash_index_reader() / get_binary_index_reader() touch the
        // corrupt trailer.
        assert!(
            matches!(
                corrupt_block.point_read(b"a", SeqNo::MAX, &default_comparator()),
                Err(crate::Error::InvalidTrailer)
            ),
            "point_read with hash index on corrupt block must return InvalidTrailer",
        );

        Ok(())
    }
}

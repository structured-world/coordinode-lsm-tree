// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use crate::io::Cursor;
use crate::io::{ReadBytesExt, WriteBytesExt};
use crate::{SeqNo, UserKey};
use crate::{
    coding::{Decode, Encode},
    table::{
        block::{BlockOffset, Decodable, Encodable, TRAILER_START_MARKER, decoder::read_leb128},
        index_block::IndexBlockParsedItem,
        util::SliceIndexes,
    },
};
// `Seek` resolves to std under `std` (so `seek_relative` on `Cursor` comes
// from `std::io::Seek`) and to the native trait under `no_std`.
#[cfg(not(feature = "std"))]
use crate::io::Seek;
#[cfg(not(feature = "std"))]
use crate::io::{VarintReader, VarintWriter};
#[cfg(feature = "std")]
use std::io::Seek;
#[cfg(feature = "std")]
use varint_rs::{VarintReader, VarintWriter};

/// What an index entry says about the columnar row group it names, besides
/// its extent: the tag its directory must carry, and the on-disk lengths of
/// the directory and of the zone block after it, so a read that wants part of
/// the group reads exactly the directory first, or the directory and that
/// block together when it selects by their column.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct RowGroupRef {
    /// The tag the group's directory and every page stamp carry.
    pub tag: core::num::NonZeroU64,
    /// The directory's on-disk length, its block header included.
    pub directory_len: core::num::NonZeroU32,
    /// The on-disk length of the zone block between the directory and the
    /// pages, zero when the group has none.
    pub head_zones_len: u32,
}

/// Points to a block on file
#[derive(Copy, Clone, Debug, Default)]
pub struct BlockHandle {
    /// Position of block in file
    offset: BlockOffset,

    /// Size of block in bytes
    size: u32,

    /// The columnar row group the index entry names. Carried by the index
    /// entry, not by this handle's own encoding.
    row_group: Option<RowGroupRef>,
}

impl BlockHandle {
    #[must_use]
    pub fn new(offset: BlockOffset, size: u32) -> Self {
        Self {
            offset,
            size,
            row_group: None,
        }
    }

    /// This handle, naming the columnar row group `row_group`.
    #[must_use]
    pub fn with_row_group(mut self, row_group: Option<RowGroupRef>) -> Self {
        self.row_group = row_group;
        self
    }

    #[must_use]
    pub fn size(&self) -> u32 {
        self.size
    }

    #[must_use]
    pub fn offset(&self) -> BlockOffset {
        self.offset
    }

    /// The row group this handle names, when it came from a columnar table's
    /// index.
    #[must_use]
    pub fn row_group(&self) -> Option<RowGroupRef> {
        self.row_group
    }

    /// The tag of the row group this handle names.
    #[must_use]
    pub fn group_tag(&self) -> Option<core::num::NonZeroU64> {
        self.row_group.map(|group| group.tag)
    }
}

impl Encode for BlockHandle {
    fn encode_into<W: crate::io::Write>(&self, writer: &mut W) -> Result<(), crate::Error> {
        writer.write_u64_varint(*self.offset)?;
        writer.write_u32_varint(self.size)?;
        Ok(())
    }
}

impl Decode for BlockHandle {
    fn decode_from<R: crate::io::Read>(reader: &mut R) -> Result<Self, crate::Error>
    where
        Self: Sized,
    {
        let offset = reader.read_u64_varint()?;
        let size = reader.read_u32_varint()?;

        Ok(Self::new(BlockOffset(offset), size))
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

/// A full entry.
const FULL: u8 = 0;
/// An entry whose end key shares a prefix with its restart head's.
const TRUNCATED: u8 = 1;
/// [`FULL`], naming a columnar row group.
const TAGGED_FULL: u8 = 4;
/// [`TRUNCATED`], naming a columnar row group.
const TAGGED_TRUNCATED: u8 = 5;

impl KeyedBlockHandle {
    /// Writes the entry's marker (`plain`, or its tagged twin when the entry
    /// names a row group), its handle, its seqno and, when tagged, the group's
    /// tag, directory length and head zone block length.
    ///
    /// The tagged form belongs to columnar format
    /// [`COLUMNAR_FORMAT_VERSION`](crate::table::meta::COLUMNAR_FORMAT_VERSION)
    /// of the storage `V6` contract, which changes in place until `V6` is
    /// released.
    fn encode_head_into<W: crate::io::Write>(
        &self,
        writer: &mut W,
        plain: u8,
        tagged: u8,
    ) -> crate::Result<()> {
        writer.write_u8(if self.inner.row_group.is_some() {
            tagged
        } else {
            plain
        })?;
        self.inner.encode_into(writer)?;
        writer.write_u64_varint(self.seqno)?;
        if let Some(group) = self.inner.row_group {
            writer.write_u64_varint(group.tag.get())?;
            writer.write_u32_varint(group.directory_len.get())?;
            writer.write_u32_varint(group.head_zones_len)?;
        }
        Ok(())
    }
}

/// Reads the row group a tagged entry names after its seqno; `None` when it
/// cannot be read, or names a zero tag, an empty directory, or a directory and
/// head zone block longer than the group `size`, none of which a writer emits.
fn read_row_group(reader: &mut Cursor<&[u8]>, size: u32) -> Option<RowGroupRef> {
    let tag = core::num::NonZeroU64::new(reader.read_u64_varint().ok()?)?;
    let directory_len = core::num::NonZeroU32::new(reader.read_u32_varint().ok()?)?;
    let head_zones_len = reader.read_u32_varint().ok()?;
    (directory_len.get().checked_add(head_zones_len)? <= size).then_some(RowGroupRef {
        tag,
        directory_len,
        head_zones_len,
    })
}

impl Encodable<BlockOffset> for KeyedBlockHandle {
    fn encode_full_into<W: crate::io::Write>(
        &self,
        writer: &mut W,
        state: &mut BlockOffset,
    ) -> crate::Result<()> {
        // Full entry (marker 0, or 4 naming a row group):
        // [marker] [offset] [size] [seqno] [group tag, directory len; marker 4] [key len] [end key]
        // 1        2        3      4       5                                    6         7
        //
        // Per-block seqno bounds are NOT stored inline; they live in the
        // optional parallel `seqno_bounds` section keyed by block offset, so a
        // point read never pays for them. See `crate::table::seqno_bounds`.
        self.encode_head_into(writer, FULL, TAGGED_FULL)?; // 1-5

        #[expect(clippy::cast_possible_truncation, reason = "keys are u16 long max")]
        writer.write_u16_varint(self.end_key.len() as u16)?; // 6
        writer.write_all(&self.end_key)?; // 7

        *state = BlockOffset(*self.offset() + u64::from(self.size()));

        Ok(())
    }

    // TODO: see https://github.com/structured-world/coordinode-lsm-tree/issues/184
    fn encode_truncated_into<W: crate::io::Write>(
        &self,
        writer: &mut W,
        _state: &mut BlockOffset,
        shared_len: usize,
    ) -> crate::Result<()> {
        // Truncated entry (marker 1, or 5 naming a row group):
        // [marker] [offset] [size] [seqno] [group tag, directory len, head zones len; marker 5] [shared prefix len] [rest key len] [rest key]
        // 1        2        3      4       5                                                    6                   7              8
        //
        // Per-block seqno bounds live in the parallel `seqno_bounds` section,
        // never inline (see `encode_full_into`).
        self.encode_head_into(writer, TRUNCATED, TAGGED_TRUNCATED)?;

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
        let marker = reader.read_u8().ok()?;

        // Markers 2/3 were the pre-release inline seqno-bounds entries; this
        // build never writes them and there is no on-disk compat obligation, so
        // a 2/3 here means a stale-format SST or a reader/writer mismatch. Assert
        // against them (debug-only, zero release cost) so that case surfaces
        // instead of silently ending iteration like the trailer. Genuine
        // corruption is still rejected gracefully as `None` below (and caught
        // upstream by the index block's checksum).
        debug_assert!(
            marker != 2 && marker != 3,
            "stale inline seqno-bounds marker {marker} in index full entry (pre-release format, unsupported)"
        );
        if marker == TRAILER_START_MARKER {
            return None;
        }
        // Full entries are marker 0, or 4 when they name a row group. (Seqno
        // bounds are no longer inline; they live in the parallel
        // `seqno_bounds` section.)
        if marker != FULL && marker != TAGGED_FULL {
            return None;
        }

        let handle = BlockHandle::decode_from(reader).ok()?;
        let seqno = reader.read_u64_varint().ok()?;
        let row_group = if marker == TAGGED_FULL {
            Some(read_row_group(reader, handle.size())?)
        } else {
            None
        };

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
        let offset_i64 = key_len as i64;
        if key_start > entries_end {
            return None;
        }
        let key_end = key_start.checked_add(key_len)?;
        if key_end > entries_end {
            return None;
        }
        reader.seek_relative(offset_i64).ok()?;

        Some(IndexBlockParsedItem {
            prefix: None,
            end_key: SliceIndexes(key_start, key_end),
            offset: handle.offset(),
            size: handle.size(),
            seqno,
            row_group,
        })
    }

    fn parse_restart_key<'a>(
        reader: &mut Cursor<&[u8]>,
        offset: usize,
        data: &'a [u8],
        entries_end: usize,
    ) -> Option<(&'a [u8], SeqNo)> {
        // Slice-based decode: read directly from the cursor's buffer through a
        // local index instead of `Cursor::read_u8` (a `read_exact` of one byte
        // each), then advance the cursor once at the end. This is the hottest
        // function on the point-read path (index restart-key probe).
        let buf: &[u8] = reader.get_ref();
        let mut pos = usize::try_from(reader.position()).ok()?;

        let marker = *buf.get(pos)?;
        pos += 1;
        // Markers 2/3 were the pre-release inline seqno-bounds entries (no
        // on-disk compat); a 2/3 restart head means a stale-format SST or a
        // reader/writer mismatch. Assert against them so that case surfaces
        // instead of a silent miss. `debug_assert` is a no-op in release, so the
        // point-read hot path pays nothing; genuine corruption is still rejected
        // gracefully as `None` below (and caught upstream by the checksum).
        debug_assert!(
            marker != 2 && marker != 3,
            "stale inline seqno-bounds marker {marker} in index restart head (pre-release format, unsupported)"
        );
        if marker == TRAILER_START_MARKER {
            return None;
        }
        // Restart heads are full entries: marker 0, or 4 naming a row group.
        let tagged = marker == TAGGED_FULL;
        if marker != FULL && !tagged {
            return None;
        }

        // The binary-search probe only needs `(key, seqno)`, but the block
        // handle (file offset + size) is still decoded so a corrupt restart
        // head fails the probe instead of feeding forged metadata into
        // restart-table navigation: read_leb128 rejects an overlong offset,
        // and the size must fit u32 exactly as `parse_full` / `parse_truncated`
        // require.
        let (_file_offset, np) = read_leb128!(buf, pos);
        pos = np;
        let (size, np) = read_leb128!(buf, pos);
        let size = u32::try_from(size).ok()?;
        pos = np;
        let (seqno, np) = read_leb128!(buf, pos);
        pos = np;
        if tagged {
            // The probe does not need the row group, but one the entry cannot
            // carry fails it here, as `parse_full` would.
            let (tag, np) = read_leb128!(buf, pos);
            if tag == 0 {
                return None;
            }
            pos = np;
            let (directory_len, np) = read_leb128!(buf, pos);
            pos = np;
            let (head_zones_len, np) = read_leb128!(buf, pos);
            pos = np;
            let head = directory_len.checked_add(head_zones_len)?;
            if directory_len == 0 || head_zones_len > u64::from(u32::MAX) || head > u64::from(size)
            {
                return None;
            }
        }

        let (key_len_raw, np) = read_leb128!(buf, pos);
        pos = np;
        // key_len is encoded as a u16 varint on the wire; reject an overlong
        // value exactly as `read_u16_varint` did.
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

    fn parse_truncated(
        reader: &mut Cursor<&[u8]>,
        offset: usize,
        base_key_offset: usize,
        base_key_end: usize,
        entries_end: usize,
    ) -> Option<IndexBlockParsedItem> {
        let marker = reader.read_u8().ok()?;

        // Markers 2/3 were the pre-release inline seqno-bounds entries; this
        // build never writes them and there is no on-disk compat obligation, so
        // a 2/3 here means a stale-format SST or a reader/writer mismatch. Assert
        // against them (debug-only, zero release cost) so that case surfaces
        // instead of silently ending iteration like the trailer. Genuine
        // corruption is still rejected gracefully as `None` below (and caught
        // upstream by the index block's checksum).
        debug_assert!(
            marker != 2 && marker != 3,
            "stale inline seqno-bounds marker {marker} in index truncated entry (pre-release format, unsupported)"
        );
        if marker == TRAILER_START_MARKER {
            return None;
        }

        // Truncated entries are marker 1, or 5 when they name a row group.
        // (Seqno bounds live in the parallel `seqno_bounds` section, never
        // inline.)
        if marker != TRUNCATED && marker != TAGGED_TRUNCATED {
            return None;
        }

        let handle = BlockHandle::decode_from(reader).ok()?;
        let seqno = reader.read_u64_varint().ok()?;
        let row_group = if marker == TAGGED_TRUNCATED {
            Some(read_row_group(reader, handle.size())?)
        } else {
            None
        };

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
            row_group,
        })
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test code")]
mod tests;

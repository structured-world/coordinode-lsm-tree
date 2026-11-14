// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The bytes a rewrite of entries takes as tables written with one writer's
//! settings, bounded from above before any of them is written.
//!
//! The bound starts from the entries as they are, uncompressed, whatever the
//! tables they come from were written under: the destination may store them
//! without compression, or under a codec that grows them, since a block keeps
//! its compressed form even when it came out larger than its input. Every
//! block is counted at the codec's worst case and its encryption and parity
//! overheads (see [`framed_len_bound`]).

use super::Writer;
use crate::CompressionType;
use crate::table::block::{BlockType, TRAILER_LEN, framed_len_bound};
use alloc::vec::Vec;

/// What a rewrite puts through the table writers, as the bound counts it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RewriteLoad {
    /// Entries written, every version.
    pub(crate) entries: u64,
    /// Bytes of their user keys.
    pub(crate) key_bytes: u64,
    /// Bytes of their values.
    pub(crate) value_bytes: u64,
    /// Hashes their filters register: a key each, and the prefixes a prefix
    /// extractor yields of it.
    pub(crate) filter_hashes: u64,
    /// The longest user key, or range tombstone bound, among them.
    pub(crate) longest_key: u64,
    /// The longest value among them, when it is known: it bounds the
    /// statistics a columnar group keeps of its values. Without it they are
    /// bounded by the values' bytes.
    pub(crate) longest_value: Option<u64>,
    /// The highest seqno among them, which bounds the varint a block entry
    /// stores its seqno, or its delta from its restart head's, in.
    pub(crate) highest_seqno: u64,
    /// The bytes the varints of their key lengths (twice: a shared prefix
    /// and the rest of the key are each no longer) and value lengths take,
    /// when they are known; at the widest without.
    pub(crate) entry_lengths: Option<u64>,
    /// Range tombstones the tables carry.
    pub(crate) range_tombstones: u64,
    /// Bytes of their start and end keys.
    pub(crate) range_tombstone_bytes: u64,
    /// Blob files the tables' rows point into.
    pub(crate) linked_blob_files: u64,
    /// Blob objects the tables' cell rows own.
    pub(crate) owned_blob_objects: u64,
    /// For a columnar table whose rows may be written as cells: the most
    /// columns one of its row groups holds, and the bytes those columns add
    /// to each row besides the row's key and value bytes. `None` for the
    /// intrinsic layout, whose four columns the writer knows.
    pub(crate) cell_columns: Option<CellColumns>,
}

/// The columns a row group of rows written as cells holds: how many, and the
/// bytes they add to every row of the group, the rows that hold no cell in a
/// column included.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct CellColumns {
    pub(crate) count: u64,
    pub(crate) row_bytes: u64,
}

/// The field columns rows written as cells may open in a row group, gathered
/// from the rows: each field id and the bytes its column adds to every row of
/// the group, and whether a references column is opened.
///
/// A field's column takes its type's width in each row, zeros where the row
/// holds no cell there, or an offset into the bytes it holds. A row whose
/// fields do not fit the group's columns is kept whole instead and opens none,
/// so every field the rows hold, at the widest type it comes with, bounds the
/// columns of any group of them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct CellShapes {
    widths: alloc::collections::BTreeMap<u16, u8>,
    refs: bool,
}

/// What a bytes column, and the whole-value and references columns, add to
/// each row of a group: an offset.
const BYTES_OFFSET: u8 = 4;

impl CellShapes {
    /// Takes in the fields the cell row `value` names, read off its header
    /// (its field count, its reference and owner bitmaps and one descriptor
    /// per field) without its cells; a value whose header does not parse is
    /// kept whole and opens no column.
    pub(crate) fn take_row(&mut self, value: &[u8]) {
        self.take_row_past(value, &Self::default());
    }

    /// Takes in what the cell row `value` names that `known` does not hold
    /// already, as [`Self::take_row`] reads it; `true` when any of it was
    /// taken, which widens a group holding the rows of both.
    pub(crate) fn take_row_past(&mut self, value: &[u8], known: &Self) -> bool {
        let Some((count, rest)) = value.split_first_chunk::<2>() else {
            return false;
        };
        let count = usize::from(u16::from_le_bytes(*count));
        let bitmap = count.div_ceil(8);
        let Some((refs, rest)) = rest.split_at_checked(bitmap) else {
            return false;
        };
        let Some(descriptors) = rest.get(bitmap..).and_then(|rest| rest.get(..count * 4)) else {
            return false;
        };
        let mut grew = false;
        for (field, descriptor) in descriptors.as_chunks::<4>().0.iter().enumerate() {
            let [c0, c1, _, width] = *descriptor;
            if refs
                .get(field / 8)
                .is_some_and(|byte| byte & (1 << (field % 8)) != 0)
            {
                if !known.refs && !self.refs {
                    self.refs = true;
                    grew = true;
                }
            } else {
                let column = u16::from_le_bytes([c0, c1]);
                let wide = if width == 0 { BYTES_OFFSET } else { width };
                if known.widths.get(&column).is_none_or(|&had| had < wide)
                    && self.widths.get(&column).is_none_or(|&had| had < wide)
                {
                    self.take_column(column, width);
                    grew = true;
                }
            }
        }
        grew
    }

    /// Takes in the field column `column` of a type of `width` bytes, `0`
    /// for one of variable width, as a column's wire form names it.
    pub(crate) fn take_column(&mut self, column: u16, width: u8) {
        let width = if width == 0 { BYTES_OFFSET } else { width };
        let slot = self.widths.entry(column).or_insert(width);
        *slot = (*slot).max(width);
    }

    /// Takes in the references column.
    #[cfg(feature = "columnar")]
    pub(crate) fn take_refs(&mut self) {
        self.refs = true;
    }

    /// Takes `other` in.
    pub(crate) fn absorb(&mut self, other: &Self) {
        for (&column, &width) in &other.widths {
            let slot = self.widths.entry(column).or_insert(width);
            *slot = (*slot).max(width);
        }
        self.refs |= other.refs;
    }

    /// The bytes [`Self::encode`] writes: a field column more grows them, a
    /// wider type or the references column does not.
    #[cfg(feature = "columnar")]
    pub(crate) fn encoded_len(&self) -> usize {
        1 + 3 * self.widths.len()
    }

    /// The wire form a table records: whether a references column is opened,
    /// then each field column as its little-endian id and the bytes it adds
    /// to a row.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + 3 * self.widths.len());
        out.push(u8::from(self.refs));
        for (&column, &width) in &self.widths {
            out.extend_from_slice(&column.to_le_bytes());
            out.push(width);
        }
        out
    }

    /// Reads the wire form [`Self::encode`] writes; `None` when it is not one.
    pub(crate) fn decode(bytes: &[u8]) -> Option<Self> {
        let (&refs, columns) = bytes.split_first()?;
        let (columns, []) = columns.as_chunks::<3>() else {
            return None;
        };
        Some(Self {
            refs: match refs {
                0 => false,
                1 => true,
                _ => return None,
            },
            widths: columns
                .iter()
                .map(|&[c0, c1, width]| (u16::from_le_bytes([c0, c1]), width))
                .collect(),
        })
    }

    /// The columns of a group holding the rows of this and of `other`, without
    /// taking `other` in; `None` when none of them opens a field column or a
    /// references column, which leaves the four intrinsic ones.
    pub(crate) fn columns_with(&self, other: &Self) -> Option<CellColumns> {
        let mut fields = self.widths.len() as u64;
        let mut field_bytes: u64 = self.widths.values().copied().map(u64::from).sum();
        for (column, &width) in &other.widths {
            if let Some(&mine) = self.widths.get(column) {
                // The column takes the wider of the two.
                field_bytes += u64::from(width.max(mine) - mine);
            } else {
                fields += 1;
                field_bytes += u64::from(width);
            }
        }
        let refs = self.refs || other.refs;
        if fields == 0 && !refs {
            return None;
        }
        // The key's offset, the seqno, the value type and the whole value's
        // offset, as in the intrinsic layout, then the references' offset and
        // the field columns.
        let refs = u64::from(refs);
        Some(CellColumns {
            count: INTRINSIC_COLUMNS + fields + refs,
            row_bytes: INTRINSIC_ROW_BYTES + refs * u64::from(BYTES_OFFSET) + field_bytes,
        })
    }
}

/// The bytes `value` takes as a varint.
pub(crate) const fn varint_len(value: u64) -> u64 {
    let bits = 64 - value.leading_zeros() as u64;
    if bits == 0 { 1 } else { bits.div_ceil(7) }
}

impl RewriteLoad {
    /// Takes in one entry of `key_len` and `value_len` bytes at `seqno`,
    /// registering `filter_hashes` hashes in its table's filter.
    #[inline]
    pub(crate) fn take_entry(
        &mut self,
        key_len: u64,
        value_len: u64,
        seqno: crate::SeqNo,
        filter_hashes: u64,
    ) {
        let lengths = 2 * varint_len(key_len) + varint_len(value_len);
        self.entry_lengths = Some(match (self.entry_lengths, self.entries) {
            (Some(known), _) => known + lengths,
            (None, 0) => lengths,
            (None, entries) => entries * WIDEST_ENTRY_LENGTHS + lengths,
        });
        self.entries += 1;
        self.key_bytes += key_len;
        self.value_bytes += value_len;
        self.filter_hashes += filter_hashes;
        self.longest_key = self.longest_key.max(key_len);
        self.longest_value = Some(self.longest_value.unwrap_or(0).max(value_len));
        self.highest_seqno = self.highest_seqno.max(seqno);
    }

    /// The bytes the varints of the entries' key and value lengths take.
    fn entry_length_bytes(&self) -> u64 {
        self.entry_lengths
            .unwrap_or(self.entries * WIDEST_ENTRY_LENGTHS)
    }

    /// Takes `other` in.
    pub(crate) fn absorb(&mut self, other: &Self) {
        // An empty load knows nothing of the entries it does not hold.
        self.entry_lengths = match (self.entry_lengths, other.entry_lengths) {
            (Some(a), Some(b)) => Some(a + b),
            (mine, _) if other.entries == 0 => mine,
            (_, theirs) if self.entries == 0 => theirs,
            _ => Some(self.entry_length_bytes() + other.entry_length_bytes()),
        };
        self.longest_value = match (self.longest_value, other.longest_value) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (mine, _) if other.entries == 0 => mine,
            (_, theirs) if self.entries == 0 => theirs,
            _ => None,
        };
        self.entries += other.entries;
        self.key_bytes += other.key_bytes;
        self.value_bytes += other.value_bytes;
        self.filter_hashes += other.filter_hashes;
        self.longest_key = self.longest_key.max(other.longest_key);
        self.highest_seqno = self.highest_seqno.max(other.highest_seqno);
        self.range_tombstones += other.range_tombstones;
        self.range_tombstone_bytes += other.range_tombstone_bytes;
        self.linked_blob_files += other.linked_blob_files;
        self.owned_blob_objects += other.owned_blob_objects;
        self.cell_columns = match (self.cell_columns, other.cell_columns) {
            (Some(a), Some(b)) => Some(CellColumns {
                count: a.count.max(b.count),
                row_bytes: a.row_bytes.max(b.row_bytes),
            }),
            (a, b) => a.or(b),
        };
    }
}

/// A data block entry's shared prefix and key lengths and its value length,
/// each varint at its widest (see the data block's `Encodable`).
const WIDEST_ENTRY_LENGTHS: u64 = 3 + 3 + 5;

/// A restart pointer in a block's binary index, at its widest.
const RESTART_POINTER: u64 = 4;

/// A per-KV checksum digest at its widest, and the footer's tail.
const KV_DIGEST: u64 = 8;
const KV_FOOTER_TAIL: u64 = crate::table::block::kv_checksum::FOOTER_TAIL_LEN as u64;

/// The most bytes an index entry takes besides its end key: its marker, its
/// offset, size and seqno, a row group's tag and lengths, its key length and
/// its restart pointer (see `KeyedBlockHandle::encoded_len_bound`).
const INDEX_ENTRY_HEADER: u64 = 1 + 10 + 5 + 10 + (10 + 5 + 5) + 3 + 4;

/// A range tombstone's bytes besides its two keys: its seqno and the keys'
/// lengths, each varint at its widest.
const RANGE_TOMBSTONE_HEADER: u64 = 10 + 3 + 3;

/// The bytes an intrinsic columnar row adds besides its key and value: an
/// offset each for the key and the value, the seqno and the value type (see
/// `columnar::entry_row_bytes`).
const INTRINSIC_ROW_BYTES: u64 = 4 + 8 + 1 + 4;

/// The intrinsic layout's columns: key, seqno, value type and value.
const INTRINSIC_COLUMNS: u64 = 4;

/// The most bytes a column page takes besides its rows: its stamp, the
/// column's header (id, type, width, validity flag, values length), its
/// encoding tag, the bytes column's extra offset and a validity bitmap's
/// partial byte. A chosen encoding never stores more than the plain one,
/// whose cost it is measured against.
#[cfg(feature = "columnar")]
const PAGE_OVERHEAD: u64 =
    crate::table::column_page::PageStamp::LEN as u64 + (2 + 1 + 1 + 1 + 5) + 1 + 4 + 1;

/// A row group directory besides its per-page and per-zone-block entries:
/// version, tag, counts, head zone length and column.
#[cfg(feature = "columnar")]
const DIRECTORY_FIXED: u64 = 1 + 8 + 3 + 3 + 3 + 5 + 2;

/// One statistics zone at its widest: null count, flags and two bounds of at
/// most `ZONE_BOUND_LEN` bytes, each with its shared and suffix lengths.
#[cfg(feature = "columnar")]
const ZONE_LEN: u64 = 5 + 1 + 2 * (2 + ZONE_BOUND_LEN);

/// The most bytes a zone's bound keeps.
#[cfg(feature = "columnar")]
const ZONE_BOUND_LEN: u64 = crate::table::column_page::ZONE_BOUND_LEN as u64;

/// A zone block besides its zones: the group tag, the column count and one
/// column id.
#[cfg(feature = "columnar")]
const ZONE_BLOCK_FIXED: u64 = 8 + 3 + 2;

/// The inner zstd frame a data block is split into, whose ends the block
/// layout section records.
#[cfg(zstd_any)]
const ZSTD_INNER_BLOCK: u64 = 128 * 1024;

impl Writer {
    /// Bytes `tables` tables written with this writer's settings take
    /// together when they hold `load`, bounded from above; the meta payload
    /// each table ends with is `meta_without_keys` besides its keys (see
    /// [`Self::meta_payload_len`]).
    pub(crate) fn rewrite_bound(
        &self,
        load: &RewriteLoad,
        tables: u64,
        meta_without_keys: u64,
    ) -> u64 {
        let tables = tables.max(1);
        let encryption = self.encryption.as_deref();
        let none = CompressionType::None;
        // A block of `payload` bytes as written, and the frames of `blocks`
        // blocks holding `payload` bytes together. Every frame's overheads are
        // a constant past a part that grows no faster than its payload, so the
        // sum is the frame of the whole and a frame of one byte for every
        // block past the first.
        let framed = |payload: u64, block_type, compression| {
            framed_len_bound(payload, block_type, compression, encryption, self.ecc)
        };
        // Every count of blocks passed is at least the tables', so one.
        let frames = |payload: u64, blocks: u64, block_type, compression| {
            debug_assert!(blocks > 0, "a count of blocks is at least one");
            framed(payload, block_type, compression)
                + (blocks - 1) * framed(1, block_type, compression)
        };

        let longest = load.longest_key;

        // Data blocks, or columnar row groups, and their index entries.
        #[cfg(feature = "columnar")]
        let (data, index_entries) = if self.use_columnar {
            self.columnar_bound(load, tables, &frames)
        } else {
            self.row_bound(load, tables, &frames)
        };
        #[cfg(not(feature = "columnar"))]
        let (data, index_entries) = self.row_bound(load, tables, &frames);

        // The block index: one block per table, written twice (at the head
        // and as the tail mirror), or partitions and a top-level index written
        // twice. An adaptive index takes either, so both are bounded.
        let index_codec = self.index_block_compression;
        // Each entry holds one key whole, a different entry's for each, so
        // the keys of any set of entries take no more than all keys do.
        let keys_of = |entries: u64| (entries * longest).min(load.key_bytes);
        let index_payload = index_entries * INDEX_ENTRY_HEADER + keys_of(index_entries);
        let full_index = 2 * frames(
            index_payload + tables * TRAILER_LEN as u64,
            tables,
            BlockType::Index,
            index_codec,
        );
        let partitions = index_payload / u64::from(self.index_partition_size.max(1)) + tables;
        let partitioned_index = frames(
            index_payload + partitions * TRAILER_LEN as u64,
            partitions,
            BlockType::Index,
            index_codec,
        ) + 2 * frames(
            partitions * INDEX_ENTRY_HEADER + keys_of(partitions) + tables * TRAILER_LEN as u64,
            tables,
            BlockType::Index,
            index_codec,
        );
        let index = full_index.max(partitioned_index);

        // The filter: one per table, or partitions and their top-level index.
        // A filter's size grows with its hashes at no more than a linear rate
        // past constant floors and a term concave in them, so `n` filters over
        // `h` hashes in all take at most `n` filters of the even share and the
        // floors once more each.
        let filter = if self.filtered {
            let policy = self
                .filter_sizing
                .as_ref()
                .map_or(self.bloom_policy, |plan| plan.bound_policy());
            let size =
                |n: u64| policy.filter_size_bound(usize::try_from(n).unwrap_or(usize::MAX)) as u64;
            let filters = |count: u64| {
                count
                    * (framed(
                        size(load.filter_hashes.div_ceil(count)),
                        BlockType::Filter,
                        none,
                    ) + size(1))
            };
            let whole = filters(tables);
            if self.partitioned_filter {
                let filter_partitions = size(load.filter_hashes)
                    / u64::from(self.filter_partition_size.max(1))
                    + tables;
                let partitioned = filters(filter_partitions)
                    + frames(
                        filter_partitions * INDEX_ENTRY_HEADER
                            + keys_of(filter_partitions)
                            + tables * TRAILER_LEN as u64,
                        tables,
                        BlockType::Index,
                        index_codec,
                    );
                whole.max(partitioned)
            } else {
                whole
            }
        } else {
            0
        };

        // The sections a table carries past its blocks, each a block of its
        // own per table, uncompressed. Their in-memory sizes bound their
        // encodings, as the writer counts them as it goes.
        let data_blocks = index_entries;
        let mut sections = 0u64;
        if self.use_seqno_in_index {
            sections +=
                data_blocks * core::mem::size_of::<(super::BlockOffset, (u64, u64))>() as u64;
        }
        if self.use_zone_map {
            let stats = core::mem::size_of::<crate::table::zone_map::ColumnStats>() as u64;
            let slot = core::mem::size_of::<(
                super::BlockOffset,
                Vec<crate::table::zone_map::ColumnStats>,
            )>() as u64;
            sections += if self.use_columnar {
                // A group's stats per column, its byte columns' bounds whole:
                // two keys, and two values of the group, which together take
                // at most twice its values when the longest one is not known.
                // A field column's bounds are pieces of its rows' values.
                let cells = self.cell_columns(load);
                let columns = cells.map_or(INTRINSIC_COLUMNS, |cells| cells.count);
                // Each value column's bounds are a cell of it at the most,
                // which is no longer than the value it came from: the value
                // column, or each column past the key, seqno and value type
                // of a group of cells.
                let value_columns = cells.map_or(1, |cells| cells.count - 3);
                let values = match load.longest_value {
                    Some(longest) => {
                        (data_blocks * value_columns * 2 * longest).min(2 * load.value_bytes)
                    }
                    None => 2 * load.value_bytes,
                };
                data_blocks * (slot + columns * stats) + 2 * keys_of(data_blocks) + values
            } else {
                data_blocks * (slot + stats) + 2 * keys_of(data_blocks)
            };
        }
        #[cfg(zstd_any)]
        if matches!(self.data_block_compression, CompressionType::Zstd(_)) {
            // An entry per data block cut into several inner frames, and an
            // end per frame.
            let slot = core::mem::size_of::<(super::BlockOffset, Vec<u32>)>() as u64;
            let entry_bytes = load.key_bytes + load.value_bytes;
            sections += data_blocks * (slot + 4) + 4 * (entry_bytes / ZSTD_INNER_BLOCK);
        }
        if self.delete_strategy != crate::config::DeleteStrategy::CopyOnWrite {
            // A row per entry at most: a container per chunk of rows, dense
            // past a few hundred of them, and the set's count.
            let chunks = load.entries / u64::from(crate::table::delete_bitmap::CHUNK_ROWS) + tables;
            sections += tables * 4 + chunks * (4 + 1 + 8 * 32);
        }
        if load.range_tombstones > 0 {
            // A table holds the pieces of the tombstones its keys meet, at
            // most every tombstone. A piece clipped to the table ends where
            // the tombstone does or at a key of the outputs, at most a byte
            // past the longest key (an exclusive end past the last one).
            let clipped_ends = load.range_tombstones * 2 * (load.longest_key + 1);
            sections += tables
                * (8 + load.range_tombstone_bytes
                    + clipped_ends
                    + load.range_tombstones * RANGE_TOMBSTONE_HEADER);
        }
        if load.linked_blob_files > 0 {
            sections += tables
                * (4 + load.linked_blob_files
                    * (super::LINKED_BLOB_FILE_FIXED_LEN as u64 + 4 + 2 * longest));
        }
        if load.owned_blob_objects > 0 {
            // Each table's count, and every object once, in the table that
            // owns it.
            let count = super::owned_blob_objects_len(0);
            sections += tables * count
                + load.owned_blob_objects * (super::owned_blob_objects_len(1) - count);
        }
        let section_blocks = tables * 8;
        let sections = frames(sections, section_blocks, BlockType::Meta, none);

        // The locator: a retrieval ribbon over the keys, sized like a filter.
        let locator = match self.locator {
            Some(spec) => {
                let per_table = |n: u64| {
                    crate::table::locator::section_size_estimate(
                        usize::try_from(n).unwrap_or(usize::MAX),
                        spec,
                        data_blocks,
                        load.entries,
                    )
                    .map_or(0, |len| len as u64)
                };
                tables
                    * (framed(
                        per_table(load.entries.div_ceil(tables)),
                        BlockType::Locator,
                        none,
                    ) + per_table(1))
            }
            None => 0,
        };

        // Every table's tail: its version byte, separator, table of contents
        // and two meta copies, and the pieces a table of a single entry holds
        // at the least, which the totals above may leave out of a small one.
        let (per_table, _) = self.tombstone_only_overhead_with(meta_without_keys, longest, 0);

        data + index + filter + sections + locator + tables * per_table
    }

    /// The field columns `load` opens, when this writer splits cell rows into
    /// them; any other layout keeps every value whole.
    fn cell_columns(&self, load: &RewriteLoad) -> Option<CellColumns> {
        load.cell_columns
            .filter(|_| self.value_layout == Some(crate::table::meta::ValueLayout::Cells))
    }

    /// Whether this writer's filter takes prefixes of the keys.
    pub(crate) fn has_prefix_extractor(&self) -> bool {
        self.prefix_extractor.is_some()
    }

    /// Hashes one entry of `key` registers in this writer's filter, at the
    /// most: the key, and each prefix its extractor yields of it.
    pub(crate) fn filter_hashes_bound(&self, key: &[u8]) -> u64 {
        1 + self
            .prefix_extractor
            .as_ref()
            .map_or(0, |extractor| extractor.prefixes(key).count() as u64)
    }

    /// Hashes the entries of a table recording `meta` register in this
    /// writer's filter, at the most, read off what the table records: its
    /// distinct keys without a prefix extractor; the hashes its own filter
    /// holds when that filter indexes this writer's extractor; otherwise a
    /// hash per entry and one per key byte, a prefix being at least a byte
    /// longer than the one before it.
    pub(crate) fn filter_hashes_of(&self, meta: &crate::table::meta::ParsedMeta) -> u64 {
        let entries = meta.item_count;
        let Some(extractor) = &self.prefix_extractor else {
            return meta.key_count.unwrap_or(entries);
        };
        let per_byte = || {
            entries
                + meta
                    .sum_user_key_bytes
                    .unwrap_or_else(|| entries * u64::from(u16::MAX))
        };
        match (meta.prefix_extractor.as_deref(), meta.filter_hashes) {
            (Some(name), Some(hashes)) if name == extractor.name().as_bytes() => hashes,
            _ => per_byte(),
        }
    }

    /// The row-major data blocks `load` takes in `tables` tables, and how
    /// many there are, which is the count of index entries.
    fn row_bound(
        &self,
        load: &RewriteLoad,
        tables: u64,
        frames: &impl Fn(u64, u64, BlockType, CompressionType) -> u64,
    ) -> (u64, u64) {
        let entries = load.entries;
        let entry_bytes = load.key_bytes + load.value_bytes;
        // A block closes once its keys and values reach the block size, after
        // one entry at the least, and each table's last one may be short.
        let blocks = (entry_bytes / u64::from(self.data_block_size.max(1))).min(entries) + tables;
        let restarts = entries / u64::from(self.data_block_restart_interval.max(1)) + blocks;
        let hash = if self.hash_indexed {
            // A block's buckets: its entries at the ratio, rounded up, at
            // least one: the product truncated and one more per block.
            let ratio = f64::from(self.data_block_hash_ratio);
            #[expect(
                clippy::cast_precision_loss,
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "an entry count times a small ratio, truncated"
            )]
            let buckets = (entries as f64 * ratio) as u64;
            buckets + 2 * blocks
        } else {
            0
        };
        let kv = if self.kv_checksum.is_some() {
            entries * KV_DIGEST + blocks * KV_FOOTER_TAIL
        } else {
            0
        };
        // An entry's value type, its seqno or its zigzagged delta from its
        // restart head's (no more than twice the highest seqno), and the
        // varints of its lengths.
        let seqno = load
            .highest_seqno
            .checked_mul(2)
            .map_or(10, |delta| varint_len(delta + 1));
        let payload = entry_bytes
            + entries * (1 + seqno)
            + load.entry_length_bytes()
            + restarts * RESTART_POINTER
            + hash
            + kv
            + blocks * TRAILER_LEN as u64;
        (
            frames(
                payload,
                blocks,
                BlockType::Data,
                self.data_block_compression,
            ),
            blocks,
        )
    }

    /// The columnar row groups `load` takes in `tables` tables, and how many
    /// there are, which is the count of index entries.
    #[cfg(feature = "columnar")]
    fn columnar_bound(
        &self,
        load: &RewriteLoad,
        tables: u64,
        frames: &impl Fn(u64, u64, BlockType, CompressionType) -> u64,
    ) -> (u64, u64) {
        let entries = load.entries;
        let (columns, column_row_bytes) = self
            .cell_columns(load)
            .map_or((INTRINSIC_COLUMNS, INTRINSIC_ROW_BYTES), |cells| {
                (cells.count, cells.row_bytes)
            });
        // A row's bytes as a group counts them; a validity bit per column.
        let row_bytes = load.key_bytes + load.value_bytes + entries * column_row_bytes;
        let rows_payload = row_bytes + (entries * columns).div_ceil(8);
        // Groups and their row pages are cut by their sizes, after a row at
        // the least, each group's and each table's last one short.
        let groups = (row_bytes / u64::from(self.row_group_size.max(1))).min(entries) + tables;
        let row_pages =
            (row_bytes / u64::from(self.columnar_page_size.max(1))).min(entries) + groups;
        let pages = row_pages * columns;
        let page_payload = rows_payload + pages * PAGE_OVERHEAD;
        let directory = groups * (DIRECTORY_FIXED + columns * (3 + 7)) + row_pages * 5 + pages * 5;
        // A group of several row pages holds a zone per row page and column,
        // its bounds cut to `ZONE_BOUND_LEN` and no longer than the column's
        // values: a key, a seqno and a value type in the intrinsic columns.
        let zone = |bound: u64| ZONE_LEN - 2 * (ZONE_BOUND_LEN - bound.min(ZONE_BOUND_LEN));
        let zones_per_row_page =
            zone(load.longest_key) + zone(8) + zone(1) + (columns - 3) * ZONE_LEN;
        let zones = groups * columns * ZONE_BLOCK_FIXED + row_pages * zones_per_row_page;
        let plain_blocks = groups * (1 + columns);
        (
            frames(
                page_payload,
                pages,
                BlockType::ColumnPage,
                self.data_block_compression,
            ) + frames(
                directory + zones,
                plain_blocks,
                BlockType::ColumnPageDirectory,
                CompressionType::None,
            ),
            groups,
        )
    }
}

#[cfg(test)]
mod tests;

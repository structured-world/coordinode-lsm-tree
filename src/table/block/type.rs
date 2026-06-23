// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

/// The mutually-exclusive ROLE of a block within a table file.
///
/// `BlockType` answers "what is this block and which parser decodes
/// its logical structure" — not "how was its payload post-processed".
/// Orthogonal, composable transform layers (compression, encryption,
/// Reed-Solomon ECC, per-KV checksum footer) are NOT roles: they
/// stack on top of a block of any role and are recorded in the block
/// header's `block_flags` bitfield, not here. A compressed, encrypted,
/// per-KV-checked data block is still `BlockType::Data` with three
/// transform bits set.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BlockType {
    /// User key/value entries (the SST data payload).
    Data,
    /// Block handles (offset/size/key/seqno) pointing at data blocks.
    Index,
    /// Bloom / Ribbon filter bit arrays.
    Filter,
    /// Sorted key/value table properties (the SST metadata section).
    Meta,
    /// Range tombstone entries, written as a separate block.
    RangeTombstone,
    /// One section of the Blocks-based manifest (e.g. `tables`,
    /// `blob_files`, `format_version`). Payload is the raw section
    /// bytes that the manifest writer puts there. All Block-level
    /// protections (XXH3, optional ECC, optional AEAD) apply, so
    /// the section gains bit-rot defence + (optional) encryption /
    /// recovery automatically.
    Manifest,
    /// Footer Block of the Blocks-based manifest. Payload carries
    /// `manifest_layout_version`, flags, section count, and the
    /// table of contents (TOC) for the surrounding `Manifest`
    /// section Blocks. Written last (primary read target) and
    /// mirrored at file offset 0 in a 4 KiB padded region for
    /// partial-write recovery.
    ManifestFooter,
    /// Optional per-table index of inner zstd-block layouts: for each data
    /// block that compressed into >= 2 inner zstd blocks, its file offset and
    /// the cumulative decompressed END offsets of those inner blocks. Lets a
    /// range query partial-decode only the inner blocks covering a key range.
    /// Absent unless the table has at least one such multi-inner-block block.
    BlockLayout,
    /// Optional per-table retrieval-ribbon locator section: maps each key to a
    /// packed `(block_id, slot)` for O(1) point reads. Present by default (block
    /// precision); absent when the level's locator policy is disabled or the
    /// per-SST ribbon could not be built.
    Locator,
    /// Optional per-table seqno-bounds section: maps each data block's file
    /// offset to its `[seqno_min, seqno_max]`, powering the `scan_since_seqno`
    /// block-skip without bloating the index entries. Absent unless
    /// `seqno_in_index` is on.
    SeqnoBounds,
    /// Optional per-table zone-map section: maps each data block's file offset
    /// to per-column `(min, max, null_count, row_count)` statistics, letting a
    /// range / analytical scan skip a block whose stats prove no predicate
    /// match without decoding it. Kept parallel to the index (like
    /// [`Self::SeqnoBounds`]) so point reads never load it. Off by default;
    /// absent unless the zone-map policy is enabled.
    ZoneMap,
    /// A PAX / rowgroup columnar block: a row-group laid out as per-column
    /// chunks rather than row-major key/value entries. Each chunk is a typed,
    /// codec-tagged opaque byte array plus a validity bitmap. Produced by the
    /// transpose on flush / major-compaction for a columnar tree / CF and read
    /// as a `ColumnBatch`. The wire tag is reserved unconditionally; the codec
    /// framework that fills these blocks is built only with the `columnar`
    /// feature.
    Columnar,
    /// Optional per-table positional delete-bitmap section: marks, by row
    /// position, which rows of the table's columnar segment are deleted. A pure
    /// membership set (MVCC reconciled at materialization via the compaction
    /// watermark), applied as a mask at scan time. Kept parallel to the data
    /// (like [`Self::ZoneMap`]) so a read without deletes pays nothing. Absent
    /// unless the segment has materialized deletes.
    DeleteBitmap,
}

// Wire tags are renumbered contiguously `0..=6` for V5. The previous
// non-contiguous numbering (Manifest/ManifestFooter at 7/8, with holes left
// by the removed `DataKvChecked` / `FilterKvChecked` variants) is NOT kept:
// V5 is a pre-release on-disk format (block magic bumped to `[L,S,M,4]`, see
// `crate::file::MAGIC_BYTES`), so any in-development V5 artifact is
// regenerated rather than migrated, and released pre-V5 tables are already
// rejected at the magic check before a BlockType byte is ever read. No
// compatibility shim or format-discriminator bump is added for the renumber.
impl From<BlockType> for u8 {
    fn from(val: BlockType) -> Self {
        match val {
            BlockType::Data => 0,
            BlockType::Index => 1,
            BlockType::Filter => 2,
            BlockType::Meta => 3,
            BlockType::RangeTombstone => 4,
            BlockType::Manifest => 5,
            BlockType::ManifestFooter => 6,
            BlockType::BlockLayout => 7,
            BlockType::Locator => 8,
            BlockType::SeqnoBounds => 9,
            BlockType::ZoneMap => 10,
            BlockType::Columnar => 11,
            BlockType::DeleteBitmap => 12,
        }
    }
}

impl TryFrom<u8> for BlockType {
    type Error = crate::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Data),
            1 => Ok(Self::Index),
            2 => Ok(Self::Filter),
            3 => Ok(Self::Meta),
            4 => Ok(Self::RangeTombstone),
            5 => Ok(Self::Manifest),
            6 => Ok(Self::ManifestFooter),
            7 => Ok(Self::BlockLayout),
            8 => Ok(Self::Locator),
            9 => Ok(Self::SeqnoBounds),
            10 => Ok(Self::ZoneMap),
            11 => Ok(Self::Columnar),
            12 => Ok(Self::DeleteBitmap),
            _ => Err(crate::Error::InvalidTag(("BlockType", value))),
        }
    }
}

#[expect(clippy::expect_used, reason = "test code")]
#[cfg(test)]
mod tests;

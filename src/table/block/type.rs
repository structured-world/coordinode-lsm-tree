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
            _ => Err(crate::Error::InvalidTag(("BlockType", value))),
        }
    }
}

#[expect(clippy::expect_used, reason = "test code")]
#[cfg(test)]
mod tests {
    use super::BlockType;

    #[test]
    fn block_type_wire_tags_roundtrip_all_variants() {
        // Every variant must survive a u8 -> BlockType -> u8 round-trip
        // on its locked, contiguous wire tag. Per-KV checking is a
        // transform flag (header block_flags), not a block role, so
        // there is no checked-twin variant here — a checked data block
        // is BlockType::Data with the KV_CHECKSUM_FOOTER bit set.
        for (tag, variant) in [
            (0u8, BlockType::Data),
            (1, BlockType::Index),
            (2, BlockType::Filter),
            (3, BlockType::Meta),
            (4, BlockType::RangeTombstone),
            (5, BlockType::Manifest),
            (6, BlockType::ManifestFooter),
            (7, BlockType::BlockLayout),
        ] {
            assert_eq!(
                u8::from(variant),
                tag,
                "{variant:?} must encode to wire tag {tag}"
            );
            assert_eq!(
                BlockType::try_from(tag).expect("known tag must decode"),
                variant,
                "wire tag {tag} must decode to {variant:?}"
            );
        }
    }

    #[test]
    fn block_type_rejects_unknown_wire_tag() {
        // Forward-incompatibility guard: a tag this build doesn't know
        // (newer writer, older reader) must surface as a typed error,
        // not a silent coercion to a known variant. 8 is the first
        // unused tag past the contiguous range.
        assert!(BlockType::try_from(8).is_err());
        assert!(BlockType::try_from(255).is_err());
    }
}

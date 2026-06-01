// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BlockType {
    Data,
    Index,
    Filter,
    Meta,
    RangeTombstone,
    /// Data block carrying a per-entry checksum trailer region (per-KV
    /// protection, V5-3). Wire-tag 5. Layout extends [`Self::Data`]
    /// with a `kv_checksums_offset` prefix and a trailing
    /// `kv_checksums_array`; on read the reader verifies each entry's
    /// checksum so a RAM bit-flip that slipped past the block-level
    /// checksum (corrupted in the memtable before the block checksum
    /// was computed) is caught per entry rather than silently served.
    /// A reader that decodes tag 5 MUST take the verifying path — never
    /// fold it into the [`Self::Data`] fast path.
    DataKvChecked,
    /// Filter block carrying per-region checksums (per-KV protection,
    /// V5-3). Wire-tag 6. Each filter layer's bit array gets its own
    /// checksum so a bit-flip surfaces as a per-layer verify failure
    /// (fail-open to a Data read) instead of a silent false-negative
    /// ("key absent" when present).
    FilterKvChecked,
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
}

impl From<BlockType> for u8 {
    fn from(val: BlockType) -> Self {
        match val {
            BlockType::Data => 0,
            BlockType::Index => 1,
            BlockType::Filter => 2,
            BlockType::Meta => 3,
            BlockType::RangeTombstone => 4,
            BlockType::DataKvChecked => 5,
            BlockType::FilterKvChecked => 6,
            // Manifest blocks take 7 and 8 to avoid collision with the
            // per-KV checked variants (5, 6) in shipped V5 files.
            BlockType::Manifest => 7,
            BlockType::ManifestFooter => 8,
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
            5 => Ok(Self::DataKvChecked),
            6 => Ok(Self::FilterKvChecked),
            7 => Ok(Self::Manifest),
            8 => Ok(Self::ManifestFooter),
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
        // on its locked wire tag. The per-KV-checked variants (5, 6) are
        // the focus: an SST written with DataKvChecked / FilterKvChecked
        // must decode back to the same variant, never silently fold into
        // the plain Data / Filter fast path (which would skip per-entry
        // verification and defeat the RAM bit-flip defence).
        for (tag, variant) in [
            (0u8, BlockType::Data),
            (1, BlockType::Index),
            (2, BlockType::Filter),
            (3, BlockType::Meta),
            (4, BlockType::RangeTombstone),
            (5, BlockType::DataKvChecked),
            (6, BlockType::FilterKvChecked),
            (7, BlockType::Manifest),
            (8, BlockType::ManifestFooter),
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
        // not a silent coercion to a known variant.
        assert!(BlockType::try_from(9).is_err());
        assert!(BlockType::try_from(255).is_err());
    }
}

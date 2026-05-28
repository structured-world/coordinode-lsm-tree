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
            // Wire tags 5 and 6 are reserved for the parallel V5-3
            // per-KV protection feature (`DataKvChecked` /
            // `FilterKvChecked`); manifest blocks take 7 and 8 to
            // avoid collision in shipped V5 files.
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
            // 5, 6 reserved for V5-3 per-KV checked variants.
            7 => Ok(Self::Manifest),
            8 => Ok(Self::ManifestFooter),
            _ => Err(crate::Error::InvalidTag(("BlockType", value))),
        }
    }
}

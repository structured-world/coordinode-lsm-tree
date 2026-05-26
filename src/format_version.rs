// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

/// Disk format version
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum FormatVersion {
    /// Version for 1.x.x releases
    V1 = 1,

    /// Version for 2.x.x releases
    V2,

    /// Version for 3.x.x releases
    V3,

    /// Version for range-tombstone SST semantics
    V4,

    /// Two on-disk changes shipped together in this format version
    /// (V5 had not been released when both landed, so they collapse
    /// into the same version bump):
    ///
    /// 1. `BuRR` (Bumped Ribbon Retrieval) filter wire format. Filter
    ///    blocks are no longer Bloom-encoded; the `filter_type` byte +
    ///    per-layer header layout is documented in
    ///    `src/table/filter/ribbon/burr/wire.rs`.
    ///
    /// 2. Per-block Reed-Solomon Page ECC: the block header gains a
    ///    `ecc_length: u32` field and the on-disk block can carry a
    ///    Reed-Solomon parity trailer immediately after the
    ///    XXH3-covered payload bytes. When `Config::page_ecc(false)`
    ///    (the default), `ecc_length = 0` and no parity bytes
    ///    follow. The block magic was bumped to `[L,S,M,4]`
    ///    (was `[L,S,M,3]` on pre-V5 versions) so a pre-V5 reader
    ///    that bypasses the manifest gate fails fast at block header
    ///    decode rather than misreading the moved field as checksum
    ///    bytes.
    ///
    /// V3 / V4 ↔ V5 incompatibility is enforced primarily by the
    /// manifest version gate at `Tree::open` (returns
    /// `InvalidVersion` for anything other than V5).
    V5,
}

impl std::fmt::Display for FormatVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", u8::from(*self))
    }
}

impl From<FormatVersion> for u8 {
    fn from(value: FormatVersion) -> Self {
        match value {
            FormatVersion::V1 => 1,
            FormatVersion::V2 => 2,
            FormatVersion::V3 => 3,
            FormatVersion::V4 => 4,
            FormatVersion::V5 => 5,
        }
    }
}

impl TryFrom<u8> for FormatVersion {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::V1),
            2 => Ok(Self::V2),
            3 => Ok(Self::V3),
            4 => Ok(Self::V4),
            5 => Ok(Self::V5),
            _ => Err(()),
        }
    }
}

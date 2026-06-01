// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

/// Block / SST disk format version.
///
/// This enum tracks the on-disk layout of Blocks and SST files: block
/// header layout, filter wire format, range-tombstone encoding, ECC
/// trailer geometry. It is the version persisted in the manifest's
/// `format_version` section and gated at `Tree::open`.
///
/// ## Relationship to the manifest layout version
///
/// `FormatVersion` and [`crate::manifest_blocks::MANIFEST_LAYOUT_VERSION_V1`]
/// evolve at **independent cadences**:
///
/// | Concept | Type | Tracks |
/// |---------|------|--------|
/// | `FormatVersion` | This enum (V1..V5) | Block / SST on-disk layout |
/// | `manifest_layout_version` | `u8` in manifest Footer Block | Manifest file structure (footer fields, TOC encoding, head-mirror geometry) |
///
/// A block format bump does NOT force a manifest layout bump and
/// vice versa. The CURRENT pointer's canonical digest binds the
/// manifest layout version (so a manifest-only break is detected
/// at recovery), and the manifest's `format_version` section binds
/// this enum (so a block-format-only break is detected at
/// `Tree::open`).
///
/// ## Amendment policy
///
/// Once a value is **published to crates.io** (any released binary
/// writes that value to disk), **any** subsequent change to the
/// on-disk bytes under that value is a breaking change that MUST
/// bump to a new variant. This applies regardless of whether the
/// change is otherwise additive: a reader running the old code is
/// not free to interpret unknown bytes.
///
/// The amendment window is the **pre-release period**: while a
/// `FormatVersion` is being actively developed and no published
/// binary writes it, the on-disk bytes under that version MAY be
/// amended in place (no enum bump required). The release that
/// crystallises the variant ends this window.
///
/// Same rule applies to `manifest_layout_version` independently:
/// pre-publication amendments are free; post-publication changes
/// require a new layout-version constant.
///
/// **Practical checklist for any PR that touches on-disk bytes:**
///
/// 1. Identify which layer the change touches (Block/SST → this
///    enum; manifest framing → `manifest_layout_version`).
/// 2. If that layer's current value has shipped to crates.io,
///    add a new variant / constant instead of amending in place.
/// 3. The OTHER layer's value stays unless its layer also changed.
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
    /// 2. Per-block transform flags + Page ECC. The self-describing block
    ///    types (`Meta` / `Manifest` / `ManifestFooter`) carry a
    ///    `block_flags: u8` byte with the transform-presence bits;
    ///    `ECC_PARITY` marks that a Reed-Solomon parity trailer follows
    ///    the XXH3-covered payload (its length is derived from
    ///    `data_length`, not stored). SST block types (`Data` / `Index` /
    ///    `Filter` / `RangeTombstone`) keep the compact header WITHOUT this
    ///    byte: their parity / per-KV-footer presence is a per-SST property
    ///    read from the table descriptor (`page_ecc` / `kv_checksum_algo`),
    ///    not a serialized header flag. `KV_CHECKSUM_FOOTER` (set on the
    ///    self-describing types) marks a per-entry checksum footer.
    ///    When `Config::page_ecc(false)` (the default) no parity bytes
    ///    follow; likewise no footer unless per-KV checksums are enabled.
    ///    The block
    ///    magic was bumped to `[L,S,M,4]` (was `[L,S,M,3]` on pre-V5
    ///    versions) so a pre-V5 reader that bypasses the manifest gate
    ///    fails fast at block header decode rather than misreading the
    ///    new layout.
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

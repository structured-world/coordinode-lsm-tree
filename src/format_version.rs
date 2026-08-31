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
/// ## Supported versions
///
/// **V5 is the ONLY supported on-disk format.** The engine neither reads
/// nor migrates pre-V5 layouts: there are no legacy decode paths, no
/// upgrade tooling, and no backward-compat variations anywhere in the
/// codebase. Discriminants 1–4 are reserved history — opening a tree that
/// carries one fails with [`crate::Error::InvalidVersion`], at whichever gate
/// sees the format first: a V1 directory is caught by its `version` marker
/// file before any manifest is read, while V2–V4 are caught by this enum's
/// `TryFrom` while the manifest is being decoded. The same single-format rule
/// applies to every subsidiary format (blob frames, manifest layout): each has
/// exactly one readable shape, the one the current writer emits.
///
/// The retired discriminants are reserved as NUMBERS, not as names: this enum
/// carries no `V1`–`V4` variants. Keeping them as deprecated stubs would add
/// four public names that no file can ever decode into and that exist only to
/// keep a downstream exhaustive `match` compiling — a compatibility shim for a
/// layout the engine deliberately cannot read. A caller matching on this enum
/// should be matching what the writer emits, and that is one shape.
///
/// This crate offers no upgrade path and plans none: a pre-V5 database is not
/// adopted, converted or repaired here — it fails at the format gate above and
/// stays that way. That is a statement about THIS engine, not about the data:
/// the store is still readable by the engine that wrote it, which is where a
/// conversion would have to happen.
///
/// What the refusal buys is that recovery, salvage, patrol scrub and verify
/// have no second layout to reason about: every one of them can assume the
/// shape the current writer emits, with no branch for a shape it might also
/// have to accept.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum FormatVersion {
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
    /// Pre-V5 ↔ V5 incompatibility is enforced primarily by the
    /// manifest version gate at `Tree::open` (returns
    /// `InvalidVersion` for anything other than V5).
    V5 = 5,
}

impl core::fmt::Display for FormatVersion {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", u8::from(*self))
    }
}

impl From<FormatVersion> for u8 {
    fn from(value: FormatVersion) -> Self {
        match value {
            FormatVersion::V5 => 5,
        }
    }
}

impl TryFrom<u8> for FormatVersion {
    type Error = ();

    /// Only the V5 discriminant decodes. Discriminants 1–4 named retired
    /// formats no shipped reader supports; they fail here so the manifest
    /// gate reports `InvalidVersion` instead of any code path pretending
    /// a legacy layout is readable.
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            5 => Ok(Self::V5),
            _ => Err(()),
        }
    }
}

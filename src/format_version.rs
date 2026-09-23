// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

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
/// | `FormatVersion` | This enum (V1..V6) | Block / SST on-disk layout and the version snapshot's records |
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
/// **V6 is the ONLY supported on-disk format.** The engine neither reads
/// nor migrates earlier layouts: there are no legacy decode paths and no
/// backward-compat variations anywhere in the engine. Discriminants 1–5 are
/// reserved history: opening a tree that carries one always fails, at
/// whichever gate notices first, and which error comes back depends on which
/// that is. A V1 directory is caught by its `version` marker file in
/// `Tree::open` before any manifest is read. Pre-V5 manifests are usually
/// caught earlier still, by the Blocks manifest reader: their framing is not
/// the one it decodes, so they fail at the footer rather than at a version
/// field. This `TryFrom` is the gate for the remaining shape: a manifest that
/// frames like the current one but declares a version this engine does not
/// write, a V5 manifest among them, which fails with
/// [`crate::Error::InvalidVersion`]. The same single-format rule applies to
/// every subsidiary format (blob frames, manifest layout): each has exactly
/// one readable shape, the one the current writer emits.
///
/// The retired discriminants are reserved as NUMBERS, not as names: this enum
/// carries no `V1`–`V5` variants. Keeping them as deprecated stubs would add
/// public names that no file can ever decode into and that exist only to keep
/// a downstream exhaustive `match` compiling — a compatibility shim for a
/// layout the engine deliberately cannot read. A caller matching on this enum
/// should be matching what the writer emits, and that is one shape.
///
/// The engine offers no upgrade path: an earlier database is not adopted,
/// converted or repaired here — it fails at the format gate above and stays
/// that way. A V5 store is converted by the separate offline converter, which
/// carries the V5 decoder so the engine does not have to.
///
/// What the refusal buys is that recovery, salvage, patrol scrub and verify
/// have no second layout to reason about: every one of them can assume the
/// shape the current writer emits, with no branch for a shape it might also
/// have to accept.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum FormatVersion {
    /// The 6.0 layout. Relative to V5, which 5.x releases write:
    ///
    /// 1. The version snapshot's `tables` section holds one framed record per
    ///    table, each naming its level and run, and no level, run or table
    ///    counts. V5 stored a level's run count in a byte and silently
    ///    truncated it past 255 runs, leaving a written tree unopenable.
    ///
    /// 2. The `BuRR` filter and retrieval-locator sections use the packed
    ///    `r`-bit solution layout (`src/table/filter/ribbon/burr/wire.rs`).
    ///
    /// Carried over from V5 unchanged: per-block transform flags and Page ECC
    /// on the self-describing block types (`Meta` / `Manifest` /
    /// `ManifestFooter`), the compact header on SST block types, and the
    /// `[L,S,M,4]` block magic.
    ///
    /// V5 ↔ V6 incompatibility is enforced by the manifest version gate at
    /// `Tree::open`, which returns `InvalidVersion` for anything other than
    /// V6.
    V6 = 6,
}

impl core::fmt::Display for FormatVersion {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", u8::from(*self))
    }
}

impl From<FormatVersion> for u8 {
    fn from(value: FormatVersion) -> Self {
        match value {
            FormatVersion::V6 => 6,
        }
    }
}

impl TryFrom<u8> for FormatVersion {
    type Error = ();

    /// Only the V6 discriminant decodes. Discriminants 1–5 named retired
    /// formats this engine does not read; they fail here so the manifest
    /// gate reports `InvalidVersion` instead of any code path pretending
    /// a legacy layout is readable.
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            6 => Ok(Self::V6),
            _ => Err(()),
        }
    }
}

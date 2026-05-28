// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Blocks-based manifest framing (V5-2, #297).
//!
//! Replaces the upstream `sfa` sectioned-archive file format for the
//! per-version manifest files (`v{N}`). Each manifest is a sequence
//! of standard lsm-tree [`Block`](crate::table::block::Block)s:
//!
//! ```text
//! file layout (manifest_layout_version = 1):
//!   [0 .. HEAD_FOOTER_RESERVED_SIZE]   head footer mirror (4 KiB,
//!                                       zero-padded; populated only when
//!                                       runtime `manifest_footer_mirror`
//!                                       is enabled)
//!   [HEAD_FOOTER_RESERVED_SIZE ..]     section Block 0
//!                                       section Block 1
//!                                       ...
//!                                       section Block N
//!   [.. EOF]                            tail footer Block (primary read
//!                                       target; carries the TOC of
//!                                       section offsets and the manifest
//!                                       layout version)
//! ```
//!
//! All Block-level protections (XXH3-128 checksum, optional ECC, optional
//! AEAD) apply through the standard [`Block::write_into`] /
//! [`Block::from_reader`] pipeline.
//! Manifest gets bit-rot defence + (optional) encryption + (optional)
//! single-block recovery "for free" by reusing existing infrastructure.
//!
//! Section names mirror the previous sfa archive's section names so
//! existing callers in [`crate::Manifest`] / [`crate::version::recovery`]
//! see the same logical surface during the migration; only the underlying
//! framing changes.
//!
//! [`Block::write_into`]: crate::table::block::Block::write_into
//! [`Block::from_reader`]: crate::table::block::Block::from_reader

pub mod current_digest;
pub mod footer;
pub mod reader;
pub mod writer;

/// Manifest file layout version carried in the footer payload.
///
/// Bumped only when the manifest file layout itself evolves
/// (footer fields, TOC encoding, head-mirror geometry); decoupled
/// from the crate-level [`crate::FormatVersion`] which tracks
/// block / SST layout.
pub const MANIFEST_LAYOUT_VERSION_V1: u8 = 1;

/// Fixed-size reservation at file offset 0 for the head footer mirror.
///
/// 4 KiB matches typical filesystem block size and page-alignment
/// for direct-IO compatibility.
///
/// Hard limit on footer Block size — see footer encode path for
/// the safety-net check that rejects payloads that would overflow
/// this region. Hitting that limit signals a writer bug or forged
/// manifest, not a legitimate capacity exhaustion: realistic
/// production manifests use ~5% of the reserved space.
pub const HEAD_FOOTER_RESERVED_SIZE: u64 = 4 * 1024;

/// Footer payload flag: bit 0 indicates the head mirror at file
/// offset 0 was populated by the writer.
///
/// When clear, readers skip the head-fallback path on tail-verify
/// failure.
pub const FLAG_FOOTER_MIRROR_ENABLED: u8 = 1 << 0;

/// Hard cap on the on-disk size of a single manifest section Block.
///
/// Realistic production manifests carry KB-scale sections (table
/// list, blob-file list, format metadata); the largest plausible
/// section is the `tables` block on a heavily-populated tree, which
/// still sits comfortably under 16 MiB even with thousands of
/// tables. Capping here keeps the reader from ever allocating a
/// multi-hundred-MiB buffer driven by a forged or corrupted TOC.
///
/// Bumped only when `manifest_layout_version` changes — increasing
/// it is additive (older readers reject the bigger block as
/// oversized, newer readers accept it).
pub const MAX_MANIFEST_BLOCK_SIZE: u32 = 16 * 1024 * 1024;

/// Size in bytes of the trailing footer-size pointer.
///
/// Written at the very end of every manifest file (a little-endian
/// `u32`). The reader reads these last 4 bytes first to discover
/// the footer Block's on-disk size, then seeks to
/// `file_len - 4 - size` to position itself at the footer Block
/// start. Without this hint the reader would have to scan backwards
/// through the file looking for the footer's magic header.
pub const TAIL_FOOTER_SIZE_HINT_BYTES: u64 = 4;

/// Maximum length in bytes of a section name.
///
/// The UTF-8 bytes stored in each TOC entry. Generous cap that
/// holds every name the current writer emits (`format_version`,
/// `tree_type`, `level_count`, `filter_hash_type`,
/// `comparator_name`, `tables`, `blob_files`, `blob_gc_stats`) with
/// room to spare for additive growth. Hitting this cap signals a
/// programming error rather than a legitimate need; bump in a
/// layout-version-2 if real production names ever approach it.
pub const MAX_SECTION_NAME_BYTES: usize = 64;

/// AAD tree-id sentinel used for manifest Blocks: `u64::MAX`.
///
/// The manifest file lives at the per-tree folder level (one file
/// per version, no per-table sharding), so AAD doesn't need to bind
/// a specific tree id — per-tree encryption-provider isolation
/// (each tree's `KeyChain` decrypts only its own blocks) is the
/// substitute defence per the AAD design's allowed-zero list.
pub const MANIFEST_TREE_ID_SENTINEL: u64 = u64::MAX;

/// AAD table-id sentinel used for manifest Blocks: `u64::MAX`.
///
/// The manifest is not an SST and has no `TableId`; the sentinel
/// keeps the per-block AAD discriminator non-zero so cross-format
/// substitution between manifest and data Blocks fails AEAD verify.
pub const MANIFEST_TABLE_ID_SENTINEL: u64 = u64::MAX;

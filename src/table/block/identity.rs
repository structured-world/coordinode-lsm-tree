// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Identity context threaded through the Block I/O API.
//!
//! Every call to [`Block::write_into`] / [`Block::from_reader`] /
//! [`Block::from_file`] carries a `BlockIdentity` describing which
//! block, of which table, with which compression context. The
//! Block layer uses this to construct AAD (Additional
//! Authenticated Data) for AEAD encryption — see the AAD-bound
//! wire format spec for the cryptographic role each field plays.
//!
//! **Why a context struct vs. inline arguments.** The natural
//! alternative — pass `aad: &[u8]` directly to the Block API — got
//! a previous attempt into trouble: callers wrote `aad: &[]`
//! everywhere because composing the right AAD bytes at each call
//! site is fiddly and the type system couldn't enforce
//! correctness. With `BlockIdentity`, every call site contributes
//! its OWN local context (the writer/scanner/reader already knows
//! its table id, the block offset, etc.) and the Block layer
//! computes AAD once, internally. Adding a new AAD-relevant field
//! later means adding it to `BlockIdentity` rather than chasing
//! down 90+ call sites.
//!
//! **Field requirements.** Production call sites SHOULD populate
//! every field with the real value from their local context. Test
//! call sites that don't exercise AAD-sensitive paths may use
//! [`BlockIdentity::for_test`] which defaults `dict_id` and
//! `window_log` to zero.
//!
//! **Allowed zero exceptions in production code** (each individually
//! documented at the call site):
//!
//! - `table_id = 0` is allowed when reading a META block that
//!   itself CARRIES the `table_id` field — there's no way to
//!   know the id before the block is parsed (chicken-and-egg).
//!   Cross-store substitution is still prevented because the
//!   meta payload's own id field is part of the verified body.
//! - `block_offset = 0` is allowed in two cases until AAD
//!   wiring lands:
//!   - Index / filter writers that hand their blocks to
//!     `sfa::Writer` (the sectioned-file-archive wrapper):
//!     the wrapper doesn't expose a byte-position cursor, and
//!     the [`crate::table::writer::index::BlockIndexWriter`] /
//!     [`crate::table::writer::filter::FilterWriter`] traits
//!     don't surface `block_offset` to `finish()`. Extending
//!     those signatures is a follow-up.
//!   - Sequential scanners reading via `BufReader`: the buffer
//!     doesn't expose its own byte offset, and the scan path
//!     walks blocks in order so per-block offset bookkeeping
//!     isn't load-bearing for the scan itself.
//!
//! `table_id = 0` AND `block_offset = 0` together — both zero
//! at the same call site — is reserved for tests; a CI canary
//! to enforce that is tracked as a follow-up. Outside the
//! exceptions listed above, defaulting either to zero in
//! production weakens block-swap resistance and should be
//! avoided.

use crate::table::block::BlockType;

/// Identifies a block for encryption AAD and audit purposes.
///
/// Carried through the Block I/O API instead of separate
/// `block_type` / `aad: &[u8]` arguments — see the module
/// docstring for the rationale.
#[derive(Clone, Copy, Debug)]
pub struct BlockIdentity {
    /// Identifier of the owning store unit — for SST blocks this is
    /// the per-tree [`crate::TableId`] (a `u64` alias); for blob
    /// files it is the [`crate::vlog::BlobFileId`] (also a `u64`
    /// alias). Binds the block to its store unit: AAD constructed
    /// from this prevents a block from store A being substituted
    /// for a block at the same offset in store B (block-swap
    /// attack).
    ///
    /// **Not** the same as [`crate::table::GlobalTableId`], which
    /// packs `(tree_id, table_id)` for cross-tree disambiguation;
    /// at the Block-I/O boundary the AAD discriminator only needs
    /// per-store uniqueness within the running process.
    pub table_id: u64,

    /// Byte offset of the block's start within the SST file.
    /// Combined with `table_id`, gives the AAD a globally unique
    /// per-block discriminator.
    pub block_offset: u64,

    /// Whether this is a Data, Filter, Index, or Meta block.
    /// Was previously a separate `block_type: BlockType`
    /// argument on the Block API; now lives here so the call site
    /// only computes one context value.
    pub block_type: BlockType,

    /// Zstd dictionary id used for this block, or `0` if no
    /// dictionary applies. Binds the block to a specific
    /// dictionary version so that decompressing with a different
    /// dictionary (whether by mistake or by attack) surfaces as
    /// an AEAD authentication failure rather than as silently
    /// wrong plaintext.
    pub dict_id: u32,

    /// Zstd `window_log` advertised in the frame header, or `0` if
    /// no zstd compression applies. Binds the block to a
    /// specific decompression-memory budget; attempts to substitute
    /// a block with a different `window_log` (a known "window bomb"
    /// vector) fail AEAD authentication.
    pub window_log: u8,
}

impl BlockIdentity {
    /// Test-only constructor with conservative defaults for the
    /// compression-context fields (`dict_id = 0`, `window_log = 0`).
    /// Use this in test fixtures that don't exercise zstd
    /// dictionary or window-budget paths; in production code,
    /// populate every field explicitly from the local context.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn for_test(table_id: u64, block_offset: u64, block_type: BlockType) -> Self {
        Self {
            table_id,
            block_offset,
            block_type,
            dict_id: 0,
            window_log: 0,
        }
    }
}

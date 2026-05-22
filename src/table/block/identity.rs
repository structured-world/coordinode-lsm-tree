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
//! **Field requirements.** Production call sites MUST populate
//! every field with the real value from their local context. Test
//! call sites that don't exercise AAD-sensitive paths may use
//! [`BlockIdentity::for_test`] which defaults `dict_id` and
//! `window_log` to zero. Defaulting `table_id` or `block_offset`
//! to zero in non-test code is a bug: it makes blocks from
//! different tables / offsets bind to the same AAD, breaking the
//! block-swap resistance guarantee the wire format is designed
//! around.

use crate::table::block::BlockType;

/// Identifies a block for encryption AAD and audit purposes.
///
/// Carried through the Block I/O API instead of separate
/// `block_type` / `aad: &[u8]` arguments — see the module
/// docstring for the rationale.
#[derive(Clone, Copy, Debug)]
pub struct BlockIdentity {
    /// Global table identifier (`(tree_id, table_id)` packed) of
    /// the SST that owns this block. Binds the block to its
    /// table: AAD constructed from this prevents a block from
    /// table A being substituted for a block at the same offset
    /// in table B (block-swap attack).
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

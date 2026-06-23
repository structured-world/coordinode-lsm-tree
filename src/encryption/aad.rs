// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! AAD (Additional Authenticated Data) construction for AAD-bound encrypted
//! blocks, per the wire-format spec in `docs/aad-block-format.md` §5.3.
//!
//! AAD is the 23-byte buffer that is fed to the AEAD primitive alongside the
//! ciphertext and nonce, but is **never** written to disk. It mixes
//! disk-mirrored fields (header byte, key epoch, block type, suite id,
//! compression type, dict id, window log) with one caller-supplied identity
//! field (table id) so that the AEAD tag binds the ciphertext to its
//! block-identity, codec context, and key epoch. A block's byte offset and the
//! owning tree id are intentionally NOT bound — see [`build`] for why.
//!
//! ## Why a separate module
//!
//! The AAD construction is pure-byte arithmetic with no dependency on a
//! specific AEAD primitive or on `structured-zstd`'s skippable-frame envelope.
//! Keeping it isolated lets the per-suite encrypt / decrypt path and the
//! on-disk wire encoder share one source of truth for the AAD layout, and
//! lets the unit tests check the byte layout against the spec without
//! pulling in any crypto deps.
//!
//! ## Type-reuse contract
//!
//! [`BlockType`] (block discriminator) and [`BlockIdentity`] (table id +
//! per-block codec context) are re-exported
//! straight from [`crate::table::block`]; we deliberately do NOT define
//! second copies in this module to avoid drift between the Block I/O
//! API and the AAD constructor. The AAD-specific bits that don't fit on
//! the existing identity type (header byte, key epoch, suite id, codec
//! discriminator) live on [`EncryptionContext`], the small per-block
//! struct passed alongside `BlockIdentity` into [`build`].
//!
//! ## Layout (23 bytes, big-endian for `TableID` and `DictID`)
//!
//! ```text
//! Offset  Size  Field             Source
//! ──────  ────  ───────────────   ──────────────────────────────
//! 0       4     MagicMetadata     Literal 0x184D2A50 LE bytes
//! 4       1     HeaderByte        Mirror of disk
//! 5       1     KeyEpoch          Mirror of disk
//! 6       1     BlockType         Mirror of disk
//! 7       1     SuiteID           Mirror of disk
//! 8       8     TableID           u64 BE, caller-supplied
//! 16      1     CompressionType   Mirror of disk
//! 17      4     DictID            u32 BE, mirror of disk
//! 21      1     WindowLog         Mirror of disk
//! 22      1     BlockFlags        Mirror of disk (transform-presence
//!                                 bitfield: KV footer / ECC / compressed /
//!                                 encrypted) — binds the block's transform
//!                                 stack so a flag relabel fails AEAD
//! ══════
//! Total   23 bytes
//! ```
//!
//! Neither a block's byte offset nor the owning tree id is bound. The offset is
//! unknown to several writers and varies across MID/tail/head mirrors, so
//! binding it would break those paths and force serial encryption. The tree id
//! is a process-ephemeral counter (not durable across reopen), so binding it
//! would fail AEAD verify after a restart. Cross-table substitution is bound via
//! `table_id`; cross-tree substitution is prevented by per-tree key isolation.

use core::convert::TryFrom;

pub use crate::table::block::{BlockIdentity, BlockType};

/// Length of the AAD buffer in bytes.
///
/// Spec-locked: see `docs/aad-block-format.md` §5.3. Neither a block's byte
/// offset nor the owning tree id is bound (see [`build`]), so the buffer is
/// 23 bytes.
pub const AAD_LEN: usize = 23;

/// First four bytes of the AAD: the literal `MetadataFrame` magic.
///
/// `0x184D2A50`, written little-endian per RFC 8878 skippable-frame
/// conventions. Binds the AAD to the AAD-bound format identity so that
/// an attacker who lifts the metadata bytes into a future format with a
/// different magic gets a different AAD and verification fails.
pub const MAGIC_METADATA_LE: [u8; 4] = [0x50, 0x2A, 0x4D, 0x18];

/// Format version encoded in the high nibble of the `HeaderByte`. v1 is the
/// only version defined by `docs/aad-block-format.md`.
///
/// `HeaderByte = (FORMAT_VERSION_V1 << 4) | 0`; the low nibble is reserved
/// and MUST be zero.
pub const FORMAT_VERSION_V1: u8 = 0b0001;

/// The full `HeaderByte` value for v1 with the reserved low nibble at zero
/// (i.e. `0x10`). Spec §5.1 says high nibble = version, low nibble = 0.
pub const HEADER_BYTE_V1: u8 = FORMAT_VERSION_V1 << 4;

/// AEAD primitive used to encrypt the block body, per the §7 suite registry.
///
/// The numeric encoding matches the on-disk `SuiteID` byte at `MetadataFrame`
/// offset 11 and is also mirrored into the AAD at offset 7. Each suite
/// declares its own `NONCE_LEN` (v1 suites: 12 bytes); see [`Self::nonce_len`].
///
/// New AEAD primitives MUST claim a new byte and update both the registry
/// table in `docs/aad-block-format.md` §7 and [`Self::nonce_len`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum SuiteId {
    /// `0x02`: AES-256-GCM (12-byte nonce, 16-byte tag).
    Aes256Gcm = 0x02,
    /// `0x03`: ChaCha20-Poly1305 (12-byte nonce, 16-byte tag).
    ChaCha20Poly1305 = 0x03,
}

impl SuiteId {
    /// Nonce length (bytes) for this suite. Drives the variable-length
    /// `Nonce` field on disk at `MetadataFrame` offset 19 and the
    /// `PayloadLen == 27 + NONCE_LEN` structural check.
    #[must_use]
    pub const fn nonce_len(self) -> usize {
        match self {
            Self::Aes256Gcm | Self::ChaCha20Poly1305 => 12,
        }
    }

    /// On-disk discriminator byte (mirror of `repr(u8)`). Pulled out as a
    /// method so call sites stay readable when casting an enum to its byte
    /// representation without an `as u8` clippy expect.
    #[must_use]
    pub const fn as_byte(self) -> u8 {
        self as u8
    }
}

impl TryFrom<u8> for SuiteId {
    type Error = u8;
    fn try_from(value: u8) -> Result<Self, u8> {
        match value {
            0x02 => Ok(Self::Aes256Gcm),
            0x03 => Ok(Self::ChaCha20Poly1305),
            other => Err(other),
        }
    }
}

/// AAD-specific per-block context.
///
/// Holds the four fields the spec embeds into AAD that do NOT live on
/// [`BlockIdentity`] (which already carries the block-physical context:
/// block type, dict id, window log, plus the three identity fields).
/// Bundled into a single struct so the constructor takes two arguments
/// instead of six and so the field order matches the AAD byte layout
/// from `docs/aad-block-format.md` §5.3.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EncryptionContext {
    /// `HeaderByte` at `MetadataPayload` offset 0 / AAD offset 4. For v1
    /// blocks this is [`HEADER_BYTE_V1`]; a decoder that sees any other
    /// value MUST reject the block as
    /// [`super::error::DecryptError::UnsupportedFormatVersion`].
    pub header_byte: u8,
    /// `KeyEpoch` index into the caller's key chain.
    pub key_epoch: u8,
    /// AEAD suite selector; mirrors disk byte at `MetadataPayload`
    /// offset 11 and drives the variable-length `Nonce` field width
    /// (see [`SuiteId::nonce_len`]).
    pub suite_id: SuiteId,
    /// `CompressionType` codec discriminator (matches the leading byte
    /// of `impl Encode for compression::CompressionType`). v1
    /// spec-defined tags: 0 = None, 1 = Lz4, 3 = Zstd, 4 = `ZstdDict`.
    /// Stored as a raw `u8` here because only the leading discriminator
    /// byte participates in AAD; level (for Zstd) and dict-fingerprint
    /// (already carried on [`BlockIdentity::dict_id`]) live elsewhere.
    pub compression_type: u8,
    /// `block_flags` transform-presence bitfield, mirror of the
    /// `Block::Header` byte (see `crate::table::block::header::block_flags`:
    /// `KV_CHECKSUM_FOOTER` / `ECC_PARITY` / `COMPRESSED` / `ENCRYPTED`).
    /// Bound in the AAD so an attacker cannot relabel a block's transform
    /// stack (e.g. clear the per-KV footer bit) under a forged
    /// non-cryptographic header checksum — the same anti-relabel rationale
    /// that puts `block_type` and `compression_type` in the AAD. The full
    /// byte is mirrored verbatim; the `COMPRESSED` / `ENCRYPTED` bits are
    /// redundant with `compression_type` / the suite but kept so the AAD is
    /// a faithful mirror of the header byte with no masking logic.
    pub block_flags: u8,
}

impl EncryptionContext {
    /// Construct a v1 encryption context. The `header_byte` field is
    /// pinned to [`HEADER_BYTE_V1`]; callers that need to round-trip a
    /// different version byte (e.g. negative tests) build the struct
    /// directly.
    #[must_use]
    pub const fn v1(
        key_epoch: u8,
        suite_id: SuiteId,
        compression_type: u8,
        block_flags: u8,
    ) -> Self {
        Self {
            header_byte: HEADER_BYTE_V1,
            key_epoch,
            suite_id,
            compression_type,
            block_flags,
        }
    }
}

/// Build the 23-byte AAD buffer from the encryption context and the
/// per-block identity.
///
/// The returned array is the exact buffer to pass to the AEAD primitive
/// as `associated_data` for both `encrypt_in_place_detached` and
/// `decrypt_in_place_detached`. Both writer and reader call this with
/// the same inputs; a mismatch in any single byte causes the AEAD tag
/// to fail verification.
///
/// Layout matches `docs/aad-block-format.md` §5.3 byte-for-byte.
#[must_use]
pub fn build(ctx: &EncryptionContext, identity: &BlockIdentity) -> [u8; AAD_LEN] {
    // Stack-allocated; the compiler emits a sequence of direct writes,
    // no heap and no zero-init beyond the initial `[0; AAD_LEN]`.
    let mut buf = [0u8; AAD_LEN];

    // Offset 0..4: MagicMetadata (literal, format-identity binding).
    buf[0..4].copy_from_slice(&MAGIC_METADATA_LE);

    // Offset 4..8: disk-mirrored 4-byte preamble.
    buf[4] = ctx.header_byte;
    buf[5] = ctx.key_epoch;
    buf[6] = u8::from(identity.block_type);
    buf[7] = ctx.suite_id.as_byte();

    // Offset 8..16: the u64 BE table id (NEVER on disk). Neither a block's byte
    // offset nor the owning tree id is bound: the offset is unknown to several
    // writers (sfa-wrapped index/filter writers, the parallel compressor) and
    // varies across MID/tail/head mirrors, so binding it would break those paths
    // and force serial encryption; the tree id is a process-ephemeral counter
    // (not durable across reopen), so binding it would fail AEAD verify after a
    // restart. Cross-table substitution is bound via table_id; cross-tree
    // substitution is prevented by per-tree key isolation (a tree's blocks
    // decrypt only under its own key). Intra-file block-swap degrades only to a
    // lookup miss (a value is inseparable from its key inside the authenticated
    // block), not forgery.
    buf[8..16].copy_from_slice(&identity.table_id.to_be_bytes());

    // Offset 16..22: disk-mirrored codec context.
    buf[16] = ctx.compression_type;
    buf[17..21].copy_from_slice(&identity.dict_id.to_be_bytes());
    buf[21] = identity.window_log;

    // Offset 22: disk-mirrored transform-presence flags.
    buf[22] = ctx.block_flags;

    buf
}

#[cfg(test)]
mod tests;

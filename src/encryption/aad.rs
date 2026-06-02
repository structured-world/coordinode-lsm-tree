// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! AAD (Additional Authenticated Data) construction for AAD-bound encrypted
//! blocks, per the wire-format spec in `docs/aad-block-format.md` §5.3.
//!
//! AAD is the 39-byte buffer that is fed to the AEAD primitive alongside the
//! ciphertext and nonce, but is **never** written to disk. It mixes
//! disk-mirrored fields (header byte, key epoch, block type, suite id,
//! compression type, dict id, window log) with caller-supplied identity
//! fields (tree id, table id, block offset) so that the AEAD tag binds the
//! ciphertext to its exact block-identity, codec context, and key epoch.
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
//! [`BlockType`] (block discriminator) and [`BlockIdentity`] (tree id /
//! table id / block offset + per-block codec context) are re-exported
//! straight from [`crate::table::block`]; we deliberately do NOT define
//! second copies in this module to avoid drift between the Block I/O
//! API and the AAD constructor. The AAD-specific bits that don't fit on
//! the existing identity type (header byte, key epoch, suite id, codec
//! discriminator) live on [`EncryptionContext`], the small per-block
//! struct passed alongside `BlockIdentity` into [`build`].
//!
//! ## Layout (39 bytes, big-endian for u64 identity fields)
//!
//! ```text
//! Offset  Size  Field             Source
//! ──────  ────  ───────────────   ──────────────────────────────
//! 0       4     MagicMetadata     Literal 0x184D2A50 LE bytes
//! 4       1     HeaderByte        Mirror of disk
//! 5       1     KeyEpoch          Mirror of disk
//! 6       1     BlockType         Mirror of disk
//! 7       1     SuiteID           Mirror of disk
//! 8       8     TreeID            u64 BE, caller-supplied
//! 16      8     TableID           u64 BE, caller-supplied
//! 24      8     BlockOffset       u64 BE, caller-supplied
//! 32      1     CompressionType   Mirror of disk
//! 33      4     DictID            u32 BE, mirror of disk
//! 37      1     WindowLog         Mirror of disk
//! 38      1     BlockFlags        Mirror of disk (transform-presence
//!                                 bitfield: KV footer / ECC / compressed /
//!                                 encrypted) — binds the block's transform
//!                                 stack so a flag relabel fails AEAD
//! ══════
//! Total   39 bytes
//! ```

use core::convert::TryFrom;

pub use crate::table::block::{BlockIdentity, BlockType};

/// Length of the AAD buffer in bytes. Spec-locked: see
/// `docs/aad-block-format.md` §5.3.
pub const AAD_LEN: usize = 39;

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

/// Build the 39-byte AAD buffer from the encryption context and the
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
    // no heap and no zero-init beyond the initial `[0; 38]`.
    let mut buf = [0u8; AAD_LEN];

    // Offset 0..4: MagicMetadata (literal, format-identity binding).
    buf[0..4].copy_from_slice(&MAGIC_METADATA_LE);

    // Offset 4..8: disk-mirrored 4-byte preamble.
    buf[4] = ctx.header_byte;
    buf[5] = ctx.key_epoch;
    buf[6] = u8::from(identity.block_type);
    buf[7] = ctx.suite_id.as_byte();

    // Offset 8..32: three u64 BE identity fields (NEVER on disk).
    buf[8..16].copy_from_slice(&identity.tree_id.to_be_bytes());
    buf[16..24].copy_from_slice(&identity.table_id.to_be_bytes());
    buf[24..32].copy_from_slice(&identity.block_offset.to_be_bytes());

    // Offset 32..38: disk-mirrored codec context.
    buf[32] = ctx.compression_type;
    buf[33..37].copy_from_slice(&identity.dict_id.to_be_bytes());
    buf[37] = identity.window_log;

    // Offset 38: disk-mirrored transform-presence flags.
    buf[38] = ctx.block_flags;

    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(tree_id: u64, table_id: u64, block_offset: u64, bt: BlockType) -> BlockIdentity {
        BlockIdentity {
            tree_id,
            table_id,
            block_offset,
            block_type: bt,
            dict_id: 0,
            window_log: 0,
        }
    }

    #[test]
    fn aad_len_matches_spec() {
        // Hard-coded to catch any drift between `AAD_LEN` and the spec's
        // 39-byte total. The encoder/decoder rely on this exact size.
        assert_eq!(AAD_LEN, 39);
    }

    #[test]
    fn magic_bytes_are_little_endian_for_skippable_frame() {
        // RFC 8878 skippable frames carry their magic in little-endian
        // form; the spec's `0x184D2A50` therefore appears on disk as
        // `50 2A 4D 18` and must appear identically in the AAD so an
        // attacker cannot port the metadata into a future format with a
        // different magic and reuse the AEAD tag.
        assert_eq!(MAGIC_METADATA_LE, [0x50, 0x2A, 0x4D, 0x18]);
        assert_eq!(u32::from_le_bytes(MAGIC_METADATA_LE), 0x184D_2A50);
    }

    #[test]
    fn header_byte_v1_packs_version_in_high_nibble() {
        // High nibble = 1, low nibble = 0. The decoder shifts right by 4
        // to extract the version, so 0x10 must equal version 1.
        assert_eq!(HEADER_BYTE_V1, 0x10);
        assert_eq!(HEADER_BYTE_V1 >> 4, FORMAT_VERSION_V1);
        assert_eq!(HEADER_BYTE_V1 & 0x0F, 0);
    }

    #[test]
    fn suite_nonce_lengths_match_registry() {
        // Locks the §7 registry into code so a future suite addition
        // forces a deliberate update here.
        assert_eq!(SuiteId::Aes256Gcm.nonce_len(), 12);
        assert_eq!(SuiteId::ChaCha20Poly1305.nonce_len(), 12);
    }

    #[test]
    fn suite_id_byte_round_trip() {
        for suite in [SuiteId::Aes256Gcm, SuiteId::ChaCha20Poly1305] {
            assert_eq!(SuiteId::try_from(suite.as_byte()), Ok(suite));
        }
    }

    #[test]
    fn suite_id_rejects_unknown_byte() {
        // 0x00 / 0x01 / 0x04..=0xFF are reserved per the §7 registry.
        for byte in [0u8, 1, 4, 0x10, 0xFF] {
            assert_eq!(SuiteId::try_from(byte), Err(byte));
        }
    }

    #[test]
    fn aad_layout_is_byte_exact_for_a_concrete_block() {
        // Synthesise a concrete AAD payload and check every offset
        // matches the spec. The values picked here are non-degenerate:
        // each field uses a recognisable bit pattern so a layout mistake
        // (e.g. swapping table_id and block_offset) is immediately
        // visible in the diff.
        let ctx = EncryptionContext::v1(
            0x55, // key_epoch
            SuiteId::ChaCha20Poly1305,
            3,    // CompressionType::Zstd
            0x05, // block_flags: KV_CHECKSUM_FOOTER | COMPRESSED (bits 0,2)
        );
        let identity = BlockIdentity {
            tree_id: 0x0102_0304_0506_0708,
            table_id: 0x1112_1314_1516_1718,
            block_offset: 0x2122_2324_2526_2728,
            block_type: BlockType::Index, // 1
            dict_id: 0xDEAD_BEEF,
            window_log: 21,
        };

        let aad = build(&ctx, &identity);

        // MagicMetadata (LE): 50 2A 4D 18
        assert_eq!(&aad[0..4], &[0x50, 0x2A, 0x4D, 0x18]);
        // HeaderByte: 0x10 (v1)
        assert_eq!(aad[4], 0x10);
        // KeyEpoch
        assert_eq!(aad[5], 0x55);
        // BlockType: Index = 1
        assert_eq!(aad[6], 1);
        // SuiteID: ChaCha20-Poly1305 = 0x03
        assert_eq!(aad[7], 0x03);
        // TreeID (BE)
        assert_eq!(&aad[8..16], &0x0102_0304_0506_0708u64.to_be_bytes());
        // TableID (BE)
        assert_eq!(&aad[16..24], &0x1112_1314_1516_1718u64.to_be_bytes());
        // BlockOffset (BE)
        assert_eq!(&aad[24..32], &0x2122_2324_2526_2728u64.to_be_bytes());
        // CompressionType
        assert_eq!(aad[32], 3);
        // DictID (BE)
        assert_eq!(&aad[33..37], &0xDEAD_BEEFu32.to_be_bytes());
        // WindowLog
        assert_eq!(aad[37], 21);
        // BlockFlags
        assert_eq!(aad[38], 0x05);
    }

    #[test]
    fn aad_for_zero_identity_is_well_formed() {
        // The `tree_id = 0` placeholder path still produces a valid
        // 39-byte AAD; the cross-tree defence in that case relies on
        // per-tree key isolation, not on AAD bytes. The fixture also
        // pins the AES-256-GCM byte at the SuiteID offset for the
        // zero-codec / zero-block-type happy path.
        let ctx = EncryptionContext::v1(0, SuiteId::Aes256Gcm, 0, 0);
        let id = identity(0, 0, 0, BlockType::Data);
        let aad = build(&ctx, &id);
        assert_eq!(aad.len(), AAD_LEN);
        assert_eq!(&aad[0..4], &MAGIC_METADATA_LE);
        assert_eq!(aad[4], HEADER_BYTE_V1);
        assert_eq!(aad[5], 0); // KeyEpoch
        assert_eq!(aad[6], 0); // BlockType::Data
        assert_eq!(aad[7], 0x02); // SuiteId::Aes256Gcm
        assert!(aad[8..].iter().all(|&b| b == 0));
    }

    #[test]
    fn aad_changes_when_block_offset_changes() {
        // Cross-block-relocation defence: same identity except for
        // `block_offset` must produce a different AAD, so the same
        // AEAD ciphertext + tag will not verify after a relocation.
        let ctx = EncryptionContext::v1(1, SuiteId::Aes256Gcm, 0, 0);
        let a = build(&ctx, &identity(1, 2, 100, BlockType::Data));
        let b = build(&ctx, &identity(1, 2, 101, BlockType::Data));
        assert_ne!(a, b);
        // Only the block_offset bytes (24..32) differ.
        assert_eq!(&a[..24], &b[..24]);
        assert_eq!(&a[32..], &b[32..]);
    }

    #[test]
    fn aad_changes_when_block_type_changes() {
        // Block-type-relabel defence: same identity except for
        // `block_type` must produce a different AAD, so an attacker
        // cannot relabel a Data block as an Index block to bypass
        // type-specific decode paths.
        let ctx = EncryptionContext::v1(1, SuiteId::Aes256Gcm, 0, 0);
        let a = build(&ctx, &identity(1, 2, 100, BlockType::Data));
        let b = build(&ctx, &identity(1, 2, 100, BlockType::Index));
        assert_ne!(a, b);
        // Only the block_type byte (offset 6) differs.
        assert_eq!(&a[..6], &b[..6]);
        assert_eq!(&a[7..], &b[7..]);
    }

    #[test]
    fn aad_changes_when_block_flags_changes() {
        // Transform-relabel defence: same identity + codec, differing only
        // in `block_flags`, must produce a different AAD — so an attacker
        // cannot flip a transform bit (e.g. clear the per-KV footer bit)
        // and still pass AEAD verification.
        let a = build(
            &EncryptionContext::v1(1, SuiteId::Aes256Gcm, 0, 0),
            &identity(1, 2, 100, BlockType::Data),
        );
        let b = build(
            &EncryptionContext::v1(1, SuiteId::Aes256Gcm, 0, 0x01),
            &identity(1, 2, 100, BlockType::Data),
        );
        assert_ne!(a, b);
        // Only the block_flags byte (offset 38) differs.
        assert_eq!(&a[..38], &b[..38]);
        assert_ne!(a[38], b[38]);
    }
}

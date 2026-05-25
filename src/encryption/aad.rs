// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! AAD (Additional Authenticated Data) construction for AAD-bound encrypted
//! blocks, per the wire-format spec in `docs/aad-block-format.md` §5.3.
//!
//! AAD is the 38-byte buffer that is fed to the AEAD primitive alongside the
//! ciphertext and nonce, but is **never** written to disk. It mixes
//! disk-mirrored fields (`HeaderByte`, `KeyEpoch`, `BlockType`, `SuiteID`,
//! `CompressionType`, `DictID`, `WindowLog`) with caller-supplied identity
//! fields (`TreeID`, `TableID`, `BlockOffset`) so that the AEAD tag binds the
//! ciphertext to its exact block-identity, codec context, and key epoch.
//!
//! ## Why a separate module
//!
//! The AAD construction is pure-byte arithmetic with no dependency on a
//! specific AEAD primitive or on `structured-zstd`'s skippable-frame envelope.
//! Keeping it isolated lets the per-suite encrypt/decrypt path and the
//! on-disk wire encoder share one source of truth for the AAD layout, and
//! lets the unit tests check the byte layout against the spec without
//! pulling in any crypto deps.
//!
//! ## Layout (38 bytes, big-endian for u64 identity fields)
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
//! ══════
//! Total   38 bytes
//! ```

use core::convert::TryFrom;

/// Length of the AAD buffer in bytes. Spec-locked: see
/// `docs/aad-block-format.md` §5.3.
pub const AAD_LEN: usize = 38;

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
    /// `Nonce` field on disk at `MetadataFrame` offset 18 and the
    /// `PayloadLen == 26 + NONCE_LEN` structural check.
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

/// Block type discriminator, mirrored into both the on-disk `MetadataPayload`
/// at offset 10 and the AAD at offset 6.
///
/// Tags match the spec §5.1 registry. New variants get a fresh tag and
/// extend both the spec and [`Self::from_byte`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum BlockType {
    /// Data block (user KV entries). Tag `0`.
    Data = 0,
    /// Index block (top-level or partitioned index). Tag `1`.
    Index = 1,
    /// Filter block (Bloom / `BuRR` membership filter). Tag `2`.
    Filter = 2,
    /// Meta block (table-level metadata). Tag `3`.
    Meta = 3,
    /// Range-tombstone block. Tag `4`.
    RangeTombstone = 4,
}

impl BlockType {
    /// On-disk byte for this block type.
    #[must_use]
    pub const fn as_byte(self) -> u8 {
        self as u8
    }
}

/// Caller-supplied block identity used to bind the AEAD AAD to a specific
/// position in a specific SST file in a specific tree.
///
/// **NEVER written to disk.** All three fields are reconstructed at decrypt
/// time from the reader's context (`AbstractTree::id()`, the SST file's
/// `TableId`, the read cursor's byte position). The writer must feed the
/// same values from its own context into AAD construction; an attacker who
/// relocates a block to a different file / different offset gets a
/// non-matching AAD and decryption fails.
///
/// ## `tree_id = 0` placeholder
///
/// Call sites that have not yet plumbed a real tree id are permitted to
/// pass `0` so that the migration to AAD-bound blocks is not blocked. The
/// cross-tree substitution defence from `docs/aad-block-format.md` §3 is
/// then NOT covered by AAD for those call sites: any two trees feeding
/// `tree_id = 0` collapse the pair to just `(0, table_id)`, and `table_id`
/// is only unique within a tree. Such callers MUST instead provide
/// per-tree encryption-provider key isolation (a different encryption key
/// per tree) so that AEAD verification fails on cross-tree-substituted
/// blocks even when the AAD-bound `TreeID` collides.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockIdentity {
    /// Owning tree id. Source: `AbstractTree::id()` on read, the same
    /// tree's id on write. See "`tree_id = 0` placeholder" above for
    /// the per-tree key-isolation caveat.
    pub tree_id: u64,
    /// SST file's per-tree `TableId` (derived from the file path /
    /// table metadata). Pair with `tree_id` gives the globally-unique
    /// block identity that defeats cross-tree substitution.
    pub table_id: u64,
    /// Read or write cursor's byte position within the SST file. Binds
    /// the block to its position to defeat same-file relocations.
    pub block_offset: u64,
}

impl BlockIdentity {
    /// Construct a `BlockIdentity` from the three identity fields.
    #[must_use]
    pub const fn new(tree_id: u64, table_id: u64, block_offset: u64) -> Self {
        Self {
            tree_id,
            table_id,
            block_offset,
        }
    }
}

/// Disk-mirrored metadata fields, condensed into a struct.
///
/// The writer commits these to disk; the reader parses them out of the
/// `MetadataFrame` and passes them, together with the caller-supplied
/// `BlockIdentity`, into AAD construction (and, later, into the
/// wire-format encoder / decoder).
///
/// Excludes the variable-length `Nonce` and the `AEADTag`: those are
/// AEAD inputs, not AAD inputs (see spec §5.3 closing paragraph).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataHeader {
    /// `HeaderByte` at `MetadataPayload` offset 0 / AAD offset 4. For v1
    /// blocks this is [`HEADER_BYTE_V1`]; a decoder that sees any other
    /// value MUST reject the block as `UnsupportedFormatVersion`.
    pub header_byte: u8,
    /// `KeyEpoch` index into the caller's key chain.
    pub key_epoch: u8,
    /// Block type discriminator; mirrors disk byte at `MetadataPayload`
    /// offset 10.
    pub block_type: BlockType,
    /// AEAD suite selector; mirrors disk byte at `MetadataPayload`
    /// offset 11 and drives the variable-length `Nonce` field width.
    pub suite_id: SuiteId,
    /// `CompressionType` codec discriminator (matches the leading byte of
    /// `impl Encode for compression::CompressionType`). v1 spec-defined
    /// tags: 0 = None, 1 = Lz4, 3 = Zstd, 4 = `ZstdDict`.
    pub compression_type: u8,
    /// `DictID`: zstd dictionary fingerprint (0 if no dict).
    pub dict_id: u32,
    /// Raw zstd window log (NOT the encoded `Window_Descriptor` byte). 0
    /// when no zstd / no window enforcement, otherwise `10..=31` per
    /// RFC 8878 §3.1.1.1.2.
    pub window_log: u8,
}

impl MetadataHeader {
    /// Construct a v1 header. The `header_byte` field is pinned to
    /// [`HEADER_BYTE_V1`]; callers that need to round-trip a different
    /// version byte (e.g. negative tests) build the struct directly.
    #[must_use]
    pub const fn v1(
        key_epoch: u8,
        block_type: BlockType,
        suite_id: SuiteId,
        compression_type: u8,
        dict_id: u32,
        window_log: u8,
    ) -> Self {
        Self {
            header_byte: HEADER_BYTE_V1,
            key_epoch,
            block_type,
            suite_id,
            compression_type,
            dict_id,
            window_log,
        }
    }
}

/// Build the 38-byte AAD buffer from the disk-mirrored header and the
/// caller-supplied block identity.
///
/// The returned array is the exact buffer to pass to the AEAD primitive
/// as `associated_data` for both `encrypt_in_place_detached` and
/// `decrypt_in_place_detached`. Both writer and reader call this with the
/// same inputs; a mismatch in any single byte causes the AEAD tag to
/// fail verification.
///
/// Layout matches `docs/aad-block-format.md` §5.3 byte-for-byte.
#[must_use]
pub fn build(header: &MetadataHeader, identity: &BlockIdentity) -> [u8; AAD_LEN] {
    // Stack-allocated; the compiler emits a sequence of direct writes,
    // no heap and no zero-init beyond the initial `[0; 38]`.
    let mut buf = [0u8; AAD_LEN];

    // Offset 0..4: MagicMetadata (literal, format-identity binding).
    buf[0..4].copy_from_slice(&MAGIC_METADATA_LE);

    // Offset 4..8: disk-mirrored 4-byte preamble.
    buf[4] = header.header_byte;
    buf[5] = header.key_epoch;
    buf[6] = header.block_type.as_byte();
    buf[7] = header.suite_id.as_byte();

    // Offset 8..32: three u64 BE identity fields (NEVER on disk).
    buf[8..16].copy_from_slice(&identity.tree_id.to_be_bytes());
    buf[16..24].copy_from_slice(&identity.table_id.to_be_bytes());
    buf[24..32].copy_from_slice(&identity.block_offset.to_be_bytes());

    // Offset 32..38: disk-mirrored codec context.
    buf[32] = header.compression_type;
    buf[33..37].copy_from_slice(&header.dict_id.to_be_bytes());
    buf[37] = header.window_log;

    buf
}

/// Parse a `SuiteId` from its on-disk byte. Used by the wire-format
/// decoder before AAD construction (the AAD is built only after the
/// decoder has resolved `NONCE_LEN` from the suite byte).
///
/// # Errors
///
/// Returns the offending byte if it does not match any registered suite
/// in the §7 registry. The caller is expected to surface this as
/// `DecryptError::UnsupportedSuite { suite_id: byte }` once that error
/// type lands (see [`super::error`]).
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

impl TryFrom<u8> for BlockType {
    type Error = u8;
    fn try_from(value: u8) -> Result<Self, u8> {
        match value {
            0 => Ok(Self::Data),
            1 => Ok(Self::Index),
            2 => Ok(Self::Filter),
            3 => Ok(Self::Meta),
            4 => Ok(Self::RangeTombstone),
            other => Err(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aad_len_matches_spec() {
        // Hard-coded to catch any drift between `AAD_LEN` and the spec's
        // 38-byte total. The encoder/decoder rely on this exact size.
        assert_eq!(AAD_LEN, 38);
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
    fn block_type_byte_round_trip() {
        for bt in [
            BlockType::Data,
            BlockType::Index,
            BlockType::Filter,
            BlockType::Meta,
            BlockType::RangeTombstone,
        ] {
            assert_eq!(BlockType::try_from(bt.as_byte()), Ok(bt));
        }
    }

    #[test]
    fn block_type_rejects_unknown_byte() {
        for byte in [5u8, 0x10, 0xFF] {
            assert_eq!(BlockType::try_from(byte), Err(byte));
        }
    }

    #[test]
    fn aad_layout_is_byte_exact_for_a_concrete_block() {
        // Synthesise a concrete AAD payload and check every offset
        // matches the spec. The values picked here are non-degenerate:
        // each field uses a recognisable bit pattern so a layout mistake
        // (e.g. swapping table_id and block_offset) is immediately
        // visible in the diff.
        let header = MetadataHeader::v1(
            0x55,             // key_epoch
            BlockType::Index, // 1
            SuiteId::ChaCha20Poly1305,
            3,           // CompressionType::Zstd
            0xDEAD_BEEF, // dict_id
            21,          // window_log
        );
        let identity = BlockIdentity::new(
            0x0102_0304_0506_0708, // tree_id
            0x1112_1314_1516_1718, // table_id
            0x2122_2324_2526_2728, // block_offset
        );

        let aad = build(&header, &identity);

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
    }

    #[test]
    fn aad_for_zero_identity_is_well_formed() {
        // The `tree_id = 0` placeholder path still produces a valid
        // 38-byte AAD; the cross-tree defence in that case relies on
        // per-tree key isolation, not on AAD bytes. Tested here so a
        // future panic-on-zero check in `BlockIdentity::new` would be
        // caught immediately.
        let header = MetadataHeader::v1(0, BlockType::Data, SuiteId::Aes256Gcm, 0, 0, 0);
        let identity = BlockIdentity::new(0, 0, 0);
        let aad = build(&header, &identity);
        assert_eq!(aad.len(), AAD_LEN);
        // Magic at the front, HeaderByte=0x10 at offset 4, SuiteID=0x02
        // at offset 7. The remaining 30 bytes are zero (KeyEpoch,
        // BlockType, TreeID/TableID/BlockOffset, CompressionType,
        // DictID, WindowLog).
        assert_eq!(&aad[0..4], &MAGIC_METADATA_LE);
        assert_eq!(aad[4], HEADER_BYTE_V1);
        assert_eq!(aad[5], 0); // KeyEpoch
        assert_eq!(aad[6], 0); // BlockType::Data
        assert_eq!(aad[7], 0x02); // SuiteId::Aes256Gcm
        assert!(aad[8..].iter().all(|&b| b == 0));
    }

    #[test]
    fn aad_changes_when_block_offset_changes() {
        // The cross-block-relocation defence: same identity except
        // for `block_offset` must produce a different AAD, so the same
        // AEAD ciphertext+tag will not verify after a relocation.
        let header = MetadataHeader::v1(1, BlockType::Data, SuiteId::Aes256Gcm, 0, 0, 0);
        let a = build(&header, &BlockIdentity::new(1, 2, 100));
        let b = build(&header, &BlockIdentity::new(1, 2, 101));
        assert_ne!(a, b);
        // Only the block_offset bytes (24..32) should differ.
        assert_eq!(&a[..24], &b[..24]);
        assert_eq!(&a[32..], &b[32..]);
    }
}

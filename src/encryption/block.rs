// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Top-level AAD-bound encrypted block encode / decode entry points.
//!
//! Implements the wire format from `docs/aad-block-format.md` §5:
//! two consecutive Zstandard skippable frames — a fixed-size
//! `MetadataFrame` (magic `0x184D2A50`) carrying the cryptographic
//! parameters, followed by a variable-size `BodyFrame` (magic
//! `0x184D2A51`) carrying the AEAD ciphertext.
//!
//! Two public functions, one for each direction:
//!
//! - [`encrypt_block`]: takes plaintext + per-block identity + per-block
//!   crypto context + key chain, produces the serialised
//!   `MetadataFrame ‖ BodyFrame` byte sequence.
//! - [`decrypt_block`]: takes serialised bytes + per-block identity +
//!   key chain, recovers the original plaintext or surfaces a typed
//!   [`DecryptError`].
//!
//! The `MetadataFrame` layout is byte-aligned with §5.1; the
//! `BodyFrame` layout is §5.2; AAD construction follows §5.3.
//! Both writer and
//! reader hit the same [`aad::build`](super::aad::build) call with the same inputs, so
//! the AEAD tag binds the ciphertext to the full block-identity +
//! codec context + key-epoch tuple. AAD is never written to disk.

use std::io::Cursor;

use aes_gcm::aead::Generate;
use byteorder::{BigEndian, ReadBytesExt};
use structured_zstd::skippable::SkippableFrame;

use super::aad::{AAD_LEN, BlockIdentity, EncryptionContext, FORMAT_VERSION_V1, SuiteId, build};
use super::aead::{TAG_LEN, decrypt_in_place, encrypt_in_place};
use super::error::DecryptError;
use super::key_chain::KeyChain;

/// `MetadataFrame` magic: `0x184D2A50` LE bytes. Variant 0 of the
/// Zstandard skippable-frame range.
const METADATA_VARIANT: u8 = 0;
/// `BodyFrame` magic: `0x184D2A51` LE bytes. Variant 1.
const BODY_VARIANT: u8 = 1;

/// Base of the Zstandard skippable-frame magic range
/// (RFC 8878 §3.1.2). Variants 0..=15 share this base; we use 0
/// for `MetadataFrame` and 1 for `BodyFrame`.
const SKIPPABLE_MAGIC_START: u32 = 0x184D_2A50;

/// Decodes one skippable-frame header (8 bytes: 4-byte LE magic +
/// 4-byte LE payload length), enforces the variant and the
/// caller's `min..=max` `PayloadLen` window BEFORE allocating,
/// then reads exactly that many bytes into a `Vec<u8>`.
///
/// Replaces a direct [`SkippableFrame::decode_from`] call so the
/// `PayloadLen` validation happens ahead of the allocation — the
/// upstream API allocates the full declared length first and only
/// then surfaces caller-side caps, which means a forged
/// `PayloadLen = u32::MAX` would burn a 4 GiB allocation attempt
/// before the read even started. Decoding the header manually
/// rejects oversized / undersized frames at the cost of 8 bytes
/// of upfront I/O.
///
/// `expected_variant`:
/// - `Some(v)`: only this exact variant is acceptable; any other
///   value (including out-of-range bytes) is rejected.
/// - `None`: any variant in 0..=15 is acceptable; the variant byte
///   is returned alongside the payload so the caller can dispatch.
fn read_framed_payload_len<R: std::io::Read>(
    reader: &mut R,
    expected_variant: Option<u8>,
    min_payload: u32,
    max_payload: u32,
    err_ctor: fn(&'static str) -> DecryptError,
) -> Result<u32, DecryptError> {
    let mut header = [0u8; 8];
    reader
        .read_exact(&mut header)
        .map_err(|_| err_ctor("truncated skippable-frame header"))?;

    // 4-byte LE magic. Within the skippable-frame range
    // (variants 0..=15) this is `SKIPPABLE_MAGIC_START + variant`.
    let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    let variant = magic.wrapping_sub(SKIPPABLE_MAGIC_START);
    if variant > 15 {
        return Err(err_ctor("magic outside skippable-frame range"));
    }
    // `variant > 15` already excludes any value outside u8 range,
    // so the subsequent narrowing cast is exact — guarded above.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "guarded by `variant > 15` immediately above"
    )]
    let variant_byte = variant as u8;
    if let Some(v) = expected_variant
        && variant_byte != v
    {
        return Err(err_ctor("wrong frame magic / variant"));
    }

    // 4-byte LE payload length. Reject outside the [min, max]
    // window BEFORE allocating.
    let payload_len = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
    if payload_len < min_payload {
        return Err(err_ctor("PayloadLen below spec minimum"));
    }
    if payload_len > max_payload {
        return Err(err_ctor("PayloadLen exceeds cap"));
    }
    Ok(payload_len)
}

/// Reads + validates the skippable-frame header (via
/// [`read_framed_payload_len`]) and then reads exactly the declared
/// payload bytes into a `Vec<u8>`. Use when the payload bytes are needed
/// (decrypt); use [`read_framed_payload_len`] alone when only the length
/// matters (forensic metadata parse), to skip the body allocation.
fn read_framed_payload<R: std::io::Read>(
    reader: &mut R,
    expected_variant: Option<u8>,
    min_payload: u32,
    max_payload: u32,
    err_ctor: fn(&'static str) -> DecryptError,
) -> Result<Vec<u8>, DecryptError> {
    let payload_len =
        read_framed_payload_len(reader, expected_variant, min_payload, max_payload, err_ctor)?;
    let mut payload = vec![0u8; payload_len as usize];
    reader
        .read_exact(&mut payload)
        .map_err(|_| err_ctor("truncated frame payload"))?;
    Ok(payload)
}

/// `MetadataPayload` size for v1 suites: 39 bytes
/// (= `27 + NONCE_LEN` where v1 suites declare `NONCE_LEN` = 12).
const METADATA_PAYLOAD_LEN_V1: u32 = 39;

/// Upper bound on the encrypted body payload (256 MiB). Mirrors
/// the block-write cap on the plaintext path; rejecting larger
/// frames before allocation guards against a forged `BodyFrame`
/// `PayloadLen` triggering an unbounded `Vec` allocation on read.
const MAX_BODY_LEN: u32 = 256 * 1024 * 1024;

/// Encodes the 39-byte `MetadataPayload` (v1 suites only).
///
/// Layout per `docs/aad-block-format.md` §5.1 (skippable-frame
/// payload, NOT including the 8-byte SFA framing header which
/// [`SkippableFrame::encode_into`] adds):
///
/// | Offset | Size | Field             |
/// |--------|------|-------------------|
/// | 0      | 1    | HeaderByte        |
/// | 1      | 1    | KeyEpoch          |
/// | 2      | 1    | BlockType         |
/// | 3      | 1    | SuiteID           |
/// | 4      | 1    | CompressionType   |
/// | 5      | 4    | DictID (u32 BE)   |
/// | 9      | 1    | WindowLog         |
/// | 10     | 1    | BlockFlags        |
/// | 11     | 12   | Nonce             |
/// | 23     | 16   | AEADTag           |
fn encode_metadata_payload(
    ctx: EncryptionContext,
    identity: &BlockIdentity,
    nonce: &[u8; 12],
    tag: &[u8; TAG_LEN],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(METADATA_PAYLOAD_LEN_V1 as usize);
    out.push(ctx.header_byte);
    out.push(ctx.key_epoch);
    out.push(u8::from(identity.block_type));
    out.push(ctx.suite_id.as_byte());
    out.push(ctx.compression_type);
    // Use the infallible byte-array path — `Vec` extension by
    // a fixed-size slice cannot fail, sidestepping the
    // `.write_u32::<BigEndian>` ? / .expect / log-and-continue
    // shape that fights the storage crate's
    // `#[deny(clippy::expect_used)]` / `_unwrap_used)]` lints
    // without any partial-write risk on a theoretical error path.
    out.extend_from_slice(&identity.dict_id.to_be_bytes());
    out.push(identity.window_log);
    out.push(ctx.block_flags);
    out.extend_from_slice(nonce);
    out.extend_from_slice(tag);
    debug_assert_eq!(
        out.len(),
        METADATA_PAYLOAD_LEN_V1 as usize,
        "v1 MetadataPayload must be exactly 39 bytes"
    );
    out
}

/// Decoded `MetadataPayload` view — parses the 39 bytes into the
/// fields required to reconstruct the AAD and decrypt the body.
struct ParsedMetadata {
    suite_id: SuiteId,
    nonce: [u8; 12],
    tag: [u8; TAG_LEN],
    ctx: EncryptionContext,
    block_type_byte: u8,
    dict_id: u32,
    window_log: u8,
}

fn decode_metadata_payload(payload: &[u8]) -> Result<ParsedMetadata, DecryptError> {
    if payload.len() != METADATA_PAYLOAD_LEN_V1 as usize {
        return Err(DecryptError::MalformedMetadataFrame(
            "MetadataPayload length != 39 for v1",
        ));
    }
    let mut cursor = Cursor::new(payload);
    let read_u8 = |c: &mut Cursor<&[u8]>| {
        c.read_u8()
            .map_err(|_| DecryptError::MalformedMetadataFrame("truncated MetadataPayload"))
    };
    let header_byte = read_u8(&mut cursor)?;
    // Format version is the high nibble; v1 = 0b0001. Per spec
    // §4.8 (locked decisions): the low nibble is reserved and MUST
    // be zero on write, but readers MUST IGNORE it on the read
    // path so future suites can use those bits for forward-compatible
    // extensions without requiring a wire-format bump. Validate ONLY
    // the high nibble here.
    if (header_byte >> 4) != FORMAT_VERSION_V1 {
        return Err(DecryptError::UnsupportedFormatVersion { header_byte });
    }
    let key_epoch = read_u8(&mut cursor)?;
    let block_type_byte = read_u8(&mut cursor)?;
    let suite_byte = read_u8(&mut cursor)?;
    let suite_id = SuiteId::try_from(suite_byte)
        .map_err(|s| DecryptError::UnsupportedSuite { suite_id: s })?;
    let compression_type = read_u8(&mut cursor)?;
    // Spec §5.1 row "CompressionType": tag values are 0 = None,
    // 1 = Lz4, 3 = Zstd, 4 = ZstdDict. Tag 2 and tags >= 5 are
    // reserved / unallocated and must be rejected.
    if !matches!(compression_type, 0 | 1 | 3 | 4) {
        return Err(DecryptError::MalformedMetadataFrame(
            "CompressionType byte not in spec registry (0, 1, 3, 4)",
        ));
    }
    let dict_id = cursor
        .read_u32::<BigEndian>()
        .map_err(|_| DecryptError::MalformedMetadataFrame("truncated DictID"))?;
    let window_log = read_u8(&mut cursor)?;
    // Spec §5.1 row "WindowLog": valid values are 0 (no zstd /
    // no window enforcement, used for CompressionType::None or
    // non-zstd codecs) or 10..=31 (RFC 8878 §3.1.1.1.2 decoded
    // window-descriptor range). Any other byte is malformed and
    // must be rejected BEFORE any AEAD work.
    if window_log != 0 && !(10..=31).contains(&window_log) {
        return Err(DecryptError::MalformedMetadataFrame(
            "WindowLog outside valid range (must be 0 or 10..=31)",
        ));
    }
    // Cross-field invariants per spec §5.1:
    // - `DictID` is non-zero ONLY when `CompressionType == 4`
    //   (`ZstdDict`); other codecs must record `DictID = 0`.
    // - `WindowLog` is non-zero ONLY when `CompressionType` is a
    //   zstd-family codec (tags 3 or 4); non-zstd codecs must
    //   record `WindowLog = 0`.
    // Rejecting impossible combinations here keeps the AAD-bound
    // codec context structurally valid: an attacker that flips a
    // single CompressionType byte to relabel zstd-encrypted data
    // as plaintext-with-a-dictionary breaks at this gate before
    // the AEAD even runs.
    if compression_type != 4 && dict_id != 0 {
        return Err(DecryptError::MalformedMetadataFrame(
            "non-zero DictID with non-ZstdDict CompressionType",
        ));
    }
    if !matches!(compression_type, 3 | 4) && window_log != 0 {
        return Err(DecryptError::MalformedMetadataFrame(
            "non-zero WindowLog with non-zstd CompressionType",
        ));
    }
    // BlockFlags: transform-presence bitfield mirrored from the
    // Block::Header. The AAD binds the whole byte, so a relabel of any known
    // transform bit fails AEAD verification. Reject any bit outside the KNOWN
    // mask BEFORE running the AEAD: for an encrypted block this byte is the
    // only transform descriptor the reader can trust, so a forward-
    // incompatible block (one whose post-decrypt transform stack this build
    // does not understand) must fail-fast rather than authenticate and then
    // mis-process. Mirrors the same rejection in `Header::decode_from`.
    let block_flags = read_u8(&mut cursor)?;
    if block_flags & !crate::table::block::header::block_flags::KNOWN != 0 {
        return Err(DecryptError::MalformedMetadataFrame(
            "unknown bits set in BlockFlags",
        ));
    }
    // Zero-init scratch buffer that gets overwritten by the next
    // `read_exact` from the on-disk `MetadataPayload`. NOT a
    // hard-coded nonce: this is the read side, and the bytes that
    // end up here are whatever the writer wrote — `[0u8; 12]` is
    // just the initial fill before the read overwrites it.
    // CodeQL's "hard-coded cryptographic value" heuristic flags
    // the zero-init pattern; suppressing here with a comment.
    let mut nonce = [0u8; 12];
    std::io::Read::read_exact(&mut cursor, &mut nonce)
        .map_err(|_| DecryptError::MalformedMetadataFrame("truncated Nonce"))?;
    let mut tag = [0u8; TAG_LEN];
    std::io::Read::read_exact(&mut cursor, &mut tag)
        .map_err(|_| DecryptError::MalformedMetadataFrame("truncated AEADTag"))?;

    Ok(ParsedMetadata {
        suite_id,
        nonce,
        tag,
        ctx: EncryptionContext {
            header_byte,
            key_epoch,
            suite_id,
            compression_type,
            block_flags,
        },
        block_type_byte,
        dict_id,
        window_log,
    })
}

/// Key-free structural view of an AAD-bound encrypted block's metadata,
/// parsed from the on-disk `MetadataFrame` WITHOUT the encryption key.
///
/// Produced by [`parse_encrypted_block_metadata`] for offline forensic
/// inspection — e.g. an on-call engineer examining a block that fails to
/// decode, with no live process and no key. Every field is read from the
/// on-disk `MetadataFrame` mirror; nothing here requires decryption and the
/// ciphertext body is never touched beyond bounds-checking its frame.
///
/// The three AAD-binding fields `tree_id`, `table_id` and `block_offset` are
/// deliberately NOT stored on disk (they are supplied from read context at
/// decrypt time), so they are absent here — reconstructing the AAD for offline
/// verification requires the caller to supply them from the file path /
/// inventory.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct EncryptedBlockMetadata {
    /// Wire-format version (high nibble of the on-disk `HeaderByte`); `1` for v1.
    pub format_version: u8,
    /// Key epoch the block was sealed under (selects the key in the chain).
    pub key_epoch: u8,
    /// Raw `BlockType` discriminator byte (mirrors the block header).
    pub block_type: u8,
    /// AEAD suite the block was sealed with.
    pub suite_id: SuiteId,
    /// Raw `CompressionType` tag (`0` None, `1` Lz4, `3` Zstd, `4` `ZstdDict`).
    pub compression_type: u8,
    /// zstd dictionary id (non-zero only for `ZstdDict`).
    pub dict_id: u32,
    /// zstd window-log descriptor (`0`, or `10..=31`).
    pub window_log: u8,
    /// Transform-presence bitfield mirrored from the block header.
    pub block_flags: u8,
    /// 12-byte AEAD nonce.
    pub nonce: [u8; 12],
    /// 16-byte AEAD authentication tag.
    pub aead_tag: [u8; TAG_LEN],
    /// Length in bytes of the encrypted `BodyFrame` payload (the ciphertext).
    pub ciphertext_len: usize,
}

/// Parses the metadata of an AAD-bound encrypted block WITHOUT decrypting it.
///
/// Walks the on-disk `MetadataFrame ‖ BodyFrame` envelope, validating the
/// `MetadataFrame` structure (magic, length, codec-context invariants) and the
/// `BodyFrame` bounds, and returns the [`EncryptedBlockMetadata`] view. No key
/// is consulted and no plaintext is produced — this is the read-only structural
/// parse a forensics tool uses to inspect a block that fails to decode.
///
/// Trailing bytes after the `BodyFrame` (e.g. a Page ECC parity trailer added
/// outside the encryption envelope) are ignored, so this accepts either the
/// bare envelope or an envelope followed by such a trailer.
///
/// # Errors
///
/// Returns [`DecryptError::MalformedMetadataFrame`] / [`DecryptError::MalformedBodyFrame`]
/// for a structurally invalid envelope, [`DecryptError::UnsupportedFormatVersion`]
/// for an unknown wire version, or [`DecryptError::UnsupportedSuite`] for an
/// unregistered suite byte.
///
/// # Examples
///
/// ```
/// use lsm_tree::encryption::{
///     StaticKeyChain, encrypt_block, parse_encrypted_block_metadata,
///     aad::{BlockIdentity, BlockType, EncryptionContext, SuiteId},
/// };
///
/// let chain = StaticKeyChain::new().with_key(1, [0x42; 32]);
/// let ctx = EncryptionContext::v1(1, SuiteId::Aes256Gcm, 0, 0);
/// let id = BlockIdentity { table_id: 7, block_type: BlockType::Data, dict_id: 0, window_log: 0 };
/// let sealed = encrypt_block(b"payload bytes", &id, &ctx, &chain).unwrap();
///
/// let meta = parse_encrypted_block_metadata(&sealed).unwrap();
/// assert_eq!(meta.format_version, 1);
/// assert_eq!(meta.key_epoch, 1);
/// assert_eq!(meta.suite_id, SuiteId::Aes256Gcm);
/// assert_eq!(meta.dict_id, 0);
/// ```
pub fn parse_encrypted_block_metadata(
    bytes: &[u8],
) -> Result<EncryptedBlockMetadata, DecryptError> {
    let mut cursor = Cursor::new(bytes);
    let metadata_payload = read_framed_payload(
        &mut cursor,
        Some(METADATA_VARIANT),
        METADATA_PAYLOAD_LEN_V1,
        METADATA_PAYLOAD_LEN_V1,
        DecryptError::MalformedMetadataFrame,
    )?;
    let parsed = decode_metadata_payload(&metadata_payload)?;
    // Forensic parse needs only the body LENGTH, not its bytes — read the
    // BodyFrame header and validate its bounds without allocating / copying
    // the ciphertext (which can be up to 256 MiB).
    let ciphertext_len = read_framed_payload_len(
        &mut cursor,
        Some(BODY_VARIANT),
        1,
        MAX_BODY_LEN,
        DecryptError::MalformedBodyFrame,
    )?;
    // The length-only read does not consume the payload, so verify the input
    // actually contains the advertised ciphertext bytes — otherwise a block cut
    // off right after the BodyFrame header would be reported as structurally
    // valid with a ciphertext_len for bytes that aren't there.
    let remaining = u64::try_from(bytes.len())
        .unwrap_or(u64::MAX)
        .saturating_sub(cursor.position());
    if u64::from(ciphertext_len) > remaining {
        return Err(DecryptError::MalformedBodyFrame("truncated frame payload"));
    }
    let ciphertext_len = ciphertext_len as usize;
    Ok(EncryptedBlockMetadata {
        format_version: parsed.ctx.header_byte >> 4,
        key_epoch: parsed.ctx.key_epoch,
        block_type: parsed.block_type_byte,
        suite_id: parsed.suite_id,
        compression_type: parsed.ctx.compression_type,
        dict_id: parsed.dict_id,
        window_log: parsed.window_log,
        block_flags: parsed.ctx.block_flags,
        nonce: parsed.nonce,
        aead_tag: parsed.tag,
        ciphertext_len,
    })
}

/// Reconstructs the [`AAD_LEN`]-byte AAD an AEAD verify of this block would
/// use, from the on-disk `MetadataFrame` plus the caller-supplied `table_id`,
/// WITHOUT the key.
///
/// For offline forensic AEAD verification with an externally-held key: the AAD
/// is never written to disk, so a key holder needs to rebuild it to check the
/// tag. The on-disk-mirrored fields (header byte, key epoch, block type, suite,
/// compression type, dict id, window log, block flags) come from the block;
/// `table_id` is an AAD-binding field that is NOT stored on disk and must be
/// supplied from read context (the owning SST's table id).
///
/// Note: the block's byte offset and owning tree id are intentionally NOT
/// bound in the AAD (see [`build`]), so they are not required here — only
/// `table_id` is.
///
/// # Errors
///
/// Same envelope-structure errors as [`parse_encrypted_block_metadata`] for a
/// malformed `MetadataFrame`, plus [`DecryptError::MalformedMetadataFrame`] if
/// the on-disk block-type byte is not a known [`crate::table::block::BlockType`].
///
/// # Examples
///
/// ```
/// use lsm_tree::encryption::{
///     StaticKeyChain, encrypt_block, reconstruct_block_aad,
///     aad::{BlockIdentity, BlockType, EncryptionContext, SuiteId},
/// };
///
/// let chain = StaticKeyChain::new().with_key(1, [0x42; 32]);
/// let ctx = EncryptionContext::v1(1, SuiteId::Aes256Gcm, 0, 0);
/// let id = BlockIdentity { table_id: 7, block_type: BlockType::Data, dict_id: 0, window_log: 0 };
/// let sealed = encrypt_block(b"payload bytes", &id, &ctx, &chain).unwrap();
///
/// // Reconstruct the AAD with the owning table id (the only AAD field not on disk).
/// let aad = reconstruct_block_aad(&sealed, 7).unwrap();
/// assert_eq!(aad.len(), 23);
/// ```
pub fn reconstruct_block_aad(bytes: &[u8], table_id: u64) -> Result<[u8; AAD_LEN], DecryptError> {
    let mut cursor = Cursor::new(bytes);
    let metadata_payload = read_framed_payload(
        &mut cursor,
        Some(METADATA_VARIANT),
        METADATA_PAYLOAD_LEN_V1,
        METADATA_PAYLOAD_LEN_V1,
        DecryptError::MalformedMetadataFrame,
    )?;
    let parsed = decode_metadata_payload(&metadata_payload)?;
    let block_type = crate::table::block::BlockType::try_from(parsed.block_type_byte)
        .map_err(|_| DecryptError::MalformedMetadataFrame("unknown BlockType byte"))?;
    let identity = BlockIdentity {
        table_id,
        block_type,
        dict_id: parsed.dict_id,
        window_log: parsed.window_log,
    };
    Ok(build(&parsed.ctx, &identity))
}

/// Seals `plaintext` into the AAD-bound `MetadataFrame ‖ BodyFrame`
/// byte sequence.
///
/// Reads the active key from `key_chain` at `ctx.key_epoch`, draws
/// a fresh 12-byte nonce from a CSPRNG, builds the 39-byte AAD via
/// [`aad::build`](super::aad::build), encrypts the plaintext in place via
/// [`encrypt_in_place`], and serialises the result.
///
/// # Errors
///
/// All failure paths surface as [`crate::Error::Encrypt`] carrying
/// a `&'static str` description of which invariant or step failed:
///
/// - `"HeaderByte high nibble does not match FORMAT_VERSION_V1 ..."` /
///   `"HeaderByte low nibble is reserved ..."` — `ctx.header_byte`
///   does not match the v1 wire-format contract (spec §4.8).
/// - `"KeyEpoch not present in caller's KeyChain"` — `ctx.key_epoch`
///   is unknown to the supplied [`KeyChain`]. The encode path
///   distinguishes this from the symmetric decode-side
///   [`DecryptError::UnknownKeyEpoch`] only by error type
///   (`Error::Encrypt` vs `DecryptError`); semantically both
///   signal "epoch not in chain", but on the write path the
///   caller controls the chain so a missing epoch is a caller
///   configuration bug rather than the bit-rot / key-rotation
///   drift signal the decode-side variant carries.
/// - `"plaintext must be non-empty ..."` /
///   `"plaintext exceeds 256 MiB body cap"` — payload outside the
///   spec §5.3 `BodyFrame` range.
/// - `"invalid CompressionType ..."` /
///   `"non-zero DictID ..."` /
///   `"non-zero WindowLog ..."` /
///   `"WindowLog outside valid range ..."` — spec §5.1
///   cross-field codec invariants violated.
/// - AEAD primitive rejections (e.g. wrong nonce length for the
///   suite — defensive; the caller's CSPRNG always produces 12
///   bytes for v1 suites) propagate as `Error::Encrypt` with the
///   message from the underlying AEAD crate.
pub fn encrypt_block(
    plaintext: &[u8],
    identity: &BlockIdentity,
    ctx: &EncryptionContext,
    key_chain: &dyn KeyChain,
) -> crate::Result<Vec<u8>> {
    // Spec `docs/aad-block-format.md` §4.8: the HeaderByte high
    // nibble encodes the format version (must be V1 = 0x1 today)
    // and the low nibble is RESERVED and MUST be zero on write.
    // Readers ignore the low nibble for forward-compatibility, so
    // a caller setting reserved bits would silently produce output
    // that future suites might interpret differently — catch the
    // shape violation at write time. `EncryptionContext::v1`
    // sets the correct byte automatically, but the struct's fields
    // are `pub` so a hand-rolled context could land here with a
    // wrong byte.
    if (ctx.header_byte >> 4) != FORMAT_VERSION_V1 {
        return Err(crate::Error::Encrypt(
            "HeaderByte high nibble does not match FORMAT_VERSION_V1 (spec §4.8)",
        ));
    }
    if (ctx.header_byte & 0x0F) != 0 {
        return Err(crate::Error::Encrypt(
            "HeaderByte low nibble is reserved and must be zero on write (spec §4.8)",
        ));
    }
    // Symmetric to the decrypt path: reject any BlockFlags bit outside the
    // KNOWN transform mask so this version never PRODUCES a block its own
    // decrypt would reject as forward-incompatible. `EncryptionContext` fields
    // are `pub`, so a hand-rolled context could carry reserved bits.
    if ctx.block_flags & !crate::table::block::header::block_flags::KNOWN != 0 {
        return Err(crate::Error::Encrypt(
            "BlockFlags has bits set outside the known transform mask",
        ));
    }

    // Look up the key for this epoch. Missing epoch on encode is a
    // CALLER configuration bug — the caller owns the chain — so
    // surface it as `Error::Encrypt` rather than `Unrecoverable`
    // (the latter is reserved for disk-corruption / missing-file
    // conditions and would route through the wrong recovery
    // / alerting paths in the consumer).
    let key = key_chain.key(ctx.key_epoch).ok_or_else(|| {
        log::error!(
            "encrypt_block: KeyEpoch {} not present in caller's KeyChain",
            ctx.key_epoch,
        );
        crate::Error::Encrypt("KeyEpoch not present in caller's KeyChain")
    })?;

    // Spec `docs/aad-block-format.md` §5.3 row "BodyFrame
    // PayloadLen": valid range `[1, 256 MiB]` for v1 suites.
    // Both bounds are enforced on the write path so an empty or
    // oversized plaintext fails at encode time rather than
    // producing a sealed block the decoder would later reject.
    if plaintext.is_empty() {
        return Err(crate::Error::Encrypt(
            "plaintext must be non-empty per AAD-bound spec (BodyFrame PayloadLen >= 1)",
        ));
    }
    if plaintext.len() > MAX_BODY_LEN as usize {
        return Err(crate::Error::Encrypt("plaintext exceeds 256 MiB body cap"));
    }

    // Spec §5.1 cross-field invariants. The read path enforces
    // these in `decode_metadata_payload`, so producing a block
    // here with an invalid combination yields output that
    // `decrypt_block` is guaranteed to reject as
    // `MalformedMetadataFrame`. Mirror the checks on the encode
    // path so the failure surfaces at write time (a caller bug)
    // rather than as silent "unreadable later" data corruption.
    //
    // Valid `CompressionType` tags per spec: 0 = None, 1 = Lz4,
    // 3 = Zstd, 4 = ZstdDict.
    if !matches!(ctx.compression_type, 0 | 1 | 3 | 4) {
        return Err(crate::Error::Encrypt(
            "invalid CompressionType (spec §5.1: must be 0=None, 1=Lz4, 3=Zstd, or 4=ZstdDict)",
        ));
    }
    // `DictID` is non-zero ONLY when `CompressionType == 4`
    // (ZstdDict); other codecs must record `DictID = 0`.
    if identity.dict_id != 0 && ctx.compression_type != 4 {
        return Err(crate::Error::Encrypt(
            "non-zero DictID with non-ZstdDict CompressionType (spec §5.1)",
        ));
    }
    // `WindowLog` is non-zero ONLY when `CompressionType` is a
    // zstd-family codec (tags 3 or 4); non-zstd codecs must
    // record `WindowLog = 0`.
    if identity.window_log != 0 && !matches!(ctx.compression_type, 3 | 4) {
        return Err(crate::Error::Encrypt(
            "non-zero WindowLog with non-zstd CompressionType (spec §5.1)",
        ));
    }
    // Spec §5.1: WindowLog values are 0 (no zstd / no window
    // enforcement) or 10..=31 (RFC 8878 §3.1.1.1.2 decoded
    // window-descriptor range). Any other value is structurally
    // invalid.
    if identity.window_log != 0 && !(10..=31).contains(&identity.window_log) {
        return Err(crate::Error::Encrypt(
            "WindowLog outside valid range (spec §5.1: must be 0 or 10..=31)",
        ));
    }

    // CSPRNG-derived 12-byte nonce. `<[u8; 12]>::generate()`
    // pulls fresh entropy from getrandom's OS-backed `SysRng`
    // (same path the legacy `Aes256GcmProvider` uses to seed its
    // thread-local ChaCha20). Panics on OS entropy failure — a
    // process that can't read entropy from the kernel cannot
    // produce a unique nonce, and silently reusing one would
    // break GCM's confidentiality. Treat that as an unrecoverable
    // environment fault, same as the rest of the encryption
    // module already does.
    let nonce: [u8; 12] = <[u8; 12]>::generate();

    // Build the 23-byte AAD: binds ciphertext to format identity,
    // header byte, key epoch, block type, suite id, table id,
    // compression type, dict id, window log, and block_flags. (Block
    // offset and tree id are intentionally not bound — see aad::build.)
    let aad = build(ctx, identity);

    // Encrypt the plaintext in-place; move it into an owned Vec
    // first so the original slice stays unmodified on the caller
    // side.
    let mut body = plaintext.to_vec();
    let tag = encrypt_in_place(ctx.suite_id, key, &nonce, &aad, &mut body)?;

    // Pack the MetadataPayload and wrap both halves in
    // SkippableFrames. encode_into appends the 8-byte SFA framing
    // header (magic + length) ahead of each payload.
    let metadata_payload = encode_metadata_payload(*ctx, identity, &nonce, &tag);
    let metadata_frame = SkippableFrame::new(METADATA_VARIANT, metadata_payload)
        .map_err(|_| crate::Error::Encrypt("MetadataFrame construction failed"))?;
    let body_frame = SkippableFrame::new(BODY_VARIANT, body)
        .map_err(|_| crate::Error::Encrypt("BodyFrame construction failed"))?;

    let total_size = metadata_frame.serialized_size() + body_frame.serialized_size();
    let mut out = Vec::with_capacity(total_size);
    metadata_frame
        .encode_into(&mut out)
        .map_err(|_| crate::Error::Encrypt("MetadataFrame serialisation failed"))?;
    body_frame
        .encode_into(&mut out)
        .map_err(|_| crate::Error::Encrypt("BodyFrame serialisation failed"))?;
    Ok(out)
}

/// Output of [`decrypt_block`]: the decrypted plaintext PLUS the
/// on-disk-recorded codec context that participated in the AAD.
///
/// The caller must thread `dict_id` and `window_log` through
/// `structured_zstd::decoding::FrameDecoder::expect_dict_id` /
/// `expect_window_log` (or equivalent) when feeding the
/// plaintext into a zstd frame decoder — that's the spec's
/// post-decrypt validation contract from
/// `docs/aad-block-format.md` §5.3, preventing inner-frame header
/// mismatch / `DoS` via crafted zstd frames.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DecryptedBlock {
    /// Recovered plaintext from the `BodyFrame`'s AEAD ciphertext.
    /// For zstd-compressed blocks this is the compressed inner
    /// frame; the caller decompresses with the
    /// `expect_dict_id`/`expect_window_log` setters
    /// initialised from [`Self::dict_id`] / [`Self::window_log`].
    pub plaintext: Vec<u8>,

    /// Codec discriminator parsed from `MetadataPayload` offset 4.
    /// Spec-defined tags: 0=None, 1=Lz4, 3=Zstd, 4=ZstdDict.
    pub compression_type: u8,

    /// `DictID` parsed from `MetadataPayload` offset 5 (u32 BE).
    /// Zero when not using a zstd dictionary.
    pub dict_id: u32,

    /// `WindowLog` parsed from `MetadataPayload` offset 9. Zero
    /// when not using zstd; `10..=31` per RFC 8878 otherwise.
    pub window_log: u8,

    /// `block_flags` transform-presence bitfield parsed from
    /// `MetadataPayload` offset 10 (mirror of the `Block::Header` byte).
    /// Lets the caller know which transform layers the block carries —
    /// e.g. whether to strip a per-KV checksum footer — after decryption.
    pub block_flags: u8,
}

/// Recovers plaintext from the `MetadataFrame ‖ BodyFrame` byte
/// sequence produced by [`encrypt_block`].
///
/// Reads the `MetadataFrame`, parses the 39-byte payload, decodes
/// the `BodyFrame`, reconstructs the AAD from `identity` + the
/// parsed `EncryptionContext`, looks up the matching key from
/// `key_chain`, runs [`decrypt_in_place`], then requires the input
/// to end exactly at the `BodyFrame`: the encrypted-block format
/// defines no trailing frames, so any extra bytes are rejected.
///
/// `identity` MUST supply the AAD-bound `table_id` that is NOT
/// recorded on disk; a mismatch propagates through the AAD and
/// surfaces as [`DecryptError::AeadVerificationFailed`]. A block's
/// byte offset and the owning tree id are deliberately not bound
/// (see [`crate::encryption::aad::build`]). The on-disk-recorded AAD
/// fields (`HeaderByte`, `KeyEpoch`, `BlockType`, `SuiteID`,
/// `CompressionType`, `DictID`, `WindowLog`) are read back from the
/// `MetadataPayload` regardless of what the caller supplies on
/// `identity.block_type` / `identity.dict_id` / `identity.window_log`
/// — those fields are IGNORED on the read path because the disk is
/// the source of truth for them.
///
/// The returned `compression_type` / `dict_id` / `window_log`
/// fields on [`DecryptedBlock`] are the spec's post-decrypt
/// validation contract: the caller MUST pass them through
/// `FrameDecoder::expect_dict_id` / `expect_window_log`
/// before any zstd decode (per `docs/aad-block-format.md` §5.3).
/// `decrypt_block` does not do the decompression itself — the
/// crate's Block I/O layer owns that step.
///
/// # Errors
///
/// See [`DecryptError`] for the failure-mode taxonomy.
pub fn decrypt_block(
    bytes: &[u8],
    identity: &BlockIdentity,
    key_chain: &dyn KeyChain,
) -> Result<DecryptedBlock, DecryptError> {
    let mut cursor = Cursor::new(bytes);

    // ── MetadataFrame ──────────────────────────────────────────
    // v1 MetadataPayload is fixed at exactly 39 bytes; reject any
    // other declared length upfront so a forged `PayloadLen`
    // can't allocate-and-then-discard.
    let metadata_payload = read_framed_payload(
        &mut cursor,
        Some(METADATA_VARIANT),
        METADATA_PAYLOAD_LEN_V1,
        METADATA_PAYLOAD_LEN_V1,
        DecryptError::MalformedMetadataFrame,
    )?;
    let parsed = decode_metadata_payload(&metadata_payload)?;

    // ── BodyFrame ──────────────────────────────────────────────
    // Spec `docs/aad-block-format.md` §5.3 row "BodyFrame
    // PayloadLen": valid range `[1, 256 MiB]` for v1 suites.
    // Empty body is forbidden by spec — encrypt_block enforces
    // the matching rule on the write side.
    let mut ciphertext = read_framed_payload(
        &mut cursor,
        Some(BODY_VARIANT),
        1,
        MAX_BODY_LEN,
        DecryptError::MalformedBodyFrame,
    )?;

    // ── Strict end-of-block ────────────────────────────────────
    // The encrypted-block wire format is exactly MetadataFrame ‖
    // BodyFrame. No trailing frames are defined (ECC-at-rest lives
    // in the Page ECC parity trailer outside the encryption
    // envelope), so any bytes past the BodyFrame are malformed
    // trailing data and the block is rejected rather than silently
    // accepted.
    let pos = cursor.position();
    let total = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if pos != total {
        return Err(DecryptError::MalformedBodyFrame(
            "unexpected trailing bytes after BodyFrame",
        ));
    }

    // ── AAD reconstruction ──────────────────────────────────────
    // Reconstruct the BlockIdentity that participates in AAD using
    // the on-disk-mirrored fields from the MetadataPayload (dict_id,
    // window_log, block_type) plus the caller-supplied identity
    // (table_id). Block-type byte goes through TryFrom so an unknown
    // discriminator surfaces as MalformedMetadataFrame rather than
    // being silently coerced.
    let block_type = crate::table::block::BlockType::try_from(parsed.block_type_byte)
        .map_err(|_| DecryptError::MalformedMetadataFrame("unknown BlockType byte"))?;
    let aad_identity = BlockIdentity {
        table_id: identity.table_id,
        block_type,
        dict_id: parsed.dict_id,
        window_log: parsed.window_log,
    };
    let aad = build(&parsed.ctx, &aad_identity);
    debug_assert_eq!(aad.len(), AAD_LEN);

    // ── Key lookup ──────────────────────────────────────────────
    let key = key_chain
        .key(parsed.ctx.key_epoch)
        .ok_or(DecryptError::UnknownKeyEpoch {
            key_epoch: parsed.ctx.key_epoch,
        })?;

    // ── AEAD verify + decrypt in-place ──────────────────────────
    decrypt_in_place(
        parsed.suite_id,
        key,
        &parsed.nonce,
        &aad,
        &parsed.tag,
        &mut ciphertext,
    )?;
    Ok(DecryptedBlock {
        plaintext: ciphertext,
        compression_type: parsed.ctx.compression_type,
        dict_id: parsed.dict_id,
        window_log: parsed.window_log,
        block_flags: parsed.ctx.block_flags,
    })
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::indexing_slicing, reason = "test code")]
mod tests {
    use super::*;
    use crate::encryption::key_chain::StaticKeyChain;
    use crate::table::block::BlockType;

    const TEST_KEY: [u8; 32] = [0x42; 32];
    const TEST_KEY_OTHER: [u8; 32] = [0x55; 32];

    fn id() -> BlockIdentity {
        BlockIdentity {
            table_id: 0x1234_5678_9ABC_DEF0,
            block_type: BlockType::Data,
            dict_id: 0,
            window_log: 0,
        }
    }

    fn ctx() -> EncryptionContext {
        EncryptionContext::v1(0, SuiteId::Aes256Gcm, 0, 0)
    }

    fn chain() -> StaticKeyChain {
        StaticKeyChain::new().with_key(0, TEST_KEY)
    }

    #[test]
    fn parse_metadata_is_key_free_and_matches_seal() {
        // Forensic parse needs no key: seal a block, then read its
        // MetadataPayload structurally with `parse_encrypted_block_metadata`
        // (no KeyChain) and confirm the fields mirror what was sealed.
        let plaintext = b"forensic payload bytes";
        let sealed = encrypt_block(plaintext, &id(), &ctx(), &chain()).unwrap();

        let meta = parse_encrypted_block_metadata(&sealed).unwrap();
        assert_eq!(meta.format_version, 1);
        assert_eq!(meta.key_epoch, 0);
        assert_eq!(meta.suite_id, SuiteId::Aes256Gcm);
        assert_eq!(meta.block_type, u8::from(BlockType::Data));
        assert_eq!(meta.compression_type, 0);
        assert_eq!(meta.dict_id, 0);
        assert_eq!(meta.window_log, 0);
        assert!(meta.ciphertext_len > 0, "body must carry ciphertext");

        // Suite is reflected for ChaCha too (proves it reads the on-disk
        // SuiteID byte, not a hard-coded default).
        let chacha_ctx = EncryptionContext::v1(0, SuiteId::ChaCha20Poly1305, 0, 0);
        let sealed_cc = encrypt_block(plaintext, &id(), &chacha_ctx, &chain()).unwrap();
        let meta_cc = parse_encrypted_block_metadata(&sealed_cc).unwrap();
        assert_eq!(meta_cc.suite_id, SuiteId::ChaCha20Poly1305);

        // Garbage / truncated input is a typed error, never a panic.
        assert!(parse_encrypted_block_metadata(b"not a frame").is_err());
        assert!(parse_encrypted_block_metadata(&sealed[..10]).is_err());
    }

    #[test]
    fn parse_metadata_rejects_truncated_body() {
        // Regression: the forensic parser reads only the BodyFrame header (for
        // ciphertext_len) without the payload. A block cut off right after that
        // header — full MetadataFrame + BodyFrame header, but zero ciphertext
        // bytes — must still be rejected, not reported as structurally valid
        // with a ciphertext_len for bytes that aren't there.
        let sealed = encrypt_block(b"forensic payload bytes", &id(), &ctx(), &chain()).unwrap();
        // MetadataFrame = 8-byte SFA header + 39-byte payload; BodyFrame header
        // = 8 bytes. Cut to exactly that boundary: header present, body absent.
        let cut = 8 + METADATA_PAYLOAD_LEN_V1 as usize + 8;
        assert!(
            sealed.len() > cut,
            "test setup: sealed block must extend past the body header",
        );
        let err = parse_encrypted_block_metadata(&sealed[..cut])
            .expect_err("truncated body must be rejected");
        assert!(
            matches!(err, DecryptError::MalformedBodyFrame(_)),
            "expected MalformedBodyFrame for a truncated body, got {err:?}",
        );
    }

    #[test]
    fn reconstruct_aad_matches_seal_with_correct_table_id() {
        // The AAD is never on disk; offline AEAD verification needs it rebuilt.
        // Reconstructing with the SAME table_id the block was sealed under must
        // yield byte-for-byte the AAD encrypt_block used.
        let sealed = encrypt_block(b"forensic payload bytes", &id(), &ctx(), &chain()).unwrap();
        let expected = build(&ctx(), &id());
        let got = reconstruct_block_aad(&sealed, id().table_id).unwrap();
        assert_eq!(got.len(), AAD_LEN);
        assert_eq!(
            got, expected,
            "reconstructed AAD must match the sealing AAD"
        );

        // A different table_id binds to a different AAD (table_id IS in the AAD).
        let other = reconstruct_block_aad(&sealed, id().table_id ^ 1).unwrap();
        assert_ne!(got, other, "table_id must affect the reconstructed AAD");

        // Malformed input is a typed error, not a panic.
        assert!(reconstruct_block_aad(b"not a frame", 0).is_err());
    }

    #[test]
    fn roundtrip_aes_recovers_plaintext() {
        let plaintext = b"the quick brown fox";
        let sealed = encrypt_block(plaintext, &id(), &ctx(), &chain()).unwrap();
        let recovered = decrypt_block(&sealed, &id(), &chain()).unwrap();
        assert_eq!(&recovered.plaintext[..], plaintext);
        // Codec context echoes back from the MetadataPayload — the
        // caller is expected to thread these through structured-zstd's
        // FrameDecoder::expect_dict_id / expect_window_log
        // setters when feeding the plaintext into a zstd decode.
        assert_eq!(recovered.compression_type, 0);
        assert_eq!(recovered.dict_id, 0);
        assert_eq!(recovered.window_log, 0);
    }

    #[test]
    fn roundtrip_chacha_recovers_plaintext() {
        let plaintext = b"the quick brown fox";
        let chacha_ctx = EncryptionContext::v1(0, SuiteId::ChaCha20Poly1305, 0, 0);
        let sealed = encrypt_block(plaintext, &id(), &chacha_ctx, &chain()).unwrap();
        let recovered = decrypt_block(&sealed, &id(), &chain()).unwrap();
        assert_eq!(&recovered.plaintext[..], plaintext);
    }

    #[test]
    fn wrong_key_in_chain_surfaces_aead_failure() {
        let plaintext = b"the quick brown fox";
        let sealed = encrypt_block(plaintext, &id(), &ctx(), &chain()).unwrap();
        // Reader's chain has a DIFFERENT 32-byte key under the same epoch.
        let wrong = StaticKeyChain::new().with_key(0, TEST_KEY_OTHER);
        let err = decrypt_block(&sealed, &id(), &wrong).unwrap_err();
        assert!(
            matches!(err, DecryptError::AeadVerificationFailed),
            "expected AeadVerificationFailed, got {err:?}",
        );
    }

    #[test]
    fn missing_key_epoch_surfaces_unknown_key_epoch() {
        let plaintext = b"the quick brown fox";
        // Writer uses epoch 0; reader's chain has epoch 1 only.
        let sealed = encrypt_block(plaintext, &id(), &ctx(), &chain()).unwrap();
        let no_epoch_zero = StaticKeyChain::new().with_key(1, TEST_KEY);
        let err = decrypt_block(&sealed, &id(), &no_epoch_zero).unwrap_err();
        assert!(
            matches!(err, DecryptError::UnknownKeyEpoch { key_epoch: 0 }),
            "expected UnknownKeyEpoch {{ key_epoch: 0 }}, got {err:?}",
        );
    }

    #[test]
    fn cross_identity_substitution_surfaces_aead_failure() {
        // Same plaintext, sealed under one BlockIdentity. Reader
        // attempts to decrypt with a DIFFERENT BlockIdentity (table_id
        // flipped). AAD binds table_id (not tree_id — that is no longer
        // part of the identity), so the mismatch surfaces as AEAD failure.
        let plaintext = b"the quick brown fox";
        let sealed = encrypt_block(plaintext, &id(), &ctx(), &chain()).unwrap();
        let mut wrong_id = id();
        wrong_id.table_id ^= 0x1; // flip one bit of the table id
        let err = decrypt_block(&sealed, &wrong_id, &chain()).unwrap_err();
        assert!(matches!(err, DecryptError::AeadVerificationFailed));
    }

    #[test]
    fn trailing_bytes_after_body_are_rejected() {
        // The encrypted-block format is exactly MetadataFrame ‖
        // BodyFrame; nothing follows. A well-formed block with extra
        // bytes appended (e.g. a stray skippable frame from a
        // retired extension, or junk) must be rejected, not silently
        // accepted by ignoring the tail.
        let plaintext = b"the quick brown fox";
        let mut sealed = encrypt_block(plaintext, &id(), &ctx(), &chain()).unwrap();
        // The clean block round-trips.
        assert!(decrypt_block(&sealed, &id(), &chain()).is_ok());
        // Append trailing bytes; decrypt must now reject.
        sealed.extend_from_slice(&[0xAB, 0xCD, 0xEF, 0x01]);
        let err = decrypt_block(&sealed, &id(), &chain()).unwrap_err();
        assert!(
            matches!(err, DecryptError::MalformedBodyFrame(_)),
            "expected MalformedBodyFrame for trailing bytes, got {err:?}",
        );
    }

    #[test]
    fn truncated_input_surfaces_malformed_metadata() {
        let plaintext = b"the quick brown fox";
        let sealed = encrypt_block(plaintext, &id(), &ctx(), &chain()).unwrap();
        // Cut to just the first frame header (no payload).
        let truncated = &sealed[..6];
        let err = decrypt_block(truncated, &id(), &chain()).unwrap_err();
        assert!(matches!(err, DecryptError::MalformedMetadataFrame(_)));
    }

    #[test]
    fn encrypt_block_rejects_empty_plaintext() {
        // Spec docs/aad-block-format.md §5.3 row "BodyFrame
        // PayloadLen": valid range [1, 256 MiB] for v1 suites.
        // encrypt_block enforces the >= 1 floor so an empty input
        // can't produce a sealed block the decoder would reject.
        let err = encrypt_block(&[], &id(), &ctx(), &chain()).unwrap_err();
        assert!(
            matches!(err, crate::Error::Encrypt(_)),
            "expected Error::Encrypt for empty plaintext, got {err:?}",
        );
    }

    #[test]
    fn encrypt_block_rejects_unknown_block_flags_bit() {
        // Symmetric to the decrypt-side rejection: encrypt_block must refuse to
        // PRODUCE a block whose BlockFlags carry a bit outside the KNOWN mask,
        // so this version never seals something its own decrypt rejects as
        // forward-incompatible.
        let mut c = ctx();
        c.block_flags = 0x10; // reserved bit, outside KNOWN
        let err = encrypt_block(b"payload", &id(), &c, &chain()).unwrap_err();
        assert!(
            matches!(err, crate::Error::Encrypt(_)),
            "expected Error::Encrypt for unknown BlockFlags bit, got {err:?}",
        );
    }

    #[test]
    fn invalid_window_log_surfaces_malformed_metadata() {
        // WindowLog spec: 0 (no enforcement) or 10..=31. Tamper a
        // sealed block to put a forbidden value (9) in the
        // WindowLog byte; the decoder must reject before any AEAD
        // work even though the AEAD tag is over the AAD that
        // includes window_log (so a subsequent tag-verify would
        // ALSO fail, but the structural check fires first).
        let plaintext = b"the quick brown fox";
        let mut sealed = encrypt_block(plaintext, &id(), &ctx(), &chain()).unwrap();
        // MetadataFrame layout: [4 magic][4 PayloadLen][1 HeaderByte]
        // [1 KeyEpoch][1 BlockType][1 SuiteID][1 CompressionType]
        // [4 DictID][1 WindowLog][...]. WindowLog is at offset 8 + 9
        // = 17 from the start of the sealed bytes.
        sealed[17] = 9; // invalid (< 10, not zero)
        let err = decrypt_block(&sealed, &id(), &chain()).unwrap_err();
        assert!(
            matches!(err, DecryptError::MalformedMetadataFrame(_)),
            "expected MalformedMetadataFrame for WindowLog=9, got {err:?}",
        );
    }

    #[test]
    fn oversized_body_payload_len_rejected_before_alloc() {
        // Forge the BodyFrame's PayloadLen to advertise the maximum
        // legal u32 — a naive decoder would try to allocate ~4 GiB
        // before realising the underlying reader has no such data.
        // The upfront cap rejects before any allocation.
        let plaintext = b"the quick brown fox";
        let mut sealed = encrypt_block(plaintext, &id(), &ctx(), &chain()).unwrap();
        // MetadataFrame total size = 8 (framing) + METADATA_PAYLOAD_LEN_V1.
        // BodyFrame starts right after it; its PayloadLen is at frame offset 4.
        // Derive from the constant so this keeps hitting PayloadLen (not the
        // BodyFrame magic) if the payload length ever changes again.
        let metadata_frame_len = 8 + METADATA_PAYLOAD_LEN_V1 as usize;
        let body_payload_len_at = metadata_frame_len + 4;
        sealed[body_payload_len_at..body_payload_len_at + 4]
            .copy_from_slice(&u32::MAX.to_le_bytes());
        let err = decrypt_block(&sealed, &id(), &chain()).unwrap_err();
        assert!(
            matches!(err, DecryptError::MalformedBodyFrame(_)),
            "expected MalformedBodyFrame for oversized BodyFrame PayloadLen, got {err:?}",
        );
    }

    #[test]
    fn unknown_block_flags_bit_rejected_before_aead() {
        // For an encrypted block the BlockFlags byte is the only transform
        // descriptor the reader can trust. A byte with a reserved bit set (a
        // forward-incompatible transform stack this build cannot process) must
        // be rejected structurally — before the AEAD runs — not authenticated
        // and then mis-processed.
        let plaintext = b"the quick brown fox";
        let mut sealed = encrypt_block(plaintext, &id(), &ctx(), &chain()).unwrap();
        // BlockFlags sits at MetadataFrame payload offset 10 → absolute offset
        // 8 (framing) + 10 = 18. Set a reserved bit (1<<4) outside KNOWN.
        const BLOCK_FLAGS_AT: usize = 8 + 10;
        sealed[BLOCK_FLAGS_AT] |= 0x10;
        let err = decrypt_block(&sealed, &id(), &chain()).unwrap_err();
        assert!(
            matches!(err, DecryptError::MalformedMetadataFrame(_)),
            "expected MalformedMetadataFrame for unknown BlockFlags bit, got {err:?}",
        );
    }
}

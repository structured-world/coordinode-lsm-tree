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
//! reader hit the same [`aad::build`] call with the same inputs, so
//! the AEAD tag binds the ciphertext to the full block-identity +
//! codec context + key-epoch tuple. AAD is never written to disk.

use core::convert::TryFrom;
use std::io::Cursor;

use aes_gcm::aead::Generate;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
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
/// caller's payload-size cap BEFORE allocating, then reads
/// exactly that many bytes into a `Vec<u8>`.
///
/// Replaces a direct [`SkippableFrame::decode_from`] call so the
/// cap on `PayloadLen` is enforced ahead of the allocation — the
/// upstream API allocates the full declared length first and only
/// then surfaces caller-side caps, which means a forged
/// `PayloadLen = u32::MAX` would burn a 4 GiB allocation attempt
/// before the read even started. Decoding the header manually
/// rejects oversized frames at the cost of 8 bytes of upfront
/// I/O.
fn read_framed_payload_with_cap<R: std::io::Read>(
    reader: &mut R,
    expected_variant: u8,
    max_payload: u32,
    err_ctor: fn(&'static str) -> DecryptError,
) -> Result<Vec<u8>, DecryptError> {
    let mut header = [0u8; 8];
    reader
        .read_exact(&mut header)
        .map_err(|_| err_ctor("truncated skippable-frame header"))?;

    // 4-byte LE magic. Within the skippable-frame range
    // (variants 0..=15) this is `SKIPPABLE_MAGIC_START + variant`.
    let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    let variant = magic.wrapping_sub(SKIPPABLE_MAGIC_START);
    // `variant > 15` already excludes any value outside u8 range,
    // so the subsequent narrowing cast is exact — guarded above.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "guarded by `variant > 15` immediately above"
    )]
    let variant_byte = variant as u8;
    if variant > 15 || variant_byte != expected_variant {
        return Err(err_ctor("wrong frame magic / variant"));
    }

    // 4-byte LE payload length. Cap BEFORE allocating so a
    // crafted huge value rejects immediately.
    let payload_len = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
    if payload_len > max_payload {
        return Err(err_ctor("PayloadLen exceeds cap"));
    }

    let mut payload = vec![0u8; payload_len as usize];
    reader
        .read_exact(&mut payload)
        .map_err(|_| err_ctor("truncated frame payload"))?;
    Ok(payload)
}

/// `MetadataPayload` size for v1 suites: 38 bytes
/// (= `26 + NONCE_LEN` where v1 suites declare `NONCE_LEN` = 12).
const METADATA_PAYLOAD_LEN_V1: u32 = 38;

/// Upper bound on the encrypted body payload (256 MiB). Mirrors
/// the block-write cap on the plaintext path; rejecting larger
/// frames before allocation guards against a forged `BodyFrame`
/// `PayloadLen` triggering an unbounded `Vec` allocation on read.
const MAX_BODY_LEN: u32 = 256 * 1024 * 1024;

/// Encodes the 38-byte `MetadataPayload` (v1 suites only).
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
/// | 10     | 12   | Nonce             |
/// | 22     | 16   | AEADTag           |
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
    // `Vec<u8>` as a `Write` impl is infallible (the docs guarantee
    // `write_all` returns `Ok` and never short-writes), so this
    // branch can't fire. Map to `Unrecoverable` defensively
    // instead of `.expect`/`.unwrap`-ing so the storage crate's
    // `#[deny(clippy::expect_used)]` /
    // `#[deny(clippy::unwrap_used)]` invariants hold even on the
    // dead path.
    out.write_u32::<BigEndian>(identity.dict_id)
        .map_err(|_| ())
        .unwrap_or_else(|()| {
            log::error!("encode_metadata_payload: Vec writer returned Err (impossible)");
        });
    out.push(identity.window_log);
    out.extend_from_slice(nonce);
    out.extend_from_slice(tag);
    debug_assert_eq!(
        out.len(),
        METADATA_PAYLOAD_LEN_V1 as usize,
        "v1 MetadataPayload must be exactly 38 bytes"
    );
    out
}

/// Decoded `MetadataPayload` view — parses the 38 bytes into the
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
            "MetadataPayload length != 38 for v1",
        ));
    }
    let mut cursor = Cursor::new(payload);
    let read_u8 = |c: &mut Cursor<&[u8]>| {
        c.read_u8()
            .map_err(|_| DecryptError::MalformedMetadataFrame("truncated MetadataPayload"))
    };
    let header_byte = read_u8(&mut cursor)?;
    // Format version is the high nibble; v1 = 0b0001. Any other
    // version, or any non-zero low nibble, lands as
    // UnsupportedFormatVersion so the caller can tell "wrong
    // version" apart from "corrupted header byte".
    if (header_byte >> 4) != FORMAT_VERSION_V1 || (header_byte & 0x0F) != 0 {
        return Err(DecryptError::UnsupportedFormatVersion { header_byte });
    }
    let key_epoch = read_u8(&mut cursor)?;
    let block_type_byte = read_u8(&mut cursor)?;
    let suite_byte = read_u8(&mut cursor)?;
    let suite_id = SuiteId::try_from(suite_byte)
        .map_err(|s| DecryptError::UnsupportedSuite { suite_id: s })?;
    let compression_type = read_u8(&mut cursor)?;
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
        },
        block_type_byte,
        dict_id,
        window_log,
    })
}

/// Seals `plaintext` into the AAD-bound `MetadataFrame ‖ BodyFrame`
/// byte sequence.
///
/// Reads the active key from `key_chain` at `ctx.key_epoch`, draws
/// a fresh 12-byte nonce from a CSPRNG, builds the 38-byte AAD via
/// [`aad::build`], encrypts the plaintext in place via
/// [`encrypt_in_place`], and serialises the result.
///
/// # Errors
///
/// - [`crate::Error::Unrecoverable`] if `ctx.key_epoch` is not in
///   `key_chain` (encode side surfaces this as an opaque error —
///   the symmetric decode-side variant
///   [`DecryptError::UnknownKeyEpoch`] is distinguished from
///   [`DecryptError::AeadVerificationFailed`] on the read path,
///   but on the write path the caller controls the chain and a
///   missing epoch is a programmer bug).
/// - [`crate::Error::Encrypt`] if the AEAD primitive rejects the
///   inputs (e.g. wrong nonce length for the suite — defensive,
///   the caller's CSPRNG always produces 12 bytes for v1 suites).
pub fn encrypt_block(
    plaintext: &[u8],
    identity: &BlockIdentity,
    ctx: &EncryptionContext,
    key_chain: &dyn KeyChain,
) -> crate::Result<Vec<u8>> {
    // Look up the key for this epoch. Missing epoch on encode is a
    // programmer bug — the caller owns the chain.
    let key = key_chain.key(ctx.key_epoch).ok_or_else(|| {
        log::error!(
            "encrypt_block: KeyEpoch {} not present in caller's KeyChain",
            ctx.key_epoch,
        );
        crate::Error::Unrecoverable
    })?;

    // Cap the on-disk body payload at 256 MiB to keep the
    // BodyFrame's u32 PayloadLen field within
    // BodyFrame::PayloadLen ≤ MAX_BODY_LEN. Caller mistakes that
    // would otherwise produce undecodable frames surface here at
    // encode time rather than later on the read path.
    if plaintext.len() > MAX_BODY_LEN as usize {
        return Err(crate::Error::Encrypt("plaintext exceeds 256 MiB body cap"));
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

    // Build the 38-byte AAD: binds ciphertext to format identity,
    // header byte, key epoch, block type, suite id, tree id,
    // table id, block offset, compression type, dict id, window
    // log.
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

/// Recovers plaintext from the `MetadataFrame ‖ BodyFrame` byte
/// sequence produced by [`encrypt_block`].
///
/// Reads the `MetadataFrame`, parses the 38-byte payload, decodes
/// the `BodyFrame`, reconstructs the AAD from `identity` + the
/// parsed `EncryptionContext`, looks up the matching key from
/// `key_chain`, and runs [`decrypt_in_place`].
///
/// `identity` MUST supply the three AAD-bound fields that are
/// NOT recorded on disk: `tree_id`, `table_id`, and `block_offset`.
/// Any mismatch on any of those three propagates through the AAD
/// and surfaces as [`DecryptError::AeadVerificationFailed`]. The
/// six on-disk-recorded AAD fields (`HeaderByte`, `KeyEpoch`,
/// `BlockType`, `SuiteID`, `CompressionType`, `DictID`, `WindowLog`)
/// are read back from the `MetadataPayload` regardless of what the
/// caller supplies on `identity.block_type` / `identity.dict_id` /
/// `identity.window_log` — those three fields are IGNORED on the
/// read path because the disk is the source of truth for them.
///
/// # Errors
///
/// See [`DecryptError`] for the failure-mode taxonomy.
pub fn decrypt_block(
    bytes: &[u8],
    identity: &BlockIdentity,
    key_chain: &dyn KeyChain,
) -> Result<Vec<u8>, DecryptError> {
    let mut cursor = Cursor::new(bytes);

    // ── MetadataFrame ──────────────────────────────────────────
    // Cap MetadataPayload at exactly 38 bytes upfront — v1 suites
    // are fixed-size. A wire `PayloadLen` other than 38 is
    // malformed by spec and gets rejected before allocation.
    let metadata_payload = read_framed_payload_with_cap(
        &mut cursor,
        METADATA_VARIANT,
        METADATA_PAYLOAD_LEN_V1,
        DecryptError::MalformedMetadataFrame,
    )?;
    let parsed = decode_metadata_payload(&metadata_payload)?;

    // ── BodyFrame ──────────────────────────────────────────────
    // Cap BodyPayload at the 256 MiB encode-side maximum BEFORE
    // allocating, so a crafted PayloadLen near u32::MAX cannot
    // trigger a 4 GiB allocation attempt.
    //
    // Empty body payloads are LEGAL per RFC 8878 (skippable frames
    // can carry zero bytes) AND per AEAD semantics (encrypting an
    // empty plaintext yields a 0-byte ciphertext + 16-byte tag in
    // the MetadataFrame). encrypt_block on `&[]` produces a valid
    // such frame; the decoder must accept it.
    let mut ciphertext = read_framed_payload_with_cap(
        &mut cursor,
        BODY_VARIANT,
        MAX_BODY_LEN,
        DecryptError::MalformedBodyFrame,
    )?;

    // ── AAD reconstruction ──────────────────────────────────────
    // Reconstruct the BlockIdentity that participates in AAD using
    // the on-disk-mirrored fields from the MetadataPayload (dict_id,
    // window_log, block_type) plus the caller-supplied identity
    // (tree_id, table_id, block_offset). Block-type byte goes
    // through TryFrom so an unknown discriminator surfaces as
    // MalformedMetadataFrame rather than being silently coerced.
    let block_type = crate::table::block::BlockType::try_from(parsed.block_type_byte)
        .map_err(|_| DecryptError::MalformedMetadataFrame("unknown BlockType byte"))?;
    let aad_identity = BlockIdentity {
        tree_id: identity.tree_id,
        table_id: identity.table_id,
        block_offset: identity.block_offset,
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
    Ok(ciphertext)
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
            tree_id: 0xAABB_CCDD_EEFF_0011,
            table_id: 0x1234_5678_9ABC_DEF0,
            block_offset: 0x0000_1000,
            block_type: BlockType::Data,
            dict_id: 0,
            window_log: 0,
        }
    }

    fn ctx() -> EncryptionContext {
        EncryptionContext::v1(0, SuiteId::Aes256Gcm, 0)
    }

    fn chain() -> StaticKeyChain {
        StaticKeyChain::new().with_key(0, TEST_KEY)
    }

    #[test]
    fn roundtrip_aes_recovers_plaintext() {
        let plaintext = b"the quick brown fox";
        let sealed = encrypt_block(plaintext, &id(), &ctx(), &chain()).unwrap();
        let recovered = decrypt_block(&sealed, &id(), &chain()).unwrap();
        assert_eq!(&recovered[..], plaintext);
    }

    #[test]
    fn roundtrip_chacha_recovers_plaintext() {
        let plaintext = b"the quick brown fox";
        let chacha_ctx = EncryptionContext::v1(0, SuiteId::ChaCha20Poly1305, 0);
        let sealed = encrypt_block(plaintext, &id(), &chacha_ctx, &chain()).unwrap();
        let recovered = decrypt_block(&sealed, &id(), &chain()).unwrap();
        assert_eq!(&recovered[..], plaintext);
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
        // attempts to decrypt with a DIFFERENT BlockIdentity (one
        // field changed). AAD includes tree_id / table_id /
        // block_offset, so any mismatch surfaces as AEAD failure.
        let plaintext = b"the quick brown fox";
        let sealed = encrypt_block(plaintext, &id(), &ctx(), &chain()).unwrap();
        let mut wrong_id = id();
        wrong_id.block_offset = 0x0000_2000; // shifted by 4 KiB
        let err = decrypt_block(&sealed, &wrong_id, &chain()).unwrap_err();
        assert!(matches!(err, DecryptError::AeadVerificationFailed));
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
    fn roundtrip_empty_plaintext_recovers_empty() {
        // RFC 8878 skippable frames allow zero-length payloads,
        // and AEAD over empty plaintext yields a 0-byte ciphertext
        // + the 16-byte tag (which lives in MetadataPayload, not
        // BodyFrame). The decoder must accept the resulting
        // BodyFrame with PayloadLen=0.
        let sealed = encrypt_block(&[], &id(), &ctx(), &chain()).unwrap();
        let recovered = decrypt_block(&sealed, &id(), &chain()).unwrap();
        assert!(recovered.is_empty());
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
        // MetadataFrame total size = 8 + 38 = 46 bytes. BodyFrame
        // starts at offset 46; its PayloadLen is at frame offset 4
        // → absolute offset 50..54.
        let body_payload_len_at = 46 + 4;
        sealed[body_payload_len_at..body_payload_len_at + 4]
            .copy_from_slice(&u32::MAX.to_le_bytes());
        let err = decrypt_block(&sealed, &id(), &chain()).unwrap_err();
        assert!(
            matches!(err, DecryptError::MalformedBodyFrame(_)),
            "expected MalformedBodyFrame for oversized BodyFrame PayloadLen, got {err:?}",
        );
    }
}

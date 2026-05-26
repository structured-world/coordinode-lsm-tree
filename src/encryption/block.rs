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

use super::aad::{
    AAD_LEN, BlockIdentity, EncryptionContext, FORMAT_VERSION_V1, HEADER_BYTE_V1,
    MAGIC_METADATA_LE, SuiteId, build,
};
use super::aead::{TAG_LEN, decrypt_in_place, encrypt_in_place};
use super::error::DecryptError;
use super::key_chain::KeyChain;

/// `MetadataFrame` magic: `0x184D2A50` LE bytes. Variant 0 of the
/// Zstandard skippable-frame range.
const METADATA_VARIANT: u8 = 0;
/// `BodyFrame` magic: `0x184D2A51` LE bytes. Variant 1.
const BODY_VARIANT: u8 = 1;

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
/// `identity` MUST match the values the writer fed into
/// [`encrypt_block`] (tree id, table id, block offset, block
/// type, dict id, window log); any mismatch propagates through
/// the AAD and surfaces as
/// [`DecryptError::AeadVerificationFailed`]. The four fields the
/// writer recorded on disk (header byte, key epoch, suite id,
/// compression type) are read back from the `MetadataPayload`, NOT
/// taken from caller-supplied identity.
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
    let metadata_frame = SkippableFrame::decode_from(&mut cursor)
        .map_err(|_| DecryptError::MalformedMetadataFrame("MetadataFrame decode failed"))?;
    if metadata_frame.magic_variant() != METADATA_VARIANT {
        return Err(DecryptError::MalformedMetadataFrame(
            "first frame must be MetadataFrame variant 0",
        ));
    }
    let parsed = decode_metadata_payload(metadata_frame.payload())?;

    // ── BodyFrame ──────────────────────────────────────────────
    let body_frame = SkippableFrame::decode_from(&mut cursor)
        .map_err(|_| DecryptError::MalformedBodyFrame("BodyFrame decode failed"))?;
    if body_frame.magic_variant() != BODY_VARIANT {
        return Err(DecryptError::MalformedBodyFrame(
            "second frame must be BodyFrame variant 1",
        ));
    }
    let mut ciphertext = body_frame.into_payload();
    if ciphertext.is_empty() {
        return Err(DecryptError::MalformedBodyFrame("zero-length body payload"));
    }
    // BodyFrame's u32 PayloadLen field is already bounded by
    // SkippableFrame::decode_from to representable usize; cap
    // here too at the encode-side maximum so an attacker can't
    // hand us a 4 GiB legitimately-framed but undecryptable body.
    if ciphertext.len() > MAX_BODY_LEN as usize {
        return Err(DecryptError::MalformedBodyFrame(
            "body payload exceeds 256 MiB",
        ));
    }

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
    let _ = MAGIC_METADATA_LE; // doc anchor — referenced in aad::build internals.
    let _ = HEADER_BYTE_V1; // doc anchor for the format-version constant.

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
}

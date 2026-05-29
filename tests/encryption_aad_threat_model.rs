// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! AAD-bound encryption threat-model regression suite.
//!
//! Each scenario in this file exercises one documented attack against
//! the AAD-bound block format and asserts that the call surfaces a
//! TYPED [`DecryptError`] variant (NOT a panic, NOT a silent
//! wrong-plaintext, NOT a generic IO error). The specific variant
//! depends on which gate catches the tamper:
//!
//! - [`DecryptError::AeadVerificationFailed`] for scenarios that
//!   reach AEAD tag verification (block swap, cross-table swap,
//!   cross-tree swap, BlockType / CompressionType / supported-suite
//!   relabel, ciphertext / nonce bit-flip).
//! - [`DecryptError::MalformedMetadataFrame`] for tampers that
//!   violate the codec-consistency gate before AAD is rebuilt
//!   (WindowLog!=0 with non-zstd CompressionType, DictID!=0 with
//!   non-ZstdDict CompressionType).
//! - [`DecryptError::UnsupportedSuite`] for unregistered suite
//!   byte (caught at the typed-byte gate).
//! - [`DecryptError::UnsupportedFormatVersion`] for HeaderByte
//!   high-nibble tamper.
//! - [`DecryptError::UnknownKeyEpoch`] / `AeadVerificationFailed`
//!   for KeyEpoch tamper (depends on whether the swapped epoch
//!   happens to be in the chain).
//!
//! The contract pinned across all scenarios: NO silent
//! wrong-plaintext.
//!
//! Reference: AAD-bound block wire format spec (locked layout, 38-byte
//! AAD) — see `docs/aad-block-format.md` §5.3.
//!
//! First-wave coverage:
//! 1. Block swap (same key, different block_offset)
//! 2. Cross-table swap (same key, different table_id)
//! 3. Cross-tree swap (same key, different tree_id)
//! 4. Per-AAD-field modification (key_epoch, suite, header byte,
//!    compression_type, block_type, dict_id, window_log) — exhaustive
//!    over every field the AAD pulls from EncryptionContext + BlockIdentity
//! 5. Tampered ciphertext (single bit-flip in BodyFrame payload)
//!
//! Out of first-wave (require structured-zstd integration):
//! - Dict substitution (needs ZstdDict frame inner-id check)
//! - Decompression-bomb window swap (needs zstd window-log inner check)
//! - Suite downgrade beyond AAD coverage (needs key-chain suite metadata)
//! - Codec / decompression library version drift (needs zstd content checksum)

#![cfg(all(feature = "encryption", feature = "zstd"))]

use lsm_tree::encryption::{
    DecryptError, StaticKeyChain,
    aad::{BlockIdentity, BlockType, EncryptionContext, SuiteId},
    decrypt_block, encrypt_block,
};

const KEY_EPOCH: u8 = 1;
const KEY_BYTES: [u8; 32] = [0x42; 32];

fn key_chain() -> StaticKeyChain {
    StaticKeyChain::new().with_key(KEY_EPOCH, KEY_BYTES)
}

fn ctx() -> EncryptionContext {
    ctx_with_suite(SuiteId::Aes256Gcm)
}

fn ctx_with_suite(suite: SuiteId) -> EncryptionContext {
    EncryptionContext::v1(KEY_EPOCH, suite, 0)
}

fn identity(tree_id: u64, table_id: u64, block_offset: u64) -> BlockIdentity {
    BlockIdentity {
        tree_id,
        table_id,
        block_offset,
        block_type: BlockType::Data,
        dict_id: 0,
        window_log: 0,
    }
}

const PLAINTEXT_A: &[u8] = b"block A payload bytes for AAD threat-model regression tests";
const PLAINTEXT_B: &[u8] = b"block B payload bytes for AAD threat-model regression tests";

#[test]
fn block_swap_at_same_offset_fails_aead_verify() {
    // Two blocks under the same (tree_id, table_id, key, suite), encrypted
    // at distinct block_offsets. Swap them on "disk": present block B's
    // bytes when the caller's identity says offset = A's offset. AAD
    // construction at decrypt mixes the caller-supplied block_offset
    // into the tag input, so verify must fail.
    let chain = key_chain();
    let id_a = identity(7, 99, 0x1000);
    let id_b = identity(7, 99, 0x2000);

    let bytes_a = encrypt_block(PLAINTEXT_A, &id_a, &ctx(), &chain).expect("encrypt A");
    let bytes_b = encrypt_block(PLAINTEXT_B, &id_b, &ctx(), &chain).expect("encrypt B");

    // Sanity: each round-trips under its OWN identity.
    assert_eq!(
        decrypt_block(&bytes_a, &id_a, &chain)
            .expect("rt A")
            .plaintext,
        PLAINTEXT_A
    );
    assert_eq!(
        decrypt_block(&bytes_b, &id_b, &chain)
            .expect("rt B")
            .plaintext,
        PLAINTEXT_B
    );

    // The attack: substitute B's on-disk bytes at A's offset.
    let err = decrypt_block(&bytes_b, &id_a, &chain).expect_err("must fail");
    assert!(
        matches!(err, DecryptError::AeadVerificationFailed),
        "expected AeadVerificationFailed, got {err:?}"
    );
}

#[test]
fn cross_table_swap_fails_aead_verify() {
    // Block sealed for table_id=66 — same tree, same key, same offset —
    // attempt decrypt with table_id=99 identity. AAD includes table_id,
    // verify must fail.
    let chain = key_chain();
    let id_sealed = identity(7, 66, 0x1000);
    let id_attempt = identity(7, 99, 0x1000);

    let bytes = encrypt_block(PLAINTEXT_A, &id_sealed, &ctx(), &chain).expect("encrypt");
    let err = decrypt_block(&bytes, &id_attempt, &chain).expect_err("must fail");
    assert!(
        matches!(err, DecryptError::AeadVerificationFailed),
        "expected AeadVerificationFailed, got {err:?}"
    );
}

#[test]
fn cross_tree_swap_fails_aead_verify() {
    // Two trees sharing a key chain — collision risk if tree_id were
    // omitted from AAD. AAD covers tree_id, so a block sealed for
    // tree_id=10 must not decrypt under tree_id=20 even when every
    // other field matches.
    let chain = key_chain();
    let id_sealed = identity(10, 99, 0x1000);
    let id_attempt = identity(20, 99, 0x1000);

    let bytes = encrypt_block(PLAINTEXT_A, &id_sealed, &ctx(), &chain).expect("encrypt");
    let err = decrypt_block(&bytes, &id_attempt, &chain).expect_err("must fail");
    assert!(
        matches!(err, DecryptError::AeadVerificationFailed),
        "expected AeadVerificationFailed, got {err:?}"
    );
}

// MetadataPayload byte offsets (absolute, including the 8-byte SFA
// header). Mirrors `docs/aad-block-format.md` §5.1.
const META_HEADER_BYTE: usize = 8;
const META_KEY_EPOCH: usize = 9;
const META_BLOCK_TYPE: usize = 10;
const META_SUITE_ID: usize = 11;
const META_COMPRESSION_TYPE: usize = 12;
const META_DICT_ID_START: usize = 13; // 4 bytes BE
const META_WINDOW_LOG: usize = 17;
const META_NONCE_START: usize = 18; // 12 bytes
// MetadataFrame total = 8 (sfa header) + 38 (payload) = 46.
// BodyFrame at absolute offset 46: 8-byte sfa header + body bytes.
const BODY_FIRST_PAYLOAD_BYTE: usize = 46 + 8;

#[test]
fn block_type_tamper_on_disk_fails_aead_verify() {
    // BlockType is AAD-mirrored on disk at MetadataPayload offset 2.
    // The decoder pulls this byte FROM the on-disk metadata (not from
    // caller identity), so tampering must surface at AAD verify.
    // Flip Data (0) → Index (1).
    let chain = key_chain();
    let id = identity(7, 99, 0x1000);
    let mut bytes = encrypt_block(PLAINTEXT_A, &id, &ctx(), &chain).expect("encrypt");
    bytes[META_BLOCK_TYPE] = 1; // Index discriminator

    let err = decrypt_block(&bytes, &id, &chain).expect_err("must fail");
    assert!(
        matches!(err, DecryptError::AeadVerificationFailed),
        "expected AeadVerificationFailed, got {err:?}"
    );
}

#[test]
fn dict_id_tamper_on_disk_fails_aead_verify() {
    // DictID is AAD-mirrored at MetadataPayload offset 5 (u32 BE).
    // Flipping any of the 4 bytes must surface at AAD verify because
    // the decoder rebuilds AAD from these on-disk bytes.
    let chain = key_chain();
    let id = identity(7, 99, 0x1000);
    let mut bytes = encrypt_block(PLAINTEXT_A, &id, &ctx(), &chain).expect("encrypt");
    bytes[META_DICT_ID_START + 3] ^= 0x01; // flip low bit of u32 BE

    let err = decrypt_block(&bytes, &id, &chain).expect_err("must fail");
    // Accept either AAD verify failure OR the metadata-consistency
    // gate that fires when the tampered dict_id implies a codec
    // context the on-disk CompressionType doesn't agree with: both
    // are typed errors, neither leaks plaintext.
    assert!(
        matches!(
            err,
            DecryptError::AeadVerificationFailed | DecryptError::MalformedMetadataFrame(_)
        ),
        "expected AeadVerificationFailed or MalformedMetadataFrame, got {err:?}"
    );
}

#[test]
fn window_log_tamper_on_disk_fails() {
    // WindowLog is AAD-mirrored at MetadataPayload offset 9. Closes
    // the decompression-bomb vector: substituting a larger
    // WindowLog onto a smaller-window block must fail AAD verify
    // BEFORE the decompressor allocates the window buffer.
    let chain = key_chain();
    let id = identity(7, 99, 0x1000);
    let mut bytes = encrypt_block(PLAINTEXT_A, &id, &ctx(), &chain).expect("encrypt");
    // The original block was sealed with CompressionType=None and
    // WindowLog=0. Setting WindowLog to a non-zero value on the
    // sealed bytes makes the on-disk MetadataPayload internally
    // inconsistent (WindowLog!=0 requires zstd codec). The decoder
    // catches this at the metadata-consistency gate BEFORE
    // rebuilding AAD, surfacing MalformedMetadataFrame — which is
    // the early-rejection branch of the accept-list below. A future
    // companion test that seals with zstd would exercise the
    // AAD-verify branch; this test pins the rejection contract
    // either way: no silent acceptance of a mismatched WindowLog.
    bytes[META_WINDOW_LOG] = 10;

    let err = decrypt_block(&bytes, &id, &chain).expect_err("must fail");
    // Same accept-list as dict_id: AAD verify OR the codec-consistency
    // gate that catches WindowLog!=0 with CompressionType=None on the
    // read path.
    assert!(
        matches!(
            err,
            DecryptError::AeadVerificationFailed | DecryptError::MalformedMetadataFrame(_)
        ),
        "expected AeadVerificationFailed or MalformedMetadataFrame, got {err:?}"
    );
}

#[test]
fn compression_type_tamper_on_disk_fails_aead_verify() {
    // CompressionType is AAD-mirrored at MetadataPayload offset 4.
    // Defends per-block codec rotation: an attacker cannot swap an
    // None-compressed block's tag byte to claim Lz4 / Zstd.
    let chain = key_chain();
    let id = identity(7, 99, 0x1000);
    let mut bytes = encrypt_block(PLAINTEXT_A, &id, &ctx(), &chain).expect("encrypt");
    bytes[META_COMPRESSION_TYPE] = 1; // claim Lz4 (was None=0)

    let err = decrypt_block(&bytes, &id, &chain).expect_err("must fail");
    assert!(
        matches!(err, DecryptError::AeadVerificationFailed),
        "expected AeadVerificationFailed, got {err:?}"
    );
}

#[test]
fn suite_id_supported_relabel_fails_aead_verify() {
    // SuiteID is AAD-mirrored at MetadataPayload offset 3. Swap
    // AES-256-GCM (0x02) -> ChaCha20-Poly1305 (0x03) — both
    // SUPPORTED suites, so decode_metadata_payload accepts the byte
    // and the decrypt path proceeds to rebuild AAD and verify the
    // tag. The AAD now disagrees on byte 7 (suite_id mirror), so
    // AEAD verify must surface AeadVerificationFailed. This pins
    // the contract that SuiteID is part of AAD — a regression that
    // dropped it from AAD construction would let an attacker swap
    // a block's declared suite freely between supported suites.
    let chain = key_chain();
    let id = identity(7, 99, 0x1000);
    let mut bytes = encrypt_block(PLAINTEXT_A, &id, &ctx(), &chain).expect("encrypt");
    // 0x02 ^ 0x01 = 0x03 (supported suite).
    bytes[META_SUITE_ID] ^= 0x01;

    let err = decrypt_block(&bytes, &id, &chain).expect_err("must fail");
    assert!(
        matches!(err, DecryptError::AeadVerificationFailed),
        "expected AeadVerificationFailed, got {err:?}"
    );
}

#[test]
fn suite_id_unsupported_rejected_before_aead() {
    // Companion to the supported-relabel case: flipping SuiteID to
    // an unregistered value must fail at metadata-payload decode
    // BEFORE the AEAD primitive runs. Documents the second valid
    // failure mode (early rejection) so the suite-relabel coverage
    // is complete: supported relabels go through AEAD verify,
    // unsupported ones are caught at the typed-byte gate.
    let chain = key_chain();
    let id = identity(7, 99, 0x1000);
    let mut bytes = encrypt_block(PLAINTEXT_A, &id, &ctx(), &chain).expect("encrypt");
    // 0x02 ^ 0x0F = 0x0D (unregistered suite byte).
    bytes[META_SUITE_ID] ^= 0x0F;

    let err = decrypt_block(&bytes, &id, &chain).expect_err("must fail");
    // Pin the exact typed variants the early-suite-byte gate is allowed
    // to return: UnsupportedSuite (current impl) or MalformedMetadataFrame
    // (acceptable alternative if the decoder rolls the unknown-suite case
    // into the generic malformed-payload variant). Broadening this to
    // "any non-AeadVerificationFailed error" would silently accept
    // unrelated regressions (UnknownKeyEpoch, MalformedBodyFrame, etc.).
    assert!(
        matches!(
            err,
            DecryptError::UnsupportedSuite { .. } | DecryptError::MalformedMetadataFrame(_)
        ),
        "expected UnsupportedSuite or MalformedMetadataFrame, got {err:?}"
    );
}

#[test]
fn key_epoch_downgrade_to_unknown_epoch_fails() {
    // The key chain has epoch=1 only. A block sealed at epoch=1 with
    // its on-disk MetadataPayload re-stamped to epoch=2 routes the
    // decrypt path through a key lookup that misses; the error
    // variant differs from AeadVerificationFailed because the failure
    // surfaces BEFORE the AEAD primitive runs. The block-decrypt API
    // routes a missing-epoch lookup through a typed variant
    // (KeyEpochNotInChain or similar). The test asserts the call
    // fails — the exact variant is documented by the impl, not pinned
    // here, so future variant renames don't fail the test for the
    // wrong reason.
    let chain = key_chain();
    let id = identity(7, 99, 0x1000);

    let mut bytes = encrypt_block(PLAINTEXT_A, &id, &ctx(), &chain).expect("encrypt");

    // Flip the KeyEpoch byte inside the MetadataFrame payload.
    bytes[META_KEY_EPOCH] = bytes[META_KEY_EPOCH].wrapping_add(1);

    let err = decrypt_block(&bytes, &id, &chain).expect_err("must fail");
    // Pin the exact expected variant: the local chain holds only
    // epoch=1, the tamper deterministically rewrites the on-disk
    // KeyEpoch to 2, so the lookup MUST miss and surface
    // UnknownKeyEpoch with the tampered byte. Accepting any
    // DecryptError would silently pass a regression where the
    // tamper started failing at an unrelated gate (AEAD verify,
    // MalformedMetadataFrame, etc.).
    assert!(
        matches!(err, DecryptError::UnknownKeyEpoch { key_epoch: 2 }),
        "expected UnknownKeyEpoch {{ key_epoch: 2 }}, got {err:?}"
    );
}

#[test]
fn header_byte_tamper_fails() {
    // HeaderByte is AAD-mirrored at MetadataFrame payload offset 0.
    // Flipping it should fail verify (or, if the high nibble becomes
    // an unsupported format version, surface UnsupportedFormatVersion
    // BEFORE AEAD). Either typed error is acceptable — the point is
    // no silent wrong-plaintext.
    let chain = key_chain();
    let id = identity(7, 99, 0x1000);
    let mut bytes = encrypt_block(PLAINTEXT_A, &id, &ctx(), &chain).expect("encrypt");

    // MetadataPayload offset 0 = HeaderByte.
    bytes[META_HEADER_BYTE] = bytes[META_HEADER_BYTE].wrapping_add(0x10); // bump high nibble

    let err = decrypt_block(&bytes, &id, &chain).expect_err("must fail");
    assert!(
        matches!(
            err,
            DecryptError::AeadVerificationFailed | DecryptError::UnsupportedFormatVersion { .. }
        ),
        "expected AeadVerificationFailed or UnsupportedFormatVersion, got {err:?}"
    );
}

#[test]
fn ciphertext_bit_flip_fails_aead_verify() {
    // Flip a single bit inside the BodyFrame ciphertext bytes. AEAD
    // tag verification must catch it. This is the canonical AEAD
    // integrity property; the test pins behaviour against future
    // refactors of the body framing.
    let chain = key_chain();
    let id = identity(7, 99, 0x1000);
    let mut bytes = encrypt_block(PLAINTEXT_A, &id, &ctx(), &chain).expect("encrypt");

    // BodyFrame payload starts at offset 54 (pure ciphertext — nonce
    // and tag both live inside MetadataPayload). Pick a byte deep
    // inside the ciphertext region.
    let target_byte = BODY_FIRST_PAYLOAD_BYTE + (PLAINTEXT_A.len() / 2);
    assert!(
        target_byte < bytes.len(),
        "test sanity: payload longer than target"
    );
    bytes[target_byte] ^= 0x01;

    let err = decrypt_block(&bytes, &id, &chain).expect_err("must fail");
    assert!(
        matches!(err, DecryptError::AeadVerificationFailed),
        "expected AeadVerificationFailed, got {err:?}"
    );
}

#[test]
fn nonce_bit_flip_fails_aead_verify() {
    // The 12-byte nonce lives inside MetadataPayload at offsets
    // 18-29 (see the MetadataPayload layout: HeaderByte ... DictID
    // ... WindowLog ... Nonce ... Tag — the BodyFrame payload is
    // pure ciphertext, no nonce inside it). Flipping a single bit
    // re-derives the AEAD keystream and the tag mismatches. Pinned
    // because nonce tampering is the easiest attack to misclassify
    // as "garbled plaintext" if the verify step were ever weakened.
    let chain = key_chain();
    let id = identity(7, 99, 0x1000);
    let mut bytes = encrypt_block(PLAINTEXT_A, &id, &ctx(), &chain).expect("encrypt");

    // Nonce lives inside MetadataPayload at offsets 18-29 (NOT in
    // BodyFrame — see the MetadataPayload layout above). Flip the
    // top bit of the first nonce byte.
    bytes[META_NONCE_START] ^= 0x80;

    let err = decrypt_block(&bytes, &id, &chain).expect_err("must fail");
    assert!(
        matches!(err, DecryptError::AeadVerificationFailed),
        "expected AeadVerificationFailed, got {err:?}"
    );
}

#[test]
fn round_trip_under_correct_identity_succeeds() {
    // Sanity baseline: with EVERYTHING matching, the round-trip is
    // bit-exact. Guards against the trap of all-tests-pass-because-
    // everything-fails.
    let chain = key_chain();
    let id = identity(7, 99, 0x1000);
    let bytes = encrypt_block(PLAINTEXT_A, &id, &ctx(), &chain).expect("encrypt");
    let decrypted = decrypt_block(&bytes, &id, &chain).expect("decrypt");
    assert_eq!(decrypted.plaintext, PLAINTEXT_A);
}

// ============================================================================
// ChaCha20-Poly1305 cross-suite coverage.
//
// The AES-256-GCM matrix above covers the full AAD-bound threat model.
// The ChaCha20-Poly1305 implementation walks the same AAD construction
// path but uses a different AEAD primitive; a regression in the
// ChaCha20-only branch (e.g., AAD field dropped from the chacha
// keystream, nonce miswire on the chacha side) would slip past the
// AES-only matrix. These tests pin the most consequential scenarios
// under the other supported suite so a per-suite regression surfaces.
// Not exhaustive (38 lines × 2 = noise); the AES matrix is the canonical
// scenario set.
// ============================================================================

#[test]
fn chacha_round_trip_succeeds() {
    // Baseline guard: encrypt + decrypt under ChaCha20-Poly1305 round-trips
    // bit-exact. Any breakage of the per-suite encrypt / decrypt wiring
    // surfaces here first.
    let chain = key_chain();
    let id = identity(7, 99, 0x1000);
    let bytes = encrypt_block(
        PLAINTEXT_A,
        &id,
        &ctx_with_suite(SuiteId::ChaCha20Poly1305),
        &chain,
    )
    .expect("encrypt");
    let decrypted = decrypt_block(&bytes, &id, &chain).expect("decrypt");
    assert_eq!(decrypted.plaintext, PLAINTEXT_A);
}

#[test]
fn chacha_block_swap_at_same_offset_fails_aead_verify() {
    // Most consequential AAD attack under ChaCha: substitute block B's
    // ChaCha-sealed bytes at A's offset. AEAD verify must catch the
    // block_offset mismatch on the chacha path the same way it does
    // on the AES path.
    let chain = key_chain();
    let id_a = identity(7, 99, 0x1000);
    let id_b = identity(7, 99, 0x2000);

    let bytes_b = encrypt_block(
        PLAINTEXT_B,
        &id_b,
        &ctx_with_suite(SuiteId::ChaCha20Poly1305),
        &chain,
    )
    .expect("encrypt B");

    let err = decrypt_block(&bytes_b, &id_a, &chain).expect_err("must fail");
    assert!(
        matches!(err, DecryptError::AeadVerificationFailed),
        "expected AeadVerificationFailed, got {err:?}"
    );
}

#[test]
fn chacha_ciphertext_bit_flip_fails_aead_verify() {
    // ChaCha20-Poly1305's keystream + Poly1305 tag must catch ciphertext
    // bit-flips with the same guarantee as AES-GCM. A regression that
    // weakened only the chacha verify step would slip past the AES
    // matrix.
    let chain = key_chain();
    let id = identity(7, 99, 0x1000);
    let mut bytes = encrypt_block(
        PLAINTEXT_A,
        &id,
        &ctx_with_suite(SuiteId::ChaCha20Poly1305),
        &chain,
    )
    .expect("encrypt");

    let target_byte = BODY_FIRST_PAYLOAD_BYTE + (PLAINTEXT_A.len() / 2);
    assert!(target_byte < bytes.len(), "test sanity");
    bytes[target_byte] ^= 0x01;

    let err = decrypt_block(&bytes, &id, &chain).expect_err("must fail");
    assert!(
        matches!(err, DecryptError::AeadVerificationFailed),
        "expected AeadVerificationFailed, got {err:?}"
    );
}

#[test]
fn chacha_cross_tree_swap_fails_aead_verify() {
    // AAD-bound tree_id must reject cross-tree substitution on the
    // chacha path. Companion to the AES cross_tree_swap test.
    let chain = key_chain();
    let id_sealed = identity(10, 99, 0x1000);
    let id_attempt = identity(20, 99, 0x1000);
    let bytes = encrypt_block(
        PLAINTEXT_A,
        &id_sealed,
        &ctx_with_suite(SuiteId::ChaCha20Poly1305),
        &chain,
    )
    .expect("encrypt");

    let err = decrypt_block(&bytes, &id_attempt, &chain).expect_err("must fail");
    assert!(
        matches!(err, DecryptError::AeadVerificationFailed),
        "expected AeadVerificationFailed, got {err:?}"
    );
}

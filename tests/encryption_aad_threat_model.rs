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
//!   reach AEAD tag verification (cross-table swap, BlockType /
//!   CompressionType / supported-suite relabel, ciphertext / nonce
//!   bit-flip).
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
//! Reference: AAD-bound block wire format spec (locked layout, 23-byte
//! AAD) — see `docs/aad-block-format.md` §5.3.
//!
//! First-wave coverage:
//! 1. Block position and owning tree id are intentionally NOT bound in
//!    AAD (offset-independent so encryption parallelises; tree id is
//!    process-ephemeral). Same-table and shared-key cross-tree swaps are
//!    therefore AAD-valid — caught one layer up by the authenticated
//!    index + structural layout, and (cross-tree) by per-tree key
//!    isolation, not by per-block AEAD.
//! 2. Cross-table swap (same key, different table_id)
//! 4. Per-AAD-field modification. Each field has at least one tamper
//!    test that pins TYPED rejection; for the fields whose obvious
//!    tamper hits an early gate (key_epoch -> UnknownKeyEpoch,
//!    window_log / dict_id -> codec-consistency, header_byte high
//!    nibble -> UnsupportedFormatVersion) there is an additional
//!    AAD-verify-forcing companion that constructs a VALID metadata
//!    config so the tamper propagates into AAD reconstruction and
//!    AEAD verify catches the field's actual AAD binding.
//! 5. Tampered ciphertext (single bit-flip in BodyFrame payload)
//! 6. ChaCha20-Poly1305 cross-suite coverage of the most consequential
//!    scenarios (round-trip, cross-table swap, ciphertext bit-flip).
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
    EncryptionContext::v1(KEY_EPOCH, suite, 0, 0)
}

fn identity(table_id: u64) -> BlockIdentity {
    BlockIdentity {
        table_id,
        block_type: BlockType::Data,
        dict_id: 0,
        window_log: 0,
    }
}

const PLAINTEXT_A: &[u8] = b"block A payload bytes for AAD threat-model regression tests";
const PLAINTEXT_B: &[u8] = b"block B payload bytes for AAD threat-model regression tests";

#[test]
fn same_table_block_swap_is_not_bound_in_aad() {
    // AAD intentionally does NOT bind block position: every block of the
    // same (tree_id, table_id, type, dict, window) shares identical AAD,
    // so encryption needs no on-disk offset and parallelises freely.
    // The consequence — pinned here as a regression guard — is that two
    // data blocks of the SAME table are interchangeable at the AEAD layer:
    // presenting B's sealed bytes under A's identity VERIFIES and returns
    // B's plaintext. Block-position integrity is provided one layer up by
    // the authenticated index (which maps key ranges to offsets) plus the
    // structural file layout, not by per-block AEAD. If this assertion ever
    // flips to a failure, someone re-introduced an offset (or other
    // position) field into AAD and broke parallel encryption.
    let chain = key_chain();
    let id = identity(99);

    let bytes_a = encrypt_block(PLAINTEXT_A, &id, &ctx(), &chain).expect("encrypt A");
    let bytes_b = encrypt_block(PLAINTEXT_B, &id, &ctx(), &chain).expect("encrypt B");

    // Each round-trips under the shared identity.
    assert_eq!(
        decrypt_block(&bytes_a, &id, &chain)
            .expect("rt A")
            .plaintext,
        PLAINTEXT_A
    );

    // Swap: B's bytes decrypt fine under the same identity (no position
    // binding) and yield B's plaintext.
    assert_eq!(
        decrypt_block(&bytes_b, &id, &chain)
            .expect("same-table swap is AAD-valid by design")
            .plaintext,
        PLAINTEXT_B
    );
}

#[test]
fn cross_table_swap_fails_aead_verify() {
    // Block sealed for table_id=66 — same tree, same key — attempt
    // decrypt with table_id=99 identity. AAD includes table_id,
    // verify must fail.
    let chain = key_chain();
    let id_sealed = identity(66);
    let id_attempt = identity(99);

    let bytes = encrypt_block(PLAINTEXT_A, &id_sealed, &ctx(), &chain).expect("encrypt");
    let err = decrypt_block(&bytes, &id_attempt, &chain).expect_err("must fail");
    assert!(
        matches!(err, DecryptError::AeadVerificationFailed),
        "expected AeadVerificationFailed, got {err:?}"
    );
}

#[test]
fn cross_tree_swap_is_not_bound_in_aad() {
    // The owning tree id is intentionally NOT part of AAD (it is a
    // process-ephemeral counter, not durable across reopen). So two
    // trees that share a key chain and a table id produce interchangeable
    // blocks at the AEAD layer — pinned here as a regression guard for
    // the offset/tree-independent AAD. Cross-tree substitution is instead
    // prevented by per-tree KEY ISOLATION: in production each tree owns a
    // distinct key, so a block sealed by tree A's key fails to decrypt
    // under tree B's key. That key-layer defence is exercised by
    // `wrong_key_in_chain_surfaces_aead_failure` (in the encryption unit
    // tests). With a deliberately shared key + table id here, the swap
    // VERIFIES — if this ever starts failing, someone re-introduced a
    // tree/position field into AAD.
    //
    // Two SEPARATE key-chain instances carrying the SAME key material model
    // the documented "different tree, same key" case (a misconfiguration
    // where an operator shares one key across trees): tree A seals, tree B
    // reads. Because the key bytes match and the AAD carries no tree id, the
    // block decrypts — demonstrating exactly why distinct per-tree keys (not
    // AAD) are the cross-tree defence.
    let chain_a = key_chain();
    let chain_b = key_chain();
    let id = identity(99);

    let bytes = encrypt_block(PLAINTEXT_A, &id, &ctx(), &chain_a).expect("encrypt");
    assert_eq!(
        decrypt_block(&bytes, &id, &chain_b)
            .expect("shared-key cross-tree swap is AAD-valid by design")
            .plaintext,
        PLAINTEXT_A
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
const META_BLOCK_FLAGS: usize = 18;
const META_NONCE_START: usize = 19; // 12 bytes
// MetadataFrame total = 8 (sfa header) + 39 (payload) = 47.
// BodyFrame at absolute offset 47: 8-byte sfa header + body bytes.
const BODY_FIRST_PAYLOAD_BYTE: usize = 47 + 8;

#[test]
fn block_type_tamper_on_disk_fails_aead_verify() {
    // BlockType is AAD-mirrored on disk at MetadataPayload offset 2.
    // The decoder pulls this byte FROM the on-disk metadata (not from
    // caller identity), so tampering must surface at AAD verify.
    // Flip Data (0) → Index (1).
    let chain = key_chain();
    let id = identity(99);
    let mut bytes = encrypt_block(PLAINTEXT_A, &id, &ctx(), &chain).expect("encrypt");
    bytes[META_BLOCK_TYPE] = 1; // Index discriminator

    let err = decrypt_block(&bytes, &id, &chain).expect_err("must fail");
    assert!(
        matches!(err, DecryptError::AeadVerificationFailed),
        "expected AeadVerificationFailed, got {err:?}"
    );
}

#[test]
fn dict_id_tamper_on_disk_fails() {
    // DictID is AAD-mirrored at MetadataPayload offset 5 (u32 BE).
    // In this fixture the sealed block uses CompressionType=None
    // (dict_id=0), so flipping any of the 4 bytes to a non-zero
    // value violates the codec-consistency invariant (dict_id!=0
    // requires CompressionType=ZstdDict) and the decoder rejects
    // at MalformedMetadataFrame BEFORE AAD verify runs. The
    // companion test dict_id_under_zstd_dict_tamper_fails_aead_verify
    // covers the AAD-verify branch by sealing with a valid
    // ZstdDict context. Test pinned to typed rejection either way.
    let chain = key_chain();
    let id = identity(99);
    let mut bytes = encrypt_block(PLAINTEXT_A, &id, &ctx(), &chain).expect("encrypt");
    bytes[META_DICT_ID_START + 3] ^= 0x01; // flip low bit of u32 BE

    let err = decrypt_block(&bytes, &id, &chain).expect_err("must fail");
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
    // WindowLog is AAD-mirrored at MetadataPayload offset 9 and
    // participates in the decompression-bomb defence. This fixture
    // seals with CompressionType=None, so the tamper here surfaces
    // a TYPED rejection at the metadata-consistency gate, not at
    // AEAD verify (see the inner comment for why).
    let chain = key_chain();
    let id = identity(99);
    let mut bytes = encrypt_block(PLAINTEXT_A, &id, &ctx(), &chain).expect("encrypt");
    // The original block was sealed with CompressionType=None and
    // WindowLog=0. Setting WindowLog to a non-zero value on the
    // sealed bytes makes the on-disk MetadataPayload internally
    // inconsistent (WindowLog!=0 requires a zstd codec). The decoder
    // catches this at the metadata-consistency gate BEFORE
    // rebuilding AAD, surfacing MalformedMetadataFrame — the
    // early-rejection branch of the accept-list below. The
    // AAD-verify branch (tamper on a block sealed with a valid zstd
    // context) is covered by the companion test
    // window_log_under_zstd_tamper_fails_aead_verify. This test pins
    // the rejection contract either way: no silent acceptance of a
    // mismatched WindowLog.
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
    let id = identity(99);
    let mut bytes = encrypt_block(PLAINTEXT_A, &id, &ctx(), &chain).expect("encrypt");
    bytes[META_COMPRESSION_TYPE] = 1; // claim Lz4 (was None=0)

    let err = decrypt_block(&bytes, &id, &chain).expect_err("must fail");
    assert!(
        matches!(err, DecryptError::AeadVerificationFailed),
        "expected AeadVerificationFailed, got {err:?}"
    );
}

#[test]
fn block_flags_tamper_on_disk_fails_aead_verify() {
    // BlockFlags is AAD-mirrored at MetadataPayload offset 10. Defends
    // the block's transform stack: an attacker cannot flip a transform
    // bit (e.g. clear the per-KV checksum footer bit, or relabel a block
    // as compressed/encrypted) under a forged non-cryptographic header
    // checksum — the AAD binds the whole byte so any flip fails AEAD.
    let chain = key_chain();
    let id = identity(99);
    let mut bytes = encrypt_block(PLAINTEXT_A, &id, &ctx(), &chain).expect("encrypt");
    bytes[META_BLOCK_FLAGS] ^= 0x01; // flip the KV_CHECKSUM_FOOTER bit (was 0)

    let err = decrypt_block(&bytes, &id, &chain).expect_err("must fail");
    assert!(
        matches!(err, DecryptError::AeadVerificationFailed),
        "expected AeadVerificationFailed, got {err:?}"
    );
}

#[test]
fn suite_id_supported_relabel_rejected() {
    // Suite-swap REJECTION coverage (NOT a SuiteID-in-AAD proof).
    //
    // Swap AES-256-GCM (0x02) -> ChaCha20-Poly1305 (0x03) on the
    // sealed bytes. Both are SUPPORTED suites so the metadata gate
    // accepts the byte and the decrypt path runs.
    //
    // This does NOT isolate SuiteID's AAD binding: the suite byte
    // ALSO selects the AEAD primitive at decrypt, so a ChaCha20
    // keystream is run over AES-GCM ciphertext and the Poly1305 tag
    // mismatches regardless of whether SuiteID is in aad::build.
    // The standalone proof that SuiteID is part of AAD lives in
    // suite_id_is_part_of_aad below (direct aad::build byte compare).
    // What this test pins is the end-to-end contract: a block whose
    // declared suite was swapped between two supported suites does
    // NOT decrypt.
    let chain = key_chain();
    let id = identity(99);
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
fn suite_id_is_part_of_aad() {
    // Standalone proof that SuiteID participates in AAD construction,
    // which the end-to-end suite_id_supported_relabel_rejected test
    // CANNOT isolate (the suite byte there also switches the AEAD
    // primitive, confounding the tag failure). Here we call the AAD
    // constructor directly with two contexts that differ ONLY in
    // suite_id and assert the produced AAD bytes differ. A
    // regression that dropped SuiteID from aad::build would make the
    // two buffers identical and fail this assertion.
    use lsm_tree::encryption::aad::{EncryptionContext, build};

    let id = identity(99);
    let aad_aes = build(
        &EncryptionContext::v1(KEY_EPOCH, SuiteId::Aes256Gcm, 0, 0),
        &id,
    );
    let aad_chacha = build(
        &EncryptionContext::v1(KEY_EPOCH, SuiteId::ChaCha20Poly1305, 0, 0),
        &id,
    );

    assert_ne!(
        aad_aes, aad_chacha,
        "AAD must differ when only suite_id changes — SuiteID dropped from aad::build?"
    );
    // The difference must be exactly the suite_id mirror byte (AAD
    // offset 7 per docs/aad-block-format.md §5.3), nothing else.
    for (i, (a, c)) in aad_aes.iter().zip(aad_chacha.iter()).enumerate() {
        if i == 7 {
            assert_ne!(a, c, "AAD offset 7 (suite_id) must differ");
        } else {
            assert_eq!(
                a, c,
                "AAD byte {i} must NOT change when only suite_id flips"
            );
        }
    }
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
    let id = identity(99);
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
    // surfaces BEFORE the AEAD primitive runs. The miss is pinned to
    // the exact typed variant `UnknownKeyEpoch { key_epoch: 2 }`
    // (the deterministic tamper rewrites the byte to 2), so a
    // regression that started failing at an unrelated gate (AEAD
    // verify, MalformedMetadataFrame) would be caught rather than
    // silently accepted.
    let chain = key_chain();
    let id = identity(99);

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
    let id = identity(99);
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
    let id = identity(99);
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
    let id = identity(99);
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
    let id = identity(99);
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
    let id = identity(99);
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
fn chacha_cross_table_swap_fails_aead_verify() {
    // Cross-table swap under ChaCha: a block sealed for table_id=66 must
    // not decrypt under table_id=99, since AAD binds table_id. Mirrors the
    // AES `cross_table_swap_fails_aead_verify` on the chacha path. (Same-
    // table position swap is intentionally AAD-valid; see
    // `same_table_block_swap_is_not_bound_in_aad`.)
    let chain = key_chain();
    let id_sealed = identity(66);
    let id_attempt = identity(99);

    let bytes = encrypt_block(
        PLAINTEXT_B,
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

#[test]
fn chacha_ciphertext_bit_flip_fails_aead_verify() {
    // ChaCha20-Poly1305's keystream + Poly1305 tag must catch ciphertext
    // bit-flips with the same guarantee as AES-GCM. A regression that
    // weakened only the chacha verify step would slip past the AES
    // matrix.
    let chain = key_chain();
    let id = identity(99);
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

// ============================================================================
// AAD-verify-forcing companions.
//
// Several tamper tests above land on early-rejection gates (UnknownKeyEpoch,
// MalformedMetadataFrame for codec-consistency, UnsupportedFormatVersion).
// Those gates catch tampers BEFORE AAD is rebuilt, which means a regression
// that drops the corresponding field from AAD construction would still pass
// the tests above. The tests below construct VALID metadata configurations
// that bypass the early gates and force the failure to reach AEAD verify,
// pinning each field's actual AAD binding.
// ============================================================================

const KEY_EPOCH_2: u8 = 2;
// MUST equal KEY_BYTES. If epoch 2 held different key material, the
// key_epoch_supported_relabel test below would fail for the WRONG
// reason — a regression that dropped KeyEpoch from AAD construction
// would still fail tag verification because the AEAD key differed.
// Same key on both epochs makes the AAD byte the ONLY input that
// changes between encrypt and decrypt, so a green test proves the
// KeyEpoch field really is AAD-bound.
const KEY_BYTES_2: [u8; 32] = KEY_BYTES;

#[test]
fn key_epoch_supported_relabel_fails_aead_verify() {
    // Chain holds BOTH epoch=1 and epoch=2 with the SAME key bytes
    // (see KEY_BYTES_2 = KEY_BYTES). Block is sealed under epoch=1;
    // tamper rewrites the on-disk KeyEpoch byte to 2. The key lookup
    // now SUCCEEDS (epoch=2 is in the chain), metadata decodes
    // cleanly, AAD is rebuilt with KeyEpoch=2, and AEAD verify
    // catches the mismatch with the original AAD's KeyEpoch=1.
    //
    // Same-key setup matters: if KeyEpoch were dropped from AAD,
    // the AEAD primitive would see identical key + identical AAD
    // and silently decrypt to the CORRECT original plaintext — a
    // false-OK that this test would catch as a missing expect_err.
    // Different-key setup would mask the regression because
    // wrong-key decrypt would fail the tag regardless of AAD
    // binding, so the test would still pass but for the wrong reason.
    let chain = StaticKeyChain::new()
        .with_key(KEY_EPOCH, KEY_BYTES)
        .with_key(KEY_EPOCH_2, KEY_BYTES_2);
    let id = identity(99);
    let mut bytes = encrypt_block(PLAINTEXT_A, &id, &ctx(), &chain).expect("encrypt");
    bytes[META_KEY_EPOCH] = KEY_EPOCH_2;

    let err = decrypt_block(&bytes, &id, &chain).expect_err("must fail");
    assert!(
        matches!(err, DecryptError::AeadVerificationFailed),
        "expected AeadVerificationFailed, got {err:?}"
    );
}

#[test]
fn header_byte_low_nibble_tamper_fails_aead_verify() {
    // HeaderByte = (FORMAT_VERSION_V1 << 4) | reserved_low_nibble.
    // Writers MUST zero the low nibble (spec §4.8); readers MUST
    // ignore it for forward compat. Flipping ONLY a low-nibble bit
    // keeps the high nibble = V1 so the format-version gate passes,
    // and the tampered byte propagates into AAD reconstruction —
    // AEAD verify catches the mismatch.
    let chain = key_chain();
    let id = identity(99);
    let mut bytes = encrypt_block(PLAINTEXT_A, &id, &ctx(), &chain).expect("encrypt");
    bytes[META_HEADER_BYTE] |= 0x01; // set lowest reserved bit

    let err = decrypt_block(&bytes, &id, &chain).expect_err("must fail");
    assert!(
        matches!(err, DecryptError::AeadVerificationFailed),
        "expected AeadVerificationFailed, got {err:?}"
    );
}

#[test]
fn window_log_under_zstd_tamper_fails_aead_verify() {
    // Seal with a VALID zstd codec context (compression_type=3,
    // window_log in [10, 31]). The on-disk metadata is internally
    // consistent, so a WindowLog tamper at decode time does NOT hit
    // the codec-consistency gate — it propagates into AAD
    // reconstruction and AEAD verify catches the mismatch. Pins the
    // AAD-binding of WindowLog beyond what the early-gate test
    // verifies.
    let chain = key_chain();
    let mut id = identity(99);
    id.window_log = 15;
    let zstd_ctx = EncryptionContext::v1(KEY_EPOCH, SuiteId::Aes256Gcm, 3, 0);
    let mut bytes = encrypt_block(PLAINTEXT_A, &id, &zstd_ctx, &chain).expect("encrypt");
    bytes[META_WINDOW_LOG] = 20; // valid for zstd, different from sealed

    let err = decrypt_block(&bytes, &id, &chain).expect_err("must fail");
    assert!(
        matches!(err, DecryptError::AeadVerificationFailed),
        "expected AeadVerificationFailed, got {err:?}"
    );
}

#[test]
fn dict_id_under_zstd_dict_tamper_fails_aead_verify() {
    // Seal with a VALID zstd-dict codec context (compression_type=4,
    // non-zero dict_id). On-disk metadata is internally consistent,
    // so a DictID tamper at decode time bypasses the codec-consistency
    // gate and lands on AAD verify. Pins the AAD-binding of DictID.
    let chain = key_chain();
    let mut id = identity(99);
    id.dict_id = 0x1234_5678;
    let zstd_dict_ctx = EncryptionContext::v1(KEY_EPOCH, SuiteId::Aes256Gcm, 4, 0);
    let mut bytes = encrypt_block(PLAINTEXT_A, &id, &zstd_dict_ctx, &chain).expect("encrypt");
    // Flip a single byte of the u32 BE dict_id — same codec ctx,
    // different dict identity.
    bytes[META_DICT_ID_START + 3] ^= 0x01;

    let err = decrypt_block(&bytes, &id, &chain).expect_err("must fail");
    assert!(
        matches!(err, DecryptError::AeadVerificationFailed),
        "expected AeadVerificationFailed, got {err:?}"
    );
}

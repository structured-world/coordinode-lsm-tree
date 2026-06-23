use super::*;
use crate::encryption::aad::{BlockType, EncryptionContext, build};
use crate::table::block::BlockIdentity as TableBlockIdentity;

/// 32-byte test key. Synthesised from a stable byte pattern so the
/// fixture is deterministic across runs but visibly fake (never
/// confused with real key material in a debugger).
const TEST_KEY: [u8; 32] = [0x42; 32];
/// Alt 32-byte key for "wrong key" assertions.
const TEST_KEY_OTHER: [u8; 32] = [0x55; 32];

/// Build a canonical AAD for a concrete test block.
fn test_aad(suite: SuiteId) -> [u8; AAD_LEN] {
    let ctx = EncryptionContext::v1(
        7, // key_epoch
        suite, 0, // CompressionType::None tag
        0, // block_flags: no transform layers
    );
    let identity = TableBlockIdentity::for_test(0xAB, BlockType::Data);
    build(&ctx, &identity)
}

// SAFETY: hard-coded test-only nonce. CodeQL flags this as a
// "hard-coded cryptographic value" because reusing a nonce with
// the same key in production AES-GCM / ChaCha20-Poly1305
// collapses both ciphertexts' confidentiality. That's true and
// important — but in unit tests we WANT a deterministic input
// so the assertion is reproducible across runs. Each test
// either uses the nonce against a single encrypt/decrypt pair
// (no key+nonce reuse) or pairs it with an intentionally
// tampered AAD / key / ciphertext to assert tag failure (where
// recovery of the plaintext is exactly the property we DON'T
// want). Production callers must NEVER reuse this pattern;
// [`crate::encryption::thread_local_rng`] / the AAD-bound wire
// encoder is the right path for live writes.
fn test_nonce() -> [u8; 12] {
    [
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C,
    ]
}

fn roundtrip_for(suite: SuiteId) {
    let plaintext = b"hello AAD-bound world";
    let aad = test_aad(suite);
    let nonce = test_nonce();

    let mut buf = plaintext.to_vec();
    let tag =
        encrypt_in_place(suite, &TEST_KEY, &nonce, &aad, &mut buf).expect("encrypt should succeed");
    // Ciphertext must NOT equal plaintext (otherwise the AEAD did
    // nothing).
    assert_ne!(&buf[..], &plaintext[..]);

    decrypt_in_place(suite, &TEST_KEY, &nonce, &aad, &tag, &mut buf)
        .expect("decrypt should succeed with matching key + aad");
    assert_eq!(&buf[..], &plaintext[..]);
}

#[test]
fn aes256gcm_roundtrip_recovers_plaintext() {
    roundtrip_for(SuiteId::Aes256Gcm);
}

#[test]
fn chacha20poly1305_roundtrip_recovers_plaintext() {
    roundtrip_for(SuiteId::ChaCha20Poly1305);
}

fn wrong_key_fails_for(suite: SuiteId) {
    let aad = test_aad(suite);
    let nonce = test_nonce();
    let mut buf = b"abc".to_vec();
    let tag = encrypt_in_place(suite, &TEST_KEY, &nonce, &aad, &mut buf).unwrap();

    // Decrypt with a different key. Must fail with the typed
    // AeadVerificationFailed variant (NOT a panic, NOT a partial
    // plaintext leak).
    let err = decrypt_in_place(suite, &TEST_KEY_OTHER, &nonce, &aad, &tag, &mut buf)
        .expect_err("wrong key must fail");
    assert!(matches!(err, DecryptError::AeadVerificationFailed));
}

#[test]
fn aes256gcm_wrong_key_fails_with_aead_verification_failed() {
    wrong_key_fails_for(SuiteId::Aes256Gcm);
}

#[test]
fn chacha20poly1305_wrong_key_fails_with_aead_verification_failed() {
    wrong_key_fails_for(SuiteId::ChaCha20Poly1305);
}

fn tampered_aad_fails_for(suite: SuiteId) {
    let aad = test_aad(suite);
    let nonce = test_nonce();
    let mut buf = b"abc".to_vec();
    let tag = encrypt_in_place(suite, &TEST_KEY, &nonce, &aad, &mut buf).unwrap();

    // Flip a single bit of the AAD before decrypt. Any AAD byte
    // change is supposed to fail the tag check; this is the core
    // contract of the AAD-bound format.
    let mut bad_aad = aad;
    bad_aad[5] ^= 0x01;
    let err = decrypt_in_place(suite, &TEST_KEY, &nonce, &bad_aad, &tag, &mut buf)
        .expect_err("AAD tamper must fail");
    assert!(matches!(err, DecryptError::AeadVerificationFailed));
}

#[test]
fn aes256gcm_tampered_aad_byte_fails_verification() {
    tampered_aad_fails_for(SuiteId::Aes256Gcm);
}

#[test]
fn chacha20poly1305_tampered_aad_byte_fails_verification() {
    tampered_aad_fails_for(SuiteId::ChaCha20Poly1305);
}

fn tampered_ciphertext_fails_for(suite: SuiteId) {
    let aad = test_aad(suite);
    let nonce = test_nonce();
    let mut buf = b"abcdef".to_vec();
    let tag = encrypt_in_place(suite, &TEST_KEY, &nonce, &aad, &mut buf).unwrap();

    // Flip a ciphertext bit; tag check must fail.
    buf[0] ^= 0x01;
    let err = decrypt_in_place(suite, &TEST_KEY, &nonce, &aad, &tag, &mut buf)
        .expect_err("ciphertext tamper must fail");
    assert!(matches!(err, DecryptError::AeadVerificationFailed));
}

#[test]
fn aes256gcm_tampered_ciphertext_fails_verification() {
    tampered_ciphertext_fails_for(SuiteId::Aes256Gcm);
}

#[test]
fn chacha20poly1305_tampered_ciphertext_fails_verification() {
    tampered_ciphertext_fails_for(SuiteId::ChaCha20Poly1305);
}

#[test]
fn wrong_nonce_length_returns_malformed_body_frame_error() {
    let aad = test_aad(SuiteId::Aes256Gcm);
    let mut buf = b"abc".to_vec();
    // 11 bytes instead of the suite's 12. CodeQL flags this as
    // a hard-coded nonce; that's intentional for this test: the
    // value is never used as a real cryptographic nonce because
    // `decrypt_in_place` short-circuits with `MalformedBodyFrame`
    // before reaching the AEAD primitive (length mismatch).
    let short_nonce = [0u8; 11];
    let bogus_tag = [0u8; TAG_LEN];

    let err = decrypt_in_place(
        SuiteId::Aes256Gcm,
        &TEST_KEY,
        &short_nonce,
        &aad,
        &bogus_tag,
        &mut buf,
    )
    .expect_err("wrong nonce length must fail");
    assert!(matches!(err, DecryptError::MalformedBodyFrame(_)));
}

/// Cross-suite mismatch: encrypt under AES-256-GCM, attempt to
/// decrypt under ChaCha20-Poly1305 with the same key/nonce/aad
/// fixture. AAD includes the `SuiteId` byte (offset 7), so even if
/// the caller-side dispatch were buggy and tried the wrong cipher,
/// the AAD byte itself would differ and the tag would fail. This
/// test confirms the AEAD layer's behaviour for that scenario
/// (must NOT silently succeed under a different suite).
#[test]
fn cross_suite_decrypt_fails_verification() {
    let aes_aad = test_aad(SuiteId::Aes256Gcm);
    let nonce = test_nonce();
    let mut buf = b"abc".to_vec();
    let tag = encrypt_in_place(SuiteId::Aes256Gcm, &TEST_KEY, &nonce, &aes_aad, &mut buf).unwrap();

    // Decrypt under ChaCha. Even with the same AAD bytes, the
    // primitive itself is different, so the tag is mathematically
    // wrong. (In production the caller resolves the suite from
    // the on-disk SuiteID byte, so this combination would never
    // arise outside the test harness.)
    let err = decrypt_in_place(
        SuiteId::ChaCha20Poly1305,
        &TEST_KEY,
        &nonce,
        &aes_aad,
        &tag,
        &mut buf,
    )
    .expect_err("cross-suite decrypt must fail");
    assert!(matches!(err, DecryptError::AeadVerificationFailed));
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! AEAD (Authenticated Encryption with Associated Data) primitives for
//! the AAD-bound block format.
//!
//! Sits on top of the [`super::aad`] module: takes a [`super::aad::SuiteId`]
//! discriminator + key material + the 38-byte AAD buffer and produces /
//! verifies an `(nonce, ciphertext, tag)` triple. Pure AEAD layer; the
//! on-disk skippable-frame envelope that wraps these bytes is its own
//! module (wire format, separate slice of #251).
//!
//! ## Suite registry
//!
//! - [`SuiteId::Aes256Gcm`]: 256-bit AES key, 12-byte nonce, 16-byte tag.
//!   Backed by the `aes-gcm` crate.
//! - [`SuiteId::ChaCha20Poly1305`]: 256-bit `ChaCha20` key, 12-byte
//!   nonce, 16-byte tag. Backed by the `chacha20poly1305` crate.
//!
//! Both suites share the same `aead` 0.6 trait surface, so the dispatch
//! is a thin match on the suite id; no per-suite extension trait.
//!
//! ## Key material
//!
//! Keys are caller-supplied 32-byte buffers. This module does not own a
//! key chain. The `KeyEpoch -> key` mapping lives in the wire-format
//! slice (separate follow-up) and is not in scope for this module.

#![cfg(feature = "encryption")]

use super::aad::SuiteId;
use super::error::DecryptError;

/// Fixed AEAD tag length for every v1 suite in the registry (16 bytes).
///
/// Bound at the trait level by `aead::AeadInOut`; this constant is the
/// concrete spec contract (`docs/aad-block-format.md` §5.1, `AEADTag`).
pub const TAG_LEN: usize = 16;

/// Encrypts `plaintext` in place: the buffer is overwritten with the
/// ciphertext and the AEAD tag is returned separately.
///
/// Caller MUST pass a nonce of the correct length for the suite (see
/// [`SuiteId::nonce_len`]); passing a wrong length is a programmer error
/// and the underlying AEAD primitive will reject it via
/// `crate::Error::Encrypt`.
///
/// The 32-byte `key` MUST match the key the reader will use for the
/// matching `KeyEpoch`. The 38-byte `aad` is the buffer produced by
/// [`super::aad::build`] and is bound into the tag.
///
/// # Errors
///
/// Returns [`crate::Error::Encrypt`] if the AEAD primitive rejects the
/// inputs (wrong nonce length, allocator failure inside the underlying
/// implementation, etc.). Encryption itself is infallible on valid
/// inputs; this only surfaces programmer / runtime errors.
pub fn encrypt_in_place(
    suite: SuiteId,
    key: &[u8; 32],
    nonce: &[u8],
    aad: &[u8],
    plaintext: &mut [u8],
) -> crate::Result<[u8; TAG_LEN]> {
    use aes_gcm::aead::{AeadInOut, KeyInit, Nonce};

    match suite {
        SuiteId::Aes256Gcm => {
            let cipher = aes_gcm::Aes256Gcm::new(key.into());
            let nonce = Nonce::<aes_gcm::Aes256Gcm>::try_from(nonce)
                .map_err(|_| crate::Error::Encrypt("AES-256-GCM nonce length mismatch"))?;
            let tag = cipher
                .encrypt_inout_detached(&nonce, aad, plaintext.into())
                .map_err(|_| crate::Error::Encrypt("AES-256-GCM encryption failed"))?;
            let mut out = [0u8; TAG_LEN];
            out.copy_from_slice(&tag);
            Ok(out)
        }
        SuiteId::ChaCha20Poly1305 => {
            let cipher = chacha20poly1305::ChaCha20Poly1305::new(key.into());
            let nonce = Nonce::<chacha20poly1305::ChaCha20Poly1305>::try_from(nonce)
                .map_err(|_| crate::Error::Encrypt("ChaCha20-Poly1305 nonce length mismatch"))?;
            let tag = cipher
                .encrypt_inout_detached(&nonce, aad, plaintext.into())
                .map_err(|_| crate::Error::Encrypt("ChaCha20-Poly1305 encryption failed"))?;
            let mut out = [0u8; TAG_LEN];
            out.copy_from_slice(&tag);
            Ok(out)
        }
    }
}

/// Decrypts `ciphertext` in place using the suite-bound AEAD primitive.
///
/// The buffer is overwritten with the recovered plaintext only when
/// verification succeeds. On AEAD tag mismatch the buffer contents are
/// undefined (the underlying primitive may have written partial
/// plaintext before catching the mismatch); callers MUST treat the
/// buffer as poisoned in that case.
///
/// # Errors
///
/// Returns [`DecryptError::AeadVerificationFailed`] on tag mismatch
/// (tampering, wrong key, wrong nonce, wrong AAD: all surface as the
/// same opaque variant by design; see [`DecryptError`] docs for the
/// rationale). Returns [`DecryptError::MalformedBodyFrame`] if the
/// supplied nonce isn't the right length for the suite.
pub fn decrypt_in_place(
    suite: SuiteId,
    key: &[u8; 32],
    nonce: &[u8],
    aad: &[u8],
    tag: &[u8; TAG_LEN],
    ciphertext: &mut [u8],
) -> Result<(), DecryptError> {
    use aes_gcm::aead::{AeadInOut, KeyInit, Nonce, Tag};

    match suite {
        SuiteId::Aes256Gcm => {
            let cipher = aes_gcm::Aes256Gcm::new(key.into());
            let nonce = Nonce::<aes_gcm::Aes256Gcm>::try_from(nonce)
                .map_err(|_| DecryptError::MalformedBodyFrame("nonce length != suite nonce_len"))?;
            let aead_tag = Tag::<aes_gcm::Aes256Gcm>::try_from(&tag[..])
                .map_err(|_| DecryptError::MalformedBodyFrame("tag length != 16"))?;
            cipher
                .decrypt_inout_detached(&nonce, aad, ciphertext.into(), &aead_tag)
                .map_err(|_| DecryptError::AeadVerificationFailed)
        }
        SuiteId::ChaCha20Poly1305 => {
            let cipher = chacha20poly1305::ChaCha20Poly1305::new(key.into());
            let nonce = Nonce::<chacha20poly1305::ChaCha20Poly1305>::try_from(nonce)
                .map_err(|_| DecryptError::MalformedBodyFrame("nonce length != suite nonce_len"))?;
            let aead_tag = Tag::<chacha20poly1305::ChaCha20Poly1305>::try_from(&tag[..])
                .map_err(|_| DecryptError::MalformedBodyFrame("tag length != 16"))?;
            cipher
                .decrypt_inout_detached(&nonce, aad, ciphertext.into(), &aead_tag)
                .map_err(|_| DecryptError::AeadVerificationFailed)
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test code"
)]
mod tests {
    use super::*;
    use crate::encryption::aad::{BlockType, EncryptionContext, build};
    use crate::table::block::BlockIdentity as TableBlockIdentity;

    /// 32-byte test key. Synthesised from a stable byte pattern so the
    /// fixture is deterministic across runs but visibly fake (never
    /// confused with real key material in a debugger).
    const TEST_KEY: [u8; 32] = [0x42; 32];
    /// Alt 32-byte key for "wrong key" assertions.
    const TEST_KEY_OTHER: [u8; 32] = [0x55; 32];

    /// Build a canonical 38-byte AAD for a concrete test block.
    fn test_aad(suite: SuiteId) -> [u8; 38] {
        let ctx = EncryptionContext::v1(
            7, // key_epoch
            suite, 0, // CompressionType::None tag
        );
        let identity = TableBlockIdentity::for_test(0xAB, 0xCD, BlockType::Data);
        build(&ctx, &identity)
    }

    fn test_nonce() -> [u8; 12] {
        // Deterministic non-zero pattern; production code uses a CSPRNG.
        [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C,
        ]
    }

    fn roundtrip_for(suite: SuiteId) {
        let plaintext = b"hello AAD-bound world";
        let aad = test_aad(suite);
        let nonce = test_nonce();

        let mut buf = plaintext.to_vec();
        let tag = encrypt_in_place(suite, &TEST_KEY, &nonce, &aad, &mut buf)
            .expect("encrypt should succeed");
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
        // 11 bytes instead of the suite's 12.
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
        let tag =
            encrypt_in_place(SuiteId::Aes256Gcm, &TEST_KEY, &nonce, &aes_aad, &mut buf).unwrap();

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
}

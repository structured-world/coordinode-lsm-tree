// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! AEAD (Authenticated Encryption with Associated Data) primitives for
//! the AAD-bound block format.
//!
//! Sits on top of the [`super::aad`] module: takes a [`super::aad::SuiteId`]
//! discriminator + key material + the 39-byte AAD buffer and produces /
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

use super::aad::{AAD_LEN, SuiteId};
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
/// matching `KeyEpoch`. The 39-byte `aad` is the buffer produced by
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
    aad: &[u8; AAD_LEN],
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
    aad: &[u8; AAD_LEN],
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
#[expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test code"
)]
mod tests;

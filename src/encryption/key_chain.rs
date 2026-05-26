// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Key chain: caller-side registry mapping `KeyEpoch` → 32-byte key.
//!
//! Per the AAD-bound block format (`docs/aad-block-format.md` §5.1),
//! every encrypted block records a 1-byte `KeyEpoch` in its
//! `MetadataFrame`. On read the decoder uses the epoch to look up
//! the right 32-byte key from the caller's key chain; on write the
//! caller chooses which epoch to use (by setting
//! `EncryptionContext::key_epoch` before calling `encrypt_block`)
//! and the encoder records that choice into the frame. The
//! [`KeyChain`] trait itself has no "active epoch" concept — it is
//! a passive lookup table from `KeyEpoch` to key bytes; epoch
//! selection / rotation policy lives in the caller (typically a
//! key-management service or a configuration loader).
//!
//! The chain is caller-managed (not stored in the LSM): different
//! deployments have different key-rotation policies (hardware HSM,
//! cloud KMS, file-backed, in-memory), and the LSM only needs the
//! single trait method `key(epoch) -> Option<&[u8; 32]>` to seal
//! and unseal. Concrete implementations (a KMS-backed chain, a
//! file-backed chain, a one-shot chain for tests) live outside the
//! LSM crate; this module ships only the trait and an in-memory
//! reference impl that the LSM's own tests use.

#[cfg(feature = "std")]
use std::collections::HashMap;

/// Caller-supplied registry mapping `KeyEpoch` bytes to the 32-byte
/// AEAD keys the LSM uses to seal / unseal blocks.
///
/// The trait carries only the lookup method; key rotation, key
/// derivation, and key storage are all caller concerns.
///
/// `key(epoch)` returning `None` surfaces as
/// [`crate::encryption::error::DecryptError::UnknownKeyEpoch`] on
/// the read path — distinct from
/// [`crate::encryption::error::DecryptError::AeadVerificationFailed`]
/// so operators can tell key-rotation drift apart from active
/// tampering.
pub trait KeyChain: Send + Sync {
    /// Returns the 32-byte key for `epoch`, or `None` if the chain
    /// does not know that epoch.
    fn key(&self, epoch: u8) -> Option<&[u8; 32]>;
}

/// In-memory `KeyChain` keyed by [`u8`] → 32-byte key.
///
/// Lives in this crate for the LSM's own unit / integration tests
/// and for deployments that load keys from a single file at
/// startup and never rotate at runtime (the simplest production
/// shape). KMS-backed chains, file-backed chains with
/// hot-rotation, etc. implement [`KeyChain`] outside the LSM.
#[cfg(feature = "std")]
#[derive(Default, Debug, Clone)]
pub struct StaticKeyChain {
    keys: HashMap<u8, [u8; 32]>,
}

#[cfg(feature = "std")]
impl StaticKeyChain {
    /// Build an empty key chain.
    #[must_use]
    pub fn new() -> Self {
        Self {
            keys: HashMap::new(),
        }
    }

    /// Install `key` at `epoch`. If an entry already exists for
    /// `epoch` it is replaced; the previous value is dropped.
    #[must_use]
    pub fn with_key(mut self, epoch: u8, key: [u8; 32]) -> Self {
        self.keys.insert(epoch, key);
        self
    }

    /// Number of installed key epochs.
    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Whether the chain has no installed keys.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

#[cfg(feature = "std")]
impl KeyChain for StaticKeyChain {
    fn key(&self, epoch: u8) -> Option<&[u8; 32]> {
        self.keys.get(&epoch)
    }
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;

    #[test]
    fn static_chain_returns_installed_key() {
        let chain = StaticKeyChain::new().with_key(7, [0x42; 32]);
        assert_eq!(chain.key(7), Some(&[0x42; 32]));
    }

    #[test]
    fn static_chain_missing_epoch_returns_none() {
        let chain = StaticKeyChain::new().with_key(7, [0x42; 32]);
        assert!(chain.key(8).is_none());
    }

    #[test]
    fn with_key_replaces_existing_epoch() {
        let chain = StaticKeyChain::new()
            .with_key(1, [0x01; 32])
            .with_key(1, [0x02; 32]);
        assert_eq!(chain.key(1), Some(&[0x02; 32]));
        assert_eq!(chain.len(), 1);
    }
}

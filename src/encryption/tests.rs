use super::*;

#[test]
fn encryption_provider_trait_is_object_safe() {
    // Compile-time check: the trait can be used as a trait object.
    fn _assert_object_safe(_: &dyn EncryptionProvider) {}
}

/// Minimal provider that only implements required methods,
/// exercising the default `encrypt_vec/decrypt_vec` implementations.
struct XorProvider;

impl core::panic::UnwindSafe for XorProvider {}
impl core::panic::RefUnwindSafe for XorProvider {}

impl EncryptionProvider for XorProvider {
    fn encrypt(&self, plaintext: &[u8]) -> crate::Result<Vec<u8>> {
        Ok(plaintext.iter().map(|b| b ^ 0xAA).collect())
    }

    fn max_overhead(&self) -> u32 {
        0
    }

    fn decrypt(&self, ciphertext: &[u8]) -> crate::Result<Vec<u8>> {
        Ok(ciphertext.iter().map(|b| b ^ 0xAA).collect())
    }
}

#[test]
fn default_encrypt_vec_delegates_to_encrypt() -> crate::Result<()> {
    let provider = XorProvider;
    let plaintext = b"test default encrypt_vec";

    let via_encrypt = provider.encrypt(plaintext)?;
    let via_encrypt_vec = provider.encrypt_vec(plaintext.to_vec())?;
    assert_eq!(via_encrypt, via_encrypt_vec);

    let decrypted = provider.decrypt(&via_encrypt_vec)?;
    assert_eq!(decrypted, plaintext);
    Ok(())
}

#[test]
fn default_decrypt_vec_delegates_to_decrypt() -> crate::Result<()> {
    let provider = XorProvider;
    let plaintext = b"test default decrypt_vec";

    let ciphertext = provider.encrypt(plaintext)?;

    let via_decrypt = provider.decrypt(&ciphertext)?;
    let via_decrypt_vec = provider.decrypt_vec(ciphertext)?;
    assert_eq!(via_decrypt, via_decrypt_vec);
    assert_eq!(via_decrypt_vec, plaintext);
    Ok(())
}

#[test]
fn default_aad_block_methods_reject_unsupported_provider() {
    // A provider that implements only the opaque surface (no AAD
    // override) must surface a typed error from the AAD-bound block
    // entry points rather than silently mis-encrypting: the trait
    // defaults return `Encrypt` / `Decrypt`. This pins the contract
    // that only AAD-capable providers may serve the block path.
    let provider = XorProvider;
    // The capability flag and the default block methods must agree: if
    // the default ever regressed to `true`, `Config::open` would let an
    // unsupported provider through and only fail mid-I/O.
    assert!(
        !provider.supports_aad_block_path(),
        "default providers must advertise the AAD block path as unsupported",
    );
    let identity =
        crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data);

    let enc = provider.encrypt_block_aad(b"plaintext", &identity, 0, 0);
    assert!(
        matches!(enc, Err(crate::Error::Encrypt(_))),
        "default encrypt_block_aad must reject, got {enc:?}"
    );

    let dec = provider.decrypt_block_aad(b"ciphertext", &identity);
    assert!(
        matches!(dec, Err(crate::Error::Decrypt(_))),
        "default decrypt_block_aad must reject, got {dec:?}"
    );
}

#[cfg(feature = "encryption")]
mod aes256gcm {
    use super::*;

    fn test_key() -> [u8; 32] {
        [0x42; 32]
    }

    #[test]
    fn roundtrip_basic() -> crate::Result<()> {
        let provider = Aes256GcmProvider::new(&test_key());
        let plaintext = b"hello world, this is a block of data!";

        let ciphertext = provider.encrypt(plaintext)?;
        assert_ne!(&ciphertext[..], plaintext.as_slice());
        assert_eq!(
            ciphertext.len(),
            Aes256GcmProvider::NONCE_LEN + plaintext.len() + Aes256GcmProvider::TAG_LEN,
        );

        let decrypted = provider.decrypt(&ciphertext)?;
        assert_eq!(decrypted, plaintext);
        Ok(())
    }

    #[test]
    fn roundtrip_empty() -> crate::Result<()> {
        let provider = Aes256GcmProvider::new(&test_key());
        let plaintext = b"";

        let ciphertext = provider.encrypt(plaintext)?;
        let decrypted = provider.decrypt(&ciphertext)?;
        assert_eq!(decrypted, plaintext);
        Ok(())
    }

    #[test]
    fn different_nonces_produce_different_ciphertexts() -> crate::Result<()> {
        let provider = Aes256GcmProvider::new(&test_key());
        let plaintext = b"deterministic input";

        let ct1 = provider.encrypt(plaintext)?;
        let ct2 = provider.encrypt(plaintext)?;
        assert_ne!(
            ct1, ct2,
            "random nonces should produce different ciphertexts"
        );

        // Both decrypt to the same plaintext
        assert_eq!(provider.decrypt(&ct1)?, provider.decrypt(&ct2)?,);
        Ok(())
    }

    #[test]
    fn wrong_key_fails_decrypt() -> crate::Result<()> {
        let provider1 = Aes256GcmProvider::new(&[0x01; 32]);
        let provider2 = Aes256GcmProvider::new(&[0x02; 32]);

        let ciphertext = provider1.encrypt(b"secret")?;
        let result = provider2.decrypt(&ciphertext);
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn tampered_ciphertext_fails_decrypt() -> crate::Result<()> {
        let provider = Aes256GcmProvider::new(&test_key());
        let mut ciphertext = provider.encrypt(b"data")?;

        // Flip a byte in the ciphertext body
        let mid = Aes256GcmProvider::NONCE_LEN + 1;
        if mid < ciphertext.len() {
            #[expect(clippy::indexing_slicing)]
            {
                ciphertext[mid] ^= 0xFF;
            }
        }

        let result = provider.decrypt(&ciphertext);
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn truncated_ciphertext_fails_decrypt() -> crate::Result<()> {
        let provider = Aes256GcmProvider::new(&test_key());
        let result = provider.decrypt(&[0u8; 10]); // less than nonce + tag
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn from_slice_rejects_wrong_length() {
        assert!(Aes256GcmProvider::from_slice(&[0u8; 16]).is_err());
        assert!(Aes256GcmProvider::from_slice(&[0u8; 31]).is_err());
        assert!(Aes256GcmProvider::from_slice(&[0u8; 33]).is_err());
        assert!(Aes256GcmProvider::from_slice(&[0u8; 32]).is_ok());
    }

    #[test]
    fn roundtrip_large_payload() -> crate::Result<()> {
        let provider = Aes256GcmProvider::new(&test_key());
        let plaintext = vec![0xAB_u8; 64 * 1024]; // 64 KiB

        let ciphertext = provider.encrypt(&plaintext)?;
        let decrypted = provider.decrypt(&ciphertext)?;
        assert_eq!(decrypted, plaintext);
        Ok(())
    }

    /// Verify the thread-local CSPRNG produces unique nonces across many
    /// encrypt calls — no nonce reuse even under rapid sequential use.
    #[test]
    fn thread_local_rng_produces_unique_nonces() -> crate::Result<()> {
        let provider = Aes256GcmProvider::new(&test_key());
        let plaintext = b"nonce uniqueness test";

        let mut nonces = std::collections::HashSet::new();
        for _ in 0..1000 {
            let ct = provider.encrypt(plaintext)?;

            #[expect(clippy::indexing_slicing, reason = "ct always >= NONCE_LEN")]
            #[expect(clippy::expect_used, reason = "test assertion")]
            let nonce: [u8; Aes256GcmProvider::NONCE_LEN] = ct[..Aes256GcmProvider::NONCE_LEN]
                .try_into()
                .expect("nonce has expected length");

            assert!(
                nonces.insert(nonce),
                "nonce collision detected — CSPRNG produced duplicate nonce"
            );
        }
        Ok(())
    }

    /// Verify `ForkAwareRng` actually REPLACES the inner RNG (not just
    /// restores PID bookkeeping) when it detects a PID change.
    ///
    /// Stamps a deterministic large `word_pos` into the inner ChaCha20Rng,
    /// triggers fake-PID reseed, and asserts `word_pos` was reset to a
    /// fresh-RNG value. If the `*rng_ref = new_chacha_rng()` line were
    /// removed from `with_rng`, the stamped offset would survive and this
    /// test would fail.
    /// Distinctive word_pos that cannot occur naturally on a freshly-seeded
    /// RNG after a single u64 draw (which advances word_pos by 2).
    const SENTINEL_WORD_POS: u128 = 0xDEAD_BEEF_u128;

    #[test]
    fn fork_aware_rng_reseeds_on_pid_change() {
        let rng = ForkAwareRng::new();

        // Initialize the lazy RNG.
        let _ = rng.with_rng(aes_gcm::aead::rand_core::Rng::next_u64);
        rng.rng.borrow_mut().set_word_pos(SENTINEL_WORD_POS);
        assert_eq!(rng.rng.borrow().get_word_pos(), SENTINEL_WORD_POS);

        // Simulate fork: stamp a fake PID different from the real one.
        let real_pid = std::process::id();
        rng.pid.set(real_pid ^ 1);

        // Next access detects PID mismatch → replaces the inner RNG with
        // a fresh seed from the OS RNG. After one u64 draw on the fresh
        // RNG, word_pos must be
        // a small fresh-RNG value, NOT SENTINEL_WORD_POS + 2.
        let _ = rng.with_rng(aes_gcm::aead::rand_core::Rng::next_u64);

        assert_eq!(
            rng.pid.get(),
            real_pid,
            "PID should be restored to real process ID after reseed"
        );

        let post_word_pos = rng.rng.borrow().get_word_pos();
        assert!(
            post_word_pos < SENTINEL_WORD_POS,
            "inner RNG was not replaced on reseed: post word_pos {post_word_pos:#x} \
             should be a fresh-RNG value, not {SENTINEL_WORD_POS:#x}+ \
             (would indicate fork-safety reseed is broken)"
        );
    }

    #[test]
    fn encrypt_vec_roundtrip() -> crate::Result<()> {
        let provider = Aes256GcmProvider::new(&test_key());
        let plaintext = b"block data for encrypt_vec test";

        let ciphertext = provider.encrypt_vec(plaintext.to_vec())?;
        assert_eq!(
            ciphertext.len(),
            Aes256GcmProvider::NONCE_LEN + plaintext.len() + Aes256GcmProvider::TAG_LEN,
        );

        // encrypt_vec output must be decryptable by decrypt
        let decrypted = provider.decrypt(&ciphertext)?;
        assert_eq!(decrypted, plaintext);
        Ok(())
    }

    #[test]
    fn decrypt_vec_roundtrip() -> crate::Result<()> {
        let provider = Aes256GcmProvider::new(&test_key());
        let plaintext = b"block data for decrypt_vec test";

        // encrypt output must be decryptable by decrypt_vec
        let ciphertext = provider.encrypt(plaintext)?;
        let decrypted = provider.decrypt_vec(ciphertext)?;
        assert_eq!(decrypted, plaintext);
        Ok(())
    }

    #[test]
    fn encrypt_vec_decrypt_vec_roundtrip() -> crate::Result<()> {
        let provider = Aes256GcmProvider::new(&test_key());
        let plaintext = vec![0xCD_u8; 16 * 1024]; // 16 KiB

        let ciphertext = provider.encrypt_vec(plaintext.clone())?;
        let decrypted = provider.decrypt_vec(ciphertext)?;
        assert_eq!(decrypted, plaintext);
        Ok(())
    }

    #[test]
    fn encrypt_vec_empty() -> crate::Result<()> {
        let provider = Aes256GcmProvider::new(&test_key());

        let ciphertext = provider.encrypt_vec(vec![])?;
        let decrypted = provider.decrypt_vec(ciphertext)?;
        assert!(decrypted.is_empty());
        Ok(())
    }

    #[test]
    fn decrypt_vec_truncated_fails() -> crate::Result<()> {
        let provider = Aes256GcmProvider::new(&test_key());
        let result = provider.decrypt_vec(vec![0u8; 10]);
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn decrypt_vec_tampered_fails() -> crate::Result<()> {
        let provider = Aes256GcmProvider::new(&test_key());
        let mut ciphertext = provider.encrypt_vec(b"data".to_vec())?;

        let mid = Aes256GcmProvider::NONCE_LEN + 1;
        if mid < ciphertext.len() {
            #[expect(clippy::indexing_slicing)]
            {
                ciphertext[mid] ^= 0xFF;
            }
        }

        let result = provider.decrypt_vec(ciphertext);
        assert!(result.is_err());
        Ok(())
    }
}

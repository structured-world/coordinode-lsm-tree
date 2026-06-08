// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

//! Block-level encryption at rest.
//!
//! This module defines the [`EncryptionProvider`] trait for pluggable
//! block-level encryption and, behind the `encryption` feature, ships a
//! ready-to-use [`Aes256GcmProvider`] implementation.
//!
//! ## Pipeline order
//!
//! - **Write:** raw data → compress → **encrypt** → checksum → disk
//! - **Read:** disk → verify checksum → **decrypt** → decompress → raw data
//!
//! Checksums protect the encrypted (on-disk) bytes so that corruption is
//! detected cheaply before any decryption attempt.
//!
//! ## AAD-bound block format
//!
//! The AAD-bound on-disk block format from
//! `docs/aad-block-format.md` ships in the submodules:
//!
//! - `aad`: pure-byte AAD construction, `aad::SuiteId` /
//!   `aad::EncryptionContext`, plus re-exports of the existing
//!   `crate::table::block::BlockType` / `crate::table::block::BlockIdentity`
//!   so the AAD path and the Block I/O path share one identity type.
//! - `error`: `DecryptError` enum with one variant per decode-time
//!   failure mode (re-exported at the crate's encryption surface).
//! - `aead` (feature `encryption`): per-suite AEAD dispatch
//!   (AES-256-GCM, ChaCha20-Poly1305) that takes the 39-byte AAD
//!   from `aad::build` and produces / verifies the
//!   `(nonce, ciphertext, tag)` triple.
//! - `key_chain`: the `KeyChain` trait + in-memory
//!   `StaticKeyChain` reference impl, decoupling KeyEpoch → key
//!   lookup from the LSM crate.
//! - `block` (features `encryption` + `zstd_any`): top-level
//!   `encrypt_block` / `decrypt_block` entry points wrapping
//!   the `MetadataFrame ‖ BodyFrame` skippable-frame wire format
//!   and returning `DecryptedBlock` (plaintext + parsed codec
//!   context for the caller to thread through
//!   `structured_zstd::FrameDecoder` setters).
//!
//! (Module and item names above are inline rather than intra-doc links
//! because several — `aead`, `block`, `encrypt_block`, `decrypt_block`,
//! `DecryptedBlock` — are feature-gated and absent from doc builds that
//! omit those features, which would break the links.)
//!
//! Still pending in a follow-up: replacing the legacy
//! [`Aes256GcmProvider`] Block I/O code path below with the
//! `encrypt_block` / `decrypt_block` entry points at every
//! `Block::write_into` / `Block::from_reader` site.

pub mod aad;
#[cfg(feature = "encryption")]
pub mod aead;
// `block` depends on `structured_zstd::skippable::SkippableFrame`,
// which is only in the dep graph when the `zstd` cargo feature is
// enabled (the dep is `optional`, pulled in by `feature = "zstd"`).
// `encryption` alone doesn't bring zstd, so gate the wire-format
// module on both features. Without `zstd`, the AAD types + AEAD
// primitives still compile; only the SkippableFrame-wrapped
// encrypt_block / decrypt_block entry points are absent.
#[cfg(all(feature = "encryption", zstd_any))]
pub mod block;
pub mod error;
pub mod key_chain;

// Top-level re-exports so callers can spell `crate::encryption::
// encrypt_block` / `decrypt_block` / `DecryptedBlock` /
// `KeyChain` / `DecryptError` directly, instead of paging
// through the submodule path each time. Matches the surface
// shape #251's design discussion settled on.
#[cfg(all(feature = "encryption", zstd_any))]
pub use block::{DecryptedBlock, decrypt_block, encrypt_block};
pub use error::DecryptError;
pub use key_chain::KeyChain;
#[cfg(feature = "std")]
pub use key_chain::StaticKeyChain;

/// Block encryption provider.
///
/// Implementors handle key management, nonce generation, and algorithm
/// selection. The trait is object-safe so it can be stored as
/// `Arc<dyn EncryptionProvider>`.
///
/// # Contract
///
/// - [`encrypt`](EncryptionProvider::encrypt) must be deterministic in output
///   *format* (but not value — nonces should be random or unique).
/// - [`decrypt`](EncryptionProvider::decrypt) must accept the exact byte
///   sequence returned by `encrypt` and recover the original plaintext.
/// - Both methods must be safe to call concurrently from multiple threads.
pub trait EncryptionProvider:
    Send + Sync + std::panic::UnwindSafe + std::panic::RefUnwindSafe
{
    /// Encrypt `plaintext`, returning an opaque ciphertext blob.
    ///
    /// The returned bytes may include a nonce/IV prefix and an
    /// authentication tag — the layout is provider-defined.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::Encrypt`] if the encryption operation fails.
    fn encrypt(&self, plaintext: &[u8]) -> crate::Result<Vec<u8>>;

    /// Maximum number of bytes that encryption adds to a plaintext payload.
    ///
    /// Used by block I/O to account for encryption overhead in size
    /// validation. For AES-256-GCM this is 28 (12-byte nonce + 16-byte tag).
    ///
    /// Returns `u32` because block sizes are `u32`-bounded on disk.
    fn max_overhead(&self) -> u32;

    /// Whether this provider implements the AAD-bound block path
    /// ([`encrypt_block_aad`](Self::encrypt_block_aad) /
    /// [`decrypt_block_aad`](Self::decrypt_block_aad)).
    ///
    /// On a `zstd` build the live block I/O path routes encrypted blocks
    /// through the AAD-bound envelope, so a provider that only implements the
    /// opaque [`encrypt`](Self::encrypt) / [`decrypt`](Self::decrypt) surface
    /// would fail on the first encrypted read/write. The store validates this
    /// capability when an encryption provider is configured (see
    /// `Config::open`) and rejects an unsupported provider up front rather than
    /// failing mid-I/O. Defaults to `false`; AAD-capable providers (e.g.
    /// [`Aes256GcmProvider`]) override it to `true`.
    #[must_use]
    fn supports_aad_block_path(&self) -> bool {
        false
    }

    /// Decrypt `ciphertext` previously produced by [`encrypt`](EncryptionProvider::encrypt).
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::Decrypt`] if the ciphertext is invalid,
    /// tampered, or encrypted with a different key.
    fn decrypt(&self, ciphertext: &[u8]) -> crate::Result<Vec<u8>>;

    /// Encrypt an owned plaintext buffer, reusing its allocation when possible.
    ///
    /// The default implementation delegates to [`encrypt`](EncryptionProvider::encrypt).
    /// Providers may override this to avoid an extra allocation by prepending
    /// the nonce and appending the tag in-place.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::Encrypt`] if the encryption operation fails.
    fn encrypt_vec(&self, plaintext: Vec<u8>) -> crate::Result<Vec<u8>> {
        self.encrypt(&plaintext)
    }

    /// Decrypt an owned ciphertext buffer, reusing its allocation when possible.
    ///
    /// The default implementation delegates to [`decrypt`](EncryptionProvider::decrypt).
    /// Providers may override this to decrypt in-place, stripping the nonce
    /// prefix and tag suffix without a second allocation.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::Decrypt`] if the ciphertext is invalid,
    /// tampered, or encrypted with a different key.
    fn decrypt_vec(&self, ciphertext: Vec<u8>) -> crate::Result<Vec<u8>> {
        self.decrypt(&ciphertext)
    }

    /// AAD-bound block encryption: seal `plaintext` into a `MetadataFrame ‖
    /// BodyFrame` blob whose AEAD tag binds the block's identity and transform
    /// stack (anti-swap / anti-relabel), per `docs/aad-block-format.md`. The
    /// per-block AAD inputs (`identity`, `compression_type`, `block_flags`) come
    /// from the block layer; the provider supplies the key material (key chain +
    /// active epoch + suite). This is the live block-I/O encryption path
    /// (`BlockTransform`), distinct from the opaque [`encrypt`](Self::encrypt)
    /// the manifest subsystem still uses.
    ///
    /// # Errors
    ///
    /// The default returns [`crate::Error::Encrypt`]: a provider that cannot bind
    /// AAD must not serve the block path. AAD-capable providers (e.g.
    /// [`Aes256GcmProvider`]) override this.
    fn encrypt_block_aad(
        &self,
        _plaintext: &[u8],
        _identity: &crate::table::block::BlockIdentity,
        _compression_type: u8,
        _block_flags: u8,
    ) -> crate::Result<Vec<u8>> {
        Err(crate::Error::Encrypt(
            "provider does not support AAD-bound block encryption",
        ))
    }

    /// Inverse of [`encrypt_block_aad`](Self::encrypt_block_aad): verify the AAD
    /// binding and recover the plaintext. The transform fields the writer bound
    /// (`compression_type`, `block_flags`, `key_epoch`, `suite_id`) are read back
    /// from the frame's `MetadataFrame`; the reader supplies only `identity` (the
    /// out-of-band tree/table + dict/window context, from the per-SST
    /// descriptor + block handle). Any mismatch fails AEAD verification.
    ///
    /// # Errors
    ///
    /// [`crate::Error::Decrypt`] on tag-verify failure / malformed frame, or for
    /// providers that do not bind AAD (the default).
    fn decrypt_block_aad(
        &self,
        _ciphertext: &[u8],
        _identity: &crate::table::block::BlockIdentity,
    ) -> crate::Result<Vec<u8>> {
        Err(crate::Error::Decrypt(
            "provider does not support AAD-bound block decryption",
        ))
    }
}

// ---------------------------------------------------------------------------
// AES-256-GCM implementation (feature-gated)
// ---------------------------------------------------------------------------

/// AES-256-GCM encryption provider.
///
/// Each [`encrypt`](EncryptionProvider::encrypt) call generates a random
/// 12-byte nonce and prepends it to the ciphertext:
///
/// ```text
/// [nonce; 12 bytes][ciphertext + GCM tag; N + 16 bytes]
/// ```
///
/// Overhead per block: **28 bytes** (12 nonce + 16 auth tag).
///
/// # Key management
///
/// The caller is responsible for providing and rotating the 256-bit key.
/// This provider does not persist or derive keys.
#[cfg(feature = "encryption")]
pub struct Aes256GcmProvider {
    cipher: aes_gcm::Aes256Gcm,
    /// Key material for the AAD-bound block path
    /// ([`encrypt_block_aad`](EncryptionProvider::encrypt_block_aad)). The
    /// opaque [`encrypt`](EncryptionProvider::encrypt) (manifest subsystem) uses
    /// `cipher` directly; the block path looks the key up by epoch here.
    key_chain: crate::encryption::key_chain::StaticKeyChain,
    /// Active key epoch sealed into every block's `MetadataFrame`.
    key_epoch: u8,
    /// AEAD suite tag (AES-256-GCM) mirrored into the AAD.
    suite_id: crate::encryption::aad::SuiteId,
}

#[cfg(feature = "encryption")]
impl Aes256GcmProvider {
    /// Nonce size for AES-256-GCM (96 bits).
    const NONCE_LEN: usize = 12;

    /// GCM authentication tag size (128 bits).
    const TAG_LEN: usize = 16;

    /// Total per-block overhead: nonce + tag.
    pub const OVERHEAD: usize = Self::NONCE_LEN + Self::TAG_LEN;

    /// Create a new provider from a 256-bit (32-byte) key.
    ///
    /// The key length is enforced at compile time by the `[u8; 32]` type.
    /// For runtime-checked construction from a slice, use [`from_slice`](Self::from_slice).
    #[must_use]
    pub fn new(key: &[u8; 32]) -> Self {
        use aes_gcm::KeyInit;

        Self {
            cipher: aes_gcm::Aes256Gcm::new(key.into()),
            // Single-epoch shortcut: the one supplied key is epoch 0. Deployments
            // that rotate keys construct a provider over a multi-epoch chain.
            key_chain: crate::encryption::key_chain::StaticKeyChain::new().with_key(0, *key),
            key_epoch: 0,
            suite_id: crate::encryption::aad::SuiteId::Aes256Gcm,
        }
    }

    /// Create a provider from a key slice, returning an error if the
    /// length is not 32 bytes.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::Encrypt`] if `key` is not exactly 32 bytes.
    pub fn from_slice(key: &[u8]) -> crate::Result<Self> {
        let key: &[u8; 32] = key
            .try_into()
            .map_err(|_| crate::Error::Encrypt("AES-256-GCM key must be exactly 32 bytes"))?;
        Ok(Self::new(key))
    }
}

/// Create a new [`ChaCha20Rng`](rand_chacha::ChaCha20Rng) seeded from the OS RNG
/// (via the getrandom-backed `SysRng` exposed by aes-gcm's `Generate` trait).
///
/// Returns the RNG directly (not `Result`) because callers are
/// `thread_local!` init and fork-reseed, neither of which can propagate
/// errors. This function will panic if OS entropy is unavailable.
#[cfg(feature = "encryption")]
fn new_chacha_rng() -> rand_chacha::ChaCha20Rng {
    use aes_gcm::aead::Generate;
    use aes_gcm::aead::rand_core::SeedableRng;

    // `<[u8; 32]>::generate()` pulls 32 bytes from the getrandom-backed
    // `SysRng` and panics on OS entropy failure (same semantics as the
    // previous `ChaCha20Rng::from_rng(OsRng).expect(...)`).
    let seed: [u8; 32] = <[u8; 32]>::generate();
    rand_chacha::ChaCha20Rng::from_seed(seed)
}

/// Thread-local CSPRNG wrapper with fork-aware PID tracking.
///
/// On each access, compares the stored PID with `std::process::id()`.
/// If they differ (i.e. the process was forked), the RNG is reseeded
/// from the OS RNG to avoid nonce reuse across processes.
#[cfg(feature = "encryption")]
struct ForkAwareRng {
    pid: std::cell::Cell<u32>,
    rng: std::cell::RefCell<rand_chacha::ChaCha20Rng>,
}

#[cfg(feature = "encryption")]
impl ForkAwareRng {
    fn new() -> Self {
        Self {
            pid: std::cell::Cell::new(std::process::id()),
            rng: std::cell::RefCell::new(new_chacha_rng()),
        }
    }

    fn with_rng<R>(&self, f: impl FnOnce(&mut rand_chacha::ChaCha20Rng) -> R) -> R {
        let mut rng_ref = self.rng.borrow_mut();
        let current_pid = std::process::id();
        if self.pid.get() != current_pid {
            // Process was forked; reseed RNG to avoid nonce reuse across PIDs.
            self.pid.set(current_pid);
            *rng_ref = new_chacha_rng();
        }

        // The RefMut guard is held while f() runs. This is safe because
        // f() only generates a 12-byte nonce (no reentrant RNG access).
        // Deref-coercion: &mut RefMut<ChaCha20Rng> → &mut ChaCha20Rng
        // (explicit &mut *rng_ref is denied by clippy::explicit_auto_deref).
        f(&mut rng_ref)
    }
}

#[cfg(feature = "encryption")]
thread_local! {
    // Module-scope so all monomorphizations of `thread_local_rng`
    // share a single thread-local instance.
    static THREAD_RNG: ForkAwareRng = ForkAwareRng::new();
}

/// Access a thread-local CSPRNG seeded from the OS RNG in a fork-aware way.
///
/// Using a thread-local [`ChaCha20Rng`](rand_chacha::ChaCha20Rng) avoids a
/// `getrandom` syscall on every nonce generation, which saves 1-10 µs per
/// block under contention. The RNG is cryptographically secure and seeded
/// from the OS RNG on first access per thread, and is lazily reseeded on the
/// next use if the process ID changes (e.g., after a `fork()`) to reduce
/// the risk of nonce reuse across processes.
#[cfg(feature = "encryption")]
fn thread_local_rng<R>(f: impl FnOnce(&mut rand_chacha::ChaCha20Rng) -> R) -> R {
    THREAD_RNG.with(|state| state.with_rng(f))
}

#[cfg(feature = "encryption")]
impl EncryptionProvider for Aes256GcmProvider {
    fn max_overhead(&self) -> u32 {
        // The block path under zstd seals the AAD-bound `MetadataFrame ‖
        // BodyFrame` envelope, whose framing overhead over the plaintext is
        // 8 (meta SFA header) + 39 (meta payload, incl. 12-byte nonce) +
        // 8 (body SFA header) + 16 (auth tag) = 71 bytes. Without zstd the
        // opaque `[nonce ‖ ciphertext ‖ tag]` form is used: NONCE_LEN +
        // TAG_LEN = 28. This bound sizes the read-path payload check.
        #[cfg(zstd_any)]
        {
            8 + 39 + 8 + 16
        }
        #[cfg(not(zstd_any))]
        #[expect(clippy::cast_possible_truncation, reason = "OVERHEAD is 28")]
        {
            Self::OVERHEAD as u32
        }
    }

    // The AAD-bound block entry points are only compiled under `zstd_any` (the
    // envelope wraps `SkippableFrame`); on a non-zstd build this provider falls
    // back to the opaque block path, which needs no AAD capability, so the
    // default `false` is correct there.
    #[cfg(zstd_any)]
    fn supports_aad_block_path(&self) -> bool {
        true
    }

    fn encrypt(&self, plaintext: &[u8]) -> crate::Result<Vec<u8>> {
        // aes-gcm 0.11.0-rc.3 prerelease surface (pinned in Cargo.toml).
        // Migration trigger: bump when aes-gcm 0.11.0 stable ships.
        use aes_gcm::aead::{AeadInOut, Generate, Nonce};

        let nonce = thread_local_rng(Nonce::<aes_gcm::Aes256Gcm>::generate_from_rng);

        let mut buf = Vec::with_capacity(Self::NONCE_LEN + plaintext.len() + Self::TAG_LEN);
        buf.extend_from_slice(&nonce);
        buf.extend_from_slice(plaintext);

        // encrypt_inout_detached operates on buf[NONCE_LEN..] (the plaintext portion).
        // Indexing is safe: buf was allocated as nonce + plaintext.
        //
        // AAD wiring (block context: table_id, offset, block_type, dict_id, window_log)
        // tracked separately — see lsm-tree #250/#251/#252.
        #[expect(
            clippy::indexing_slicing,
            reason = "buf length = NONCE_LEN + plaintext.len()"
        )]
        let tag = self
            .cipher
            .encrypt_inout_detached(&nonce, b"", (&mut buf[Self::NONCE_LEN..]).into())
            .map_err(|_| crate::Error::Encrypt("AES-256-GCM encryption failed"))?;

        buf.extend_from_slice(&tag);

        Ok(buf)
    }

    fn decrypt(&self, ciphertext: &[u8]) -> crate::Result<Vec<u8>> {
        use aes_gcm::aead::{AeadInOut, Nonce, Tag};

        let min_len = Self::NONCE_LEN + Self::TAG_LEN;
        if ciphertext.len() < min_len {
            return Err(crate::Error::Decrypt(
                "ciphertext too short for AES-256-GCM (need nonce + tag)",
            ));
        }

        #[expect(clippy::indexing_slicing, reason = "length checked above")]
        let nonce = Nonce::<aes_gcm::Aes256Gcm>::try_from(&ciphertext[..Self::NONCE_LEN])
            .map_err(|_| crate::Error::Decrypt("AES-256-GCM nonce length mismatch"))?;

        // Safe: ciphertext.len() >= NONCE_LEN + TAG_LEN checked above
        let tag_start = ciphertext.len() - Self::TAG_LEN;

        #[expect(clippy::indexing_slicing, reason = "length checked above")]
        let tag = Tag::<aes_gcm::Aes256Gcm>::try_from(&ciphertext[tag_start..])
            .map_err(|_| crate::Error::Decrypt("AES-256-GCM tag length mismatch"))?;

        #[expect(clippy::indexing_slicing, reason = "length checked above")]
        let mut buf = ciphertext[Self::NONCE_LEN..tag_start].to_vec();

        self.cipher
            .decrypt_inout_detached(&nonce, b"", (&mut buf[..]).into(), &tag)
            .map_err(|_| {
                crate::Error::Decrypt("AES-256-GCM decryption failed (bad key or tampered data)")
            })?;

        Ok(buf)
    }

    fn encrypt_vec(&self, mut buf: Vec<u8>) -> crate::Result<Vec<u8>> {
        use aes_gcm::aead::{AeadInOut, Generate, Nonce};

        let nonce = thread_local_rng(Nonce::<aes_gcm::Aes256Gcm>::generate_from_rng);

        // Reserve space for nonce prefix + tag suffix in one allocation,
        // then shift plaintext right and write the nonce into the gap.
        let plaintext_len = buf.len();
        buf.reserve(Self::NONCE_LEN + Self::TAG_LEN);
        buf.resize(plaintext_len + Self::NONCE_LEN, 0);
        buf.copy_within(..plaintext_len, Self::NONCE_LEN);
        #[expect(
            clippy::indexing_slicing,
            reason = "buf was just resized to include NONCE_LEN"
        )]
        buf[..Self::NONCE_LEN].copy_from_slice(&nonce);

        #[expect(
            clippy::indexing_slicing,
            reason = "buf length ≥ NONCE_LEN after resize + copy_within"
        )]
        let tag = self
            .cipher
            .encrypt_inout_detached(&nonce, b"", (&mut buf[Self::NONCE_LEN..]).into())
            .map_err(|_| crate::Error::Encrypt("AES-256-GCM encryption failed"))?;

        buf.extend_from_slice(&tag);

        Ok(buf)
    }

    fn decrypt_vec(&self, mut buf: Vec<u8>) -> crate::Result<Vec<u8>> {
        use aes_gcm::aead::{AeadInOut, Nonce, Tag};

        // Error::Decrypt takes &'static str — can't include runtime lengths
        // without changing the upstream error type to accept String/Cow.
        let min_len = Self::NONCE_LEN + Self::TAG_LEN;
        if buf.len() < min_len {
            return Err(crate::Error::Decrypt(
                "ciphertext too short for AES-256-GCM (need nonce + tag)",
            ));
        }

        // Copy nonce and tag to the stack before mutating the buffer.
        #[expect(clippy::indexing_slicing, reason = "length checked above")]
        let nonce = Nonce::<aes_gcm::Aes256Gcm>::try_from(&buf[..Self::NONCE_LEN])
            .map_err(|_| crate::Error::Decrypt("AES-256-GCM nonce length mismatch"))?;

        let tag_start = buf.len() - Self::TAG_LEN;
        #[expect(clippy::indexing_slicing, reason = "length checked above")]
        let tag = Tag::<aes_gcm::Aes256Gcm>::try_from(&buf[tag_start..])
            .map_err(|_| crate::Error::Decrypt("AES-256-GCM tag length mismatch"))?;

        // Strip nonce prefix and tag suffix via copy_within + truncate
        // (single memmove, avoids Drain iterator adapter overhead).
        buf.copy_within(Self::NONCE_LEN..tag_start, 0);
        buf.truncate(tag_start - Self::NONCE_LEN);

        self.cipher
            .decrypt_inout_detached(&nonce, b"", (&mut buf[..]).into(), &tag)
            .map_err(|_| {
                crate::Error::Decrypt("AES-256-GCM decryption failed (bad key or tampered data)")
            })?;

        Ok(buf)
    }

    // The AAD-bound entry points (`encrypt_block` / `decrypt_block`) live in the
    // `block` module, gated on `zstd_any` (it wraps `SkippableFrame`). Without
    // zstd this provider falls back to the trait default (unsupported), since the
    // wire-format frame is unavailable.
    #[cfg(zstd_any)]
    fn encrypt_block_aad(
        &self,
        plaintext: &[u8],
        identity: &crate::table::block::BlockIdentity,
        compression_type: u8,
        block_flags: u8,
    ) -> crate::Result<Vec<u8>> {
        let ctx = crate::encryption::aad::EncryptionContext::v1(
            self.key_epoch,
            self.suite_id,
            compression_type,
            block_flags,
        );
        crate::encryption::encrypt_block(plaintext, identity, &ctx, &self.key_chain)
    }

    #[cfg(zstd_any)]
    fn decrypt_block_aad(
        &self,
        ciphertext: &[u8],
        identity: &crate::table::block::BlockIdentity,
    ) -> crate::Result<Vec<u8>> {
        // The frame self-describes its transform fields (key_epoch / suite /
        // compression_type / block_flags); the reader supplies only `identity`.
        match crate::encryption::decrypt_block(ciphertext, identity, &self.key_chain) {
            Ok(decrypted) => Ok(decrypted.plaintext),
            Err(e) => {
                // Map the typed AAD-decrypt failure into the crate error; the
                // specific variant (block-swap / dict-sub / tamper) is logged for
                // diagnostics since `Error::Decrypt` carries only a static reason.
                log::debug!("AAD-bound block decryption failed: {e:?}");
                Err(crate::Error::Decrypt(
                    "AAD-bound block decryption failed (tampered, wrong key, or malformed frame)",
                ))
            }
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::doc_markdown,
    clippy::redundant_clone,
    clippy::unnecessary_wraps,
    clippy::redundant_closure_for_method_calls
)]
mod tests {
    use super::*;

    #[test]
    fn encryption_provider_trait_is_object_safe() {
        // Compile-time check: the trait can be used as a trait object.
        fn _assert_object_safe(_: &dyn EncryptionProvider) {}
    }

    /// Minimal provider that only implements required methods,
    /// exercising the default `encrypt_vec/decrypt_vec` implementations.
    struct XorProvider;

    impl std::panic::UnwindSafe for XorProvider {}
    impl std::panic::RefUnwindSafe for XorProvider {}

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
}

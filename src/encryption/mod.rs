// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

//! Block-level encryption at rest.
//!
//! This module defines the [`EncryptionProvider`] trait for pluggable
//! block-level encryption and, behind the `encryption` feature, ships a
//! ready-to-use `Aes256GcmProvider` implementation.
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
//! `Aes256GcmProvider` Block I/O code path below with the
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

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

// Top-level re-exports so callers can spell `crate::encryption::
// encrypt_block` / `decrypt_block` / `DecryptedBlock` /
// `KeyChain` / `DecryptError` directly, instead of paging
// through the submodule path each time. Matches the surface
// shape #251's design discussion settled on.
#[cfg(all(feature = "encryption", zstd_any))]
pub use block::{
    DecryptedBlock, EncryptedBlockMetadata, decrypt_block, encrypt_block,
    parse_encrypted_block_metadata, reconstruct_block_aad,
};
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
    Send + Sync + core::panic::UnwindSafe + core::panic::RefUnwindSafe
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
    /// `Aes256GcmProvider`) override it to `true`.
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
    /// `Aes256GcmProvider`) override this.
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
    ///
    /// This and the two fields below form the AAD-bound block state, read only by
    /// `encrypt_block_aad` / `decrypt_block_aad`. Those entry points are
    /// `zstd_any`-gated (they wrap the zstd `SkippableFrame`), so without zstd the
    /// provider cannot do block AAD and these fields are unused — gate them with it.
    #[cfg(zstd_any)]
    key_chain: crate::encryption::key_chain::StaticKeyChain,
    /// Active key epoch sealed into every block's `MetadataFrame`.
    #[cfg(zstd_any)]
    key_epoch: u8,
    /// AEAD suite tag (AES-256-GCM) mirrored into the AAD.
    #[cfg(zstd_any)]
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
            #[cfg(zstd_any)]
            key_chain: crate::encryption::key_chain::StaticKeyChain::new().with_key(0, *key),
            #[cfg(zstd_any)]
            key_epoch: 0,
            #[cfg(zstd_any)]
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

/// Current process id for fork detection. Under `std` this is the real PID;
/// under `no_std` there is no process/fork concept, so it is a constant — the
/// PID never "changes", the reseed branch never fires, and the RNG behaves as
/// a plain thread-local CSPRNG.
#[cfg(feature = "encryption")]
fn process_pid() -> u32 {
    #[cfg(feature = "std")]
    {
        std::process::id()
    }
    #[cfg(not(feature = "std"))]
    {
        0
    }
}

/// Thread-local CSPRNG wrapper with fork-aware PID tracking.
///
/// On each access, compares the stored PID with [`process_pid`]. If they
/// differ (i.e. the process was forked), the RNG is reseeded from the OS RNG
/// to avoid nonce reuse across processes.
#[cfg(feature = "encryption")]
struct ForkAwareRng {
    pid: core::cell::Cell<u32>,
    rng: core::cell::RefCell<rand_chacha::ChaCha20Rng>,
}

#[cfg(feature = "encryption")]
impl ForkAwareRng {
    fn new() -> Self {
        Self {
            pid: core::cell::Cell::new(process_pid()),
            rng: core::cell::RefCell::new(new_chacha_rng()),
        }
    }

    fn with_rng<R>(&self, f: impl FnOnce(&mut rand_chacha::ChaCha20Rng) -> R) -> R {
        let mut rng_ref = self.rng.borrow_mut();
        let current_pid = process_pid();
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
mod tests;

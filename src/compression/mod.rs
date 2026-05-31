// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

#[cfg(feature = "zstd")]
mod zstd_backend;

use crate::coding::{Decode, Encode};
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

#[cfg(zstd_any)]
use std::sync::Arc;

#[cfg(feature = "zstd")]
use once_cell::race::OnceBox;

/// Zstd compression backend operations.
///
/// Abstracts the zstd implementation so callsites are independent of the
/// underlying crate. Enabled by the `zstd` feature (pure Rust, no C
/// dependencies). Produces RFC 8878 compliant zstd frames.
#[cfg(zstd_any)]
pub trait CompressionProvider {
    /// Compress `data` at the given zstd level (1–22).
    fn compress(data: &[u8], level: i32) -> crate::Result<Vec<u8>>;

    /// Decompress a zstd frame, pre-allocating `capacity` bytes.
    fn decompress(data: &[u8], capacity: usize) -> crate::Result<Vec<u8>>;

    /// Compress `data` using a zstd dictionary.
    ///
    /// `dict_raw` may be either a finalized zstd dictionary (header bytes
    /// `37 A4 30 EC`, i.e. little-endian integer `0xEC30A437`, followed by
    /// entropy tables and content — produced by `zstd --train`; accessible
    /// via [`ZstdDictionary::raw`] for persistence and interop) or raw content
    /// bytes (bare bytes used as LZ77 history). The zstd backend in this crate
    /// accepts either representation.
    fn compress_with_dict(data: &[u8], level: i32, dict_raw: &[u8]) -> crate::Result<Vec<u8>>;

    /// Decompress a zstd frame that was compressed with a dictionary.
    ///
    /// `dict` provides the raw dictionary bytes and a 64-bit fingerprint used
    /// as the TLS cache key. Implementations cache the parsed decoder in
    /// thread-local storage keyed by that fingerprint to avoid re-parsing the
    /// dictionary on every call.
    fn decompress_with_dict(
        data: &[u8],
        dict: &ZstdDictionary,
        capacity: usize,
    ) -> crate::Result<Vec<u8>>;
}

/// The active zstd backend (pure Rust via `structured-zstd`).
#[cfg(feature = "zstd")]
pub type ZstdBackend = zstd_backend::ZstdProvider;

/// Pre-trained zstd dictionary for improved compression of small blocks.
///
/// Zstd dictionaries significantly improve compression ratios for blocks
/// in the 4–64 KiB range typical of LSM-trees, especially when data has
/// recurring patterns (e.g., structured keys, repeated prefixes,
/// JSON/MessagePack values).
///
/// The dictionary is identified by a 32-bit ID derived from its content
/// (truncated xxh3 hash). This ID is stored alongside compressed blocks
/// so readers can detect dictionary mismatches.
///
/// # Example
///
/// ```ignore
/// use lsm_tree::ZstdDictionary;
///
/// let samples: &[u8] = &training_data;
/// let dict = ZstdDictionary::new(samples);
/// ```
#[cfg(zstd_any)]
pub struct ZstdDictionary {
    /// Full 64-bit xxh3 hash used as the collision-resistant cache key for the
    /// thread-local `FrameDecoder`. The public `id() -> u32` method returns
    /// the lower 32 bits for external consumers.
    id: u64,
    raw: Arc<[u8]>,
    /// Lazily-parsed shared `DictionaryHandle` (Arc-backed inside structured-zstd).
    /// Populated on first decompress call and reused across all subsequent calls
    /// and all threads — eliminates the per-thread dictionary re-parse the TLS
    /// `FrameDecoder` cache used to incur on every miss.
    /// `OnceBox::get_or_try_init` guarantees one successful parse across
    /// racing threads via a single CAS on the slot pointer: the winner's
    /// `Box<DictionaryHandle>` becomes the stable `&T`, racing losers drop
    /// their unused `Box` allocations and read the winner's value on the
    /// next iteration. No auxiliary mutex is needed because the slot is
    /// lock-free; the only contention is the brief CAS window during the
    /// cold-start race. This keeps the `new()` constructor infallible AND
    /// preserves the single-parse contract.
    #[cfg(feature = "zstd")]
    prepared: Arc<OnceBox<structured_zstd::decoding::DictionaryHandle>>,
}

#[cfg(zstd_any)]
impl Clone for ZstdDictionary {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            raw: Arc::clone(&self.raw),
            #[cfg(feature = "zstd")]
            prepared: Arc::clone(&self.prepared),
        }
    }
}

/// Two dictionaries are equal when their full 64-bit xxh3 fingerprints agree.
/// Equality is defined by the 64-bit `id` field; hash collisions between
/// dictionaries with different raw bytes are theoretically possible but
/// extremely unlikely given the xxh3-64 collision probability.
#[cfg(zstd_any)]
impl PartialEq for ZstdDictionary {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

#[cfg(zstd_any)]
impl Eq for ZstdDictionary {}

#[cfg(zstd_any)]
impl ZstdDictionary {
    /// Creates a new dictionary handle from raw bytes.
    ///
    /// `raw` may be either:
    ///
    /// * A **finalized zstd dictionary** — bytes starting with the magic
    ///   `37 A4 30 EC` (as produced by `zstd --train`; accessible via
    ///   [`ZstdDictionary::raw`] for persistence and interop).  The backend
    ///   parses it with the full entropy-table decoder.
    /// * A **raw content dictionary** — arbitrary bytes used as LZ77 history
    ///   (no magic header).  Useful when the caller controls the training data
    ///   and does not need the full entropy-table overhead.
    ///
    /// Both forms are accepted by [`CompressionProvider::compress_with_dict`]
    /// and [`CompressionProvider::decompress_with_dict`].
    ///
    /// The handle stores the full 64-bit xxh3 hash of `raw` internally.
    /// [`Self::id`] returns the lower 32 bits for external consumers
    /// (config validation, frame header); `id64` (crate-internal) exposes the
    /// full fingerprint for use as a cache key.
    #[must_use]
    pub fn new(raw: &[u8]) -> Self {
        Self {
            id: compute_dict_id(raw),
            raw: Arc::from(raw),
            #[cfg(feature = "zstd")]
            prepared: Arc::new(OnceBox::new()),
        }
    }

    /// Returns the shared pre-parsed `DictionaryHandle`, parsing on first call
    /// and reusing the cached handle on every subsequent call (across threads).
    ///
    /// The handle wraps an `Arc<Dictionary>` inside structured-zstd, so cloning
    /// it is an atomic refcount bump — cheap enough to use on every decompress
    /// call. Frame decoders register the dictionary via
    /// `FrameDecoder::add_dict_handle`, which shares the same `Arc` rather than
    /// cloning the underlying entropy tables.
    ///
    /// On the first call we attempt finalized-dict parsing (magic bytes
    /// `37 A4 30 EC`); buffers without that prefix are treated as raw-content
    /// dictionaries via `Dictionary::from_raw_content` with the same synthetic
    /// 32-bit id formula the compressor uses (`xxh3(raw) as u32, clamped ≥ 1`).
    /// Parse failures are NOT cached — the next caller will retry — but the
    /// raw bytes are immutable for the dictionary's lifetime so a successful
    /// parse on one thread is permanent.
    #[cfg(feature = "zstd")]
    pub(crate) fn prepared_handle(
        &self,
    ) -> crate::Result<structured_zstd::decoding::DictionaryHandle> {
        use structured_zstd::decoding::{Dictionary, DictionaryHandle};
        const DICT_MAGIC: [u8; 4] = [0x37, 0xA4, 0x30, 0xEC];

        // `OnceBox::get_or_try_init` is the canonical single-init-
        // across-racers primitive: the closure runs at most once
        // globally regardless of contention; concurrent callers race on
        // a single CAS and the losers drop their unused `Box` while the
        // winners' value becomes the stable `&T`. The fast path (cached
        // value) is lock-free; the slow path runs exactly once per
        // `ZstdDictionary` lifetime even under heavy cold-start
        // contention. On a parse failure the OnceBox stays empty and
        // the next caller retries from scratch — preserving the
        // retry-on-failure contract pinned by the rejection test.
        // `Box::new(handle)` is the OnceBox API requirement: the slot
        // owns a heap allocation rather than the value inline, which
        // is what lets the type stay no-std + alloc compatible.
        self.prepared
            .get_or_try_init(|| -> crate::Result<Box<DictionaryHandle>> {
                let handle = if self.raw.starts_with(&DICT_MAGIC) {
                    DictionaryHandle::decode_dict(&self.raw)
                        .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?
                } else {
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "intentional: lower 32 bits of xxh3 as internal dict id (matches compressor)"
                    )]
                    let raw_content_id = (self.id as u32).max(1);
                    let dict = Dictionary::from_raw_content(raw_content_id, self.raw.to_vec())
                        .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;
                    DictionaryHandle::from_dictionary(dict)
                };
                Ok(Box::new(handle))
            })
            .cloned()
    }

    /// Returns a 32-bit fingerprint derived from the dictionary content.
    ///
    /// The fingerprint is the lower 32 bits of the xxh3-64 hash of the raw
    /// dictionary bytes.  It is stable for a given byte sequence and is
    /// intended for config validation (matching a `CompressionType::ZstdDict`
    /// `dict_id` field against the supplied `ZstdDictionary`) and external
    /// interop.
    ///
    /// The value may theoretically be `0` (probability ≈ 1/2³²). Backends
    /// that embed a dict ID in the zstd frame header (where id=0 is reserved)
    /// are responsible for clamping to at least 1 themselves.  Config
    /// validation is unaffected: both sides derive the ID from the same bytes
    /// and therefore agree even in the zero case.
    #[must_use]
    #[expect(
        clippy::cast_possible_truncation,
        reason = "intentional: public API returns 32-bit fingerprint"
    )]
    pub fn id(&self) -> u32 {
        self.id as u32
    }

    /// Returns the full 64-bit xxh3 fingerprint used as a collision-resistant
    /// cache key inside the TLS decoder.
    #[cfg(feature = "zstd")]
    #[must_use]
    pub(crate) fn id64(&self) -> u64 {
        self.id
    }

    /// Returns the raw dictionary bytes.
    #[must_use]
    pub fn raw(&self) -> &[u8] {
        &self.raw
    }
}

#[cfg(zstd_any)]
impl std::fmt::Debug for ZstdDictionary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZstdDictionary")
            .field("id", &format_args!("{:#018x}", self.id))
            .field("size", &self.raw.len())
            .finish_non_exhaustive() // `prepared` cache omitted — implementation detail
    }
}

/// Compute the full 64-bit xxh3 dictionary fingerprint.
///
/// The full 64-bit value is used as the collision-resistant cache key inside
/// the pure Rust backend's thread-local `FrameDecoder`. The public `id()`
/// method returns only the lower 32 bits for backward-compatible display.
#[cfg(zstd_any)]
fn compute_dict_id(raw: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(raw)
}

/// Compression algorithm to use
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CompressionType {
    /// No compression
    ///
    /// Not recommended.
    None,

    /// LZ4 compression
    ///
    /// Recommended for use cases with a focus
    /// on speed over compression ratio.
    #[cfg(feature = "lz4")]
    Lz4,

    /// Zstd compression
    ///
    /// Provides significantly better compression ratios than LZ4
    /// with reasonable decompression speed (~1.5 GB/s).
    ///
    /// Compression level can be adjusted (1-22, default 3):
    /// - 1 optimizes for speed
    /// - 3 is a good default (recommended)
    /// - 9+ optimizes for compression ratio
    ///
    /// Recommended for cold/archival data where compression ratio
    /// matters more than raw speed.
    // NOTE: Uses i32 (not a validated newtype) to match upstream's public API and
    // the zstd crate's compress(data, level: i32) signature. Validated levels are
    // produced by CompressionType::zstd() and Decode::decode_from; direct construction
    // via CompressionType::Zstd(level) must uphold the 1..=22 invariant.
    #[cfg(zstd_any)]
    Zstd(i32),

    /// Zstd compression with a pre-trained dictionary
    ///
    /// Uses a pre-trained dictionary for significantly better compression
    /// ratios on small blocks (4–64 KiB), especially when data has recurring
    /// patterns.
    ///
    /// `level` is the compression level (1–22), `dict_id` identifies the
    /// dictionary (truncated xxh3 hash of the dictionary bytes). The actual
    /// dictionary must be provided via [`Config`](crate::Config) or the relevant writer/reader.
    #[cfg(zstd_any)]
    ZstdDict {
        /// Compression level (1–22)
        level: i32,

        /// Dictionary fingerprint for mismatch detection
        dict_id: u32,
    },
}

impl CompressionType {
    /// Returns the zstd dictionary id encoded in this compression
    /// configuration, or `0` when no dictionary applies. Used to
    /// populate [`crate::table::block::BlockIdentity::dict_id`]
    /// from a `CompressionType` at the call site without each
    /// caller re-doing the `ZstdDict { dict_id, .. }` destructure.
    #[must_use]
    pub fn dict_id(&self) -> u32 {
        #[cfg(zstd_any)]
        if let Self::ZstdDict { dict_id, .. } = self {
            return *dict_id;
        }
        0
    }

    /// Validate a zstd compression level.
    ///
    /// Accepts levels in the range 1..=22 and returns an error otherwise.
    #[cfg(zstd_any)]
    fn validate_zstd_level(level: i32) -> crate::Result<()> {
        if !(1..=22).contains(&level) {
            // NOTE: Uses Error::other (not ErrorKind::InvalidInput) to match
            // upstream's error style and minimize fork divergence.
            return Err(crate::Error::Io(std::io::Error::other(format!(
                "invalid zstd compression level {level}, expected 1..=22"
            ))));
        }
        Ok(())
    }

    /// Create a zstd compression configuration with a checked level.
    ///
    /// This is the recommended way to construct a `CompressionType::Zstd`
    /// value, as it validates the level before any I/O occurs.
    ///
    /// # Errors
    ///
    /// Returns an error if `level` is outside the valid range `1..=22`.
    #[cfg(zstd_any)]
    pub fn zstd(level: i32) -> crate::Result<Self> {
        Self::validate_zstd_level(level)?;
        Ok(Self::Zstd(level))
    }

    /// Create a zstd dictionary compression configuration with checked level.
    ///
    /// The `dict_id` should come from [`ZstdDictionary::id`] to ensure
    /// consistency between the compression type stored on disk and the
    /// dictionary used at runtime.
    ///
    /// # Errors
    ///
    /// Returns an error if `level` is outside the valid range `1..=22`.
    #[cfg(zstd_any)]
    pub fn zstd_dict(level: i32, dict_id: u32) -> crate::Result<Self> {
        Self::validate_zstd_level(level)?;
        Ok(Self::ZstdDict { level, dict_id })
    }
}

impl std::fmt::Display for CompressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::None => "none",

                #[cfg(feature = "lz4")]
                Self::Lz4 => "lz4",

                #[cfg(zstd_any)]
                Self::Zstd(_) => "zstd",

                #[cfg(zstd_any)]
                Self::ZstdDict { .. } => "zstd+dict",
            }
        )
    }
}

impl Encode for CompressionType {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), crate::Error> {
        match self {
            Self::None => {
                writer.write_u8(0)?;
            }

            #[cfg(feature = "lz4")]
            Self::Lz4 => {
                writer.write_u8(1)?;
            }

            #[cfg(zstd_any)]
            Self::Zstd(level) => {
                writer.write_u8(3)?;
                // Catch invalid levels in debug builds (e.g. direct Zstd(999) construction).
                // Not a runtime error — encoding must stay infallible for encode_into_vec().
                debug_assert!(
                    (1..=22).contains(level),
                    "zstd level {level} outside valid range 1..=22"
                );
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "level range 1..=22 fits i8"
                )]
                writer.write_i8(*level as i8)?;
            }

            #[cfg(zstd_any)]
            Self::ZstdDict { level, dict_id } => {
                writer.write_u8(4)?;
                debug_assert!(
                    (1..=22).contains(level),
                    "zstd level {level} outside valid range 1..=22"
                );
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "level range 1..=22 fits i8"
                )]
                writer.write_i8(*level as i8)?;
                byteorder::WriteBytesExt::write_u32::<byteorder::LittleEndian>(writer, *dict_id)?;
            }
        }

        Ok(())
    }
}

impl Decode for CompressionType {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, crate::Error> {
        let tag = reader.read_u8()?;

        match tag {
            0 => Ok(Self::None),

            #[cfg(feature = "lz4")]
            1 => Ok(Self::Lz4),

            #[cfg(zstd_any)]
            3 => {
                let level = i32::from(reader.read_i8()?);
                // Reuse the shared validation logic to ensure consistent checks.
                Self::validate_zstd_level(level)?;
                Ok(Self::Zstd(level))
            }

            #[cfg(zstd_any)]
            4 => {
                let level = i32::from(reader.read_i8()?);
                Self::validate_zstd_level(level)?;
                let dict_id = byteorder::ReadBytesExt::read_u32::<byteorder::LittleEndian>(reader)?;
                Ok(Self::ZstdDict { level, dict_id })
            }

            tag => Err(crate::Error::InvalidTag(("CompressionType", tag))),
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::useless_vec,
    clippy::expect_used,
    reason = "test code"
)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn compression_serialize_none() {
        let serialized = CompressionType::None.encode_into_vec();
        assert_eq!(1, serialized.len());
    }

    #[cfg(feature = "lz4")]
    mod lz4 {
        use super::*;
        use test_log::test;

        #[test]
        fn compression_serialize_lz4() {
            let serialized = CompressionType::Lz4.encode_into_vec();
            assert_eq!(1, serialized.len());
        }
    }

    #[cfg(zstd_any)]
    mod zstd {
        use super::*;
        use test_log::test;

        #[test]
        fn compression_serialize_zstd() {
            let serialized = CompressionType::Zstd(3).encode_into_vec();
            assert_eq!(2, serialized.len());
        }

        #[test]
        fn compression_roundtrip_zstd() {
            for level in [1, 3, 9, 19] {
                let original = CompressionType::Zstd(level);
                let serialized = original.encode_into_vec();
                let decoded =
                    CompressionType::decode_from(&mut &serialized[..]).expect("decode failed");
                assert_eq!(original, decoded);
            }
        }

        #[test]
        fn compression_display_zstd() {
            assert_eq!(format!("{}", CompressionType::Zstd(3)), "zstd");
        }

        #[test]
        fn compression_zstd_rejects_invalid_level() {
            for invalid_level in [0, 23, -1, 200] {
                let result = CompressionType::zstd(invalid_level);
                assert!(result.is_err(), "level {invalid_level} should be rejected");
            }
        }

        #[test]
        fn compression_zstd_decode_rejects_invalid_level() {
            // Serialize a valid zstd value, then corrupt the level byte
            let valid = CompressionType::Zstd(3).encode_into_vec();
            assert_eq!(valid.len(), 2);

            // Flip level byte to 0 (out of range 1..=22)
            let corrupted = vec![valid[0], 0];
            let result = CompressionType::decode_from(&mut &corrupted[..]);
            assert!(result.is_err(), "level 0 should be rejected on decode");

            // Flip level byte to 23 (out of range)
            let corrupted = vec![valid[0], 23];
            let result = CompressionType::decode_from(&mut &corrupted[..]);
            assert!(result.is_err(), "level 23 should be rejected on decode");
        }

        #[test]
        fn compression_serialize_zstd_dict() {
            let serialized = CompressionType::ZstdDict {
                level: 3,
                dict_id: 0xDEAD_BEEF,
            }
            .encode_into_vec();
            // tag=4, level=3 as i8, dict_id=0xDEAD_BEEF in little-endian
            assert_eq!(serialized, [4, 3, 0xEF, 0xBE, 0xAD, 0xDE]);
        }

        #[test]
        fn compression_roundtrip_zstd_dict() {
            for level in [1, 3, 9, 19] {
                for dict_id in [0, 1, 0xDEAD_BEEF, u32::MAX] {
                    let original = CompressionType::ZstdDict { level, dict_id };
                    let serialized = original.encode_into_vec();
                    let decoded =
                        CompressionType::decode_from(&mut &serialized[..]).expect("decode failed");
                    assert_eq!(original, decoded);
                }
            }
        }

        #[test]
        fn compression_display_zstd_dict() {
            assert_eq!(
                format!(
                    "{}",
                    CompressionType::ZstdDict {
                        level: 3,
                        dict_id: 42
                    }
                ),
                "zstd+dict"
            );
        }

        #[test]
        fn compression_zstd_dict_rejects_invalid_level() {
            for invalid_level in [0, 23, -1, 200] {
                let result = CompressionType::zstd_dict(invalid_level, 42);
                assert!(result.is_err(), "level {invalid_level} should be rejected");
            }
        }

        #[test]
        fn compression_zstd_dict_decode_rejects_invalid_level() {
            // Serialize a valid ZstdDict, then corrupt the level byte to 0
            let mut buf = CompressionType::ZstdDict {
                level: 3,
                dict_id: 42,
            }
            .encode_into_vec();
            assert_eq!(buf[0], 4); // tag
            buf[1] = 0; // corrupt level to 0 (out of range 1..=22)

            let result = CompressionType::decode_from(&mut &buf[..]);
            assert!(result.is_err(), "level 0 should be rejected on decode");
        }

        #[test]
        fn zstd_dictionary_id_deterministic() {
            let dict_bytes = b"sample dictionary content for testing";
            let d1 = ZstdDictionary::new(dict_bytes);
            let d2 = ZstdDictionary::new(dict_bytes);
            assert_eq!(d1.id(), d2.id());
        }

        #[test]
        fn zstd_dictionary_different_content_different_id() {
            let d1 = ZstdDictionary::new(b"dictionary one");
            let d2 = ZstdDictionary::new(b"dictionary two");
            assert_ne!(d1.id(), d2.id());
        }

        #[test]
        fn zstd_dictionary_raw_roundtrip() {
            let raw = b"my dictionary bytes";
            let dict = ZstdDictionary::new(raw);
            assert_eq!(dict.raw(), raw);
        }

        #[test]
        fn zstd_dictionary_debug_format() {
            let dict = ZstdDictionary::new(b"test");
            let debug = format!("{dict:?}");
            assert!(debug.contains("ZstdDictionary"));
            assert!(debug.contains("size: 4"));
        }

        // --- prepared_handle: pre-parsed `DictionaryHandle` cache ---
        //
        // The whole point of #232: parse the dictionary ONCE per
        // `ZstdDictionary` instance and reuse the Arc-backed handle on every
        // subsequent decompress call, across all threads. The tests below
        // pin the contract: success / memoization / shared-OnceCell-across-
        // clones / both finalized + raw-content paths / error surfacing.

        #[cfg(feature = "zstd")]
        #[test]
        fn prepared_handle_raw_content_dict_parses_and_memoizes() {
            // Raw-content path: no magic prefix. structured-zstd builds a
            // `Dictionary` from the bytes treated as LZ77 history. First
            // call parses; second call must hit the OnceCell cache and
            // return a handle that compares-equal to the first.
            let dict = ZstdDictionary::new(b"raw-content training bytes here");
            let h1 = dict
                .prepared_handle()
                .expect("first call must parse raw-content dict");
            let h2 = dict
                .prepared_handle()
                .expect("second call must hit the cache");
            assert_eq!(
                h1.id(),
                h2.id(),
                "cached handle must report the same dict id"
            );
        }

        #[cfg(feature = "zstd")]
        #[test]
        fn prepared_handle_rejects_corrupted_finalized_magic() {
            // Bytes that LOOK like a finalized dict (magic prefix matches)
            // but are otherwise malformed must surface a parse error
            // through `prepared_handle` rather than panicking. The OnceCell
            // must NOT be populated with anything on failure — otherwise a
            // future caller would skip the (now-deterministically-failing)
            // parse and silently fall back to a stale cached value, breaking
            // the retry-on-failure contract.
            let mut bad = vec![0x37, 0xA4, 0x30, 0xEC]; // valid magic
            bad.extend_from_slice(&[0xFF; 16]); // garbage payload
            let dict = ZstdDictionary::new(&bad);
            let result = dict.prepared_handle();
            assert!(
                result.is_err(),
                "corrupted finalized dict must surface parse error",
            );
            assert!(
                dict.prepared.get().is_none(),
                "failed parse must NOT populate the OnceCell — retry-on-failure contract",
            );
        }

        #[cfg(feature = "zstd")]
        #[test]
        fn prepared_handle_shared_across_clones() {
            // `ZstdDictionary::clone` shares the inner `Arc<OnceCell<…>>`.
            // Parsing through one clone must be visible to the other —
            // otherwise each clone would re-parse independently, defeating
            // the purpose of the cache when dictionaries are distributed
            // across threads via clones.
            let dict_a = ZstdDictionary::new(b"shared dict bytes for clone test");
            let dict_b = dict_a.clone();

            let _ = dict_a
                .prepared_handle()
                .expect("parse via dict_a must succeed");
            // After dict_a parsed, dict_b's OnceCell (same Arc) must be
            // populated. We cannot directly observe "did not re-parse"
            // without instrumentation, but we can assert the cached
            // handle round-trips through dict_b and reports the same id.
            let h_b = dict_b
                .prepared_handle()
                .expect("dict_b must see cached handle");
            assert_eq!(h_b.id(), dict_a.id());
            // Cross-check OnceCell state directly: it is .get()-readable
            // from both clones.
            assert!(
                dict_b.prepared.get().is_some(),
                "OnceCell must be populated on dict_b after dict_a parsed",
            );
        }

        #[cfg(feature = "zstd")]
        #[test]
        fn prepared_handle_is_lazy_and_populated_after_first_call() {
            // The cache contract is lazy-init: `ZstdDictionary::new` must
            // NOT eagerly parse, and the OnceCell must transition from
            // `None` to `Some(_)` precisely on the first `prepared_handle`
            // call. This pins both halves of the contract — a regression
            // either way (eager parse OR no caching) lights up the assert.
            //
            // The end-to-end "real finalized dict parses successfully" path
            // is exercised by the existing `zstd_backend` round-trip suite
            // (which feeds real compressed frames through `decompress_with_dict`,
            // implicitly going through `prepared_handle`); duplicating the
            // dict-builder here would require linking the zstd dict trainer
            // and adds no coverage over what the backend tests already give.
            let dict = ZstdDictionary::new(b"laziness test bytes");
            assert!(
                dict.prepared.get().is_none(),
                "ZstdDictionary::new must NOT eagerly parse the dictionary",
            );
            let _ = dict.prepared_handle().expect("explicit parse must succeed");
            assert!(
                dict.prepared.get().is_some(),
                "OnceCell must be populated after first prepared_handle call",
            );
        }
    }
}

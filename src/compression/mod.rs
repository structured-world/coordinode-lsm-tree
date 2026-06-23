// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

#[cfg(feature = "zstd")]
mod zstd_backend;

use crate::coding::{Decode, Encode};
use crate::io::{Read, ReadBytesExt, Write, WriteBytesExt};

#[cfg(zstd_any)]
use alloc::sync::Arc;

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

    /// Compress `data`, additionally returning the inner zstd-block layout of
    /// the produced frame: the cumulative decompressed END offset of each inner
    /// block (a monotonically increasing prefix sum whose last entry equals the
    /// total decompressed size). A reader can binary-search this array to map a
    /// decompressed byte offset to the inner-block index covering it, then
    /// partial-decode only the covering blocks (see
    /// [`FrameDecoder::decode_blocks_partial`](structured_zstd::decoding::FrameDecoder::decode_blocks_partial)).
    ///
    /// The layout is returned **empty** when the frame is a single inner block
    /// (nothing to skip, so the caller persists no per-block table) or when the
    /// backend could not capture it. The compressed bytes are identical to
    /// [`compress`](Self::compress); only the side-channel layout differs.
    ///
    /// # Errors
    ///
    /// Returns an error if compression fails.
    fn compress_with_layout(data: &[u8], level: i32) -> crate::Result<(Vec<u8>, Vec<u32>)>;

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
                        .map_err(|e| crate::Error::Io(crate::io::Error::other(e.to_string())))?
                } else {
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "intentional: lower 32 bits of xxh3 as internal dict id (matches compressor)"
                    )]
                    let raw_content_id = (self.id as u32).max(1);
                    let dict = Dictionary::from_raw_content(raw_content_id, self.raw.to_vec())
                        .map_err(|e| crate::Error::Io(crate::io::Error::other(e.to_string())))?;
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
impl core::fmt::Debug for ZstdDictionary {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
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
            return Err(crate::Error::Io(crate::io::Error::other(format!(
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

impl core::fmt::Display for CompressionType {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
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
                crate::io::WriteBytesExt::write_u32::<crate::io::LittleEndian>(writer, *dict_id)?;
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
                let dict_id = crate::io::ReadBytesExt::read_u32::<crate::io::LittleEndian>(reader)?;
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
mod tests;

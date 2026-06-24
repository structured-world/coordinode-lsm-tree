// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! `RuntimeConfig` POD types — alloc-friendly (no std dependency).
//!
//! Lives in `runtime_config::types` so `no_std` consumers (block
//! decoders, format constants) can read configuration values
//! without pulling the `ArcSwap`-based handle in
//! `runtime_config::handle`.

/// Algorithm used for integrity checksums (block-level and per-KV).
///
/// Default: [`Self::Xxh3_64`] — fastest on modern SIMD hardware,
/// 64-bit collision space. Single primitive used across the crate
/// keeps the audit surface small.
///
/// [`Self::Crc32c`] is provided for callers who prefer the
/// mathematically-proven burst-error detection guarantees of CRC32C
/// (100% detection of 1-bit, 2-bit-in-32-bit-window, odd-count, and
/// burst ≤ 32-bit errors) — at the cost of slightly slower compute
/// on bulk paths and pulling a second hash dependency.
///
/// [`Self::Xxh3Low32`] truncates `XXH3-64` to its low 32 bits —
/// same compute cost as [`Self::Xxh3_64`], half the storage. Suited
/// for per-entry checksums where space matters more than collision
/// resistance.
///
/// **Intended live-migration semantic (effective once downstream
/// format PRs wire this into block I/O):** changing the configured
/// algorithm will affect subsequent writes only. Existing blocks
/// will self-describe via their own `checksum_type` byte (the
/// `BlockHeader` extension lands with the V5-batch per-KV /
/// manifest hardening PRs), so readers will handle mixed
/// algorithms in the same Tree transparently. Compaction will
/// rewrite source blocks per the current algorithm.
///
/// In this PR the algorithm is purely a configuration value —
/// existing block I/O still uses the current hardcoded path; no
/// `checksum_type` byte is written to disk yet. The discriminator
/// API ([`Self::wire_tag`] / [`Self::from_wire_tag`]) is provided
/// here so downstream wire-format PRs can encode the choice
/// without re-litigating the value assignment.
//
// no-std: pure data type — compiles under `--no-default-features --features alloc`.
//
// Naming: `Xxh3_64` keeps the underscore between the algorithm
// family (`Xxh3`) and the digest-bit-width suffix (`64`). This
// matches the upstream xxhash crate (`Xxh3_64Hasher`) and reads as
// "XXH3-64" rather than "version 364". `non_camel_case_types`
// allows underscores between digit groups, so this passes
// `clippy ... -D warnings` cleanly.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum ChecksumAlgorithm {
    /// XXH3-64. 8 bytes. ~50-100 GB/s on AVX2 / NEON. Default.
    #[default]
    Xxh3_64,

    /// Low 32 bits of XXH3-64. 4 bytes. Same compute as
    /// [`Self::Xxh3_64`], half the storage. Per-entry use case.
    Xxh3Low32,

    /// CRC32C (Castagnoli polynomial). 4 bytes. HW-accelerated on
    /// x86 (SSE 4.2 `_mm_crc32_*`) and ARM (CRC32 instructions),
    /// ~30 GB/s. Mathematically-proven burst-error detection.
    Crc32c,
}

impl ChecksumAlgorithm {
    /// Size in bytes of the stored digest for this algorithm.
    #[must_use]
    pub const fn digest_size(self) -> usize {
        match self {
            Self::Xxh3_64 => 8,
            Self::Xxh3Low32 | Self::Crc32c => 4,
        }
    }

    /// Whether this algorithm can actually compute a digest in this build.
    ///
    /// [`Self::Xxh3_64`] / [`Self::Xxh3Low32`] are always available;
    /// [`Self::Crc32c`] only when the `crc32c` cargo feature is enabled (its
    /// [`Self::compute`] returns `None` otherwise). Config validation uses this
    /// to reject selecting an uncompiled algorithm up front, rather than
    /// silently skipping digests or failing later at flush.
    #[must_use]
    pub const fn is_available(self) -> bool {
        match self {
            Self::Xxh3_64 | Self::Xxh3Low32 => true,
            #[cfg(feature = "crc32c")]
            Self::Crc32c => true,
            #[cfg(not(feature = "crc32c"))]
            Self::Crc32c => false,
        }
    }

    /// Compute the digest of `bytes` under this algorithm, returned as a
    /// `u64` (the low [`Self::digest_size`] bytes are the meaningful
    /// digest; wider algorithms fill more of the word).
    ///
    /// Returns `None` when the selected algorithm is not compiled into
    /// this build: [`Self::Crc32c`] needs the `crc32c` cargo feature.
    /// The default [`Self::Xxh3_64`] and [`Self::Xxh3Low32`] are always
    /// available. Callers on a fallible path translate `None` into
    /// [`crate::Error::FeatureUnsupported`]; the `Option` return keeps
    /// this method `core`-clean (no dependency on the std-bound crate
    /// error type) so no-std verify paths can call it.
    #[must_use]
    pub fn compute(self, bytes: &[u8]) -> Option<u64> {
        match self {
            Self::Xxh3_64 => Some(crate::hash::hash64(bytes)),
            // Low 32 bits of the same XXH3-64 digest: identical compute,
            // half the stored width (see `digest_size`).
            Self::Xxh3Low32 => Some(crate::hash::hash64(bytes) & 0xFFFF_FFFF),
            #[cfg(feature = "crc32c")]
            Self::Crc32c => Some(u64::from(crc32c::crc32c(bytes))),
            #[cfg(not(feature = "crc32c"))]
            Self::Crc32c => None,
        }
    }

    /// Streaming variant of [`Self::compute`]: digests the concatenation of
    /// `chunks` in order without the caller assembling a contiguous buffer.
    /// The result is identical to `compute(&chunks.concat())` — XXH3's
    /// incremental `update` and CRC32C's `crc32c_append` are both
    /// order-preserving over the same byte sequence — so digests stay
    /// stable whether computed one-shot or streamed.
    ///
    /// Used on the per-KV digest hot path (`kv_digest`) to avoid a fresh
    /// per-entry allocation. Returns `None` under the same not-compiled-in
    /// conditions as [`Self::compute`].
    // With the `crc32c` feature on, every arm returns `Some`, so clippy sees
    // the `Option` as superfluous — but in the no-`crc32c` build the `Crc32c`
    // arm returns `None`, so the wrap is required. (The public `compute` is
    // exempt via avoid-breaking-exported-api; this pub(crate) sibling is not.)
    #[cfg_attr(
        feature = "crc32c",
        expect(
            clippy::unnecessary_wraps,
            reason = "the Crc32c arm returns None when the crc32c feature is off"
        )
    )]
    pub(crate) fn compute_chunks(self, chunks: &[&[u8]]) -> Option<u64> {
        match self {
            Self::Xxh3_64 | Self::Xxh3Low32 => {
                let mut hasher = xxhash_rust::xxh3::Xxh3::new();
                for chunk in chunks {
                    hasher.update(chunk);
                }
                let digest = hasher.digest();
                Some(if matches!(self, Self::Xxh3Low32) {
                    digest & 0xFFFF_FFFF
                } else {
                    digest
                })
            }
            #[cfg(feature = "crc32c")]
            Self::Crc32c => {
                let mut crc = 0u32;
                for chunk in chunks {
                    crc = crc32c::crc32c_append(crc, chunk);
                }
                Some(u64::from(crc))
            }
            #[cfg(not(feature = "crc32c"))]
            Self::Crc32c => None,
        }
    }

    /// On-disk discriminator byte. Stable across format versions —
    /// new variants take fresh values; existing variants never
    /// change. Downstream `BlockHeader` extension (per #297 / #298
    /// design) carries this byte to dispatch verify on read.
    #[must_use]
    pub const fn wire_tag(self) -> u8 {
        match self {
            Self::Xxh3_64 => 0,
            Self::Xxh3Low32 => 1,
            Self::Crc32c => 2,
        }
    }

    /// Recover algorithm from the on-disk discriminator byte.
    /// Returns `None` for unknown tags so callers can reject
    /// forward-incompatible blocks explicitly rather than
    /// silently misinterpreting.
    #[must_use]
    pub const fn from_wire_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(Self::Xxh3_64),
            1 => Some(Self::Xxh3Low32),
            2 => Some(Self::Crc32c),
            _ => None,
        }
    }
}

/// Per-KV checksum policy: which data blocks get a per-entry checksum footer.
///
/// A footer-bearing block stays [`crate::table::block::BlockType::Data`] with
/// the `KV_CHECKSUM_FOOTER` block-header flag set — per-KV checking is a
/// transform layer, not a block role.
///
/// The block-level checksum covers the bytes as written to disk but
/// only as one digest over the whole block; the per-entry footer lets
/// a scrub localise which entry diverged. Catching a RAM bit-flip that
/// corrupts an entry WHILE it sits in the memtable, before the block
/// is compiled, additionally requires
/// [`KvChecksumComputePoint::AtInsert`], which computes the digest at
/// insert and verifies it at flush; the default
/// [`KvChecksumComputePoint::AtBlockCompile`] computes the digests when
/// the block is compiled. Default [`Self::Off`] emits no per-KV footer (the
/// `KV_CHECKSUM_FOOTER` bit clear), zero per-KV overhead.
///
/// Selection granularity:
/// - [`Self::Off`] / [`Self::AllLevels`] are unconditional.
/// - [`Self::PerLevel`] gates on the LSM level via a [`LevelMask`]
///   bitmask (hot tier L0/L1 typically), so cold archival levels
///   skip the per-entry overhead.
/// - [`Self::PerTable`] gates on an inclusive [`TableIdRange`], so a
///   specific table-id span (e.g. a compliance-sensitive table) opts
///   in independently of level.
///
/// Toggle takes effect on the next block compile; existing blocks
/// keep their original footer flag and read transparently. Compaction
/// rewrites source blocks per the current policy, so the choice
/// migrates live without downtime.
//
// no-std: pure data — compiles under `--no-default-features --features alloc`.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum KvChecksumPolicy {
    /// No per-KV checksums. Blocks carry no per-KV footer
    /// (the `KV_CHECKSUM_FOOTER` header bit clear).
    /// Default — zero compute, zero storage, zero memtable overhead.
    #[default]
    Off,

    /// Every data block on every level carries a per-entry checksum
    /// footer (the `KV_CHECKSUM_FOOTER` header flag set).
    AllLevels,

    /// Only data blocks on levels selected by the [`LevelMask`] carry
    /// the per-entry footer. Levels outside the mask emit a plain data
    /// block with the flag clear.
    PerLevel(LevelMask),

    /// Only data blocks whose owning table id falls in the inclusive
    /// [`TableIdRange`] carry the per-entry footer.
    PerTable(TableIdRange),
}

impl KvChecksumPolicy {
    /// Whether a data block written at `level` for table `table_id`
    /// must emit the per-entry checksum trailer under this policy.
    ///
    /// Drives whether the writer sets the `KV_CHECKSUM_FOOTER` header
    /// flag (and appends the footer) at block compile: `true` emits the
    /// footer, `false` emits a plain data block.
    #[must_use]
    pub const fn applies(self, level: u8, table_id: u64) -> bool {
        match self {
            Self::Off => false,
            Self::AllLevels => true,
            Self::PerLevel(mask) => mask.contains(level),
            Self::PerTable(range) => range.contains(table_id),
        }
    }
}

/// When the per-KV checksum digest is computed.
///
/// [`Self::AtBlockCompile`] (default) computes each entry's digest when the
/// data block is built at flush / compaction. This is the Pebble-parity
/// mode: no memtable overhead, but it does NOT cover a RAM bit-flip that
/// corrupts an entry while it sits in the memtable before the block is
/// compiled.
///
/// [`Self::AtInsert`] computes the digest at memtable insert and stores it in
/// the skiplist node's 4-byte reserved slot, fixing the entry's digest the
/// moment it enters RAM. At flush, the digest is re-derived over the entry's
/// current bytes and compared against the stored value: a divergence means the
/// entry was corrupted while it sat in the memtable (a RAM bit-flip during
/// residence), surfaced as [`crate::Error::MemtableKvChecksumMismatch`]. The
/// digest domain is logical, so the re-derivation at flush (and the digest the
/// block writer emits) equals the carried value when the bytes are intact.
///
/// `AtInsert` requires a 4-byte algorithm ([`ChecksumAlgorithm::Xxh3Low32`] /
/// [`ChecksumAlgorithm::Crc32c`]) so the digest fits the node's reserved slot
/// with zero size growth; selecting it with the 8-byte [`ChecksumAlgorithm::Xxh3_64`]
/// is rejected at config-validation time. Off-by-default `AtBlockCompile`
/// leaves the memtable node untouched (zero overhead).
//
// no-std: pure data — compiles under `--no-default-features --features alloc`.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum KvChecksumComputePoint {
    /// Compute at block compile (flush / compaction). Default. No memtable
    /// overhead; does not cover the memtable-residence window.
    #[default]
    AtBlockCompile,

    /// Compute at memtable insert and verify at flush (covers the RAM-residence
    /// window). Requires a 4-byte algorithm; the digest lives in the node's
    /// reserved slot, so enabling it adds no per-entry memtable bytes.
    AtInsert,
}

/// Page ECC-at-rest scheme: the correction algorithm matched to the
/// failure mode and storage budget.
///
/// ECC is off by default ([`RuntimeConfig::page_ecc`] = `false`); these
/// variants only take effect once it is enabled. They are ordered
/// cheapest-first — pick the lowest tier that covers the failure mode you
/// care about. The engine never defaults to a high-overhead scheme.
///
/// - [`Self::Secded`] — Hsiao SECDED, per word: single-bit correct +
///   double-bit detect (the default `(72, 64)` shape is 12.5% overhead). It
///   is the type-level default and is supported at [`EccGranularity::Block`]:
///   on a block checksum mismatch the SEC-DED trailer heals an isolated bit
///   flip before the heavier shard recovery. Page granularity is not yet
///   wired, so an enabled `Secded` config must use Block granularity. Matches
///   the dominant single-bit-rot failure mode of DRAM and disks.
/// - [`Self::Xor`] — one XOR parity shard over `data_shards` data shards
///   (RAID-5 equivalent): recovers one fully-lost shard. Overhead =
///   `1 / data_shards` (e.g. 10 → 10%). XOR is computed directly (no RS
///   engine), so it is far cheaper than Reed-Solomon for single-erasure.
/// - [`Self::ReedSolomon`] — `parity_shards` Reed-Solomon parity shards
///   over `data_shards`: recovers up to `parity_shards` lost shards or
///   bursts. Overhead = `parity_shards / data_shards`. For higher
///   tolerance only.
//
// no-std: pure data — compiles under `--no-default-features --features alloc`.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum EccScheme {
    /// Hsiao SECDED per word — single-bit correct, double-bit detect (the
    /// default `(72, 64)` shape is 12.5% overhead). The type-level default;
    /// supported when ECC is enabled at [`EccGranularity::Block`]. Page
    /// granularity is not yet wired for `Secded`.
    #[default]
    Secded,

    /// One XOR parity shard over `data_shards` shards (RAID-5):
    /// recovers one lost shard. Overhead = `1 / data_shards`.
    Xor {
        /// Number of data shards the block is split into. The single
        /// parity shard is their XOR; overhead is `1 / data_shards`.
        data_shards: u8,
    },

    /// `parity_shards` Reed-Solomon parity shards over `data_shards`:
    /// recovers up to `parity_shards` lost shards. Overhead =
    /// `parity_shards / data_shards`.
    ReedSolomon {
        /// Number of data shards the block is split into.
        data_shards: u8,
        /// Number of Reed-Solomon parity shards (max recoverable shard
        /// losses).
        parity_shards: u8,
    },
}

impl EccScheme {
    /// Shard layout `(data_shards, parity_shards)` for the shard-based
    /// schemes; `None` for [`Self::Secded`] (per-word Hamming, not
    /// shard-based). [`Self::Xor`] is single-parity (`parity_shards = 1`).
    #[must_use]
    pub const fn shard_params(self) -> Option<(usize, usize)> {
        match self {
            Self::Secded => None,
            Self::Xor { data_shards } => Some((data_shards as usize, 1)),
            Self::ReedSolomon {
                data_shards,
                parity_shards,
            } => Some((data_shards as usize, parity_shards as usize)),
        }
    }
}

/// Granularity at which Page ECC parity is computed.
///
/// Exactly one level is active per SST (never both). [`Self::Block`]
/// computes parity over the whole block payload (one parity region per
/// block). [`Self::Page`] computes parity per sector-aligned page,
/// matching the physical bit-rot / bad-sector unit.
//
// no-std: pure data — compiles under `--no-default-features --features alloc`.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum EccGranularity {
    /// One parity region over the whole block payload.
    #[default]
    Block,

    /// Per sector-aligned page parity (matches the physical failure unit).
    Page,
}

/// Fixed width of the per-SST `descriptor#page_ecc` value: `[kind,
/// data_shards, parity_shards, granularity]`.
pub const ECC_DESCRIPTOR_LEN: usize = 4;

/// Encodes the per-SST ECC descriptor value `[kind, data_shards,
/// parity_shards, granularity]`.
///
/// `None` (ECC off) encodes as all-zero. There is no legacy / implicit
/// scheme: `kind == 0` is the only "off" encoding and every enabled SST
/// records its scheme explicitly.
///
/// `kind`: 0 = off, 1 = `Secded`, 2 = `Xor`, 3 = `ReedSolomon`.
#[must_use]
pub fn ecc_descriptor_bytes(cfg: Option<(EccScheme, EccGranularity)>) -> [u8; ECC_DESCRIPTOR_LEN] {
    let Some((scheme, gran)) = cfg else {
        return [0; ECC_DESCRIPTOR_LEN];
    };
    let gran_byte = match gran {
        EccGranularity::Block => 0,
        EccGranularity::Page => 1,
    };
    match scheme {
        EccScheme::Secded => [1, 0, 0, gran_byte],
        EccScheme::Xor { data_shards } => [2, data_shards, 1, gran_byte],
        EccScheme::ReedSolomon {
            data_shards,
            parity_shards,
        } => [3, data_shards, parity_shards, gran_byte],
    }
}

/// Outcome of decoding a per-SST `descriptor#page_ecc` value.
///
/// The decoder is a faithful, lenient reader: it never hard-fails on a
/// 4-byte value (only a wrong length is unparseable). Anything that is not
/// canonical `Off` or a recognized scheme + granularity decodes to
/// [`Self::Unrecognized`] rather than an error, so the read path can apply
/// the three-state ECC contract: a recognized+applicable scheme is used
/// for recovery; an unrecognized one is a "typing" warning (data still
/// reads via its checksum, but ECC recovery is unavailable — recompaction
/// re-stamps the block with a supported scheme).
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum EccDescriptor {
    /// `kind == 0` with canonical all-zero reserved bytes: ECC is off.
    Off,

    /// A recognized scheme + granularity pair. Round-trips through
    /// [`ecc_descriptor_bytes`]. Whether it is *applicable* (i.e. the
    /// read path can size + recover its trailer) is decided downstream:
    /// [`EccScheme::Secded`] and [`EccGranularity::Page`] are recognized
    /// here but not yet applicable.
    Recognized(EccScheme, EccGranularity),

    /// A 4-byte value that is neither canonical `Off` nor a recognized
    /// scheme: unknown `kind`, unknown granularity byte, non-canonical
    /// reserved bytes, a zero shard count, or a non-canonical shard layout
    /// (e.g. `Xor` whose parity byte is not `1`, `ReedSolomon` with fewer
    /// than two parity shards). The read path treats this as ECC present
    /// but unusable — a warning, not a read failure.
    Unrecognized,
}

/// Decodes a per-SST ECC descriptor value written by
/// [`ecc_descriptor_bytes`].
///
/// # Errors
///
/// Returns [`crate::Error::InvalidTrailer`] only when the value is not
/// exactly [`ECC_DESCRIPTOR_LEN`] bytes (it is not a descriptor at all).
/// Any 4-byte value decodes to an [`EccDescriptor`] (possibly
/// [`EccDescriptor::Unrecognized`]); see that type for the three-state
/// contract.
pub fn ecc_descriptor_from_bytes(bytes: &[u8]) -> crate::Result<EccDescriptor> {
    let [kind, data_shards, parity_shards, gran_byte] =
        *<&[u8; ECC_DESCRIPTOR_LEN]>::try_from(bytes).map_err(|_| crate::Error::InvalidTrailer)?;
    if kind == 0 {
        // `Off` carries no scheme: the remaining three bytes are
        // reserved-zero. A non-zero reserved byte is a non-canonical /
        // corrupted descriptor — surface it as `Unrecognized` (a warning at
        // the read layer) rather than silently treating the SST as "ECC off".
        if data_shards != 0 || parity_shards != 0 || gran_byte != 0 {
            return Ok(EccDescriptor::Unrecognized);
        }
        return Ok(EccDescriptor::Off);
    }
    let granularity = match gran_byte {
        0 => EccGranularity::Block,
        1 => EccGranularity::Page,
        _ => return Ok(EccDescriptor::Unrecognized),
    };
    let scheme = match kind {
        1 => {
            // `Secded` is per-word Hamming with no shard layout: its shard
            // bytes are reserved-zero. Non-zero reserved bytes are
            // non-canonical → `Unrecognized`.
            if data_shards != 0 || parity_shards != 0 {
                return Ok(EccDescriptor::Unrecognized);
            }
            EccScheme::Secded
        }
        2 => {
            // Canonical `Xor` has exactly one parity shard (the descriptor
            // writer always emits parity byte `1`).
            if data_shards == 0 || parity_shards != 1 {
                return Ok(EccDescriptor::Unrecognized);
            }
            EccScheme::Xor { data_shards }
        }
        3 => {
            // Canonical `ReedSolomon` has at least two parity shards; a
            // single parity shard is expressed as `Xor`.
            if data_shards == 0 || parity_shards < 2 {
                return Ok(EccDescriptor::Unrecognized);
            }
            EccScheme::ReedSolomon {
                data_shards,
                parity_shards,
            }
        }
        _ => return Ok(EccDescriptor::Unrecognized),
    };
    Ok(EccDescriptor::Recognized(scheme, granularity))
}

/// Bitmask over LSM levels for [`KvChecksumPolicy::PerLevel`].
///
/// Bit `n` set means level `n` is selected. A `u8` covers levels
/// `0..=7`; the engine's default level count is 7, so one byte is
/// sufficient. Levels `>= 8` are never selected (their bit cannot be
/// represented), which is the safe default for a config that only
/// targets the hot tier.
//
// no-std: pure data — compiles under `--no-default-features --features alloc`.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct LevelMask(u8);

impl LevelMask {
    /// Empty mask — selects no levels.
    #[must_use]
    pub const fn none() -> Self {
        Self(0)
    }

    /// Build a mask from a raw bitmask byte (bit `n` = level `n`).
    #[must_use]
    pub const fn from_bits(bits: u8) -> Self {
        Self(bits)
    }

    /// The raw bitmask byte.
    #[must_use]
    pub const fn bits(self) -> u8 {
        self.0
    }

    /// Return a copy with `level` added to the selection. Levels
    /// `>= 8` are out of range for the `u8` mask and are ignored
    /// (the returned mask is unchanged).
    #[must_use]
    pub const fn with_level(self, level: u8) -> Self {
        if level < 8 {
            Self(self.0 | (1u8 << level))
        } else {
            self
        }
    }

    /// Whether `level` is selected by this mask. Levels `>= 8` are
    /// never selected.
    #[must_use]
    pub const fn contains(self, level: u8) -> bool {
        level < 8 && (self.0 & (1u8 << level)) != 0
    }
}

/// Inclusive table-id range for [`KvChecksumPolicy::PerTable`].
///
/// Both endpoints are inclusive: `TableIdRange { start, end }` selects
/// every `table_id` with `start <= table_id <= end`. A range with
/// `start > end` selects nothing.
//
// no-std: pure data — compiles under `--no-default-features --features alloc`.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct TableIdRange {
    /// Inclusive lower bound.
    pub start: u64,
    /// Inclusive upper bound.
    pub end: u64,
}

impl TableIdRange {
    /// Build an inclusive range. `start > end` yields an empty range
    /// (selects nothing) rather than panicking.
    #[must_use]
    pub const fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }

    /// Whether `table_id` falls within `[start, end]` inclusive.
    #[must_use]
    pub const fn contains(self, table_id: u64) -> bool {
        self.start <= table_id && table_id <= self.end
    }
}

/// Runtime-toggleable configuration.
///
/// Fields here can change while the Tree is open via
/// [`crate::Tree::update_runtime_config`]. Each field documents
/// when its change takes effect (next write vs next compaction vs
/// next manifest commit). Read paths are config-independent — each
/// block / manifest self-describes via its own header, so existing
/// on-disk data stays in its original format and reads
/// transparently regardless of current `RuntimeConfig`.
///
/// Downstream features (#297 manifest hardening, #298 per-KV
/// protection, #224 `scan_since_seqno`) extend this struct with
/// their own fields. The fields listed below are the minimal
/// foundation set; future PRs add fields additively.
///
/// `#[non_exhaustive]` so adding fields downstream is not a
/// breaking change to struct-literal construction — callers use
/// [`Self::default`] + builder-style updates.
//
// no-std: pure data — compiles under `--no-default-features --features alloc`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
#[expect(
    clippy::struct_excessive_bools,
    reason = "config POD: each bool is an independent on/off feature toggle \
              (manifest mirror, manifest per-KV checksums, page ECC, seqno-in-index); \
              folding them into enums would obscure the per-feature config contract"
)]
pub struct RuntimeConfig {
    /// Algorithm for Block-level integrity (the checksum in
    /// `BlockHeader`). Default: [`ChecksumAlgorithm::Xxh3_64`].
    ///
    /// Toggle takes effect on the next block write. Existing blocks
    /// carry their own `checksum_type` byte (added by downstream
    /// `BlockHeader` extension PR) so readers dispatch per-block.
    /// Compaction rewrites source blocks per the current algorithm.
    pub block_checksum_algo: ChecksumAlgorithm,

    /// Algorithm for the per-KV checksum footer carried by data blocks
    /// with the `KV_CHECKSUM_FOOTER` header flag set. Independent from
    /// [`Self::block_checksum_algo`]. Default:
    /// [`ChecksumAlgorithm::Xxh3_64`].
    pub kv_checksum_algo: ChecksumAlgorithm,

    /// Which data blocks emit a per-entry checksum footer (recorded via
    /// the `KV_CHECKSUM_FOOTER` header flag). Default
    /// [`KvChecksumPolicy::Off`] keeps blocks free of the per-KV footer
    /// (the `KV_CHECKSUM_FOOTER` bit clear).
    ///
    /// Toggle takes effect on the next block compile; existing blocks
    /// keep their original footer flag and read transparently.
    /// Compaction migrates source blocks to the current policy over
    /// time. See [`KvChecksumPolicy`] for selection granularity.
    pub kv_checksums: KvChecksumPolicy,

    /// When the per-KV digest is computed:
    /// [`KvChecksumComputePoint::AtBlockCompile`] (default) at flush /
    /// compaction, or [`KvChecksumComputePoint::AtInsert`] at memtable
    /// insert (the memtable-residence-window mode that catches a RAM
    /// bit-flip while a record sits in the memtable). `AtInsert` requires a
    /// 4-byte [`Self::kv_checksum_algo`] (`Xxh3Low32` / `Crc32c`) that is
    /// compiled into the build; `RuntimeConfigHandle::try_update` (behind
    /// [`crate::Tree::update_runtime_config`]) rejects `AtInsert` with the
    /// 8-byte `Xxh3_64`, or with an uncompiled algorithm, via a typed error.
    pub kv_checksum_compute_point: KvChecksumComputePoint,

    /// When `true`, every manifest write reserves a 4 KiB region at
    /// file offset 0 and, after writing the tail footer Block,
    /// copies the footer Block bytes into that head region. On read,
    /// the reader tries the tail footer first; if XXH3 verification
    /// fails (partial write / tail bit-rot), it falls back to the
    /// head mirror. Disabling this leaves the head region as zeros
    /// — readers detect the absence by the zero magic and skip the
    /// fallback path.
    ///
    /// Default: `true`. The 4 KiB head region is reserved
    /// unconditionally by the writer so the file layout stays
    /// stable regardless of this setting; flipping the flag only
    /// controls whether the writer copies the tail footer Block
    /// into that region. Cost when on: 4 KiB of meaningful mirror
    /// bytes per manifest file (negligible — manifests are KB-MB
    /// scale). Cost when off: the same 4 KiB stays zero-padded,
    /// and the reader has no fallback if the tail footer fails
    /// verification (partial mid-update or tail bit-rot is
    /// unrecoverable).
    ///
    /// Toggle takes effect on the next manifest write. Existing
    /// manifests are read transparently regardless of current
    /// setting (the head region's contents are self-describing).
    pub manifest_footer_mirror: bool,

    /// When `true`, the manifest's `tables` / `blob_files` /
    /// `blob_gc_stats` section Blocks use [`crate::table::block::
    /// BlockType::Manifest`] payloads that carry per-entry XXH3
    /// checksums in addition to the Block-level checksum, matching
    /// `RocksDB` MANIFEST's per-record CRC32 granularity. When `false`,
    /// only the Block-level checksum is used — bit-rot in any byte
    /// of a section corrupts the whole section's recovery rather
    /// than a single entry.
    ///
    /// Default: `true` (Benchmark Symmetry Invariant: match the
    /// granularity `RocksDB` ships out-of-the-box, so apples-to-apples
    /// benchmarks don't pay for an opt-in).
    ///
    /// **Wiring status:** the toggle is plumbed through
    /// `RuntimeConfig` only. It is NOT yet persisted into the
    /// manifest footer payload (footer carries
    /// `manifest_layout_version`, `flags` for the mirror bit, and
    /// the TOC; no slot for this flag yet). The per-entry checksum
    /// framing this would control lands in a separate change that
    /// sets the `KV_CHECKSUM_FOOTER` header flag on manifest section
    /// blocks AND extends the footer flags / payload to record
    /// the toggle. Until then changing this flag has no on-disk
    /// effect — the field is surfaced for forward-compat ergonomics
    /// in the `RuntimeConfig` builder, not for any current
    /// behaviour change.
    pub manifest_kv_checksums: bool,

    /// Global default for Page ECC (Reed-Solomon `(4, 2)` parity
    /// trailer). Intended to apply to ALL Blocks (data, index,
    /// filter, manifest) once the data-block writer path consumes
    /// `RuntimeConfig`. Other `_ecc_override` fields fall back to
    /// this value when `None`.
    ///
    /// **Wiring status (current release):** consulted only by
    /// `manifest_blocks::{writer, reader}` when picking the
    /// `BlockTransform` variant for manifest sections / footer
    /// Blocks. Data-block ECC is still gated by
    /// [`crate::Config::page_ecc`] (compile-time tree config),
    /// because the SST writer path doesn't yet thread a
    /// `RuntimeConfigHandle` through every Block emission point.
    /// Toggling this field via [`crate::Tree::update_runtime_config`]
    /// affects the *next* manifest write only; data Blocks remain
    /// on the Tree's static `Config::page_ecc`. The wiring through
    /// SST writers lands in a follow-up that introduces
    /// per-emission `RuntimeConfig` snapshots.
    ///
    /// Default: `false` (explicit opt-in; users pay nothing unless
    /// they enable it). When `true`, every Block written through a
    /// runtime-aware path carries the parity trailer and gains
    /// single-block bit-flip recovery on read at the cost of
    /// `≈ N/2 + small overhead` storage and negligible compute.
    pub page_ecc: bool,

    /// Per-scope override for data Blocks: `None` inherits
    /// [`Self::page_ecc`]; `Some(false)` disables ECC on data Blocks
    /// even when [`Self::page_ecc`] is `true` (useful when callers
    /// want manifest ECC for critical metadata but not the data-block
    /// overhead). `Some(true)` is the inverse.
    pub data_block_ecc_override: Option<bool>,

    /// Per-scope override for the per-KV checksum footer region within
    /// footer-bearing data Blocks (#298). `None` inherits
    /// [`Self::page_ecc`]; `Some(false)` keeps the kv-checksum
    /// region outside the parity calculation even when global ECC
    /// is enabled (useful when the per-KV checksums themselves are
    /// considered sufficient and ECC is wanted only on value bytes).
    pub kv_checksums_ecc_override: Option<bool>,

    /// Page ECC scheme used when ECC is enabled ([`Self::page_ecc`] or a
    /// per-scope override is `true`). The type-level default
    /// [`EccScheme::Secded`] is a placeholder valid only while ECC is OFF:
    /// it is not wired yet (#255), so turning ECC on while this is still
    /// `Secded` is rejected — an enabled config must set an explicit
    /// shard-based scheme ([`EccScheme::Xor`] / [`EccScheme::ReedSolomon`]).
    /// The scheme is recorded per-SST so the reader can re-derive the
    /// parity layout (existing SSTs keep the scheme they were written with;
    /// compaction migrates over time).
    pub ecc_scheme: EccScheme,

    /// Granularity at which Page ECC parity is computed when ECC is on.
    /// Default [`EccGranularity::Block`]. Exactly one level is active per
    /// SST; recorded per-SST alongside [`Self::ecc_scheme`].
    pub ecc_granularity: EccGranularity,

    /// When `true`, a read that recovers a block from its Page-ECC parity, and
    /// confirms the on-disk fault is persistent (via a cache-bypassing re-read),
    /// flags the owning SST for a healing recompaction — the corrected data is
    /// rewritten into a fresh SST so the latent fault is not re-corrected on
    /// every subsequent read. Drive the rewrite with
    /// [`compaction::EccHeal`](crate::compaction::EccHeal) over
    /// [`Tree::heal_hints`](crate::Tree::heal_hints), leader-only in a clustered
    /// deployment (compaction is a background mutation).
    ///
    /// Default `false`: correction-on-read still happens whenever ECC is
    /// enabled; only the rewrite *scheduling* is opt-in, because it costs
    /// compaction I/O. Toggling takes effect immediately for subsequent reads.
    pub auto_heal: bool,

    /// Per-block seqno bounds for `scan_since_seqno` block-skip (#224). When
    /// `true`, SSTs written by the next flush / compaction emit the optional
    /// parallel `seqno_bounds` section, recording each data block's
    /// `seqno_min` / `seqno_max` keyed by its file offset. A seqno-scoped scan
    /// then skips any block whose bounds cannot overlap the target window
    /// without reading it.
    ///
    /// Default `false`: no section is emitted (zero extra bytes) and
    /// `scan_since_seqno` falls back to a per-entry filter. The index entries
    /// are byte-identical regardless of this setting, so a point read pays
    /// nothing either way, and a tree mixing SSTs with and without the section
    /// reads correctly (a missing section simply means full-filter scan for
    /// that SST).
    ///
    /// Toggle takes effect on the next compaction / flush; existing SSTs keep
    /// whatever they were written with. Compaction migrates source SSTs to the
    /// current setting over time.
    pub seqno_in_index: bool,

    /// Whether new SSTs carry the optional `zone_map` section: per-data-block
    /// per-column statistics (for row blocks, the block's key min / max + row
    /// count) that let a predicate scan skip blocks that cannot match without
    /// reading them. Kept parallel to the index, so a point read pays nothing.
    ///
    /// Explicit opt-in, default `false` (no section emitted). Takes effect on
    /// the next compaction / flush; existing SSTs keep whatever they were
    /// written with, and a missing section just means no block-skip for that SST.
    pub zone_map: bool,

    /// Whether new SSTs store their data column-organized (a PAX row-group per
    /// block) instead of row-major. The entry's intrinsic fields (user key,
    /// seqno, value type, value) become separate per-block columns, so a scan
    /// can read keys without decoding values and each field compresses with a
    /// structure-aware codec. Reads reconstruct the exact entries.
    ///
    /// Explicit opt-in, default `false` (row-major). Takes effect on the next
    /// compaction / flush; existing SSTs keep their original layout, and the
    /// reader picks the layout per block from the block type, so a tree can hold
    /// both row and columnar SSTs at once.
    pub columnar: bool,

    /// Per-level row-level delete strategy for columnar segments: copy-on-write
    /// (rewrite and drop deleted rows), merge-on-read (record deletes in the
    /// positional delete-bitmap and defer the rewrite), or adaptive (merge-on-read
    /// until a purge threshold). Read-heavy lower levels can favour copy-on-write
    /// while write-heavy upper levels favour merge-on-read; changing the policy
    /// migrates segments at the next compaction. Default: adaptive (5% purge
    /// threshold) in every level.
    pub delete_strategy: crate::config::DeleteStrategyPolicy,

    /// Index-size threshold (bytes) at or below which an SST's block index
    /// is written single-level; above it the index spills to a two-level
    /// (partitioned) layout. A single-level index is one block reached by a
    /// single lookup, so a point read costs one index level instead of two.
    /// On hot levels the block is pinned resident; on cold levels (per the
    /// index-block pinning policy) it is paged through the shared block
    /// cache, so a high threshold does not pin unbounded index RAM — the
    /// cold-level single-level index is evictable just like a two-level
    /// index's bottom partitions, but cheaper (one cache load + one
    /// iterator versus two levels).
    ///
    /// Default 4 MiB keeps SSTs up to a few-hundred-MB single-level (where
    /// single-level beats two-level on point reads at every measured size
    /// up to 1M keys) while genuinely huge indexes still partition. Takes
    /// effect on the next flush / compaction; existing SSTs keep their
    /// original layout (each SST self-describes it). `0` forces always-
    /// partition (spill on the first index entry).
    pub index_partition_spill_threshold: u64,

    /// When `true`, the writer clears per-file copy-on-write
    /// ([`crate::fs::Fs::try_disable_cow`]) on each newly created SST / blob
    /// file when the backing filesystem is copy-on-write (Btrfs). SST files
    /// are write-once-then-read-only, so `CoW` gives them no benefit while
    /// imposing a fragmentation penalty (~20% write throughput on Btrfs);
    /// clearing it recovers the ext4-equivalent baseline.
    ///
    /// Default `true`: correct out of the box on any filesystem (a no-op on
    /// non-`CoW` filesystems, where [`crate::fs::Fs::try_disable_cow`] does
    /// nothing). Set `false` to preserve `CoW`, e.g. when running on a Btrfs
    /// subvolume whose FS-level snapshots depend on per-file `CoW`.
    ///
    /// Takes effect on the next SST creation; existing files are not
    /// re-flagged (the inode flag only applies to a still-empty file).
    pub disable_cow_on_sst_files: bool,

    /// When `true`, [`crate::AbstractTree::create_checkpoint`] clones SST /
    /// blob files via reflink ([`crate::fs::Fs::reflink_file`], i.e. `FICLONE`
    /// / `clonefile`) when the filesystem supports it, falling back to a hard
    /// link otherwise. Reflink gives an independent inode (modifying the
    /// checkpoint never touches the original) with no max-links-per-inode
    /// constraint, while still sharing data blocks copy-on-write for O(1)
    /// cost.
    ///
    /// Default `true`. On a filesystem without reflink the checkpoint driver
    /// uses the existing hard-link path, so the setting is a no-op there.
    pub use_reflink_for_checkpoint: bool,

    /// Opt-in write admission control. When `false` (default) the tree never
    /// computes an admission state and the gated write path always admits, so
    /// there is zero overhead and behaviour is unchanged. When `true`, the
    /// engine maintains a cached read-only predicate (see
    /// [`crate::AbstractTree::write_admission`]) driven by [`Self::storage_limit_bytes`]
    /// (and, once wired, the filesystem free-space probe), and the gated write
    /// path ([`crate::AbstractTree::try_insert`] / batch apply) declines writes with
    /// [`crate::Error::StorageFull`] while the tree is over budget.
    ///
    /// Toggle takes effect on the next admission check.
    pub storage_admission_check: bool,

    /// Soft byte budget for the tree's live on-disk footprint, enforced by the
    /// admission gate when [`Self::storage_admission_check`] is `true`. `None`
    /// (default) means unbounded. Live-toggleable: raising it (or a compaction
    /// reclaiming space) clears read-only on the next check with no restart,
    /// because the predicate is computed, not latched.
    ///
    /// Internal flush / compaction are never gated by this budget — a reserved
    /// headroom band is always kept available so the engine can flush the
    /// active memtable and run a space-reclaiming compaction even at the limit.
    /// That is what makes the budget a safe soft limit rather than a hard wall.
    pub storage_limit_bytes: Option<u64>,

    /// Opt-in tight-space compaction. When `false` (default) a merge whose
    /// transient output cannot fit the free space (and has no fitting smaller
    /// subset) is simply skipped, leaving the level un-compacted until space
    /// frees up elsewhere. When `true`, such a merge instead runs an
    /// incremental, resumable reclaim: it rewrites the run one key-range slice
    /// at a time, installs each finished slice, and punches the consumed input
    /// extents ([`crate::fs::Fs::punch_hole`]) so the peak transient footprint
    /// is one slice rather than the whole run. This lets the engine compact even
    /// when free space is smaller than the data being rewritten.
    ///
    /// Engages only on backends that advertise the punch-hole capability and
    /// only for inputs not pinned by an older snapshot; otherwise the merge
    /// falls back to the skip behaviour. Has no effect unless
    /// [`Self::storage_admission_check`] is also `true` (the admission gate is
    /// what detects the no-fit condition that triggers tight mode).
    ///
    /// Toggle takes effect on the next compaction cycle.
    pub tight_space_compaction: bool,

    /// Opt-in write-backpressure thresholds. With every field `None` (default)
    /// the verdict is always [`crate::Backpressure::None`] and the write path is
    /// byte-for-byte unchanged. When set,
    /// [`crate::AbstractTree::write_backpressure`] reports a `Slowdown` / `Stop`
    /// verdict from the live L0 table count and pending compaction bytes, which
    /// the caller honours in its own write loop (the engine never blocks
    /// internally — it does not own the compaction that would drain the debt).
    /// Live-toggleable: the verdict is computed, not latched, so it clears as
    /// soon as compaction drains below the thresholds.
    pub backpressure: crate::backpressure::BackpressureThresholds,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            block_checksum_algo: ChecksumAlgorithm::Xxh3_64,
            kv_checksum_algo: ChecksumAlgorithm::Xxh3_64,
            kv_checksums: KvChecksumPolicy::Off,
            kv_checksum_compute_point: KvChecksumComputePoint::AtBlockCompile,
            manifest_footer_mirror: true,
            manifest_kv_checksums: true,
            page_ecc: false,
            data_block_ecc_override: None,
            kv_checksums_ecc_override: None,
            ecc_scheme: EccScheme::Secded,
            ecc_granularity: EccGranularity::Block,
            auto_heal: false,
            seqno_in_index: false,
            zone_map: false,
            columnar: false,
            delete_strategy: crate::config::DeleteStrategyPolicy::default(),
            index_partition_spill_threshold: crate::table::writer::DEFAULT_SPILL_THRESHOLD,
            disable_cow_on_sst_files: true,
            use_reflink_for_checkpoint: true,
            storage_admission_check: false,
            storage_limit_bytes: None,
            tight_space_compaction: false,
            backpressure: crate::backpressure::BackpressureThresholds::OFF,
        }
    }
}

impl RuntimeConfig {
    /// Effective Page ECC setting for data Blocks: the per-scope
    /// override when set, else the global [`Self::page_ecc`] default.
    #[must_use]
    pub fn data_block_ecc(&self) -> bool {
        self.data_block_ecc_override.unwrap_or(self.page_ecc)
    }

    /// Effective Page ECC setting for the per-KV checksum footer region
    /// within footer-bearing data Blocks (#298): the per-scope override
    /// when set, else the global [`Self::page_ecc`] default.
    #[must_use]
    pub fn kv_checksums_ecc(&self) -> bool {
        self.kv_checksums_ecc_override.unwrap_or(self.page_ecc)
    }

    /// Effective Page ECC setting for manifest Blocks. No per-scope
    /// override — manifest is critical metadata and always tracks the
    /// global [`Self::page_ecc`] (per Q3 decision in the V5-2 issue).
    #[must_use]
    pub const fn manifest_ecc(&self) -> bool {
        self.page_ecc
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
mod tests;

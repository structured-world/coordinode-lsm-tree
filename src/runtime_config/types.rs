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
/// Live migration semantic: changing the configured algorithm
/// affects subsequent writes only. Existing blocks self-describe
/// via their own `checksum_type` byte (added by downstream PR
/// landing the `BlockHeader` extension), so readers handle mixed
/// algorithms in the same Tree transparently. Compaction rewrites
/// source blocks per the current algorithm.
//
// no-std: pure data type — compiles under `--no-default-features --features alloc`.
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
pub struct RuntimeConfig {
    /// Algorithm for Block-level integrity (the checksum in
    /// `BlockHeader`). Default: [`ChecksumAlgorithm::Xxh3_64`].
    ///
    /// Toggle takes effect on the next block write. Existing blocks
    /// carry their own `checksum_type` byte (added by downstream
    /// `BlockHeader` extension PR) so readers dispatch per-block.
    /// Compaction rewrites source blocks per the current algorithm.
    pub block_checksum_algo: ChecksumAlgorithm,

    /// Algorithm for per-KV checksums inside `DataKvChecked` /
    /// `FilterKvChecked` blocks (added by #298 implementation).
    /// Independent from [`Self::block_checksum_algo`]. Default:
    /// [`ChecksumAlgorithm::Xxh3_64`].
    pub kv_checksum_algo: ChecksumAlgorithm,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            block_checksum_algo: ChecksumAlgorithm::Xxh3_64,
            kv_checksum_algo: ChecksumAlgorithm::Xxh3_64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksum_algorithm_default_is_xxh3_64() {
        // Default chosen for speed on modern SIMD hardware and
        // codebase consistency (every other hash site uses XXH3).
        // Locked here so a regression switching the default to
        // something slower (e.g. CRC32C) lights up the test.
        assert_eq!(ChecksumAlgorithm::default(), ChecksumAlgorithm::Xxh3_64);
    }

    #[test]
    fn checksum_algorithm_digest_sizes_match_spec() {
        // Per design (#298 Q5): Xxh3_64 stores 8 bytes; Xxh3Low32
        // and Crc32c store 4 bytes. Wire format depends on this
        // — wrong value would silently mis-frame downstream blocks.
        assert_eq!(ChecksumAlgorithm::Xxh3_64.digest_size(), 8);
        assert_eq!(ChecksumAlgorithm::Xxh3Low32.digest_size(), 4);
        assert_eq!(ChecksumAlgorithm::Crc32c.digest_size(), 4);
    }

    #[test]
    fn checksum_algorithm_wire_tag_roundtrip() {
        // Every variant must roundtrip through its on-disk
        // discriminator. If we ever add a new variant without
        // wiring it into both directions, this catches the gap
        // before it ships as a corrupt-looking block on disk.
        for algo in [
            ChecksumAlgorithm::Xxh3_64,
            ChecksumAlgorithm::Xxh3Low32,
            ChecksumAlgorithm::Crc32c,
        ] {
            let tag = algo.wire_tag();
            assert_eq!(ChecksumAlgorithm::from_wire_tag(tag), Some(algo));
        }
    }

    #[test]
    fn checksum_algorithm_wire_tag_rejects_unknown() {
        // Forward-incompatible blocks (newer writer, older reader)
        // must surface as a parse failure rather than be silently
        // misinterpreted as a known algorithm.
        assert_eq!(ChecksumAlgorithm::from_wire_tag(255), None);
        assert_eq!(ChecksumAlgorithm::from_wire_tag(3), None);
    }

    #[test]
    fn runtime_config_default_uses_xxh3_64_everywhere() {
        // Default RuntimeConfig must match RocksDB-like baseline
        // for benchmark symmetry (#353): block-level checksum on,
        // no per-KV machinery configured yet (downstream PRs add
        // policy fields). Both algo slots default to Xxh3_64.
        let cfg = RuntimeConfig::default();
        assert_eq!(cfg.block_checksum_algo, ChecksumAlgorithm::Xxh3_64);
        assert_eq!(cfg.kv_checksum_algo, ChecksumAlgorithm::Xxh3_64);
    }
}

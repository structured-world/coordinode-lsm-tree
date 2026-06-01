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

/// Per-KV checksum policy: which data blocks get a per-entry checksum
/// trailer ([`crate::table::block::BlockType::DataKvChecked`]).
///
/// The block-level checksum covers the bytes as written to disk; it
/// does NOT catch a RAM bit-flip that corrupts a memtable entry
/// BEFORE the block checksum is computed at flush. Per-KV checksums
/// computed at memtable insert close that window. Default
/// [`Self::Off`] is wire-identical to the pre-per-KV format: blocks
/// stay [`crate::table::block::BlockType::Data`] with zero overhead.
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
/// keep their original `BlockType` and read transparently via
/// per-block dispatch. Compaction rewrites source blocks per the
/// current policy, so the choice migrates live without downtime.
//
// no-std: pure data — compiles under `--no-default-features --features alloc`.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum KvChecksumPolicy {
    /// No per-KV checksums. Blocks are wire-identical to the
    /// pre-per-KV [`crate::table::block::BlockType::Data`] format.
    /// Default — zero compute, zero storage, zero memtable overhead.
    #[default]
    Off,

    /// Every data block on every level carries a per-entry checksum
    /// trailer ([`crate::table::block::BlockType::DataKvChecked`]).
    AllLevels,

    /// Only data blocks on levels selected by the [`LevelMask`] carry
    /// the per-entry trailer. Levels outside the mask emit plain
    /// [`crate::table::block::BlockType::Data`].
    PerLevel(LevelMask),

    /// Only data blocks whose owning table id falls in the inclusive
    /// [`TableIdRange`] carry the per-entry trailer.
    PerTable(TableIdRange),
}

impl KvChecksumPolicy {
    /// Whether a data block written at `level` for table `table_id`
    /// must emit the per-entry checksum trailer under this policy.
    ///
    /// Drives the writer's [`crate::table::block::BlockType`] choice at
    /// block compile: `true` selects `DataKvChecked`, `false` selects
    /// the plain `Data` fast path.
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
/// [`Self::AtInsert`] computes the digest at memtable insert and carries it
/// to the block. This closes the memtable-residence window (the entry's
/// digest is fixed the moment it enters RAM), at the cost of storing the
/// digest in the memtable node. The digest must fit the node's reserved
/// space, so `AtInsert` is only valid with a 4-byte algorithm
/// ([`ChecksumAlgorithm::Xxh3Low32`] / [`ChecksumAlgorithm::Crc32c`]);
/// pairing it with the 8-byte [`ChecksumAlgorithm::Xxh3_64`] is rejected at
/// config-validation time.
//
// no-std: pure data — compiles under `--no-default-features --features alloc`.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum KvChecksumComputePoint {
    /// Compute at block compile (flush / compaction). Default. No memtable
    /// overhead; does not cover the memtable-residence window.
    #[default]
    AtBlockCompile,

    /// Compute at memtable insert and carry. Covers the full RAM lifecycle;
    /// requires a 4-byte algorithm.
    AtInsert,
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

    /// Which data blocks emit a per-entry checksum trailer
    /// ([`crate::table::block::BlockType::DataKvChecked`]). Default
    /// [`KvChecksumPolicy::Off`] keeps blocks wire-identical to the
    /// pre-per-KV [`crate::table::block::BlockType::Data`] format.
    ///
    /// Toggle takes effect on the next block compile; existing blocks
    /// keep their original `BlockType` and read transparently.
    /// Compaction migrates source blocks to the current policy over
    /// time. See [`KvChecksumPolicy`] for selection granularity.
    pub kv_checksums: KvChecksumPolicy,

    /// When the per-KV digest is computed: [`KvChecksumComputePoint::
    /// AtBlockCompile`] (default) at flush / compaction, or
    /// [`KvChecksumComputePoint::AtInsert`] at memtable insert (covers the
    /// memtable-residence window, requires a 4-byte
    /// [`Self::kv_checksum_algo`]). `AtInsert` paired with the 8-byte
    /// `Xxh3_64` is rejected by `update_runtime_config`.
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
    /// introduces the `DataKvChecked` Block variant for manifest
    /// sections AND extends the footer flags / payload to record
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

    /// Per-scope override for the per-KV checksum region within
    /// `DataKvChecked` Blocks (#298). `None` inherits
    /// [`Self::page_ecc`]; `Some(false)` keeps the kv-checksum
    /// region outside the parity calculation even when global ECC
    /// is enabled (useful when the per-KV checksums themselves are
    /// considered sufficient and ECC is wanted only on value bytes).
    pub kv_checksums_ecc_override: Option<bool>,
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

    /// Effective Page ECC setting for the per-KV checksum region
    /// within `DataKvChecked` Blocks (#298): the per-scope override
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
mod tests {
    use super::*;

    #[test]
    fn kv_checksum_policy_default_is_off() {
        // Off is the wire-identical, zero-overhead default: a tree that
        // never opts in must produce BlockType::Data blocks and pay no
        // per-entry cost. A regression flipping this default would
        // silently change the on-disk format for every existing user.
        assert_eq!(KvChecksumPolicy::default(), KvChecksumPolicy::Off);
    }

    #[test]
    fn kv_checksum_compute_point_default_is_at_block_compile() {
        // AtBlockCompile is the zero-memtable-overhead default. Flipping
        // it to AtInsert would change the memtable hot path for everyone,
        // so the default is pinned.
        assert_eq!(
            KvChecksumComputePoint::default(),
            KvChecksumComputePoint::AtBlockCompile
        );
        assert_eq!(
            RuntimeConfig::default().kv_checksum_compute_point,
            KvChecksumComputePoint::AtBlockCompile
        );
    }

    #[test]
    fn kv_checksum_policy_off_never_applies() {
        // Off must reject every (level, table) pair — no DataKvChecked
        // block is ever emitted under the default policy.
        let p = KvChecksumPolicy::Off;
        assert!(!p.applies(0, 0));
        assert!(!p.applies(7, u64::MAX));
    }

    #[test]
    fn kv_checksum_policy_all_levels_always_applies() {
        // AllLevels must select every (level, table) pair, including
        // out-of-mask-range levels (>= 8) that PerLevel can't reach.
        let p = KvChecksumPolicy::AllLevels;
        assert!(p.applies(0, 0));
        assert!(p.applies(9, 12345));
    }

    #[test]
    fn kv_checksum_policy_per_level_gates_on_mask() {
        // PerLevel applies only to levels whose bit is set. Hot-tier
        // selection (L0 + L1) must include 0 and 1 and exclude the
        // rest, regardless of table id.
        let mask = LevelMask::none().with_level(0).with_level(1);
        let p = KvChecksumPolicy::PerLevel(mask);
        assert!(p.applies(0, 999));
        assert!(p.applies(1, 999));
        assert!(!p.applies(2, 999));
        assert!(!p.applies(6, 999));
    }

    #[test]
    fn level_mask_out_of_range_level_is_never_selected() {
        // A u8 mask covers levels 0..=7. with_level on an out-of-range
        // level must be a no-op (not wrap into bit 0 via shift overflow)
        // and contains must report false for those levels.
        let mask = LevelMask::none().with_level(8).with_level(255);
        assert_eq!(mask.bits(), 0, "out-of-range levels must not set any bit");
        assert!(!mask.contains(8));
        assert!(!mask.contains(255));
    }

    #[test]
    fn level_mask_bits_roundtrip() {
        // Raw-bits constructor and accessor must round-trip so a
        // persisted mask byte reconstructs the same selection.
        let mask = LevelMask::none().with_level(0).with_level(3);
        assert_eq!(mask.bits(), 0b0000_1001);
        assert_eq!(LevelMask::from_bits(0b0000_1001), mask);
    }

    #[test]
    fn kv_checksum_policy_per_table_gates_on_inclusive_range() {
        // PerTable applies only inside the inclusive [start, end] span,
        // independent of level. Both endpoints are members; one past
        // each end is not.
        let p = KvChecksumPolicy::PerTable(TableIdRange::new(10, 20));
        assert!(p.applies(0, 10));
        assert!(p.applies(5, 20));
        assert!(!p.applies(0, 9));
        assert!(!p.applies(0, 21));
    }

    #[test]
    fn table_id_range_inverted_selects_nothing() {
        // A range with start > end is empty rather than panicking, so a
        // misconfigured range degrades to "no per-KV checksums" instead
        // of a crash or an all-match.
        let r = TableIdRange::new(20, 10);
        assert!(!r.contains(10));
        assert!(!r.contains(15));
        assert!(!r.contains(20));
    }

    #[test]
    fn runtime_config_default_kv_checksums_off() {
        // The wired RuntimeConfig field must default Off so the struct
        // default stays wire-compatible with pre-per-KV trees.
        assert_eq!(RuntimeConfig::default().kv_checksums, KvChecksumPolicy::Off);
    }

    #[test]
    fn checksum_algorithm_default_is_xxh3_64() {
        // Default chosen for speed on modern SIMD hardware and
        // codebase consistency (every other hash site uses XXH3).
        // Locked here so a regression switching the default to
        // something slower (e.g. CRC32C) lights up the test.
        assert_eq!(ChecksumAlgorithm::default(), ChecksumAlgorithm::Xxh3_64);
    }

    #[test]
    fn checksum_algorithm_compute_xxh3_64_matches_canonical_hash() {
        // Xxh3_64 must produce exactly the crate's canonical hash64 so a
        // per-KV digest is byte-identical to every other XXH3 site (the
        // reader recomputes with hash64 on verify).
        let data = b"per-kv checksum payload bytes";
        assert_eq!(
            ChecksumAlgorithm::Xxh3_64.compute(data),
            Some(crate::hash::hash64(data))
        );
    }

    #[test]
    fn checksum_algorithm_compute_xxh3low32_is_low_32_bits() {
        // Xxh3Low32 is the low 32 bits of the same digest: same compute,
        // half the stored width. The high bits must be zero so the
        // stored 4 bytes round-trip without truncation surprises.
        let data = b"per-kv checksum payload bytes";
        let full = crate::hash::hash64(data);
        let got = ChecksumAlgorithm::Xxh3Low32
            .compute(data)
            .expect("Xxh3Low32 is always available");
        assert_eq!(got, full & 0xFFFF_FFFF);
        assert_eq!(got >> 32, 0, "high 32 bits must be clear");
    }

    #[test]
    #[cfg(feature = "crc32c")]
    fn checksum_algorithm_compute_crc32c_when_feature_on() {
        // With the crc32c feature, Crc32c computes a non-trivial digest
        // that fits in 32 bits and is order-sensitive (a real checksum,
        // not a stub returning a constant).
        let a = ChecksumAlgorithm::Crc32c
            .compute(b"abc")
            .expect("crc32c feature enabled");
        let b = ChecksumAlgorithm::Crc32c
            .compute(b"acb")
            .expect("crc32c feature enabled");
        assert_eq!(a >> 32, 0, "CRC32C digest fits in 32 bits");
        assert_ne!(a, b, "CRC32C must be order-sensitive");
    }

    #[test]
    #[cfg(not(feature = "crc32c"))]
    fn checksum_algorithm_compute_crc32c_none_when_feature_off() {
        // Without the feature, selecting Crc32c must surface as None so a
        // caller translates it into a typed "not compiled in" error
        // rather than silently substituting another algorithm.
        assert_eq!(ChecksumAlgorithm::Crc32c.compute(b"abc"), None);
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
        // Default RuntimeConfig must match `RocksDB`-like baseline
        // for benchmark symmetry (#353): block-level checksum on,
        // no per-KV machinery configured yet (downstream PRs add
        // policy fields). Both algo slots default to Xxh3_64.
        let cfg = RuntimeConfig::default();
        assert_eq!(cfg.block_checksum_algo, ChecksumAlgorithm::Xxh3_64);
        assert_eq!(cfg.kv_checksum_algo, ChecksumAlgorithm::Xxh3_64);
    }

    #[test]
    fn runtime_config_default_manifest_safety_on() {
        // Per V5-2 Q-decisions: manifest footer mirror and per-KV
        // checksums default ON. Footer mirror gives partial-write
        // recovery for ~4 KiB cost; per-KV checksums match `RocksDB`
        // MANIFEST per-record CRC granularity so apples-to-apples
        // benchmarks aren't paying for an opt-in we don't ship.
        // Both defaults are load-bearing — flipping them silently
        // would regress durability for new users.
        let cfg = RuntimeConfig::default();
        assert!(cfg.manifest_footer_mirror);
        assert!(cfg.manifest_kv_checksums);
    }

    #[test]
    fn runtime_config_default_page_ecc_off_with_no_overrides() {
        // ECC is explicit opt-in per Q3: zero cost unless enabled.
        // Per-scope overrides default to None (inherit global).
        let cfg = RuntimeConfig::default();
        assert!(!cfg.page_ecc);
        assert_eq!(cfg.data_block_ecc_override, None);
        assert_eq!(cfg.kv_checksums_ecc_override, None);
    }

    #[test]
    fn runtime_config_ecc_helpers_inherit_global_when_no_override() {
        // Helper methods are the call-site API for "should I emit
        // ECC parity here". With no override, every scope tracks
        // the global page_ecc flag.
        let on = RuntimeConfig {
            page_ecc: true,
            ..RuntimeConfig::default()
        };
        assert!(on.data_block_ecc());
        assert!(on.kv_checksums_ecc());
        assert!(on.manifest_ecc());

        let off = RuntimeConfig::default();
        assert!(!off.data_block_ecc());
        assert!(!off.kv_checksums_ecc());
        assert!(!off.manifest_ecc());
    }

    #[test]
    fn runtime_config_ecc_overrides_take_precedence_over_global() {
        // Per Q3 refinement: per-scope override beats global. This
        // is what enables "manifest-only ECC" (global ON, data
        // override Some(false)) and "data without kv-checksum-region
        // ECC" (global ON, kv override Some(false)). Manifest has no
        // override knob — that's also locked in here.
        let suppressed = RuntimeConfig {
            page_ecc: true,
            data_block_ecc_override: Some(false),
            kv_checksums_ecc_override: Some(false),
            ..RuntimeConfig::default()
        };
        assert!(!suppressed.data_block_ecc());
        assert!(!suppressed.kv_checksums_ecc());
        // Manifest ignores per-scope overrides — always tracks global.
        assert!(suppressed.manifest_ecc());

        let forced = RuntimeConfig {
            page_ecc: false,
            data_block_ecc_override: Some(true),
            kv_checksums_ecc_override: Some(true),
            ..RuntimeConfig::default()
        };
        assert!(forced.data_block_ecc());
        assert!(forced.kv_checksums_ecc());
        // Same for the inverse: manifest stays at global, which is
        // off here, even when data + kv overrides force ECC on.
        assert!(!forced.manifest_ecc());
    }
}

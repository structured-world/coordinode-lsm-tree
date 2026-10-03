//! Head-to-head comparison harness: `coordinode-lsm-tree` vs RocksDB.
//!
//! Each criterion group runs the same workload through both engines
//! and produces side-by-side timings for the gh-pages dashboard
//! (per [#244]). The harness intentionally mirrors
//! `structured-zstd`'s `compare_ffi.rs` shape so the merge / chart
//! scripts in `.github/scripts/` are byte-for-byte reusable. The
//! `docs/BENCHMARKS.md` operator guide lands in a follow-up commit
//! on this branch alongside the gh-pages workflow port.
//!
//! [#244]: https://github.com/structured-world/coordinode-lsm-tree/issues/244
//!
//! Run locally:
//!
//! ```text
//! cd tools/compare-rocksdb && cargo bench
//! ```
//!
//! On macOS, `librocksdb-sys`'s `bindgen` build script needs to
//! find `libclang.dylib`. Brew's LLVM puts it under
//! `/opt/homebrew/opt/llvm/lib`; export both
//! `LIBCLANG_PATH` (bindgen) and `DYLD_FALLBACK_LIBRARY_PATH`
//! (dyld for the build-script binary) before invoking cargo:
//!
//! ```text
//! export LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib
//! export DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/opt/llvm/lib
//! cd tools/compare-rocksdb && cargo bench
//! ```
//!
//! Linux CI uses the distro `libclang.so` which `bindgen` finds
//! without env-var help.
//!
//! ## Criterion settings come from the command line
//!
//! Sample count, warm-up and measurement window are NOT set in code: the
//! groups inherit whatever the Criterion CLI passes (the benchmark workflow
//! runs `--sample-size 10 --warm-up-time 0.5 --measurement-time 0.5
//! --noplot`). A group-level `sample_size(..)` silently overrides the CLI,
//! and with the cold-write arms costing seconds per iteration a 100-sample
//! default would multiply them tenfold.
//!
//! Ten samples is Criterion's floor, so an arm whose single iteration
//! outlasts the window costs eleven iterations (the samples plus the
//! warm-up) whatever the window says, and Criterion warns that it could not
//! fit them; for those arms the cost is set by their size alone.
//!
//! After the run the harness writes `summary.json` next to Criterion's
//! results (see [`write_summary`]): every arm's mean and confidence interval
//! and what each group measures, which the published page is drawn from.
//!
//! Warm read arms open their on-disk state ONCE per arm (see
//! [`WarmEngine`]): Criterion re-enters a `bench_with_input` routine
//! closure for the warm-up pass and for every sample, so anything built
//! inside the closure is rebuilt once per sample. The state itself is
//! written once per engine, codec and size and copied for each arm and
//! each overwrite iteration that starts from it (see [`SeedStates`]).
//!
//! ## Engine matrix
//!
//! The shared workload closure is parameterised over an [`Engine`]
//! enum so the per-engine glue (open, put, get, flush, close) lives
//! in exactly one place per engine and the workload code stays
//! engine-agnostic. Three engines today: `ours`, `rocksdb`, and
//! `surrealkv` (pure-Rust embedded LSM/MVCC). SurrealKV has no zstd
//! codec, so it overlays ONLY on the `None`-compression groups (see
//! [`engines_for`]); the `_zstd22` groups stay ours-vs-rocksdb.
//!
//! ## Compression axis + cross-engine overlay
//!
//! Every scenario is run twice: once with `None` block compression
//! (the `<scenario>` group) and once with zstd at level 22, the maximum
//! "ultra" level (the `<scenario>_zstd22` group). Both engines are
//! configured identically per variant — ours via
//! `CompressionType::Zstd(22)`, RocksDB via `DBCompressionType::Zstd`
//! pinned to level 22 — so the no-compression and high-ratio paths sit
//! side-by-side on the dashboard.
//!
//! Within each group every engine runs in the SAME process and the SAME
//! invocation, so criterion plots them as an overlay (ours vs rocksdb,
//! plus surrealkv on the `None` groups) on one chart. Because the
//! comparison is a ratio measured on one host in one run, it stays
//! meaningful even if the bench host's CPU changes between runs — the
//! absolute numbers move, the relative gap does not.
//!
//! ## Workload coverage
//!
//! - `write_throughput/{1k,10k,70k}` — bulk insert N keys, 256-byte
//!   values, random keys. Cold-start: each iteration opens an empty
//!   engine, writes N, flushes. Dominated by the fixed open + flush
//!   cost at small N.
//! - `point_read/{1k,10k,70k}` — read N random keys from an engine
//!   pre-populated with N keys and flushed to disk. Warm: the engine
//!   is opened + populated + flushed ONCE outside the timed window,
//!   so the measurement is steady-state read latency (block cache +
//!   bloom filter + on-disk block fetch), not setup cost. The `ours` /
//!   `rocksdb` series use a binary-search data-block index; the same chart
//!   overlays `ours-hash-index` / `rocksdb-hash-index` series (data-block
//!   hash index ON — ours: 1.33 buckets/entry; RocksDB: `BinaryAndHash` @
//!   0.75) and an `ours-ribbon` series (retrieval-ribbon locator: key ->
//!   block + restart in O(1), skipping both the index-block and in-block
//!   searches) so every index strategy is compared head-to-head on one plot.
//! - `range_scan/{1k,10k,70k}` — full forward scan reading every value
//!   from a warm, pre-populated engine. Steady-state sequential-scan
//!   throughput (block decode + iterator advance).
//! - `seek_random/{1k,10k,70k}` — seek to each (scattered) key and read
//!   the value at the cursor, on a warm engine. Seek-then-read latency
//!   (index descent + cursor positioning + block decode).
//! - `overwrite/{1k,10k,70k}` — rewrite the whole keyspace into an engine
//!   that already holds one copy (the first copy is written outside the
//!   timed window). Overwrite cost (memtable churn over existing keys +
//!   a superseding flush), distinct from cold first-insert.
//!
//! Each of the above also has a `_zstd22` sibling. The read siblings run 70k
//! only, the one size whose working set overflows the block cache so a read
//! decodes a block at all (see [`read_sizes`]); the write siblings run every
//! size.
//!
//! Values carry the compressibility RocksDB's own `db_bench` gives its values
//! by default (see [`ValuePool`]), and every RocksDB read borrows the value
//! out of the block as ours does (`get_pinned`, the raw iterator), so neither
//! engine pays for a copy or a codec shortcut the other does not. Not yet portable
//! head-to-head: `readwhilewriting` (concurrency) and `mergerandom`
//! (merge-operator semantics differ across engines) from [#244]'s list.

use std::collections::HashMap;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group};
// `Guard` is a trait, used (not dead) for its `.value()` method on the
// `IterGuardImpl` items yielded by `tree.iter()` / `tree.range()` in the
// range_scan and seek_random scenarios — there is no direct path
// reference, so it reads as unused at a glance but the import is required
// for method resolution (clippy `-D warnings` confirms it is live).
use lsm_tree::{
    AbstractTree, CompressionType, Config, Guard, MAX_SEQNO, SequenceNumberCounter,
    config::{
        CompressionPolicy, HashRatioPolicy, KvSeparationOptions, LocatorPolicy, LocatorPolicyEntry,
        LocatorPrecision, PinningPolicy,
    },
    runtime_config::{KvChecksumPolicy, RuntimeConfig},
};

/// In-block index strategy overlaid as a separate `point_read` series.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum IndexStrategy {
    /// Binary search over the restart array (the engine default).
    Binary,
    /// Data-block hash index (key -> restart by hash). Supported by both ours
    /// and RocksDB.
    HashIndex,
    /// Retrieval-ribbon locator (ours only): key -> (block_id, slot) in O(1),
    /// skipping both the index-block and in-block searches.
    Ribbon,
}
use surrealkv::{
    Durability as SkvDurability, LSMIterator as _, Options as SkvOptions, Tree as SkvTree,
    TreeBuilder as SkvTreeBuilder,
};

/// Full-keyspace scan bounds for SurrealKV's `range(start, end)` (start
/// inclusive, end exclusive). Keys are 16-byte big-endian; a 17-byte all-`0xFF`
/// upper bound sorts after every 16-byte key, so the half-open range covers the
/// whole keyspace.
const SKV_MIN_KEY: &[u8] = &[0u8; 16];
const SKV_MAX_KEY: &[u8] = &[0xFFu8; 17];

/// A multi-thread tokio runtime for SurrealKV. Each surrealkv bench arm owns
/// one for its whole duration: `build()` spawns background compaction tasks via
/// `tokio::spawn` (so it must run inside a runtime context, here via
/// `block_on`), and those tasks need worker threads to keep running while the
/// tree is read. The runtime must outlive the tree.
fn skv_runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Runtime::new()
}

/// Opens a fresh SurrealKV tree at `dir` on `rt`. Compression is left at the
/// default (`None`) — SurrealKV has no zstd, and it only runs in the
/// None-compression groups (see [`engines_for`]). `build()` is sync but spawns
/// background tasks, so it runs inside `rt.block_on` to see the runtime.
///
/// Returns `Result` so open failures propagate to the benchmark boundary (where
/// the engine label is attached) rather than panicking on the I/O path — same
/// shape as `open_ours` / `rocksdb::DB::open`.
fn open_surrealkv(
    rt: &tokio::runtime::Runtime,
    dir: &std::path::Path,
) -> Result<SkvTree, Box<dyn std::error::Error>> {
    let path = dir.to_path_buf();
    let tree = rt.block_on(async move {
        let opts = SkvOptions::new().with_path(path);
        SkvTreeBuilder::with_options(opts).build()
    })?;
    Ok(tree)
}

/// Populates a SurrealKV tree with `inputs` in a single write transaction and
/// commits with `Immediate` durability (fsync) — the flush-to-disk equivalent
/// of `ours`' `flush_active_memtable` / rocksdb's `flush`, so the warm-read
/// groups start from an on-disk state. `commit()` is async; driven via
/// `rt.block_on`.
///
/// NOTE: both engines are MVCC — ours tags every write with a sequence number
/// and reads a snapshot via `get(key, seqno)`; surrealkv versions per
/// transaction. So the write asymmetry here is NOT "MVCC vs flat": it is the
/// write PATH. surrealkv runs a real transaction (begin / set / commit) with a
/// per-commit `Immediate` fsync, plus vlog (KV-separation) and B+tree index
/// upkeep, whereas our arm does seqno-tagged memtable inserts + one terminal
/// flush. Read the comparison as two MVCC LSMs with different transaction /
/// index layers, not a byte-for-byte equivalent setup.
fn populate_surrealkv(
    rt: &tokio::runtime::Runtime,
    dir: &std::path::Path,
    inputs: &WorkloadInputs,
) -> Result<SkvTree, Box<dyn std::error::Error>> {
    let tree = open_surrealkv(rt, dir)?;
    let mut txn = tree.begin()?;
    for (key, value) in inputs.keys.iter().zip(inputs.values.iter()) {
        txn.set(key.as_slice(), value.as_slice())?;
    }
    txn.set_durability(SkvDurability::Immediate);
    rt.block_on(txn.commit())?;
    Ok(tree)
}

/// Builds a warm, on-disk SurrealKV tree for the read / scan / seek / overwrite
/// groups: a tokio runtime plus a tree already populated and fsynced. Returns
/// both so the caller binds them as `let (rt, tree) = ...` — the runtime drops
/// LAST (after the tree), keeping its background compaction tasks alive for the
/// whole timed read phase. Fallible end-to-end so the open/begin/set/commit I/O
/// path propagates errors; the caller panics once with the engine label at the
/// Criterion closure boundary (which cannot itself return `Result`), mirroring
/// how `run_write_throughput` surfaces failures.
fn setup_surrealkv_warm(
    dir: &std::path::Path,
    inputs: &WorkloadInputs,
) -> Result<(tokio::runtime::Runtime, SkvTree), Box<dyn std::error::Error>> {
    let rt = skv_runtime()?;
    let tree = populate_surrealkv(&rt, dir, inputs)?;
    Ok((rt, tree))
}

/// Engine under test. The harness runs each workload once per
/// variant and emits per-engine timings under the same criterion
/// `BenchmarkGroup`, so the gh-pages dashboard can plot them
/// side-by-side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Engine {
    Ours,
    RocksDb,
    /// SurrealKV — pure-Rust embedded LSM/MVCC store. No zstd codec
    /// (only None / Snappy), so it participates ONLY in the
    /// None-compression (codec-neutral) groups — see [`engines_for`].
    SurrealKv,
    /// Our engine in its KV-separated (`blob_tree`) configuration: values at or
    /// above [`BLOB_SEPARATION_THRESHOLD`] are stored out-of-line in blob files,
    /// so the key-LSM the reads walk is smaller (like surrealkv's vlog). Drives
    /// the same `AbstractTree` API as `Ours`; participates only in the
    /// None-compression groups where surrealkv overlays (see [`engines_for`]).
    BlobTree,
}

impl Engine {
    fn label(self) -> &'static str {
        match self {
            Self::Ours => "ours",
            Self::RocksDb => "rocksdb",
            Self::SurrealKv => "surrealkv",
            Self::BlobTree => "blob_tree",
        }
    }

    /// Whether this engine opens our tree with KV-separation enabled.
    fn kv_separated(self) -> bool {
        matches!(self, Self::BlobTree)
    }
}

/// KV-separation threshold for the `blob_tree` arm: values at or above this many
/// bytes are stored out-of-line. The benchmark value is 256 bytes (see
/// [`VALUE_SIZE`]), so 128 separates every value out-of-line, mirroring
/// surrealkv's vlog and isolating the "smaller key-LSM" read effect the
/// `blob_tree` arm exists to measure. Below the default 1 KiB threshold, which
/// would leave the 256-byte values inlined (no separation, no measurement).
const BLOB_SEPARATION_THRESHOLD: u32 = 128;

/// Engines to overlay for a given compression variant.
///
/// SurrealKV has no zstd codec (its `CompressionType` is `None` / `Snappy`
/// only), so it cannot match the `Zstd22` variant apples-to-apples — adding a
/// non-zstd line to a zstd22 graph would misrepresent the comparison. It is
/// therefore restricted to the `None`-compression groups, where all three
/// engines run codec-neutral. The `_zstd22` groups stay ours-vs-rocksdb.
fn engines_for(compression: Compression) -> &'static [Engine] {
    match compression {
        Compression::None => &[
            Engine::Ours,
            Engine::RocksDb,
            Engine::SurrealKv,
            Engine::BlobTree,
        ],
        Compression::Zstd22 => &[Engine::Ours, Engine::RocksDb],
    }
}

/// Element counts of a warm-read group (`point_read`, `multi_get`,
/// `range_scan`, `seek_random`).
///
/// The block cache is 16 MiB on both engines and every value is 256 bytes, so
/// at 1k and 10k the whole key set stays in the cache after the warm-up: no
/// timed read decodes a block, and a zstd-22 group at those sizes measures
/// exactly what the uncompressed group at the same size does (it did, within
/// noise, on every engine and size). Only 70k overflows the cache, so the
/// zstd-22 read groups run that size alone.
fn read_sizes(compression: Compression) -> &'static [u64] {
    match compression {
        Compression::None => &[1_000, 10_000, 70_000],
        Compression::Zstd22 => &[70_000],
    }
}

/// Compression axis of the engine matrix. Each workload runs once per
/// variant so the dashboard plots the `None` baseline and the
/// high-ratio zstd path side-by-side, with both engines configured the
/// same way per variant (apples-to-apples).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Compression {
    /// No block compression — the `None`-policy baseline.
    None,
    /// Zstd at level 22 (the maximum / "ultra" level) on both engines.
    Zstd22,
}

impl Compression {
    /// Zstd maximum level. `CompressionType::Zstd` upholds a `1..=22`
    /// invariant, so 22 is the highest valid setting; RocksDB's zstd
    /// accepts the same level range.
    const ZSTD_MAX_LEVEL: i32 = 22;
}

/// Which lsm-tree-only on-disk opt-ins are active for the `ours` engine, per
/// the Benchmark Symmetry Invariant: any feature RocksDB has no equivalent for
/// must be OFF when we publish a head-to-head, or we either pay for protection
/// the competitor lacks (losing a comparison we should win) or win unfairly on
/// a benchmark where the competitor lacks a feature we enable by default.
///
/// Only OUR config moves across presets; RocksDB is the fixed baseline. The
/// default is [`Preset::RocksDbParity`], so the public dashboard is honest
/// out of the box.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Preset {
    /// Every lsm-tree-only opt-in OFF, matching RocksDB's durability defaults.
    /// The single source of truth for "disable features RocksDB has no
    /// equivalent for": when a new opt-in lands it must be turned off here too.
    RocksDbParity,
    /// Production defaults (manifest hardening + FS-aware optimizations on):
    /// what a real lsm-tree deployment runs.
    LsmTreeDefault,
    /// Every opt-in ON: the worst-case protection-overhead measurement.
    LsmTreeParanoid,
}

impl Preset {
    /// Selects the preset from the `LSM_BENCH_PRESET` env var
    /// (`rocksdb-parity` | `lsm-default` | `lsm-paranoid`), defaulting to
    /// [`Preset::RocksDbParity`] (also used for any unrecognized value).
    fn from_env() -> Self {
        match std::env::var("LSM_BENCH_PRESET").as_deref() {
            Ok("lsm-default") => Self::LsmTreeDefault,
            Ok("lsm-paranoid") => Self::LsmTreeParanoid,
            _ => Self::RocksDbParity,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::RocksDbParity => "rocksdb-parity",
            Self::LsmTreeDefault => "lsm-default",
            Self::LsmTreeParanoid => "lsm-paranoid",
        }
    }
}

/// The preset for this whole bench process, resolved once from the environment.
/// Cached so every engine open in the run sees the same preset (and the choice
/// is logged exactly once, to stderr, for the dashboard provenance).
fn active_preset() -> Preset {
    static PRESET: std::sync::OnceLock<Preset> = std::sync::OnceLock::new();
    *PRESET.get_or_init(|| {
        let p = Preset::from_env();
        eprintln!(
            "compare-rocksdb: lsm-tree preset = {} (set LSM_BENCH_PRESET to override)",
            p.label()
        );
        p
    })
}

/// Applies the active [`Preset`]'s on-disk feature toggles to our engine config.
/// `RocksDbParity` explicitly disables every lsm-tree-only opt-in (even those
/// already off by default) so the preset stays correct if a default ever flips,
/// and documents the full parity surface in one place.
fn apply_preset(config: Config, preset: Preset) -> Config {
    let mut rc = RuntimeConfig::default();
    match preset {
        Preset::RocksDbParity => {
            // Disable every feature RocksDB has no equivalent for.
            rc.manifest_footer_mirror = false;
            rc.kv_checksums = KvChecksumPolicy::Off;
            rc.seqno_in_index = false;
            rc.page_ecc = false;
            rc.disable_cow_on_sst_files = false;
            rc.use_reflink_for_checkpoint = false;
            // Keep manifest per-record checksums ON: this matches RocksDB's
            // per-record MANIFEST CRC32 granularity (same durability profile),
            // so it is parity, not an extra opt-in.
            rc.manifest_kv_checksums = true;
            // `Config::page_ecc` is the separate tree-open gate for DATA-block
            // ECC (the runtime `page_ecc` above covers manifest blocks).
            // The retrieval-ribbon locator is on by default (block precision);
            // RocksDB has no equivalent, so parity disables it.
            //
            // Index and filter layout as RocksDB's defaults have them: one
            // index and one filter block per table (`kBinarySearch`, a full
            // filter), held by the table reader rather than the block cache
            // (`cache_index_and_filter_blocks = false`), at every level. Ours
            // partitions the index at every level by default so a corrupt
            // block costs one partition, not the whole table; RocksDB pays no
            // such second index lookup, so parity turns it off.
            config
                .with_runtime_config(rc)
                .page_ecc(false)
                .locator_policy(LocatorPolicy::disabled())
                .index_block_partitioning_policy(PinningPolicy::disabled())
                .filter_block_partitioning_policy(PinningPolicy::disabled())
                .index_block_pinning_policy(PinningPolicy::all(true))
                .filter_block_pinning_policy(PinningPolicy::all(true))
        }
        // Production defaults are exactly `RuntimeConfig::default()` + the
        // `Config` defaults, so leave the config untouched.
        Preset::LsmTreeDefault => config,
        Preset::LsmTreeParanoid => {
            rc.manifest_footer_mirror = true;
            rc.manifest_kv_checksums = true;
            rc.kv_checksums = KvChecksumPolicy::AllLevels;
            rc.seqno_in_index = true;
            rc.page_ecc = true;
            config.with_runtime_config(rc).page_ecc(true)
        }
    }
}

/// Deterministic but pseudo-random key derivation. Each key is the
/// big-endian encoding of `(i * GOLDEN_RATIO_64) wrapping_mul()` —
/// avoids hot-path RNG cost inside the timing loop while still
/// spreading keys across the keyspace so the bloom filter and
/// block-cache behaviour stays realistic.
fn key_for(i: u64) -> [u8; 16] {
    // `0x9E37_79B9_7F4A_7C15` = floor(2^64 / phi); standard mixing
    // constant for sequence-to-quasi-random mapping.
    const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;
    let mixed = i.wrapping_mul(GOLDEN);
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&mixed.to_be_bytes());
    out[8..].copy_from_slice(&i.to_be_bytes());
    out
}

/// Bytes per value in every workload but the sub-compaction one.
const VALUE_SIZE: usize = 256;

/// Value bytes with the compressibility RocksDB's own `db_bench` writes by
/// default (`--compression_ratio=0.5`): a 1 MiB pool of 100-byte pieces, each
/// 50 random printable bytes written twice, out of which consecutive values
/// are cut in turn. A codec does real work on them and roughly halves them, on
/// both engines alike. A constant fill would let any codec collapse every
/// block, and the compressed groups would measure little beyond the codec's
/// per-block setup.
struct ValuePool {
    bytes: Vec<u8>,
}

impl ValuePool {
    const SIZE: usize = 1 << 20;
    const PIECE: usize = 100;

    fn new() -> Self {
        // xorshift64 from a fixed seed: the same bytes on every run and host.
        let mut state: u64 = 0x2545_F491_4F6C_DD1D;
        let mut bytes = Vec::with_capacity(Self::SIZE + Self::PIECE);
        while bytes.len() < Self::SIZE {
            let mut half = [0_u8; Self::PIECE / 2];
            for byte in &mut half {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                // Printable ASCII, the alphabet RocksDB draws its pieces from;
                // `state % 95` is below 95, so the cast cannot truncate.
                *byte = b' ' + (state % 95) as u8;
            }
            bytes.extend_from_slice(&half);
            bytes.extend_from_slice(&half);
        }
        bytes.truncate(Self::SIZE);
        Self { bytes }
    }

    /// The `i`-th value: the pool's `i`-th `VALUE_SIZE` slice, wrapping at its
    /// end as RocksDB's generator does. Each block holds distinct values, and a
    /// block is compressed on its own, so the wrap gives a codec nothing to
    /// match across blocks.
    fn value(&self, i: u64) -> &[u8] {
        let slots = (Self::SIZE / VALUE_SIZE) as u64;
        let start = (i % slots) as usize * VALUE_SIZE;
        &self.bytes[start..start + VALUE_SIZE]
    }
}

/// Precomputed (key, value) workload for a given `n_keys`. Built
/// ONCE outside the timing loop so the bench measures engine
/// write throughput, not the per-key key derivation and value
/// allocation cost (which otherwise dominates at the 1k / 10k scale).
struct WorkloadInputs {
    keys: Vec<[u8; 16]>,
    values: Vec<Vec<u8>>,
}

impl WorkloadInputs {
    fn build(n_keys: u64) -> Self {
        let n = usize::try_from(n_keys).expect("n_keys fits in usize");
        let pool = ValuePool::new();
        let mut keys = Vec::with_capacity(n);
        let mut values = Vec::with_capacity(n);
        for i in 0..n_keys {
            keys.push(key_for(i));
            values.push(pool.value(i).to_vec());
        }
        Self { keys, values }
    }
}

/// RocksDB `Options` configured to match our engine's defaults so the
/// head-to-head stays apples-to-apples:
///
/// - **No compression** — our default `data_block_compression_policy`
///   writes L0 with `None`.
/// - **10-bits/key bloom filter** — `Config::default()` gives our engine
///   `Bloom(BitsPerKey(10.0))`. RocksDB has NO filter policy by default,
///   so without this it would skip the bloom construction our engine
///   pays at flush (write side) and the bloom probe per lookup (read
///   side).
/// - **16 MiB block cache** — matches our default per-tree cache
///   capacity, so neither engine gets an unfair cache-size edge.
///
/// `create_if_missing` is set here too. WAL handling is per-call
/// (`WriteOptions::disable_wal`) since it only applies to the write
/// path.
///
/// The `compression` argument selects the codec to match our engine's
/// per-variant setting: `None` leaves RocksDB uncompressed; `Zstd22`
/// sets `DBCompressionType::Zstd` and pins the level to 22 via
/// `set_compression_options`.
fn rocksdb_options(compression: Compression, hash_index: bool) -> rocksdb::Options {
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    let cache = rocksdb::Cache::new_lru_cache(16 * 1024 * 1024);
    block_opts.set_block_cache(&cache);
    // bits_per_key = 10.0, block_based = false → modern full-block filter,
    // the closest match to our `BitsPerKey(10.0)` policy.
    block_opts.set_bloom_filter(10.0, false);
    if hash_index {
        // Data-block hash index: a point get resolves a key to its in-block
        // offset by hash instead of binary-searching the restart array. The
        // 0.75 utilization is RocksDB's recommended default and is the rough
        // equal of our 1.33 buckets/entry (1 / 1.33 ≈ 0.75) for an
        // apples-to-apples hash-index overlay.
        block_opts.set_data_block_index_type(rocksdb::DataBlockIndexType::BinaryAndHash);
        block_opts.set_data_block_hash_ratio(0.75);
    }
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    match compression {
        Compression::None => opts.set_compression_type(rocksdb::DBCompressionType::None),
        Compression::Zstd22 => {
            opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
            // (window_bits, level, strategy, max_dict_bytes). -14 is RocksDB's
            // default zstd window-bits sentinel, strategy 0 / max_dict 0 keep
            // every other zstd parameter at its default — only the level is
            // pinned to 22 to match our `CompressionType::Zstd(22)`.
            opts.set_compression_options(-14, Compression::ZSTD_MAX_LEVEL, 0, 0);
        }
    }
    opts.set_block_based_table_factory(&block_opts);
    opts
}

/// Opens our engine at `dir` with the block-compression policy for the
/// given `compression` variant. Both arms set the policy EXPLICITLY:
/// `None` pins `CompressionPolicy::all(None)` rather than relying on the
/// `Config` default (which becomes `[None, Lz4]` if the `lz4` feature is
/// ever enabled on this bench crate, silently compressing the supposed
/// "uncompressed baseline"); `Zstd22` applies level-22 zstd to every
/// level. Keeping the `None` arm explicit holds the baseline apples-to-
/// apples with RocksDB's `DBCompressionType::None`.
///
/// When `kv_separated` is set, values at or above [`BLOB_SEPARATION_THRESHOLD`]
/// are stored out-of-line (the `blob_tree` arm); the blob files inherit the
/// `None`-compression baseline so the separated path stays codec-neutral too.
fn open_ours(
    dir: &std::path::Path,
    compression: Compression,
    kv_separated: bool,
    strategy: IndexStrategy,
    row_cache: bool,
) -> Result<lsm_tree::AnyTree, Box<dyn std::error::Error>> {
    let config = Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    );
    // The symmetry preset (RocksDbParity by default) FIRST, so our opt-ins match
    // RocksDB's feature set as the baseline and each variant's own settings
    // below land on top of it. Applied last, the parity preset's locator
    // switch-off undid the ribbon variant's locator, so that variant measured
    // the plain read path under the ribbon's name.
    let config = apply_preset(config, active_preset());
    // Row cache: a key->resolved-value layer in front of the block cache so a
    // repeat point read skips the index walk + data-block decode.
    //
    // BOTH branches build the cache explicitly. Leaving the `false` arms on the
    // library default stopped working the moment that default became ON: the
    // one-time verification read below touches every key, which would populate
    // the row cache for the plain, hash-index and ribbon arms too, and their
    // timed reads would then all be row-cache hits. The variants this suite
    // exists to separate would collapse into one, and the published numbers
    // would say the wrong thing about every one of them. 16 MiB either way,
    // matching the library default capacity.
    let config = config.use_cache(std::sync::Arc::new(
        lsm_tree::Cache::with_capacity_bytes(16 * 1024 * 1024).with_row_cache(row_cache),
    ));
    // Data-block hash index: a point get resolves a key to its in-block offset
    // by hash instead of binary-searching the restart array. 1.33 buckets/entry
    // is the rough equal of RocksDB's 0.75 utilization for the hash-index
    // overlay. Default policy (0.0) leaves it off for the binary-search arms.
    let config = if strategy == IndexStrategy::HashIndex {
        config.data_block_hash_ratio_policy(HashRatioPolicy::all(1.33))
    } else {
        config
    };
    // Retrieval-ribbon locator: a point get resolves the key to its data block
    // and restart in O(1), skipping both the index-block and in-block searches.
    // Restart precision (per-sub-block) is the recommended default.
    let config = if strategy == IndexStrategy::Ribbon {
        config.locator_policy(LocatorPolicy::all(LocatorPolicyEntry::Enabled {
            precision: LocatorPrecision::Restart,
            block_id_bits: None,
            slot_bits: None,
        }))
    } else {
        config
    };
    let config = match compression {
        Compression::None => {
            config.data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
        }
        Compression::Zstd22 => config.data_block_compression_policy(CompressionPolicy::all(
            CompressionType::Zstd(Compression::ZSTD_MAX_LEVEL),
        )),
    };
    let config = if kv_separated {
        // Blobs stay `None`-compressed (the bench crate has no `lz4` feature, so
        // the default blob compression is already `None`; set it explicitly so a
        // future feature flip cannot silently compress blobs).
        config
            .with_kv_separation(Some(
                KvSeparationOptions::default().separation_threshold(BLOB_SEPARATION_THRESHOLD),
            ))
            .blob_compression(CompressionType::None)
    } else {
        config
    };
    Ok(config.open()?)
}

/// Workload: bulk-insert `inputs.keys.len()` (key, value) pairs
/// into a freshly-opened engine. The `Instant::now()` snapshot is
/// taken BEFORE the engine open and the elapsed capture is taken
/// IMMEDIATELY AFTER the terminal flush — before the engine handle
/// drops — so the measurement covers cold-start cost (engine open,
/// first-write path through memtable init) plus N writes plus the
/// explicit flush, but NOT the close/drop time (which is dominated
/// by background compaction finalisation and would otherwise
/// contaminate "write throughput" numbers with shutdown work).
///
/// Apples-to-apples configuration:
///
///   - **Compression / bloom / cache matched via [`rocksdb_options`].**
///     None compression on both sides; RocksDB gets the same 10-bits/key
///     bloom filter and 16 MiB block cache our engine has by default, so
///     RocksDB also builds a bloom filter at flush (the work our engine
///     does) instead of skipping it. A future `write_throughput_lz4`
///     variant can flip compression on both.
///
///   - **No WAL on either side.** lsm-tree has no WAL —
///     durability is the caller's responsibility, and
///     `flush_active_memtable` is the explicit barrier. RocksDB is
///     given `WriteOptions::disable_wal(true)` so it does the
///     same shape of work (memtable insert + terminal flush)
///     rather than paying the per-`put` WAL fsync that our crate
///     never does. A future `write_throughput_durable` variant
///     can flip both back (lsm-tree consumers would layer their
///     own journal; RocksDB would re-enable its WAL).
///
/// What this is NOT measuring: steady-state per-write throughput
/// on an already-warm engine — that needs the engine kept open
/// across iterations, which the harness deliberately doesn't do
/// (each iteration starts from an empty database to keep results
/// reproducible across criterion warmup vs measurement phases).
/// Keys / values are precomputed in `inputs` so the timed body
/// does NO per-key allocation.
fn run_write_throughput(
    skv_rt: Option<&tokio::runtime::Runtime>,
    engine: Engine,
    compression: Compression,
    inputs: &WorkloadInputs,
) -> Result<Duration, Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let start = std::time::Instant::now();
    let elapsed = match engine {
        Engine::Ours | Engine::BlobTree => {
            let tree = open_ours(
                dir.path(),
                compression,
                engine.kv_separated(),
                IndexStrategy::Binary,
                false,
            )?;
            // Zip the seqno counter as a native `u64` instead of
            // enumerate()+try_from(usize). lsm-tree's `insert` takes
            // SeqNo (= u64) directly; using `0u64..` avoids the
            // per-iteration `usize -> u64` checked-cast that the
            // RocksDB arm doesn't pay, keeping the timed inner loops
            // structurally symmetric. The counter is bounded by
            // `WorkloadInputs::build(n_keys: u64)` so it can never
            // overflow within the iteration.
            for ((key, value), seqno) in inputs.keys.iter().zip(inputs.values.iter()).zip(0u64..) {
                tree.insert(key, value, seqno);
            }
            tree.flush_active_memtable(0)?;
            // Capture BEFORE `tree` drops so close-time background
            // work doesn't leak into the timed window.
            start.elapsed()
        }
        Engine::RocksDb => {
            // Bloom (10 bits/key) + 16 MiB cache + no compression, matching
            // our engine's defaults — see `rocksdb_options`. Our engine
            // builds a bloom filter at flush, so giving RocksDB the same
            // keeps the write comparison apples-to-apples.
            let opts = rocksdb_options(compression, false);
            // Match our engine's durability shape: lsm-tree has no
            // WAL — durability is the caller's responsibility, and
            // `flush_active_memtable` is the equivalent of an
            // explicit fsync barrier. Configure RocksDB to NOT
            // double-write the WAL on each `put` so the head-to-head
            // measures the same kind of work (memtable insert +
            // terminal flush) rather than penalising RocksDB for
            // its built-in WAL.
            let db = rocksdb::DB::open(&opts, dir.path())?;
            let mut write_opts = rocksdb::WriteOptions::default();
            write_opts.disable_wal(true);
            for (key, value) in inputs.keys.iter().zip(inputs.values.iter()) {
                db.put_opt(key, value, &write_opts)?;
            }
            db.flush()?;
            // Capture BEFORE `db` drops so close-time background
            // work doesn't leak into the timed window.
            start.elapsed()
        }
        Engine::SurrealKv => {
            // One write transaction, committed with Immediate durability
            // (fsync) — the flush barrier equivalent of the other engines'
            // terminal flush. `commit()` is async; driven via the runtime.
            // The runtime is prebuilt once per variant (see
            // `write_throughput_variant`) and borrowed here so tokio executor
            // bootstrap is NOT charged to the timed write window.
            let rt =
                skv_rt.expect("surrealkv runtime must be prebuilt for None-compression benches");
            let tree = open_surrealkv(rt, dir.path())?;
            let mut txn = tree.begin()?;
            for (key, value) in inputs.keys.iter().zip(inputs.values.iter()) {
                txn.set(key.as_slice(), value.as_slice())?;
            }
            txn.set_durability(SkvDurability::Immediate);
            rt.block_on(txn.commit())?;
            start.elapsed()
        }
    };
    drop(dir);
    Ok(elapsed)
}

fn bench_write_throughput(c: &mut Criterion) {
    // `None` baseline + `Zstd22` high-ratio variant, each in its own
    // criterion group so the existing baseline charts stay intact and
    // the zstd path lands as a sibling group on the dashboard.
    write_throughput_variant(c, "write_throughput", Compression::None);
    write_throughput_variant(c, "write_throughput_zstd22", Compression::Zstd22);
}

fn write_throughput_variant(c: &mut Criterion, group_name: &str, compression: Compression) {
    let mut group = c.benchmark_group(group_name);
    // SurrealKV participates only in the None-compression group (no zstd
    // codec). Build its tokio runtime ONCE here, outside the timed write
    // window, and reuse it across every sample: `Runtime::new` spins up worker
    // threads, and charging that executor bootstrap to each timed write sample
    // would bias surrealkv's throughput downward vs the other engines (which
    // only pay open/write/flush). `None` for the zstd group where it doesn't run.
    let skv_rt = match compression {
        Compression::None => Some(skv_runtime().expect("surrealkv: tokio runtime")),
        Compression::Zstd22 => None,
    };
    for &n in &[1_000_u64, 10_000, 70_000] {
        // Precompute the keys + values ONCE per `n` (outside the
        // criterion warmup / measurement loop), so the timed body
        // does no per-iteration allocation.
        let inputs = WorkloadInputs::build(n);
        group.throughput(Throughput::Elements(n));
        for &engine in engines_for(compression) {
            group.bench_with_input(BenchmarkId::new(engine.label(), n), &n, |b, _| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        // Criterion's `iter_custom` closure must
                        // return a `Duration`, not a `Result`.
                        // `run_write_throughput` returns
                        // `Result<Duration, ...>` so the engine
                        // helpers themselves use `?` propagation
                        // throughout, but at this boundary an I/O
                        // failure invalidates the run — there is
                        // no meaningful Duration to report — so
                        // surface it as a bench panic with the
                        // engine label for diagnosis.
                        total +=
                            run_write_throughput(skv_rt.as_ref(), engine, compression, &inputs)
                                .unwrap_or_else(|e| {
                                    panic!(
                                        "run_write_throughput failed for {}: {e}",
                                        engine.label()
                                    )
                                });
                    }
                    total
                });
            });
        }
    }
    group.finish();
}

/// Workload: point-read every key from an engine pre-populated with
/// `inputs.keys.len()` keys and flushed to disk.
///
/// In contrast to [`run_write_throughput`]'s cold-start measurement,
/// the engine here is opened ONCE on a copy of its populated and flushed
/// seed state ([`SeedStates`]), outside the criterion timing window, and
/// kept warm for the whole benchmark. The timed body issues one `get` per
/// stored key, so the number reflects warm steady-state read latency (lookup
/// path + bloom filter + block decode), NOT the open / write / flush setup
/// cost.
///
/// Note this is a CACHE-WARM read: the engine stays open across the
/// criterion warmup and measurement sweeps, so after the first pass
/// the working set is largely block-cache resident (both engines use
/// their default cache; lsm-tree's is 16 MiB). The number is "read a
/// resident key", not "fault a block in from disk" — forcing cold
/// misses would need per-iteration cache capping/clearing, which a
/// future `point_read_cold` variant can add.
///
/// Keys are read in insertion order (the `inputs.keys` `Vec` order),
/// which is NOT the on-disk sorted order the engine stores them in
/// after flush. Because `key_for` spreads keys quasi-randomly across
/// the keyspace, iterating them in insertion order still produces a
/// scattered on-disk access pattern (realistic for the bloom filter
/// and block cache) without a per-iteration shuffle.
///
/// Apples-to-apples configuration matches [`run_write_throughput`] via
/// [`rocksdb_options`]: compression `None`, a matching 10-bits/key bloom
/// filter, and a 16 MiB block cache on both sides, so the bloom probe and
/// cache behaviour the latency claim above describes apply to RocksDB too
/// (not just our engine). RocksDB writes with the WAL disabled during the
/// (untimed) populate phase. Reads themselves take no special options on
/// either engine.
///
/// Setup failures (open / insert / flush) and read failures panic
/// with the engine label: a benchmark that can't populate or read
/// the database has no meaningful Duration to report. The "every key
/// is present" invariant is checked ONCE before the timed window (so
/// a broken setup fails loudly) and the timed loop itself stays a
/// bare `get` + `black_box` with no per-read branch.
fn point_read_variant(
    c: &mut Criterion,
    group_name: &str,
    compression: Compression,
    hash_overlays: bool,
    seeds: &mut SeedStates,
) {
    // Series overlaid on this ONE chart: every base engine with binary-search
    // data-block index, plus — when `hash_overlays` is set — the data-block
    // hash index (ours + RocksDB) and the retrieval-ribbon locator (ours only)
    // as additional index strategies on the same chart.
    // Tuple: (label, engine, index strategy, row_cache). The row cache is a cache
    // property orthogonal to the index strategy, so it rides as a 4th field.
    let mut series: Vec<(&str, Engine, IndexStrategy, bool)> = engines_for(compression)
        .iter()
        .map(|&engine| (engine.label(), engine, IndexStrategy::Binary, false))
        .collect();
    if hash_overlays {
        series.push((
            "ours-hash-index",
            Engine::Ours,
            IndexStrategy::HashIndex,
            false,
        ));
        series.push((
            "rocksdb-hash-index",
            Engine::RocksDb,
            IndexStrategy::HashIndex,
            false,
        ));
        series.push(("ours-ribbon", Engine::Ours, IndexStrategy::Ribbon, false));
        // Row cache: binary-search index + a key->value cache in front, so a
        // repeat point read skips the index walk + data-block decode entirely.
        series.push(("ours-row-cache", Engine::Ours, IndexStrategy::Binary, true));
    }

    let mut group = c.benchmark_group(group_name);
    for &n in read_sizes(compression) {
        let inputs = WorkloadInputs::build(n);
        group.throughput(Throughput::Elements(n));
        for &(label, engine, strategy, row_cache) in &series {
            // Built once per arm on the first closure entry (see `WarmEngine`):
            // the criterion warm-up / measurement loop only ever pays for
            // reads, never for open / write / flush.
            let mut warm: Option<WarmEngine> = None;
            group.bench_with_input(BenchmarkId::new(label, n), &n, |b, _| {
                let warm = warm.get_or_insert_with(|| {
                    let warm =
                        WarmEngine::build(engine, compression, strategy, row_cache, &inputs, seeds);
                    // One-time hit check OUTSIDE the timed window: enforce the
                    // workload contract ("read every stored key") so a
                    // setup/flush regression can't silently become a miss-read
                    // benchmark, without taxing each timed `get` with a branch.
                    // `MAX_SEQNO` (not `u64::MAX`, whose MSB is reserved) reads
                    // the latest visible version.
                    match &warm {
                        WarmEngine::Ours { tree, .. } => {
                            for key in &inputs.keys {
                                assert!(
                                    tree.get(key, MAX_SEQNO).expect("ours: verify").is_some(),
                                    "ours: key unexpectedly missing"
                                );
                            }
                        }
                        WarmEngine::RocksDb { db, .. } => {
                            for key in &inputs.keys {
                                assert!(
                                    db.get_pinned(key).expect("rocksdb: verify").is_some(),
                                    "rocksdb: key unexpectedly missing"
                                );
                            }
                        }
                        WarmEngine::SurrealKv { tree, .. } => {
                            let txn = tree.begin().expect("surrealkv: begin");
                            for key in &inputs.keys {
                                assert!(
                                    txn.get(key.as_slice())
                                        .expect("surrealkv: verify")
                                        .is_some(),
                                    "surrealkv: key unexpectedly missing"
                                );
                            }
                        }
                    }
                    warm
                });
                match warm {
                    WarmEngine::Ours { tree, .. } => {
                        b.iter_custom(|iters| {
                            let start = std::time::Instant::now();
                            for _ in 0..iters {
                                for key in &inputs.keys {
                                    let got = tree.get(key, MAX_SEQNO).expect("ours: get");
                                    std::hint::black_box(got);
                                }
                            }
                            start.elapsed()
                        });
                    }
                    WarmEngine::RocksDb { db, .. } => {
                        // `get_pinned` hands back a view of the block, as our
                        // `get` does; plain `get` would copy every value into a
                        // fresh `Vec` and charge RocksDB an allocation ours
                        // never pays.
                        b.iter_custom(|iters| {
                            let start = std::time::Instant::now();
                            for _ in 0..iters {
                                for key in &inputs.keys {
                                    let got = db.get_pinned(key).expect("rocksdb: get");
                                    std::hint::black_box(got);
                                }
                            }
                            start.elapsed()
                        });
                    }
                    WarmEngine::SurrealKv { tree, .. } => {
                        // One read snapshot per closure entry, reused across its
                        // iterations, the closest analogue of the other engines'
                        // direct warm reads (a consistent view, no per-get txn
                        // churn). `begin` is microseconds, so re-taking it on
                        // each entry costs nothing measurable.
                        let txn = tree.begin().expect("surrealkv: begin");
                        b.iter_custom(|iters| {
                            let start = std::time::Instant::now();
                            for _ in 0..iters {
                                for key in &inputs.keys {
                                    let got = txn.get(key.as_slice()).expect("surrealkv: get");
                                    std::hint::black_box(got);
                                }
                            }
                            start.elapsed()
                        });
                    }
                }
            });
        }
    }
    group.finish();
}

/// Opens the RocksDB instance at `dir`, new or seeded, with the matched
/// options. The one open path for every arm that reads or overwrites a seeded
/// state, so a seed is reopened exactly as it was written.
fn open_rocksdb(dir: &std::path::Path, compression: Compression, hash_index: bool) -> rocksdb::DB {
    let opts = rocksdb_options(compression, hash_index);
    // Open through a column-family descriptor that carries the SAME options:
    // the default CF is what every read hits, and the descriptor form is what
    // makes `cf_handle(DEFAULT)` exist for `batched_multi_get_cf`. `DB::open_cf`
    // is deliberately not used: it gives each named CF `Options::default()`, so
    // the compression / bloom / cache configured above would silently not apply
    // to the data.
    let default_cf =
        rocksdb::ColumnFamilyDescriptor::new(rocksdb::DEFAULT_COLUMN_FAMILY_NAME, opts.clone());
    rocksdb::DB::open_cf_descriptors(&opts, dir, [default_cf]).expect("rocksdb: open")
}

/// Opens a RocksDB instance at `dir` with the matched options, populates
/// it with `inputs` (WAL disabled, matching the untimed populate phase of
/// our seeded scenarios), and flushes. Writes RocksDB's [`SeedStates`].
fn populate_rocksdb(
    dir: &std::path::Path,
    compression: Compression,
    hash_index: bool,
    inputs: &WorkloadInputs,
) -> rocksdb::DB {
    let db = open_rocksdb(dir, compression, hash_index);
    let mut write_opts = rocksdb::WriteOptions::default();
    write_opts.disable_wal(true);
    for (key, value) in inputs.keys.iter().zip(inputs.values.iter()) {
        db.put_opt(key, value, &write_opts).expect("rocksdb: put");
    }
    db.flush().expect("rocksdb: flush");
    db
}

/// Populates our engine at `dir` and flushes, returning the handle.
/// Companion to [`populate_rocksdb`] for our [`SeedStates`]. `kv_separated`
/// selects the `blob_tree` (KV-separated) configuration; `strategy` /
/// `row_cache` select the `point_read` index-strategy series.
fn populate_ours(
    dir: &std::path::Path,
    compression: Compression,
    inputs: &WorkloadInputs,
    kv_separated: bool,
    strategy: IndexStrategy,
    row_cache: bool,
) -> lsm_tree::AnyTree {
    let tree = open_ours(dir, compression, kv_separated, strategy, row_cache).expect("ours: open");
    for ((key, value), seqno) in inputs.keys.iter().zip(inputs.values.iter()).zip(0u64..) {
        tree.insert(key, value, seqno);
    }
    tree.flush_active_memtable(0).expect("ours: flush");
    tree
}

/// What one seeded on-disk state is written with: the engine, the codec, the
/// index strategy and the key count. The row cache is not part of it: it lives
/// in memory only, so the arms with and without it start from the same files.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct SeedKey {
    engine: Engine,
    compression: Compression,
    strategy: IndexStrategy,
    n: usize,
}

/// The key set written once and flushed, per [`SeedKey`], closed, and copied
/// into a fresh directory for every read arm and every overwrite iteration
/// that starts from it.
///
/// Every read group and `overwrite` starts from that same state, and writing
/// it is the expensive part at zstd-22: RocksDB takes ~47 s to write 70k rows
/// and ~9 s for 10k on the bench runner, against well under a second for the
/// reads that follow. Written per arm, the four read groups would pay it four
/// times and `overwrite` once per iteration, eleven times per arm. A copy
/// reopens the files the engine flushed and closed, so a timed read or
/// overwrite acts on the same on-disk state as on the engine that wrote it;
/// the caches it starts with differ, and Criterion's warm-up pass fills them
/// before any sample is taken.
///
/// SurrealKV is not seeded. It runs only in the uncompressed groups, where
/// writing the key set takes a fraction of a second, and a reopened SurrealKV
/// starts from whatever its recovery rebuilds rather than from the state its
/// writer left, which would change what its arms measure.
#[derive(Default)]
struct SeedStates {
    dirs: HashMap<SeedKey, tempfile::TempDir>,
}

impl SeedStates {
    /// A fresh directory holding a copy of the state `inputs` seeds for this
    /// engine, codec and index strategy, written on first use.
    fn checkout(
        &mut self,
        engine: Engine,
        compression: Compression,
        strategy: IndexStrategy,
        inputs: &WorkloadInputs,
    ) -> tempfile::TempDir {
        let key = SeedKey {
            engine,
            compression,
            strategy,
            n: inputs.keys.len(),
        };
        let seed = self
            .dirs
            .entry(key)
            .or_insert_with(|| write_seed(key, inputs));
        let dir = tempfile::tempdir().expect("tempdir");
        copy_dir(seed.path(), dir.path()).expect("copy seed state");
        dir
    }
}

/// Writes the seed state for `key` into a new directory and closes the engine
/// before returning, so the files are complete before anything copies them.
fn write_seed(key: SeedKey, inputs: &WorkloadInputs) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("seed tempdir");
    match key.engine {
        Engine::Ours | Engine::BlobTree => drop(populate_ours(
            dir.path(),
            key.compression,
            inputs,
            key.engine.kv_separated(),
            key.strategy,
            false,
        )),
        Engine::RocksDb => drop(populate_rocksdb(
            dir.path(),
            key.compression,
            key.strategy == IndexStrategy::HashIndex,
            inputs,
        )),
        Engine::SurrealKv => unreachable!("surrealkv writes its own state per arm"),
    }
    dir
}

/// The groups that start from a written and flushed key set: the warm reads
/// and `overwrite`. They share one [`SeedStates`], so each state is written
/// once for all of them. Each runs a `None` baseline and a `Zstd22` sibling
/// group.
fn bench_seeded(c: &mut Criterion) {
    let mut seeds = SeedStates::default();
    // The `point_read` group ADDS the hash-index series (`ours-hash-index`,
    // `rocksdb-hash-index`) as extra overlays ON THE SAME chart alongside the
    // binary-search `ours` / `rocksdb` lines, so one chart shows both index
    // strategies head-to-head.
    point_read_variant(c, "point_read", Compression::None, true, &mut seeds);
    point_read_variant(
        c,
        "point_read_zstd22",
        Compression::Zstd22,
        false,
        &mut seeds,
    );
    multi_get_variant(c, "multi_get", Compression::None, &mut seeds);
    multi_get_variant(c, "multi_get_zstd22", Compression::Zstd22, &mut seeds);
    range_scan_variant(c, "range_scan", Compression::None, &mut seeds);
    range_scan_variant(c, "range_scan_zstd22", Compression::Zstd22, &mut seeds);
    seek_random_variant(c, "seek_random", Compression::None, &mut seeds);
    seek_random_variant(c, "seek_random_zstd22", Compression::Zstd22, &mut seeds);
    overwrite_variant(c, "overwrite", Compression::None, &mut seeds);
    overwrite_variant(c, "overwrite_zstd22", Compression::Zstd22, &mut seeds);
}

/// Warm on-disk state for one read arm (`point_read`, `multi_get`,
/// `range_scan`, `seek_random`): the engine opened on a copy of its seed
/// state and the directory holding the copy.
///
/// Criterion re-enters a `bench_with_input` routine closure for the warm-up
/// pass and again for EVERY sample, so state built inside the closure is
/// rebuilt once per sample. Each arm keeps one `Option<WarmEngine>` outside
/// its closure and fills it on the first entry, so the closure only ever
/// times reads.
///
/// Field order is drop order: the engine closes before its directory is
/// removed, and the SurrealKV runtime outlives the tree so the background
/// tasks `build()` spawned stay alive for the whole read phase.
enum WarmEngine {
    Ours {
        tree: lsm_tree::AnyTree,
        _dir: tempfile::TempDir,
    },
    RocksDb {
        db: rocksdb::DB,
        _dir: tempfile::TempDir,
    },
    SurrealKv {
        tree: SkvTree,
        _rt: tokio::runtime::Runtime,
        _dir: tempfile::TempDir,
    },
}

impl WarmEngine {
    /// Builds the warm state for `engine` with the matched per-engine
    /// configuration (`compression` on both; `strategy` selects the data-block
    /// hash index on both ours and RocksDB, ribbon / `row_cache` are ours-only).
    fn build(
        engine: Engine,
        compression: Compression,
        strategy: IndexStrategy,
        row_cache: bool,
        inputs: &WorkloadInputs,
        seeds: &mut SeedStates,
    ) -> Self {
        match engine {
            Engine::Ours | Engine::BlobTree => {
                let dir = seeds.checkout(engine, compression, strategy, inputs);
                let tree = open_ours(
                    dir.path(),
                    compression,
                    engine.kv_separated(),
                    strategy,
                    row_cache,
                )
                .expect("ours: open");
                Self::Ours { tree, _dir: dir }
            }
            Engine::RocksDb => {
                let dir = seeds.checkout(engine, compression, strategy, inputs);
                let db = open_rocksdb(
                    dir.path(),
                    compression,
                    strategy == IndexStrategy::HashIndex,
                );
                Self::RocksDb { db, _dir: dir }
            }
            Engine::SurrealKv => {
                let dir = tempfile::tempdir().expect("tempdir");
                let (rt, tree) = setup_surrealkv_warm(dir.path(), inputs)
                    .unwrap_or_else(|e| panic!("surrealkv: warm setup: {e}"));
                Self::SurrealKv {
                    tree,
                    _rt: rt,
                    _dir: dir,
                }
            }
        }
    }
}

/// Batched read head-to-head: one `multi_get` call resolves the whole key set,
/// versus the per-key `point_read` loop in [`point_read_variant`]. This is where
/// our batched read path (one bloom probe and one data-block decode shared by the
/// co-located keys of each table) meets RocksDB's optimized batched MultiGet
/// (`batched_multi_get_cf`, not the legacy per-key `multi_get`). At `n = 70k` the
/// working set exceeds the 16 MiB block cache, so the batch's blocks are cold.
///
/// Apples-to-apples matches [`point_read_variant`]: identical [`WorkloadInputs`],
/// the same seeded state and matched compression / bloom / 16 MiB cache
/// ([`SeedStates`]). The only difference from `point_read` is one batched
/// call instead of an N-iteration `get` loop. SurrealKV has no batch-get API, so
/// it is omitted here (its sequential cost is already on the `point_read` chart);
/// the series is `ours` vs `rocksdb` (plus `blob_tree` on the None variant).
fn multi_get_variant(
    c: &mut Criterion,
    group_name: &str,
    compression: Compression,
    seeds: &mut SeedStates,
) {
    // Only engines with a real batch-get API overlay here (SurrealKV has none).
    let series: Vec<(&str, Engine)> = engines_for(compression)
        .iter()
        .copied()
        .filter(|engine| !matches!(engine, Engine::SurrealKv))
        .map(|engine| (engine.label(), engine))
        .collect();

    let mut group = c.benchmark_group(group_name);
    for &n in read_sizes(compression) {
        let inputs = WorkloadInputs::build(n);
        group.throughput(Throughput::Elements(n));
        for &(label, engine) in &series {
            // This measures WARM steady-state batched-MultiGet throughput, NOT
            // cold first-touch latency: every engine opens its seed and probes
            // once per arm outside the timed window (see `WarmEngine`), so all
            // arms (ours, blob_tree, rocksdb) enter the loop equally warmed.
            // That symmetry is the point of the comparison. Cold fan-out
            // latency is a separate, OS-cache-dropping measurement, not this
            // bench.
            let mut warm: Option<WarmEngine> = None;
            group.bench_with_input(BenchmarkId::new(label, n), &n, |b, _| {
                let warm = warm.get_or_insert_with(|| {
                    let warm = WarmEngine::build(
                        engine,
                        compression,
                        IndexStrategy::Binary,
                        false,
                        &inputs,
                        seeds,
                    );
                    // One-time "every key present" contract check OUTSIDE the
                    // timed window (mirrors point_read), so a setup regression
                    // can't quietly become a miss-read benchmark. Cardinality
                    // before presence: a batched API that dropped positions
                    // would otherwise pass the presence check while the timed
                    // loop measures fewer than `n` lookups.
                    match &warm {
                        WarmEngine::Ours { tree, .. } => {
                            let probe = tree
                                .multi_get(inputs.keys.iter(), MAX_SEQNO)
                                .expect("ours: verify");
                            assert_eq!(
                                probe.len(),
                                inputs.keys.len(),
                                "ours: multi_get must return one result per input key"
                            );
                            assert!(
                                probe.iter().all(Option::is_some),
                                "ours: key unexpectedly missing"
                            );
                        }
                        WarmEngine::RocksDb { db, .. } => {
                            let cf = db
                                .cf_handle(rocksdb::DEFAULT_COLUMN_FAMILY_NAME)
                                .expect("rocksdb: default cf");
                            let probe = db.batched_multi_get_cf(&cf, inputs.keys.iter(), false);
                            assert_eq!(
                                probe.len(),
                                inputs.keys.len(),
                                "rocksdb: batched_multi_get_cf must return one result per input key"
                            );
                            assert!(
                                probe.iter().all(|r| matches!(r, Ok(Some(_)))),
                                "rocksdb: key unexpectedly missing"
                            );
                        }
                        WarmEngine::SurrealKv { .. } => {
                            unreachable!("surrealkv is filtered out of the multi_get series")
                        }
                    }
                    warm
                });
                match warm {
                    WarmEngine::Ours { tree, .. } => {
                        b.iter_custom(|iters| {
                            let start = std::time::Instant::now();
                            for _ in 0..iters {
                                let got = tree
                                    .multi_get(inputs.keys.iter(), MAX_SEQNO)
                                    .expect("ours: multi_get");
                                std::hint::black_box(got);
                            }
                            start.elapsed()
                        });
                    }
                    WarmEngine::RocksDb { db, .. } => {
                        // `batched_multi_get_cf` is RocksDB's OPTIMIZED batched
                        // MultiGet (batched bloom probes + coalesced block reads,
                        // NOT the legacy per-key `multi_get`); it needs the CF
                        // handle `open_rocksdb`'s descriptor open provides.
                        // `sorted_input = false`: keys arrive in insertion order
                        // and RocksDB sorts internally, exactly as ours does.
                        let cf = db
                            .cf_handle(rocksdb::DEFAULT_COLUMN_FAMILY_NAME)
                            .expect("rocksdb: default cf");
                        b.iter_custom(|iters| {
                            let start = std::time::Instant::now();
                            for _ in 0..iters {
                                let got = db.batched_multi_get_cf(&cf, inputs.keys.iter(), false);
                                std::hint::black_box(got);
                            }
                            start.elapsed()
                        });
                    }
                    WarmEngine::SurrealKv { .. } => {
                        unreachable!("surrealkv is filtered out of the multi_get series")
                    }
                }
            });
        }
    }
    group.finish();
}

/// Workload: full forward scan reading every value. The engine is
/// opened ONCE on its seed state outside the timed window (warm, like
/// [`point_read_variant`]); the timed body iterates the whole keyspace
/// front-to-back and touches each value, so the number reflects
/// steady-state sequential-scan throughput (block decode + iterator
/// advance), not setup cost.
fn range_scan_variant(
    c: &mut Criterion,
    group_name: &str,
    compression: Compression,
    seeds: &mut SeedStates,
) {
    let mut group = c.benchmark_group(group_name);
    for &n in read_sizes(compression) {
        let inputs = WorkloadInputs::build(n);
        group.throughput(Throughput::Elements(n));
        for &engine in engines_for(compression) {
            // Built once per arm on the first closure entry (see `WarmEngine`).
            let mut warm: Option<WarmEngine> = None;
            group.bench_with_input(BenchmarkId::new(engine.label(), n), &n, |b, _| {
                let warm = warm.get_or_insert_with(|| {
                    WarmEngine::build(
                        engine,
                        compression,
                        IndexStrategy::Binary,
                        false,
                        &inputs,
                        seeds,
                    )
                });
                match warm {
                    WarmEngine::Ours { tree, .. } => {
                        b.iter_custom(|iters| {
                            let start = std::time::Instant::now();
                            for _ in 0..iters {
                                for guard in tree.iter(MAX_SEQNO, None) {
                                    let v = guard.value().expect("ours: scan value");
                                    std::hint::black_box(v);
                                }
                            }
                            start.elapsed()
                        });
                    }
                    WarmEngine::RocksDb { db, .. } => {
                        // The raw iterator lends each value out of the block, as
                        // ours does; the boxed iterator would copy every key and
                        // value into fresh allocations on RocksDB's side only.
                        b.iter_custom(|iters| {
                            let start = std::time::Instant::now();
                            for _ in 0..iters {
                                let mut it = db.raw_iterator();
                                it.seek_to_first();
                                while it.valid() {
                                    std::hint::black_box(it.value());
                                    it.next();
                                }
                                it.status().expect("rocksdb: scan");
                            }
                            start.elapsed()
                        });
                    }
                    WarmEngine::SurrealKv { tree, .. } => {
                        let txn = tree.begin().expect("surrealkv: begin");
                        b.iter_custom(|iters| {
                            let start = std::time::Instant::now();
                            for _ in 0..iters {
                                let mut iter = txn
                                    .range(SKV_MIN_KEY, SKV_MAX_KEY)
                                    .expect("surrealkv: range");
                                iter.seek_first().expect("surrealkv: seek_first");
                                while iter.valid() {
                                    let v = iter.value().expect("surrealkv: scan value");
                                    std::hint::black_box(v);
                                    iter.next().expect("surrealkv: scan next");
                                }
                            }
                            start.elapsed()
                        });
                    }
                }
            });
        }
    }
    group.finish();
}

/// Workload: seek to each key (in insertion order, i.e. scattered across
/// the sorted keyspace) and read the single value the cursor lands on.
/// Warm: the engine is opened ONCE on its seed state outside the timed
/// window. This measures seek-then-read latency (index descent + block
/// decode + cursor positioning), the closest head-to-head analogue of a
/// `seekrandom` workload.
fn seek_random_variant(
    c: &mut Criterion,
    group_name: &str,
    compression: Compression,
    seeds: &mut SeedStates,
) {
    let mut group = c.benchmark_group(group_name);
    for &n in read_sizes(compression) {
        let inputs = WorkloadInputs::build(n);
        group.throughput(Throughput::Elements(n));
        for &engine in engines_for(compression) {
            // Built once per arm on the first closure entry (see `WarmEngine`).
            let mut warm: Option<WarmEngine> = None;
            group.bench_with_input(BenchmarkId::new(engine.label(), n), &n, |b, _| {
                let warm = warm.get_or_insert_with(|| {
                    WarmEngine::build(
                        engine,
                        compression,
                        IndexStrategy::Binary,
                        false,
                        &inputs,
                        seeds,
                    )
                });
                match warm {
                    WarmEngine::Ours { tree, .. } => {
                        b.iter_custom(|iters| {
                            let start = std::time::Instant::now();
                            for _ in 0..iters {
                                for key in &inputs.keys {
                                    let lo: &[u8] = key;
                                    let got = tree
                                        .range(lo.., MAX_SEQNO, None)
                                        .next()
                                        .map(|g| g.value().expect("ours: seek value"));
                                    std::hint::black_box(got);
                                }
                            }
                            start.elapsed()
                        });
                    }
                    WarmEngine::RocksDb { db, .. } => {
                        // A fresh iterator per seek, as ours opens a fresh range;
                        // the raw iterator lends the value instead of copying it.
                        b.iter_custom(|iters| {
                            let start = std::time::Instant::now();
                            for _ in 0..iters {
                                for key in &inputs.keys {
                                    let mut it = db.raw_iterator();
                                    it.seek(key);
                                    std::hint::black_box(it.value());
                                    it.status().expect("rocksdb: seek");
                                }
                            }
                            start.elapsed()
                        });
                    }
                    WarmEngine::SurrealKv { tree, .. } => {
                        let txn = tree.begin().expect("surrealkv: begin");
                        b.iter_custom(|iters| {
                            let start = std::time::Instant::now();
                            for _ in 0..iters {
                                for key in &inputs.keys {
                                    // Seek to the first key >= this key, read its
                                    // value — the SurrealKV analogue of the
                                    // index-descent-then-read the other engines do.
                                    let mut it = txn
                                        .range(key.as_slice(), SKV_MAX_KEY)
                                        .expect("surrealkv: seek range");
                                    it.seek_first().expect("surrealkv: seek_first");
                                    let got = if it.valid() {
                                        Some(it.value().expect("surrealkv: seek value"))
                                    } else {
                                        None
                                    };
                                    std::hint::black_box(got);
                                }
                            }
                            start.elapsed()
                        });
                    }
                }
            });
        }
    }
    group.finish();
}

/// Workload: rewrite the entire keyspace into an engine that already
/// holds one copy of it. The first copy is the seed state ([`SeedStates`]),
/// written OUTSIDE the timed window; the timed body writes every key a
/// second time and flushes, so the number reflects overwrite cost (memtable
/// churn over existing keys + a flush that supersedes prior versions) rather
/// than cold first-insert cost. Every timed iteration opens a fresh copy of
/// the seed, so each measurement starts from the same one-copy state.
fn overwrite_variant(
    c: &mut Criterion,
    group_name: &str,
    compression: Compression,
    seeds: &mut SeedStates,
) {
    let mut group = c.benchmark_group(group_name);
    for &n in &[1_000_u64, 10_000, 70_000] {
        let inputs = WorkloadInputs::build(n);
        group.throughput(Throughput::Elements(n));
        for &engine in engines_for(compression) {
            group.bench_with_input(BenchmarkId::new(engine.label(), n), &n, |b, _| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        match engine {
                            Engine::Ours | Engine::BlobTree => {
                                // First copy (untimed): the seed, so the timed
                                // pass overwrites existing keys.
                                let dir = seeds.checkout(
                                    engine,
                                    compression,
                                    IndexStrategy::Binary,
                                    &inputs,
                                );
                                let tree = open_ours(
                                    dir.path(),
                                    compression,
                                    engine.kv_separated(),
                                    IndexStrategy::Binary,
                                    false,
                                )
                                .expect("ours: open");
                                let start = std::time::Instant::now();
                                // Second seqno range so the overwrite produces a
                                // newer version of every key.
                                for ((key, value), seqno) in
                                    inputs.keys.iter().zip(inputs.values.iter()).zip(n..)
                                {
                                    tree.insert(key, value, seqno);
                                }
                                tree.flush_active_memtable(0)
                                    .expect("ours: overwrite flush");
                                total += start.elapsed();
                            }
                            Engine::RocksDb => {
                                let dir = seeds.checkout(
                                    engine,
                                    compression,
                                    IndexStrategy::Binary,
                                    &inputs,
                                );
                                let db = open_rocksdb(dir.path(), compression, false);
                                let mut write_opts = rocksdb::WriteOptions::default();
                                write_opts.disable_wal(true);
                                let start = std::time::Instant::now();
                                for (key, value) in inputs.keys.iter().zip(inputs.values.iter()) {
                                    db.put_opt(key, value, &write_opts)
                                        .expect("rocksdb: overwrite put");
                                }
                                db.flush().expect("rocksdb: overwrite flush");
                                total += start.elapsed();
                            }
                            Engine::SurrealKv => {
                                // First copy (untimed) so the timed pass
                                // overwrites existing keys; SurrealKV is not
                                // seeded (see `SeedStates`).
                                let dir = tempfile::tempdir().expect("tempdir");
                                let (rt, tree) = setup_surrealkv_warm(dir.path(), &inputs)
                                    .unwrap_or_else(|e| panic!("surrealkv: warm setup: {e}"));
                                let start = std::time::Instant::now();
                                let mut txn = tree.begin().expect("surrealkv: begin");
                                for (key, value) in inputs.keys.iter().zip(inputs.values.iter()) {
                                    txn.set(key.as_slice(), value.as_slice())
                                        .expect("surrealkv: overwrite set");
                                }
                                txn.set_durability(SkvDurability::Immediate);
                                rt.block_on(txn.commit())
                                    .expect("surrealkv: overwrite commit");
                                total += start.elapsed();
                            }
                        }
                    }
                    total
                });
            });
        }
    }
    group.finish();
}

// P50 / P99 / P999 percentile capture is deferred to a follow-up
// commit. Criterion's default reporter gives mean + CI only,
// which hides tail-latency regressions; structured-zstd's
// `benches/bloom.rs` ports Vitter's Algorithm R reservoir +
// per-iteration `iter_custom` to expose percentiles to stderr,
// and that same pattern wires here once the workload surface is
// fleshed out (YCSB-A/C, bloom negative probes). The cross-engine
// overlay path (each scenario runs both engines in the same process
// so the ratio stays host-independent) and the None/zstd22
// compression axis are in place; readwhilewriting (concurrency) and
// mergerandom (merge-operator semantics differ across engines) are
// the remaining db_bench scenarios not yet portable head-to-head.

/// L0 tables built before timing the compaction. Their key ranges overlap
/// (the golden-ratio key scatter spreads consecutive indices across the
/// keyspace), so neither engine can "trivially move" them to the next level
/// without rewriting — the timed compaction actually merges + recompresses.
const COMPACTION_FLUSHES: u64 = 6;
/// Worker threads for parallel block compression on both engines (ours via
/// `compaction_threads`; RocksDB via `compression_options_parallel_threads`).
const COMPACTION_THREADS: usize = 4;

/// Bottom-level target file size for the split shape's setup: small enough
/// that the populated bottom level holds several tables — the boundaries
/// the timed compaction splits on — on both engines.
const SUBCOMPACTION_BOTTOM_TARGET: u64 = 1024 * 1024;
/// Sub-compaction worker threads (ours: range-parallel split; RocksDB:
/// `max_subcompactions`).
const SUBCOMPACTION_THREADS: usize = 4;

/// The two compaction head-to-heads.
#[derive(Clone, Copy)]
enum CompactionShape {
    /// `COMPACTION_FLUSHES` overlapping L0 tables merged into one level, with
    /// parallel block compression and no range split (RocksDB
    /// `max_subcompactions = 1`), so both engines run the same mechanism.
    Major,
    /// A full-keyspace overwrite in `COMPACTION_FLUSHES` L0 tables merged into a
    /// bottom level of several tables: the range-parallel split (ours
    /// `subcompaction_min_bytes = 0`, so the bottom's table boundaries drive the
    /// partition; RocksDB `max_subcompactions = 4`).
    Split,
}

impl CompactionShape {
    fn threads(self) -> usize {
        match self {
            Self::Major => COMPACTION_THREADS,
            Self::Split => SUBCOMPACTION_THREADS,
        }
    }
}

/// Our tree as the compaction benches open it, on a fresh or a copied
/// directory: zstd at `level` and the shape's threads.
fn open_ours_for_compaction(
    dir: &std::path::Path,
    level: i32,
    shape: CompactionShape,
) -> Result<lsm_tree::AnyTree, Box<dyn std::error::Error>> {
    let config = Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::Zstd(level)))
    .compaction_threads(shape.threads())
    .subcompaction_min_bytes(match shape {
        // No range split, so this shape isolates parallel block compression,
        // matching RocksDB's `max_subcompactions(1)`.
        CompactionShape::Major => u64::MAX,
        CompactionShape::Split => 0,
    });
    Ok(apply_preset(config, active_preset()).open()?)
}

/// RocksDB as the compaction benches open it. The block options are the
/// matched ones every other group uses (see [`rocksdb_options`]): with a bare
/// `Options::default()` RocksDB would write its compaction output without the
/// bloom filter ours builds there, and skip that work.
fn open_rocksdb_for_compaction(
    dir: &std::path::Path,
    level: i32,
    shape: CompactionShape,
) -> Result<rocksdb::DB, Box<dyn std::error::Error>> {
    let mut opts = rocksdb_options(Compression::None, false);
    // Hold every table where the setup put it until the compaction timed.
    opts.set_disable_auto_compactions(true);
    opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
    // (window_bits, level, strategy, max_dict_bytes); -14 = default window.
    opts.set_compression_options(-14, level, 0, 0);
    // Our compaction threads drive both the range split and the
    // block-compression pool, so RocksDB gets both knobs.
    opts.set_compression_options_parallel_threads(shape.threads() as i32);
    match shape {
        CompactionShape::Major => opts.set_max_subcompactions(1),
        CompactionShape::Split => {
            opts.set_max_subcompactions(SUBCOMPACTION_THREADS as u32);
            // Several bottom files, so the split has boundaries to cut on.
            opts.set_target_file_size_base(SUBCOMPACTION_BOTTOM_TARGET);
        }
    }
    Ok(rocksdb::DB::open(&opts, dir)?)
}

/// RocksDB's manual compaction defaults to
/// `bottommost_level_compaction = kIfHaveCompactionFilter`: with no compaction
/// filter it leaves data above an existing bottom level instead of rewriting
/// the bottom, a near no-op, whereas ours' `major_compact` merges everything
/// into the bottom. `Force` makes both engines do the same work.
fn force_bottommost() -> rocksdb::CompactOptions {
    let mut compact_opts = rocksdb::CompactOptions::default();
    compact_opts.set_bottommost_level_compaction(rocksdb::BottommostLevelCompaction::Force);
    compact_opts
}

/// Writes the state a `shape` compaction starts from into `dir` and closes the
/// engine. For [`CompactionShape::Split`] first a bottom level of several
/// tables, then for both shapes the key set again in `COMPACTION_FLUSHES` L0
/// tables, flushed at the interior boundaries and once more after the loop, so
/// floor division leaves no stray remainder table.
fn write_compaction_state(
    engine: Engine,
    level: i32,
    shape: CompactionShape,
    inputs: &WorkloadInputs,
    dir: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let total = inputs.keys.len() as u64;
    let flush_points: Vec<u64> = (1..COMPACTION_FLUSHES)
        .map(|b| (b * total) / COMPACTION_FLUSHES)
        .collect();
    let rows = || inputs.keys.iter().zip(inputs.values.iter());
    match engine {
        Engine::Ours => {
            let tree = open_ours_for_compaction(dir, level, shape)?;
            let mut base = 0;
            if matches!(shape, CompactionShape::Split) {
                for ((key, value), seqno) in rows().zip(0u64..) {
                    tree.insert(key, value, seqno);
                }
                tree.flush_active_memtable(0)?;
                tree.major_compact(SUBCOMPACTION_BOTTOM_TARGET, 0)?;
                base = total;
            }
            // A newer seqno range, so the L0 copy supersedes the bottom one.
            for (written, ((key, value), seqno)) in (1u64..).zip(rows().zip(base..)) {
                tree.insert(key, value, seqno);
                if flush_points.contains(&written) {
                    tree.flush_active_memtable(0)?;
                }
            }
            tree.flush_active_memtable(0)?;
        }
        Engine::RocksDb => {
            let db = open_rocksdb_for_compaction(dir, level, shape)?;
            let mut write_opts = rocksdb::WriteOptions::default();
            write_opts.disable_wal(true);
            if matches!(shape, CompactionShape::Split) {
                for (key, value) in rows() {
                    db.put_opt(key, value, &write_opts)?;
                }
                db.flush()?;
                db.compact_range_opt(None::<&[u8]>, None::<&[u8]>, &force_bottommost());
            }
            for (written, (key, value)) in (1u64..).zip(rows()) {
                db.put_opt(key, value, &write_opts)?;
                if flush_points.contains(&written) {
                    db.flush()?;
                }
            }
            db.flush()?;
        }
        // The compaction benches are zstd-level workloads; SurrealKV has no zstd
        // codec, and blob_tree overlays only the read/write groups, so the
        // compaction loop is fixed to ours+rocksdb. The arms exist only for
        // exhaustiveness.
        Engine::SurrealKv | Engine::BlobTree => {
            unreachable!("the compaction benches run ours and rocksdb only")
        }
    }
    Ok(())
}

/// Times one compaction of the state in `dir`: the open happens before the
/// clock starts, so only the compaction is measured.
fn time_compaction(
    engine: Engine,
    level: i32,
    shape: CompactionShape,
    dir: &std::path::Path,
) -> Result<Duration, Box<dyn std::error::Error>> {
    match engine {
        Engine::Ours => {
            let tree = open_ours_for_compaction(dir, level, shape)?;
            let start = std::time::Instant::now();
            tree.major_compact(u64::MAX, 0)?;
            Ok(start.elapsed())
        }
        Engine::RocksDb => {
            let db = open_rocksdb_for_compaction(dir, level, shape)?;
            let compact_opts = force_bottommost();
            let start = std::time::Instant::now();
            db.compact_range_opt(None::<&[u8]>, None::<&[u8]>, &compact_opts);
            Ok(start.elapsed())
        }
        Engine::SurrealKv | Engine::BlobTree => {
            unreachable!("the compaction benches run ours and rocksdb only")
        }
    }
}

fn bench_compaction(c: &mut Criterion) {
    // Compaction output lands in the bottommost level. RocksDB's manual
    // compaction compresses the bottommost output at zstd's default level (3)
    // regardless of the configured `compression_opts.level` — the level setting
    // does not reach the bottommost level, and the only way to override it
    // (`set_bottommost_compression_options`) cannot carry parallel_threads, so it
    // would force RocksDB single-threaded there. So the honest apples-to-apples
    // compaction codec comparison is pinned to level 3 — the level RocksDB
    // actually performs on the bottommost output — with both engines at 4-thread
    // parallel block compression (RocksDB inherits the 4 threads from
    // `compression_opts`; ours via `compaction_threads`). As structured-zstd's
    // level-3 encoder improves, the gain shows directly against RocksDB here.
    compaction_variant(c, "major_compact_zstd3", 3, CompactionShape::Major);
}

/// Reports compaction tail latency (P50/P95/P99) to stderr from per-iteration
/// durations — Criterion's overlay only plots mean/CI. Each iteration is one
/// whole compaction, so this is the distribution of compaction wall-times.
fn report_percentiles(label: &str, mut samples: Vec<Duration>) {
    if samples.is_empty() {
        return;
    }
    samples.sort_unstable();
    let pick = |p: f64| {
        let idx = (((samples.len() - 1) as f64) * p).round() as usize;
        samples[idx.min(samples.len() - 1)]
    };
    eprintln!(
        "  [{label}] n={} P50={:?} P95={:?} P99={:?}",
        samples.len(),
        pick(0.50),
        pick(0.95),
        pick(0.99),
    );
}

/// One compaction head-to-head: per size and engine, the starting state is
/// written once (see [`write_compaction_state`]) and every iteration compacts
/// a fresh copy of it, so an iteration times its compaction alone.
fn compaction_variant(c: &mut Criterion, group_name: &str, level: i32, shape: CompactionShape) {
    let sizes: &[u64] = match shape {
        CompactionShape::Major => &[10_000, 40_000],
        CompactionShape::Split => &[40_000, 100_000],
    };
    let mut group = c.benchmark_group(group_name);
    for &n in sizes {
        let inputs = match shape {
            CompactionShape::Major => WorkloadInputs::build(n),
            CompactionShape::Split => WorkloadInputs::incompressible(n),
        };
        group.throughput(Throughput::Elements(n));
        for engine in [Engine::Ours, Engine::RocksDb] {
            let mut state: Option<tempfile::TempDir> = None;
            // Collected across every closure entry (Criterion re-enters the
            // routine for warm-up and per sample) and reported ONCE after the
            // arm. Every iteration is an independent full compaction of the
            // same starting state, so the warm-up iterations are the same
            // population as the measured ones and belong in the distribution.
            let mut samples = Vec::new();
            group.bench_with_input(BenchmarkId::new(engine.label(), n), &n, |b, _| {
                let state = state.get_or_insert_with(|| {
                    let dir = tempfile::tempdir().expect("compaction state tempdir");
                    write_compaction_state(engine, level, shape, &inputs, dir.path())
                        .unwrap_or_else(|e| {
                            panic!("compaction setup failed for {}: {e}", engine.label())
                        });
                    dir
                });
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let work = tempfile::tempdir().expect("compaction work tempdir");
                        copy_dir(state.path(), work.path()).expect("copy compaction state");
                        let elapsed = time_compaction(engine, level, shape, work.path())
                            .unwrap_or_else(|e| {
                                panic!("compaction failed for {}: {e}", engine.label())
                            });
                        samples.push(elapsed);
                        total += elapsed;
                    }
                    total
                });
            });
            report_percentiles(&format!("{group_name}/{}/{n}", engine.label()), samples);
        }
    }
    group.finish();
}

/// High-entropy 256-byte value: an xorshift fill so zstd spends the most
/// time it can per block during sub-compaction, which is the work the
/// range-parallel split exists to spread across threads.
fn value_incompressible(i: u64) -> Vec<u8> {
    let mut s = i.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut v = vec![0_u8; VALUE_SIZE];
    for chunk in v.chunks_mut(8) {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        let bytes = s.to_le_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
    v
}

impl WorkloadInputs {
    /// The sub-compaction workload: the same keys with high-entropy values.
    fn incompressible(n_keys: u64) -> Self {
        let n = usize::try_from(n_keys).expect("n_keys fits in usize");
        let mut keys = Vec::with_capacity(n);
        let mut values = Vec::with_capacity(n);
        for i in 0..n_keys {
            keys.push(key_for(i));
            values.push(value_incompressible(i));
        }
        Self { keys, values }
    }
}

/// Recursively copies `src` into `dst` (created if missing): how a seeded or
/// compaction starting state is handed to an arm or an iteration.
fn copy_dir(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let to = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(&entry.path(), &to)?;
        } else {
            std::fs::copy(entry.path(), to)?;
        }
    }
    Ok(())
}

/// Sub-compaction head-to-head: our range-parallel split vs RocksDB
/// `max_subcompactions`. Pinned to zstd level 3 — the level RocksDB actually
/// applies to bottommost compaction output (see [`bench_compaction`]) — with
/// both engines at 4-thread block compression, so the comparison is honest and
/// tracks structured-zstd's level-3 encoder progress against RocksDB.
fn bench_subcompaction(c: &mut Criterion) {
    compaction_variant(c, "subcompaction_zstd3", 3, CompactionShape::Split);
}

criterion_group!(
    benches,
    bench_write_throughput,
    bench_seeded,
    bench_compaction,
    bench_subcompaction
);

/// What each group measures, as the published page states it next to the
/// group's chart. Kept here, beside the code that does the measuring.
const GROUP_NOTES: &[(&str, &str)] = &[
    (
        "write_throughput",
        "Bulk insert of N fresh keys into an empty engine, then one flush. Covers open, memtable inserts and the flush that writes the table.",
    ),
    (
        "overwrite",
        "Every key written a second time into an engine that already holds one copy, then one flush. Memtable churn over existing keys and a superseding flush.",
    ),
    (
        "point_read",
        "One get per stored key on a warm engine. Also our hash index, ribbon locator and row cache, and RocksDB's hash index, on the same chart.",
    ),
    (
        "multi_get",
        "One batched lookup of the whole key set on a warm engine (RocksDB: batched_multi_get_cf).",
    ),
    (
        "range_scan",
        "A full forward scan reading every value on a warm engine.",
    ),
    (
        "seek_random",
        "A seek to each key in scattered order, reading the value under the cursor.",
    ),
    (
        "major_compact_zstd3",
        "Six overlapping L0 tables merged into one level, 4-thread block compression, no range split. zstd level 3, the level RocksDB applies to bottommost output.",
    ),
    (
        "subcompaction_zstd3",
        "A full overwrite in six L0 tables merged into a bottom level of several tables, split into 4 parallel ranges. High-entropy values. zstd level 3.",
    ),
];

/// Where Criterion keeps its results for this run: `CRITERION_HOME` when set,
/// otherwise `criterion/` under the cargo target directory.
fn criterion_home() -> std::path::PathBuf {
    if let Some(home) = std::env::var_os("CRITERION_HOME") {
        return home.into();
    }
    let target = std::env::var_os("CARGO_TARGET_DIR").map_or_else(
        || std::path::PathBuf::from("target"),
        std::path::PathBuf::from,
    );
    target.join("criterion")
}

/// Collects every `new/benchmark.json` + `new/estimates.json` pair under `dir`.
fn collect_estimates(
    dir: &std::path::Path,
    out: &mut Vec<serde_json::Value>,
) -> std::io::Result<()> {
    let new = dir.join("new");
    let (bench, estimates) = (new.join("benchmark.json"), new.join("estimates.json"));
    if bench.is_file() && estimates.is_file() {
        let bench: serde_json::Value = serde_json::from_slice(&std::fs::read(bench)?)?;
        let estimates: serde_json::Value = serde_json::from_slice(&std::fs::read(estimates)?)?;
        let mean = &estimates["mean"];
        out.push(serde_json::json!({
            "group": bench["group_id"],
            "engine": bench["function_id"],
            "n": bench["throughput"]["Elements"],
            // Nanoseconds for one call of the routine: the whole key set.
            "mean_ns": mean["point_estimate"],
            "lower_ns": mean["confidence_interval"]["lower_bound"],
            "upper_ns": mean["confidence_interval"]["upper_bound"],
        }));
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            collect_estimates(&entry.path(), out)?;
        }
    }
    Ok(())
}

/// Writes `summary.json` beside Criterion's results: the run's preset, what
/// each group measures, and the mean time (with its confidence interval) of
/// every arm that ran. The published page is drawn from it.
fn write_summary() -> std::io::Result<()> {
    let home = criterion_home();
    let mut results = Vec::new();
    if home.is_dir() {
        collect_estimates(&home, &mut results)?;
    }
    let groups: Vec<serde_json::Value> = GROUP_NOTES
        .iter()
        .map(|(name, note)| serde_json::json!({ "name": name, "note": note }))
        .collect();
    let summary = serde_json::json!({
        "preset": active_preset().label(),
        "groups": groups,
        "results": results,
    });
    std::fs::create_dir_all(&home)?;
    std::fs::write(home.join("summary.json"), serde_json::to_vec(&summary)?)
}

/// `criterion_main!` with the summary written after the run.
fn main() {
    benches();
    Criterion::default().configure_from_args().final_summary();
    if let Err(e) = write_summary() {
        panic!("compare-rocksdb: writing summary.json failed: {e}");
    }
}

//! Head-to-head comparison harness: `coordinode-lsm-tree` vs RocksDB.
//!
//! Each group runs the same workload through every engine and reports, per
//! engine, its time per operation and its time relative to RocksDB, for the
//! published head-to-head page (per [#244]).
//!
//! [#244]: https://github.com/structured-world/coordinode-lsm-tree/issues/244
//!
//! Run locally (a substring filter selects groups; `--help` lists the
//! measurement flags):
//!
//! ```text
//! cd tools/compare-rocksdb && cargo bench
//! cd tools/compare-rocksdb && cargo bench --bench compare -- point_read
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
//! ## Paired measurement
//!
//! The engines of a group are measured in the same rounds, one sample each
//! per round, taking turns at going first (see the `paired` module). The
//! hosts the benchmark runs on are shared: a burst of other work there lasts
//! seconds, and measured one engine after another it fell on whichever
//! engine was running, so two runs of one commit disagreed by a median 15-20%
//! on the ratio of the two engines and by up to 3x on single arms. Measured in
//! rounds, a burst slows every engine of the rounds it overlaps, and the
//! per-round ratio divides it out. The page reports the median of those
//! ratios with a distribution-free confidence interval, and each engine's
//! median time per operation beside it.
//!
//! An arm runs as many iterations per sample as fill [`paired::Settings`]'s
//! sample target, and a group runs as many rounds as fit its budget, between
//! a floor and a ceiling; an arm whose single iteration outlasts the budget
//! runs the floor of rounds whatever the budget says, so for those arms the
//! cost is set by their size alone.
//!
//! After the run the harness writes `target/head-to-head/summary.json` (see
//! [`write_summary`]): every arm's estimates and what each group measures,
//! which the published page is drawn from.
//!
//! Every engine of a warm read group is opened on its on-disk state before
//! the rounds start and stays open through them (see [`WarmEngine`]). The
//! state itself is written once per engine, codec and size and copied for
//! each arm and each overwrite iteration that starts from it (see
//! [`SeedStates`]).
//!
//! ## Engine matrix
//!
//! The shared workload closure is parameterised over an [`Engine`]
//! enum so the per-engine glue (open, put, get, flush, close) lives
//! in exactly one place per engine and the workload code stays
//! engine-agnostic. The engines: `ours`, `rocksdb`, `surrealkv` (pure-Rust
//! embedded LSM/MVCC), and each of ours and RocksDB with its values in blob
//! files, uncompressed (`blob_tree`, `rocksdb_blob`), at zstd level 3
//! (`blob_tree_zstd`, `rocksdb_blob_zstd`) and at zstd level 22
//! (`blob_tree_zstd22`, `rocksdb_blob_zstd22`, see [`BlobCodec`]). Every group runs
//! the inline and blob arms of both engines ([`HEAD_TO_HEAD`]); SurrealKV
//! compresses with zstd at a fixed level 3 only, so it overlays ONLY on the
//! `None`-compression groups (see [`engines_for`]).
//!
//! ## Blob groups
//!
//! The `_blobs_1mib` and `_blobs_4mib` groups write, read, scan and overwrite
//! 64 MiB of real blobs: 64 values of 1 MiB and 16 of 4 MiB, random bytes no
//! codec shrinks, generated before any timing. Each engine runs with the
//! values inline and in blob files, neither caching blob values, so a blob is
//! read from its file through the page cache on both (see [`BLOB_SPECS`]).
//!
//! ## Compression axes + cross-engine overlay
//!
//! The keys and the values are compressed apart, each with a codec and a
//! level of its own, on both engines alike. The tables, which hold the keys
//! (and the inline arms' values with them), are the group's axis: every
//! scenario runs once with `None` block compression (the `<scenario>` group)
//! and once with zstd at level 22, the maximum "ultra" level (the
//! `<scenario>_zstd22` group), ours via `CompressionType::Zstd(22)` and
//! RocksDB via `DBCompressionType::Zstd` pinned to level 22. The blob files,
//! which hold the separated values, are the arm's axis within the group:
//! uncompressed, zstd 3 or zstd 22, ours through its blob compression and
//! RocksDB through its blob compression type and `blob_compression_opts`. Each
//! engine runs its own defaults around that codec: ours, under zstd, gives each
//! blob file a dictionary of its own (`COMPARE_BLOB_DICT=0` turns it off), and
//! RocksDB's blob files have none.
//!
//! Within each group every engine runs in the SAME process and the SAME
//! rounds (ours vs rocksdb, plus surrealkv on the `None` groups). Because the
//! comparison is a ratio measured on one host in one run, it stays
//! meaningful even if the bench host's CPU changes between runs: the
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
//!   a superseding flush), distinct from cold first-insert. An
//!   `ours-deferred-wal` series runs our engine with a caller-kept log and a
//!   deferred flush: one sync per batch, the durability SurrealKV's commit
//!   gives.
//! - `overwrite_sustained`: rounds of rewrites of a hot key set over a tree
//!   several memtables large, compaction running behind the writes and a
//!   reader alongside. Time per write, with write and space amplification and
//!   the read percentiles reported beside it (see [`Sustained`]).
//!
//! Each of the above also has a `_zstd22` sibling, run at every size (see
//! [`SIZES`]).
//!
//! Values carry the compressibility RocksDB's own `db_bench` gives its values
//! by default (see [`ValuePool`]), and every RocksDB read borrows the value
//! out of the block as ours does (`get_pinned`, the raw iterator), and both
//! compress blocks on the same [`COMPRESSION_THREADS`] threads, so neither
//! engine pays for a copy, a codec shortcut or a core the other does not. Not yet portable
//! head-to-head: `readwhilewriting` (concurrency) and `mergerandom`
//! (merge-operator semantics differ across engines) from [#244]'s list.

mod paired;

use std::cell::Cell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

// `Guard` is a trait, used (not dead) for its `.value()` method on the
// `IterGuardImpl` items yielded by `tree.iter()` / `tree.range()` in the
// range_scan and seek_random scenarios — there is no direct path
// reference, so it reads as unused at a glance but the import is required
// for method resolution (clippy `-D warnings` confirms it is live).
use lsm_tree::{
    AbstractTree, CompressionType, Config, Guard, MAX_SEQNO, SequenceNumberCounter,
    config::{
        CompressionPolicy, FlushDurability, HashRatioPolicy, KvSeparationOptions, LocatorPolicy,
        LocatorPolicyEntry, LocatorPrecision, PinningPolicy,
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
/// arm's routine boundary (which cannot itself return `Result`), mirroring
/// how `run_write_throughput` surfaces failures.
fn setup_surrealkv_warm(
    dir: &std::path::Path,
    inputs: &WorkloadInputs,
) -> Result<(tokio::runtime::Runtime, SkvTree), Box<dyn std::error::Error>> {
    let rt = skv_runtime()?;
    let tree = populate_surrealkv(&rt, dir, inputs)?;
    Ok((rt, tree))
}

/// Engine under test. The harness runs each workload once per variant, every
/// engine an arm of the same group, so the published page plots them
/// side-by-side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Engine {
    Ours,
    RocksDb,
    /// SurrealKV, the pure-Rust embedded LSM/MVCC store. Its zstd runs at a
    /// fixed level 3, so it participates ONLY in the None-compression
    /// (codec-neutral) groups (see [`engines_for`]).
    SurrealKv,
    /// Our engine in its KV-separated (`blob_tree`) configuration: values at or
    /// above [`BLOB_SEPARATION_THRESHOLD`] are stored out-of-line in blob files,
    /// so the key-LSM the reads walk is smaller (like surrealkv's vlog). Drives
    /// the same `AbstractTree` API as `Ours`, its blob files compressed as the
    /// codec says.
    BlobTree(BlobCodec),
    /// RocksDB with blob files: values at or above [`BLOB_SEPARATION_THRESHOLD`]
    /// in blob files, no blob cache, as RocksDB defaults it, so a value is read
    /// from its file through the page cache. The counterpart `blob_tree` is
    /// measured against, its blob files compressed as the codec says.
    RocksDbBlob(BlobCodec),
}

impl Engine {
    fn label(self) -> &'static str {
        match self {
            Self::Ours => "ours",
            Self::RocksDb => "rocksdb",
            Self::SurrealKv => "surrealkv",
            Self::BlobTree(BlobCodec::Off) => "blob_tree",
            Self::BlobTree(BlobCodec::Zstd) => "blob_tree_zstd",
            Self::BlobTree(BlobCodec::Zstd22) => "blob_tree_zstd22",
            Self::RocksDbBlob(BlobCodec::Off) => "rocksdb_blob",
            Self::RocksDbBlob(BlobCodec::Zstd) => "rocksdb_blob_zstd",
            Self::RocksDbBlob(BlobCodec::Zstd22) => "rocksdb_blob_zstd22",
        }
    }

    /// The blob codec this engine opens our tree with, if it separates values.
    fn kv_separated(self) -> Option<BlobCodec> {
        match self {
            Self::BlobTree(codec) => Some(codec),
            _ => None,
        }
    }

    /// The blob codec this engine opens RocksDB with, if it uses blob files.
    fn blob_files(self) -> Option<BlobCodec> {
        match self {
            Self::RocksDbBlob(codec) => Some(codec),
            _ => None,
        }
    }
}

/// How the blob arms compress their blob files, the same on both engines and
/// apart from the tables, whose compression is the group's: the keys and the
/// values each have a codec and a level of their own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum BlobCodec {
    /// Blob files uncompressed.
    Off,
    /// Blob files at zstd's default level, 3.
    Zstd,
    /// Blob files at zstd's maximum level, 22.
    Zstd22,
}

impl BlobCodec {
    /// The zstd level of the blob files, `None` for none.
    fn zstd_level(self) -> Option<i32> {
        match self {
            Self::Off => None,
            Self::Zstd => Some(3),
            Self::Zstd22 => Some(Compression::ZSTD_MAX_LEVEL),
        }
    }

    /// Our blob compression. Set in both arms, so a default that changes with a
    /// feature of the bench crate cannot compress the uncompressed arm.
    fn ours(self) -> CompressionType {
        self.zstd_level()
            .map_or(CompressionType::None, CompressionType::Zstd)
    }

    /// RocksDB's blob compression type; its level is set apart (see
    /// [`set_blob_files`]).
    fn rocksdb(self) -> rocksdb::DBCompressionType {
        match self {
            Self::Off => rocksdb::DBCompressionType::None,
            Self::Zstd | Self::Zstd22 => rocksdb::DBCompressionType::Zstd,
        }
    }
}

/// Our engine and RocksDB, each with its values inline and in blob files, the
/// blob files uncompressed and compressed at the default and the maximum
/// level: the arms of every group.
const HEAD_TO_HEAD: &[Engine] = &[
    Engine::Ours,
    Engine::RocksDb,
    Engine::BlobTree(BlobCodec::Off),
    Engine::RocksDbBlob(BlobCodec::Off),
    Engine::BlobTree(BlobCodec::Zstd),
    Engine::RocksDbBlob(BlobCodec::Zstd),
    Engine::BlobTree(BlobCodec::Zstd22),
    Engine::RocksDbBlob(BlobCodec::Zstd22),
];

/// KV-separation threshold for the `blob_tree` arm: values at or above this many
/// bytes are stored out-of-line. The benchmark value is 256 bytes (see
/// [`VALUE_SIZE`]), so 128 separates every value out-of-line, mirroring
/// surrealkv's vlog and isolating the "smaller key-LSM" read effect the
/// `blob_tree` arm exists to measure. Below the default 1 KiB threshold, which
/// would leave the 256-byte values inlined (no separation, no measurement).
/// `rocksdb_blob` separates at the same size.
const BLOB_SEPARATION_THRESHOLD: u32 = 128;

/// Engines to overlay for a given compression variant.
///
/// SurrealKV's zstd is fixed at level 3 (no level setting), so it cannot match
/// the `Zstd22` variant apples-to-apples: adding a level-3 line to a zstd22
/// graph would misrepresent the comparison. It is
/// therefore restricted to the `None`-compression groups, where every engine
/// runs codec-neutral. The `_zstd22` groups run the [`HEAD_TO_HEAD`] arms.
fn engines_for(compression: Compression) -> &'static [Engine] {
    match compression {
        Compression::None => &[
            Engine::Ours,
            Engine::RocksDb,
            Engine::SurrealKv,
            Engine::BlobTree(BlobCodec::Off),
            Engine::RocksDbBlob(BlobCodec::Off),
            Engine::BlobTree(BlobCodec::Zstd),
            Engine::RocksDbBlob(BlobCodec::Zstd),
            Engine::BlobTree(BlobCodec::Zstd22),
            Engine::RocksDbBlob(BlobCodec::Zstd22),
        ],
        Compression::Zstd22 => HEAD_TO_HEAD,
    }
}

/// Engines of the blob groups. SurrealKV writes its values to its log whatever
/// their size and is not seeded (see [`SeedStates`]); at these sizes its arms
/// would mostly time writing the state they start from.
const BLOB_ENGINES: &[Engine] = HEAD_TO_HEAD;

/// What a group's values are.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Values {
    /// [`VALUE_SIZE`] bytes cut from the [`ValuePool`], compressible as
    /// RocksDB's `db_bench` makes its values.
    Pool,
    /// Blobs of this many random bytes, which no codec shrinks, so a blob
    /// read moves every byte of it.
    Random(usize),
}

/// What one family of groups writes and which engines it runs: the key counts
/// it is measured at, its values and its engines.
#[derive(Clone, Copy)]
struct Spec {
    sizes: &'static [u64],
    values: Values,
    engines: &'static [Engine],
}

impl Spec {
    /// The key counts and engines every write group of `compression` runs.
    fn writes(compression: Compression) -> Self {
        Self {
            sizes: SIZES,
            values: Values::Pool,
            engines: engines_for(compression),
        }
    }

    /// The key counts and engines every warm read group of `compression` runs.
    fn reads(compression: Compression) -> Self {
        Self {
            sizes: SIZES,
            values: Values::Pool,
            engines: engines_for(compression),
        }
    }
}

/// The blob groups: 64 MiB of random values at each blob size, the suffix
/// naming the size in the group's name.
const BLOB_SPECS: &[(&str, Spec)] = &[
    (
        "_blobs_1mib",
        Spec {
            sizes: &[64],
            values: Values::Random(1 << 20),
            engines: BLOB_ENGINES,
        },
    ),
    (
        "_blobs_4mib",
        Spec {
            sizes: &[16],
            values: Values::Random(4 << 20),
            engines: BLOB_ENGINES,
        },
    ),
];

/// Element counts of every write and warm-read group, compressed or not.
///
/// The block cache is 16 MiB on both engines and every value is 256 bytes, so
/// at 1k and 10k the inline arms of a zstd-22 read group decode no block after
/// the warm-up and measure what the uncompressed group does; the blob arms read
/// their values from blob files, whose frames RocksDB does not cache, so those
/// sizes still tell the blob paths apart.
const SIZES: &[u64] = &[1_000, 10_000, 70_000];

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
        let slots = u64::try_from(Self::SIZE / VALUE_SIZE).expect("slot count fits u64");
        let start =
            usize::try_from(i % slots).expect("a slot below the count fits usize") * VALUE_SIZE;
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
    /// What the values are, so a seeded state is told apart by them.
    kind: Values,
}

impl WorkloadInputs {
    fn build(n_keys: u64) -> Self {
        Self::of(n_keys, Values::Pool)
    }

    /// `n_keys` keys with `values`, all generated here, before any timing.
    fn of(n_keys: u64, values: Values) -> Self {
        let n = usize::try_from(n_keys).expect("n_keys fits in usize");
        let pool = ValuePool::new();
        let mut keys = Vec::with_capacity(n);
        let mut held = Vec::with_capacity(n);
        for i in 0..n_keys {
            keys.push(key_for(i));
            held.push(match values {
                Values::Pool => pool.value(i).to_vec(),
                Values::Random(len) => random_value(i, len),
            });
        }
        Self {
            keys,
            values: held,
            kind: values,
        }
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
/// - **[`COMPRESSION_THREADS`] block-compression threads**, as ours gets in
///   [`open_ours`].
///
/// `create_if_missing` is set here too. WAL handling is per-call
/// (`WriteOptions::disable_wal`) since it only applies to the write
/// path.
///
/// The `compression` argument selects the codec to match our engine's
/// per-variant setting: `None` leaves RocksDB uncompressed; `Zstd22`
/// sets `DBCompressionType::Zstd` and pins the level to 22 via
/// `set_compression_options`.
///
/// `blob_files` stores values at or above [`BLOB_SEPARATION_THRESHOLD`] in blob
/// files, compressed as the codec says, the same as `blob_tree` keeps them,
/// without a blob cache: the RocksDB default, as not caching values is ours.
fn rocksdb_options(
    compression: Compression,
    hash_index: bool,
    blob_files: Option<BlobCodec>,
) -> rocksdb::Options {
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
    // The blob level has no setter of its own: it is the options string's,
    // read into the options first, so the cache and table factory set below
    // are held by the options the engine opens with.
    if let Some(level) = blob_files.and_then(BlobCodec::zstd_level) {
        opts = opts
            .get_options_from_string(format!("blob_compression_opts={{level={level}}}"))
            .expect("rocksdb: blob compression level");
    }
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
    opts.set_compression_options_parallel_threads(
        i32::try_from(COMPRESSION_THREADS).expect("COMPRESSION_THREADS fits i32"),
    );
    opts.set_block_based_table_factory(&block_opts);
    if let Some(codec) = blob_files {
        set_blob_files(&mut opts, codec);
    }
    opts
}

/// RocksDB's blob files as the blob arms use them: values at or above
/// [`BLOB_SEPARATION_THRESHOLD`], compressed as `codec` says, at the level
/// [`rocksdb_options`] reads in.
fn set_blob_files(opts: &mut rocksdb::Options, codec: BlobCodec) {
    opts.set_enable_blob_files(true);
    opts.set_min_blob_size(u64::from(BLOB_SEPARATION_THRESHOLD));
    opts.set_blob_compression_type(codec.rocksdb());
}

/// Threads compressing a table's blocks, on both engines and in every group:
/// ours through `compaction_threads`, which sizes the pool its table writers
/// compress on at flush and compaction alike; RocksDB through
/// `compression_options_parallel_threads`, which does the same for its table
/// builder. Left at their defaults, ours compresses on half the host's cores
/// and RocksDB on one, so a write group would compare unequal parallelism and
/// its ratio would move with the host's core count and with whatever else
/// keeps those cores busy.
const COMPRESSION_THREADS: usize = 4;

/// Opens our engine at `dir` as [`ours_config`] configures it.
fn open_ours(
    dir: &std::path::Path,
    compression: Compression,
    kv_separated: Option<BlobCodec>,
    strategy: IndexStrategy,
    row_cache: bool,
) -> Result<lsm_tree::AnyTree, Box<dyn std::error::Error>> {
    Ok(ours_config(dir, compression, kv_separated, strategy, row_cache).open()?)
}

/// Our engine's configuration at `dir` with the block-compression policy for
/// the given `compression` variant. Both arms set the policy EXPLICITLY:
/// `None` pins `CompressionPolicy::all(None)` rather than relying on the
/// `Config` default (which becomes `[None, Lz4]` if the `lz4` feature is
/// ever enabled on this bench crate, silently compressing the supposed
/// "uncompressed baseline"); `Zstd22` applies level-22 zstd to every
/// level. Keeping the `None` arm explicit holds the baseline apples-to-
/// apples with RocksDB's `DBCompressionType::None`.
///
/// When `kv_separated` is set, values at or above [`BLOB_SEPARATION_THRESHOLD`]
/// are stored out-of-line (the `blob_tree` arms), the blob files compressed as
/// its codec says whatever the tables are compressed with.
fn ours_config(
    dir: &std::path::Path,
    compression: Compression,
    kv_separated: Option<BlobCodec>,
    strategy: IndexStrategy,
    row_cache: bool,
) -> Config {
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
    let config = apply_preset(config, active_preset()).compaction_threads(COMPRESSION_THREADS);
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
    separate_values(config, kv_separated)
}

/// `config` with values at or above [`BLOB_SEPARATION_THRESHOLD`] in blob files
/// compressed as `blobs` says, or unchanged without it.
fn separate_values(config: Config, blobs: Option<BlobCodec>) -> Config {
    match blobs {
        Some(codec) => {
            // A byte count from `name`, `None` without it. A value that is
            // not one stops the run, as `COMPARE_SIZES` does: a typo would
            // otherwise measure the default under the name of the setting.
            let byte_count = |name: &str| {
                std::env::var(name).ok().map(|v| {
                    v.trim()
                        .parse::<u32>()
                        .unwrap_or_else(|_| panic!("{name}: not a byte count: {v}"))
                })
            };
            let mut opts =
                KvSeparationOptions::default().separation_threshold(BLOB_SEPARATION_THRESHOLD);
            // Blob block size under study: `COMPARE_BLOB_BLOCK` bytes, the
            // library default without it.
            if let Some(bytes) = byte_count("COMPARE_BLOB_BLOCK") {
                opts = opts.block_size(bytes);
            }
            // A dictionary of each zstd blob file's own: `COMPARE_BLOB_DICT`
            // bytes, `0` for none, the library default (64 KiB) without it.
            // The zstd blob arms therefore run our default layout, a file's
            // own dictionary included, against RocksDB's blob files, which
            // have none: the comparison is of the two engines' defaults.
            if let Some(bytes) = byte_count("COMPARE_BLOB_DICT") {
                opts = opts.file_dictionary(bytes);
            }
            config
                .with_kv_separation(Some(opts))
                .blob_compression(codec.ours())
        }
        None => config,
    }
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
/// (each iteration starts from an empty database, so the warm-up and every
/// sample measure the same work).
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
        Engine::Ours | Engine::BlobTree(_) => {
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
        Engine::RocksDb | Engine::RocksDbBlob(_) => {
            // Bloom (10 bits/key) + 16 MiB cache + no compression, matching
            // our engine's defaults — see `rocksdb_options`. Our engine
            // builds a bloom filter at flush, so giving RocksDB the same
            // keeps the write comparison apples-to-apples.
            let opts = rocksdb_options(compression, false, engine.blob_files());
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
    settle();
    Ok(elapsed)
}

fn bench_write_throughput(run: &mut Run) {
    // `None` baseline + `Zstd22` high-ratio variant, each its own group.
    write_throughput_variant(
        run,
        "write_throughput",
        Compression::None,
        Spec::writes(Compression::None),
    );
    write_throughput_variant(
        run,
        "write_throughput_zstd22",
        Compression::Zstd22,
        Spec::writes(Compression::Zstd22),
    );
    for (suffix, spec) in BLOB_SPECS {
        write_throughput_variant(
            run,
            &format!("write_throughput{suffix}"),
            Compression::None,
            *spec,
        );
    }
}

fn write_throughput_variant(run: &mut Run, group_name: &str, compression: Compression, spec: Spec) {
    if !run.wants(group_name) {
        return;
    }
    // SurrealKV participates only in the None-compression group (no zstd
    // codec). Build its tokio runtime ONCE here, outside the timed write
    // window, and reuse it across every sample: `Runtime::new` spins up worker
    // threads, and charging that executor bootstrap to each timed write sample
    // would bias surrealkv's throughput downward vs the other engines (which
    // only pay open/write/flush). `None` where it doesn't run.
    let skv_rt = spec
        .engines
        .contains(&Engine::SurrealKv)
        .then(|| skv_runtime().expect("surrealkv: tokio runtime"));
    for n in run.sizes_of(spec.sizes) {
        // Precompute the keys + values ONCE per `n`, so the timed body does no
        // per-iteration allocation.
        let inputs = WorkloadInputs::of(n, spec.values);
        let arms = spec
            .engines
            .iter()
            .map(|&engine| {
                let (inputs, skv_rt) = (&inputs, skv_rt.as_ref());
                paired::Arm::new(engine.label(), move |iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        // An I/O failure invalidates the run: there is no
                        // meaningful Duration to report, so it surfaces as a
                        // bench panic with the engine label.
                        total += run_write_throughput(skv_rt, engine, compression, inputs)
                            .unwrap_or_else(|e| {
                                panic!("run_write_throughput failed for {}: {e}", engine.label())
                            });
                    }
                    total
                })
            })
            .collect();
        run.measure(group_name, n, arms);
    }
}

/// Workload: point-read every key from an engine pre-populated with
/// `inputs.keys.len()` keys and flushed to disk.
///
/// In contrast to [`run_write_throughput`]'s cold-start measurement,
/// the engine here is opened ONCE on a copy of its populated and flushed
/// seed state ([`SeedStates`]), outside the timing window, and kept warm
/// through the group's rounds. The timed body issues one `get` per
/// stored key, so the number reflects warm steady-state read latency (lookup
/// path + bloom filter + block decode), NOT the open / write / flush setup
/// cost.
///
/// Note this is a CACHE-WARM read: the engine stays open across the
/// warm-up and every round, so after the first pass
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
    run: &mut Run,
    group_name: &str,
    compression: Compression,
    spec: Spec,
    hash_overlays: bool,
    seeds: &mut SeedStates,
) {
    if !run.wants(group_name) {
        return;
    }
    // Series overlaid on this ONE chart: every base engine with binary-search
    // data-block index, plus — when `hash_overlays` is set — the data-block
    // hash index (ours + RocksDB) and the retrieval-ribbon locator (ours only)
    // as additional index strategies on the same chart.
    // Tuple: (label, engine, index strategy, row_cache). The row cache is a cache
    // property orthogonal to the index strategy, so it rides as a 4th field.
    let mut series: Vec<(&str, Engine, IndexStrategy, bool)> = spec
        .engines
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
    series.retain(|&(label, ..)| run.wants_engine(label));

    for n in run.sizes_of(spec.sizes) {
        let inputs = WorkloadInputs::of(n, spec.values);
        // Every arm opened and checked before the rounds: they only ever pay
        // for reads, never for open / write / flush.
        let warm: Vec<(&'static str, WarmEngine)> = series
            .iter()
            .map(|&(label, engine, strategy, row_cache)| {
                let warm =
                    WarmEngine::build(engine, compression, strategy, row_cache, &inputs, seeds);
                verify_point_reads(&warm, &inputs);
                (label, warm)
            })
            .collect();
        let arms = warm
            .iter()
            .map(|(label, warm)| paired::Arm {
                label,
                routine: point_read_routine(warm, &inputs),
                metrics: None,
            })
            .collect();
        run.measure(group_name, n, arms);
    }
}

/// Workload: point-read every key once from an engine opened cold.
///
/// Every iteration copies the seed state into a fresh directory, drops the
/// copy's pages from the OS page cache, opens the engine on it with an empty
/// block cache and reads each stored key once, in insertion order (scattered
/// on disk, see [`point_read_variant`]). Only the reads are timed, each on its
/// own, so the group reports the read latency's median and p99 beside the
/// mean: the first read of a block, a filter or a separated value goes to the
/// device, the later reads of a block read it from the cache it filled.
///
/// The page cache is dropped with `dd iflag=nocache` per file on Linux; on
/// another OS the copy's pages stay cached and only the engines' own caches
/// start empty.
fn point_read_cold_variant(
    run: &mut Run,
    group_name: &str,
    compression: Compression,
    spec: Spec,
    seeds: &mut SeedStates,
) {
    if !run.wants(group_name) {
        return;
    }
    let engines: Vec<Engine> = run
        .engines_of(spec.engines)
        .into_iter()
        .filter(|engine| !matches!(engine, Engine::SurrealKv))
        .collect();
    for n in run.sizes_of(spec.sizes) {
        let inputs = WorkloadInputs::of(n, spec.values);
        let arms = engines
            .iter()
            .map(|&engine| {
                let seed = seeds
                    .seed(engine, compression, IndexStrategy::Binary, &inputs)
                    .to_path_buf();
                let last = Rc::new(Cell::new(ColdSample::default()));
                let (inputs, sample) = (&inputs, Rc::clone(&last));
                paired::Arm::new(engine.label(), move |iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let dir = copy_of(&seed);
                        drop_page_cache(dir.path());
                        let mut nanos = cold_reads(engine, compression, dir.path(), inputs);
                        total += Duration::from_nanos(nanos.iter().sum());
                        sample.set(ColdSample {
                            read_p50_us: quantile_us(&mut nanos, 500),
                            read_p99_us: quantile_us(&mut nanos, 990),
                        });
                    }
                    total
                })
                .with_metrics(move || last.get().metrics())
            })
            .collect();
        run.measure(group_name, n, arms);
    }
}

/// The latencies of one cold pass: `engine` opened on `dir` and every key of
/// `inputs` read once, each read timed on its own, in nanoseconds.
fn cold_reads(
    engine: Engine,
    compression: Compression,
    dir: &Path,
    inputs: &WorkloadInputs,
) -> Vec<u64> {
    let mut nanos = Vec::with_capacity(inputs.keys.len());
    let mut time = |read: &mut dyn FnMut()| {
        let at = std::time::Instant::now();
        read();
        nanos.push(u64::try_from(at.elapsed().as_nanos()).unwrap_or(u64::MAX));
    };
    match engine {
        Engine::Ours | Engine::BlobTree(_) => {
            let tree = open_ours(
                dir,
                compression,
                engine.kv_separated(),
                IndexStrategy::Binary,
                false,
            )
            .expect("ours cold: open");
            for key in &inputs.keys {
                time(&mut || {
                    let got = tree.get(key, MAX_SEQNO).expect("ours cold: get");
                    assert!(got.is_some(), "ours cold: key unexpectedly missing");
                    std::hint::black_box(got);
                });
            }
        }
        Engine::RocksDb | Engine::RocksDbBlob(_) => {
            let db = open_rocksdb(dir, compression, false, engine.blob_files());
            for key in &inputs.keys {
                time(&mut || {
                    let got = db.get_pinned(key).expect("rocksdb cold: get");
                    assert!(got.is_some(), "rocksdb cold: key unexpectedly missing");
                    std::hint::black_box(got);
                });
            }
        }
        Engine::SurrealKv => unreachable!("surrealkv is not seeded"),
    }
    nanos
}

/// Drops the pages of every file under `dir` from the OS page cache, so the
/// next reads of them go to the device. The files are clean: [`copy_of`]
/// synced them.
fn drop_page_cache(dir: &Path) {
    if !cfg!(target_os = "linux") {
        return;
    }
    // An entry that cannot be read would stay warm and pass for a cold read.
    for entry in std::fs::read_dir(dir).expect("read dir") {
        let entry = entry.expect("read dir entry");
        let path = entry.path();
        if entry.file_type().expect("entry file type").is_dir() {
            drop_page_cache(&path);
            continue;
        }
        let mut input = std::ffi::OsString::from("if=");
        input.push(&path);
        let status = std::process::Command::new("dd")
            .args([
                input.as_os_str(),
                "iflag=nocache".as_ref(),
                "count=0".as_ref(),
            ])
            .args(["status=none"])
            .status()
            .expect("run dd");
        assert!(status.success(), "dd iflag=nocache failed: {status}");
    }
}

/// What one cold pass measured besides its total read time.
#[derive(Clone, Copy, Default)]
struct ColdSample {
    read_p50_us: f64,
    read_p99_us: f64,
}

impl ColdSample {
    fn metrics(self) -> Vec<(&'static str, f64)> {
        vec![
            ("read_p50_us", self.read_p50_us),
            ("read_p99_us", self.read_p99_us),
        ]
    }
}

/// One-time hit check OUTSIDE the timed window: enforces the workload contract
/// ("read every stored key") so a setup/flush regression can't silently become
/// a miss-read benchmark, without taxing each timed `get` with a branch.
/// `MAX_SEQNO` (not `u64::MAX`, whose MSB is reserved) reads the latest
/// visible version.
fn verify_point_reads(warm: &WarmEngine, inputs: &WorkloadInputs) {
    match warm {
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
}

/// One `get` per stored key, timed.
///
/// With `COMPARE_HOLD` set, ours and RocksDB keep every value of a pass
/// alive until it ends, as a batched read does. Dropped one by one, each
/// value hands its just-freed buffer to the next read, so a point read is
/// compared with `multi_get` on equal work only when it holds them too.
fn point_read_routine<'a>(warm: &'a WarmEngine, inputs: &'a WorkloadInputs) -> paired::Routine<'a> {
    match warm {
        WarmEngine::Ours { tree, .. } if std::env::var_os("COMPARE_HOLD").is_some() => {
            Box::new(move |iters| {
                let start = std::time::Instant::now();
                for _ in 0..iters {
                    let mut held = Vec::with_capacity(inputs.keys.len());
                    for key in &inputs.keys {
                        held.push(tree.get(key, MAX_SEQNO).expect("ours: get"));
                    }
                    std::hint::black_box(held);
                }
                start.elapsed()
            })
        }
        WarmEngine::Ours { tree, .. } => Box::new(move |iters| {
            let start = std::time::Instant::now();
            for _ in 0..iters {
                for key in &inputs.keys {
                    let got = tree.get(key, MAX_SEQNO).expect("ours: get");
                    std::hint::black_box(got);
                }
            }
            start.elapsed()
        }),
        WarmEngine::RocksDb { db, .. } if std::env::var_os("COMPARE_HOLD").is_some() => {
            Box::new(move |iters| {
                let start = std::time::Instant::now();
                for _ in 0..iters {
                    let mut held = Vec::with_capacity(inputs.keys.len());
                    for key in &inputs.keys {
                        held.push(db.get_pinned(key).expect("rocksdb: get"));
                    }
                    std::hint::black_box(held);
                }
                start.elapsed()
            })
        }
        // `get_pinned` hands back a view of the block, as our `get` does;
        // plain `get` would copy every value into a fresh `Vec` and charge
        // RocksDB an allocation ours never pays.
        WarmEngine::RocksDb { db, .. } => Box::new(move |iters| {
            let start = std::time::Instant::now();
            for _ in 0..iters {
                for key in &inputs.keys {
                    let got = db.get_pinned(key).expect("rocksdb: get");
                    std::hint::black_box(got);
                }
            }
            start.elapsed()
        }),
        WarmEngine::SurrealKv { tree, .. } => {
            // One read snapshot for the whole arm, the closest analogue of the
            // other engines' direct warm reads (a consistent view, no per-get
            // txn churn).
            let txn = tree.begin().expect("surrealkv: begin");
            Box::new(move |iters| {
                let start = std::time::Instant::now();
                for _ in 0..iters {
                    for key in &inputs.keys {
                        let got = txn.get(key.as_slice()).expect("surrealkv: get");
                        std::hint::black_box(got);
                    }
                }
                start.elapsed()
            })
        }
    }
}

/// Opens the RocksDB instance at `dir`, new or seeded, with the matched
/// options. The one open path for every arm that reads or overwrites a seeded
/// state, so a seed is reopened exactly as it was written.
fn open_rocksdb(
    dir: &std::path::Path,
    compression: Compression,
    hash_index: bool,
    blob_files: Option<BlobCodec>,
) -> rocksdb::DB {
    let opts = rocksdb_options(compression, hash_index, blob_files);
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
    blob_files: Option<BlobCodec>,
    inputs: &WorkloadInputs,
) -> rocksdb::DB {
    let db = open_rocksdb(dir, compression, hash_index, blob_files);
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
/// selects a `blob_tree` (KV-separated) configuration; `strategy` /
/// `row_cache` select the `point_read` index-strategy series.
fn populate_ours(
    dir: &std::path::Path,
    compression: Compression,
    inputs: &WorkloadInputs,
    kv_separated: Option<BlobCodec>,
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
/// index strategy, the key count and the values. The row cache is not part of
/// it: it lives in memory only, so the arms with and without it start from the
/// same files.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct SeedKey {
    engine: Engine,
    compression: Compression,
    strategy: IndexStrategy,
    n: usize,
    values: Values,
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
/// the caches it starts with differ, and the warm-up before the rounds fills
/// them before any sample is taken.
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
    /// The directory holding the state `inputs` seeds for this engine, codec
    /// and index strategy, written on first use. Never handed to an engine
    /// itself: every arm and iteration works on a [`copy_of`] it.
    fn seed(
        &mut self,
        engine: Engine,
        compression: Compression,
        strategy: IndexStrategy,
        inputs: &WorkloadInputs,
    ) -> &Path {
        let key = SeedKey {
            engine,
            compression,
            strategy,
            n: inputs.keys.len(),
            values: inputs.kind,
        };
        self.dirs
            .entry(key)
            .or_insert_with(|| write_seed(key, inputs))
            .path()
    }
}

/// A fresh directory holding a copy of the state at `seed`, on disk before it
/// is returned (see [`settle`]).
fn copy_of(seed: &Path) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    copy_dir(seed, dir.path()).expect("copy seed state");
    settle();
    dir
}

/// Writes out every dirty page and pending metadata change of the host's file
/// systems, outside any timed window. Copying a starting state and removing a
/// finished one leave tens of megabytes for the kernel to write back later;
/// left alone, that write-back lands in the next engine's timed syncs, and in
/// the write groups it doubled one engine's time while halving another's.
fn settle() {
    let status = std::process::Command::new("sync")
        .status()
        .expect("run sync");
    assert!(status.success(), "sync failed: {status}");
}

/// Writes the seed state for `key` into a new directory and closes the engine
/// before returning, so the files are complete before anything copies them.
fn write_seed(key: SeedKey, inputs: &WorkloadInputs) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("seed tempdir");
    match key.engine {
        Engine::Ours | Engine::BlobTree(_) => drop(populate_ours(
            dir.path(),
            key.compression,
            inputs,
            key.engine.kv_separated(),
            key.strategy,
            false,
        )),
        Engine::RocksDb | Engine::RocksDbBlob(_) => drop(populate_rocksdb(
            dir.path(),
            key.compression,
            key.strategy == IndexStrategy::HashIndex,
            key.engine.blob_files(),
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
fn bench_seeded(run: &mut Run) {
    let mut seeds = SeedStates::default();
    let (none, zstd) = (Compression::None, Compression::Zstd22);
    // The `point_read` group ADDS the hash-index series (`ours-hash-index`,
    // `rocksdb-hash-index`) alongside the binary-search `ours` / `rocksdb`
    // arms, so one chart shows both index strategies head-to-head.
    point_read_variant(run, "point_read", none, Spec::reads(none), true, &mut seeds);
    point_read_variant(
        run,
        "point_read_zstd22",
        zstd,
        Spec::reads(zstd),
        false,
        &mut seeds,
    );
    // The cold reads run the head-to-head arms on both table codecs: a cold
    // read is where blob files and tables meet the device.
    let cold = Spec {
        sizes: SIZES,
        values: Values::Pool,
        engines: HEAD_TO_HEAD,
    };
    point_read_cold_variant(run, "point_read_cold", none, cold, &mut seeds);
    point_read_cold_variant(run, "point_read_cold_zstd22", zstd, cold, &mut seeds);
    for (suffix, spec) in BLOB_SPECS {
        point_read_cold_variant(
            run,
            &format!("point_read_cold{suffix}"),
            none,
            *spec,
            &mut seeds,
        );
    }
    multi_get_variant(run, "multi_get", none, Spec::reads(none), &mut seeds);
    multi_get_variant(run, "multi_get_zstd22", zstd, Spec::reads(zstd), &mut seeds);
    range_scan_variant(run, "range_scan", none, Spec::reads(none), &mut seeds);
    range_scan_variant(
        run,
        "range_scan_zstd22",
        zstd,
        Spec::reads(zstd),
        &mut seeds,
    );
    seek_random_variant(run, "seek_random", none, Spec::reads(none), &mut seeds);
    seek_random_variant(
        run,
        "seek_random_zstd22",
        zstd,
        Spec::reads(zstd),
        &mut seeds,
    );
    overwrite_variant(run, "overwrite", none, Spec::writes(none), &mut seeds);
    overwrite_variant(
        run,
        "overwrite_zstd22",
        zstd,
        Spec::writes(zstd),
        &mut seeds,
    );
    // Blobs: tables uncompressed, as random bytes give a codec nothing to do;
    // the compressed blob arms show what each engine stores and reads for a
    // value its codec cannot shrink. No seek group: with one key per blob it
    // reads what a point read does.
    for (suffix, spec) in BLOB_SPECS {
        let spec = *spec;
        point_read_variant(
            run,
            &format!("point_read{suffix}"),
            none,
            spec,
            false,
            &mut seeds,
        );
        multi_get_variant(run, &format!("multi_get{suffix}"), none, spec, &mut seeds);
        range_scan_variant(run, &format!("range_scan{suffix}"), none, spec, &mut seeds);
        overwrite_variant(run, &format!("overwrite{suffix}"), none, spec, &mut seeds);
    }
}

/// Warm on-disk state for one read arm (`point_read`, `multi_get`,
/// `range_scan`, `seek_random`): the engine opened on a copy of its seed
/// state and the directory holding the copy. Every arm of a group is built
/// before its rounds and stays open through them, so a sample only ever times
/// reads.
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
            Engine::Ours | Engine::BlobTree(_) => {
                let dir = copy_of(seeds.seed(engine, compression, strategy, inputs));
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
            Engine::RocksDb | Engine::RocksDbBlob(_) => {
                let dir = copy_of(seeds.seed(engine, compression, strategy, inputs));
                let db = open_rocksdb(
                    dir.path(),
                    compression,
                    strategy == IndexStrategy::HashIndex,
                    engine.blob_files(),
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
    run: &mut Run,
    group_name: &str,
    compression: Compression,
    spec: Spec,
    seeds: &mut SeedStates,
) {
    if !run.wants(group_name) {
        return;
    }
    // Only engines with a real batch-get API overlay here (SurrealKV has none).
    let engines: Vec<Engine> = run
        .engines_of(spec.engines)
        .into_iter()
        .filter(|engine| !matches!(engine, Engine::SurrealKv))
        .collect();

    for n in run.sizes_of(spec.sizes) {
        let inputs = WorkloadInputs::of(n, spec.values);
        // This measures WARM steady-state batched-MultiGet throughput, NOT
        // cold first-touch latency: every engine opens its seed and probes once
        // before the rounds (see `WarmEngine`), so all arms (ours, blob_tree,
        // rocksdb) enter the rounds equally warmed. That symmetry is the point
        // of the comparison. Cold fan-out latency is a separate,
        // OS-cache-dropping measurement, not this bench.
        let warm: Vec<(Engine, WarmEngine)> = engines
            .iter()
            .map(|&engine| {
                let warm = WarmEngine::build(
                    engine,
                    compression,
                    IndexStrategy::Binary,
                    false,
                    &inputs,
                    seeds,
                );
                verify_multi_get(&warm, &inputs);
                (engine, warm)
            })
            .collect();
        let arms = warm
            .iter()
            .map(|(engine, warm)| paired::Arm {
                label: engine.label(),
                routine: multi_get_routine(warm, &inputs),
                metrics: None,
            })
            .collect();
        run.measure(group_name, n, arms);
    }
}

/// One-time "every key present" contract check OUTSIDE the timed window
/// (mirrors [`verify_point_reads`]), so a setup regression can't quietly
/// become a miss-read benchmark. Cardinality before presence: a batched API
/// that dropped positions would otherwise pass the presence check while the
/// timed loop measures fewer than `n` lookups.
fn verify_multi_get(warm: &WarmEngine, inputs: &WorkloadInputs) {
    match warm {
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
}

/// One batched lookup of the whole key set, timed.
fn multi_get_routine<'a>(warm: &'a WarmEngine, inputs: &'a WorkloadInputs) -> paired::Routine<'a> {
    match warm {
        WarmEngine::Ours { tree, .. } => Box::new(move |iters| {
            let start = std::time::Instant::now();
            for _ in 0..iters {
                let got = tree
                    .multi_get(inputs.keys.iter(), MAX_SEQNO)
                    .expect("ours: multi_get");
                std::hint::black_box(got);
            }
            start.elapsed()
        }),
        WarmEngine::RocksDb { db, .. } => {
            // `batched_multi_get_cf` is RocksDB's OPTIMIZED batched MultiGet
            // (batched bloom probes + coalesced block reads, NOT the legacy
            // per-key `multi_get`); it needs the CF handle `open_rocksdb`'s
            // descriptor open provides. `sorted_input = false`: keys arrive in
            // insertion order and RocksDB sorts internally, exactly as ours
            // does.
            let cf = db
                .cf_handle(rocksdb::DEFAULT_COLUMN_FAMILY_NAME)
                .expect("rocksdb: default cf");
            Box::new(move |iters| {
                let start = std::time::Instant::now();
                for _ in 0..iters {
                    let got = db.batched_multi_get_cf(&cf, inputs.keys.iter(), false);
                    std::hint::black_box(got);
                }
                start.elapsed()
            })
        }
        WarmEngine::SurrealKv { .. } => {
            unreachable!("surrealkv is filtered out of the multi_get series")
        }
    }
}

/// Workload: full forward scan reading every value. The engine is
/// opened ONCE on its seed state outside the timed window (warm, like
/// [`point_read_variant`]); the timed body iterates the whole keyspace
/// front-to-back and touches each value, so the number reflects
/// steady-state sequential-scan throughput (block decode + iterator
/// advance), not setup cost.
fn range_scan_variant(
    run: &mut Run,
    group_name: &str,
    compression: Compression,
    spec: Spec,
    seeds: &mut SeedStates,
) {
    warm_read_group(
        run,
        group_name,
        compression,
        spec,
        seeds,
        range_scan_routine,
    );
}

/// The warm read groups whose arms are every engine of `spec` with the
/// binary-search index: opens each before the rounds (see [`WarmEngine`]) and
/// times `routine` on it.
fn warm_read_group(
    run: &mut Run,
    group_name: &str,
    compression: Compression,
    spec: Spec,
    seeds: &mut SeedStates,
    routine: for<'a> fn(&'a WarmEngine, &'a WorkloadInputs) -> paired::Routine<'a>,
) {
    if !run.wants(group_name) {
        return;
    }
    for n in run.sizes_of(spec.sizes) {
        let inputs = WorkloadInputs::of(n, spec.values);
        let warm: Vec<(Engine, WarmEngine)> = run
            .engines_of(spec.engines)
            .into_iter()
            .map(|engine| {
                let warm = WarmEngine::build(
                    engine,
                    compression,
                    IndexStrategy::Binary,
                    false,
                    &inputs,
                    seeds,
                );
                (engine, warm)
            })
            .collect();
        let arms = warm
            .iter()
            .map(|(engine, warm)| paired::Arm {
                label: engine.label(),
                routine: routine(warm, &inputs),
                metrics: None,
            })
            .collect();
        run.measure(group_name, n, arms);
    }
}

/// A full forward scan reading every value, timed.
fn range_scan_routine<'a>(
    warm: &'a WarmEngine,
    _inputs: &'a WorkloadInputs,
) -> paired::Routine<'a> {
    match warm {
        WarmEngine::Ours { tree, .. } => Box::new(move |iters| {
            let start = std::time::Instant::now();
            for _ in 0..iters {
                for guard in tree.iter(MAX_SEQNO, None) {
                    let v = guard.value().expect("ours: scan value");
                    std::hint::black_box(v);
                }
            }
            start.elapsed()
        }),
        // The raw iterator lends each value out of the block, as ours does;
        // the boxed iterator would copy every key and value into fresh
        // allocations on RocksDB's side only.
        WarmEngine::RocksDb { db, .. } => Box::new(move |iters| {
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
        }),
        WarmEngine::SurrealKv { tree, .. } => {
            let txn = tree.begin().expect("surrealkv: begin");
            Box::new(move |iters| {
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
            })
        }
    }
}

/// Workload: seek to each key (in insertion order, i.e. scattered across
/// the sorted keyspace) and read the single value the cursor lands on.
/// Warm: the engine is opened ONCE on its seed state outside the timed
/// window. This measures seek-then-read latency (index descent + block
/// decode + cursor positioning), the closest head-to-head analogue of a
/// `seekrandom` workload.
fn seek_random_variant(
    run: &mut Run,
    group_name: &str,
    compression: Compression,
    spec: Spec,
    seeds: &mut SeedStates,
) {
    warm_read_group(
        run,
        group_name,
        compression,
        spec,
        seeds,
        seek_random_routine,
    );
}

/// A seek to each key and a read of the value under the cursor, timed.
fn seek_random_routine<'a>(
    warm: &'a WarmEngine,
    inputs: &'a WorkloadInputs,
) -> paired::Routine<'a> {
    match warm {
        WarmEngine::Ours { tree, .. } => Box::new(move |iters| {
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
        }),
        // A fresh iterator per seek, as ours opens a fresh range; the raw
        // iterator lends the value instead of copying it.
        WarmEngine::RocksDb { db, .. } => Box::new(move |iters| {
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
        }),
        WarmEngine::SurrealKv { tree, .. } => {
            let txn = tree.begin().expect("surrealkv: begin");
            Box::new(move |iters| {
                let start = std::time::Instant::now();
                for _ in 0..iters {
                    for key in &inputs.keys {
                        // Seek to the first key >= this key and read its value:
                        // the SurrealKV analogue of the index-descent-then-read
                        // the other engines do.
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
            })
        }
    }
}

/// The write-ahead log a caller of our engine keeps itself: one
/// length-prefixed record per write, made durable by one `sync_data` per
/// commit. Under [`lsm_tree::config::FlushDurability::Deferred`] the log is
/// what makes an acknowledged write durable, and a flush syncs nothing.
struct CallerWal {
    file: std::io::BufWriter<std::fs::File>,
}

impl CallerWal {
    fn create(path: &Path) -> std::io::Result<Self> {
        Ok(Self {
            file: std::io::BufWriter::new(std::fs::File::create(path)?),
        })
    }

    /// Appends every write of `inputs` and syncs the log once.
    fn commit(&mut self, inputs: &WorkloadInputs) -> std::io::Result<()> {
        use std::io::Write;
        for (key, value) in inputs.keys.iter().zip(inputs.values.iter()) {
            for field in [key.as_slice(), value.as_slice()] {
                let len = u32::try_from(field.len())
                    .map_err(|_| std::io::Error::other("a field over 4 GiB"))?;
                self.file.write_all(&len.to_le_bytes())?;
                self.file.write_all(field)?;
            }
        }
        self.file.flush()?;
        self.file.get_ref().sync_data()
    }
}

/// Workload: rewrite the entire keyspace into an engine that already
/// holds one copy of it. The first copy is the seed state ([`SeedStates`]),
/// written OUTSIDE the timed window; the timed body writes every key a
/// second time and flushes, so the number reflects overwrite cost (memtable
/// churn over existing keys + a flush that supersedes prior versions) rather
/// than cold first-insert cost. Every timed iteration opens a fresh copy of
/// the seed, so each measurement starts from the same one-copy state.
///
/// The `ours-deferred-wal` series runs our engine the way a caller with its
/// own write-ahead log does: the log is written and synced, then the keys go
/// into the memtable and a deferred flush writes the table without a sync. One
/// sync makes the batch durable, as one does in SurrealKV's commit. The barrier
/// that later makes the deferred tables durable is not timed: a caller runs one
/// per many flushes, as SurrealKV's commit leaves its own memtable flush for
/// later.
fn overwrite_variant(
    run: &mut Run,
    group_name: &str,
    compression: Compression,
    spec: Spec,
    seeds: &mut SeedStates,
) {
    if !run.wants(group_name) {
        return;
    }
    for n in run.sizes_of(spec.sizes) {
        let inputs = WorkloadInputs::of(n, spec.values);
        // Every seed written before the rounds; SurrealKV is not seeded (see
        // `SeedStates`) and writes its first copy per iteration instead.
        let seeded: Vec<(Engine, Option<PathBuf>)> = run
            .engines_of(spec.engines)
            .into_iter()
            .map(|engine| {
                let seed = (engine != Engine::SurrealKv).then(|| {
                    seeds
                        .seed(engine, compression, IndexStrategy::Binary, &inputs)
                        .to_path_buf()
                });
                (engine, seed)
            })
            .collect();
        // The deferred-log arm answers SurrealKV's one-sync commit on the
        // groups SurrealKV's charts share; the blob groups compare blob paths.
        // It starts from our engine's seed whether or not the plain `ours` arm
        // runs too, so `COMPARE_ENGINES=ours-deferred-wal` runs it alone.
        let ours_seed = (spec.values == Values::Pool
            && spec.engines.contains(&Engine::Ours)
            && run.wants_engine("ours-deferred-wal"))
        .then(|| {
            seeds
                .seed(Engine::Ours, compression, IndexStrategy::Binary, &inputs)
                .to_path_buf()
        });
        let mut arms: Vec<_> = seeded
            .into_iter()
            .map(|(engine, seed)| {
                let inputs = &inputs;
                paired::Arm::new(engine.label(), move |iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        match engine {
                            Engine::Ours | Engine::BlobTree(_) => {
                                // First copy (untimed): the seed, so the timed
                                // pass overwrites existing keys.
                                let dir = copy_of(seed.as_deref().expect("ours is seeded"));
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
                            Engine::RocksDb | Engine::RocksDbBlob(_) => {
                                let dir = copy_of(seed.as_deref().expect("rocksdb is seeded"));
                                let db = open_rocksdb(
                                    dir.path(),
                                    compression,
                                    false,
                                    engine.blob_files(),
                                );
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
                                let (rt, tree) = setup_surrealkv_warm(dir.path(), inputs)
                                    .unwrap_or_else(|e| panic!("surrealkv: warm setup: {e}"));
                                settle();
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
                        // The engine and its folder are gone: their removal
                        // goes to disk before the next timed write.
                        settle();
                    }
                    total
                })
            })
            .collect();
        if let Some(seed) = ours_seed {
            let inputs = &inputs;
            arms.push(paired::Arm::new("ours-deferred-wal", move |iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let dir = copy_of(&seed);
                    let tree =
                        ours_config(dir.path(), compression, None, IndexStrategy::Binary, false)
                            .flush_durability(FlushDurability::Deferred)
                            .open()
                            .expect("ours-deferred-wal: open");
                    // Beside the tree's folder, not in it: the engine owns
                    // what its folder holds.
                    let wal_dir = tempfile::tempdir().expect("caller wal dir");
                    let mut wal =
                        CallerWal::create(&wal_dir.path().join("caller.wal")).expect("caller wal");
                    let start = std::time::Instant::now();
                    wal.commit(inputs).expect("ours-deferred-wal: log commit");
                    for ((key, value), seqno) in
                        inputs.keys.iter().zip(inputs.values.iter()).zip(n..)
                    {
                        tree.insert(key, value, seqno);
                    }
                    tree.flush_active_memtable(0)
                        .expect("ours-deferred-wal: overwrite flush");
                    total += start.elapsed();
                    drop((tree, wal, wal_dir, dir));
                    settle();
                }
                total
            }));
        }
        run.measure(group_name, n, arms);
    }
}

/// The sustained overwrite: rounds of rewrites of a hot key set over a tree
/// several memtables large, with compaction running behind the writes.
#[derive(Clone, Copy)]
struct Sustained {
    /// Keys the tree holds before the rewrites.
    keys: u64,
    /// The hot set: the first `hot` of those keys, which the key derivation
    /// spreads over the whole key space.
    hot: u64,
    /// Rewrites of the hot set.
    passes: u64,
}

impl Sustained {
    /// Memtable bytes at which a writer flushes, on both engines.
    const MEMTABLE_BYTES: u64 = 4 << 20;
    /// Target size of a table, on both engines.
    const TABLE_TARGET: u64 = 2 << 20;
    /// L0 runs that start a compaction into L1, on both engines; L1 holds this
    /// many tables' worth, as ours sizes its first level.
    const L0_TRIGGER: u8 = 4;
    /// L0 runs at which a write waits for compaction: RocksDB's default
    /// `level0_stop_writes_trigger`, applied to ours by the writer.
    const L0_STOP: usize = 36;
    /// Deferred flushes one durability barrier covers on ours: up to this many
    /// memtables' worth of tables wait unsynced, a bound a caller's own log
    /// would hold.
    const PERSIST_EVERY: u32 = 8;

    fn new(measuring: bool) -> Self {
        if measuring {
            Self {
                keys: 100_000,
                hot: 20_000,
                passes: 15,
            }
        } else {
            Self {
                keys: 5_000,
                hot: 1_000,
                passes: 3,
            }
        }
    }

    fn writes(self) -> u64 {
        self.hot * self.passes
    }

    /// Bytes of a key and its value.
    fn entry_bytes() -> u64 {
        u64::try_from(16 + VALUE_SIZE).expect("an entry's size fits u64")
    }
}

/// What one sustained run measured besides the writer's time.
#[derive(Clone, Copy, Default)]
struct SustainedSample {
    /// Bytes compaction wrote per byte the rewrites wrote, until compaction
    /// had nothing left to do after them.
    write_amplification: f64,
    /// Bytes on disk over the live data's bytes when the last write landed.
    space_amplification: f64,
    read_p50_us: f64,
    read_p99_us: f64,
}

impl SustainedSample {
    /// The sample of a run of `shape` whose compaction wrote `compacted`
    /// bytes, whose folder held `disk` bytes when the last write landed and
    /// whose reads took `reads` nanoseconds each.
    fn new(shape: Sustained, compacted: u64, disk: u64, reads: &mut [u64]) -> Self {
        Self {
            write_amplification: ratio(compacted, shape.writes() * Sustained::entry_bytes()),
            space_amplification: ratio(disk, shape.keys * Sustained::entry_bytes()),
            read_p50_us: quantile_us(reads, 500),
            read_p99_us: quantile_us(reads, 990),
        }
    }

    fn metrics(self) -> Vec<(&'static str, f64)> {
        vec![
            ("write_amplification", self.write_amplification),
            ("space_amplification", self.space_amplification),
            ("read_p50_us", self.read_p50_us),
            ("read_p99_us", self.read_p99_us),
        ]
    }
}

/// Bytes of the files under `dir`.
fn dir_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| match entry.file_type() {
                    Ok(kind) if kind.is_dir() => dir_bytes(&entry.path()),
                    Ok(_) => entry.metadata().map_or(0, |m| m.len()),
                    Err(_) => 0,
                })
                .sum()
        })
        .unwrap_or(0)
}

/// `a / b` for counts of bytes or nanoseconds. Both stay far below 2^53, where
/// an `f64` holds every integer, so the conversions are exact.
fn ratio(a: u64, b: u64) -> f64 {
    let exact = |x: u64| {
        f64::from(u32::try_from(x >> 21).expect("below 2^53")) * 2_097_152.0
            + f64::from(u32::try_from(x & 0x1F_FFFF).expect("21 bits"))
    };
    exact(a) / exact(b)
}

/// The quantile `per_mille / 1000` of `nanos`, in microseconds.
fn quantile_us(nanos: &mut [u64], per_mille: usize) -> f64 {
    if nanos.is_empty() {
        return 0.0;
    }
    nanos.sort_unstable();
    let at = (nanos.len() - 1) * per_mille / 1_000;
    ratio(nanos[at], 1_000)
}

/// Point reads of random keys until `stop` is set, timed one by one.
fn read_while(stop: &AtomicBool, keys: &[[u8; 16]], mut get: impl FnMut(&[u8])) -> Vec<u64> {
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut nanos = Vec::new();
    while !stop.load(Ordering::Acquire) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        // Below the key count, which fits usize.
        let key = &keys[usize::try_from(state % keys.len() as u64).expect("an index fits usize")];
        let at = std::time::Instant::now();
        get(key);
        nanos.push(u64::try_from(at.elapsed().as_nanos()).unwrap_or(u64::MAX));
    }
    nanos
}

/// Raises its flag as it drops: a writer that panics still releases the
/// threads that run until the flag, so the scope unwinds into a failed bench
/// instead of waiting for them forever.
struct RaiseOnDrop<'a>(&'a AtomicBool);

impl Drop for RaiseOnDrop<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

/// The engine's read counters at one moment of the sustained scenario.
#[cfg(feature = "metrics")]
struct Diagnosis {
    filter_queries: u64,
    filter_skips: u64,
    loads_cached: u64,
    loads_io: u64,
    data_loads: u64,
    index_loads: u64,
    filter_loads: u64,
    blob_reads: u64,
    blob_bytes: u64,
}

#[cfg(feature = "metrics")]
impl Diagnosis {
    fn take(tree: &lsm_tree::AnyTree) -> Self {
        let m = tree.metrics();
        Self {
            filter_queries: m.filter_queries() as u64,
            filter_skips: m.io_skipped_by_filter() as u64,
            loads_cached: m.block_load_cached_count() as u64,
            loads_io: m.block_load_io_count() as u64,
            data_loads: m.data_block_load_count() as u64,
            index_loads: m.index_block_load_count() as u64,
            filter_loads: m.filter_block_load_count() as u64,
            blob_reads: m.blob_read_count() as u64,
            blob_bytes: m.blob_bytes_read(),
        }
    }

    /// Prints what each read cost the engine since `before`, how many L0 runs
    /// the reads saw on average, and how long the writer waited on the L0 stop.
    fn report(
        &self,
        before: &Self,
        reads: u64,
        l0_seen: u64,
        stalled: Duration,
        elapsed: Duration,
        tables: usize,
    ) {
        let per = |now: u64, then: u64| (now - then) as f64 / reads.max(1) as f64;
        eprintln!(
            "sustained diagnosis: {reads} reads, l0 runs seen {:.1}, per read: \
             filter queries {:.2}, filter skips {:.2}, block loads cached {:.2} io {:.2} \
             (data {:.2}, index {:.2}, filter {:.2}), blob reads {:.3} bytes {:.0}; \
             writer stalled {:.1}% of {elapsed:?}; \
             {tables} tables at the end",
            l0_seen as f64 / reads.max(1) as f64,
            per(self.filter_queries, before.filter_queries),
            per(self.filter_skips, before.filter_skips),
            per(self.loads_cached, before.loads_cached),
            per(self.loads_io, before.loads_io),
            per(self.data_loads, before.data_loads),
            per(self.index_loads, before.index_loads),
            per(self.filter_loads, before.filter_loads),
            per(self.blob_reads, before.blob_reads),
            per(self.blob_bytes, before.blob_bytes),
            100.0 * stalled.as_secs_f64() / elapsed.as_secs_f64(),
        );
    }
}

/// Spawns `f` on a scoped thread named `name`, so a profile of the sustained
/// scenario tells its reader, flusher and compactor apart.
fn spawn_named<'scope, 'env, T: Send + 'scope>(
    scope: &'scope std::thread::Scope<'scope, 'env>,
    name: &str,
    f: impl FnOnce() -> T + Send + 'scope,
) -> std::thread::ScopedJoinHandle<'scope, T> {
    std::thread::Builder::new()
        .name(name.to_owned())
        .spawn_scoped(scope, f)
        .expect("spawn a named bench thread")
}

/// Our engine's sustained run: the writer seals a memtable at its size and
/// waits at [`Sustained::L0_STOP`] L0 runs, one thread flushes, one runs the
/// leveled strategy whenever it has work, another reads. Returns the writer's
/// time and the rest of the sample.
fn sustained_ours(
    shape: Sustained,
    compression: Compression,
    blobs: Option<BlobCodec>,
    keys: &[[u8; 16]],
    pool: &ValuePool,
) -> (Duration, SustainedSample) {
    let dir = tempfile::tempdir().expect("ours sustained dir");
    let tree = ours_config(dir.path(), compression, blobs, IndexStrategy::Binary, false)
        .flush_durability(FlushDurability::Deferred)
        .open()
        .expect("ours sustained: open");
    let strategy: Arc<dyn lsm_tree::compaction::CompactionStrategy> = Arc::new(
        lsm_tree::compaction::Leveled::default()
            .with_table_target_size(Sustained::TABLE_TARGET)
            .with_l0_threshold(Sustained::L0_TRIGGER)
            .with_dynamic_level_bytes(true),
    );
    let flush_at = |tree: &lsm_tree::AnyTree| {
        if tree.active_memtable().size() >= Sustained::MEMTABLE_BYTES {
            tree.flush_active_memtable(0)
                .expect("ours sustained: flush");
        }
    };
    let drain = |tree: &lsm_tree::AnyTree, watermark: u64| -> u64 {
        let mut written = 0;
        loop {
            let result = tree
                .compact(Arc::clone(&strategy), watermark)
                .expect("ours sustained: compact");
            written += result.work.measured.written;
            if matches!(
                result.action,
                lsm_tree::compaction::CompactionAction::Nothing
            ) {
                return written;
            }
        }
    };
    // The tree the rewrites start from, compacted until the strategy rests.
    for (seqno, key) in (0..).zip(keys) {
        tree.insert(key, pool.value(seqno), seqno);
        flush_at(&tree);
    }
    tree.flush_active_memtable(0)
        .expect("ours sustained: seed flush");
    drain(&tree, shape.keys);

    #[cfg(feature = "metrics")]
    let before = Diagnosis::take(&tree);
    #[cfg(feature = "metrics")]
    let l0_seen = AtomicU64::new(0);
    #[cfg(feature = "metrics")]
    let stalled = AtomicU64::new(0);

    let stop = AtomicBool::new(false);
    let writing_done = AtomicBool::new(false);
    let watermark = AtomicU64::new(shape.keys);
    let (elapsed, disk, compacted, mut reads) = std::thread::scope(|scope| {
        // Before the first spawn: a panic spawning a later helper releases
        // those already running too.
        let (_writing_done, _stop) = (RaiseOnDrop(&writing_done), RaiseOnDrop(&stop));
        let compactor = spawn_named(scope, "ours-compactor", || {
            let mut written = 0;
            while !stop.load(Ordering::Acquire) {
                let result = tree
                    .compact(Arc::clone(&strategy), watermark.load(Ordering::Acquire))
                    .expect("ours sustained: compact");
                written += result.work.measured.written;
                if matches!(
                    result.action,
                    lsm_tree::compaction::CompactionAction::Nothing
                ) {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            written + drain(&tree, watermark.load(Ordering::Acquire))
        });
        let reader = spawn_named(scope, "ours-reader", || {
            read_while(&stop, keys, |key| {
                #[cfg(feature = "metrics")]
                l0_seen.fetch_add(tree.l0_run_count() as u64, Ordering::Relaxed);
                std::hint::black_box(tree.get(key, MAX_SEQNO).expect("ours sustained: get"));
            })
        });
        // Flushes run on a thread of their own, as RocksDB's do: the writer
        // only seals a full memtable. Under deferred durability a flush writes
        // its table unsynced, and every `PERSIST_EVERY` flushes one barrier
        // syncs them all and writes the manifest once.
        let flusher = spawn_named(scope, "ours-flusher", || {
            let mut flushes = 0u32;
            loop {
                let lock = tree.get_flush_lock();
                let flushed = tree
                    .flush(&lock, watermark.load(Ordering::Acquire))
                    .expect("ours sustained: flush");
                drop(lock);
                if flushed.is_some() {
                    flushes += 1;
                    if flushes.is_multiple_of(Sustained::PERSIST_EVERY) {
                        tree.persist_flushes().expect("ours sustained: persist");
                    }
                } else if writing_done.load(Ordering::Acquire) {
                    // The writer seals its last memtable before it says it is
                    // done, so a flush that found nothing may have run just
                    // before that seal: go round once more unless none waits.
                    if tree.sealed_memtable_count() == 0 {
                        break;
                    }
                } else {
                    std::thread::sleep(Duration::from_micros(100));
                }
            }
        });
        let start = std::time::Instant::now();
        for write in 0..shape.writes() {
            #[cfg(feature = "metrics")]
            let stalled_at = std::time::Instant::now();
            while tree.l0_run_count() >= Sustained::L0_STOP {
                // The compactor runs until `stop`: finished now, it panicked,
                // and L0 would never drain.
                assert!(
                    !compactor.is_finished(),
                    "ours sustained: the compactor stopped early"
                );
                std::thread::yield_now();
            }
            #[cfg(feature = "metrics")]
            stalled.fetch_add(
                u64::try_from(stalled_at.elapsed().as_nanos()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            let seqno = shape.keys + write;
            let key = &keys[usize::try_from(write % shape.hot).expect("an index fits usize")];
            tree.insert(key, pool.value(seqno), seqno);
            watermark.store(seqno + 1, Ordering::Release);
            if tree.active_memtable().size() >= Sustained::MEMTABLE_BYTES {
                // One sealed memtable at a time, as RocksDB's two write
                // buffers allow: the writer waits for the flusher to take it.
                while tree.sealed_memtable_count() > 0 {
                    // The flusher runs until `writing_done`: finished now, it
                    // panicked, and the sealed memtable would never go.
                    assert!(
                        !flusher.is_finished(),
                        "ours sustained: the flusher stopped early"
                    );
                    std::thread::yield_now();
                }
                tree.rotate_memtable();
            }
        }
        tree.rotate_memtable();
        writing_done.store(true, Ordering::Release);
        flusher.join().expect("ours sustained: flusher");
        tree.persist_flushes()
            .expect("ours sustained: last persist");
        let elapsed = start.elapsed();
        let disk = dir_bytes(dir.path());
        stop.store(true, Ordering::Release);
        let reads = reader.join().expect("ours sustained: reader");
        let compacted = compactor.join().expect("ours sustained: compactor");
        (elapsed, disk, compacted, reads)
    });
    #[cfg(feature = "metrics")]
    Diagnosis::take(&tree).report(
        &before,
        reads.len() as u64,
        l0_seen.load(Ordering::Relaxed),
        Duration::from_nanos(stalled.load(Ordering::Relaxed)),
        elapsed,
        tree.table_count(),
    );
    (
        elapsed,
        SustainedSample::new(shape, compacted, disk, &mut reads),
    )
}

/// RocksDB's options for the sustained run: the matched ones every group uses,
/// with the memtable, table and level sizes [`Sustained`] sets for both
/// engines, and statistics on to count compaction bytes.
fn rocksdb_sustained_options(
    compression: Compression,
    blob_files: Option<BlobCodec>,
) -> rocksdb::Options {
    let mut opts = rocksdb_options(compression, false, blob_files);
    opts.set_write_buffer_size(
        usize::try_from(Sustained::MEMTABLE_BYTES).expect("the memtable size fits usize"),
    );
    opts.set_target_file_size_base(Sustained::TABLE_TARGET);
    opts.set_max_bytes_for_level_base(Sustained::TABLE_TARGET * u64::from(Sustained::L0_TRIGGER));
    opts.set_level_zero_file_num_compaction_trigger(i32::from(Sustained::L0_TRIGGER));
    opts.set_level_zero_stop_writes_trigger(
        i32::try_from(Sustained::L0_STOP).expect("the stop trigger fits i32"),
    );
    opts.set_level_compaction_dynamic_level_bytes(true);
    opts.enable_statistics();
    opts
}

/// Waits until RocksDB runs and plans no compaction.
fn rocksdb_quiesce(db: &rocksdb::DB) {
    let busy = |property: &str| {
        db.property_int_value(property)
            .expect("rocksdb: property")
            .unwrap_or(0)
            > 0
    };
    while busy("rocksdb.compaction-pending") || busy("rocksdb.num-running-compactions") {
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// RocksDB's sustained run, its background jobs flushing and compacting as
/// configured; the same writes, reads and measures as [`sustained_ours`].
fn sustained_rocksdb(
    shape: Sustained,
    compression: Compression,
    blob_files: Option<BlobCodec>,
    keys: &[[u8; 16]],
    pool: &ValuePool,
) -> (Duration, SustainedSample) {
    let dir = tempfile::tempdir().expect("rocksdb sustained dir");
    let opts = rocksdb_sustained_options(compression, blob_files);
    let db = rocksdb::DB::open(&opts, dir.path()).expect("rocksdb sustained: open");
    let mut write_opts = rocksdb::WriteOptions::default();
    write_opts.disable_wal(true);
    for (seqno, key) in (0..).zip(keys) {
        db.put_opt(key, pool.value(seqno), &write_opts)
            .expect("rocksdb sustained: seed put");
    }
    db.flush().expect("rocksdb sustained: seed flush");
    rocksdb_quiesce(&db);
    let compacted_before = opts.get_ticker_count(rocksdb::statistics::Ticker::CompactWriteBytes);

    let stop = AtomicBool::new(false);
    let (elapsed, disk, mut reads) = std::thread::scope(|scope| {
        let reader = scope.spawn(|| {
            read_while(&stop, keys, |key| {
                std::hint::black_box(db.get_pinned(key).expect("rocksdb sustained: get"));
            })
        });
        let _stop = RaiseOnDrop(&stop);
        let start = std::time::Instant::now();
        for write in 0..shape.writes() {
            let key = &keys[usize::try_from(write % shape.hot).expect("an index fits usize")];
            db.put_opt(key, pool.value(shape.keys + write), &write_opts)
                .expect("rocksdb sustained: put");
        }
        db.flush().expect("rocksdb sustained: last flush");
        let elapsed = start.elapsed();
        let disk = dir_bytes(dir.path());
        stop.store(true, Ordering::Release);
        let reads = reader.join().expect("rocksdb sustained: reader");
        (elapsed, disk, reads)
    });
    rocksdb_quiesce(&db);
    let compacted =
        opts.get_ticker_count(rocksdb::statistics::Ticker::CompactWriteBytes) - compacted_before;
    (
        elapsed,
        SustainedSample::new(shape, compacted, disk, &mut reads),
    )
}

/// The sustained overwrite groups, uncompressed and at zstd 22: our engine
/// against RocksDB, the writer's time per write timed, write and space
/// amplification and the read percentiles reported beside it.
fn bench_overwrite_sustained(run: &mut Run) {
    let shape = Sustained::new(run.measuring);
    let keys: Vec<[u8; 16]> = (0..shape.keys).map(key_for).collect();
    let pool = ValuePool::new();
    for (group, compression) in [
        ("overwrite_sustained", Compression::None),
        ("overwrite_sustained_zstd22", Compression::Zstd22),
    ] {
        if !run.wants(group) {
            continue;
        }
        let arms = run
            .engines_of(HEAD_TO_HEAD)
            .into_iter()
            .map(|engine| {
                let last = Rc::new(Cell::new(SustainedSample::default()));
                let (keys, pool, sample) = (&keys, &pool, Rc::clone(&last));
                paired::Arm::new(engine.label(), move |iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let (elapsed, measured) = match engine {
                            Engine::RocksDb | Engine::RocksDbBlob(_) => sustained_rocksdb(
                                shape,
                                compression,
                                engine.blob_files(),
                                keys,
                                pool,
                            ),
                            Engine::Ours | Engine::BlobTree(_) => sustained_ours(
                                shape,
                                compression,
                                engine.kv_separated(),
                                keys,
                                pool,
                            ),
                            Engine::SurrealKv => {
                                unreachable!("the sustained groups run ours and rocksdb only")
                            }
                        };
                        total += elapsed;
                        sample.set(measured);
                    }
                    total
                })
                .with_metrics(move || last.get().metrics())
            })
            .collect();
        run.measure(group, shape.writes(), arms);
    }
}

/// L0 tables built before timing the compaction. Their key ranges overlap
/// (the golden-ratio key scatter spreads consecutive indices across the
/// keyspace), so neither engine can "trivially move" them to the next level
/// without rewriting — the timed compaction actually merges + recompresses.
const COMPACTION_FLUSHES: u64 = 6;
/// Worker threads for parallel block compression on both engines (ours via
/// `compaction_threads`; RocksDB via `compression_options_parallel_threads`):
/// the same count every group compresses with.
const COMPACTION_THREADS: usize = COMPRESSION_THREADS;

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
/// directory: zstd at `level`, the shape's threads, and values in blob files
/// when `blobs` names their codec.
fn open_ours_for_compaction(
    dir: &std::path::Path,
    level: i32,
    shape: CompactionShape,
    blobs: Option<BlobCodec>,
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
    Ok(separate_values(apply_preset(config, active_preset()), blobs).open()?)
}

/// RocksDB as the compaction benches open it. The block options are the
/// matched ones every other group uses (see [`rocksdb_options`]): with a bare
/// `Options::default()` RocksDB would write its compaction output without the
/// bloom filter ours builds there, and skip that work.
fn open_rocksdb_for_compaction(
    dir: &std::path::Path,
    level: i32,
    shape: CompactionShape,
    blob_files: Option<BlobCodec>,
) -> Result<rocksdb::DB, Box<dyn std::error::Error>> {
    let mut opts = rocksdb_options(Compression::None, false, blob_files);
    // Hold every table where the setup put it until the compaction timed.
    opts.set_disable_auto_compactions(true);
    opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
    // (window_bits, level, strategy, max_dict_bytes); -14 = default window.
    opts.set_compression_options(-14, level, 0, 0);
    // Our compaction threads drive both the range split and the
    // block-compression pool, so RocksDB gets both knobs.
    opts.set_compression_options_parallel_threads(
        i32::try_from(shape.threads()).expect("thread count fits i32"),
    );
    match shape {
        CompactionShape::Major => opts.set_max_subcompactions(1),
        CompactionShape::Split => {
            opts.set_max_subcompactions(
                u32::try_from(SUBCOMPACTION_THREADS).expect("thread count fits u32"),
            );
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
    let total = u64::try_from(inputs.keys.len()).expect("key count fits u64");
    let flush_points: Vec<u64> = (1..COMPACTION_FLUSHES)
        .map(|b| (b * total) / COMPACTION_FLUSHES)
        .collect();
    let rows = || inputs.keys.iter().zip(inputs.values.iter());
    match engine {
        Engine::Ours | Engine::BlobTree(_) => {
            let tree = open_ours_for_compaction(dir, level, shape, engine.kv_separated())?;
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
        Engine::RocksDb | Engine::RocksDbBlob(_) => {
            let db = open_rocksdb_for_compaction(dir, level, shape, engine.blob_files())?;
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
        // The compaction benches are zstd-level workloads and SurrealKV has no
        // level setting, so they run the head-to-head arms only.
        Engine::SurrealKv => {
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
        Engine::Ours | Engine::BlobTree(_) => {
            let tree = open_ours_for_compaction(dir, level, shape, engine.kv_separated())?;
            let start = std::time::Instant::now();
            tree.major_compact(u64::MAX, 0)?;
            Ok(start.elapsed())
        }
        Engine::RocksDb | Engine::RocksDbBlob(_) => {
            let db = open_rocksdb_for_compaction(dir, level, shape, engine.blob_files())?;
            let compact_opts = force_bottommost();
            let start = std::time::Instant::now();
            db.compact_range_opt(None::<&[u8]>, None::<&[u8]>, &compact_opts);
            Ok(start.elapsed())
        }
        Engine::SurrealKv => {
            unreachable!("the compaction benches run ours and rocksdb only")
        }
    }
}

fn bench_compaction(run: &mut Run) {
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
    compaction_variant(run, "major_compact_zstd3", 3, CompactionShape::Major);
}

/// One compaction head-to-head: per size and engine, the starting state is
/// written once before the rounds (see [`write_compaction_state`]) and every
/// iteration compacts a fresh copy of it, so an iteration times its
/// compaction alone.
fn compaction_variant(run: &mut Run, group_name: &str, level: i32, shape: CompactionShape) {
    if !run.wants(group_name) {
        return;
    }
    let sizes: &[u64] = match shape {
        CompactionShape::Major => &[10_000, 40_000],
        CompactionShape::Split => &[40_000, 100_000],
    };
    for n in run.sizes_of(sizes) {
        let inputs = match shape {
            CompactionShape::Major => WorkloadInputs::build(n),
            CompactionShape::Split => WorkloadInputs::incompressible(n),
        };
        let states: Vec<(Engine, tempfile::TempDir)> = run
            .engines_of(HEAD_TO_HEAD)
            .into_iter()
            .map(|engine| {
                let dir = tempfile::tempdir().expect("compaction state tempdir");
                write_compaction_state(engine, level, shape, &inputs, dir.path()).unwrap_or_else(
                    |e| panic!("compaction setup failed for {}: {e}", engine.label()),
                );
                (engine, dir)
            })
            .collect();
        let arms = states
            .iter()
            .map(|(engine, state)| {
                let engine = *engine;
                paired::Arm::new(engine.label(), move |iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let work = copy_of(state.path());
                        total += time_compaction(engine, level, shape, work.path()).unwrap_or_else(
                            |e| panic!("compaction failed for {}: {e}", engine.label()),
                        );
                        drop(work);
                        settle();
                    }
                    total
                })
            })
            .collect();
        run.measure(group_name, n, arms);
    }
}

/// High-entropy 256-byte value: an xorshift fill so zstd spends the most
/// time it can per block during sub-compaction, which is the work the
/// range-parallel split exists to spread across threads.
fn value_incompressible(i: u64) -> Vec<u8> {
    random_value(i, VALUE_SIZE)
}

/// `len` bytes of an xorshift stream seeded by `i`: the same on every run and
/// host, and different for every `i`.
fn random_value(i: u64, len: usize) -> Vec<u8> {
    let mut s = i.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut v = vec![0_u8; len];
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
        Self {
            keys,
            values,
            kind: Values::Random(VALUE_SIZE),
        }
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
fn bench_subcompaction(run: &mut Run) {
    compaction_variant(run, "subcompaction_zstd3", 3, CompactionShape::Split);
}

/// What each group measures, as the published page states it next to the
/// group's chart. Kept here, beside the code that does the measuring.
const GROUP_NOTES: &[(&str, &str)] = &[
    (
        "write_throughput",
        "Bulk insert of N fresh keys into an empty engine, then one flush. Covers open, memtable inserts and the flush that writes the table. Both engines compress blocks on 4 threads.",
    ),
    (
        "overwrite",
        "Every key written a second time into an engine that already holds one copy, then one flush. Memtable churn over existing keys and a superseding flush. Both engines compress blocks on 4 threads. ours-deferred-wal: our engine with a caller-kept log, the log written and synced once and the flush deferred, one sync per batch as in SurrealKV's commit.",
    ),
    (
        "overwrite_sustained",
        "100,000 keys, then 15 rewrites of a hot set of 20,000 of them, with compaction behind the writes and a reader alongside. 4 MiB memtables, 2 MiB tables, 4 L0 runs per compaction, no write-ahead log on either. Ours flushes on a thread of its own as RocksDB does, one sealed memtable at a time as RocksDB's two write buffers allow, with deferred durability and one sync barrier per 8 flushes; it runs leveled compaction on another thread and stops writes at 36 L0 runs, RocksDB's stop trigger. Time per write; beside it the compaction bytes per written byte until compaction rests, the disk bytes over live bytes when the last write lands, and the reader's p50 and p99.",
    ),
    (
        "point_read",
        "One get per stored key on a warm engine. Also our hash index, ribbon locator and row cache, and RocksDB's hash index, on the same chart.",
    ),
    (
        "point_read_cold",
        "One get per stored key on an engine opened on a fresh copy of the state, its files dropped from the OS page cache and its block cache empty. Only the reads are timed, each on its own; beside the mean, their p50 and p99.",
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

/// Where an arm keeps its values, which decides the arm its ratio is taken
/// against: a tree with blob files against RocksDB with blob files under the
/// same codec, every other arm against plain RocksDB. The page shows one
/// layout at a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Layout {
    /// Values in the tables.
    Inline,
    /// Values in blob files.
    Separated,
}

impl Layout {
    const ALL: [Self; 2] = [Self::Inline, Self::Separated];

    /// A blob arm keeps its values in blob files whatever its codec; every
    /// blob arm is one of [`HEAD_TO_HEAD`].
    fn of(label: &str) -> Self {
        if blob_codec_of(label).is_some() {
            Self::Separated
        } else {
            Self::Inline
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Inline => "inline",
            Self::Separated => "separated",
        }
    }

    /// The arms the ratios of this layout are taken against.
    fn baselines(self) -> Vec<&'static str> {
        let mut baselines: Vec<&'static str> = HEAD_TO_HEAD
            .iter()
            .filter(|engine| Self::of(engine.label()) == self)
            .map(|engine| baseline_of(engine.label()))
            .collect();
        baselines.dedup();
        baselines
    }
}

/// The codec of the blob arm labelled `label`, ours or RocksDB's; `None` for
/// an arm that keeps its values in the tables.
fn blob_codec_of(label: &str) -> Option<BlobCodec> {
    HEAD_TO_HEAD
        .iter()
        .find(|engine| engine.label() == label)
        .and_then(|engine| engine.kv_separated().or(engine.blob_files()))
}

/// The arm the ratio of the arm labelled `label` is taken against: RocksDB
/// with blob files under the same codec for a blob arm, so the ratio compares
/// the engines and not the codecs, plain RocksDB for every other arm.
fn baseline_of(label: &str) -> &'static str {
    blob_codec_of(label)
        .map_or(Engine::RocksDb, Engine::RocksDbBlob)
        .label()
}

/// One run of the harness: how groups are measured, which run, and what they
/// measured.
struct Run {
    settings: paired::Settings,
    /// Only groups whose name contains it run, or whose name it is when it
    /// ends in `$`.
    filter: Option<String>,
    /// Started by `cargo bench`, which passes `--bench`. Without it (`cargo
    /// test` runs a bench target too) every group runs once as a smoke test
    /// and no summary is written, so a test run neither takes the minutes a
    /// measurement does nor replaces the last measurement's summary.
    measuring: bool,
    /// Only arms with these labels run, from `COMPARE_ENGINES` (comma
    /// separated): one engine's profile or system calls without the others'.
    engines: Option<Vec<String>>,
    /// Only these key counts run, from `COMPARE_SIZES` (comma separated): one
    /// size's profile without the others' setup and rounds.
    sizes: Option<Vec<u64>>,
    results: Vec<paired::ArmResult>,
}

impl Run {
    const USAGE: &str = "usage: compare [FILTER] [--warm-up SECS] [--sample SECS] \
        [--budget SECS] [--min-rounds N] [--max-rounds N]";

    /// Settings from the command line. A measurement defaults to the published
    /// run's settings, a smoke test to one round with no warm-up; a flag given
    /// explicitly holds in either.
    ///
    /// The budget is per group and key count. Ten rounds is the floor: the
    /// confidence interval of a median needs six values to reach 95% at all,
    /// and ten give it the ranks 2 and 9. The defaults keep the whole matrix
    /// at a few minutes on the bench runner.
    fn from_args() -> Self {
        let (mut warm_up, mut sample_target, mut budget) = (None, None, None);
        let (mut min_rounds, mut max_rounds) = (None, None);
        let mut measuring = false;
        let mut filter = None;
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            let mut value = || {
                args.next()
                    .unwrap_or_else(|| panic!("{arg} needs a value\n{}", Self::USAGE))
            };
            let secs = |v: String| {
                Duration::try_from_secs_f64(v.parse().unwrap_or(f64::NAN))
                    .unwrap_or_else(|_| panic!("not a number of seconds: {v}\n{}", Self::USAGE))
            };
            let count = |v: String| {
                v.parse::<usize>()
                    .unwrap_or_else(|_| panic!("not a count: {v}\n{}", Self::USAGE))
            };
            match arg.as_str() {
                // `cargo bench` passes it to every bench binary it runs.
                "--bench" => measuring = true,
                // A test runner listing tests (nextest's `--list --format
                // terse`): this target has none of its own to offer.
                "--list" => std::process::exit(0),
                "--format" => drop(value()),
                "--warm-up" => warm_up = Some(secs(value())),
                "--sample" => sample_target = Some(secs(value())),
                "--budget" => budget = Some(secs(value())),
                "--min-rounds" => min_rounds = Some(count(value())),
                "--max-rounds" => max_rounds = Some(count(value())),
                "-h" | "--help" => {
                    eprintln!("{}", Self::USAGE);
                    std::process::exit(0);
                }
                flag if flag.starts_with('-') => panic!("unknown flag {flag}\n{}", Self::USAGE),
                _ => filter = Some(arg),
            }
        }
        let settings = if measuring {
            paired::Settings {
                warm_up: warm_up.unwrap_or(Duration::from_millis(300)),
                sample_target: sample_target.unwrap_or(Duration::from_millis(20)),
                budget: budget.unwrap_or(Duration::from_secs(4)),
                min_rounds: min_rounds.unwrap_or(10),
                max_rounds: max_rounds.unwrap_or(40),
            }
        } else {
            paired::Settings {
                warm_up: warm_up.unwrap_or(Duration::ZERO),
                sample_target: sample_target.unwrap_or(Duration::ZERO),
                budget: budget.unwrap_or(Duration::ZERO),
                min_rounds: min_rounds.unwrap_or(1),
                max_rounds: max_rounds.unwrap_or(1),
            }
        };
        assert!(
            settings.min_rounds >= 1 && settings.min_rounds <= settings.max_rounds,
            "rounds: need 1 <= --min-rounds <= --max-rounds\n{}",
            Self::USAGE
        );
        let engines = std::env::var("COMPARE_ENGINES")
            .ok()
            .map(|list| list.split(',').map(|e| e.trim().to_owned()).collect());
        let sizes = std::env::var("COMPARE_SIZES").ok().map(|list| {
            list.split(',')
                .map(|n| {
                    n.trim()
                        .parse()
                        .unwrap_or_else(|_| panic!("COMPARE_SIZES: not a key count: {n}"))
                })
                .collect()
        });
        Self {
            settings,
            filter,
            measuring,
            engines,
            sizes,
            results: Vec::new(),
        }
    }

    /// Whether group `group` runs: its name contains the filter, or is the
    /// filter exactly when it ends in `$` (`write_throughput$` leaves out
    /// `write_throughput_zstd22`).
    fn wants(&self, group: &str) -> bool {
        self.filter
            .as_deref()
            .is_none_or(|f| match f.strip_suffix('$') {
                Some(exact) => group == exact,
                None => group.contains(f),
            })
    }

    /// Whether the arm labelled `label` runs.
    fn wants_engine(&self, label: &str) -> bool {
        self.engines
            .as_ref()
            .is_none_or(|engines| engines.iter().any(|e| e == label))
    }

    /// The key counts of `sizes` that run, taken out so a group's loop can
    /// measure into the run.
    fn sizes_of(&self, sizes: &[u64]) -> Vec<u64> {
        sizes
            .iter()
            .copied()
            .filter(|n| self.sizes.as_ref().is_none_or(|wanted| wanted.contains(n)))
            .collect()
    }

    /// The engines of `engines` that run, so a filtered run does not set up
    /// the states of the arms it leaves out.
    fn engines_of(&self, engines: &[Engine]) -> Vec<Engine> {
        engines
            .iter()
            .copied()
            .filter(|engine| self.wants_engine(engine.label()))
            .collect()
    }

    fn measure(&mut self, group: &str, n: u64, mut arms: Vec<paired::Arm<'_>>) {
        arms.retain(|arm| self.wants_engine(arm.label));
        if arms.is_empty() {
            return;
        }
        let results = paired::run(group, n, &baseline_of, arms, &self.settings);
        self.results.extend(results);
    }
}

/// Where the summary goes: `head-to-head/` under the cargo target directory.
fn summary_path() -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR")
        .map_or_else(|| PathBuf::from("target"), PathBuf::from)
        .join("head-to-head")
        .join("summary.json")
}

fn estimate_json(e: paired::Estimate) -> serde_json::Value {
    serde_json::json!({ "median": e.median, "lower": e.lower, "upper": e.upper })
}

/// Writes the summary the published page is drawn from: the run's preset, the
/// baselines of each value layout, what each group measures, and every arm's
/// layout, time per operation, baseline and time relative to it, each a
/// median with its 95% interval.
fn write_summary(run: &Run) -> std::io::Result<PathBuf> {
    let results: Vec<serde_json::Value> = run
        .results
        .iter()
        .map(|r| {
            serde_json::json!({
                "group": r.group,
                "engine": r.label,
                "values": Layout::of(r.label).label(),
                "n": r.n,
                "rounds": r.rounds,
                "ns_per_op": estimate_json(r.ns_per_op),
                "baseline": r.baseline,
                "vs_baseline": r.vs_baseline.map(estimate_json),
                "metrics": r.metrics.iter().map(|(name, e)| serde_json::json!({
                    "name": name,
                    "median": e.median,
                    "lower": e.lower,
                    "upper": e.upper,
                })).collect::<Vec<_>>(),
            })
        })
        .collect();
    let groups: Vec<serde_json::Value> = GROUP_NOTES
        .iter()
        .map(|(name, note)| serde_json::json!({ "name": name, "note": note }))
        .collect();
    let summary = serde_json::json!({
        "preset": active_preset().label(),
        "baselines": Layout::ALL
            .iter()
            .map(|layout| (layout.label().to_owned(), layout.baselines().into()))
            .collect::<serde_json::Map<String, serde_json::Value>>(),
        "settings": {
            "warm_up_s": run.settings.warm_up.as_secs_f64(),
            "sample_s": run.settings.sample_target.as_secs_f64(),
            "budget_s": run.settings.budget.as_secs_f64(),
            "min_rounds": run.settings.min_rounds,
            "max_rounds": run.settings.max_rounds,
        },
        "groups": groups,
        "results": results,
    });
    let path = summary_path();
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(&path, serde_json::to_vec(&summary)?)?;
    Ok(path)
}

/// Fails unless RocksDB writes its blob files at the level each blob arm
/// names: the level comes from an options string, which a misspelled key or
/// a struct RocksDB no longer reads would leave at its default unnoticed.
fn check_rocksdb_blob_levels() {
    for codec in [BlobCodec::Zstd, BlobCodec::Zstd22] {
        let Some(level) = codec.zstd_level() else {
            continue;
        };
        let dir = tempfile::tempdir().expect("tempdir");
        drop(
            rocksdb::DB::open(
                &rocksdb_options(Compression::None, false, Some(codec)),
                dir.path(),
            )
            .expect("rocksdb: open"),
        );
        // RocksDB can leave several OPTIONS files and `read_dir` lists them in
        // no order; the newest has the highest number, and the zero-padded
        // names sort by it.
        let newest = std::fs::read_dir(dir.path())
            .expect("rocksdb: list")
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|path| {
                path.file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with("OPTIONS-"))
            })
            .max()
            .expect("rocksdb wrote no OPTIONS file");
        let options = std::fs::read_to_string(newest).expect("rocksdb: OPTIONS");
        let blob = options
            .lines()
            .find(|line| line.trim_start().starts_with("blob_compression_opts="))
            .expect("rocksdb: no blob_compression_opts in OPTIONS");
        assert!(
            blob.contains(&format!(";level={level};"))
                || blob.contains(&format!("{{level={level};"))
                || blob.contains(&format!(";level={level}}}")),
            "rocksdb blob files not at level {level}: {blob}"
        );
    }
}

fn main() {
    let mut run = Run::from_args();
    check_rocksdb_blob_levels();
    let mode = if run.measuring {
        "measurement"
    } else {
        "smoke test (no --bench)"
    };
    eprintln!("compare-rocksdb: {mode}, {:?}", run.settings);
    bench_write_throughput(&mut run);
    bench_seeded(&mut run);
    bench_overwrite_sustained(&mut run);
    bench_compaction(&mut run);
    bench_subcompaction(&mut run);
    if !run.measuring {
        return;
    }
    match write_summary(&run) {
        Ok(path) => eprintln!("compare-rocksdb: summary at {}", path.display()),
        Err(e) => panic!("compare-rocksdb: writing the summary failed: {e}"),
    }
}

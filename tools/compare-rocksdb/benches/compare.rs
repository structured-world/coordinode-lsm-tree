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
//! ## Engine matrix
//!
//! The shared workload closure is parameterised over an [`Engine`]
//! enum so the per-engine glue (open, put, get, flush, close) lives
//! in exactly one place per engine and the workload code stays
//! engine-agnostic. Adding a third engine (Pebble, LevelDB,
//! sled, …) is "add a variant + match arm" — no workload rewrite.
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
//! Within each group both engines run in the SAME process and the SAME
//! invocation, so criterion plots them as an overlay (ours vs rocksdb)
//! on one chart. Because the comparison is a ratio measured on one host
//! in one run, it stays meaningful even if the bench host's CPU changes
//! between runs — the absolute numbers move, the relative gap does not.
//!
//! ## Workload coverage
//!
//! - `write_throughput/{1k,10k}` — bulk insert N keys, 256-byte
//!   values, random keys. Cold-start: each iteration opens an empty
//!   engine, writes N, flushes. Dominated by the fixed open + flush
//!   cost at small N.
//! - `point_read/{1k,10k}` — read N random keys from an engine
//!   pre-populated with N keys and flushed to disk. Warm: the engine
//!   is opened + populated + flushed ONCE outside the timed window,
//!   so the measurement is steady-state read latency (block cache +
//!   bloom filter + on-disk block fetch), not setup cost.
//! - `range_scan/{1k,10k}` — full forward scan reading every value
//!   from a warm, pre-populated engine. Steady-state sequential-scan
//!   throughput (block decode + iterator advance).
//! - `seek_random/{1k,10k}` — seek to each (scattered) key and read
//!   the value at the cursor, on a warm engine. Seek-then-read latency
//!   (index descent + cursor positioning + block decode).
//! - `overwrite/{1k,10k}` — rewrite the whole keyspace into an engine
//!   that already holds one copy (the first copy is written outside the
//!   timed window). Overwrite cost (memtable churn over existing keys +
//!   a superseding flush), distinct from cold first-insert.
//!
//! Each of the above also has a `_zstd22` sibling. Not yet portable
//! head-to-head: `readwhilewriting` (concurrency) and `mergerandom`
//! (merge-operator semantics differ across engines) from [#244]'s list.

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
// `Guard` is a trait, used (not dead) for its `.value()` method on the
// `IterGuardImpl` items yielded by `tree.iter()` / `tree.range()` in the
// range_scan and seek_random scenarios — there is no direct path
// reference, so it reads as unused at a glance but the import is required
// for method resolution (clippy `-D warnings` confirms it is live).
use lsm_tree::{
    AbstractTree, CompressionType, Config, Guard, MAX_SEQNO, SequenceNumberCounter,
    config::CompressionPolicy,
};

/// Engine under test. The harness runs each workload once per
/// variant and emits per-engine timings under the same criterion
/// `BenchmarkGroup`, so the gh-pages dashboard can plot them
/// side-by-side.
#[derive(Debug, Clone, Copy)]
enum Engine {
    Ours,
    RocksDb,
}

impl Engine {
    fn label(self) -> &'static str {
        match self {
            Self::Ours => "ours",
            Self::RocksDb => "rocksdb",
        }
    }
}

/// Compression axis of the engine matrix. Each workload runs once per
/// variant so the dashboard plots the `None` baseline and the
/// high-ratio zstd path side-by-side, with both engines configured the
/// same way per variant (apples-to-apples).
#[derive(Debug, Clone, Copy)]
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

/// Fixed 256-byte value. The first 8 bytes vary with the key so
/// the engines can't dedupe / compress the entire payload to a
/// single block.
fn value_for(i: u64) -> Vec<u8> {
    let mut v = vec![0xAA_u8; 256];
    v[..8].copy_from_slice(&i.to_be_bytes());
    v
}

/// Precomputed (key, value) workload for a given `n_keys`. Built
/// ONCE outside the timing loop so the bench measures engine
/// write throughput, not the per-key
/// `key_for(i)` / `value_for(i)` allocation + fill cost (which
/// otherwise dominates at the 1k / 10k scale).
struct WorkloadInputs {
    keys: Vec<[u8; 16]>,
    values: Vec<Vec<u8>>,
}

impl WorkloadInputs {
    fn build(n_keys: u64) -> Self {
        let n = usize::try_from(n_keys).expect("n_keys fits in usize");
        let mut keys = Vec::with_capacity(n);
        let mut values = Vec::with_capacity(n);
        for i in 0..n_keys {
            keys.push(key_for(i));
            values.push(value_for(i));
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
fn rocksdb_options(compression: Compression) -> rocksdb::Options {
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    let cache = rocksdb::Cache::new_lru_cache(16 * 1024 * 1024);
    block_opts.set_block_cache(&cache);
    // bits_per_key = 10.0, block_based = false → modern full-block filter,
    // the closest match to our `BitsPerKey(10.0)` policy.
    block_opts.set_bloom_filter(10.0, false);
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
fn open_ours(
    dir: &std::path::Path,
    compression: Compression,
) -> Result<lsm_tree::AnyTree, Box<dyn std::error::Error>> {
    let config = Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    );
    let config = match compression {
        Compression::None => {
            config.data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
        }
        Compression::Zstd22 => config.data_block_compression_policy(CompressionPolicy::all(
            CompressionType::Zstd(Compression::ZSTD_MAX_LEVEL),
        )),
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
    engine: Engine,
    compression: Compression,
    inputs: &WorkloadInputs,
) -> Result<Duration, Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let start = std::time::Instant::now();
    let elapsed = match engine {
        Engine::Ours => {
            let tree = open_ours(dir.path(), compression)?;
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
            let opts = rocksdb_options(compression);
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
    for &n in &[1_000_u64, 10_000_u64] {
        // Precompute the keys + values ONCE per `n` (outside the
        // criterion warmup / measurement loop), so the timed body
        // does no per-iteration allocation.
        let inputs = WorkloadInputs::build(n);
        group.throughput(Throughput::Elements(n));
        for engine in [Engine::Ours, Engine::RocksDb] {
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
                        total += run_write_throughput(engine, compression, &inputs).unwrap_or_else(
                            |e| panic!("run_write_throughput failed for {}: {e}", engine.label()),
                        );
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
/// the engine here is opened, populated and flushed ONCE — outside
/// the criterion timing window — and kept warm for the whole
/// benchmark. The timed body issues one `get` per stored key, so the
/// number reflects warm steady-state read latency (lookup path +
/// bloom filter + block decode), NOT the open / write / flush setup
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
fn bench_point_read(c: &mut Criterion) {
    // `None` baseline + `Zstd22` high-ratio variant in sibling groups,
    // mirroring `bench_write_throughput`.
    point_read_variant(c, "point_read", Compression::None);
    point_read_variant(c, "point_read_zstd22", Compression::Zstd22);
}

fn point_read_variant(c: &mut Criterion, group_name: &str, compression: Compression) {
    let mut group = c.benchmark_group(group_name);
    for &n in &[1_000_u64, 10_000_u64] {
        let inputs = WorkloadInputs::build(n);
        group.throughput(Throughput::Elements(n));
        for engine in [Engine::Ours, Engine::RocksDb] {
            group.bench_with_input(BenchmarkId::new(engine.label(), n), &n, |b, _| {
                // The temp dir + engine handle outlive every timed
                // iteration: build the on-disk database once here so
                // the criterion warmup / measurement loop only ever
                // pays for reads, never for open / write / flush.
                let dir = tempfile::tempdir().expect("tempdir");
                match engine {
                    Engine::Ours => {
                        let tree = open_ours(dir.path(), compression).expect("ours: open");
                        for ((key, value), seqno) in
                            inputs.keys.iter().zip(inputs.values.iter()).zip(0u64..)
                        {
                            tree.insert(key, value, seqno);
                        }
                        tree.flush_active_memtable(0).expect("ours: flush");
                        // One-time hit check OUTSIDE the timed window: enforce
                        // the workload contract ("read every stored key") so a
                        // setup/flush regression can't silently become a
                        // miss-read benchmark, without taxing each timed `get`
                        // with a branch. `MAX_SEQNO` (not `u64::MAX`, whose MSB
                        // is reserved) reads the latest visible version.
                        for key in &inputs.keys {
                            assert!(
                                tree.get(key, MAX_SEQNO).expect("ours: verify").is_some(),
                                "ours: key unexpectedly missing"
                            );
                        }
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
                    Engine::RocksDb => {
                        // Bloom (10 bits/key) + 16 MiB cache + no compression,
                        // matching our engine's defaults so the per-`get`
                        // overhead is attributable, not a config artefact —
                        // see `rocksdb_options`.
                        let opts = rocksdb_options(compression);
                        let db = rocksdb::DB::open(&opts, dir.path()).expect("rocksdb: open");
                        let mut write_opts = rocksdb::WriteOptions::default();
                        write_opts.disable_wal(true);
                        for (key, value) in inputs.keys.iter().zip(inputs.values.iter()) {
                            db.put_opt(key, value, &write_opts).expect("rocksdb: put");
                        }
                        db.flush().expect("rocksdb: flush");
                        // Same one-time, outside-the-timed-window hit check as
                        // the `ours` arm: enforce "read every stored key"
                        // without a per-read branch in the measured loop.
                        for key in &inputs.keys {
                            assert!(
                                db.get(key).expect("rocksdb: verify").is_some(),
                                "rocksdb: key unexpectedly missing"
                            );
                        }
                        b.iter_custom(|iters| {
                            let start = std::time::Instant::now();
                            for _ in 0..iters {
                                for key in &inputs.keys {
                                    let got = db.get(key).expect("rocksdb: get");
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

/// Opens a RocksDB instance at `dir` with the matched options, populates
/// it with `inputs` (WAL disabled, matching the untimed populate phase of
/// our warm read scenarios), and flushes. Used by the warm read groups
/// (`range_scan`, `seek_random`) so their per-engine setup lives in one
/// place rather than being copy-pasted per scenario.
fn populate_rocksdb(
    dir: &std::path::Path,
    compression: Compression,
    inputs: &WorkloadInputs,
) -> rocksdb::DB {
    let opts = rocksdb_options(compression);
    let db = rocksdb::DB::open(&opts, dir).expect("rocksdb: open");
    let mut write_opts = rocksdb::WriteOptions::default();
    write_opts.disable_wal(true);
    for (key, value) in inputs.keys.iter().zip(inputs.values.iter()) {
        db.put_opt(key, value, &write_opts).expect("rocksdb: put");
    }
    db.flush().expect("rocksdb: flush");
    db
}

/// Populates our engine at `dir` and flushes, returning the warm handle.
/// Companion to [`populate_rocksdb`] for the warm read groups.
fn populate_ours(
    dir: &std::path::Path,
    compression: Compression,
    inputs: &WorkloadInputs,
) -> lsm_tree::AnyTree {
    let tree = open_ours(dir, compression).expect("ours: open");
    for ((key, value), seqno) in inputs.keys.iter().zip(inputs.values.iter()).zip(0u64..) {
        tree.insert(key, value, seqno);
    }
    tree.flush_active_memtable(0).expect("ours: flush");
    tree
}

fn bench_range_scan(c: &mut Criterion) {
    range_scan_variant(c, "range_scan", Compression::None);
    range_scan_variant(c, "range_scan_zstd22", Compression::Zstd22);
}

/// Workload: full forward scan reading every value. The engine is
/// populated + flushed ONCE outside the timed window (warm, like
/// [`point_read_variant`]); the timed body iterates the whole keyspace
/// front-to-back and touches each value, so the number reflects
/// steady-state sequential-scan throughput (block decode + iterator
/// advance), not setup cost.
fn range_scan_variant(c: &mut Criterion, group_name: &str, compression: Compression) {
    let mut group = c.benchmark_group(group_name);
    for &n in &[1_000_u64, 10_000_u64] {
        let inputs = WorkloadInputs::build(n);
        group.throughput(Throughput::Elements(n));
        for engine in [Engine::Ours, Engine::RocksDb] {
            group.bench_with_input(BenchmarkId::new(engine.label(), n), &n, |b, _| {
                let dir = tempfile::tempdir().expect("tempdir");
                match engine {
                    Engine::Ours => {
                        let tree = populate_ours(dir.path(), compression, &inputs);
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
                    Engine::RocksDb => {
                        let db = populate_rocksdb(dir.path(), compression, &inputs);
                        b.iter_custom(|iters| {
                            let start = std::time::Instant::now();
                            for _ in 0..iters {
                                for kv in db.iterator(rocksdb::IteratorMode::Start) {
                                    let (_k, v) = kv.expect("rocksdb: scan");
                                    std::hint::black_box(v);
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

fn bench_seek_random(c: &mut Criterion) {
    seek_random_variant(c, "seek_random", Compression::None);
    seek_random_variant(c, "seek_random_zstd22", Compression::Zstd22);
}

/// Workload: seek to each key (in insertion order, i.e. scattered across
/// the sorted keyspace) and read the single value the cursor lands on.
/// Warm: the engine is populated + flushed ONCE outside the timed window.
/// This measures seek-then-read latency (index descent + block decode +
/// cursor positioning), the closest head-to-head analogue of a
/// `seekrandom` workload.
fn seek_random_variant(c: &mut Criterion, group_name: &str, compression: Compression) {
    let mut group = c.benchmark_group(group_name);
    for &n in &[1_000_u64, 10_000_u64] {
        let inputs = WorkloadInputs::build(n);
        group.throughput(Throughput::Elements(n));
        for engine in [Engine::Ours, Engine::RocksDb] {
            group.bench_with_input(BenchmarkId::new(engine.label(), n), &n, |b, _| {
                let dir = tempfile::tempdir().expect("tempdir");
                match engine {
                    Engine::Ours => {
                        let tree = populate_ours(dir.path(), compression, &inputs);
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
                    Engine::RocksDb => {
                        let db = populate_rocksdb(dir.path(), compression, &inputs);
                        b.iter_custom(|iters| {
                            let start = std::time::Instant::now();
                            for _ in 0..iters {
                                for key in &inputs.keys {
                                    let mut it = db.iterator(rocksdb::IteratorMode::From(
                                        key,
                                        rocksdb::Direction::Forward,
                                    ));
                                    let got = it.next().map(|kv| kv.expect("rocksdb: seek").1);
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

fn bench_overwrite(c: &mut Criterion) {
    overwrite_variant(c, "overwrite", Compression::None);
    overwrite_variant(c, "overwrite_zstd22", Compression::Zstd22);
}

/// Workload: rewrite the entire keyspace into an engine that already
/// holds one copy of it. The first populate + flush happens OUTSIDE the
/// timed window; the timed body writes every key a second time and
/// flushes, so the number reflects overwrite cost (memtable churn over
/// existing keys + a flush that supersedes prior versions) rather than
/// cold first-insert cost. A fresh engine is built per timed iteration
/// so each measurement starts from the same one-copy state.
fn overwrite_variant(c: &mut Criterion, group_name: &str, compression: Compression) {
    let mut group = c.benchmark_group(group_name);
    for &n in &[1_000_u64, 10_000_u64] {
        let inputs = WorkloadInputs::build(n);
        group.throughput(Throughput::Elements(n));
        for engine in [Engine::Ours, Engine::RocksDb] {
            group.bench_with_input(BenchmarkId::new(engine.label(), n), &n, |b, _| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let dir = tempfile::tempdir().expect("tempdir");
                        match engine {
                            Engine::Ours => {
                                // First copy (untimed): populate + flush so the
                                // timed pass overwrites existing keys.
                                let tree = populate_ours(dir.path(), compression, &inputs);
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
                                let db = populate_rocksdb(dir.path(), compression, &inputs);
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

criterion_group!(
    benches,
    bench_write_throughput,
    bench_point_read,
    bench_range_scan,
    bench_seek_random,
    bench_overwrite
);
criterion_main!(benches);

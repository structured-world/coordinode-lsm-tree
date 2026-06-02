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
//!
//! Follow-up commits expand to range scans, mixed YCSB-A/C, and
//! bloom-filter negative probes per [#244]'s workload list.

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lsm_tree::{AbstractTree, Config, MAX_SEQNO, SequenceNumberCounter};

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
///   - **Compression: None on both sides.** lsm-tree's default
///     `data_block_compression_policy` writes L0 with `None`, so
///     RocksDB is set to `DBCompressionType::None` too. A future
///     `write_throughput_lz4` variant can flip both.
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
    inputs: &WorkloadInputs,
) -> Result<Duration, Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let start = std::time::Instant::now();
    let elapsed = match engine {
        Engine::Ours => {
            let tree = Config::new(
                dir.path(),
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            )
            .open()?;
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
            let mut opts = rocksdb::Options::default();
            opts.create_if_missing(true);
            // Match our engine's durability shape: lsm-tree has no
            // WAL — durability is the caller's responsibility, and
            // `flush_active_memtable` is the equivalent of an
            // explicit fsync barrier. Configure RocksDB to NOT
            // double-write the WAL on each `put` so the head-to-head
            // measures the same kind of work (memtable insert +
            // terminal flush) rather than penalising RocksDB for
            // its built-in WAL.
            opts.set_compression_type(rocksdb::DBCompressionType::None);
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
    let mut group = c.benchmark_group("write_throughput");
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
                        total += run_write_throughput(engine, &inputs).unwrap_or_else(|e| {
                            panic!("run_write_throughput failed for {}: {e}", engine.label())
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
/// Apples-to-apples configuration matches [`run_write_throughput`]:
/// compression `None` on both sides; RocksDB writes with the WAL
/// disabled during the (untimed) populate phase. Reads themselves
/// take no special options on either engine.
///
/// Setup failures (open / insert / flush) and read failures panic
/// with the engine label: a benchmark that can't populate or read
/// the database has no meaningful Duration to report. The "every key
/// is present" invariant is checked ONCE before the timed window (so
/// a broken setup fails loudly) and the timed loop itself stays a
/// bare `get` + `black_box` with no per-read branch.
fn bench_point_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("point_read");
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
                        let tree = Config::new(
                            dir.path(),
                            SequenceNumberCounter::default(),
                            SequenceNumberCounter::default(),
                        )
                        .open()
                        .expect("ours: open");
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
                        let mut opts = rocksdb::Options::default();
                        opts.create_if_missing(true);
                        opts.set_compression_type(rocksdb::DBCompressionType::None);
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

// P50 / P99 / P999 percentile capture is deferred to a follow-up
// commit. Criterion's default reporter gives mean + CI only,
// which hides tail-latency regressions; structured-zstd's
// `benches/bloom.rs` ports Vitter's Algorithm R reservoir +
// per-iteration `iter_custom` to expose percentiles to stderr,
// and that same pattern wires here once the workload surface is
// fleshed out (point reads, range scans, YCSB-A/C). Foundation
// commit prioritises wiring up the cross-engine path; the
// percentile harness lands alongside the dashboard JSON merger.

criterion_group!(benches, bench_write_throughput, bench_point_read);
criterion_main!(benches);

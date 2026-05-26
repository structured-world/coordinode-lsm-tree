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
//! cargo bench --features compare-rocksdb --bench compare_rocksdb
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
//! cargo bench --features compare-rocksdb --bench compare_rocksdb
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
//! ## Workload coverage (this initial commit)
//!
//! - `write_throughput/{1k,10k}` — bulk insert N keys, 256-byte
//!   values, random keys.
//!
//! Follow-up commits expand to point reads, range scans, mixed
//! YCSB-A/C, bloom-filter probes per [#244]'s workload list.

#![cfg(feature = "compare-rocksdb")]

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};

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
/// into a freshly-opened engine, returning total wall time
/// including the final flush. Per-iteration setup (tempdir +
/// engine open) lives inside the criterion `iter_custom` closure
/// so the timing loop sees a cold engine on every iteration —
/// measuring steady-state write throughput, not first-key path
/// overhead. Keys / values are precomputed in `inputs` so the
/// timed body does NO per-key allocation.
fn run_write_throughput(engine: Engine, inputs: &WorkloadInputs) -> Duration {
    let dir = tempfile::tempdir().expect("tempdir");
    let start = std::time::Instant::now();
    match engine {
        Engine::Ours => {
            let tree = Config::new(
                dir.path(),
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            )
            .open()
            .expect("open ours");
            for (i, (key, value)) in inputs.keys.iter().zip(inputs.values.iter()).enumerate() {
                tree.insert(key, value, i as u64);
            }
            tree.flush_active_memtable(0).expect("flush ours");
        }
        Engine::RocksDb => {
            let mut opts = rocksdb::Options::default();
            opts.create_if_missing(true);
            let db = rocksdb::DB::open(&opts, dir.path()).expect("open rocksdb");
            for (key, value) in inputs.keys.iter().zip(inputs.values.iter()) {
                db.put(key, value).expect("put rocksdb");
            }
            db.flush().expect("flush rocksdb");
        }
    }
    let elapsed = start.elapsed();
    drop(dir);
    elapsed
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
                        total += run_write_throughput(engine, &inputs);
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
// fleshed out (point reads, range scans, YCSB-A/C). Foundation
// commit prioritises wiring up the cross-engine path; the
// percentile harness lands alongside the dashboard JSON merger.

criterion_group!(benches, bench_write_throughput);
criterion_main!(benches);

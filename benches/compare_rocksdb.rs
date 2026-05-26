//! Head-to-head comparison harness: `coordinode-lsm-tree` vs RocksDB.
//!
//! Each criterion group runs the same workload through both engines
//! and produces side-by-side timings for the gh-pages dashboard
//! (see `docs/BENCHMARKS.md`). The harness intentionally mirrors
//! `structured-zstd`'s `compare_ffi.rs` shape so the merge / chart
//! scripts in `.github/scripts/` are byte-for-byte reusable.
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
//! YCSB-A/C, bloom-filter probes per `docs/BENCHMARKS.md` §Workloads.

#![cfg(feature = "compare-rocksdb")]

use std::path::PathBuf;
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

/// Workload: bulk-insert `n_keys` (key, value) pairs into a
/// freshly-opened engine, returning total wall time including the
/// final flush. Per-iteration setup (tempdir + engine open) lives
/// inside the criterion `iter_custom` closure so the timing loop
/// sees a cold engine on every iteration — measuring steady-state
/// write throughput, not first-key path overhead.
fn run_write_throughput(engine: Engine, n_keys: u64) -> Duration {
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
            for i in 0..n_keys {
                tree.insert(key_for(i), value_for(i), i);
            }
            tree.flush_active_memtable(0).expect("flush ours");
        }
        Engine::RocksDb => {
            let mut opts = rocksdb::Options::default();
            opts.create_if_missing(true);
            let db = rocksdb::DB::open(&opts, dir.path()).expect("open rocksdb");
            for i in 0..n_keys {
                db.put(key_for(i), value_for(i)).expect("put rocksdb");
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
        group.throughput(Throughput::Elements(n));
        for engine in [Engine::Ours, Engine::RocksDb] {
            group.bench_with_input(BenchmarkId::new(engine.label(), n), &n, |b, &n_keys| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        total += run_write_throughput(engine, n_keys);
                    }
                    total
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_write_throughput);
criterion_main!(benches);

// Suppress "unused" on the path module — kept for follow-up
// workloads that need to inspect on-disk artefacts (e.g. range
// scan after explicit compaction file count check).
#[allow(dead_code)]
fn _unused_path_anchor() -> PathBuf {
    PathBuf::new()
}

//! Microbenchmark for `longest_shared_prefix_length` (issue #219).
//!
//! Compares the dispatched implementation (SIMD on x86_64+AVX2 / aarch64+NEON,
//! u64 scalar elsewhere) against the original byte-by-byte loop on key sizes
//! representative of LSM block encoding (graph node ids, document paths,
//! prefixed timeseries keys).

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lsm_tree::longest_shared_prefix_length;
use rand::{RngExt, SeedableRng, rngs::StdRng};

/// Pre-SIMD reference implementation kept for relative speedup measurement.
fn lsp_reference(s1: &[u8], s2: &[u8]) -> usize {
    s1.iter().zip(s2.iter()).take_while(|(a, b)| a == b).count()
}

/// Build a key pair that shares `shared` bytes then differs.
/// `total` is the full key length.
fn pair(shared: usize, total: usize, seed: u64) -> (Vec<u8>, Vec<u8>) {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut a = vec![0_u8; total];
    rng.fill(&mut a[..]);
    let mut b = a.clone();
    if shared < total {
        // Flip every byte from `shared` onward so the first mismatching byte is at `shared`.
        for byte in &mut b[shared..] {
            *byte ^= 0xFF;
        }
    }
    (a, b)
}

fn bench_lsp(c: &mut Criterion) {
    // Key sizes representative of LSM workloads:
    //   16B — short ids
    //   32B — typical graph node keys (`node:<shard>:<id>`)
    //   64B — document paths
    //   128B — timeseries with prefix metadata
    //   256B — long composite keys
    //   1024B — outlier large keys
    for &total in &[16_usize, 32, 64, 128, 256, 1024] {
        // Match for the full common prefix — exercises the full SIMD loop.
        let (a, b) = pair(total, total, 0xDEADBEEF);
        let mut group = c.benchmark_group(format!("lsp/full_match/{total}B"));
        group.throughput(Throughput::Bytes(total as u64));
        group.bench_function(BenchmarkId::new("dispatched", total), |bench| {
            bench.iter(|| {
                longest_shared_prefix_length(std::hint::black_box(&a), std::hint::black_box(&b))
            });
        });
        group.bench_function(BenchmarkId::new("byte_loop", total), |bench| {
            bench.iter(|| lsp_reference(std::hint::black_box(&a), std::hint::black_box(&b)));
        });
        group.finish();

        // Early mismatch at byte `total / 4` — represents typical block-encoding case
        // where most keys share a moderate prefix with the restart base key.
        let (a, b) = pair(total / 4, total, 0xC0FFEE);
        let mut group = c.benchmark_group(format!("lsp/quarter_match/{total}B"));
        group.throughput(Throughput::Bytes(total as u64));
        group.bench_function(BenchmarkId::new("dispatched", total), |bench| {
            bench.iter(|| {
                longest_shared_prefix_length(std::hint::black_box(&a), std::hint::black_box(&b))
            });
        });
        group.bench_function(BenchmarkId::new("byte_loop", total), |bench| {
            bench.iter(|| lsp_reference(std::hint::black_box(&a), std::hint::black_box(&b)));
        });
        group.finish();
    }
}

criterion_group!(benches, bench_lsp);
criterion_main!(benches);

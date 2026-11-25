//! The crate's xxh3-64 and xxh3-128 against `xxhash-rust`'s, on the lengths the engine
//! hashes: a key, a short entry, the end of xxh3's short paths, a packed
//! record, a table block, and large values on either side of the wide
//! kernel's threshold. Both run in one binary on one CPU, so their times
//! compare directly.
//!
//! The criterion groups give each length's throughput; `latency` times calls
//! one at a time and prints their P50, P99 and P999, so a slow call shows in
//! the tail instead of averaging into a sample.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use std::time::Instant;

const LENGTHS: [usize; 9] = [16, 100, 240, 300, 1_024, 4_096, 32_768, 65_536, 1 << 20];

/// A megabyte of bytes with no short period.
fn corpus() -> Vec<u8> {
    (0..(1usize << 20))
        .map(|at| (at.wrapping_mul(0x9E37_79B9) >> 7) as u8)
        .collect()
}

fn xxh3_64(c: &mut Criterion) {
    let data = corpus();
    let mut group = c.benchmark_group("xxh3_64");
    for len in LENGTHS {
        let input = &data[..len];
        group.throughput(Throughput::Bytes(len as u64));
        group.bench_with_input(BenchmarkId::new("ours", len), input, |b, input| {
            b.iter(|| lsm_tree::hash::hash64(black_box(input)));
        });
        group.bench_with_input(BenchmarkId::new("xxhash_rust", len), input, |b, input| {
            b.iter(|| xxhash_rust::xxh3::xxh3_64(black_box(input)));
        });
    }
    group.finish();
}

/// The same lengths through xxh3-128, the digest of a table block.
fn xxh3_128(c: &mut Criterion) {
    let data = corpus();
    let mut group = c.benchmark_group("xxh3_128");
    for len in LENGTHS {
        let input = &data[..len];
        group.throughput(Throughput::Bytes(len as u64));
        group.bench_with_input(BenchmarkId::new("ours", len), input, |b, input| {
            b.iter(|| lsm_tree::hash::hash128(black_box(input)));
        });
        group.bench_with_input(BenchmarkId::new("xxhash_rust", len), input, |b, input| {
            b.iter(|| xxhash_rust::xxh3::xxh3_128(black_box(input)));
        });
    }
    group.finish();
}

/// The nanoseconds of `calls` calls of `hash`, each timed alone, after a
/// warm-up of a thousand.
fn timed(calls: usize, hash: &mut dyn FnMut()) -> Vec<u64> {
    for _ in 0..1_000 {
        hash();
    }
    (0..calls)
        .map(|_| {
            let start = Instant::now();
            hash();
            u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
        })
        .collect()
}

/// P50, P99 and P999 of `samples`.
fn percentiles(mut samples: Vec<u64>) -> [u64; 3] {
    samples.sort_unstable();
    let last = samples.len() - 1;
    [0.5, 0.99, 0.999].map(|q| samples[(last as f64 * q) as usize])
}

/// Per-call latency of every length through each digest, ours and
/// `xxhash-rust`'s, printed as a table. The timer's own cost is printed first:
/// below about a hundred nanoseconds a call's figure is mostly the timer, and
/// its tail mostly interrupts. It runs on every invocation, criterion's filter
/// naming its groups only.
fn latency(_: &mut Criterion) {
    let data = corpus();
    let [p50, p99, p999] = percentiles(timed(200_000, &mut || {}));
    eprintln!("latency/timer: p50 {p50} ns, p99 {p99} ns, p999 {p999} ns");
    for len in LENGTHS {
        let input = &data[..len];
        // Enough calls for a P999 of ten samples, about 20 ms of hashing at
        // the long lengths.
        let calls = (20_000_000 / len.max(64)).clamp(10_000, 200_000);
        let arms: [(&str, &mut dyn FnMut()); 4] = [
            ("xxh3_64/ours", &mut || {
                black_box(lsm_tree::hash::hash64(black_box(input)));
            }),
            ("xxh3_64/xxhash_rust", &mut || {
                black_box(xxhash_rust::xxh3::xxh3_64(black_box(input)));
            }),
            ("xxh3_128/ours", &mut || {
                black_box(lsm_tree::hash::hash128(black_box(input)));
            }),
            ("xxh3_128/xxhash_rust", &mut || {
                black_box(xxhash_rust::xxh3::xxh3_128(black_box(input)));
            }),
        ];
        for (name, hash) in arms {
            let [p50, p99, p999] = percentiles(timed(calls, hash));
            eprintln!("latency/{name}/{len}: p50 {p50} ns, p99 {p99} ns, p999 {p999} ns");
        }
    }
}

criterion_group!(benches, xxh3_64, xxh3_128, latency);
criterion_main!(benches);

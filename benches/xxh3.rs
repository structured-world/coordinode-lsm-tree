//! The crate's xxh3-64 and xxh3-128 against `xxhash-rust`'s, on the lengths the engine
//! hashes: a key, a short entry, the end of xxh3's short paths, a packed
//! record, a table block, and large values on either side of the wide
//! kernel's threshold. Both run in one binary on one CPU, so their times
//! compare directly.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;

const LENGTHS: [usize; 9] = [16, 100, 240, 300, 1_024, 4_096, 32_768, 65_536, 1 << 20];

fn xxh3_64(c: &mut Criterion) {
    let data: Vec<u8> = (0..(1usize << 20))
        .map(|at| (at.wrapping_mul(0x9E37_79B9) >> 7) as u8)
        .collect();
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
    let data: Vec<u8> = (0..(1usize << 20))
        .map(|at| (at.wrapping_mul(0x9E37_79B9) >> 7) as u8)
        .collect();
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

criterion_group!(benches, xxh3_64, xxh3_128);
criterion_main!(benches);

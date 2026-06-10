// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Block-cache hot-path throughput (#429).
//!
//! Drives warm point reads through a [`Tree`](lsm_tree::Tree) whose block cache
//! is large enough to hold the whole dataset, so every read is a block-cache
//! hit. This isolates the cache `get` path — the only thing the in-tree
//! S3-FIFO cache changed versus the previous `quick_cache` backing — end to end
//! through the public API (identical on both implementations, so the
//! cross-branch delta is purely the cache).
//!
//! Two access patterns: a sequential sweep (predictable, best case for any
//! cache) and a pseudo-random sweep (the realistic read pattern, where shard
//! routing + per-shard lock cost shows).
//!
//! Each pattern is measured two ways: a `b.iter()` throughput pass (Criterion's
//! mean/median estimate, lowest noise for the sub-microsecond `get` path) and an
//! `iter_custom` latency pass that timestamps every individual call and prints
//! P50/P99/P999. Tail latency is where shard-level lock contention and eviction
//! pauses surface — invisible in a throughput mean, which is exactly the point
//! of reporting percentiles alongside it.

use criterion::{Criterion, criterion_group, criterion_main};
use lsm_tree::{AbstractTree, Cache, Config, SeqNo, SequenceNumberCounter};
use std::sync::Arc;
use std::time::{Duration, Instant};

const N: u64 = 50_000;

fn key(i: u64) -> Vec<u8> {
    format!("key-{i:08}").into_bytes()
}

/// Opens a tree with a 256 MiB block cache (far larger than the dataset),
/// inserts `N` small KV pairs, flushes, and reads every key once to warm the
/// block cache. Returns the temp dir (kept alive) and the open tree.
fn warm_tree() -> (tempfile::TempDir, lsm_tree::AnyTree, Vec<Vec<u8>>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = Arc::new(Cache::with_capacity_bytes(256 * 1_024 * 1_024));
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .use_cache(cache)
    .open()
    .expect("open tree");

    for i in 0..N {
        tree.insert(key(i), b"value".as_slice(), i + 1);
    }
    tree.flush_active_memtable(0).expect("flush");

    // Pre-generate keys so the benchmarked closure doesn't pay per-iteration
    // formatting/allocation (constant across both implementations anyway, but
    // keeping it out sharpens the cache signal).
    let keys: Vec<Vec<u8>> = (0..N).map(key).collect();

    // Warm the block cache: every key read once pulls its block resident.
    for k in &keys {
        let _ = tree.get(k, SeqNo::MAX).expect("warm read");
    }

    (dir, tree, keys)
}

fn warm_point_read_sequential(c: &mut Criterion) {
    let (_dir, tree, keys) = warm_tree();
    let mut i = 0usize;
    c.bench_function("block_cache/warm_point_read_sequential", |b| {
        b.iter(|| {
            i = (i + 1) % keys.len();
            std::hint::black_box(tree.get(&keys[i], SeqNo::MAX).expect("get"));
        });
    });
}

fn warm_point_read_random(c: &mut Criterion) {
    let (_dir, tree, keys) = warm_tree();
    // Cheap deterministic LCG so the access pattern is reproducible run to run
    // (no time/rand dependency) yet scattered across shards.
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    c.bench_function("block_cache/warm_point_read_random", |b| {
        b.iter(|| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let idx = (state >> 33) as usize % keys.len();
            std::hint::black_box(tree.get(&keys[idx], SeqNo::MAX).expect("get"));
        });
    });
}

/// Sorts the collected per-call durations and prints P50/P99/P999 to stderr.
///
/// Criterion's own report covers throughput (mean/median); this surfaces the
/// tail the mean hides. Nearest-rank percentile (no interpolation) — for the
/// millions of samples a sub-microsecond op collects, the rank error is far
/// below timer resolution.
fn report_percentiles(name: &str, samples: &mut [Duration]) {
    if samples.is_empty() {
        return;
    }
    samples.sort_unstable();
    // Nearest-rank in pure integer math: round((n-1) * num / den), no float cast
    // (lossy f64→usize). `n * num` can't overflow usize on any realistic sample
    // count (n is a few million, num ≤ 999), so plain `*` is provably safe here.
    let at = |num: usize, den: usize| {
        let n = samples.len() - 1;
        let idx = (n * num + den / 2) / den;
        samples[idx]
    };
    eprintln!(
        "{name}: P50={:?} P99={:?} P999={:?} max={:?} (n={})",
        at(50, 100),
        at(99, 100),
        at(999, 1000),
        samples[samples.len() - 1],
        samples.len(),
    );
}

fn warm_point_read_sequential_latency(c: &mut Criterion) {
    let (_dir, tree, keys) = warm_tree();
    let mut i = 0usize;
    // Accumulate across every `iter_custom` batch (Criterion calls the routine
    // repeatedly through warmup + sampling); print percentiles once, after the
    // whole bench returns, over the full sample set.
    let mut samples: Vec<Duration> = Vec::new();
    c.bench_function("block_cache/warm_point_read_sequential_latency", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                i = (i + 1) % keys.len();
                let start = Instant::now();
                std::hint::black_box(tree.get(&keys[i], SeqNo::MAX).expect("get"));
                let elapsed = start.elapsed();
                samples.push(elapsed);
                total += elapsed;
            }
            total
        });
    });
    report_percentiles("warm_point_read_sequential", &mut samples);
}

fn warm_point_read_random_latency(c: &mut Criterion) {
    let (_dir, tree, keys) = warm_tree();
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut samples: Vec<Duration> = Vec::new();
    c.bench_function("block_cache/warm_point_read_random_latency", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let idx = (state >> 33) as usize % keys.len();
                let start = Instant::now();
                std::hint::black_box(tree.get(&keys[idx], SeqNo::MAX).expect("get"));
                let elapsed = start.elapsed();
                samples.push(elapsed);
                total += elapsed;
            }
            total
        });
    });
    report_percentiles("warm_point_read_random", &mut samples);
}

criterion_group!(
    benches,
    warm_point_read_sequential,
    warm_point_read_random,
    warm_point_read_sequential_latency,
    warm_point_read_random_latency,
);
criterion_main!(benches);

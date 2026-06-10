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

use criterion::{Criterion, criterion_group, criterion_main};
use lsm_tree::{AbstractTree, Cache, Config, SeqNo, SequenceNumberCounter};
use std::sync::Arc;

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

criterion_group!(benches, warm_point_read_sequential, warm_point_read_random);
criterion_main!(benches);

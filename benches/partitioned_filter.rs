// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Point reads through a partitioned filter.
//!
//! A point read on a table with a partitioned filter loads the one partition
//! covering its key before it can rule the table out, so the partition's size
//! is what each read pays when the block cache does not hold it. The tree
//! partitions and unpins its filters on every level, and its block cache is
//! far smaller than the filter, so reads keep loading partitions. Keys absent
//! from the table measure the path the filter exists for, present keys the
//! partition load ahead of the data block.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use lsm_tree::{AbstractTree, Cache, Config, SeqNo, SequenceNumberCounter, config::PinningPolicy};
use std::sync::Arc;

const N: u64 = 200_000;

fn key(i: u64) -> Vec<u8> {
    format!("key-{i:08}").into_bytes()
}

/// Bytes the files under `path` take.
fn dir_bytes(path: &std::path::Path) -> u64 {
    std::fs::read_dir(path)
        .expect("read dir")
        .map(|entry| {
            let entry = entry.expect("dir entry");
            let meta = entry.metadata().expect("metadata");
            if meta.is_dir() {
                dir_bytes(&entry.path())
            } else {
                meta.len()
            }
        })
        .sum()
}

/// A tree of `N` keys in one table whose filter is partitioned and unpinned,
/// read through a block cache of `cache_bytes`.
fn tree(cache_bytes: u64) -> (tempfile::TempDir, lsm_tree::AnyTree) {
    let dir = tempfile::tempdir().expect("tempdir");
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .use_cache(Arc::new(Cache::with_capacity_bytes(cache_bytes)))
    .filter_block_partitioning_policy(PinningPolicy::all(true))
    .filter_block_pinning_policy(PinningPolicy::all(false))
    .open()
    .expect("open tree");
    for i in 0..N {
        tree.insert(key(i), b"value".as_slice(), i + 1);
    }
    tree.flush_active_memtable(0).expect("flush");
    (dir, tree)
}

fn point_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("partitioned_filter");
    for cache_kib in [64_u64, 1_024] {
        let (dir, tree) = tree(cache_kib * 1_024);
        // The bytes axis: more, smaller partitions grow the top-level index.
        eprintln!("partitioned_filter: tree bytes {}", dir_bytes(dir.path()));
        // A stride coprime with N visits the keys in an order no partition
        // or block repeats from one read to the next.
        let mut i = 0_u64;
        group.bench_function(BenchmarkId::new("absent", cache_kib), |b| {
            b.iter(|| {
                i = (i + 7_919) % N;
                let mut k = key(i);
                k.push(b'x');
                std::hint::black_box(tree.get(&k, SeqNo::MAX).expect("get"));
            });
        });
        group.bench_function(BenchmarkId::new("present", cache_kib), |b| {
            b.iter(|| {
                i = (i + 7_919) % N;
                std::hint::black_box(tree.get(key(i), SeqNo::MAX).expect("get"));
            });
        });
    }
    group.finish();
}

criterion_group!(benches, point_reads);
criterion_main!(benches);

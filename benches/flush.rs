// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Flush throughput with a real per-block transform (zstd).
//!
//! Times `flush_active_memtable` alone: the memtable is populated outside the
//! timed window, so the number is the write-side cost of turning a full
//! memtable into an L0 SST — block encode + compression + write + sync. This
//! is the path the flush-side parallel block compression targets, which the
//! `at_insert` bench (no compression, transform is identity) cannot see.
//! Requires the `zstd` feature.

#![expect(
    clippy::expect_used,
    reason = "benchmark setup favors concise panic messages"
)]

use criterion::{Criterion, criterion_group, criterion_main};
use lsm_tree::config::CompressionPolicy;
use lsm_tree::{AbstractTree, AnyTree, CompressionType, Config, SequenceNumberCounter};
use std::time::{Duration, Instant};

const KEYS: u64 = 40_000;
/// Same spectrum reasoning as the compaction bench: level 1 shows per-block
/// pipeline overhead, level 22 shows the codec-CPU-dominated case.
const ZSTD_LEVELS: [i32; 2] = [1, 22];

/// Opens a tree with zstd at `level` on every LSM level and fills the active
/// memtable with `KEYS` compressible entries, WITHOUT flushing.
fn build_unflushed_tree(level: i32) -> (AnyTree, tempfile::TempDir) {
    let folder = tempfile::TempDir::new().expect("tempdir");
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(
        CompressionType::zstd(level).expect("valid zstd level"),
    ))
    .open()
    .expect("open");

    for i in 0..KEYS {
        let key = format!("key_{i:08}");
        // Compressible payload so the codec has real, parallelizable work.
        let value = format!("row-{i}-{}", "the quick brown fox ".repeat(8));
        tree.insert(key, value, i);
    }
    (tree, folder)
}

fn bench_flush(c: &mut Criterion) {
    for level in ZSTD_LEVELS {
        let mut group = c.benchmark_group(format!("flush_zstd{level}"));
        group.sample_size(10);

        group.bench_function("flush", |b| {
            // iter_custom: the populate is per-iteration setup, only the flush
            // itself is timed.
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let (tree, _folder) = build_unflushed_tree(level);
                    let start = Instant::now();
                    tree.flush_active_memtable(0).expect("flush");
                    total += start.elapsed();
                    std::hint::black_box(&tree);
                }
                total
            });
        });

        group.finish();
    }
}

criterion_group!(benches, bench_flush);
criterion_main!(benches);

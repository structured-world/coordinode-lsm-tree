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

/// Reports per-flush tail latency (P50/P95/P99) to stderr — Criterion's
/// summary only surfaces mean/CI, and each iteration here is one whole flush.
fn report_percentiles(label: &str, mut samples: Vec<Duration>) {
    if samples.is_empty() {
        return;
    }
    samples.sort_unstable();
    let pick = |p: f64| {
        let idx = (((samples.len() - 1) as f64) * p).round() as usize;
        samples[idx.min(samples.len() - 1)]
    };
    eprintln!(
        "  [{label}] n={} P50={:?} P95={:?} P99={:?}",
        samples.len(),
        pick(0.50),
        pick(0.95),
        pick(0.99),
    );
}

/// Two working-set sizes: 1k isolates the flush's FIXED cost (file create,
/// table recover-after-write, fsyncs, manifest edit append) that dominates the
/// overwrite/1k head-to-head; 40k shows the per-block encode + codec cost the
/// parallel pipeline targets.
const KEY_COUNTS: [u64; 2] = [1_000, 40_000];
/// Same spectrum reasoning as the compaction bench: level 1 shows per-block
/// pipeline overhead, level 22 shows the codec-CPU-dominated case.
const ZSTD_LEVELS: [i32; 2] = [1, 22];

/// Opens a tree with zstd at `level` on every LSM level and fills the active
/// memtable with `keys` compressible entries, WITHOUT flushing. `mem` swaps in
/// [`MemFs`](lsm_tree::fs::MemFs), turning the flush into its pure CPU cost
/// (encode + codec + bookkeeping) with every syscall and fsync removed — the
/// spread between the two arms is the flush's I/O share.
fn build_unflushed_tree(keys: u64, level: i32, mem: bool) -> (AnyTree, Option<tempfile::TempDir>) {
    let (folder, config) = if mem {
        let config = Config::new(
            "/bench",
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_fs(lsm_tree::fs::MemFs::new());
        (None, config)
    } else {
        let folder = tempfile::TempDir::new().expect("tempdir");
        let config = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        );
        (Some(folder), config)
    };
    let tree = config
        .data_block_compression_policy(CompressionPolicy::all(
            CompressionType::zstd(level).expect("valid zstd level"),
        ))
        .open()
        .expect("open");

    for i in 0..keys {
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

        for keys in KEY_COUNTS {
            for mem in [false, true] {
                let label = if mem {
                    format!("{keys}_memfs")
                } else {
                    format!("{keys}")
                };
                group.bench_function(label.clone(), |b| {
                    // iter_custom: the populate is per-iteration setup, only
                    // the flush itself is timed; per-flush durations feed the
                    // percentile report below.
                    let mut samples = Vec::new();
                    b.iter_custom(|iters| {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let (tree, _folder) = build_unflushed_tree(keys, level, mem);
                            let start = Instant::now();
                            tree.flush_active_memtable(0).expect("flush");
                            let elapsed = start.elapsed();
                            samples.push(elapsed);
                            total += elapsed;
                            std::hint::black_box(&tree);
                        }
                        total
                    });
                    report_percentiles(&format!("zstd{level}/{label}"), samples);
                });
            }
        }

        group.finish();
    }
}

criterion_group!(benches, bench_flush);
criterion_main!(benches);

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

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

#[path = "util/percentiles.rs"]
mod percentiles;
use percentiles::report_percentiles;

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

/// A run of flushes into one tree on disk, each timed alone, with the
/// default table file pool and with none. With the pool most flushes take a
/// file made ahead and sync no folder, while one in a batch makes the batch:
/// the tail percentiles show what those refills cost, against the flush that
/// syncs its folder every time.
fn bench_flush_series(c: &mut Criterion) {
    /// Keys per flush: small enough that the fixed cost (file, syncs, edit)
    /// is what is measured.
    const KEYS: u64 = 1_000;
    /// Flushes between compactions, outside the timing, so the version the
    /// install rewrites stays the same size through the run.
    const COMPACT_EVERY: u64 = 32;

    let mut group = c.benchmark_group("flush_series");
    group.sample_size(20);
    // Each sync mode: under `Full` every sync flushes the device cache
    // (`F_FULLFSYNC` on macOS), the case where a folder sync costs most.
    let modes = [
        ("normal", lsm_tree::fs::SyncMode::Normal),
        ("full", lsm_tree::fs::SyncMode::Full),
    ];
    for ((mode, sync_mode), pool) in modes
        .into_iter()
        .flat_map(|mode| [16, 0].map(|pool| (mode, pool)))
    {
        let label = format!("{mode}/pool{pool}");
        // One tree and one sample set for every call criterion makes, so the
        // run is one series whose refills fall once per batch.
        let folder = tempfile::TempDir::new().expect("tempdir");
        let tree = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .table_file_pool(pool)
        .sync_mode(sync_mode)
        .open()
        .expect("open");
        let mut seqno = 0u64;
        let mut flushes = 0u64;
        let mut samples = Vec::new();
        group.bench_function(label.clone(), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    for i in 0..KEYS {
                        tree.insert(format!("key_{i:08}"), format!("value-{seqno}"), seqno);
                        seqno += 1;
                    }
                    let start = Instant::now();
                    tree.flush_active_memtable(0).expect("flush");
                    let elapsed = start.elapsed();
                    samples.push(elapsed);
                    total += elapsed;
                    flushes += 1;
                    if flushes.is_multiple_of(COMPACT_EVERY) {
                        tree.major_compact(64 * 1024 * 1024, 0).expect("compact");
                    }
                }
                total
            });
        });
        report_percentiles(&format!("flush_series/{label}"), samples);
    }
    group.finish();
}

criterion_group!(benches, bench_flush, bench_flush_series);
criterion_main!(benches);

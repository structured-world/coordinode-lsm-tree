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

/// One arm of the flush series: a tree on disk and the time of each flush.
struct SeriesArm {
    label: &'static str,
    _folder: tempfile::TempDir,
    tree: AnyTree,
    seqno: u64,
    flushes: u64,
    samples: Vec<Duration>,
}

impl SeriesArm {
    /// Keys per flush: small enough that the fixed cost (file, syncs, edit)
    /// is what is measured.
    const KEYS: u64 = 1_000;
    /// Flushes between compactions, outside the timing, so the version the
    /// install rewrites stays the same size through the run.
    const COMPACT_EVERY: u64 = 32;

    fn new(label: &'static str, pool: usize, sync_mode: lsm_tree::fs::SyncMode) -> Self {
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
        Self {
            label,
            _folder: folder,
            tree,
            seqno: 0,
            flushes: 0,
            samples: Vec::new(),
        }
    }

    /// Fills a memtable, then times its flush alone.
    fn flush(&mut self) -> Duration {
        for i in 0..Self::KEYS {
            self.tree.insert(
                format!("key_{i:08}"),
                format!("value-{}", self.seqno),
                self.seqno,
            );
            self.seqno += 1;
        }
        let start = Instant::now();
        self.tree.flush_active_memtable(0).expect("flush");
        let elapsed = start.elapsed();
        self.samples.push(elapsed);
        self.flushes += 1;
        if self.flushes.is_multiple_of(Self::COMPACT_EVERY) {
            self.tree
                .major_compact(64 * 1024 * 1024, 0)
                .expect("compact");
        }
        elapsed
    }
}

/// A run of flushes into one tree on disk, each timed alone, with the
/// default table file pool and with none. With the pool most flushes take a
/// file made ahead and sync no folder, while one in a batch makes the batch:
/// the tail percentiles show what those refills cost, against the flush that
/// syncs its folder every time.
///
/// The two arms run interleaved in one loop, their order alternating every
/// round (A B, B A), so whatever else loads the host lands on both alike
/// rather than on whichever arm happened to run at the time.
fn bench_flush_series(c: &mut Criterion) {
    let mut group = c.benchmark_group("flush_series");
    group.sample_size(20);
    // Each sync mode: under `Full` every sync flushes the device cache
    // (`F_FULLFSYNC` on macOS), the case where a folder sync costs most.
    let modes = [
        ("normal", lsm_tree::fs::SyncMode::Normal),
        ("full", lsm_tree::fs::SyncMode::Full),
    ];
    for (mode, sync_mode) in modes {
        let mut arms = [
            SeriesArm::new("pool16", 16, sync_mode),
            SeriesArm::new("pool0", 0, sync_mode),
        ];
        let mut round = 0u64;
        group.bench_function(format!("{mode}/interleaved"), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    if round.is_multiple_of(2) {
                        total += arms[0].flush();
                        total += arms[1].flush();
                    } else {
                        total += arms[1].flush();
                        total += arms[0].flush();
                    }
                    round += 1;
                }
                total
            });
        });
        for arm in arms {
            let mean = arm.samples.iter().sum::<Duration>()
                / u32::try_from(arm.samples.len().max(1)).expect("sample count fits u32");
            eprintln!("  [flush_series/{mode}/{}] mean={mean:?}", arm.label);
            report_percentiles(&format!("flush_series/{mode}/{}", arm.label), arm.samples);
        }
    }
    group.finish();
}

criterion_group!(benches, bench_flush, bench_flush_series);
criterion_main!(benches);

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Head-to-head compaction throughput at a 4-thread budget, comparing the
//! SAME mechanism on both engines: parallel block compression within one
//! compaction.
//!
//! - Ours: `compaction_threads = 4` (the parallel block-compression pipeline).
//! - RocksDB: `compression_options_parallel_threads = 4`, `max_subcompactions
//!   = 1` (so the only parallelism is block compression, not range-split —
//!   apples-to-apples with our parallel block-compression path).
//!
//! Both build the same FLUSHES zstd L0 tables, then a single full compaction is
//! timed (setup excluded via `iter_custom`). Run at the two ends of the zstd
//! spectrum — level 1 (cheap compression; per-block overhead most visible) and
//! level 22 (max; codec CPU dominates).

use criterion::{Criterion, criterion_group, criterion_main};
use lsm_tree::config::CompressionPolicy;
use lsm_tree::{AbstractTree, CompressionType, Config, SequenceNumberCounter};
use std::time::{Duration, Instant};

const KEYS: u64 = 40_000;
const FLUSHES: u64 = 6;
const THREADS: usize = 4;
// The two ends of the zstd spectrum: level 1 (fastest — compression is cheap,
// so per-block overhead is most visible) and level 22 ("ultra", max — codec
// CPU dominates). Level 6 in between is uninformative, so it is skipped.
const ZSTD_LEVELS: [i32; 2] = [1, 22];

fn key_for(batch: u64, i: u64) -> Vec<u8> {
    // Interleave keys across batches (batch b owns i*FLUSHES + b) so the L0
    // tables have FULLY OVERLAPPING key ranges. Otherwise disjoint ranges let
    // either engine "trivially move" L0 files to the next level without
    // rewriting (no recompression) — which measures file moves, not the
    // parallel-compression path under test. Keys stay globally unique, so all
    // KEYS rows survive the merge.
    format!("key_{:08}", i * FLUSHES + batch).into_bytes()
}

fn value_for(i: u64) -> Vec<u8> {
    // Compressible payload so zstd has real, parallelizable work.
    format!("row-{i}-{}", "the quick brown fox ".repeat(8)).into_bytes()
}

/// Builds FLUSHES zstd L0 tables in our engine, then times one full
/// `major_compact` (parallel block compression across THREADS workers).
fn ours_compaction(level: i32) -> Duration {
    let dir = tempfile::tempdir().unwrap();
    let tree = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::Zstd(level)))
    .compaction_threads(THREADS)
    .open()
    .unwrap();

    let per = KEYS / FLUSHES;
    let mut seqno = 0u64;
    for batch in 0..FLUSHES {
        for i in 0..per {
            tree.insert(key_for(batch, i), value_for(i), seqno);
            seqno += 1;
        }
        tree.flush_active_memtable(0).unwrap();
    }

    let start = Instant::now();
    tree.major_compact(u64::MAX, 0).unwrap();
    start.elapsed()
}

/// Builds FLUSHES zstd L0 tables in RocksDB (auto-compaction off), then times
/// one full `compact_range` with parallel block compression across THREADS.
fn rocksdb_compaction(level: i32) -> Duration {
    let dir = tempfile::tempdir().unwrap();

    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    // Hold L0 tables until we trigger the single explicit compaction.
    opts.set_disable_auto_compactions(true);
    opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
    // (window_bits, level, strategy, max_dict_bytes); -14 = default zstd window.
    opts.set_compression_options(-14, level, 0, 0);
    // The mechanism under test: parallel block compression within a compaction.
    opts.set_compression_options_parallel_threads(THREADS as i32);
    // Isolate block-compression parallelism (no range-split sub-compactions),
    // matching our parallel block-compression scope.
    opts.set_max_subcompactions(1);

    let db = rocksdb::DB::open(&opts, &dir).unwrap();
    let mut wopts = rocksdb::WriteOptions::default();
    wopts.disable_wal(true);

    let per = KEYS / FLUSHES;
    for batch in 0..FLUSHES {
        for i in 0..per {
            db.put_opt(key_for(batch, i), value_for(i), &wopts).unwrap();
        }
        db.flush().unwrap(); // memtable → one L0 SST
    }

    let start = Instant::now();
    db.compact_range(None::<&[u8]>, None::<&[u8]>);
    start.elapsed()
}

fn bench_compaction(c: &mut Criterion) {
    for level in ZSTD_LEVELS {
        let mut group = c.benchmark_group(format!("major_compact_zstd{level}_4threads"));
        group.sample_size(10);

        group.bench_function("ours", |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    total += ours_compaction(level);
                }
                total
            });
        });

        group.bench_function("rocksdb", |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    total += rocksdb_compaction(level);
                }
                total
            });
        });

        group.finish();
    }
}

criterion_group!(benches, bench_compaction);
criterion_main!(benches);

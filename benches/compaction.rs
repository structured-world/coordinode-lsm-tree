// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Compaction throughput: serial vs parallel block compression.
//!
//! Measures `major_compact` of several zstd-compressed L0 tables into one,
//! varying `compaction_threads`. The CPU-bound work farmed to workers is block
//! compression, so the parallel speedup shows up only with a real codec (zstd)
//! and enough data — hence the compressible payload below. Requires the `zstd`
//! feature.

use criterion::{Criterion, criterion_group, criterion_main};
use lsm_tree::config::CompressionPolicy;
use lsm_tree::{AbstractTree, AnyTree, CompressionType, Config, SequenceNumberCounter};
use std::time::{Duration, Instant};

/// Reports compaction tail latency (P50/P95/P99) from the per-iteration
/// `major_compact` durations to stderr, since Criterion's default summary only
/// surfaces mean/CI. Each iteration is one whole compaction, so these are the
/// distribution of compaction wall-times.
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

const KEYS: u64 = 40_000;
const FLUSHES: u64 = 6;
/// The two ends of the zstd spectrum: level 1 (cheap compression — per-block
/// overhead most visible) and level 22 (max — codec CPU dominates, parallel
/// win clearest). Level 6 in between is uninformative, so it is skipped.
const ZSTD_LEVELS: [i32; 2] = [1, 22];

/// Builds a tree of `FLUSHES` zstd-compressed L0 tables ready for one big
/// `major_compact`. `threads` selects serial (1) vs parallel (>1) compression.
fn build_filled_tree(threads: usize, level: i32) -> (AnyTree, tempfile::TempDir) {
    let folder = tempfile::tempdir().unwrap();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(
        CompressionType::zstd(level).unwrap(),
    ))
    .compaction_threads(threads)
    .open()
    .unwrap();

    let per = KEYS / FLUSHES;
    let mut seqno = 0u64;
    for batch in 0..FLUSHES {
        for i in 0..per {
            let key = format!("key_{batch:02}_{i:08}");
            // Compressible payload so zstd has real, parallelizable work.
            let value = format!("row-{i}-{}", "the quick brown fox ".repeat(8));
            tree.insert(key, value, seqno);
            seqno += 1;
        }
        tree.flush_active_memtable(0).unwrap();
    }
    (tree, folder)
}

fn bench_major_compact(c: &mut Criterion) {
    for level in ZSTD_LEVELS {
        let mut group = c.benchmark_group(format!("major_compact_zstd{level}"));
        group.sample_size(10);

        for threads in [1usize, 4] {
            group.bench_function(format!("threads_{threads}"), |b| {
                // iter_custom so each compaction is timed individually: the
                // setup (building L0) is excluded, and per-iteration durations
                // feed the percentile report below (tail latency, not just mean).
                let mut samples = Vec::new();
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let (tree, _folder) = build_filled_tree(threads, level);
                        let start = Instant::now();
                        tree.major_compact(u64::MAX, 0).unwrap();
                        let elapsed = start.elapsed();
                        samples.push(elapsed);
                        total += elapsed;
                        std::hint::black_box(&tree);
                    }
                    total
                });
                report_percentiles(&format!("zstd{level}/threads_{threads}"), samples);
            });
        }

        group.finish();
    }
}

criterion_group!(benches, bench_major_compact);
criterion_main!(benches);

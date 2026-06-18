// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Tight-space compaction wall-time.
//!
//! Rewrites a single gated table on a near-full simulated disk (`MemFs` capped
//! just above the live footprint) two ways:
//!
//! - `tight`: the opt-in sliced-and-punched reclaim, which completes where a
//!   normal merge would be skipped. It re-opens the input once per slice, so it
//!   trades extra I/O for fitting in the headroom.
//! - `ample`: the same rewrite with plenty of free space (one normal merge),
//!   the baseline the tight path is measured against.
//!
//! Each iteration rebuilds the tree (setup excluded) and times one
//! `major_compact`; per-iteration durations feed the percentile report.

use criterion::{Criterion, criterion_group, criterion_main};
use lsm_tree::fs::MemFs;
use lsm_tree::{AbstractTree, AnyTree, Config, SequenceNumberCounter};
use std::sync::Arc;
use std::time::{Duration, Instant};

const KEYS: u64 = 20_000;
const VALUE: [u8; 100] = [0xCD; 100];

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

/// Builds one flushed table on a `MemFs`, then (when `tight`) caps the disk just
/// above the footprint and opts in to tight-space reclaim. Returns everything
/// the timed `major_compact` needs kept alive.
fn build_gated_tree(tight: bool) -> (lsm_tree::Tree, MemFs, tempfile::TempDir) {
    let folder = tempfile::tempdir().unwrap();
    let mem = MemFs::with_capacity(u64::MAX);
    let tree = match Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(mem.clone()))
    .open()
    .unwrap()
    {
        AnyTree::Standard(t) => t,
        AnyTree::Blob(_) => unreachable!("standard tree"),
    };

    for i in 0..KEYS {
        tree.insert(format!("key{i:08}").as_bytes(), VALUE.to_vec(), i);
    }
    tree.flush_active_memtable(0).unwrap();

    let used = tree.storage_stats().unwrap().used_bytes;
    if tight {
        // Free space far below the rewrite's transient need → the gate skips and
        // the tight path slices instead.
        mem.set_capacity(used + used / 4);
        tree.update_runtime_config(|c| {
            c.storage_admission_check = true;
            c.tight_space_compaction = true;
        })
        .unwrap();
    }
    (tree, mem, folder)
}

fn bench_tight_space(c: &mut Criterion) {
    let mut group = c.benchmark_group("tight_space_compaction");
    group.sample_size(10);

    for (label, tight) in [("ample", false), ("tight", true)] {
        group.bench_function(label, |b| {
            let mut samples = Vec::new();
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let (tree, _mem, _folder) = build_gated_tree(tight);
                    let start = Instant::now();
                    tree.major_compact(64 * 1024 * 1024, 0).unwrap();
                    let elapsed = start.elapsed();
                    samples.push(elapsed);
                    total += elapsed;
                    std::hint::black_box(&tree);
                }
                total
            });
            report_percentiles(label, samples);
        });
    }

    group.finish();
}

criterion_group!(benches, bench_tight_space);
criterion_main!(benches);

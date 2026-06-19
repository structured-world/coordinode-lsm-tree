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
//!
//! `tight_space_blob_defrag` mirrors this for a KV-separated tree whose blob
//! files are half-dead (a stale, live generation): the timed `major_compact`
//! RELOCATES the live blobs into fresh compact files, sliced-and-punched under a
//! tight disk (`tight`) or in one pass with ample space (`ample`).

use criterion::{Criterion, criterion_group, criterion_main};
use lsm_tree::fs::MemFs;
use lsm_tree::{AbstractTree, AnyTree, Config, KvSeparationOptions, SequenceNumberCounter};
use std::sync::Arc;
use std::time::{Duration, Instant};

const KEYS: u64 = 20_000;
const VALUE: [u8; 100] = [0xCD; 100];

/// Number of KV-separated keys for the blob-defrag arm (each value is a blob).
const BLOB_KEYS: u64 = 8_000;

/// Deterministic high-entropy value so blob payload does not compress away (the
/// relocation transient must be real for the gate to skip the full merge).
fn blob_value(i: u64, generation: u8) -> Vec<u8> {
    let mut s = (i + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ (u64::from(generation) << 1);
    (0..200u32)
        .map(|_| {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            (s >> 24) as u8
        })
        .collect()
}

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

/// Builds a KV-separated tree whose blob files are ~half dead (a stale, live
/// generation) so a merge must RELOCATE them, then (when `tight`) caps the disk
/// so the full relocation is gated and the sliced blob-defrag path runs. The
/// returned tree's next `major_compact` is the relocating compaction to time.
fn build_gated_blob_tree(tight: bool) -> (lsm_tree::BlobTree, MemFs, tempfile::TempDir) {
    let folder = tempfile::tempdir().unwrap();
    let mem = MemFs::with_capacity(u64::MAX);
    let tree = match Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(mem.clone()))
    .with_kv_separation(Some(
        KvSeparationOptions::default()
            .separation_threshold(64)
            .age_cutoff(1.0)
            .staleness_threshold(0.1)
            .file_target_size(48 * 1024),
    ))
    .open()
    .unwrap()
    {
        AnyTree::Blob(t) => t,
        AnyTree::Standard(_) => unreachable!("blob tree"),
    };

    for i in 0..BLOB_KEYS {
        tree.insert(format!("key{i:08}").as_bytes(), blob_value(i, 1), i);
    }
    tree.flush_active_memtable(0).unwrap();
    // Overwrite EVEN keys → every gen-1 blob file ends up ~half dead.
    for i in (0..BLOB_KEYS).step_by(2) {
        tree.insert(
            format!("key{i:08}").as_bytes(),
            blob_value(i, 2),
            BLOB_KEYS + i,
        );
    }
    tree.flush_active_memtable(0).unwrap();

    // Prep merge (ample, high watermark) records the fragmentation that makes the
    // next merge a relocating one; fragmentation is learned during a merge.
    let gc_watermark = 4 * BLOB_KEYS;
    tree.index
        .update_runtime_config(|c| {
            c.storage_admission_check = true;
            c.storage_limit_bytes = None;
        })
        .unwrap();
    tree.major_compact(64 * 1024 * 1024, gc_watermark).unwrap();

    if tight {
        let used = tree.storage_stats().unwrap().used_bytes;
        mem.set_capacity(used + used / 4);
        tree.index
            .update_runtime_config(|c| {
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

/// Times the relocating blob-defrag compaction on a near-full disk (`tight`)
/// against the same relocation with ample space (`ample` baseline).
fn bench_blob_defrag(c: &mut Criterion) {
    let mut group = c.benchmark_group("tight_space_blob_defrag");
    group.sample_size(10);

    let gc_watermark = 4 * BLOB_KEYS;
    for (label, tight) in [("ample", false), ("tight", true)] {
        group.bench_function(label, |b| {
            let mut samples = Vec::new();
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let (tree, _mem, _folder) = build_gated_blob_tree(tight);
                    let start = Instant::now();
                    tree.major_compact(64 * 1024 * 1024, gc_watermark).unwrap();
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

criterion_group!(benches, bench_tight_space, bench_blob_defrag);
criterion_main!(benches);

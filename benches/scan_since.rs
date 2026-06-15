// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Benchmarks for the `seqno_in_index` block-skip optimization behind
//! `Tree::scan_since_seqno` (the CDC / incremental-scan primitive).
//!
//! Three head-to-head comparisons of `seqno_in_index = true` vs `false`:
//!   - `scan_since/sparse_1pct` — only the top 1% of seqnos are recent, so
//!     block-skip should read a tiny fraction of the data blocks (target: a
//!     large speedup vs the full per-entry filter).
//!   - `write_throughput` — bulk insert + flush; the seqno-bounded index is
//!     slightly larger, so this measures the write-side cost (target: small).
//!   - `point_lookup` — random `get`; the index format must not affect the
//!     point-read path (target: indistinguishable).

#![expect(
    clippy::expect_used,
    reason = "benchmark setup favors concise panic messages"
)]

use criterion::{Criterion, criterion_group, criterion_main};
use lsm_tree::runtime_config::RuntimeConfig;
use lsm_tree::{AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter, Tree};
use tempfile::TempDir;

/// Build a freshly-flushed single-run tree of `n` entries (seqno = key index),
/// with the seqno-bounded index on or off. Small data blocks make the per-block
/// seqno bounds meaningful.
fn build_tree(seqno_in_index: bool, n: u64) -> (TempDir, Tree) {
    let dir = TempDir::new().expect("tempdir");
    let mut rc = RuntimeConfig::default();
    rc.seqno_in_index = seqno_in_index;
    let any = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_runtime_config(rc)
    .open()
    .expect("open");
    let tree = match any {
        AnyTree::Standard(t) => t,
        AnyTree::Blob(_) => panic!("expected Standard tree"),
    };
    for i in 0..n {
        tree.insert(format!("key{i:08}").as_bytes(), b"value", i);
    }
    tree.flush_active_memtable(0).expect("flush");
    (dir, tree)
}

/// Run `iters` timed invocations of `op` for Criterion's `iter_custom`, also
/// recording each op's individual latency and printing P99/P999 to stderr.
///
/// Aggregate throughput still comes from Criterion's own statistics over the
/// returned total; the per-op tail percentiles surface latency regressions that
/// a central-tendency-only view would hide. stderr keeps the line out of
/// Criterion's stdout report.
fn timed_with_tail<F: FnMut()>(label: &str, iters: u64, mut op: F) -> std::time::Duration {
    use std::time::Instant;
    let mut samples = Vec::with_capacity(iters as usize);
    let started = Instant::now();
    for _ in 0..iters {
        let t0 = Instant::now();
        op();
        samples.push(t0.elapsed());
    }
    let total = started.elapsed();
    if !samples.is_empty() {
        samples.sort_unstable();
        let pct =
            |per_mille: usize| samples[(samples.len() * per_mille / 1000).min(samples.len() - 1)];
        eprintln!(
            "{label}: p50={:?} p99={:?} p999={:?}",
            pct(500),
            pct(990),
            pct(999)
        );
    }
    total
}

/// Sparse-changes scan: target near the top of the seqno range so only ~1% of
/// entries qualify. With `seqno_in_index = true` the scan skips data blocks
/// whose `seqno_max < target`; with it off, every block is read and filtered.
fn bench_scan_since_sparse(c: &mut Criterion) {
    let n = 100_000u64;
    let target: SeqNo = n - n / 100; // top 1% are "recent"

    let mut group = c.benchmark_group("scan_since/sparse_1pct");
    for &on in &[false, true] {
        let (_dir, tree) = build_tree(on, n);
        // Sanity: both formats must return the same number of recent entries.
        let count = tree.scan_since_seqno(target).expect("scan").count();
        assert_eq!(count as u64, n / 100, "1% of entries must qualify");

        let label = if on {
            "seqno_index_on"
        } else {
            "seqno_index_off"
        };
        group.bench_function(label, |b| {
            b.iter_custom(|iters| {
                timed_with_tail(label, iters, || {
                    let c = tree.scan_since_seqno(target).expect("scan").count();
                    std::hint::black_box(c);
                })
            });
        });
    }
    group.finish();
}

/// Write-side cost of the seqno-bounded index: bulk insert + flush from an empty
/// tree each iteration.
fn bench_write_throughput(c: &mut Criterion) {
    let n = 20_000u64;
    let mut group = c.benchmark_group("scan_since/write_throughput");
    for &on in &[false, true] {
        let label = if on {
            "seqno_index_on"
        } else {
            "seqno_index_off"
        };
        group.bench_function(label, |b| {
            b.iter_custom(|iters| {
                timed_with_tail(label, iters, || {
                    let (dir, _tree) = build_tree(on, n);
                    std::hint::black_box(&dir);
                })
            });
        });
    }
    group.finish();
}

/// Point-lookup cost: the index format must not affect `get`. Random-ish key
/// order via a multiplicative hash over the keyspace.
fn bench_point_lookup(c: &mut Criterion) {
    let n = 100_000u64;
    // Precompute a shuffled key set ONCE so the timed loop measures only `get`,
    // not per-iteration `format!` allocation (which would dwarf a ~1 us lookup
    // and mask the index-format delta this bench is meant to isolate).
    let keys: Vec<Vec<u8>> = (0..n)
        .map(|i| {
            let idx = i.wrapping_mul(2_654_435_761) % n;
            format!("key{idx:08}").into_bytes()
        })
        .collect();

    let mut group = c.benchmark_group("scan_since/point_lookup");
    for &on in &[false, true] {
        let (_dir, tree) = build_tree(on, n);
        let label = if on {
            "seqno_index_on"
        } else {
            "seqno_index_off"
        };
        group.bench_function(label, |b| {
            let mut i = 0usize;
            b.iter_custom(|iters| {
                timed_with_tail(label, iters, || {
                    let key = &keys[i % keys.len()];
                    i = i.wrapping_add(1);
                    let v = tree.get(key, SeqNo::MAX).expect("get");
                    std::hint::black_box(v);
                })
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_scan_since_sparse,
    bench_write_throughput,
    bench_point_lookup
);
criterion_main!(benches);

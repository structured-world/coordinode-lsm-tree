// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Columnar (PAX) read-path benchmark matrix: row-major vs columnar, with and
//! without per-block zone maps. The columnar layout's wins and overhead
//! boundaries are otherwise unproven on the bench; this exercises them
//! end-to-end through the public read API, which reconstructs rows from columnar
//! blocks transparently.
//!
//!   - `columnar/full_scan` — iterate every row. Columnar pays the
//!     transpose-back-to-row cost here, so this is the overhead boundary.
//!   - `columnar/point_lookup` — random `get`; the layout must not regress the
//!     point-read path.
//!
//! Three layouts per operation: `row` (baseline), `columnar`, and
//! `columnar+zonemap`.

#![cfg(feature = "columnar")]
#![expect(
    clippy::expect_used,
    reason = "benchmark setup favors concise panic messages"
)]

use criterion::{Criterion, criterion_group, criterion_main};
use lsm_tree::runtime_config::RuntimeConfig;
use lsm_tree::{AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter, Tree};
use tempfile::TempDir;

#[derive(Clone, Copy)]
struct Layout {
    columnar: bool,
    zone_map: bool,
    label: &'static str,
}

const LAYOUTS: [Layout; 3] = [
    Layout {
        columnar: false,
        zone_map: false,
        label: "row",
    },
    Layout {
        columnar: true,
        zone_map: false,
        label: "columnar",
    },
    Layout {
        columnar: true,
        zone_map: true,
        label: "columnar+zonemap",
    },
];

/// Build a freshly-flushed single-run tree of `n` entries under `layout`.
fn build_tree(layout: Layout, n: u64) -> (TempDir, Tree) {
    let dir = TempDir::new().expect("tempdir");
    let mut rc = RuntimeConfig::default();
    rc.columnar = layout.columnar;
    rc.zone_map = layout.zone_map;
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
        tree.insert(
            format!("key{i:08}").as_bytes(),
            format!("value-{i:08}-payload").as_bytes(),
            i,
        );
    }
    tree.flush_active_memtable(0).expect("flush");
    (dir, tree)
}

/// Run `iters` timed invocations of `op` for Criterion's `iter_custom`, also
/// recording each op's individual latency and printing P50/P99/P999 to stderr so
/// a tail regression the aggregate would hide still surfaces.
fn timed_with_tail<F: FnMut()>(label: &str, iters: u64, mut op: F) -> std::time::Duration {
    use std::time::Instant;
    let mut samples = Vec::with_capacity(usize::try_from(iters).unwrap_or(0));
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

/// Full-row scan: the columnar overhead boundary (rows are transposed back from
/// columns). Every layout must return the same row count.
fn bench_full_scan(c: &mut Criterion) {
    let n = 100_000u64;
    let mut group = c.benchmark_group("columnar/full_scan");
    for layout in LAYOUTS {
        let (_dir, tree) = build_tree(layout, n);
        let count = tree.iter(SeqNo::MAX, None).count();
        assert_eq!(
            count as u64, n,
            "[{}] full scan must see every row",
            layout.label
        );

        group.bench_function(layout.label, |b| {
            b.iter_custom(|iters| {
                timed_with_tail(layout.label, iters, || {
                    let c = tree.iter(SeqNo::MAX, None).count();
                    std::hint::black_box(c);
                })
            });
        });
    }
    group.finish();
}

/// Point-lookup cost: the layout must not regress `get`. Random-ish key order via
/// a multiplicative hash, precomputed so the timed loop measures only `get`.
fn bench_point_lookup(c: &mut Criterion) {
    let n = 100_000u64;
    let keys: Vec<Vec<u8>> = (0..n)
        .map(|i| {
            let idx = i.wrapping_mul(2_654_435_761) % n;
            format!("key{idx:08}").into_bytes()
        })
        .collect();

    let mut group = c.benchmark_group("columnar/point_lookup");
    for layout in LAYOUTS {
        let (_dir, tree) = build_tree(layout, n);
        group.bench_function(layout.label, |b| {
            let mut i = 0usize;
            b.iter_custom(|iters| {
                timed_with_tail(layout.label, iters, || {
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

criterion_group!(benches, bench_full_scan, bench_point_lookup);
criterion_main!(benches);

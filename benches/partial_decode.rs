// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Range-query throughput over large cold-tier zstd blocks (#257).
//!
//! Builds a tree with 512 KiB data blocks at zstd L19 (above the partial tier's
//! 256 KiB engage floor; production cold blocks are far larger), so each block
//! splits into many inner zstd blocks and the `block_layout` section is present.
//! Times small range reads with a tiny block cache. With the partial path on, a
//! read decodes only the inner blocks covering its key range and a wider read
//! RESUMES (decoding just the new tail blocks); with it off, each cold read
//! decodes the whole block. Toggle `LSM_PARTIAL_DECODE` for the A/B.

use criterion::{Criterion, criterion_group, criterion_main};
use lsm_tree::config::{BlockSizePolicy, CompressionPolicy};
use lsm_tree::{AbstractTree, CompressionType, Config, Guard, SeqNo, SequenceNumberCounter};
use std::time::{Duration, Instant};

const N: u64 = 60_000;
const SPAN: u64 = 4; // keys read per range

fn key(i: u64) -> Vec<u8> {
    format!("key-{i:08}").into_bytes()
}

/// Runs `iters` distinct small range reads marching by `stride`, capturing
/// per-query latency to report tail percentiles (P50/P99/P999) on stderr in
/// addition to the aggregate elapsed Duration criterion measures. Tail latency
/// is the signal that matters for the partial-decode path (a cold-block decode
/// is a tail event), so it is surfaced explicitly rather than hidden in a mean.
fn run_window_reads(tree: &lsm_tree::AnyTree, iters: u64, stride: u64) -> Duration {
    let mut lat_us: Vec<u64> =
        Vec::with_capacity(usize::try_from(iters).expect("iters fits usize"));
    let mut lo = 0u64;
    let batch_start = Instant::now();
    for _ in 0..iters {
        lo = (lo + stride) % (N - SPAN);
        let t0 = Instant::now();
        for g in tree.range(key(lo)..key(lo + SPAN), SeqNo::MAX, None) {
            std::hint::black_box(g.key().unwrap());
        }
        lat_us.push(u64::try_from(t0.elapsed().as_micros()).expect("elapsed micros fits u64"));
    }
    let elapsed = batch_start.elapsed();
    if !lat_us.is_empty() {
        lat_us.sort_unstable();
        let at = |num: usize, den: usize| lat_us[(lat_us.len() * num / den).min(lat_us.len() - 1)];
        eprintln!(
            "    tail latency: p50={}us p99={}us p999={}us",
            at(50, 100),
            at(99, 100),
            at(999, 1000),
        );
    }
    elapsed
}

fn bench_partial_range(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(
        CompressionType::zstd(19).expect("level"),
    ))
    // Above the partial tier's 256 KiB engage floor (production cold blocks are
    // far larger; this is the smallest size at which the path turns on).
    .data_block_size_policy(BlockSizePolicy::all(512 * 1024))
    // Tiny block cache so each distinct read is cache-cold (the partial-decode
    // win is on the first decode; warm reads hit the cache regardless).
    .use_cache(std::sync::Arc::new(lsm_tree::Cache::with_capacity_bytes(
        1024 * 1024,
    )))
    .open()
    .unwrap();

    for i in 0..N {
        tree.insert(
            key(i),
            format!("value-{i:08}-padding-padding").into_bytes(),
            0,
        );
    }
    tree.flush_active_memtable(0).unwrap();

    // In-block marching: a small step keeps many consecutive windows inside the
    // same block with a monotonically rising upper, so the partial path RESUMES
    // (decoding only each new tail) and eventually promotes to a full block.
    // Exercises that resume keeps this at parity with full decode + block cache
    // (it was a ~3x regression before the resumable decoder).
    c.bench_function("range_small_window_marching_in_block", |b| {
        b.iter_custom(|iters| run_window_reads(&tree, iters, 137));
    });

    // Favorable-to-partial pattern: a large prime stride makes each query land
    // in a DIFFERENT block (no in-block monotonic growth), so each is a single
    // cold touch — the partial path decodes only the inner blocks up to the
    // window and skips the trailing ones.
    c.bench_function("range_small_window_uniform_cross_block", |b| {
        b.iter_custom(|iters| run_window_reads(&tree, iters, 6_151));
    });
}

criterion_group!(benches, bench_partial_range);
criterion_main!(benches);

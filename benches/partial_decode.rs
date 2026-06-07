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
        b.iter_custom(|iters| {
            let start = Instant::now();
            let mut lo = 0u64;
            for _ in 0..iters {
                lo = (lo + 137) % (N - SPAN);
                for g in tree.range(key(lo)..key(lo + SPAN), SeqNo::MAX, None) {
                    std::hint::black_box(g.key().unwrap());
                }
            }
            start.elapsed()
        });
    });

    // Favorable-to-partial pattern: a large prime stride makes each query land
    // in a DIFFERENT block (no in-block monotonic growth), so each is a single
    // cold touch — the partial path decodes only the inner blocks up to the
    // window and skips the trailing ones.
    c.bench_function("range_small_window_uniform_cross_block", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            let mut lo = 0u64;
            for _ in 0..iters {
                lo = (lo + 6_151) % (N - SPAN);
                for g in tree.range(key(lo)..key(lo + SPAN), SeqNo::MAX, None) {
                    std::hint::black_box(g.key().unwrap());
                }
            }
            start.elapsed()
        });
    });

    let _ = Duration::from_secs(0);
}

criterion_group!(benches, bench_partial_range);
criterion_main!(benches);

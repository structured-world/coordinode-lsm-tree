// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Range-query throughput over large cold-tier zstd blocks (#257).
//!
//! Builds a tree with 256 KiB data blocks at zstd L19 (so each block splits
//! into many inner zstd blocks and the `block_layout` section is present), then
//! times many DISTINCT small range reads spread across the tree with a tiny
//! block cache (so reads are cache-cold). On a build with the partial-decode
//! path, each read decodes only the inner blocks covering its key range; on a
//! build without it, each read decodes the whole 256 KiB block — the A/B delta
//! is the win. Run on this branch and on the pre-#257 baseline to compare.

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
    .data_block_size_policy(BlockSizePolicy::all(256 * 1024))
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

    // Adversarial-to-partial pattern: small step keeps many consecutive windows
    // inside the same large block with a monotonically rising upper, so the
    // partial path repeatedly re-decodes a growing prefix (no resumable decode
    // upstream) while full-decode + block-cache amortizes one decode per block.
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

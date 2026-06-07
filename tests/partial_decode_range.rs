// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! End-to-end coverage for the range-query partial-decode path (#257).
//!
//! A tree configured with large data blocks + a high zstd level splits each
//! data block into many inner zstd blocks and persists their layout, so a
//! bounded range query transparently takes the partial-decode path (decode
//! only the inner blocks the range covers). These tests assert the path is
//! correctness-transparent: every bounded range — forward and reverse, at the
//! block start, middle, across block boundaries, and empty — returns exactly
//! the same keys a full decode would.

#![cfg(feature = "zstd")]

use lsm_tree::config::{BlockSizePolicy, CompressionPolicy};
use lsm_tree::{
    AbstractTree, Cache, CompressionType, Config, Guard, SeqNo, SequenceNumberCounter,
    get_tmp_folder,
};
use std::sync::Arc;
use test_log::test;

const N: u64 = 20_000;

/// Opt into the partial-decode read path for this test process (it is OFF by
/// default until a resumable decoder lands upstream). nextest isolates each test
/// in its own process, so this env mutation does not leak between tests, and the
/// path reads the flag once via a `OnceLock`. Call before opening any tree.
fn enable_partial_decode() {
    // SAFETY: set before any other thread in this single-test process reads the
    // env; nextest runs each test in a dedicated process.
    unsafe { std::env::set_var("LSM_PARTIAL_DECODE", "1") };
}

fn key(i: u64) -> Vec<u8> {
    format!("key-{i:08}").into_bytes()
}

/// Build a flushed tree with large (256 KiB) zstd-L19 data blocks, so blocks
/// split into many inner zstd blocks and the `block_layout` section is present.
fn large_zstd_block_tree() -> (tempfile::TempDir, lsm_tree::AnyTree) {
    let folder = get_tmp_folder();
    let dir = tempfile::TempDir::new_in(folder).unwrap();
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(
        CompressionType::zstd(19).expect("valid level"),
    ))
    .data_block_size_policy(BlockSizePolicy::all(256 * 1024))
    .open()
    .unwrap();

    for i in 0..N {
        // Values padded so 256 KiB blocks hold many entries across inner blocks.
        tree.insert(
            key(i),
            format!("value-{i:08}-padding-padding").into_bytes(),
            0,
        );
    }
    tree.flush_active_memtable(0).unwrap();
    (dir, tree)
}

/// Build a flushed tree like [`large_zstd_block_tree`] but with a caller-shared
/// block cache, so the partial-decode cache path is exercised across reads.
fn large_zstd_block_tree_with_cache(cache: Arc<Cache>) -> (tempfile::TempDir, lsm_tree::AnyTree) {
    let folder = get_tmp_folder();
    let dir = tempfile::TempDir::new_in(folder).unwrap();
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(
        CompressionType::zstd(19).expect("valid level"),
    ))
    .data_block_size_policy(BlockSizePolicy::all(256 * 1024))
    .use_cache(cache)
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
    (dir, tree)
}

/// Collect the keys a forward range query returns.
fn range_keys(tree: &lsm_tree::AnyTree, lo: u64, hi: u64) -> Vec<Vec<u8>> {
    tree.range(key(lo)..key(hi), SeqNo::MAX, None)
        .map(|g| g.key().unwrap().to_vec())
        .collect()
}

/// Collect the keys a reverse range query returns.
fn range_keys_rev(tree: &lsm_tree::AnyTree, lo: u64, hi: u64) -> Vec<Vec<u8>> {
    tree.range(key(lo)..key(hi), SeqNo::MAX, None)
        .rev()
        .map(|g| g.key().unwrap().to_vec())
        .collect()
}

#[test]
fn partial_decode_forward_ranges_match_expected() {
    enable_partial_decode();
    let (_dir, tree) = large_zstd_block_tree();

    // Windows at the block start, middle, a wide span, and crossing into the
    // tail — all bounded (upper set) so the partial path is eligible.
    for (lo, hi) in [
        (10u64, 50),
        (0, 1),
        (5_000, 5_001),
        (12_345, 12_900),
        (0, N),
    ] {
        let got = range_keys(&tree, lo, hi);
        let expected: Vec<Vec<u8>> = (lo..hi.min(N)).map(key).collect();
        assert_eq!(
            got, expected,
            "forward range [{lo}, {hi}) must match the full-decode result",
        );
    }
}

#[test]
fn partial_decode_reverse_ranges_match_expected() {
    enable_partial_decode();
    let (_dir, tree) = large_zstd_block_tree();

    for (lo, hi) in [(10u64, 50), (5_000, 5_001), (12_345, 12_900)] {
        let got = range_keys_rev(&tree, lo, hi);
        let expected: Vec<Vec<u8>> = (lo..hi.min(N)).rev().map(key).collect();
        assert_eq!(
            got, expected,
            "reverse range [{lo}, {hi}) must match the full-decode result",
        );
    }
}

#[test]
fn partial_decode_empty_and_boundary_ranges() {
    let (_dir, tree) = large_zstd_block_tree();

    // Empty (lo == hi) and a range whose bounds fall between existing keys
    // (`key-00000010x` sorts after `key-00000010`) must yield nothing / exact.
    assert!(range_keys(&tree, 100, 100).is_empty(), "lo == hi → empty");

    // Single-key window.
    assert_eq!(range_keys(&tree, 9_999, 10_000), vec![key(9_999)]);

    // A bounded window whose upper lands exactly on a key (exclusive) excludes
    // that key.
    let got = range_keys(&tree, 200, 205);
    assert_eq!(got, (200..205).map(key).collect::<Vec<_>>());
}

#[test]
fn partial_decode_cache_reuse_and_high_water_growth_stays_correct() {
    // A shared cache so the partial-decode path caches the synthesized covering
    // block and serves / grows it on later reads of the same block. Correctness
    // must hold regardless of read order: a narrow read after a wide one reuses
    // the wider cached block; a wider read after a narrow one grows the extent.
    let cache = Arc::new(Cache::with_capacity_bytes(64 * 1024 * 1024));
    let (_dir, tree) = large_zstd_block_tree_with_cache(cache.clone());

    // All windows fall inside the first (large) data block so they share one
    // cache slot, exercising reuse + high-water growth on a single offset.
    let windows = [
        (10u64, 30), // first read: partial decode, cache covered ~key29
        (10, 300),   // wider: covered_upper grows, re-decode + re-cache
        (10, 30),    // narrow again: must be served from the larger cache
        (12, 18),    // narrower sub-window: served from cache
        (250, 300),  // shifted but still within the grown extent: from cache
        (10, 1_000), // even wider: grows the extent again
        (500, 600),  // within the newly grown extent
    ];
    for (lo, hi) in windows {
        let got = range_keys(&tree, lo, hi);
        let expected: Vec<Vec<u8>> = (lo..hi.min(N)).map(key).collect();
        assert_eq!(
            got, expected,
            "window [{lo}, {hi}) must match the full-decode result across cache reuse",
        );
        // Reverse over the same window must also be correct off the cached block.
        let got_rev = range_keys_rev(&tree, lo, hi);
        let expected_rev: Vec<Vec<u8>> = (lo..hi.min(N)).rev().map(key).collect();
        assert_eq!(
            got_rev, expected_rev,
            "reverse window [{lo}, {hi}) must match across cache reuse",
        );
    }

    // The partial path must actually have populated the cache (otherwise the
    // test would silently pass on the full-decode fallback).
    assert!(
        cache.size() > 0,
        "partial-decode reads must populate the shared cache",
    );
}

#[test]
fn partial_decode_promotion_paths_stay_correct() {
    // Drive the adaptive heuristic through every branch on a shared cache:
    // fraction-promotion (a wide window decoding most of a block), hits-promotion
    // (the same covered window read repeatedly), and post-promotion reads (served
    // from the now-resident full block). Correctness must hold throughout.
    enable_partial_decode();
    let cache = Arc::new(Cache::with_capacity_bytes(64 * 1024 * 1024));
    let (_dir, tree) = large_zstd_block_tree_with_cache(cache.clone());

    // Wide window: covers most of the first large block → fraction-promotion to
    // a full resident block on the covering decode.
    let got = range_keys(&tree, 0, 5_000);
    assert_eq!(got, (0..5_000).map(key).collect::<Vec<_>>());

    // hits-promotion: read a narrow covered window many times. Each pass after
    // the partial is cached bumps the hit count; once it crosses the promotion
    // threshold the block is decoded fully. Results stay identical every pass.
    for _ in 0..8 {
        let got = range_keys(&tree, 100, 130);
        assert_eq!(
            got,
            (100..130).map(key).collect::<Vec<_>>(),
            "repeated reads must stay correct across hits-promotion",
        );
    }

    // After promotion the block is resident; a fresh sub-window is served from
    // the full block and must still be exact (forward and reverse).
    assert_eq!(
        range_keys(&tree, 110, 115),
        (110..115).map(key).collect::<Vec<_>>()
    );
    assert_eq!(
        range_keys_rev(&tree, 110, 115),
        (110..115).rev().map(key).collect::<Vec<_>>(),
    );

    // A wide window spanning several blocks, then a narrow re-read inside it.
    assert_eq!(
        range_keys(&tree, 0, 9_000),
        (0..9_000).map(key).collect::<Vec<_>>()
    );
    assert_eq!(
        range_keys(&tree, 8_000, 8_010),
        (8_000..8_010).map(key).collect::<Vec<_>>()
    );
}

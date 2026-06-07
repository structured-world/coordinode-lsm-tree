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
    AbstractTree, CompressionType, Config, Guard, SeqNo, SequenceNumberCounter, get_tmp_folder,
};
use test_log::test;

const N: u64 = 20_000;

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

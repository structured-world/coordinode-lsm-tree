// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Integration tests for `AbstractTree::approximate_range_stats` (#499): the
//! estimate is computed from SST block-index offsets + the memtable, never
//! reading a data block, so it is approximate (block granularity). Tests assert
//! the invariants (empty → zero, full range = exact entry count, monotonic in
//! range width, memtable-only is counted, KV-separation includes blob bytes)
//! and bound the sub-range error loosely.

use lsm_tree::{
    AbstractTree, AnyTree, Config, KvSeparationOptions, SeqNo, SequenceNumberCounter, Tree,
    config::BlockSizePolicy, get_tmp_folder,
};
use test_log::test;

fn key(i: u32) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

/// Standard tree spread across several disjoint-range SSTs with small data
/// blocks, so a range query spans multiple SSTs and several blocks per SST.
fn build_standard(folder: &std::path::Path) -> Tree {
    let any = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .open()
    .expect("open");
    let AnyTree::Standard(tree) = any else {
        panic!("expected standard tree");
    };
    // 5 SSTs × 200 keys (disjoint key ranges), 200-byte values.
    for sst in 0..5u32 {
        for i in 0..200u32 {
            tree.insert(key(sst * 200 + i), vec![b'v'; 200], 0);
        }
        tree.flush_active_memtable(0).expect("flush");
    }
    tree
}

#[test]
fn empty_range_is_zero() {
    let folder = get_tmp_folder();
    let tree = build_standard(folder.path());
    // A range past every stored key.
    let stats = tree
        .approximate_range_stats(key(900_000)..key(999_999), SeqNo::MAX)
        .expect("stats");
    assert_eq!(stats.bytes, 0, "empty range has no bytes");
    assert_eq!(stats.key_count, 0, "empty range has no keys");
}

#[test]
fn full_range_key_count_is_exact_after_flush() {
    let folder = get_tmp_folder();
    let tree = build_standard(folder.path());
    // Everything is flushed, so the full-range fraction is 1.0 per SST: the
    // estimated key count must equal the true total entry count exactly.
    let stats = tree
        .approximate_range_stats::<&[u8], _>(.., SeqNo::MAX)
        .expect("stats");
    assert_eq!(
        stats.key_count,
        tree.approximate_len() as u64,
        "full-range count equals total entries"
    );
    // Full-range bytes ≈ the data sections of every SST: positive and not above
    // total disk space (which also includes index / filter / meta overhead).
    assert!(stats.bytes > 0, "full range has bytes");
    assert!(
        stats.bytes <= tree.disk_space(),
        "data-section estimate {} must not exceed disk_space {}",
        stats.bytes,
        tree.disk_space()
    );
    assert!(
        stats.bytes > tree.disk_space() / 2,
        "data sections should dominate disk_space"
    );
}

#[test]
fn subrange_count_is_close_to_actual() {
    let folder = get_tmp_folder();
    let tree = build_standard(folder.path());
    // A sub-range spanning two SSTs (keys 300..700 → SST 1, 2, 3 boundaries).
    let lo = key(300);
    let hi = key(700);
    let actual = tree.range(lo.clone()..hi.clone(), SeqNo::MAX, None).count() as u64;
    let est = tree
        .approximate_range_stats(lo..hi, SeqNo::MAX)
        .expect("stats")
        .key_count;
    // Block-granularity estimate: allow ~30% error around the true count.
    let lo_b = actual * 7 / 10;
    let hi_b = actual * 13 / 10;
    assert!(
        est >= lo_b && est <= hi_b,
        "estimate {est} should be within ~30% of actual {actual}"
    );
}

#[test]
fn wider_range_estimates_at_least_as_much() {
    let folder = get_tmp_folder();
    let tree = build_standard(folder.path());
    let narrow = tree
        .approximate_range_stats(key(0)..key(250), SeqNo::MAX)
        .expect("stats");
    let wide = tree
        .approximate_range_stats(key(0)..key(750), SeqNo::MAX)
        .expect("stats");
    assert!(
        wide.bytes >= narrow.bytes,
        "wider range must not estimate fewer bytes ({} vs {})",
        wide.bytes,
        narrow.bytes
    );
    assert!(wide.key_count >= narrow.key_count, "wider range, more keys");
}

#[test]
fn memtable_only_range_is_counted() {
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("open");
    let AnyTree::Standard(tree) = any else {
        panic!("expected standard tree");
    };
    // Insert without flushing: the contribution comes entirely from the memtable.
    for i in 0..100u32 {
        tree.insert(key(i), vec![b'v'; 64], 0);
    }
    let stats = tree
        .approximate_range_stats::<&[u8], _>(.., SeqNo::MAX)
        .expect("stats");
    assert!(stats.bytes > 0, "memtable range has bytes");
    // All 100 keys fall in the full range.
    assert_eq!(stats.key_count, 100, "memtable full-range count is exact");
}

#[test]
fn subrange_within_a_single_block_is_nonzero() {
    // 30 keys with the default (4 KiB) block size land in ONE data block. A
    // sub-range inside that block must still report a non-empty estimate — the
    // boundary block has to be counted, not excluded.
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("open");
    let AnyTree::Standard(tree) = any else {
        panic!("expected standard tree");
    };
    for i in 0..30u32 {
        tree.insert(key(i), vec![b'v'; 64], 0);
    }
    tree.flush_active_memtable(0).expect("flush");
    let blocks: u64 = tree
        .current_version()
        .iter_tables()
        .map(|t| t.metadata.data_block_count)
        .sum();
    assert_eq!(blocks, 1, "test precondition: a single data block");

    let stats = tree
        .approximate_range_stats(key(10)..key(20), SeqNo::MAX)
        .expect("stats");
    assert!(
        stats.key_count > 0 && stats.bytes > 0,
        "a sub-range inside a single block must be non-empty, got {stats:?}"
    );
}

#[test]
fn all_bound_kinds_are_handled() {
    use std::ops::Bound;

    // Flushed tree: exercises the SST data-block offset bound arms (Excluded
    // lower, Included upper, and the half-bounded RangeTo / RangeFrom).
    let f1 = get_tmp_folder();
    let tree = build_standard(f1.path());
    let excl_incl = tree
        .approximate_range_stats(
            (Bound::Excluded(key(300)), Bound::Included(key(700))),
            SeqNo::MAX,
        )
        .expect("stats");
    assert!(excl_incl.key_count > 0, "excluded..=included over SSTs");
    let to = tree
        .approximate_range_stats(..key(500), SeqNo::MAX)
        .expect("stats");
    let from = tree
        .approximate_range_stats(key(500).., SeqNo::MAX)
        .expect("stats");
    assert!(
        to.key_count > 0 && from.key_count > 0,
        "half-bounded over SSTs"
    );
    // The two halves together cover at least the full count.
    assert!(to.key_count + from.key_count >= tree.approximate_len() as u64);

    // Memtable-only tree: exercises the same bound arms on the memtable path.
    let f2 = get_tmp_folder();
    let any = Config::new(
        f2.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("open");
    let AnyTree::Standard(mt) = any else {
        panic!("expected standard tree");
    };
    for i in 0..100u32 {
        mt.insert(key(i), vec![b'v'; 64], 0);
    }
    let m_excl_incl = mt
        .approximate_range_stats(
            (Bound::Excluded(key(10)), Bound::Included(key(50))),
            SeqNo::MAX,
        )
        .expect("stats");
    assert!(
        m_excl_incl.key_count > 0,
        "excluded..=included over memtable"
    );
    let m_to = mt
        .approximate_range_stats(..key(50), SeqNo::MAX)
        .expect("stats");
    let m_from = mt
        .approximate_range_stats(key(50).., SeqNo::MAX)
        .expect("stats");
    assert!(
        m_to.key_count > 0 && m_from.key_count > 0,
        "half-bounded over memtable"
    );
    // A non-empty memtable queried past every key contributes nothing.
    let m_empty = mt
        .approximate_range_stats(key(900)..key(999), SeqNo::MAX)
        .expect("stats");
    assert_eq!(
        m_empty.key_count, 0,
        "memtable range past all keys is empty"
    );
    assert_eq!(m_empty.bytes, 0);
}

#[test]
fn kv_separated_range_includes_blob_bytes() {
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    .open()
    .expect("open");
    let AnyTree::Blob(tree): AnyTree = any else {
        panic!("expected blob tree");
    };
    // 200 entries with 1 KiB values → values live in blob files.
    for i in 0..200u32 {
        tree.insert(key(i), vec![b'v'; 1024], 0);
    }
    tree.flush_active_memtable(0).expect("flush");

    let blob_on_disk = tree.current_version().blob_files.on_disk_size();
    assert!(
        blob_on_disk > 0,
        "values should be separated into blob files"
    );

    let stats = tree
        .approximate_range_stats::<&[u8], _>(.., SeqNo::MAX)
        .expect("stats");
    assert_eq!(stats.key_count, 200, "full-range count is exact");
    // The estimate must include the blob value bytes, not just the index tree's
    // key + pointer bytes (the on-disk blob size depends on value compressibility,
    // so assert against the actual blob footprint rather than a fixed figure).
    assert!(
        stats.bytes >= blob_on_disk,
        "KV-separated estimate {} must include the blob bytes {blob_on_disk}",
        stats.bytes
    );
    // Full range ≈ total disk usage (data sections + blobs).
    let disk = tree.disk_space();
    assert!(
        stats.bytes >= disk / 2 && stats.bytes <= disk,
        "full-range estimate {} should approximate disk_space {disk}",
        stats.bytes
    );
}

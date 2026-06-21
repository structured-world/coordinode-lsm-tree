// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Integration tests for `AbstractTree::level_segment_stats` (#508): per-LSM
//! level and per-segment size + entry stats for tiering / placement. Tests
//! assert the acceptance criteria: per-level / per-segment values are reported,
//! they reconcile with the tree-level aggregate, and reading them does not
//! trigger a data-block scan (verified via the data-block load counter).

use lsm_tree::{AbstractTree, AnyTree, Config, SequenceNumberCounter, Tree, get_tmp_folder};
use test_log::test;

fn key(i: u32) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

fn open(folder: &std::path::Path) -> Tree {
    let any = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("open");
    let AnyTree::Standard(tree) = any else {
        panic!("expected standard tree");
    };
    tree
}

#[test]
fn empty_tree_has_no_segments() {
    let folder = get_tmp_folder();
    let tree = open(folder.path());
    let levels = tree.level_segment_stats().expect("stats");
    let total_segments: usize = levels.iter().map(|l| l.segment_count).sum();
    assert_eq!(total_segments, 0, "no segments before any flush");
    assert!(
        levels
            .iter()
            .all(|l| l.used_bytes == 0 && l.item_count == 0),
        "empty levels carry zero size and count"
    );
}

#[test]
fn per_level_totals_reconcile_with_tree_aggregate() {
    let folder = get_tmp_folder();
    let tree = open(folder.path());
    for sst in 0..4u32 {
        for i in 0..100u32 {
            tree.insert(key(sst * 100 + i), vec![b'v'; 100], 0);
        }
        tree.flush_active_memtable(0).expect("flush");
    }

    let levels = tree.level_segment_stats().expect("stats");
    let aggregate = tree.storage_stats().expect("storage stats");

    let sum_bytes: u64 = levels.iter().map(|l| l.used_bytes).sum();
    let sum_items: u64 = levels.iter().map(|l| l.item_count).sum();
    // A standard tree has no blob files, so the SST sum equals the tree total.
    assert_eq!(sum_bytes, aggregate.used_bytes, "byte totals reconcile");
    assert_eq!(sum_items, aggregate.item_count, "item totals reconcile");

    // Each level's aggregate equals the sum over its own segments.
    for level in &levels {
        let seg_bytes: u64 = level.segments.iter().map(|s| s.used_bytes).sum();
        let seg_items: u64 = level.segments.iter().map(|s| s.item_count).sum();
        assert_eq!(level.used_bytes, seg_bytes, "level bytes = sum of segments");
        assert_eq!(level.item_count, seg_items, "level items = sum of segments");
        assert_eq!(level.segment_count, level.segments.len());
        assert!(
            level.segments.iter().all(|s| s.level == level.level),
            "each segment carries its level index"
        );
    }
}

#[test]
fn multiple_levels_are_reported_after_compaction() {
    let folder = get_tmp_folder();
    let tree = open(folder.path());
    // Several L0 SSTs, then compact them down a level.
    for sst in 0..4u32 {
        for i in 0..100u32 {
            tree.insert(key(sst * 100 + i), vec![b'v'; 100], 0);
        }
        tree.flush_active_memtable(0).expect("flush");
    }
    tree.major_compact(u64::MAX, 1_000).expect("compact");
    // A fresh L0 SST on top of the compacted level.
    for i in 1000..1100u32 {
        tree.insert(key(i), vec![b'v'; 100], 1_000);
    }
    tree.flush_active_memtable(1_000).expect("flush");

    let levels = tree.level_segment_stats().expect("stats");
    let populated = levels.iter().filter(|l| l.segment_count > 0).count();
    assert!(
        populated >= 2,
        "expected segments across at least two levels, got {populated}"
    );
    // Reconciliation still holds across the multi-level layout.
    let sum_items: u64 = levels.iter().map(|l| l.item_count).sum();
    assert_eq!(sum_items, tree.storage_stats().expect("agg").item_count);
}

#[test]
fn point_reads_bump_segment_access_stats() {
    let folder = get_tmp_folder();
    let tree = open(folder.path());
    for i in 0..50u32 {
        tree.insert(key(i), vec![b'v'; 100], 0);
    }
    tree.flush_active_memtable(0).expect("flush");

    let before: u64 = tree
        .level_segment_stats()
        .expect("stats")
        .iter()
        .map(|l| l.reads)
        .sum();
    for _ in 0..10 {
        let _ = tree.get(key(5), lsm_tree::SeqNo::MAX).expect("get");
    }
    let levels = tree.level_segment_stats().expect("stats");
    let after: u64 = levels.iter().map(|l| l.reads).sum();
    assert!(
        after >= before + 10,
        "ten point reads must bump the segment read counter by >= 10 ({before} -> {after})"
    );
    // On a std build a probed segment records its last-access time.
    assert!(
        levels
            .iter()
            .flat_map(|l| &l.segments)
            .filter(|s| s.reads > 0)
            .all(|s| s.last_access_secs > 0),
        "a read segment records its last-access time"
    );
}

#[cfg(feature = "metrics")]
#[test]
fn reading_stats_does_not_scan_data_blocks() {
    let folder = get_tmp_folder();
    let tree = open(folder.path());
    for sst in 0..3u32 {
        for i in 0..100u32 {
            tree.insert(key(sst * 100 + i), vec![b'v'; 100], 0);
        }
        tree.flush_active_memtable(0).expect("flush");
    }
    let before = tree.metrics().data_block_load_count();
    let _ = tree.level_segment_stats().expect("stats");
    let after = tree.metrics().data_block_load_count();
    assert_eq!(
        before, after,
        "computing per-level stats must not load any data block"
    );
}

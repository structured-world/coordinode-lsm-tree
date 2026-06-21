// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Integration tests for `AbstractTree::approximate_range_cardinality` (#505):
//! a zone-map-based row-count estimate plus a selectivity ratio, computed
//! without reading any data block. Tests assert the acceptance criteria: the
//! row count is within a bounded (block-granularity) error of the true count,
//! and selectivity is monotonic in predicate tightness.

use lsm_tree::{
    AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter, Tree, config::BlockSizePolicy,
    get_tmp_folder,
};
use test_log::test;

fn key(i: u32) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

/// Tree with the zone map enabled, spread across several disjoint-range SSTs
/// with small data blocks so a range spans multiple blocks per SST.
fn build_zonemapped(folder: &std::path::Path) -> Tree {
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
    tree.update_runtime_config(|cfg| cfg.zone_map = true)
        .expect("enable zone map");
    for sst in 0..5u32 {
        for i in 0..200u32 {
            tree.insert(key(sst * 200 + i), vec![b'v'; 200], 0);
        }
        tree.flush_active_memtable(0).expect("flush");
    }
    tree
}

#[test]
fn full_range_rows_equal_total_and_selectivity_one() {
    let folder = get_tmp_folder();
    let tree = build_zonemapped(folder.path());
    // Every block's recorded row count sums to the exact entry total.
    let card = tree
        .approximate_range_cardinality::<&[u8], _>(.., SeqNo::MAX)
        .expect("cardinality");
    assert_eq!(
        card.rows,
        tree.approximate_len() as u64,
        "full-range rows equal the total entry count"
    );
    assert!(
        (card.selectivity - 1.0).abs() < 1e-9,
        "full-range selectivity is 1.0, got {}",
        card.selectivity
    );
}

#[test]
fn empty_range_is_zero() {
    let folder = get_tmp_folder();
    let tree = build_zonemapped(folder.path());
    let card = tree
        .approximate_range_cardinality(key(900_000)..key(999_999), SeqNo::MAX)
        .expect("cardinality");
    assert_eq!(card.rows, 0, "empty range has no rows");
    assert_eq!(card.selectivity, 0.0, "empty range has zero selectivity");
}

#[test]
fn range_in_a_data_gap_with_zone_map_is_zero() {
    // A single SST with a hole in the key space: keys 0..100 and 300..400, none
    // in 100..300. With one key per data block (block target below the value
    // size, so no block straddles the gap) and the zone map present, a query
    // that falls entirely in the gap must report zero rows, NOT fall back to a
    // byte-fraction estimate.
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(64))
    .open()
    .expect("open");
    let AnyTree::Standard(tree) = any else {
        panic!("expected standard tree");
    };
    tree.update_runtime_config(|cfg| cfg.zone_map = true)
        .expect("enable zone map");
    for i in 0..100u32 {
        tree.insert(key(i), vec![b'v'; 200], 0);
    }
    for i in 300..400u32 {
        tree.insert(key(i), vec![b'v'; 200], 0);
    }
    tree.flush_active_memtable(0).expect("flush");

    let card = tree
        .approximate_range_cardinality(key(150)..key(250), SeqNo::MAX)
        .expect("cardinality");
    assert_eq!(
        card.rows, 0,
        "a range in a data gap has no rows, got {card:?}"
    );
    assert_eq!(card.selectivity, 0.0);
}

#[test]
fn subrange_rows_within_bounded_error() {
    let folder = get_tmp_folder();
    let tree = build_zonemapped(folder.path());
    let lo = key(300);
    let hi = key(700);
    let actual = tree.range(lo.clone()..hi.clone(), SeqNo::MAX, None).count() as u64;
    let est = tree
        .approximate_range_cardinality(lo..hi, SeqNo::MAX)
        .expect("cardinality")
        .rows;
    // Block-granularity: the estimate counts the boundary blocks in full, so it
    // is at least the true count and within a small relative error.
    assert!(
        est >= actual,
        "estimate {est} must cover the actual {actual}"
    );
    assert!(
        est <= actual + actual / 10 + 16,
        "estimate {est} should be within ~10% of actual {actual}"
    );
}

#[test]
fn selectivity_monotonic_in_predicate_tightness() {
    let folder = get_tmp_folder();
    let tree = build_zonemapped(folder.path());
    let narrow = tree
        .approximate_range_cardinality(key(0)..key(100), SeqNo::MAX)
        .expect("cardinality");
    let mid = tree
        .approximate_range_cardinality(key(0)..key(500), SeqNo::MAX)
        .expect("cardinality");
    let wide = tree
        .approximate_range_cardinality(key(0)..key(900), SeqNo::MAX)
        .expect("cardinality");
    assert!(
        narrow.selectivity <= mid.selectivity && mid.selectivity <= wide.selectivity,
        "selectivity must grow with range width: {} <= {} <= {}",
        narrow.selectivity,
        mid.selectivity,
        wide.selectivity
    );
    assert!(narrow.rows <= mid.rows && mid.rows <= wide.rows);
}

#[test]
fn memtable_only_is_counted() {
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
    for i in 0..100u32 {
        tree.insert(key(i), vec![b'v'; 64], 0);
    }
    let card = tree
        .approximate_range_cardinality::<&[u8], _>(.., SeqNo::MAX)
        .expect("cardinality");
    assert_eq!(card.rows, 100, "memtable rows counted");
    assert!((card.selectivity - 1.0).abs() < 1e-9);
}

#[test]
fn falls_back_without_zone_map() {
    // With the zone map disabled (default), the row count falls back to the
    // byte-fraction estimate and still reflects the range.
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .open()
    .expect("open");
    let AnyTree::Standard(tree) = any else {
        panic!("expected standard tree");
    };
    for i in 0..400u32 {
        tree.insert(key(i), vec![b'v'; 200], 0);
    }
    tree.flush_active_memtable(0).expect("flush");
    let full = tree
        .approximate_range_cardinality::<&[u8], _>(.., SeqNo::MAX)
        .expect("cardinality");
    assert!(full.rows > 0, "fallback row estimate is non-empty");
    let narrow = tree
        .approximate_range_cardinality(key(0)..key(100), SeqNo::MAX)
        .expect("cardinality");
    assert!(
        narrow.selectivity <= full.selectivity,
        "fallback selectivity monotonic"
    );
}

fn build_fallback(folder: &std::path::Path) -> Tree {
    // Zone map OFF (default), small blocks: exercises the byte-fraction branch.
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
    for i in 0..400u32 {
        tree.insert(key(i), vec![b'v'; 200], 0);
    }
    tree.flush_active_memtable(0).expect("flush");
    tree
}

fn build_memtable_only(folder: &std::path::Path) -> Tree {
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
    for i in 0..100u32 {
        tree.insert(key(i), vec![b'v'; 64], 0);
    }
    tree
}

#[test]
fn all_bound_kinds_in_cardinality() {
    use std::ops::Bound;

    // Run the excluded-lower / included-upper / half-bounded RangeTo / RangeFrom
    // bound arms through every branch: zone-map, byte-fraction fallback, memtable.
    let check = |tree: &Tree, a: u32, b: u32| {
        let excl_incl = tree
            .approximate_range_cardinality(
                (Bound::Excluded(key(a)), Bound::Included(key(b))),
                SeqNo::MAX,
            )
            .expect("cardinality");
        assert!(excl_incl.rows > 0, "excluded..=included is non-empty");
        let to = tree
            .approximate_range_cardinality(..key(b), SeqNo::MAX)
            .expect("cardinality");
        let from = tree
            .approximate_range_cardinality(key(a).., SeqNo::MAX)
            .expect("cardinality");
        assert!(to.rows > 0 && from.rows > 0, "half-bounded is non-empty");
        assert!(to.selectivity <= 1.0 && from.selectivity <= 1.0);
    };

    let f1 = get_tmp_folder();
    check(&build_zonemapped(f1.path()), 300, 700);
    let f2 = get_tmp_folder();
    check(&build_fallback(f2.path()), 50, 150);
    let f3 = get_tmp_folder();
    let mt = build_memtable_only(f3.path());
    check(&mt, 10, 50);
    // A non-empty memtable queried past every key contributes nothing.
    let empty = mt
        .approximate_range_cardinality(key(900)..key(999), SeqNo::MAX)
        .expect("cardinality");
    assert_eq!(empty.rows, 0);
    assert_eq!(empty.selectivity, 0.0);

    // Byte-fraction (no zone map) branch where the upper bound runs past the
    // last key while the table still overlaps: the block-offset lookup for the
    // upper bound falls through to the data-section end.
    let f4 = get_tmp_folder();
    let fb = build_fallback(f4.path());
    let spill = fb
        .approximate_range_cardinality(key(50)..key(99_999), SeqNo::MAX)
        .expect("cardinality");
    assert!(spill.rows > 0, "an in-range lower bound still selects rows");
}

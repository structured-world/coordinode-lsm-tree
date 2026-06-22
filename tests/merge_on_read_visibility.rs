// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! MVCC visibility of merge-on-read deletes, end to end, across read paths and
//! strategies:
//!
//! - a below-watermark delete materialized into the bitmap is hidden on the
//!   point AND range paths (and the segment carries the bitmap, proving block
//!   reuse rather than a re-transpose drop);
//! - an above-watermark delete is NOT materialized (the bitmap stays empty) and
//!   keeps the normal seqno-aware visibility: the row is visible to a snapshot
//!   below the delete seqno and hidden at or above it;
//! - the adaptive strategy purges (copy-on-write, no bitmap) once the deleted
//!   fraction crosses the threshold.

#![cfg(feature = "columnar")]

use lsm_tree::config::{DeleteStrategy, DeleteStrategyPolicy};
use lsm_tree::{
    AbstractTree, AnyTree, Config, Guard, SeqNo, SequenceNumberCounter, UserKey, get_tmp_folder,
};
use test_log::test;

fn key(i: u32) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

fn value(i: u32) -> Vec<u8> {
    format!("value-{i}-payload").into_bytes()
}

fn open_with(folder: &std::path::Path, strategy: DeleteStrategy) -> lsm_tree::Tree {
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
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
        cfg.delete_strategy = DeleteStrategyPolicy::all(strategy);
    })
    .expect("configure columnar delete strategy");
    tree
}

fn bitmap_materialized(tree: &lsm_tree::Tree) -> bool {
    let version = tree.current_version();
    version.iter_tables().any(|t| !t.delete_bitmap().is_empty())
}

#[test]
fn merge_on_read_delete_is_hidden_on_point_and_range_paths() {
    let folder = get_tmp_folder();
    let tree = open_with(folder.path(), DeleteStrategy::MergeOnRead);

    let n = 200u32;
    for i in 0..n {
        tree.insert(key(i), value(i), u64::from(i));
    }
    // Below-watermark range delete of the first 50 keys.
    tree.remove_range(
        UserKey::from(&key(0)[..]),
        UserKey::from(&key(50)[..]),
        1000,
    );
    tree.flush_active_memtable(0).expect("flush");
    tree.major_compact(64 * 1024 * 1024, 5000).expect("compact");

    assert!(
        bitmap_materialized(&tree),
        "merge-on-read must materialize a delete-bitmap (block reuse)",
    );

    // Point path: deleted absent, survivors keep their values.
    for i in 0..n {
        let got = tree.get(key(i), SeqNo::MAX).expect("get");
        if i < 50 {
            assert!(got.is_none(), "point: deleted key {i} must be absent");
        } else {
            assert_eq!(
                &*got.expect("present"),
                value(i).as_slice(),
                "point: key {i}"
            );
        }
    }

    // Range path: the deleted keys never appear; every survivor does, once, with
    // its exact value.
    let mut seen = std::collections::BTreeSet::new();
    for guard in tree.range(key(0)..key(999_999), SeqNo::MAX, None) {
        let (k, v) = guard.into_inner().expect("range item");
        let i: u32 = std::str::from_utf8(&k).unwrap()[1..].parse().unwrap();
        assert!(seen.insert(i), "range: duplicate key index {i} yielded");
        assert!(i >= 50, "range: deleted key index {i} must not be yielded");
        assert_eq!(&*v, value(i).as_slice(), "range: value for key {i}");
    }
    assert_eq!(
        seen.len(),
        (n - 50) as usize,
        "range yields exactly the survivors"
    );
    for i in 50..n {
        assert!(seen.contains(&i), "range: missing survivor key index {i}");
    }
}

#[test]
fn above_watermark_delete_keeps_seqno_visibility_and_no_bitmap() {
    let folder = get_tmp_folder();
    let tree = open_with(folder.path(), DeleteStrategy::MergeOnRead);

    for i in 0..100u32 {
        tree.insert(key(i), value(i), u64::from(i));
    }
    // Delete at seqno 1000, compacted with a watermark (500) BELOW it: the delete
    // is not visible to every live snapshot, so it must NOT materialize.
    tree.remove_range(
        UserKey::from(&key(0)[..]),
        UserKey::from(&key(50)[..]),
        1000,
    );
    tree.flush_active_memtable(0).expect("flush");
    tree.major_compact(64 * 1024 * 1024, 500).expect("compact");

    assert!(
        !bitmap_materialized(&tree),
        "an above-watermark delete must not be materialized into a bitmap",
    );

    // Seqno-aware visibility through the retained range tombstone: visible to a
    // snapshot below the delete seqno, hidden at or above it.
    for i in 0..50u32 {
        assert!(
            tree.get(key(i), 999).expect("get@999").is_some(),
            "key {i} must be visible to a snapshot (999) below the delete seqno",
        );
        assert!(
            tree.get(key(i), 1500).expect("get@1500").is_none(),
            "key {i} must be hidden at a snapshot (1500) above the delete seqno",
        );
    }
    // Survivors are always visible.
    for i in 50..100u32 {
        assert!(tree.get(key(i), SeqNo::MAX).expect("get").is_some());
    }
}

#[test]
fn adaptive_purges_to_copy_on_write_over_threshold() {
    let folder = get_tmp_folder();
    // 5% purge threshold; a 50% delete is far above it, so the next compaction
    // purges (copy-on-write: drop the rows, write no bitmap).
    let tree = open_with(
        folder.path(),
        DeleteStrategy::Adaptive {
            purge_threshold_percent: 5,
        },
    );

    for i in 0..100u32 {
        tree.insert(key(i), value(i), u64::from(i));
    }
    tree.remove_range(
        UserKey::from(&key(0)[..]),
        UserKey::from(&key(50)[..]),
        1000,
    );
    tree.flush_active_memtable(0).expect("flush");
    tree.major_compact(64 * 1024 * 1024, 5000).expect("compact");

    assert!(
        !bitmap_materialized(&tree),
        "over the adaptive threshold the deletes are purged copy-on-write, no bitmap",
    );
    for i in 0..100u32 {
        let got = tree.get(key(i), SeqNo::MAX).expect("get");
        if i < 50 {
            assert!(got.is_none(), "purged key {i} must be absent");
        } else {
            assert!(got.is_some(), "survivor {i} must be present");
        }
    }
}

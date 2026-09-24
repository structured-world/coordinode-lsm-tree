// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! End-to-end round trip for a columnar tree: entries written column-organized
//! on flush are read back exactly through the normal point-read and range
//! paths, and tombstones still hide rows. The reader reconstructs the row
//! entries from each PAX block on load, so the existing read machinery is
//! reused unchanged.

#![cfg(feature = "columnar")]

use lsm_tree::{AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter, get_tmp_folder};
use test_log::test;

fn key(i: u32) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

fn value(i: u32) -> Vec<u8> {
    format!("value-{i}-payload").into_bytes()
}

fn open_columnar(folder: &std::path::Path) -> lsm_tree::Tree {
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
    tree.update_runtime_config(|cfg| cfg.columnar = true)
        .expect("enable columnar");
    tree
}

#[test]
fn columnar_tree_round_trips_through_flush() {
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());
    let n = 500u32;
    for i in 0..n {
        tree.insert(key(i), value(i), 0);
    }
    tree.flush_active_memtable(0).expect("flush");

    // The write path must actually have produced columnar SSTs, else this suite
    // could pass on a row-major regression (the reader is transparent).
    let tables: usize = tree.current_version().iter_tables().count();
    assert!(tables > 0, "expected at least one flushed SST");
    assert!(
        tree.current_version()
            .iter_tables()
            .all(|t| t.metadata.columnar),
        "flushed SSTs must be columnar, not row-major"
    );

    // Every key reads back its exact value through the columnar -> row reader.
    for i in 0..n {
        let got = tree
            .get(key(i), SeqNo::MAX)
            .expect("get")
            .expect("key present");
        assert_eq!(&*got, value(i).as_slice(), "value mismatch for key {i}");
    }

    // A full range scan returns every row.
    let scanned = tree.range(key(0)..key(999_999), SeqNo::MAX, None).count();
    assert_eq!(scanned, n as usize, "range must see every row");
}

#[test]
fn columnar_survives_major_compaction() {
    // Two flushes produce two columnar SSTs; a major compaction merges them
    // (reading columnar blocks through the scan path and re-writing columnar
    // blocks through the compaction writer). Every row must survive.
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());
    for i in 0..300u32 {
        tree.insert(key(i), value(i), 0);
    }
    tree.flush_active_memtable(0).expect("flush");
    for i in 300..600u32 {
        tree.insert(key(i), value(i), 0);
    }
    tree.flush_active_memtable(0).expect("flush");
    tree.major_compact(64 * 1024 * 1024, 0).expect("compact");
    assert!(
        tree.current_version()
            .iter_tables()
            .all(|t| t.metadata.columnar),
        "the compacted SST must also be columnar"
    );

    for i in 0..600u32 {
        let got = tree
            .get(key(i), SeqNo::MAX)
            .expect("get")
            .expect("key present after compaction");
        assert_eq!(
            &*got,
            value(i).as_slice(),
            "value mismatch after compaction for key {i}"
        );
    }
    let scanned = tree.range(key(0)..key(999_999), SeqNo::MAX, None).count();
    assert_eq!(scanned, 600, "range must see every row after compaction");
}

#[test]
fn columnar_groups_of_many_row_pages_survive_major_compaction() {
    // Groups cut into row pages end in a zone block. The compaction reads its
    // inputs group by group off a stream, so a reader that stopped at the last
    // page would take the zone block for the next group's directory and fail
    // or misread every group after the first; every row surviving, and the
    // newer version winning, is what says it did not.
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .columnar_row_group_size_policy(lsm_tree::config::BlockSizePolicy::all(64 * 1_024))
    .columnar_page_size_policy(lsm_tree::config::BlockSizePolicy::all(1_024))
    .open()
    .expect("open");
    let AnyTree::Standard(tree) = any else {
        panic!("expected standard tree");
    };
    tree.update_runtime_config(|cfg| cfg.columnar = true)
        .expect("enable columnar");
    for i in 0..3_000u32 {
        tree.insert(key(i), value(i), 0);
    }
    tree.flush_active_memtable(0).expect("flush");
    for i in (0..3_000u32).step_by(3) {
        tree.insert(key(i), format!("newer-{i}").into_bytes(), 1);
    }
    tree.flush_active_memtable(0).expect("flush");
    tree.major_compact(64 * 1024 * 1024, 2).expect("compact");

    for i in 0..3_000u32 {
        let got = tree
            .get(key(i), SeqNo::MAX)
            .expect("get")
            .expect("key present after compaction");
        let expected = if i % 3 == 0 {
            format!("newer-{i}").into_bytes()
        } else {
            value(i)
        };
        assert_eq!(&*got, expected.as_slice(), "key {i} after compaction");
    }
    let scanned = tree.range(key(0)..key(999_999), SeqNo::MAX, None).count();
    assert_eq!(scanned, 3_000, "range must see every row after compaction");
}

/// Groups of row pages under encryption and Page-ECC: the zone block and the
/// directory go through the same transforms as the pages, so a point read, a
/// predicate scan on the key and one on the value (which reads the zone
/// block), and a compaction all see the rows they would in a plain table.
#[cfg(all(feature = "encryption", feature = "page_ecc"))]
#[test]
fn row_pages_and_their_zones_read_back_under_encryption_and_ecc() {
    use lsm_tree::table::columnar::{COL_USER_KEY, COL_VALUE};
    use lsm_tree::table::columnar_predicate::ColumnRangePredicate;

    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_encryption(Some(std::sync::Arc::new(
        lsm_tree::encryption::Aes256GcmProvider::new(&[7; 32]),
    )))
    .page_ecc(true)
    .columnar_row_group_size_policy(lsm_tree::config::BlockSizePolicy::all(64 * 1_024))
    .columnar_page_size_policy(lsm_tree::config::BlockSizePolicy::all(1_024))
    .open()
    .expect("open");
    let AnyTree::Standard(tree) = any else {
        panic!("expected standard tree");
    };
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
    })
    .expect("enable columnar");
    for i in 0..2_000u32 {
        tree.insert(key(i), value(i), 0);
    }
    tree.flush_active_memtable(0).expect("flush");

    let rows = |predicate: &ColumnRangePredicate| -> u32 {
        tree.columnar_scan(&[COL_USER_KEY, COL_VALUE], Some(predicate), SeqNo::MAX, ..)
            .expect("scan")
            .map(|batch| batch.expect("batch").row_count)
            .sum()
    };
    let check = |label: &str| {
        for i in (0..2_000u32).step_by(97) {
            let got = tree
                .get(key(i), SeqNo::MAX)
                .expect("get")
                .expect("key present");
            assert_eq!(&*got, value(i).as_slice(), "{label}: key {i}");
        }
        let by_key = ColumnRangePredicate {
            column_id: COL_USER_KEY,
            lower: Some(key(700)),
            upper: Some(key(709)),
        };
        assert_eq!(rows(&by_key), 10, "{label}: a key range");
        let by_value = ColumnRangePredicate {
            column_id: COL_VALUE,
            lower: Some(value(1_500)),
            upper: Some(value(1_500)),
        };
        assert_eq!(rows(&by_value), 1, "{label}: one value");
    };
    check("flushed");
    tree.major_compact(64 * 1024 * 1024, 0).expect("compact");
    check("compacted");
}

#[test]
fn columnar_tombstone_hides_row() {
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());
    tree.insert(key(1), value(1), 0);
    tree.flush_active_memtable(0).expect("flush");
    assert!(tree.get(key(1), SeqNo::MAX).expect("get").is_some());

    tree.remove(key(1), 1);
    tree.flush_active_memtable(0).expect("flush");
    assert!(
        tree.get(key(1), SeqNo::MAX).expect("get").is_none(),
        "tombstone must hide the row in a columnar tree"
    );
}

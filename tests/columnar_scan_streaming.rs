// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The projected columnar scan streams: it reads a table as it yields, not
//! before, and a key range reads only the blocks that can hold it. Each test
//! states its claim in the bytes the scan asked of the filesystem, the counter
//! that sees a table read in full whether or not the rows come back.

#![cfg(all(feature = "metrics", feature = "columnar"))]

use lsm_tree::table::columnar::{COL_USER_KEY, COL_VALUE};
use lsm_tree::{
    AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter, Tree, UserKey, get_tmp_folder,
};
use tempfile::TempDir;
use test_log::test;

/// Rows of the fixture: enough row groups that a bounded prefix of the table
/// is a small fraction of it.
const ROWS: u32 = 20_000;

fn key(i: u32) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

/// A columnar tree holding [`ROWS`] unique keys with 256-byte values in one
/// flushed segment, so every scan takes the single-segment path.
fn columnar_segment() -> (TempDir, Tree) {
    let folder = get_tmp_folder();
    let AnyTree::Standard(tree) = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("open") else {
        panic!("expected a standard tree");
    };
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
    })
    .expect("enable columnar");
    for i in 0..ROWS {
        tree.insert(key(i), vec![b'v'; 256], u64::from(i));
    }
    tree.flush_active_memtable(0).expect("flush");
    (folder, tree)
}

/// Bytes a full scan of a fresh [`columnar_segment`] reads, and the rows it
/// returns.
fn full_scan_bytes() -> (u64, u32) {
    let (_folder, tree) = columnar_segment();
    let m = tree.metrics();
    let before = m.bytes_read();
    let mut rows = 0;
    for batch in tree
        .columnar_scan(&[COL_USER_KEY, COL_VALUE], None, SeqNo::MAX, ..)
        .expect("scan")
    {
        rows += batch.expect("batch").row_count;
    }
    (m.bytes_read() - before, rows)
}

#[test]
fn a_narrow_range_reads_only_the_blocks_that_can_hold_it() {
    // A hundred keys inside a table of twenty thousand: the scan seeks to the
    // first block that can hold the lower bound and stops past the upper one,
    // instead of reading the whole table to return a handful of rows.
    let (full, full_rows) = full_scan_bytes();
    assert_eq!(full_rows, ROWS);

    let (_folder, tree) = columnar_segment();
    let m = tree.metrics();
    let before = m.bytes_read();
    let mut rows = 0;
    for batch in tree
        .columnar_scan(
            &[COL_USER_KEY, COL_VALUE],
            None,
            SeqNo::MAX,
            UserKey::from(key(10_000))..UserKey::from(key(10_100)),
        )
        .expect("scan")
    {
        rows += batch.expect("batch").row_count;
    }
    assert_eq!(rows, 100, "the range holds exactly 100 rows");
    let read = m.bytes_read() - before;
    assert!(
        read * 20 < full,
        "a 100-key range read {read} B of a table a full scan reads in {full} B",
    );
}

/// A columnar tree whose two flushed segments interleave their keys, even
/// keys in one and odd in the other, so both form one overlapping group.
fn overlapping_segments() -> (TempDir, Tree) {
    let folder = get_tmp_folder();
    let AnyTree::Standard(tree) = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("open") else {
        panic!("expected a standard tree");
    };
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
    })
    .expect("enable columnar");
    for parity in 0..2 {
        for i in (parity..ROWS).step_by(2) {
            tree.insert(key(i), vec![b'v'; 256], u64::from(i));
        }
        tree.flush_active_memtable(0).expect("flush");
    }
    (folder, tree)
}

#[test]
fn an_overlapping_group_yields_its_first_batch_before_reading_the_group() {
    // The merge advances every source by the rows it needs, so the first batch
    // comes after a bounded prefix of each segment rather than after the whole
    // group was read, joined and sorted; a scan dropped there reads no more.
    let (full, full_rows) = {
        let (_folder, tree) = overlapping_segments();
        let m = tree.metrics();
        let before = m.bytes_read();
        let mut rows = 0;
        for batch in tree
            .columnar_scan(&[COL_USER_KEY, COL_VALUE], None, SeqNo::MAX, ..)
            .expect("scan")
        {
            rows += batch.expect("batch").row_count;
        }
        (m.bytes_read() - before, rows)
    };
    assert_eq!(full_rows, ROWS);

    let (_folder, tree) = overlapping_segments();
    let m = tree.metrics();
    let before = m.bytes_read();
    let mut scan = tree
        .columnar_scan(&[COL_USER_KEY, COL_VALUE], None, SeqNo::MAX, ..)
        .expect("scan");
    let first = scan.next().expect("a first batch").expect("batch");
    assert!(first.row_count > 0);
    let at_first = m.bytes_read() - before;
    drop(scan);

    assert!(
        at_first * 10 < full,
        "the first merged batch came after reading {at_first} B of a {full} B group",
    );
    assert_eq!(m.bytes_read() - before, at_first);
}

#[test]
fn a_wide_overlapping_group_holds_no_more_than_the_scan_budget() {
    // Eight segments share one budget, so each reads its row groups in runs of
    // row pages that fit an eighth of it, rather than a whole group apiece:
    // a wider group takes more, smaller reads instead of more memory.
    const SEGMENTS: u32 = 8;
    const BUDGET: u64 = 64 * 1_024;
    let folder = get_tmp_folder();
    let AnyTree::Standard(tree) = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .columnar_scan_budget(BUDGET)
    .open()
    .expect("open") else {
        panic!("expected a standard tree");
    };
    tree.update_runtime_config(|cfg| cfg.columnar = true)
        .expect("enable columnar");
    for segment in 0..SEGMENTS {
        for i in (segment..ROWS).step_by(SEGMENTS as usize) {
            tree.insert(key(i), vec![b'v'; 256], u64::from(i));
        }
        tree.flush_active_memtable(0).expect("flush");
    }

    let mut scan = tree
        .columnar_scan(&[COL_USER_KEY, COL_VALUE], None, SeqNo::MAX, ..)
        .expect("scan");
    let mut rows = 0;
    for batch in &mut scan {
        rows += batch.expect("batch").row_count;
    }
    assert_eq!(rows, ROWS, "every key once");
    assert_eq!(scan.oversized_reads(), 0, "every row page fits a share");
    assert!(
        scan.peak_payload_bytes() <= BUDGET,
        "the scan held {} B of pages under a {BUDGET} B budget",
        scan.peak_payload_bytes(),
    );
}

#[test]
fn a_row_page_larger_than_its_share_is_read_and_counted() {
    // A budget smaller than one row page cannot be kept: the scan still reads
    // each page, one at a time, returns every row, and counts every read that
    // went past the share instead of failing or stalling.
    const BUDGET: u64 = 1;
    let folder = get_tmp_folder();
    let AnyTree::Standard(tree) = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .columnar_scan_budget(BUDGET)
    .open()
    .expect("open") else {
        panic!("expected a standard tree");
    };
    tree.update_runtime_config(|cfg| cfg.columnar = true)
        .expect("enable columnar");
    for parity in 0..2 {
        for i in (parity..ROWS).step_by(2) {
            tree.insert(key(i), vec![b'v'; 256], u64::from(i));
        }
        tree.flush_active_memtable(0).expect("flush");
    }

    let mut scan = tree
        .columnar_scan(&[COL_USER_KEY, COL_VALUE], None, SeqNo::MAX, ..)
        .expect("scan");
    let mut rows = 0;
    for batch in &mut scan {
        rows += batch.expect("batch").row_count;
    }
    assert_eq!(rows, ROWS, "every key once");
    assert!(
        scan.oversized_reads() > 0,
        "reads past a one-byte share are counted",
    );
}

#[test]
fn a_single_segment_yields_its_first_batch_before_reading_the_rest() {
    // The first batch comes after a bounded prefix of the table, and a scan
    // dropped there reads nothing more: the whole table is not read up front.
    let (full, _) = full_scan_bytes();

    let (_folder, tree) = columnar_segment();
    let m = tree.metrics();
    let before = m.bytes_read();
    let mut scan = tree
        .columnar_scan(&[COL_USER_KEY, COL_VALUE], None, SeqNo::MAX, ..)
        .expect("scan");
    let first = scan.next().expect("a first batch").expect("batch");
    assert!(first.row_count > 0);
    let at_first = m.bytes_read() - before;
    drop(scan);
    let after_drop = m.bytes_read() - before;

    assert!(
        at_first * 10 < full,
        "the first batch came after reading {at_first} B of a {full} B table",
    );
    assert_eq!(
        after_drop, at_first,
        "a scan dropped after its first batch reads nothing more",
    );
}

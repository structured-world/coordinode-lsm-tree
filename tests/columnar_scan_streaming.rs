// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The projected columnar scan streams: it reads a table as it yields, not
//! before, and a key range reads only the blocks that can hold it. Each test
//! states its claim in the bytes the scan asked of the filesystem, the counter
//! that sees a table read in full whether or not the rows come back.

#![cfg(all(feature = "metrics", feature = "columnar"))]

use lsm_tree::table::columnar::{COL_USER_KEY, COL_VALUE};
use lsm_tree::table::columnar_predicate::{ColumnRangePredicate, PredicateApply};
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
fn a_read_past_the_share_is_counted_even_when_its_rows_are_filtered_out() {
    // A pushed-down predicate reads every row page its zones admit and then
    // keeps one row of it. What the read held is the pages, not the one row it
    // returns, so a page larger than the share is a read past it even though
    // the batch it yields is tiny.
    const BUDGET: u64 = 1_024;
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
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
    })
    .expect("enable columnar");
    for i in 0..ROWS {
        tree.insert(key(i), vec![b'v'; 256], u64::from(i));
    }
    tree.flush_active_memtable(0).expect("flush");

    let predicate = ColumnRangePredicate {
        column_id: COL_USER_KEY,
        lower: Some(key(5)),
        upper: Some(key(5)),
        apply: PredicateApply::Filter,
    };
    let mut scan = tree
        .columnar_scan(&[COL_USER_KEY, COL_VALUE], Some(&predicate), SeqNo::MAX, ..)
        .expect("scan");
    let mut rows = 0;
    for batch in &mut scan {
        rows += batch.expect("batch").row_count;
    }
    assert_eq!(rows, 1, "the predicate keeps one row");
    assert!(
        scan.oversized_reads() > 0,
        "the row page read to find that row is larger than the {BUDGET} B share",
    );
}

/// A columnar tree whose row groups and row pages are cut at `size` bytes.
fn columnar_tree_cut_at(size: u32) -> (TempDir, Tree) {
    use lsm_tree::config::BlockSizePolicy;

    let folder = get_tmp_folder();
    let AnyTree::Standard(tree) = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .columnar_row_group_size_policy(BlockSizePolicy::all(size))
    .columnar_page_size_policy(BlockSizePolicy::all(size))
    .open()
    .expect("open") else {
        panic!("expected a standard tree");
    };
    tree.update_runtime_config(|cfg| cfg.columnar = true)
        .expect("enable columnar");
    (folder, tree)
}

#[test]
fn a_merge_over_large_row_pages_cuts_its_output_at_the_target_row_count() {
    // Two segments whose keys interleave, each one row page holding every row:
    // no source's page is spent before the end, so the output is cut by its
    // row count alone, into batches of at most the target.
    const TARGET: u32 = 4_096;
    let (_folder, tree) = columnar_tree_cut_at(1 << 20);
    for parity in 0..2 {
        for i in (parity..ROWS).step_by(2) {
            tree.insert(key(i), [b'v'], u64::from(i));
        }
        tree.flush_active_memtable(0).expect("flush");
    }

    let mut sizes = Vec::new();
    for batch in tree
        .columnar_scan(&[COL_USER_KEY], None, SeqNo::MAX, ..)
        .expect("scan")
    {
        sizes.push(batch.expect("batch").row_count);
    }
    assert_eq!(sizes.iter().sum::<u32>(), ROWS, "every key once");
    assert!(sizes.iter().all(|&n| n <= TARGET), "{sizes:?}");
    assert_eq!(
        sizes.first(),
        Some(&TARGET),
        "the first batch is cut at the target"
    );
}

#[test]
fn a_segment_holding_only_a_range_delete_merges_with_the_rows_it_covers() {
    // A flush of a range delete alone writes a segment holding only the
    // deletion, overlapping the rows it covers: the two merge, and the
    // covered rows are not returned.
    let (_folder, tree) = columnar_tree_cut_at(16 * 1_024);
    for i in 0..100 {
        tree.insert(key(i), vec![b'v'; 16], u64::from(i));
    }
    tree.flush_active_memtable(0).expect("flush");
    tree.remove_range(key(10), key(20), 1_000);
    tree.flush_active_memtable(0).expect("flush");

    let mut keys = Vec::new();
    for batch in tree
        .columnar_scan(&[COL_USER_KEY], None, SeqNo::MAX, ..)
        .expect("scan")
    {
        let batch = batch.expect("batch");
        let column = &batch.columns[0];
        for row in 0..batch.row_count {
            keys.push(bytes_cell(&column.data, batch.row_count, row));
        }
    }
    let expected: Vec<Vec<u8>> = (0..100)
        .filter(|i| !(10..20).contains(i))
        .map(key)
        .collect();
    assert_eq!(keys, expected);
}

/// Row `row` of a bytes column of `rows` rows.
fn bytes_cell(data: &[u8], rows: u32, row: u32) -> Vec<u8> {
    let offset = |i: u32| {
        let at = i as usize * 4;
        u32::from_le_bytes(data[at..at + 4].try_into().expect("offset")) as usize
    };
    let base = (rows as usize + 1) * 4;
    data[base + offset(row)..base + offset(row + 1)].to_vec()
}

/// Every file beneath `dir`, deepest first.
fn files_under(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir).expect("read dir") {
        let path = entry.expect("entry").path();
        if path.is_dir() {
            files.extend(files_under(&path));
        } else {
            files.push(path);
        }
    }
    files
}

#[test]
fn a_page_that_fails_to_read_ends_its_group_with_the_error() {
    // A scan yields the batches read before a damaged page, then that page's
    // error, then nothing more of the group: it neither stops at the first
    // batch nor skips the damage.
    let (folder, tree) = columnar_segment();
    // Reopened after the damage, so the pages come from disk rather than
    // from what the flush left cached.
    drop(tree);
    let largest = files_under(folder.path())
        .into_iter()
        .max_by_key(|p| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0))
        .expect("the table file");
    let mut bytes = std::fs::read(&largest).expect("read table");
    let middle = bytes.len() / 2;
    for byte in &mut bytes[middle..middle + 64] {
        *byte ^= 0xff;
    }
    std::fs::write(&largest, bytes).expect("damage table");
    let AnyTree::Standard(tree) = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("reopen") else {
        panic!("expected a standard tree");
    };

    let mut scan = tree
        .columnar_scan(&[COL_USER_KEY, COL_VALUE], None, SeqNo::MAX, ..)
        .expect("scan");
    let mut rows = 0;
    let mut failed = false;
    for batch in &mut scan {
        match batch {
            Ok(batch) => rows += batch.row_count,
            Err(_) => {
                failed = true;
                break;
            }
        }
    }
    assert!(failed, "the damaged page is reported");
    assert!(rows > 0, "the pages before it were yielded");
    assert!(
        scan.next().is_none(),
        "the failed group yields nothing more"
    );
}

#[test]
fn a_scan_whose_rows_are_all_filtered_still_reports_what_it_held() {
    // A range falling between two keys reads the row group around it and
    // keeps none of its rows. The pages were held all the same, so the peak
    // the scan reports is theirs, not zero.
    let (_folder, tree) = columnar_tree_cut_at(16 * 1_024);
    for i in (0..ROWS).step_by(2) {
        tree.insert(key(i), vec![b'v'; 256], u64::from(i));
    }
    tree.flush_active_memtable(0).expect("flush");

    let lo = UserKey::from(key(1_001));
    let hi = UserKey::from(format!("k{:06}x", 1_001).into_bytes());
    let mut scan = tree
        .columnar_scan(&[COL_USER_KEY, COL_VALUE], None, SeqNo::MAX, lo..hi)
        .expect("scan");
    assert!(scan.next().is_none(), "no key lies in the range");
    assert!(
        scan.peak_payload_bytes() > 0,
        "the row group read to find that out was held"
    );
}

#[test]
fn a_merge_reports_the_batches_it_read_past_even_when_it_kept_none_of_them() {
    // A source whose first row pages hold only rows too new for the snapshot
    // reads them and moves on to the one row it can see. Those pages were
    // held while it looked, so the peak the scan reports is theirs, not the
    // small page it settled on.
    // Wide rows past the page size, each a page of its own; then narrow rows
    // enough to fill pages without them, so the page the visible row lands
    // in holds narrow rows only.
    const WIDE: usize = 8_000;
    let (_folder, tree) = columnar_tree_cut_at(4 * 1_024);
    tree.insert(key(0), [b'a'], 2);
    tree.insert(key(100), [b'a'], 3);
    tree.flush_active_memtable(0).expect("flush");
    for i in 1..20 {
        tree.insert(key(i), vec![b'w'; WIDE], 1_000 + u64::from(i));
    }
    for i in 20..99 {
        tree.insert(key(i), vec![b'n'; 100], 1_000 + u64::from(i));
    }
    tree.insert(key(99), [b'n'], 1);
    tree.flush_active_memtable(0).expect("flush");

    let mut scan = tree
        .columnar_scan(&[COL_USER_KEY, COL_VALUE], None, 500, ..)
        .expect("scan");
    let mut rows = 0;
    for batch in &mut scan {
        rows += batch.expect("batch").row_count;
    }
    assert_eq!(rows, 3, "the three rows the snapshot sees");
    assert!(
        scan.peak_payload_bytes() >= WIDE as u64,
        "the wide pages read past were held: peak {} B",
        scan.peak_payload_bytes(),
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

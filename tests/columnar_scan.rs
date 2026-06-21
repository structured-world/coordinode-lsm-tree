// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Vectorized columnar scan: projection decodes only the requested columns, and
//! a key-range predicate filters to exactly the rows a naive row scan would
//! keep, skipping out-of-range blocks via the zone-map.

#![cfg(feature = "columnar")]

use lsm_tree::table::columnar::{
    COL_SEQNO, COL_USER_KEY, COL_VALUE, COL_VALUE_TYPE, column_batch_to_entries,
};
use lsm_tree::table::columnar_predicate::ColumnRangePredicate;
use lsm_tree::{AbstractTree, AnyTree, Config, SequenceNumberCounter, get_tmp_folder};
use test_log::test;

fn key(i: u32) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

/// Opens a standard tree with the columnar layout and zone-map both enabled, so
/// flushed SSTs are column-organized and carry the per-block key range used for
/// block skipping.
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
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
    })
    .expect("enable columnar + zone-map");
    tree
}

#[test]
fn columnar_scan_projects_only_the_requested_columns() {
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());
    let n = 4000u32; // enough rows to span several data blocks
    for i in 0..n {
        tree.insert(key(i), vec![b'v'; 80], 0);
    }
    tree.flush_active_memtable(0).expect("flush");

    let version = tree.current_version();
    let table = version.iter_tables().next().expect("one flushed SST");

    // Project only the user-key column: every returned batch must carry that
    // column alone, proving the value column was never decoded.
    let batches = table
        .columnar_scan(&[COL_USER_KEY], None)
        .expect("columnar scan");
    assert!(batches.len() > 1, "test wants a multi-block SST");
    for batch in &batches {
        assert!(
            batch.columns.iter().all(|c| c.column_id == COL_USER_KEY),
            "a key-only projection must not decode any other column"
        );
    }
    let total: usize = batches.iter().map(|b| b.row_count as usize).sum();
    assert_eq!(total, n as usize, "projection must still see every row");
}

#[test]
fn columnar_scan_predicate_equals_a_naive_filter() {
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());
    let n = 4000u32;
    for i in 0..n {
        tree.insert(key(i), vec![b'v'; 80], 0);
    }
    tree.flush_active_memtable(0).expect("flush");

    let version = tree.current_version();
    let table = version.iter_tables().next().expect("one flushed SST");

    // Keys in [k001000, k001999]: a contiguous middle slice that lets the
    // zone-map skip the blocks entirely below or above it.
    let lo = key(1000);
    let hi = key(1999);
    let pred = ColumnRangePredicate {
        column_id: COL_USER_KEY,
        lower: Some(lo.clone()),
        upper: Some(hi.clone()),
    };
    let all = [COL_USER_KEY, COL_SEQNO, COL_VALUE_TYPE, COL_VALUE];

    let batches = table
        .columnar_scan(&all, Some(&pred))
        .expect("columnar scan with predicate");

    // Flatten the surviving rows back to keys, in scan order.
    let mut got: Vec<Vec<u8>> = Vec::new();
    for batch in &batches {
        for entry in column_batch_to_entries(batch).expect("untranspose") {
            got.push(entry.key.user_key.to_vec());
        }
    }

    // A naive row scan filtered by the same bounds.
    let expected: Vec<Vec<u8>> = (1000..=1999u32).map(key).collect();
    assert_eq!(got, expected, "predicate scan must equal the naive filter");
}

#[test]
fn columnar_scan_predicate_on_an_unprojected_column_still_filters() {
    // Project only the value column, but filter on the (unprojected) key column.
    // The predicate must still apply, not be silently bypassed, and the output
    // must carry only the projected value column.
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());
    let n = 4000u32;
    for i in 0..n {
        tree.insert(key(i), vec![b'v'; 80], 0);
    }
    tree.flush_active_memtable(0).expect("flush");

    let version = tree.current_version();
    let table = version.iter_tables().next().expect("one flushed SST");

    let pred = ColumnRangePredicate {
        column_id: COL_USER_KEY,
        lower: Some(key(1000)),
        upper: Some(key(1999)),
    };
    let batches = table
        .columnar_scan(&[COL_VALUE], Some(&pred))
        .expect("columnar scan");

    let total: usize = batches.iter().map(|b| b.row_count as usize).sum();
    assert_eq!(
        total, 1000,
        "a predicate on an unprojected column must still filter the rows"
    );
    for batch in &batches {
        assert!(
            batch.columns.iter().all(|c| c.column_id == COL_VALUE),
            "the output must carry only the projected value column"
        );
    }
}

#[test]
fn columnar_scan_errors_on_a_non_columnar_sst() {
    // A tree without the columnar layout flushes a row-major SST; columnar_scan
    // must reject it rather than misread row blocks as column batches.
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
    tree.insert(key(0), vec![b'v'; 8], 0);
    tree.flush_active_memtable(0).expect("flush");

    let version = tree.current_version();
    let table = version.iter_tables().next().expect("one flushed SST");
    assert!(
        table.columnar_scan(&[COL_USER_KEY], None).is_err(),
        "scanning a row-major SST as columnar must error"
    );
}

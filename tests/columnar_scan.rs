// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Vectorized columnar scan: projection decodes only the requested columns, and
//! a key-range predicate filters to exactly the rows a naive row scan would
//! keep, skipping out-of-range blocks via the zone-map.

#![cfg(feature = "columnar")]

use lsm_tree::config::{DeleteStrategy, DeleteStrategyPolicy};
use lsm_tree::table::columnar::{
    COL_SEQNO, COL_USER_KEY, COL_VALUE, COL_VALUE_TYPE, Column, ColumnBatch, TypeTag,
    column_batch_to_entries, entries_to_column_batch,
};
use lsm_tree::table::columnar_predicate::ColumnRangePredicate;
use lsm_tree::{
    AbstractTree, AnyTree, Config, Error, InternalValue, SeqNo, SequenceNumberCounter, UserKey,
    ValueType, get_tmp_folder,
};
use test_log::test;

fn key(i: u32) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

/// Builds a consumer columnar batch whose value is one fixed-4 sub-column
/// (id 3, holding `a` little-endian). Per-row seqnos are 0, so the ingestion
/// assigns the atomic global sequence number. `rows` must be sorted by key.
fn subcol_batch(rows: &[(Vec<u8>, u32)]) -> ColumnBatch {
    let entries: Vec<InternalValue> = rows
        .iter()
        .map(|(k, _)| InternalValue::from_components(k.clone(), b"ignored", 0, ValueType::Value))
        .collect();
    let mut batch = entries_to_column_batch(&entries).expect("transpose");
    // Replace the single opaque value column with one fixed-4 sub-column (id 3).
    batch.columns.pop();
    let mut data = Vec::with_capacity(rows.len() * 4);
    for (_, a) in rows {
        data.extend_from_slice(&a.to_le_bytes());
    }
    batch.columns.push(Column {
        column_id: 3,
        type_tag: TypeTag::Fixed(4),
        validity: None,
        data,
    });
    batch
}

/// Opens a columnar standard tree and ingests `rows` as one new SST, returning
/// the tree (wrapped in [`AnyTree`]). Each call creates a separate segment with
/// its own `global_seqno`.
fn ingest_segment(any: &AnyTree, rows: &[(Vec<u8>, u32)]) {
    let mut ingest = any.ingestion().expect("ingestion");
    ingest
        .write_columnar_batch(&subcol_batch(rows))
        .expect("write batch");
    ingest.finish().expect("finish");
}

/// Opens an empty columnar tree (columnar + zone-map enabled).
fn open_columnar_any(folder: &std::path::Path) -> AnyTree {
    let any = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("open");
    let AnyTree::Standard(tree) = &any else {
        panic!("expected standard tree");
    };
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
    })
    .expect("enable columnar + zone-map");
    any
}

/// Flattens a tree-level columnar scan into `(key, sub-column-3 value)` pairs in
/// yield order, asserting every batch carries exactly `expect_columns`.
fn scan_to_pairs(
    tree: &lsm_tree::Tree,
    projection: &[u16],
    predicate: Option<&ColumnRangePredicate>,
    seqno: SeqNo,
    expect_columns: &[u16],
) -> Vec<(Vec<u8>, u32)> {
    let mut out = Vec::new();
    for batch in tree
        .columnar_scan(projection, predicate, seqno, ..)
        .expect("scan")
    {
        let batch = batch.expect("batch");
        let mut ids: Vec<u16> = batch.columns.iter().map(|c| c.column_id).collect();
        ids.sort_unstable();
        let mut want = expect_columns.to_vec();
        want.sort_unstable();
        assert_eq!(
            ids, want,
            "each batch carries exactly the projected columns"
        );
        let key_col = batch
            .columns
            .iter()
            .find(|c| c.column_id == COL_USER_KEY)
            .expect("key column projected for this assertion");
        let val_col = batch
            .columns
            .iter()
            .find(|c| c.column_id == 3)
            .expect("value sub-column projected for this assertion");
        // The key column is a Bytes column: rebuild its (row+1) offset table view.
        let rows = batch.row_count as usize;
        let off = |i: usize| {
            let b: [u8; 4] = key_col.data[i * 4..i * 4 + 4].try_into().unwrap();
            u32::from_le_bytes(b) as usize
        };
        let payload = &key_col.data[(rows + 1) * 4..];
        for i in 0..rows {
            let k = payload[off(i)..off(i + 1)].to_vec();
            let a = u32::from_le_bytes(val_col.data[i * 4..i * 4 + 4].try_into().unwrap());
            out.push((k, a));
        }
    }
    out
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

// ---------------------------------------------------------------------------
// Tree-level projected columnar scan (#566): lifts the per-SST scan across the
// whole tree, owning segment selection, MVCC visibility, delete-masking, and
// cross-segment ordering / newest-wins merge.
// ---------------------------------------------------------------------------

fn standard(any: &AnyTree) -> &lsm_tree::Tree {
    match any {
        AnyTree::Standard(t) => t,
        AnyTree::Blob(_) => panic!("expected standard tree"),
    }
}

#[test]
fn tree_columnar_scan_streams_disjoint_segments_in_key_order() {
    // Two ingested segments with disjoint key ranges (the append-only common
    // case) stream verbatim and concatenate in global key order.
    let folder = get_tmp_folder();
    let any = open_columnar_any(folder.path());
    ingest_segment(&any, &[(key(0), 10), (key(1), 11)]);
    ingest_segment(&any, &[(key(2), 12), (key(3), 13)]);

    let got = scan_to_pairs(
        standard(&any),
        &[COL_USER_KEY, 3],
        None,
        SeqNo::MAX,
        &[COL_USER_KEY, 3],
    );
    let expected: Vec<(Vec<u8>, u32)> =
        vec![(key(0), 10), (key(1), 11), (key(2), 12), (key(3), 13)];
    assert_eq!(
        got, expected,
        "disjoint segments yield every row in key order"
    );
}

#[test]
fn tree_columnar_scan_overlapping_segments_keep_newest() {
    // The same key is written in an older then a newer segment (an overwrite).
    // The scan must return one row per key — the newest version — not duplicates.
    let folder = get_tmp_folder();
    let any = open_columnar_any(folder.path());
    ingest_segment(&any, &[(key(0), 100), (key(1), 101), (key(2), 102)]);
    // Newer segment overwrites k1, adds k3; overlaps the first segment's range.
    ingest_segment(&any, &[(key(1), 201), (key(3), 203)]);

    let got = scan_to_pairs(
        standard(&any),
        &[COL_USER_KEY, 3],
        None,
        SeqNo::MAX,
        &[COL_USER_KEY, 3],
    );
    let expected: Vec<(Vec<u8>, u32)> = vec![
        (key(0), 100),
        (key(1), 201), // newest wins over 101
        (key(2), 102),
        (key(3), 203),
    ];
    assert_eq!(
        got, expected,
        "overlapping segments merge newest-seqno-wins, no duplicate keys"
    );
}

#[test]
fn tree_columnar_scan_projection_decodes_only_requested_columns() {
    // Projecting only sub-column 3 across multiple segments yields batches that
    // carry that column alone — the intrinsic value / key columns are not decoded.
    let folder = get_tmp_folder();
    let any = open_columnar_any(folder.path());
    ingest_segment(&any, &[(key(0), 10), (key(1), 11)]);
    ingest_segment(&any, &[(key(2), 12)]);

    let tree = standard(&any);
    let mut rows = 0u32;
    for batch in tree
        .columnar_scan(&[3], None, SeqNo::MAX, ..)
        .expect("scan")
    {
        let batch = batch.expect("batch");
        assert!(
            batch.columns.iter().all(|c| c.column_id == 3),
            "a sub-column-3 projection must not decode any other column"
        );
        rows += batch.row_count;
    }
    assert_eq!(rows, 3, "projection still sees every row across segments");
}

#[test]
fn tree_columnar_scan_predicate_filters_across_segments() {
    // A key-range predicate prunes rows across two disjoint segments, leaving the
    // contiguous middle slice.
    let folder = get_tmp_folder();
    let any = open_columnar_any(folder.path());
    ingest_segment(&any, &[(key(0), 0), (key(1), 1), (key(2), 2)]);
    ingest_segment(&any, &[(key(3), 3), (key(4), 4), (key(5), 5)]);

    let pred = ColumnRangePredicate {
        column_id: COL_USER_KEY,
        lower: Some(key(1)),
        upper: Some(key(4)),
    };
    let got = scan_to_pairs(
        standard(&any),
        &[COL_USER_KEY, 3],
        Some(&pred),
        SeqNo::MAX,
        &[COL_USER_KEY, 3],
    );
    let expected: Vec<(Vec<u8>, u32)> = vec![(key(1), 1), (key(2), 2), (key(3), 3), (key(4), 4)];
    assert_eq!(got, expected, "predicate filters rows across segments");
}

#[test]
fn tree_columnar_scan_snapshot_excludes_newer_segment() {
    // MVCC visibility is segment-granular for uniform-seqno ingested segments: a
    // snapshot at the newer segment's base sees the older segment only.
    let folder = get_tmp_folder();
    let any = open_columnar_any(folder.path());
    ingest_segment(&any, &[(key(0), 10), (key(1), 11)]);
    ingest_segment(&any, &[(key(2), 12), (key(3), 13)]);

    let tree = standard(&any);
    let version = tree.current_version();
    let mut bases: Vec<SeqNo> = version.iter_tables().map(|t| t.global_seqno()).collect();
    bases.sort_unstable();
    let (older, newer) = (bases[0], bases[1]);
    assert!(older < newer, "two ingestions get increasing seqno bases");

    // Snapshot == newer base: the newer segment (base == snapshot) is excluded by
    // the exclusive rule; the older segment (base < snapshot) is visible.
    let got = scan_to_pairs(tree, &[COL_USER_KEY, 3], None, newer, &[COL_USER_KEY, 3]);
    assert_eq!(
        got,
        vec![(key(0), 10), (key(1), 11)],
        "a snapshot at the newer base sees only the older segment"
    );

    // A snapshot at or below the oldest base sees nothing.
    assert!(
        tree.columnar_scan(&[COL_USER_KEY, 3], None, older, ..)
            .expect("scan")
            .next()
            .is_none(),
        "a snapshot at the oldest base sees no segment"
    );
}

#[test]
fn tree_columnar_scan_full_row_reconstruction_unaffected() {
    // The row path (get) still reconstructs whole rows after the same data is
    // scanned column-wise — columnar_scan does not perturb full-row reads.
    use lsm_tree::table::columnar::unframe_value_cells;

    let folder = get_tmp_folder();
    let any = open_columnar_any(folder.path());
    ingest_segment(&any, &[(key(0), 7), (key(1), 8)]);

    // Column scan sees the sub-column values.
    let got = scan_to_pairs(
        standard(&any),
        &[COL_USER_KEY, 3],
        None,
        SeqNo::MAX,
        &[COL_USER_KEY, 3],
    );
    assert_eq!(got, vec![(key(0), 7), (key(1), 8)]);

    // Full-row get reconstructs each row's framed value (one fixed-4 sub-column).
    let tags = [TypeTag::Fixed(4)];
    let v0 = any.get(key(0), SeqNo::MAX).expect("get").expect("k0");
    assert_eq!(
        unframe_value_cells(v0.as_ref(), &tags).expect("unframe"),
        vec![&7u32.to_le_bytes()[..]],
    );
    let v1 = any.get(key(1), SeqNo::MAX).expect("get").expect("k1");
    assert_eq!(
        unframe_value_cells(v1.as_ref(), &tags).expect("unframe"),
        vec![&8u32.to_le_bytes()[..]],
    );
}

#[test]
fn tree_columnar_scan_drops_internal_columns_when_unprojected() {
    // When the caller does not project the key column, the overlap-merge path
    // still decodes it internally but must drop it from the output, leaving
    // exactly the projected value sub-column.
    let folder = get_tmp_folder();
    let any = open_columnar_any(folder.path());
    ingest_segment(&any, &[(key(0), 100), (key(1), 101)]);
    ingest_segment(&any, &[(key(1), 201)]); // overlap forces the merge path

    let tree = standard(&any);
    let mut values: Vec<u32> = Vec::new();
    for batch in tree
        .columnar_scan(&[3], None, SeqNo::MAX, ..)
        .expect("scan")
    {
        let batch = batch.expect("batch");
        assert!(
            batch.columns.iter().all(|c| c.column_id == 3),
            "merge output must carry only the projected value column"
        );
        let col = &batch.columns[0];
        for i in 0..batch.row_count as usize {
            values.push(u32::from_le_bytes(
                col.data[i * 4..i * 4 + 4].try_into().unwrap(),
            ));
        }
    }
    assert_eq!(
        values,
        vec![100, 201],
        "merge keeps newest, drops key column"
    );
}

/// Flattens a tree-level columnar scan projecting only the key column into keys.
fn scan_keys(tree: &lsm_tree::Tree, seqno: SeqNo) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    for batch in tree
        .columnar_scan(&[COL_USER_KEY], None, seqno, ..)
        .expect("scan")
    {
        let batch = batch.expect("batch");
        assert!(
            batch.columns.iter().all(|c| c.column_id == COL_USER_KEY),
            "key-only projection drops the internally-decoded seqno column"
        );
        let key_col = &batch.columns[0];
        let rows = batch.row_count as usize;
        let off = |i: usize| {
            let b: [u8; 4] = key_col.data[i * 4..i * 4 + 4].try_into().unwrap();
            u32::from_le_bytes(b) as usize
        };
        let payload = &key_col.data[(rows + 1) * 4..];
        for i in 0..rows {
            out.push(payload[off(i)..off(i + 1)].to_vec());
        }
    }
    out
}

#[test]
fn tree_columnar_scan_masks_per_row_seqno_when_snapshot_straddles_segment() {
    // A flush-produced columnar segment carries per-row seqnos, so a snapshot can
    // straddle it. The scan must drop rows whose seqno is not visible (the
    // partial-visibility path), not return the whole segment.
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());
    tree.insert(key(0), b"a".to_vec(), 1);
    tree.insert(key(1), b"b".to_vec(), 2);
    tree.insert(key(2), b"c".to_vec(), 3);
    tree.flush_active_memtable(0).expect("flush");

    // Snapshot 2: only rows with seqno < 2 are visible (k0 @ seqno 1).
    assert_eq!(
        scan_keys(&tree, 2),
        vec![key(0)],
        "partial visibility keeps only rows below the snapshot"
    );
    // Snapshot MAX: every row is visible.
    assert_eq!(scan_keys(&tree, SeqNo::MAX), vec![key(0), key(1), key(2)]);
}

/// Reads a bytes column's row values (offset-table layout: `(rows+1)` u32
/// offsets, then the payload).
fn bytes_rows(col: &Column, rows: usize) -> Vec<Vec<u8>> {
    let off = |i: usize| {
        let b: [u8; 4] = col.data[i * 4..i * 4 + 4].try_into().unwrap();
        u32::from_le_bytes(b) as usize
    };
    let payload = &col.data[(rows + 1) * 4..];
    (0..rows)
        .map(|i| payload[off(i)..off(i + 1)].to_vec())
        .collect()
}

/// Two overlapping segments can hold DIFFERENT values for one key at one
/// caller-assigned seqno; the tree serves the NEWER run's value. The merge
/// path must break the tie by source recency — `group_by_overlap` sorts
/// segments by minimum key, so combined order alone can put the OLDER
/// segment's row first and the stable sort would keep it.
#[test]
fn tree_columnar_scan_breaks_equal_seqno_ties_by_source_recency() {
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());

    // OLDER segment spans [a, c] (its lower minimum key sorts it first).
    tree.insert(key(0), b"za".to_vec(), 1);
    tree.insert(key(2), b"old".to_vec(), 5);
    tree.flush_active_memtable(0).expect("flush");
    // NEWER segment spans [b, c] with a DIFFERENT value at the same seqno.
    tree.insert(key(1), b"zb".to_vec(), 2);
    tree.insert(key(2), b"new".to_vec(), 5);
    tree.flush_active_memtable(0).expect("flush");

    // Ground truth: the read path serves the newer run's tied value.
    assert_eq!(
        tree.get(key(2), SeqNo::MAX).expect("get").as_deref(),
        Some(b"new".as_ref()),
        "the tree itself serves the newer source at a seqno tie",
    );

    let mut got: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for batch in tree
        .columnar_scan(&[COL_USER_KEY, COL_VALUE], None, SeqNo::MAX, ..)
        .expect("scan")
    {
        let batch = batch.expect("batch");
        let rows = batch.row_count as usize;
        let key_col = batch
            .columns
            .iter()
            .find(|c| c.column_id == COL_USER_KEY)
            .expect("key column");
        let val_col = batch
            .columns
            .iter()
            .find(|c| c.column_id == COL_VALUE)
            .expect("value column");
        got.extend(
            bytes_rows(key_col, rows)
                .into_iter()
                .zip(bytes_rows(val_col, rows)),
        );
    }
    assert_eq!(
        got,
        vec![
            (key(0), b"za".to_vec()),
            (key(1), b"zb".to_vec()),
            (key(2), b"new".to_vec()),
        ],
        "the columnar merge must agree with the read path at a seqno tie",
    );
}

#[test]
fn tree_columnar_scan_applies_an_unmaterialized_range_tombstone() {
    // Rows inserted, then `remove_range`, then flushed: the columnar segment
    // carries the range tombstone in its RT section (no positional delete
    // bitmap exists until a later relocation). The scan must suppress the
    // covered keys exactly as the point and ordinary range reads do.
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());
    tree.insert(key(0), b"a".to_vec(), 1);
    tree.insert(key(1), b"b".to_vec(), 2);
    tree.insert(key(2), b"c".to_vec(), 3);
    tree.remove_range(key(1), key(2), 4); // deletes k1 (half-open span)
    tree.flush_active_memtable(0).expect("flush");

    assert_eq!(
        scan_keys(&tree, SeqNo::MAX),
        vec![key(0), key(2)],
        "a range-tombstone-covered key must not surface from the columnar scan"
    );
    // Below the deletion the covered key is still visible.
    assert_eq!(
        scan_keys(&tree, 4),
        vec![key(0), key(1), key(2)],
        "a snapshot below the range deletion still sees the covered key"
    );
}

#[test]
fn tree_columnar_scan_singleton_segment_dedups_overwritten_key() {
    // A flush-produced columnar segment holds every MVCC version of an
    // overwritten key. A SINGLETON segment (no overlapping neighbor) must still
    // return one row per key — the newest visible version — exactly like the
    // overlapping-merge path does.
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());
    tree.insert(key(0), b"old".to_vec(), 1);
    tree.insert(key(1), b"only".to_vec(), 2);
    tree.insert(key(0), b"new".to_vec(), 3); // overwrite k0
    tree.flush_active_memtable(0).expect("flush");

    // All-visible scan: each key exactly once, k0 at its newest value.
    let mut got: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for batch in tree
        .columnar_scan(&[COL_USER_KEY, COL_VALUE], None, SeqNo::MAX, ..)
        .expect("scan")
    {
        let batch = batch.expect("batch");
        let rows = batch.row_count as usize;
        let key_col = batch
            .columns
            .iter()
            .find(|c| c.column_id == COL_USER_KEY)
            .expect("key column");
        let val_col = batch
            .columns
            .iter()
            .find(|c| c.column_id == COL_VALUE)
            .expect("value column");
        got.extend(
            bytes_rows(key_col, rows)
                .into_iter()
                .zip(bytes_rows(val_col, rows)),
        );
    }
    assert_eq!(
        got,
        vec![(key(0), b"new".to_vec()), (key(1), b"only".to_vec()),],
        "singleton segment dedups an overwritten key to its newest version"
    );

    // A snapshot straddling the segment sees k0's OLD version (the newest one
    // visible below the snapshot) — once, not zero or two times.
    assert_eq!(
        scan_keys(&tree, 2),
        vec![key(0)],
        "straddling snapshot keeps the newest VISIBLE version, once"
    );
}

#[test]
fn tree_columnar_scan_singleton_predicate_runs_after_dedup() {
    // The newest version of k0 fails the predicate while its shadowed older
    // version matches: the key must be dropped, not resurrected through the
    // older matching version. Mirrors the overlapping-merge path's
    // predicate-after-dedup ordering.
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());
    tree.insert(key(0), b"match".to_vec(), 1);
    tree.insert(key(1), b"match".to_vec(), 2);
    tree.insert(key(0), b"zzz-miss".to_vec(), 3); // newest k0 fails the predicate
    tree.flush_active_memtable(0).expect("flush");

    let pred = ColumnRangePredicate {
        column_id: COL_VALUE,
        lower: Some(b"match".to_vec()),
        upper: Some(b"match".to_vec()),
    };
    let mut got: Vec<Vec<u8>> = Vec::new();
    for batch in tree
        .columnar_scan(&[COL_USER_KEY], Some(&pred), SeqNo::MAX, ..)
        .expect("scan")
    {
        let batch = batch.expect("batch");
        let rows = batch.row_count as usize;
        let key_col = batch
            .columns
            .iter()
            .find(|c| c.column_id == COL_USER_KEY)
            .expect("key column");
        got.extend(bytes_rows(key_col, rows));
    }
    assert_eq!(
        got,
        vec![key(1)],
        "a key whose newest version fails the predicate is dropped, not \
         served from a shadowed older matching version"
    );
}

#[test]
fn tree_columnar_scan_applies_delete_bitmap_masking() {
    // A columnar segment carrying a positional delete-bitmap (built by relocating
    // a range tombstone under the Adaptive merge-on-read strategy) must have its
    // deleted rows masked out by the tree-level scan.
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("open");
    let AnyTree::Standard(tree) = &any else {
        panic!("expected standard tree");
    };
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
        // High purge threshold: the delete relocates into a bitmap (masked),
        // not a physical purge, so the segment carries a delete-bitmap.
        cfg.delete_strategy = DeleteStrategyPolicy::all(DeleteStrategy::Adaptive {
            purge_threshold_percent: 90,
        });
    })
    .expect("enable columnar adaptive");

    for i in 0..10u32 {
        tree.insert(key(i), vec![b'v'; 16], u64::from(i) + 1);
    }
    tree.remove_range(UserKey::from(&key(0)[..]), UserKey::from(&key(4)[..]), 1000);
    tree.flush_active_memtable(0).expect("flush");
    tree.major_compact(64 * 1024 * 1024, 5000)
        .expect("relocate");

    {
        let version = tree.current_version();
        let tables: Vec<_> = version.iter_tables().collect();
        assert_eq!(tables.len(), 1, "one relocated segment");
        assert!(
            tables[0].delete_density().is_some(),
            "segment must carry a delete-bitmap for this test to be meaningful",
        );
    }

    // Scan the whole tree: the deleted keys [0,4) must be masked out.
    let got = scan_keys(tree, SeqNo::MAX);
    let expected: Vec<Vec<u8>> = (4..10u32).map(key).collect();
    assert_eq!(
        got, expected,
        "tree-level scan masks the segment's positional deletes"
    );
}

#[test]
fn tree_columnar_scan_errors_on_mixed_mode_tree() {
    // If a non-columnar segment overlaps the range (a mixed-mode tree), the scan
    // must reject the request rather than silently skip that segment's data.
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("open");
    let AnyTree::Standard(tree) = &any else {
        panic!("expected standard tree");
    };

    // A row-major segment (columnar disabled) overlapping the scan range.
    tree.insert(key(0), vec![b'v'; 8], 0);
    tree.flush_active_memtable(0).expect("flush row segment");
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
    })
    .expect("enable columnar");
    ingest_segment(&any, &[(key(1), 11)]);

    assert!(
        matches!(
            tree.columnar_scan(&[3], None, SeqNo::MAX, ..),
            Err(Error::FeatureUnsupported(_))
        ),
        "a non-columnar segment overlapping the range must be rejected"
    );
}

#[test]
fn tree_columnar_scan_empty_when_range_misses_every_segment() {
    // An empty tree, and a range below all data, both yield no batches.
    let folder = get_tmp_folder();
    let any = open_columnar_any(folder.path());

    assert!(
        standard(&any)
            .columnar_scan(&[3], None, SeqNo::MAX, ..)
            .expect("scan")
            .next()
            .is_none(),
        "an empty tree yields no batches"
    );

    ingest_segment(&any, &[(key(5), 50), (key(6), 60)]);
    let upper = key(2);
    let got: Vec<_> = standard(&any)
        .columnar_scan(
            &[COL_USER_KEY, 3],
            None,
            SeqNo::MAX,
            ..UserKey::from(&upper[..]),
        )
        .expect("scan")
        .collect();
    assert!(
        got.is_empty(),
        "a range that misses every segment yields no batches"
    );
}

#[test]
fn tree_columnar_scan_predicate_on_unprojected_column_still_filters() {
    // Filter on the key column but project only the value sub-column: the
    // predicate must still apply across segments, and the output carries only the
    // projected column.
    let folder = get_tmp_folder();
    let any = open_columnar_any(folder.path());
    ingest_segment(&any, &[(key(0), 0), (key(1), 1), (key(2), 2)]);
    ingest_segment(&any, &[(key(3), 3), (key(4), 4)]);

    let pred = ColumnRangePredicate {
        column_id: COL_USER_KEY,
        lower: Some(key(1)),
        upper: Some(key(3)),
    };
    let tree = standard(&any);
    let mut rows = 0u32;
    for batch in tree
        .columnar_scan(&[3], Some(&pred), SeqNo::MAX, ..)
        .expect("scan")
    {
        let batch = batch.expect("batch");
        assert!(
            batch.columns.iter().all(|c| c.column_id == 3),
            "output carries only the projected value column"
        );
        rows += batch.row_count;
    }
    assert_eq!(
        rows, 3,
        "predicate on the unprojected key column still filters"
    );
}

// EXPLAIN ANALYZE at a consumer reads per-scan block-decode counts by diffing
// the standard `metrics` block-load counters around the scan: the columnar scan
// participates in that accounting, and zone-map-skipped blocks are never loaded,
// so the delta is exactly the blocks decoded (pushdown effectiveness). No
// columnar-specific stats API is needed.
#[cfg(feature = "metrics")]
#[test]
fn tree_columnar_scan_block_decode_count_drops_with_predicate() {
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());
    let n = 4000u32; // spans several data blocks
    for i in 0..n {
        tree.insert(key(i), vec![b'v'; 80], 0);
    }
    tree.flush_active_memtable(0).expect("flush");

    let metrics = tree.metrics();

    // Full scan: count the data blocks decoded.
    let before = metrics.data_block_load_count();
    for batch in tree
        .columnar_scan(&[COL_USER_KEY], None, SeqNo::MAX, ..)
        .expect("scan")
    {
        batch.expect("batch");
    }
    let full = metrics.data_block_load_count() - before;

    // Narrow predicate: the zone-map prunes the blocks outside [k001000, k001099],
    // so strictly fewer data blocks are decoded.
    let pred = ColumnRangePredicate {
        column_id: COL_USER_KEY,
        lower: Some(key(1000)),
        upper: Some(key(1099)),
    };
    let before = metrics.data_block_load_count();
    for batch in tree
        .columnar_scan(&[COL_USER_KEY], Some(&pred), SeqNo::MAX, ..)
        .expect("scan")
    {
        batch.expect("batch");
    }
    let pruned = metrics.data_block_load_count() - before;

    assert!(full > 1, "test wants a multi-block segment, got {full}");
    assert!(
        pruned < full,
        "zone-map predicate must skip blocks (decoded {pruned} with predicate vs {full} without)"
    );
}

#[test]
fn tree_columnar_scan_blob_tree_unsupported() {
    // Columnar scan is a standard-tree feature; a blob tree must reject it.
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(Default::default()))
    .open()
    .expect("open blob tree");
    assert!(
        matches!(
            any.columnar_scan(&[3], None, SeqNo::MAX, ..),
            Err(lsm_tree::Error::FeatureUnsupported(_))
        ),
        "columnar scan over a blob tree must be rejected"
    );
}

/// Collects `(key, sub-column-3)` pairs from a bounded-range tree scan, asserting
/// each batch carries exactly the key + value-3 columns.
fn scan_range_pairs<R: std::ops::RangeBounds<UserKey>>(
    tree: &lsm_tree::Tree,
    range: R,
) -> Vec<(Vec<u8>, u32)> {
    let mut out = Vec::new();
    for batch in tree
        .columnar_scan(&[COL_USER_KEY, 3], None, SeqNo::MAX, range)
        .expect("scan")
    {
        let batch = batch.expect("batch");
        let key_col = batch
            .columns
            .iter()
            .find(|c| c.column_id == COL_USER_KEY)
            .expect("key column");
        let val_col = batch
            .columns
            .iter()
            .find(|c| c.column_id == 3)
            .expect("value column");
        let rows = batch.row_count as usize;
        let off = |i: usize| {
            let b: [u8; 4] = key_col.data[i * 4..i * 4 + 4].try_into().unwrap();
            u32::from_le_bytes(b) as usize
        };
        let payload = &key_col.data[(rows + 1) * 4..];
        for i in 0..rows {
            let k = payload[off(i)..off(i + 1)].to_vec();
            let a = u32::from_le_bytes(val_col.data[i * 4..i * 4 + 4].try_into().unwrap());
            out.push((k, a));
        }
    }
    out
}

#[test]
fn tree_columnar_scan_filters_rows_to_the_requested_range() {
    // A bounded range that PARTIALLY overlaps a single segment must return only
    // the in-range rows — not the whole segment (the range is a row filter, not
    // just a segment selector).
    let folder = get_tmp_folder();
    let any = open_columnar_any(folder.path());
    ingest_segment(
        &any,
        &[
            (key(0), 0),
            (key(1), 1),
            (key(2), 2),
            (key(3), 3),
            (key(4), 4),
        ],
    );
    let tree = standard(&any);

    // [k1, k4): exclusive upper → k1, k2, k3.
    let got = scan_range_pairs(tree, UserKey::from(&key(1)[..])..UserKey::from(&key(4)[..]));
    assert_eq!(
        got,
        vec![(key(1), 1), (key(2), 2), (key(3), 3)],
        "only rows inside the requested range are returned",
    );

    // Inclusive upper [k1, k3] → k1, k2, k3.
    let got = scan_range_pairs(
        tree,
        UserKey::from(&key(1)[..])..=UserKey::from(&key(3)[..]),
    );
    assert_eq!(got, vec![(key(1), 1), (key(2), 2), (key(3), 3)]);
}

#[test]
fn tree_columnar_scan_overlap_merge_filters_rows_to_the_range() {
    // The overlap-merge path must also enforce the row-level range: a bounded scan
    // over two overlapping segments returns only in-range keys.
    let folder = get_tmp_folder();
    let any = open_columnar_any(folder.path());
    ingest_segment(&any, &[(key(0), 0), (key(2), 2), (key(4), 4)]);
    ingest_segment(&any, &[(key(1), 1), (key(3), 3), (key(5), 5)]); // overlaps → merge

    let tree = standard(&any);
    // [k2, k4]: inclusive → k2, k3, k4.
    let got = scan_range_pairs(
        tree,
        UserKey::from(&key(2)[..])..=UserKey::from(&key(4)[..]),
    );
    assert_eq!(
        got,
        vec![(key(2), 2), (key(3), 3), (key(4), 4)],
        "the merge path drops rows outside the requested range",
    );
}

#[test]
fn tree_columnar_scan_merge_preserves_a_nullable_sub_column() {
    // The overlap-merge gather (`take_rows`) must carry a nullable value
    // sub-column's validity bitmap through. Build two overlapping segments where
    // one row's value is null, force the merge path, and assert the null survives
    // in the recovered column.
    let folder = get_tmp_folder();
    let any = open_columnar_any(folder.path());

    // Segment A: k0 (valid), k2 (NULL) — a nullable fixed-4 sub-column (id 3).
    let mut batch_a = entries_to_column_batch(&[
        InternalValue::from_components(key(0), b"x".to_vec(), 0, ValueType::Value),
        InternalValue::from_components(key(2), b"x".to_vec(), 0, ValueType::Value),
    ])
    .expect("transpose");
    batch_a.columns.pop();
    batch_a.columns.push(Column {
        column_id: 3,
        type_tag: TypeTag::Fixed(4),
        // LSB-first: row 0 valid (bit set), row 1 (k2) null (bit clear) → 0b01.
        validity: Some(vec![0b0000_0001]),
        data: vec![10, 0, 0, 0, 0, 0, 0, 0],
    });
    {
        let mut ingest = any.ingestion().expect("ingestion");
        ingest.write_columnar_batch(&batch_a).expect("write A");
        ingest.finish().expect("finish A");
    }
    // Segment B: k1 — overlaps A's [k0, k2] range, forcing the merge path.
    ingest_segment(&any, &[(key(1), 11)]);

    let tree = standard(&any);
    let mut col3: Option<Column> = None;
    for batch in tree
        .columnar_scan(&[COL_USER_KEY, 3], None, SeqNo::MAX, ..)
        .expect("scan")
    {
        let batch = batch.expect("batch");
        // Output is one merged batch in key order: k0, k1, k2.
        if let Some(c) = batch.columns.into_iter().find(|c| c.column_id == 3) {
            assert!(col3.is_none(), "merge yields a single batch here");
            col3 = Some(c);
        }
    }
    let col3 = col3.expect("value sub-column present");
    let validity = col3
        .validity
        .expect("nullable column keeps its validity bitmap");
    let is_valid = |row: usize| validity[row / 8] & (1 << (row % 8)) != 0;
    // Key order: row0=k0 (valid), row1=k1 (valid), row2=k2 (NULL).
    assert!(is_valid(0), "k0 is non-null");
    assert!(is_valid(1), "k1 is non-null");
    assert!(!is_valid(2), "k2's null survives the merge gather");
}

/// Ingests a single-key segment whose value is one variable-width (Bytes) sub-
/// column (id 3), so a value-column predicate can actually row-filter it.
fn ingest_bytes_value(any: &AnyTree, k: &[u8], value: &[u8]) {
    let mut batch = entries_to_column_batch(&[InternalValue::from_components(
        k.to_vec(),
        b"x".to_vec(),
        0,
        ValueType::Value,
    )])
    .expect("transpose");
    batch.columns.pop();
    // One-row Bytes column: (row + 1) u32 offset table [0, len] then the payload.
    let mut data = Vec::new();
    data.extend_from_slice(&0u32.to_le_bytes());
    data.extend_from_slice(&(value.len() as u32).to_le_bytes());
    data.extend_from_slice(value);
    batch.columns.push(Column {
        column_id: 3,
        type_tag: TypeTag::Bytes,
        validity: None,
        data,
    });
    let mut ingest = any.ingestion().expect("ingestion");
    ingest.write_columnar_batch(&batch).expect("write");
    ingest.finish().expect("finish");
}

/// A point tombstone is not a value: when it is a key's newest visible version
/// the key is GONE, and the scan must drop the whole run rather than emit the
/// tombstone row. Emitting it disagrees with the point read, and a caller that
/// did not project the value-type column cannot even tell the row apart from a
/// live one with an empty value.
#[test]
fn tree_columnar_scan_suppresses_a_key_its_newest_row_deletes() {
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());

    // One flushed segment holding both versions of `k1`: an older value and the
    // newer deletion, plus an untouched neighbour.
    tree.insert(key(1), b"v".to_vec(), 1);
    tree.insert(key(2), b"v".to_vec(), 1);
    tree.remove(key(1), 2);
    tree.flush_active_memtable(0).expect("flush");

    assert!(
        tree.get(key(1), SeqNo::MAX).expect("get").is_none(),
        "the point read reports the key as deleted",
    );

    let mut keys: Vec<Vec<u8>> = Vec::new();
    for batch in tree
        .columnar_scan(&[COL_USER_KEY], None, SeqNo::MAX, ..)
        .expect("scan")
    {
        let batch = batch.expect("batch");
        let key_col = batch
            .columns
            .iter()
            .find(|c| c.column_id == COL_USER_KEY)
            .expect("key column");
        let rows = batch.row_count as usize;
        let off = |i: usize| {
            let b: [u8; 4] = key_col.data[i * 4..i * 4 + 4].try_into().unwrap();
            u32::from_le_bytes(b) as usize
        };
        let payload = &key_col.data[(rows + 1) * 4..];
        for i in 0..rows {
            keys.push(payload[off(i)..off(i + 1)].to_vec());
        }
    }
    assert_eq!(
        keys,
        vec![key(2)],
        "the deleted key must not surface as a row while the point read calls \
         it absent; the live neighbour still does",
    );
}

/// A bulk-ingested segment stores every row at LOCAL seqno 0 and takes its
/// effective ordering from the segment's `global_seqno`, which is what tree
/// visibility and every other read surface use. A projected seqno column must
/// therefore be translated to that global coordinate — handing back the stored
/// zero reports a commit sequence number the tree never had.
#[test]
fn tree_columnar_scan_projects_seqnos_in_global_coordinates() {
    let folder = get_tmp_folder();
    let any = open_columnar_any(folder.path());
    // Two DISJOINT singletons: the first ingestion takes global seqno 0 on a
    // fresh tree, so a second one is needed for a nonzero offset to exist.
    ingest_segment(&any, &[(key(1), 10)]);
    ingest_segment(&any, &[(key(9), 20)]);

    let tree = standard(&any);
    let version = tree.current_version();
    let mut globals: Vec<u64> = version
        .iter_tables()
        .map(lsm_tree::Table::global_seqno)
        .collect();
    globals.sort_unstable();
    assert!(
        globals.last().is_some_and(|&g| g > 0),
        "the second ingestion takes a nonzero global seqno, got {globals:?}",
    );
    drop(version);

    let mut seqnos: Vec<u64> = Vec::new();
    for batch in tree
        .columnar_scan(&[COL_USER_KEY, COL_SEQNO], None, SeqNo::MAX, ..)
        .expect("scan")
    {
        let batch = batch.expect("batch");
        let seqno_col = batch
            .columns
            .iter()
            .find(|c| c.column_id == COL_SEQNO)
            .expect("seqno column projected");
        for i in 0..batch.row_count as usize {
            seqnos.push(u64::from_le_bytes(
                seqno_col.data[i * 8..i * 8 + 8].try_into().unwrap(),
            ));
        }
    }
    seqnos.sort_unstable();
    assert_eq!(
        seqnos, globals,
        "each row carries the EFFECTIVE seqno of its segment (local + global), \
         not the stored local zero",
    );
}

/// The same rule on the ZERO-COPY path: a segment with one version per key is
/// streamed verbatim, and a key whose single row is a tombstone would ride
/// straight through it.
#[test]
fn tree_columnar_scan_suppresses_a_deleted_key_on_the_verbatim_path() {
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());

    // Distinct keys, one version each — the segment is provably unique, so the
    // dedup path is skipped entirely.
    tree.insert(key(1), b"v".to_vec(), 1);
    tree.remove(key(2), 1);
    tree.flush_active_memtable(0).expect("flush");

    assert!(
        tree.get(key(2), SeqNo::MAX).expect("get").is_none(),
        "the point read reports the deleted key as absent",
    );
    assert_eq!(
        scan_keys(&tree, SeqNo::MAX),
        vec![key(1)],
        "a tombstone must not stream through as a row just because its segment \
         holds one version per key",
    );
}

/// And on the OVERLAP path: the newest version wins across segments, so a newer
/// segment's tombstone has to remove the key rather than be emitted.
#[test]
fn tree_columnar_scan_suppresses_a_deleted_key_across_overlapping_segments() {
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());

    // Two flushed segments over the SAME key range, so the scan row-merges them.
    tree.insert(key(1), b"v".to_vec(), 1);
    tree.insert(key(2), b"v".to_vec(), 1);
    tree.flush_active_memtable(0).expect("flush");
    tree.remove(key(1), 2);
    tree.insert(key(2), b"w".to_vec(), 2);
    tree.flush_active_memtable(0).expect("flush");

    assert!(
        tree.get(key(1), SeqNo::MAX).expect("get").is_none(),
        "the point read reports the deleted key as absent",
    );
    assert_eq!(
        scan_keys(&tree, SeqNo::MAX),
        vec![key(2)],
        "the newer segment's tombstone removes the key instead of surfacing as \
         a row; the overwritten neighbour still yields its newest version",
    );
}

/// A merge chain is not a version chain: the older rows are the merge's INPUTS,
/// not data the newest row shadows. Newest-version-wins dedup would hand back
/// the raw operand while a point read hands back the merged value, and it drops
/// the base row, so the consumer cannot even resolve the chain itself. The scan
/// must refuse rather than disagree with the read path.
#[test]
fn tree_columnar_scan_refuses_a_tree_that_merges() {
    use std::sync::Arc;

    struct Append;
    impl lsm_tree::MergeOperator for Append {
        fn merge(
            &self,
            _key: &[u8],
            base_value: Option<&[u8]>,
            operands: &[&[u8]],
        ) -> lsm_tree::Result<lsm_tree::UserValue> {
            let mut out = base_value.unwrap_or(b"").to_vec();
            for op in operands {
                out.extend_from_slice(op);
            }
            Ok(out.into())
        }
    }

    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(Append)))
    .open()
    .expect("open");
    let tree = standard(&any);
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
    })
    .expect("enable columnar + zone-map");

    tree.insert(key(1), b"A".to_vec(), 1);
    tree.merge(key(1), b"B".to_vec(), 2);
    tree.flush_active_memtable(0).expect("flush");

    assert_eq!(
        tree.get(key(1), SeqNo::MAX)
            .expect("get")
            .as_deref()
            .map(<[u8]>::to_vec),
        Some(b"AB".to_vec()),
        "the read path resolves the chain, which is the behaviour the scan \
         would have to reproduce",
    );

    let err = tree
        .columnar_scan(&[COL_USER_KEY, COL_VALUE], None, SeqNo::MAX, ..)
        .err()
        .expect("a merging tree must be refused, not served raw operands");
    assert!(
        matches!(err, Error::FeatureUnsupported(_)),
        "the refusal names the unsupported combination, got {err:?}",
    );
}

#[test]
fn tree_columnar_scan_applies_predicate_after_newest_version_wins() {
    // MVCC + predicate ordering in the overlap-merge path: when a key's NEWEST
    // visible version does NOT match the predicate but an older version does, the
    // key must be OMITTED (the newest version shadows the older) — not returned as
    // the stale matching older row.
    let folder = get_tmp_folder();
    let any = open_columnar_any(folder.path());
    ingest_bytes_value(&any, &key(1), b"aaa"); // older: matches predicate
    ingest_bytes_value(&any, &key(1), b"zzz"); // newer, same key: does NOT match

    let pred = ColumnRangePredicate {
        column_id: 3,
        lower: Some(b"aaa".to_vec()),
        upper: Some(b"aaa".to_vec()),
    };
    let tree = standard(&any);
    let mut keys: Vec<Vec<u8>> = Vec::new();
    for batch in tree
        .columnar_scan(&[COL_USER_KEY, 3], Some(&pred), SeqNo::MAX, ..)
        .expect("scan")
    {
        let batch = batch.expect("batch");
        let key_col = batch
            .columns
            .iter()
            .find(|c| c.column_id == COL_USER_KEY)
            .expect("key column");
        let rows = batch.row_count as usize;
        let off = |i: usize| {
            let b: [u8; 4] = key_col.data[i * 4..i * 4 + 4].try_into().unwrap();
            u32::from_le_bytes(b) as usize
        };
        let payload = &key_col.data[(rows + 1) * 4..];
        for i in 0..rows {
            keys.push(payload[off(i)..off(i + 1)].to_vec());
        }
    }
    assert!(
        keys.is_empty(),
        "k1's newest version (zzz) fails the predicate, so k1 is omitted, not \
         returned as the stale older matching version; got {keys:?}",
    );
}

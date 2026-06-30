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

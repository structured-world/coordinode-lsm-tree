// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! End-to-end columnar ingest: a consumer hands the ingestion a pre-transposed
//! `ColumnBatch` whose value is split into typed sub-columns. The batch is
//! stored as a columnar block (sub-columns kept, not re-transposed), and point
//! reads reconstruct each row's value, which unframes back to the original
//! sub-cells.

#![cfg(feature = "columnar")]

use lsm_tree::table::columnar::{
    Column, ColumnBatch, TypeTag, entries_to_column_batch, unframe_value_cells,
    unframe_value_cells_nullable,
};
use lsm_tree::{
    AbstractTree, AnyTree, Config, InternalValue, SeqNo, SequenceNumberCounter, ValueType,
    get_tmp_folder,
};
use test_log::test;

/// Builds a consumer batch for two sorted keys whose value is two sub-columns:
/// a fixed-4 column and a variable-width bytes column. Per-row seqnos are 0, so
/// the ingestion assigns the atomic global sequence number.
fn two_subcolumn_batch() -> ColumnBatch {
    let mut batch = entries_to_column_batch(&[
        InternalValue::from_components(b"k0", b"ignored", 0, ValueType::Value),
        InternalValue::from_components(b"k1", b"ignored", 0, ValueType::Value),
    ])
    .expect("transpose");
    // Drop the single opaque value column; add two value sub-columns.
    batch.columns.pop();
    batch.columns.push(Column {
        column_id: 3,
        type_tag: TypeTag::Fixed(4),
        validity: None,
        data: vec![1, 0, 0, 0, 2, 0, 0, 0], // row 0 = 1, row 1 = 2
    });
    let mut bytes_data = Vec::new();
    for off in [0u32, 2, 5] {
        bytes_data.extend_from_slice(&off.to_le_bytes());
    }
    bytes_data.extend_from_slice(b"aabbb"); // row 0 = "aa", row 1 = "bbb"
    batch.columns.push(Column {
        column_id: 4,
        type_tag: TypeTag::Bytes,
        validity: None,
        data: bytes_data,
    });
    batch
}

#[test]
fn columnar_ingest_round_trips_value_subcolumns() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Columnar ingest requires the columnar layout; enable it on the inner tree.
    let AnyTree::Standard(tree) = &any else {
        panic!("expected standard tree");
    };
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
    })
    .expect("enable columnar");

    let batch = two_subcolumn_batch();
    let mut ingest = any.ingestion()?;
    ingest.write_columnar_batch(&batch)?;
    ingest.finish()?;

    let tags = [TypeTag::Fixed(4), TypeTag::Bytes];
    let v0 = any.get(b"k0", SeqNo::MAX)?.expect("k0 present");
    assert_eq!(
        unframe_value_cells(v0.as_ref(), &tags)?,
        vec![&[1, 0, 0, 0][..], &b"aa"[..]],
    );
    let v1 = any.get(b"k1", SeqNo::MAX)?.expect("k1 present");
    assert_eq!(
        unframe_value_cells(v1.as_ref(), &tags)?,
        vec![&[2, 0, 0, 0][..], &b"bbb"[..]],
    );
    Ok(())
}

#[test]
fn columnar_ingest_rejected_without_columnar_layout() -> lsm_tree::Result<()> {
    // A row-mode tree (columnar disabled) must refuse a columnar batch rather
    // than write a columnar block under a row descriptor.
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let batch = two_subcolumn_batch();
    let mut ingest = any.ingestion()?;
    assert!(
        matches!(
            ingest.write_columnar_batch(&batch),
            Err(lsm_tree::Error::FeatureUnsupported(_))
        ),
        "columnar ingest must be rejected when the layout is not columnar",
    );
    Ok(())
}

#[test]
fn columnar_ingest_projects_individual_value_subcolumns() -> lsm_tree::Result<()> {
    // A projection scan over a consumer-ingested segment decodes only the
    // requested value sub-column, never the others or the intrinsic value.
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    let AnyTree::Standard(tree) = &any else {
        panic!("expected standard tree");
    };
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
    })
    .expect("enable columnar");

    {
        let mut ingest = any.ingestion()?;
        ingest.write_columnar_batch(&two_subcolumn_batch())?;
        ingest.finish()?;
    }

    let version = tree.current_version();
    let table = version.iter_tables().next().expect("one ingested SST");

    // Project only the fixed-4 sub-column (id 3): every batch carries it alone,
    // and its bytes are the two ingested values verbatim.
    let batches = table.columnar_scan(&[3], None)?;
    let mut col3_data = Vec::new();
    let mut rows = 0u32;
    for b in &batches {
        assert!(
            b.columns.iter().all(|c| c.column_id == 3),
            "a sub-column-3 projection must not decode any other column",
        );
        rows += b.row_count;
        for col in b.columns.iter().filter(|c| c.column_id == 3) {
            col3_data.extend_from_slice(&col.data);
        }
    }
    assert_eq!(rows, 2, "projection still sees every row");
    assert_eq!(
        col3_data,
        vec![1, 0, 0, 0, 2, 0, 0, 0],
        "fixed-4 sub-column bytes"
    );

    // Project only the bytes sub-column (id 4): again decoded in isolation, and
    // its payload is the two ingested values verbatim (asserting the id alone
    // would also pass for an empty or wrong-row column).
    let batches = table.columnar_scan(&[4], None)?;
    let mut col4_data = Vec::new();
    let mut rows = 0u32;
    for b in &batches {
        assert!(
            b.columns.iter().all(|c| c.column_id == 4),
            "a sub-column-4 projection must not decode any other column",
        );
        rows += b.row_count;
        for col in b.columns.iter().filter(|c| c.column_id == 4) {
            col4_data.extend_from_slice(&col.data);
        }
    }
    assert_eq!(rows, 2, "projection still sees every row");
    // The bytes column body: (row_count + 1) little-endian u32 offsets [0, 2, 5]
    // followed by the concatenated payload "aabbb" (row 0 = "aa", row 1 = "bbb").
    let mut want_col4 = Vec::new();
    for off in [0u32, 2, 5] {
        want_col4.extend_from_slice(&off.to_le_bytes());
    }
    want_col4.extend_from_slice(b"aabbb");
    assert_eq!(col4_data, want_col4, "projected bytes sub-column payload");
    Ok(())
}

/// One row whose value is a single fixed-4 sub-column (id 3), the smallest batch
/// that still exercises the ingest contract checks.
fn one_fixed_subcolumn_batch(key: &[u8], seqno: u64) -> ColumnBatch {
    let mut batch = entries_to_column_batch(&[InternalValue::from_components(
        key.to_vec(),
        b"x".to_vec(),
        seqno,
        ValueType::Value,
    )])
    .expect("transpose");
    batch.columns.pop();
    batch.columns.push(Column {
        column_id: 3,
        type_tag: TypeTag::Fixed(4),
        validity: None,
        data: vec![0, 0, 0, 0],
    });
    batch
}

#[test]
fn columnar_ingest_rejects_unsorted_keys() -> lsm_tree::Result<()> {
    // Two rows in descending key order: the ingest must reject them rather than
    // write a block whose sorted index would be corrupt.
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    let AnyTree::Standard(tree) = &any else {
        panic!("expected standard tree");
    };
    tree.update_runtime_config(|cfg| cfg.columnar = true)
        .expect("enable columnar");

    let mut batch = entries_to_column_batch(&[
        InternalValue::from_components(b"k1".to_vec(), b"x".to_vec(), 0, ValueType::Value),
        InternalValue::from_components(b"k0".to_vec(), b"x".to_vec(), 0, ValueType::Value),
    ])?;
    batch.columns.pop();
    batch.columns.push(Column {
        column_id: 3,
        type_tag: TypeTag::Fixed(4),
        validity: None,
        data: vec![0, 0, 0, 0, 0, 0, 0, 0],
    });

    let mut ingest = any.ingestion()?;
    assert!(
        matches!(
            ingest.write_columnar_batch(&batch),
            Err(lsm_tree::Error::InvalidHeader(_))
        ),
        "descending keys must be rejected",
    );
    Ok(())
}

#[test]
fn columnar_ingest_rejects_nonzero_seqno() -> lsm_tree::Result<()> {
    // A non-zero per-row seqno would read back shifted by the table's assigned
    // sequence number, so the ingest must reject it.
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    let AnyTree::Standard(tree) = &any else {
        panic!("expected standard tree");
    };
    tree.update_runtime_config(|cfg| cfg.columnar = true)
        .expect("enable columnar");

    let mut ingest = any.ingestion()?;
    assert!(
        matches!(
            ingest.write_columnar_batch(&one_fixed_subcolumn_batch(b"k0", 5)),
            Err(lsm_tree::Error::FeatureUnsupported(_))
        ),
        "a non-zero row seqno must be rejected",
    );
    Ok(())
}

#[test]
fn columnar_ingest_rejects_batch_out_of_order_with_a_prior_write() -> lsm_tree::Result<()> {
    // A columnar batch whose first key precedes a key already written through the
    // same ingestion must be rejected, not silently produce an unsorted run (this
    // cross-call guard holds even across writer table rotations).
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    let AnyTree::Standard(tree) = &any else {
        panic!("expected standard tree");
    };
    tree.update_runtime_config(|cfg| cfg.columnar = true)
        .expect("enable columnar");

    let mut ingest = any.ingestion()?;
    ingest.write(b"z".to_vec(), b"v".to_vec())?;
    assert!(
        matches!(
            ingest.write_columnar_batch(&one_fixed_subcolumn_batch(b"a", 0)),
            Err(lsm_tree::Error::InvalidHeader(_))
        ),
        "a batch whose first key precedes a prior write must be rejected",
    );
    Ok(())
}

#[test]
fn columnar_ingest_after_a_row_write_keeps_block_order() -> lsm_tree::Result<()> {
    // A row write buffers a chunk; a following (in-order) columnar batch must not
    // register its block before that buffered chunk is spilled, or the sorted
    // block index is left out of order and the row-written key is lost.
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    let AnyTree::Standard(tree) = &any else {
        panic!("expected standard tree");
    };
    tree.update_runtime_config(|cfg| cfg.columnar = true)
        .expect("enable columnar");

    let mut ingest = any.ingestion()?;
    ingest.write(b"a".to_vec(), b"va".to_vec())?;
    ingest.write_columnar_batch(&one_fixed_subcolumn_batch(b"b", 0))?;
    ingest.finish()?;

    assert!(
        any.get(b"a", SeqNo::MAX)?.is_some(),
        "the row-written key must survive the following columnar batch",
    );
    assert!(
        any.get(b"b", SeqNo::MAX)?.is_some(),
        "the columnar-batch key must be present",
    );
    Ok(())
}

#[test]
fn columnar_ingest_rejected_on_a_blob_tree() -> lsm_tree::Result<()> {
    // KV-separated (blob) trees do not support columnar batch ingest.
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(Default::default()))
    .open()?;

    let mut ingest = any.ingestion()?;
    assert!(
        matches!(
            ingest.write_columnar_batch(&one_fixed_subcolumn_batch(b"k0", 0)),
            Err(lsm_tree::Error::FeatureUnsupported(_))
        ),
        "columnar ingest is not supported on a blob tree",
    );
    Ok(())
}

/// Builds a same-layout batch (value = a fixed-4 sub-column id 3 and a bytes
/// sub-column id 4) for the given rows, used to exercise rowgroup accumulation.
fn fixed_bytes_batch(rows: &[(&[u8], u32, &[u8])]) -> ColumnBatch {
    let entries: Vec<_> = rows
        .iter()
        .map(|(k, _, _)| {
            InternalValue::from_components(k.to_vec(), b"x".to_vec(), 0, ValueType::Value)
        })
        .collect();
    let mut batch = entries_to_column_batch(&entries).expect("transpose");
    batch.columns.pop();
    batch.columns.push(Column {
        column_id: 3,
        type_tag: TypeTag::Fixed(4),
        validity: None,
        data: rows.iter().flat_map(|(_, f, _)| f.to_le_bytes()).collect(),
    });
    let mut bytes_data = Vec::new();
    let mut acc = 0u32;
    bytes_data.extend_from_slice(&acc.to_le_bytes());
    for (_, _, b) in rows {
        acc += u32::try_from(b.len()).unwrap();
        bytes_data.extend_from_slice(&acc.to_le_bytes());
    }
    for (_, _, b) in rows {
        bytes_data.extend_from_slice(b);
    }
    batch.columns.push(Column {
        column_id: 4,
        type_tag: TypeTag::Bytes,
        validity: None,
        data: bytes_data,
    });
    batch
}

/// Builds a batch whose value is a single fixed-4 sub-column (id 3) for the given
/// rows: a different layout from [`fixed_bytes_batch`], so the two cannot merge.
fn fixed_only_batch(rows: &[(&[u8], u32)]) -> ColumnBatch {
    let entries: Vec<_> = rows
        .iter()
        .map(|(k, _)| {
            InternalValue::from_components(k.to_vec(), b"x".to_vec(), 0, ValueType::Value)
        })
        .collect();
    let mut batch = entries_to_column_batch(&entries).expect("transpose");
    batch.columns.pop();
    batch.columns.push(Column {
        column_id: 3,
        type_tag: TypeTag::Fixed(4),
        validity: None,
        data: rows.iter().flat_map(|(_, f)| f.to_le_bytes()).collect(),
    });
    batch
}

#[test]
fn columnar_ingest_merges_small_batches_into_one_rowgroup() -> lsm_tree::Result<()> {
    // Three small same-layout batches accumulate into one rowgroup, written as a
    // single columnar block (not one block per batch), and every row reads back.
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    let AnyTree::Standard(tree) = &any else {
        panic!("expected standard tree");
    };
    tree.update_runtime_config(|cfg| cfg.columnar = true)
        .expect("enable columnar");

    let mut ingest = any.ingestion()?;
    ingest.write_columnar_batch(&fixed_bytes_batch(&[(b"k0", 1, b"a"), (b"k1", 2, b"bb")]))?;
    ingest.write_columnar_batch(&fixed_bytes_batch(&[
        (b"k2", 3, b"ccc"),
        (b"k3", 4, b"dddd"),
    ]))?;
    ingest.write_columnar_batch(&fixed_bytes_batch(&[(b"k4", 5, b"e"), (b"k5", 6, b"ff")]))?;
    ingest.finish()?;

    let version = tree.current_version();
    let table = version.iter_tables().next().expect("one ingested SST");
    let batches = table.columnar_scan(&[3, 4], None)?;
    assert_eq!(
        batches.len(),
        1,
        "three small same-layout batches merge into a single rowgroup block",
    );
    assert_eq!(
        batches.iter().map(|b| b.row_count).sum::<u32>(),
        6,
        "the merged rowgroup holds every row",
    );

    // Spot-check the first and last rows reconstruct to their sub-cells.
    let tags = [TypeTag::Fixed(4), TypeTag::Bytes];
    let v0 = any.get(b"k0", SeqNo::MAX)?.expect("k0");
    assert_eq!(
        unframe_value_cells(v0.as_ref(), &tags)?,
        vec![&1u32.to_le_bytes()[..], &b"a"[..]],
    );
    let v5 = any.get(b"k5", SeqNo::MAX)?.expect("k5");
    assert_eq!(
        unframe_value_cells(v5.as_ref(), &tags)?,
        vec![&6u32.to_le_bytes()[..], &b"ff"[..]],
    );
    Ok(())
}

#[test]
fn columnar_ingest_flushes_the_rowgroup_on_a_layout_change() -> lsm_tree::Result<()> {
    // A batch with a different column layout cannot extend the pending rowgroup,
    // so it flushes what is buffered and starts a new block. Both blocks carry
    // sub-column 3, so a projection over it sees two batches (two blocks).
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    let AnyTree::Standard(tree) = &any else {
        panic!("expected standard tree");
    };
    tree.update_runtime_config(|cfg| cfg.columnar = true)
        .expect("enable columnar");

    let mut ingest = any.ingestion()?;
    ingest.write_columnar_batch(&fixed_only_batch(&[(b"k0", 1), (b"k1", 2)]))?;
    ingest.write_columnar_batch(&fixed_bytes_batch(&[
        (b"k2", 3, b"ccc"),
        (b"k3", 4, b"dddd"),
    ]))?;
    ingest.finish()?;

    let version = tree.current_version();
    let table = version.iter_tables().next().expect("one ingested SST");
    let batches = table.columnar_scan(&[3], None)?;
    assert_eq!(
        batches.len(),
        2,
        "a layout change flushes the pending rowgroup into its own block",
    );
    for k in [b"k0".as_slice(), b"k1", b"k2", b"k3"] {
        assert!(any.get(k, SeqNo::MAX)?.is_some(), "every key is readable");
    }
    Ok(())
}

#[test]
fn columnar_ingest_rotates_the_rowgroup_at_the_size_threshold() -> lsm_tree::Result<()> {
    // The third flush trigger: a same-layout stream whose accumulated size crosses
    // the target data-block size flushes mid-ingest, so a large stream produces
    // more than one block even though every batch shares the layout (unlike the
    // small-batch merge, which stays one block).
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    let AnyTree::Standard(tree) = &any else {
        panic!("expected standard tree");
    };
    tree.update_runtime_config(|cfg| cfg.columnar = true)
        .expect("enable columnar");

    // Each row carries a 4 KiB bytes value, so a handful of same-layout batches
    // far exceed the target data-block size and the rowgroup rotates before
    // finish.
    let big = vec![b'x'; 4096];
    let mut ingest = any.ingestion()?;
    for key in [b"k0".as_slice(), b"k1", b"k2", b"k3"] {
        ingest.write_columnar_batch(&fixed_bytes_batch(&[(key, 1, &big)]))?;
    }
    ingest.finish()?;

    let version = tree.current_version();
    let table = version.iter_tables().next().expect("one ingested SST");
    let batches = table.columnar_scan(&[3, 4], None)?;
    assert!(
        batches.len() >= 2,
        "size-threshold rotation splits a large same-layout stream into multiple blocks (got {})",
        batches.len(),
    );
    assert_eq!(
        batches.iter().map(|b| b.row_count).sum::<u32>(),
        4,
        "every row is present across the rotated blocks",
    );
    for k in [b"k0".as_slice(), b"k1", b"k2", b"k3"] {
        assert!(any.get(k, SeqNo::MAX)?.is_some(), "every key is readable");
    }
    Ok(())
}

#[test]
fn columnar_ingest_round_trips_a_nullable_value_subcolumn() -> lsm_tree::Result<()> {
    // A value sub-column may be absent for some rows (a sparse field). Ingest a
    // batch whose fixed-4 sub-column is null for the second row, then read both
    // rows back: the reconstructed value unframes to the present / absent cells.
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    let AnyTree::Standard(tree) = &any else {
        panic!("expected standard tree");
    };
    tree.update_runtime_config(|cfg| cfg.columnar = true)
        .expect("enable columnar");

    let mut batch = entries_to_column_batch(&[
        InternalValue::from_components(b"k0".to_vec(), b"ignored".to_vec(), 0, ValueType::Value),
        InternalValue::from_components(b"k1".to_vec(), b"ignored".to_vec(), 0, ValueType::Value),
    ])?;
    batch.columns.pop();
    // Bytes sub-column, always present: "aa", "bbb".
    let mut bytes_data = Vec::new();
    for off in [0u32, 2, 5] {
        bytes_data.extend_from_slice(&off.to_le_bytes());
    }
    bytes_data.extend_from_slice(b"aabbb");
    batch.columns.push(Column {
        column_id: 3,
        type_tag: TypeTag::Bytes,
        validity: None,
        data: bytes_data,
    });
    // Fixed-4 sub-column, row 0 present, row 1 null (validity bit 0 set only).
    batch.columns.push(Column {
        column_id: 4,
        type_tag: TypeTag::Fixed(4),
        validity: Some(vec![0b0000_0001]),
        data: vec![1, 0, 0, 0, 0, 0, 0, 0],
    });

    let mut ingest = any.ingestion()?;
    ingest.write_columnar_batch(&batch)?;
    ingest.finish()?;

    let tags = [TypeTag::Bytes, TypeTag::Fixed(4)];
    let v0 = any.get(b"k0", SeqNo::MAX)?.expect("k0 present");
    assert_eq!(
        unframe_value_cells_nullable(v0.as_ref(), &tags)?,
        vec![Some(&b"aa"[..]), Some(&[1, 0, 0, 0][..])],
    );
    let v1 = any.get(b"k1", SeqNo::MAX)?.expect("k1 present");
    assert_eq!(
        unframe_value_cells_nullable(v1.as_ref(), &tags)?,
        vec![Some(&b"bbb"[..]), None],
        "the fixed sub-column is absent for the second row",
    );
    Ok(())
}

/// Like [`two_subcolumn_batch`] but for keys `k2`/`k3` and with a third value
/// sub-column (id 5), modelling a newer schema version that added a field.
fn three_subcolumn_batch() -> ColumnBatch {
    let mut batch = entries_to_column_batch(&[
        InternalValue::from_components(b"k2", b"ignored", 0, ValueType::Value),
        InternalValue::from_components(b"k3", b"ignored", 0, ValueType::Value),
    ])
    .expect("transpose");
    batch.columns.pop();
    batch.columns.push(Column {
        column_id: 3,
        type_tag: TypeTag::Fixed(4),
        validity: None,
        data: vec![3, 0, 0, 0, 4, 0, 0, 0], // row 0 = 3, row 1 = 4
    });
    let mut bytes_data = Vec::new();
    for off in [0u32, 2, 5] {
        bytes_data.extend_from_slice(&off.to_le_bytes());
    }
    bytes_data.extend_from_slice(b"ccddd"); // row 0 = "cc", row 1 = "ddd"
    batch.columns.push(Column {
        column_id: 4,
        type_tag: TypeTag::Bytes,
        validity: None,
        data: bytes_data,
    });
    batch.columns.push(Column {
        column_id: 5,
        type_tag: TypeTag::Fixed(4),
        validity: None,
        data: vec![5, 0, 0, 0, 6, 0, 0, 0], // the new field: row 0 = 5, row 1 = 6
    });
    batch
}

/// Schema-evolution read contract: two segments written at different schema
/// versions coexist. The first carries value sub-columns {3, 4}; the second adds
/// a new sub-column 5. A projection for column 5 across both segments returns it
/// from the new-schema segment and omits it gracefully (no error) from the
/// old-schema one, while the shared column 3 is satisfied by both. The storage
/// layer is schema-free, so no migration is needed to read mixed old/new data.
#[test]
fn columnar_scan_spans_segments_with_evolving_schema() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    let AnyTree::Standard(tree) = &any else {
        panic!("expected standard tree");
    };
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
    })
    .expect("enable columnar");

    // Two ingestions, two segments: old schema {3,4} then new schema {3,4,5}.
    {
        let mut ingest = any.ingestion()?;
        ingest.write_columnar_batch(&two_subcolumn_batch())?;
        ingest.finish()?;
    }
    {
        let mut ingest = any.ingestion()?;
        ingest.write_columnar_batch(&three_subcolumn_batch())?;
        ingest.finish()?;
    }

    let version = tree.current_version();
    let tables: Vec<_> = version.iter_tables().collect();
    assert_eq!(tables.len(), 2, "one segment per ingestion");

    // Project the new column 5 across both segments. Exactly one carries it; the
    // other returns batches without it (graceful omission, not an error).
    let mut with_col5 = 0;
    let mut without_col5 = 0;
    let mut col5_data = Vec::new();
    for table in &tables {
        let batches = table.columnar_scan(&[5], None)?;
        let present = batches
            .iter()
            .any(|b| b.columns.iter().any(|c| c.column_id == 5));
        if present {
            with_col5 += 1;
            for b in &batches {
                for c in b.columns.iter().filter(|c| c.column_id == 5) {
                    col5_data.extend_from_slice(&c.data);
                }
            }
        } else {
            without_col5 += 1;
        }
    }
    assert_eq!(with_col5, 1, "only the new-schema segment carries column 5");
    assert_eq!(
        without_col5, 1,
        "the old-schema segment omits column 5 gracefully",
    );
    assert_eq!(
        col5_data,
        vec![5, 0, 0, 0, 6, 0, 0, 0],
        "the new sub-column's bytes are the ingested values verbatim",
    );

    // The shared sub-column 3 is satisfied by BOTH segments.
    for table in &tables {
        let batches = table.columnar_scan(&[3], None)?;
        assert!(
            batches
                .iter()
                .any(|b| b.columns.iter().any(|c| c.column_id == 3)),
            "the shared column 3 is present in every segment",
        );
    }
    Ok(())
}

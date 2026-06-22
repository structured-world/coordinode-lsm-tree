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

    // Project only the bytes sub-column (id 4): again decoded in isolation.
    let batches = table.columnar_scan(&[4], None)?;
    for b in &batches {
        assert!(
            b.columns.iter().all(|c| c.column_id == 4),
            "a sub-column-4 projection must not decode any other column",
        );
    }
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

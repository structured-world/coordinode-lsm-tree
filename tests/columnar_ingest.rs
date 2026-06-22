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
        ingest.write_columnar_batch(&batch).is_err(),
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

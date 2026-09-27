// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Reads of encoded column pages: every read path sees the values the
//! encoding stands for, exceptions included, and a predicate answered from
//! the encoding builds nothing it does not return.

#![cfg(feature = "columnar")]

use lsm_tree::config::{ColumnEncoding, ColumnEncodingPolicy};
use lsm_tree::inspect::read_column_encodings;
use lsm_tree::table::columnar::{
    COL_USER_KEY, Column, Expression, Number, TypeTag, entries_to_column_batch,
};
use lsm_tree::table::columnar_predicate::{ColumnRangePredicate, PredicateApply, PredicateSupport};
use lsm_tree::{
    AnyTree, Config, InternalValue, SeqNo, SequenceNumberCounter, ValueType, get_tmp_folder,
};
use test_log::test;

const VALUE: u16 = 3;

/// The value column's type: an unsigned little-endian 4-byte integer.
fn u32_le() -> Number {
    use lsm_tree::table::columnar::{ByteOrder, NumberKind};
    Number::new(NumberKind::Unsigned, 4, ByteOrder::Little).expect("a u32")
}

fn key(i: u32) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

/// A columnar tree whose pages take their cheapest encoding, holding one
/// ingested table whose value is a `u32` sub-column of `values`, row `i`
/// under `key(i)`.
fn ingested(folder: &std::path::Path, values: &[u32]) -> AnyTree {
    let any = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .column_encoding_policy(ColumnEncodingPolicy::all(ColumnEncoding::Auto))
    .open()
    .expect("open");
    let AnyTree::Standard(tree) = &any else {
        panic!("a standard tree");
    };
    tree.update_runtime_config(|rc| {
        rc.columnar = true;
        rc.zone_map = true;
    })
    .expect("columnar and zone map");
    let rows = u32::try_from(values.len()).expect("rows");
    let entries: Vec<InternalValue> = (0..rows)
        .map(|i| InternalValue::from_components(key(i), b"", 0, ValueType::Value))
        .collect();
    let mut batch = entries_to_column_batch(&entries).expect("transpose");
    batch.columns.pop();
    batch.columns.push(Column {
        column_id: VALUE,
        type_tag: TypeTag::Number(u32_le()),
        validity: None,
        data: values
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect::<Vec<u8>>()
            .into(),
    });
    let mut ingestion = any.ingestion().expect("ingestion");
    ingestion.write_columnar_batch(&batch).expect("write");
    ingestion.finish().expect("finish");
    any
}

/// The encodings of every page of the value column in `folder`'s tables.
fn value_encodings(folder: &std::path::Path) -> Vec<Expression> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(folder.join("tables")).expect("tables") {
        let path = entry.expect("entry").path();
        if path.is_file() {
            out.extend(
                read_column_encodings(&path)
                    .expect("encodings")
                    .into_iter()
                    .filter(|page| page.column_id == VALUE)
                    .map(|page| page.expression),
            );
        }
    }
    out
}

/// The keys a scan of `tree` under `predicate`, projecting `projection`,
/// returns, and how far the predicate ran.
fn scan_keys(
    tree: &AnyTree,
    projection: &[u16],
    predicate: &ColumnRangePredicate,
) -> (
    Vec<Vec<u8>>,
    Vec<lsm_tree::table::columnar::ColumnBatch>,
    Option<PredicateSupport>,
) {
    let AnyTree::Standard(tree) = tree else {
        panic!("a standard tree");
    };
    let mut scan = tree
        .columnar_scan(projection, Some(predicate), SeqNo::MAX, ..)
        .expect("scan");
    let mut keys = Vec::new();
    let mut batches = Vec::new();
    for batch in scan.by_ref() {
        let batch = batch.expect("batch");
        let column = batch
            .columns
            .iter()
            .find(|c| c.column_id == COL_USER_KEY)
            .expect("key column");
        let rows = batch.row_count as usize;
        let offset = |i: usize| {
            u32::from_le_bytes(column.data[i * 4..i * 4 + 4].try_into().expect("offset")) as usize
        };
        let payload = &column.data[(rows + 1) * 4..];
        keys.extend((0..rows).map(|i| payload[offset(i)..offset(i + 1)].to_vec()));
        batches.push(batch);
    }
    let support = scan.predicate_support();
    (keys, batches, support)
}

/// `value >= lower`, in the column's comparable encoding.
fn at_least(lower: u32) -> ColumnRangePredicate {
    ColumnRangePredicate {
        column_id: VALUE,
        lower: Some(u32_le().comparable(&lower.to_le_bytes()).expect("a u32")),
        upper: None,
        apply: PredicateApply::Filter,
    }
}

/// Values `0..=7` with one row of `1000`: the page holding it is packed in
/// three bits with the outlier as an exception, and `value > 100` returns
/// exactly that row. The zone map, the predicate on the encoding, the
/// selection and the rows built all see the exception; any of them taking
/// its range from the base and width would see `0..=7` and drop the row.
#[test]
fn an_exception_is_found_by_every_read_path() {
    const ROWS: u32 = 4000;
    const OUTLIER: u32 = 2345;
    let folder = get_tmp_folder();
    let values: Vec<u32> = (0..ROWS)
        .map(|i| if i == OUTLIER { 1000 } else { (i * 5) % 8 })
        .collect();
    let tree = ingested(folder.path(), &values);

    let encodings = value_encodings(folder.path());
    assert!(
        encodings.iter().any(|e| matches!(
            e,
            Expression::Ordinals(inner)
                if **inner == Expression::Ffor { bit_width: 3, exceptions: 1 }
        )),
        "the outlier's page is packed in 3 bits with one exception; got {encodings:?}",
    );

    let (keys, _, support) = scan_keys(&tree, &[COL_USER_KEY, VALUE], &at_least(101));
    assert_eq!(keys, vec![key(OUTLIER)], "exactly the outlier's row");
    assert_eq!(support, Some(PredicateSupport::Exact));
}

/// A predicate over a column of short runs of a few values is answered from
/// its runs or dictionary, and a column read only for the predicate is never
/// built: the scan copies the keys it returns and nothing else.
#[cfg(feature = "metrics")]
#[test]
fn a_predicate_over_runs_builds_only_the_rows_it_returns() {
    use lsm_tree::AbstractTree;

    const ROWS: u32 = 4000;
    let folder = get_tmp_folder();
    let values: Vec<u32> = (0..ROWS).map(|i| (i / 20) % 5).collect();
    let tree = ingested(folder.path(), &values);

    let encodings = value_encodings(folder.path());
    assert!(
        !encodings.is_empty()
            && encodings.iter().all(|e| matches!(
                e,
                Expression::Rle { .. } | Expression::Dict { .. } | Expression::Constant
            )),
        "the value column is runs, a dictionary or constant; got {encodings:?}",
    );

    let equals_two = ColumnRangePredicate {
        upper: Some(u32_le().comparable(&2u32.to_le_bytes()).expect("a u32")),
        ..at_least(2)
    };
    let metrics = tree.metrics();
    let before = metrics.bytes_copied();
    let (keys, batches, support) = scan_keys(&tree, &[COL_USER_KEY], &equals_two);
    let copied = metrics.bytes_copied() - before;

    let expected: Vec<Vec<u8>> = (0..ROWS)
        .filter(|&i| values[i as usize] == 2)
        .map(key)
        .collect();
    assert_eq!(keys, expected);
    assert_eq!(support, Some(PredicateSupport::Exact));
    let returned: u64 = batches
        .iter()
        .flat_map(|b| &b.columns)
        .map(|c| c.data.len() as u64)
        .sum();
    assert_eq!(
        copied, returned,
        "only the returned keys are built; the predicate column never is",
    );
}

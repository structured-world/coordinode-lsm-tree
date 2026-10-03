// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A projected scan of a blob tree returns exactly the fields a row read
//! returns, however late it reads them.
//!
//! Random writes of rows written as cells (a status, a price and a body of
//! varying width, separated into blob files from a threshold), metadata-only
//! updates that keep a row's body by reference, whole values in the same
//! layout, point and range deletes, flushes and compactions, in row or
//! columnar tables, are scanned at a random snapshot over a random range,
//! projecting the compact fields with or without the body, under a random
//! predicate on the price. Every scan must match the row read path, whose
//! values are parsed back into the same fields.

#![cfg(feature = "columnar")]

mod common;

use common::guard_to_kv;
use lsm_tree::blob_tree::field_row::{Cell, FIRST_FIELD_COLUMN, Field, TypeTag};
use lsm_tree::table::column_type::{ByteOrder, Number, NumberKind};
use lsm_tree::table::columnar::{COL_USER_KEY, ColumnBatch};
use lsm_tree::table::columnar_predicate::{ColumnRangePredicate, PredicateApply};
use lsm_tree::{
    Absent, AbstractTree, AnyTree, BlobTree, Config, KvSeparationOptions, ProjectedField,
    ProjectedRow, Projection, SeqNo, SequenceNumberCounter, UserKey, ValueProjector,
};
use proptest::prelude::*;
use std::ops::Bound;
use std::sync::Arc;

const KEY_SPACE: u16 = 300;
const STATUS: u16 = FIRST_FIELD_COLUMN;
const PRICE: u16 = FIRST_FIELD_COLUMN + 1;
const BODY: u16 = FIRST_FIELD_COLUMN + 2;

fn key(i: u16) -> Vec<u8> {
    format!("k{i:04}").into_bytes()
}

fn u32_le() -> TypeTag {
    TypeTag::Number(Number::new(NumberKind::Unsigned, 4, ByteOrder::Little).expect("a u32"))
}

#[derive(Debug, Clone)]
enum Op {
    /// `len` consecutive keys from `start` written as cells, body `width`
    /// bytes wide.
    Cells {
        start: u16,
        len: u16,
        width: u16,
        fill: u8,
    },
    /// `len` consecutive keys from `start` written whole, in the same layout.
    Whole {
        start: u16,
        len: u16,
        width: u16,
        fill: u8,
    },
    /// The status of the rows in `start..start + len` that are cell rows
    /// changed, their bodies kept by reference.
    Restatus {
        start: u16,
        len: u16,
    },
    Remove {
        at: u16,
    },
    RemoveRange {
        lo: u16,
        hi: u16,
    },
    Flush,
    Compact,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => (0..KEY_SPACE, 1..80u16, 0..400u16, any::<u8>())
            .prop_map(|(start, len, width, fill)| Op::Cells { start, len, width, fill }),
        2 => (0..KEY_SPACE, 1..40u16, 0..400u16, any::<u8>())
            .prop_map(|(start, len, width, fill)| Op::Whole { start, len, width, fill }),
        2 => (0..KEY_SPACE, 1..80u16).prop_map(|(start, len)| Op::Restatus { start, len }),
        1 => (0..KEY_SPACE).prop_map(|at| Op::Remove { at }),
        1 => (0..KEY_SPACE, 0..KEY_SPACE).prop_map(|(a, b)| Op::RemoveRange {
            lo: a.min(b),
            hi: a.max(b),
        }),
        3 => Just(Op::Flush),
        1 => Just(Op::Compact),
    ]
}

#[derive(Debug, Clone)]
struct Scan {
    snapshot: u8,
    lo: Bound<u16>,
    hi: Bound<u16>,
    /// An inclusive range of prices the predicate keeps, or none.
    price: Option<(u32, u32)>,
    body: bool,
}

fn bound() -> impl Strategy<Value = Bound<u16>> {
    prop_oneof![
        Just(Bound::Unbounded),
        (0..KEY_SPACE).prop_map(Bound::Included),
        (0..KEY_SPACE).prop_map(Bound::Excluded),
    ]
}

fn scan() -> impl Strategy<Value = Scan> {
    (
        any::<u8>(),
        bound(),
        bound(),
        prop::option::of((0..u32::from(KEY_SPACE), 0..u32::from(KEY_SPACE))),
        any::<bool>(),
    )
        .prop_map(|(snapshot, lo, hi, price, body)| Scan {
            snapshot,
            lo,
            hi,
            price: price.map(|(a, b)| (a.min(b), a.max(b))),
            body,
        })
}

/// A row's logical value, as a plain read returns it: the status
/// length-prefixed, the price bare, the body length-prefixed.
fn framed(status: &[u8], price: u32, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&u32::try_from(status.len()).expect("small").to_le_bytes());
    out.extend_from_slice(status);
    out.extend_from_slice(&price.to_le_bytes());
    out.extend_from_slice(&u32::try_from(body.len()).expect("small").to_le_bytes());
    out.extend_from_slice(body);
    out
}

/// The fields of a logical value.
fn parse(value: &[u8]) -> (Vec<u8>, [u8; 4], Vec<u8>) {
    let len = |at: usize| u32::from_le_bytes(value[at..at + 4].try_into().expect("4")) as usize;
    let status_len = len(0);
    let status = value[4..4 + status_len].to_vec();
    let price: [u8; 4] = value[4 + status_len..8 + status_len].try_into().expect("4");
    let body_at = 8 + status_len;
    let body = value[body_at + 4..body_at + 4 + len(body_at)].to_vec();
    (status, price, body)
}

/// Reads a whole value's fields, the layout every row of the test has.
struct Fields;

impl ValueProjector for Fields {
    fn project(
        &self,
        _key: &[u8],
        value: &[u8],
        row: &mut ProjectedRow<'_>,
    ) -> lsm_tree::Result<()> {
        let (status, price, body) = parse(value);
        for index in 0..row.fields().len() {
            match row.fields()[index].column_id() {
                STATUS => row.set(index, &status)?,
                PRICE => row.set(index, &price)?,
                BODY => row.set(index, &body)?,
                _ => {}
            }
        }
        Ok(())
    }
}

/// Each row of `batches`: its key and its cell in each projected field.
fn rows_of(batch: &ColumnBatch) -> Vec<Vec<Option<Vec<u8>>>> {
    (0..batch.row_count)
        .map(|row| {
            batch
                .columns
                .iter()
                .map(|column| {
                    let valid = column
                        .validity
                        .as_ref()
                        .is_none_or(|bits| bits[row as usize / 8] >> (row % 8) & 1 == 1);
                    valid.then(|| match column.type_tag.fixed_width() {
                        Some(width) => {
                            let width = usize::from(width);
                            column.data[row as usize * width..(row as usize + 1) * width].to_vec()
                        }
                        None => {
                            let at = |i: u32| {
                                let i = i as usize * 4;
                                u32::from_le_bytes(column.data[i..i + 4].try_into().expect("4"))
                                    as usize
                            };
                            let base = (batch.row_count as usize + 1) * 4;
                            column.data[base + at(row)..base + at(row + 1)].to_vec()
                        }
                    })
                })
                .collect()
        })
        .collect()
}

fn fail(what: &str) -> impl FnOnce(lsm_tree::Error) -> TestCaseError + '_ {
    move |e| TestCaseError::fail(format!("{what}: {e}"))
}

fn write(tree: &BlobTree, ops: &[Op]) -> Result<SeqNo, TestCaseError> {
    let mut seqno: SeqNo = 1;
    for op in ops {
        match *op {
            Op::Cells {
                start,
                len,
                width,
                fill,
            } => {
                for i in start..start.saturating_add(len).min(KEY_SPACE) {
                    let price = u32::from(i).to_le_bytes();
                    let body = vec![fill.wrapping_add(i as u8); usize::from(width)];
                    tree.insert_cells(
                        key(i),
                        &[
                            Field::bytes(STATUS, b"new"),
                            Field {
                                column: PRICE,
                                tag: u32_le(),
                                cell: Cell::Value(&price),
                            },
                            Field::bytes(BODY, &body),
                        ],
                        seqno,
                    )
                    .map_err(fail("insert cells"))?;
                    seqno += 1;
                }
            }
            Op::Whole {
                start,
                len,
                width,
                fill,
            } => {
                for i in start..start.saturating_add(len).min(KEY_SPACE) {
                    let body = vec![fill; usize::from(width)];
                    tree.insert(key(i), framed(b"whole", u32::from(i), &body), seqno);
                    seqno += 1;
                }
            }
            Op::Restatus { start, len } => {
                for i in start..start.saturating_add(len).min(KEY_SPACE) {
                    let row = match tree.get_cells(key(i), SeqNo::MAX) {
                        Ok(Some(row)) => row,
                        // Absent, or stored whole.
                        Ok(None) | Err(lsm_tree::Error::BlobRef(_)) => continue,
                        Err(e) => return Err(fail("get cells")(e)),
                    };
                    let mut fields = row.fields().map_err(fail("fields"))?;
                    fields[0].cell = Cell::Value(b"edited");
                    tree.insert_cells(key(i), &fields, seqno)
                        .map_err(fail("restatus"))?;
                    seqno += 1;
                }
            }
            Op::Remove { at } => {
                tree.remove(key(at), seqno);
                seqno += 1;
            }
            Op::RemoveRange { lo, hi } => {
                if lo < hi {
                    tree.index.remove_range(key(lo), key(hi), seqno);
                    seqno += 1;
                }
            }
            Op::Flush => {
                tree.flush_active_memtable(0).map_err(fail("flush"))?;
            }
            Op::Compact => {
                tree.flush_active_memtable(0).map_err(fail("flush"))?;
                tree.major_compact(common::COMPACTION_TARGET, 0)
                    .map_err(fail("compact"))?;
            }
        }
    }
    Ok(seqno)
}

fn run(ops: &[Op], scans: &[Scan], columnar: bool) -> Result<(), TestCaseError> {
    let folder = lsm_tree::get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(128),
    ))
    .columnar_page_size_policy(lsm_tree::config::BlockSizePolicy::all(1_024))
    .open()
    .map_err(fail("open"))?;
    let AnyTree::Blob(tree) = &any else {
        return Err(TestCaseError::fail("expected a blob tree"));
    };
    tree.index
        .update_runtime_config(|rc| rc.columnar = columnar)
        .map_err(fail("columnar"))?;
    let seqno = write(tree, ops)?;

    for scan in scans {
        let snapshot = 1 + (seqno * SeqNo::from(scan.snapshot)).div_ceil(255);
        let bounds = (
            scan.lo.map(|i| UserKey::from(key(i))),
            scan.hi.map(|i| UserKey::from(key(i))),
        );
        let mut expected = Vec::new();
        for (k, value) in tree
            .range(bounds.clone(), snapshot, None)
            .map(guard_to_kv)
            .collect::<lsm_tree::Result<Vec<_>>>()
            .map_err(fail("row read"))?
        {
            let (status, price, body) = parse(&value);
            if scan
                .price
                .is_some_and(|(lo, hi)| !(lo..=hi).contains(&u32::from_le_bytes(price)))
            {
                continue;
            }
            let mut row = vec![Some(k), Some(status), Some(price.to_vec())];
            if scan.body {
                row.push(Some(body));
            }
            expected.push(row);
        }

        let mut projection = Projection::new()
            .column(COL_USER_KEY)
            .field(
                ProjectedField::new(STATUS, TypeTag::Bytes, Absent::Null).map_err(fail("field"))?,
            )
            .field(ProjectedField::new(PRICE, u32_le(), Absent::Null).map_err(fail("field"))?)
            .projector(Arc::new(Fields));
        if scan.body {
            projection = projection.field(
                ProjectedField::new(BODY, TypeTag::Bytes, Absent::Null).map_err(fail("field"))?,
            );
        }
        let predicate = scan.price.map(|(lo, hi)| ColumnRangePredicate {
            column_id: PRICE,
            lower: Some(lo.to_be_bytes().to_vec()),
            upper: Some(hi.to_be_bytes().to_vec()),
            apply: PredicateApply::Filter,
        });
        let mut got = Vec::new();
        for batch in any
            .columnar_scan(projection, predicate.as_ref(), snapshot, bounds)
            .map_err(fail("scan"))?
        {
            let batch = batch.map_err(fail("batch"))?;
            prop_assert!(batch.row_count > 0, "an empty batch was yielded");
            got.extend(rows_of(&batch));
        }
        prop_assert_eq!(got, expected, "the scan disagrees with the row read");
    }
    Ok(())
}

proptest! {
    #![proptest_config(common::proptest_config())]

    #[test]
    fn a_blob_tree_scan_returns_what_a_row_read_returns(
        ops in prop::collection::vec(op(), 1..25),
        scans in prop::collection::vec(scan(), 1..5),
        columnar in any::<bool>(),
    ) {
        run(&ops, &scans, columnar)?;
    }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A projected field that a segment was written without reads as its
//! declaration says (null, a default, or an error), the same way on every
//! path of the scan, and never as an older version's value.

#![cfg(feature = "columnar")]

use lsm_tree::table::columnar::{
    COL_USER_KEY, Column, ColumnBatch, TypeTag, entries_to_column_batch, frame_value_cells,
    unframe_value_cells,
};
use lsm_tree::{
    Absent, AbstractTree, AnyTree, Config, Error, InternalValue, ProjectedField, ProjectedRow,
    Projection, SeqNo, SequenceNumberCounter, Slice, ValueProjector, ValueType, get_tmp_folder,
};
use test_log::test;

fn key(i: u32) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

/// Opens an empty tree that writes columnar segments.
fn open_columnar(folder: &std::path::Path) -> AnyTree {
    let any = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("open");
    let AnyTree::Standard(tree) = &any else {
        panic!("expected a standard tree");
    };
    tree.update_runtime_config(|cfg| cfg.columnar = true)
        .expect("enable columnar");
    any
}

/// Ingests one segment of `keys`, whose value sub-columns are `columns`: each
/// an id and one fixed-4 cell per key.
fn ingest(any: &AnyTree, keys: &[u32], columns: &[(u16, &[u32])]) {
    let typed: Vec<(u16, TypeTag, &[u32])> = columns
        .iter()
        .map(|&(id, cells)| (id, TypeTag::Fixed(4), cells))
        .collect();
    ingest_typed(any, keys, &typed);
}

/// [`ingest`] with each column's type named: a 4-byte cell per key.
fn ingest_typed(any: &AnyTree, keys: &[u32], columns: &[(u16, TypeTag, &[u32])]) {
    let entries: Vec<InternalValue> = keys
        .iter()
        .map(|&k| InternalValue::from_components(key(k), b"ignored", 0, ValueType::Value))
        .collect();
    let mut batch = entries_to_column_batch(&entries).expect("transpose");
    batch.columns.pop();
    for &(id, type_tag, cells) in columns {
        assert_eq!(keys.len(), cells.len());
        batch.columns.push(Column {
            column_id: id,
            type_tag,
            validity: None,
            data: cells
                .iter()
                .flat_map(|cell| cell.to_le_bytes())
                .collect::<Vec<u8>>()
                .into(),
        });
    }
    let mut ingestion = any.ingestion().expect("ingestion");
    ingestion.write_columnar_batch(&batch).expect("write");
    ingestion.finish().expect("finish");
}

/// Row `row` of a bytes column of `rows` rows: a `(rows + 1)`-entry
/// little-endian `u32` offset table, then the payload.
fn bytes_cell(data: &[u8], rows: u32, row: u32) -> Vec<u8> {
    let offset = |i: u32| {
        let at = i as usize * 4;
        u32::from_le_bytes(data[at..at + 4].try_into().expect("offset")) as usize
    };
    let payload = (rows as usize + 1) * 4;
    data[payload + offset(row)..payload + offset(row + 1)].to_vec()
}

/// The tree behind `any`.
fn standard(any: &AnyTree) -> &lsm_tree::Tree {
    let AnyTree::Standard(tree) = any else {
        panic!("expected a standard tree");
    };
    tree
}

/// A fixed-4 field with id `id` and the absence `absent`.
fn field(id: u16, absent: Absent) -> ProjectedField {
    ProjectedField::new(id, TypeTag::Fixed(4), absent).expect("field")
}

/// The projection of the key and the fields 3 and 4, where 4 reads as
/// `absent` in a row without it.
fn projection(absent: Absent) -> Projection {
    Projection::new()
        .column(COL_USER_KEY)
        .field(field(3, Absent::Error))
        .field(field(4, absent))
}

/// The rows the scan yields: each key with its field 4, `None` when null.
fn rows(
    tree: &lsm_tree::Tree,
    projection: &Projection,
) -> lsm_tree::Result<Vec<(Vec<u8>, Option<u32>)>> {
    let mut out = Vec::new();
    for batch in tree.columnar_scan(projection, None, SeqNo::MAX, ..)? {
        let batch: ColumnBatch = batch?;
        let ids: Vec<u16> = batch.columns.iter().map(|c| c.column_id).collect();
        assert_eq!(
            vec![COL_USER_KEY, 3, 4],
            ids,
            "the projection's columns, in its order"
        );
        let (keys, fourth) = (&batch.columns[0], &batch.columns[2]);
        for row in 0..batch.row_count {
            let key = bytes_cell(&keys.data, batch.row_count, row);
            let present = fourth
                .validity
                .as_ref()
                .is_none_or(|bits| bits[row as usize / 8] >> (row % 8) & 1 == 1);
            let at = row as usize * 4;
            let value = present.then(|| {
                u32::from_le_bytes(fourth.data[at..at + 4].try_into().expect("fixed-4 cell"))
            });
            out.push((key, value));
        }
    }
    Ok(out)
}

/// Segments over one key space, some written before field 4 existed: an
/// older one without it that a newer one with it overlaps (merged), and a
/// disjoint one without it (streamed on its own).
fn mixed_generations(any: &AnyTree) {
    ingest(any, &[0, 2], &[(3, &[10, 12])]);
    ingest(any, &[1], &[(3, &[11]), (4, &[41])]);
    ingest(any, &[10], &[(3, &[20])]);
}

#[test]
fn a_field_a_segment_lacks_reads_as_null_when_declared_nullable() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    mixed_generations(&any);
    assert_eq!(
        vec![
            (key(0), None),
            (key(1), Some(41)),
            (key(2), None),
            (key(10), None),
        ],
        rows(standard(&any), &projection(Absent::Null))?,
    );
    Ok(())
}

#[test]
fn a_field_a_segment_lacks_reads_as_its_default() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    mixed_generations(&any);
    let default = Absent::Default(Slice::from(&7u32.to_le_bytes()[..]));
    assert_eq!(
        vec![
            (key(0), Some(7)),
            (key(1), Some(41)),
            (key(2), Some(7)),
            (key(10), Some(7)),
        ],
        rows(standard(&any), &projection(default))?,
    );
    Ok(())
}

#[test]
fn a_field_a_segment_lacks_fails_the_scan_when_declared_required() {
    // Both paths refuse: the merged group and the segment streamed alone.
    for disjoint_only in [false, true] {
        let folder = get_tmp_folder();
        let any = open_columnar(folder.path());
        if disjoint_only {
            ingest(&any, &[10], &[(3, &[20])]);
        } else {
            ingest(&any, &[0, 2], &[(3, &[10, 12])]);
            ingest(&any, &[1], &[(3, &[11]), (4, &[41])]);
        }
        let got = rows(standard(&any), &projection(Absent::Error));
        assert!(
            matches!(got, Err(Error::Projection(_))),
            "a required field is missing, got {got:?}"
        );
    }
}

/// A required field an older, shadowed version lacks does not fail the scan:
/// only the rows returned are held to their declarations.
#[test]
fn a_shadowed_version_without_a_required_field_does_not_fail_the_scan() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    ingest(&any, &[0], &[(3, &[10])]);
    ingest(&any, &[0], &[(3, &[100]), (4, &[400])]);
    assert_eq!(
        vec![(key(0), Some(400))],
        rows(standard(&any), &projection(Absent::Error))?,
    );
    Ok(())
}

/// A predicate over a field some segments lack runs against the declared
/// default, exactly, on the merged group and on a segment streamed alone
/// alike: the rows it yields never depend on which segment a row came from.
#[test]
fn a_predicate_over_a_missing_field_runs_against_the_declared_default() -> lsm_tree::Result<()> {
    use lsm_tree::table::columnar::{ByteOrder, Number, NumberKind};
    use lsm_tree::table::columnar_predicate::{
        ColumnRangePredicate, PredicateApply, PredicateSupport,
    };

    // Field 4 is an ordered number, so a predicate over it runs.
    let u32_le = Number::new(NumberKind::Unsigned, 4, ByteOrder::Little)?;
    let number = TypeTag::Number(u32_le);
    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    // Older segments without field 4: one overlapped by a newer segment with
    // it (merged), one disjoint (streamed alone).
    ingest(&any, &[0, 2], &[(3, &[10, 12])]);
    ingest_typed(
        &any,
        &[1],
        &[(3, TypeTag::Fixed(4), &[11]), (4, number, &[41])],
    );
    ingest(&any, &[10], &[(3, &[20])]);

    let projection = Projection::new()
        .column(COL_USER_KEY)
        .field(field(3, Absent::Error))
        .field(ProjectedField::new(
            4,
            number,
            Absent::Default(Slice::from(&7u32.to_le_bytes()[..])),
        )?);
    // The rows a scan yields, each key with its field 4, and how far its
    // predicate ran.
    type Scanned = (Vec<(Vec<u8>, u32)>, Option<PredicateSupport>);
    let scan_equal_to = |value: u32| -> lsm_tree::Result<Scanned> {
        let bound = u32_le.comparable(&value.to_le_bytes())?;
        let predicate = ColumnRangePredicate {
            column_id: 4,
            lower: Some(bound.clone()),
            upper: Some(bound),
            apply: PredicateApply::Filter,
        };
        let mut scan =
            standard(&any).columnar_scan(projection.clone(), Some(&predicate), SeqNo::MAX, ..)?;
        let mut got = Vec::new();
        for batch in &mut scan {
            let batch = batch?;
            let (keys, fourth) = (&batch.columns[0], &batch.columns[2]);
            for row in 0..batch.row_count {
                let at = row as usize * 4;
                got.push((
                    bytes_cell(&keys.data, batch.row_count, row),
                    u32::from_le_bytes(fourth.data[at..at + 4].try_into().expect("u32 cell")),
                ));
            }
        }
        Ok((got, scan.predicate_support()))
    };
    // The default matches: the rows without the field, from the merged group
    // and from the segment streamed alone alike.
    assert_eq!(
        (
            vec![(key(0), 7), (key(2), 7), (key(10), 7)],
            Some(PredicateSupport::Exact),
        ),
        scan_equal_to(7)?,
    );
    // The default does not match: no row without the field comes back, from
    // either path.
    assert_eq!(
        (vec![(key(1), 41)], Some(PredicateSupport::Exact)),
        scan_equal_to(41)?,
    );
    Ok(())
}

/// A column projected by id alone that an older, fully shadowed segment lacks
/// does not fail the scan; a row returned without it does.
#[test]
fn a_shadowed_version_without_a_column_projected_by_id_does_not_fail_the_scan()
-> lsm_tree::Result<()> {
    let by_id = Projection::new().column(COL_USER_KEY).column(4);
    let fourth = |tree: &lsm_tree::Tree| -> lsm_tree::Result<Vec<(Vec<u8>, u32)>> {
        let mut got = Vec::new();
        for batch in tree.columnar_scan(&by_id, None, SeqNo::MAX, ..)? {
            let batch = batch?;
            let (keys, fourth) = (&batch.columns[0], &batch.columns[1]);
            for row in 0..batch.row_count {
                let at = row as usize * 4;
                got.push((
                    bytes_cell(&keys.data, batch.row_count, row),
                    u32::from_le_bytes(fourth.data[at..at + 4].try_into().expect("u32 cell")),
                ));
            }
        }
        Ok(got)
    };

    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    ingest(&any, &[0], &[(3, &[10])]);
    ingest(&any, &[0], &[(3, &[100]), (4, &[400])]);
    assert_eq!(vec![(key(0), 400)], fourth(standard(&any))?);

    // The newest version lacks it: the row returned is refused.
    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    ingest(&any, &[0], &[(3, &[10]), (4, &[40])]);
    ingest(&any, &[0], &[(3, &[100])]);
    let got = fourth(standard(&any));
    assert!(matches!(got, Err(Error::Projection(_))), "got {got:?}");
    Ok(())
}

/// A column projected by id that only some merged segments carry is held
/// only to the rows the predicate returns: rows without it that the
/// predicate drops fail nothing, and neither does an output it empties.
#[test]
fn a_column_projected_by_id_is_held_only_to_the_rows_the_predicate_returns() -> lsm_tree::Result<()>
{
    use lsm_tree::table::columnar::{COL_SEQNO, Number};
    use lsm_tree::table::columnar_predicate::{ColumnRangePredicate, PredicateApply};

    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    // An older segment without column 4 around a newer one with it: one
    // merged output takes rows of both.
    ingest(&any, &[0, 2], &[(3, &[10, 12])]);
    ingest(&any, &[1], &[(3, &[11]), (4, &[41])]);
    let tree = standard(&any);
    let newest = tree.get_highest_seqno().expect("ingested");
    let by_id = Projection::new().column(COL_USER_KEY).column(4);
    let scan_seqnos = |lower: u64, upper: u64| -> lsm_tree::Result<Vec<(Vec<u8>, u32)>> {
        let predicate = ColumnRangePredicate {
            column_id: COL_SEQNO,
            lower: Some(Number::U64_LE.comparable(&lower.to_le_bytes())?),
            upper: Some(Number::U64_LE.comparable(&upper.to_le_bytes())?),
            apply: PredicateApply::Filter,
        };
        let mut got = Vec::new();
        for batch in tree.columnar_scan(&by_id, Some(&predicate), SeqNo::MAX, ..)? {
            let batch = batch?;
            let (keys, fourth) = (&batch.columns[0], &batch.columns[1]);
            for row in 0..batch.row_count {
                let at = row as usize * 4;
                got.push((
                    bytes_cell(&keys.data, batch.row_count, row),
                    u32::from_le_bytes(fourth.data[at..at + 4].try_into().expect("u32 cell")),
                ));
            }
        }
        Ok(got)
    };
    // Only the newer segment's row passes.
    assert_eq!(vec![(key(1), 41)], scan_seqnos(newest, newest)?);
    // No row passes.
    assert_eq!(
        Vec::<(Vec<u8>, u32)>::new(),
        scan_seqnos(newest + 10, newest + 20)?
    );
    Ok(())
}

/// A newer version written without field 4 does not take the older
/// version's field 4: absence reads as declared, not by inheritance.
#[test]
fn a_newer_version_without_a_field_does_not_inherit_the_older_value() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    ingest(&any, &[0], &[(3, &[10]), (4, &[40])]);
    ingest(&any, &[0], &[(3, &[100])]);
    assert_eq!(
        vec![(key(0), None)],
        rows(standard(&any), &projection(Absent::Null))?,
    );
    Ok(())
}

/// Reads fields 3 and 4 out of a row value framed from those two fixed-4
/// cells, the form a read returns for a row of a two-column segment.
struct TwoCells;

impl ValueProjector for TwoCells {
    fn project(
        &self,
        _key: &[u8],
        value: &[u8],
        row: &mut ProjectedRow<'_>,
    ) -> lsm_tree::Result<()> {
        let cells = unframe_value_cells(value, &[TypeTag::Fixed(4), TypeTag::Fixed(4)])?;
        for (index, id) in row
            .fields()
            .iter()
            .map(ProjectedField::column_id)
            .enumerate()
            .collect::<Vec<_>>()
        {
            if let Some(cell) = id.checked_sub(3).and_then(|at| cells.get(usize::from(at))) {
                row.set(index, cell)?;
            }
        }
        Ok(())
    }
}

/// A compaction that rewrites segments whose value was split into columns
/// folds those columns into one value; a field is still read out of it through
/// the projector instead of reading as absent.
#[test]
fn a_field_folded_into_the_value_by_a_compaction_is_read_through_the_projector()
-> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    ingest(&any, &[0, 2], &[(3, &[10, 12]), (4, &[40, 42])]);
    ingest(&any, &[1], &[(3, &[11]), (4, &[41])]);
    let tree = standard(&any);
    tree.major_compact(64 * 1024 * 1024, 0)?;

    let projection = projection(Absent::Null).projector(std::sync::Arc::new(TwoCells));
    assert_eq!(
        vec![(key(0), Some(40)), (key(1), Some(41)), (key(2), Some(42))],
        rows(tree, &projection)?,
    );
    Ok(())
}

/// Rows flushed into a columnar tree keep each value whole; a declared field
/// lies inside it, which only a projector reads, so a scan without one is
/// refused rather than reading the field as absent, once such a row is
/// returned.
#[test]
fn declared_fields_over_whole_values_without_a_projector_are_refused() {
    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    let tree = standard(&any);
    tree.insert(key(0), 7u32.to_le_bytes(), 0);
    tree.flush_active_memtable(0).expect("flush");

    let got = rows(tree, &projection(Absent::Null));
    assert!(matches!(got, Err(Error::Projection(_))), "got {got:?}");
    // By id the value column still reads as stored.
    let by_id = tree
        .columnar_scan(&[COL_USER_KEY, 3], None, SeqNo::MAX, ..)
        .and_then(|scan| scan.collect::<lsm_tree::Result<Vec<_>>>());
    assert!(by_id.is_ok(), "got {by_id:?}");
}

/// A row value holding fields 3 and 4, in the form a read of a two-column
/// segment returns and `TwoCells` reads.
fn row_value(third: u32, fourth: u32) -> Vec<u8> {
    frame_value_cells(&[
        (TypeTag::Fixed(4), &third.to_le_bytes()[..]),
        (TypeTag::Fixed(4), &fourth.to_le_bytes()[..]),
    ])
    .expect("frame")
}

/// The projection of the key and fields 3 and 4, read through `TwoCells`.
fn projected() -> Projection {
    projection(Absent::Null).projector(std::sync::Arc::new(TwoCells))
}

/// An update in the memtable over a columnar base is what the scan returns,
/// read through the projector, as a read returns it.
#[test]
fn an_update_in_the_memtable_over_a_columnar_base_is_returned() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    ingest(&any, &[0, 1], &[(3, &[10, 11]), (4, &[40, 41])]);
    let tree = standard(&any);
    let seqno = tree.get_highest_seqno().map_or(0, |s| s + 1);
    tree.insert(key(0), row_value(100, 400), seqno);
    assert_eq!(
        vec![(key(0), Some(400)), (key(1), Some(41))],
        rows(tree, &projected())?,
    );
    Ok(())
}

/// A deletion in the memtable over a columnar base hides the key, as a read
/// reports it absent.
#[test]
fn a_delete_in_the_memtable_over_a_columnar_base_hides_the_key() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    ingest(&any, &[0, 1], &[(3, &[10, 11]), (4, &[40, 41])]);
    let tree = standard(&any);
    let seqno = tree.get_highest_seqno().map_or(0, |s| s + 1);
    tree.remove(key(0), seqno);
    assert_eq!(vec![(key(1), Some(41))], rows(tree, &projected())?);
    Ok(())
}

/// A range tombstone spanning a columnar segment, a row segment and memtable
/// rows removes the keys of all three that it is newer than.
#[test]
fn a_range_tombstone_spanning_both_layouts_removes_their_keys() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    let tree = standard(&any);
    // A row segment, written with the columnar layout off.
    tree.update_runtime_config(|cfg| cfg.columnar = false)?;
    tree.insert(key(1), row_value(11, 41), 1);
    tree.flush_active_memtable(0)?;
    tree.update_runtime_config(|cfg| cfg.columnar = true)?;
    ingest(&any, &[0, 2, 4], &[(3, &[10, 12, 14]), (4, &[40, 42, 44])]);
    let seqno = tree.get_highest_seqno().map_or(0, |s| s + 1);
    tree.insert(key(3), row_value(13, 43), seqno);
    tree.remove_range(key(0), key(4), seqno + 1);
    // A row written after the deletion is not covered by it.
    tree.insert(key(2), row_value(102, 402), seqno + 2);
    assert_eq!(
        vec![(key(2), Some(402)), (key(4), Some(44))],
        rows(tree, &projected())?,
    );
    Ok(())
}

/// The same rows read the same whether they sit in the memtable, a row
/// segment or a columnar segment.
#[test]
fn the_same_rows_read_the_same_from_every_source() -> lsm_tree::Result<()> {
    let expected = vec![(key(0), Some(40)), (key(1), Some(41))];

    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    let tree = standard(&any);
    tree.insert(key(0), row_value(10, 40), 1);
    tree.insert(key(1), row_value(11, 41), 2);
    assert_eq!(expected, rows(tree, &projected())?, "from the memtable");

    tree.update_runtime_config(|cfg| cfg.columnar = false)?;
    tree.flush_active_memtable(0)?;
    assert_eq!(expected, rows(tree, &projected())?, "from a row segment");

    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    ingest(&any, &[0, 1], &[(3, &[10, 11]), (4, &[40, 41])]);
    assert_eq!(
        expected,
        rows(standard(&any), &projected())?,
        "from a columnar segment"
    );
    Ok(())
}

/// Adds its operand to field 4 of a value framed as `row_value` frames it,
/// keeping field 3; a missing base counts as both fields zero.
struct AddToFourth;

impl lsm_tree::MergeOperator for AddToFourth {
    fn merge(
        &self,
        _key: &[u8],
        base: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> lsm_tree::Result<lsm_tree::UserValue> {
        let fixed = |cell: &[u8]| u32::from_le_bytes(cell.try_into().expect("fixed-4"));
        let (third, mut fourth) = match base {
            Some(base) => {
                let cells = unframe_value_cells(base, &[TypeTag::Fixed(4), TypeTag::Fixed(4)])?;
                (fixed(cells[0]), fixed(cells[1]))
            }
            None => (0, 0),
        };
        for operand in operands {
            fourth += fixed(operand);
        }
        Ok(row_value(third, fourth).into())
    }
}

/// A merge chain whose base is a columnar row split into fields and whose
/// operand is a memtable row returns the merged value's fields, as a read
/// returns the merged value, not the raw operand.
#[test]
fn a_merge_chain_over_a_columnar_base_returns_the_merged_fields() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(std::sync::Arc::new(AddToFourth)))
    .open()?;
    standard(&any).update_runtime_config(|cfg| cfg.columnar = true)?;
    ingest(&any, &[0, 1], &[(3, &[10, 11]), (4, &[40, 41])]);
    let tree = standard(&any);
    let seqno = tree.get_highest_seqno().map_or(0, |s| s + 1);
    tree.merge(key(0), 2u32.to_le_bytes(), seqno);
    assert_eq!(
        vec![(key(0), Some(42)), (key(1), Some(41))],
        rows(tree, &projected())?,
    );
    Ok(())
}

/// A version newer than the snapshot is never handed to the projector, so one
/// it cannot read does not fail a scan of an older snapshot; on a table read
/// alone and on overlapping tables merged alike.
#[test]
fn a_version_newer_than_the_snapshot_is_not_projected() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    let tree = standard(&any);
    tree.insert(key(0), row_value(10, 40), 1);
    tree.insert(key(0), b"not two cells".to_vec(), 4);
    tree.flush_active_memtable(0)?;
    let scan_at = |snapshot: SeqNo| -> lsm_tree::Result<Vec<(Vec<u8>, u32)>> {
        let mut got = Vec::new();
        for batch in tree.columnar_scan(projected(), None, snapshot, ..)? {
            let batch = batch?;
            let (keys, fourth) = (&batch.columns[0], &batch.columns[2]);
            for row in 0..batch.row_count {
                let at = row as usize * 4;
                got.push((
                    bytes_cell(&keys.data, batch.row_count, row),
                    u32::from_le_bytes(fourth.data[at..at + 4].try_into().expect("u32 cell")),
                ));
            }
        }
        Ok(got)
    };
    assert_eq!(vec![(key(0), 40)], scan_at(3)?, "a table read alone");

    // A second table over the same keys, also newer than the snapshot in
    // part: the two are merged.
    tree.insert(key(1), row_value(11, 41), 2);
    tree.insert(key(1), b"not two cells".to_vec(), 5);
    tree.flush_active_memtable(0)?;
    assert_eq!(
        vec![(key(0), 40), (key(1), 41)],
        scan_at(3)?,
        "overlapping tables merged"
    );
    Ok(())
}

/// Without a merge operator a read returns an operand's bytes as the value,
/// so the scan reads its fields through the projector the same way.
#[test]
fn an_operand_without_a_merge_operator_is_projected_as_the_read_returns_it() -> lsm_tree::Result<()>
{
    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    ingest(&any, &[0, 1], &[(3, &[10, 11]), (4, &[40, 41])]);
    let tree = standard(&any);
    let seqno = tree.get_highest_seqno().map_or(0, |s| s + 1);
    tree.merge(key(0), row_value(100, 400), seqno);
    assert_eq!(
        Some(row_value(100, 400).into()),
        tree.get(key(0), SeqNo::MAX)?,
        "a read returns the operand"
    );
    assert_eq!(
        vec![(key(0), Some(400)), (key(1), Some(41))],
        rows(tree, &projected())?,
    );
    Ok(())
}

/// A merge chain is resolved in the version the scan started on: clearing the
/// tree while the scan is open changes nothing it returns.
#[test]
fn a_merge_chain_is_resolved_in_the_version_the_scan_started_on() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(std::sync::Arc::new(AddToFourth)))
    .open()?;
    standard(&any).update_runtime_config(|cfg| cfg.columnar = true)?;
    ingest(&any, &[0, 1], &[(3, &[10, 11]), (4, &[40, 41])]);
    let tree = standard(&any);
    let seqno = tree.get_highest_seqno().map_or(0, |s| s + 1);
    tree.merge(key(0), 2u32.to_le_bytes(), seqno);

    let scan = tree.columnar_scan(projected(), None, SeqNo::MAX, ..)?;
    tree.clear()?;
    let mut got = Vec::new();
    for batch in scan {
        let batch = batch?;
        let (keys, fourth) = (&batch.columns[0], &batch.columns[2]);
        for row in 0..batch.row_count {
            let at = row as usize * 4;
            got.push((
                bytes_cell(&keys.data, batch.row_count, row),
                u32::from_le_bytes(fourth.data[at..at + 4].try_into().expect("u32 cell")),
            ));
        }
    }
    assert_eq!(vec![(key(0), 42), (key(1), 41)], got);
    Ok(())
}

/// The scan reads the version it started on: a compaction that rewrites the
/// segments while the scan is open, into another layout, changes nothing it
/// returns.
#[test]
fn a_compaction_during_the_scan_changes_nothing_it_returns() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    ingest(&any, &[0, 2], &[(3, &[10, 12]), (4, &[40, 42])]);
    ingest(&any, &[1], &[(3, &[11]), (4, &[41])]);
    let tree = standard(&any);
    let seqno = tree.get_highest_seqno().map_or(0, |s| s + 1);
    tree.insert(key(3), row_value(13, 43), seqno);

    let scan = tree.columnar_scan(projected(), None, SeqNo::MAX, ..)?;
    tree.flush_active_memtable(0)?;
    tree.major_compact(64 * 1024 * 1024, 0)?;
    let mut got = Vec::new();
    for batch in scan {
        let batch = batch?;
        got.push(batch.row_count);
    }
    assert_eq!(
        4,
        got.iter().sum::<u32>(),
        "every row of the version scanned"
    );
    assert_eq!(
        vec![
            (key(0), Some(40)),
            (key(1), Some(41)),
            (key(2), Some(42)),
            (key(3), Some(43)),
        ],
        rows(tree, &projected())?,
        "the compacted layout reads the same",
    );
    Ok(())
}

/// A row source's declared fields are read through the projector, so a scan
/// without one is refused rather than reading them as absent, once such a row
/// is returned.
#[test]
fn declared_fields_over_memtable_rows_without_a_projector_are_refused() {
    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    let tree = standard(&any);
    tree.insert(key(0), row_value(10, 40), 0);
    let got = rows(tree, &projection(Absent::Null));
    assert!(matches!(got, Err(Error::Projection(_))), "got {got:?}");
}

/// Whole values a newer split table shadows, and a flushed table holding
/// only deletions, are never returned, so a scan of split tables needs no
/// projector because of them; and a shadowed whole value the projector
/// cannot read is not handed to it.
#[test]
fn shadowed_or_deleted_whole_values_need_no_projector() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    let tree = standard(&any);
    // Older whole values, one the projector cannot read.
    let mut ingestion = any.ingestion()?;
    ingestion.write(key(0), b"not two cells".to_vec())?;
    ingestion.write(key(1), row_value(11, 41))?;
    ingestion.finish()?;
    // A newer split table over both keys.
    ingest(&any, &[0, 1], &[(3, &[100, 101]), (4, &[400, 401])]);
    assert_eq!(
        vec![(key(0), Some(400)), (key(1), Some(401))],
        rows(tree, &projection(Absent::Null))?,
        "shadowed, no projector"
    );
    assert_eq!(
        vec![(key(0), Some(400)), (key(1), Some(401))],
        rows(tree, &projected())?,
        "shadowed, with a projector that cannot read one of them"
    );

    // A table holding only a deletion over the split one.
    let mut ingestion = any.ingestion()?;
    ingestion.write_tombstone(key(0))?;
    ingestion.finish()?;
    assert_eq!(
        vec![(key(1), Some(401))],
        rows(tree, &projection(Absent::Null))?,
        "a flushed deletion"
    );
    Ok(())
}

/// A merge operand ingested into a split table is resolved as a read resolves
/// it, and its fields read out of the merged value.
#[test]
fn an_operand_in_a_split_table_is_resolved_as_the_read_resolves_it() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(std::sync::Arc::new(AddToFourth)))
    .open()?;
    standard(&any).update_runtime_config(|cfg| cfg.columnar = true)?;
    let tree = standard(&any);
    tree.insert(key(0), row_value(10, 40), 1);
    tree.flush_active_memtable(0)?;
    // A split batch whose row is an operand.
    let entries = [InternalValue::from_components(
        key(0),
        b"ignored",
        0,
        ValueType::MergeOperand,
    )];
    let mut batch = entries_to_column_batch(&entries).expect("transpose");
    batch.columns.pop();
    batch.columns.push(Column {
        column_id: 3,
        type_tag: TypeTag::Fixed(4),
        validity: None,
        data: 2u32.to_le_bytes().to_vec().into(),
    });
    let mut ingestion = any.ingestion()?;
    ingestion.write_columnar_batch(&batch)?;
    ingestion.finish()?;

    let read = tree.get(key(0), SeqNo::MAX)?.expect("the key is present");
    let cells = unframe_value_cells(&read, &[TypeTag::Fixed(4), TypeTag::Fixed(4)])?;
    let fourth = u32::from_le_bytes(cells[1].try_into().expect("fixed-4"));
    assert_eq!(vec![(key(0), Some(fourth))], rows(tree, &projected())?);
    Ok(())
}

/// A version a newer one shadows is never returned, so a field it stores
/// under another type than declared does not fail the scan; returned, it
/// does.
#[test]
fn a_shadowed_version_storing_a_field_under_another_type_does_not_fail_the_scan()
-> lsm_tree::Result<()> {
    use lsm_tree::table::columnar::{ByteOrder, Number, NumberKind};

    let number = TypeTag::Number(Number::new(NumberKind::Unsigned, 4, ByteOrder::Little)?);
    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    ingest_typed(
        &any,
        &[0, 1],
        &[(3, TypeTag::Fixed(4), &[10, 11]), (4, number, &[40, 41])],
    );
    ingest(&any, &[0], &[(3, &[100]), (4, &[400])]);
    let got = rows(standard(&any), &projection(Absent::Null));
    assert!(
        matches!(got, Err(Error::Projection(_))),
        "key 1 is returned from the mistyped version, got {got:?}"
    );

    ingest(&any, &[1], &[(3, &[101]), (4, &[401])]);
    assert_eq!(
        vec![(key(0), Some(400)), (key(1), Some(401))],
        rows(standard(&any), &projection(Absent::Null))?,
        "every mistyped version is shadowed"
    );
    Ok(())
}

/// The same on a segment streamed alone: rows of it a predicate filters out
/// are not returned, so the field it stores under another type does not fail
/// the scan.
#[test]
fn filtered_out_rows_storing_a_field_under_another_type_do_not_fail_the_scan()
-> lsm_tree::Result<()> {
    use lsm_tree::table::columnar::{ByteOrder, Number, NumberKind};
    use lsm_tree::table::columnar_predicate::{ColumnRangePredicate, PredicateApply};

    let u32_le = Number::new(NumberKind::Unsigned, 4, ByteOrder::Little)?;
    let number = TypeTag::Number(u32_le);
    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    ingest_typed(
        &any,
        &[0, 1],
        &[(3, number, &[10, 11]), (4, number, &[40, 41])],
    );
    // Field 3 is stored as declared and judged by the predicate once the
    // batch is read, which keeps no row; field 4 is stored under another type.
    let judged = Projection::new()
        .column(COL_USER_KEY)
        .field(ProjectedField::new(3, number, Absent::Null)?)
        .field(field(4, Absent::Null));
    let bound = u32_le.comparable(&1_000u32.to_le_bytes())?;
    let predicate = ColumnRangePredicate {
        column_id: 3,
        lower: Some(bound.clone()),
        upper: Some(bound),
        apply: PredicateApply::Filter,
    };
    let mut returned = 0;
    for batch in standard(&any).columnar_scan(&judged, Some(&predicate), SeqNo::MAX, ..)? {
        returned += batch?.row_count;
    }
    assert_eq!(0, returned);
    assert!(
        matches!(
            rows(standard(&any), &projection(Absent::Null)),
            Err(Error::Projection(_))
        ),
        "returned, the rows fail the scan"
    );
    Ok(())
}

/// A predicate over a column the values do not hold, here the seqno, runs
/// before any value is read: a row it filters out is never handed to the
/// projector, so a value the projector cannot read does not fail the scan.
#[test]
fn a_row_the_predicate_filters_out_is_not_projected() -> lsm_tree::Result<()> {
    use lsm_tree::table::columnar::{COL_SEQNO, Number};
    use lsm_tree::table::columnar_predicate::{ColumnRangePredicate, PredicateApply};

    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    let tree = standard(&any);
    tree.insert(key(0), row_value(10, 40), 0);
    tree.insert(key(1), b"not two cells".to_vec(), 1);
    let bound = Number::U64_LE.comparable(&0u64.to_le_bytes())?;
    let predicate = ColumnRangePredicate {
        column_id: COL_SEQNO,
        lower: Some(bound.clone()),
        upper: Some(bound),
        apply: PredicateApply::Filter,
    };
    let mut got = Vec::new();
    for batch in tree.columnar_scan(projected(), Some(&predicate), SeqNo::MAX, ..)? {
        let batch = batch?;
        for row in 0..batch.row_count {
            got.push(bytes_cell(&batch.columns[0].data, batch.row_count, row));
        }
    }
    assert_eq!(vec![key(0)], got);
    Ok(())
}

/// A scan at the latest snapshot reads what the tree held when it was
/// created: a write that lands in the memtable afterwards, whose key lies past
/// every key the scan grouped, is not returned, nor is a deletion that lands
/// afterwards applied.
#[test]
fn a_write_after_the_scan_was_created_is_not_returned() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    let tree = standard(&any);
    // The ingestion seals the memtable it finds, so the row that makes the
    // active memtable a segment of the scan is written after it.
    ingest(&any, &[1], &[(3, &[11]), (4, &[41])]);
    let seqno = tree.get_highest_seqno().map_or(0, |s| s + 1);
    tree.insert(key(0), row_value(10, 40), seqno);

    let scan = tree.columnar_scan(projected(), None, SeqNo::MAX, ..)?;
    let seqno = tree.get_highest_seqno().map_or(0, |s| s + 1);
    tree.insert(key(5), row_value(15, 45), seqno);
    tree.remove(key(1), seqno + 1);
    let mut got = Vec::new();
    for batch in scan {
        let batch = batch?;
        for row in 0..batch.row_count {
            got.push(bytes_cell(&batch.columns[0].data, batch.row_count, row));
        }
    }
    assert_eq!(vec![key(0), key(1)], got);
    Ok(())
}

/// A fixed-width field of width zero has no cell to hold, and a column of it
/// is one no batch may carry, so declaring one is refused up front instead of
/// failing, or panicking, once rows are projected into it.
#[test]
fn a_zero_width_field_is_refused() {
    for absent in [
        Absent::Null,
        Absent::Error,
        Absent::Default(Slice::from(&[][..])),
    ] {
        let got = ProjectedField::new(5, TypeTag::Fixed(0), absent.clone());
        assert!(
            matches!(got, Err(Error::Projection(_))),
            "{absent:?}: got {got:?}"
        );
    }
}

/// A tree merging with [`AddToFourth`] whose key 0 is a flushed whole value
/// `row_value(10, 40)` under a split table holding an operand for it: one
/// column `id` of type `type_tag` whose cell is 2.
fn split_operand_over_a_whole_value(
    folder: &std::path::Path,
    id: u16,
    type_tag: TypeTag,
) -> lsm_tree::Result<AnyTree> {
    let any = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(std::sync::Arc::new(AddToFourth)))
    .open()?;
    standard(&any).update_runtime_config(|cfg| cfg.columnar = true)?;
    let tree = standard(&any);
    tree.insert(key(0), row_value(10, 40), 1);
    tree.flush_active_memtable(0)?;
    let entries = [InternalValue::from_components(
        key(0),
        b"ignored",
        0,
        ValueType::MergeOperand,
    )];
    let mut batch = entries_to_column_batch(&entries).expect("transpose");
    batch.columns.pop();
    batch.columns.push(Column {
        column_id: id,
        type_tag,
        validity: None,
        data: 2u32.to_le_bytes().to_vec().into(),
    });
    let mut ingestion = any.ingestion()?;
    ingestion.write_columnar_batch(&batch)?;
    ingestion.finish()?;
    Ok(any)
}

/// A resolved operand is the merged value, read whole: a field projected by
/// id alone cannot be read out of it, as out of any whole value, so the scan
/// refuses it instead of returning the operand's own cell.
#[test]
fn a_field_projected_by_id_is_not_read_off_a_resolved_operand() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let any = split_operand_over_a_whole_value(folder.path(), 3, TypeTag::Fixed(4))?;
    let projection = Projection::new().column(COL_USER_KEY).column(3);
    let mut got = Vec::new();
    let outcome = (|| -> lsm_tree::Result<()> {
        for batch in standard(&any).columnar_scan(&projection, None, SeqNo::MAX, ..)? {
            let batch = batch?;
            let third = &batch.columns[1];
            for row in 0..batch.row_count {
                let at = row as usize * 4;
                got.push(third.data[at..at + 4].to_vec());
            }
        }
        Ok(())
    })();
    assert!(
        matches!(outcome, Err(Error::Projection(_))),
        "got {outcome:?} with cells {got:?}"
    );
    Ok(())
}

/// A predicate over a column no field declares cannot judge a resolved
/// operand, whose merged value is read whole: the row comes back and the
/// predicate reports it did not run, instead of judging the operand's cell.
#[test]
fn a_predicate_over_an_undeclared_column_does_not_judge_a_resolved_operand() -> lsm_tree::Result<()>
{
    use lsm_tree::table::columnar::{ByteOrder, Number, NumberKind};
    use lsm_tree::table::columnar_predicate::PredicateSupport;

    let number = TypeTag::Number(Number::new(NumberKind::Unsigned, 4, ByteOrder::Little)?);
    let folder = get_tmp_folder();
    let any = split_operand_over_a_whole_value(folder.path(), 5, number)?;
    let (keys, support) = keys_filtered_on_five(standard(&any), projected(), 2)?;
    assert_eq!(Some(PredicateSupport::Unsupported), support);
    assert_eq!(vec![key(0)], keys);
    Ok(())
}

/// A memtable holding only deletions has no value to read a field out of, so a
/// scan of split tables under it needs no projector: the deletions apply.
#[test]
fn deletions_in_the_memtable_need_no_projector() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    ingest(
        &any,
        &[0, 1, 2, 3],
        &[(3, &[10, 11, 12, 13]), (4, &[40, 41, 42, 43])],
    );
    let tree = standard(&any);
    let seqno = tree.get_highest_seqno().map_or(0, |s| s + 1);
    tree.remove_range(key(0), key(2), seqno);
    assert_eq!(
        vec![(key(2), Some(42)), (key(3), Some(43))],
        rows(tree, &projection(Absent::Null))?,
        "a range deletion alone"
    );
    // A value written and then deleted is shadowed within the memtable.
    tree.insert(key(3), row_value(103, 403), seqno + 1);
    tree.remove(key(3), seqno + 2);
    assert_eq!(
        vec![(key(2), Some(42))],
        rows(tree, &projection(Absent::Null))?,
        "a point deletion over a shadowed value"
    );
    Ok(())
}

/// The keys a scan with `predicate` over column 5 yields, and how far the
/// predicate ran.
fn keys_filtered_on_five(
    tree: &lsm_tree::Tree,
    projection: Projection,
    value: u32,
) -> lsm_tree::Result<(
    Vec<Vec<u8>>,
    Option<lsm_tree::table::columnar_predicate::PredicateSupport>,
)> {
    use lsm_tree::table::columnar::{ByteOrder, Number, NumberKind};
    use lsm_tree::table::columnar_predicate::{ColumnRangePredicate, PredicateApply};

    let bound = Number::new(NumberKind::Unsigned, 4, ByteOrder::Little)?
        .comparable(&value.to_le_bytes())?;
    let predicate = ColumnRangePredicate {
        column_id: 5,
        lower: Some(bound.clone()),
        upper: Some(bound),
        apply: PredicateApply::Filter,
    };
    let mut scan = tree.columnar_scan(projection, Some(&predicate), SeqNo::MAX, ..)?;
    let mut keys = Vec::new();
    for batch in &mut scan {
        let batch = batch?;
        let at = batch
            .columns
            .iter()
            .position(|c| c.column_id == COL_USER_KEY)
            .expect("key column");
        for row in 0..batch.row_count {
            keys.push(bytes_cell(&batch.columns[at].data, batch.row_count, row));
        }
    }
    Ok((keys, scan.predicate_support()))
}

/// A predicate over a column no field declares, which one merged segment was
/// written without, does not fail the merge: the rows lacking it cannot be
/// judged, so they come back and the predicate reports it did not run.
#[test]
fn a_predicate_over_an_undeclared_column_one_merged_segment_lacks_is_not_run()
-> lsm_tree::Result<()> {
    use lsm_tree::table::columnar::{ByteOrder, Number, NumberKind};
    use lsm_tree::table::columnar_predicate::PredicateSupport;

    let number = TypeTag::Number(Number::new(NumberKind::Unsigned, 4, ByteOrder::Little)?);
    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    ingest_typed(
        &any,
        &[0, 2],
        &[(3, TypeTag::Fixed(4), &[10, 12]), (5, number, &[50, 52])],
    );
    ingest(&any, &[1], &[(3, &[11])]);
    let projection = Projection::new().column(COL_USER_KEY).column(3);
    let (keys, support) = keys_filtered_on_five(standard(&any), projection, 50)?;
    // Which of the rows carrying the column are judged depends on where the
    // merge cuts its output; the matching one and the one lacking it are
    // returned either way.
    assert_eq!(Some(PredicateSupport::Unsupported), support);
    assert!(
        keys.contains(&key(0)) && keys.contains(&key(1)),
        "got {keys:?}"
    );
    Ok(())
}

/// The same over a memtable row merged with a columnar segment holding the
/// column: the row's value is whole, so the undeclared column is not read
/// out of it and the predicate reports it did not run.
#[test]
fn a_predicate_over_an_undeclared_column_across_a_memtable_row_is_not_run() -> lsm_tree::Result<()>
{
    use lsm_tree::table::columnar::{ByteOrder, Number, NumberKind};
    use lsm_tree::table::columnar_predicate::PredicateSupport;

    let number = TypeTag::Number(Number::new(NumberKind::Unsigned, 4, ByteOrder::Little)?);
    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    ingest_typed(
        &any,
        &[0, 1, 2],
        &[
            (3, TypeTag::Fixed(4), &[10, 11, 12]),
            (4, TypeTag::Fixed(4), &[40, 41, 42]),
            (5, number, &[50, 51, 52]),
        ],
    );
    let tree = standard(&any);
    let seqno = tree.get_highest_seqno().map_or(0, |s| s + 1);
    // Memtable rows on both sides of a columnar one: one output takes rows
    // of both layouts.
    tree.insert(key(0), row_value(100, 400), seqno);
    tree.insert(key(2), row_value(102, 402), seqno + 1);
    assert_eq!(
        (
            vec![key(0), key(1), key(2)],
            Some(PredicateSupport::Unsupported)
        ),
        keys_filtered_on_five(tree, projected(), 51)?,
    );
    Ok(())
}

/// A column id named twice (a declared field and the raw value by id, in
/// either order) leaves the output with two columns under one id, so the scan
/// is refused rather than picking one by position.
#[test]
fn a_column_id_projected_twice_is_refused() {
    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    let tree = standard(&any);
    tree.insert(key(0), row_value(10, 40), 0);
    tree.flush_active_memtable(0).expect("flush");
    let twice = [
        Projection::new()
            .field(field(3, Absent::Null))
            .column(3)
            .projector(std::sync::Arc::new(TwoCells)),
        Projection::new()
            .column(3)
            .field(field(3, Absent::Null))
            .projector(std::sync::Arc::new(TwoCells)),
    ];
    for projection in twice {
        let got = tree.columnar_scan(&projection, None, SeqNo::MAX, ..).err();
        assert!(matches!(got, Some(Error::Projection(_))), "got {got:?}");
    }
}

/// A segment that stores a declared field under another type is refused, not
/// misread.
#[test]
fn a_field_stored_under_another_type_fails_the_scan() {
    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    ingest(&any, &[0], &[(3, &[10]), (4, &[40])]);
    let projection = Projection::new()
        .field(ProjectedField::new(4, TypeTag::Fixed(8), Absent::Null).expect("field"));
    let got = standard(&any)
        .columnar_scan(&projection, None, SeqNo::MAX, ..)
        .and_then(|scan| scan.collect::<lsm_tree::Result<Vec<_>>>());
    assert!(matches!(got, Err(Error::Projection(_))), "got {got:?}");
}

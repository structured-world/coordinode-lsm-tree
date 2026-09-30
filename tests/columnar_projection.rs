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
    let entries: Vec<InternalValue> = keys
        .iter()
        .map(|&k| InternalValue::from_components(key(k), b"ignored", 0, ValueType::Value))
        .collect();
    let mut batch = entries_to_column_batch(&entries).expect("transpose");
    batch.columns.pop();
    for &(id, cells) in columns {
        assert_eq!(keys.len(), cells.len());
        batch.columns.push(Column {
            column_id: id,
            type_tag: TypeTag::Fixed(4),
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
/// refused rather than reading the field as absent.
#[test]
fn declared_fields_over_whole_values_without_a_projector_are_refused() {
    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    let tree = standard(&any);
    tree.insert(key(0), 7u32.to_le_bytes(), 0);
    tree.flush_active_memtable(0).expect("flush");

    let got = tree
        .columnar_scan(projection(Absent::Null), None, SeqNo::MAX, ..)
        .err();
    assert!(matches!(got, Some(Error::Projection(_))), "got {got:?}");
    // By id the value column still reads as stored.
    assert!(
        tree.columnar_scan(&[COL_USER_KEY, 3], None, SeqNo::MAX, ..)
            .is_ok()
    );
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

/// A row source's declared fields are read through the projector, so a scan
/// without one is refused rather than reading them as absent.
#[test]
fn declared_fields_over_memtable_rows_without_a_projector_are_refused() {
    let folder = get_tmp_folder();
    let any = open_columnar(folder.path());
    let tree = standard(&any);
    tree.insert(key(0), row_value(10, 40), 0);
    let got = tree
        .columnar_scan(projection(Absent::Null), None, SeqNo::MAX, ..)
        .err();
    assert!(matches!(got, Some(Error::Projection(_))), "got {got:?}");
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

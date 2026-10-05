use super::*;
use crate::table::columnar::{ByteOrder, NumberKind, column_batch_to_entries};
use crate::vlog::ValueHandle;
use test_log::test;

const STATUS: u16 = crate::blob_tree::field_row::FIRST_FIELD_COLUMN;
const BODY: u16 = STATUS + 1;
const SCORE: u16 = STATUS + 2;

fn u32_le() -> TypeTag {
    TypeTag::Number(Number::new(NumberKind::Unsigned, 4, ByteOrder::Little).unwrap())
}

fn indirection(offset: u64, size: u32) -> BlobIndirection {
    BlobIndirection {
        vhandle: ValueHandle {
            blob_file_id: 9,
            offset,
            on_disk_size: size,
        },
        size,
    }
}

fn cell_row(key: &str, seqno: u64, fields: &[RowField<'_>]) -> InternalValue {
    InternalValue::from_components(key, encode_row(fields).unwrap(), seqno, ValueType::CellRow)
}

fn column(batch: &ColumnBatch, id: u16) -> Option<&Column> {
    batch.columns.iter().find(|c| c.column_id == id)
}

/// A group of mixed rows (plain values, an indirection, a tombstone, cell rows
/// with values and references) splits each cell row into its fields and
/// gives every row back exactly as it was written.
#[test]
fn a_mixed_group_gives_every_row_back_as_written() {
    let score = 7u32.to_le_bytes();
    let entries = alloc::vec![
        InternalValue::from_components("a", "plain", 5, ValueType::Value),
        cell_row(
            "b",
            4,
            &[
                RowField::bytes(STATUS, RowCell::Value(b"draft")),
                RowField::bytes(
                    BODY,
                    RowCell::Ref {
                        indirection: indirection(0, 4_096),
                        owner: true,
                    },
                ),
                RowField {
                    column: SCORE,
                    tag: u32_le(),
                    cell: RowCell::Value(&score),
                },
            ],
        ),
        cell_row(
            "c",
            3,
            &[RowField::bytes(
                BODY,
                RowCell::Ref {
                    indirection: indirection(4_096, 100),
                    owner: false,
                },
            )],
        ),
        cell_row("d", 2, &[]),
        InternalValue::from_components(
            "e",
            indirection(9_000, 2_000).encode_into_vec(),
            1,
            ValueType::Indirection,
        ),
        InternalValue::from_components("f", "", 1, ValueType::Tombstone),
    ];
    let batch = entries_to_cells_batch(&entries).unwrap();
    assert_eq!(column_batch_to_entries(&batch).unwrap(), entries);

    // The compact fields are columns of their own type; only the split rows
    // hold a cell in them.
    let status = column(&batch, STATUS).unwrap();
    assert_eq!(status.type_tag, TypeTag::Bytes);
    assert_eq!(status.validity.as_deref(), Some(&[0b0000_0010][..]));
    assert_eq!(column(&batch, SCORE).unwrap().type_tag, u32_le());
    // The body is only ever a reference: no column holds it as a value.
    assert!(column(&batch, BODY).is_none());
    // The references of rows b and c, and the whole values of a, e and f.
    let refs = column(&batch, CELL_REFS_COLUMN).unwrap();
    assert_eq!(refs.validity.as_deref(), Some(&[0b0000_0110][..]));
    let whole = column(&batch, WHOLE_VALUE_COLUMN).unwrap();
    assert_eq!(whole.validity.as_deref(), Some(&[0b0011_0001][..]));
}

/// A group survives its own encoding: what the pages decode to is the same
/// rows.
#[test]
fn a_cells_group_round_trips_through_its_encoding() {
    let entries = alloc::vec![
        cell_row(
            "a",
            1,
            &[
                RowField::bytes(STATUS, RowCell::Value(b"x")),
                RowField::bytes(
                    BODY,
                    RowCell::Ref {
                        indirection: indirection(64, 10),
                        owner: true,
                    },
                ),
            ],
        ),
        InternalValue::from_components("b", "whole", 1, ValueType::Value),
    ];
    let batch = entries_to_cells_batch(&entries).unwrap();
    let decoded = ColumnBatch::decode(&batch.encode().unwrap().into()).unwrap();
    assert_eq!(column_batch_to_entries(&decoded).unwrap(), entries);
}

/// A cell row whose field holds a value under another type than the group's
/// column of that id, or of another width than its type, is kept whole: the
/// column could not give it back as written.
#[test]
fn a_cell_row_that_does_not_fit_the_columns_is_kept_whole() {
    let four = 1u32.to_le_bytes();
    let entries = alloc::vec![
        cell_row(
            "a",
            1,
            &[RowField {
                column: SCORE,
                tag: u32_le(),
                cell: RowCell::Value(&four),
            }],
        ),
        cell_row("b", 1, &[RowField::bytes(SCORE, RowCell::Value(b"text"))]),
        cell_row(
            "c",
            1,
            &[RowField {
                column: STATUS,
                tag: TypeTag::Fixed(2),
                cell: RowCell::Value(b"abc"),
            }],
        ),
    ];
    let batch = entries_to_cells_batch(&entries).unwrap();
    assert_eq!(column_batch_to_entries(&batch).unwrap(), entries);
    let whole = column(&batch, WHOLE_VALUE_COLUMN).unwrap();
    assert_eq!(whole.validity.as_deref(), Some(&[0b0000_0110][..]));
    assert!(
        column(&batch, STATUS).is_none(),
        "no row fits a column of that id"
    );
}

/// A reference counts toward its column's type as a value does: a row whose
/// reference names another type than the group's column of that id is kept
/// whole, and so is a row whose value disagrees with a reference split
/// before it. Otherwise the column would read a returned reference as
/// mistyped for a type its own row never had.
#[test]
fn a_reference_of_another_type_than_its_column_is_kept_whole() {
    let fixed = |column: u16, offset: u64| RowField {
        column,
        tag: TypeTag::Fixed(4),
        cell: RowCell::Ref {
            indirection: indirection(offset, 4),
            owner: true,
        },
    };
    let entries = alloc::vec![
        cell_row("a", 1, &[RowField::bytes(SCORE, RowCell::Value(b"text"))]),
        cell_row("b", 1, &[fixed(SCORE, 0)]),
        cell_row("c", 1, &[fixed(STATUS, 64)]),
        cell_row("d", 1, &[RowField::bytes(STATUS, RowCell::Value(b"text"))]),
    ];
    let batch = entries_to_cells_batch(&entries).unwrap();
    assert_eq!(column_batch_to_entries(&batch).unwrap(), entries);
    let whole = column(&batch, WHOLE_VALUE_COLUMN).unwrap();
    assert_eq!(whole.validity.as_deref(), Some(&[0b0000_1010][..]));
    let refs = column(&batch, CELL_REFS_COLUMN).unwrap();
    assert_eq!(refs.validity.as_deref(), Some(&[0b0000_0100][..]));
    assert_eq!(column(&batch, SCORE).unwrap().type_tag, TypeTag::Bytes);
    assert!(
        column(&batch, STATUS).is_none(),
        "the only split row of that id holds it as a reference"
    );
}

/// A group whose whole-value or references column is not a byte column, or
/// that lists a system column among its value columns, is not one a writer
/// produced, and is refused.
#[test]
fn value_columns_a_writer_never_produces_are_refused() {
    let written = [
        (STATUS, TypeTag::Bytes),
        (SCORE, u32_le()),
        (CELL_REFS_COLUMN, TypeTag::Bytes),
        (WHOLE_VALUE_COLUMN, TypeTag::Bytes),
    ];
    assert!(check_value_columns(&written).is_ok());
    for column in [
        (WHOLE_VALUE_COLUMN, u32_le()),
        (CELL_REFS_COLUMN, TypeTag::Fixed(8)),
        (crate::table::columnar::COL_SEQNO, TypeTag::Fixed(8)),
    ] {
        assert!(
            matches!(check_value_columns(&[column]), Err(Error::InvalidHeader(_))),
            "{column:?} accepted"
        );
    }
}

/// A cell row that does not decode is kept whole, as written, rather than
/// dropped or refused: the group stores what it was given.
#[test]
fn a_damaged_cell_row_is_kept_whole() {
    let entries = alloc::vec![InternalValue::from_components(
        "a",
        b"\xff\xff".to_vec(),
        1,
        ValueType::CellRow
    )];
    let batch = entries_to_cells_batch(&entries).unwrap();
    assert_eq!(column_batch_to_entries(&batch).unwrap(), entries);
}

/// A group in which a row holds both a whole value and fields, or a split row
/// that is not a cell row, is refused: no writer stores either.
#[test]
fn a_row_no_group_stores_is_refused() {
    let both = [
        (STATUS, TypeTag::Bytes, Some(&b"x"[..])),
        (WHOLE_VALUE_COLUMN, TypeTag::Bytes, Some(&b"v"[..])),
    ];
    assert!(row_value(ValueType::CellRow, &both).is_err());
    let split_value = [
        (STATUS, TypeTag::Bytes, Some(&b"x"[..])),
        (WHOLE_VALUE_COLUMN, TypeTag::Bytes, None),
    ];
    assert!(row_value(ValueType::Value, &split_value).is_err());
    let intrinsic_id = [(1, TypeTag::Bytes, Some(&b"x"[..]))];
    assert!(row_value(ValueType::CellRow, &intrinsic_id).is_err());
}

/// One column held both as a value and as a reference is refused rather than
/// encoded as a row with the column twice.
#[test]
fn a_column_held_as_value_and_reference_is_refused() {
    let refs = encode_refs(&[RowField::bytes(
        STATUS,
        RowCell::Ref {
            indirection: indirection(0, 5),
            owner: false,
        },
    )])
    .unwrap()
    .unwrap();
    let cells = [
        (STATUS, TypeTag::Bytes, Some(&b"x"[..])),
        (CELL_REFS_COLUMN, TypeTag::Bytes, Some(&refs[..])),
    ];
    assert!(row_value(ValueType::CellRow, &cells).is_err());
}

/// A references cell that is cut short, carries a bad owner byte or names an
/// id that is not a field's is refused.
#[test]
fn a_malformed_references_cell_is_refused() {
    let refs = encode_refs(&[RowField::bytes(
        STATUS,
        RowCell::Ref {
            indirection: indirection(0, 5),
            owner: true,
        },
    )])
    .unwrap()
    .unwrap();
    for cut in 1..refs.len() {
        assert!(
            decode_refs(&refs[..cut], &mut Vec::new()).is_err(),
            "cut to {cut}"
        );
    }
    let mut owner = refs.clone();
    owner[4] = 2;
    assert!(decode_refs(&owner, &mut Vec::new()).is_err());
    let mut column = refs;
    column[..2].copy_from_slice(&0u16.to_le_bytes());
    assert!(decode_refs(&column, &mut Vec::new()).is_err());
}

/// A blob tree's columnar tables, flushed and compacted, record the cells
/// layout and store a cell row's compact field as a column of its own type,
/// its separated body only in the references column.
#[test]
fn a_blob_trees_columnar_tables_split_cell_rows() -> crate::Result<()> {
    use crate::blob_tree::field_row::{Cell, Field};
    use crate::table::meta::ValueLayout;
    use crate::{AbstractTree, AnyTree, Config, KvSeparationOptions, SeqNo, SequenceNumberCounter};

    let folder = crate::get_tmp_folder();
    let AnyTree::Blob(tree) = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(64),
    ))
    .open()?
    else {
        panic!("a tree with kv separation opens as a blob tree");
    };
    tree.index.update_runtime_config(|rc| rc.columnar = true)?;
    let score = 3u32.to_le_bytes();
    let body = alloc::vec![b'b'; 1_000];
    for (key, seqno) in [("a", 0), ("b", 1)] {
        tree.insert_cells(
            key,
            &[
                Field::bytes(STATUS, b"draft"),
                Field {
                    column: SCORE,
                    tag: u32_le(),
                    cell: Cell::Value(&score),
                },
                Field::bytes(BODY, &body),
            ],
            seqno,
        )?;
    }
    tree.insert("plain", "value", 2);

    let check = |stage: &str| -> crate::Result<()> {
        let version = tree.index.current_version();
        let mut tables = version.iter_tables().peekable();
        assert!(tables.peek().is_some(), "{stage}: a table");
        for table in tables {
            assert_eq!(table.metadata.value_layout, ValueLayout::Cells, "{stage}");
            let handle = *table
                .data_block_handles()
                .next()
                .expect("a data block")?
                .as_ref();
            let batch = table
                .load_columnar_block_masked(&handle)?
                .expect("live rows");
            assert_eq!(
                column(&batch, SCORE).map(|c| c.type_tag),
                Some(u32_le()),
                "{stage}: the score is a column of its type"
            );
            assert!(
                column(&batch, BODY).is_none(),
                "{stage}: the body is a reference"
            );
            assert!(column(&batch, CELL_REFS_COLUMN).is_some(), "{stage}");
        }
        Ok(())
    };
    tree.flush_active_memtable(0)?;
    check("flush")?;
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    check("compaction")?;
    assert!(tree.get("a", SeqNo::MAX)?.is_some());
    Ok(())
}

/// An ingested batch may not use the ids the engine keeps for itself.
#[test]
fn an_ingested_batch_may_not_use_a_reserved_id() {
    let column = |column_id| Column {
        column_id,
        type_tag: TypeTag::Bytes,
        validity: None,
        data: build_bytes_column([&b"v"[..]].into_iter()).unwrap().into(),
    };
    for id in [CELL_REFS_COLUMN, WHOLE_VALUE_COLUMN] {
        assert!(super::super::check_ingested_field_ids(&[column(id)]).is_err());
    }
    assert!(super::super::check_ingested_field_ids(&[column(CELL_REFS_COLUMN - 1)]).is_ok());
}

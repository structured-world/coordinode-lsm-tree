// A projected columnar scan of a blob tree: rows written as cells and whole
// values, across the memtable, row tables and columnar tables, read late:
// compact fields without any blob read, heavy fields only for the rows the
// scan returns.

#![cfg(all(feature = "columnar", feature = "metrics"))]

use std::sync::Arc;

use lsm_tree::{
    Absent, AbstractTree, AnyTree, BlobTree, Config, KvSeparationOptions, ProjectedField,
    ProjectedRow, Projection, SeqNo, SequenceNumberCounter, ValueProjector,
    blob_tree::field_row::{Cell, FIRST_FIELD_COLUMN, Field, TypeTag},
    get_tmp_folder,
    table::column_type::{ByteOrder, Number, NumberKind},
    table::columnar::{COL_USER_KEY, ColumnBatch},
    table::columnar_predicate::{ColumnRangePredicate, PredicateApply},
};
use test_log::test;

const STATUS: u16 = FIRST_FIELD_COLUMN;
const PRICE: u16 = FIRST_FIELD_COLUMN + 1;
const BODY: u16 = FIRST_FIELD_COLUMN + 2;

/// Bodies at or above this many bytes go to a blob file.
const THRESHOLD: u32 = 64;

fn u32_le() -> TypeTag {
    TypeTag::Number(Number::new(NumberKind::Unsigned, 4, ByteOrder::Little).expect("a u32"))
}

fn open(path: &std::path::Path, columnar: bool) -> lsm_tree::Result<(AnyTree, BlobTree)> {
    let any = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(THRESHOLD),
    ))
    .blob_compression(lsm_tree::CompressionType::None)
    .open()?;
    let AnyTree::Blob(tree) = &any else {
        panic!("a tree with kv separation opens as a blob tree");
    };
    let tree = tree.clone();
    tree.index
        .update_runtime_config(|rc| rc.columnar = columnar)?;
    Ok((any, tree))
}

fn insert(tree: &BlobTree, key: &str, status: &[u8], price: u32, body: &[u8], seqno: SeqNo) {
    let price = price.to_le_bytes();
    tree.insert_cells(
        key,
        &[
            Field::bytes(STATUS, status),
            Field {
                column: PRICE,
                tag: u32_le(),
                cell: Cell::Value(&price),
            },
            Field::bytes(BODY, body),
        ],
        seqno,
    )
    .expect("insert cells");
}

fn field(column: u16, tag: TypeTag) -> ProjectedField {
    ProjectedField::new(column, tag, Absent::Null).expect("a field")
}

/// The rows of a scan: each row's key and its cell in each projected column
/// after the key, `None` for a null cell.
type Rows = Vec<(Vec<u8>, Vec<Option<Vec<u8>>>)>;

fn rows(batches: impl Iterator<Item = lsm_tree::Result<ColumnBatch>>) -> lsm_tree::Result<Rows> {
    let mut out = Vec::new();
    for batch in batches {
        let batch = batch?;
        let keys = &batch.columns[0];
        assert_eq!(keys.column_id, COL_USER_KEY);
        for row in 0..batch.row_count {
            let cell = |column: &lsm_tree::table::columnar::Column| {
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
                        let offset = |at: u32| {
                            let at = at as usize * 4;
                            u32::from_le_bytes(column.data[at..at + 4].try_into().expect("4"))
                                as usize
                        };
                        let base = (batch.row_count as usize + 1) * 4;
                        column.data[base + offset(row)..base + offset(row + 1)].to_vec()
                    }
                })
            };
            let key = cell(keys).expect("a key");
            out.push((key, batch.columns[1..].iter().map(cell).collect()));
        }
    }
    Ok(out)
}

/// Rows in the memtable, in row tables and in columnar tables: a projection
/// of the compact fields reads them all and no blob, and a projection of the
/// body reads each body once, for the rows returned.
#[test]
fn compact_fields_are_read_without_any_blob_read() -> lsm_tree::Result<()> {
    for columnar in [false, true] {
        let folder = get_tmp_folder();
        let (any, tree) = open(folder.path(), columnar)?;
        let body = |i: u8| vec![b'a' + i; 500];
        insert(&tree, "a", b"draft", 10, &body(0), 0);
        insert(&tree, "b", b"draft", 20, &body(1), 1);
        tree.flush_active_memtable(0)?;
        insert(&tree, "c", b"final", 30, &body(2), 2);
        tree.flush_active_memtable(0)?;
        // An update in the memtable over a flushed base, and a fresh row.
        insert(&tree, "a", b"final", 11, &body(0), 3);
        insert(&tree, "d", b"draft", 40, &body(3), 4);

        let compact = Projection::new()
            .column(COL_USER_KEY)
            .field(field(STATUS, TypeTag::Bytes))
            .field(field(PRICE, u32_le()));
        let before = any.metrics().blob_read_count();
        let got = rows(any.columnar_scan(compact, None, SeqNo::MAX, ..)?)?;
        assert_eq!(
            any.metrics().blob_read_count(),
            before,
            "columnar={columnar}: compact fields read no blob"
        );
        let expect = |key: &str, status: &[u8], price: u32| {
            (
                key.as_bytes().to_vec(),
                vec![Some(status.to_vec()), Some(price.to_le_bytes().to_vec())],
            )
        };
        assert_eq!(
            got,
            vec![
                expect("a", b"final", 11),
                expect("b", b"draft", 20),
                expect("c", b"final", 30),
                expect("d", b"draft", 40),
            ],
            "columnar={columnar}"
        );

        let with_body = Projection::new()
            .column(COL_USER_KEY)
            .field(field(BODY, TypeTag::Bytes));
        let got = rows(any.columnar_scan(with_body, None, SeqNo::MAX, ..)?)?;
        assert_eq!(
            got,
            (0..4u8)
                .map(|i| { (vec![b'a' + i], vec![Some(body(i))],) })
                .collect::<Rows>(),
            "columnar={columnar}"
        );
    }
    Ok(())
}

/// The predicate runs on the newest visible version of each key: an older
/// version that matches does not stand in for a newer one that does not,
/// whatever source each sits in. And the body is read only for the rows the
/// predicate keeps.
#[test]
fn the_predicate_judges_the_newest_version_and_gates_the_body() -> lsm_tree::Result<()> {
    for columnar in [false, true] {
        let folder = get_tmp_folder();
        let (any, tree) = open(folder.path(), columnar)?;
        for i in 0..20u32 {
            insert(
                &tree,
                &format!("k{i:02}"),
                b"s",
                i,
                &[b'b'; 300],
                u64::from(i),
            );
        }
        tree.flush_active_memtable(0)?;
        // The cheap version of k00 is replaced by an expensive one.
        insert(&tree, "k00", b"s", 100, &[b'n'; 300], 20);

        // Bounds in the comparable encoding: an unsigned integer big-endian.
        let cheap = ColumnRangePredicate {
            column_id: PRICE,
            lower: Some(0u32.to_be_bytes().to_vec()),
            upper: Some(4u32.to_be_bytes().to_vec()),
            apply: PredicateApply::Filter,
        };
        let projection = Projection::new()
            .column(COL_USER_KEY)
            .field(field(PRICE, u32_le()))
            .field(field(BODY, TypeTag::Bytes));
        let m = any.metrics();
        let (requests, bytes) = (m.blob_read_count(), m.blob_bytes_read());
        let prefetched = m.blob_bytes_prefetched();
        let got = rows(any.columnar_scan(projection, Some(&cheap), SeqNo::MAX, ..)?)?;
        let keys: Vec<&[u8]> = got.iter().map(|(key, _)| key.as_slice()).collect();
        assert_eq!(
            keys,
            [&b"k01"[..], b"k02", b"k03", b"k04"],
            "columnar={columnar}: k00's newest version is not cheap"
        );
        // The four bodies sit next to each other in one blob file: read in
        // one request, and nothing of the sixteen bodies the predicate drops.
        assert_eq!(
            m.blob_read_count() - requests,
            1,
            "columnar={columnar}: the kept bodies are read together"
        );
        assert!(
            m.blob_bytes_prefetched() - prefetched >= 4 * 300,
            "columnar={columnar}: the one request is counted as prefetched"
        );
        let read = m.blob_bytes_read() - bytes;
        assert!(
            read < 6 * 300,
            "columnar={columnar}: {read} body bytes read for four bodies of 300"
        );
    }
    Ok(())
}

/// The same with the newer version in a columnar table of its own, zone maps
/// on: that table's price zones are disjoint from the predicate, yet its keys
/// and seqnos are read, since they shadow the older version that matches.
#[test]
fn a_newer_segment_disjoint_from_the_predicate_still_shadows() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let (any, tree) = open(folder.path(), true)?;
    tree.index.update_runtime_config(|rc| rc.zone_map = true)?;
    // The old version: price 10, matches.
    insert(&tree, "item", b"s", 10, b"old", 0);
    insert(&tree, "other", b"s", 15, b"kept", 1);
    tree.flush_active_memtable(0)?;
    // The new version: price 100, in a table whose prices are all 100.
    insert(&tree, "item", b"s", 100, b"new", 2);
    tree.flush_active_memtable(0)?;

    let below_20 = ColumnRangePredicate {
        column_id: PRICE,
        lower: Some(0u32.to_be_bytes().to_vec()),
        upper: Some(19u32.to_be_bytes().to_vec()),
        apply: PredicateApply::Filter,
    };
    let projection = Projection::new()
        .column(COL_USER_KEY)
        .field(field(PRICE, u32_le()))
        .field(field(BODY, TypeTag::Bytes));
    let got = rows(any.columnar_scan(projection, Some(&below_20), SeqNo::MAX, ..)?)?;
    assert_eq!(
        got,
        vec![(
            b"other".to_vec(),
            vec![Some(15u32.to_le_bytes().to_vec()), Some(b"kept".to_vec())]
        )],
        "the item's newest version costs 100, so no version of it is returned"
    );
    // What a read returns agrees.
    assert_eq!(
        tree.get_cells("item", SeqNo::MAX)?
            .expect("present")
            .resolve(PRICE)?
            .as_deref(),
        Some(&100u32.to_le_bytes()[..])
    );
    Ok(())
}

/// Reads the fields of a plain value: a status byte string and a price.
struct PlainProjector;

impl ValueProjector for PlainProjector {
    fn project(
        &self,
        _key: &[u8],
        value: &[u8],
        row: &mut ProjectedRow<'_>,
    ) -> lsm_tree::Result<()> {
        let (status, price) = value.split_at(value.len() - 4);
        for index in 0..row.fields().len() {
            match row.fields()[index].column_id() {
                STATUS => row.set(index, status)?,
                PRICE => row.set(index, price)?,
                _ => {}
            }
        }
        Ok(())
    }
}

/// Values written whole, small and separated into a blob file, read through
/// the projector beside rows written as cells, and a deletion removing a key.
#[test]
fn whole_values_are_read_through_the_projector() -> lsm_tree::Result<()> {
    for columnar in [false, true] {
        let folder = get_tmp_folder();
        let (any, tree) = open(folder.path(), columnar)?;
        let plain = |status: &[u8], price: u32| [status, &price.to_le_bytes()].concat();
        tree.insert("a", plain(b"small", 1), 0);
        tree.insert("b", plain(&[b'x'; 200], 2), 1);
        insert(&tree, "c", b"cell", 3, b"short", 2);
        tree.insert("d", plain(b"gone", 4), 3);
        tree.flush_active_memtable(0)?;
        tree.remove("d", 4);

        let projection = Projection::new()
            .column(COL_USER_KEY)
            .field(field(STATUS, TypeTag::Bytes))
            .field(field(PRICE, u32_le()))
            .projector(Arc::new(PlainProjector));
        let got = rows(any.columnar_scan(projection, None, SeqNo::MAX, ..)?)?;
        let expect = |key: &str, status: &[u8], price: u32| {
            (
                key.as_bytes().to_vec(),
                vec![Some(status.to_vec()), Some(price.to_le_bytes().to_vec())],
            )
        };
        assert_eq!(
            got,
            vec![
                expect("a", b"small", 1),
                expect("b", &[b'x'; 200], 2),
                expect("c", b"cell", 3),
            ],
            "columnar={columnar}"
        );
    }
    Ok(())
}

/// A scan of the keys alone reads no value: neither a value kept in a blob
/// file, nor a cell row's referenced field, nor the inline values of the
/// table.
#[test]
fn a_key_only_scan_reads_no_value() -> lsm_tree::Result<()> {
    for columnar in [false, true] {
        let folder = get_tmp_folder();
        let (any, tree) = open(folder.path(), columnar)?;
        tree.insert("a", vec![b'v'; 500], 0);
        insert(&tree, "b", b"cell", 2, &[b'b'; 500], 1);
        tree.insert("c", b"small".to_vec(), 2);
        tree.flush_active_memtable(0)?;

        let m = any.metrics();
        let blobs = m.blob_read_count();
        let keys = Projection::new().column(COL_USER_KEY);
        let got = rows(any.columnar_scan(keys, None, SeqNo::MAX, ..)?)?;
        assert_eq!(
            got.into_iter().map(|(key, _)| key).collect::<Vec<_>>(),
            [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
            "columnar={columnar}"
        );
        assert_eq!(
            m.blob_read_count(),
            blobs,
            "columnar={columnar}: a key-only scan read a blob"
        );
    }
    Ok(())
}

/// The value type a blob tree's rows come back with is a value's, whether the
/// row is kept in a blob file or written as cells; a predicate on that opaque
/// fixed-width column is not judged by the scan (it reports it unsupported
/// and keeps every row), so a stored type never decides a row, and the caller
/// filters on the type returned.
#[test]
fn a_value_type_predicate_leaves_the_returned_type_to_the_caller() -> lsm_tree::Result<()> {
    use lsm_tree::table::columnar::COL_VALUE_TYPE;
    use lsm_tree::table::columnar_predicate::PredicateSupport;

    for columnar in [false, true] {
        let folder = get_tmp_folder();
        let (any, tree) = open(folder.path(), columnar)?;
        tree.insert("a", vec![b'v'; 500], 0);
        insert(&tree, "b", b"cell", 2, &[b'b'; 500], 1);
        tree.insert("c", b"small".to_vec(), 2);
        tree.flush_active_memtable(0)?;

        for stored in [
            lsm_tree::ValueType::Indirection,
            lsm_tree::ValueType::CellRow,
        ] {
            let tag = vec![u8::from(stored)];
            let predicate = ColumnRangePredicate {
                column_id: COL_VALUE_TYPE,
                lower: Some(tag.clone()),
                upper: Some(tag),
                apply: PredicateApply::Filter,
            };
            let projection = Projection::new()
                .column(COL_USER_KEY)
                .column(COL_VALUE_TYPE);
            let mut scan = any.columnar_scan(projection, Some(&predicate), SeqNo::MAX, ..)?;
            let got = rows(scan.by_ref())?;
            assert_eq!(
                scan.predicate_support(),
                Some(PredicateSupport::Unsupported),
                "columnar={columnar}"
            );
            let value = vec![u8::from(lsm_tree::ValueType::Value)];
            assert_eq!(
                got,
                [b"a", b"b", b"c"]
                    .map(|key| (key.to_vec(), vec![Some(value.clone())]))
                    .to_vec(),
                "columnar={columnar}: every row is kept and reads as a value under {stored:?}"
            );
        }
    }
    Ok(())
}

/// A predicate on a field held by reference reads that field's objects first
/// and drops the rows it rejects before any other field of theirs is read;
/// the rows kept come back with their value type read as a value.
#[test]
fn a_predicate_on_a_referenced_field_judges_its_objects() -> lsm_tree::Result<()> {
    use lsm_tree::table::columnar::COL_VALUE_TYPE;

    for columnar in [false, true] {
        let folder = get_tmp_folder();
        let (any, tree) = open(folder.path(), columnar)?;
        for (i, key) in ["a", "b", "c"].into_iter().enumerate() {
            let byte = b'a' + i as u8;
            insert(&tree, key, b"s", i as u32, &[byte; 100], i as u64);
        }
        tree.flush_active_memtable(0)?;

        let only_b = ColumnRangePredicate {
            column_id: BODY,
            lower: Some(b"b".to_vec()),
            upper: Some(vec![b'b'; 100]),
            apply: PredicateApply::Filter,
        };
        let projection = Projection::new()
            .column(COL_USER_KEY)
            .column(COL_VALUE_TYPE)
            .field(field(BODY, TypeTag::Bytes));
        let got = rows(any.columnar_scan(projection, Some(&only_b), SeqNo::MAX, ..)?)?;
        assert_eq!(
            got,
            vec![(
                b"b".to_vec(),
                vec![
                    Some(vec![u8::from(lsm_tree::ValueType::Value)]),
                    Some(vec![b'b'; 100])
                ]
            )],
            "columnar={columnar}"
        );
    }
    Ok(())
}

/// A declared field a row does not hold reads as the field's absent form,
/// beside rows that hold it.
#[test]
fn a_field_some_rows_lack_reads_as_absent_there() -> lsm_tree::Result<()> {
    for columnar in [false, true] {
        let folder = get_tmp_folder();
        let (any, tree) = open(folder.path(), columnar)?;
        insert(&tree, "a", b"s", 1, b"body", 0);
        tree.insert_cells("b", &[Field::bytes(STATUS, b"only")], 1)?;
        tree.flush_active_memtable(0)?;

        let projection = Projection::new()
            .column(COL_USER_KEY)
            .field(field(PRICE, u32_le()));
        assert_eq!(
            rows(any.columnar_scan(projection, None, SeqNo::MAX, ..)?)?,
            vec![
                (b"a".to_vec(), vec![Some(1u32.to_le_bytes().to_vec())]),
                (b"b".to_vec(), vec![None]),
            ],
            "columnar={columnar}"
        );
    }
    Ok(())
}

/// A table holding only values written whole carries no column for a
/// declared field: the fields come out of the values through the projector.
#[test]
fn a_table_of_whole_values_reads_its_fields_through_the_projector() -> lsm_tree::Result<()> {
    for columnar in [false, true] {
        let folder = get_tmp_folder();
        let (any, tree) = open(folder.path(), columnar)?;
        let plain = |status: &[u8], price: u32| [status, &price.to_le_bytes()].concat();
        tree.insert("a", plain(b"one", 1), 0);
        tree.insert("b", plain(b"two", 2), 1);
        tree.flush_active_memtable(0)?;

        let projection = Projection::new()
            .column(COL_USER_KEY)
            .field(field(STATUS, TypeTag::Bytes))
            .field(field(PRICE, u32_le()))
            .projector(Arc::new(PlainProjector));
        assert_eq!(
            rows(any.columnar_scan(projection, None, SeqNo::MAX, ..)?)?,
            vec![
                (
                    b"a".to_vec(),
                    vec![Some(b"one".to_vec()), Some(1u32.to_le_bytes().to_vec())]
                ),
                (
                    b"b".to_vec(),
                    vec![Some(b"two".to_vec()), Some(2u32.to_le_bytes().to_vec())]
                ),
            ],
            "columnar={columnar}"
        );
    }
    Ok(())
}

/// A predicate on a field with a default judges a value written whole by the
/// field the projector reads out of it, not by the default its empty cell in
/// the field's column would read as: neither drops a whole value that
/// matches nor keeps one that does not.
#[test]
fn a_whole_value_is_judged_by_its_field_not_the_default() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let (any, tree) = open(folder.path(), true)?;
    let plain = |status: &[u8], price: u32| [status, &price.to_le_bytes()].concat();
    tree.insert("a", plain(b"cheap", 1), 0);
    tree.insert("b", plain(b"dear", 50), 1);
    insert(&tree, "c", b"cell", 3, b"short", 2);
    tree.flush_active_memtable(0)?;

    let cheap = ColumnRangePredicate {
        column_id: PRICE,
        lower: Some(0u32.to_be_bytes().to_vec()),
        upper: Some(5u32.to_be_bytes().to_vec()),
        apply: PredicateApply::Filter,
    };
    let keys = |default: u32| -> lsm_tree::Result<Vec<Vec<u8>>> {
        let price = ProjectedField::new(
            PRICE,
            u32_le(),
            Absent::Default(default.to_le_bytes().to_vec().into()),
        )?;
        let projection = Projection::new()
            .column(COL_USER_KEY)
            .field(price)
            .projector(Arc::new(PlainProjector));
        Ok(
            rows(any.columnar_scan(projection, Some(&cheap), SeqNo::MAX, ..)?)?
                .into_iter()
                .map(|(key, _)| key)
                .collect(),
        )
    };
    let want = vec![b"a".to_vec(), b"c".to_vec()];
    assert_eq!(
        keys(99)?,
        want,
        "a default outside the range drops no whole value"
    );
    assert_eq!(
        keys(2)?,
        want,
        "a default inside the range keeps no whole value"
    );
    Ok(())
}

/// A field stored under another type than declared fails the scan; a value
/// column projected by id alone is refused up front.
#[test]
fn a_mistyped_field_or_a_field_by_id_is_refused() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let (any, tree) = open(folder.path(), true)?;
    insert(&tree, "a", b"draft", 1, b"body", 0);

    let mistyped = Projection::new().field(field(PRICE, TypeTag::Bytes));
    let result: lsm_tree::Result<Vec<ColumnBatch>> =
        any.columnar_scan(mistyped, None, SeqNo::MAX, ..)?.collect();
    assert!(
        matches!(result, Err(lsm_tree::Error::Projection(_))),
        "{result:?}"
    );

    let by_id = any.columnar_scan(&[STATUS], None, SeqNo::MAX, ..);
    assert!(
        matches!(by_id, Err(lsm_tree::Error::Projection(_))),
        "a value column by id"
    );
    Ok(())
}

/// A row the predicate drops does not fail the scan over a projected field it
/// stores under another type, in the memtable, a row table or a columnar
/// table; a row it keeps does.
#[test]
fn a_mistyped_field_fails_only_the_rows_the_predicate_keeps() -> lsm_tree::Result<()> {
    for (columnar, flushed) in [(false, false), (false, true), (true, true)] {
        let folder = get_tmp_folder();
        let (any, tree) = open(folder.path(), columnar)?;
        insert(&tree, "a", b"s", 1, b"body", 0);
        let price = 50u32.to_le_bytes();
        let body = 7u32.to_le_bytes();
        tree.insert_cells(
            "b",
            &[
                Field {
                    column: PRICE,
                    tag: u32_le(),
                    cell: Cell::Value(&price),
                },
                // The body, stored as a number.
                Field {
                    column: BODY,
                    tag: u32_le(),
                    cell: Cell::Value(&body),
                },
            ],
            1,
        )?;
        if flushed {
            tree.flush_active_memtable(0)?;
        }

        let projection = Projection::new()
            .column(COL_USER_KEY)
            .field(field(PRICE, u32_le()))
            .field(field(BODY, TypeTag::Bytes));
        let range = |lower: u32, upper: u32| ColumnRangePredicate {
            column_id: PRICE,
            lower: Some(lower.to_be_bytes().to_vec()),
            upper: Some(upper.to_be_bytes().to_vec()),
            apply: PredicateApply::Filter,
        };
        let got =
            rows(any.columnar_scan(projection.clone(), Some(&range(0, 19)), SeqNo::MAX, ..)?)?;
        assert_eq!(
            got,
            vec![(
                b"a".to_vec(),
                vec![Some(1u32.to_le_bytes().to_vec()), Some(b"body".to_vec())]
            )],
            "columnar={columnar}, flushed={flushed}: the dropped row does not fail the scan"
        );

        let kept: lsm_tree::Result<Vec<ColumnBatch>> = any
            .columnar_scan(projection, Some(&range(40, 60)), SeqNo::MAX, ..)?
            .collect();
        assert!(
            matches!(kept, Err(lsm_tree::Error::Projection(_))),
            "columnar={columnar}, flushed={flushed}: a kept mistyped row fails: {kept:?}"
        );
    }
    Ok(())
}

/// A tree whose columnar tables cut rows into small pages and keep every
/// field inline, so a field is read page by page.
fn open_paged(path: &std::path::Path) -> lsm_tree::Result<(AnyTree, BlobTree)> {
    let any = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(1 << 20),
    ))
    .columnar_row_group_size_policy(lsm_tree::config::BlockSizePolicy::all(64 * 1_024))
    .columnar_page_size_policy(lsm_tree::config::BlockSizePolicy::all(1_024))
    .open()?;
    let AnyTree::Blob(tree) = &any else {
        panic!("a tree with kv separation opens as a blob tree");
    };
    let tree = tree.clone();
    tree.index.update_runtime_config(|rc| rc.columnar = true)?;
    Ok((any, tree))
}

/// Two overlapping columnar tables of rows with a price and a 200-byte note,
/// merged by the scan; each key's price is its number.
fn paged_notes(tree: &BlobTree, rows: u32) -> lsm_tree::Result<()> {
    let note = |i: u32, round: u8| vec![b'a' + round + (i % 20) as u8; 200];
    for round in 0..2u8 {
        for i in (0..rows).filter(|i| round == 0 || i % 3 == 0) {
            insert(
                tree,
                &format!("k{i:05}"),
                b"s",
                i,
                &note(i, round),
                u64::from(round) * u64::from(rows) + u64::from(i),
            );
        }
        tree.flush_active_memtable(0)?;
    }
    Ok(())
}

/// A sparse predicate on a compact field reads the note only from the pages
/// that hold a row it keeps: the note's other pages are read by a scan that
/// keeps every row, after, and that is most of them.
#[test]
fn the_payload_of_rows_the_predicate_drops_is_not_read() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let (any, tree) = open_paged(folder.path())?;
    let rows = 2_000;
    paged_notes(&tree, rows)?;
    let m = any.metrics();
    let cheap = ColumnRangePredicate {
        column_id: PRICE,
        lower: Some(0u32.to_be_bytes().to_vec()),
        upper: Some(19u32.to_be_bytes().to_vec()),
        apply: PredicateApply::Filter,
    };
    let metadata = Projection::new()
        .column(COL_USER_KEY)
        .field(field(PRICE, u32_le()));
    let notes = Projection::new()
        .column(COL_USER_KEY)
        .field(field(PRICE, u32_le()))
        .field(field(BODY, TypeTag::Bytes));

    // The keys and prices first, so what follows reads only notes.
    rows_of_scan(&any, metadata, None)?;

    let before = m.bytes_read();
    let (useful, materialized) = (m.payload_bytes_useful(), m.bytes_materialized());
    let incidental = m.payload_bytes_incidental();
    let sparse = rows_of_scan(&any, notes.clone(), Some(&cheap))?;
    let sparse_read = m.bytes_read() - before;
    let sparse_materialized = m.bytes_materialized() - materialized;
    assert_eq!(sparse.len(), 20);
    assert_eq!(
        m.payload_bytes_useful() - useful,
        20 * 200,
        "the useful payload read is the twenty notes kept"
    );
    assert!(
        m.payload_bytes_incidental() > incidental,
        "the pages the kept notes sit in held other notes too"
    );
    for (key, cells) in &sparse {
        let i: u32 = std::str::from_utf8(&key[1..])
            .expect("utf8")
            .parse()
            .expect("a number");
        let round = u8::from(i.is_multiple_of(3));
        assert_eq!(cells[1], Some(vec![b'a' + round + (i % 20) as u8; 200]));
    }

    let before = m.bytes_read();
    let materialized = m.bytes_materialized();
    let dense = rows_of_scan(&any, notes, None)?;
    let dense_read = m.bytes_read() - before;
    let dense_materialized = m.bytes_materialized() - materialized;
    assert_eq!(dense.len(), rows as usize);
    assert!(
        sparse_read * 10 < dense_read,
        "the sparse scan read {sparse_read} bytes of notes, the rest of them took {dense_read}"
    );
    // What is materialised follows the rows returned, a hundredth of them.
    assert!(
        sparse_materialized * 50 < dense_materialized,
        "materialised {sparse_materialized} for twenty rows, {dense_materialized} for all"
    );
    Ok(())
}

/// A field a page stores under another type than declared fails the scan of
/// the rows returned from it when the page is read late too, not only when it
/// is decoded with the rest.
#[test]
fn a_mistyped_field_read_late_fails_the_rows_returned() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let (any, tree) = open_paged(folder.path())?;
    for i in 0..2_000u32 {
        let price = i.to_le_bytes();
        let body = (i ^ 0xA5A5).to_le_bytes();
        tree.insert_cells(
            format!("k{i:05}"),
            &[
                Field {
                    column: PRICE,
                    tag: u32_le(),
                    cell: Cell::Value(&price),
                },
                // The note, stored as a number.
                Field {
                    column: BODY,
                    tag: u32_le(),
                    cell: Cell::Value(&body),
                },
            ],
            u64::from(i),
        )?;
    }
    tree.flush_active_memtable(0)?;

    let sparse = ColumnRangePredicate {
        column_id: PRICE,
        lower: Some(0u32.to_be_bytes().to_vec()),
        upper: Some(19u32.to_be_bytes().to_vec()),
        apply: PredicateApply::Filter,
    };
    let notes = Projection::new()
        .column(COL_USER_KEY)
        .field(field(PRICE, u32_le()))
        .field(field(BODY, TypeTag::Bytes));
    let result: lsm_tree::Result<Vec<ColumnBatch>> = any
        .columnar_scan(notes, Some(&sparse), SeqNo::MAX, ..)?
        .collect();
    assert!(
        matches!(result, Err(lsm_tree::Error::Projection(_))),
        "{result:?}"
    );
    Ok(())
}

/// A sparse predicate on a field kept in a blob file reads each row's object
/// to judge it, but the notes beside it only from the pages that hold a row
/// it keeps: the pages of the rows it drops give it their references alone.
#[test]
fn a_predicate_on_a_referenced_field_reads_no_note_of_a_row_it_drops() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default()
            .separation_threshold(1 << 20)
            .cell_separation_threshold(PRICE, 0),
    ))
    .columnar_row_group_size_policy(lsm_tree::config::BlockSizePolicy::all(64 * 1_024))
    .columnar_page_size_policy(lsm_tree::config::BlockSizePolicy::all(1_024))
    .open()?;
    let AnyTree::Blob(tree) = &any else {
        panic!("a tree with kv separation opens as a blob tree");
    };
    tree.index.update_runtime_config(|rc| rc.columnar = true)?;
    let rows = 2_000u32;
    for i in 0..rows {
        insert(
            tree,
            &format!("k{i:05}"),
            b"s",
            i,
            &[b'n'; 200],
            u64::from(i),
        );
    }
    tree.flush_active_memtable(0)?;

    let m = any.metrics();
    let first_20 = ColumnRangePredicate {
        column_id: PRICE,
        lower: Some(0u32.to_be_bytes().to_vec()),
        upper: Some(19u32.to_be_bytes().to_vec()),
        apply: PredicateApply::Filter,
    };
    // The same predicate without the notes first: what judging the rows
    // reads (their references and objects), read again below from the cache.
    let prices = Projection::new()
        .column(COL_USER_KEY)
        .field(field(PRICE, u32_le()));
    let before = m.bytes_read();
    assert_eq!(rows_of_scan(&any, prices, Some(&first_20))?.len(), 20);
    let judging = m.bytes_read() - before;

    let notes = Projection::new()
        .column(COL_USER_KEY)
        .field(field(PRICE, u32_le()))
        .field(field(BODY, TypeTag::Bytes));
    let before = m.bytes_read();
    let kept = rows_of_scan(&any, notes, Some(&first_20))?;
    let read = m.bytes_read() - before;
    assert_eq!(kept.len(), 20);
    for (i, (_, cells)) in (0u32..).zip(&kept) {
        assert_eq!(cells[0], Some(i.to_le_bytes().to_vec()));
        assert_eq!(cells[1], Some(vec![b'n'; 200]));
    }
    let all_notes = u64::from(rows) * 200;
    assert!(
        read < judging + all_notes / 4,
        "{read} bytes read with the notes, {judging} without, out of {all_notes} bytes of notes"
    );
    Ok(())
}

/// `len` bytes no block compression shrinks, the same for one `seed`: what
/// the pages of a note cost to read then shows in the bytes read.
fn noise(seed: u32, len: usize) -> Vec<u8> {
    // xorshift32; a zero state would stay zero.
    let mut state = seed.wrapping_mul(0x9E37_79B9) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state.to_le_bytes()[0]
        })
        .collect()
}

/// A predicate on a field kept in a blob file whose rows it keeps all at the
/// start of a table turns the scan to reading the notes with the rest, and
/// the rows it drops after them turn it back: rows read with the rest count
/// toward the density by the predicate's verdict too, not as candidates.
#[test]
fn a_dense_start_of_a_referenced_predicate_gives_way_to_its_sparse_tail() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default()
            .separation_threshold(1 << 20)
            .cell_separation_threshold(PRICE, 0),
    ))
    .columnar_row_group_size_policy(lsm_tree::config::BlockSizePolicy::all(64 * 1_024))
    .columnar_page_size_policy(lsm_tree::config::BlockSizePolicy::all(1_024))
    .open()?;
    let AnyTree::Blob(tree) = &any else {
        panic!("a tree with kv separation opens as a blob tree");
    };
    tree.index.update_runtime_config(|rc| rc.columnar = true)?;
    // Enough row groups that the few a turn back to late reads takes, each
    // read whole, stay a small part of the table.
    let rows = 12_000u32;
    for i in 0..rows {
        insert(
            tree,
            &format!("k{i:05}"),
            b"s",
            i,
            &noise(i, 200),
            u64::from(i),
        );
    }
    tree.flush_active_memtable(0)?;

    let m = any.metrics();
    let first_third = ColumnRangePredicate {
        column_id: PRICE,
        lower: Some(0u32.to_be_bytes().to_vec()),
        upper: Some((rows / 3 - 1).to_be_bytes().to_vec()),
        apply: PredicateApply::Filter,
    };
    let read_with = |projection: Projection| -> lsm_tree::Result<u64> {
        let before = m.bytes_read();
        assert_eq!(
            rows_of_scan(&any, projection, Some(&first_third))?.len(),
            (rows / 3) as usize
        );
        Ok(m.bytes_read() - before)
    };
    // What judging the rows reads, every row's reference and object, whether
    // or not the notes are read too: the first pass brings the references
    // into the cache, the second costs what judging costs again.
    let prices = || {
        Projection::new()
            .column(COL_USER_KEY)
            .field(field(PRICE, u32_le()))
    };
    read_with(prices())?;
    let judging = read_with(prices())?;
    let with_notes = read_with(
        Projection::new()
            .column(COL_USER_KEY)
            .field(field(PRICE, u32_le()))
            .field(field(BODY, TypeTag::Bytes)),
    )?;
    let notes_read = with_notes
        .checked_sub(judging)
        .expect("the notes add to what judging reads");
    // A third of the notes is kept; the rest read is the row groups the scan
    // reads with the rest while it sees its choices thin out, a row group
    // being one batch of a cursor reading everything.
    let all_notes = u64::from(rows) * 200;
    assert!(
        notes_read * 5 < all_notes * 3,
        "{notes_read} bytes of notes read for a third of the rows, out of {all_notes}"
    );
    Ok(())
}

/// Rows the predicate keeps all at the start of a table turn the scan to
/// reading the note with the rest, and the long run of rows it drops after
/// them turns it back: most of the notes are never read.
#[test]
fn a_dense_start_does_not_keep_a_sparse_scan_reading_everything() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let (any, tree) = open_paged(folder.path())?;
    let rows = 3_000u32;
    for i in 0..rows {
        insert(
            &tree,
            &format!("k{i:05}"),
            b"s",
            i,
            &[b'n'; 200],
            u64::from(i),
        );
    }
    tree.flush_active_memtable(0)?;
    let m = any.metrics();
    let metadata = Projection::new()
        .column(COL_USER_KEY)
        .field(field(PRICE, u32_le()));
    rows_of_scan(&any, metadata, None)?;

    let first_200 = ColumnRangePredicate {
        column_id: PRICE,
        lower: Some(0u32.to_be_bytes().to_vec()),
        upper: Some(199u32.to_be_bytes().to_vec()),
        apply: PredicateApply::Filter,
    };
    let notes = Projection::new()
        .column(COL_USER_KEY)
        .field(field(PRICE, u32_le()))
        .field(field(BODY, TypeTag::Bytes));
    let before = m.bytes_read();
    assert_eq!(rows_of_scan(&any, notes, Some(&first_200))?.len(), 200);
    let read = m.bytes_read() - before;
    let all_notes = u64::from(rows) * 200;
    assert!(
        read * 2 < all_notes,
        "{read} bytes read for 200 notes out of {all_notes} bytes of them"
    );
    Ok(())
}

/// The density of the choices is judged over the last pages read, not the
/// whole segment so far: after a dense third of the table the scan turns back
/// to reading only the kept rows' notes within a few pages, instead of waiting
/// for three times as many sparse pages as it saw dense ones.
#[test]
fn a_long_dense_start_gives_way_to_a_sparse_tail_within_a_few_pages() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let (any, tree) = open_paged(folder.path())?;
    let rows = 3_000u32;
    for i in 0..rows {
        insert(
            &tree,
            &format!("k{i:05}"),
            b"s",
            i,
            &[b'n'; 200],
            u64::from(i),
        );
    }
    tree.flush_active_memtable(0)?;
    let m = any.metrics();
    let metadata = Projection::new()
        .column(COL_USER_KEY)
        .field(field(PRICE, u32_le()));
    rows_of_scan(&any, metadata, None)?;

    let first_third = ColumnRangePredicate {
        column_id: PRICE,
        lower: Some(0u32.to_be_bytes().to_vec()),
        upper: Some((rows / 3 - 1).to_be_bytes().to_vec()),
        apply: PredicateApply::Filter,
    };
    let notes = Projection::new()
        .column(COL_USER_KEY)
        .field(field(PRICE, u32_le()))
        .field(field(BODY, TypeTag::Bytes));
    let before = m.bytes_read();
    assert_eq!(
        rows_of_scan(&any, notes, Some(&first_third))?.len(),
        (rows / 3) as usize
    );
    let read = m.bytes_read() - before;
    let all_notes = u64::from(rows) * 200;
    // A third of the notes is kept; the rest read is the few pages the scan
    // needs to see the choices thinned out.
    assert!(
        read * 2 < all_notes,
        "{read} bytes read for a third of the notes out of {all_notes} bytes of them"
    );
    Ok(())
}

/// A predicate keeping one row per page needs every page of the note: the
/// scan reads it with the rest once it sees its choices are dense, and
/// returns the same rows as one that reads every note.
#[test]
fn a_dense_choice_reads_the_payload_with_the_rest() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let (any, tree) = open_paged(folder.path())?;
    let rows = 2_000;
    paged_notes(&tree, rows)?;
    let every_fourth_price = Projection::new()
        .column(COL_USER_KEY)
        .field(field(PRICE, u32_le()))
        .field(field(BODY, TypeTag::Bytes));
    let all = rows_of_scan(&any, every_fourth_price.clone(), None)?;
    // Prices 0..=rows: every row kept, the densest choice there is.
    let kept = ColumnRangePredicate {
        column_id: PRICE,
        lower: Some(0u32.to_be_bytes().to_vec()),
        upper: Some(rows.to_be_bytes().to_vec()),
        apply: PredicateApply::Filter,
    };
    assert_eq!(rows_of_scan(&any, every_fourth_price, Some(&kept))?, all);
    Ok(())
}

/// A predicate that keeps every row costs the scan no copy of its output
/// beyond the one a scan without a predicate makes: rows already judged on
/// their stored column are not gathered again to apply it once more.
#[test]
fn a_predicate_keeping_every_row_copies_no_more_than_none() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let (any, tree) = open_paged(folder.path())?;
    let rows = 2_000u32;
    for i in 0..rows {
        insert(
            &tree,
            &format!("k{i:05}"),
            b"s",
            i,
            &[b'n'; 200],
            u64::from(i),
        );
    }
    tree.flush_active_memtable(0)?;
    let m = any.metrics();
    let notes = Projection::new()
        .column(COL_USER_KEY)
        .field(field(PRICE, u32_le()))
        .field(field(BODY, TypeTag::Bytes));
    let every_price = ColumnRangePredicate {
        column_id: PRICE,
        lower: Some(0u32.to_be_bytes().to_vec()),
        upper: Some(rows.to_be_bytes().to_vec()),
        apply: PredicateApply::Filter,
    };

    let before = m.bytes_copied();
    let all = rows_of_scan(&any, notes.clone(), None)?;
    let unfiltered = m.bytes_copied() - before;
    let before = m.bytes_copied();
    let kept = rows_of_scan(&any, notes, Some(&every_price))?;
    let filtered = m.bytes_copied() - before;
    assert_eq!(kept, all);
    assert!(
        filtered <= unfiltered,
        "copied {filtered} bytes with a predicate keeping every row, {unfiltered} without one"
    );
    Ok(())
}

/// A scan reads the objects of the version it was created on: a compaction
/// that relocates the bodies, and drops the blob file they were in, between
/// the scan's creation and its reads, changes nothing it returns.
#[test]
fn a_scan_reads_its_objects_through_a_compaction_that_moves_them() -> lsm_tree::Result<()> {
    for columnar in [false, true] {
        let folder = get_tmp_folder();
        let any = Config::new(
            folder.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(
            KvSeparationOptions::default()
                .separation_threshold(THRESHOLD)
                .age_cutoff(1.0),
        ))
        .blob_compression(lsm_tree::CompressionType::None)
        .open()?;
        let AnyTree::Blob(tree) = &any else {
            panic!("a tree with kv separation opens as a blob tree");
        };
        tree.index
            .update_runtime_config(|rc| rc.columnar = columnar)?;
        let body = |i: u32| vec![b'a' + (i % 26) as u8; 300];
        for i in 0..50u32 {
            insert(tree, &format!("k{i:02}"), b"s", i, &body(i), u64::from(i));
        }
        // A filler in the same blob file that turns to garbage, so the next
        // compaction relocates the bodies.
        tree.insert("zz-filler", vec![b'f'; 16_384], 50);
        tree.flush_active_memtable(0)?;
        tree.insert("zz-filler", "small", 51);
        tree.flush_active_memtable(0)?;
        tree.major_compact(64_000_000, SeqNo::MAX)?;

        let projection = Projection::new()
            .column(COL_USER_KEY)
            .field(field(BODY, TypeTag::Bytes));
        // The documents only: the filler is stored whole, with no projector.
        let documents = ..lsm_tree::UserKey::from("zz");
        let scan = any.columnar_scan(projection, None, SeqNo::MAX, documents)?;
        // The filler is charged, and nothing has moved the bodies yet.
        assert!(
            tree.stale_blob_bytes() > 0,
            "columnar={columnar}: the bodies' file holds garbage when the scan is made"
        );
        tree.major_compact(64_000_000, SeqNo::MAX)?;
        tree.major_compact(64_000_000, SeqNo::MAX)?;
        // The bodies moved and their old file went, under the scan.
        assert_eq!(
            tree.stale_blob_bytes(),
            0,
            "columnar={columnar}: the compactions relocated the bodies"
        );

        let got = rows(scan)?;
        assert_eq!(got.len(), 50, "columnar={columnar}");
        for (key, cells) in &got {
            let i: u32 = std::str::from_utf8(&key[1..])
                .expect("utf8")
                .parse()
                .expect("a number");
            assert_eq!(cells[0], Some(body(i)), "columnar={columnar}");
        }
    }
    Ok(())
}

fn rows_of_scan(
    any: &AnyTree,
    projection: Projection,
    predicate: Option<&ColumnRangePredicate>,
) -> lsm_tree::Result<Rows> {
    rows(any.columnar_scan(projection, predicate, SeqNo::MAX, ..)?)
}

/// The scan returns, for every key, the fields a point read of it returns,
/// across updates, deletions, flushes and compactions.
#[test]
fn the_scan_agrees_with_point_reads() -> lsm_tree::Result<()> {
    for columnar in [false, true] {
        let folder = get_tmp_folder();
        let (any, tree) = open(folder.path(), columnar)?;
        let mut seqno = 0;
        for round in 0..4u32 {
            for i in 0..30u32 {
                if (i + round) % 7 == 0 {
                    tree.remove(format!("k{i:02}"), seqno);
                } else {
                    let body = vec![b'0' + (i % 10) as u8; if i % 3 == 0 { 10 } else { 300 }];
                    insert(
                        &tree,
                        &format!("k{i:02}"),
                        b"s",
                        round * 100 + i,
                        &body,
                        seqno,
                    );
                }
                seqno += 1;
            }
            tree.flush_active_memtable(0)?;
            if round == 2 {
                tree.major_compact(64_000_000, SeqNo::MAX)?;
            }
        }

        let projection = Projection::new()
            .column(COL_USER_KEY)
            .field(field(PRICE, u32_le()))
            .field(field(BODY, TypeTag::Bytes));
        let got = rows(any.columnar_scan(projection, None, SeqNo::MAX, ..)?)?;
        let mut expected = Rows::new();
        for i in 0..30u32 {
            let key = format!("k{i:02}");
            let Some(row) = tree.get_cells(&key, SeqNo::MAX).ok().flatten() else {
                continue;
            };
            expected.push((
                key.into_bytes(),
                vec![
                    row.resolve(PRICE)?.map(|v| v.to_vec()),
                    row.resolve(BODY)?.map(|v| v.to_vec()),
                ],
            ));
        }
        assert_eq!(got, expected, "columnar={columnar}");
    }
    Ok(())
}

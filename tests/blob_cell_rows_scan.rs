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
        let before = any.metrics().blob_read_count();
        let got = rows(any.columnar_scan(projection, Some(&cheap), SeqNo::MAX, ..)?)?;
        let keys: Vec<&[u8]> = got.iter().map(|(key, _)| key.as_slice()).collect();
        assert_eq!(
            keys,
            [&b"k01"[..], b"k02", b"k03", b"k04"],
            "columnar={columnar}: k00's newest version is not cheap"
        );
        assert_eq!(
            any.metrics().blob_read_count() - before,
            4,
            "columnar={columnar}: one body read per row kept"
        );
    }
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
    let sparse = rows_of_scan(&any, notes.clone(), Some(&cheap))?;
    let sparse_read = m.bytes_read() - before;
    assert_eq!(sparse.len(), 20);
    for (key, cells) in &sparse {
        let i: u32 = std::str::from_utf8(&key[1..])
            .expect("utf8")
            .parse()
            .expect("a number");
        let round = u8::from(i.is_multiple_of(3));
        assert_eq!(cells[1], Some(vec![b'a' + round + (i % 20) as u8; 200]));
    }

    let before = m.bytes_read();
    let dense = rows_of_scan(&any, notes, None)?;
    let dense_read = m.bytes_read() - before;
    assert_eq!(dense.len(), rows as usize);
    assert!(
        sparse_read * 10 < dense_read,
        "the sparse scan read {sparse_read} bytes of notes, the rest of them took {dense_read}"
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

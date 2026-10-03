// Rows written as cells in a blob tree whose tables are columnar: each cell
// row is split into the columns of its fields, and every read gives back the
// row as it was written, through flushes, compactions and a reopen.

#![cfg(feature = "columnar")]

use lsm_tree::{
    AbstractTree, AnyTree, BlobTree, Config, Guard, KvSeparationOptions, SeqNo,
    SequenceNumberCounter,
    blob_tree::field_row::{Cell, FIRST_FIELD_COLUMN, Field},
    get_tmp_folder,
    table::column_type::{ByteOrder, Number, NumberKind},
};
use test_log::test;

const STATUS: u16 = FIRST_FIELD_COLUMN;
const SCORE: u16 = FIRST_FIELD_COLUMN + 1;
const BODY: u16 = FIRST_FIELD_COLUMN + 2;

fn open(path: &std::path::Path) -> lsm_tree::Result<BlobTree> {
    let tree = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(64),
    ))
    .blob_compression(lsm_tree::CompressionType::None)
    .open()?;
    let AnyTree::Blob(tree) = tree else {
        panic!("a tree with kv separation opens as a blob tree");
    };
    tree.index.update_runtime_config(|rc| rc.columnar = true)?;
    Ok(tree)
}

fn u32_le() -> lsm_tree::blob_tree::field_row::TypeTag {
    lsm_tree::blob_tree::field_row::TypeTag::Number(
        Number::new(NumberKind::Unsigned, 4, ByteOrder::Little).expect("a u32"),
    )
}

/// What a plain read of a document row returns: the status length-prefixed,
/// the score bare, the body length-prefixed.
fn document(status: &[u8], score: u32, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    for (bytes, prefixed) in [
        (status, true),
        (&score.to_le_bytes()[..], false),
        (body, true),
    ] {
        if prefixed {
            out.extend_from_slice(&u32::try_from(bytes.len()).expect("small").to_le_bytes());
        }
        out.extend_from_slice(bytes);
    }
    out
}

fn insert_document(
    tree: &BlobTree,
    key: &str,
    status: &[u8],
    score: u32,
    body: &[u8],
    seqno: SeqNo,
) -> lsm_tree::Result<()> {
    let score = score.to_le_bytes();
    tree.insert_cells(
        key,
        &[
            Field::bytes(STATUS, status),
            Field {
                column: SCORE,
                tag: u32_le(),
                cell: Cell::Value(&score),
            },
            Field::bytes(BODY, body),
        ],
        seqno,
    )?;
    Ok(())
}

/// Cell rows beside plain values, a large value and a deletion, flushed into a
/// columnar table: every key reads back as written, by point read, by range,
/// and as cells with the body a reference.
#[test]
fn cell_rows_read_back_from_a_columnar_table() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path())?;
    let body = vec![b'b'; 1_000];
    insert_document(&tree, "doc-a", b"draft", 1, &body, 0)?;
    insert_document(&tree, "doc-b", b"final", 2, b"short", 1)?;
    tree.insert("plain", "value", 2);
    tree.insert("large", vec![b'l'; 500], 3);
    tree.insert("gone", "x", 4);
    tree.remove("gone", 5);
    tree.flush_active_memtable(0)?;
    assert!(
        tree.index
            .current_version()
            .iter_tables()
            .all(|t| t.metadata.columnar)
    );

    assert_eq!(
        tree.get("doc-a", SeqNo::MAX)?.as_deref(),
        Some(&document(b"draft", 1, &body)[..])
    );
    assert_eq!(
        tree.get("doc-b", SeqNo::MAX)?.as_deref(),
        Some(&document(b"final", 2, b"short")[..])
    );
    assert_eq!(
        tree.get("plain", SeqNo::MAX)?.as_deref(),
        Some(&b"value"[..])
    );
    assert_eq!(
        tree.get("large", SeqNo::MAX)?.as_deref(),
        Some(&vec![b'l'; 500][..])
    );
    assert!(tree.get("gone", SeqNo::MAX)?.is_none());

    let keys: Vec<Vec<u8>> = tree
        .iter(SeqNo::MAX, None)
        .map(|guard| guard.key().map(|key| key.to_vec()))
        .collect::<lsm_tree::Result<_>>()?;
    assert_eq!(
        keys,
        [&b"doc-a"[..], b"doc-b", b"large", b"plain"].map(<[u8]>::to_vec)
    );

    let row = tree.get_cells("doc-a", SeqNo::MAX)?.expect("written");
    let fields = row.fields()?;
    assert_eq!(
        fields.iter().map(|f| f.column).collect::<Vec<_>>(),
        [STATUS, SCORE, BODY]
    );
    assert!(matches!(fields[0].cell, Cell::Value(b"draft")));
    assert!(matches!(&fields[2].cell, Cell::Ref(r) if r.size() == 1_000));
    assert_eq!(row.resolve(BODY)?.as_deref(), Some(&body[..]));
    Ok(())
}

/// A metadata-only update of a row in a columnar table keeps its body where it
/// is, and the rows survive a major compaction and a reopen, the object still
/// charged once when its last holder goes.
#[test]
fn cell_rows_survive_compaction_and_reopen_in_columnar_tables() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let body = vec![b'b'; 4_096];
    {
        let tree = open(folder.path())?;
        insert_document(&tree, "doc", b"draft", 1, &body, 0)?;
        tree.flush_active_memtable(0)?;
        let blob_bytes = tree.current_version().blob_files.on_disk_size();

        let row = tree.get_cells("doc", SeqNo::MAX)?.expect("written");
        let mut fields = row.fields()?;
        fields[0].cell = Cell::Value(b"final");
        tree.insert_cells("doc", &fields, 1)?;
        drop(row);
        tree.flush_active_memtable(0)?;
        assert_eq!(
            tree.current_version().blob_files.on_disk_size(),
            blob_bytes,
            "the update wrote no blob bytes"
        );

        tree.major_compact(64_000_000, SeqNo::MAX)?;
        assert_eq!(tree.stale_blob_bytes(), 0, "the body passed to the update");
        assert_eq!(
            tree.get("doc", SeqNo::MAX)?.as_deref(),
            Some(&document(b"final", 1, &body)[..])
        );
    }
    let tree = open(folder.path())?;
    assert_eq!(
        tree.get("doc", SeqNo::MAX)?.as_deref(),
        Some(&document(b"final", 1, &body)[..])
    );
    tree.insert("doc", "gone", 2);
    tree.flush_active_memtable(0)?;
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    assert_eq!(tree.stale_blob_bytes(), 4_096, "the body is charged once");
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    assert_eq!(tree.blob_file_count(), 0);
    Ok(())
}

/// Two rows that hold one field id under different types land in one table:
/// the second cannot share the first one's column, so it is kept whole, and
/// both read back as written.
#[test]
fn rows_of_one_column_id_under_two_types_both_read_back() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path())?;
    let score = 9u32.to_le_bytes();
    tree.insert_cells(
        "a",
        &[Field {
            column: SCORE,
            tag: u32_le(),
            cell: Cell::Value(&score),
        }],
        0,
    )?;
    tree.insert_cells("b", &[Field::bytes(SCORE, b"nine")], 0)?;
    tree.flush_active_memtable(0)?;
    assert_eq!(tree.get("a", SeqNo::MAX)?.as_deref(), Some(&score[..]));
    assert_eq!(
        tree.get("b", SeqNo::MAX)?.as_deref(),
        Some(&b"\x04\0\0\0nine"[..])
    );
    let row = tree.get_cells("b", SeqNo::MAX)?.expect("written");
    assert!(matches!(row.fields()?[0].cell, Cell::Value(b"nine")));
    Ok(())
}

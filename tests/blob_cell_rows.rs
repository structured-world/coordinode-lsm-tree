// Rows written as cells, each heavy cell kept in a blob file on its own, and
// metadata-only updates that keep a heavy cell without rewriting it.

use lsm_tree::{
    AbstractTree, AnyTree, BlobTree, Config, Guard, KvSeparationOptions, SeqNo,
    SequenceNumberCounter,
    blob_tree::field_row::{Cell, FIRST_FIELD_COLUMN, Field, RowCells, TypeTag},
    fs::{CrashFs, Fault, FaultFs, FaultOp, FaultRule, MemFs},
    get_tmp_folder,
    io::ErrorKind,
    table::column_type::{ByteOrder, Number, NumberKind},
};
use std::sync::Arc;
use test_log::test;

/// Cells at or above this many bytes go to a blob file at flush.
const THRESHOLD: u32 = 64;

fn open(path: &std::path::Path, opts: KvSeparationOptions) -> lsm_tree::Result<BlobTree> {
    let tree = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(opts.separation_threshold(THRESHOLD)))
    .blob_compression(lsm_tree::CompressionType::None)
    .open()?;
    let AnyTree::Blob(tree) = tree else {
        panic!("a tree with kv separation opens as a blob tree");
    };
    Ok(tree)
}

/// The logical value a plain read returns for a row: every cell as a
/// little-endian `u32` length and its bytes.
fn framed(cells: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    for cell in cells {
        out.extend_from_slice(&u32::try_from(cell.len()).expect("small cell").to_le_bytes());
        out.extend_from_slice(cell);
    }
    out
}

/// The column of a row's first field: the status of a document, or its only
/// field.
const STATUS: u16 = FIRST_FIELD_COLUMN;
/// The column of a row's second field: the body of a document.
const BODY: u16 = FIRST_FIELD_COLUMN + 1;

/// A row of byte fields, field `i` in column `STATUS + i`.
fn bytes<'a>(values: &[&'a [u8]]) -> Vec<Field<'a>> {
    values
        .iter()
        .zip(STATUS..)
        .map(|(value, column)| Field::bytes(column, value))
        .collect()
}

/// The latest version of `key`, read as cells.
fn row_of(tree: &BlobTree, key: &str) -> lsm_tree::Result<RowCells> {
    Ok(tree.get_cells(key, SeqNo::MAX)?.expect("row exists"))
}

/// The field `row` holds in `column` as a reference, borrowed from the read.
fn reference(row: &RowCells, column: u16) -> lsm_tree::Result<Field<'_>> {
    let field = row
        .fields()?
        .into_iter()
        .find(|field| field.column == column)
        .expect("the row has the column");
    assert!(
        matches!(field.cell, Cell::Ref(_)),
        "column {column} is not a reference"
    );
    Ok(field)
}

/// A row reads back as its cells before and after the flush that moves its
/// heavy cell into a blob file, and only the heavy cell moves.
#[test]
fn a_cell_row_reads_back_before_and_after_flush() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), KvSeparationOptions::default())?;
    let body = vec![b'b'; 1_000];

    tree.insert_cells("doc", &bytes(&[b"draft", &body]), 0)?;
    assert_eq!(
        tree.get("doc", SeqNo::MAX)?.as_deref(),
        Some(&framed(&[b"draft", &body])[..])
    );

    tree.flush_active_memtable(0)?;
    assert_eq!(tree.blob_file_count(), 1);
    assert_eq!(
        tree.get("doc", SeqNo::MAX)?.as_deref(),
        Some(&framed(&[b"draft", &body])[..])
    );
    assert_eq!(
        tree.size_of("doc", SeqNo::MAX)?,
        Some(u32::try_from(framed(&[b"draft", &body]).len()).expect("small row"))
    );
    // A range read sizes the row the same way, without reading its object.
    let guard = tree
        .prefix("doc", SeqNo::MAX, None)
        .next()
        .expect("the row");
    assert_eq!(
        guard.size()?,
        u32::try_from(framed(&[b"draft", &body]).len()).expect("small row")
    );

    // Only the body became a reference; the status stayed in the row.
    let row = tree.get_cells("doc", SeqNo::MAX)?.expect("row exists");
    let fields = row.fields()?;
    assert!(matches!(fields[0].cell, Cell::Value(b"draft")));
    assert!(matches!(&fields[1].cell, Cell::Ref(r) if r.size() == 1_000));
    assert_eq!(
        fields.iter().map(|f| (f.column, f.tag)).collect::<Vec<_>>(),
        [(STATUS, TypeTag::Bytes), (BODY, TypeTag::Bytes)],
        "a separated field keeps its column and type"
    );
    Ok(())
}

/// A metadata-only update keeps the heavy cell where it is: flushing it writes
/// no blob bytes, and the old and new versions both read back.
#[test]
fn a_metadata_only_update_writes_no_blob_bytes() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), KvSeparationOptions::default())?;
    let body = vec![b'b'; 4_096];

    tree.insert_cells("doc", &bytes(&[b"draft", &body]), 0)?;
    tree.flush_active_memtable(0)?;
    let blob_bytes = tree.current_version().blob_files.on_disk_size();

    let row = row_of(&tree, "doc")?;
    assert!(format!("{row:?}").contains("RowCells"), "{row:?}");
    let body_ref = reference(&row, BODY)?;
    let Cell::Ref(blob_ref) = &body_ref.cell else {
        panic!("the body is a reference");
    };
    assert!(
        tree.current_version()
            .blob_files
            .contains_key(blob_ref.blob_file_id()),
        "the reference names the blob file the flush wrote"
    );
    tree.insert_cells("doc", &[Field::bytes(STATUS, b"final"), body_ref], 1)?;
    tree.flush_active_memtable(0)?;

    assert_eq!(tree.blob_file_count(), 1, "the update wrote no blob file");
    assert_eq!(
        tree.current_version().blob_files.on_disk_size(),
        blob_bytes,
        "the update wrote no blob bytes"
    );
    assert_eq!(
        tree.get("doc", SeqNo::MAX)?.as_deref(),
        Some(&framed(&[b"final", &body])[..])
    );
    assert_eq!(
        tree.get("doc", 1)?.as_deref(),
        Some(&framed(&[b"draft", &body])[..]),
        "the older version still reads at its snapshot"
    );
    Ok(())
}

/// Two versions share one object; collecting the older one leaves the object
/// uncharged and readable through the newer one. Only when no version holds
/// it is it charged, once, and its file goes.
#[test]
fn a_shared_object_is_charged_once_when_its_last_holder_goes() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), KvSeparationOptions::default())?;
    let body = vec![b'b'; 4_096];

    tree.insert_cells("doc", &bytes(&[b"draft", &body]), 0)?;
    tree.flush_active_memtable(0)?;
    let row = row_of(&tree, "doc")?;
    tree.insert_cells(
        "doc",
        &[Field::bytes(STATUS, b"final"), reference(&row, BODY)?],
        1,
    )?;
    drop(row);
    tree.flush_active_memtable(0)?;

    // The owner is collected, the borrower survives: nothing is garbage.
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    assert_eq!(
        tree.stale_blob_bytes(),
        0,
        "a borrowed object is not charged"
    );
    assert_eq!(tree.blob_file_count(), 1);
    assert_eq!(
        tree.get("doc", SeqNo::MAX)?.as_deref(),
        Some(&framed(&[b"final", &body])[..])
    );

    // A plain overwrite lets go of the object: it is charged once, not once
    // per version that held it, and the next install drops the file, which
    // holds nothing else.
    tree.insert("doc", "gone", 2);
    tree.flush_active_memtable(0)?;
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    assert_eq!(tree.stale_blob_bytes(), 4_096, "the object is charged once");
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    assert_eq!(tree.blob_file_count(), 0, "the file held only the object");
    assert_eq!(tree.get("doc", SeqNo::MAX)?.as_deref(), Some(&b"gone"[..]));
    Ok(())
}

/// A weak delete consumes the put before it whatever form the put takes in a
/// blob tree, a cell row or a separated value: the table counts the pair as
/// reclaimable, and a compaction below the watermark drops both and charges
/// the objects they owned.
#[test]
fn a_weak_delete_consumes_a_cell_row_and_a_separated_value() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), KvSeparationOptions::default())?;
    let body = vec![b'b'; 4_096];

    // Both pairs in one table: its writer sees each put right after its delete.
    tree.insert_cells("doc", &bytes(&[b"draft", &body]), 0)?;
    tree.remove_weak("doc", 1);
    tree.insert("plain", body.clone(), 2);
    tree.remove_weak("plain", 3);
    tree.flush_active_memtable(0)?;
    assert_eq!(tree.weak_tombstone_count(), 2);
    assert_eq!(tree.weak_tombstone_reclaimable_count(), 2);
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    assert_eq!(tree.weak_tombstone_count(), 0);
    for key in ["doc", "plain"] {
        assert_eq!(tree.get(key, SeqNo::MAX)?, None);
    }

    // The puts and the deletes in different tables: the compaction meets them.
    let folder = get_tmp_folder();
    let tree = open(folder.path(), KvSeparationOptions::default())?;
    tree.insert_cells("doc", &bytes(&[b"draft", &body]), 0)?;
    tree.insert("plain", body.clone(), 1);
    tree.flush_active_memtable(0)?;
    tree.remove_weak("doc", 2);
    tree.remove_weak("plain", 3);
    tree.flush_active_memtable(0)?;
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    assert_eq!(
        tree.weak_tombstone_count(),
        0,
        "each weak delete leaves with the put it consumed"
    );
    assert_eq!(tree.stale_blob_bytes(), 2 * 4_096);
    for key in ["doc", "plain"] {
        assert_eq!(tree.get(key, SeqNo::MAX)?, None);
    }
    Ok(())
}

/// A reference read from one key is refused under another, and one object in
/// two cells is refused: either would let an object outlive its accounting.
#[test]
fn a_reference_is_refused_under_another_key_or_twice_in_a_row() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), KvSeparationOptions::default())?;
    let body = vec![b'b'; 1_000];
    tree.insert_cells("a", &bytes(&[&body]), 0)?;
    tree.flush_active_memtable(0)?;
    let row = row_of(&tree, "a")?;
    let body_ref = reference(&row, STATUS)?;

    let foreign = tree.insert_cells("b", &[body_ref], 1);
    assert!(
        matches!(foreign, Err(lsm_tree::Error::BlobRef(_))),
        "{foreign:?}"
    );
    let twice = tree.insert_cells(
        "a",
        &[
            body_ref,
            Field {
                column: BODY,
                ..body_ref
            },
        ],
        1,
    );
    assert!(
        matches!(twice, Err(lsm_tree::Error::BlobRef(_))),
        "{twice:?}"
    );
    assert!(tree.get("b", SeqNo::MAX)?.is_none(), "nothing was written");
    Ok(())
}

/// A reference names a frame by file number and offset, and file numbers are
/// local to a tree: one read from another tree is refused even when that tree
/// has the same key with a frame at the same file number and offset, rather
/// than resolving to whatever the other tree keeps there.
#[test]
fn a_reference_from_another_tree_is_refused() -> lsm_tree::Result<()> {
    let folder_a = get_tmp_folder();
    let folder_b = get_tmp_folder();
    let a = open(folder_a.path(), KvSeparationOptions::default())?;
    let b = open(folder_b.path(), KvSeparationOptions::default())?;
    a.insert_cells("doc", &bytes(&[&vec![b'a'; 1_000]]), 0)?;
    a.flush_active_memtable(0)?;
    b.insert_cells("doc", &bytes(&[&vec![b'b'; 1_000]]), 0)?;
    b.flush_active_memtable(0)?;

    let row = row_of(&a, "doc")?;
    let refused = b.insert_cells("doc", &[reference(&row, STATUS)?], 1);
    assert!(
        matches!(refused, Err(lsm_tree::Error::BlobRef(_))),
        "{refused:?}"
    );
    assert_eq!(
        b.get("doc", SeqNo::MAX)?.as_deref(),
        Some(&framed(&[&vec![b'b'; 1_000]])[..])
    );
    Ok(())
}

/// A reference whose key was overwritten and whose owner was collected is
/// stale even while its blob file lives on for another object: writing it
/// would make a row hold an object the accounting already charged as
/// garbage, so it is refused.
#[test]
fn a_reference_whose_owner_was_collected_is_refused() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), KvSeparationOptions::default())?;
    tree.insert_cells("doc", &bytes(&[&vec![b'd'; 1_000]]), 0)?;
    // Another object in the same blob file keeps the file alive.
    tree.insert_cells("other", &bytes(&[&vec![b'o'; 1_000]]), 0)?;
    tree.flush_active_memtable(0)?;

    let row = row_of(&tree, "doc")?;
    let stale = reference(&row, STATUS)?;
    tree.insert("doc", "overwritten", 1);
    tree.flush_active_memtable(0)?;
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    assert_eq!(
        tree.blob_file_count(),
        1,
        "the file lives on for the other row"
    );

    let refused = tree.insert_cells("doc", &[stale], 2);
    assert!(
        matches!(refused, Err(lsm_tree::Error::BlobRef(_))),
        "{refused:?}"
    );
    assert_eq!(
        tree.get("doc", SeqNo::MAX)?.as_deref(),
        Some(&b"overwritten"[..])
    );
    Ok(())
}

/// A version not written as cells has no cells to hand out.
#[test]
fn get_cells_refuses_a_plain_value() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), KvSeparationOptions::default())?;
    tree.insert("plain", "value", 0);
    assert!(matches!(
        tree.get_cells("plain", SeqNo::MAX),
        Err(lsm_tree::Error::BlobRef(_))
    ));
    assert!(tree.get_cells("absent", SeqNo::MAX)?.is_none());
    Ok(())
}

/// A range read of cells hands out the rows written as cells, in key order,
/// across the memtable and the tables, and passes over keys stored whole or
/// deleted; a metadata-only update of every row through it writes no blob
/// bytes.
#[test]
fn range_cells_reads_cell_rows_for_a_metadata_only_update() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), KvSeparationOptions::default())?;
    let body = vec![b'b'; 1_000];
    tree.insert_cells("a", &bytes(&[b"draft", &body]), 0)?;
    tree.insert("b-plain", "value", 1);
    tree.insert_cells("c", &bytes(&[b"draft", &body]), 2)?;
    tree.insert_cells("d-gone", &bytes(&[b"draft", &body]), 3)?;
    tree.flush_active_memtable(0)?;
    tree.remove("d-gone", 4);
    tree.insert_cells("e", &bytes(&[b"draft", &body]), 5)?;
    tree.flush_active_memtable(0)?;
    // A row in the memtable is read as well as the flushed ones.
    tree.insert_cells("f", &bytes(&[b"draft", b"short"]), 6)?;
    let blob_bytes = tree.current_version().blob_files.on_disk_size();

    let rows: Vec<RowCells> = tree
        .range_cells::<&str, _>(.., SeqNo::MAX)?
        .collect::<lsm_tree::Result<_>>()?;
    let keys: Vec<&[u8]> = rows.iter().map(|row| &**row.key()).collect();
    assert_eq!(keys, [&b"a"[..], b"c", b"e", b"f"]);

    let in_range: Vec<RowCells> = tree
        .range_cells("b".."d", SeqNo::MAX)?
        .collect::<lsm_tree::Result<_>>()?;
    assert_eq!(in_range.len(), 1, "only c lies in b..d");

    for (seqno, row) in (10..).zip(&rows) {
        let mut fields = row.fields()?;
        fields[0].cell = Cell::Value(b"final");
        tree.insert_cells(row.key().clone(), &fields, seqno)?;
    }
    drop(rows);
    tree.flush_active_memtable(0)?;
    assert_eq!(
        tree.current_version().blob_files.on_disk_size(),
        blob_bytes,
        "the update wrote no blob bytes"
    );
    for key in ["a", "c", "e"] {
        assert_eq!(
            tree.get(key, SeqNo::MAX)?.as_deref(),
            Some(&framed(&[b"final", &body])[..])
        );
    }
    assert_eq!(
        tree.get("f", SeqNo::MAX)?.as_deref(),
        Some(&framed(&[b"final", b"short"])[..])
    );
    Ok(())
}

/// Rows of one range read share the read: a row held after the others are
/// dropped still resolves its object through a relocation that moved it,
/// and its reference is then refused as stale.
#[test]
fn a_row_of_a_range_read_keeps_its_read() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(
        folder.path(),
        KvSeparationOptions::default().age_cutoff(1.0),
    )?;
    let body = vec![b'b'; 4_096];
    stale_file_with_a_body(&tree, &body)?;

    let mut rows = tree
        .range_cells::<&str, _>(.., SeqNo::MAX)?
        .collect::<lsm_tree::Result<Vec<_>>>()?;
    assert_eq!(rows.len(), 1);
    let row = rows.remove(0);
    drop(rows);
    tree.major_compact(64_000_000, SeqNo::MAX)?;

    assert_eq!(row.resolve(BODY)?.as_deref(), Some(&body[..]));
    let stale = tree.insert_cells(
        "doc",
        &[Field::bytes(STATUS, b"x"), reference(&row, BODY)?],
        2,
    );
    assert!(
        matches!(stale, Err(lsm_tree::Error::BlobRef(_))),
        "{stale:?}"
    );
    Ok(())
}

/// Builds a blob file holding `doc`'s body and a garbage value, so a major
/// compaction relocates the body.
fn stale_file_with_a_body(tree: &BlobTree, body: &[u8]) -> lsm_tree::Result<()> {
    let filler = vec![b'f'; 8_192];
    tree.insert_cells("doc", &bytes(&[b"draft", body]), 0)?;
    tree.insert("filler", &filler, 0);
    tree.flush_active_memtable(0)?;
    tree.insert("filler", "small", 1);
    tree.flush_active_memtable(0)?;
    // The compaction that drops the old filler charges it; files are picked
    // for relocation by what is charged before a compaction starts.
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    assert_eq!(tree.stale_blob_bytes(), 8_192, "the filler is garbage");
    Ok(())
}

/// A relocation moves the body; the reference read before it is refused as
/// stale, and a fresh read hands out the moved one.
#[test]
fn a_reference_moved_by_relocation_is_refused_as_stale() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(
        folder.path(),
        KvSeparationOptions::default().age_cutoff(1.0),
    )?;
    let body = vec![b'b'; 4_096];
    stale_file_with_a_body(&tree, &body)?;

    let old_row = row_of(&tree, "doc")?;
    let before = reference(&old_row, BODY)?;
    tree.major_compact(64_000_000, SeqNo::MAX)?;

    let stale = tree.insert_cells("doc", &[Field::bytes(STATUS, b"final"), before], 2);
    assert!(
        matches!(stale, Err(lsm_tree::Error::BlobRef(_))),
        "{stale:?}"
    );
    // The read that handed the stale reference out still resolves it: it
    // holds the version it saw, whose blob file the relocation retired.
    assert_eq!(old_row.resolve(BODY)?.as_deref(), Some(&body[..]));
    assert_eq!(old_row.resolve(STATUS)?.as_deref(), Some(&b"draft"[..]));
    assert_eq!(
        old_row.resolve(BODY + 7)?,
        None,
        "the row has no such column"
    );

    let new_row = row_of(&tree, "doc")?;
    tree.insert_cells(
        "doc",
        &[Field::bytes(STATUS, b"final"), reference(&new_row, BODY)?],
        2,
    )?;
    assert_eq!(
        tree.get("doc", SeqNo::MAX)?.as_deref(),
        Some(&framed(&[b"final", &body])[..])
    );
    Ok(())
}

/// A whole-table drop of the table owning an object charges it, yet the blob
/// file stays while a kept table borrows the object: the newest version still
/// reads it after the owner's table is dropped.
#[test]
fn a_dropped_owner_keeps_the_file_a_kept_table_borrows_from() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), KvSeparationOptions::default())?;
    let body = vec![b'b'; 4_096];
    tree.insert_cells("doc", &bytes(&[b"draft", &body]), 0)?;
    tree.flush_active_memtable(0)?;
    let row = row_of(&tree, "doc")?;
    tree.insert_cells(
        "doc",
        &[Field::bytes(STATUS, b"final"), reference(&row, BODY)?],
        1,
    )?;
    drop(row);
    // A second key keeps the borrower's table out of the dropped range.
    tree.insert("zzz", "other", 1);
    tree.flush_active_memtable(0)?;
    assert_eq!(tree.table_count(), 2);

    // Only the owner's table lies wholly inside the range.
    tree.drop_range("doc"..="doc")?;
    assert_eq!(tree.table_count(), 1, "the owner's table is dropped");
    assert_eq!(tree.blob_file_count(), 1, "the borrowed body's file stays");
    assert_eq!(
        tree.get("doc", SeqNo::MAX)?.as_deref(),
        Some(&framed(&[b"final", &body])[..])
    );
    Ok(())
}

/// Two kept versions of a key that hold one object, moved by a relocation,
/// share the one copy the relocation writes: the object is copied once, and
/// the newest version reads it.
#[test]
fn a_relocation_copies_an_object_its_versions_share_once() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(
        folder.path(),
        KvSeparationOptions::default().age_cutoff(1.0),
    )?;
    let body = vec![b'b'; 4_096];
    stale_file_with_a_body(&tree, &body)?;
    let row = row_of(&tree, "doc")?;
    tree.insert_cells(
        "doc",
        &[Field::bytes(STATUS, b"final"), reference(&row, BODY)?],
        2,
    )?;
    drop(row);
    tree.flush_active_memtable(0)?;

    // Every version is kept, so both holders of the body are in the pass.
    tree.major_compact(64_000_000, 0)?;
    let on_disk = tree.current_version().blob_files.on_disk_size();
    assert!(
        on_disk < 2 * body.len() as u64,
        "the shared body was copied once, not per version: {on_disk} bytes"
    );
    assert_eq!(
        tree.get("doc", SeqNo::MAX)?.as_deref(),
        Some(&framed(&[b"final", &body])[..])
    );
    assert_eq!(
        tree.stale_blob_bytes(),
        0,
        "the copy is owned once and live"
    );
    Ok(())
}

/// A row in the memtable that borrows an object keeps its blob file through a
/// relocation that moves every table's reference out of it: no table links
/// the file for that row until it is flushed.
#[test]
fn a_memtable_reference_keeps_its_file_through_relocation() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(
        folder.path(),
        KvSeparationOptions::default().age_cutoff(1.0),
    )?;
    let body = vec![b'b'; 4_096];
    stale_file_with_a_body(&tree, &body)?;

    let row = row_of(&tree, "doc")?;
    tree.insert_cells(
        "doc",
        &[Field::bytes(STATUS, b"final"), reference(&row, BODY)?],
        2,
    )?;
    drop(row);
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    assert_eq!(
        tree.get("doc", SeqNo::MAX)?.as_deref(),
        Some(&framed(&[b"final", &body])[..]),
        "the memtable row still reads its object"
    );

    // Once the row is in a table and that table is compacted, the old file
    // can go and the row still reads.
    tree.flush_active_memtable(0)?;
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    assert_eq!(
        tree.get("doc", SeqNo::MAX)?.as_deref(),
        Some(&framed(&[b"final", &body])[..])
    );
    Ok(())
}

/// Dropping the table that owns an object keeps the file while a memtable row
/// borrows the object.
#[test]
fn drop_range_keeps_a_file_a_memtable_row_borrows_from() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), KvSeparationOptions::default())?;
    let body = vec![b'b'; 4_096];
    tree.insert_cells("doc", &bytes(&[b"draft", &body]), 0)?;
    tree.flush_active_memtable(0)?;

    let row = row_of(&tree, "doc")?;
    tree.insert_cells(
        "doc",
        &[Field::bytes(STATUS, b"final"), reference(&row, BODY)?],
        1,
    )?;
    drop(row);
    tree.drop_range::<&str, _>(..)?;
    assert_eq!(tree.table_count(), 0, "the owner's table is gone");
    assert_eq!(
        tree.blob_file_count(),
        1,
        "the borrowed object's file stays"
    );
    assert_eq!(
        tree.get("doc", SeqNo::MAX)?.as_deref(),
        Some(&framed(&[b"final", &body])[..])
    );
    Ok(())
}

/// Rows, their references and the blob accounting are what the tables say:
/// after a reopen the rows read back, hand out working references, and the
/// shared object is still charged once.
#[test]
fn cell_rows_survive_a_reopen() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let body = vec![b'b'; 4_096];
    {
        let tree = open(folder.path(), KvSeparationOptions::default())?;
        tree.insert_cells("doc", &bytes(&[b"draft", &body]), 0)?;
        tree.flush_active_memtable(0)?;
        let row = row_of(&tree, "doc")?;
        tree.insert_cells(
            "doc",
            &[Field::bytes(STATUS, b"final"), reference(&row, BODY)?],
            1,
        )?;
        drop(row);
        tree.flush_active_memtable(0)?;
    }
    let tree = open(folder.path(), KvSeparationOptions::default())?;
    assert_eq!(
        tree.get("doc", SeqNo::MAX)?.as_deref(),
        Some(&framed(&[b"final", &body])[..])
    );
    let row = row_of(&tree, "doc")?;
    tree.insert_cells(
        "doc",
        &[Field::bytes(STATUS, b"done"), reference(&row, BODY)?],
        2,
    )?;
    drop(row);
    tree.flush_active_memtable(0)?;
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    assert_eq!(tree.stale_blob_bytes(), 0);
    assert_eq!(
        tree.get("doc", SeqNo::MAX)?.as_deref(),
        Some(&framed(&[b"done", &body])[..])
    );
    Ok(())
}

/// A snapshot held below a compaction's watermark keeps the versions it reads
/// and the objects they hold.
#[test]
fn a_held_snapshot_keeps_the_objects_it_reads() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), KvSeparationOptions::default())?;
    let first = vec![b'1'; 4_096];
    let second = vec![b'2'; 4_096];
    tree.insert_cells("doc", &bytes(&[&first]), 0)?;
    tree.flush_active_memtable(0)?;
    tree.insert_cells("doc", &bytes(&[&second]), 1)?;
    tree.flush_active_memtable(0)?;

    // A reader at seqno 1 still sees the first version: the watermark keeps it.
    tree.major_compact(64_000_000, 1)?;
    assert_eq!(tree.get("doc", 1)?.as_deref(), Some(&framed(&[&first])[..]));
    assert_eq!(
        tree.get("doc", SeqNo::MAX)?.as_deref(),
        Some(&framed(&[&second])[..])
    );
    Ok(())
}

fn open_on(fs: Arc<dyn lsm_tree::fs::Fs>) -> lsm_tree::Result<BlobTree> {
    let tree = Config::new(
        "/db",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(THRESHOLD),
    ))
    .blob_compression(lsm_tree::CompressionType::None)
    .with_shared_fs(fs)
    .open()?;
    let AnyTree::Blob(tree) = tree else {
        panic!("a tree with kv separation opens as a blob tree");
    };
    Ok(tree)
}

/// Fails the next manifest commit, as a power loss right after it would.
fn fail_next_commit(injector: &lsm_tree::fs::FaultInjector) {
    injector.arm(
        FaultRule::new(FaultOp::SyncData, Fault::Error(ErrorKind::Other))
            .on_path("edits")
            .once(),
    );
}

/// A crash between publishing a borrowed reference and dropping its owner, at
/// either end, loses no object and leaks none: reachability is what the
/// durable tables say, and the accounting rebuilt from them still charges the
/// object exactly once when its last holder goes.
#[test]
fn an_interrupted_publication_loses_and_leaks_no_object() -> lsm_tree::Result<()> {
    let crash = CrashFs::new(MemFs::new());
    let body = vec![b'b'; 4_096];

    // The borrower's flush never becomes durable: the owner alone holds it.
    {
        let fault = FaultFs::new(crash.clone());
        let injector = fault.injector();
        let tree = open_on(Arc::new(fault))?;
        tree.insert_cells("doc", &bytes(&[b"draft", &body]), 0)?;
        tree.flush_active_memtable(0)?;
        let row = row_of(&tree, "doc")?;
        tree.insert_cells(
            "doc",
            &[Field::bytes(STATUS, b"final"), reference(&row, BODY)?],
            1,
        )?;
        drop(row);
        fail_next_commit(&injector);
        assert!(tree.flush_active_memtable(0).is_err());
    }
    crash.crash();
    {
        let tree = open_on(crash.inner())?;
        assert_eq!(
            tree.get("doc", SeqNo::MAX)?.as_deref(),
            Some(&framed(&[b"draft", &body])[..]),
            "the durable owner still reads its object"
        );
        assert_eq!(tree.stale_blob_bytes(), 0);
        assert_eq!(tree.blob_file_count(), 1);
    }

    // The borrower is durable; the compaction that drops the owner is not.
    {
        let fault = FaultFs::new(crash.clone());
        let injector = fault.injector();
        let tree = open_on(Arc::new(fault))?;
        let row = row_of(&tree, "doc")?;
        tree.insert_cells(
            "doc",
            &[Field::bytes(STATUS, b"final"), reference(&row, BODY)?],
            1,
        )?;
        drop(row);
        tree.flush_active_memtable(0)?;
        fail_next_commit(&injector);
        assert!(tree.major_compact(64_000_000, SeqNo::MAX).is_err());
    }
    crash.crash();
    let tree = open_on(crash.inner())?;
    assert_eq!(
        tree.get("doc", SeqNo::MAX)?.as_deref(),
        Some(&framed(&[b"final", &body])[..]),
    );
    assert_eq!(
        tree.stale_blob_bytes(),
        0,
        "nothing was charged by the lost edit"
    );

    // The compaction runs again, then the object's last holder goes: it is
    // charged once and its file is reclaimed.
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    assert_eq!(tree.stale_blob_bytes(), 0);
    tree.insert("doc", "gone", 2);
    tree.flush_active_memtable(0)?;
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    assert_eq!(tree.stale_blob_bytes(), 4_096, "the object is charged once");
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    assert_eq!(tree.blob_file_count(), 0, "no object leaked");
    Ok(())
}

/// Per-field references multiply the references per row without breaking
/// the locality accounting: rows of three heavy cells, then a metadata-only
/// update of every row borrowing all three, then a compaction, keep one blob
/// file read in one run, and nothing in it is garbage.
#[test]
fn per_field_references_keep_the_locality_accounting() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), KvSeparationOptions::default())?;
    let fields: Vec<Vec<u8>> = (0..3u8).map(|f| vec![b'a' + f; 1_000]).collect();
    let one_run = lsm_tree::BlobReferenceStats { count: 1, depth: 1 };

    for i in 0..10u64 {
        tree.insert_cells(
            format!("k{i:02}"),
            &bytes(&[b"draft", &fields[0], &fields[1], &fields[2]]),
            i,
        )?;
    }
    tree.flush_active_memtable(0)?;
    assert_eq!(tree.storage_stats()?.blob_references, one_run);

    for i in 0..10u64 {
        let key = format!("k{i:02}");
        let row = tree.get_cells(&key, SeqNo::MAX)?.expect("row exists");
        let mut updated = row.fields()?;
        updated[0].cell = Cell::Value(b"final");
        tree.insert_cells(key, &updated, 10 + i)?;
    }
    tree.flush_active_memtable(0)?;
    assert_eq!(tree.blob_file_count(), 1, "the update wrote no blob file");
    assert_eq!(tree.storage_stats()?.blob_references, one_run);

    tree.major_compact(64_000_000, SeqNo::MAX)?;
    assert_eq!(tree.storage_stats()?.blob_references, one_run);
    assert_eq!(
        tree.stale_blob_bytes(),
        0,
        "every object passed to its update"
    );
    for i in 0..10u64 {
        assert_eq!(
            tree.get(format!("k{i:02}"), SeqNo::MAX)?.as_deref(),
            Some(&framed(&[b"final", &fields[0], &fields[1], &fields[2]])[..])
        );
    }
    Ok(())
}

/// Each column separates at its own threshold, wherever its field sits in
/// the row: a large field kept with the compact attributes stays inline, a
/// small rarely read one goes to a blob file, and a column without its own
/// threshold uses the tree's.
#[test]
fn each_column_separates_at_its_own_threshold() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(
        folder.path(),
        KvSeparationOptions::default()
            .cell_separation_threshold(10, u32::MAX)
            .cell_separation_threshold(20, 0),
    )?;
    let large = vec![b'l'; 4_096];
    // Given out of order: the threshold follows the column, not the place,
    // and the row is stored in column order.
    tree.insert_cells(
        "doc",
        &[
            Field::bytes(20, b"tiny"),
            Field::bytes(10, &large),
            Field::bytes(30, &large),
        ],
        0,
    )?;
    tree.flush_active_memtable(0)?;

    let row = tree.get_cells("doc", SeqNo::MAX)?.expect("row exists");
    let fields = row.fields()?;
    assert_eq!(
        fields.iter().map(|f| f.column).collect::<Vec<_>>(),
        [10, 20, 30]
    );
    assert!(
        matches!(fields[0].cell, Cell::Value(_)),
        "column 10 never separates"
    );
    assert!(
        matches!(fields[1].cell, Cell::Ref(_)),
        "column 20 always separates"
    );
    assert!(
        matches!(fields[2].cell, Cell::Ref(_)),
        "column 30 uses the tree's threshold"
    );
    assert_eq!(
        tree.get("doc", SeqNo::MAX)?.as_deref(),
        Some(&framed(&[&large, b"tiny", &large])[..])
    );
    Ok(())
}

/// Ingestion separates each heavy field on its own, as a flush does.
#[test]
fn ingested_fields_separate_per_field() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), KvSeparationOptions::default())?;
    let body = vec![b'b'; 1_000];
    let mut ingestion = lsm_tree::blob_tree::ingest::BlobIngestion::new(&tree)?;
    ingestion.write_cells("doc".into(), &bytes(&[b"draft", &body]))?;
    ingestion.finish()?;

    assert_eq!(tree.blob_file_count(), 1);
    assert_eq!(
        tree.get("doc", SeqNo::MAX)?.as_deref(),
        Some(&framed(&[b"draft", &body])[..])
    );
    let row = tree.get_cells("doc", SeqNo::MAX)?.expect("row exists");
    assert!(matches!(row.fields()?[1].cell, Cell::Ref(_)));
    Ok(())
}

/// Rows written as cells are ingested in key order, each read back with its
/// own fields.
#[test]
fn ingested_cell_rows_read_back_in_key_order() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), KvSeparationOptions::default())?;
    let body = vec![b'b'; 1_000];
    let mut ingestion = lsm_tree::blob_tree::ingest::BlobIngestion::new(&tree)?;
    for key in ["a", "b", "c"] {
        ingestion.write_cells(key.into(), &bytes(&[key.as_bytes(), &body]))?;
    }
    ingestion.finish()?;
    for key in ["a", "b", "c"] {
        assert_eq!(
            tree.get(key, SeqNo::MAX)?.as_deref(),
            Some(&framed(&[key.as_bytes(), &body])[..])
        );
    }
    Ok(())
}

/// A row ingested out of key order is a caller's bug, refused before any of
/// its fields reaches a blob file.
#[test]
#[should_panic(expected = "next key in ingestion must be ordered")]
fn a_cell_row_ingested_out_of_order_panics() {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), KvSeparationOptions::default()).expect("open");
    let mut ingestion =
        lsm_tree::blob_tree::ingest::BlobIngestion::new(&tree).expect("an ingestion");
    ingestion
        .write_cells("b".into(), &bytes(&[b"second"]))
        .expect("the first row");
    ingestion
        .write_cells("a".into(), &bytes(&[b"first"]))
        .expect("the out-of-order write panics before it returns");
}

/// An ingested key has no earlier version, so a reference in an ingested
/// row would name an object no version of the key holds: it is refused.
#[test]
fn an_ingested_reference_is_refused() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), KvSeparationOptions::default())?;
    tree.insert_cells("doc", &bytes(&[&vec![b'b'; 1_000]]), 0)?;
    tree.flush_active_memtable(0)?;
    let row = row_of(&tree, "doc")?;

    let other_folder = get_tmp_folder();
    let other = open(other_folder.path(), KvSeparationOptions::default())?;
    let mut ingestion = lsm_tree::blob_tree::ingest::BlobIngestion::new(&other)?;
    let refused = ingestion.write_cells("doc".into(), &[reference(&row, STATUS)?]);
    assert!(
        matches!(refused, Err(lsm_tree::Error::BlobRef(_))),
        "{refused:?}"
    );
    Ok(())
}

/// Two fields in one column, a fixed-width field of another width, or a
/// column that is not a field id, are refused before anything is written.
#[test]
fn a_row_with_an_ambiguous_layout_is_refused() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), KvSeparationOptions::default())?;
    let u32_le = TypeTag::Number(Number::new(NumberKind::Unsigned, 4, ByteOrder::Little)?);

    let twice = tree.insert_cells(
        "doc",
        &[Field::bytes(STATUS, b"a"), Field::bytes(STATUS, b"b")],
        0,
    );
    assert!(
        matches!(twice, Err(lsm_tree::Error::CellRow(_))),
        "{twice:?}"
    );
    let narrow = Field {
        column: STATUS,
        tag: u32_le,
        cell: Cell::Value(b"abc"),
    };
    let wrong_width = tree.insert_cells("doc", &[narrow], 0);
    assert!(
        matches!(wrong_width, Err(lsm_tree::Error::CellRow(_))),
        "{wrong_width:?}"
    );
    // The key column's id: a columnar table could not store the field as it.
    let intrinsic = tree.insert_cells("doc", &[Field::bytes(0, b"a")], 0);
    assert!(
        matches!(intrinsic, Err(lsm_tree::Error::CellRow(_))),
        "{intrinsic:?}"
    );
    assert!(
        tree.get("doc", SeqNo::MAX)?.is_none(),
        "nothing was written"
    );
    Ok(())
}

/// A row of typed fields reads as the columnar format frames a row's value
/// sub-columns: fixed-width fields bare, byte fields length-prefixed, in the
/// memtable and after the flush that separates the byte field. The types
/// come back with the fields.
#[test]
fn typed_fields_read_with_their_columnar_framing() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), KvSeparationOptions::default())?;
    let u64_le = TypeTag::Number(Number::new(NumberKind::Unsigned, 8, ByteOrder::Little)?);
    let id = 42u64.to_le_bytes();
    let body = vec![b'b'; 1_000];
    let row = [
        Field {
            column: STATUS,
            tag: u64_le,
            cell: Cell::Value(&id),
        },
        Field {
            column: BODY,
            tag: TypeTag::Fixed(2),
            cell: Cell::Value(b"ok"),
        },
        Field::bytes(BODY + 1, &body),
    ];
    let mut expected = Vec::new();
    expected.extend_from_slice(&id);
    expected.extend_from_slice(b"ok");
    expected.extend_from_slice(&framed(&[&body]));

    tree.insert_cells("doc", &row, 0)?;
    assert_eq!(tree.get("doc", SeqNo::MAX)?.as_deref(), Some(&expected[..]));
    tree.flush_active_memtable(0)?;
    assert_eq!(tree.get("doc", SeqNo::MAX)?.as_deref(), Some(&expected[..]));
    assert_eq!(
        tree.size_of("doc", SeqNo::MAX)?,
        Some(u32::try_from(expected.len()).expect("small row"))
    );

    let read = row_of(&tree, "doc")?;
    let fields = read.fields()?;
    assert_eq!(
        fields.iter().map(|f| (f.column, f.tag)).collect::<Vec<_>>(),
        [
            (STATUS, u64_le),
            (BODY, TypeTag::Fixed(2)),
            (BODY + 1, TypeTag::Bytes)
        ]
    );
    assert!(matches!(fields[2].cell, Cell::Ref(_)));
    assert_eq!(read.resolve(STATUS)?.as_deref(), Some(&id[..]));
    Ok(())
}

/// Appends each operand to the base.
struct Concat;

impl lsm_tree::MergeOperator for Concat {
    fn merge(
        &self,
        _key: &[u8],
        base: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> lsm_tree::Result<lsm_tree::UserValue> {
        let mut out = base.unwrap_or_default().to_vec();
        for operand in operands {
            out.extend_from_slice(operand);
        }
        Ok(out.into())
    }
}

/// A merge operator over a row written as cells receives the row's logical
/// value, its body read from its blob file, exactly as a plain read returns
/// it: in the memtable, flushed, and after a compaction folds the operands.
#[test]
fn a_merge_operator_folds_onto_a_cell_rows_logical_value() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let AnyTree::Blob(tree) = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(THRESHOLD),
    ))
    .with_merge_operator(Some(Arc::new(Concat)))
    .blob_compression(lsm_tree::CompressionType::None)
    .open()?
    else {
        panic!("a tree with kv separation opens as a blob tree");
    };
    let body = vec![b'b'; 1_000];
    tree.insert_cells("doc", &bytes(&[b"draft", &body]), 0)?;
    let mut expected = framed(&[b"draft", &body]);
    expected.extend_from_slice(b"+one");
    tree.merge("doc", "+one", 1);
    assert_eq!(tree.get("doc", SeqNo::MAX)?.as_deref(), Some(&expected[..]));
    tree.flush_active_memtable(0)?;
    assert_eq!(tree.get("doc", SeqNo::MAX)?.as_deref(), Some(&expected[..]));
    tree.merge("doc", "+two", 2);
    expected.extend_from_slice(b"+two");
    tree.flush_active_memtable(0)?;
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    assert_eq!(tree.get("doc", SeqNo::MAX)?.as_deref(), Some(&expected[..]));
    Ok(())
}

/// A checkpoint of a tree whose rows borrow objects holds every object they
/// reference: the checkpoint opens on its own, reads the rows, hands out
/// their references, and a metadata-only update in it writes no blob bytes.
#[test]
fn a_checkpoint_holds_the_objects_its_rows_reference() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let checkpoint = get_tmp_folder();
    let target = checkpoint.path().join("copy");
    let body = vec![b'b'; 4_096];
    {
        let tree = open(folder.path(), KvSeparationOptions::default())?;
        tree.insert_cells("doc", &bytes(&[b"draft", &body]), 0)?;
        tree.flush_active_memtable(0)?;
        let row = row_of(&tree, "doc")?;
        tree.insert_cells(
            "doc",
            &[Field::bytes(STATUS, b"final"), reference(&row, BODY)?],
            1,
        )?;
        drop(row);
        tree.create_checkpoint(&target)?;
        // The original moves on: its owner version collected, the file kept
        // for the borrower.
        tree.major_compact(64_000_000, SeqNo::MAX)?;
    }
    let copy = open(&target, KvSeparationOptions::default())?;
    assert_eq!(
        copy.get("doc", SeqNo::MAX)?.as_deref(),
        Some(&framed(&[b"final", &body])[..])
    );
    let blob_bytes = copy.current_version().blob_files.on_disk_size();
    let row = row_of(&copy, "doc")?;
    copy.insert_cells(
        "doc",
        &[Field::bytes(STATUS, b"done"), reference(&row, BODY)?],
        2,
    )?;
    drop(row);
    copy.flush_active_memtable(0)?;
    assert_eq!(copy.current_version().blob_files.on_disk_size(), blob_bytes);
    assert_eq!(
        copy.get("doc", SeqNo::MAX)?.as_deref(),
        Some(&framed(&[b"done", &body])[..])
    );
    Ok(())
}

// Rows written as cells, each heavy cell kept in a blob file on its own, and
// metadata-only updates that keep a heavy cell without rewriting it.

use lsm_tree::{
    AbstractTree, AnyTree, BlobTree, Config, KvSeparationOptions, SeqNo, SequenceNumberCounter,
    blob_tree::field_row::{Cell, RowCells},
    fs::{CrashFs, Fault, FaultFs, FaultOp, FaultRule, MemFs},
    get_tmp_folder,
    io::ErrorKind,
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

/// The latest version of `key`, read as cells.
fn row_of(tree: &BlobTree, key: &str) -> lsm_tree::Result<RowCells> {
    Ok(tree.get_cells(key, SeqNo::MAX)?.expect("row exists"))
}

/// The reference `row` holds in cell `index`, borrowed from the read.
fn reference(row: &RowCells, index: usize) -> lsm_tree::Result<Cell<'_>> {
    match row.cells()?.swap_remove(index) {
        reference @ Cell::Ref(_) => Ok(reference),
        Cell::Value(_) => panic!("cell {index} is not a reference"),
    }
}

/// A row reads back as its cells before and after the flush that moves its
/// heavy cell into a blob file, and only the heavy cell moves.
#[test]
fn a_cell_row_reads_back_before_and_after_flush() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), KvSeparationOptions::default())?;
    let body = vec![b'b'; 1_000];

    tree.insert_cells("doc", &[Cell::Value(b"draft"), Cell::Value(&body)], 0)?;
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

    // Only the body became a reference; the status stayed in the row.
    let row = tree.get_cells("doc", SeqNo::MAX)?.expect("row exists");
    let cells = row.cells()?;
    assert!(matches!(cells[0], Cell::Value(b"draft")));
    assert!(matches!(&cells[1], Cell::Ref(r) if r.size() == 1_000));
    Ok(())
}

/// A metadata-only update keeps the heavy cell where it is: flushing it writes
/// no blob bytes, and the old and new versions both read back.
#[test]
fn a_metadata_only_update_writes_no_blob_bytes() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), KvSeparationOptions::default())?;
    let body = vec![b'b'; 4_096];

    tree.insert_cells("doc", &[Cell::Value(b"draft"), Cell::Value(&body)], 0)?;
    tree.flush_active_memtable(0)?;
    let blob_bytes = tree.current_version().blob_files.on_disk_size();

    let row = row_of(&tree, "doc")?;
    let body_ref = reference(&row, 1)?;
    tree.insert_cells("doc", &[Cell::Value(b"final"), body_ref], 1)?;
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

    tree.insert_cells("doc", &[Cell::Value(b"draft"), Cell::Value(&body)], 0)?;
    tree.flush_active_memtable(0)?;
    let row = row_of(&tree, "doc")?;
    tree.insert_cells("doc", &[Cell::Value(b"final"), reference(&row, 1)?], 1)?;
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

/// A reference read from one key is refused under another, and one object in
/// two cells is refused: either would let an object outlive its accounting.
#[test]
fn a_reference_is_refused_under_another_key_or_twice_in_a_row() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), KvSeparationOptions::default())?;
    let body = vec![b'b'; 1_000];
    tree.insert_cells("a", &[Cell::Value(&body)], 0)?;
    tree.flush_active_memtable(0)?;
    let row = row_of(&tree, "a")?;
    let body_ref = reference(&row, 0)?;

    let foreign = tree.insert_cells("b", &[body_ref], 1);
    assert!(
        matches!(foreign, Err(lsm_tree::Error::BlobRef(_))),
        "{foreign:?}"
    );
    let twice = tree.insert_cells("a", &[body_ref, body_ref], 1);
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
    a.insert_cells("doc", &[Cell::Value(&vec![b'a'; 1_000])], 0)?;
    a.flush_active_memtable(0)?;
    b.insert_cells("doc", &[Cell::Value(&vec![b'b'; 1_000])], 0)?;
    b.flush_active_memtable(0)?;

    let row = row_of(&a, "doc")?;
    let refused = b.insert_cells("doc", &[reference(&row, 0)?], 1);
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
    tree.insert_cells("doc", &[Cell::Value(&vec![b'd'; 1_000])], 0)?;
    // Another object in the same blob file keeps the file alive.
    tree.insert_cells("other", &[Cell::Value(&vec![b'o'; 1_000])], 0)?;
    tree.flush_active_memtable(0)?;

    let row = row_of(&tree, "doc")?;
    let stale = reference(&row, 0)?;
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

/// Builds a blob file holding `doc`'s body and a garbage value, so a major
/// compaction relocates the body.
fn stale_file_with_a_body(tree: &BlobTree, body: &[u8]) -> lsm_tree::Result<()> {
    let filler = vec![b'f'; 8_192];
    tree.insert_cells("doc", &[Cell::Value(b"draft"), Cell::Value(body)], 0)?;
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
    let before = reference(&old_row, 1)?;
    tree.major_compact(64_000_000, SeqNo::MAX)?;

    let stale = tree.insert_cells("doc", &[Cell::Value(b"final"), before], 2);
    assert!(
        matches!(stale, Err(lsm_tree::Error::BlobRef(_))),
        "{stale:?}"
    );
    // The read that handed the stale reference out still resolves it: it
    // holds the version it saw, whose blob file the relocation retired.
    assert_eq!(&*old_row.resolve(1)?, &body[..]);

    let new_row = row_of(&tree, "doc")?;
    tree.insert_cells("doc", &[Cell::Value(b"final"), reference(&new_row, 1)?], 2)?;
    assert_eq!(
        tree.get("doc", SeqNo::MAX)?.as_deref(),
        Some(&framed(&[b"final", &body])[..])
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
    tree.insert_cells("doc", &[Cell::Value(b"final"), reference(&row, 1)?], 2)?;
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
    tree.insert_cells("doc", &[Cell::Value(b"draft"), Cell::Value(&body)], 0)?;
    tree.flush_active_memtable(0)?;

    let row = row_of(&tree, "doc")?;
    tree.insert_cells("doc", &[Cell::Value(b"final"), reference(&row, 1)?], 1)?;
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
        tree.insert_cells("doc", &[Cell::Value(b"draft"), Cell::Value(&body)], 0)?;
        tree.flush_active_memtable(0)?;
        let row = row_of(&tree, "doc")?;
        tree.insert_cells("doc", &[Cell::Value(b"final"), reference(&row, 1)?], 1)?;
        drop(row);
        tree.flush_active_memtable(0)?;
    }
    let tree = open(folder.path(), KvSeparationOptions::default())?;
    assert_eq!(
        tree.get("doc", SeqNo::MAX)?.as_deref(),
        Some(&framed(&[b"final", &body])[..])
    );
    let row = row_of(&tree, "doc")?;
    tree.insert_cells("doc", &[Cell::Value(b"done"), reference(&row, 1)?], 2)?;
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
    tree.insert_cells("doc", &[Cell::Value(&first)], 0)?;
    tree.flush_active_memtable(0)?;
    tree.insert_cells("doc", &[Cell::Value(&second)], 1)?;
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
        tree.insert_cells("doc", &[Cell::Value(b"draft"), Cell::Value(&body)], 0)?;
        tree.flush_active_memtable(0)?;
        let row = row_of(&tree, "doc")?;
        tree.insert_cells("doc", &[Cell::Value(b"final"), reference(&row, 1)?], 1)?;
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
        tree.insert_cells("doc", &[Cell::Value(b"final"), reference(&row, 1)?], 1)?;
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
            &[
                Cell::Value(b"draft"),
                Cell::Value(&fields[0]),
                Cell::Value(&fields[1]),
                Cell::Value(&fields[2]),
            ],
            i,
        )?;
    }
    tree.flush_active_memtable(0)?;
    assert_eq!(tree.storage_stats()?.blob_references, one_run);

    for i in 0..10u64 {
        let key = format!("k{i:02}");
        let row = tree.get_cells(&key, SeqNo::MAX)?.expect("row exists");
        let mut cells = row.cells()?;
        cells[0] = Cell::Value(b"final");
        tree.insert_cells(key, &cells, 10 + i)?;
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

/// Each cell position separates at its own threshold: a large field kept
/// with the compact attributes stays inline, a small rarely read one goes to
/// a blob file, and a position without its own threshold uses the tree's.
#[test]
fn each_cell_position_separates_at_its_own_threshold() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(
        folder.path(),
        KvSeparationOptions::default().cell_separation_thresholds(vec![u32::MAX, 0]),
    )?;
    let large = vec![b'l'; 4_096];
    tree.insert_cells(
        "doc",
        &[
            Cell::Value(&large),
            Cell::Value(b"tiny"),
            Cell::Value(&large),
        ],
        0,
    )?;
    tree.flush_active_memtable(0)?;

    let row = tree.get_cells("doc", SeqNo::MAX)?.expect("row exists");
    let cells = row.cells()?;
    assert!(
        matches!(cells[0], Cell::Value(_)),
        "position 0 never separates"
    );
    assert!(
        matches!(cells[1], Cell::Ref(_)),
        "position 1 always separates"
    );
    assert!(
        matches!(cells[2], Cell::Ref(_)),
        "position 2 uses the tree's threshold"
    );
    assert_eq!(
        tree.get("doc", SeqNo::MAX)?.as_deref(),
        Some(&framed(&[&large, b"tiny", &large])[..])
    );
    Ok(())
}

/// Ingestion separates each heavy cell on its own, as a flush does.
#[test]
fn ingested_cells_separate_per_cell() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), KvSeparationOptions::default())?;
    let body = vec![b'b'; 1_000];
    let mut ingestion = lsm_tree::blob_tree::ingest::BlobIngestion::new(&tree)?;
    ingestion.write_cells("doc".into(), &[b"draft", &body])?;
    ingestion.finish()?;

    assert_eq!(tree.blob_file_count(), 1);
    assert_eq!(
        tree.get("doc", SeqNo::MAX)?.as_deref(),
        Some(&framed(&[b"draft", &body])[..])
    );
    let row = tree.get_cells("doc", SeqNo::MAX)?.expect("row exists");
    assert!(matches!(row.cells()?[1], Cell::Ref(_)));
    Ok(())
}

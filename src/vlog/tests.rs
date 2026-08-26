use super::*;
use test_log::test;

#[test]
fn vlog_recovery_missing_blob_file_returns_unrecoverable() {
    // Manifest says blob id=0 exists, but the blobs folder is empty.
    // Recovery should fail with Unrecoverable because blob_files.len() < ids.len().
    let dir = tempfile::tempdir().unwrap();
    let result = recover_blob_files(
        dir.path(),
        &[(0, Checksum::from_raw(0), 0)],
        0,
        None,
        &(Arc::new(crate::fs::StdFs) as Arc<dyn crate::fs::Fs>),
    );
    assert!(matches!(result, Err(crate::Error::Unrecoverable)));
}

#[test]
fn vlog_recovery_nonexistent_folder_no_ids_returns_empty() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("no_such_dir");
    let (blob_files, orphans) = recover_blob_files(
        &missing,
        &[],
        0,
        None,
        &(Arc::new(crate::fs::StdFs) as Arc<dyn crate::fs::Fs>),
    )
    .unwrap();
    assert!(blob_files.is_empty());
    assert!(orphans.is_empty());
}

#[test]
fn vlog_recovery_nonexistent_folder_with_ids_returns_unrecoverable() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("no_such_dir");
    let result = recover_blob_files(
        &missing,
        &[(0, Checksum::from_raw(0), 0)],
        0,
        None,
        &(Arc::new(crate::fs::StdFs) as Arc<dyn crate::fs::Fs>),
    );
    assert!(matches!(result, Err(crate::Error::Unrecoverable)));
}

/// A crashed manifest repair leaves its in-progress salvage copy behind. It
/// is published by an atomic rename, so a surviving one was never referenced
/// by any manifest: recovery must sweep it like any other orphan rather than
/// abort on a name that is not a blob id, which would leave the tree
/// unopenable until an operator intervened.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn vlog_recovery_sweeps_a_crashed_salvage_copy() {
    let dir = tempfile::tempdir().unwrap();
    let leftover = dir.path().join("7.salvage-tmp");
    std::fs::write(&leftover, b"partial salvage copy").unwrap();

    let (blob_files, orphans) = recover_blob_files(
        dir.path(),
        &[],
        0,
        None,
        &(Arc::new(crate::fs::StdFs) as Arc<dyn crate::fs::Fs>),
    )
    .expect("a repair's own leftover must not make the tree unopenable");

    assert!(blob_files.is_empty(), "the leftover is not a blob file");
    assert_eq!(orphans, vec![leftover], "it is swept as an orphan");
}

#[test]
fn recover_blob_file_on_non_blob_file_errors() {
    // A file that is not a valid blob (no SFA trailer / `meta` section) must
    // surface an error instead of producing a bogus BlobFile, so the repair
    // caller can report it and skip it rather than wiring corruption into the
    // rebuilt manifest.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("000042");
    std::fs::write(&path, b"this is not a blob file").unwrap();

    let fs: Arc<dyn crate::fs::Fs> = Arc::new(crate::fs::StdFs);
    let result = recover_blob_file(path.as_path(), 42, Checksum::from_raw(0), 0, &fs);
    // `BlobFile` is not `Debug`, so assert on the boolean rather than the value.
    assert!(result.is_err(), "recovering a non-blob file must fail");
}

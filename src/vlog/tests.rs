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

/// A crashed manifest repair can leave its own working files in `blobs/`: a
/// `{id}.remap` sidecar (the durable record of a published replacement's
/// offset relocation) and the `.tmp_{pid}_{seq}` staging file its atomic
/// write goes through. Neither parses as a blob id, so without an explicit
/// skip the plain open aborts the whole recovery and the tree cannot be
/// opened at all — the operator loses the retry that would finish the
/// relocation. The sidecar must SURVIVE (the retry reads it); the staging
/// file is never-referenced garbage and is swept like any other orphan.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn vlog_recovery_keeps_a_remap_sidecar_and_sweeps_its_staging_file() {
    let dir = tempfile::tempdir().unwrap();
    let sidecar = dir.path().join("7.remap");
    let staging = dir.path().join(".tmp_1234_9");
    std::fs::write(&sidecar, b"remap payload").unwrap();
    std::fs::write(&staging, b"partial").unwrap();

    let (blob_files, orphans) = recover_blob_files(
        dir.path(),
        &[],
        0,
        None,
        &(Arc::new(crate::fs::StdFs) as Arc<dyn crate::fs::Fs>),
    )
    .expect("a repair's own leftovers must not make the tree unopenable");

    assert!(blob_files.is_empty(), "neither leftover is a blob file");
    assert_eq!(
        orphans,
        vec![staging],
        "the staging file is swept; the sidecar is NOT (a repair retry \
         needs it to finish the relocation)",
    );
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

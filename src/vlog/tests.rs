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

/// The blob twin of the table-prefix rule: a reclaim whose link probe cannot
/// answer (or that reports a COMPLETED checkpoint's surviving link) while the
/// deletion pause is inactive must be RETAINED for `retry_pending_reclaims`,
/// not discarded — the dropping view holds the only record of the reclaim,
/// and nothing else would ever free the consumed prefix once it is gone.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn blob_punch_on_drop_retains_the_reclaim_when_the_link_probe_cannot_answer() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;

    let memfs = MemFs::new();
    let root = std::path::absolute("/blobs")?;
    memfs.create_dir_all(&root)?;
    let path = root.join("0");
    {
        let fs_dyn: Arc<dyn Fs> = Arc::new(memfs.clone());
        let mut w = super::blob_file::writer::Writer::new(path.clone(), 0, 0, &*fs_dyn)?;
        w.write(b"a", 1, &[b'x'; 64])?;
        w.write(b"b", 2, &[b'y'; 64])?;
        w.finish()?;
    }
    // The consumed prefix ends where the second frame begins.
    let first_frame_end = super::BlobFileScanner::new(&path, &memfs, 0)?
        .next()
        .expect("first frame")?
        .frame_end;

    let fault = FaultFs::new(memfs.clone());
    let injector = fault.injector();
    injector.arm(FaultRule::new(
        FaultOp::HardLinkCount,
        Fault::Error(ErrorKind::PermissionDenied),
    ));
    let fs: Arc<dyn Fs> = Arc::new(fault);

    let blob = recover_blob_file(&path, 0, Checksum::from_raw(0), 0, &fs)?;
    let pause = crate::deletion_pause::DeletionPause::new_shared();
    blob.install_deletion_pause(Arc::clone(&pause));
    blob.mark_punch_on_drop(first_frame_end);
    drop(blob);

    assert!(
        pause.has_pending_reclaims(),
        "a blob reclaim whose link probe cannot answer must be retained for a \
         retry, not discarded",
    );

    // The probe recovers (the checkpoint is gone): the retry finishes the
    // reclaim.
    injector.clear();
    pause.retry_pending_reclaims();
    assert!(
        !pause.has_pending_reclaims(),
        "an exclusively-owned blob file's retained reclaim is finished by the retry",
    );
    let data_start = {
        let mut file = memfs.open(&path, &crate::fs::FsOpenOptions::new().read(true))?;
        let reader = crate::sfa::Reader::from_reader(&mut file)?;
        reader
            .toc()
            .section(b"data")
            .expect("blob file has a data section")
            .pos()
    };
    let punched_len = usize::try_from(first_frame_end - data_start).expect("small fixture");
    let file = memfs.open(&path, &crate::fs::FsOpenOptions::new().read(true))?;
    let prefix = crate::file::read_exact(&*file, data_start, punched_len)?;
    assert!(
        prefix.iter().all(|&b| b == 0),
        "the retried reclaim punches the consumed data prefix",
    );
    Ok(())
}

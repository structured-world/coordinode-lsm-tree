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

/// Two directory entries of the SAME id — `0` beside a noncanonical `00`, what
/// a repair leaves when its post-commit removal of the displaced copy fails —
/// both match the manifest id and both used to be recovered. The version this
/// feeds keys by id, so directory order decided which one won: a copy whose
/// metadata parses but whose value frames rotted could replace the
/// authoritative blob and make reads fail. The manifest's checksum names one of
/// them, and that is the one that must survive.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
#[expect(clippy::indexing_slicing, reason = "test code")]
fn recovery_keeps_the_blob_copy_the_manifest_checksum_names() {
    use crate::fs::{Fs, MemFs};

    let memfs = MemFs::new();
    let fs: Arc<dyn Fs> = Arc::new(memfs.clone());
    let blobs = std::path::absolute("/blobs").expect("absolute");
    memfs.create_dir_all(&blobs).expect("mkdir");

    // The authoritative file, written through the real blob writer so its
    // metadata section and frames are genuine.
    let mut writer = crate::vlog::BlobFileWriter::new(
        crate::SequenceNumberCounter::default(),
        &blobs,
        0,
        None,
        Arc::clone(&fs),
    )
    .expect("writer");
    for i in 0..20u32 {
        writer
            .write(
                format!("k{i:04}").as_bytes(),
                u64::from(i) + 1,
                &[b'v'; 128],
            )
            .expect("write");
    }
    let written = writer.finish().expect("finish");
    assert_eq!(written.len(), 1, "one blob file");

    let authoritative = blobs.join("0");
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(
        &mut memfs
            .open(&authoritative, &crate::fs::FsOpenOptions::new().read(true))
            .expect("open"),
        &mut bytes,
    )
    .expect("read");
    let checksum = Checksum::from_raw(
        crate::file::checksum_from_with_overrides(&*fs, &authoritative, 0, &[]).expect("digest"),
    );

    // The stale twin: same id (`00` parses to 0), one flipped byte in a frame.
    let mut rotted = bytes.clone();
    let Some(byte) = rotted.get_mut(64) else {
        panic!("the written blob reaches the flipped offset");
    };
    *byte ^= 0xFF;
    let mut f = memfs
        .open(
            &blobs.join("00"),
            &crate::fs::FsOpenOptions::new().write(true).create_new(true),
        )
        .expect("create twin");
    std::io::Write::write_all(&mut f, &rotted).expect("write twin");
    drop(f);

    let (recovered, orphans) =
        recover_blob_files(&blobs, &[(0, checksum, 0)], 0, None, &fs).expect("recover");
    assert_eq!(recovered.len(), 1, "one copy per id survives");
    assert_eq!(
        &*recovered[0].0.path, &*authoritative,
        "the copy whose digest matches the manifest is the one kept",
    );
    assert!(
        orphans.iter().any(|p| p.ends_with("00")),
        "the losing copy is reported as an orphan for the caller to sweep: {orphans:?}",
    );
}

/// Ranking a duplicate needs its digest, and a read that fails for reasons
/// outside the file — a refused mount, an exhausted allocator — is not a
/// mismatch. Scoring it as one drops the authoritative copy to the filename
/// tiebreak, and when that copy carries the alternate spelling the stale
/// canonical file wins and the healthy one is deleted as an orphan: permanent
/// loss from a fault a retry would clear.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn a_refused_digest_read_does_not_decide_the_blob_duplicate() {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;

    let memfs = MemFs::new();
    let fault = FaultFs::new(memfs.clone());
    let injector = fault.injector();
    let fs: Arc<dyn Fs> = Arc::new(fault);
    let blobs = std::path::absolute("/blobs").expect("absolute");
    memfs.create_dir_all(&blobs).expect("mkdir");

    let mut writer = crate::vlog::BlobFileWriter::new(
        crate::SequenceNumberCounter::default(),
        &blobs,
        0,
        None,
        Arc::clone(&fs),
    )
    .expect("writer");
    for i in 0..20u32 {
        writer
            .write(
                format!("k{i:04}").as_bytes(),
                u64::from(i) + 1,
                &[b'v'; 128],
            )
            .expect("write");
    }
    assert_eq!(writer.finish().expect("finish").len(), 1, "one blob file");

    // The AUTHORITATIVE copy takes the alternate spelling; the canonical name
    // holds a rotted twin. Only the digest can tell them apart.
    let canonical = blobs.join("0");
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(
        &mut memfs
            .open(&canonical, &crate::fs::FsOpenOptions::new().read(true))
            .expect("open"),
        &mut bytes,
    )
    .expect("read");
    let checksum = Checksum::from_raw(
        crate::file::checksum_from_with_overrides(&*fs, &canonical, 0, &[]).expect("digest"),
    );
    let authoritative = blobs.join("00");
    memfs.rename(&canonical, &authoritative).expect("rename");
    let mut rotted = bytes;
    let Some(byte) = rotted.get_mut(64) else {
        panic!("the written blob reaches the flipped offset");
    };
    *byte ^= 0xFF;
    let mut f = memfs
        .open(
            &canonical,
            &crate::fs::FsOpenOptions::new().write(true).create_new(true),
        )
        .expect("create twin");
    std::io::Write::write_all(&mut f, &rotted).expect("write twin");
    drop(f);

    // Refuse the SECOND open of the authoritative copy. The first is the
    // recovery walk reading its meta section; the second is the digest read
    // that ranks it against the twin — so the fault lands exactly on the
    // comparison, not before it.
    injector.arm(
        FaultRule::new(FaultOp::Open, Fault::Error(ErrorKind::PermissionDenied))
            .on_path(authoritative.display().to_string())
            .skip(1),
    );
    let result = recover_blob_files(&blobs, &[(0, checksum, 0)], 0, None, &fs);
    injector.clear();

    assert!(
        matches!(result, Err(crate::Error::Io(ref e)) if e.kind() == ErrorKind::PermissionDenied),
        "a refused digest read must surface, not hand the id to the stale copy",
    );
    assert!(
        memfs.exists(&authoritative).expect("stat"),
        "the healthy copy must still be on disk for the retry",
    );
}

/// When an id has duplicates and NEITHER reproduces the manifest's checksum,
/// the filename is not a tiebreak, it is a guess between two wrong answers.
/// Acting on it opens the tree on the wrong generation and deletes the other
/// copy as an orphan, though the damaged authoritative file may still be
/// salvageable frame by frame. Recovery must refuse and leave that to a repair.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn recovery_refuses_when_no_blob_duplicate_matches_the_manifest() {
    use crate::fs::{Fs, MemFs};
    use std::io::Write;

    let memfs = MemFs::new();
    let fs: Arc<dyn Fs> = Arc::new(memfs.clone());
    let blobs = std::path::absolute("/blobs_no_match").expect("absolute");
    memfs.create_dir_all(&blobs).expect("mkdir");

    let mut writer = crate::vlog::BlobFileWriter::new(
        crate::SequenceNumberCounter::default(),
        &blobs,
        0,
        None,
        Arc::clone(&fs),
    )
    .expect("writer");
    for i in 0..20u32 {
        writer
            .write(
                format!("k{i:04}").as_bytes(),
                u64::from(i) + 1,
                &[b'v'; 128],
            )
            .expect("write");
    }
    assert_eq!(writer.finish().expect("finish").len(), 1, "one blob file");

    let canonical = blobs.join("0");
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(
        &mut memfs
            .open(&canonical, &crate::fs::FsOpenOptions::new().read(true))
            .expect("open"),
        &mut bytes,
    )
    .expect("read");
    // The manifest's digest is of the UNTOUCHED file; both copies on disk then
    // differ from it (each flipped at a different offset), so neither can prove
    // authority.
    let checksum = Checksum::from_raw(
        crate::file::checksum_from_with_overrides(&*fs, &canonical, 0, &[]).expect("digest"),
    );
    for (path, at) in [(canonical.clone(), 64usize), (blobs.join("00"), 96usize)] {
        let mut damaged = bytes.clone();
        let Some(byte) = damaged.get_mut(at) else {
            panic!("the written blob reaches the flipped offset");
        };
        *byte ^= 0xFF;
        memfs.remove_file(&path).ok();
        let mut f = memfs
            .open(
                &path,
                &crate::fs::FsOpenOptions::new().write(true).create_new(true),
            )
            .expect("create copy");
        f.write_all(&damaged).expect("write copy");
    }

    let result = recover_blob_files(&blobs, &[(0, checksum, 0)], 0, None, &fs);
    assert!(
        matches!(result, Err(crate::Error::Unrecoverable)),
        "with no copy matching the manifest, picking one by filename would open \
         the wrong generation and delete the other",
    );
}

/// A blob file's tight-space punch can be deferred by a checkpoint's hard link
/// exactly as a table's. That intent lives only in the running tree's queue, so
/// after a restart the manifest still recovers the blob with a positive
/// `live_data_start` while its consumed prefix stays allocated. Recovery
/// re-derives the table reclaims; it must re-derive the blob ones too.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn a_reopen_reclaims_a_committed_blob_prefix() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use crate::{AbstractTree, AnyTree, Config, KvSeparationOptions, SequenceNumberCounter};
    use std::io::{Read as _, Seek as _, SeekFrom, Write as _};

    let memfs = Arc::new(MemFs::new());
    let root = std::path::absolute("/db_blob_reclaim")?;
    let config = || {
        Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(memfs.clone())
        .with_kv_separation(Some(
            KvSeparationOptions::default().separation_threshold(64),
        ))
    };
    {
        let AnyTree::Blob(tree) = config().open()? else {
            panic!("expected Blob tree");
        };
        for i in 0..64u64 {
            tree.insert(format!("k{i:05}").as_bytes(), alloc::vec![b'v'; 256], i);
        }
        tree.flush_active_memtable(0)?;
    }

    let blobs = root.join("blobs");
    let blob = memfs
        .read_dir(&blobs)?
        .into_iter()
        .find(|e| !e.is_dir)
        .expect("the flush spilled a blob file")
        .path;
    let data_start = {
        let mut file = memfs.open(&blob, &crate::fs::FsOpenOptions::new().read(true))?;
        let reader = crate::sfa::Reader::from_reader(&mut file)?;
        reader
            .toc()
            .section(b"data")
            .expect("a blob file has a data section")
            .pos()
    };
    let original = {
        let mut bytes = Vec::new();
        memfs
            .open(&blob, &crate::fs::FsOpenOptions::new().read(true))?
            .read_to_end(&mut bytes)?;
        bytes
    };
    // The frontier must land on a FRAME boundary, as a real relocation's does:
    // a punch inside a frame leaves an unparseable record and the file is
    // rejected outright rather than recovered restricted.
    let frontier = super::BlobFileScanner::new(&blob, &*memfs, 0)?
        .next()
        .expect("the blob file holds at least one frame")?
        .frame_end;

    // Punch, then rebuild the manifest from the on-disk geometry so the
    // frontier is COMMITTED, exactly as a completed relocation leaves it.
    memfs.remove_file(&root.join("current"))?;
    memfs.punch_hole(&blob, data_start, frontier - data_start)?;
    config().repair()?;

    // Now restore the prefix bytes: the manifest still says the prefix is
    // consumed, but the blocks are allocated again, which is the shape a
    // deferred punch leaves after a restart.
    {
        let mut file = memfs.open(&blob, &crate::fs::FsOpenOptions::new().write(true))?;
        file.seek(SeekFrom::Start(data_start))?;
        let end = usize::try_from(frontier).expect("test offsets fit usize");
        let begin = usize::try_from(data_start).expect("test offsets fit usize");
        file.write_all(original.get(begin..end).expect("prefix in range"))?;
        file.flush()?;
    }
    let allocated_before = memfs
        .allocated_size(&blob)?
        .expect("MemFs reports allocation");

    let AnyTree::Blob(tree) = config().open()? else {
        panic!("expected Blob tree");
    };
    let allocated_after = memfs
        .allocated_size(&blob)?
        .expect("MemFs reports allocation");
    assert!(
        allocated_after < allocated_before,
        "the reopen must re-derive the blob's punch intent \
         (allocated {allocated_before} before, {allocated_after} after)",
    );
    assert!(
        tree.get(b"k00063", crate::MAX_SEQNO)?.is_some(),
        "the live suffix must still be served",
    );
    Ok(())
}

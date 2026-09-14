use super::*;
use crate::fs::StdFs;
use test_log::test;

fn store() -> (tempfile::TempDir, Arc<dyn Fs>, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let fs: Arc<dyn Fs> = Arc::new(StdFs);
    let folder = dir.path().join(crate::file::DICTS_FOLDER);
    (dir, fs, folder)
}

#[test]
fn a_written_dictionary_reads_back_byte_for_byte() -> crate::Result<()> {
    let (_dir, fs, folder) = store();
    let dict = ZstdDictionary::new(b"representative content for a dictionary");

    write(&*fs, &folder, &dict, None, SyncMode::Normal)?;
    let read = read_one(&*fs, &folder, dict.id(), None)?;

    assert_eq!(read.raw(), dict.raw());
    assert_eq!(read.id(), dict.id());
    Ok(())
}

#[test]
#[cfg(feature = "encryption")]
fn an_encrypted_dictionary_is_sealed_on_disk_and_opens_only_under_its_key() -> crate::Result<()> {
    use crate::encryption::Aes256GcmProvider;

    // A dictionary keeps literal stretches of its training records, so on an
    // encrypted tree the file must not carry them in the clear, and a reader
    // without the key must be refused rather than handed bytes to hash.
    let (_dir, fs, folder) = store();
    let dict = ZstdDictionary::new(b"a training record that must not reach the disk in the clear");
    let key = Aes256GcmProvider::new(&[0x11; 32]);

    write(&*fs, &folder, &dict, Some(&key), SyncMode::Normal)?;

    let on_disk = std::fs::read(folder.join(dict.id().to_string()))?;
    assert!(
        !on_disk.windows(dict.raw().len()).any(|w| w == dict.raw()),
        "the dictionary is not stored in the clear",
    );
    assert_eq!(
        read_one(&*fs, &folder, dict.id(), Some(&key))?.raw(),
        dict.raw(),
        "and opens back to the same bytes under the key",
    );

    let other = Aes256GcmProvider::new(&[0x22; 32]);
    assert!(
        matches!(
            read_one(&*fs, &folder, dict.id(), Some(&other)),
            Err(crate::Error::Decrypt(_)),
        ),
        "a different key does not open it",
    );

    // Rewriting the same id under the key still recognises it as held.
    write(&*fs, &folder, &dict, Some(&key), SyncMode::Normal)?;
    Ok(())
}

#[test]
fn the_folder_is_created_on_the_first_write() -> crate::Result<()> {
    let (_dir, fs, folder) = store();
    assert!(
        !fs.exists(&folder)?,
        "no folder before the first dictionary"
    );

    write(
        &*fs,
        &folder,
        &ZstdDictionary::new(b"content"),
        None,
        SyncMode::Normal,
    )?;

    assert!(fs.exists(&folder)?);
    Ok(())
}

#[test]
fn writing_an_id_already_held_is_a_no_op() -> crate::Result<()> {
    let (_dir, fs, folder) = store();
    let dict = ZstdDictionary::new(b"content");

    write(&*fs, &folder, &dict, None, SyncMode::Normal)?;
    write(&*fs, &folder, &dict, None, SyncMode::Normal)?;

    assert_eq!(read_one(&*fs, &folder, dict.id(), None)?.raw(), dict.raw());
    Ok(())
}

#[test]
fn rewriting_a_held_dictionary_still_syncs_its_directory() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};

    // A registration retried after a crash between the RENAME and the directory
    // sync finds the final file already there. Returning without syncing would
    // leave the directory entry non-durable while the caller goes on to durably
    // register the id, so a power loss could keep the version edit and lose the
    // dictionary.
    //
    // Observed through an injected fault rather than a call counter: with the
    // directory sync armed to fail, a path that syncs REPORTS the failure and a
    // path that skips it silently succeeds.
    let dir = tempfile::tempdir()?;
    let folder = dir.path().join(crate::file::DICTS_FOLDER);
    let dict = ZstdDictionary::new(b"content whose registration is retried");

    let fs = FaultFs::new(StdFs);
    write(&fs, &folder, &dict, None, SyncMode::Normal)?;

    fs.injector().arm(
        FaultRule::new(
            FaultOp::SyncDirectory,
            Fault::Error(crate::io::ErrorKind::Other),
        )
        .on_path(crate::file::DICTS_FOLDER),
    );

    assert!(
        write(&fs, &folder, &dict, None, SyncMode::Normal).is_err(),
        "the retry must sync the directory, so the armed failure surfaces",
    );
    Ok(())
}

/// Arms a directory-sync failure on the tree root, letting the first matching
/// sync through. The root path is a substring of `dicts/` too, so the skip is
/// what separates them: a write syncs `dicts/` first and its parent second.
fn fail_the_parent_sync(fs: &crate::fs::FaultFs<StdFs>, root: &std::path::Path) {
    use crate::fs::{Fault, FaultOp, FaultRule};
    fs.injector().arm(
        FaultRule::new(
            FaultOp::SyncDirectory,
            Fault::Error(crate::io::ErrorKind::Other),
        )
        .on_path(root.to_string_lossy())
        .skip(1),
    );
}

#[test]
fn a_retry_after_a_failed_parent_sync_still_syncs_the_parent() -> crate::Result<()> {
    use crate::fs::FaultFs;

    // The first attempt creates `dicts/` and fails syncing its entry in the
    // tree root. The retry finds the folder already there, and if it takes
    // that as done it registers the dictionary on top of a folder a power loss
    // can still take away, while the manifest naming it survives.
    let dir = tempfile::tempdir()?;
    let folder = dir.path().join(crate::file::DICTS_FOLDER);
    let dict = ZstdDictionary::new(b"content whose folder entry is not yet durable");

    let fs = FaultFs::new(StdFs);
    fail_the_parent_sync(&fs, dir.path());
    assert!(
        write(&fs, &folder, &dict, None, SyncMode::Normal).is_err(),
        "the first attempt fails on the parent sync",
    );
    assert!(fs.exists(&folder)?, "and leaves the folder behind");

    // Re-armed from scratch, so the retry's first sync (`dicts/`) passes and
    // only a sync of the parent can trip it.
    fs.injector().clear();
    fail_the_parent_sync(&fs, dir.path());
    assert!(
        write(&fs, &folder, &dict, None, SyncMode::Normal).is_err(),
        "the retry syncs the parent again, so the armed failure surfaces",
    );
    Ok(())
}

#[test]
fn rewriting_a_held_dictionary_still_syncs_the_parent() -> crate::Result<()> {
    use crate::fs::FaultFs;

    // The file already exists: a previous attempt got as far as the rename.
    // Its folder may be as fresh as the file, so the entry of `dicts/` in the
    // tree root is as much in doubt as the file's entry in `dicts/`.
    let dir = tempfile::tempdir()?;
    let folder = dir.path().join(crate::file::DICTS_FOLDER);
    let dict = ZstdDictionary::new(b"content registered twice");

    let fs = FaultFs::new(StdFs);
    write(&fs, &folder, &dict, None, SyncMode::Normal)?;

    fail_the_parent_sync(&fs, dir.path());
    assert!(
        write(&fs, &folder, &dict, None, SyncMode::Normal).is_err(),
        "the rewrite syncs the parent, so the armed failure surfaces",
    );
    Ok(())
}

#[test]
fn writing_a_different_dictionary_under_a_held_id_is_refused() -> crate::Result<()> {
    let (_dir, fs, folder) = store();
    let held = ZstdDictionary::new(b"the dictionary this id belongs to");
    write(&*fs, &folder, &held, None, SyncMode::Normal)?;

    // A DIFFERENT dictionary claiming the same id: what a collision of the
    // truncated 32-bit hash looks like from here. Treating the write as already
    // done would leave the old bytes on disk while the caller writes new blocks
    // against the new ones under the same id, and every such block would
    // decompress into plausible garbage.
    let colliding = ZstdDictionary::new(b"entirely different content");
    let colliding = colliding.with_id_for_test(held.id());

    let err = write(&*fs, &folder, &colliding, None, SyncMode::Normal).unwrap_err();
    match err {
        crate::Error::ZstdDictMismatch { expected, .. } => assert_eq!(expected, held.id()),
        other => panic!("expected ZstdDictMismatch, got {other:?}"),
    }

    // The dictionary already there is untouched.
    assert_eq!(read_one(&*fs, &folder, held.id(), None)?.raw(), held.raw());
    Ok(())
}

#[test]
fn reading_a_dictionary_the_tree_does_not_hold_reports_not_found() {
    let (_dir, fs, folder) = store();

    let err = read_one(&*fs, &folder, 12345, None).unwrap_err();

    // The caller distinguishes "never registered" from "corrupt", so this must
    // not surface as a mismatch.
    match err {
        crate::Error::Io(e) => assert_eq!(e.kind(), crate::io::ErrorKind::NotFound),
        other => panic!("expected NotFound, got {other:?}"),
    }
}

#[test]
fn a_flipped_bit_is_caught_because_the_name_is_the_digest() -> crate::Result<()> {
    let (_dir, fs, folder) = store();
    let dict = ZstdDictionary::new(b"content that will be corrupted on disk");
    write(&*fs, &folder, &dict, None, SyncMode::Normal)?;

    // Corrupt one byte under the live name. A silently altered dictionary is
    // the worst failure this store can have: every block written against it
    // would decompress to plausible garbage instead of failing, so the read
    // must refuse rather than hand the bytes back.
    let path = folder.join(dict.id().to_string());
    let mut raw = std::fs::read(&path).unwrap();
    *raw.first_mut().unwrap() ^= 0x01;
    std::fs::write(&path, &raw).unwrap();

    let err = read_one(&*fs, &folder, dict.id(), None).unwrap_err();
    match err {
        crate::Error::ZstdDictMismatch { expected, got } => {
            assert_eq!(expected, dict.id());
            assert_ne!(got, Some(dict.id()), "the corrupted bytes hash elsewhere");
        }
        other => panic!("expected ZstdDictMismatch, got {other:?}"),
    }
    Ok(())
}

#[test]
fn a_truncated_dictionary_is_caught_the_same_way() -> crate::Result<()> {
    let (_dir, fs, folder) = store();
    let dict = ZstdDictionary::new(b"content long enough to truncate");
    write(&*fs, &folder, &dict, None, SyncMode::Normal)?;

    let path = folder.join(dict.id().to_string());
    let raw = std::fs::read(&path).unwrap();
    let half = raw.get(..raw.len() / 2).unwrap();
    std::fs::write(&path, half).unwrap();

    assert!(matches!(
        read_one(&*fs, &folder, dict.id(), None),
        Err(crate::Error::ZstdDictMismatch { .. }),
    ));
    Ok(())
}

#[test]
fn the_scan_loads_every_dictionary_the_folder_holds() -> crate::Result<()> {
    let (_dir, fs, folder) = store();
    let a = ZstdDictionary::new(b"aaaaaaaaaaaaaaaaaaaa");
    let b = ZstdDictionary::new(b"bbbbbbbbbbbbbbbbbbbb");
    write(&*fs, &folder, &a, None, SyncMode::Normal)?;
    write(&*fs, &folder, &b, None, SyncMode::Normal)?;

    let set = read_all(&*fs, &folder, None)?;

    assert_eq!(set.len(), 2);
    assert_eq!(
        set.get(a.id()).map(|d| d.raw().to_vec()),
        Some(a.raw().to_vec())
    );
    assert_eq!(
        set.get(b.id()).map(|d| d.raw().to_vec()),
        Some(b.raw().to_vec())
    );
    Ok(())
}

#[test]
fn the_scan_of_an_empty_folder_is_an_empty_set() -> crate::Result<()> {
    let (_dir, fs, folder) = store();
    assert!(read_all(&*fs, &folder, None)?.is_empty());

    fs.create_dir_all(&folder)?;
    assert!(read_all(&*fs, &folder, None)?.is_empty());
    Ok(())
}

#[test]
fn the_scan_fails_on_a_corrupt_dictionary_rather_than_skipping_it() -> crate::Result<()> {
    let (_dir, fs, folder) = store();
    let good = ZstdDictionary::new(b"aaaaaaaaaaaaaaaaaaaa");
    let bad = ZstdDictionary::new(b"bbbbbbbbbbbbbbbbbbbb");
    write(&*fs, &folder, &good, None, SyncMode::Normal)?;
    write(&*fs, &folder, &bad, None, SyncMode::Normal)?;

    let path = folder.join(bad.id().to_string());
    let mut raw = std::fs::read(&path).unwrap();
    *raw.first_mut().unwrap() ^= 0x01;
    std::fs::write(&path, &raw).unwrap();

    // Skipping it would turn a detectable corruption into "unknown dictionary
    // id" on the first table that needs it, far from the cause.
    assert!(read_all(&*fs, &folder, None).is_err());
    Ok(())
}

/// Flips the first byte of the dictionary filed under `id`.
fn damage(folder: &std::path::Path, id: DictId) {
    let path = folder.join(id.to_string());
    let mut raw = std::fs::read(&path).unwrap();
    *raw.first_mut().unwrap() ^= 0x01;
    std::fs::write(&path, &raw).unwrap();
}

#[test]
fn the_repair_scan_skips_a_damaged_dictionary_and_keeps_the_rest() -> crate::Result<()> {
    // The strict scan fails on the damaged file; the repair's reports it and
    // loads everything else, touching nothing on disk.
    let (_dir, fs, folder) = store();
    let good = ZstdDictionary::new(b"aaaaaaaaaaaaaaaaaaaa");
    let bad = ZstdDictionary::new(b"bbbbbbbbbbbbbbbbbbbb");
    write(&*fs, &folder, &good, None, SyncMode::Normal)?;
    write(&*fs, &folder, &bad, None, SyncMode::Normal)?;
    damage(&folder, bad.id());

    let (set, damaged) = read_all_skipping_damaged(&*fs, &folder, None)?;

    assert!(set.get(good.id()).is_some(), "the intact one loads");
    assert!(set.get(bad.id()).is_none(), "the damaged one does not");
    assert_eq!(damaged, vec![bad.id()]);
    assert!(
        fs.exists(&folder.join(bad.id().to_string()))?,
        "nothing is moved by the scan itself",
    );
    Ok(())
}

#[test]
fn setting_aside_moves_only_a_file_that_is_still_damaged() -> crate::Result<()> {
    let (_dir, fs, folder) = store();
    let dict = ZstdDictionary::new(b"content that will be damaged");
    write(&*fs, &folder, &dict, None, SyncMode::Normal)?;
    damage(&folder, dict.id());

    let aside = set_aside_if_damaged(&*fs, &folder, dict.id(), None, SyncMode::Normal)?;
    let expected = folder.join(format!("{}{}", dict.id(), crate::file::DICT_DAMAGED_SUFFIX));
    assert_eq!(aside.as_deref(), Some(expected.as_path()));
    assert!(!fs.exists(&folder.join(dict.id().to_string()))?);
    assert!(
        matches!(
            DictDirEntry::classify(&format!(
                "{}{}",
                dict.id(),
                crate::file::DICT_DAMAGED_SUFFIX
            )),
            DictDirEntry::Foreign,
        ),
        "the new name is one no open reads and no sweep removes",
    );

    // Nothing under the name any more, and an intact one is left alone.
    assert!(set_aside_if_damaged(&*fs, &folder, dict.id(), None, SyncMode::Normal)?.is_none());
    write(&*fs, &folder, &dict, None, SyncMode::Normal)?;
    assert!(set_aside_if_damaged(&*fs, &folder, dict.id(), None, SyncMode::Normal)?.is_none());
    Ok(())
}

#[test]
fn writing_over_a_damaged_copy_of_the_same_id_heals_it() -> crate::Result<()> {
    // A damaged file no longer hashes to its name, so it is not a different
    // dictionary claiming the id: the correct bytes replace it.
    let (_dir, fs, folder) = store();
    let dict = ZstdDictionary::new(b"content written twice, damaged in between");
    write(&*fs, &folder, &dict, None, SyncMode::Normal)?;
    damage(&folder, dict.id());

    write(&*fs, &folder, &dict, None, SyncMode::Normal)?;
    assert_eq!(read_one(&*fs, &folder, dict.id(), None)?.raw(), dict.raw());
    Ok(())
}

#[test]
fn the_scan_ignores_files_the_engine_does_not_own() -> crate::Result<()> {
    let (_dir, fs, folder) = store();
    let dict = ZstdDictionary::new(b"content");
    write(&*fs, &folder, &dict, None, SyncMode::Normal)?;
    std::fs::write(folder.join("notes.txt"), b"mine").unwrap();
    std::fs::write(folder.join("7.tmp"), b"unpublished").unwrap();

    let set = read_all(&*fs, &folder, None)?;

    assert_eq!(set.len(), 1, "only the published dictionary is loaded");
    assert!(set.get(dict.id()).is_some());
    Ok(())
}

#[test]
fn removing_a_dictionary_leaves_the_others() -> crate::Result<()> {
    let (_dir, fs, folder) = store();
    let a = ZstdDictionary::new(b"aaaaaaaaaaaaaaaaaaaa");
    let b = ZstdDictionary::new(b"bbbbbbbbbbbbbbbbbbbb");
    write(&*fs, &folder, &a, None, SyncMode::Normal)?;
    write(&*fs, &folder, &b, None, SyncMode::Normal)?;

    remove(&*fs, &folder, a.id(), SyncMode::Normal)?;

    assert!(read_one(&*fs, &folder, a.id(), None).is_err());
    assert_eq!(read_one(&*fs, &folder, b.id(), None)?.raw(), b.raw());
    Ok(())
}

#[test]
fn removing_an_absent_dictionary_succeeds() -> crate::Result<()> {
    let (_dir, fs, folder) = store();
    fs.create_dir_all(&folder)?;

    remove(&*fs, &folder, 4242, SyncMode::Normal)?;
    Ok(())
}

#[test]
fn the_sweep_takes_temps_and_leaves_everything_else() -> crate::Result<()> {
    let (_dir, fs, folder) = store();
    let dict = ZstdDictionary::new(b"content");
    write(&*fs, &folder, &dict, None, SyncMode::Normal)?;

    // A crashed registration leaves this behind.
    let temp = folder.join(format!("{}{DICT_TMP_SUFFIX}", 777));
    std::fs::write(&temp, b"half-written").unwrap();
    // An operator's file, which the engine does not own and must not touch.
    let foreign = folder.join("notes.txt");
    std::fs::write(&foreign, b"mine").unwrap();

    sweep_temps(&*fs, &folder)?;

    assert!(!fs.exists(&temp)?, "the unpublished temp is disposable");
    assert!(fs.exists(&foreign)?, "a foreign name is never swept");
    assert_eq!(
        read_one(&*fs, &folder, dict.id(), None)?.raw(),
        dict.raw(),
        "a published dictionary survives the sweep",
    );
    Ok(())
}

#[test]
fn sweeping_a_folder_that_does_not_exist_succeeds() -> crate::Result<()> {
    let (_dir, fs, folder) = store();
    sweep_temps(&*fs, &folder)?;
    Ok(())
}

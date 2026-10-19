// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::*;
use crate::SequenceNumberCounter;
use crate::file::repair_report_path as path;
use crate::fs::{Fs, MemFs};
use std::sync::Arc;

fn a_report() -> RepairReport {
    RepairReport {
        recovered: 3,
        salvaged: 1,
        unreadable: 1,
        unreadable_files: vec![(PathBuf::from("/db/tables/7"), "bad footer".into())],
        excluded_files: vec![(PathBuf::from("/db/tables/4"), "covered by 9".into())],
        lost_coverage: vec![
            (
                PathBuf::from("/db/tables/7"),
                UserKey::from(&b"a"[..]),
                UserKey::from(&b"m"[..]),
                Some(42),
            ),
            (
                PathBuf::from("/db/tables/8"),
                UserKey::from(&b"n"[..]),
                UserKey::from(&b"z"[..]),
                None,
            ),
        ],
        unknowable_losses: vec![PathBuf::from("/db/tables/11")],
        blob_files_salvaged: vec![(PathBuf::from("/db/blobs/2"), "salvaged".into())],
        damaged_dictionaries: vec![(PathBuf::from("/db/dicts/5.damaged"), "moved".into())],
        method: crate::repair::REPAIR_METHOD,
        warnings: vec![
            crate::repair::WARN_ALL_AT_L0,
            crate::repair::WARN_BLOB_FRAGMENTATION,
        ],
    }
}

fn config(fs: &Arc<MemFs>) -> Config {
    Config::new(
        "/db",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(fs.clone())
}

/// A copy reads back as the report it was written from, field for field.
#[test]
fn a_written_copy_reads_back_as_its_report() -> crate::Result<()> {
    let fs = Arc::new(MemFs::new());
    let config = config(&fs);
    fs.create_dir_all(&config.path)?;
    let report = a_report();
    write(&config, 9, &report)?;
    let back = read_all(&config, Some(9))?.ok_or(crate::Error::Unrecoverable)?;
    assert_eq!(format!("{back:?}"), format!("{report:?}"));
    assert!(
        read_all(&config, Some(8))?.is_none(),
        "the copy is for snapshot 9 only"
    );
    Ok(())
}

/// A copy whose bytes no longer check out still says a repair lost
/// something: the loss is unscoped, so the whole history is replayed.
#[test]
fn a_damaged_copy_leaves_the_loss_unscoped() -> crate::Result<()> {
    use std::io::{Seek, SeekFrom, Write};

    let fs = Arc::new(MemFs::new());
    let config = config(&fs);
    fs.create_dir_all(&config.path)?;
    write(&config, 9, &a_report())?;
    {
        let mut file = fs.open(
            &path(&config.path, 9),
            &FsOpenOptions::new().read(true).write(true),
        )?;
        file.seek(SeekFrom::Start(12))?;
        file.write_all(&[0xFF])?;
    }
    let back = read_all(&config, None)?.ok_or(crate::Error::Unrecoverable)?;
    assert_eq!(back.wal_replay_scope(), crate::WalReplayScope::FullHistory);
    assert_eq!(back.unknowable_losses, vec![path(&config.path, 9)]);
    Ok(())
}

/// `body` closed by its checksum, as a copy is: a decode that refuses it then
/// refuses its shape, not its checksum.
fn sealed(mut body: Vec<u8>) -> Vec<u8> {
    let checksum = crate::hash::hash64(&body);
    put_u64(&mut body, checksum);
    body
}

/// The encoded copy of `report` without its checksum.
fn body_of(report: &RepairReport) -> Vec<u8> {
    let mut encoded = encode(report);
    encoded.truncate(encoded.len() - CHECKSUM_LEN);
    encoded
}

/// A copy whose checksum holds yet whose shape is not the one this engine
/// writes (another magic, a bound tag outside 0/1, a warning it does not
/// know, bytes after the last field) is refused rather than misread: each
/// would otherwise hand out a report that names other losses than the
/// repair's.
#[test]
fn a_checksummed_copy_of_another_shape_is_refused() {
    let report = a_report();
    assert!(
        decode(&sealed(body_of(&report))).is_some(),
        "self-check: the untouched body decodes"
    );

    let mut other_magic = b"LSMRPRT2".to_vec();
    other_magic.extend(body_of(&report).iter().skip(MAGIC.len()));
    assert!(decode(&sealed(other_magic)).is_none(), "another magic");

    let mut trailing = body_of(&report);
    trailing.push(0);
    assert!(
        decode(&sealed(trailing)).is_none(),
        "bytes after the last field"
    );

    // The warnings are the last field: one index per warning, so the last
    // byte of the body is the last warning's index.
    let mut unknown_warning = body_of(&report);
    if let Some(last) = unknown_warning.last_mut() {
        *last = u8::MAX;
    }
    assert!(
        decode(&sealed(unknown_warning)).is_none(),
        "an unknown warning"
    );

    // One loss and nothing else before it: its bound tag sits right after
    // the three counted fields and the loss's path and keys.
    let one_loss = RepairReport {
        recovered: 0,
        salvaged: 0,
        unreadable: 0,
        unreadable_files: Vec::new(),
        excluded_files: Vec::new(),
        lost_coverage: vec![(
            PathBuf::from("/p"),
            UserKey::from(&b"a"[..]),
            UserKey::from(&b"b"[..]),
            None,
        )],
        unknowable_losses: Vec::new(),
        blob_files_salvaged: Vec::new(),
        damaged_dictionaries: Vec::new(),
        method: crate::repair::REPAIR_METHOD,
        warnings: Vec::new(),
    };
    let mut bad_tag = body_of(&one_loss);
    let tag_at = MAGIC.len() + 8 + 8 + 8 + 8 + 8 + (8 + 2) + (8 + 1) + (8 + 1);
    let Some(tag) = bad_tag.get_mut(tag_at) else {
        panic!("self-check: the body holds the bound tag");
    };
    assert_eq!(*tag, 0, "self-check: the bound tag");
    *tag = 2;
    assert!(
        decode(&sealed(bad_tag)).is_none(),
        "a bound tag outside 0/1"
    );
}

/// Copies of several snapshots read back as one report holding the losses of
/// each, so an open that finds more than one hands out every owed replay.
#[test]
fn several_copies_read_back_folded_into_one() -> crate::Result<()> {
    let fs = Arc::new(MemFs::new());
    let config = config(&fs);
    fs.create_dir_all(&config.path)?;
    let mut later = a_report();
    later.lost_coverage = vec![(
        PathBuf::from("/db/tables/12"),
        UserKey::from(&b"p"[..]),
        UserKey::from(&b"q"[..]),
        Some(77),
    )];
    write(&config, 9, &a_report())?;
    write(&config, 10, &later)?;

    let folded = read_all(&config, None)?.ok_or(crate::Error::Unrecoverable)?;
    for loss in a_report().lost_coverage.iter().chain(&later.lost_coverage) {
        assert!(
            folded.lost_coverage.contains(loss),
            "{loss:?} is missing from {:?}",
            folded.lost_coverage,
        );
    }
    assert_eq!(folded.lost_coverage.len(), 3);
    Ok(())
}

/// A copy sealed under a key the reader does not hold cannot say what was
/// lost, so it leaves the loss unscoped like a damaged copy does.
#[test]
#[cfg(feature = "encryption")]
fn a_copy_that_does_not_decrypt_leaves_the_loss_unscoped() -> crate::Result<()> {
    use crate::Aes256GcmProvider;

    let fs = Arc::new(MemFs::new());
    let sealed_under =
        |key: u8| config(&fs).with_encryption(Some(Arc::new(Aes256GcmProvider::new(&[key; 32]))));
    fs.create_dir_all(&sealed_under(1).path)?;
    write(&sealed_under(1), 9, &a_report())?;

    let own = read_all(&sealed_under(1), None)?.ok_or(crate::Error::Unrecoverable)?;
    assert_eq!(
        format!("{own:?}"),
        format!("{:?}", a_report()),
        "under its own key the copy reads back as its report"
    );

    let back = read_all(&sealed_under(2), None)?.ok_or(crate::Error::Unrecoverable)?;
    assert_eq!(back.wal_replay_scope(), crate::WalReplayScope::FullHistory);
    assert_eq!(back.unknowable_losses, vec![path(&sealed_under(2).path, 9)]);
    assert_eq!(back.warnings, vec![crate::repair::WARN_REPORT_COPY_DAMAGED]);
    Ok(())
}

/// Retiring removes every copy and its temporary, and nothing else.
#[test]
fn retiring_removes_every_copy_and_only_those() -> crate::Result<()> {
    let fs = Arc::new(MemFs::new());
    let config = config(&fs);
    fs.create_dir_all(&config.path)?;
    write(&config, 9, &a_report())?;
    for name in ["repair-report-3.tmp", "repair-report-notes", "v9"] {
        fs.open(
            &config.path.join(name),
            &FsOpenOptions::new().write(true).create(true),
        )?;
    }
    assert!(retire(&config).is_ok());
    let mut left: Vec<String> = fs
        .read_dir(&config.path)?
        .into_iter()
        .map(|e| e.file_name)
        .collect();
    left.sort();
    assert_eq!(left, vec!["repair-report-notes", "v9"]);
    Ok(())
}

/// A removal that fails partway leaves the copy of the newest snapshot,
/// which a repair over an earlier one wrote with that one's losses folded in:
/// the copy that carries every loss is the last to go, whatever order the
/// folder lists its files in.
#[test]
fn a_failed_retirement_keeps_the_newest_copy() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};

    // The listing order of the in-memory folder changes from one instance to
    // the next; enough instances list the newest copy first.
    for _ in 0..64 {
        let fs = Arc::new(FaultFs::new(MemFs::new()));
        let config = Config::new(
            "/db",
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(fs.clone());
        fs.create_dir_all(&config.path)?;
        let mut newest = a_report();
        newest.lost_coverage.push((
            PathBuf::from("/db/tables/12"),
            UserKey::from(&b"p"[..]),
            UserKey::from(&b"q"[..]),
            Some(77),
        ));
        write(&config, 9, &a_report())?;
        write(&config, 10, &newest)?;
        fs.injector().arm(
            FaultRule::new(
                FaultOp::RemoveFile,
                Fault::Error(crate::io::ErrorKind::PermissionDenied),
            )
            .on_path("repair-report-9"),
        );

        assert!(matches!(retire(&config), Err(Retire::Kept(_))));
        let kept = read_all(&config, Some(10))?.ok_or(crate::Error::Unrecoverable)?;
        assert_eq!(kept.lost_coverage, newest.lost_coverage);
    }
    Ok(())
}

/// The name grammar owns only the exact shapes the repair writes.
#[test]
fn only_the_exact_names_are_report_copies() {
    use crate::file::repair_report_name;
    assert_eq!(repair_report_name("repair-report-9"), Some((9, false)));
    assert_eq!(repair_report_name("repair-report-9.tmp"), Some((9, true)));
    assert_eq!(repair_report_name("repair-report-"), None);
    assert_eq!(repair_report_name("repair-report-9.bak"), None);
    assert_eq!(repair_report_name("repair-report-+9"), None);
    assert_eq!(repair_report_name("v9"), None);
    // Only the spelling the repair writes: a leading zero names a file the
    // engine never made, and reading it back by its id would open another.
    assert_eq!(repair_report_name("repair-report-09"), None);
    assert_eq!(repair_report_name("repair-report-00.tmp"), None);
    assert_eq!(repair_report_name("repair-report-0"), Some((0, false)));
}

/// A file spelled like a copy but not as the repair writes one is not a copy:
/// the owed report is looked up by its canonical name, so taking the stray
/// spelling for one would look for a file that is never there, forever.
#[test]
fn a_stray_spelling_is_no_owed_report() -> crate::Result<()> {
    let fs = Arc::new(MemFs::new());
    let config = config(&fs);
    fs.create_dir_all(&config.path)?;
    fs.open(
        &config.path.join("repair-report-01"),
        &FsOpenOptions::new().write(true).create(true),
    )?;
    assert!(crate::file::owed_repair_report(&*fs, &config.path)?.is_none());
    Ok(())
}

/// A later repair of a salvaged replacement names the same path with a
/// narrower range: folding keeps the earlier, wider loss beside it, so the
/// replay still reaches every key and the higher bound, and keeps each note
/// of a blob file salvaged twice.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn folding_keeps_every_distinct_loss_under_one_path() {
    let narrower = (
        PathBuf::from("/db/tables/7"),
        UserKey::from(&b"c"[..]),
        UserKey::from(&b"d"[..]),
        Some(10),
    );
    let mut later = RepairReport {
        recovered: 1,
        salvaged: 1,
        unreadable: 0,
        unreadable_files: Vec::new(),
        excluded_files: Vec::new(),
        lost_coverage: vec![narrower.clone()],
        unknowable_losses: Vec::new(),
        blob_files_salvaged: vec![(PathBuf::from("/db/blobs/2"), "again".into())],
        damaged_dictionaries: Vec::new(),
        method: crate::repair::REPAIR_METHOD,
        warnings: Vec::new(),
    };
    let earlier = a_report();
    let wider = earlier
        .lost_coverage
        .first()
        .cloned()
        .expect("the earlier report names /db/tables/7");
    crate::repair::fold_report(&mut later, earlier);
    assert!(later.lost_coverage.contains(&narrower));
    assert!(
        later.lost_coverage.contains(&wider),
        "the earlier, wider loss of the same path survives"
    );
    assert_eq!(
        later.blob_files_salvaged,
        vec![
            (PathBuf::from("/db/blobs/2"), "again".into()),
            (PathBuf::from("/db/blobs/2"), "salvaged".into()),
        ],
    );

    // The same entry twice is listed once.
    let mut twice = a_report();
    crate::repair::fold_report(&mut twice, a_report());
    assert_eq!(twice.lost_coverage, a_report().lost_coverage);
    assert_eq!(twice.unreadable, 1);
}

/// Folding an earlier report adds its losses once, keeps the counts of the
/// later run, and keeps the later run's entries first.
#[test]
fn folding_adds_the_earlier_losses_once() {
    let mut later = RepairReport {
        recovered: 5,
        salvaged: 0,
        unreadable: 0,
        unreadable_files: Vec::new(),
        excluded_files: Vec::new(),
        lost_coverage: Vec::new(),
        unknowable_losses: vec![PathBuf::from("/db/tables/11")],
        blob_files_salvaged: vec![(PathBuf::from("/db/blobs/3"), "later".into())],
        damaged_dictionaries: Vec::new(),
        method: crate::repair::REPAIR_METHOD,
        warnings: vec![crate::repair::WARN_ALL_AT_L0],
    };
    crate::repair::fold_report(&mut later, a_report());
    assert_eq!(later.recovered, 5);
    assert_eq!(later.salvaged, 0);
    assert_eq!(later.unreadable, 1);
    assert_eq!(later.lost_coverage.len(), 2);
    assert_eq!(
        later.unknowable_losses,
        vec![PathBuf::from("/db/tables/11")],
        "a path named twice is listed once"
    );
    assert_eq!(
        later
            .blob_files_salvaged
            .iter()
            .map(|(p, _)| p.clone())
            .collect::<Vec<_>>(),
        vec![PathBuf::from("/db/blobs/3"), PathBuf::from("/db/blobs/2")],
    );
    assert_eq!(
        later.warnings,
        vec![
            crate::repair::WARN_ALL_AT_L0,
            crate::repair::WARN_BLOB_FRAGMENTATION
        ],
    );
}

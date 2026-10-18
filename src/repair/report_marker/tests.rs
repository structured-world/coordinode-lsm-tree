// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::*;
use crate::SequenceNumberCounter;
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

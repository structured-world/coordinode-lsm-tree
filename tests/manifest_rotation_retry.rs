// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A manifest rotation that fails after writing its snapshot and its log but
//! before the directory sync that installs them must not block the installs
//! that follow: the retry derives the same version id, and what the failed
//! attempt left is garbage, not a reason to refuse every later rotation until
//! the process restarts.

#![cfg(feature = "std")]

use lsm_tree::fs::{Fault, FaultFs, FaultOp, FaultRule, StdFs};
use lsm_tree::io::ErrorKind;
use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};
use test_log::test;

/// The folder syncs a compaction's install issues for its output's new entry
/// before the tree folder's: none where the output is written into a pooled
/// file, one where a folder sync does nothing and so there is no pool
/// (Windows).
const UNPOOLED_FOLDER_SYNCS: u64 = if cfg!(windows) { 1 } else { 0 };

#[test]
fn a_rotation_failed_at_its_install_does_not_block_the_next_install() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = FaultFs::new(StdFs);
    let injector = fs.injector();
    let open = |fs| {
        Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        // Every install rotates, so each flush writes and installs a fresh
        // snapshot.
        .manifest_log_rotate_bytes(0)
        // The armed sync counts the folder syncs in order: none may come from
        // a refill on another thread.
        .table_file_refill_inline(true)
        .with_fs(fs)
        .open()
    };

    {
        let tree = open(fs)?;
        tree.insert("k0", "v0", 0);
        tree.flush_active_memtable(0)?;

        // The next flush writes its table into a file pooled with the first
        // one's, so its first directory sync is the one that would install
        // its snapshot and log: it fails. Where there is no pool (Windows) its
        // file is made for it, and its install syncs the tables folder first.
        injector.arm(
            FaultRule::new(FaultOp::SyncDirectory, Fault::Error(ErrorKind::Other))
                .skip(UNPOOLED_FOLDER_SYNCS)
                .once(),
        );
        tree.insert("k1", "v1", 1);
        assert!(
            tree.flush_active_memtable(1).is_err(),
            "the armed directory sync must fail this install"
        );

        // The retry derives the same version id. It must install, not refuse
        // on what the failed attempt left.
        tree.insert("k2", "v2", 2);
        tree.flush_active_memtable(2)?;

        for k in ["k0", "k1", "k2"] {
            assert!(tree.get(k, 3)?.is_some(), "{k} must read after the retry");
        }
    }

    let tree = open(FaultFs::new(StdFs))?;
    for k in ["k0", "k1", "k2"] {
        assert!(tree.get(k, 3)?.is_some(), "{k} must read after reopening");
    }
    Ok(())
}

/// An install whose directory sync fails is withdrawn: the operation that
/// asked for it reports the failure and removes what it wrote for it (a
/// compaction its outputs), so the snapshot it named must not stay in place
/// for a reopen to take over the files that are gone.
#[test]
fn a_compaction_whose_install_fails_reopens_on_the_previous_generation() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = FaultFs::new(StdFs);
    let injector = fs.injector();
    let open = |fs| {
        Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .manifest_log_rotate_bytes(0)
        .table_file_refill_inline(true)
        .with_fs(fs)
        .open()
    };

    {
        let tree = open(fs)?;
        for (seqno, k) in (0u64..).zip(["k0", "k1"]) {
            tree.insert(k, "v", seqno);
            tree.flush_active_memtable(0)?;
        }
        // The compaction writes its output into a pooled file, so its first
        // directory sync is that of the install that names the output: it
        // fails. Where there is no pool (Windows) the output's file is made
        // for it, and the install syncs the tables folder first.
        injector.arm(
            FaultRule::new(FaultOp::SyncDirectory, Fault::Error(ErrorKind::Other))
                .skip(UNPOOLED_FOLDER_SYNCS)
                .once(),
        );
        assert!(
            tree.major_compact(64 * 1024 * 1024, 0).is_err(),
            "the armed directory sync must fail the compaction's install"
        );
    }

    let tree = open(FaultFs::new(StdFs))?;
    for k in ["k0", "k1"] {
        assert!(tree.get(k, 3)?.is_some(), "{k} must read after reopening");
    }
    Ok(())
}

/// The tables in the tree's tables folder.
fn table_files(dir: &std::path::Path) -> std::io::Result<usize> {
    Ok(table_names(dir)?.len())
}

/// The names of the tables in the tree's tables folder. The empty files the
/// folder keeps for tables to come are no tables.
fn table_names(dir: &std::path::Path) -> std::io::Result<std::collections::BTreeSet<String>> {
    let mut names = std::collections::BTreeSet::new();
    for entry in std::fs::read_dir(dir.join("tables"))? {
        let entry = entry?;
        // A retired table the tree unlinks in the background between the
        // listing and the stat is gone, and not counted.
        match entry.metadata() {
            Ok(meta) if meta.len() > 0 => {
                names.insert(entry.file_name().to_string_lossy().into_owned());
            }
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    Ok(names)
}

/// The output a compaction of unknown install outcome keeps goes once a later
/// install writes the full snapshot that supersedes the failed edit: no
/// version names it from then on, and it is not left on disk, holding its
/// room, until the tree is reopened.
#[test]
fn a_kept_output_goes_once_a_later_install_supersedes_its_edit() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = FaultFs::new(StdFs);
    let injector = fs.injector();
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fs)
    .open()?;
    for (seqno, k) in (0u64..).zip(["k0", "k1"]) {
        tree.insert(k, "v", seqno);
        tree.flush_active_memtable(0)?;
    }
    let inputs = table_names(dir.path())?;
    injector.arm(
        FaultRule::new(FaultOp::SyncData, Fault::Error(ErrorKind::Other))
            .on_path("edits-")
            .once(),
    );
    assert!(matches!(
        tree.major_compact(64 * 1024 * 1024, 0),
        Err(lsm_tree::Error::InstallOutcomeUnknown(_))
    ));
    let kept: Vec<String> = table_names(dir.path())?
        .difference(&inputs)
        .cloned()
        .collect();
    assert_eq!(kept.len(), 1, "the compaction keeps its output");

    // The retry rotates, superseding the failed edit.
    tree.major_compact(64 * 1024 * 1024, 0)?;
    let names = table_names(dir.path())?;
    for name in &kept {
        assert!(
            !names.contains(name),
            "the kept output {name} goes once superseded"
        );
    }
    for k in ["k0", "k1"] {
        assert!(tree.get(k, 3)?.is_some(), "{k} must read after the retry");
    }
    Ok(())
}

/// A compaction whose install cannot be withdrawn leaves its outcome unknown:
/// the snapshot naming its output may be the one a reopen finds, so the
/// output stays and the reopen reads every key from it.
#[test]
fn a_compaction_whose_install_cannot_be_withdrawn_keeps_its_output() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let snapshot = dir.path().join("v").to_string_lossy().into_owned();
    let fs = FaultFs::new(StdFs);
    let injector = fs.injector();
    let open = |fs| {
        Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .manifest_log_rotate_bytes(0)
        .table_file_refill_inline(true)
        .with_fs(fs)
        .open()
    };

    {
        let tree = open(fs)?;
        for (seqno, k) in (0u64..).zip(["k0", "k1"]) {
            tree.insert(k, "v", seqno);
            tree.flush_active_memtable(0)?;
        }
        // The install's directory sync fails, then the removal that would
        // withdraw its snapshot; the first removal is that of a leftover
        // temporary. The output is written into a pooled file, so the
        // install's sync is the first one, except where a folder sync does
        // nothing and so there is no pool (Windows): there the output's file
        // is made for it, and the install syncs its folder first.
        injector.arm(
            FaultRule::new(FaultOp::SyncDirectory, Fault::Error(ErrorKind::Other))
                .skip(UNPOOLED_FOLDER_SYNCS)
                .once(),
        );
        injector.arm(
            FaultRule::new(FaultOp::RemoveFile, Fault::Error(ErrorKind::Other))
                .on_path(snapshot)
                .skip(1)
                .once(),
        );
        assert!(
            matches!(
                tree.major_compact(64 * 1024 * 1024, 0),
                Err(lsm_tree::Error::InstallOutcomeUnknown(_))
            ),
            "the install can be neither made durable nor withdrawn"
        );
    }

    // The snapshot stayed, naming the compaction's output.
    let tree = open(FaultFs::new(StdFs))?;
    for k in ["k0", "k1"] {
        assert!(tree.get(k, 3)?.is_some(), "{k} must read after reopening");
    }
    Ok(())
}

/// A withdrawal whose own directory sync fails is not durable either: the
/// compaction's output stays on disk, for whichever generation a power loss
/// leaves, and the reopen's orphan sweep takes it when it is not named.
#[test]
fn a_compaction_whose_withdrawal_is_not_durable_keeps_its_output() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = FaultFs::new(StdFs);
    let injector = fs.injector();
    let open = |fs| {
        Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .manifest_log_rotate_bytes(0)
        .table_file_refill_inline(true)
        .with_fs(fs)
        .open()
    };

    {
        let tree = open(fs)?;
        for (seqno, k) in (0u64..).zip(["k0", "k1"]) {
            tree.insert(k, "v", seqno);
            tree.flush_active_memtable(0)?;
        }
        assert_eq!(table_files(dir.path())?, 2);
        // The install's directory sync fails, and so does the withdrawal's:
        // the output is written into a pooled file, so these are the first
        // two, after the output's own folder sync where there is no pool.
        injector.arm(
            FaultRule::new(FaultOp::SyncDirectory, Fault::Error(ErrorKind::Other))
                .skip(UNPOOLED_FOLDER_SYNCS)
                .times(2),
        );
        assert!(
            tree.major_compact(64 * 1024 * 1024, 0).is_err(),
            "the armed directory syncs must fail the compaction's install"
        );
        assert_eq!(
            table_files(dir.path())?,
            3,
            "the inputs and the output stay"
        );
    }

    let tree = open(FaultFs::new(StdFs))?;
    for k in ["k0", "k1"] {
        assert!(tree.get(k, 3)?.is_some(), "{k} must read after reopening");
    }
    Ok(())
}

/// An edit appended to the log whose sync fails may still reach the disk,
/// whole: the compaction that asked for it keeps its output, which a reopen
/// replaying the edit reads, and the next install rotates rather than append
/// after the edit and make it durable.
#[test]
fn a_compaction_whose_edit_sync_fails_keeps_its_output() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = FaultFs::new(StdFs);
    let injector = fs.injector();
    let open = |fs| {
        Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_fs(fs)
        .open()
    };

    {
        let tree = open(fs)?;
        for (seqno, k) in (0u64..).zip(["k0", "k1"]) {
            tree.insert(k, "v", seqno);
            tree.flush_active_memtable(0)?;
        }
        // The edit is written to the log; its sync fails.
        injector.arm(
            FaultRule::new(FaultOp::SyncData, Fault::Error(ErrorKind::Other))
                .on_path("edits-")
                .once(),
        );
        assert!(
            matches!(
                tree.major_compact(64 * 1024 * 1024, 0),
                Err(lsm_tree::Error::InstallOutcomeUnknown(_))
            ),
            "the armed sync must fail the compaction's install"
        );

        // The retry installs, and so does a flush after it.
        tree.major_compact(64 * 1024 * 1024, 0)?;
        tree.insert("k2", "v", 2);
        tree.flush_active_memtable(0)?;
        for k in ["k0", "k1", "k2"] {
            assert!(tree.get(k, 3)?.is_some(), "{k} must read after the retry");
        }
    }

    // The log still holds the failed edit: a reopen over it reads every key.
    let tree = open(FaultFs::new(StdFs))?;
    for k in ["k0", "k1", "k2"] {
        assert!(tree.get(k, 3)?.is_some(), "{k} must read after reopening");
    }
    Ok(())
}

/// A reopen right after an edit whose sync failed replays it when it reached
/// the log: the output it names is still there.
#[test]
fn a_reopen_after_a_failed_edit_sync_reads_the_output_it_names() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = FaultFs::new(StdFs);
    let injector = fs.injector();
    let open = |fs| {
        Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_fs(fs)
        .open()
    };

    {
        let tree = open(fs)?;
        for (seqno, k) in (0u64..).zip(["k0", "k1"]) {
            tree.insert(k, "v", seqno);
            tree.flush_active_memtable(0)?;
        }
        injector.arm(
            FaultRule::new(FaultOp::SyncData, Fault::Error(ErrorKind::Other))
                .on_path("edits-")
                .once(),
        );
        assert!(tree.major_compact(64 * 1024 * 1024, 0).is_err());
    }

    let tree = open(FaultFs::new(StdFs))?;
    for k in ["k0", "k1"] {
        assert!(tree.get(k, 3)?.is_some(), "{k} must read after reopening");
    }
    Ok(())
}

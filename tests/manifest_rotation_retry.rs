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
        .with_fs(fs)
        .open()
    };

    {
        let tree = open(fs)?;
        tree.insert("k0", "v0", 0);
        tree.flush_active_memtable(0)?;

        // The next flush syncs its table's folder, then writes its snapshot
        // and log and fails the directory sync that would install them.
        injector.arm(
            FaultRule::new(FaultOp::SyncDirectory, Fault::Error(ErrorKind::Other))
                .skip(1)
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

/// The highest snapshot id in `dir`.
fn installed(dir: &std::path::Path) -> lsm_tree::Result<u64> {
    let mut highest = None;
    for entry in std::fs::read_dir(dir)? {
        let name = entry?.file_name();
        if let Some(id) = name
            .to_str()
            .and_then(|n| n.strip_prefix('v'))
            .and_then(|id| id.parse::<u64>().ok())
        {
            highest = highest.max(Some(id));
        }
    }
    highest.ok_or(lsm_tree::Error::Unrecoverable)
}

/// A rotation that cannot carry the repair report the tree still owes fails
/// before it installs anything, and the next install rotates again and
/// carries it: the report stays with the installed snapshot throughout.
#[test]
fn a_rotation_that_cannot_carry_the_owed_report_is_retried() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let config = |fs: FaultFs<StdFs>| {
        Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .manifest_log_rotate_bytes(0)
        .with_fs(fs)
    };
    {
        let tree = config(FaultFs::new(StdFs)).open()?;
        tree.insert("k0", "v0", 0);
        tree.flush_active_memtable(0)?;
    }
    let owed = format!("repair-report-{}", installed(dir.path())?);
    std::fs::write(dir.path().join(&owed), b"an owed report")?;

    let fs = FaultFs::new(StdFs);
    let injector = fs.injector();
    let tree = config(fs).open()?;
    injector.arm(
        FaultRule::new(FaultOp::Open, Fault::Error(ErrorKind::Other))
            .on_path("repair-report-")
            .once(),
    );
    tree.insert("k1", "v1", 1);
    assert!(
        tree.flush_active_memtable(1).is_err(),
        "the rotation cannot read the report it carries"
    );
    tree.insert("k2", "v2", 2);
    tree.flush_active_memtable(2)?;

    let carried = dir
        .path()
        .join(format!("repair-report-{}", installed(dir.path())?));
    assert_eq!(std::fs::read(&carried)?, b"an owed report");
    for k in ["k0", "k1", "k2"] {
        assert!(tree.get(k, 3)?.is_some(), "{k} must read after the retry");
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
        .with_fs(fs)
        .open()
    };

    {
        let tree = open(fs)?;
        for (seqno, k) in (0u64..).zip(["k0", "k1"]) {
            tree.insert(k, "v", seqno);
            tree.flush_active_memtable(0)?;
        }
        // The compaction syncs its output's folder, then fails the directory
        // sync of the install that names the output.
        injector.arm(
            FaultRule::new(FaultOp::SyncDirectory, Fault::Error(ErrorKind::Other))
                .skip(1)
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

/// The files of the tree's tables folder.
fn table_files(dir: &std::path::Path) -> std::io::Result<usize> {
    Ok(std::fs::read_dir(dir.join("tables"))?.count())
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
        // temporary.
        injector.arm(
            FaultRule::new(FaultOp::SyncDirectory, Fault::Error(ErrorKind::Other))
                .skip(1)
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
        // The install's directory sync fails, and so does the withdrawal's.
        injector.arm(
            FaultRule::new(FaultOp::SyncDirectory, Fault::Error(ErrorKind::Other))
                .skip(1)
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

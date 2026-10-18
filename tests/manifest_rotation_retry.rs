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
        for (seqno, k) in ["k0", "k1"].into_iter().enumerate() {
            tree.insert(k, "v", seqno as u64);
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

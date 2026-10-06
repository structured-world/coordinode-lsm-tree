// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A manifest rotation that fails after writing its snapshot but before
//! `current` names it must not block the installs that follow: the retry
//! derives the same version id, and the snapshot left behind is garbage, not
//! a reason to refuse every later rotation until the process restarts.

#![cfg(feature = "std")]

use lsm_tree::fs::{Fault, FaultFs, FaultOp, FaultRule, StdFs};
use lsm_tree::io::ErrorKind;
use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};
use test_log::test;

#[test]
fn a_rotation_failed_before_current_moves_does_not_block_the_next_install() -> lsm_tree::Result<()>
{
    let dir = tempfile::tempdir()?;
    let fs = FaultFs::new(StdFs);
    let injector = fs.injector();
    let open = |fs| {
        Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        // Every install rotates, so each flush writes a fresh snapshot and
        // repoints `current`.
        .manifest_log_rotate_bytes(0)
        .with_fs(fs)
        .open()
    };

    {
        let tree = open(fs)?;
        tree.insert("k0", "v0", 0);
        tree.flush_active_memtable(0)?;

        // The next rotation writes its snapshot, then fails to repoint
        // `current`: the snapshot stays on disk, named by nothing.
        injector.arm(
            FaultRule::new(FaultOp::Rename, Fault::Error(ErrorKind::Other))
                .on_path("current")
                .once(),
        );
        tree.insert("k1", "v1", 1);
        assert!(
            tree.flush_active_memtable(1).is_err(),
            "the armed rename must fail this install"
        );

        // The retry derives the same version id. It must install, not refuse
        // on the snapshot the failed attempt left.
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

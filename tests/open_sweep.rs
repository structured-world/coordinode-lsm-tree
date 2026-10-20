// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! What an open removes that no version names, and what it does when it cannot
//! remove it.

use lsm_tree::fs::{Fault, FaultFs, FaultOp, FaultRule, StdFs};
use lsm_tree::{AbstractTree, Config, MAX_SEQNO, SequenceNumberCounter, get_tmp_folder};
use test_log::test;

fn config(folder: &std::path::Path) -> Config {
    Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
}

/// A tree at `folder` holding one flushed key, closed again.
fn a_tree(folder: &std::path::Path) -> lsm_tree::Result<()> {
    let tree = config(folder).open()?;
    tree.insert("a", "value", 0);
    tree.flush_active_memtable(0)?;
    Ok(())
}

/// A restriction bound whose table is gone, and a repair replacement's
/// sidecar whose replacement is gone, are engine files no version names: an
/// open removes both and serves the tree.
#[test]
fn an_open_removes_sidecars_whose_table_is_gone() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    a_tree(folder.path())?;
    let tables = folder.path().join("tables");
    let bound = tables.join("99.restrict-bound");
    let companion = tables.join("98.repair-tmp.restrict-bound");
    std::fs::write(&bound, b"orphan")?;
    std::fs::write(&companion, b"orphan")?;

    let tree = config(folder.path()).open()?;
    assert_eq!(tree.get("a", MAX_SEQNO)?.as_deref(), Some(&b"value"[..]));
    assert!(!bound.exists(), "the orphaned restriction bound is removed");
    assert!(
        !companion.exists(),
        "the orphaned replacement sidecar is removed"
    );
    Ok(())
}

/// Only names the engine writes are its manifest files: an open leaves a
/// file of another spelling in the tree root alone.
#[test]
fn an_open_keeps_foreign_files_in_the_tree_root() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    a_tree(folder.path())?;
    let foreign = ["vendor-notes.txt", "v1.bak", "edits-old", "v007"];
    for name in foreign {
        std::fs::write(folder.path().join(name), b"not the engine's")?;
    }

    let tree = config(folder.path()).open()?;
    assert_eq!(tree.get("a", MAX_SEQNO)?.as_deref(), Some(&b"value"[..]));
    for name in foreign {
        assert!(folder.path().join(name).exists(), "{name} is kept");
    }
    Ok(())
}

/// An orphan the open cannot remove fails the open with the removal's error:
/// the open does not serve a tree whose leftovers it was asked to clear.
#[test]
fn an_open_that_cannot_remove_an_orphan_fails() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    a_tree(folder.path())?;
    std::fs::write(
        folder.path().join("tables").join("99.restrict-bound"),
        b"orphan",
    )?;

    let fs = FaultFs::new(StdFs);
    fs.injector().arm(
        FaultRule::new(
            FaultOp::RemoveFile,
            Fault::Error(lsm_tree::io::ErrorKind::PermissionDenied),
        )
        .on_path("99.restrict-bound"),
    );
    assert!(config(folder.path()).with_fs(fs).open().is_err());
    Ok(())
}

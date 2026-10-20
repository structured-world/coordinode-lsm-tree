// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A repair's report rides in the snapshot it describes until
//! `open_or_repair` hands it out and installs a snapshot that no longer owes
//! it. The report reaches the caller at least once, and a repeat carries the
//! same id.

#![cfg(feature = "std")]

use lsm_tree::fs::{Fault, FaultFs, FaultOp, FaultRule, StdFs};
use lsm_tree::io::ErrorKind;
use lsm_tree::{AbstractTree, Config, RepairPolicy, SequenceNumberCounter, get_tmp_folder};
use test_log::test;

fn config(folder: &std::path::Path) -> Config {
    Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
}

/// A tree at `folder` holding key `a`, its manifest lost, so an open has to
/// repair it.
fn a_tree_needing_repair(folder: &std::path::Path) -> lsm_tree::Result<()> {
    {
        let tree = config(folder).open()?;
        tree.insert("a", "value", 0);
        tree.flush_active_memtable(0)?;
    }
    for entry in std::fs::read_dir(folder)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with('v') || name.starts_with("edits-") {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

/// A filesystem whose renames onto snapshot names in `folder` fail after the
/// first `skip`: the repair installs one, the open after it a second, and the
/// install that retires the report a third.
fn failing_snapshot_renames(folder: &std::path::Path, skip: u64) -> FaultFs<StdFs> {
    let fs = FaultFs::new(StdFs);
    fs.injector().arm(
        FaultRule::new(FaultOp::Rename, Fault::Error(ErrorKind::Other))
            .on_path(folder.join("v").to_string_lossy())
            .skip(skip)
            .once(),
    );
    fs
}

/// A repair whose snapshot was proven not installed leaves no report to hand
/// out: the report lives in that snapshot, so it went with it, whatever id
/// the next open installs the tree it finds under.
#[test]
fn a_repair_that_never_installed_hands_out_no_report() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    {
        let tree = config(folder.path()).open()?;
        tree.insert("a", "value", 0);
        tree.flush_active_memtable(0)?;
    }
    // A reopen installs the tree as a snapshot with no edits after it, so the
    // next open reinstalls it under the id above that snapshot: the id a
    // repair takes too.
    drop(config(folder.path()).open()?);

    let failed = config(folder.path())
        .with_fs(failing_snapshot_renames(folder.path(), 0))
        .repair();
    assert!(failed.is_err(), "the rebuilt snapshot is not installed");

    let (tree, report) = config(folder.path()).open_or_repair(RepairPolicy::default())?;
    assert!(
        report.is_none(),
        "no report for a repair that never committed: {report:?}"
    );
    assert!(tree.get("a", 1)?.is_some());
    Ok(())
}

/// A report whose retiring install fails before installing anything stays
/// owed: the call fails without it, and the next call hands it out, once.
#[test]
fn a_report_whose_retiring_install_fails_is_handed_out_by_the_next_call() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    a_tree_needing_repair(folder.path())?;

    let first = config(folder.path())
        .with_fs(failing_snapshot_renames(folder.path(), 2))
        .open_or_repair(RepairPolicy::default());
    assert!(
        matches!(first, Err(lsm_tree::Error::Io(_))),
        "the report is withheld while a snapshot still owes it: {:?}",
        first.map(|(_, report)| report)
    );

    let (tree, report) = config(folder.path()).open_or_repair(RepairPolicy::default())?;
    assert!(report.is_some(), "the next call hands the report out");
    assert!(tree.get("a", 1)?.is_some());
    drop(tree);
    let (_tree, again) = config(folder.path()).open_or_repair(RepairPolicy::default())?;
    assert!(again.is_none(), "once");
    Ok(())
}

/// A retiring install whose outcome is unknown may leave the snapshot that
/// owes the report installed after a power loss, so the report is handed out
/// now, inside the error, and a later call that finds it owed hands it out
/// again under the same id: a caller that recorded the id skips it.
#[test]
fn a_report_handed_out_twice_carries_one_id() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    a_tree_needing_repair(folder.path())?;
    let withheld = config(folder.path())
        .with_fs(failing_snapshot_renames(folder.path(), 2))
        .open_or_repair(RepairPolicy::default());
    assert!(withheld.is_err(), "self-check: the report is still owed");

    // The open installs the tree once and syncs the folder for it; the
    // retiring install's sync fails, and so does its withdrawal's, which
    // removed the name: the outcome is unknown, the old snapshot visible.
    let fs = FaultFs::new(StdFs);
    fs.injector().arm(
        FaultRule::new(FaultOp::SyncDirectory, Fault::Error(ErrorKind::Other))
            .on_path(folder.path().to_string_lossy())
            .skip(1)
            .times(2),
    );
    let first = match config(folder.path())
        .with_fs(fs)
        .open_or_repair(RepairPolicy::default())
    {
        Err(lsm_tree::Error::RepairedButUnopened { report, .. }) => report,
        other => panic!(
            "expected the report inside RepairedButUnopened, got {:?}",
            other.map(|(_, report)| report)
        ),
    };

    let (_tree, again) = config(folder.path()).open_or_repair(RepairPolicy::default())?;
    let again = again.ok_or(lsm_tree::Error::Unrecoverable)?;
    assert_eq!(again.id, first.id, "the repeat carries the same id");
    assert_eq!(again.lost_coverage, first.lost_coverage);
    Ok(())
}

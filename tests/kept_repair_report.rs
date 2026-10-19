// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A repair report kept on disk (a repair whose install outcome was unknown)
//! reaches the caller exactly once: `open_or_repair` hands it out and retires
//! the copy, and a retirement that does not finish never loses it.

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

/// A tree at `folder` with a report copy kept for its installed snapshot.
fn a_tree_owing_a_report(folder: &std::path::Path) -> lsm_tree::Result<()> {
    {
        let tree = config(folder).open()?;
        tree.insert("a", "value", 0);
        tree.flush_active_memtable(0)?;
    }
    let mut installed = None;
    for entry in std::fs::read_dir(folder)? {
        let name = entry?.file_name();
        if let Some(id) = name
            .to_str()
            .and_then(|n| n.strip_prefix('v'))
            .and_then(|id| id.parse::<u64>().ok())
        {
            installed = installed.max(Some(id));
        }
    }
    let installed = installed.ok_or(lsm_tree::Error::Unrecoverable)?;
    std::fs::write(
        folder.join(format!("repair-report-{installed}")),
        b"a report the caller has not seen",
    )?;
    Ok(())
}

/// Whether `folder` still holds a report copy.
fn holds_a_copy(folder: &std::path::Path) -> lsm_tree::Result<bool> {
    for entry in std::fs::read_dir(folder)? {
        if entry?
            .file_name()
            .to_str()
            .is_some_and(|n| n.starts_with("repair-report-"))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// A copy that cannot be removed still carries the report, so the call fails
/// rather than hand it out, and the next call hands it out.
#[test]
fn a_kept_report_whose_copy_cannot_be_removed_is_handed_out_later() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    a_tree_owing_a_report(folder.path())?;

    let fs = FaultFs::new(StdFs);
    // The open's own sweep removes the copy of the generation it replaces
    // first; the second removal is the retirement.
    fs.injector().arm(
        FaultRule::new(
            FaultOp::RemoveFile,
            Fault::Error(ErrorKind::PermissionDenied),
        )
        .on_path("repair-report-")
        .skip(1)
        .once(),
    );
    let first = config(folder.path())
        .with_fs(fs)
        .open_or_repair(RepairPolicy::default());
    assert!(first.is_err(), "the copy could not be retired");
    assert!(
        holds_a_copy(folder.path())?,
        "the copy still carries the report"
    );

    let (_tree, report) = config(folder.path()).open_or_repair(RepairPolicy::default())?;
    assert!(report.is_some(), "the next call hands the report out");
    assert!(!holds_a_copy(folder.path())?, "and retires the copy");
    Ok(())
}

/// A removal that cannot be made durable may come back after a power loss,
/// so the report is handed out now, inside the error.
#[test]
fn a_kept_report_whose_retirement_is_not_durable_rides_out_in_the_error() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    a_tree_owing_a_report(folder.path())?;

    let fs = FaultFs::new(StdFs);
    // The open's install syncs the tree folder once; the next sync of it is
    // the retirement's.
    fs.injector().arm(
        FaultRule::new(FaultOp::SyncDirectory, Fault::Error(ErrorKind::Other))
            .on_path(folder.path().to_string_lossy())
            .skip(1)
            .once(),
    );
    match config(folder.path())
        .with_fs(fs)
        .open_or_repair(RepairPolicy::default())
    {
        Err(lsm_tree::Error::RepairedButUnopened { .. }) => {}
        other => panic!(
            "expected the report inside RepairedButUnopened, got {:?}",
            other.map(|(_, report)| report)
        ),
    }
    Ok(())
}

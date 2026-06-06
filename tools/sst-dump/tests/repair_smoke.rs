// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! End-to-end smoke test for `sst-dump repair`: build a real tree via
//! `lsm-tree`, delete its manifest, drive the `repair` subcommand against the DB
//! directory, and assert the manifest is rebuilt (exit 0) and the tree reopens
//! with all keys intact.

use lsm_tree::{AbstractTree, Config, MAX_SEQNO, SequenceNumberCounter};
use std::process::Command;

const SST_DUMP_BIN: &str = env!("CARGO_BIN_EXE_sst-dump");

/// Keep in sync with the copy in the `lsm-tree` crate's `tests/repair.rs` (a
/// separate crate, so the helper cannot be shared directly): both encode the
/// manifest file-naming convention (`v{N}` + `current`).
fn nuke_manifest(dir: &std::path::Path) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let is_version = name
            .strip_prefix('v')
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version || name == "current" {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

#[test]
fn repair_rebuilds_manifest_and_db_reopens() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;

    {
        let tree = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;
        for i in 0u64..200 {
            tree.insert(format!("key-{i:06}"), format!("value-{i}"), 1 + i);
        }
        tree.flush_active_memtable(0)?;
    }

    nuke_manifest(dir.path())?;

    let out = Command::new(SST_DUMP_BIN)
        .arg(dir.path())
        .arg("repair")
        .output()?;

    assert!(
        out.status.success(),
        "repair should exit 0, stderr: {}",
        String::from_utf8_lossy(&out.stderr),
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("manifest rebuilt"),
        "unexpected output: {stdout}",
    );

    // The DB must reopen and serve every key after the rebuild.
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    for i in 0u64..200 {
        let got = tree.get(format!("key-{i:06}"), MAX_SEQNO)?;
        assert_eq!(got.as_deref(), Some(format!("value-{i}").as_bytes()));
    }

    Ok(())
}

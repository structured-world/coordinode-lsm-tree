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
    assert!(
        stdout.contains("wal replay:    tail only"),
        "a lossless repair must still state the replay obligation, so an \
         external-WAL operator never has to infer it: {stdout}",
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

/// A repair that drops a table regresses persisted state below an external
/// WAL's trim watermark, so the tail replay that watermark implies is no
/// longer sufficient. The CLI is the operator's whole view of the repair:
/// if it reports success without naming that obligation, the replay stops at
/// the tail and superseded or deleted values stay visible.
#[test]
fn repair_prints_the_wal_replay_obligation_when_coverage_is_lost()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;

    {
        let tree = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;
        for i in 0u64..50 {
            tree.insert(format!("key-{i:06}"), format!("value-{i}"), 1 + i);
        }
        tree.flush_active_memtable(0)?;
        for i in 50u64..100 {
            tree.insert(format!("key-{i:06}"), format!("value-{i}"), 1 + i);
        }
        tree.flush_active_memtable(0)?;
    }

    nuke_manifest(dir.path())?;

    // Truncate one table to nothing readable: its metadata never parses, so
    // repair cannot scope the loss by seqno at all.
    let victim = std::fs::read_dir(dir.path().join("tables"))?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .min()
        .ok_or("the tree wrote at least one table")?;
    std::fs::write(&victim, b"not an sst")?;

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
        stdout.contains("wal replay:    REQUIRED"),
        "the lost coverage must be reported as a replay obligation: {stdout}",
    );
    assert!(
        stdout.contains("unscopable") || stdout.contains("lost "),
        "the obligation must name the affected file: {stdout}",
    );
    // The destroyed table's metadata never parsed, so the loss has no key
    // range at all. Pointing at "the ranges below" would have the operator
    // reconcile an EMPTY list and call the repair done.
    assert!(
        stdout.contains("ENTIRE KEYSPACE"),
        "an unlocalizable loss must state the whole-keyspace obligation, not refer to \
         ranges that do not exist: {stdout}",
    );
    assert!(
        stdout.contains("merge operands are NOT blindly replayable"),
        "re-applying a surviving merge operand folds it twice: the exception must be \
         stated where the obligation is: {stdout}",
    );

    Ok(())
}

/// A stray `blobs/` directory beside a STANDARD tree is not evidence of KV
/// separation. Inferring one from its mere presence commits a blob manifest
/// over standard SSTs, and that traps the store: the application's standard
/// open then fails, and a standard repair declines to touch the now-clean blob
/// manifest. An empty directory must leave the rebuild standard.
#[test]
fn an_empty_blobs_dir_does_not_make_a_standard_tree_a_blob_tree()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;

    {
        let tree = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;
        for i in 0u64..50 {
            tree.insert(format!("key-{i:06}"), format!("value-{i}"), 1 + i);
        }
        tree.flush_active_memtable(0)?;
    }
    // What an operator, a partial copy, or a backup tool can leave behind.
    std::fs::create_dir_all(dir.path().join("blobs"))?;
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

    // The proof is the reopen: a blob manifest would fail a standard open with
    // a tree-type mismatch, and no later repair could undo it.
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    for i in 0u64..50 {
        assert_eq!(
            tree.get(format!("key-{i:06}"), MAX_SEQNO)?.as_deref(),
            Some(format!("value-{i}").as_bytes()),
        );
    }
    Ok(())
}

/// An unreadable `blobs/` means the scan could not ANSWER, not that the tree is
/// standard. Publishing a tree type on that guess is unrecoverable, so the
/// command must stop and let the operator retry or state the type.
#[test]
#[cfg(unix)]
fn an_unreadable_blobs_dir_stops_the_repair_instead_of_guessing()
-> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir()?;
    {
        let tree = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;
        tree.insert("k", "v", 1);
        tree.flush_active_memtable(0)?;
    }
    let blobs = dir.path().join("blobs");
    std::fs::create_dir_all(&blobs)?;
    nuke_manifest(dir.path())?;
    std::fs::set_permissions(&blobs, std::fs::Permissions::from_mode(0o000))?;

    let out = Command::new(SST_DUMP_BIN)
        .arg(dir.path())
        .arg("repair")
        .output()?;
    // Restore before asserting so the tempdir can always be cleaned up.
    std::fs::set_permissions(&blobs, std::fs::Permissions::from_mode(0o755))?;

    assert!(
        !out.status.success(),
        "an inconclusive scan must not publish a tree type: {}",
        String::from_utf8_lossy(&out.stdout),
    );
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("--tree-type"),
        "the operator is told how to proceed: {}",
        String::from_utf8_lossy(&out.stderr),
    );
    Ok(())
}

/// The name is not the evidence: a file under `blobs/` whose name PARSES as a
/// blob id (an operator's backup called `0`, say) is still not a blob file.
/// Inferring from the name alone publishes a blob manifest over a standard
/// store, and every later standard open then fails with a type mismatch while
/// the committed manifest looks clean, so no repair will touch it.
#[test]
fn a_numeric_non_blob_file_does_not_make_a_standard_tree_a_blob_tree()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;

    {
        let tree = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;
        for i in 0u64..50 {
            tree.insert(format!("key-{i:06}"), format!("value-{i}"), 1 + i);
        }
        tree.flush_active_memtable(0)?;
    }
    std::fs::create_dir_all(dir.path().join("blobs"))?;
    std::fs::write(dir.path().join("blobs").join("0"), b"operator backup")?;
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

    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    for i in 0u64..50 {
        assert_eq!(
            tree.get(format!("key-{i:06}"), MAX_SEQNO)?.as_deref(),
            Some(format!("value-{i}").as_bytes()),
        );
    }
    Ok(())
}

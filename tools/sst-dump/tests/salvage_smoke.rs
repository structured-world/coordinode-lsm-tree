// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! End-to-end smoke test for `sst-dump salvage`: build a real SST via
//! `lsm-tree`, drive the `salvage` subcommand against it, and assert the
//! subcommand reports a full recovery (exit 0) and writes a reopenable copy.

use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};
use std::process::Command;

const SST_DUMP_BIN: &str = env!("CARGO_BIN_EXE_sst-dump");

/// Returns the lexicographically-first numerically-named SST file under
/// `<dir>/tables/` (one flush produces exactly one).
fn sole_sst(dir: &std::path::Path) -> std::path::PathBuf {
    let mut ssts: Vec<std::path::PathBuf> = std::fs::read_dir(dir.join("tables"))
        .expect("tables dir exists")
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| {
            p.is_file()
                && p.file_name()
                    .is_some_and(|n| n.to_string_lossy().chars().all(|c| c.is_ascii_digit()))
        })
        .collect();
    ssts.sort();
    ssts.into_iter().next().expect("one SST file under tables/")
}

#[test]
fn salvage_recovers_a_healthy_sst() -> Result<(), Box<dyn std::error::Error>> {
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

    let source = sole_sst(dir.path());
    let dest = dir.path().join("salvaged.sst");

    let out = Command::new(SST_DUMP_BIN)
        .arg(&source)
        .arg("salvage")
        .arg(&dest)
        .output()?;

    assert!(
        out.status.success(),
        "salvage should exit 0, stderr: {}",
        String::from_utf8_lossy(&out.stderr),
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("fully recovered"),
        "a healthy SST should fully recover, output: {stdout}",
    );
    assert!(
        dest.is_file(),
        "the salvaged SST should be written to the destination",
    );

    // The salvaged copy reopens and serves every key (reopen the source tree's
    // tables dir would need a manifest; instead just confirm the file is a
    // non-empty SST that the verify subcommand accepts).
    let verify = Command::new(SST_DUMP_BIN)
        .arg(&dest)
        .arg("verify")
        .output()?;
    assert!(
        verify.status.success(),
        "the salvaged SST should pass verify, stderr: {}",
        String::from_utf8_lossy(&verify.stderr),
    );
    Ok(())
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Disaster-recovery: rebuilding the manifest from on-disk SSTs.
//!
//! The contract under test: after the manifest (and its `current` pointer) is
//! gone, `Config::repair` reconstructs a manifest from the SST files alone such
//! that every previously written key is still readable on reopen. Recent
//! unlogged version edits may be lost, but no readable SST's data is dropped.

#![cfg(feature = "std")]

use lsm_tree::{AbstractTree, Config, KvSeparationOptions, MAX_SEQNO, SequenceNumberCounter};
use test_log::test;

fn key(i: u64) -> String {
    format!("k{i:05}")
}

/// Removes every `v{N}` manifest file and the `current` pointer from a tree
/// directory, simulating a manifest loss while leaving the SSTs intact.
fn nuke_manifest(dir: &std::path::Path) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let is_version = name
            .strip_prefix('v')
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version || name == "current" {
            std::fs::remove_file(entry.path()).unwrap();
        }
    }
}

fn count_sst_files(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir.join("tables"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.file_name().to_string_lossy().parse::<u64>().is_ok())
        .count()
}

#[test]
fn repair_rebuilds_manifest_and_preserves_all_keys() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;

    // Three flushes → three L0 tables, with an overwrite in the last batch so
    // repair has to preserve the latest value across overlapping L0 runs.
    {
        let tree = Config::new(
            &dir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        for i in 0..100 {
            tree.insert(key(i), format!("v0-{i}"), i);
        }
        tree.flush_active_memtable(0)?;

        for i in 100..200 {
            tree.insert(key(i), format!("v0-{i}"), i);
        }
        tree.flush_active_memtable(0)?;

        // Overwrite the first 50 keys with higher seqnos in a fresh table.
        for i in 0..50 {
            tree.insert(key(i), format!("v1-{i}"), 1_000 + i);
        }
        tree.flush_active_memtable(0)?;
    }

    let sst_count = count_sst_files(dir.path());
    assert!(sst_count >= 3, "expected at least 3 SSTs, got {sst_count}");

    nuke_manifest(dir.path());

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair()?;

    assert_eq!(
        report.recovered, sst_count,
        "every SST on disk must be recovered",
    );
    assert_eq!(report.unreadable, 0, "no SST should be unreadable");

    // Reopen and verify every key reads back its latest value.
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    for i in 0..50 {
        assert_eq!(
            tree.get(key(i), MAX_SEQNO)?.as_deref(),
            Some(format!("v1-{i}").as_bytes()),
            "overwritten key {} must read the latest value after repair",
            key(i),
        );
    }
    for i in 50..200 {
        assert_eq!(
            tree.get(key(i), MAX_SEQNO)?.as_deref(),
            Some(format!("v0-{i}").as_bytes()),
            "key {} must survive repair",
            key(i),
        );
    }

    Ok(())
}

#[test]
fn repair_skips_unreadable_file_but_recovers_the_rest() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;

    {
        let tree = Config::new(
            &dir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;
        for i in 0..100 {
            tree.insert(key(i), format!("v0-{i}"), i);
        }
        tree.flush_active_memtable(0)?;
    }

    let good_count = count_sst_files(dir.path());
    assert!(good_count >= 1);

    nuke_manifest(dir.path());

    // Drop a garbage file with a table-id-shaped name into the tables folder.
    // A free id well above any the tree allocated avoids colliding with a real
    // table that could then be silently overwritten.
    let bogus = dir.path().join("tables").join("999999");
    std::fs::write(&bogus, b"not a valid sst file at all").unwrap();

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair()?;

    assert_eq!(report.recovered, good_count, "real SSTs must be recovered");
    assert_eq!(report.unreadable, 1, "the garbage file must be reported");
    assert!(
        report.unreadable_files[0].0.ends_with("999999"),
        "the reported unreadable path must be the garbage file",
    );

    // The intact data is still fully readable.
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    for i in 0..100 {
        assert_eq!(
            tree.get(key(i), MAX_SEQNO)?.as_deref(),
            Some(format!("v0-{i}").as_bytes()),
        );
    }

    Ok(())
}

#[test]
fn repair_rejects_kv_separated_trees() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;

    {
        let tree = Config::new(
            &dir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(KvSeparationOptions::default()))
        .open()?;
        tree.insert("k", "v", 0);
        tree.flush_active_memtable(0)?;
    }

    nuke_manifest(dir.path());

    let result = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default()))
    .repair();

    assert!(
        matches!(result, Err(lsm_tree::Error::FeatureUnsupported(_))),
        "blob trees are not yet repairable, got {result:?}",
    );

    Ok(())
}

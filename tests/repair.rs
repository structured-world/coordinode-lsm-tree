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
///
/// Keep in sync with the copy in `tools/sst-dump/tests/repair_smoke.rs` (a
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

fn count_sst_files(dir: &std::path::Path) -> std::io::Result<usize> {
    Ok(std::fs::read_dir(dir.join("tables"))?
        .filter_map(Result::ok)
        .filter(|e| e.file_name().to_string_lossy().parse::<u64>().is_ok())
        .count())
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

    let sst_count = count_sst_files(dir.path())?;
    assert!(sst_count >= 3, "expected at least 3 SSTs, got {sst_count}");

    nuke_manifest(dir.path())?;

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

    let good_count = count_sst_files(dir.path())?;
    assert!(good_count >= 1);

    nuke_manifest(dir.path())?;

    // Drop a garbage file with a table-id-shaped name into the tables folder.
    // A free id well above any the tree allocated avoids colliding with a real
    // table that could then be silently overwritten.
    let bogus = dir.path().join("tables").join("999999");
    std::fs::write(&bogus, b"not a valid sst file at all")?;

    // A macOS Finder artifact must be silently skipped, not counted as
    // unreadable.
    std::fs::write(dir.path().join("tables").join(".DS_Store"), b"\x00")?;

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
fn repair_with_no_ssts_produces_empty_readable_tree() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;

    // Open and close without ever flushing: the manifest exists but no SST does
    // (manifest lost before the first flush is the scenario).
    {
        let _tree = Config::new(
            &dir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;
    }

    nuke_manifest(dir.path())?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair()?;

    assert_eq!(report.recovered, 0, "no SSTs to recover");
    assert_eq!(report.unreadable, 0);

    // The rebuilt (empty) manifest must still open cleanly and read as empty.
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    assert_eq!(tree.get("anything", MAX_SEQNO)?, None);

    Ok(())
}

#[test]
fn repair_reports_non_table_id_filename_as_unreadable() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;

    {
        let tree = Config::new(
            &dir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;
        for i in 0..20 {
            tree.insert(key(i), format!("v0-{i}"), i);
        }
        tree.flush_active_memtable(0)?;
    }

    let good_count = count_sst_files(dir.path())?;
    nuke_manifest(dir.path())?;

    // A non-numeric file name cannot be a table id. Repair must move it out of
    // `tables/` (Tree::open rejects non-numeric names outright), otherwise repair
    // would report success while the DB still cannot reopen.
    let bad = dir.path().join("tables").join("not-a-table-id");
    std::fs::write(&bad, b"whatever")?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair()?;

    assert_eq!(report.recovered, good_count);
    assert_eq!(report.unreadable, 1);
    assert!(
        report.unreadable_files[0].0.ends_with("not-a-table-id"),
        "the non-numeric file must be the reported unreadable entry",
    );
    assert!(
        report.unreadable_files[0].1.contains("table id"),
        "the reason should explain the name is not a table id, got: {}",
        report.unreadable_files[0].1,
    );

    // The junk must no longer sit in `tables/` — repair quarantines it so the
    // tree reopens cleanly WITHOUT any manual cleanup.
    assert!(
        !dir.path().join("tables").join("not-a-table-id").exists(),
        "non-table-id file must be moved out of tables/ by repair",
    );
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    for i in 0..20 {
        assert_eq!(
            tree.get(key(i), MAX_SEQNO)?.as_deref(),
            Some(format!("v0-{i}").as_bytes()),
        );
    }

    Ok(())
}

// A production-written SST that is later corrupted must be rejected by
// `Table::recover` during repair (per-block / structural validation), reported,
// and skipped — while intact SSTs still recover and the tree reopens.
#[test]
fn repair_rejects_corrupted_sst_and_recovers_the_rest() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;

    {
        let tree = Config::new(
            &dir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;
        for i in 0..25 {
            tree.insert(key(i), format!("v0-{i}"), i);
        }
        tree.flush_active_memtable(0)?;
        for i in 25..50 {
            tree.insert(key(i), format!("v0-{i}"), 1000 + i);
        }
        tree.flush_active_memtable(0)?;
    }

    let total = count_sst_files(dir.path())?;
    assert!(
        total >= 2,
        "need two SSTs (intact + corrupted), got {total}"
    );
    nuke_manifest(dir.path())?;

    // Corrupt the newest SST (highest table id = second flush, keys 25..50) by
    // tampering its SFA trailer, so `Table::recover` rejects it. Production write
    // path: a real flushed table, single trailing region flipped.
    let tables = dir.path().join("tables");
    let newest = std::fs::read_dir(&tables)?
        .filter_map(Result::ok)
        .filter_map(|e| {
            e.file_name()
                .to_string_lossy()
                .parse::<u64>()
                .ok()
                .map(|id| (id, e.path()))
        })
        .max_by_key(|(id, _)| *id)
        .expect("at least one SST")
        .1;
    let mut bytes = std::fs::read(&newest)?;
    let n = bytes.len();
    for b in &mut bytes[n - 32..] {
        *b ^= 0xFF;
    }
    std::fs::write(&newest, &bytes)?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair()?;

    assert_eq!(report.unreadable, 1, "the corrupted SST must be rejected");
    assert!(
        report.unreadable_files[0]
            .0
            .ends_with(newest.file_name().expect("file name")),
        "the corrupted SST must be the reported unreadable entry",
    );
    assert_eq!(
        report.recovered,
        total - 1,
        "every intact SST must still be recovered",
    );

    // Reopen succeeds; the intact SST's keys read back (the corrupted SST's keys
    // are gone — it was not added to the manifest and is orphan-cleaned on open).
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    for i in 0..25 {
        assert_eq!(
            tree.get(key(i), MAX_SEQNO)?.as_deref(),
            Some(format!("v0-{i}").as_bytes()),
            "intact key {} must survive repair",
            key(i),
        );
    }

    Ok(())
}

// A table-id-named entry that cannot even be opened (here a dangling symlink)
// must be reported via the checksum step's failure path, not abort the repair.
#[cfg(unix)]
#[test]
fn repair_reports_unopenable_file_as_unreadable() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;

    {
        let tree = Config::new(
            &dir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;
        for i in 0..20 {
            tree.insert(key(i), format!("v0-{i}"), i);
        }
        tree.flush_active_memtable(0)?;
    }

    let good_count = count_sst_files(dir.path())?;
    nuke_manifest(dir.path())?;

    // Dangling symlink with a valid table-id name: `read_dir` lists it and the
    // name parses, but opening it to checksum fails.
    let dangling = dir.path().join("tables").join("888888");
    std::os::unix::fs::symlink(dir.path().join("does-not-exist"), &dangling)?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair()?;

    assert_eq!(
        report.recovered, good_count,
        "real SSTs must still be recovered"
    );
    assert_eq!(report.unreadable, 1, "the unopenable file must be reported");
    assert!(report.unreadable_files[0].0.ends_with("888888"));

    Ok(())
}

#[test]
fn repair_fails_when_a_bad_filename_cannot_be_quarantined() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let big = |i: u64| format!("{i:08}").repeat(512);

    {
        let tree = Config::new(
            &dir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(KvSeparationOptions::default()))
        .open()?;
        for i in 0..10 {
            tree.insert(key(i), big(i).as_bytes(), i);
        }
        tree.flush_active_memtable(0)?;
    }
    assert!(count_blob_files(dir.path())? >= 1);

    nuke_manifest(dir.path())?;

    // A non-numeric name in blobs/ would make the reopened tree's blob recovery
    // (which parses every name) abort, so repair must quarantine it.
    std::fs::write(dir.path().join("blobs").join("not-a-blob-id"), b"junk")?;
    // Block the quarantine: occupy the `repair-quarantine` directory path with a
    // regular file so the rename's `create_dir_all` fails. Repair must then abort
    // rather than report success while leaving the un-quarantined name in place
    // (which would make the tree unopenable).
    std::fs::write(dir.path().join("repair-quarantine"), b"blocker")?;

    let result = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default()))
    .repair();

    assert!(
        result.is_err(),
        "repair must fail when it cannot quarantine a bad filename, got {result:?}",
    );

    Ok(())
}

#[test]
fn repair_fails_when_a_bad_table_filename_cannot_be_quarantined() -> lsm_tree::Result<()> {
    // Sibling of the blob-side test above, covering the standard `tables/`
    // quarantine path so the false-success regression cannot slip back for
    // standard trees.
    let dir = tempfile::tempdir()?;

    {
        let tree = Config::new(
            &dir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;
        for i in 0..10 {
            tree.insert(key(i), format!("v-{i}").as_bytes(), i);
        }
        tree.flush_active_memtable(0)?;
    }
    assert!(count_sst_files(dir.path())? >= 1);

    nuke_manifest(dir.path())?;

    // A non-numeric name in tables/ would make the reopened tree's recovery
    // (which parses every name) abort, so repair must quarantine it.
    std::fs::write(dir.path().join("tables").join("not-a-table-id"), b"junk")?;
    // Block the quarantine by occupying the `repair-quarantine` directory path
    // with a regular file. Repair must abort rather than report success while
    // leaving the un-quarantined name in place.
    std::fs::write(dir.path().join("repair-quarantine"), b"blocker")?;

    let result = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair();

    assert!(
        result.is_err(),
        "repair must fail when it cannot quarantine a bad table filename, got {result:?}",
    );

    Ok(())
}

/// Counts numerically-named blob files in a tree's `blobs/` folder.
fn count_blob_files(dir: &std::path::Path) -> std::io::Result<usize> {
    let blobs = dir.join("blobs");
    if !blobs.exists() {
        return Ok(0);
    }
    Ok(std::fs::read_dir(blobs)?
        .filter_map(Result::ok)
        .filter(|e| e.file_name().to_string_lossy().parse::<u64>().is_ok())
        .count())
}

#[test]
fn repair_rebuilds_blob_tree_manifest_and_preserves_values() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;

    // ~4 KiB values, above the 1 KiB KV-separation threshold, so they spill into
    // the value log as blob files: the artifact a blob-tree repair must
    // rediscover (a plain SST scan would otherwise lose them).
    let big = |i: u64| format!("{i:08}").repeat(512);

    {
        let tree = Config::new(
            &dir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(KvSeparationOptions::default()))
        .open()?;

        for i in 0..50 {
            tree.insert(key(i), big(i).as_bytes(), i);
        }
        tree.flush_active_memtable(0)?;

        for i in 50..100 {
            tree.insert(key(i), big(i).as_bytes(), i);
        }
        tree.flush_active_memtable(0)?;
    }

    let blob_count = count_blob_files(dir.path())?;
    assert!(
        blob_count >= 1,
        "expected at least one blob file to exist, got {blob_count}",
    );
    let sst_count = count_sst_files(dir.path())?;

    nuke_manifest(dir.path())?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default()))
    .repair()?;

    assert_eq!(
        report.recovered, sst_count,
        "every SST on disk must be recovered",
    );
    assert_eq!(report.unreadable, 0, "no file should be unreadable");

    // Reopen and verify every blob-backed value reads back, proving the blob
    // files were rediscovered and wired into the rebuilt manifest.
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default()))
    .open()?;

    for i in 0..100 {
        assert_eq!(
            tree.get(key(i), MAX_SEQNO)?.as_deref(),
            Some(big(i).as_bytes()),
            "blob-backed value for key {} must survive repair",
            key(i),
        );
    }

    Ok(())
}

/// Returns the SST file paths under `<dir>/tables/`, sorted by id.
fn sorted_sst_paths(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut v: Vec<std::path::PathBuf> = std::fs::read_dir(dir.join("tables"))
        .expect("tables dir exists")
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .is_some_and(|n| n.to_string_lossy().parse::<u64>().is_ok())
        })
        .collect();
    v.sort();
    v
}

/// Flips a byte a quarter into the file, which lands in the data-block region
/// (data is written first; index / filter / meta live at the tail), so the SST
/// still opens but one data block fails its checksum.
fn corrupt_data_region(path: &std::path::Path) -> std::io::Result<()> {
    let mut bytes = std::fs::read(path)?;
    let at = bytes.len() / 4;
    if let Some(b) = bytes.get_mut(at) {
        *b ^= 0xFF;
    }
    std::fs::write(path, &bytes)
}

/// `repair_with_salvage` block-salvages an SST whose data is corrupt (whole-file
/// recovery succeeds because the data section is read lazily, but a block fails
/// verification): the corrupt block is dropped and the rest is recovered,
/// instead of leaving a table that errors on read.
#[test]
fn repair_with_salvage_recovers_a_block_corrupt_sst() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    {
        let tree = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;
        for i in 0..500 {
            tree.insert(key(i), format!("v-{i}"), i);
        }
        tree.flush_active_memtable(0)?;
        for i in 500..1000 {
            tree.insert(key(i), format!("v-{i}"), i);
        }
        tree.flush_active_memtable(0)?;
    }

    let ssts = sorted_sst_paths(dir.path());
    assert_eq!(ssts.len(), 2, "two flushes produce two SSTs");
    let victim = ssts.first().expect("an SST to corrupt");
    corrupt_data_region(victim)?;

    nuke_manifest(dir.path())?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair_with_salvage(true)?;
    assert_eq!(
        report.salvaged, 1,
        "the block-corrupt SST is salvaged, not dropped: {:?}",
        report.unreadable_files,
    );
    assert_eq!(
        report.recovered, 2,
        "both tables are referenced by the rebuilt manifest",
    );

    // The tree reopens and every read succeeds: the corrupt block was dropped,
    // so its keys read as absent rather than erroring. Most keys survive (the
    // intact SST in full, plus every block of the corrupt SST but the dropped
    // one).
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    let mut present = 0u64;
    for i in 0..1000 {
        if tree.get(key(i), MAX_SEQNO)?.is_some() {
            present += 1;
        }
    }
    assert!(present > 0, "data was recovered");
    assert!(
        present < 1000,
        "the corrupt block's keys are dropped, got {present}/1000",
    );
    Ok(())
}

/// An SST whose container (SFA trailer) is corrupt cannot be opened even in
/// salvage mode, so repair reports it unreadable rather than salvaging it.
#[test]
fn repair_with_salvage_reports_an_unopenable_sst_as_unreadable() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    {
        let tree = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;
        for i in 0..200 {
            tree.insert(key(i), format!("v-{i}"), i);
        }
        tree.flush_active_memtable(0)?;
    }

    let ssts = sorted_sst_paths(dir.path());
    let victim = ssts.first().expect("an SST to corrupt");
    // Truncate away the tail (SFA trailer + section mirrors): the container is
    // unparseable, so even salvage-mode recovery cannot open it.
    let mut bytes = std::fs::read(victim)?;
    bytes.truncate(bytes.len() / 2);
    std::fs::write(victim, &bytes)?;

    nuke_manifest(dir.path())?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair_with_salvage(true)?;
    assert_eq!(report.salvaged, 0, "an unopenable SST cannot be salvaged");
    assert_eq!(
        report.recovered, 0,
        "nothing is recovered from the only (corrupt) SST",
    );
    assert_eq!(
        report.unreadable, 1,
        "the SST is reported unreadable: {:?}",
        report.unreadable_files,
    );
    Ok(())
}

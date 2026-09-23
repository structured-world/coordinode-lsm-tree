// A level may hold any number of runs: nothing in the engine bounds L0, and
// flushes that outpace compaction build it up. The manifest has to persist
// such a level exactly, or a tree that was written without error does not
// reopen.

mod common;

use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};

/// More runs than a byte counts, so a snapshot that stored a level's run count
/// in a `u8` would record 44 of them and lose its place in the section.
const RUNS: usize = 300;

#[test]
fn a_level_with_more_than_255_runs_reopens_with_every_run() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    // A zero rotation threshold asks for a full snapshot on every upgrade, so
    // the reopen below depends on what the manifest wrote once the level
    // passed 255 runs.
    let open = || {
        Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .manifest_log_rotate_bytes(0)
        .open()
    };

    {
        let tree = open()?;
        for i in 0..RUNS {
            let key = format!("k{i:05}");
            let seqno = 2 * u64::try_from(i).expect("a run index fits u64");
            // Every flush also rewrites `zzz`, so each table's key range
            // overlaps every other's and L0 cannot fold two of them into one
            // run: disjoint tables share a run.
            tree.insert(key.as_bytes(), key.as_bytes(), seqno + 1);
            tree.insert(b"zzz", key.as_bytes(), seqno + 2);
            tree.flush_active_memtable(seqno + 2)?;
        }
        assert_eq!(tree.l0_run_count(), RUNS, "every flush is its own L0 run");
    }

    let tree = open()?;
    assert_eq!(
        tree.l0_run_count(),
        RUNS,
        "the reopened tree must carry every run the manifest recorded",
    );
    for i in 0..RUNS {
        let key = format!("k{i:05}");
        assert_eq!(
            tree.get(key.as_bytes(), lsm_tree::MAX_SEQNO)?.as_deref(),
            Some(key.as_bytes()),
            "{key} must read back after reopen",
        );
    }
    // The newest run must still shadow the older ones, so run order survives
    // as well as run membership.
    let newest = format!("k{:05}", RUNS - 1);
    assert_eq!(
        tree.get(b"zzz", lsm_tree::MAX_SEQNO)?.as_deref(),
        Some(newest.as_bytes()),
    );
    Ok(())
}

/// While a level is too wide for a snapshot the edit log carries it; once
/// compaction narrows the level, a rotation must leave the snapshot holding
/// the whole version and the log empty again.
#[test]
fn rotation_resumes_once_compaction_narrows_the_level() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .manifest_log_rotate_bytes(0)
    .open()?;

    for i in 0..RUNS {
        let key = format!("k{i:05}");
        let seqno = 2 * u64::try_from(i).expect("a run index fits u64");
        tree.insert(key.as_bytes(), key.as_bytes(), seqno + 1);
        tree.insert(b"zzz", key.as_bytes(), seqno + 2);
        tree.flush_active_memtable(seqno + 2)?;
    }
    assert!(
        edit_log_bytes(dir.path())? > 0,
        "a level wider than a snapshot must be carried by the edit log",
    );

    tree.major_compact(u64::MAX, 0)?;
    assert_eq!(
        tree.l0_run_count(),
        0,
        "compaction moved every run out of L0"
    );
    assert_eq!(
        edit_log_bytes(dir.path())?,
        0,
        "the upgrade after compaction must rotate to a fresh snapshot",
    );
    Ok(())
}

/// A checkpoint writes its manifest from the captured version, so a level
/// wider than a snapshot has to reach the checkpoint through its edit log.
#[test]
fn a_checkpoint_of_a_level_with_more_than_255_runs_opens_with_every_run() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    for i in 0..RUNS {
        let key = format!("k{i:05}");
        let seqno = 2 * u64::try_from(i).expect("a run index fits u64");
        tree.insert(key.as_bytes(), key.as_bytes(), seqno + 1);
        tree.insert(b"zzz", key.as_bytes(), seqno + 2);
        tree.flush_active_memtable(seqno + 2)?;
    }

    let checkpoint = dir.path().join("checkpoint");
    tree.create_checkpoint(&checkpoint)?;
    let copy = Config::new(
        &checkpoint,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    assert_eq!(copy.l0_run_count(), RUNS);
    for i in 0..RUNS {
        let key = format!("k{i:05}");
        assert_eq!(
            copy.get(key.as_bytes(), lsm_tree::MAX_SEQNO)?.as_deref(),
            Some(key.as_bytes()),
            "{key} must read back from the checkpoint",
        );
    }
    let newest = format!("k{:05}", RUNS - 1);
    assert_eq!(
        copy.get(b"zzz", lsm_tree::MAX_SEQNO)?.as_deref(),
        Some(newest.as_bytes()),
    );
    Ok(())
}

/// More tables than one edit-log record can describe: an edit carries every
/// table of each level it changes, and an appended record holds at most 64 KiB.
const TABLES: usize = 2_000;

/// Writes `TABLES` keys and compacts them into a table each. A value as large
/// as a data block ends a block per key, and a one-byte table target then ends
/// a table per block, so one compaction installs the whole level in one edit.
fn fill_one_table_per_key(tree: &impl AbstractTree) -> lsm_tree::Result<Vec<u8>> {
    let value = vec![7u8; 4096];
    for i in 0..TABLES {
        let key = format!("k{i:05}");
        let seqno = u64::try_from(i).expect("a table index fits u64") + 1;
        tree.insert(key.as_bytes(), &value, seqno);
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(1, 0)?;
    assert_eq!(tree.table_count(), TABLES, "every key became a table");
    Ok(value)
}

#[test]
fn a_level_too_large_for_one_edit_record_compacts_and_reopens() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let open = || {
        Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()
    };

    let value = fill_one_table_per_key(&open()?)?;

    let tree = open()?;
    assert_eq!(tree.table_count(), TABLES);
    for i in 0..TABLES {
        let key = format!("k{i:05}");
        assert_eq!(
            tree.get(key.as_bytes(), lsm_tree::MAX_SEQNO)?.as_deref(),
            Some(value.as_slice()),
            "{key} must read back after reopen",
        );
    }
    Ok(())
}

/// Repair makes every recovered table a run of its own in L0, so a tree of
/// more tables than a snapshot counts runs is rebuilt into a level only the
/// edit log can carry, and one of more than an appended record describes.
/// The rebuilt tree has to open, and keep flushing on top of that level.
#[test]
fn repair_of_more_tables_than_a_snapshot_counts_runs_opens_and_flushes() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let open = || {
        Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()
    };

    let value = fill_one_table_per_key(&open()?)?;
    common::nuke_manifest(dir.path())?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair()?;
    assert_eq!(report.recovered, TABLES);

    {
        let tree = open()?;
        assert_eq!(tree.l0_run_count(), TABLES, "repair made a run per table");
        tree.insert(b"zzz", b"after repair", 1_000_000);
        tree.flush_active_memtable(0)?;
    }

    let tree = open()?;
    assert_eq!(tree.table_count(), TABLES + 1);
    for i in 0..TABLES {
        let key = format!("k{i:05}");
        assert_eq!(
            tree.get(key.as_bytes(), lsm_tree::MAX_SEQNO)?.as_deref(),
            Some(value.as_slice()),
            "{key} must read back after repair",
        );
    }
    assert_eq!(
        tree.get(b"zzz", lsm_tree::MAX_SEQNO)?.as_deref(),
        Some(&b"after repair"[..]),
    );
    Ok(())
}

/// Bytes held by every edit log in the tree folder.
fn edit_log_bytes(folder: &std::path::Path) -> lsm_tree::Result<u64> {
    let mut total = 0;
    for entry in std::fs::read_dir(folder)? {
        let entry = entry?;
        if entry.file_name().to_string_lossy().starts_with("edits-") {
            total += entry.metadata()?.len();
        }
    }
    Ok(total)
}

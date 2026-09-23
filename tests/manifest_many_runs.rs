// A level may hold any number of runs: nothing in the engine bounds L0, and
// flushes that outpace compaction build it up. The manifest has to persist
// such a level exactly, or a tree that was written without error does not
// reopen.

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

/// While a level is too wide for a snapshot the manifest grows its edit log;
/// once compaction narrows the level, the next upgrade must rotate again, or
/// the log would grow for the life of the tree.
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

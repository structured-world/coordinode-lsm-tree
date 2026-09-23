// A level may hold any number of runs: nothing in the engine bounds L0, and
// flushes that outpace compaction build it up. The manifest snapshot has to
// persist such a level exactly, or a tree that was written without error does
// not reopen.

use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};

/// More runs than a byte counts, so a snapshot that stored a level's run count
/// in a `u8` would record 44 of them and lose its place in the section.
const RUNS: u64 = 300;

#[test]
fn a_level_with_more_than_255_runs_reopens_with_every_run() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    // Rotating on every upgrade makes each flush write a full snapshot, so the
    // reopen below reads the level from the snapshot rather than replaying it
    // from the edit log.
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
            // Every flush also rewrites `zzz`, so each table's key range
            // overlaps every other's and L0 cannot fold two of them into one
            // run: disjoint tables share a run.
            tree.insert(key.as_bytes(), key.as_bytes(), 2 * i + 1);
            tree.insert(b"zzz", key.as_bytes(), 2 * i + 2);
            tree.flush_active_memtable(2 * i + 2)?;
        }
        assert_eq!(
            tree.l0_run_count(),
            RUNS as usize,
            "every flush is its own L0 run"
        );
    }

    let tree = open()?;
    assert_eq!(
        tree.l0_run_count(),
        RUNS as usize,
        "the reopened tree must carry every run the snapshot recorded",
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

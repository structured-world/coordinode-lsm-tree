//! The L0 run count a writer paces itself by follows every version: each
//! flush adds a run and a compaction into L1 takes them away.

use lsm_tree::{AbstractTree, Config, SeqNo, SequenceNumberCounter, get_tmp_folder};

#[test]
fn the_l0_run_count_follows_flushes_and_compaction() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    assert_eq!(tree.l0_run_count(), 0);
    // Each flush spans the same keys, so it overlaps the runs before it and
    // lands as a run of its own.
    for run in 1..=3_u64 {
        tree.insert("a", b"value", run * 2);
        tree.insert("z", b"value", run * 2 + 1);
        tree.flush_active_memtable(0)?;
        assert_eq!(tree.l0_run_count(), run as usize, "after flush {run}");
    }
    tree.major_compact(u64::MAX, SeqNo::MAX)?;
    assert_eq!(tree.l0_run_count(), 0, "after the compaction");
    Ok(())
}

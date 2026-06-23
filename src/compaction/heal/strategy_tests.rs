use crate::{AbstractTree, AnyTree, Config, GlobalTableId, SequenceNumberCounter};
use alloc::sync::Arc;

/// A queued id that is no longer present in the tree (already compacted away
/// since the hint) is dropped: the strategy drains it and chooses nothing,
/// rather than emitting a merge for a missing table.
#[test]
fn ecc_heal_drops_ids_no_longer_in_the_tree() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let AnyTree::Standard(tree) = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?
    else {
        unreachable!("standard tree configured (no kv separation)");
    };
    tree.insert("k", "v", 1);
    tree.flush_active_memtable(1)?;

    // Flag an SST id that does not exist in this tree.
    let hints = tree.heal_hints();
    hints.record(GlobalTableId::from((tree.id(), 999_999)));
    assert!(!hints.is_empty());

    let result = tree.compact(Arc::new(super::Strategy::new(hints.clone(), u64::MAX)), 0)?;
    assert!(
        hints.is_empty(),
        "a stale id must be drained, not left queued; got {result:?}",
    );
    Ok(())
}

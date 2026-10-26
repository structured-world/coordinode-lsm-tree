//! A rebuild clears a tree, reinstalls a source's history at its original
//! seqnos, then lowers the retention floor to the source's: the reinstalled
//! history is served above the lowered floor and refused at or below it, live
//! and after a reopen, while the reinstall is in progress nothing below the
//! clear is served, and a request that would raise the floor is refused
//! without changing it.

use lsm_tree::{
    AbstractTree, AnyTree, Config, Error, SeqNo, SequenceNumberCounter, get_tmp_folder,
};
use test_log::test;

/// The floor the source of the history held.
const SOURCE_FLOOR: SeqNo = 5;
/// The seqnos the reinstalled history carries, both above the source floor.
const FIRST: SeqNo = 10;
const SECOND: SeqNo = 20;
/// Where the tree's own counter stands when it is cleared, above the history.
const COUNTER: SeqNo = 100;

fn open(folder: &std::path::Path, kv_separated: bool, next: SeqNo) -> lsm_tree::Result<AnyTree> {
    let mut config = Config::new(
        folder,
        SequenceNumberCounter::new(next),
        SequenceNumberCounter::default(),
    );
    if kv_separated {
        config = config.with_kv_separation(Some(Default::default()));
    }
    config.open()
}

fn assert_below_retention(result: lsm_tree::Result<Option<lsm_tree::UserValue>>, floor: SeqNo) {
    match result {
        Err(Error::SnapshotBelowRetention {
            oldest_retained, ..
        }) => assert_eq!(oldest_retained, floor),
        other => panic!("expected SnapshotBelowRetention at {floor}, got {other:?}"),
    }
}

/// Clears a tree holding unrelated data, then reinstalls `"k"` at `FIRST` and
/// `SECOND`, as a rebuild from a source would. Returns the floor the clear set.
fn cleared_and_reinstalled(tree: &AnyTree) -> lsm_tree::Result<SeqNo> {
    tree.insert("stale", "x", COUNTER - 1);
    tree.flush_active_memtable(0)?;
    tree.clear()?;
    let cleared = tree.retention_floor();
    assert!(
        cleared >= COUNTER,
        "the clear sets the floor at its own seqno"
    );
    tree.insert("k", "v1", FIRST);
    tree.insert("k", "v2", SECOND);
    Ok(cleared)
}

fn assert_lowered_floor_serves_the_reinstalled_history(kv_separated: bool) -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), kv_separated, COUNTER)?;
    let cleared = cleared_and_reinstalled(&tree)?;

    // Mid-rebuild, the history is refused rather than served partial.
    assert_below_retention(tree.get("k", FIRST + 1), cleared);

    tree.lower_retention_floor(SOURCE_FLOOR)?;

    assert_eq!(tree.retention_floor(), SOURCE_FLOOR);
    assert_eq!(tree.oldest_retained_seqno(), SOURCE_FLOOR);
    assert_below_retention(tree.get("k", SOURCE_FLOOR), SOURCE_FLOOR);
    assert_eq!(tree.get("k", FIRST + 1)?.as_deref(), Some(b"v1".as_slice()));
    assert_eq!(
        tree.get("k", SECOND + 1)?.as_deref(),
        Some(b"v2".as_slice())
    );

    tree.flush_active_memtable(0)?;
    drop(tree);
    let tree = open(folder.path(), kv_separated, COUNTER + 100)?;
    assert_eq!(
        tree.retention_floor(),
        SOURCE_FLOOR,
        "the lowered floor survives a reopen"
    );
    assert_below_retention(tree.get("k", SOURCE_FLOOR), SOURCE_FLOOR);
    assert_eq!(tree.get("k", FIRST + 1)?.as_deref(), Some(b"v1".as_slice()));
    assert_eq!(
        tree.get("k", SECOND + 1)?.as_deref(),
        Some(b"v2".as_slice())
    );
    Ok(())
}

#[test]
fn lower_retention_floor_after_reinstall_serves_history_above_it() -> lsm_tree::Result<()> {
    assert_lowered_floor_serves_the_reinstalled_history(false)
}

#[test]
fn lower_retention_floor_on_blob_tree_serves_history_above_it() -> lsm_tree::Result<()> {
    assert_lowered_floor_serves_the_reinstalled_history(true)
}

fn assert_raise_is_refused(kv_separated: bool) -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), kv_separated, COUNTER)?;
    let cleared = cleared_and_reinstalled(&tree)?;

    match tree.lower_retention_floor(cleared + 1) {
        Err(Error::RetentionFloorRaise { current, requested }) => {
            assert_eq!((current, requested), (cleared, cleared + 1));
        }
        other => panic!("expected RetentionFloorRaise, got {other:?}"),
    }
    assert_eq!(tree.retention_floor(), cleared, "a refusal changes nothing");

    // The floor as it stands is no raise: nothing to do.
    tree.lower_retention_floor(cleared)?;
    assert_eq!(tree.retention_floor(), cleared);

    drop(tree);
    let tree = open(folder.path(), kv_separated, COUNTER + 100)?;
    assert_eq!(
        tree.retention_floor(),
        cleared,
        "nor does it after a reopen"
    );
    Ok(())
}

#[test]
fn lower_retention_floor_above_the_current_is_refused() -> lsm_tree::Result<()> {
    assert_raise_is_refused(false)
}

#[test]
fn lower_retention_floor_above_the_current_on_blob_tree_is_refused() -> lsm_tree::Result<()> {
    assert_raise_is_refused(true)
}

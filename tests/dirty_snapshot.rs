use lsm_tree::{AbstractTree, Config, Guard, SequenceNumberCounter, get_tmp_folder};
use test_log::test;

#[test]
fn reclaimed_range_refuses_older_snapshots_but_not_an_open_reader() -> lsm_tree::Result<()> {
    // A table a range tombstone reclaimed is gone at every snapshot up to the
    // tombstone, so a new read at an older snapshot is refused rather than
    // answered without it. A reader that resolved its version before the drop
    // keeps reading it.
    let dir = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();

    let tree = Config::new(dir.path(), seqno.clone(), SequenceNumberCounter::default()).open()?;

    tree.insert("a", "a", seqno.next());
    tree.flush_active_memtable(0)?;
    tree.insert("b", "b", seqno.next());
    tree.flush_active_memtable(0)?;

    let snapshot_seqno = seqno.get();
    assert_eq!(b"a", &*tree.get("a", snapshot_seqno)?.unwrap());
    let mut reader = tree.iter(snapshot_seqno, None);

    let tombstone = seqno.next();
    tree.remove_range("a", "b", tombstone);
    tree.flush_active_memtable(0)?;
    tree.compact(
        std::sync::Arc::new(lsm_tree::compaction::Fifo::new(u64::MAX, None)),
        tombstone + 1,
    )?;
    assert_eq!(2, tree.table_count(), "the table of a is dropped");
    assert!(matches!(
        tree.get("a", snapshot_seqno),
        Err(lsm_tree::Error::SnapshotBelowRetention { .. })
    ));

    let (key, _) = reader
        .next()
        .expect("the reader's first row")
        .into_inner()?;
    assert_eq!(&*key, b"a", "the open reader still sees the dropped row");

    Ok(())
}

use lsm_tree::fs::MemFs;
// Guard trait import required for IterGuardImpl::into_inner() method dispatch.
use lsm_tree::{AbstractTree, Config, Guard, SeqNo, SequenceNumberCounter};
use test_log::test;

#[test]
fn open_tree_with_memfs() -> lsm_tree::Result<()> {
    let tree = Config::new(
        "/virtual/tree",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(MemFs::new())
    .open()?;

    assert!(tree.is_empty(SeqNo::MAX, None)?);

    tree.insert("key1", "value1", 0);
    tree.insert("key2", "value2", 1);
    tree.insert("key3", "value3", 2);

    assert_eq!(tree.len(SeqNo::MAX, None)?, 3);

    let val = tree.get("key2", SeqNo::MAX)?.expect("key2 should exist");
    assert_eq!(&*val, b"value2");

    Ok(())
}

#[test]
fn memfs_tree_flush_and_read() -> lsm_tree::Result<()> {
    let tree = Config::new(
        "/virtual/flush",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(MemFs::new())
    .open()?;

    for i in 0u64..100 {
        tree.insert(format!("key_{i:05}"), format!("val_{i}"), i);
    }

    // Flush memtable to SST (in-memory via MemFs)
    tree.flush_active_memtable(0)?;

    // Reads should still work after flush (from SST)
    let val = tree
        .get("key_00050", SeqNo::MAX)?
        .expect("key should exist after flush");
    assert_eq!(&*val, b"val_50");

    assert_eq!(tree.len(SeqNo::MAX, None)?, 100);

    Ok(())
}

#[test]
fn memfs_tree_delete_and_range() -> lsm_tree::Result<()> {
    let tree = Config::new(
        "/virtual/range",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(MemFs::new())
    .open()?;

    tree.insert("a", "1", 0);
    tree.insert("b", "2", 1);
    tree.insert("c", "3", 2);
    tree.remove("b", 3);

    let items: Vec<_> = tree
        .iter(SeqNo::MAX, None)
        .map(|guard| {
            let (k, v) = guard.into_inner().unwrap();
            (
                String::from_utf8(k.to_vec()).unwrap(),
                String::from_utf8(v.to_vec()).unwrap(),
            )
        })
        .collect();

    assert_eq!(
        items,
        vec![("a".into(), "1".into()), ("c".into(), "3".into())]
    );

    Ok(())
}

#[test]
fn memfs_tree_multiple_flushes() -> lsm_tree::Result<()> {
    let tree = Config::new(
        "/virtual/multi_flush",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(MemFs::new())
    .open()?;

    // First batch
    for i in 0u64..50 {
        tree.insert(format!("key_{i:05}"), format!("batch1_{i}"), i);
    }
    tree.flush_active_memtable(0)?;

    // Second batch — some overwrites, some new
    for i in 25u64..75 {
        tree.insert(format!("key_{i:05}"), format!("batch2_{i}"), 50 + i);
    }
    tree.flush_active_memtable(0)?;

    // Verify latest values
    let val = tree
        .get("key_00030", SeqNo::MAX)?
        .expect("overwritten key should exist");
    assert_eq!(&*val, b"batch2_30");

    let val = tree
        .get("key_00010", SeqNo::MAX)?
        .expect("original key should exist");
    assert_eq!(&*val, b"batch1_10");

    assert_eq!(tree.len(SeqNo::MAX, None)?, 75);

    Ok(())
}

#[test]
fn memfs_shared_across_trees() -> lsm_tree::Result<()> {
    let fs = MemFs::new();

    let tree1 = Config::new(
        "/virtual/tree1",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fs.clone())
    .open()?;

    let tree2 = Config::new(
        "/virtual/tree2",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fs)
    .open()?;

    tree1.insert("from_tree1", "hello", 0);
    tree2.insert("from_tree2", "world", 0);

    assert!(tree1.get("from_tree1", SeqNo::MAX)?.is_some());
    assert!(tree1.get("from_tree2", SeqNo::MAX)?.is_none());
    assert!(tree2.get("from_tree2", SeqNo::MAX)?.is_some());
    assert!(tree2.get("from_tree1", SeqNo::MAX)?.is_none());

    Ok(())
}

// NOTE: Compaction is not yet fully supported with MemFs.
// There are remaining `std::fs` bypass points in the compaction
// finalization path that produce ENOENT when running fully in-memory.
// Tracked as a known limitation — see mem_fs.rs module docs.

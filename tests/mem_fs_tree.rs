use lsm_tree::fs::MemFs;
// Guard trait import required for IterGuardImpl::into_inner() method dispatch.
use lsm_tree::{AbstractTree, Config, Guard, SeqNo, SequenceNumberCounter};
use test_log::test;

/// Returns a unique virtual path for each test to avoid host-path collisions.
/// Using `tempfile::tempdir` ensures no leftover on-disk state can interfere.
fn test_path(name: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join(name);
    (dir, path)
}

#[test]
fn open_tree_with_memfs() -> lsm_tree::Result<()> {
    let (_dir, path) = test_path("tree");
    let tree = Config::new(
        &path,
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
    let (_dir, path) = test_path("flush");
    let tree = Config::new(
        &path,
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
    let (_dir, path) = test_path("range");
    let tree = Config::new(
        &path,
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
        .map(|guard| guard.into_inner())
        .collect::<lsm_tree::Result<Vec<_>>>()?;

    assert_eq!(items.len(), 2);
    assert_eq!(&*items[0].0, b"a");
    assert_eq!(&*items[0].1, b"1");
    assert_eq!(&*items[1].0, b"c");
    assert_eq!(&*items[1].1, b"3");

    Ok(())
}

#[test]
fn memfs_tree_multiple_flushes() -> lsm_tree::Result<()> {
    let (_dir, path) = test_path("multi_flush");
    let tree = Config::new(
        &path,
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
    use std::sync::Arc;

    let (_dir1, path1) = test_path("shared_tree1");
    let (_dir2, path2) = test_path("shared_tree2");

    // Exercise Config::with_shared_fs(Arc<dyn Fs>) for shared backend reuse.
    let fs: Arc<dyn lsm_tree::fs::Fs> = Arc::new(MemFs::new());

    let tree1 = Config::new(
        &path1,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::clone(&fs))
    .open()?;

    let tree2 = Config::new(
        &path2,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::clone(&fs))
    .open()?;

    tree1.insert("from_tree1", "hello", 0);
    tree2.insert("from_tree2", "world", 0);

    // Flush to backend so assertions hit the shared Fs, not just
    // each tree's private memtable.
    tree1.flush_active_memtable(0)?;
    tree2.flush_active_memtable(0)?;

    // Verify data isolation between trees on the shared backend.
    assert!(tree1.get("from_tree1", SeqNo::MAX)?.is_some());
    assert!(tree1.get("from_tree2", SeqNo::MAX)?.is_none());
    assert!(tree2.get("from_tree2", SeqNo::MAX)?.is_some());
    assert!(tree2.get("from_tree1", SeqNo::MAX)?.is_none());

    // Prove the shared MemFs was actually used: tables directory exists
    // in the virtual filesystem, not on the real host disk.
    let tables1 = path1.join("tables");
    let tables2 = path2.join("tables");
    assert!(
        fs.exists(&tables1)?,
        "tables dir should exist in MemFs after flush"
    );
    assert!(
        fs.exists(&tables2)?,
        "tables dir should exist in MemFs after flush"
    );
    assert!(
        !tables1.exists(),
        "tables dir should NOT exist on host disk"
    );
    assert!(
        !tables2.exists(),
        "tables dir should NOT exist on host disk"
    );

    Ok(())
}

/// Verifies the full MemFs round-trip: create tree → write → flush → drop → reopen → read.
///
/// This is the core scenario unlocked by #209 (migrating Tree::open recovery
/// path to the Fs trait). Before that change, `Tree::open` used `std::fs`
/// directly for version detection and recovery, so MemFs trees could not be
/// reopened after drop.
#[test]
fn memfs_reopen_recovers_flushed_data() -> lsm_tree::Result<()> {
    use std::sync::Arc;

    let (_dir, path) = test_path("reopen");
    let fs: Arc<dyn lsm_tree::fs::Fs> = Arc::new(MemFs::new());

    // Phase 1: create, write, flush, drop
    {
        let tree = Config::new(
            &path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(Arc::clone(&fs))
        .open()?;

        tree.insert("alpha", "one", 0);
        tree.insert("beta", "two", 1);
        tree.insert("gamma", "three", 2);

        tree.flush_active_memtable(0)?;

        assert_eq!(tree.len(SeqNo::MAX, None)?, 3);
    } // tree dropped here — MemFs retains all data

    // Phase 2: reopen from the same MemFs and verify data survived
    {
        let tree = Config::new(
            &path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(Arc::clone(&fs))
        .open()?;

        assert_eq!(tree.len(SeqNo::MAX, None)?, 3);

        let val = tree
            .get("alpha", SeqNo::MAX)?
            .expect("alpha should survive reopen");
        assert_eq!(&*val, b"one");

        let val = tree
            .get("beta", SeqNo::MAX)?
            .expect("beta should survive reopen");
        assert_eq!(&*val, b"two");

        let val = tree
            .get("gamma", SeqNo::MAX)?
            .expect("gamma should survive reopen");
        assert_eq!(&*val, b"three");
    }

    Ok(())
}

// NOTE: Compaction is not yet fully supported with MemFs.
// There are remaining `std::fs` bypass points in the compaction
// finalization path that produce ENOENT when running fully in-memory.
// Tracked as a known limitation — see mem_fs.rs module docs.

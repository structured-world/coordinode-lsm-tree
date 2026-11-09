use lsm_tree::{
    AbstractTree, KvSeparationOptions, SeqNo, SequenceNumberCounter, blob_tree::FragmentationEntry,
    get_tmp_folder,
};
use std::sync::Arc;
use test_log::test;

mod common;

/// Deletes every key of these trees with a range tombstone at `seqno`, flushes
/// it and drops the tables it spans in a compaction just above it.
fn reclaim_all(tree: &lsm_tree::AnyTree, seqno: SeqNo) -> lsm_tree::Result<()> {
    tree.remove_range("a", "z", seqno);
    tree.flush_active_memtable(0)?;
    tree.compact(
        Arc::new(lsm_tree::compaction::Fifo::new(u64::MAX, None)),
        seqno + 1,
    )?;
    Ok(())
}

#[test]
fn blob_tree_nuke_gc_stats() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);

    {
        let tree = lsm_tree::Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(KvSeparationOptions::default()))
        .blob_compression(lsm_tree::CompressionType::None)
        .open()?;

        assert!(tree.get("big", SeqNo::MAX)?.is_none());
        tree.insert("big", &big_value, 0);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        reclaim_all(&tree, 1)?;

        // NOTE: Because the blob does not have any incoming references anymore
        // it is pruned from the Version
        assert_eq!(0, tree.blob_file_count());
        assert_eq!(1, tree.table_count(), "only the tombstone's table stays");

        let gc_stats = tree.current_version().gc_stats().clone();

        // "big":0 was dropped
        assert_eq!(
            &{
                let mut map = lsm_tree::HashMap::default();
                let size = big_value.len() as u64;
                let stored = common::stored_len(b"big", size);
                map.insert(0, FragmentationEntry::new(1, size, stored));
                map
            },
            &*gc_stats,
        );
    }

    Ok(())
}

/// Two tables linking one blob file, dropped by one edit, charge the file for
/// what both own on disk, not for the first table's share alone.
#[test]
fn dropping_two_tables_of_one_blob_file_charges_both_on_disk() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let big_value = b"neptune!".repeat(128_000);

    let tree = lsm_tree::Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    // No file is old enough to relocate, so a compaction only passes the
    // values' references through.
    .with_kv_separation(Some(KvSeparationOptions::default().age_cutoff(0.0)))
    .blob_compression(lsm_tree::CompressionType::None)
    .open()?;

    // Keys long enough to fill a data block each, so a table closes after
    // every key once it is past its target.
    let (a, b) = (b"a".repeat(8_192), b"b".repeat(8_192));
    tree.insert(&a, &big_value, 0);
    tree.insert(&b, &big_value, 0);
    tree.flush_active_memtable(0)?;
    assert_eq!(1, tree.blob_file_count());
    // Splits the one table in two, each linking the same blob file.
    tree.major_compact(1, 1_000)?;
    assert_eq!(2, tree.table_count());
    assert_eq!(1, tree.blob_file_count());

    tree.remove_range("a", "z", 1);
    tree.flush_active_memtable(0)?;
    tree.compact(Arc::new(lsm_tree::compaction::Fifo::new(u64::MAX, None)), 2)?;

    let size = big_value.len() as u64;
    let stored = common::stored_len(&a, size) + common::stored_len(&b, size);
    assert_eq!(
        &{
            let mut map = lsm_tree::HashMap::default();
            map.insert(0, FragmentationEntry::new(2, size * 2, stored));
            map
        },
        &*tree.current_version().gc_stats().clone(),
    );
    Ok(())
}

#[test]
fn blob_tree_nuke_gc_stats_multi() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);

    {
        let tree = lsm_tree::Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(KvSeparationOptions::default()))
        .blob_compression(lsm_tree::CompressionType::None)
        .open()?;

        assert!(tree.get("big", SeqNo::MAX)?.is_none());
        tree.insert("big", &big_value, 0);
        tree.insert("big", &big_value, 1);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        reclaim_all(&tree, 2)?;

        // NOTE: Because the blob does not have any incoming references anymore
        // it is pruned from the Version
        assert_eq!(0, tree.blob_file_count());
        assert_eq!(1, tree.table_count(), "only the tombstone's table stays");

        let gc_stats = tree.current_version().gc_stats().clone();

        // "big":0, big":1 were dropped
        assert_eq!(
            &{
                let mut map = lsm_tree::HashMap::default();
                let size = big_value.len() as u64;
                let stored = common::stored_len(b"big", size);
                map.insert(0, FragmentationEntry::new(2, size * 2, stored * 2));
                map
            },
            &*gc_stats,
        );
    }

    Ok(())
}

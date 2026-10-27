use lsm_tree::{AbstractTree, KvSeparationOptions, SeqNo, SequenceNumberCounter, get_tmp_folder};
use test_log::test;

#[test]
fn blob_tree_major_compact_drop_dead_files() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);
    let new_big_value = b"winter!".repeat(128_000);

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
        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        tree.insert("big", &big_value, 1);
        tree.flush_active_memtable(0)?;
        assert_eq!(2, tree.table_count());
        assert_eq!(2, tree.blob_file_count());

        tree.insert("big", &big_value, 2);
        tree.flush_active_memtable(0)?;
        assert_eq!(3, tree.table_count());
        assert_eq!(3, tree.blob_file_count());

        tree.insert("big", &big_value, 3);
        tree.flush_active_memtable(0)?;
        assert_eq!(4, tree.table_count());
        assert_eq!(4, tree.blob_file_count());

        tree.insert("big", &new_big_value, 4);
        tree.flush_active_memtable(0)?;
        assert_eq!(5, tree.table_count());
        assert_eq!(5, tree.blob_file_count());

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, new_big_value);

        {
            let gc_stats = tree.current_version().gc_stats().clone();

            assert_eq!(&lsm_tree::HashMap::default(), &*gc_stats);
        }

        // The compaction that drops the four superseded values charges their
        // files in full, and the same install drops them: no later compaction
        // is needed to reclaim them.
        tree.major_compact(64_000_000, 1_000)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        {
            let gc_stats = tree.current_version().gc_stats().clone();
            assert_eq!(&lsm_tree::HashMap::default(), &*gc_stats);
        }

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, new_big_value);

        // Nothing is left for another compaction to drop.
        tree.major_compact(64_000_000, 1_000)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());
    }

    Ok(())
}

// A caller that deletes every key of a blob file and runs one compaction past
// the watermark to return the space gets it back from that compaction: on a
// store that goes quiet afterwards no other compaction would ever drop the
// file.
#[test]
fn blob_tree_one_compaction_drops_a_file_whose_every_value_it_deleted() -> lsm_tree::Result<()> {
    const KEYS: u64 = 64;

    let folder = get_tmp_folder();
    let path = folder.path();
    let tree = lsm_tree::Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default()))
    .blob_compression(lsm_tree::CompressionType::None)
    .open()?;

    let value = b"payload!".repeat(4_096);
    for key in 0..KEYS {
        tree.insert(format!("k{key:03}"), &value, key);
    }
    tree.flush_active_memtable(0)?;
    assert_eq!(1, tree.blob_file_count());
    let on_disk = tree.disk_space();

    for key in 0..KEYS {
        tree.remove(format!("k{key:03}"), KEYS + key);
    }
    tree.flush_active_memtable(0)?;

    tree.major_compact(64_000_000, SeqNo::MAX)?;
    assert_eq!(0, tree.blob_file_count(), "the emptied blob file stays");
    assert!(
        tree.disk_space() < on_disk / 8,
        "the blob file's bytes are still counted: {} of {on_disk}",
        tree.disk_space()
    );

    // Gone from the folder too, once the tree is done with it.
    tree.close().wait();
    let blobs = std::fs::read_dir(path.join("blobs"))?.count();
    assert_eq!(0, blobs, "a blob file is left in the folder");
    Ok(())
}

use super::*;
use crate::fs::{Fs, FsOpenOptions, MemFs};
use std::io::Write;

#[test]
fn drains_queued_deletions_on_drop() {
    let fs: Arc<dyn Fs> = Arc::new(MemFs::default());
    let paths: Vec<PathBuf> = (0..16).map(|i| PathBuf::from(format!("/f{i}"))).collect();
    for p in &paths {
        let mut f = fs
            .open(p, &FsOpenOptions::new().write(true).create(true))
            .unwrap();
        f.write_all(b"data").unwrap();
        f.flush().unwrap();
        assert!(fs.open(p, &FsOpenOptions::new().read(true)).is_ok());
    }

    {
        let deleter = BackgroundDeleter::new(None);
        for p in &paths {
            deleter.enqueue(Arc::clone(&fs), p.clone());
        }
        // Drop drains the queue and joins the worker: every enqueued unlink
        // has completed by the time this scope ends.
    }

    for p in &paths {
        assert!(
            fs.open(p, &FsOpenOptions::new().read(true)).is_err(),
            "{} should have been unlinked by the background deleter",
            p.display(),
        );
    }
}

/// A deleter whose worker never started does its unlinks where they are
/// asked for, so no directory entry is left behind, and leaves a deleted
/// file's cache entries to the cache: the walk is not the caller's to pay.
#[test]
fn a_deleter_without_a_worker_unlinks_in_place_and_walks_no_cache() {
    let fs: Arc<dyn Fs> = Arc::new(MemFs::default());
    let path = PathBuf::from("/f");
    let mut f = fs
        .open(&path, &FsOpenOptions::new().write(true).create(true))
        .unwrap();
    f.write_all(b"data").unwrap();
    f.flush().unwrap();
    drop(f);

    let block = crate::table::Block {
        header: crate::table::block::Header {
            uncompressed_length: 64,
            ..crate::table::block::Header::test_dummy(crate::table::block::BlockType::Data)
        },
        data: crate::Slice::from(&[0u8; 64][..]),
    };
    let cache = Arc::new(crate::Cache::with_capacity_bytes(1 << 20));
    let (table, at) = (
        crate::table::GlobalTableId::from((5, 9)),
        crate::table::BlockOffset(0),
    );
    cache.insert_block(table, at, block);

    let deleter = BackgroundDeleter {
        sender: None,
        worker: None,
        life: None,
        owner: Some((Arc::clone(&cache), 5)),
    };
    deleter.enqueue(Arc::clone(&fs), path.clone());
    assert!(fs.open(&path, &FsOpenOptions::new().read(true)).is_err());
    // A file already gone is logged, not a failure of the caller.
    deleter.enqueue(Arc::clone(&fs), path);

    deleter.evict_table(9);
    deleter.evict_blob_file(3);
    assert!(cache.get_block(table, at).is_some(), "left to the cache");
}

/// A deleter with no cache to evict from takes an eviction for nothing, and
/// one with an interval waits it out after each unlink.
#[test]
fn a_deleter_without_a_cache_ignores_evictions_and_paces_its_unlinks() {
    let fs: Arc<dyn Fs> = Arc::new(MemFs::default());
    let path = PathBuf::from("/f");
    let mut f = fs
        .open(&path, &FsOpenOptions::new().write(true).create(true))
        .unwrap();
    f.write_all(b"data").unwrap();
    f.flush().unwrap();
    drop(f);
    {
        let deleter = BackgroundDeleter::new(Some(Duration::from_millis(1)));
        deleter.evict_table(9);
        deleter.evict_blob_file(3);
        deleter.enqueue(Arc::clone(&fs), path.clone());
    }
    assert!(fs.open(&path, &FsOpenOptions::new().read(true)).is_err());
}

/// The tables a compaction drops each queue an eviction and then an unlink.
/// The worker takes every eviction queued out in one walk over the cache,
/// with the unlinks between them still done in their order: stopping at the
/// first unlink, it walked the whole cache once per table.
#[cfg(feature = "metrics")]
#[test]
fn evictions_queued_between_unlinks_take_one_walk_of_the_cache() {
    let fs: Arc<dyn Fs> = Arc::new(MemFs::default());
    let paths: Vec<PathBuf> = (0..9).map(|i| PathBuf::from(format!("/f{i}"))).collect();
    for p in &paths {
        let mut f = fs
            .open(p, &FsOpenOptions::new().write(true).create(true))
            .unwrap();
        f.write_all(b"data").unwrap();
        f.flush().unwrap();
    }
    let cache = Arc::new(crate::Cache::with_capacity_bytes(1 << 20));

    // Every job queued before the worker's loop is run, here on this thread:
    // what it finds queued does not depend on how threads are scheduled.
    let (sender, receiver) = channel::<DeleteJob>();
    for (table, path) in paths.iter().enumerate() {
        sender
            .send(DeleteJob::Evict {
                cache: Arc::clone(&cache),
                dead: crate::cache::DeadFile::Table {
                    tree: 5,
                    table: table as u64,
                },
            })
            .unwrap();
        sender
            .send(DeleteJob::Unlink {
                fs: Arc::clone(&fs),
                path: path.clone(),
            })
            .unwrap();
    }
    drop(sender);
    BackgroundDeleter::run(&receiver, None);

    let (_, walks) = cache.dead_entries_dropped();
    assert_eq!(walks, 1, "nine tables' evictions");
    for p in &paths {
        assert!(fs.open(p, &FsOpenOptions::new().read(true)).is_err());
    }
}

/// A tree's deleter told that a table and a blob file were deleted takes
/// what they left in the tree's cache out of it, by the time its queue is
/// drained, and leaves the tree's other files' entries where they are. Without
/// it those entries stay until the cache evicts them by itself, holding
/// capacity no read can use.
#[test]
fn a_deleted_files_entries_leave_the_trees_cache() {
    use crate::table::block::BlockType;
    use crate::vlog::blob_file::frame::decoded_frame;

    let block = || crate::table::Block {
        header: crate::table::block::Header {
            uncompressed_length: 1_024,
            ..crate::table::block::Header::test_dummy(BlockType::Data)
        },
        data: crate::Slice::from(&[0u8; 1_024][..]),
    };
    let cache = Arc::new(crate::Cache::with_capacity_bytes(1 << 20));
    let at = crate::table::BlockOffset(0);
    let dead = crate::table::GlobalTableId::from((5, 9));
    let live = crate::table::GlobalTableId::from((5, 10));
    cache.insert_block(dead, at, block());
    cache.insert_block(live, at, block());
    let frame_at = |file| crate::cache::BlobFrameAt {
        blob_file_id: file,
        frame: 0,
    };
    for file in [3, 4] {
        cache.insert_blob_frame(5, frame_at(file), Arc::new(decoded_frame(&[(b"k", b"v")])));
    }

    {
        let deleter = BackgroundDeleter::spawn(None, None, Some((Arc::clone(&cache), 5)));
        deleter.evict_table(9);
        deleter.evict_blob_file(3);
    }

    assert!(cache.get_block(dead, at).is_none(), "the deleted table's");
    assert!(cache.get_block(live, at).is_some(), "a live table's");
    assert!(cache.get_blob_frame(5, frame_at(3)).is_none());
    assert!(cache.get_blob_frame(5, frame_at(4)).is_some());
}

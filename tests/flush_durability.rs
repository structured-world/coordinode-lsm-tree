// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Flushes whose durability waits for a barrier: what a deferred flush syncs,
//! what a power loss leaves of it before and after the barrier, and the
//! installs that persist it on their own.

use lsm_tree::config::FlushDurability;
use lsm_tree::fs::{CountingFs, CrashFs, Fs, MemFs};
use lsm_tree::{AbstractTree, AnyTree, Config, KvSeparationOptions, SequenceNumberCounter};
use std::path::Path;
use std::sync::Arc;
use test_log::test;

const DB: &str = "/db";

/// A tree at [`DB`] on `fs` whose flushes are deferred.
fn deferred(fs: Arc<dyn Fs>) -> Config {
    Config::new(
        DB,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(fs)
    .flush_durability(FlushDurability::Deferred)
}

/// The tree under `config`, failing the test when it does not open.
fn open(config: Config) -> AnyTree {
    config.open().expect("the tree opens")
}

/// The value of `key`, as a string.
fn value(tree: &AnyTree, key: &str) -> Option<String> {
    tree.get(key, u64::MAX)
        .expect("get")
        .map(|v| String::from_utf8(v.to_vec()).expect("utf-8 value"))
}

/// The files in `folder` on `fs` named like tables and holding data: an empty
/// one is a pooled file, kept for a table to come.
fn table_files(fs: &dyn Fs, folder: &Path) -> Vec<u64> {
    let mut ids: Vec<u64> = fs
        .read_dir(folder)
        .expect("tables folder")
        .into_iter()
        .filter_map(|entry| {
            let id = entry.file_name.parse::<u64>().ok()?;
            let len = fs.metadata(&folder.join(&entry.file_name)).ok()?.len;
            (len > 0).then_some(id)
        })
        .collect();
    ids.sort_unstable();
    ids
}

/// A deferred flush issues no sync at all, of a file, a folder or the
/// manifest; the barrier issues them. Guards against a writer, a folder sync
/// or a manifest edit left on the flush path, which would keep the cost the
/// mode exists to take away.
#[test]
fn a_deferred_flush_syncs_nothing_until_the_barrier() -> lsm_tree::Result<()> {
    let fs = Arc::new(CountingFs::new(Arc::new(MemFs::new())));
    // No pool: a refill would sync the folder for files of its own.
    let tree = open(deferred(fs.clone()).table_file_pool(0));
    tree.insert("a", "1", 0);

    let before = fs.stats().snapshot();
    tree.flush_active_memtable(0)?;
    let flushed = fs
        .stats()
        .snapshot()
        .since(&before)
        .expect("one counter set");
    assert_eq!(flushed.syncs(), 0, "the flush synced: {flushed:?}");
    assert_eq!(
        value(&tree, "a").as_deref(),
        Some("1"),
        "readable before the barrier"
    );

    let before = fs.stats().snapshot();
    tree.persist_flushes()?;
    let persisted = fs
        .stats()
        .snapshot()
        .since(&before)
        .expect("one counter set");
    assert!(
        persisted
            .origin(lsm_tree::fs::WriteOrigin::FlushTable)
            .syncs
            >= 1,
        "the barrier syncs the table: {persisted:?}"
    );
    assert!(
        persisted.origin(lsm_tree::fs::WriteOrigin::Manifest).syncs >= 1,
        "the barrier syncs the manifest: {persisted:?}"
    );

    // Nothing is pending once the barrier returns: a second one writes nothing.
    let before = fs.stats().snapshot();
    tree.persist_flushes()?;
    let again = fs
        .stats()
        .snapshot()
        .since(&before)
        .expect("one counter set");
    assert_eq!(again.syncs(), 0, "an idle barrier synced: {again:?}");
    Ok(())
}

/// A power loss before the barrier takes the flushes after the last barrier
/// and nothing before it, and the tree opens at that barrier with the lost
/// flushes' tables removed. Guards against the manifest naming a table a
/// deferred flush never synced, which would fail the open after the power
/// loss, and against a lost table left on disk.
#[test]
fn a_power_loss_before_the_barrier_reopens_at_the_last_barrier() -> lsm_tree::Result<()> {
    let crash = CrashFs::new(MemFs::new());
    let tree = open(deferred(Arc::new(crash.clone())));
    tree.insert("kept", "1", 0);
    tree.flush_active_memtable(0)?;
    tree.persist_flushes()?;
    tree.insert("lost", "2", 1);
    tree.flush_active_memtable(0)?;
    assert_eq!(value(&tree, "lost").as_deref(), Some("2"));

    crash.crash();
    // The dropped tree's own persist finds its unsynced table gone and fails,
    // which it only logs.
    drop(tree);

    let tree = open(deferred(Arc::new(crash.clone())));
    assert_eq!(
        value(&tree, "kept").as_deref(),
        Some("1"),
        "the barrier's flush"
    );
    assert_eq!(value(&tree, "lost"), None, "the flush after the barrier");
    assert_eq!(tree.table_count(), 1);
    assert_eq!(
        table_files(&crash, &Path::new(DB).join("tables")).len(),
        1,
        "no table of the lost flush is left on disk"
    );
    assert_eq!(tree.get_highest_persisted_seqno(), Some(0));
    Ok(())
}

/// What the barrier covered survives a power loss. Guards against a barrier
/// that writes the manifest without syncing the tables it names.
#[test]
fn a_power_loss_after_the_barrier_keeps_every_flush_before_it() -> lsm_tree::Result<()> {
    let crash = CrashFs::new(MemFs::new());
    let tree = open(deferred(Arc::new(crash.clone())).table_file_pool(0));
    for (seqno, key) in [(0, "a"), (1, "b"), (2, "c")] {
        tree.insert(key, "v", seqno);
        tree.flush_active_memtable(0)?;
    }
    tree.persist_flushes()?;
    crash.crash();
    drop(tree);

    let tree = open(deferred(Arc::new(crash.clone())));
    for key in ["a", "b", "c"] {
        assert_eq!(value(&tree, key).as_deref(), Some("v"), "{key}");
    }
    assert_eq!(tree.table_count(), 3);
    Ok(())
}

/// The highest persisted seqno, by which a caller trims its log, counts only
/// what a barrier made durable. Guards against a caller dropping log records
/// a power loss would take from the tree.
#[test]
fn the_persisted_seqno_waits_for_the_barrier() -> lsm_tree::Result<()> {
    let tree = open(deferred(Arc::new(MemFs::new())));
    tree.insert("a", "1", 5);
    tree.flush_active_memtable(0)?;
    assert_eq!(tree.get_highest_persisted_seqno(), None);
    tree.persist_flushes()?;
    assert_eq!(tree.get_highest_persisted_seqno(), Some(5));
    Ok(())
}

/// A compaction is durable on its own and persists the deferred flushes with
/// it: one that consumed them leaves after a power loss its output and no
/// trace of its inputs. Guards against a compaction edit written from the
/// current version but over a manifest that never named its inputs, and
/// against the inputs being needed after it.
#[test]
fn a_compaction_over_deferred_flushes_survives_a_power_loss() -> lsm_tree::Result<()> {
    let crash = CrashFs::new(MemFs::new());
    let tree = open(deferred(Arc::new(crash.clone())));
    for (seqno, key) in [(0, "a"), (1, "b"), (2, "c")] {
        tree.insert(key, "v", seqno);
        tree.flush_active_memtable(0)?;
    }
    tree.major_compact(u64::MAX, 0)?;
    assert_eq!(tree.table_count(), 1);
    crash.crash();
    drop(tree);

    let tree = open(deferred(Arc::new(crash.clone())));
    for key in ["a", "b", "c"] {
        assert_eq!(value(&tree, key).as_deref(), Some("v"), "{key}");
    }
    assert_eq!(tree.table_count(), 1, "the compaction's output alone");
    assert_eq!(
        table_files(&crash, &Path::new(DB).join("tables")).len(),
        1,
        "no input is left on disk"
    );
    Ok(())
}

/// A flush deferred before a compaction and one deferred after it: the
/// compaction persists the first, the second stays pending, and a power loss
/// takes only the second. Guards against the persisted version losing track
/// of the deferred files a compaction did not consume.
#[test]
fn a_compaction_persists_the_flushes_it_did_not_consume() -> lsm_tree::Result<()> {
    let crash = CrashFs::new(MemFs::new());
    let tree = open(deferred(Arc::new(crash.clone())));
    for (seqno, key) in [(0, "a"), (1, "b")] {
        tree.insert(key, "v", seqno);
        tree.flush_active_memtable(0)?;
    }
    tree.major_compact(u64::MAX, 0)?;
    tree.insert("after", "v", 2);
    tree.flush_active_memtable(0)?;
    crash.crash();
    drop(tree);

    let tree = open(deferred(Arc::new(crash.clone())));
    assert_eq!(value(&tree, "a").as_deref(), Some("v"));
    assert_eq!(value(&tree, "b").as_deref(), Some("v"));
    assert_eq!(value(&tree, "after"), None, "flushed after the compaction");
    Ok(())
}

/// A tree that is dropped persists its deferred flushes. Guards against a
/// clean close leaving a caller to replay what it flushed.
#[test]
fn a_dropped_tree_persists_its_deferred_flushes() -> lsm_tree::Result<()> {
    let crash = CrashFs::new(MemFs::new());
    {
        let tree = open(deferred(Arc::new(crash.clone())));
        tree.insert("a", "1", 0);
        tree.flush_active_memtable(0)?;
    }
    crash.crash();

    let tree = open(deferred(Arc::new(crash.clone())));
    assert_eq!(value(&tree, "a").as_deref(), Some("1"));
    Ok(())
}

/// A checkpoint taken with deferred flushes pending holds them through a
/// power loss: it persists before it links. Guards against a checkpoint
/// linking table files whose bytes nothing synced.
#[test]
fn a_checkpoint_persists_the_deferred_flushes_first() -> lsm_tree::Result<()> {
    let crash = CrashFs::new(MemFs::new());
    let tree = open(deferred(Arc::new(crash.clone())));
    tree.insert("a", "1", 0);
    tree.flush_active_memtable(0)?;
    tree.create_checkpoint(Path::new("/checkpoint"))?;
    crash.crash();
    drop(tree);

    let restored = open(
        Config::new(
            "/checkpoint",
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(Arc::new(crash.clone())),
    );
    assert_eq!(value(&restored, "a").as_deref(), Some("1"));
    Ok(())
}

/// With values kept apart, the blob files of a deferred flush follow its
/// tables: lost with them before the barrier, kept with them after it.
/// Guards against a blob file the barrier forgot, which would leave a table
/// pointing at values that are gone.
#[test]
fn a_blob_trees_deferred_flush_persists_its_blob_files() -> lsm_tree::Result<()> {
    let blob_config = |crash: &CrashFs| {
        deferred(Arc::new(crash.clone()))
            .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    };
    let big = "x".repeat(4_096);

    let crash = CrashFs::new(MemFs::new());
    let tree = open(blob_config(&crash));
    tree.insert("kept", &big, 0);
    tree.flush_active_memtable(0)?;
    tree.persist_flushes()?;
    tree.insert("lost", &big, 1);
    tree.flush_active_memtable(0)?;
    crash.crash();
    drop(tree);

    let tree = open(blob_config(&crash));
    assert_eq!(value(&tree, "kept").as_deref(), Some(big.as_str()));
    assert_eq!(value(&tree, "lost"), None);
    assert_eq!(tree.blob_file_count(), 1, "the barrier's blob file alone");
    Ok(())
}

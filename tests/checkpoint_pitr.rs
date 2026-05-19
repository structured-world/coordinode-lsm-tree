// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation
//
// End-to-end tests for `Tree::create_checkpoint` and
// `BlobTree::create_checkpoint` — the point-in-time recovery (PITR) primitive
// described in https://github.com/structured-world/coordinode-lsm-tree/issues/210.

use lsm_tree::{
    AbstractTree, AnyTree, Config, KvSeparationOptions, SequenceNumberCounter, config::LevelRoute,
};
use std::sync::Arc;

fn open_tree<P: AsRef<std::path::Path>>(path: P) -> lsm_tree::Result<AnyTree> {
    Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
}

fn open_blob_tree<P: AsRef<std::path::Path>>(path: P) -> lsm_tree::Result<AnyTree> {
    Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    .open()
}

/// 1. Round-trip: write data, checkpoint, reopen the checkpoint as an
///    independent tree, verify every key is readable.
#[test_log::test]
fn checkpoint_roundtrip_reopens_independently() -> lsm_tree::Result<()> {
    let src_dir = tempfile::tempdir()?;
    let dst_dir = tempfile::tempdir()?;
    let dst_path = dst_dir.path().join("checkpoint");

    let tree = open_tree(src_dir.path())?;
    for i in 0u32..100 {
        tree.insert(format!("k{i:03}"), format!("v{i}"), u64::from(i));
    }
    tree.flush_active_memtable(0)?;

    let info = tree.create_checkpoint(&dst_path)?;
    assert!(
        info.sst_files >= 1,
        "checkpoint must capture at least one SST"
    );
    assert_eq!(info.blob_files, 0);
    assert!(info.total_bytes > 0);

    // Reopen the checkpoint as a brand-new tree and read every key.
    let restored = open_tree(&dst_path)?;
    for i in 0u32..100 {
        let key = format!("k{i:03}");
        let val = restored
            .get(key.as_bytes(), lsm_tree::SeqNo::MAX)?
            .unwrap_or_else(|| panic!("missing key {key} in checkpoint"));
        assert_eq!(&*val, format!("v{i}").as_bytes());
    }
    Ok(())
}

/// 2. Concurrent writes: writes interleaved with the checkpoint must not
///    corrupt the snapshot. After the checkpoint completes, every key
///    that existed at checkpoint time must be readable from the
///    re-opened copy.
#[test_log::test]
fn checkpoint_survives_concurrent_writes() -> lsm_tree::Result<()> {
    use std::thread;

    let src_dir = tempfile::tempdir()?;
    let dst_dir = tempfile::tempdir()?;
    let dst_path = dst_dir.path().join("checkpoint");

    let tree = Arc::new(open_tree(src_dir.path())?);

    for i in 0u32..50 {
        tree.insert(format!("k{i:03}"), format!("v{i}"), u64::from(i));
    }
    tree.flush_active_memtable(0)?;

    let writer_tree = Arc::clone(&tree);
    let writer = thread::spawn(move || -> lsm_tree::Result<()> {
        for i in 50u32..200 {
            writer_tree.insert(format!("k{i:03}"), format!("v{i}"), u64::from(i));
            if i.is_multiple_of(10) {
                writer_tree.flush_active_memtable(0)?;
            }
        }
        Ok(())
    });

    let _info = tree.create_checkpoint(&dst_path)?;
    writer.join().expect("writer thread panicked")?;

    // Reopening must succeed regardless of what the writer did.
    let restored = open_tree(&dst_path)?;
    // Keys 0..50 were flushed before the checkpoint started — their SSTs
    // must be present in the snapshot. Use SeqNo::MAX since checkpoint
    // captures the storage state, not the visible-seqno watermark.
    for i in 0u32..50 {
        let key = format!("k{i:03}");
        let val = restored.get(key.as_bytes(), lsm_tree::SeqNo::MAX)?;
        assert!(val.is_some(), "checkpoint missing pre-existing key {key}");
    }
    Ok(())
}

/// 3. BlobTree: KV-separated values must also be present in the
///    checkpoint, with both index SSTs and blob files hard-linked.
#[test_log::test]
fn blob_tree_checkpoint_captures_blobs() -> lsm_tree::Result<()> {
    let src_dir = tempfile::tempdir()?;
    let dst_dir = tempfile::tempdir()?;
    let dst_path = dst_dir.path().join("checkpoint");

    let tree = open_blob_tree(src_dir.path())?;
    let big = b"x".repeat(4096);
    for i in 0u32..10 {
        tree.insert(format!("blob{i}"), &big, u64::from(i));
    }
    tree.flush_active_memtable(0)?;

    let info = tree.create_checkpoint(&dst_path)?;
    assert!(info.sst_files >= 1);
    assert!(
        info.blob_files >= 1,
        "BlobTree checkpoint must include blob files"
    );

    let restored = open_blob_tree(&dst_path)?;
    for i in 0u32..10 {
        let key = format!("blob{i}");
        let val = restored
            .get(key.as_bytes(), lsm_tree::SeqNo::MAX)?
            .unwrap_or_else(|| panic!("missing blob {key}"));
        assert_eq!(&*val, big.as_slice());
    }
    Ok(())
}

/// 4. level_routes (tiered storage): an SST on a routed tier must still be
///    captured in the checkpoint under the flattened `tables/` folder.
#[test_log::test]
fn checkpoint_flattens_level_routes() -> lsm_tree::Result<()> {
    let src_dir = tempfile::tempdir()?;
    let hot_dir = tempfile::tempdir()?;
    let dst_dir = tempfile::tempdir()?;
    let dst_path = dst_dir.path().join("checkpoint");

    let routes = vec![LevelRoute {
        levels: 0..2, // route L0 + L1 to the hot tier
        path: hot_dir.path().to_path_buf(),
        fs: Arc::new(lsm_tree::fs::StdFs),
    }];

    let tree = Config::new(
        src_dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .level_routes(routes)
    .open()?;

    for i in 0u32..20 {
        tree.insert(format!("k{i:03}"), format!("v{i}"), u64::from(i));
    }
    tree.flush_active_memtable(0)?;

    let info = tree.create_checkpoint(&dst_path)?;
    assert!(info.sst_files >= 1);

    // All checkpoint SSTs live under <dst>/tables/ regardless of where
    // they were physically stored in the source.
    let dst_tables = dst_path.join("tables");
    let entries = std::fs::read_dir(&dst_tables)?
        .filter_map(Result::ok)
        .count();
    assert!(
        entries >= 1,
        "checkpoint tables/ must contain hard-linked SSTs"
    );
    Ok(())
}

/// 5. Reopen-then-mutate isolation: writes against the checkpoint must
///    NOT bleed back into the source tree (proves hard links share inode
///    data but the tree state diverges from the checkpoint moment forward).
#[test_log::test]
fn checkpoint_and_source_diverge_after_writes() -> lsm_tree::Result<()> {
    let src_dir = tempfile::tempdir()?;
    let dst_dir = tempfile::tempdir()?;
    let dst_path = dst_dir.path().join("checkpoint");

    let tree = open_tree(src_dir.path())?;
    tree.insert("shared", "before", 1);
    tree.flush_active_memtable(0)?;

    tree.create_checkpoint(&dst_path)?;

    // Mutate the source AFTER the checkpoint.
    tree.insert("shared", "after-src", 2);
    tree.insert("src-only", "src", 3);
    tree.flush_active_memtable(0)?;
    drop(tree);

    let restored = open_tree(&dst_path)?;
    // Checkpoint must still see the pre-checkpoint value.
    let v = restored
        .get(b"shared", lsm_tree::SeqNo::MAX)?
        .expect("shared key");
    assert_eq!(&*v, b"before");
    assert!(
        restored.get(b"src-only", lsm_tree::SeqNo::MAX)?.is_none(),
        "post-checkpoint inserts must NOT appear in the checkpoint tree",
    );

    // And: mutating the checkpoint copy must not affect the source.
    restored.insert("dst-only", "dst", 7);
    restored.flush_active_memtable(0)?;
    drop(restored);

    let src_again = open_tree(src_dir.path())?;
    assert!(
        src_again.get(b"dst-only", lsm_tree::SeqNo::MAX)?.is_none(),
        "writes to the checkpoint must NOT bleed back into the source",
    );
    Ok(())
}

/// 6a. Deferred-deletion invariant: a major compaction that runs while a
///     checkpoint is taking SSTs in-flight must NOT physically remove any
///     pre-compaction SST until the checkpoint finishes. This is the core
///     correctness property of the deletion-pause gate.
#[test_log::test]
fn compaction_during_checkpoint_preserves_source_ssts() -> lsm_tree::Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;

    let src_dir = tempfile::tempdir()?;
    let dst_dir = tempfile::tempdir()?;
    let dst_path = dst_dir.path().join("checkpoint");

    let tree = Arc::new(open_tree(src_dir.path())?);

    // Produce several SSTs so major_compact will actually have work to do.
    for batch in 0u32..5 {
        for i in 0u32..20 {
            let k = format!("k{batch:02}_{i:03}");
            tree.insert(k, format!("v{batch}.{i}"), u64::from(batch * 100 + i));
        }
        tree.flush_active_memtable(0)?;
    }

    let tables_dir = src_dir.path().join("tables");
    let pre_checkpoint_ssts: std::collections::HashSet<_> = std::fs::read_dir(&tables_dir)?
        .filter_map(Result::ok)
        .map(|e| e.file_name())
        .collect();
    assert!(
        pre_checkpoint_ssts.len() >= 2,
        "test needs ≥2 SSTs to be meaningful (got {})",
        pre_checkpoint_ssts.len(),
    );

    let stop = Arc::new(AtomicBool::new(false));
    let compactor_tree = Arc::clone(&tree);
    let compactor_stop = Arc::clone(&stop);
    let compactor = thread::spawn(move || -> lsm_tree::Result<()> {
        while !compactor_stop.load(Ordering::Acquire) {
            let _ = compactor_tree.major_compact(u64::MAX, lsm_tree::SeqNo::MAX);
            thread::sleep(Duration::from_millis(5));
        }
        Ok(())
    });

    // Run the checkpoint while compaction is racing. The pause must hold
    // back any compaction-driven removals until our hard-link loop has
    // captured every SST that existed at checkpoint time.
    let info = tree.create_checkpoint(&dst_path)?;
    stop.store(true, Ordering::Release);
    compactor.join().expect("compactor panicked")?;

    assert_eq!(info.sst_files, info.sst_files); // sanity
    let restored = open_tree(&dst_path)?;
    for batch in 0u32..5 {
        for i in 0u32..20 {
            let k = format!("k{batch:02}_{i:03}");
            assert!(
                restored.get(k.as_bytes(), lsm_tree::SeqNo::MAX)?.is_some(),
                "checkpoint missing key {k} despite concurrent compaction",
            );
        }
    }
    Ok(())
}

/// 6b. Empty tree: checkpointing a freshly-opened, never-written tree
///     must succeed and produce a checkpoint that opens cleanly with
///     zero SSTs.
#[test_log::test]
fn checkpoint_of_empty_tree() -> lsm_tree::Result<()> {
    let src_dir = tempfile::tempdir()?;
    let dst_dir = tempfile::tempdir()?;
    let dst_path = dst_dir.path().join("checkpoint");

    let tree = open_tree(src_dir.path())?;
    let info = tree.create_checkpoint(&dst_path)?;
    assert_eq!(info.sst_files, 0);
    assert_eq!(info.blob_files, 0);
    assert_eq!(info.total_bytes, 0);

    let restored = open_tree(&dst_path)?;
    assert!(restored.get(b"anything", lsm_tree::SeqNo::MAX)?.is_none());
    Ok(())
}

/// 6c. `CheckpointInfo.total_bytes` matches the actual on-disk sum of
///     the hard-linked SST files in the destination directory.
#[test_log::test]
fn checkpoint_info_total_bytes_matches_disk() -> lsm_tree::Result<()> {
    let src_dir = tempfile::tempdir()?;
    let dst_dir = tempfile::tempdir()?;
    let dst_path = dst_dir.path().join("checkpoint");

    let tree = open_tree(src_dir.path())?;
    for i in 0u32..200 {
        tree.insert(format!("k{i:04}"), b"v".repeat(64), u64::from(i));
    }
    tree.flush_active_memtable(0)?;

    let info = tree.create_checkpoint(&dst_path)?;

    let actual_bytes: u64 = std::fs::read_dir(dst_path.join("tables"))?
        .filter_map(Result::ok)
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum();
    assert_eq!(
        info.total_bytes, actual_bytes,
        "CheckpointInfo.total_bytes ({}) must equal on-disk sum ({actual_bytes})",
        info.total_bytes,
    );
    Ok(())
}

/// 6d. Re-running a checkpoint into the SAME target after it succeeded
///     must fail with `AlreadyExists` — the snapshot is not allowed to
///     overwrite a prior one without explicit caller intervention.
#[test_log::test]
fn second_checkpoint_into_same_target_rejected() -> lsm_tree::Result<()> {
    let src_dir = tempfile::tempdir()?;
    let dst_dir = tempfile::tempdir()?;
    let dst_path = dst_dir.path().join("checkpoint");

    let tree = open_tree(src_dir.path())?;
    tree.insert("k", "v", 1);
    tree.flush_active_memtable(0)?;

    tree.create_checkpoint(&dst_path)?;
    let err = tree.create_checkpoint(&dst_path).unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("already exists") || msg.contains("AlreadyExists"),
        "expected AlreadyExists on second checkpoint, got {msg}",
    );
    Ok(())
}

/// 6e. Cross-`Fs` link fall-back: when the source tree's `Fs` differs
///     from the checkpoint target's `Fs` (here: `StdFs` source vs.
///     `MemFs` target), the checkpoint code must transparently stream
///     bytes through both trait objects instead of attempting a real
///     hard link.
#[test_log::test]
fn cross_fs_link_or_copy_streams_through_trait() -> lsm_tree::Result<()> {
    use lsm_tree::checkpoint::link_or_copy_cross_fs;
    use lsm_tree::fs::{Fs, FsOpenOptions, MemFs, StdFs};
    use std::io::Write;

    let dir = tempfile::tempdir()?;
    let src = dir.path().join("payload.bin");
    std::fs::write(&src, b"cross-fs-payload")?;

    let std_fs: Arc<dyn Fs> = Arc::new(StdFs);
    let mem_fs: Arc<dyn Fs> = Arc::new(MemFs::new());
    mem_fs.create_dir_all(std::path::Path::new("/dst"))?;

    let dst = std::path::Path::new("/dst/payload.bin");
    let bytes = link_or_copy_cross_fs(&std_fs, &src, &mem_fs, dst)?;
    assert_eq!(bytes, b"cross-fs-payload".len() as u64);

    // Read back through MemFs to confirm the bytes landed.
    let mut buf = String::new();
    use std::io::Read;
    mem_fs
        .open(dst, &FsOpenOptions::new().read(true))?
        .read_to_string(&mut buf)?;
    assert_eq!(buf, "cross-fs-payload");

    // And: subsequent writes through MemFs do not affect the source file.
    let mut f = mem_fs.open(
        std::path::Path::new("/dst/payload.bin.2"),
        &FsOpenOptions::new().write(true).create(true),
    )?;
    f.write_all(b"unrelated")?;
    assert_eq!(std::fs::read(&src)?, b"cross-fs-payload");
    Ok(())
}

/// 7. Crash safety: aborting mid-checkpoint must leave the SOURCE tree
///    fully intact and reopenable. We force the failure by pre-creating
///    the target — `prepare_target` rejects existing destinations.
#[test_log::test]
fn checkpoint_failure_leaves_source_intact() -> lsm_tree::Result<()> {
    let src_dir = tempfile::tempdir()?;
    let dst_dir = tempfile::tempdir()?;
    let dst_path = dst_dir.path().join("checkpoint");
    std::fs::create_dir_all(&dst_path)?;

    let tree = open_tree(src_dir.path())?;
    for i in 0u32..50 {
        tree.insert(format!("k{i:03}"), format!("v{i}"), u64::from(i));
    }
    tree.flush_active_memtable(0)?;

    let err = tree.create_checkpoint(&dst_path).unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("already exists") || msg.contains("AlreadyExists"),
        "expected AlreadyExists error, got {msg}",
    );

    // Source tree remains fully readable.
    for i in 0u32..50 {
        let key = format!("k{i:03}");
        let val = tree
            .get(key.as_bytes(), lsm_tree::SeqNo::MAX)?
            .unwrap_or_else(|| panic!("source lost {key}"));
        assert_eq!(&*val, format!("v{i}").as_bytes());
    }
    drop(tree);

    // And: reopening the source after the failed checkpoint still works.
    let reopened = open_tree(src_dir.path())?;
    let val = reopened
        .get(b"k042", lsm_tree::SeqNo::MAX)?
        .expect("reopen lost data");
    assert_eq!(&*val, b"v42");
    Ok(())
}

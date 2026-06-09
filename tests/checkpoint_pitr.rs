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

    // Use a SHARED visible_seqno generator and fetch_max it past every
    // inserted record's seqno. Without this, info.seqno (captured from
    // the visible_seqno generator) stays at whatever value internal
    // version transitions bumped it to — small, unrelated to the
    // 0..200 record seqnos this test uses — and the watermark assertion
    // below becomes vacuous (the `i as u64 <= info.seqno` guard rarely
    // fires, so checkpoint regressions silently pass).
    let visible_seqno = SequenceNumberCounter::default();
    let tree = Arc::new(
        Config::new(
            src_dir.path(),
            SequenceNumberCounter::default(),
            visible_seqno.clone(),
        )
        .open()?,
    );

    for i in 0u32..50 {
        tree.insert(format!("k{i:03}"), format!("v{i}"), u64::from(i));
        visible_seqno.fetch_max(u64::from(i) + 1);
    }
    tree.flush_active_memtable(0)?;

    // mpsc handshake guarantees the writer has performed at least one
    // post-flush insert BEFORE create_checkpoint runs. Without it, a
    // fast scheduler can finish create_checkpoint before the worker
    // touches the tree, collapsing the test into a single-threaded
    // smoke test that does not exercise the concurrency invariant.
    let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
    let writer_tree = Arc::clone(&tree);
    let writer_seqno = visible_seqno.clone();
    let writer = thread::spawn(move || -> lsm_tree::Result<()> {
        for i in 50u32..200 {
            writer_tree.insert(format!("k{i:03}"), format!("v{i}"), u64::from(i));
            writer_seqno.fetch_max(u64::from(i) + 1);
            if i == 50 {
                started_tx
                    .send(())
                    .expect("checkpoint thread dropped writer-start receiver");
            }
            if i.is_multiple_of(10) {
                writer_tree.flush_active_memtable(0)?;
            }
        }
        Ok(())
    });

    started_rx
        .recv()
        .expect("writer thread exited before issuing a concurrent write");
    let info = tree.create_checkpoint(&dst_path)?;
    writer.join().expect("writer thread panicked")?;

    // Reopening must succeed regardless of what the writer did.
    let restored = open_tree(&dst_path)?;

    // PITR watermark contract: every key with `seqno < info.seqno` MUST
    // be present in the checkpoint. visible_seqno is "lowest excluded"
    // (the next-seqno-to-allocate) — a record with `seqno = N` is
    // committed/visible iff `visible_seqno >= N + 1`, equivalently
    // `seqno < visible_seqno`. The strict `<` matters under concurrent
    // scheduling: writer's `insert(seqno=i)` and `fetch_max(i + 1)` are
    // NOT atomic, so main can observe `visible_seqno = i` between
    // those two steps. At that moment record-i may or may not be in
    // the flushed memtable; only records with seqno strictly less than
    // the captured watermark are guaranteed committed.
    //
    // We make NO claim about keys with `i >= info.seqno` — those may
    // or may not be in the snapshot depending on whether the writer
    // had advanced its watermark past `i` before checkpoint sampled
    // visible_seqno.
    for i in 0u32..200 {
        let key = format!("k{i:03}");
        let got = restored.get(key.as_bytes(), lsm_tree::SeqNo::MAX)?;
        if u64::from(i) < info.seqno {
            assert!(
                got.is_some(),
                "PITR watermark violated: key {key} (seqno {i}) < info.seqno ({}) but missing from checkpoint",
                info.seqno,
            );
        }
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

    // Verify the routed tier is actually being used BEFORE checkpoint.
    // Without this guard the test would still pass if `level_routes` were
    // silently ignored and SSTs landed in the default source directory —
    // the checkpoint code would just hard-link from the wrong place.
    let hot_tables = hot_dir.path().join("tables");
    let hot_entries = std::fs::read_dir(&hot_tables)?.collect::<std::io::Result<Vec<_>>>()?;
    assert!(
        !hot_entries.is_empty(),
        "level_routes route should have produced ≥1 SST in {}",
        hot_tables.display(),
    );

    let info = tree.create_checkpoint(&dst_path)?;
    assert!(info.sst_files >= 1);

    // All checkpoint SSTs live under <dst>/tables/ regardless of where
    // they were physically stored in the source. Errors are propagated
    // (not silenced with `filter_map(Result::ok)`) so a partial / corrupt
    // checkpoint surfaces as a failed I/O instead of a smaller count.
    let dst_tables = dst_path.join("tables");
    let entries = std::fs::read_dir(&dst_tables)?
        .collect::<std::io::Result<Vec<_>>>()?
        .len();
    assert!(
        entries >= 1,
        "checkpoint tables/ must contain hard-linked SSTs"
    );
    Ok(())
}

/// 4b. Regression for MVCC GC leak through checkpoint flush.
///
/// `run_checkpoint` calls `flush_active_memtable(threshold)` to make sure
/// the active memtable is in SSTs before linking. The threshold value
/// drives `CompactionStream`'s `gc_seqno_threshold` — anything below it
/// can be dropped from older versions of the same key. Passing
/// `SeqNo::MAX` would mean "no caller cares about any historical seqno",
/// which silently destroys MVCC history that the SOURCE tree's readers
/// rely on.
///
/// Scenario:
///   - Two versions of "k": seqno 1 ("v1") and seqno 2 ("v2")
///   - Both live in the active memtable
///   - Take a checkpoint (triggers the internal flush)
///   - Read "k" at seqno 2 on the source — must return "v1" (the
///     version visible to a snapshot reading below seqno 2)
///
/// With the bug (threshold = SeqNo::MAX) the flush merges the two
/// versions and drops "v1" because 1 < MAX, so the read returns "v2"
/// (or None) — a snapshot-visibility violation caused entirely by
/// taking a checkpoint.
#[test_log::test]
fn checkpoint_flush_must_not_drop_source_mvcc_history() -> lsm_tree::Result<()> {
    let src_dir = tempfile::tempdir()?;
    let dst_dir = tempfile::tempdir()?;
    let dst_path = dst_dir.path().join("checkpoint");

    let tree = open_tree(src_dir.path())?;
    tree.insert("k", "v1", 1);
    tree.insert("k", "v2", 2);

    // Sanity: the older version is visible to a seqno-1 reader BEFORE
    // the checkpoint (both versions still in the active memtable).
    let pre = tree.get(b"k", 2)?;
    assert_eq!(
        pre.as_deref(),
        Some(&b"v1"[..]),
        "pre-checkpoint sanity: seqno-2 read sees seqno-1 version of k",
    );

    tree.create_checkpoint(&dst_path)?;

    // Post-checkpoint: the source tree's reader at the same seqno MUST
    // still see "v1". Without the fix the checkpoint-triggered flush
    // uses `SeqNo::MAX` as its GC threshold and discards every older
    // version of every key.
    let post = tree.get(b"k", 2)?;
    assert_eq!(
        post.as_deref(),
        Some(&b"v1"[..]),
        "checkpoint flush must not drop MVCC history needed by source readers",
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
        .collect::<std::io::Result<Vec<_>>>()?
        .into_iter()
        .map(|e| e.file_name())
        .collect();
    assert!(
        pre_checkpoint_ssts.len() >= 2,
        "test needs ≥2 SSTs to be meaningful (got {})",
        pre_checkpoint_ssts.len(),
    );

    // RAII guard that stops + joins the compactor thread on every exit
    // path. Without this, an early-returning `?` would detach the thread
    // and let it keep mutating the tree behind concurrent tests.
    //
    // The Drop impl propagates BOTH panics and Err results:
    //
    // - `Err(Box<dyn Any>)` from `join()` means the worker thread
    //   panicked. We `resume_unwind` so the panic surfaces as a test
    //   failure instead of being silently dropped.
    // - `Ok(Err(e))` means the worker returned an error from
    //   `major_compact` — we `panic!` to fail the test loudly.
    //
    // Both branches check `thread::panicking()` first: if the test body
    // is already unwinding from its own failure, re-panicking would
    // abort the process instead of letting the original panic propagate.
    struct CompactorGuard {
        stop: Arc<AtomicBool>,
        handle: Option<thread::JoinHandle<lsm_tree::Result<()>>>,
    }
    impl Drop for CompactorGuard {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            let Some(h) = self.handle.take() else {
                return;
            };
            match h.join() {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    if !thread::panicking() {
                        panic!("compactor thread returned error: {err:?}");
                    }
                }
                Err(payload) => {
                    if !thread::panicking() {
                        std::panic::resume_unwind(payload);
                    }
                }
            }
        }
    }

    // mpsc handshake guarantees the compactor has entered its loop
    // BEFORE create_checkpoint runs. Without it, a fast scheduler can
    // finish create_checkpoint before the worker calls major_compact
    // even once, making the test pass without exercising the
    // deletion-pause race it is named for.
    let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
    let stop = Arc::new(AtomicBool::new(false));
    let compactor_tree = Arc::clone(&tree);
    let compactor_stop = Arc::clone(&stop);
    let handle = thread::spawn(move || -> lsm_tree::Result<()> {
        let mut announced = false;
        while !compactor_stop.load(Ordering::Acquire) {
            // Errors from major_compact MUST surface — silently
            // discarding them would let a checkpoint race that
            // corrupted compaction state pass the test as green.
            compactor_tree.major_compact(u64::MAX, lsm_tree::SeqNo::MAX)?;
            // Announce AFTER the first compaction completes so the
            // main thread blocks on `recv()` until the worker has
            // actually mutated tree state. Signalling before the call
            // would let a fast scheduler run the checkpoint before
            // any compaction work began, defeating the race coverage.
            if !announced {
                started_tx
                    .send(())
                    .expect("checkpoint thread dropped compactor-start receiver");
                announced = true;
            }
            thread::sleep(Duration::from_millis(5));
        }
        Ok(())
    });
    let _compactor = CompactorGuard {
        stop: Arc::clone(&stop),
        handle: Some(handle),
    };

    started_rx
        .recv()
        .expect("compactor thread exited before reaching its loop");

    // Run the checkpoint while compaction is racing. The pause must hold
    // back any compaction-driven removals until our hard-link loop has
    // captured every SST that existed at checkpoint time. If
    // `create_checkpoint` returns Err, the `?` propagates and
    // `CompactorGuard::drop` stops + joins the worker cleanly.
    let info = tree.create_checkpoint(&dst_path)?;
    assert!(
        info.sst_files >= 1,
        "checkpoint captured no SSTs despite {} pre-existing on disk",
        pre_checkpoint_ssts.len(),
    );

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

    // Propagate I/O errors instead of swallowing them with `Result::ok`,
    // so a corrupted / unreadable entry fails the test instead of being
    // silently excluded from the byte total.
    let entries =
        std::fs::read_dir(dst_path.join("tables"))?.collect::<std::io::Result<Vec<_>>>()?;
    let actual_bytes: u64 = entries
        .into_iter()
        .map(|e| e.metadata())
        .collect::<std::io::Result<Vec<_>>>()?
        .into_iter()
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
    // Assert on the structural variant rather than the Debug string,
    // which is OS-/std-formatter-specific. The second checkpoint must
    // surface `Io(NotFound | AlreadyExists)`-class refusal so callers
    // can detect double-target programmatically.
    match err {
        lsm_tree::Error::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::AlreadyExists => {}
        other => panic!("expected Io(AlreadyExists) on second checkpoint, got {other:?}"),
    }
    Ok(())
}

// 6e. Cross-`Fs` link fall-back is covered by the inline unit test
// `cross_fs_link_or_copy_streams_through_trait` in `src/checkpoint.rs`
// (the helper is `pub(crate)`, so the test lives next to it).

/// 7. Crash safety: aborting mid-checkpoint must leave the SOURCE tree
///    fully intact and reopenable. We force the failure by pre-creating
///    the target — `prepare_target` rejects existing destinations.
#[test_log::test]
fn checkpoint_failure_leaves_source_intact() -> lsm_tree::Result<()> {
    let src_dir = tempfile::tempdir()?;
    let dst_dir = tempfile::tempdir()?;

    let tree = open_tree(src_dir.path())?;
    for i in 0u32..50 {
        tree.insert(format!("k{i:03}"), format!("v{i}"), u64::from(i));
    }
    tree.flush_active_memtable(0)?;

    // ── (a) Early-reject path: target already exists ─────────────────
    // Atomic `Fs::create_dir(target)` inside `prepare_target` returns
    // `AlreadyExists`, so we never enter the link/metadata stage.
    let early_dst = dst_dir.path().join("early");
    std::fs::create_dir_all(&early_dst)?;
    let err = tree.create_checkpoint(&early_dst).unwrap_err();
    // Assert structurally — the Debug format of std::io::Error varies
    // by OS and rustc release. Pattern-match on the variant + ErrorKind
    // so the test guards the programmatic contract (Io(AlreadyExists))
    // that callers actually rely on for early-reject detection.
    assert!(
        matches!(
            &err,
            lsm_tree::Error::Io(io_err)
                if io_err.kind() == std::io::ErrorKind::AlreadyExists,
        ),
        "expected Io(AlreadyExists) on early reject, got {err:?}",
    );
    // Early reject must NOT damage the pre-existing target (it isn't
    // ours to clean up).
    assert!(
        early_dst.exists(),
        "early reject must leave the pre-existing target alone",
    );

    // ── (b) Post-prepare failure path (Unix-only) ───────────────────
    // Force `link_tables` to fail by chmod-ing the source SST to 000
    // AFTER `prepare_target` would normally have succeeded. The outer
    // `PartialCheckpointGuard` (armed right after prepare_target returns)
    // must remove the entire partial checkpoint so a retry on the same
    // path succeeds. Restricted to Unix because Windows has no portable
    // way to make an open file unreadable from another process.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let post_dst = dst_dir.path().join("post");
        let src_tables = src_dir.path().join("tables");
        let orig_perm = std::fs::metadata(&src_tables)?.permissions();

        // Strip read+execute on the SOURCE tables/ directory. We strip
        // the *directory* not the SST file because `link(2)` does NOT
        // require read permission on the source file (it just bumps the
        // inode's link count) — but it DOES require search (x) permission
        // on every directory component of the source path. Without `x`
        // on `tables/`, the kernel cannot resolve `tables/<id>` and
        // `link()` fails with `EACCES`.
        std::fs::set_permissions(&src_tables, std::fs::Permissions::from_mode(0o000))?;

        let result = tree.create_checkpoint(&post_dst);
        // Restore perms BEFORE assertions so source remains usable even
        // if a later assertion fails.
        std::fs::set_permissions(&src_tables, orig_perm)?;

        let err = result.expect_err("create_checkpoint should fail when src tables/ is unreadable");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("Permission")
                || msg.contains("denied")
                || msg.contains("Os")
                || msg.contains("error"),
            "expected post-prepare I/O error, got {msg}",
        );

        // PartialCheckpointGuard must have removed the partial checkpoint.
        assert!(
            !post_dst.exists(),
            "PartialCheckpointGuard must remove partial checkpoint after \
             post-prepare failure; {} still present",
            post_dst.display(),
        );

        // And a retry against the same path now succeeds, proving no
        // stale state leaked.
        let info = tree.create_checkpoint(&post_dst)?;
        assert!(info.sst_files >= 1);
    }

    // Portable fallback (non-Unix): exercise just the success-then-
    // cleanup-then-retry chain to verify run_checkpoint is idempotent
    // against a freshly cleaned target.
    #[cfg(not(unix))]
    {
        let post_dst = dst_dir.path().join("post");
        let info1 = tree.create_checkpoint(&post_dst)?;
        assert!(info1.sst_files >= 1);
        std::fs::remove_dir_all(&post_dst)?;
        let info2 = tree.create_checkpoint(&post_dst)?;
        assert_eq!(info1.sst_files, info2.sst_files);
        assert_eq!(info1.version_id, info2.version_id);
    }

    // ── Source intact across both failure modes ──────────────────────
    for i in 0u32..50 {
        let key = format!("k{i:03}");
        let val = tree
            .get(key.as_bytes(), lsm_tree::SeqNo::MAX)?
            .unwrap_or_else(|| panic!("source lost {key}"));
        assert_eq!(&*val, format!("v{i}").as_bytes());
    }
    drop(tree);

    // And: reopening the source after the failed checkpoints still works.
    let reopened = open_tree(src_dir.path())?;
    let val = reopened
        .get(b"k042", lsm_tree::SeqNo::MAX)?
        .expect("reopen lost data");
    assert_eq!(&*val, b"v42");
    Ok(())
}

/// A checkpoint whose `current` pointer is missing (the crash-recovery
/// hole described in `write_current_for_version`) MUST be rejected by
/// `Tree::open` rather than silently treated as fresh-tree state. This
/// regression test removes the file after a successful checkpoint and
/// asserts open returns an error.
#[test_log::test]
fn checkpoint_open_rejects_missing_current_pointer() -> lsm_tree::Result<()> {
    let src_dir = tempfile::tempdir()?;
    let dst_dir = tempfile::tempdir()?;
    let dst_path = dst_dir.path().join("checkpoint");

    let tree = open_tree(src_dir.path())?;
    for i in 0u32..10 {
        tree.insert(format!("k{i:03}"), format!("v{i}"), u64::from(i));
    }
    tree.flush_active_memtable(0)?;
    tree.create_checkpoint(&dst_path)?;
    drop(tree);

    // Tamper: delete the `current` pointer that
    // `write_current_for_version` writes last during checkpoint sealing.
    let current = dst_path.join("current");
    assert!(
        current.exists(),
        "checkpoint sealing must have produced a `current` pointer",
    );
    std::fs::remove_file(&current)?;

    // Open MUST fail (no fresh-tree fallback) so callers detect the
    // half-written checkpoint instead of mistaking it for empty data.
    // The error carries the path + remediation hint so a programmatic
    // caller can surface it without reading logs.
    let err = match open_tree(&dst_path) {
        Ok(_) => panic!("open_tree on a checkpoint with no `current` must error, got Ok"),
        Err(e) => e,
    };
    match err {
        lsm_tree::Error::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::InvalidData => {
            let msg = io_err.to_string();
            assert!(
                msg.contains("current") && msg.contains("version artifacts"),
                "error must explain the half-written-checkpoint condition, got: {msg}",
            );
        }
        other => panic!("expected Io(InvalidData) with diagnostic message, got {other:?}"),
    }
    Ok(())
}

/// Same crash-recovery contract as above, but for a `current` file that
/// exists yet contains garbage instead of a valid version pointer.
/// `Tree::open` MUST refuse to interpret the directory as a fresh tree.
#[test_log::test]
fn checkpoint_open_rejects_corrupt_current_pointer() -> lsm_tree::Result<()> {
    let src_dir = tempfile::tempdir()?;
    let dst_dir = tempfile::tempdir()?;
    let dst_path = dst_dir.path().join("checkpoint");

    let tree = open_tree(src_dir.path())?;
    for i in 0u32..10 {
        tree.insert(format!("k{i:03}"), format!("v{i}"), u64::from(i));
    }
    tree.flush_active_memtable(0)?;
    tree.create_checkpoint(&dst_path)?;
    drop(tree);

    let current = dst_path.join("current");
    std::fs::write(&current, b"this is not a valid version pointer")?;

    // Assert the specific failure mode the parser catches.
    // `get_current_version` reads `u64 version_id | u128 checksum |
    // u8 checksum_type` and rejects any checksum_type byte that
    // isn't 0 (xxh3) with `Error::InvalidTag(("ChecksumType", _))`.
    // Writing 36 bytes of junk guarantees we hit that branch — checking
    // for InvalidTag instead of just `is_err()` ensures a future refactor
    // that masks the corruption behind a different error path fails the
    // test instead of silently passing.
    let err = match open_tree(&dst_path) {
        Ok(_) => panic!("open_tree on a checkpoint with corrupt `current` must error, got Ok"),
        Err(e) => e,
    };
    match err {
        lsm_tree::Error::InvalidTag((field, _)) => assert_eq!(
            field, "ChecksumType",
            "expected ChecksumType InvalidTag from corrupt `current`, got field {field}",
        ),
        other => panic!("expected Error::InvalidTag(ChecksumType, _), got {other:?}"),
    }
    Ok(())
}

/// Regression test for the manifest-GC race: a concurrent compaction
/// can run `SuperVersions::maintenance` between `current_version()`
/// inside the checkpoint driver and the time `copy_metadata` reaches
/// the source `v<id>` file. The captured Version still lives in
/// memory, but the on-disk `v<id>` is gone — the source-file copy
/// path would fail. The fix serialises the captured Version from
/// memory into the checkpoint dir, so the race cannot fail the
/// checkpoint.
///
/// We simulate the race by deleting the source `v<id>` AFTER
/// `current_version()` would have captured it. In this test there is
/// no concurrent maintenance because we drive the deletion directly;
/// the in-memory SuperVersions still references the deleted file's
/// id, exactly as it would mid-race.
#[test_log::test]
fn checkpoint_survives_concurrent_manifest_gc_of_captured_version() -> lsm_tree::Result<()> {
    let src_dir = tempfile::tempdir()?;
    let dst_dir = tempfile::tempdir()?;
    let dst_path = dst_dir.path().join("checkpoint");

    let tree = open_tree(src_dir.path())?;
    for i in 0u32..5 {
        tree.insert(format!("k{i:02}"), format!("v{i}"), u64::from(i));
    }
    tree.flush_active_memtable(0)?;

    // Identify the live version file the same way recovery does — read
    // the version id out of `current` rather than picking the first
    // matching `vN` from `read_dir`. read_dir's order is unspecified and
    // multiple vN files may coexist (the previous flush leaves the prior
    // version around for snapshot readers until manifest GC runs), so a
    // naive .find() could remove a STALE file and silently turn the
    // test into a no-op that passes even when the race fix regresses.
    //
    // The `current` wire format is `u64 version_id | u128 checksum |
    // u8 checksum_type` — the first 8 LE bytes give us the live id.
    let current_bytes = std::fs::read(src_dir.path().join("current"))?;
    let version_id = u64::from_le_bytes(
        current_bytes[..8]
            .try_into()
            .expect("`current` stores the version id in the first 8 bytes"),
    );
    let live_v = format!("v{version_id}");
    std::fs::remove_file(src_dir.path().join(&live_v))?;

    // With the fix, the checkpoint serialises the in-memory Version
    // into target/v<id> and succeeds. Without the fix, the copy of
    // the (now missing) source v<id> file fails with NotFound.
    let info = tree.create_checkpoint(&dst_path)?;
    assert!(info.sst_files >= 1);

    // The reopened checkpoint must read every pre-deletion key back
    // — proving the captured version was written into the snapshot
    // independently of the deleted source file.
    let restored = open_tree(&dst_path)?;
    for i in 0u32..5 {
        let key = format!("k{i:02}");
        let val = restored
            .get(key.as_bytes(), lsm_tree::SeqNo::MAX)?
            .unwrap_or_else(|| panic!("missing key {key} in checkpoint"));
        assert_eq!(&*val, format!("v{i}").as_bytes());
    }
    Ok(())
}

/// A checkpoint hard-links the source SST/blob files (shared inode). Reclaiming
/// the source (e.g. `clear()`) must NOT truncate those inodes, or it would zero
/// the checkpoint's copy too. Regression guard for the truncate-based reclaim.
#[cfg(unix)]
#[test_log::test]
fn clear_does_not_truncate_checkpoint_hardlinked_ssts() -> lsm_tree::Result<()> {
    use std::os::unix::fs::MetadataExt;

    let src_dir = tempfile::tempdir()?;
    let dst_dir = tempfile::tempdir()?;
    let dst_path = dst_dir.path().join("checkpoint");

    let any = open_tree(src_dir.path())?;
    let tree = match &any {
        AnyTree::Standard(t) => t.clone(),
        AnyTree::Blob(_) => panic!("expected Standard tree"),
    };
    // Force the hard-link checkpoint path (shared inode) instead of reflink
    // (independent CoW inode), so the truncate-vs-shared-inode hazard is
    // exercised on every filesystem, not only non-reflink ones.
    tree.update_runtime_config(|c| {
        c.use_reflink_for_checkpoint = false;
    })?;
    for i in 0u32..500 {
        tree.insert(
            format!("k{i:04}"),
            b"payload-value-data".as_slice(),
            u64::from(i),
        );
    }
    tree.flush_active_memtable(0)?;

    let info = any.create_checkpoint(&dst_path)?;
    assert!(
        info.sst_files >= 1,
        "checkpoint must capture at least one SST"
    );

    // Precondition: the checkpoint must have hard-linked (shared inode) for this
    // test to exercise the bug. If the platform fell back to a copy, nlink == 1
    // and there is nothing to corrupt — fail loudly so the gap is visible.
    let mut shared = false;
    let mut stack = vec![dst_path.clone()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d)?.flatten() {
            let m = e.metadata()?;
            if m.is_dir() {
                stack.push(e.path());
            } else if m.nlink() > 1 {
                shared = true;
            }
        }
    }
    assert!(
        shared,
        "checkpoint must hard-link a source file (same-fs) to test this"
    );

    // Reclaim the source: marks the SSTs deleted and runs the truncate path.
    tree.clear()?;

    // The checkpoint must still be fully readable.
    let restored = open_tree(&dst_path)?;
    for i in 0u32..500 {
        let key = format!("k{i:04}");
        assert!(
            restored
                .get(key.as_bytes(), lsm_tree::SeqNo::MAX)?
                .is_some(),
            "checkpoint key {key} must survive source clear() — inode was truncated",
        );
    }
    Ok(())
}

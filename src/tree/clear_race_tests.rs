// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A `clear` landing between a flush or a compaction writing its outputs and
//! installing them: the outputs were written from what `clear` removed, so
//! installing them would bring cleared keys back, and a move of tables that
//! are gone has nothing to move.

use crate::compaction::{CompactionStrategy, MoveDown};
use crate::{AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter};
use std::sync::Arc;

const KEYS: usize = 200;

fn open(path: &std::path::Path, blob: bool) -> crate::Result<AnyTree> {
    let config = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    );
    let config = if blob {
        config.with_kv_separation(Some(
            crate::KvSeparationOptions::default().separation_threshold(1),
        ))
    } else {
        config
    };
    config.open()
}

fn write_keys(tree: &AnyTree, seqno: SeqNo) {
    for (i, seqno) in (0..KEYS).zip(seqno..) {
        tree.insert(format!("key{i:04}"), vec![b'v'; 64], seqno);
    }
}

/// Clears `tree` once, right before the next output install.
fn clear_before_install(tree: &AnyTree) {
    let target = tree.clone();
    tree.tree_config()
        .arm_before_output_install(move || target.clear().expect("clear"));
}

/// The files left in `folder` under `path`, absent counting as none. The
/// empty files the tables folder keeps for tables to come are no tables.
fn files_in(path: &std::path::Path, folder: &str) -> usize {
    let pooled = folder == crate::file::TABLES_FOLDER;
    std::fs::read_dir(path.join(folder)).map_or(0, |entries| {
        entries
            .filter_map(Result::ok)
            .filter(|entry| !pooled || entry.metadata().is_ok_and(|meta| meta.len() > 0))
            .count()
    })
}

/// The tree reads nothing, holds no table, leaves no table or blob file once
/// dropped, still reads nothing after a reopen, and takes writes, flushes and
/// compactions again.
fn assert_cleared(path: &std::path::Path, tree: AnyTree, blob: bool) -> crate::Result<()> {
    assert_eq!(
        tree.iter(SeqNo::MAX, None).count(),
        0,
        "cleared keys came back"
    );
    assert_eq!(tree.table_count(), 0);
    drop(tree);
    assert_eq!(
        files_in(path, crate::file::TABLES_FOLDER),
        0,
        "a table file stayed"
    );
    assert_eq!(
        files_in(path, crate::file::BLOBS_FOLDER),
        0,
        "a blob file stayed"
    );

    let tree = open(path, blob)?;
    assert_eq!(
        tree.iter(SeqNo::MAX, None).count(),
        0,
        "cleared keys came back on reopen"
    );
    write_keys(&tree, 10_000);
    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, 0)?;
    assert_eq!(tree.iter(SeqNo::MAX, None).count(), KEYS);
    Ok(())
}

/// A flush whose sealed memtables `clear` dropped installs nothing.
#[test]
fn a_flush_after_a_clear_installs_nothing() -> crate::Result<()> {
    for blob in [false, true] {
        let folder = tempfile::tempdir()?;
        let tree = open(folder.path(), blob)?;
        write_keys(&tree, 1);
        clear_before_install(&tree);
        tree.flush_active_memtable(0)?;
        assert_cleared(folder.path(), tree, blob)?;
    }
    Ok(())
}

/// The tables of a flush a clear declined do not come back after a power loss
/// that left them on disk: they describe themselves, and the clear's manifest
/// decides that flush. Guards against cleared keys returning through an open
/// that installs self-described flushes.
#[test]
fn a_flush_a_clear_declined_stays_out_after_a_power_loss() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    for blob in [false, true] {
        let crash = crate::fs::CrashFs::new(crate::fs::MemFs::new());
        let config = |fs: Arc<dyn crate::fs::Fs>| {
            let config = Config::new(
                "/db",
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            )
            .with_shared_fs(fs);
            if blob {
                config.with_kv_separation(Some(
                    crate::KvSeparationOptions::default().separation_threshold(1),
                ))
            } else {
                config
            }
        };
        let fs = Arc::new(FaultFs::new(crash.clone()));
        let tree = config(fs.clone()).open()?;
        write_keys(&tree, 1);
        clear_before_install(&tree);
        // The declined files stay where the flush synced them, as when the
        // power goes before their removal.
        for op in [FaultOp::RemoveFile, FaultOp::SetLen, FaultOp::Rename] {
            fs.injector().arm(FaultRule::new(
                op,
                Fault::Error(crate::io::ErrorKind::Other),
            ));
        }
        tree.flush_active_memtable(0)?;
        crash.crash();
        drop(tree);

        let tree = config(Arc::new(crash)).open()?;
        assert_eq!(
            tree.iter(SeqNo::MAX, None).count(),
            0,
            "cleared keys came back, blob: {blob}"
        );
        assert_eq!(tree.table_count(), 0);
    }
    Ok(())
}

/// The tables of a flush a clear declined, left on the disk by a power loss,
/// stay out of a repair of the manifest lost with it: the clear's mark gives
/// the recency below which everything it dropped lies, and the flush took its
/// own with the memtables it wrote, before the clear. Guards against cleared
/// keys returning through a repair that keeps the flushes no manifest names.
#[test]
fn a_flush_a_clear_declined_stays_out_of_a_repair() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs};
    for blob in [false, true] {
        let crash = crate::fs::CrashFs::new(crate::fs::MemFs::new());
        let config = |fs: Arc<dyn Fs>| {
            let config = Config::new(
                "/db",
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            )
            .with_shared_fs(fs);
            if blob {
                config.with_kv_separation(Some(
                    crate::KvSeparationOptions::default().separation_threshold(1),
                ))
            } else {
                config
            }
        };
        let fs = Arc::new(FaultFs::new(crash.clone()));
        let tree = config(fs.clone()).open()?;
        let root = tree.tree_config().path.clone();
        let (tables, _) = tree.tree_config().tables_folder_for_level(0);
        write_keys(&tree, 1);
        clear_before_install(&tree);
        // The declined files stay where the flush synced them, as when the
        // power goes before their removal.
        for op in [FaultOp::RemoveFile, FaultOp::SetLen, FaultOp::Rename] {
            fs.injector().arm(FaultRule::new(
                op,
                Fault::Error(crate::io::ErrorKind::Other),
            ));
        }
        tree.flush_active_memtable(0)?;
        crash.crash();
        drop(tree);

        let mut left = 0;
        for entry in crash.read_dir(&tables)? {
            if matches!(
                crate::file::TableDirEntry::classify(&entry.file_name),
                crate::file::TableDirEntry::Table(_)
            ) && crash.metadata(&entry.path)?.len > 0
            {
                left += 1;
            }
        }
        assert!(
            left > 0,
            "self-check: the flush's table is on disk, blob: {blob}"
        );
        for entry in crash.read_dir(&root)? {
            let manifest = entry
                .file_name
                .strip_prefix('v')
                .or_else(|| entry.file_name.strip_prefix("edits-"))
                .is_some_and(|rest| rest.parse::<u64>().is_ok());
            if manifest {
                crash.remove_file(&entry.path)?;
            }
        }
        config(Arc::new(crash.clone())).repair()?;

        let tree = config(Arc::new(crash)).open()?;
        assert_eq!(
            tree.iter(SeqNo::MAX, None).count(),
            0,
            "cleared keys came back through a repair, blob: {blob}"
        );
    }
    Ok(())
}

/// A flush that returned survives a power loss after a clear that failed while
/// it was written, and a manifest written before its install. The failed clear
/// left the flush's memtables in place, so the flush installs; guards against
/// a manifest deciding its number while its tables were still to come, which
/// an open then reads as a flush that is gone.
#[test]
fn a_flush_a_failed_clear_raced_survives_a_power_loss() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    for blob in [false, true] {
        let crash = crate::fs::CrashFs::new(crate::fs::MemFs::new());
        let config = |fs: Arc<dyn crate::fs::Fs>| {
            let config = Config::new(
                "/db",
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            )
            .with_shared_fs(fs);
            if blob {
                config.with_kv_separation(Some(
                    crate::KvSeparationOptions::default().separation_threshold(1),
                ))
            } else {
                config
            }
        };
        let fs = Arc::new(FaultFs::new(crash.clone()));
        let tree = config(fs.clone()).open()?;
        tree.insert("a", "1", 0);
        tree.flush_active_memtable(0)?;
        tree.insert("b", "2", 1);

        let target = tree.clone();
        let injector = fs.clone();
        // A manifest written while the flush is in flight, its outcome taken
        // back once the flush returns.
        let persisted = Arc::new(parking_lot::Mutex::new(None));
        let outcome = Arc::clone(&persisted);
        tree.tree_config().arm_before_output_install(move || {
            injector.injector().arm(
                FaultRule::new(FaultOp::Write, Fault::Error(crate::io::ErrorKind::Other))
                    .on_path("edits")
                    .once(),
            );
            assert!(target.clear().is_err(), "self-check: the clear fails");
            *outcome.lock() = Some(target.persist_flushes());
        });
        tree.flush_active_memtable(0)?;
        let Some(persisted) = persisted.lock().take() else {
            panic!("the hook ran before the flush's install");
        };
        persisted?;
        assert_eq!(tree.get("b", SeqNo::MAX)?.as_deref(), Some(b"2".as_slice()));
        // The power goes now: the tree, dropped after it, writes nothing more.
        for op in [
            FaultOp::Open,
            FaultOp::Write,
            FaultOp::Rename,
            FaultOp::SyncDirectory,
        ] {
            fs.injector().arm(FaultRule::new(
                op,
                Fault::Error(crate::io::ErrorKind::Other),
            ));
        }
        crash.crash();
        drop(tree);

        let tree = config(Arc::new(crash)).open()?;
        assert_eq!(
            tree.get("a", SeqNo::MAX)?.as_deref(),
            Some(b"1".as_slice()),
            "blob: {blob}"
        );
        assert_eq!(
            tree.get("b", SeqNo::MAX)?.as_deref(),
            Some(b"2".as_slice()),
            "the flush that returned is gone, blob: {blob}"
        );
    }
    Ok(())
}

/// A synced flush after a `clear` whose manifest append failed rotates the
/// manifest before it returns: the record that append left at the end of the
/// edit log may reach the disk torn, which an open in the default recovery
/// mode refuses the store over. Guards against the flush returning with only
/// its tables to describe it, and the open after a power loss refusing the
/// store it was acknowledged in.
#[test]
fn a_flush_after_a_failed_manifest_append_survives_a_power_loss() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs as _, FsOpenOptions};
    let crash = crate::fs::CrashFs::new(crate::fs::MemFs::new());
    let config = |fs: Arc<dyn crate::fs::Fs>| {
        Config::new(
            "/db",
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(fs)
    };
    let fs = Arc::new(FaultFs::new(crash.clone()));
    let tree = config(fs.clone()).open()?;
    tree.insert("a", "1", 0);
    tree.flush_active_memtable(0)?;

    // The clear's append writes its record and fails to sync it.
    fs.injector().arm(
        FaultRule::new(FaultOp::SyncData, Fault::Error(crate::io::ErrorKind::Other))
            .on_path("edits")
            .once(),
    );
    assert!(tree.clear().is_err(), "self-check: the clear fails");
    // The kernel writes part of that record back on its own: the log ends
    // torn on the disk. The folder as the config holds it, which a platform
    // may have made absolute.
    let log = crash
        .read_dir(&tree.tree_config().path)?
        .into_iter()
        .find(|entry| entry.file_name.starts_with("edits-"))
        .map(|entry| entry.path);
    let Some(log) = log else {
        panic!("the manifest has an edit log");
    };
    let file = crash.open(&log, &FsOpenOptions::new().write(true))?;
    let len = file.metadata()?.len;
    assert!(len > 2, "self-check: the record was written");
    file.set_len(len - 2)?;
    file.sync_data()?;
    drop(file);

    tree.insert("b", "2", 1);
    tree.flush_active_memtable(0)?;
    // The power goes now: the tree, dropped after it, writes nothing more.
    for op in [
        FaultOp::Open,
        FaultOp::Write,
        FaultOp::Rename,
        FaultOp::SyncDirectory,
    ] {
        fs.injector().arm(FaultRule::new(
            op,
            Fault::Error(crate::io::ErrorKind::Other),
        ));
    }
    crash.crash();
    drop(tree);

    let tree = config(Arc::new(crash)).open()?;
    assert_eq!(tree.get("a", SeqNo::MAX)?.as_deref(), Some(b"1".as_slice()));
    assert_eq!(
        tree.get("b", SeqNo::MAX)?.as_deref(),
        Some(b"2".as_slice()),
        "the flush that returned"
    );
    Ok(())
}

/// A merge whose input tables `clear` dropped installs nothing.
#[test]
fn a_merge_after_a_clear_installs_nothing() -> crate::Result<()> {
    for blob in [false, true] {
        let folder = tempfile::tempdir()?;
        let tree = open(folder.path(), blob)?;
        write_keys(&tree, 1);
        tree.flush_active_memtable(0)?;
        write_keys(&tree, 1_000);
        tree.flush_active_memtable(0)?;
        clear_before_install(&tree);
        tree.major_compact(u64::MAX, 0)?;
        assert_cleared(folder.path(), tree, blob)?;
    }
    Ok(())
}

/// An insert that takes its seqno after a `clear` took its own, while the
/// clear persists, survives the clear: the clear drops only what its seqno
/// covers. Guards against the insert landing in the memtable the clear is
/// about to discard, gone for snapshots the retention floor still accepts.
#[test]
fn an_insert_while_a_clear_persists_survives_it() -> crate::Result<()> {
    for blob in [false, true] {
        let folder = tempfile::tempdir()?;
        let tree = open(folder.path(), blob)?;
        write_keys(&tree, 1);
        let history = match &tree {
            AnyTree::Standard(tree) => Arc::clone(&tree.version_history),
            AnyTree::Blob(tree) => Arc::clone(&tree.index.version_history),
        };

        let writer_tree = tree.clone();
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        let done_rx = std::sync::Mutex::new(done_rx);
        let writer = Arc::new(std::sync::Mutex::new(None));
        let writer_slot = Arc::clone(&writer);
        history.write().before_persist = Some(Box::new(move || {
            let tree = writer_tree.clone();
            let done_tx = done_tx.clone();
            writer_slot
                .lock()
                .expect("writer slot")
                .replace(std::thread::spawn(move || {
                    let seqno = tree.tree_config().seqno.next();
                    tree.insert("late", "kept", seqno);
                    done_tx.send(()).ok();
                }));
            // Room for the insert to land inside the window, if it can.
            done_rx
                .lock()
                .expect("receiver")
                .recv_timeout(std::time::Duration::from_millis(250))
                .ok();
        }));
        tree.clear()?;
        history.write().before_persist = None;
        let writer = writer.lock().expect("writer slot").take();
        writer.expect("the clear persisted").join().expect("writer");

        assert_eq!(
            tree.get("late", SeqNo::MAX)?.as_deref(),
            Some(b"kept".as_slice()),
            "blob: {blob}"
        );
        assert_eq!(tree.iter(SeqNo::MAX, None).count(), 1, "blob: {blob}");
    }
    Ok(())
}

/// A move whose tables `clear` dropped moves nothing and does not panic.
#[test]
fn a_move_after_a_clear_moves_nothing() -> crate::Result<()> {
    for blob in [false, true] {
        let folder = tempfile::tempdir()?;
        let tree = open(folder.path(), blob)?;
        write_keys(&tree, 1);
        tree.flush_active_memtable(0)?;
        clear_before_install(&tree);
        let strategy: Arc<dyn CompactionStrategy> = Arc::new(MoveDown(0, 1));
        tree.compact(strategy, 0)?;
        assert_cleared(folder.path(), tree, blob)?;
    }
    Ok(())
}

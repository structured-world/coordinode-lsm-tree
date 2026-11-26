// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! An ingestion that replaces a tree's whole content in one install: what a
//! reader, a later write at a low seqno and a power loss see.

use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
use crate::iter_guard::IterGuard as _;
use crate::{AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

fn config(path: &std::path::Path, blob: bool) -> Config {
    let config = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    );
    if blob {
        config.with_kv_separation(Some(
            crate::KvSeparationOptions::default().separation_threshold(1),
        ))
    } else {
        config
    }
}

fn config_on(fs: Arc<dyn crate::fs::Fs>, blob: bool) -> Config {
    config(std::path::Path::new("/db"), blob).with_shared_fs(fs)
}

/// The tree's content before a replace: a flushed table, a sealed memtable
/// and the active one, every key written at seqnos up to 100.
fn write_old(tree: &AnyTree) -> crate::Result<()> {
    tree.insert("shared", "old", 1);
    tree.insert("flushed", "old", 2);
    tree.flush_active_memtable(0)?;
    tree.insert("sealed", "old", 3);
    tree.rotate_memtable();
    tree.insert("active", "old", 100);
    Ok(())
}

fn replace(tree: &AnyTree, keys: &[(&str, &str)]) -> crate::Result<()> {
    let mut ingestion = tree.replacing_ingestion()?;
    for (key, value) in keys {
        ingestion.write(*key, *value)?;
    }
    ingestion.finish()
}

/// Closes `tree` and waits until its table file pool and background deleter
/// are done, so the files left are final and a reopen finds the folder
/// unlocked.
fn close(tree: AnyTree) {
    tree.close().wait();
}

fn value(tree: &AnyTree, key: &str) -> crate::Result<Option<Vec<u8>>> {
    Ok(tree.get(key, SeqNo::MAX)?.map(|v| v.to_vec()))
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

/// The replace marks in the tables folder under `path`, as
/// `(ingestion, bound)`, in order.
fn marks(path: &std::path::Path) -> Vec<(u64, u64)> {
    let mut marks: Vec<(u64, u64)> = std::fs::read_dir(path.join(crate::file::TABLES_FOLDER))
        .expect("the tables folder")
        .filter_map(Result::ok)
        .filter_map(|entry| {
            match crate::file::TableDirEntry::classify(&entry.file_name().to_string_lossy()) {
                crate::file::TableDirEntry::ReplaceMark(run, bound) => Some((run, bound)),
                _ => None,
            }
        })
        .collect();
    marks.sort_unstable();
    marks
}

/// The replace marks in the tables folder of `tree`, read through `fs`: the
/// folder is the one the tree itself resolved, whatever the platform makes of
/// its path.
fn marks_on(fs: &dyn crate::fs::Fs, tree: &AnyTree) -> crate::Result<Vec<(u64, u64)>> {
    let (folder, _) = tree.tree_config().tables_folder_for_level(0);
    let mut marks: Vec<(u64, u64)> = fs
        .read_dir(&folder)?
        .into_iter()
        .filter_map(
            |entry| match crate::file::TableDirEntry::classify(&entry.file_name) {
                crate::file::TableDirEntry::ReplaceMark(run, bound) => Some((run, bound)),
                _ => None,
            },
        )
        .collect();
    marks.sort_unstable();
    Ok(marks)
}

/// The install of a replace leaves one mark for all of its tables; an open
/// puts back a mark that is missing, and a checkpoint takes it along. The
/// mark stays while the tree is open, past the last table of its run, and the
/// next open, which removes what the install dropped, removes it. A repair
/// without a manifest reads the install from the mark, so a run without one
/// reads as never installed.
#[test]
fn a_replace_leaves_one_mark_until_an_open_finds_its_run_gone() -> crate::Result<()> {
    for blob in [false, true] {
        let folder = tempfile::tempdir()?;
        let open = || config(folder.path(), blob).table_target_size(4_096).open();
        let tree = open()?;
        write_old(&tree)?;
        assert!(marks(folder.path()).is_empty(), "blob: {blob}");

        let mut ingestion = tree.replacing_ingestion()?;
        for x in 0..20_000u64 {
            ingestion.write(x.to_be_bytes(), b"new")?;
        }
        ingestion.finish()?;
        assert!(
            tree.table_count() > 1,
            "self-check: several tables, blob: {blob}"
        );
        let marked = marks(folder.path());
        let [(run, bound)] = marked[..] else {
            panic!("one mark for the install, blob: {blob}: {marked:?}");
        };
        assert!(run < bound, "the bound is taken at the install: {marked:?}");
        close(tree);

        // An install the power cut before its mark: the open leaves it.
        std::fs::remove_file(
            folder
                .path()
                .join(crate::file::TABLES_FOLDER)
                .join(format!("{run}.replaced-{bound}")),
        )?;
        let tree = open()?;
        assert_eq!(marks(folder.path()), marked, "blob: {blob}");

        let target = tempfile::tempdir()?;
        let checkpoint = target.path().join("checkpoint");
        tree.create_checkpoint(&checkpoint)?;
        assert_eq!(marks(&checkpoint), marked, "blob: {blob}");

        tree.insert("later", "x", 5);
        tree.flush_active_memtable(0)?;
        tree.major_compact(u64::MAX, 0)?;
        close(tree);
        assert_eq!(
            marks(folder.path()),
            marked,
            "the bound holds while the tree is open, blob: {blob}"
        );

        close(open()?);
        assert!(
            marks(folder.path()).is_empty(),
            "an open found the run gone and kept its mark, blob: {blob}"
        );
    }
    Ok(())
}

/// A replace that ingested nothing leaves its mark too: the install dropped
/// the earlier tables all the same. The next open removes it.
#[test]
fn an_empty_replace_leaves_its_mark() -> crate::Result<()> {
    for blob in [false, true] {
        let folder = tempfile::tempdir()?;
        let tree = config(folder.path(), blob).open()?;
        write_old(&tree)?;
        replace(&tree, &[])?;
        assert_eq!(marks(folder.path()).len(), 1, "blob: {blob}");
        close(tree);
        close(config(folder.path(), blob).open()?);
        assert!(marks(folder.path()).is_empty(), "blob: {blob}");
    }
    Ok(())
}

/// A replace that fails at its install marks nothing: a mark says the install
/// is durable.
#[test]
fn a_replace_that_failed_leaves_no_mark() -> crate::Result<()> {
    for blob in [false, true] {
        let fs = Arc::new(FaultFs::new(crate::fs::MemFs::new()));
        let tree = config_on(fs.clone(), blob).open()?;
        tree.insert("a", "old", 1);
        tree.flush_active_memtable(0)?;
        tree.persist_flushes()?;

        fs.injector().arm(
            FaultRule::new(FaultOp::Write, Fault::Error(crate::io::ErrorKind::Other))
                .on_path("edits"),
        );
        assert!(
            replace(&tree, &[("new", "1")]).is_err(),
            "self-check: the replace fails"
        );
        fs.injector().clear();
        assert!(marks_on(&*fs, &tree)?.is_empty(), "blob: {blob}");
    }
    Ok(())
}

/// A mark that cannot be made does not fail the replace, which is installed
/// and durable by then: the replace returns, reads the new content, and the
/// next open leaves the mark the manifest owes.
#[test]
fn a_replace_whose_mark_fails_is_marked_by_the_next_open() -> crate::Result<()> {
    for blob in [false, true] {
        let fs = Arc::new(FaultFs::new(crate::fs::MemFs::new()));
        let tree = config_on(fs.clone(), blob).open()?;
        write_old(&tree)?;

        fs.injector().arm(
            FaultRule::new(FaultOp::Open, Fault::Error(crate::io::ErrorKind::Other))
                .on_path(".replaced-"),
        );
        replace(&tree, &[("new", "1")])?;
        fs.injector().clear();
        assert_eq!(value(&tree, "new")?.as_deref(), Some(b"1".as_slice()));
        assert!(marks_on(&*fs, &tree)?.is_empty(), "blob: {blob}");
        close(tree);

        let tree = config_on(fs.clone(), blob).open()?;
        assert_eq!(marks_on(&*fs, &tree)?.len(), 1, "blob: {blob}");
        assert_eq!(value(&tree, "new")?.as_deref(), Some(b"1".as_slice()));
    }
    Ok(())
}

/// A mark an open finds is not taken as durable: the sync after its making
/// may be the one that failed. The open syncs its folder, so the mark is
/// there after a power loss. Guards against an open that leaves a mark it
/// found as it is, which a power loss then takes back.
#[test]
fn an_open_makes_the_mark_it_finds_durable() -> crate::Result<()> {
    use crate::fs::Fs;
    for blob in [false, true] {
        let crash = crate::fs::CrashFs::new(crate::fs::MemFs::new());
        let fs: Arc<dyn Fs> = Arc::new(crash.clone());
        let tree = config_on(Arc::clone(&fs), blob).open()?;
        write_old(&tree)?;
        replace(&tree, &[("new", "1")])?;
        let marked = marks_on(&*fs, &tree)?;
        let [(run, bound)] = marked[..] else {
            panic!("one mark for the install, blob: {blob}: {marked:?}");
        };
        let (folder, _) = tree.tree_config().tables_folder_for_level(0);
        close(tree);

        // The mark as a failed folder sync leaves it: there, and not durable.
        let mark = crate::replace_mark::mark_path(&folder, run, bound);
        fs.remove_file(&mark)?;
        fs.sync_directory(&folder)?;
        drop(fs.open(
            &mark,
            &crate::fs::FsOpenOptions::new().write(true).create(true),
        )?);

        let tree = config_on(Arc::clone(&fs), blob).open()?;
        crash.crash();
        assert_eq!(marks_on(&*fs, &tree)?, marked, "blob: {blob}");
    }
    Ok(())
}

/// A name is a replace mark only in the spelling the engine writes: anything
/// else in the folder is an operator's file, which no scan removes.
#[test]
fn a_replace_mark_name_is_owned_in_the_engines_spelling_alone() {
    use crate::file::TableDirEntry;
    assert_eq!(
        TableDirEntry::classify("5.replaced-9"),
        TableDirEntry::ReplaceMark(5, 9)
    );
    for foreign in [
        "5.replaced-",
        ".replaced-9",
        "05.replaced-9",
        "5.replaced-09",
        "5.replaced-backup",
        "5.replaced-9.tmp",
    ] {
        assert_eq!(
            TableDirEntry::classify(foreign),
            TableDirEntry::Foreign,
            "{foreign}"
        );
    }
}

/// After a replace the tree holds the ingested keys alone: none of the
/// flushed, sealed or active content stays, and the earlier tables and blob
/// files are deleted once nothing holds them, as the reopen confirms.
#[test]
fn a_replace_leaves_only_the_ingested_content() -> crate::Result<()> {
    for blob in [false, true] {
        let folder = tempfile::tempdir()?;
        let tree = config(folder.path(), blob).open()?;
        write_old(&tree)?;
        replace(&tree, &[("new", "1"), ("shared", "new")])?;

        for key in ["flushed", "sealed", "active"] {
            assert_eq!(value(&tree, key)?, None, "{key} stayed, blob: {blob}");
        }
        assert_eq!(value(&tree, "shared")?.as_deref(), Some(b"new".as_slice()));
        assert_eq!(value(&tree, "new")?.as_deref(), Some(b"1".as_slice()));
        assert_eq!(tree.iter(SeqNo::MAX, None).count(), 2, "blob: {blob}");
        let tables = tree.table_count();
        let blob_files = tree.blob_file_count();
        assert_eq!(
            blob_files > 0,
            blob,
            "self-check: blob files only in a blob tree"
        );
        close(tree);
        assert_eq!(
            files_in(folder.path(), crate::file::TABLES_FOLDER),
            tables,
            "an earlier table stayed, blob: {blob}"
        );
        assert_eq!(
            files_in(folder.path(), crate::file::BLOBS_FOLDER),
            blob_files,
            "an earlier blob file stayed"
        );

        let tree = config(folder.path(), blob).open()?;
        assert_eq!(value(&tree, "flushed")?, None, "blob: {blob}");
        assert_eq!(value(&tree, "shared")?.as_deref(), Some(b"new".as_slice()));
        assert_eq!(tree.iter(SeqNo::MAX, None).count(), 2, "blob: {blob}");
    }
    Ok(())
}

/// A replace that ingested nothing empties the tree, as `clear` does.
#[test]
fn an_empty_replace_empties_the_tree() -> crate::Result<()> {
    for blob in [false, true] {
        let folder = tempfile::tempdir()?;
        let tree = config(folder.path(), blob).open()?;
        write_old(&tree)?;
        replace(&tree, &[])?;
        assert_eq!(tree.iter(SeqNo::MAX, None).count(), 0, "blob: {blob}");
        assert_eq!(tree.table_count(), 0);
        assert_eq!(tree.blob_file_count(), 0);
        close(tree);

        let tree = config(folder.path(), blob).open()?;
        assert_eq!(tree.iter(SeqNo::MAX, None).count(), 0, "blob: {blob}");
    }
    Ok(())
}

/// A key the earlier content and the ingestion both hold reads present to a
/// reader running through every replace: the install swaps the content in
/// one step. Guards against a replace built from a clear and a later install,
/// which leaves a window where the key is gone.
#[test]
fn a_reader_never_sees_a_shared_key_absent() -> crate::Result<()> {
    for blob in [false, true] {
        let folder = tempfile::tempdir()?;
        let tree = config(folder.path(), blob).open()?;
        tree.insert("shared", "0", 1);
        tree.flush_active_memtable(0)?;

        let done = Arc::new(AtomicBool::new(false));
        let (started, reading) = std::sync::mpsc::channel();
        let reader = {
            let tree = tree.clone();
            let done = Arc::clone(&done);
            std::thread::spawn(move || -> crate::Result<usize> {
                let mut reads = 0;
                loop {
                    assert!(
                        tree.get("shared", SeqNo::MAX)?.is_some(),
                        "the shared key read absent after {reads} reads"
                    );
                    reads += 1;
                    if reads == 1 {
                        // The replaces start once the reader runs.
                        started.send(()).ok();
                    }
                    if done.load(Ordering::Acquire) {
                        return Ok(reads);
                    }
                }
            })
        };
        reading.recv().expect("the reader started");
        let replaced = (0..50).try_for_each(|round| {
            let value = round.to_string();
            replace(&tree, &[("other", "x"), ("shared", &value)])
        });
        // The reader stops before a failed replace is reported, so it does not
        // outlive the test.
        done.store(true, Ordering::Release);
        reader.join().expect("reader")?;
        replaced?;
        assert_eq!(value(&tree, "shared")?.as_deref(), Some(b"49".as_slice()));
    }
    Ok(())
}

/// A write at a seqno above 0 and below the tree's current seqno, as an entry
/// applied after a snapshot install keeps its commit timestamp, reads over the
/// ingested row, before and after a flush and a compaction and a reopen. The
/// ingested rows sit at seqno 0, below every such write.
#[test]
fn a_write_below_the_current_seqno_reads_over_the_ingested_row() -> crate::Result<()> {
    for blob in [false, true] {
        let folder = tempfile::tempdir()?;
        let tree = config(folder.path(), blob).open()?;
        write_old(&tree)?;
        tree.tree_config().seqno.fetch_max(1_000);
        replace(&tree, &[("k", "ingested"), ("kept", "ingested")])?;
        assert!(
            tree.tree_config().seqno.get() > 5,
            "self-check: the write goes below the current seqno"
        );

        tree.insert("k", "later", 5);
        assert_eq!(value(&tree, "k")?.as_deref(), Some(b"later".as_slice()));
        tree.flush_active_memtable(0)?;
        assert_eq!(value(&tree, "k")?.as_deref(), Some(b"later".as_slice()));
        tree.major_compact(u64::MAX, SeqNo::MAX)?;
        assert_eq!(
            value(&tree, "k")?.as_deref(),
            Some(b"later".as_slice()),
            "blob: {blob}"
        );
        assert_eq!(
            value(&tree, "kept")?.as_deref(),
            Some(b"ingested".as_slice())
        );
        close(tree);

        let tree = config(folder.path(), blob).open()?;
        assert_eq!(value(&tree, "k")?.as_deref(), Some(b"later".as_slice()));
        assert_eq!(
            value(&tree, "kept")?.as_deref(),
            Some(b"ingested".as_slice())
        );
    }
    Ok(())
}

/// A read at a seqno the replace dropped data below is refused, not answered
/// from the new content alone, after a reopen too.
#[test]
fn a_read_below_a_replace_is_refused() -> crate::Result<()> {
    for blob in [false, true] {
        let folder = tempfile::tempdir()?;
        let tree = config(folder.path(), blob).open()?;
        write_old(&tree)?;
        // The writer's seqnos run ahead of the content's, as they do for a
        // caller that hands out seqnos from the tree's counter.
        tree.tree_config().seqno.fetch_max(1_000);
        replace(&tree, &[("new", "1")])?;
        let refused = |tree: &AnyTree| {
            matches!(
                tree.get("new", 500),
                Err(crate::Error::SnapshotBelowRetention { .. })
            )
        };
        assert!(refused(&tree), "blob: {blob}");
        assert_eq!(value(&tree, "new")?.as_deref(), Some(b"1".as_slice()));
        close(tree);
        let tree = config(folder.path(), blob).open()?;
        assert!(refused(&tree), "after a reopen, blob: {blob}");
    }
    Ok(())
}

/// A snapshot read under way when the replace lands keeps the earlier
/// content: the version it holds keeps its files until it drops.
#[test]
fn an_iterator_opened_before_a_replace_reads_the_earlier_content() -> crate::Result<()> {
    for blob in [false, true] {
        let folder = tempfile::tempdir()?;
        let tree = config(folder.path(), blob).open()?;
        for i in 0..100u64 {
            tree.insert(format!("key{i:03}"), vec![b'v'; 64], i + 1);
        }
        tree.flush_active_memtable(0)?;
        let mut before = tree.iter(SeqNo::MAX, None);
        let (first_key, first_value) = before.next().expect("a first key").into_inner()?;
        replace(&tree, &[("new", "1")])?;
        assert_eq!(&*first_key, b"key000");
        assert_eq!(&*first_value, [b'v'; 64].as_slice());
        // Every row the earlier version held, key and value, read after the
        // replace: a blob tree's values come from the earlier blob files.
        let mut rows = 1;
        for (i, guard) in (1..).zip(before) {
            let (key, value) = guard.into_inner()?;
            assert_eq!(&*key, format!("key{i:03}").as_bytes(), "blob: {blob}");
            assert_eq!(&*value, [b'v'; 64].as_slice(), "blob: {blob}");
            rows += 1;
        }
        assert_eq!(rows, 100, "blob: {blob}");
        assert_eq!(tree.iter(SeqNo::MAX, None).count(), 1, "blob: {blob}");
    }
    Ok(())
}

/// A merge whose inputs the replace dropped installs nothing: its outputs
/// were written from the earlier content. A merge writes with the compaction
/// state released, so a replace can land between its write and its install;
/// a move holds that state from its choice to its install, and a replace
/// waits for it.
#[test]
fn a_merge_across_a_replace_installs_nothing() -> crate::Result<()> {
    for blob in [false, true] {
        let folder = tempfile::tempdir()?;
        let tree = config(folder.path(), blob).open()?;
        tree.insert("a", "old", 1);
        tree.flush_active_memtable(0)?;
        tree.insert("b", "old", 2);
        tree.flush_active_memtable(0)?;

        let target = tree.clone();
        tree.tree_config().arm_before_output_install(move || {
            replace(&target, &[("new", "1")]).expect("replace");
        });
        tree.major_compact(u64::MAX, 0)?;
        assert_eq!(value(&tree, "a")?, None, "blob: {blob}");
        assert_eq!(value(&tree, "b")?, None, "blob: {blob}");
        assert_eq!(tree.iter(SeqNo::MAX, None).count(), 1, "blob: {blob}");
        close(tree);

        let tree = config(folder.path(), blob).open()?;
        assert_eq!(tree.iter(SeqNo::MAX, None).count(), 1, "blob: {blob}");
    }
    Ok(())
}

/// A power loss after a replace returned opens the ingested content alone,
/// even when the earlier tables and blob files are still on the disk and the
/// earlier flushes described only themselves. Guards against an open adopting
/// a self-described flush of the dropped content back.
#[test]
fn a_replace_that_returned_survives_a_power_loss() -> crate::Result<()> {
    for blob in [false, true] {
        let crash = crate::fs::CrashFs::new(crate::fs::MemFs::new());
        let fs = Arc::new(FaultFs::new(crash.clone()));
        let tree = config_on(fs.clone(), blob).open()?;
        write_old(&tree)?;
        tree.flush_active_memtable(0)?;
        // The earlier files stay where they were, as when the power goes
        // before their removal.
        for op in [FaultOp::RemoveFile, FaultOp::SetLen] {
            fs.injector().arm(FaultRule::new(
                op,
                Fault::Error(crate::io::ErrorKind::Other),
            ));
        }
        replace(&tree, &[("new", "1"), ("shared", "new")])?;
        // The power goes now: the tree, dropped after it, writes nothing more,
        // so the reopen sees the replace only if it was durable on return.
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

        let tree = config_on(Arc::new(crash), blob).open()?;
        assert_eq!(value(&tree, "flushed")?, None, "blob: {blob}");
        assert_eq!(value(&tree, "sealed")?, None, "blob: {blob}");
        assert_eq!(value(&tree, "shared")?.as_deref(), Some(b"new".as_slice()));
        assert_eq!(tree.iter(SeqNo::MAX, None).count(), 2, "blob: {blob}");
    }
    Ok(())
}

/// A replace whose written tables do not open for the install fails and
/// leaves the earlier content, memtable rows included: nothing is dropped
/// before the new run is in hand. The tree takes a replace again afterwards.
#[test]
fn a_replace_whose_tables_do_not_open_keeps_the_earlier_content() -> crate::Result<()> {
    for blob in [false, true] {
        let fs = Arc::new(FaultFs::new(crate::fs::MemFs::new()));
        let tree = config_on(fs.clone(), blob).open()?;
        write_old(&tree)?;

        // Armed once the ingestion's writer has finished its tables, so the
        // open that fails is the install's.
        let injector = fs.clone();
        tree.tree_config().arm_before_output_install(move || {
            injector.injector().arm(
                FaultRule::new(FaultOp::Open, Fault::Error(crate::io::ErrorKind::Other))
                    .on_path(crate::file::TABLES_FOLDER),
            );
        });
        assert!(
            replace(&tree, &[("new", "1")]).is_err(),
            "self-check: the replace fails, blob: {blob}"
        );
        fs.injector().clear();

        for key in ["shared", "flushed", "sealed", "active"] {
            assert_eq!(
                value(&tree, key)?.as_deref(),
                Some(b"old".as_slice()),
                "{key}, blob: {blob}"
            );
        }
        assert_eq!(value(&tree, "new")?, None, "blob: {blob}");

        replace(&tree, &[("new", "2")])?;
        assert_eq!(value(&tree, "new")?.as_deref(), Some(b"2".as_slice()));
        assert_eq!(tree.iter(SeqNo::MAX, None).count(), 1, "blob: {blob}");
    }
    Ok(())
}

/// A replace whose manifest write fails leaves the earlier content, and a
/// power loss after it opens the earlier content alone: the ingested files,
/// named by no manifest, are not adopted.
#[test]
fn a_replace_that_failed_keeps_the_earlier_content_through_a_power_loss() -> crate::Result<()> {
    for blob in [false, true] {
        let crash = crate::fs::CrashFs::new(crate::fs::MemFs::new());
        let fs = Arc::new(FaultFs::new(crash.clone()));
        let tree = config_on(fs.clone(), blob).open()?;
        tree.insert("a", "old", 1);
        tree.flush_active_memtable(0)?;
        tree.persist_flushes()?;

        fs.injector().arm(
            FaultRule::new(FaultOp::Write, Fault::Error(crate::io::ErrorKind::Other))
                .on_path("edits"),
        );
        assert!(
            replace(&tree, &[("new", "1")]).is_err(),
            "self-check: the replace fails"
        );
        assert_eq!(value(&tree, "a")?.as_deref(), Some(b"old".as_slice()));
        assert_eq!(value(&tree, "new")?, None, "blob: {blob}");
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

        let tree = config_on(Arc::new(crash), blob).open()?;
        assert_eq!(value(&tree, "a")?.as_deref(), Some(b"old".as_slice()));
        assert_eq!(value(&tree, "new")?, None, "blob: {blob}");
    }
    Ok(())
}

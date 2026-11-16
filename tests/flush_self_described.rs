// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A synced flush writes no manifest edit: its tables carry the flush's number,
//! their place in its run and the run's length, and it returns once they and
//! their folders are synced. The open installs every whole flush above the
//! manifest's watermark from the tables themselves, and leaves out a flush a
//! power loss cut short, which never returned.

use lsm_tree::fs::{CrashFs, Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
use lsm_tree::{AbstractTree, AnyTree, Config, KvSeparationOptions, SequenceNumberCounter};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use test_log::test;

const DB: &str = "/db";

/// `path` as a tree opened at it names it.
fn at(path: &str) -> PathBuf {
    std::path::absolute(path).expect("an absolute path")
}

/// A disk a power loss can take, and the backend a tree runs on it through.
struct Disk {
    crash: CrashFs,
    fs: Arc<FaultFs<CrashFs>>,
}

impl Disk {
    fn new() -> Self {
        Self::over(CrashFs::new(MemFs::new()))
    }

    /// The disk `crash` as it came back from a power loss.
    fn over(crash: CrashFs) -> Self {
        Self {
            fs: Arc::new(FaultFs::new(crash.clone())),
            crash,
        }
    }

    fn fs(&self) -> Arc<dyn Fs> {
        self.fs.clone()
    }

    /// Fails the next `op` on a path containing `path`.
    fn fail_once(&self, op: FaultOp, path: &str) {
        self.fs.injector().arm(
            FaultRule::new(op, Fault::Error(lsm_tree::io::ErrorKind::Other))
                .on_path(path)
                .once(),
        );
    }

    /// Cuts the power under `tree` and returns the disk as it comes back:
    /// what was never synced is gone, and the tree, dropped after it, changes
    /// nothing on its way out.
    fn power_loss(self, tree: AnyTree) -> CrashFs {
        for op in [
            FaultOp::Open,
            FaultOp::RemoveFile,
            FaultOp::Rename,
            FaultOp::CreateDir,
            FaultOp::CreateDirAll,
            FaultOp::SyncDirectory,
            FaultOp::HardLink,
            FaultOp::RemoveDir,
            FaultOp::RemoveDirAll,
        ] {
            self.fs.injector().arm(FaultRule::new(
                op,
                Fault::Error(lsm_tree::io::ErrorKind::Other),
            ));
        }
        self.crash.crash();
        drop(tree);
        self.crash
    }
}

/// A tree at [`DB`] on `fs`, without a table file pool: a flush makes its
/// table's directory entry, so losing the folder's sync loses the table.
fn config(fs: Arc<dyn Fs>) -> Config {
    Config::new(
        DB,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(fs)
    .table_file_pool(0)
}

/// [`config`] with every value kept apart in blob files.
fn blob_config(fs: Arc<dyn Fs>) -> Config {
    config(fs).with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
}

fn value(tree: &AnyTree, key: &str) -> Option<String> {
    tree.get(key, u64::MAX)
        .expect("get")
        .map(|v| String::from_utf8(v.to_vec()).expect("utf-8 value"))
}

/// The files in `folder` on `fs` named like tables or blob files and holding
/// data.
fn data_files(fs: &dyn Fs, folder: &Path) -> Vec<u64> {
    let mut ids: Vec<u64> = fs
        .read_dir(folder)
        .map(|entries| {
            entries
                .into_iter()
                .filter_map(|entry| {
                    let id = entry.file_name.parse::<u64>().ok()?;
                    let len = fs.metadata(&folder.join(&entry.file_name)).ok()?.len;
                    (len > 0).then_some(id)
                })
                .collect()
        })
        .unwrap_or_default();
    ids.sort_unstable();
    ids
}

/// The bytes the manifest's edit logs at [`DB`] hold, all generations.
fn edit_log_bytes(fs: &dyn Fs) -> u64 {
    fs.read_dir(&at(DB))
        .expect("the tree's folder")
        .into_iter()
        .filter(|entry| entry.file_name.starts_with("edits-"))
        .map(|entry| {
            fs.metadata(&at(DB).join(&entry.file_name))
                .expect("metadata")
                .len
        })
        .sum()
}

/// Flushes that returned survive a power loss right after them, tables and
/// blob files, though no manifest edit names them. Guards against a flush
/// returning before its files are synced, and against an open that installs
/// only what the manifest names.
#[test]
fn flushes_that_returned_survive_a_power_loss_without_an_edit() -> lsm_tree::Result<()> {
    let big = "x".repeat(4_096);
    let disk = Disk::new();
    let tree = blob_config(disk.fs()).open()?;
    let logged = edit_log_bytes(&disk.crash);
    for (seqno, key) in [(0, "a"), (1, "b"), (2, "c")] {
        tree.insert(key, &big, seqno);
        tree.flush_active_memtable(0)?;
    }
    assert_eq!(
        edit_log_bytes(&disk.crash),
        logged,
        "a synced flush writes no manifest edit"
    );
    let crash = disk.power_loss(tree);

    let disk = Disk::over(crash);
    let tree = blob_config(disk.fs()).open()?;
    for key in ["a", "b", "c"] {
        assert_eq!(value(&tree, key).as_deref(), Some(big.as_str()), "{key}");
    }
    assert_eq!(tree.table_count(), 3);
    assert_eq!(tree.blob_file_count(), 3);

    // The open recorded them: a further flush and power loss keep all four.
    tree.insert("d", &big, 3);
    tree.flush_active_memtable(0)?;
    let crash = disk.power_loss(tree);
    let tree = blob_config(Arc::new(crash)).open()?;
    for key in ["a", "b", "c", "d"] {
        assert_eq!(value(&tree, key).as_deref(), Some(big.as_str()), "{key}");
    }
    assert_eq!(tree.table_count(), 4);
    Ok(())
}

/// A flush whose table sync fails never returned, and is gone after a power
/// loss; the flush before it stays, and a later one survives the next power
/// loss. Guards against an open that serves a table that never reached the
/// disk, and against a later flush taking the lost one's number and mixing
/// with what it left.
#[test]
fn a_flush_whose_table_did_not_reach_the_disk_is_gone() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let tree = config(disk.fs()).open()?;
    tree.insert("kept", "1", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("lost", "2", 1);
    disk.fail_once(FaultOp::SyncData, "tables");
    assert!(
        tree.flush_active_memtable(0).is_err(),
        "the flush fails on its table's sync"
    );
    let crash = disk.power_loss(tree);

    let disk = Disk::over(crash);
    let tree = config(disk.fs()).open()?;
    assert_eq!(value(&tree, "kept").as_deref(), Some("1"));
    assert_eq!(value(&tree, "lost"), None, "the flush never returned");
    assert_eq!(tree.table_count(), 1);

    tree.insert("after", "3", 2);
    tree.flush_active_memtable(0)?;
    let crash = disk.power_loss(tree);

    let tree = config(Arc::new(crash.clone())).open()?;
    assert_eq!(value(&tree, "kept").as_deref(), Some("1"));
    assert_eq!(value(&tree, "after").as_deref(), Some("3"));
    assert_eq!(value(&tree, "lost"), None);
    assert_eq!(tree.table_count(), 2);
    assert_eq!(
        data_files(&crash, &at(DB).join("tables")).len(),
        2,
        "no table of the lost flush is left on disk"
    );
    Ok(())
}

/// As above, with the table's bytes durable and its directory entry not.
/// Guards against an open that reads a missing table as an error.
#[test]
fn a_flush_whose_table_entry_did_not_reach_the_disk_is_gone() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let tree = config(disk.fs()).open()?;
    tree.insert("kept", "1", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("lost", "2", 1);
    disk.fail_once(FaultOp::SyncDirectory, "tables");
    assert!(
        tree.flush_active_memtable(0).is_err(),
        "the flush fails on its folder's sync"
    );
    let crash = disk.power_loss(tree);

    let tree = config(Arc::new(crash)).open()?;
    assert_eq!(value(&tree, "kept").as_deref(), Some("1"));
    assert_eq!(value(&tree, "lost"), None);
    assert_eq!(tree.table_count(), 1);
    Ok(())
}

/// A blob file that did not reach the disk leaves its flush out too: its
/// table may be whole, but it points at values that are gone. Guards against
/// an open that checks a flush's tables and not its blob files.
#[test]
fn a_flush_whose_blob_file_did_not_reach_the_disk_is_gone() -> lsm_tree::Result<()> {
    let big = "x".repeat(4_096);
    let disk = Disk::new();
    let tree = blob_config(disk.fs()).open()?;
    tree.insert("kept", &big, 0);
    tree.flush_active_memtable(0)?;

    tree.insert("lost", &big, 1);
    disk.fail_once(FaultOp::SyncData, "blobs");
    assert!(
        tree.flush_active_memtable(0).is_err(),
        "the flush fails on its blob file's sync"
    );
    let crash = disk.power_loss(tree);

    let tree = blob_config(Arc::new(crash)).open()?;
    assert_eq!(value(&tree, "kept").as_deref(), Some(big.as_str()));
    assert_eq!(value(&tree, "lost"), None);
    assert_eq!(tree.blob_file_count(), 1);
    Ok(())
}

/// A flush is installed whole or not at all: with one table of its run gone,
/// none of the others is served. Guards against an open that installs the
/// tables of a flush it finds, rather than the flush.
#[test]
fn a_flush_missing_a_table_of_its_run_is_left_out_whole() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let tree = config(disk.fs()).table_target_size(1_024).open()?;
    tree.insert("kept", "1", 0);
    tree.flush_active_memtable(0)?;
    let before = data_files(&disk.crash, &at(DB).join("tables"));

    let filler = "y".repeat(256);
    for i in 0..64u64 {
        tree.insert(format!("run-{i:03}"), &filler, 1 + i);
    }
    tree.flush_active_memtable(0)?;
    let run: Vec<u64> = data_files(&disk.crash, &at(DB).join("tables"))
        .into_iter()
        .filter(|id| !before.contains(id))
        .collect();
    assert!(
        run.len() >= 2,
        "self-check: the flush wrote a run of tables"
    );
    let crash = disk.power_loss(tree);

    crash.remove_file(&at(DB).join("tables").join(run[run.len() / 2].to_string()))?;
    let tree = config(Arc::new(crash)).table_target_size(1_024).open()?;
    assert_eq!(value(&tree, "kept").as_deref(), Some("1"));
    for i in 0..64u64 {
        assert_eq!(value(&tree, &format!("run-{i:03}")), None, "run-{i:03}");
    }
    assert_eq!(tree.table_count(), 1);
    Ok(())
}

/// A clear decides every flush before it: their tables, still on disk after a
/// power loss that took the clear's deletions, are not installed again.
/// Guards against cleared data coming back from a flush's own description.
#[test]
fn a_cleared_flush_does_not_come_back_after_a_power_loss() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let tree = config(disk.fs()).open()?;
    for (seqno, key) in [(0, "a"), (1, "b")] {
        tree.insert(key, "1", seqno);
        tree.flush_active_memtable(0)?;
    }
    tree.clear()?;
    let crash = disk.power_loss(tree);

    let tree = config(Arc::new(crash)).open()?;
    assert_eq!(value(&tree, "a"), None);
    assert_eq!(value(&tree, "b"), None);
    assert_eq!(tree.table_count(), 0);
    Ok(())
}

/// A compaction of flushed tables decides their flushes: the inputs it
/// replaced, still on disk after a power loss that took their deletion, are
/// not installed beside its output. Guards against a flush's tables coming
/// back next to the table that replaced them.
#[test]
fn a_compacted_flush_does_not_come_back_beside_its_output() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let tree = config(disk.fs()).open()?;
    tree.insert("a", "1", 0);
    tree.flush_active_memtable(0)?;
    tree.insert("a", "2", 1);
    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, 0)?;
    assert_eq!(tree.table_count(), 1);
    let crash = disk.power_loss(tree);

    let tree = config(Arc::new(crash)).open()?;
    assert_eq!(value(&tree, "a").as_deref(), Some("2"));
    assert_eq!(tree.table_count(), 1);
    Ok(())
}

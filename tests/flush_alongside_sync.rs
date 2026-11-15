// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A synced flush syncs its files at the same time as the manifest edit that
//! names them. A power loss can then keep the edit without its files; the open
//! that follows checks them, takes the edit back, and the tree is as it was
//! before the flush, which never returned.

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
        let crash = CrashFs::new(MemFs::new());
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

/// The edit is durable and the file it names is not: a flush whose table
/// sync fails after its edit was synced, then a power loss. The open takes the
/// edit back and opens on the flush before it; a later flush appends where
/// the edit was and survives the next power loss. Guards against an open that
/// trusts an edit whose table never reached the disk (it fails or serves a
/// table that is not there), and against an append after the taken-back edit,
/// which would replay it unchecked.
#[test]
fn an_edit_whose_table_did_not_reach_the_disk_is_taken_back() -> lsm_tree::Result<()> {
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

    let disk = Disk {
        fs: Arc::new(FaultFs::new(crash.clone())),
        crash,
    };
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

/// As above, with the table's bytes durable and its directory entry not: the
/// table is not there after the power loss. Guards against a check that reads
/// a missing file as an error that fails the open.
#[test]
fn an_edit_whose_table_entry_did_not_reach_the_disk_is_taken_back() -> lsm_tree::Result<()> {
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

/// A blob file that did not reach the disk takes back the edit too: its table
/// may be whole, but it points at values that are gone. Guards against a check
/// that covers the tables an edit adds and not its blob files.
#[test]
fn an_edit_whose_blob_file_did_not_reach_the_disk_is_taken_back() -> lsm_tree::Result<()> {
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

/// A flush that returned survives a power loss right after it, tables and
/// blob files: both syncs are done before it returns. Guards against a flush
/// returning before the file syncs it runs beside its edit.
#[test]
fn a_flush_that_returned_survives_a_power_loss() -> lsm_tree::Result<()> {
    let big = "x".repeat(4_096);
    let disk = Disk::new();
    let tree = blob_config(disk.fs()).open()?;
    for (seqno, key) in [(0, "a"), (1, "b"), (2, "c")] {
        tree.insert(key, &big, seqno);
        tree.flush_active_memtable(0)?;
    }
    let crash = disk.power_loss(tree);

    let tree = blob_config(Arc::new(crash)).open()?;
    for key in ["a", "b", "c"] {
        assert_eq!(value(&tree, key).as_deref(), Some(big.as_str()), "{key}");
    }
    assert_eq!(tree.table_count(), 3);
    assert_eq!(tree.blob_file_count(), 3);
    Ok(())
}

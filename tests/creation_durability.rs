// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! What a power loss keeps of a tree once each operation has returned: the
//! tree it created, the keys a flush made durable, the generation a
//! manifest rotation installed, whatever each one synced its new names with.

use lsm_tree::fs::{CrashFs, MemFs};
use lsm_tree::{AbstractTree, AnyTree, Config, KvSeparationOptions, SequenceNumberCounter};
use std::sync::Arc;

const DB: &str = "/db/tree";

fn config(fs: &CrashFs, blob: bool) -> Config {
    let config = Config::new(
        DB,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(fs.clone()));
    if blob {
        config.with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    } else {
        config
    }
}

fn reopen(fs: &CrashFs, blob: bool) -> AnyTree {
    config(fs, blob)
        .open()
        .expect("the tree reopens after the power loss")
}

/// A flush whose edit could not be synced is still in the log a later open
/// reads, and that open shows its keys as persisted, so a caller may trim its
/// own log by them. The open removes nothing, yet it makes the log it read
/// durable before it returns the tree: a power loss then keeps the keys the
/// open showed.
#[test]
fn an_open_that_removes_nothing_still_makes_the_log_it_read_durable() {
    use lsm_tree::fs::{Fault, FaultFs, FaultOp, FaultRule};
    let crash = CrashFs::new(MemFs::new());
    let fault = Arc::new(FaultFs::new(crash.clone()));
    let config = || {
        Config::new(
            DB,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(fault.clone())
    };
    {
        let tree = config().open().expect("create");
        tree.insert("key", "value", 0);
        fault.injector().arm(
            FaultRule::new(
                FaultOp::SyncData,
                Fault::Error(lsm_tree::io::ErrorKind::Other),
            )
            .on_path("edits-")
            .once(),
        );
        assert!(
            tree.flush_active_memtable(0).is_err(),
            "self-check: the flush's edit is not synced"
        );
    }
    {
        let tree = config().open().expect("an open over the unsynced edit");
        assert!(
            tree.get("key", u64::MAX).expect("get").is_some(),
            "self-check: the open shows the flushed key"
        );
    }

    crash.crash();
    let tree = config()
        .open()
        .expect("the tree opens after the power loss");
    assert!(
        tree.get("key", u64::MAX).expect("get").is_some(),
        "the key the open showed survives the power loss"
    );
}

/// A compaction whose edit could not be synced may or may not be in the log
/// after a power loss. An open before that power loss reads the edit and
/// removes the tables it took out: the open makes the log it read durable
/// before it removes any of them, so the power loss keeps the tables the log
/// it leaves names.
#[test]
fn an_open_makes_the_log_it_removes_tables_by_durable_first() {
    use lsm_tree::fs::{Fault, FaultFs, FaultOp, FaultRule};
    let crash = CrashFs::new(MemFs::new());
    let fault = Arc::new(FaultFs::new(crash.clone()));
    let config = || {
        Config::new(
            DB,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(fault.clone())
    };
    {
        let tree = config().open().expect("create");
        for i in 0..2u64 {
            tree.insert(format!("key{i}"), "value", i);
            tree.flush_active_memtable(0).expect("flush");
        }
        fault.injector().arm(
            FaultRule::new(
                FaultOp::SyncData,
                Fault::Error(lsm_tree::io::ErrorKind::Other),
            )
            .on_path("edits-")
            .once(),
        );
        assert!(
            tree.major_compact(u64::MAX, 0).is_err(),
            "self-check: the compaction's edit is not synced"
        );
    }
    drop(config().open().expect("an open over the unsynced edit"));

    crash.crash();
    let tree = config()
        .open()
        .expect("the tree opens after the power loss");
    for i in 0..2u64 {
        assert!(
            tree.get(format!("key{i}"), u64::MAX)
                .expect("get")
                .is_some(),
            "key{i}"
        );
    }
}

/// A tree that creation returned survives a power loss right after it, empty.
#[test]
fn a_created_tree_survives_a_power_loss() {
    for blob in [false, true] {
        let fs = CrashFs::new(MemFs::new());
        drop(config(&fs, blob).open().expect("create"));
        fs.crash();
        let tree = reopen(&fs, blob);
        assert_eq!(tree.table_count(), 0, "blob: {blob}");
    }
}

/// The keys of the first flush after creation survive a power loss: the
/// table's name and the edit naming it were durable when the flush returned.
#[test]
fn the_first_flush_after_creation_survives_a_power_loss() {
    for blob in [false, true] {
        let fs = CrashFs::new(MemFs::new());
        let tree = config(&fs, blob).open().expect("create");
        tree.insert("key", "value", 0);
        tree.flush_active_memtable(0).expect("flush");
        drop(tree);
        fs.crash();
        let tree = reopen(&fs, blob);
        assert_eq!(
            tree.get("key", u64::MAX).expect("get").as_deref(),
            Some(b"value".as_slice()),
            "blob: {blob}"
        );
    }
}

/// The values a KV-separated tree flushes after a reopen survive a power
/// loss: the blob folder was made with the tree, and the first edit after
/// the open syncs the tree folder before it names a blob file.
#[test]
fn a_reopened_kv_separated_tree_keeps_its_blobs_through_a_power_loss() {
    let fs = CrashFs::new(MemFs::new());
    drop(config(&fs, true).open().expect("create"));
    let tree = config(&fs, true).open().expect("reopen");
    tree.insert("key", "a value stored apart", 0);
    tree.flush_active_memtable(0).expect("flush");
    drop(tree);
    fs.crash();
    let tree = reopen(&fs, true);
    assert_eq!(
        tree.get("key", u64::MAX).expect("get").as_deref(),
        Some(b"a value stored apart".as_slice())
    );
}

/// A tree that lived holds its history in its edit log even when no table is
/// left (a dropped range, the retention floor it raised): a lost snapshot
/// over that log is refused, never taken for a fresh folder and created over.
/// The tree has rotated, so no log of the creation's generation is left to
/// stand in the way of a new one.
#[test]
fn an_open_without_a_snapshot_refuses_a_log_holding_edits() {
    use lsm_tree::fs::Fs;
    let mem = MemFs::new();
    let fs = CrashFs::new(mem.clone());
    {
        // The first install appends, the second rotates, the third appends
        // to the rotated generation's log.
        let tree = config(&fs, false)
            .manifest_log_rotate_bytes(1)
            .open()
            .expect("create");
        for i in 0..2u64 {
            tree.insert(format!("key{i}"), "value", i);
            tree.flush_active_memtable(0).expect("flush");
        }
        tree.drop_range::<&[u8], _>(..).expect("drop");
    }
    // The tree's path as the tree spells it: absolute (on Windows, with a
    // drive).
    let db = std::path::absolute(DB).expect("absolute path");
    assert!(
        mem.read_dir(&db.join("tables")).expect("tables").is_empty(),
        "self-check: no table is left"
    );
    let mut logs_with_edits = 0;
    for entry in mem.read_dir(&db).expect("tree folder") {
        if entry.file_name.starts_with('v') {
            mem.remove_file(&entry.path).expect("lose the snapshot");
        } else if entry.file_name.starts_with("edits-")
            && entry.file_name != "edits-0"
            && mem.metadata(&entry.path).expect("log").len > 0
        {
            logs_with_edits += 1;
        }
    }
    assert_eq!(
        logs_with_edits, 1,
        "self-check: a rotated generation's log holds the drop"
    );

    assert!(
        config(&fs, false).open().is_err(),
        "a log holding edits is a tree's state"
    );
}

/// A snapshot spelled otherwise than a tree writes it (`v00`, or an id past
/// the counter's range) is not something a creation cut short leaves: an
/// open refuses it rather than create a tree over it.
#[test]
fn an_open_refuses_a_snapshot_a_creation_never_names() {
    use lsm_tree::fs::{Fs, FsOpenOptions};
    for name in ["v00", "v18446744073709551616"] {
        let mem = MemFs::new();
        let fs = CrashFs::new(mem.clone());
        let db = std::path::absolute(DB).expect("absolute path");
        mem.create_dir_all(&db).expect("tree folder");
        mem.open(
            &db.join(name),
            &FsOpenOptions::new().write(true).create(true),
        )
        .expect("a snapshot file");

        assert!(
            config(&fs, false).open().is_err(),
            "{name} is a tree's state"
        );
    }
}

/// A tables folder that cannot be listed fails an open without a pointer:
/// whether it holds a tree's state is not known.
#[test]
fn an_open_fails_over_a_tables_folder_it_cannot_list() {
    use lsm_tree::fs::{Fs, FsOpenOptions};
    let mem = MemFs::new();
    let fs = CrashFs::new(mem.clone());
    let db = std::path::absolute(DB).expect("absolute path");
    mem.create_dir_all(&db).expect("tree folder");
    mem.open(
        &db.join("tables"),
        &FsOpenOptions::new().write(true).create(true),
    )
    .expect("a file where the tables folder goes");

    // The listing's own failure (`MemFs` lists a file as `Other`), not the
    // later one of making the folder over the file (`AlreadyExists`).
    match config(&fs, false).open() {
        Err(lsm_tree::Error::Io(e)) => assert_eq!(e.kind(), lsm_tree::io::ErrorKind::Other, "{e}"),
        Err(e) => panic!("not the listing's failure: {e}"),
        Ok(_) => panic!("an unlistable tables folder opened"),
    }
}

/// A backend over a [`MemFs`] that fails a probe or a sync of a creation.
struct FailFs {
    inner: MemFs,
    /// The tree folder: the probe of `fail_exists` fails once it exists.
    tree: std::path::PathBuf,
    fail_exists: Option<std::path::PathBuf>,
    /// The first sync of the first path fails, after another process made
    /// the second path, a file, in a folder the creation made.
    sibling_on_sync: std::sync::Mutex<Option<(std::path::PathBuf, std::path::PathBuf)>>,
    /// Another opener puts a file of its own in the tree folder right after
    /// this open makes the folder, before this open takes its lock.
    foreign_in_tree: bool,
}

impl lsm_tree::fs::Fs for FailFs {
    fn open(
        &self,
        path: &std::path::Path,
        opts: &lsm_tree::fs::FsOpenOptions,
    ) -> lsm_tree::io::Result<Box<dyn lsm_tree::fs::FsFile>> {
        self.inner.open(path, opts)
    }

    fn create_dir_all(&self, path: &std::path::Path) -> lsm_tree::io::Result<()> {
        let made = !self.inner.exists(path)?;
        self.inner.create_dir_all(path)?;
        if made && self.foreign_in_tree && path == self.tree {
            self.inner
                .open(
                    &self.tree.join("foreign"),
                    &lsm_tree::fs::FsOpenOptions::new().write(true).create(true),
                )
                .expect("another opener's file");
        }
        Ok(())
    }

    fn read_dir(
        &self,
        path: &std::path::Path,
    ) -> lsm_tree::io::Result<Vec<lsm_tree::fs::FsDirEntry>> {
        self.inner.read_dir(path)
    }

    fn remove_file(&self, path: &std::path::Path) -> lsm_tree::io::Result<()> {
        self.inner.remove_file(path)
    }

    fn remove_dir_all(&self, path: &std::path::Path) -> lsm_tree::io::Result<()> {
        self.inner.remove_dir_all(path)
    }

    fn remove_dir(&self, path: &std::path::Path) -> lsm_tree::io::Result<()> {
        self.inner.remove_dir(path)
    }

    fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> lsm_tree::io::Result<()> {
        self.inner.rename(from, to)
    }

    fn metadata(&self, path: &std::path::Path) -> lsm_tree::io::Result<lsm_tree::fs::FsMetadata> {
        self.inner.metadata(path)
    }

    fn sync_directory(&self, path: &std::path::Path) -> lsm_tree::io::Result<()> {
        let mut armed = self
            .sibling_on_sync
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if armed.as_ref().is_some_and(|(when, _)| when == path) {
            let (_, sibling) = armed.take().expect("armed");
            self.inner
                .open(
                    &sibling,
                    &lsm_tree::fs::FsOpenOptions::new().write(true).create(true),
                )
                .expect("another process's file");
            return Err(lsm_tree::io::Error::other("the sync fails"));
        }
        drop(armed);
        self.inner.sync_directory(path)
    }

    fn exists(&self, path: &std::path::Path) -> lsm_tree::io::Result<bool> {
        if self.fail_exists.as_deref() == Some(path) && self.inner.exists(&self.tree)? {
            return Err(lsm_tree::io::Error::other("the probe fails"));
        }
        self.inner.exists(path)
    }
}

/// `/base/new/tree` on a backend where `/base` exists, and that backend.
fn nested_tree(
    fail_exists: Option<&str>,
    sibling_on_sync: Option<(&str, &str)>,
    foreign_in_tree: bool,
) -> (MemFs, std::path::PathBuf, Config) {
    use lsm_tree::fs::Fs;
    let absolute = |path: &str| std::path::absolute(path).expect("absolute path");
    let inner = MemFs::new();
    inner.create_dir_all(&absolute("/base")).expect("base");
    let tree = absolute("/base/new/tree");
    let fs = FailFs {
        inner: inner.clone(),
        tree: tree.clone(),
        fail_exists: fail_exists.map(absolute),
        sibling_on_sync: std::sync::Mutex::new(
            sibling_on_sync.map(|(when, sibling)| (absolute(when), absolute(sibling))),
        ),
        foreign_in_tree,
    };
    let config = Config::new(
        &tree,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(fs));
    (inner, tree, config)
}

/// A failed creation takes back a folder it made above the tree only while
/// it is empty: another process that made an entry there since keeps it.
#[test]
fn a_failed_creation_keeps_what_another_process_made_above_the_tree() {
    use lsm_tree::fs::Fs;
    let (inner, tree, config) = nested_tree(None, Some(("/base/new", "/base/new/other")), false);
    assert!(config.open().is_err(), "the sync of /base/new fails");
    let other = std::path::absolute("/base/new/other").expect("absolute path");
    assert!(
        inner.exists(&other).expect("probe"),
        "another process's file survives the failed creation"
    );
    assert!(
        !inner.exists(&tree).expect("probe"),
        "the tree folder the creation made is taken back"
    );
}

/// A failure between the open making the tree folder and the creation
/// finishing takes back every folder the open made: a probe of the tree
/// folder in the creation, or one of the open itself before it.
#[test]
fn a_failed_open_takes_back_the_folders_it_made() {
    use lsm_tree::fs::Fs;
    for probe in ["/base/new/tree", "/base/new/tree/version"] {
        let (inner, _, config) = nested_tree(Some(probe), None, false);
        assert!(config.open().is_err(), "the probe of {probe} fails");
        let made = std::path::absolute("/base/new").expect("absolute path");
        assert!(
            !inner.exists(&made).expect("probe"),
            "{probe}: the folders the open made are taken back"
        );
    }
}

/// An open that fails before it creates anything takes back only what is
/// empty: what another opener put in the tree folder between this open
/// making it and taking its lock stays, and so does the folder.
#[test]
fn a_failed_open_keeps_what_another_opener_put_in_the_tree_folder() {
    use lsm_tree::fs::Fs;
    let (inner, tree, config) = nested_tree(Some("/base/new/tree/version"), None, true);
    assert!(
        config.open().is_err(),
        "the probe of the version marker fails"
    );
    assert!(
        inner.exists(&tree.join("foreign")).expect("probe"),
        "the other opener's file survives the failed open"
    );
    assert!(
        !inner.exists(&tree.join("LOCK")).expect("probe"),
        "this open's lock file is taken back"
    );
}

/// A level route inside the primary tables folder is a folder there, not a
/// table: an open without a pointer creates the tree over the route's empty
/// folder, made in advance or by a creation cut short.
#[test]
fn an_open_creates_over_an_empty_route_inside_the_tables_folder() {
    use lsm_tree::config::LevelRoute;
    use lsm_tree::fs::Fs;
    let mem = MemFs::new();
    let fs = CrashFs::new(mem.clone());
    let db = std::path::absolute(DB).expect("absolute path");
    let route = db.join("tables").join("archive");
    mem.create_dir_all(&route).expect("the route's folder");

    let tree = config(&fs, false)
        .level_routes(vec![LevelRoute {
            levels: 3..7,
            path: route,
            fs: Arc::new(fs.clone()),
        }])
        .open()
        .expect("an empty route is no tree state");
    assert_eq!(tree.table_count(), 0);
}

/// A tree that rotates on every install keeps its history in its snapshot and
/// leaves its log empty: the log of a generation past the first is a tree
/// that lived all the same, and a lost snapshot over it is refused.
#[test]
fn an_open_without_a_snapshot_refuses_a_rotated_generations_empty_log() {
    use lsm_tree::fs::Fs;
    let mem = MemFs::new();
    let fs = CrashFs::new(mem.clone());
    {
        let tree = config(&fs, false)
            .manifest_log_rotate_bytes(0)
            .open()
            .expect("create");
        tree.insert("key", "value", 0);
        tree.flush_active_memtable(0).expect("flush");
        tree.drop_range::<&[u8], _>(..).expect("drop");
    }
    let db = std::path::absolute(DB).expect("absolute path");
    assert!(
        mem.read_dir(&db.join("tables")).expect("tables").is_empty(),
        "self-check: no table is left"
    );
    let mut rotated_logs = 0;
    for entry in mem.read_dir(&db).expect("tree folder") {
        if entry.file_name.starts_with('v') {
            mem.remove_file(&entry.path).expect("lose the snapshot");
        } else if entry.file_name.starts_with("edits-") && entry.file_name != "edits-0" {
            assert_eq!(
                mem.metadata(&entry.path).expect("log").len,
                0,
                "self-check: the rotated log is empty"
            );
            rotated_logs += 1;
        }
    }
    assert_eq!(rotated_logs, 1, "self-check: one rotated generation's log");

    assert!(
        config(&fs, false).open().is_err(),
        "a rotated generation's log is a tree's state"
    );
}

/// Flushes past a manifest rotation survive a power loss: the rotated
/// generation's log, created with its snapshot, holds the edits after it.
#[test]
fn flushes_across_a_manifest_rotation_survive_a_power_loss() {
    let fs = CrashFs::new(MemFs::new());
    let tree = config(&fs, false)
        .manifest_log_rotate_bytes(1)
        .open()
        .expect("create");
    for i in 0..5u64 {
        tree.insert(format!("key{i}"), "value", i);
        tree.flush_active_memtable(0).expect("flush");
    }
    drop(tree);
    fs.crash();
    let tree = reopen(&fs, false);
    for i in 0..5u64 {
        assert!(
            tree.get(format!("key{i}"), u64::MAX)
                .expect("get")
                .is_some(),
            "key{i}"
        );
    }
}

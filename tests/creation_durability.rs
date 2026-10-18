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
/// left (a dropped range, the retention floor it raised): a missing pointer
/// over that log is refused, never taken for a fresh folder and created over.
#[test]
fn an_open_without_a_pointer_refuses_a_log_holding_edits() {
    use lsm_tree::fs::Fs;
    let mem = MemFs::new();
    let fs = CrashFs::new(mem.clone());
    {
        let tree = config(&fs, false).open().expect("create");
        tree.insert("key", "value", 0);
        tree.flush_active_memtable(0).expect("flush");
        tree.drop_range::<&[u8], _>(..).expect("drop");
    }
    // The tree's path as the tree spells it: absolute (on Windows, with a
    // drive).
    let db = std::path::absolute(DB).expect("absolute path");
    assert!(
        mem.read_dir(&db.join("tables")).expect("tables").is_empty(),
        "self-check: no table is left"
    );
    mem.remove_file(&db.join("current"))
        .expect("lose the pointer");

    assert!(
        config(&fs, false).open().is_err(),
        "a log holding edits is a tree's state"
    );
}

/// Only the exact name `v0` is what a creation cut short leaves: a snapshot
/// spelled otherwise (`v00`, or an id past the counter's range) is not
/// something a creation wrote, and an open without a pointer refuses it
/// rather than create over it.
#[test]
fn an_open_without_a_pointer_refuses_a_snapshot_a_creation_never_names() {
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

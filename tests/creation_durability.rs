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

/// A tree first opened KV-separated after it was created makes its blob
/// folder at that open, and the values its flushes write there survive a
/// power loss.
#[test]
fn a_tree_opened_kv_separated_later_keeps_its_blobs_through_a_power_loss() {
    let fs = CrashFs::new(MemFs::new());
    drop(config(&fs, false).open().expect("create"));
    let tree = config(&fs, true).open().expect("reopen KV-separated");
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

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! End-to-end Tree-level integration of the runtime-config foundation
//! (#352): `Tree::update_runtime_config` swaps the live snapshot
//! atomically, `Tree::runtime_config` returns the current snapshot,
//! and a snapshot captured before an update reflects the pre-update
//! state (load-old-then-act-old guarantee that compaction-as-migration
//! relies on).

use lsm_tree::{
    AnyTree, Config, SequenceNumberCounter, get_tmp_folder,
    runtime_config::{ChecksumAlgorithm, RuntimeConfig},
};
use std::sync::Arc;
use test_log::test;

fn open_tree(path: &std::path::Path) -> lsm_tree::Tree {
    let any = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("Tree open should succeed");

    match any {
        AnyTree::Standard(t) => t,
        AnyTree::Blob(_) => panic!("expected Standard tree, got Blob"),
    }
}

#[test]
fn tree_runtime_config_starts_at_default() {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    let cfg = tree.runtime_config();
    assert_eq!(cfg.block_checksum_algo, ChecksumAlgorithm::Xxh3_64);
    assert_eq!(cfg.kv_checksum_algo, ChecksumAlgorithm::Xxh3_64);
}

#[test]
fn tree_update_runtime_config_visible_on_next_load() {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    tree.update_runtime_config(|cfg| {
        cfg.block_checksum_algo = ChecksumAlgorithm::Crc32c;
    });

    let after = tree.runtime_config();
    assert_eq!(after.block_checksum_algo, ChecksumAlgorithm::Crc32c);
    // kv_checksum_algo stays at default — partial updates don't bleed
    // across fields.
    assert_eq!(after.kv_checksum_algo, ChecksumAlgorithm::Xxh3_64);
}

#[test]
fn tree_snapshot_outlives_update_with_pre_update_state() {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // Capture the snapshot BEFORE the update — simulates an in-flight
    // compaction that loaded its config at start.
    let snap_before: Arc<RuntimeConfig> = tree.runtime_config();

    tree.update_runtime_config(|cfg| {
        cfg.block_checksum_algo = ChecksumAlgorithm::Crc32c;
        cfg.kv_checksum_algo = ChecksumAlgorithm::Xxh3Low32;
    });

    // Pre-update snapshot still observes original values — the swap
    // doesn't reach into Arcs already in caller hands.
    assert_eq!(snap_before.block_checksum_algo, ChecksumAlgorithm::Xxh3_64);
    assert_eq!(snap_before.kv_checksum_algo, ChecksumAlgorithm::Xxh3_64);

    // Fresh load sees the updated state.
    let snap_after = tree.runtime_config();
    assert_eq!(snap_after.block_checksum_algo, ChecksumAlgorithm::Crc32c);
    assert_eq!(snap_after.kv_checksum_algo, ChecksumAlgorithm::Xxh3Low32);
}

#[test]
fn tree_runtime_config_resets_to_default_on_reopen() {
    // The runtime config is process-state, not persisted. After Tree
    // close + reopen, the snapshot resets to defaults — caller is
    // responsible for re-applying their desired runtime overrides on
    // open. Pin this contract so a future "persist runtime config to
    // manifest" change doesn't break the documented expectation
    // silently.
    let folder = get_tmp_folder();

    {
        let tree = open_tree(folder.path());
        tree.update_runtime_config(|cfg| {
            cfg.block_checksum_algo = ChecksumAlgorithm::Crc32c;
        });
        assert_eq!(
            tree.runtime_config().block_checksum_algo,
            ChecksumAlgorithm::Crc32c,
        );
    }

    let tree = open_tree(folder.path());
    assert_eq!(
        tree.runtime_config().block_checksum_algo,
        ChecksumAlgorithm::Xxh3_64,
        "runtime config must reset to default on reopen — \
         not persisted to manifest by design"
    );
}

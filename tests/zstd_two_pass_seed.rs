// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Turning the btultra2 two-pass seed off changes how the encoder searches,
//! never what a block means. The tree must read back exactly what it stored,
//! and must keep reading blocks written under the other setting, since the
//! knob is live and a tree outlives any one value of it.

#![cfg(feature = "zstd")]

use lsm_tree::{AbstractTree, CompressionType, Config, SequenceNumberCounter};
use test_log::test;

fn value_for(i: u64) -> Vec<u8> {
    // Repetitive enough that level 22 has real work to do, and distinct per
    // key so a mixed-up block surfaces as a wrong value rather than a pass.
    format!("value-{i:06}-{}", "payload".repeat(8)).into_bytes()
}

#[test]
fn a_tree_reads_back_what_it_wrote_without_the_two_pass_seed() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let tree = Config::new(&folder, seqno.clone(), SequenceNumberCounter::default())
        .data_block_compression_policy(lsm_tree::config::CompressionPolicy::all(
            CompressionType::Zstd(22),
        ))
        .open()?;
    let lsm_tree::AnyTree::Standard(tree) = tree else {
        panic!("standard tree configured (no kv separation)");
    };

    tree.update_runtime_config(|cfg| cfg.zstd_two_pass_seed = false)?;

    for i in 0..256u64 {
        tree.insert(format!("key-{i:06}"), value_for(i), seqno.next());
    }
    tree.flush_active_memtable(0)?;

    for i in 0..256u64 {
        assert_eq!(
            Some(value_for(i)),
            tree.get(format!("key-{i:06}"), lsm_tree::MAX_SEQNO)?
                .map(|v| v.to_vec()),
            "key {i} must read back unchanged",
        );
    }

    Ok(())
}

#[test]
fn blocks_written_under_both_settings_coexist() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let tree = Config::new(&folder, seqno.clone(), SequenceNumberCounter::default())
        .data_block_compression_policy(lsm_tree::config::CompressionPolicy::all(
            CompressionType::Zstd(22),
        ))
        .open()?;
    let lsm_tree::AnyTree::Standard(tree) = tree else {
        panic!("standard tree configured (no kv separation)");
    };

    // Default first: the seed is on.
    for i in 0..128u64 {
        tree.insert(format!("key-{i:06}"), value_for(i), seqno.next());
    }
    tree.flush_active_memtable(0)?;

    // Then the same tree keeps writing with it off.
    tree.update_runtime_config(|cfg| cfg.zstd_two_pass_seed = false)?;
    for i in 128..256u64 {
        tree.insert(format!("key-{i:06}"), value_for(i), seqno.next());
    }
    tree.flush_active_memtable(0)?;

    // Nothing records the setting, so every block reads the same way.
    for i in 0..256u64 {
        assert_eq!(
            Some(value_for(i)),
            tree.get(format!("key-{i:06}"), lsm_tree::MAX_SEQNO)?
                .map(|v| v.to_vec()),
            "key {i} must read back whichever setting wrote it",
        );
    }

    // A compaction rewrites the older blocks under the current setting; the
    // data still has to survive that.
    tree.major_compact(64_000_000, seqno.get())?;
    for i in 0..256u64 {
        assert_eq!(
            Some(value_for(i)),
            tree.get(format!("key-{i:06}"), lsm_tree::MAX_SEQNO)?
                .map(|v| v.to_vec()),
            "key {i} must survive a compaction that changed its strategy",
        );
    }

    Ok(())
}

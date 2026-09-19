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

/// The setting is documented as taking effect on the next block written, so a
/// FLUSH has to honour it, not only a compaction. Proven by the bytes on disk:
/// the same keys flushed under each setting produce different SSTs, which they
/// cannot do if the flush ignores the flag.
#[test]
fn flush_writes_under_the_configured_seed_setting() -> lsm_tree::Result<()> {
    fn flush_bytes(seed: bool) -> lsm_tree::Result<u64> {
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
        tree.update_runtime_config(|cfg| cfg.zstd_two_pass_seed = seed)?;

        for i in 0..256u64 {
            tree.insert(format!("key-{i:06}"), value_for(i), seqno.next());
        }
        tree.flush_active_memtable(0)?;

        // The single table this flush produced, read straight off disk.
        let mut tables: Vec<_> = std::fs::read_dir(folder.path().join("tables"))?
            .filter_map(std::result::Result::ok)
            .map(|e| e.path())
            .filter(|p| p.is_file())
            .collect();
        tables.sort();
        let table = tables
            .first()
            .unwrap_or_else(|| panic!("the flush must have written a table"));
        Ok(std::fs::metadata(table)?.len())
    }

    // Sizes, not bytes: an SST carries ids and seqnos that differ between two
    // otherwise identical runs, so only the compressed length is comparable.
    // Two flushes under the SAME setting must agree on it, or the comparison
    // below would be reading noise.
    assert_eq!(
        flush_bytes(true)?,
        flush_bytes(true)?,
        "the flushed size must be stable for the comparison below to mean anything",
    );

    let seeded = flush_bytes(true)?;
    let single = flush_bytes(false)?;
    assert_ne!(
        seeded, single,
        "a flush must write under the configured strategy, not the default. \
         The two strategies are only guaranteed to differ in the bytes they \
         search, not in the bytes they emit, so this fixture was chosen because \
         they do differ on it. If an encoder change ever makes the sizes equal \
         here, the comparison has stopped proving anything: pick a fixture where \
         they differ again rather than relaxing the assertion",
    );

    Ok(())
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

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Leveled compaction makes the same choices and writes the same tables as it
//! always has: a deterministic stream of writes, flushed and compacted step
//! by step, ends in a fingerprint of every compaction it ran and of every
//! table it left. A change to how the strategy is put together that keeps
//! its behaviour keeps the fingerprint.
//!
//! The fingerprint was taken on a 64-bit target. A 32-bit one writes other
//! tables for the same stream, so the pinned value says nothing there.

#![cfg(target_pointer_width = "64")]

use lsm_tree::compaction::{CompactionAction, Leveled};
use lsm_tree::config::CompressionPolicy;
use lsm_tree::{AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter, get_tmp_folder};
use std::sync::Arc;
use test_log::test;

/// A value of `len` bytes, the same for the same `seed`.
fn noise(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

/// Folds `bytes` into `hash`.
fn fold(hash: u64, bytes: &[u8]) -> u64 {
    bytes.iter().fold(hash, |hash, &byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01B3)
    })
}

/// The fingerprint of the compactions `strategy` runs over the write stream,
/// and of the tables it leaves.
fn fingerprint(strategy: &Arc<Leveled>) -> u64 {
    let folder = get_tmp_folder();
    // Uncompressed whatever codecs are built in: a codec's default changes
    // the tables' sizes, so where they are cut.
    let AnyTree::Standard(tree) = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::disabled())
    .open()
    .expect("open") else {
        panic!("expected a standard tree");
    };
    let mut hash = 0xCBF2_9CE4_8422_2325;
    let mut seqno = 0u64;
    for round in 0..40u64 {
        // Overwrites a moving window of a key space larger than one flush,
        // and deletes some of what it wrote before.
        for i in 0..400u64 {
            let key = (round * 97 + i * 13) % 6_000;
            tree.insert(format!("k{key:06}"), noise(round * 1_000 + i, 96), seqno);
            seqno += 1;
            if i % 50 == 0 {
                tree.remove(format!("k{:06}", (key + 7) % 6_000), seqno);
                seqno += 1;
            }
        }
        tree.flush_active_memtable(0).expect("flush");
        loop {
            let strategy: Arc<dyn lsm_tree::compaction::CompactionStrategy> = strategy.clone();
            let result = tree.compact(strategy, seqno / 2).expect("compact");
            let action: u8 = match result.action {
                CompactionAction::Nothing => break,
                CompactionAction::Merged => 1,
                CompactionAction::Moved => 2,
                CompactionAction::Dropped => 3,
            };
            hash = fold(hash, &[action, result.dest_level.unwrap_or(u8::MAX)]);
            hash = fold(hash, &(result.tables_in as u64).to_le_bytes());
            hash = fold(hash, &(result.tables_out as u64).to_le_bytes());
        }
    }
    let version = tree.current_version();
    for (level, tables) in version.iter_levels().enumerate() {
        for run in tables.iter() {
            for table in run.iter() {
                hash = fold(hash, &(level as u64).to_le_bytes());
                hash = fold(hash, &table.metadata.item_count.to_le_bytes());
                for item in table.iter() {
                    let item = item.expect("read");
                    hash = fold(hash, &item.key.user_key);
                    hash = fold(hash, &item.key.seqno.to_le_bytes());
                    hash = fold(hash, &item.value);
                }
            }
        }
    }
    for i in 0..6_000u64 {
        let got = tree.get(format!("k{i:06}"), SeqNo::MAX).expect("get");
        hash = fold(hash, got.as_deref().unwrap_or(b"-"));
    }
    hash
}

/// The leveled strategy with small tables and levels, so the stream reaches
/// several levels and every kind of choice: moves, merges into the level
/// below, intra-L0 merges.
fn strategy() -> Arc<Leveled> {
    Arc::new(
        Leveled::default()
            .with_table_target_size(16 * 1_024)
            .with_l0_threshold(3)
            .with_level_ratio_policy(vec![4.0]),
    )
}

#[test]
fn leveled_compaction_keeps_its_fingerprint() {
    let got = fingerprint(&strategy());
    assert_eq!(
        got, EXPECTED,
        "leveled compaction changed what it does: {got:#018x}"
    );
}

/// What the stream ends in under the leveled strategy as it was written.
const EXPECTED: u64 = 0xc328_82ea_28db_115b;

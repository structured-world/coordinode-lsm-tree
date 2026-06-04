// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Parallel sub-compaction correctness: a compaction split into disjoint key
//! ranges across worker threads must produce exactly the same logical content
//! as a single-threaded compaction. A split-boundary off-by-one would silently
//! lose or duplicate keys, so this compares every key against the serial path
//! and confirms the split actually happened (more output tables than serial).

#![cfg(feature = "std")]
#![expect(clippy::unwrap_used, reason = "test code")]

use lsm_tree::config::{BlockSizePolicy, KvSeparationOptions};
use lsm_tree::{AbstractTree, AnyTree, Config, MAX_SEQNO, SequenceNumberCounter};
use tempfile::{TempDir, tempdir};
use test_log::test;

const N: u64 = 4_000;

fn key_for(i: u64) -> String {
    format!("key_{i:08}")
}

fn value_for(i: u64, generation: u64) -> String {
    // Padded so blocks fill quickly (many blocks per table) and, with KV
    // separation on, the value clears the separation threshold.
    format!("gen{generation}-value-{i}-{}", "padding".repeat(8))
}

/// Builds a tree, then runs a two-phase compaction:
///   1. write generation 0, flush, compact with a small target so the bottom
///      level ends up with several tables (the boundaries a later sub-compaction
///      splits on);
///   2. overwrite every key (generation 1), flush, then `major_compact` — which,
///      with `threads > 1` and `min_bytes` low enough, splits across the
///      bottom-level boundaries into parallel sub-compactions.
fn build(threads: usize, min_bytes: u64, kv_separation: bool) -> (AnyTree, TempDir) {
    let dir = tempdir().unwrap();
    let config = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .compaction_threads(threads)
    .subcompaction_min_bytes(min_bytes);

    let config = if kv_separation {
        config.with_kv_separation(Some(
            KvSeparationOptions::default().separation_threshold(16),
        ))
    } else {
        config
    };

    let tree = config.open().unwrap();

    // Step 1: populate the bottom level with several tables (split boundaries).
    for i in 0..N {
        tree.insert(key_for(i), value_for(i, 0), i);
    }
    tree.flush_active_memtable(0).unwrap();
    tree.major_compact(4_096, 0).unwrap();

    // Step 2: overwrite everything, then compact into the populated bottom
    // level — this is the compaction that splits when parallel.
    for i in 0..N {
        tree.insert(key_for(i), value_for(i, 1), N + i);
    }
    tree.flush_active_memtable(0).unwrap();
    tree.major_compact(u64::MAX, 0).unwrap();

    (tree, dir)
}

fn assert_parallel_matches_serial(kv_separation: bool) {
    // Parallel: 4 threads, sub-compaction forced on (min size 0).
    let (parallel, _dp) = build(4, 0, kv_separation);
    // Serial baseline: single-threaded, sub-compaction disabled.
    let (serial, _ds) = build(1, u64::MAX, kv_separation);

    // Every key must read back the latest (generation 1) value on both engines.
    for i in 0..N {
        let key = key_for(i);
        let expected = value_for(i, 1);

        let p = parallel.get(&key, MAX_SEQNO).unwrap();
        assert_eq!(
            p.as_deref(),
            Some(expected.as_bytes()),
            "parallel: wrong/missing value for {key} (kv_separation={kv_separation})",
        );

        let s = serial.get(&key, MAX_SEQNO).unwrap();
        assert_eq!(
            s.as_deref(),
            Some(expected.as_bytes()),
            "serial: wrong/missing value for {key} (kv_separation={kv_separation})",
        );
    }

    // Non-vacuous: the parallel compaction must actually have split into more
    // output tables than the single-table serial compaction.
    assert!(
        parallel.table_count() > serial.table_count(),
        "sub-compaction should split into multiple tables \
         (parallel={}, serial={}, kv_separation={kv_separation})",
        parallel.table_count(),
        serial.table_count(),
    );
}

#[test]
fn sub_compaction_matches_serial_plain() {
    assert_parallel_matches_serial(false);
}

#[test]
fn sub_compaction_matches_serial_kv_separated() {
    assert_parallel_matches_serial(true);
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! End-to-end parallel block compression driven through the public `Config`
//! knobs: a tree opened with `compaction_threads > 1` builds its per-tree
//! worker pool at open and compacts through it, while a single-threaded tree
//! takes the serial path. Both must read back identical content — parallel
//! block compression only changes *how* blocks are produced, never *what*.

#![cfg(all(feature = "std", feature = "parallel"))]
#![expect(clippy::unwrap_used, reason = "test code")]

use lsm_tree::config::BlockSizePolicy;
use lsm_tree::{AbstractTree, AnyTree, Config, MAX_SEQNO, SequenceNumberCounter};
use tempfile::{TempDir, tempdir};
use test_log::test;

const N: u64 = 5_000;

fn key_for(i: u64) -> String {
    format!("key_{i:08}")
}

fn value_for(i: u64) -> String {
    format!("value-{i}-{}", "payload".repeat(6))
}

/// Builds a tree at `compaction_threads`, fills it, and major-compacts so the
/// compaction runs through the parallel (or serial) block-compression path.
fn build(compaction_threads: usize) -> (AnyTree, TempDir) {
    let dir = tempdir().unwrap();
    let tree = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .compaction_threads(compaction_threads)
    .open()
    .unwrap();

    for i in 0..N {
        tree.insert(key_for(i), value_for(i), i);
    }
    tree.flush_active_memtable(0).unwrap();
    tree.major_compact(u64::MAX, 0).unwrap();

    (tree, dir)
}

#[test]
fn parallel_compaction_matches_serial_via_config() {
    let (parallel, _dp) = build(4);
    let (serial, _ds) = build(1);

    for i in 0..N {
        let key = key_for(i);
        let expected = value_for(i);
        assert_eq!(
            parallel.get(&key, MAX_SEQNO).unwrap().as_deref(),
            Some(expected.as_bytes()),
            "parallel: wrong/missing value for {key}",
        );
        assert_eq!(
            serial.get(&key, MAX_SEQNO).unwrap().as_deref(),
            Some(expected.as_bytes()),
            "serial: wrong/missing value for {key}",
        );
    }
}

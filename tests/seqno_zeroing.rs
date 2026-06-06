// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Bottommost seqno-zeroing correctness (no range tombstones): a last-level
//! compaction zeroes the sequence numbers of entries below the GC watermark
//! that no range tombstone covers. This must never change query results — a
//! later higher-seqno overwrite has to win over a zeroed bottom entry, and
//! re-compaction must keep the latest value. (Range-tombstone interaction is
//! covered exhaustively by `prop_range_tombstone`.)

#![cfg(feature = "std")]

use lsm_tree::{AbstractTree, Config, MAX_SEQNO, SequenceNumberCounter};
use test_log::test;

const N: u64 = 200;

fn key(i: u64) -> String {
    format!("k{i:05}")
}

#[test]
fn bottommost_seqno_zeroing_preserves_latest_value() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Generation 0 at low seqnos, flushed to disk.
    for i in 0..N {
        tree.insert(key(i), format!("v0-{i}"), i);
    }
    tree.flush_active_memtable(0)?;

    // Compact to the last level with a GC watermark above every gen-0 seqno, so
    // the surviving entries get their seqnos zeroed.
    tree.major_compact(u64::MAX, 1_000)?;
    for i in 0..N {
        assert_eq!(
            tree.get(key(i), MAX_SEQNO)?.as_deref(),
            Some(format!("v0-{i}").as_bytes()),
            "gen-0 value must read back after the zeroing compaction for {}",
            key(i),
        );
    }

    // Overwrite with strictly higher seqnos. The zeroed (seqno 0) bottom entries
    // must NOT shadow these — the merge picks the highest seqno.
    for i in 0..N {
        tree.insert(key(i), format!("v1-{i}"), 2_000 + i);
    }
    tree.flush_active_memtable(0)?;
    for i in 0..N {
        assert_eq!(
            tree.get(key(i), MAX_SEQNO)?.as_deref(),
            Some(format!("v1-{i}").as_bytes()),
            "higher-seqno overwrite must win over a zeroed bottom entry for {}",
            key(i),
        );
    }

    // Re-compact to the bottom: gen-1 is zeroed, gen-0 dropped. Latest persists.
    tree.major_compact(u64::MAX, 10_000)?;
    for i in 0..N {
        assert_eq!(
            tree.get(key(i), MAX_SEQNO)?.as_deref(),
            Some(format!("v1-{i}").as_bytes()),
            "latest value must persist across a second zeroing compaction for {}",
            key(i),
        );
    }

    Ok(())
}

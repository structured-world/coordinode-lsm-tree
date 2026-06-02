// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! End-to-end durability-mode coverage: a tree opened under either
//! [`SyncMode`] must persist a flush and recover it after reopen. On macOS
//! the two modes drive different fsync syscalls (plain `fsync` vs
//! `F_FULLFSYNC`); elsewhere they are identical. Either way the persisted
//! tree must be valid and re-openable.

use lsm_tree::{AbstractTree, Config, SequenceNumberCounter, fs::SyncMode};

fn roundtrip_under(mode: SyncMode) {
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let tree = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .sync_mode(mode)
        .open()
        .expect("open with sync mode");

        for i in 0u64..256 {
            tree.insert(i.to_be_bytes(), b"v", i);
        }
        tree.flush_active_memtable(0).expect("flush");
    }

    // Reopen from disk — the flush must have produced a recoverable tree.
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .sync_mode(mode)
    .open()
    .expect("reopen");

    for i in 0u64..256 {
        let got = tree.get(i.to_be_bytes(), lsm_tree::MAX_SEQNO).expect("get");
        assert_eq!(
            got.as_deref(),
            Some(&b"v"[..]),
            "lost key {i} under {mode:?}"
        );
    }
}

#[test]
fn sync_mode_normal_flush_round_trips() {
    roundtrip_under(SyncMode::Normal);
}

#[test]
fn sync_mode_full_flush_round_trips() {
    roundtrip_under(SyncMode::Full);
}

#[test]
fn sync_mode_defaults_to_normal() {
    // The default must be the cheaper plain-fsync mode (matching RocksDB /
    // SQLite defaults), so macOS does not pay F_FULLFSYNC unless asked.
    assert_eq!(SyncMode::default(), SyncMode::Normal);
}

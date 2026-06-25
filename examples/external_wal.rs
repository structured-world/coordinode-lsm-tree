// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Worked example for the external-WAL recipe (`docs/external-wal.md`): the
//! engine has no internal WAL, so durability is the caller's job. This program
//! logs each write to an [append-only reference WAL](#) **before** applying it,
//! flushes to make a prefix durable, trims the WAL to that watermark, simulates
//! a crash (drop the tree, keeping the SST directory + WAL), reopens, and
//! replays the surviving tail strictly above the watermark.
//!
//! Run it with `cargo run --example external_wal`. It reuses the same reference
//! WAL the integration test in `tests/external_wal.rs` exercises, so the spec,
//! the example, and the test stay in lock-step.

#[path = "../tests/external_wal/reference_wal.rs"]
mod reference_wal;

use lsm_tree::{
    AbstractTree, AnyTree, Config, MAX_SEQNO, MergeOperator, SequenceNumberCounter, UserValue,
};
use reference_wal::{ReferenceWal, WalOp, WalRecord};
use std::path::Path;
use std::sync::Arc;

/// `counter` merge operator: base + sum of i64 little-endian operands.
struct CounterMerge;

impl MergeOperator for CounterMerge {
    fn merge(
        &self,
        _key: &[u8],
        base: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> lsm_tree::Result<UserValue> {
        let mut n: i64 = base.map_or(0, |b| i64::from_le_bytes(b.try_into().expect("8 bytes")));
        for op in operands {
            n += i64::from_le_bytes((*op).try_into().expect("8 bytes"));
        }
        Ok(n.to_le_bytes().to_vec().into())
    }
}

fn open(folder: &Path) -> AnyTree {
    Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(CounterMerge)))
    .open()
    .expect("open tree")
}

/// Applies one logged record at its seqno with its original operation.
fn apply(tree: &AnyTree, record: &WalRecord) {
    match &record.op {
        WalOp::Insert { key, value } => {
            tree.insert(key.as_slice(), value.as_slice(), record.seqno);
        }
        WalOp::Remove { key } => {
            tree.remove(key.as_slice(), record.seqno);
        }
        WalOp::Merge { key, value } => {
            tree.merge(key.as_slice(), value.as_slice(), record.seqno);
        }
        // The integration test covers remove_weak / remove_range / batch too; the
        // example keeps to the three commonest kinds for readability.
        other => unreachable!("example workload uses only insert/remove/merge, got {other:?}"),
    }
}

fn main() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let wal_path = dir.path().join("external.wal");

    // --- Live operation: log before apply, flush, trim to the watermark. ---
    {
        let tree = open(dir.path());
        let mut wal = ReferenceWal::create(&wal_path)?;

        // A small durable prefix (seqnos 0..=1), applied in strict order.
        for record in [
            WalRecord {
                seqno: 0,
                op: WalOp::Insert {
                    key: b"user:1".to_vec(),
                    value: b"alice".to_vec(),
                },
            },
            WalRecord {
                seqno: 1,
                op: WalOp::Merge {
                    key: b"logins".to_vec(),
                    value: 1i64.to_le_bytes().to_vec(),
                },
            },
        ] {
            wal.append(&record)?; // 1. log + fsync ...
            apply(&tree, &record); // 2. ... then apply
        }

        tree.flush_active_memtable(0)?; // 3. make the prefix durable
        let w = tree.get_highest_persisted_seqno().expect("flushed");
        println!("flushed; durable watermark W = {w}");
        wal.trim_through(w)?; // 4. the prefix is on disk; drop it from the WAL

        // Writes above W live only in the active memtable, lost on a crash.
        for record in [
            WalRecord {
                seqno: 2,
                op: WalOp::Merge {
                    key: b"logins".to_vec(),
                    value: 1i64.to_le_bytes().to_vec(),
                },
            },
            WalRecord {
                seqno: 3,
                op: WalOp::Remove {
                    key: b"user:1".to_vec(),
                },
            },
        ] {
            wal.append(&record)?;
            apply(&tree, &record);
        }

        // --- Crash: drop the tree (the memtable with seqnos 2..=3 is gone). ---
        drop(tree);
        println!("crashed (dropped the tree); SST dir + WAL survive on disk");
    }

    // --- Recovery: reopen (recovers seqnos 0..=1 from SSTs), replay above W. ---
    let tree = open(dir.path());
    let wal = ReferenceWal::open(&wal_path)?;
    for record in wal.records()? {
        apply(&tree, &record); // every surviving record is above W by construction
    }

    let logins = tree
        .get("logins", MAX_SEQNO)?
        .map(|v| i64::from_le_bytes((*v).try_into().expect("8 bytes")));
    let user1 = tree.get("user:1", MAX_SEQNO)?;
    println!("recovered: logins = {logins:?}, user:1 = {user1:?}");

    assert_eq!(
        logins,
        Some(2),
        "two merge operands folded across the crash"
    );
    assert_eq!(user1, None, "the replayed remove survived the crash");
    println!("recovery reproduced the pre-crash state exactly");
    Ok(())
}

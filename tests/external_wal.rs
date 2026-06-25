// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Executable proof of the external-WAL recipe specified in
//! `docs/external-wal.md`: drive the documented contract end-to-end through a
//! crash and recovery, and assert the recovered state is byte-for-byte what a
//! non-crashed run produced. The contract-guard tests additionally prove that a
//! deliberately wrong recovery (collapsing ops to `insert`, re-applying a merge
//! at or below the watermark, or replaying from the raw persisted maximum
//! instead of the gap-free watermark) is *detectably* wrong.

#![cfg(feature = "std")]

#[path = "external_wal/reference_wal.rs"]
mod reference_wal;

// `Guard` (the re-exported `IterGuard` trait) is required for `into_inner()` on
// scan results.
use lsm_tree::{
    AbstractTree, AnyTree, Config, Guard, MAX_SEQNO, MergeOperator, SequenceNumberCounter,
    UserValue, WriteBatch,
};
use reference_wal::{BatchEntry, ReferenceWal, WalOp, WalRecord};
use std::path::Path;
use std::sync::Arc;

/// Counter merge operator: base + sum of i64 little-endian operands. Re-applying
/// an operand folds it twice, which is exactly the footgun the strict `> W`
/// replay boundary exists to prevent.
struct CounterMerge;

impl MergeOperator for CounterMerge {
    fn merge(
        &self,
        _key: &[u8],
        base_value: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> lsm_tree::Result<UserValue> {
        let mut counter: i64 = match base_value {
            Some(bytes) if bytes.len() == 8 => {
                i64::from_le_bytes(bytes.try_into().expect("checked length"))
            }
            Some(_) => return Err(lsm_tree::Error::MergeOperator),
            None => 0,
        };
        for operand in operands {
            if operand.len() != 8 {
                return Err(lsm_tree::Error::MergeOperator);
            }
            counter += i64::from_le_bytes((*operand).try_into().expect("checked length"));
        }
        Ok(counter.to_le_bytes().to_vec().into())
    }
}

/// Opens (or reopens) a tree at `folder` with the counter merge operator. The
/// SAME operator must be configured on reopen, or merge resolution on read would
/// fail.
fn open_tree(folder: &Path) -> AnyTree {
    Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(CounterMerge)))
    .open()
    .expect("open tree")
}

/// Applies one logged record at its original seqno with its original operation,
/// never collapsing to `insert`. This is the "apply" half of log-before-apply
/// and the whole of replay.
fn apply(tree: &AnyTree, record: &WalRecord) -> lsm_tree::Result<()> {
    let seqno = record.seqno;
    match &record.op {
        WalOp::Insert { key, value } => {
            tree.insert(key.as_slice(), value.as_slice(), seqno);
        }
        WalOp::Remove { key } => {
            tree.remove(key.as_slice(), seqno);
        }
        WalOp::RemoveWeak { key } => {
            tree.remove_weak(key.as_slice(), seqno);
        }
        WalOp::RemoveRange { start, end } => {
            tree.remove_range(start.as_slice(), end.as_slice(), seqno);
        }
        WalOp::Merge { key, value } => {
            tree.merge(key.as_slice(), value.as_slice(), seqno);
        }
        WalOp::Batch { entries } => {
            let mut batch = WriteBatch::new();
            for entry in entries {
                match entry {
                    BatchEntry::Insert { key, value } => {
                        batch.insert(key.as_slice(), value.as_slice())
                    }
                    BatchEntry::Remove { key } => batch.remove(key.as_slice()),
                    BatchEntry::RemoveWeak { key } => batch.remove_weak(key.as_slice()),
                    BatchEntry::Merge { key, value } => {
                        batch.merge(key.as_slice(), value.as_slice())
                    }
                }
            }
            tree.apply_batch(batch, seqno)?;
        }
    }
    Ok(())
}

/// The full visible state at `MAX_SEQNO` as sorted `(key, value)` pairs — the
/// byte-identity fingerprint two runs must agree on.
fn snapshot(tree: &AnyTree) -> Vec<(Vec<u8>, Vec<u8>)> {
    tree.iter(MAX_SEQNO, None)
        .map(|guard| {
            let (key, value) = guard.into_inner().expect("scan entry");
            (key.to_vec(), value.to_vec())
        })
        .collect()
}

/// A deterministic workload exercising every logged write kind: `insert`,
/// `remove`, `remove_weak`, `remove_range`, `merge`, and a `WriteBatch` (which
/// itself mixes insert / remove_weak / merge). Seqnos `0..=FLUSH_AFTER` are
/// flushed (durable) before the crash; `> FLUSH_AFTER` live only in the lost
/// memtable and must be recovered from the WAL.
const FLUSH_AFTER: u64 = 6;

fn workload() -> Vec<WalRecord> {
    let i64op = |n: i64| n.to_le_bytes().to_vec();
    vec![
        WalRecord {
            seqno: 0,
            op: WalOp::Insert {
                key: b"apple".to_vec(),
                value: b"red".to_vec(),
            },
        },
        WalRecord {
            seqno: 1,
            op: WalOp::Insert {
                key: b"banana".to_vec(),
                value: b"yellow".to_vec(),
            },
        },
        WalRecord {
            seqno: 2,
            op: WalOp::Merge {
                key: b"counter".to_vec(),
                value: i64op(5),
            },
        },
        WalRecord {
            seqno: 3,
            op: WalOp::Insert {
                key: b"cherry".to_vec(),
                value: b"dark".to_vec(),
            },
        },
        WalRecord {
            seqno: 4,
            op: WalOp::Batch {
                entries: vec![
                    BatchEntry::Insert {
                        key: b"date".to_vec(),
                        value: b"brown".to_vec(),
                    },
                    BatchEntry::RemoveWeak {
                        key: b"banana".to_vec(),
                    },
                    BatchEntry::Merge {
                        key: b"counter".to_vec(),
                        value: i64op(3),
                    },
                ],
            },
        },
        WalRecord {
            seqno: 5,
            op: WalOp::Remove {
                key: b"apple".to_vec(),
            },
        },
        WalRecord {
            seqno: 6,
            op: WalOp::RemoveRange {
                start: b"cherry".to_vec(),
                end: b"date".to_vec(),
            },
        },
        // ---- flush boundary: W = 6 ----
        WalRecord {
            seqno: 7,
            op: WalOp::Insert {
                key: b"elderberry".to_vec(),
                value: b"purple".to_vec(),
            },
        },
        WalRecord {
            seqno: 8,
            op: WalOp::Merge {
                key: b"counter".to_vec(),
                value: i64op(2),
            },
        },
        WalRecord {
            seqno: 9,
            op: WalOp::Insert {
                key: b"fig".to_vec(),
                value: b"green".to_vec(),
            },
        },
        WalRecord {
            seqno: 10,
            op: WalOp::Remove {
                key: b"elderberry".to_vec(),
            },
        },
    ]
}

/// The reference WAL preserves every op kind across a round-trip and `trim_through`
/// drops exactly the records at or below the watermark.
#[test]
fn reference_wal_round_trips_and_trims_below_the_watermark() -> std::io::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("round_trip.wal");

    let records = workload();
    let mut wal = ReferenceWal::create(&path)?;
    for record in &records {
        wal.append(record)?;
    }
    assert_eq!(
        wal.records()?,
        records,
        "round-trip preserves every op kind and seqno",
    );

    wal.trim_through(FLUSH_AFTER)?;
    let kept = wal.records()?;
    assert!(
        kept.iter().all(|r| r.seqno > FLUSH_AFTER),
        "trim drops every record at or below W: {kept:?}",
    );
    assert_eq!(
        kept,
        records
            .iter()
            .filter(|r| r.seqno > FLUSH_AFTER)
            .cloned()
            .collect::<Vec<_>>(),
        "trim keeps the above-W suffix verbatim",
    );
    Ok(())
}

/// The headline contract test: run the documented recipe end-to-end through a
/// crash (drop the tree, keep the SST dir + WAL) and assert the recovered state
/// is byte-for-byte what a non-crashed run produced, for every write kind.
#[test]
fn external_wal_recipe_survives_crash_and_recovers_identical_state() -> lsm_tree::Result<()> {
    let work = workload();

    // Reference: a single no-crash run of the whole workload.
    let ref_dir = tempfile::tempdir()?;
    let reference = {
        let tree = open_tree(ref_dir.path());
        for record in &work {
            apply(&tree, record)?;
        }
        snapshot(&tree)
    };

    // Recipe run: log-before-apply, flush, trim to W, more writes, then crash.
    let dir = tempfile::tempdir()?;
    let wal_path = dir.path().join("external.wal");
    {
        let tree = open_tree(dir.path());
        let mut wal = ReferenceWal::create(&wal_path)?;

        // Phase 1 — durable prefix: apply in strict seqno order through the flush.
        for record in work.iter().filter(|r| r.seqno <= FLUSH_AFTER) {
            wal.append(record)?; // log ...
            apply(&tree, record)?; // ... before apply
        }
        tree.flush_active_memtable(0)?;
        let w = tree
            .get_highest_persisted_seqno()
            .expect("flushed tree has a persisted watermark");
        assert_eq!(
            w, FLUSH_AFTER,
            "gap-free in-order apply: W == persisted maximum"
        );
        wal.trim_through(w)?; // the prefix is durable; drop it from the WAL

        // Phase 2 — post-flush writes that live only in the active memtable.
        for record in work.iter().filter(|r| r.seqno > FLUSH_AFTER) {
            wal.append(record)?;
            apply(&tree, record)?;
        }

        // Crash: drop the tree, losing the unflushed memtable. The SST directory
        // and the WAL persist on disk.
        drop(tree);
    }

    // Recovery: reopen (recovers from SSTs up to W) and replay strictly above W.
    let recovered = {
        let tree = open_tree(dir.path());
        let wal = ReferenceWal::open(&wal_path)?;
        for record in wal.records()? {
            assert!(
                record.seqno > FLUSH_AFTER,
                "the trimmed WAL holds only records above W",
            );
            apply(&tree, &record)?;
        }
        snapshot(&tree)
    };

    assert_eq!(
        recovered, reference,
        "recovered state is byte-for-byte the non-crashed run's state",
    );
    Ok(())
}

/// Whether the visible state contains `key`.
fn has_key(state: &[(Vec<u8>, Vec<u8>)], key: &[u8]) -> bool {
    state.iter().any(|(k, _)| k.as_slice() == key)
}

/// The counter value folded by [`CounterMerge`], if the key is present.
fn counter_of(tree: &AnyTree) -> Option<i64> {
    tree.get("counter", MAX_SEQNO)
        .expect("counter read")
        .map(|v| i64::from_le_bytes((*v).try_into().expect("8-byte counter")))
}

/// Contract guard — the **original operation** must be replayed, never collapsed
/// to `insert`. A `remove` recovered as an `insert` resurrects the key, so a
/// collapsing recovery is detectably wrong.
#[test]
fn collapsing_a_remove_to_insert_is_detectably_wrong() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let wal_path = dir.path().join("external.wal");

    // insert "k" (durable), then remove "k" above the watermark.
    {
        let tree = open_tree(dir.path());
        let mut wal = ReferenceWal::create(&wal_path)?;
        let insert = WalRecord {
            seqno: 0,
            op: WalOp::Insert {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            },
        };
        wal.append(&insert)?;
        apply(&tree, &insert)?;
        tree.flush_active_memtable(0)?;
        let w = tree.get_highest_persisted_seqno().expect("flushed");
        wal.trim_through(w)?;
        let remove = WalRecord {
            seqno: 1,
            op: WalOp::Remove { key: b"k".to_vec() },
        };
        wal.append(&remove)?;
        apply(&tree, &remove)?;
        drop(tree);
    }
    let records = ReferenceWal::open(&wal_path)?.records()?;

    // Correct replay: the original `remove` wins, "k" is absent.
    let correct = {
        let tree = open_tree(dir.path());
        for record in &records {
            apply(&tree, record)?;
        }
        snapshot(&tree)
    };
    assert!(!has_key(&correct, b"k"), "the replayed remove deletes k");

    // Wrong replay: collapse the surviving record to an insert of its key.
    let wrong = {
        let tree = open_tree(dir.path());
        for record in &records {
            if let WalOp::Remove { key } = &record.op {
                tree.insert(key.as_slice(), b"resurrected".as_slice(), record.seqno);
            } else {
                apply(&tree, record)?;
            }
        }
        snapshot(&tree)
    };
    assert!(
        has_key(&wrong, b"k"),
        "collapsing the remove to an insert resurrects k — detectably wrong",
    );
    assert_ne!(correct, wrong, "the two recoveries must diverge");
    Ok(())
}

/// Contract guard — a merge operand at or below the watermark `W` is already
/// folded into the persisted SSTs; re-applying it on recovery folds it twice.
/// The strict `> W` boundary prevents this; replaying every record double-counts.
#[test]
fn re_applying_a_merge_at_or_below_the_watermark_double_counts() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let wal_path = dir.path().join("external.wal");
    let i64op = |n: i64| n.to_le_bytes().to_vec();

    // merge +5, +3 (durable: counter persists as 8), then +2 above the watermark.
    // NOTE: the WAL is deliberately NOT trimmed here, so the persisted operands
    // are still present for a wrong replay to re-fold.
    let w;
    {
        let tree = open_tree(dir.path());
        let mut wal = ReferenceWal::create(&wal_path)?;
        for (seqno, delta) in [(0u64, 5i64), (1, 3)] {
            let rec = WalRecord {
                seqno,
                op: WalOp::Merge {
                    key: b"counter".to_vec(),
                    value: i64op(delta),
                },
            };
            wal.append(&rec)?;
            apply(&tree, &rec)?;
        }
        tree.flush_active_memtable(0)?;
        w = tree.get_highest_persisted_seqno().expect("flushed");
        assert_eq!(w, 1, "both operands persisted, gap-free");
        let above = WalRecord {
            seqno: 2,
            op: WalOp::Merge {
                key: b"counter".to_vec(),
                value: i64op(2),
            },
        };
        wal.append(&above)?;
        apply(&tree, &above)?;
        drop(tree);
    }
    let records = ReferenceWal::open(&wal_path)?.records()?;

    // Correct replay: only the operand above W (+2) is re-applied: 8 + 2 = 10.
    let correct = {
        let tree = open_tree(dir.path());
        for record in records.iter().filter(|r| r.seqno > w) {
            apply(&tree, record)?;
        }
        counter_of(&tree)
    };
    assert_eq!(
        correct,
        Some(10),
        "strict > W replay yields the true counter"
    );

    // Wrong replay: re-apply every record, re-folding the persisted +5 and +3:
    // 8 + 5 + 3 + 2 = 18.
    let wrong = {
        let tree = open_tree(dir.path());
        for record in &records {
            apply(&tree, record)?;
        }
        counter_of(&tree)
    };
    assert_eq!(
        wrong,
        Some(18),
        "re-applying the at-or-below-W operands double-counts — detectably wrong",
    );
    assert_ne!(correct, wrong);
    Ok(())
}

/// Contract guard — replay must use the gap-free watermark `W`, not the raw
/// persisted maximum. A record that was logged but not applied (a crash between
/// the log write and the apply) sits below a higher applied-and-flushed seqno;
/// `W` stays below the gap, so `> W` replays it, while `> raw_maximum` skips it
/// and loses the write.
#[test]
fn replaying_from_the_raw_maximum_skips_a_logged_but_unapplied_gap() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let wal_path = dir.path().join("external.wal");

    // Apply + flush seqno 0 ("a"). Then log seqno 1 ("gap") WITHOUT applying it
    // (the crash window between log-before-apply's step 2 and step 3). Then
    // log + apply + flush seqno 2 ("b"). The applied-and-persisted prefix is 0
    // (the gap at 1 is unapplied), so W = 0 while the raw persisted maximum = 2.
    {
        let tree = open_tree(dir.path());
        let mut wal = ReferenceWal::create(&wal_path)?;

        let a = WalRecord {
            seqno: 0,
            op: WalOp::Insert {
                key: b"a".to_vec(),
                value: b"0".to_vec(),
            },
        };
        wal.append(&a)?;
        apply(&tree, &a)?;
        tree.flush_active_memtable(0)?;

        // Logged, fsynced, but the process "dies" before applying it.
        let gap = WalRecord {
            seqno: 1,
            op: WalOp::Insert {
                key: b"gap".to_vec(),
                value: b"1".to_vec(),
            },
        };
        wal.append(&gap)?;
        // (no apply)

        let b = WalRecord {
            seqno: 2,
            op: WalOp::Insert {
                key: b"b".to_vec(),
                value: b"2".to_vec(),
            },
        };
        wal.append(&b)?;
        apply(&tree, &b)?;
        tree.flush_active_memtable(0)?;

        let raw_max = tree.get_highest_persisted_seqno().expect("flushed");
        assert_eq!(raw_max, 2, "the raw persisted maximum jumps past the gap");
        // The caller's gap-free applied-and-persisted watermark is 0, NOT 2.
        wal.trim_through(0)?; // keeps seqnos 1 and 2
        drop(tree);
    }
    let records = ReferenceWal::open(&wal_path)?.records()?;

    // Correct replay (> W = 0): re-applies the gap (1) and re-applies b (2,
    // already persisted — a harmless overwrite). "gap" is recovered.
    let correct = {
        let tree = open_tree(dir.path());
        for record in records.iter().filter(|r| r.seqno > 0) {
            apply(&tree, record)?;
        }
        snapshot(&tree)
    };
    assert!(
        has_key(&correct, b"gap"),
        "the logged-but-unapplied gap is recovered"
    );

    // Wrong replay (> raw maximum = 2): skips the gap at seqno 1 entirely.
    let wrong = {
        let tree = open_tree(dir.path());
        for record in records.iter().filter(|r| r.seqno > 2) {
            apply(&tree, record)?;
        }
        snapshot(&tree)
    };
    assert!(
        !has_key(&wrong, b"gap"),
        "replaying from the raw maximum loses the gap record — detectably wrong",
    );
    assert_ne!(correct, wrong);
    Ok(())
}

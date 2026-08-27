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
use lsm_tree::fs::{CrashFs, Fs, MemFs};
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

/// Opens (or reopens) a tree at `folder` on an injected filesystem, with the
/// counter merge operator. Used by the `CrashFs` power-loss variant.
fn open_tree_on(folder: &Path, fs: Arc<dyn Fs>) -> AnyTree {
    Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(CounterMerge)))
    .with_shared_fs(fs)
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

/// The full visible state at `MAX_SEQNO` as sorted `(key, value)` pairs: the
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

        // Phase 1, durable prefix: apply in strict seqno order through the flush.
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

        // Phase 2, post-flush writes that live only in the active memtable.
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

/// Contract guard:the **original operation** must be replayed, never collapsed
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
        "collapsing the remove to an insert resurrects k, detectably wrong",
    );
    assert_ne!(correct, wrong, "the two recoveries must diverge");
    Ok(())
}

/// Contract guard:a merge operand at or below the watermark `W` is already
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
        "re-applying the at-or-below-W operands double-counts, detectably wrong",
    );
    assert_ne!(correct, wrong);
    Ok(())
}

/// Contract guard:replay must use the gap-free watermark `W`, not the raw
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
    // already persisted, a harmless overwrite). "gap" is recovered.
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
        "replaying from the raw maximum loses the gap record, detectably wrong",
    );
    assert_ne!(correct, wrong);
    Ok(())
}

/// The recipe recovers through the fault-injection harness: the engine runs on a
/// `CrashFs` over an in-memory disk whose `crash()` drops every unsynced write (a
/// stronger crash than a clean drop). The reference WAL stays on the caller's
/// real disk, unaffected by the engine's power loss, and replay above `W`
/// reconstructs the lost tail.
#[test]
fn external_wal_recipe_recovers_through_the_crash_fs_harness() -> lsm_tree::Result<()> {
    let work = workload();

    // Reference: a single no-crash run on the default filesystem.
    let ref_dir = tempfile::tempdir()?;
    let reference = {
        let tree = open_tree(ref_dir.path());
        for record in &work {
            apply(&tree, record)?;
        }
        snapshot(&tree)
    };

    let db_dir = tempfile::tempdir()?; // engine storage path (keys into MemFs)
    let wal_dir = tempfile::tempdir()?; // the caller's WAL, on the real disk
    let wal_path = wal_dir.path().join("external.wal");

    // An in-memory disk under a crash-injecting wrapper. `mem` is held so the
    // post-crash state survives the tree teardown for the reopen.
    let mem = MemFs::new();
    let crash = Arc::new(CrashFs::from_shared(Arc::new(mem.clone())));
    {
        let tree = open_tree_on(db_dir.path(), crash.clone());
        let mut wal = ReferenceWal::create(&wal_path)?;

        for record in work.iter().filter(|r| r.seqno <= FLUSH_AFTER) {
            wal.append(record)?;
            apply(&tree, record)?;
        }
        tree.flush_active_memtable(0)?;
        let w = tree
            .get_highest_persisted_seqno()
            .expect("flushed tree has a persisted watermark");
        wal.trim_through(w)?;
        for record in work.iter().filter(|r| r.seqno > FLUSH_AFTER) {
            wal.append(record)?;
            apply(&tree, record)?;
        }

        // Power loss: drop every unsynced engine write, then tear the tree down.
        // The flushed SSTs were synced, so they survive; the unflushed memtable
        // (seqnos above W) does not.
        crash.crash();
        drop(tree);
    }

    // Recover on the surviving in-memory disk and replay strictly above W.
    let recovered = {
        let tree = open_tree_on(db_dir.path(), Arc::new(mem.clone()));
        let wal = ReferenceWal::open(&wal_path)?;
        for record in wal.records()? {
            apply(&tree, &record)?;
        }
        snapshot(&tree)
    };

    assert_eq!(
        recovered, reference,
        "recovery through the crash harness reproduces the non-crashed state",
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// § Replay after repair (docs/external-wal.md section 4)
// ---------------------------------------------------------------------------

/// The section 4 workload: puts on both sides of the to-be-lost range, merge
/// operands before AND after the first flush (so some survive the repair and
/// some are lost with the excluded SST), and a memtable-only tail record.
fn repair_workload() -> Vec<WalRecord> {
    let i64op = |n: i64| n.to_le_bytes().to_vec();
    vec![
        WalRecord {
            seqno: 0,
            op: WalOp::Insert {
                key: b"a1".to_vec(),
                value: b"v1".to_vec(),
            },
        },
        WalRecord {
            seqno: 1,
            op: WalOp::Merge {
                key: b"counter".to_vec(),
                value: i64op(5),
            },
        },
        WalRecord {
            seqno: 2,
            op: WalOp::Insert {
                key: b"a2".to_vec(),
                value: b"v2".to_vec(),
            },
        },
        WalRecord {
            seqno: 3,
            op: WalOp::Merge {
                key: b"counter".to_vec(),
                value: i64op(7),
            },
        },
        // --- first flush: everything above lands in the SST the crash damages
        WalRecord {
            seqno: 4,
            op: WalOp::Insert {
                key: b"z9".to_vec(),
                value: b"v9".to_vec(),
            },
        },
        WalRecord {
            seqno: 5,
            op: WalOp::Merge {
                key: b"counter".to_vec(),
                value: i64op(11),
            },
        },
        // --- second flush: the SST above SURVIVES the repair
        WalRecord {
            seqno: 6,
            op: WalOp::Insert {
                key: b"m1".to_vec(),
                value: b"mv".to_vec(),
            },
        },
        // --- memtable only: lost with the crash, covered by the tail replay
    ]
}

/// Builds the crashed-and-damaged store: applies `records` with a flush after
/// each seqno in `flush_after`, drops the tree, corrupts the FIRST flushed
/// SST's leading data block, and removes the manifest. Returns the WAL (fully
/// retained: the archive-retention deployment from the spec).
fn crashed_store_with(
    folder: &Path,
    records: &[WalRecord],
    flush_after: &[u64],
) -> lsm_tree::Result<ReferenceWal> {
    let wal_path = folder.join("wal.log");
    let mut wal = ReferenceWal::create(&wal_path).expect("create wal");
    {
        let tree = open_tree(folder);
        for record in records {
            wal.append(record).expect("wal append");
            apply(&tree, record)?;
            if flush_after.contains(&record.seqno) {
                tree.flush_active_memtable(0)?;
            }
        }
    }

    // The first flushed SST holds seqnos 0..=3 (keys a1, a2, counter). Corrupt
    // its leading data block: whole-file recovery still parses the metadata
    // (so the repair can report the lost coverage), but block verification
    // fails and a plain repair excludes the table.
    let sst1 = folder.join("tables").join("0");
    let mut bytes = std::fs::read(&sst1).expect("read sst");
    for b in bytes.get_mut(8..24).expect("sst larger than 24 bytes") {
        *b ^= 0xFF;
    }
    std::fs::write(&sst1, &bytes).expect("write corruption");

    // The crash also took the manifest.
    for entry in std::fs::read_dir(folder).expect("read dir") {
        let entry = entry.expect("dir entry");
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let is_version = name
            .strip_prefix('v')
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version || name == "current" {
            std::fs::remove_file(entry.path()).expect("remove manifest file");
        }
    }
    Ok(wal)
}

/// The section 4 workload with flushes after seqnos 3 and 5 (see
/// [`repair_workload`]).
fn crashed_repairable_store(folder: &Path) -> lsm_tree::Result<ReferenceWal> {
    crashed_store_with(folder, &repair_workload(), &[3, 5])
}

/// A workload whose to-be-lost SST carries a RANGE DELETION and a BATCH: the
/// section 4 replay must select the range record by span overlap and replay
/// the batch per entry (merge entries presence-checked), or the tombstoned
/// key resurrects and the batch's writes vanish.
fn range_and_batch_workload() -> Vec<WalRecord> {
    let i64op = |n: i64| n.to_le_bytes().to_vec();
    vec![
        WalRecord {
            seqno: 0,
            op: WalOp::Insert {
                key: b"a1".to_vec(),
                value: b"v1".to_vec(),
            },
        },
        WalRecord {
            seqno: 1,
            op: WalOp::Insert {
                key: b"d5".to_vec(),
                value: b"doomed".to_vec(),
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
            op: WalOp::RemoveRange {
                start: b"d".to_vec(),
                end: b"e".to_vec(),
            },
        },
        WalRecord {
            seqno: 4,
            op: WalOp::Batch {
                entries: vec![
                    BatchEntry::Insert {
                        key: b"b1".to_vec(),
                        value: b"bv".to_vec(),
                    },
                    BatchEntry::Merge {
                        key: b"counter".to_vec(),
                        value: i64op(7),
                    },
                ],
            },
        },
        // --- first flush: everything above lands in the SST the crash damages
        WalRecord {
            seqno: 5,
            op: WalOp::Insert {
                key: b"z9".to_vec(),
                value: b"v9".to_vec(),
            },
        },
        // --- second flush: the SST above SURVIVES the repair
        WalRecord {
            seqno: 6,
            op: WalOp::Insert {
                key: b"m1".to_vec(),
                value: b"mv".to_vec(),
            },
        },
        // --- memtable only: lost with the crash, covered by the tail replay
    ]
}

/// Whether `key` falls inside the inclusive `[lo, hi]` coverage bounds (the
/// default bytewise comparator, matching the tree under test).
fn key_in_coverage(key: &[u8], lo: &[u8], hi: &[u8]) -> bool {
    key >= lo && key <= hi
}

/// One zone the reconciliation must cover: a reported lost key range, or —
/// when any loss is UNSCOPABLE (`unknowable_losses`) — the whole keyspace.
enum LostZone<'a> {
    /// The inclusive `[lo, hi]` bounds of one `lost_coverage` entry.
    Range(&'a [u8], &'a [u8]),
    /// No bounds exist: reconcile every retained record.
    WholeKeyspace,
}

impl LostZone<'_> {
    /// Whether a point operation's key falls inside the zone.
    fn covers_key(&self, key: &[u8]) -> bool {
        match self {
            Self::Range(lo, hi) => key_in_coverage(key, lo, hi),
            Self::WholeKeyspace => true,
        }
    }

    /// Whether a range deletion's half-open `[start, end)` span overlaps the
    /// zone.
    fn overlaps_span(&self, start: &[u8], end: &[u8]) -> bool {
        match self {
            Self::Range(lo, hi) => start <= *hi && *lo < end,
            Self::WholeKeyspace => true,
        }
    }

    /// The zone as scan bounds for the surviving-operand presence check.
    fn scan_bounds(&self) -> (std::ops::Bound<&[u8]>, std::ops::Bound<&[u8]>) {
        use std::ops::Bound;
        match self {
            Self::Range(lo, hi) => (Bound::Included(*lo), Bound::Included(*hi)),
            Self::WholeKeyspace => (Bound::Unbounded, Bound::Unbounded),
        }
    }
}

/// The section 4 reconciliation, exactly as specified: tail replay above `w`,
/// then per lost zone replay retained records at or below the bound — puts
/// and deletes blindly, merge operands only when the surviving-operand
/// multiset (from `scan_since_seqno_in_range`) does not already cover them.
/// An UNSCOPABLE loss (`unknowable_losses`) has no bounds, so the whole
/// keyspace is reconciled in ONE pass — running the per-range passes on top
/// of it would subtract the same survivors twice and double-fold merges.
fn reconcile_after_repair(
    tree: &AnyTree,
    wal: &ReferenceWal,
    report: &lsm_tree::RepairReport,
    w: u64,
) -> lsm_tree::Result<()> {
    use lsm_tree::{ScanSinceEvent, WalReplayScope};

    let records = wal.records().expect("wal records");

    // Tail replay (section 3), unchanged.
    for record in records.iter().filter(|r| r.seqno > w) {
        apply(tree, record)?;
    }

    let ceiling = match report.wal_replay_scope() {
        WalReplayScope::TailOnly => return Ok(()),
        WalReplayScope::LostUpTo(b) => b,
        WalReplayScope::FullHistory => u64::MAX,
    };

    let zones: Vec<LostZone> = if report.unknowable_losses.is_empty() {
        // COALESCE overlapping / touching ranges into one zone each, so a
        // record in the overlap of two lost coverages is processed exactly
        // once. (The per-pass survivor scan does see the previous pass's
        // replays — the scan covers the active memtable — but correctness
        // must not hinge on that subtlety.)
        let mut ranges: Vec<(&[u8], &[u8])> = report
            .lost_coverage
            .iter()
            .map(|(_, lo, hi, _)| (lo.as_ref(), hi.as_ref()))
            .collect();
        ranges.sort();
        let mut coalesced: Vec<(&[u8], &[u8])> = Vec::new();
        for (lo, hi) in ranges {
            match coalesced.last_mut() {
                Some((_, chi)) if lo <= *chi => {
                    if hi > *chi {
                        *chi = hi;
                    }
                }
                _ => coalesced.push((lo, hi)),
            }
        }
        coalesced
            .into_iter()
            .map(|(lo, hi)| LostZone::Range(lo, hi))
            .collect()
    } else {
        vec![LostZone::WholeKeyspace]
    };

    for zone in zones {
        // Surviving merge applications inside the zone, as a multiset of
        // (key, seqno, operand): the scan delivers one event per application
        // the tree will make, which is exactly the set to subtract.
        let AnyTree::Standard(standard) = tree else {
            panic!("this guard drives a standard tree");
        };
        let mut survived: std::collections::HashMap<(Vec<u8>, u64, Vec<u8>), usize> =
            std::collections::HashMap::new();
        // A compaction-FOLDED merge chain leaves a plain surviving value at
        // the chain head's seqno and no operand events, so absence from the
        // multiset does not prove an operand was lost. Track the highest
        // surviving value / point-tombstone seqno per key (and the surviving
        // range tombstones): an archived operand at or below it is already
        // incorporated — or superseded — and must not be reapplied.
        let mut superseded_floor: std::collections::HashMap<Vec<u8>, u64> =
            std::collections::HashMap::new();
        let mut surviving_range_tombstones: Vec<(Vec<u8>, Vec<u8>, u64)> = Vec::new();
        for event in standard.scan_since_seqno_in_range::<&[u8], _>(0, zone.scan_bounds())? {
            match event {
                ScanSinceEvent::MergeOperand {
                    key,
                    operand,
                    seqno,
                } => {
                    *survived
                        .entry((key.to_vec(), seqno, operand.to_vec()))
                        .or_default() += 1;
                }
                // A weak (single-delete) tombstone does NOT floor: it never
                // incorporates the key's older history — it annihilates
                // exactly its matching put during compaction and can then
                // expose an older value. Flooring at its seqno would skip
                // replaying that put from a lost SST, leaving the weak delete
                // to consume a different, older value than the source's pair.
                // Its blind replay stays idempotent (same internal key), so no
                // presence tracking is needed either.
                ScanSinceEvent::WeakTombstone { .. } => {}
                ScanSinceEvent::Insert { key, seqno, .. }
                | ScanSinceEvent::PointTombstone { key, seqno } => {
                    let floor = superseded_floor.entry(key.to_vec()).or_default();
                    // Seqno 0 is a bottommost-ZEROED survivor: per the
                    // section 4 GC-coordination rule it already incorporates
                    // the key's whole archived history (deployments that
                    // reconcile start their WAL seqnos at 1, so a genuine
                    // write cannot sit at 0), and its floor is unbounded.
                    *floor = (*floor).max(if seqno == 0 { u64::MAX } else { seqno });
                }
                ScanSinceEvent::RangeTombstone {
                    start_key,
                    end_key,
                    seqno,
                } => {
                    surviving_range_tombstones.push((start_key.to_vec(), end_key.to_vec(), seqno));
                }
            }
        }
        let record_superseded = |key: &[u8], seqno: u64| {
            superseded_floor.get(key).is_some_and(|&v| seqno <= v)
                // STRICT `<`, matching the engine's suppression rule
                // (`kv_seqno < rt.seqno`): a record TIED with the tombstone's
                // caller-assigned seqno survives a read, so it must replay.
                || surviving_range_tombstones
                    .iter()
                    .any(|(s, e, rt)| key >= s.as_slice() && key < e.as_slice() && seqno < *rt)
        };

        for record in records
            .iter()
            .filter(|r| r.seqno <= ceiling && r.seqno <= w)
        {
            match &record.op {
                WalOp::Merge { key, value } if zone.covers_key(key) => {
                    let covered = survived
                        .get_mut(&(key.clone(), record.seqno, value.clone()))
                        .filter(|count| **count > 0);
                    match covered {
                        // The operand survived the repair: replaying it would
                        // fold it twice.
                        Some(count) => *count -= 1,
                        // Folded into (or superseded by) a surviving value or
                        // tombstone: already accounted for.
                        None if record_superseded(key, record.seqno) => {}
                        None => apply(tree, record)?,
                    }
                }
                WalOp::Insert { key, .. } | WalOp::Remove { key } | WalOp::RemoveWeak { key }
                    if zone.covers_key(key) =>
                {
                    // Blind replay is idempotent against REAL surviving
                    // versions, but a record at or below the superseded
                    // floor is already incorporated (a bottommost-zeroed
                    // survivor sits at seqno 0 while embodying the whole
                    // folded history — replaying over it would resurrect a
                    // pre-fold state) or superseded outright.
                    if !record_superseded(key, record.seqno) {
                        apply(tree, record)?;
                    }
                }
                // A range deletion is selected by SPAN OVERLAP with the lost
                // coverage (its half-open `[start, end)` against the
                // inclusive `[lo, hi]`), and replays blindly like a point
                // delete: idempotent at its original seqno.
                WalOp::RemoveRange { start, end }
                    if zone.overlaps_span(start.as_slice(), end.as_slice()) =>
                {
                    apply(tree, record)?;
                }
                // A batch is a group of standalone operations sharing one
                // seqno: each entry the lost zone covers gets the same
                // treatment its standalone form gets — merge entries
                // subtract the survivors, the rest replay blindly. Entries
                // outside the zone were never lost and are skipped.
                WalOp::Batch { entries } => {
                    for entry in entries {
                        match entry {
                            BatchEntry::Insert { key, value }
                                if zone.covers_key(key)
                                    && !record_superseded(key, record.seqno) =>
                            {
                                tree.insert(key.as_slice(), value.as_slice(), record.seqno);
                            }
                            BatchEntry::Remove { key }
                                if zone.covers_key(key)
                                    && !record_superseded(key, record.seqno) =>
                            {
                                tree.remove(key.as_slice(), record.seqno);
                            }
                            BatchEntry::RemoveWeak { key }
                                if zone.covers_key(key)
                                    && !record_superseded(key, record.seqno) =>
                            {
                                tree.remove_weak(key.as_slice(), record.seqno);
                            }
                            BatchEntry::Merge { key, value } if zone.covers_key(key) => {
                                let covered = survived
                                    .get_mut(&(key.clone(), record.seqno, value.clone()))
                                    .filter(|count| **count > 0);
                                match covered {
                                    Some(count) => *count -= 1,
                                    None if record_superseded(key, record.seqno) => {}
                                    None => {
                                        tree.merge(key.as_slice(), value.as_slice(), record.seqno);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// Executable proof of section 4: a crash that also damages a flushed SST is
/// recovered by repair + the documented reconciliation, byte-for-byte to the
/// non-crashed run — including merge operands split across a lost and a
/// surviving SST, which is the case the presence check exists for.
#[test]
fn repair_and_reconciled_replay_recover_identical_state() -> lsm_tree::Result<()> {
    // Reference: the same workload applied to a healthy tree.
    let reference_dir = tempfile::tempdir().expect("tempdir");
    let reference = {
        let tree = open_tree(reference_dir.path());
        for record in repair_workload() {
            apply(&tree, &record)?;
        }
        snapshot(&tree)
    };

    let crash_dir = tempfile::tempdir().expect("tempdir");
    let wal = crashed_repairable_store(crash_dir.path())?;

    // One-call recovery entry point; no salvage, so the damaged SST is
    // excluded and its coverage reported.
    let (tree, repaired) = Config::new(
        crash_dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(CounterMerge)))
    .open_or_repair(lsm_tree::RepairPolicy::default())?;
    let report = repaired.expect("the damaged store opens only after a repair");
    assert!(
        !report.lost_coverage.is_empty(),
        "the excluded SST's coverage must be reported: {report:?}",
    );
    assert_eq!(
        report.wal_replay_scope(),
        lsm_tree::WalReplayScope::LostUpTo(3),
        "the lost SST topped out at seqno 3",
    );

    // W: everything through the last flush (seqno 5) was applied and
    // persisted before the crash; the WAL retained the full history (the
    // archive-retention deployment).
    reconcile_after_repair(&tree, &wal, &report, 5)?;

    assert_eq!(
        snapshot(&tree),
        reference,
        "repair + reconciled replay must reproduce the non-crashed state",
    );
    assert_eq!(
        counter_of(&tree),
        Some(5 + 7 + 11),
        "each merge operand folded exactly once across the lost and the \
         surviving SST",
    );
    Ok(())
}

/// Two lost SSTs with OVERLAPPING coverage must not double-process a record
/// in the overlap: the reconciliation coalesces overlapping ranges into one
/// zone, so a lost merge operand folds exactly once and a blind put replays
/// exactly once regardless of how many lost tables covered its key.
#[test]
fn reconciled_replay_coalesces_overlapping_loss_ranges() -> lsm_tree::Result<()> {
    let i64op = |n: i64| n.to_le_bytes().to_vec();
    // Seqnos start at 1, per the section 4 GC-coordination rule.
    let records = vec![
        WalRecord {
            seqno: 1,
            op: WalOp::Insert {
                key: b"a1".to_vec(),
                value: b"v1".to_vec(),
            },
        },
        WalRecord {
            seqno: 2,
            op: WalOp::Merge {
                key: b"counter".to_vec(),
                value: i64op(5),
            },
        },
        // --- flush: SST [a1, counter], damaged below
        WalRecord {
            seqno: 3,
            op: WalOp::Insert {
                key: b"b1".to_vec(),
                value: b"v3".to_vec(),
            },
        },
        WalRecord {
            seqno: 4,
            op: WalOp::Insert {
                key: b"z9".to_vec(),
                value: b"v4".to_vec(),
            },
        },
        // --- flush: SST [b1, z9], damaged below — overlaps [a1, counter]
        WalRecord {
            seqno: 5,
            op: WalOp::Insert {
                key: b"m5".to_vec(),
                value: b"mv".to_vec(),
            },
        },
        // --- memtable only: covered by the tail replay
    ];

    // Reference: the same workload applied to a healthy tree.
    let reference_dir = tempfile::tempdir().expect("tempdir");
    let reference = {
        let tree = open_tree(reference_dir.path());
        for record in &records {
            apply(&tree, record)?;
        }
        snapshot(&tree)
    };

    let crash_dir = tempfile::tempdir().expect("tempdir");
    let wal_path = crash_dir.path().join("wal.log");
    let mut wal = ReferenceWal::create(&wal_path).expect("create wal");
    {
        let tree = open_tree(crash_dir.path());
        for record in &records {
            wal.append(record).expect("wal append");
            apply(&tree, record)?;
            if record.seqno == 2 || record.seqno == 4 {
                tree.flush_active_memtable(0)?;
            }
        }
    }
    // Damage BOTH flushed SSTs' leading data blocks: their metadata still
    // parses, so both overlapping coverages are reported.
    for entry in std::fs::read_dir(crash_dir.path().join("tables")).expect("read tables dir") {
        let entry = entry.expect("dir entry");
        if entry.file_name().to_string_lossy().parse::<u64>().is_err() {
            continue;
        }
        let path = entry.path();
        let mut bytes = std::fs::read(&path).expect("read sst");
        for b in bytes.get_mut(8..24).expect("sst larger than 24 bytes") {
            *b ^= 0xFF;
        }
        std::fs::write(&path, &bytes).expect("write corruption");
    }
    for entry in std::fs::read_dir(crash_dir.path()).expect("read dir") {
        let entry = entry.expect("dir entry");
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let is_version = name
            .strip_prefix('v')
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version || name == "current" {
            std::fs::remove_file(entry.path()).expect("remove manifest file");
        }
    }

    let (tree, repaired) = Config::new(
        crash_dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(CounterMerge)))
    .open_or_repair(lsm_tree::RepairPolicy::default())?;
    let report = repaired.expect("the damaged store opens only after a repair");
    assert_eq!(
        report.lost_coverage.len(),
        2,
        "both overlapping coverages are reported: {report:?}",
    );

    reconcile_after_repair(&tree, &wal, &report, 4)?;

    assert_eq!(
        snapshot(&tree),
        reference,
        "a record in the overlap of two lost ranges must be processed once",
    );
    assert_eq!(
        counter_of(&tree),
        Some(5),
        "the lost operand in the overlap folds exactly once",
    );
    Ok(())
}

/// The engine suppresses a record under a range tombstone only STRICTLY
/// below it (`kv_seqno < rt.seqno`): a record TIED with the tombstone's
/// caller-assigned seqno survives a read. The reconciliation's coverage test
/// must use the same strict comparison, or a lost record tied with a
/// surviving range tombstone is skipped and the repaired tree diverges from
/// the pre-loss one.
#[test]
fn reconciled_replay_keeps_records_tied_with_surviving_range_tombstones() -> lsm_tree::Result<()> {
    // Seqnos start at 1, per the section 4 GC-coordination rule.
    let records = vec![
        WalRecord {
            seqno: 1,
            op: WalOp::Insert {
                key: b"a1".to_vec(),
                value: b"v1".to_vec(),
            },
        },
        WalRecord {
            seqno: 2,
            op: WalOp::Insert {
                key: b"x7".to_vec(),
                value: b"vx".to_vec(),
            },
        },
        // --- flush: SST [a1, x7], damaged below
        WalRecord {
            seqno: 2,
            op: WalOp::RemoveRange {
                start: b"x".to_vec(),
                end: b"y".to_vec(),
            },
        },
        WalRecord {
            seqno: 3,
            op: WalOp::Insert {
                key: b"z9".to_vec(),
                value: b"v3".to_vec(),
            },
        },
        // --- flush: the surviving SST carries the TIED range tombstone
        WalRecord {
            seqno: 4,
            op: WalOp::Insert {
                key: b"m1".to_vec(),
                value: b"mv".to_vec(),
            },
        },
        // --- memtable only: covered by the tail replay
    ];

    // Reference: the same workload applied to a healthy tree.
    let reference_dir = tempfile::tempdir().expect("tempdir");
    let reference = {
        let tree = open_tree(reference_dir.path());
        for record in &records {
            apply(&tree, record)?;
        }
        snapshot(&tree)
    };
    assert!(
        reference.iter().any(|(k, _)| k == b"x7"),
        "the tied record survives a read of the healthy tree: {reference:?}",
    );

    let crash_dir = tempfile::tempdir().expect("tempdir");
    let wal_path = crash_dir.path().join("wal.log");
    let mut wal = ReferenceWal::create(&wal_path).expect("create wal");
    {
        let tree = open_tree(crash_dir.path());
        let mut flushed_first = false;
        for record in &records {
            wal.append(record).expect("wal append");
            apply(&tree, record)?;
            if record.seqno == 2 && !flushed_first {
                // After the point inserts, BEFORE the tied range deletion.
                flushed_first = true;
                tree.flush_active_memtable(0)?;
            } else if record.seqno == 3 {
                tree.flush_active_memtable(0)?;
            }
        }
    }
    // Damage the FIRST SST's leading data block: metadata still parses, so
    // its coverage is reported; the tombstone-bearing SST survives.
    let sst = crash_dir.path().join("tables").join("0");
    let mut bytes = std::fs::read(&sst).expect("read sst");
    for b in bytes.get_mut(8..24).expect("sst larger than 24 bytes") {
        *b ^= 0xFF;
    }
    std::fs::write(&sst, &bytes).expect("write corruption");
    for entry in std::fs::read_dir(crash_dir.path()).expect("read dir") {
        let entry = entry.expect("dir entry");
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let is_version = name
            .strip_prefix('v')
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version || name == "current" {
            std::fs::remove_file(entry.path()).expect("remove manifest file");
        }
    }

    let (tree, repaired) = Config::new(
        crash_dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(CounterMerge)))
    .open_or_repair(lsm_tree::RepairPolicy::default())?;
    let report = repaired.expect("the damaged store opens only after a repair");
    assert!(
        !report.lost_coverage.is_empty(),
        "the excluded SST's coverage must be reported: {report:?}",
    );

    reconcile_after_repair(&tree, &wal, &report, 3)?;

    assert_eq!(
        snapshot(&tree),
        reference,
        "a lost record tied with a surviving range tombstone must be replayed",
    );
    Ok(())
}

/// A surviving weak (single-delete) tombstone must NOT raise the superseded
/// floor: unlike a value or a regular tombstone it does not incorporate the
/// key's older history — it annihilates exactly its matching put during
/// compaction and can then expose an older value. When that matching put sat
/// in a lost SST, flooring at the weak tombstone's seqno skips its replay,
/// and the weak delete later consumes a DIFFERENT (older) value than the
/// source's pair — diverging both the intermediate snapshots and the
/// compacted end state.
#[test]
fn reconciled_replay_restores_the_put_a_surviving_weak_delete_pairs_with() -> lsm_tree::Result<()> {
    // Seqnos start at 1, per the section 4 GC-coordination rule.
    let records = vec![
        WalRecord {
            seqno: 1,
            op: WalOp::Insert {
                key: b"w1".to_vec(),
                value: b"old".to_vec(),
            },
        },
        // --- flush: SST 0 (survives) holds the OLDER value
        WalRecord {
            seqno: 2,
            op: WalOp::Insert {
                key: b"w1".to_vec(),
                value: b"mid".to_vec(),
            },
        },
        // --- flush: SST 1 (damaged below) holds the put the weak delete pairs with
        WalRecord {
            seqno: 3,
            op: WalOp::RemoveWeak {
                key: b"w1".to_vec(),
            },
        },
        // --- flush: SST 2 (survives) holds the weak tombstone
    ];

    // Reference: the same workload applied to a healthy tree.
    let reference_dir = tempfile::tempdir().expect("tempdir");
    let reference = {
        let tree = open_tree(reference_dir.path());
        for record in &records {
            apply(&tree, record)?;
        }
        assert_eq!(
            tree.get(b"w1", 3)?.as_deref(),
            Some(b"mid".as_slice()),
            "self-check: a snapshot below the weak delete reads the paired put",
        );
        snapshot(&tree)
    };

    let crash_dir = tempfile::tempdir().expect("tempdir");
    let wal_path = crash_dir.path().join("wal.log");
    let mut wal = ReferenceWal::create(&wal_path).expect("create wal");
    {
        let tree = open_tree(crash_dir.path());
        for record in &records {
            wal.append(record).expect("wal append");
            apply(&tree, record)?;
            tree.flush_active_memtable(0)?;
        }
    }
    // Damage the SECOND SST's leading data block: metadata still parses, so
    // its coverage is reported; the weak-tombstone-bearing SST survives.
    let sst = crash_dir.path().join("tables").join("1");
    let mut bytes = std::fs::read(&sst).expect("read sst");
    for b in bytes.get_mut(8..24).expect("sst larger than 24 bytes") {
        *b ^= 0xFF;
    }
    std::fs::write(&sst, &bytes).expect("write corruption");
    for entry in std::fs::read_dir(crash_dir.path()).expect("read dir") {
        let entry = entry.expect("dir entry");
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let is_version = name
            .strip_prefix('v')
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version || name == "current" {
            std::fs::remove_file(entry.path()).expect("remove manifest file");
        }
    }

    let (tree, repaired) = Config::new(
        crash_dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(CounterMerge)))
    .open_or_repair(lsm_tree::RepairPolicy::default())?;
    let report = repaired.expect("the damaged store opens only after a repair");
    assert!(
        !report.lost_coverage.is_empty(),
        "the excluded SST's coverage must be reported: {report:?}",
    );

    reconcile_after_repair(&tree, &wal, &report, 3)?;

    assert_eq!(
        tree.get(b"w1", 3)?.as_deref(),
        Some(b"mid".as_slice()),
        "the put the surviving weak delete pairs with must be replayed — a \
         floor at the weak tombstone's seqno would leave the older value to \
         be consumed instead",
    );
    assert_eq!(
        snapshot(&tree),
        reference,
        "the reconciled tree mirrors the healthy tree's visible state",
    );
    Ok(())
}

/// A merge chain FOLDED by compaction leaves no `MergeOperand` events: the
/// surviving SST holds a plain `Value` carrying the chain head's seqno. When
/// an unrelated lost table's coarse key range covers that key, the archived
/// operands are absent from the survivor multiset — the reconciliation must
/// still not reapply them on top of the already-folded value.
#[test]
fn reconciled_replay_skips_operands_folded_into_surviving_values() -> lsm_tree::Result<()> {
    let i64op = |n: i64| n.to_le_bytes().to_vec();
    // Seqnos start at 1, per the section 4 GC-coordination rule: seqno 0 is
    // reserved as the bottommost-zeroed marker.
    let records = vec![
        WalRecord {
            seqno: 1,
            op: WalOp::Insert {
                key: b"counter".to_vec(),
                value: i64op(1),
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
            op: WalOp::Merge {
                key: b"counter".to_vec(),
                value: i64op(7),
            },
        },
        // --- flush + major compaction: the chain folds (and the bottommost
        //     pass zeroes the survivor's seqno)
        WalRecord {
            seqno: 4,
            op: WalOp::Insert {
                key: b"a1".to_vec(),
                value: b"v1".to_vec(),
            },
        },
        WalRecord {
            seqno: 5,
            op: WalOp::Insert {
                key: b"z5".to_vec(),
                value: b"v5".to_vec(),
            },
        },
        // --- flush: the [a1, z5] SST is the one the crash damages
        WalRecord {
            seqno: 6,
            op: WalOp::Insert {
                key: b"m9".to_vec(),
                value: b"mv".to_vec(),
            },
        },
        // --- memtable only: covered by the tail replay
    ];

    // Reference: the same workload applied to a healthy tree.
    let reference_dir = tempfile::tempdir().expect("tempdir");
    let reference = {
        let tree = open_tree(reference_dir.path());
        for record in &records {
            apply(&tree, record)?;
        }
        snapshot(&tree)
    };

    // The crashed store: the counter chain folds via compaction, then the
    // unrelated [a1, z5] SST is flushed, damaged, and the manifest removed.
    let crash_dir = tempfile::tempdir().expect("tempdir");
    let wal_path = crash_dir.path().join("wal.log");
    let mut wal = ReferenceWal::create(&wal_path).expect("create wal");
    {
        let tree = open_tree(crash_dir.path());
        for record in &records {
            wal.append(record).expect("wal append");
            apply(&tree, record)?;
            if record.seqno == 3 {
                tree.flush_active_memtable(0)?;
                // GC watermark far above the chain: the operands fold, and
                // the bottommost pass zeroes the surviving value's seqno.
                tree.major_compact(u64::MAX, 1_000)?;
            }
            if record.seqno == 5 {
                tree.flush_active_memtable(0)?;
            }
        }
    }
    // The freshly flushed [a1, z5] SST has the HIGHEST table id (the
    // compaction output was allocated earlier). Corrupt its leading data
    // block: metadata still parses, so its coverage is reported.
    let tables = crash_dir.path().join("tables");
    let damaged = std::fs::read_dir(&tables)
        .expect("read tables dir")
        .filter_map(|e| {
            let e = e.expect("dir entry");
            e.file_name()
                .to_string_lossy()
                .parse::<u64>()
                .ok()
                .map(|id| (id, e.path()))
        })
        .max_by_key(|(id, _)| *id)
        .map(|(_, path)| path)
        .expect("a flushed SST");
    let mut bytes = std::fs::read(&damaged).expect("read sst");
    for b in bytes.get_mut(8..24).expect("sst larger than 24 bytes") {
        *b ^= 0xFF;
    }
    std::fs::write(&damaged, &bytes).expect("write corruption");
    for entry in std::fs::read_dir(crash_dir.path()).expect("read dir") {
        let entry = entry.expect("dir entry");
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let is_version = name
            .strip_prefix('v')
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version || name == "current" {
            std::fs::remove_file(entry.path()).expect("remove manifest file");
        }
    }

    let (tree, repaired) = Config::new(
        crash_dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(CounterMerge)))
    .open_or_repair(lsm_tree::RepairPolicy::default())?;
    let report = repaired.expect("the damaged store opens only after a repair");
    assert!(
        report
            .lost_coverage
            .iter()
            .any(|(_, lo, hi, _)| key_in_coverage(b"counter", lo, hi)),
        "the coarse lost range must cover the folded key for this scenario: {report:?}",
    );

    reconcile_after_repair(&tree, &wal, &report, 5)?;

    assert_eq!(
        snapshot(&tree),
        reference,
        "operands folded into a surviving value must not be reapplied",
    );
    assert_eq!(
        counter_of(&tree),
        Some(1 + 5 + 7),
        "the folded chain counts each operand exactly once",
    );
    Ok(())
}

/// An UNSCOPABLE loss (`unknowable_losses`: the excluded SST's metadata never
/// parsed) reports no key range at all, so `FullHistory` cannot be served by
/// iterating `lost_coverage` — the reconciliation must scan and reconcile
/// retained records across the ENTIRE keyspace, merge presence checks
/// included, or the lost table's pre-watermark records are never replayed.
#[test]
fn reconciled_replay_covers_the_whole_keyspace_for_unknowable_losses() -> lsm_tree::Result<()> {
    // Reference: the same workload applied to a healthy tree.
    let reference_dir = tempfile::tempdir().expect("tempdir");
    let reference = {
        let tree = open_tree(reference_dir.path());
        for record in repair_workload() {
            apply(&tree, &record)?;
        }
        snapshot(&tree)
    };

    let crash_dir = tempfile::tempdir().expect("tempdir");
    let wal = crashed_store_with(crash_dir.path(), &repair_workload(), &[3, 5])?;
    // Deepen the damage: the whole first SST is garbage, so not even its
    // metadata parses — the loss is unscopable.
    std::fs::write(
        crash_dir.path().join("tables").join("0"),
        b"not a table at all",
    )
    .expect("overwrite sst");

    let (tree, repaired) = Config::new(
        crash_dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(CounterMerge)))
    .open_or_repair(lsm_tree::RepairPolicy::default())?;
    let report = repaired.expect("the damaged store opens only after a repair");
    assert!(
        report.lost_coverage.is_empty(),
        "an unparseable SST has no coverage to report: {report:?}",
    );
    assert_eq!(
        report.unknowable_losses.len(),
        1,
        "the unparseable SST is an unscopable loss: {report:?}",
    );
    assert_eq!(
        report.wal_replay_scope(),
        lsm_tree::WalReplayScope::FullHistory,
        "no bound can scope the damage",
    );

    reconcile_after_repair(&tree, &wal, &report, 5)?;

    assert_eq!(
        snapshot(&tree),
        reference,
        "the whole-keyspace reconciliation must recover the unscopable loss",
    );
    assert_eq!(
        counter_of(&tree),
        Some(5 + 7 + 11),
        "lost operands replayed, the surviving one subtracted — exactly once each",
    );
    Ok(())
}

/// Section 4 covers EVERY logged write kind: a range deletion in the lost SST
/// replays by span overlap with the lost coverage, and a batch replays per
/// entry with the same treatment its standalone form gets (merge entries
/// subtract the survivors). Skipping either resurrects the tombstoned key
/// and loses the batch's writes.
#[test]
fn reconciled_replay_covers_range_deletions_and_batches() -> lsm_tree::Result<()> {
    // Reference: the same workload applied to a healthy tree.
    let reference_dir = tempfile::tempdir().expect("tempdir");
    let reference = {
        let tree = open_tree(reference_dir.path());
        for record in range_and_batch_workload() {
            apply(&tree, &record)?;
        }
        snapshot(&tree)
    };

    let crash_dir = tempfile::tempdir().expect("tempdir");
    let wal = crashed_store_with(crash_dir.path(), &range_and_batch_workload(), &[4, 5])?;

    let (tree, repaired) = Config::new(
        crash_dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(CounterMerge)))
    .open_or_repair(lsm_tree::RepairPolicy::default())?;
    let report = repaired.expect("the damaged store opens only after a repair");
    assert!(
        !report.lost_coverage.is_empty(),
        "the excluded SST's coverage must be reported: {report:?}",
    );

    reconcile_after_repair(&tree, &wal, &report, 5)?;

    assert_eq!(
        snapshot(&tree),
        reference,
        "the reconciliation must replay the range deletion (no resurrected \
         d5) and the batch's entries (b1 present, counter complete)",
    );
    assert_eq!(
        counter_of(&tree),
        Some(5 + 7),
        "both lost merge operands — standalone and batched — folded once",
    );
    Ok(())
}

/// Contract guard: skipping the presence check — replaying every retained
/// lost-range merge operand up to `W` instead of subtracting the survivors —
/// double-folds the operand that SURVIVED the repair in the second SST. The
/// wrong recovery is detectably wrong, which is why section 4 demands the
/// `scan_since_seqno_in_range` multiset subtraction rather than any
/// seqno-window heuristic.
#[test]
fn blindly_replaying_lost_range_merges_double_counts() -> lsm_tree::Result<()> {
    let crash_dir = tempfile::tempdir().expect("tempdir");
    let wal = crashed_repairable_store(crash_dir.path())?;

    let (tree, repaired) = Config::new(
        crash_dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(CounterMerge)))
    .open_or_repair(lsm_tree::RepairPolicy::default())?;
    let report = repaired.expect("the damaged store opens only after a repair");
    let w = 5;

    // The WRONG reconciliation: the correct tail, then every retained record
    // whose key falls in a lost range replayed up to `W` — no survivor
    // subtraction. The +5 and +7 operands were genuinely lost (replaying them
    // is right), but the surviving +11 at seqno 5 is inside `key range × ≤ W`
    // and folds a second time.
    let records = wal.records().expect("wal records");
    for record in records.iter().filter(|r| r.seqno > w) {
        apply(&tree, record)?;
    }
    for (_, lo, hi, _) in &report.lost_coverage {
        for record in records.iter().filter(|r| r.seqno <= w) {
            let key = match &record.op {
                WalOp::Insert { key, .. }
                | WalOp::Remove { key }
                | WalOp::RemoveWeak { key }
                | WalOp::Merge { key, .. } => key,
                WalOp::RemoveRange { .. } | WalOp::Batch { .. } => continue,
            };
            if key_in_coverage(key, lo, hi) {
                apply(&tree, record)?;
            }
        }
    }

    assert_eq!(
        counter_of(&tree),
        Some(5 + 7 + 11 + 11),
        "without the survivor subtraction the surviving operand folds twice",
    );
    Ok(())
}

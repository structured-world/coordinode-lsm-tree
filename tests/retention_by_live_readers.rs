// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! History is retained for live readers, not for a window of snapshots.
//!
//! A snapshot above the persisted retention floor is answered from the latest
//! version: the compaction folds keep every key version such a snapshot can
//! read, and every loss they report raises the floor. So no older version has
//! to stay in the tree for a snapshot's sake, and the tables compactions
//! consumed leave the disk as soon as no reader holds the version that named
//! them. A reader that resolved a version keeps it for as long as it reads.

mod common;

use lsm_tree::{
    AbstractTree, AnyTree, Config, Error, MergeOperator, SeqNo, SequenceNumberCounter, UserValue,
};
use std::collections::BTreeSet;
use std::path::Path;
use std::time::{Duration, Instant};
use test_log::test;

/// The counter sits above the data seqnos, as it does in a deployment: inserts
/// and installs draw from the same one.
fn open(dir: &Path, seqno: &SequenceNumberCounter) -> lsm_tree::Result<AnyTree> {
    Config::new(dir, seqno.clone(), SequenceNumberCounter::default()).open()
}

fn key(i: u32) -> String {
    format!("key-{i:05}")
}

/// Table ids with a file in the tree's tables folder.
fn table_files(dir: &Path) -> BTreeSet<u64> {
    std::fs::read_dir(dir.join("tables"))
        .expect("the tables folder exists")
        .filter_map(|entry| {
            entry
                .expect("a readable entry")
                .file_name()
                .to_str()
                .and_then(|name| name.parse().ok())
        })
        .collect()
}

fn current_tables(tree: &AnyTree) -> BTreeSet<u64> {
    tree.current_version()
        .iter_tables()
        .map(lsm_tree::Table::id)
        .collect()
}

/// Waits for the tables folder to hold exactly the current version's tables.
/// A released table is unlinked off the foreground path, so the folder
/// catches up shortly after the last hold on it drops.
fn assert_only_current_tables_on_disk(tree: &AnyTree, dir: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (on_disk, current) = (table_files(dir), current_tables(tree));
        if on_disk == current {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "tables on disk {on_disk:?} outlived the current version {current:?}",
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn compaction_inside_the_window_serves_old_snapshots_from_the_latest_version()
-> lsm_tree::Result<()> {
    // The window opens after the first generation, so a snapshot at its start
    // reads generation one and a compaction at that watermark must keep it.
    let dir = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let tree = open(dir.path(), &seqno)?;

    for i in 0..100 {
        tree.insert(key(i), "v1", seqno.next());
    }
    tree.flush_active_memtable(0)?;
    let window = seqno.get();
    for i in 0..100 {
        tree.insert(key(i), "v2", seqno.next());
    }
    tree.flush_active_memtable(0)?;

    tree.major_compact(common::COMPACTION_TARGET, window)?;

    for i in 0..100 {
        assert_eq!(tree.get(key(i), window)?.as_deref(), Some(b"v1".as_slice()));
        assert_eq!(
            tree.get(key(i), SeqNo::MAX)?.as_deref(),
            Some(b"v2".as_slice())
        );
    }
    assert!(
        tree.oldest_retained_seqno() < window,
        "nothing the window reads was collected",
    );
    // Nothing reads the versions the compaction replaced, so their tables go.
    assert_only_current_tables_on_disk(&tree, dir.path());
    Ok(())
}

#[test]
fn iterator_opened_before_an_install_reads_its_own_version_until_dropped() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let tree = open(dir.path(), &seqno)?;

    for generation in ["a", "b"] {
        for i in 0..50 {
            tree.insert(key(i), generation, seqno.next());
        }
        tree.flush_active_memtable(0)?;
    }
    let before = current_tables(&tree);

    let mut reader = tree.range::<&str, _>(.., SeqNo::MAX, None);
    // Watermark 0 collects nothing, so only the reader holds the inputs.
    tree.major_compact(common::COMPACTION_TARGET, 0)?;
    assert!(
        before.is_disjoint(&current_tables(&tree)),
        "the compaction replaced every table",
    );
    assert!(
        before.is_subset(&table_files(dir.path())),
        "the reader's tables stay on disk while it reads",
    );

    let mut read = 0;
    for guard in reader.by_ref() {
        let (_, value) = common::guard_to_kv(guard)?;
        assert_eq!(value, b"b");
        read += 1;
    }
    assert_eq!(read, 50, "the reader read its own version to the end");

    drop(reader);
    assert_only_current_tables_on_disk(&tree, dir.path());
    Ok(())
}

/// What a read at `snapshot` returned, comparable across a reopen.
fn probe(tree: &AnyTree, snapshot: SeqNo) -> Result<Option<Vec<u8>>, (SeqNo, SeqNo)> {
    match tree.get("k", snapshot) {
        Ok(value) => Ok(value.map(|v| v.to_vec())),
        Err(Error::SnapshotBelowRetention {
            requested,
            oldest_retained,
        }) => Err((requested, oldest_retained)),
        Err(other) => panic!("unexpected error at snapshot {snapshot}: {other:?}"),
    }
}

#[test]
fn reads_on_either_side_of_a_reopen_agree() -> lsm_tree::Result<()> {
    // A live tree and a reopened one draw the same boundary: every read is
    // answered or refused the same way on both sides.
    let dir = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let tree = open(dir.path(), &seqno)?;

    let first = seqno.next();
    tree.insert("k", "v1", first);
    tree.flush_active_memtable(0)?;
    let second = seqno.next();
    tree.insert("k", "v2", second);
    tree.flush_active_memtable(0)?;
    // Writes elsewhere move the counter on, so the watermark sits well above
    // the last install and snapshots fall between the two.
    for _ in 0..5 {
        tree.insert("other", "x", seqno.next());
    }
    let watermark = seqno.get();
    tree.major_compact(common::COMPACTION_TARGET, watermark)?;

    let snapshots = [
        0,
        1,
        first + 1,
        second,
        second + 1,
        watermark - 1,
        watermark,
        SeqNo::MAX,
    ];
    let live: Vec<_> = snapshots.iter().map(|s| probe(&tree, *s)).collect();
    assert_eq!(
        live[snapshots.len() - 1],
        Ok(Some(b"v2".to_vec())),
        "the latest value",
    );
    assert!(
        live.iter().any(Result::is_err),
        "the collected version is refused, not answered",
    );

    drop(tree);
    let reopened = open(dir.path(), &SequenceNumberCounter::new(seqno.get()))?;
    let after: Vec<_> = snapshots.iter().map(|s| probe(&reopened, *s)).collect();
    assert_eq!(live, after, "per snapshot {snapshots:?}");
    Ok(())
}

struct ConcatMerge;

impl MergeOperator for ConcatMerge {
    fn merge(
        &self,
        _key: &[u8],
        base_value: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> lsm_tree::Result<UserValue> {
        let mut out = base_value.unwrap_or_default().to_vec();
        for operand in operands {
            out.push(b',');
            out.extend_from_slice(operand);
        }
        Ok(out.into())
    }
}

#[test]
fn merge_snapshot_between_base_and_operand_above_the_watermark_reads_the_base_alone()
-> lsm_tree::Result<()> {
    // The fold resolves merges only below the watermark, so a snapshot between
    // a base and an operand that both sit above it still sees the base alone
    // once it is served from the compaction's output.
    let dir = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let tree = Config::new(dir.path(), seqno.clone(), SequenceNumberCounter::default())
        .with_merge_operator(Some(std::sync::Arc::new(ConcatMerge)))
        .open()?;

    let watermark = seqno.next();
    let base = seqno.next();
    tree.insert("k", "base", base);
    let operand = seqno.next();
    tree.merge("k", "op", operand);
    tree.flush_active_memtable(0)?;
    tree.major_compact(common::COMPACTION_TARGET, watermark)?;

    assert_eq!(
        tree.get("k", base + 1)?.as_deref(),
        Some(b"base".as_slice()),
        "between the base and the operand",
    );
    assert!(base < operand, "the operand is invisible at `base + 1`");
    assert_eq!(
        tree.get("k", SeqNo::MAX)?.as_deref(),
        Some(b"base,op".as_slice()),
    );
    Ok(())
}

/// Runs `rounds` rounds of writes and compacts after each at a watermark that
/// keeps the last `window` rounds readable. Returns the tree, the folder and
/// the item count the current version holds.
fn run_rounds(
    rounds: u32,
    window: u32,
    keys: impl Fn(u32) -> Vec<String>,
) -> lsm_tree::Result<(AnyTree, tempfile::TempDir, u64)> {
    let dir = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let tree = open(dir.path(), &seqno)?;

    let mut round_starts = Vec::new();
    for round in 0..rounds {
        round_starts.push(seqno.get());
        for k in keys(round) {
            tree.insert(k, format!("round-{round}"), seqno.next());
        }
        tree.flush_active_memtable(0)?;
        let oldest_in_window = round.saturating_sub(window - 1) as usize;
        tree.major_compact(common::COMPACTION_TARGET, round_starts[oldest_in_window])?;
        assert_only_current_tables_on_disk(&tree, dir.path());
    }
    let items = tree
        .current_version()
        .iter_tables()
        .map(|t| t.metadata.item_count)
        .sum();
    Ok((tree, dir, items))
}

#[test]
fn retained_history_overwrite_pattern_keeps_one_version_past_the_window() -> lsm_tree::Result<()> {
    // Every round rewrites the same keys. The tables on disk are the current
    // version's alone after every round, and those hold the window's versions
    // plus the newest one below it, per key.
    const KEYS: u32 = 200;
    let (tree, _dir, items) = run_rounds(6, 2, |_| (0..KEYS).map(key).collect())?;
    assert!(
        items <= u64::from(KEYS) * 3,
        "{items} items for {KEYS} keys and a two-round window",
    );
    assert_eq!(
        tree.get(key(0), SeqNo::MAX)?.as_deref(),
        Some(b"round-5".as_slice())
    );
    Ok(())
}

#[test]
fn retained_history_append_patterns_keep_nothing_extra() -> lsm_tree::Result<()> {
    // Appends supersede nothing, so the current version holds exactly what was
    // written and nothing else stays on disk, ascending keys or scattered.
    const PER_ROUND: u32 = 200;
    const ROUNDS: u32 = 6;
    // Ascending: each round's keys sort after every earlier round's. Scattered:
    // each round's keys interleave with every earlier round's across the whole
    // key range, so its compactions rewrite, not just move, what is there.
    let ascending = |round: u32| (0..PER_ROUND).map(|i| key(round * PER_ROUND + i)).collect();
    let scattered = |round: u32| (0..PER_ROUND).map(|i| key(i * ROUNDS + round)).collect();
    for (name, keys) in [
        (
            "ascending",
            Box::new(ascending) as Box<dyn Fn(u32) -> Vec<String>>,
        ),
        ("scattered", Box::new(scattered)),
    ] {
        let (_tree, _dir, items) = run_rounds(ROUNDS, 2, keys)?;
        assert_eq!(items, u64::from(PER_ROUND * ROUNDS), "{name}");
    }
    Ok(())
}

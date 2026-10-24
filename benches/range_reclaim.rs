// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Range expiration cost for a time series kept a fixed number of days, one
//! flushed table per day:
//! - `expire`: deleting the oldest day with a range tombstone, its flush and
//!   the compaction run that follows, which unlinks the day's table;
//! - `settle`: the same expiry followed by leveled compactions until they have
//!   nothing left to do, with the bytes they read and wrote;
//! - `idle`: a compaction run that finds nothing to do, the cost every run
//!   pays for the unlink check, on a tree of many tables.
//!
//! Prints one line per measurement.

use lsm_tree::{
    AbstractTree, AnyTree, Config, SequenceNumberCounter,
    compaction::{CompactionAction, Fifo, Leveled},
};
use std::sync::Arc;
use std::time::{Duration, Instant};

const DAYS: u32 = 10;
const ROWS_PER_DAY: u32 = 20_000;
const EXPIRIES: u32 = 5;
const IDLE_TABLES: u32 = 400;
const IDLE_RUNS: usize = 2_000;

fn day_key(day: u32, row: u32) -> Vec<u8> {
    format!("ts/{day:04}/{row:08}").into_bytes()
}

/// The first key of `day`, which is also the exclusive end of the day before.
fn day_start(day: u32) -> Vec<u8> {
    format!("ts/{day:04}/").into_bytes()
}

/// A tree holding `DAYS` days, one flushed table per day, and the next seqno.
fn fill(folder: &tempfile::TempDir) -> (AnyTree, u64) {
    let tree = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .unwrap();
    let mut seqno = 0;
    for day in 0..DAYS {
        for row in 0..ROWS_PER_DAY {
            tree.insert(day_key(day, row), [b'v'; 100], seqno);
            seqno += 1;
        }
        tree.flush_active_memtable(0).unwrap();
    }
    (tree, seqno)
}

/// A compaction strategy that chooses nothing, so a run does only what every
/// run does before its strategy chooses.
fn nothing() -> Arc<Fifo> {
    Arc::new(Fifo::new(u64::MAX, None))
}

/// The bytes on disk since `before`, negative when the tree grew (the
/// tombstone's own table).
fn freed(tree: &AnyTree, before: u64) -> i128 {
    i128::from(before) - i128::from(tree.disk_space())
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn p99(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples[(samples.len() * 99 / 100).min(samples.len() - 1)]
}

fn expire() {
    let folder = tempfile::tempdir().unwrap();
    let (tree, mut seqno) = fill(&folder);
    for day in 0..EXPIRIES {
        let before = tree.disk_space();
        let start = Instant::now();
        tree.remove_range(day_start(day), day_start(day + 1), seqno);
        tree.flush_active_memtable(0).unwrap();
        let flushed = start.elapsed();
        let result = tree.compact(nothing(), seqno + 1).unwrap();
        let total = start.elapsed();
        seqno += 1;
        println!(
            "expire day={day} total={total:?} flush={flushed:?} reclaim={:?} read={} written={} tables_dropped={} freed={}",
            total - flushed,
            result.work.measured.read,
            result.work.measured.written,
            result.tables_in,
            freed(&tree, before),
        );
    }
}

fn settle() {
    let folder = tempfile::tempdir().unwrap();
    let (tree, mut seqno) = fill(&folder);
    let leveled = Arc::new(Leveled::default());
    // Settle the fill itself first: only what an expiry causes is measured.
    while tree.compact(leveled.clone(), seqno).unwrap().action != CompactionAction::Nothing {}
    for day in 0..EXPIRIES {
        let before = tree.disk_space();
        let start = Instant::now();
        tree.remove_range(day_start(day), day_start(day + 1), seqno);
        tree.flush_active_memtable(0).unwrap();
        let (mut read, mut written, mut runs) = (0, 0, 0);
        loop {
            let result = tree.compact(leveled.clone(), seqno + 1).unwrap();
            if result.action == CompactionAction::Nothing {
                break;
            }
            read += result.work.measured.read;
            written += result.work.measured.written;
            runs += 1;
        }
        let total = start.elapsed();
        seqno += 1;
        println!(
            "settle day={day} total={total:?} runs={runs} read={read} written={written} freed={}",
            freed(&tree, before),
        );
    }
}

fn idle() {
    let folder = tempfile::tempdir().unwrap();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .unwrap();
    let mut seqno = 0;
    for table in 0..IDLE_TABLES {
        for row in 0..100 {
            tree.insert(day_key(table, row), [b'v'; 100], seqno);
            seqno += 1;
        }
        tree.flush_active_memtable(0).unwrap();
    }
    let strategy = nothing();
    let samples: Vec<Duration> = (0..IDLE_RUNS)
        .map(|_| {
            let start = Instant::now();
            let result = tree.compact(strategy.clone(), seqno).unwrap();
            let elapsed = start.elapsed();
            assert_eq!(CompactionAction::Nothing, result.action);
            elapsed
        })
        .collect();
    println!(
        "idle tables={} runs={IDLE_RUNS} median={:?} p99={:?}",
        tree.table_count(),
        median(samples.clone()),
        p99(samples),
    );
}

fn main() {
    expire();
    settle();
    idle();
}

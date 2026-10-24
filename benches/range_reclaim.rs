// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Range expiration cost for a time series kept a fixed number of days, one
//! flushed table per day. Every sample starts from a fresh tree, so samples
//! repeat one state:
//! - `expire`: deleting the oldest day with a range tombstone, its flush and
//!   the compaction run that follows, which unlinks the day's table;
//! - `settle`: the same expiry followed by leveled compactions until they have
//!   nothing left to do, with the bytes they read and wrote;
//! - `idle`: a compaction run that finds nothing to do, the cost every run
//!   pays for the unlink check, on a tree of many tables;
//! - `point_read`: reads that find their key on that tree, each checked
//!   against the range tombstones of the tables around it;
//! - `point_read_tombstoned`: the same reads on the same tree after older
//!   range tombstones spanning its tables, which delete nothing.
//!
//! Prints one line per measurement: median, P99 and P999 of the timings, and
//! the bytes each sample read, wrote and freed.

use lsm_tree::{
    AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter,
    compaction::{CompactionAction, Fifo, Leveled},
};
use std::sync::Arc;
use std::time::{Duration, Instant};

const DAYS: u32 = 10;
const ROWS_PER_DAY: u32 = 20_000;
const SAMPLES: usize = 10;
const IDLE_TABLES: u32 = 400;
const IDLE_RUNS: usize = 2_000;

fn day_key(day: u32, row: u32) -> Vec<u8> {
    format!("ts/{day:04}/{row:08}").into_bytes()
}

/// The first key of `day`, which is also the exclusive end of the day before.
fn day_start(day: u32) -> Vec<u8> {
    format!("ts/{day:04}/").into_bytes()
}

fn open(folder: &tempfile::TempDir) -> lsm_tree::Result<AnyTree> {
    Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
}

/// A tree holding `DAYS` days, one flushed table per day, and the next seqno.
fn fill(folder: &tempfile::TempDir) -> lsm_tree::Result<(AnyTree, u64)> {
    let tree = open(folder)?;
    let mut seqno = 0;
    for day in 0..DAYS {
        for row in 0..ROWS_PER_DAY {
            tree.insert(day_key(day, row), [b'v'; 100], seqno);
            seqno += 1;
        }
        tree.flush_active_memtable(0)?;
    }
    Ok((tree, seqno))
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

/// Median, P99 and P999 of `samples`.
fn percentiles(mut samples: Vec<Duration>) -> String {
    samples.sort_unstable();
    let at = |per_mille: usize| samples[(samples.len() * per_mille / 1_000).min(samples.len() - 1)];
    format!("median={:?} p99={:?} p999={:?}", at(500), at(990), at(999))
}

fn expire() -> lsm_tree::Result<()> {
    let (mut flush, mut reclaim, mut total) = (Vec::new(), Vec::new(), Vec::new());
    let (mut read, mut written, mut dropped, mut freed_bytes) = (0, 0, 0, 0);
    for _ in 0..SAMPLES {
        let folder = tempfile::tempdir()?;
        let (tree, seqno) = fill(&folder)?;
        let before = tree.disk_space();
        let start = Instant::now();
        tree.remove_range(day_start(0), day_start(1), seqno);
        tree.flush_active_memtable(0)?;
        let flushed = start.elapsed();
        let result = tree.compact(nothing(), seqno + 1)?;
        let elapsed = start.elapsed();
        flush.push(flushed);
        reclaim.push(elapsed - flushed);
        total.push(elapsed);
        read = read.max(result.work.measured.read);
        written = written.max(result.work.measured.written);
        dropped = dropped.max(result.tables_in);
        freed_bytes = freed(&tree, before);
    }
    println!("expire total {}", percentiles(total));
    println!("expire flush {}", percentiles(flush));
    println!(
        "expire reclaim {} read_max={read} written_max={written} tables_dropped={dropped} freed={freed_bytes}",
        percentiles(reclaim),
    );
    Ok(())
}

fn settle() -> lsm_tree::Result<()> {
    let leveled = Arc::new(Leveled::default());
    let mut total = Vec::new();
    let (mut read, mut written, mut runs, mut freed_bytes) = (0, 0, 0, 0);
    for _ in 0..SAMPLES {
        let folder = tempfile::tempdir()?;
        let (tree, seqno) = fill(&folder)?;
        // Settle the fill itself first: only what an expiry causes is measured.
        while tree.compact(leveled.clone(), seqno)?.action != CompactionAction::Nothing {}
        let before = tree.disk_space();
        let start = Instant::now();
        tree.remove_range(day_start(0), day_start(1), seqno);
        tree.flush_active_memtable(0)?;
        let (mut sample_read, mut sample_written, mut sample_runs) = (0, 0, 0);
        loop {
            let result = tree.compact(leveled.clone(), seqno + 1)?;
            if result.action == CompactionAction::Nothing {
                break;
            }
            sample_read += result.work.measured.read;
            sample_written += result.work.measured.written;
            sample_runs += 1;
        }
        total.push(start.elapsed());
        read = read.max(sample_read);
        written = written.max(sample_written);
        runs = runs.max(sample_runs);
        freed_bytes = freed(&tree, before);
    }
    println!(
        "settle total {} runs_max={runs} read_max={read} written_max={written} freed={freed_bytes}",
        percentiles(total),
    );
    Ok(())
}

fn idle() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open(&folder)?;
    let mut seqno = 0;
    for table in 0..IDLE_TABLES {
        for row in 0..100 {
            tree.insert(day_key(table, row), [b'v'; 100], seqno);
            seqno += 1;
        }
        tree.flush_active_memtable(0)?;
    }
    let strategy = nothing();
    let mut samples = Vec::with_capacity(IDLE_RUNS);
    for _ in 0..IDLE_RUNS {
        let start = Instant::now();
        let result = tree.compact(strategy.clone(), seqno)?;
        samples.push(start.elapsed());
        assert_eq!(CompactionAction::Nothing, result.action);
    }
    println!(
        "idle tables={} runs={IDLE_RUNS} {}",
        tree.table_count(),
        percentiles(samples),
    );
    point_reads("point_read", &tree)?;
    point_reads("point_read_tombstoned", &tombstoned()?.0)
}

/// The tree `idle` builds, after 40 range tombstones each spanning the keys
/// of ten of its tables, flushed into a table of their own. They are older
/// than every row, so they delete nothing, but every hit is checked against
/// them.
fn tombstoned() -> lsm_tree::Result<(AnyTree, tempfile::TempDir)> {
    const SPANS: u32 = 40;
    let folder = tempfile::tempdir()?;
    let tree = open(&folder)?;
    let width = IDLE_TABLES / SPANS;
    for span in 0..SPANS {
        tree.remove_range(
            day_start(span * width),
            day_start((span + 1) * width),
            u64::from(span),
        );
    }
    tree.flush_active_memtable(0)?;
    let mut seqno = u64::from(SPANS);
    for table in 0..IDLE_TABLES {
        for row in 0..100 {
            tree.insert(day_key(table, row), [b'v'; 100], seqno);
            seqno += 1;
        }
        tree.flush_active_memtable(0)?;
    }
    Ok((tree, folder))
}

/// Point reads that find their key on a tree built like `idle`'s: each hit
/// is checked against the range tombstones of the tables around it. The
/// mean over batches of 1,000 gets is the amortized cost; each get timed on
/// its own gives the tail.
fn point_reads(label: &str, tree: &AnyTree) -> lsm_tree::Result<()> {
    const BATCH: u32 = 1_000;
    const BATCHES: u32 = 200;
    let key = |n: u32| day_key(n.wrapping_mul(7_919) % IDLE_TABLES, n % 100);
    let mut batch_means = Vec::new();
    for batch in 0..BATCHES {
        let start = Instant::now();
        for i in 0..BATCH {
            assert!(tree.get(key(batch * BATCH + i), SeqNo::MAX)?.is_some());
        }
        batch_means.push(start.elapsed() / BATCH);
    }
    let mut samples = Vec::new();
    for n in 0..BATCH * BATCHES {
        let key = key(n);
        let start = Instant::now();
        let found = tree.get(key, SeqNo::MAX)?;
        samples.push(start.elapsed());
        assert!(found.is_some());
    }
    println!(
        "{label} tables={} l0_runs={} amortized_per_get {}",
        tree.table_count(),
        tree.l0_run_count(),
        percentiles(batch_means),
    );
    println!(
        "{label} tables={} l0_runs={} per_get {}",
        tree.table_count(),
        tree.l0_run_count(),
        percentiles(samples),
    );
    Ok(())
}

fn main() -> lsm_tree::Result<()> {
    expire()?;
    settle()?;
    idle()
}

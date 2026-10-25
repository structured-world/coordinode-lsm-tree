// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Range expiration cost for a time series kept a fixed number of days, one
//! flushed table per day.
//!
//! First a report of what expiring costs in bytes, printed directly: every
//! sample starts from a fresh tree, so its few samples repeat one state and
//! their timings come with median and maximum only.
//! - `expire`: deleting the oldest day with a range tombstone, its flush and
//!   the compaction run that follows, which unlinks the day's table, with
//!   the bytes it read, wrote and freed;
//! - `rolling`: ten cycles of writing the next day and expiring the oldest,
//!   each followed by leveled or size-tiered compactions until they have
//!   nothing left to do, with the bytes they read and rewrote and how far the
//!   disk grew.
//!
//! Then the timings the unlink check adds, through criterion:
//! - `idle`: a compaction run that finds nothing to do on a tree of many
//!   tables;
//! - `point_read`: reads that find their key on that tree, each checked
//!   against the range tombstones of the tables around it, with the
//!   P50/P99/P999 of every get timed alone on stderr;
//! - `point_read_tombstoned`: the same reads on the same tree after older
//!   range tombstones spanning its tables, which delete nothing.

use criterion::Criterion;
use lsm_tree::{
    AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter,
    compaction::{CompactionAction, CompactionStrategy, Fifo, Leveled, SizeTiered},
};
use std::sync::Arc;
use std::time::{Duration, Instant};

const DAYS: u32 = 10;
const ROWS_PER_DAY: u32 = 20_000;
const SAMPLES: usize = 10;
const IDLE_TABLES: u32 = 400;
const ROWS_PER_TABLE: u32 = 100;

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

/// Median and maximum of `samples`, with P99 and P999 only where there are
/// enough samples to tell them from the maximum.
fn percentiles(mut samples: Vec<Duration>) -> String {
    samples.sort_unstable();
    let last = samples.len() - 1;
    let mut parts = vec![format!("median={:?}", samples[samples.len() / 2])];
    for (label, per_mille) in [("p99", 990), ("p999", 999)] {
        let index = samples.len() * per_mille / 1_000;
        if index < last {
            parts.push(format!("{label}={:?}", samples[index]));
        }
    }
    parts.push(format!("max={:?}", samples[last]));
    parts.join(" ")
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

/// Rolling retention over `DAYS` emulated days: each cycle writes the next
/// day, expires the oldest with a range tombstone and runs `strategy` until
/// it has nothing left, so the tree keeps `DAYS` days throughout. Reports
/// what the compactions read and wrote (the written bytes are the survivors
/// they rewrote), and how far the disk grew past the settled start, at its
/// peak and at the end.
fn rolling(name: &str, strategy: &Arc<dyn CompactionStrategy>) -> lsm_tree::Result<()> {
    const ROLLING_SAMPLES: usize = 3;
    let mut total = Vec::new();
    let (mut read, mut written, mut peak, mut end) = (0, 0, 0, 0);
    for _ in 0..ROLLING_SAMPLES {
        let folder = tempfile::tempdir()?;
        let (tree, mut seqno) = fill(&folder)?;
        // Settle the fill itself first: only what the expiries cause is measured.
        while tree.compact(Arc::clone(strategy), seqno)?.action != CompactionAction::Nothing {}
        let base = tree.disk_space();
        let (mut sample_read, mut sample_written, mut sample_peak) = (0, 0, base);
        let start = Instant::now();
        for cycle in 0..DAYS {
            for row in 0..ROWS_PER_DAY {
                tree.insert(day_key(DAYS + cycle, row), [b'v'; 100], seqno);
                seqno += 1;
            }
            tree.flush_active_memtable(0)?;
            tree.remove_range(day_start(cycle), day_start(cycle + 1), seqno);
            seqno += 1;
            tree.flush_active_memtable(0)?;
            sample_peak = sample_peak.max(tree.disk_space());
            loop {
                let result = tree.compact(Arc::clone(strategy), seqno)?;
                sample_peak = sample_peak.max(tree.disk_space());
                if result.action == CompactionAction::Nothing {
                    break;
                }
                sample_read += result.work.measured.read;
                sample_written += result.work.measured.written;
            }
        }
        total.push(start.elapsed());
        read = read.max(sample_read);
        written = written.max(sample_written);
        peak = peak.max(sample_peak - base);
        end = end.max(i128::from(tree.disk_space()) - i128::from(base));
    }
    println!(
        "rolling({name}) total {} read_max={read} written_max={written} peak_growth={peak} end_growth={end}",
        percentiles(total),
    );
    Ok(())
}

/// A tree of `IDLE_TABLES` flushed tables of `ROWS_PER_TABLE` rows each, and
/// the next seqno. With `spans` above zero it first holds that many range
/// tombstones, each spanning the keys of an equal share of the tables, in a
/// table of their own: older than every row, they delete nothing, but every
/// hit is checked against them.
fn many_tables(spans: u32) -> lsm_tree::Result<(AnyTree, tempfile::TempDir, u64)> {
    let folder = tempfile::tempdir()?;
    let tree = open(&folder)?;
    let mut seqno = 0;
    if spans > 0 {
        let width = IDLE_TABLES / spans;
        for span in 0..spans {
            tree.remove_range(
                day_start(span * width),
                day_start((span + 1) * width),
                seqno,
            );
            seqno += 1;
        }
        tree.flush_active_memtable(0)?;
    }
    for table in 0..IDLE_TABLES {
        for row in 0..ROWS_PER_TABLE {
            tree.insert(day_key(table, row), [b'v'; 100], seqno);
            seqno += 1;
        }
        tree.flush_active_memtable(0)?;
    }
    Ok((tree, folder, seqno))
}

/// A compaction run that finds nothing to do on a tree of many tables: the
/// cost every run pays for the unlink check.
fn idle(c: &mut Criterion) {
    let (tree, _folder, seqno) = many_tables(0).expect("fixture");
    let strategy = nothing();
    c.bench_function(
        &format!("range_reclaim/idle/{}_tables", tree.table_count()),
        |b| {
            b.iter(|| {
                let result = tree.compact(strategy.clone(), seqno).expect("compact");
                assert_eq!(CompactionAction::Nothing, result.action);
            });
        },
    );
}

/// The `n`-th key a point read asks for: a walk over every row of every table
/// in a scattered order, 7,919 being prime to the row count.
fn read_key(n: u64) -> Vec<u8> {
    let rows = u64::from(IDLE_TABLES * ROWS_PER_TABLE);
    let slot = u32::try_from(n.wrapping_mul(7_919) % rows).expect("below the row count");
    day_key(slot / ROWS_PER_TABLE, slot % ROWS_PER_TABLE)
}

/// Point reads that find their key on [`many_tables`] with and without older
/// range tombstones: each hit is checked against the tombstones of the
/// tables around it. Criterion's pass gives the mean; a second pass times
/// every get alone and prints the percentiles of all of them.
fn point_reads(c: &mut Criterion) {
    for (label, spans) in [("point_read", 0), ("point_read_tombstoned", 40)] {
        let (tree, _folder, _) = many_tables(spans).expect("fixture");
        let name = format!(
            "range_reclaim/{label}/{}_tables_{}_l0_runs",
            tree.table_count(),
            tree.l0_run_count(),
        );
        let mut n = 0;
        c.bench_function(&name, |b| {
            b.iter(|| {
                n += 1;
                assert!(tree.get(read_key(n), SeqNo::MAX).expect("get").is_some());
            });
        });
        let mut samples = Vec::new();
        c.bench_function(&format!("{name}/per_get"), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    n += 1;
                    let key = read_key(n);
                    let start = Instant::now();
                    let found = tree.get(key, SeqNo::MAX).expect("get");
                    let elapsed = start.elapsed();
                    assert!(found.is_some());
                    samples.push(elapsed);
                    total += elapsed;
                }
                total
            });
        });
        eprintln!(
            "{name} per_get n={} {}",
            samples.len(),
            percentiles(samples)
        );
    }
}

fn main() -> lsm_tree::Result<()> {
    expire()?;
    let leveled: Arc<dyn CompactionStrategy> = Arc::new(Leveled::default());
    rolling("leveled", &leveled)?;
    let tiered: Arc<dyn CompactionStrategy> = Arc::new(SizeTiered::default());
    rolling("size_tiered", &tiered)?;

    let mut c = Criterion::default().configure_from_args();
    idle(&mut c);
    point_reads(&mut c);
    c.final_summary();
    Ok(())
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Point reads of absent keys, by how filter memory is allocated.
//!
//! Two trees hold the same keys: one builds every filter at the static
//! policy, the other lets the filter advisor size them, given exactly the
//! filter bytes the static tree ended up with. Both see the same negative
//! lookups, then rewrite every table once, so the advisor sizes each filter
//! by what it observed. Two workloads:
//!
//! - `skewed`: nine in ten lookups fall in one tenth of the key space;
//! - `uniform`: lookups spread evenly. The advisor must tie here.
//!
//! `FA_PARTITIONED=1` partitions every filter.
//!
//! Each iteration reopens its tree outside the timer, so the lookups start
//! with a cold cache. The false positives (lookups the filters let through
//! to a data block read) and the filter bytes of each tree are printed once
//! per arm.
//!
//! `probe_counting` times cached point reads of present keys with the
//! advisor off and on: the cost the counters add to the read path.
//!
//! `rewrite` times the compaction that rewrites every table of the skewed
//! tree, at the static policy and sized by the advisor: the cost choosing
//! widths adds to the write path.

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use lsm_tree::{
    AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter, config::FilterAdvisor,
};
use std::time::{Duration, Instant};

/// The tree's shape: `FA_RANGES` ranges of `FA_KEYS` keys, each flushed as
/// one table (20 of 10 000 unless set), and `FA_LOOKUPS` timed lookups
/// (20 000 unless set). More ranges make a compaction with more inputs.
struct Size {
    ranges: u64,
    keys_per_range: u64,
    lookups: u64,
    /// Digits of a range number in a key, so keys sort by range.
    range_digits: usize,
}

static SIZE: std::sync::LazyLock<Size> = std::sync::LazyLock::new(|| {
    let knob = |name: &str, default: u64| {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    };
    let ranges = knob("FA_RANGES", 20).max(10);
    Size {
        ranges,
        keys_per_range: knob("FA_KEYS", 10_000),
        lookups: knob("FA_LOOKUPS", 20_000),
        range_digits: (ranges - 1).to_string().len().max(2),
    }
});

/// Present keys are even, absent ones odd, so every absent key lies inside
/// some table's range.
fn key(range: u64, i: u64) -> String {
    format!("r{range:0width$}k{i:08}", width = SIZE.range_digits)
}

/// Bits per key of the static policy, from `FA_STATIC_BITS` (10 unless set):
/// a lower one makes false positives weigh on the read time.
fn static_bits() -> f32 {
    std::env::var("FA_STATIC_BITS")
        .ok()
        .and_then(|bits| bits.parse().ok())
        .unwrap_or(10.0)
}

/// Whether filters are partitioned, from `FA_PARTITIONED` (off unless `1`).
fn partitioned() -> bool {
    std::env::var("FA_PARTITIONED").is_ok_and(|value| value == "1")
}

fn open(path: &std::path::Path, advisor: Option<FilterAdvisor>) -> lsm_tree::Result<AnyTree> {
    use lsm_tree::config::{
        BloomConstructionPolicy, FilterPolicy, FilterPolicyEntry, PinningPolicy,
    };

    Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .filter_policy(FilterPolicy::all(FilterPolicyEntry::Bloom(
        BloomConstructionPolicy::BitsPerKey(static_bits()),
    )))
    .filter_block_partitioning_policy(PinningPolicy::all(partitioned()))
    .filter_advisor(advisor)
    .open()
}

/// An advisor spending `budget` bytes over every width from 2 to 16 bits per
/// key, so a tight budget still leaves cold filters room to narrow.
fn advisor(budget: u64) -> FilterAdvisor {
    FilterAdvisor::new(budget).with_bits_per_key((2..=16).collect::<Vec<u8>>())
}

/// A small deterministic generator, so both arms see the same lookups.
struct Lookups(u64);

impl Lookups {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    /// An absent key: in the first tenth of the ranges nine times in ten
    /// when `skewed`, anywhere otherwise.
    fn absent(&mut self, skewed: bool) -> String {
        let hot = SIZE.ranges / 10;
        let range = if skewed && !self.next().is_multiple_of(10) {
            self.next() % hot
        } else {
            self.next() % SIZE.ranges
        };
        key(range, 2 * (self.next() % SIZE.keys_per_range) + 1)
    }
}

/// Writes the keys, runs the workload's lookups once and rewrites every
/// table, so an advisor sizes the filters by what those lookups probed.
fn populate(
    path: &std::path::Path,
    advisor: Option<FilterAdvisor>,
    skewed: bool,
) -> lsm_tree::Result<u64> {
    let tree = prepare(path, advisor, skewed)?;
    tree.major_compact(1 << 20, 0)?;
    Ok(tree.filter_size())
}

/// A tree of every range, after the lookups and with a table spanning every
/// range, so a major compaction rewrites them all.
fn prepare(
    path: &std::path::Path,
    advisor: Option<FilterAdvisor>,
    skewed: bool,
) -> lsm_tree::Result<AnyTree> {
    let tree = open(path, advisor)?;
    let mut seqno = 0;
    for range in 0..SIZE.ranges {
        for i in 0..SIZE.keys_per_range {
            tree.insert(key(range, 2 * i), "value", seqno);
            seqno += 1;
        }
        tree.flush_active_memtable(0)?;
    }
    let mut lookups = Lookups(0x9E37_79B9_7F4A_7C15);
    for _ in 0..SIZE.lookups {
        assert!(tree.get(lookups.absent(skewed), SeqNo::MAX)?.is_none());
    }
    // A table spanning every range makes the compaction rewrite them all.
    tree.insert(key(0, 0), "value", seqno);
    tree.insert(key(SIZE.ranges - 1, 0), "value", seqno + 1);
    tree.flush_active_memtable(0)?;
    Ok(tree)
}

/// Runs `count` of the workload's lookups on a reopened tree, returning how
/// many the filters let through to a data block read. With `latencies`, each
/// lookup's time is pushed there; the timed runs leave it out.
fn false_positives(
    tree: &AnyTree,
    skewed: bool,
    count: u64,
    mut latencies: Option<&mut Vec<Duration>>,
) -> lsm_tree::Result<usize> {
    let metrics = tree.metrics();
    let (queries, skipped) = (metrics.filter_queries(), metrics.io_skipped_by_filter());
    let mut lookups = Lookups(0xD1B5_4A32_D192_ED03);
    for _ in 0..count {
        let key = lookups.absent(skewed);
        let found = if let Some(latencies) = latencies.as_deref_mut() {
            let start = Instant::now();
            let found = tree.get(key, SeqNo::MAX)?;
            latencies.push(start.elapsed());
            found
        } else {
            tree.get(key, SeqNo::MAX)?
        };
        assert!(found.is_none());
    }
    Ok((metrics.filter_queries() - queries) - (metrics.io_skipped_by_filter() - skipped))
}

/// The median, P99 and P999 of `samples`, for printing.
fn tail(samples: &mut [Duration]) -> String {
    samples.sort_unstable();
    let at = |q: f64| {
        let last = samples.len().saturating_sub(1);
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss,
            reason = "an index into the samples"
        )]
        let index = (last as f64 * q).round() as usize;
        samples.get(index).copied().unwrap_or_default()
    };
    format!(
        "p50 {:?}, p99 {:?}, p999 {:?}",
        at(0.5),
        at(0.99),
        at(0.999)
    )
}

fn bench_absent_lookups(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_advisor_cold_absent_lookups");
    group.sample_size(10);

    for (workload, skewed) in [("skewed", true), ("uniform", false)] {
        let static_dir = tempfile::tempdir().expect("tempdir");
        let static_bytes = populate(static_dir.path(), None, skewed).expect("populate");
        let advised_dir = tempfile::tempdir().expect("tempdir");
        let advisor = advisor(static_bytes);
        let advised_bytes =
            populate(advised_dir.path(), Some(advisor.clone()), skewed).expect("populate");

        for (arm, dir, advisor) in [
            ("static", &static_dir, None),
            ("advisor", &advised_dir, Some(advisor)),
        ] {
            {
                // Ten times the timed lookups, so the counts rise above
                // their own noise.
                let counted = 10 * SIZE.lookups;
                let tree = open(dir.path(), advisor.clone()).expect("open");
                let mut latencies = Vec::new();
                let fp =
                    false_positives(&tree, skewed, counted, Some(&mut latencies)).expect("lookups");
                let memory = tree.filter_memory();
                println!(
                    "{workload}/{arm}: {fp} false positives of {counted} lookups, filters {} B \
                     in {} tables (static {static_bytes} B, advisor {advised_bytes} B); \
                     per lookup from a cold cache {}",
                    memory.serialised_bytes,
                    tree.table_count(),
                    tail(&mut latencies),
                );
            }
            group.bench_function(format!("{workload}/{arm}"), |b| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let tree = open(dir.path(), advisor.clone()).expect("open");
                        let start = Instant::now();
                        std::hint::black_box(
                            false_positives(&tree, skewed, SIZE.lookups, None).expect("lookups"),
                        );
                        total += start.elapsed();
                    }
                    total
                });
            });
        }
    }
    group.finish();
}

fn bench_probe_counting(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_advisor_probe_counting");

    for (arm, advisor) in [("off", None), ("on", Some(FilterAdvisor::new(u64::MAX)))] {
        let dir = tempfile::tempdir().expect("tempdir");
        let tree = open(dir.path(), advisor).expect("open");
        for i in 0..SIZE.keys_per_range {
            tree.insert(key(0, 2 * i), "value", i);
        }
        tree.flush_active_memtable(0).expect("flush");
        let mut i = 0;
        // Each read's time over one pass of every key, for the tail the
        // counters may add; the timed runs below measure the mean.
        let mut latencies = Vec::new();
        for _ in 0..SIZE.keys_per_range {
            i = (i + 7_919) % SIZE.keys_per_range;
            let k = key(0, 2 * i);
            let start = Instant::now();
            std::hint::black_box(tree.get(k, SeqNo::MAX).expect("get"));
            latencies.push(start.elapsed());
        }
        println!(
            "probe_counting/{arm}: per cached read {}",
            tail(&mut latencies)
        );
        group.bench_function(arm, |b| {
            b.iter_batched(
                || {
                    i = (i + 7_919) % SIZE.keys_per_range;
                    key(0, 2 * i)
                },
                |k| std::hint::black_box(tree.get(k, SeqNo::MAX).expect("get")),
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_rewrite(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_advisor_rewrite");
    group.sample_size(10);

    // The static tree's filter bytes, the budget the advisor is given.
    let budget = {
        let dir = tempfile::tempdir().expect("tempdir");
        populate(dir.path(), None, true).expect("populate")
    };
    for (arm, advisor) in [("static", None), ("advisor", Some(advisor(budget)))] {
        // One compaction is one sample, so the spread criterion reports over
        // them is this arm's tail; ten samples carry no P99 of their own.
        group.bench_function(arm, |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let dir = tempfile::tempdir().expect("tempdir");
                    let tree = prepare(dir.path(), advisor.clone(), true).expect("prepare");
                    let start = Instant::now();
                    tree.major_compact(1 << 20, 0).expect("compact");
                    total += start.elapsed();
                }
                total
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_absent_lookups,
    bench_probe_counting,
    bench_rewrite
);
criterion_main!(benches);

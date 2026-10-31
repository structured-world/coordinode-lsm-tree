// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Point reads through one block cache from many threads: what its shard locks
//! cost when they are contended, and whether a thread is left behind.
//!
//! | axis | values |
//! |---|---|
//! | cache | `warm` (holds every block, so readers only share read locks) or `tight` (a fifth of the data, so misses take write locks to evict) |
//! | order | `uniform` or `skew` (a few keys take most reads, so their shards are the contended ones) |
//! | threads | 1, 4, 8, 16 |
//!
//! Per configuration it prints the median over the rounds of the reads per
//! second, the latency percentiles of one read in 64, and the fewest and most
//! reads any one thread made: a lock that lets some threads starve shows there
//! and in the far tail before it shows in the total.
//!
//! `CACHE_CONTENTION_SECONDS` (default 3) sets the seconds per round,
//! `CACHE_CONTENTION_ROUNDS` (default 3) the rounds and
//! `CACHE_CONTENTION_THREADS`, `CACHE_CONTENTION_CACHES` and
//! `CACHE_CONTENTION_ORDERS` (comma-separated) run only the values they name.

#![expect(
    clippy::expect_used,
    reason = "only for invariants of the harness itself; setup I/O is propagated"
)]

use lsm_tree::{AbstractTree, AnyTree, Cache, Config, SeqNo, SequenceNumberCounter};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Barrier, Mutex};
use std::time::{Duration, Instant};

const KEYS: u64 = 400_000;
const VALUE_SIZE: usize = 200;
const WARM_CACHE_BYTES: u64 = 1 << 30;
const TIGHT_CACHE_BYTES: u64 = KEYS * VALUE_SIZE as u64 / 5;
const SAMPLE_EVERY: u64 = 64;

fn key(i: u64) -> [u8; 16] {
    let mut key = [b'k'; 16];
    key[8..].copy_from_slice(&i.to_be_bytes());
    key
}

fn env_or(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Whether the comma-separated list in `name` names `item`; every item when
/// the variable is unset.
fn picked(name: &str, item: &str) -> bool {
    match std::env::var(name) {
        Ok(list) => list.split(',').any(|v| v.trim() == item),
        Err(_) => true,
    }
}

/// A tree of `KEYS` keys behind a cache of `cache_bytes`, read once through.
fn fixture(dir: &std::path::Path, cache_bytes: u64) -> lsm_tree::Result<AnyTree> {
    let tree = Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    // Without the row cache: its hits would answer before any block is read.
    .use_cache(Arc::new(
        Cache::with_capacity_bytes(cache_bytes).with_row_cache(false),
    ))
    .open()?;
    let value = vec![b'v'; VALUE_SIZE];
    for i in 0..KEYS {
        tree.insert(key(i), value.as_slice(), i + 1);
    }
    tree.flush_active_memtable(0)?;
    for i in 0..KEYS {
        tree.get(key(i), SeqNo::MAX)?;
    }
    Ok(tree)
}

/// The next value of a xorshift generator.
fn next(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// The key a random `draw` reads: any with equal odds, or under `skew` a rank
/// drawn with the odds falling as its fourth power, scattered over the key
/// space so the hot keys do not share a block.
fn pick(draw: u64, skew: bool) -> u64 {
    if !skew {
        return draw % KEYS;
    }
    let unit = (draw >> 11) as f64 / (1_u64 << 53) as f64;
    let rank = (unit.powi(4) * KEYS as f64) as u64;
    rank.wrapping_mul(0x9E37_79B9_7F4A_7C15) % KEYS
}

/// What one round measured.
struct Round {
    reads_per_second: f64,
    fewest: u64,
    most: u64,
    latencies: Vec<u64>,
}

fn round(tree: &AnyTree, threads: u64, skew: bool, seconds: u64) -> lsm_tree::Result<Round> {
    let start = Barrier::new(threads as usize + 1);
    let stop = AtomicBool::new(false);
    let totals = Mutex::new(Vec::new());
    let latencies = Mutex::new(Vec::new());
    let mut elapsed = Duration::ZERO;
    let workers: lsm_tree::Result<()> = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..threads)
            .map(|thread| {
                let (start, stop, totals, latencies) = (&start, &stop, &totals, &latencies);
                scope.spawn(move || -> lsm_tree::Result<()> {
                    let mut state = 0x2545_F491_4F6C_DD1D_u64 ^ (thread + 1).wrapping_mul(0x9E37);
                    let mut samples = Vec::with_capacity(1 << 20);
                    let mut reads = 0_u64;
                    start.wait();
                    while !stop.load(Ordering::Relaxed) {
                        let at = key(pick(next(&mut state), skew));
                        if reads.is_multiple_of(SAMPLE_EVERY) {
                            let before = Instant::now();
                            std::hint::black_box(tree.get(at, SeqNo::MAX)?);
                            samples.push(before.elapsed().as_nanos() as u64);
                        } else {
                            std::hint::black_box(tree.get(at, SeqNo::MAX)?);
                        }
                        reads += 1;
                    }
                    totals.lock().expect("no thread panics").push(reads);
                    latencies
                        .lock()
                        .expect("no thread panics")
                        .append(&mut samples);
                    Ok(())
                })
            })
            .collect();
        start.wait();
        let began = Instant::now();
        std::thread::sleep(Duration::from_secs(seconds));
        stop.store(true, Ordering::Relaxed);
        elapsed = began.elapsed();
        workers
            .into_iter()
            .try_for_each(|worker| worker.join().expect("no thread panics"))
    });
    workers?;
    let totals = totals.into_inner().expect("no thread panics");
    let mut latencies = latencies.into_inner().expect("no thread panics");
    latencies.sort_unstable();
    Ok(Round {
        reads_per_second: totals.iter().sum::<u64>() as f64 / elapsed.as_secs_f64(),
        fewest: totals.iter().copied().min().unwrap_or(0),
        most: totals.iter().copied().max().unwrap_or(0),
        latencies,
    })
}

/// The value `num / den` of the way through the sorted `samples`.
fn percentile(samples: &[u64], num: usize, den: usize) -> u64 {
    samples
        .get(samples.len().saturating_sub(1) * num / den)
        .copied()
        .unwrap_or(0)
}

fn main() -> lsm_tree::Result<()> {
    // `cargo test` runs a bench target too: a moment of each then.
    let measuring = std::env::args().any(|a| a == "--bench");
    let seconds = if measuring {
        env_or("CACHE_CONTENTION_SECONDS", 3).max(1)
    } else {
        1
    };
    let rounds = if measuring {
        env_or("CACHE_CONTENTION_ROUNDS", 3).max(1)
    } else {
        1
    };
    for (cache, cache_bytes) in [("warm", WARM_CACHE_BYTES), ("tight", TIGHT_CACHE_BYTES)] {
        if !picked("CACHE_CONTENTION_CACHES", cache) {
            continue;
        }
        let dir = tempfile::tempdir()?;
        let tree = fixture(dir.path(), cache_bytes)?;
        for (order, skew) in [("uniform", false), ("skew", true)] {
            if !picked("CACHE_CONTENTION_ORDERS", order) {
                continue;
            }
            for threads in [1_u64, 4, 8, 16] {
                if !picked("CACHE_CONTENTION_THREADS", &threads.to_string()) {
                    continue;
                }
                let mut runs = (0..rounds)
                    .map(|_| round(&tree, threads, skew, seconds))
                    .collect::<lsm_tree::Result<Vec<Round>>>()?;
                runs.sort_unstable_by(|a, b| a.reads_per_second.total_cmp(&b.reads_per_second));
                let run = &runs[runs.len() / 2];
                println!(
                    "cache_contention/{cache}/{order}/t{threads}: {:.0} reads/s, thread reads {}..{}, p50 {} ns p99 {} ns p999 {} ns p9999 {} ns",
                    run.reads_per_second,
                    run.fewest,
                    run.most,
                    percentile(&run.latencies, 50, 100),
                    percentile(&run.latencies, 99, 100),
                    percentile(&run.latencies, 999, 1_000),
                    percentile(&run.latencies, 9_999, 10_000),
                );
            }
        }
    }
    Ok(())
}

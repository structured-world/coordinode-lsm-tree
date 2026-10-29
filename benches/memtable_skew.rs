// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! What a memtable insert costs as the keys skew and the writers multiply,
//! apart from any flush: the head-to-head `overwrite` scenario amortizes the
//! flush over the whole key set and does not tell an append from a hot key.
//!
//! Each configuration writes the same number of versions into a fresh tree on
//! an in-memory file system, which never flushes during the run:
//!
//! | axis | values |
//! |---|---|
//! | key order | `append` (each key above the last), `uniform`, `zipf` (s = 0.99), `hot` (one key) |
//! | key shape | 16 bytes, or 16 bytes behind a common prefix of 8, 32 or 64 bytes |
//! | writers | 1, 4, 16, 32 threads, each taking its share of the versions |
//! | route | `tree` (`Tree::insert`, through the thread's insert gate shard) or `direct` (the active memtable itself) |
//!
//! Per configuration it prints the median over the rounds of the time per
//! insert seen by a writer, the inserts per second over all of them, the
//! memtable bytes per version, and the P50/P99/P999 latency of one insert,
//! taken over every 64th insert of each writer in all rounds. Built with the
//! `metrics` feature it also prints the links retried because a concurrent
//! insert changed a neighbour first.
//!
//! The harness prints its own lines rather than run under criterion: what it
//! reports besides time (bytes per version, retried links) criterion has no
//! place for, and criterion's summary is a mean over batches, not a
//! percentile of single inserts.
//! A last single-writer pass counts the key comparisons per insert through a
//! counting comparator; that pass is not timed, since a comparator other than
//! the built-in byte order takes the skiplist off its inlined comparison.
//!
//! `MEMTABLE_SKEW_VERSIONS` (default 400000) sets the versions per
//! configuration, `MEMTABLE_SKEW_KEYS` (default 100000) the key space of the
//! skewed orders, `MEMTABLE_SKEW_ROUNDS` (default 5) the rounds.

#![expect(
    clippy::expect_used,
    reason = "benchmark setup favors concise panic messages"
)]

use lsm_tree::{
    AbstractTree, AnyTree, Config, InternalValue, Memtable, SequenceNumberCounter, UserComparator,
    ValueType,
};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

/// Bytes per value, as in the head-to-head harness.
const VALUE_SIZE: usize = 256;

/// Every this many inserts of a writer one is timed alone, for the latency
/// percentiles: timing every insert would add two clock reads to each and
/// move the throughput measured beside them.
const SAMPLE_EVERY: usize = 64;

#[derive(Clone, Copy, Debug)]
enum KeyOrder {
    Append,
    Uniform,
    Zipf,
    Hot,
}

impl KeyOrder {
    const ALL: [Self; 4] = [Self::Append, Self::Uniform, Self::Zipf, Self::Hot];

    fn label(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Uniform => "uniform",
            Self::Zipf => "zipf",
            Self::Hot => "hot",
        }
    }
}

/// Bytes of the common prefix in front of every key.
const PREFIXES: [usize; 4] = [0, 8, 32, 64];
const WRITERS: [usize; 4] = [1, 4, 16, 32];

/// xorshift64: the same stream on every run and host.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    /// A value in `[0, 1)` from the top 53 bits.
    fn unit(&mut self) -> f64 {
        const SCALE: f64 = 1.0 / (1u64 << 53) as f64;
        // The top 53 bits fit an f64 exactly.
        (self.next() >> 11) as f64 * SCALE
    }
}

/// The key indices of `versions` writes in `order` over `keys` keys.
fn key_indices(order: KeyOrder, versions: usize, keys: usize) -> Vec<u64> {
    let mut rng = Rng(0x2545_F491_4F6C_DD1D);
    match order {
        KeyOrder::Append => (0..versions as u64).collect(),
        KeyOrder::Uniform => (0..versions).map(|_| rng.next() % keys as u64).collect(),
        KeyOrder::Hot => vec![0; versions],
        KeyOrder::Zipf => {
            // Inverse transform over the normalized cumulative weights
            // 1 / rank^0.99: rank 0 is the hottest key.
            let mut cdf: Vec<f64> = (1..=keys)
                .scan(0.0, |sum, rank| {
                    *sum += 1.0 / (rank as f64).powf(0.99);
                    Some(*sum)
                })
                .collect();
            let total = *cdf.last().expect("at least one key");
            for weight in &mut cdf {
                *weight /= total;
            }
            (0..versions)
                .map(|_| {
                    let u = rng.unit();
                    cdf.partition_point(|&c| c < u).min(keys - 1) as u64
                })
                .collect()
        }
    }
}

/// The key of index `i`: `prefix` bytes in common, then 16 bytes that keep
/// keys in index order for `append` and spread them over the key space
/// otherwise.
fn key(order: KeyOrder, prefix: usize, i: u64) -> Vec<u8> {
    const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut out = vec![b'p'; prefix];
    match order {
        KeyOrder::Append => {
            out.extend_from_slice(&i.to_be_bytes());
            out.extend_from_slice(&[0; 8]);
        }
        KeyOrder::Uniform | KeyOrder::Zipf | KeyOrder::Hot => {
            out.extend_from_slice(&i.wrapping_mul(GOLDEN).to_be_bytes());
            out.extend_from_slice(&i.to_be_bytes());
        }
    }
    out
}

/// Byte order, as the built-in comparator, counting every comparison.
struct CountingComparator(AtomicU64);

impl UserComparator for CountingComparator {
    fn name(&self) -> &'static str {
        "counting-lexicographic"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        self.0.fetch_add(1, Ordering::Relaxed);
        a.cmp(b)
    }
}

fn open(comparator: Option<Arc<CountingComparator>>) -> AnyTree {
    let config = Config::new(
        Path::new("/bench"),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(lsm_tree::fs::MemFs::new());
    let config = match comparator {
        Some(comparator) => config.comparator(comparator),
        None => config,
    };
    config.open().expect("open")
}

/// How a write reaches the memtable.
#[derive(Clone, Copy)]
enum Route {
    /// `Tree::insert`: the thread's insert gate shard, then the memtable.
    Tree,
    /// The active memtable directly: its own cost alone.
    Memtable,
}

impl Route {
    const ALL: [Self; 2] = [Self::Tree, Self::Memtable];

    fn label(self) -> &'static str {
        match self {
            Self::Tree => "tree",
            Self::Memtable => "direct",
        }
    }
}

/// One configuration's workload: the keys of every write, in write order.
struct Writes {
    keys: Vec<Vec<u8>>,
    value: Vec<u8>,
}

impl Writes {
    fn new(order: KeyOrder, prefix: usize, versions: usize, keys: usize) -> Self {
        Self {
            keys: key_indices(order, versions, keys)
                .into_iter()
                .map(|i| key(order, prefix, i))
                .collect(),
            value: vec![b'v'; VALUE_SIZE],
        }
    }

    /// Writes every key into `tree` through `route` from `writers` threads,
    /// each its own contiguous share, the shares differing by one key at
    /// most; returns the time from the release of the writers to the last
    /// writer's end, the writers that ran (fewer than asked only when there
    /// are fewer keys), and the time of every [`SAMPLE_EVERY`]-th insert of
    /// each writer in nanoseconds.
    ///
    /// A key's seqno is its position in the write order, so seqnos stay unique
    /// without the writers sharing a counter: the cost measured is the
    /// memtable's, not a caller's seqno allocation.
    fn run(&self, tree: &AnyTree, writers: usize, route: Route) -> (Duration, usize, Vec<u64>) {
        let len = self.keys.len();
        let writers = writers.clamp(1, len.max(1));
        // The clock starts once every writer is ready and before any is
        // released, so no insert runs before it.
        let (ready, go) = (Barrier::new(writers + 1), Barrier::new(writers + 1));
        let memtable = tree.active_memtable();
        let (elapsed, latencies) = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..writers)
                .map(|w| {
                    let (start, end) = (len * w / writers, len * (w + 1) / writers);
                    (start as u64, &self.keys[start..end])
                })
                .map(|(first, chunk)| {
                    let (ready, go, memtable) = (&ready, &go, &memtable);
                    scope.spawn(move || {
                        let insert = |s: u64, key: &[u8]| match route {
                            Route::Tree => {
                                tree.insert(key, self.value.as_slice(), s);
                            }
                            Route::Memtable => {
                                memtable.insert(InternalValue::from_components(
                                    key,
                                    self.value.as_slice(),
                                    s,
                                    ValueType::Value,
                                ));
                            }
                        };
                        let mut latencies = Vec::with_capacity(chunk.len() / SAMPLE_EVERY + 1);
                        ready.wait();
                        go.wait();
                        for (at, (s, key)) in (first..).zip(chunk).enumerate() {
                            if at % SAMPLE_EVERY == 0 {
                                let started = Instant::now();
                                insert(s, key);
                                latencies.push(
                                    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
                                );
                            } else {
                                insert(s, key);
                            }
                        }
                        latencies
                    })
                })
                .collect();
            ready.wait();
            let at = Instant::now();
            go.wait();
            let mut latencies = Vec::new();
            for handle in handles {
                latencies.extend(handle.join().expect("writer"));
            }
            (at.elapsed(), latencies)
        });
        (elapsed, writers, latencies)
    }
}

/// The links `memtable` retried, when the build counts them.
#[cfg(feature = "metrics")]
fn cas_retries(memtable: &Memtable) -> Option<f64> {
    Some(memtable.insert_cas_retries() as f64)
}

#[cfg(not(feature = "metrics"))]
fn cas_retries(_: &Memtable) -> Option<f64> {
    None
}

/// One route's rounds at one writer count.
#[derive(Default)]
struct Samples {
    per_insert: Vec<f64>,
    per_second: Vec<f64>,
    bytes: f64,
    retries: Vec<f64>,
    /// The sampled inserts of every round, in nanoseconds.
    latencies: Vec<u64>,
}

fn median(mut samples: Vec<f64>) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples[samples.len() / 2]
}

/// The quantile of `sorted` at `per_mille` thousandths, nearest rank; zero
/// when it is empty. In integers, so the rank needs no cast from a float.
fn quantile(sorted: &[u64], per_mille: usize) -> u64 {
    let Some(last) = sorted.len().checked_sub(1) else {
        return 0;
    };
    let rank = (last * per_mille.min(1000) + 500) / 1000;
    sorted[rank]
}

fn env_or(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let measuring = std::env::args().any(|a| a == "--bench");
    // `cargo test` runs a bench target too: one small round then.
    let (versions, keys, rounds) = if measuring {
        (
            env_or("MEMTABLE_SKEW_VERSIONS", 400_000).max(1),
            env_or("MEMTABLE_SKEW_KEYS", 100_000).max(1),
            env_or("MEMTABLE_SKEW_ROUNDS", 5).max(1),
        )
    } else {
        (2_000, 500, 1)
    };
    for order in KeyOrder::ALL {
        for prefix in PREFIXES {
            let writes = Writes::new(order, prefix, versions, keys);
            for writers in WRITERS {
                let mut samples: [Samples; Route::ALL.len()] = Default::default();
                for round in 0..rounds {
                    // Each round starts with the next route, so neither always
                    // runs on the cache and clock state the other left.
                    for i in 0..Route::ALL.len() {
                        let at = (round + i) % Route::ALL.len();
                        let (route, s) = (Route::ALL[at], &mut samples[at]);
                        let tree = open(None);
                        let (elapsed, ran, latencies) = writes.run(&tree, writers, route);
                        let secs = elapsed.as_secs_f64();
                        s.per_insert.push(secs * 1e9 * ran as f64 / versions as f64);
                        s.per_second.push(versions as f64 / secs);
                        s.latencies.extend(latencies);
                        let memtable = tree.active_memtable();
                        s.bytes = memtable.size() as f64 / versions as f64;
                        s.retries.extend(cas_retries(&memtable));
                    }
                }
                for (route, mut s) in Route::ALL.into_iter().zip(samples) {
                    let retried = if s.retries.is_empty() {
                        String::new()
                    } else {
                        format!(", {:.0} links retried", median(s.retries))
                    };
                    s.latencies.sort_unstable();
                    println!(
                        "memtable/{}/{}/prefix{prefix}/t{writers}: {:.1} ns/insert, {:.0} inserts/s, {:.1} B/version{retried}, insert p50 {} ns p99 {} ns p999 {} ns",
                        route.label(),
                        order.label(),
                        median(s.per_insert),
                        median(s.per_second),
                        s.bytes,
                        quantile(&s.latencies, 500),
                        quantile(&s.latencies, 990),
                        quantile(&s.latencies, 999),
                    );
                }
            }
            let counter = Arc::new(CountingComparator(AtomicU64::new(0)));
            let tree = open(Some(Arc::clone(&counter)));
            writes.run(&tree, 1, Route::Tree);
            println!(
                "memtable/{}/prefix{prefix}/compares: {:.2} per insert",
                order.label(),
                counter.0.load(Ordering::Relaxed) as f64 / versions as f64,
            );
        }
    }
}

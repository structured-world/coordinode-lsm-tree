// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! What a scan pays for the versions it does not hand out: a full scan of
//! trees that hold the same number of stored versions, spread over fewer keys
//! as each key keeps more of them, in nanoseconds per key handed out and per
//! version stored.
//!
//! A scan resolves each key to its newest version and walks past the older
//! ones; the arms show how that walk grows with the chain, and where the
//! versions live changes what a step of it costs:
//!
//! | layout | where a key's versions are |
//! |---|---|
//! | `mem` | all in the active memtable |
//! | `table` | all in one table |
//! | `tables8` | spread over 8 tables, the newest in the last flushed |
//! | `mixed` | the older half in one table, the newer half in the memtable |
//!
//! Each layout is scanned forward (`fwd`) and backward (`rev`). No criterion,
//! for the reason `scan_split` gives: each round runs every arm once, starting
//! one arm later than the round before, and the median over the rounds is
//! reported, with the p50 and p99 of single scans beside it. Values are 64 bytes and blocks uncompressed, so a chain of a few
//! hundred versions spans several data blocks.
//!
//! `MVCC_VERSIONS` (comma-separated, default `1,4,8,32,128,512,4096`) sets the
//! chain lengths, `MVCC_ENTRIES` (default 262144) the versions stored in each
//! tree, `MVCC_ROUNDS` (default 9) the rounds. `MVCC_ARM=<layout>_<dir>` with
//! one chain length scans that arm alone for `MVCC_SECS` seconds (default 20),
//! for a profiler to attach to.

#![expect(
    clippy::expect_used,
    reason = "benchmark setup favors concise panic messages"
)]

use lsm_tree::config::{CompressionPolicy, LocatorPolicy, PinningPolicy};
use lsm_tree::{
    AbstractTree, AnyTree, Cache, CompressionType, Config, Guard, MAX_SEQNO, SequenceNumberCounter,
};
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// Bytes per value.
const VALUE_SIZE: usize = 64;

/// Keys in mixed order, unique per index, as `scan_split` writes them.
fn key_for(i: u64) -> [u8; 16] {
    const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&i.wrapping_mul(GOLDEN).to_be_bytes());
    out[8..].copy_from_slice(&i.to_be_bytes());
    out
}

/// A value distinct per key and version.
fn value_for(i: u64, version: u64) -> [u8; VALUE_SIZE] {
    let mut out = [0u8; VALUE_SIZE];
    for (chunk, word) in out.as_chunks_mut::<8>().0.iter_mut().zip(0u64..) {
        let mixed = (i ^ (version << 40) ^ word).wrapping_mul(0x2545_F491_4F6C_DD1D);
        *chunk = mixed.to_le_bytes();
    }
    out
}

/// A tree on the read path `scan_split` measures: uncompressed blocks, pinned
/// single-level index and filter, a block cache holding every block.
fn open(dir: &std::path::Path) -> AnyTree {
    Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .use_cache(Arc::new(
        Cache::with_capacity_bytes(256 * 1024 * 1024).with_row_cache(false),
    ))
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
    .locator_policy(LocatorPolicy::disabled())
    .index_block_partitioning_policy(PinningPolicy::disabled())
    .filter_block_partitioning_policy(PinningPolicy::disabled())
    .index_block_pinning_policy(PinningPolicy::all(true))
    .filter_block_pinning_policy(PinningPolicy::all(true))
    .open()
    .expect("open")
}

/// Where a key's versions are.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Layout {
    Mem,
    Table,
    Tables8,
    Mixed,
}

impl Layout {
    const ALL: [Self; 4] = [Self::Mem, Self::Table, Self::Tables8, Self::Mixed];

    fn label(self) -> &'static str {
        match self {
            Self::Mem => "mem",
            Self::Table => "table",
            Self::Tables8 => "tables8",
            Self::Mixed => "mixed",
        }
    }

    /// Into how many slices a key's versions are cut, each flushed apart but
    /// for the last of `Mem` and `Mixed`, left in the memtable.
    fn slices(self) -> u64 {
        match self {
            Self::Mem | Self::Table => 1,
            Self::Tables8 => 8,
            Self::Mixed => 2,
        }
    }

    fn last_in_memtable(self) -> bool {
        matches!(self, Self::Mem | Self::Mixed)
    }
}

/// A tree of `keys` keys with `versions` versions each, laid out as `layout`
/// says, newest version last written.
fn build(keys: u64, versions: u64, layout: Layout) -> (TempDir, AnyTree) {
    let dir = TempDir::new().expect("tempdir");
    let tree = open(dir.path());
    let slices = layout.slices().min(versions);
    let per_slice = versions.div_ceil(slices);
    let mut seqno = 0;
    for slice in 0..slices {
        for version in (slice * per_slice)..((slice + 1) * per_slice).min(versions) {
            for i in 0..keys {
                tree.insert(key_for(i), value_for(i, version), seqno);
                seqno += 1;
            }
        }
        let last = slice + 1 == slices;
        if !(last && layout.last_in_memtable()) {
            tree.flush_active_memtable(0).expect("flush");
        }
    }
    (dir, tree)
}

/// One scan arm over its own tree.
struct Arm {
    label: String,
    tree: Arc<(TempDir, AnyTree)>,
    reverse: bool,
    keys: u64,
    entries: u64,
}

impl Arm {
    /// One full scan; returns the keys it handed out.
    fn scan(&self) -> u64 {
        let tree = &self.tree.1;
        let mut keys = 0;
        let iter = tree.iter(MAX_SEQNO, None);
        if self.reverse {
            for guard in iter.rev() {
                black_box(guard.value().expect("scan value"));
                keys += 1;
            }
        } else {
            for guard in iter {
                black_box(guard.value().expect("scan value"));
                keys += 1;
            }
        }
        keys
    }

    /// Nanoseconds per key handed out over scans adding up to at least
    /// `budget`; each scan's own nanoseconds per key go to `scans`.
    fn time(&self, budget: Duration, scans: &mut Vec<f64>) -> f64 {
        let start = Instant::now();
        let mut keys = 0u64;
        while start.elapsed() < budget {
            let scan_start = Instant::now();
            let scanned = self.scan();
            scans.push(scan_start.elapsed().as_nanos() as f64 / scanned.max(1) as f64);
            keys += scanned;
        }
        start.elapsed().as_nanos() as f64 / keys as f64
    }
}

/// The arms of one chain length: every layout, each direction.
fn arms(versions: u64, entries: u64) -> Vec<Arm> {
    let keys = (entries / versions).max(1);
    let mut out = Vec::new();
    for layout in Layout::ALL {
        let tree = Arc::new(build(keys, versions, layout));
        for reverse in [false, true] {
            out.push(Arm {
                label: format!("{}_{}", layout.label(), if reverse { "rev" } else { "fwd" }),
                tree: Arc::clone(&tree),
                reverse,
                keys,
                entries: keys * versions,
            });
        }
    }
    out
}

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// The median of `v`, which is not empty.
fn median(v: Vec<f64>) -> f64 {
    percentile(v, 50)
}

/// The `pct`-th percentile of `v` (nearest rank), which is not empty.
fn percentile(mut v: Vec<f64>, pct: usize) -> f64 {
    v.sort_by(f64::total_cmp);
    let rank = (v.len() * pct).div_ceil(100).max(1);
    *v.get(rank - 1)
        .expect("a percentile of at least one sample")
}

fn main() {
    // `cargo bench` passes `--bench`; a filter argument is not supported.
    let chains: Vec<u64> = std::env::var("MVCC_VERSIONS")
        .unwrap_or_else(|_| "1,4,8,32,128,512,4096".to_owned())
        .split(',')
        .map(|s| {
            s.trim()
                .parse()
                .expect("MVCC_VERSIONS: comma-separated chain lengths")
        })
        .collect();
    let entries: u64 = env_or("MVCC_ENTRIES", 262_144);
    let rounds: usize = env_or("MVCC_ROUNDS", 9);
    assert!(rounds > 0, "MVCC_ROUNDS: at least one round");

    if let Ok(only) = std::env::var("MVCC_ARM") {
        let &[versions] = chains.as_slice() else {
            panic!("MVCC_ARM scans one chain length: set MVCC_VERSIONS to one count");
        };
        let arm = arms(versions, entries)
            .into_iter()
            .find(|arm| arm.label == only)
            .unwrap_or_else(|| panic!("no arm named {only}"));
        let secs: u64 = env_or("MVCC_SECS", 20);
        let mut scans = Vec::new();
        let ns = arm.time(Duration::from_secs(secs), &mut scans);
        println!(
            "{only}/{versions}: {ns:.1} ns/key over {secs} s, per scan p50 {:.1} p99 {:.1}",
            percentile(scans.clone(), 50),
            percentile(scans, 99),
        );
        return;
    }

    for versions in chains {
        let arms = arms(versions, entries);
        // Every arm's blocks are cached before anything is timed.
        for arm in &arms {
            assert_eq!(arm.scan(), arm.keys, "{}: every key once", arm.label);
        }
        let mut samples: Vec<Vec<f64>> = vec![Vec::with_capacity(rounds); arms.len()];
        let mut scans: Vec<Vec<f64>> = vec![Vec::new(); arms.len()];
        for round in 0..rounds {
            for place in 0..arms.len() {
                let at = (round + place) % arms.len();
                let (arm, out) = (&arms[at], &mut samples[at]);
                out.push(arm.time(Duration::from_millis(200), &mut scans[at]));
            }
        }
        for ((arm, out), scans) in arms.iter().zip(samples).zip(scans) {
            let per_key = median(out);
            let per_version = per_key * arm.keys as f64 / arm.entries as f64;
            // Per scan: a round's median hides a slow scan among fast ones.
            let (p50, p99) = (percentile(scans.clone(), 50), percentile(scans, 99));
            println!(
                "mvcc_versions/{versions}/{}: {per_key:.1} ns/key, {per_version:.2} ns/version \
                 ({} keys); per scan p50 {p50:.1} p99 {p99:.1} ns/key",
                arm.label, arm.keys,
            );
            #[cfg(feature = "metrics")]
            report_file_work(arm, versions);
        }
    }
}

/// What one more scan of `arm` asks of its files, from the tree's counters.
#[cfg(feature = "metrics")]
fn report_file_work(arm: &Arm, versions: u64) {
    let m = arm.tree.1.metrics();
    let before = (
        m.block_load_cached_count() + m.block_load_io_count(),
        m.bytes_decoded(),
    );
    arm.scan();
    println!(
        "mvcc_versions/{versions}/{}: one scan: {} block loads, {} bytes decoded",
        arm.label,
        m.block_load_cached_count() + m.block_load_io_count() - before.0,
        m.bytes_decoded() - before.1,
    );
}

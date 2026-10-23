// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Attributes the cost of a full forward scan of an uncompressed tree to the
//! parts that make it up, in nanoseconds per item.
//!
//! No criterion: its post-sample analysis runs on the same cores and lands in a
//! profile of the timed loop, and a scan item costs little enough that it
//! shows. Each round runs every arm once, in turn, so a slow stretch of the
//! host lands on all of them; the median over the rounds is reported.
//!
//! The trees match the `range_scan` head-to-head against RocksDB: 16-byte keys
//! in mixed order, 256-byte values, no block compression, a single-level index
//! held by the table, a 16 MiB block cache and no row cache. At 1k and 10k rows
//! every block stays cached; 70k rows overflow the cache.
//!
//! | arm | what it reads |
//! |---|---|
//! | `tree` | `tree.iter` over one table, every value handed out |
//! | `table` | the one table's own reader, no merge or version resolution |
//! | `tree_8_tables` | `tree.iter` over the same rows in 8 overlapping tables |
//! | `tree_32_tables` | the same in 32 overlapping tables |
//! | `tree_3_versions` | `tree.iter` over one table holding 3 versions per key |
//! | `short`, `short_8_tables` | 64 scans of 10 rows each from keys spread over one or 8 tables |
//!
//! Read the arms as differences: `tree` − `table` is the merge, version
//! resolution and hand-out above a single table; `tree_8_tables` − `tree` is
//! merging several tables; `tree_3_versions` − `tree` is skipping older
//! versions (per visible row).
//!
//! Built with the `metrics` feature, each arm also reports what one scan asks
//! of the files: block loads served from the file and from the cache, the
//! bytes read and decoded, and the table file lookups and opens.
//!
//! `SCAN_SPLIT_SIZES` (comma-separated, default `1000,10000,70000`) and
//! `SCAN_SPLIT_ROUNDS` (default 15) set the run. `SCAN_SPLIT_ARM=<arm>` with
//! one size scans that arm alone for `SCAN_SPLIT_SECS` seconds (default 20),
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

/// Bytes per value, as in the head-to-head.
const VALUE_SIZE: usize = 256;

/// The head-to-head's key: the index mixed by the golden-ratio constant, then
/// the index itself, so keys land in mixed order and stay unique.
fn key_for(i: u64) -> [u8; 16] {
    const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&i.wrapping_mul(GOLDEN).to_be_bytes());
    out[8..].copy_from_slice(&i.to_be_bytes());
    out
}

/// A value distinct per row and version; its bytes do not matter uncompressed.
fn value_for(i: u64, version: u64) -> [u8; VALUE_SIZE] {
    let mut out = [0u8; VALUE_SIZE];
    for (chunk, word) in out.chunks_exact_mut(8).zip(0u64..) {
        let mixed = (i ^ (version << 48) ^ word).wrapping_mul(0x2545_F491_4F6C_DD1D);
        chunk.copy_from_slice(&mixed.to_le_bytes());
    }
    out
}

/// Opens a tree configured as the head-to-head's `rocksdb-parity` read groups
/// are on the read path.
fn open(dir: &std::path::Path) -> AnyTree {
    Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .use_cache(Arc::new(
        Cache::with_capacity_bytes(16 * 1024 * 1024).with_row_cache(false),
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

/// A tree of `n` rows written `versions` times each and flushed into `tables`
/// tables of interleaved keys, closed and reopened so it reads from disk.
fn build(n: u64, tables: u64, versions: u64) -> (TempDir, AnyTree) {
    let dir = TempDir::new().expect("tempdir");
    {
        let tree = open(dir.path());
        let mut seqno = 0;
        let per_table = n.div_ceil(tables);
        for chunk in 0..tables {
            for version in 0..versions {
                for i in (chunk * per_table)..((chunk + 1) * per_table).min(n) {
                    tree.insert(key_for(i), value_for(i, version), seqno);
                    seqno += 1;
                }
            }
            tree.flush_active_memtable(0).expect("flush");
        }
    }
    let tree = open(dir.path());
    assert_eq!(
        tree.current_version().iter_tables().count(),
        usize::try_from(tables).expect("table count fits usize"),
        "every flush makes one table"
    );
    (dir, tree)
}

/// What an arm reads.
#[derive(Clone, Copy)]
enum Read {
    /// `tree.iter`, every value handed out.
    Tree,
    /// The first table's own reader.
    Table,
    /// [`SHORT_SCANS`] scans of [`SHORT_ROWS`] rows each, from keys spread
    /// over the tree: what building a scan costs next to reading it.
    Short,
}

/// Short scans per [`Read::Short`] pass.
const SHORT_SCANS: u64 = 64;
/// Rows each short scan reads.
const SHORT_ROWS: usize = 10;

/// One scan arm over its own tree.
struct Arm {
    label: &'static str,
    _dir: TempDir,
    tree: AnyTree,
    read: Read,
    n: u64,
}

impl Arm {
    fn new(label: &'static str, n: u64, tables: u64, versions: u64, read: Read) -> Self {
        let (dir, tree) = build(n, tables, versions);
        Self {
            label,
            _dir: dir,
            tree,
            read,
            n,
        }
    }

    /// One pass of the arm's reads; returns the rows it handed out.
    fn scan(&self) -> u64 {
        let mut rows = 0;
        match self.read {
            Read::Table => {
                let version = self.tree.current_version();
                let table = version.iter_tables().next().expect("one table");
                for item in table.iter() {
                    black_box(item.expect("table row"));
                    rows += 1;
                }
            }
            Read::Tree => {
                for guard in self.tree.iter(MAX_SEQNO, None) {
                    black_box(guard.value().expect("scan value"));
                    rows += 1;
                }
            }
            Read::Short => {
                for start in 0..SHORT_SCANS {
                    let from = key_for(start * (self.n / SHORT_SCANS));
                    for guard in self
                        .tree
                        .range(from.as_slice().., MAX_SEQNO, None)
                        .take(SHORT_ROWS)
                    {
                        black_box(guard.value().expect("scan value"));
                        rows += 1;
                    }
                }
            }
        }
        rows
    }

    /// Nanoseconds per row over scans adding up to at least `budget`.
    fn time(&self, budget: Duration) -> f64 {
        let start = Instant::now();
        let mut rows = 0u64;
        while start.elapsed() < budget {
            rows += self.scan();
        }
        start.elapsed().as_nanos() as f64 / rows as f64
    }
}

fn arms(n: u64) -> Vec<Arm> {
    vec![
        Arm::new("tree", n, 1, 1, Read::Tree),
        Arm::new("table", n, 1, 1, Read::Table),
        Arm::new("tree_8_tables", n, 8, 1, Read::Tree),
        Arm::new("tree_32_tables", n, 32, 1, Read::Tree),
        Arm::new("tree_3_versions", n, 1, 3, Read::Tree),
        Arm::new("short", n, 1, 1, Read::Short),
        Arm::new("short_8_tables", n, 8, 1, Read::Short),
    ]
}

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}

fn main() {
    // `cargo bench` passes `--bench`; a filter argument is not supported.
    let sizes: Vec<u64> = std::env::var("SCAN_SPLIT_SIZES")
        .unwrap_or_else(|_| "1000,10000,70000".to_owned())
        .split(',')
        .map(|s| {
            s.trim()
                .parse()
                .expect("SCAN_SPLIT_SIZES: comma-separated row counts")
        })
        .collect();
    let rounds: usize = env_or("SCAN_SPLIT_ROUNDS", 15);

    if let Ok(only) = std::env::var("SCAN_SPLIT_ARM") {
        let &[n] = sizes.as_slice() else {
            panic!("SCAN_SPLIT_ARM scans one size: set SCAN_SPLIT_SIZES to one count");
        };
        let arm = arms(n)
            .into_iter()
            .find(|arm| arm.label == only)
            .unwrap_or_else(|| panic!("no arm named {only}"));
        let secs: u64 = env_or("SCAN_SPLIT_SECS", 20);
        let ns = arm.time(Duration::from_secs(secs));
        println!("{only}/{n}: {ns:.1} ns/row over {secs} s");
        return;
    }

    for n in sizes {
        let arms = arms(n);
        // Warm every arm's blocks into its cache before anything is timed.
        for arm in &arms {
            arm.scan();
        }
        let mut samples: Vec<Vec<f64>> = vec![Vec::with_capacity(rounds); arms.len()];
        for _ in 0..rounds {
            for (arm, out) in arms.iter().zip(&mut samples) {
                out.push(arm.time(Duration::from_millis(200)));
            }
        }
        for (arm, out) in arms.iter().zip(samples) {
            let (lo, hi) = (
                out.iter().copied().fold(f64::INFINITY, f64::min),
                out.iter().copied().fold(0.0, f64::max),
            );
            println!(
                "scan_split/{}/{n}: median {:.1} ns/row (min {lo:.1}, max {hi:.1})",
                arm.label,
                median(out)
            );
            #[cfg(feature = "metrics")]
            report_file_work(arm, n);
        }
    }
}

/// What one more scan of `arm` asks of its files, from the tree's counters.
#[cfg(feature = "metrics")]
fn report_file_work(arm: &Arm, n: u64) {
    let m = arm.tree.metrics();
    let before = (
        m.block_load_io_count(),
        m.block_load_cached_count(),
        m.bytes_read(),
        m.bytes_decoded(),
        m.table_file_cached_count(),
        m.table_file_opened_count(),
    );
    arm.scan();
    println!(
        "scan_split/{}/{n}: one scan: {} block loads from the file, {} from the cache, \
         {} bytes read, {} decoded, {} table file lookups, {} opens",
        arm.label,
        m.block_load_io_count() - before.0,
        m.block_load_cached_count() - before.1,
        m.bytes_read() - before.2,
        m.bytes_decoded() - before.3,
        m.table_file_cached_count() - before.4,
        m.table_file_opened_count() - before.5,
    );
}

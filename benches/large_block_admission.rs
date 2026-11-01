// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! What a filter or an index block heavier than a block cache shard costs.
//!
//! One table with its filter and index not pinned, as a table below L0 holds
//! them by default, read through caches of `CACHES` bytes twice: the first pass
//! cold, the second showing what stayed resident. Without the row cache and the
//! locator, which would answer reads before the blocks measured here.
//!
//! The layouts:
//! - `oversized`: written under `WRITE_CACHE`, so as one filter and one index
//!   block, and read through a smaller cache, as an older table is;
//! - `fitted`: written under the cache it is read through;
//! - `pieces`: always partitioned, in 4 KiB pieces that fit every shard.
//!
//! The reads:
//! - `get`: point reads of existing keys, each timed alone;
//! - `seek`: a new iterator read to its first row, each timed alone.
//!
//! `cargo test` runs a small table and a few reads, `cargo bench` all of it.
//!
//! Per pass it prints the time per read, then how many filter and index blocks
//! the pass loaded, how many of those the cache served, and the bytes read from
//! the file for them. Timings are in ns, as the median, with P99 and P999 where
//! there are enough samples to tell them from the maximum.

use lsm_tree::{
    AbstractTree, AnyTree, Cache, Config, Guard, SeqNo, SequenceNumberCounter,
    config::{LocatorPolicy, PinningPolicy},
};
use std::sync::Arc;
use std::time::Instant;

const CACHES: [u64; 3] = [4 << 20, 32 << 20, 256 << 20];
/// A cache whose shard holds every whole block here.
const WRITE_CACHE: u64 = 2 << 30;
/// Reads of one pass, spread evenly over the table.
const READS: u64 = 5_000;
const VALUE: [u8; 8] = [7; 8];

#[derive(Clone, Copy, PartialEq, Eq)]
enum Layout {
    Oversized,
    Fitted,
    Pieces,
}

impl Layout {
    const fn name(self) -> &'static str {
        match self {
            Self::Oversized => "oversized",
            Self::Fitted => "fitted",
            Self::Pieces => "pieces",
        }
    }
}

/// Keys of `key_len` bytes, a fixture's table sized by them.
#[derive(Clone, Copy)]
struct Fixture {
    name: &'static str,
    keys: u64,
    key_len: usize,
}

const FIXTURES: [Fixture; 3] = [
    // Sized by the filter: about 2 MiB and 5 MiB at the default bits per key.
    Fixture {
        name: "filter_2m",
        keys: 1_700_000,
        key_len: 16,
    },
    Fixture {
        name: "filter_5m",
        keys: 4_200_000,
        key_len: 16,
    },
    // Sized by the index: long keys, so a full index of about 2 MiB.
    Fixture {
        name: "index_2m",
        keys: 1_200_000,
        key_len: 64,
    },
];

fn key(i: u64, len: usize) -> Vec<u8> {
    let mut key = vec![b'k'; len];
    let at = len - 8;
    key[at..].copy_from_slice(&i.to_be_bytes());
    key
}

fn open(
    folder: &tempfile::TempDir,
    cache: u64,
    layout: Layout,
) -> lsm_tree::Result<(AnyTree, Arc<Cache>)> {
    let cache = Arc::new(Cache::with_capacity_bytes(cache).with_row_cache(false));
    let tree = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .use_cache(Arc::clone(&cache))
    .table_target_size(u64::MAX / 2)
    .locator_policy(LocatorPolicy::disabled())
    .filter_block_pinning_policy(PinningPolicy::all(false))
    .index_block_pinning_policy(PinningPolicy::all(false))
    .filter_block_partitioning_policy(PinningPolicy::all(layout == Layout::Pieces))
    .open()?;
    if layout == Layout::Pieces {
        let AnyTree::Standard(standard) = &tree else {
            unreachable!("no separation configured");
        };
        standard.update_runtime_config(|config| config.index_partition_spill_threshold = 0)?;
    }
    Ok((tree, cache))
}

/// Median of `samples`, with P99 and P999 only where there are enough samples
/// to tell them from the maximum.
fn percentiles(mut samples: Vec<u64>) -> String {
    samples.sort_unstable();
    let mut parts = vec![format!("p50 {}", samples[samples.len() / 2])];
    for (label, per_mille) in [("p99", 990), ("p999", 999)] {
        let index = samples.len() * per_mille / 1_000;
        if index + 1 < samples.len() {
            parts.push(format!("{label} {}", samples[index]));
        }
    }
    parts.join(" ")
}

/// What the block counters moved by over `pass`, and its timings.
fn pass(
    tree: &AnyTree,
    label: &str,
    mut read: impl FnMut() -> lsm_tree::Result<Vec<u64>>,
) -> lsm_tree::Result<()> {
    let metrics = Arc::clone(tree.metrics());
    let (filters, filters_cached, filter_bytes) = (
        metrics.filter_block_load_count(),
        metrics.filter_block_load_cached_count(),
        metrics.filter_block_io(),
    );
    let (indexes, indexes_cached, index_bytes) = (
        metrics.index_block_load_count(),
        metrics.index_block_load_cached_count(),
        metrics.index_block_io(),
    );
    let samples = read()?;
    println!(
        "  {label}: {} ns, filter loads {} cached {} read {} KiB, \
         index loads {} cached {} read {} KiB",
        percentiles(samples),
        metrics.filter_block_load_count() - filters,
        metrics.filter_block_load_cached_count() - filters_cached,
        (metrics.filter_block_io() - filter_bytes) >> 10,
        metrics.index_block_load_count() - indexes,
        metrics.index_block_load_cached_count() - indexes_cached,
        (metrics.index_block_io() - index_bytes) >> 10,
    );
    Ok(())
}

fn gets(tree: &AnyTree, fixture: Fixture, reads: u64, seqno: SeqNo) -> lsm_tree::Result<Vec<u64>> {
    let mut samples = Vec::with_capacity(reads as usize);
    for i in 0..reads {
        let at = key(i * (fixture.keys / reads), fixture.key_len);
        let start = Instant::now();
        let value = tree.get(at, seqno)?;
        samples.push(start.elapsed().as_nanos() as u64);
        assert!(value.is_some());
    }
    Ok(samples)
}

fn seeks(tree: &AnyTree, fixture: Fixture, reads: u64, seqno: SeqNo) -> lsm_tree::Result<Vec<u64>> {
    let mut samples = Vec::with_capacity(reads as usize);
    for i in 0..reads {
        let at = key(i * (fixture.keys / reads), fixture.key_len);
        let start = Instant::now();
        let row = tree.range(at.., seqno, None).next();
        let row = row.map(Guard::into_inner).transpose()?;
        samples.push(start.elapsed().as_nanos() as u64);
        assert!(row.is_some());
    }
    Ok(samples)
}

fn main() -> lsm_tree::Result<()> {
    // `cargo test` runs a bench target too: a moment of each then.
    let measuring = std::env::args().any(|a| a == "--bench");
    let (scale, reads) = if measuring { (1, READS) } else { (1_000, 50) };

    for fixture in FIXTURES {
        let fixture = Fixture {
            keys: fixture.keys / scale,
            ..fixture
        };
        for layout in [Layout::Oversized, Layout::Fitted, Layout::Pieces] {
            for capacity in CACHES {
                let folder = tempfile::tempdir()?;
                let written_under = if layout == Layout::Oversized {
                    WRITE_CACHE
                } else {
                    capacity
                };
                {
                    let (tree, _) = open(&folder, written_under, layout)?;
                    for i in 0..fixture.keys {
                        tree.insert(key(i, fixture.key_len), VALUE, 0);
                    }
                    tree.flush_active_memtable(0)?;
                    assert_eq!(tree.table_count(), 1);
                }
                let (tree, cache) = open(&folder, capacity, layout)?;
                let seqno = 1;

                println!(
                    "{} {} cache {} MiB, filter {} KiB",
                    fixture.name,
                    layout.name(),
                    capacity >> 20,
                    tree.filter_size() >> 10,
                );
                pass(&tree, "get cold", || gets(&tree, fixture, reads, seqno))?;
                pass(&tree, "get warm", || gets(&tree, fixture, reads, seqno))?;
                pass(&tree, "seek cold", || seeks(&tree, fixture, reads, seqno))?;
                pass(&tree, "seek warm", || seeks(&tree, fixture, reads, seqno))?;
                println!("  cache holds {} KiB", cache.size() >> 10);
            }
        }
    }
    Ok(())
}

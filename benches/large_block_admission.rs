// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! What a filter or an index block heavier than a block cache shard costs.
//!
//! One table holding one whole filter block and one whole index block, not
//! pinned, as a table below L0 holds them by default. Each fixture is read
//! through caches of `CACHES` bytes twice: the first pass cold, the second
//! showing what stayed resident. `partitioned` writes the same keys with both
//! blocks in 4 KiB pieces, which fit every shard.
//!
//! - `get`: point reads of existing keys, each timed alone;
//! - `seek`: a new iterator read to its first row, each timed alone.
//!
//! Per pass it prints the time per read, then how many filter and index blocks
//! the pass loaded, how many of those the cache served, and the bytes read from
//! the file for them. Timings are in ns, as the median, with P99 and P999 where
//! there are enough samples to tell them from the maximum.

use lsm_tree::{
    AbstractTree, AnyTree, Cache, Config, Guard, SeqNo, SequenceNumberCounter,
    config::PinningPolicy,
};
use std::sync::Arc;
use std::time::Instant;

const CACHES: [u64; 3] = [4 << 20, 32 << 20, 256 << 20];
/// Reads of one pass, spread evenly over the table.
const READS: u64 = 5_000;
const VALUE: [u8; 8] = [7; 8];

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
    partitioned: bool,
) -> lsm_tree::Result<(AnyTree, Arc<Cache>)> {
    let cache = Arc::new(Cache::with_capacity_bytes(cache));
    let tree = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .use_cache(Arc::clone(&cache))
    .table_target_size(u64::MAX / 2)
    .filter_block_pinning_policy(PinningPolicy::all(false))
    .index_block_pinning_policy(PinningPolicy::all(false))
    .filter_block_partitioning_policy(PinningPolicy::all(partitioned))
    .index_block_partitioning_policy(PinningPolicy::all(partitioned))
    .open()?;
    if partitioned {
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

fn gets(tree: &AnyTree, fixture: Fixture, seqno: SeqNo) -> lsm_tree::Result<Vec<u64>> {
    let mut samples = Vec::with_capacity(READS as usize);
    for i in 0..READS {
        let at = key(i * (fixture.keys / READS), fixture.key_len);
        let start = Instant::now();
        let value = tree.get(at, seqno)?;
        samples.push(start.elapsed().as_nanos() as u64);
        assert!(value.is_some());
    }
    Ok(samples)
}

fn seeks(tree: &AnyTree, fixture: Fixture, seqno: SeqNo) -> lsm_tree::Result<Vec<u64>> {
    let mut samples = Vec::with_capacity(READS as usize);
    for i in 0..READS {
        let at = key(i * (fixture.keys / READS), fixture.key_len);
        let start = Instant::now();
        let row = tree.range(at.., seqno, None).next();
        let row = row.map(Guard::into_inner).transpose()?;
        samples.push(start.elapsed().as_nanos() as u64);
        assert!(row.is_some());
    }
    Ok(samples)
}

fn main() -> lsm_tree::Result<()> {
    for fixture in FIXTURES {
        for partitioned in [false, true] {
            for capacity in CACHES {
                let folder = tempfile::tempdir()?;
                let (tree, cache) = open(&folder, capacity, partitioned)?;
                for i in 0..fixture.keys {
                    tree.insert(key(i, fixture.key_len), VALUE, 0);
                }
                tree.flush_active_memtable(0)?;
                assert_eq!(tree.table_count(), 1);
                let seqno = 1;

                println!(
                    "{} {} cache {} MiB, filter {} KiB",
                    fixture.name,
                    if partitioned { "partitioned" } else { "whole" },
                    capacity >> 20,
                    tree.filter_size() >> 10,
                );
                pass(&tree, "get cold", || gets(&tree, fixture, seqno))?;
                pass(&tree, "get warm", || gets(&tree, fixture, seqno))?;
                pass(&tree, "seek cold", || seeks(&tree, fixture, seqno))?;
                pass(&tree, "seek warm", || seeks(&tree, fixture, seqno))?;
                println!("  cache holds {} KiB", cache.size() >> 10);
            }
        }
    }
    Ok(())
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Cold `multi_get` on `io_uring` when the filter and index are read too.
//!
//! Filters and indexes are partitioned and left unpinned, the block cache is
//! far smaller than the tree and the row cache is off, so a batch reads filter
//! partitions, index partitions and data blocks through the ring. Level 0
//! holds `t` tables that all span the key range, each holding every `t`-th
//! key, so a batch consults the filter of every one of them and finds each key
//! in exactly one. The last layout puts half the keys in the last level under
//! four such level-0 tables, so half a batch is read through two levels.
//!
//! Prints, per table count and batch size, the median, p99 and p999 wall time
//! per batch over at least a thousand batches, and the filter, index and data
//! bytes a batch read on average. Linux only.

#[cfg(target_os = "linux")]
fn run() -> Result<(), Box<dyn std::error::Error>> {
    use lsm_tree::config::PinningPolicy;
    use lsm_tree::{AbstractTree, Cache, Config, SeqNo, SequenceNumberCounter};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    const KEYS: u64 = 200_000;
    /// Enough batches per size that the p999 is not simply the slowest one.
    const ROUNDS: usize = 1_000;

    fn key(i: u64) -> String {
        format!("key{i:010}")
    }

    fn quantile(samples: &mut [Duration], q: f64) -> Duration {
        samples.sort_unstable();
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss,
            reason = "rank is clamped into 0..samples.len(), a bench-sized count"
        )]
        let rank = ((samples.len() as f64 * q).ceil() as usize).clamp(1, samples.len()) - 1;
        samples.get(rank).copied().unwrap_or_default()
    }

    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % KEYS
    };

    // Level 0 holding `tables` tables, over a last level holding the other
    // half of the keys when `deep`: a key of the last level is read through
    // the level-0 filters first.
    for (tables, deep) in [(1u64, false), (4, false), (16, false), (4, true)] {
        let dir = tempfile::tempdir()?;
        let tree = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(Arc::new(lsm_tree::fs::IoUringFs::new()?))
        // The index partitions from its first entry, not only past the
        // default size.
        .with_runtime_config({
            let mut runtime = lsm_tree::runtime_config::RuntimeConfig::default();
            runtime.index_partition_spill_threshold = 0;
            runtime
        })
        .filter_block_partitioning_policy(PinningPolicy::all(true))
        .index_block_partitioning_policy(PinningPolicy::all(true))
        .filter_block_pinning_policy(PinningPolicy::all(false))
        .index_block_pinning_policy(PinningPolicy::all(false))
        .use_cache(Arc::new(
            Cache::with_capacity_bytes(256 * 1_024).with_row_cache(false),
        ))
        .open()?;

        let mut seqno = 0;
        let level0: Vec<u64> = if deep {
            for i in (0..KEYS).step_by(2) {
                tree.insert(key(i), vec![0xABu8; 256], seqno);
                seqno += 1;
            }
            tree.flush_active_memtable(0)?;
            tree.major_compact(64 * 1_024 * 1_024, u64::MAX)?;
            (1..KEYS).step_by(2).collect()
        } else {
            (0..KEYS).collect()
        };
        let stride = usize::try_from(tables)?;
        for table in 0..stride {
            for &i in level0.iter().skip(table).step_by(stride) {
                tree.insert(key(i), vec![0xABu8; 256], seqno);
                seqno += 1;
            }
            tree.flush_active_memtable(0)?;
        }
        let layout = if deep {
            format!("{tables} tables over the last level")
        } else {
            format!("{tables} tables")
        };

        for batch in [8usize, 64] {
            let mut wall = Vec::with_capacity(ROUNDS);
            let metrics = tree.metrics();
            let (filter, index, data) = (
                metrics.filter_block_io(),
                metrics.index_block_io(),
                metrics.data_block_io(),
            );
            for _ in 0..ROUNDS {
                let keys: Vec<String> = (0..batch).map(|_| key(next())).collect();
                let start = Instant::now();
                let found = tree.multi_get(&keys, SeqNo::MAX)?;
                wall.push(start.elapsed());
                assert!(found.iter().all(Option::is_some), "every key exists");
            }
            let rounds = u64::try_from(ROUNDS)?;
            let metrics = tree.metrics();
            println!(
                "{layout}, batch {batch}: wall p50 {:?} p99 {:?} p999 {:?}; \
                 bytes per batch: filter {} index {} data {}",
                quantile(&mut wall, 0.5),
                quantile(&mut wall, 0.99),
                quantile(&mut wall, 0.999),
                (metrics.filter_block_io() - filter) / rounds,
                (metrics.index_block_io() - index) / rounds,
                (metrics.data_block_io() - data) / rounds,
            );
        }
    }
    Ok(())
}

fn main() {
    #[cfg(target_os = "linux")]
    if let Err(error) = run() {
        eprintln!("io_uring_cold_multi_get failed: {error}");
        std::process::exit(1);
    }
    #[cfg(not(target_os = "linux"))]
    println!("io_uring is Linux only; nothing to measure here");
}

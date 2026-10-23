// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! `multi_get` on `io_uring` over a cold block cache, where turning a key's
//! newest version into its value reads more than the level's data blocks:
//! a blob tree reads each separated value from the value log, a columnar
//! table rebuilds each row group it reads, and a key whose newest version is a
//! merge operand reads its base one level down.
//!
//! The block and blob caches are far smaller than the tree and the row cache is
//! off, so a batch reads through the ring; the OS page cache is left as the
//! writes warmed it, so the figures are the engine's cost of the reads it makes
//! and waits on, not a device's latency.
//!
//! Prints, per layout and batch size, the median, p99 and p999 wall time per
//! batch. Linux only.

#[cfg(target_os = "linux")]
fn run() -> Result<(), Box<dyn std::error::Error>> {
    use lsm_tree::{
        AbstractTree, AnyTree, Cache, Config, KvSeparationOptions, MergeOperator, SeqNo,
        SequenceNumberCounter, UserValue,
    };
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    const KEYS: u64 = 100_000;
    const ROUNDS: usize = 500;

    /// Appends every operand to the base.
    struct Concat;

    impl MergeOperator for Concat {
        fn merge(
            &self,
            _key: &[u8],
            base: Option<&[u8]>,
            operands: &[&[u8]],
        ) -> lsm_tree::Result<UserValue> {
            let mut merged = base.unwrap_or_default().to_vec();
            for operand in operands {
                merged.extend_from_slice(operand);
            }
            Ok(merged.into())
        }
    }

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

    /// A tree in `dir` on `io_uring` with caches far smaller than it.
    fn config(dir: &std::path::Path) -> Result<Config, Box<dyn std::error::Error>> {
        Ok(Config::new(
            dir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(Arc::new(lsm_tree::fs::IoUringFs::new()?))
        .use_cache(Arc::new(
            Cache::with_capacity_bytes(256 * 1_024).with_row_cache(false),
        )))
    }

    /// Times batches of `batch` random keys of `tree`, each of which exists.
    fn measure(tree: &AnyTree, layout: &str, batch: usize) -> lsm_tree::Result<()> {
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % KEYS
        };
        let mut wall = Vec::with_capacity(ROUNDS);
        for _ in 0..ROUNDS {
            let keys: Vec<String> = (0..batch).map(|_| key(next())).collect();
            let start = Instant::now();
            let found = tree.multi_get(&keys, SeqNo::MAX)?;
            wall.push(start.elapsed());
            assert!(found.iter().all(Option::is_some), "every key exists");
        }
        println!(
            "{layout}, batch {batch}: wall p50 {:?} p99 {:?} p999 {:?}",
            quantile(&mut wall, 0.5),
            quantile(&mut wall, 0.99),
            quantile(&mut wall, 0.999),
        );
        Ok(())
    }

    // Separated values, read from the value log.
    {
        let dir = tempfile::tempdir()?;
        let tree = config(dir.path())?
            .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
            .open()?;
        for i in 0..KEYS {
            tree.insert(key(i), vec![0xABu8; 1_024], i);
        }
        tree.flush_active_memtable(0)?;
        for batch in [8usize, 64] {
            measure(&tree, "blob tree", batch)?;
        }
    }

    // Row groups rebuilt from their pages.
    #[cfg(feature = "columnar")]
    {
        let dir = tempfile::tempdir()?;
        let tree = config(dir.path())?.open()?;
        if let AnyTree::Standard(standard) = &tree {
            standard.update_runtime_config(|cfg| cfg.columnar = true)?;
        }
        for i in 0..KEYS {
            tree.insert(key(i), vec![0xABu8; 256], i);
        }
        tree.flush_active_memtable(0)?;
        for batch in [8usize, 64] {
            measure(&tree, "columnar table", batch)?;
        }
    }

    // Merge operands over bases one level down.
    {
        let dir = tempfile::tempdir()?;
        let tree = config(dir.path())?
            .with_merge_operator(Some(Arc::new(Concat)))
            .open()?;
        for i in 0..KEYS {
            tree.insert(key(i), vec![0xABu8; 256], i);
        }
        tree.flush_active_memtable(0)?;
        tree.major_compact(64 * 1_024 * 1_024, u64::MAX)?;
        for i in 0..KEYS {
            tree.merge(key(i), "+", KEYS + i);
        }
        tree.flush_active_memtable(0)?;
        for batch in [8usize, 64] {
            measure(&tree, "merges over bases", batch)?;
        }
    }
    Ok(())
}

fn main() {
    #[cfg(target_os = "linux")]
    if let Err(error) = run() {
        eprintln!("io_uring_value_multi_get failed: {error}");
        std::process::exit(1);
    }
    #[cfg(not(target_os = "linux"))]
    println!("io_uring is Linux only; nothing to measure here");
}

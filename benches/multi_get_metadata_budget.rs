// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! `multi_get` over a level of many tables with full, unpinned filters, under
//! a range of `Config::multi_get_metadata_budget` values.
//!
//! Level 0 holds `TABLES` tables over disjoint key ranges, each with one
//! filter block of about 600 KiB read on demand, under a 1 MiB block cache
//! with the row cache off, so the filters stay cold from batch to batch. A
//! batch of random keys reaches every table, so a level read stage by stage
//! asks for every filter at once: the budget decides how many it holds at a
//! time. Each budget reopens the tree cold. The OS page cache is left as the
//! writes warmed it, so the figures are the engine's cost, not a device's;
//! with `MULTI_GET_DROP_CACHES=1` (Linux, as root) every batch starts on a
//! dropped page cache instead, so its reads go to the device. Built with the
//! `io-uring` feature, `MULTI_GET_IO_URING=1` reads through `IoUringFs`, the
//! backend whose queue overlaps a level's reads; the std backend reads them
//! one after another.
//!
//! Prints, per budget, the median and p99 wall time per batch and the peak of
//! live heap bytes above the level before the batches, counted by the
//! allocator, so freed memory the allocator keeps does not count. The grid
//! runs forward and then backward, so an effect of the order shows as the
//! two passes disagreeing.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

/// The system allocator, counting the bytes live and their peak.
struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every call delegates to `System` with the caller's layout and
// pointer unchanged; the counters only observe.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller upholds `alloc`'s contract, passed through.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller upholds `dealloc`'s contract, passed through.
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn run() -> Result<(), Box<dyn std::error::Error>> {
    use lsm_tree::config::PinningPolicy;
    use lsm_tree::{AbstractTree, Cache, Config, SeqNo, SequenceNumberCounter};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    const TABLES: u64 = 32;
    const KEYS_PER_TABLE: u64 = 500_000;
    const BATCH: usize = 1_024;
    const ROUNDS: usize = 100;
    const BUDGETS: [u64; 8] = [
        1 << 20,
        2 << 20,
        4 << 20,
        8 << 20,
        16 << 20,
        32 << 20,
        64 << 20,
        u64::MAX,
    ];

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

    let dir = tempfile::tempdir()?;
    let fs: Arc<dyn lsm_tree::fs::Fs> = if std::env::var_os("MULTI_GET_IO_URING").is_some() {
        #[cfg(all(feature = "io-uring", target_os = "linux"))]
        {
            Arc::new(lsm_tree::fs::IoUringFs::new()?)
        }
        #[cfg(not(all(feature = "io-uring", target_os = "linux")))]
        {
            return Err("MULTI_GET_IO_URING needs the io-uring feature on Linux".into());
        }
    } else {
        Arc::new(lsm_tree::fs::StdFs)
    };
    let config = |budget: u64| {
        Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(Arc::clone(&fs))
        .filter_block_partitioning_policy(PinningPolicy::all(false))
        .filter_block_pinning_policy(PinningPolicy::all(false))
        .index_block_pinning_policy(PinningPolicy::all(false))
        .use_cache(Arc::new(
            Cache::with_capacity_bytes(1_024 * 1_024).with_row_cache(false),
        ))
        .multi_get_metadata_budget(budget)
    };
    {
        let tree = config(u64::MAX).open()?;
        let mut seqno = 0;
        for table in 0..TABLES {
            for i in table * KEYS_PER_TABLE..(table + 1) * KEYS_PER_TABLE {
                tree.insert(key(i), vec![0xABu8; 16], seqno);
                seqno += 1;
            }
            tree.flush_active_memtable(0)?;
        }
    }

    let drop_caches = std::env::var_os("MULTI_GET_DROP_CACHES").is_some();
    if drop_caches {
        // Dirty pages are not dropped; write them out first.
        std::process::Command::new("sync").status()?;
    }
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % (TABLES * KEYS_PER_TABLE)
    };
    for (pass, budgets) in [
        ("forward", BUDGETS.to_vec()),
        ("backward", BUDGETS.iter().rev().copied().collect()),
    ] {
        for budget in budgets {
            let tree = config(budget).open()?;
            let keys: Vec<Vec<String>> = (0..ROUNDS)
                .map(|_| (0..BATCH).map(|_| key(next())).collect())
                .collect();
            let base = LIVE.load(Ordering::Relaxed);
            PEAK.store(base, Ordering::Relaxed);
            let mut wall = Vec::with_capacity(ROUNDS);
            for batch in &keys {
                if drop_caches {
                    std::fs::write("/proc/sys/vm/drop_caches", "3")?;
                }
                let start = Instant::now();
                let found = tree.multi_get(batch, SeqNo::MAX)?;
                wall.push(start.elapsed());
                assert!(found.iter().all(Option::is_some), "every key exists");
            }
            // `PEAK` starts at `base` and `fetch_max` only raises it.
            let peak = PEAK.load(Ordering::Relaxed) - base;
            let label = if budget == u64::MAX {
                "unbounded".to_owned()
            } else {
                format!("{} MiB", budget >> 20)
            };
            println!(
                "{pass} budget {label}: wall p50 {:?} p99 {:?}; peak heap above the level {} KiB",
                quantile(&mut wall, 0.5),
                quantile(&mut wall, 0.99),
                peak / 1_024,
            );
            drop(tree);
        }
    }
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("multi_get_metadata_budget failed: {error}");
        std::process::exit(1);
    }
}

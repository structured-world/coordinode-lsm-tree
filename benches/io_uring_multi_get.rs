// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Cold `multi_get` on `io_uring`: latency and caller-thread CPU per batch.
//!
//! The tree's block cache is far smaller than any batch's blocks and the row
//! cache is off, so every batch reads its blocks through the ring in chunks
//! rather than from memory. What changes between builds is what the caller
//! spends submitting and collecting those reads, which is why the caller's own
//! CPU time is reported next to the wall time.
//!
//! Prints the median and p99 of both per batch size. Linux only.

#[cfg(target_os = "linux")]
fn run() {
    use lsm_tree::{AbstractTree, Cache, Config, SeqNo, SequenceNumberCounter};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    const KEYS: u64 = 200_000;

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

    let dir = tempfile::tempdir().expect("tempdir");
    let fs = Arc::new(lsm_tree::fs::IoUringFs::new().expect("io_uring available"));
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(fs)
    .use_cache(Arc::new(
        Cache::with_capacity_bytes(16 * 1_024).with_row_cache(false),
    ))
    .open()
    .expect("open");

    for i in 0..KEYS {
        tree.insert(key(i), vec![0xABu8; 256], i);
        if (i + 1) % 20_000 == 0 {
            tree.flush_active_memtable(0).expect("flush");
        }
    }
    tree.major_compact(4 * 1_024 * 1_024, u64::MAX)
        .expect("compact");

    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % KEYS
    };

    for batch in [8usize, 64, 512] {
        let rounds = (40_000 / batch).max(200);
        let mut wall = Vec::with_capacity(rounds);
        let mut cpu = Vec::with_capacity(rounds);
        for _ in 0..rounds {
            let keys: Vec<String> = (0..batch).map(|_| key(next())).collect();
            let start = Instant::now();
            let thread = cpu_time::ThreadTime::now();
            let found = tree.multi_get(&keys, SeqNo::MAX).expect("multi_get");
            cpu.push(thread.elapsed());
            wall.push(start.elapsed());
            assert!(found.iter().all(Option::is_some), "every key exists");
        }
        println!(
            "batch {batch}: wall p50 {:?} p99 {:?}, caller cpu p50 {:?} p99 {:?} ({rounds} batches)",
            quantile(&mut wall, 0.5),
            quantile(&mut wall, 0.99),
            quantile(&mut cpu, 0.5),
            quantile(&mut cpu, 0.99),
        );
    }
}

fn main() {
    #[cfg(target_os = "linux")]
    run();
    #[cfg(not(target_os = "linux"))]
    println!("io_uring is Linux only; nothing to measure here");
}

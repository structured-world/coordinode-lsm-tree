// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Scan throughput over KV-separated values, with and without read-ahead.
//!
//! A scan resolves each separated value with its own read of a few hundred
//! bytes. Values sit in the blob file in the order the flush wrote them, which
//! is key order, so a scan's next values are its on-disk neighbours: the
//! read-ahead gathers a window of upcoming handles and reads the run they cover
//! in one go.
//!
//! Both arms scan the SAME tree on the real filesystem (a `MemFs` would turn
//! every read into a memcpy and hide the difference). Each iteration REOPENS
//! the tree so the timed scan starts against a cold cache, and only the scan is
//! timed: without that, the first iteration would warm every value and the rest
//! would measure cache hits in both arms.

use criterion::{Criterion, criterion_group, criterion_main};
use lsm_tree::{
    AbstractTree, AnyTree, Config, Guard as _, KvSeparationOptions, SeqNo, SequenceNumberCounter,
};
use std::time::{Duration, Instant};

const KEYS: u64 = 20_000;
/// Comfortably over the 1 KiB default separation threshold is not needed: the
/// bench sets the threshold to 1, and a few hundred bytes per value is the size
/// where per-value read overhead dominates, which is what read-ahead targets.
const VALUE_LEN: usize = 400;

fn open(path: &std::path::Path, scan_prefetch: u16) -> lsm_tree::Result<AnyTree> {
    Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default()
            .separation_threshold(1)
            .scan_prefetch(scan_prefetch),
    ))
    .open()
}

fn populate(path: &std::path::Path) -> lsm_tree::Result<()> {
    let seqno = SequenceNumberCounter::default();
    let tree = Config::new(path, seqno.clone(), SequenceNumberCounter::default())
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        .open()?;

    let value = vec![0xCDu8; VALUE_LEN];
    for i in 0..KEYS {
        tree.insert(format!("key{i:08}").as_bytes(), &*value, seqno.next());
    }
    tree.flush_active_memtable(0)?;
    Ok(())
}

fn scan_all(tree: &AnyTree) -> usize {
    let mut n = 0;
    for guard in tree.iter(SeqNo::MAX, None) {
        let (_, value) = guard.into_inner().expect("scan value");
        std::hint::black_box(&value);
        n += 1;
    }
    n
}

fn bench_scan(c: &mut Criterion) {
    let dir = tempfile::tempdir().expect("tempdir");
    populate(dir.path()).expect("populate");

    let mut group = c.benchmark_group("blob_scan");
    group.sample_size(20);

    for (name, window) in [("prefetch_off", 0u16), ("prefetch_on", 64)] {
        group.bench_function(name, |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    // Reopen OUTSIDE the timer: a fresh tree means a fresh
                    // cache, so every iteration reads its values from disk.
                    let tree = open(dir.path(), window).expect("open");

                    let start = Instant::now();
                    let n = scan_all(&tree);
                    total += start.elapsed();

                    assert_eq!(n as u64, KEYS, "scan must cover the whole tree");
                }
                total
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_scan);
criterion_main!(benches);

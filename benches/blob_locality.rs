// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Range scans over large separated values, by blob layout.
//!
//! Four trees hold the same keys and values, each compacted into one table:
//!
//! - `interleaved`: every flush wrote every 32nd key, so the compacted table
//!   points into all 32 blob files at once and a scan alternates between them;
//! - `interleaved_relocated`: the same layout, compacted with locality
//!   relocation on, which rewrites the values in key order;
//! - `consecutive`: every flush wrote one run of adjacent keys;
//! - `consecutive_trigger_on`: the same, compacted with locality relocation
//!   on. Good locality: the trigger must not fire, so against `consecutive`
//!   this arm shows that it costs a well-laid-out tree nothing.
//!
//! Each iteration reopens its tree outside the timer so the scan starts with a
//! cold cache, and scans one eighth of the key space. The blob read requests
//! and bytes of one scan, and each tree's compaction time and blob bytes, are
//! printed once per arm: a layout change shows in the requests first.

use core::num::NonZeroU64;
use criterion::{Criterion, criterion_group, criterion_main};
use lsm_tree::{
    AbstractTree, AnyTree, CompressionType, Config, Guard as _, KvSeparationOptions, SeqNo,
    SequenceNumberCounter,
};
use std::time::{Duration, Instant};

const FILES: u64 = 32;
const KEYS_PER_FILE: u64 = 256;
const VALUE_LEN: usize = 4_096;
const KEYS: u64 = FILES * KEYS_PER_FILE;

fn key(i: u64) -> String {
    format!("key{i:08}")
}

fn blob_opts(relocate: bool) -> KvSeparationOptions {
    let opts = KvSeparationOptions::default();
    if relocate {
        opts.relocate_for_locality(NonZeroU64::new(4).expect("non-zero"), f32::MAX)
    } else {
        opts
    }
}

fn open(path: &std::path::Path, relocate: bool) -> lsm_tree::Result<AnyTree> {
    Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .blob_compression(CompressionType::None)
    .with_kv_separation(Some(blob_opts(relocate)))
    .open()
}

/// Writes one layout, compacts it into one table and reports what the
/// compaction took.
fn populate(path: &std::path::Path, interleaved: bool, relocate: bool) -> lsm_tree::Result<()> {
    let tree = open(path, relocate)?;
    let value = vec![0xCDu8; VALUE_LEN];
    let mut seqno = 0;
    for file in 0..FILES {
        for i in 0..KEYS_PER_FILE {
            let k = if interleaved {
                i * FILES + file
            } else {
                file * KEYS_PER_FILE + i
            };
            tree.insert(key(k), &*value, seqno);
            seqno += 1;
        }
        tree.flush_active_memtable(0)?;
    }

    let start = Instant::now();
    tree.major_compact(u64::MAX, u64::MAX)?;
    let stats = tree.storage_stats()?;
    println!(
        "layout interleaved={interleaved} relocate={relocate}: compaction {:?}, blob files {}, \
         depth {}, used {} B",
        start.elapsed(),
        tree.blob_file_count(),
        stats.blob_references.depth,
        stats.used_bytes,
    );
    Ok(())
}

/// Scans the second eighth of the key space, returning how many rows it read.
fn scan(tree: &AnyTree) -> u64 {
    let (from, to) = (key(KEYS / 8), key(KEYS / 4));
    let mut n = 0;
    for guard in tree.range(from.as_str()..to.as_str(), SeqNo::MAX, None) {
        let (_, value) = guard.into_inner().expect("scan value");
        std::hint::black_box(&value);
        n += 1;
    }
    n
}

fn bench_scan(c: &mut Criterion) {
    let arms = [
        ("interleaved", true, false),
        ("interleaved_relocated", true, true),
        ("consecutive", false, false),
        ("consecutive_trigger_on", false, true),
    ];

    let mut group = c.benchmark_group("blob_locality_range_scan");
    group.sample_size(20);

    for (name, interleaved, relocate) in arms {
        let dir = tempfile::tempdir().expect("tempdir");
        populate(dir.path(), interleaved, relocate).expect("populate");

        {
            let tree = open(dir.path(), relocate).expect("open");
            let metrics = tree.metrics();
            let (requests, bytes) = (metrics.blob_read_count(), metrics.blob_bytes_read());
            assert_eq!(scan(&tree), KEYS / 8);
            println!(
                "{name}: one scan issued {} blob reads for {} B",
                metrics.blob_read_count() - requests,
                metrics.blob_bytes_read() - bytes,
            );
        }

        group.bench_function(name, |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    // Reopened outside the timer: a fresh cache per scan.
                    let tree = open(dir.path(), relocate).expect("open");
                    let start = Instant::now();
                    let n = scan(&tree);
                    total += start.elapsed();
                    assert_eq!(n, KEYS / 8, "the scan must cover its range");
                }
                total
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_scan);
criterion_main!(benches);

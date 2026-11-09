// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Reads of chunked values that the blob cache already partly holds.
//!
//! - `range_in_cached_chunk`: a 4 KiB range inside one chunk of a 1 MiB value
//!   whose chunks the cache holds, read again and again;
//! - `multi_get_cached_heads`: a multi-get of 1 MiB values whose heads a first
//!   multi-get cached, timed on a tree opened fresh for each iteration so the
//!   chunks are not cached yet.
//!
//! Each arm prints the P50, P99 and P999 of its timed reads.

use core::num::NonZeroU32;
use criterion::{Criterion, criterion_group, criterion_main};
use lsm_tree::config::BlobCacheFill;
use lsm_tree::{
    AbstractTree, AnyTree, BlobTree, CompressionType, Config, KvSeparationOptions, SeqNo,
    SequenceNumberCounter,
};
use std::time::{Duration, Instant};

const VALUE_LEN: usize = 1 << 20;
const CHUNK: u32 = 64 * 1_024;
const VALUES: usize = 8;

fn open(path: &std::path::Path) -> BlobTree {
    let AnyTree::Blob(tree) = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .blob_compression(CompressionType::None)
    .blob_cache_fill(BlobCacheFill::Always)
    .with_kv_separation(Some(
        KvSeparationOptions::default()
            .separation_threshold(64)
            .chunk_size(NonZeroU32::new(CHUNK).expect("non-zero")),
    ))
    .open()
    .expect("open") else {
        unreachable!("a blob tree");
    };
    tree
}

fn key(i: usize) -> String {
    format!("value{i:04}")
}

/// A tree of `VALUES` chunked values, flushed.
fn populate(path: &std::path::Path) {
    let tree = open(path);
    for i in 0..VALUES {
        let value: Vec<u8> = (0..VALUE_LEN).map(|b| (b % 251) as u8 ^ i as u8).collect();
        tree.insert(key(i), value, i as u64);
    }
    tree.flush_active_memtable(0).expect("flush");
}

/// Prints the median and the tail of `samples`.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    reason = "an index into a short sample list"
)]
fn report_percentiles(label: &str, mut samples: Vec<Duration>) {
    if samples.is_empty() {
        return;
    }
    samples.sort_unstable();
    let pick = |p: f64| samples[(((samples.len() - 1) as f64) * p).round() as usize];
    eprintln!(
        "  [{label}] n={} P50={:?} P99={:?} P999={:?}",
        samples.len(),
        pick(0.50),
        pick(0.99),
        pick(0.999),
    );
}

fn bench_chunked_reads(c: &mut Criterion) {
    let dir = tempfile::tempdir().expect("tempdir");
    populate(dir.path());
    let mut group = c.benchmark_group("blob_chunked_reads");

    {
        let tree = open(dir.path());
        let range = 300_000_u64..304_096;
        tree.get_range(key(0), range.clone(), SeqNo::MAX)
            .expect("warm")
            .expect("present");
        let mut samples = Vec::new();
        group.bench_function("range_in_cached_chunk", |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let start = Instant::now();
                    let bytes = tree
                        .get_range(key(0), range.clone(), SeqNo::MAX)
                        .expect("read")
                        .expect("present");
                    let elapsed = start.elapsed();
                    assert_eq!(bytes.len(), 4_096);
                    samples.push(elapsed);
                    total += elapsed;
                }
                total
            });
        });
        report_percentiles("range_in_cached_chunk", samples);
    }

    let keys: Vec<String> = (0..VALUES).map(key).collect();
    let mut samples = Vec::new();
    group.sample_size(20);
    group.bench_function("multi_get_cached_heads", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                // A fresh cache: the first multi-get caches what it caches of
                // a chunked value, the timed one reads with that cached.
                let tree = open(dir.path());
                tree.multi_get(&keys, SeqNo::MAX).expect("warm");
                let start = Instant::now();
                let values = tree.multi_get(&keys, SeqNo::MAX).expect("read");
                let elapsed = start.elapsed();
                assert!(
                    values
                        .iter()
                        .all(|v| v.as_ref().is_some_and(|v| v.len() == VALUE_LEN))
                );
                samples.push(elapsed);
                total += elapsed;
            }
            total
        });
    });
    report_percentiles("multi_get_cached_heads", samples);
    group.finish();
}

criterion_group!(benches, bench_chunked_reads);
criterion_main!(benches);

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Cold point reads of small values a zstd blob file packs as records under
//! its own dictionary, each read once on a cache that holds none of them:
//!
//! - `cold_cached`: under `BlobCacheFill::Always`, so every value read is
//!   cached as its record;
//! - `cold_uncached`: under the default fill, on a cache that has not filled,
//!   so none is.
//!
//! Each arm opens the tree afresh for every batch of reads, which is not
//! timed, and prints the P50, P99 and P999 of its reads.

use criterion::{Criterion, criterion_group, criterion_main};
use lsm_tree::config::BlobCacheFill;
use lsm_tree::{
    AbstractTree, AnyTree, BlobTree, CompressionType, Config, KvSeparationOptions, SeqNo,
    SequenceNumberCounter,
};
use std::time::{Duration, Instant};

const VALUES: usize = 20_000;
const BATCH: usize = 1_000;

fn open(path: &std::path::Path, fill: BlobCacheFill) -> BlobTree {
    let AnyTree::Blob(tree) = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .blob_compression(CompressionType::Zstd(3))
    .blob_cache_fill(fill)
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(64),
    ))
    .open()
    .expect("open") else {
        unreachable!("a blob tree");
    };
    tree
}

fn key(i: usize) -> String {
    format!("record{i:06}")
}

/// A JSON-like document of a few hundred bytes, alike enough across keys for
/// the file's dictionary to pay.
fn value(i: usize) -> Vec<u8> {
    format!(
        "{{\"id\":{i},\"tenant\":\"acme\",\"kind\":\"order\",\"region\":\"eu-west-{}\",\
         \"sku\":\"SKU-{}\",\"qty\":{},\"note\":\"customer asked for delivery before noon, \
         leave at the reception desk if nobody answers\"}}",
        i % 3,
        i % 97,
        i % 13,
    )
    .into_bytes()
}

/// A tree of `VALUES` packed values, flushed.
fn populate(path: &std::path::Path) {
    let tree = open(path, BlobCacheFill::default());
    for i in 0..VALUES {
        tree.insert(key(i), value(i), i as u64);
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

fn bench_packed_reads(c: &mut Criterion) {
    let dir = tempfile::tempdir().expect("tempdir");
    populate(dir.path());
    let mut group = c.benchmark_group("blob_packed_reads");
    group.sample_size(20);

    for (label, fill) in [
        ("cold_cached", BlobCacheFill::Always),
        ("cold_uncached", BlobCacheFill::default()),
    ] {
        let mut samples = Vec::new();
        let mut next = 0usize;
        group.bench_function(label, |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    // A fresh tree: nothing it reads is cached yet.
                    let tree = open(dir.path(), fill);
                    for _ in 0..BATCH {
                        let k = key(next % VALUES);
                        next += 7_919;
                        let start = Instant::now();
                        let got = tree.get(&k, SeqNo::MAX).expect("read");
                        let elapsed = start.elapsed();
                        assert!(got.is_some(), "present");
                        std::hint::black_box(got);
                        samples.push(elapsed);
                        total += elapsed;
                    }
                }
                total
            });
        });
        report_percentiles(label, samples);
    }
    group.finish();
}

criterion_group!(benches, bench_packed_reads);
criterion_main!(benches);

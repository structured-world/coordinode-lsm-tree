// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The flush of small values: separated into blocks they share with their
//! neighbours (`blob_<len>`), against the same values kept inline in the
//! table (`tree_<len>`), so the difference is what writing the value log of a
//! tree of small values costs. Each iteration fills a memtable outside the
//! timer and times its flush; every arm prints the P50, P99 and P999 of its
//! timed flushes. Nothing is compressed.

use criterion::{Criterion, criterion_group, criterion_main};
use lsm_tree::config::CompressionPolicy;
use lsm_tree::{AbstractTree, CompressionType, Config, KvSeparationOptions, SequenceNumberCounter};
use std::time::{Duration, Instant};

const KEYS: u64 = 50_000;
const VALUE_LENS: [usize; 2] = [120, 256];

/// Prints the median and the tail of `samples`, one timed flush each.
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

fn bench_block_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("blob_block_write");
    group.sample_size(10);
    for value_len in VALUE_LENS {
        for separated in [true, false] {
            let label = format!("{}_{value_len}", if separated { "blob" } else { "tree" });
            let mut samples = Vec::new();
            group.bench_function(&label, |b| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let folder = tempfile::tempdir().expect("tempdir");
                        let config = Config::new(
                            folder.path(),
                            SequenceNumberCounter::default(),
                            SequenceNumberCounter::default(),
                        )
                        .data_block_compression_policy(CompressionPolicy::all(
                            CompressionType::None,
                        ))
                        .blob_compression(CompressionType::None);
                        let config = if separated {
                            config.with_kv_separation(Some(
                                KvSeparationOptions::default().separation_threshold(64),
                            ))
                        } else {
                            config
                        };
                        let tree = config.open().expect("open");
                        for i in 0..KEYS {
                            let mut value = i.to_be_bytes().to_vec();
                            value.resize(value_len, (i % 251) as u8);
                            tree.insert(format!("key{i:08}"), value, i);
                        }
                        let start = Instant::now();
                        tree.flush_active_memtable(0).expect("flush");
                        let elapsed = start.elapsed();
                        samples.push(elapsed);
                        total += elapsed;
                    }
                    total
                });
            });
            report_percentiles(&label, samples);
        }
    }
    group.finish();
}

criterion_group!(benches, bench_block_write);
criterion_main!(benches);

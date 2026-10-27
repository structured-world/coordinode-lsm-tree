// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Compaction of a settled bottom level under a small update, by codec and by
//! where the update lands.
//!
//! The bottom level is compacted once below the GC watermark first, so its
//! rows already carry the seqnos a later bottommost compaction leaves them
//! with: a block no updated key falls in comes out of the next merge exactly
//! as it went in. `clustered` updates one contiguous 1% of the keys (few
//! blocks touched), `scattered` every 100th key (most blocks of small rows
//! touched), `overwrite` every key (nothing left as it was). Uncompressed
//! blocks are never copied, so `none` measures that path stays as it was.
//!
//! Each iteration builds its tree outside the timed region and times one
//! `major_compact`, on one compaction thread so the figure is the work done
//! rather than how it spread.

use criterion::{Criterion, SamplingMode, criterion_group, criterion_main};
use lsm_tree::config::CompressionPolicy;
use lsm_tree::{AbstractTree, AnyTree, CompressionType, Config, SeqNo, SequenceNumberCounter};
use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
enum Update {
    Clustered,
    Scattered,
    Overwrite,
}

impl Update {
    fn name(self) -> &'static str {
        match self {
            Self::Clustered => "clustered",
            Self::Scattered => "scattered",
            Self::Overwrite => "overwrite",
        }
    }

    fn keys(self, keys: u64) -> Box<dyn Iterator<Item = u64>> {
        match self {
            Self::Clustered => Box::new((keys / 2)..(keys / 2 + keys / 100)),
            Self::Scattered => Box::new((0..keys).step_by(100)),
            Self::Overwrite => Box::new(0..keys),
        }
    }
}

fn key(i: u64) -> String {
    format!("key{i:012}")
}

/// About 100 bytes: a random half and a repeated half, so every codec has
/// something to do.
fn value(i: u64, version: u64) -> String {
    let mut x = i.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ version;
    let mut random = String::with_capacity(48);
    for _ in 0..3 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        random.push_str(&format!("{x:016x}"));
    }
    format!("{random}-v{version}-the quick brown fox jumps")
}

fn build(codec: CompressionType, keys: u64, update: Update) -> (AnyTree, tempfile::TempDir) {
    let folder = tempfile::tempdir().unwrap();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(codec))
    .compaction_threads(1)
    .open()
    .unwrap();

    for i in 0..keys {
        tree.insert(key(i), value(i, 0), 1);
    }
    tree.flush_active_memtable(0).unwrap();
    tree.major_compact(64 * 1024 * 1024, SeqNo::MAX).unwrap();

    for i in update.keys(keys) {
        tree.insert(key(i), value(i, 1), 2);
    }
    tree.flush_active_memtable(0).unwrap();
    (tree, folder)
}

fn bench_row_block_carry(c: &mut Criterion) {
    let codecs = [
        ("none", CompressionType::None, 400_000),
        ("lz4", CompressionType::Lz4, 400_000),
        ("zstd1", CompressionType::zstd(1).unwrap(), 400_000),
        // Level 22 builds slowly; fewer keys keep the run short.
        ("zstd22", CompressionType::zstd(22).unwrap(), 50_000),
    ];
    for (name, codec, keys) in codecs {
        let mut group = c.benchmark_group(format!("row_block_carry_{name}"));
        // Each iteration is a whole compaction plus its setup: flat sampling
        // keeps the run to ten of them rather than a growing series.
        group.sample_size(10).sampling_mode(SamplingMode::Flat);
        for update in [Update::Clustered, Update::Scattered, Update::Overwrite] {
            group.bench_function(update.name(), |b| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let (tree, _folder) = build(codec, keys, update);
                        let start = Instant::now();
                        tree.major_compact(64 * 1024 * 1024, SeqNo::MAX).unwrap();
                        total += start.elapsed();
                        std::hint::black_box(&tree);
                    }
                    total
                });
            });
        }
        group.finish();
    }
}

criterion_group!(benches, bench_row_block_carry);
criterion_main!(benches);

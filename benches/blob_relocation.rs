// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The relocating compaction of compressed blob files.
//!
//! A tree of compressible values, each a frame of its own, is overwritten by
//! half, so its first blob files are half garbage; a first merge learns that,
//! and the timed merge relocates their live halves. The live values are
//! copied as stored, so what the relocation reads it does not have to decode.
//! One arm per codec; each iteration builds its tree outside the timer.

use core::num::NonZeroU32;
use criterion::{Criterion, criterion_group, criterion_main};
use lsm_tree::{
    AbstractTree, AnyTree, BlobTree, CompressionType, Config, KvSeparationOptions,
    SequenceNumberCounter,
};
use std::time::{Duration, Instant};

const KEYS: u64 = 4_000;
const VALUE_LEN: usize = 8_192;

/// A value that compresses as real data does: a seed-dependent header, then
/// a run of one seed-dependent byte.
fn value(i: u64, generation: u8) -> Vec<u8> {
    let mut v = Vec::with_capacity(VALUE_LEN);
    v.extend_from_slice(&i.to_be_bytes());
    v.push(generation);
    v.resize(VALUE_LEN, (i % 251) as u8);
    v
}

/// The tree, its first blob files half garbage and known as such: its next
/// merge under `watermark` relocates their live values.
fn build(compression: CompressionType, watermark: u64) -> (BlobTree, tempfile::TempDir) {
    let folder = tempfile::tempdir().expect("tempdir");
    let AnyTree::Blob(tree) = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .blob_compression(compression)
    .with_kv_separation(Some(
        KvSeparationOptions::default()
            .separation_threshold(64)
            // Values above the block size and within one chunk: a frame
            // each.
            .block_size(1_024)
            .chunk_size(NonZeroU32::new(1_024 * 1_024).expect("non-zero"))
            .age_cutoff(1.0)
            .staleness_threshold(0.1)
            .file_target_size(4 * 1_024 * 1_024),
    ))
    .open()
    .expect("open") else {
        unreachable!("a blob tree");
    };

    for i in 0..KEYS {
        tree.insert(format!("key{i:08}"), value(i, 1), i);
    }
    tree.flush_active_memtable(0).expect("flush");
    for i in (0..KEYS).step_by(2) {
        tree.insert(format!("key{i:08}"), value(i, 2), KEYS + i);
    }
    tree.flush_active_memtable(0).expect("flush");
    // Learns the garbage the next merge relocates.
    tree.major_compact(64 * 1_024 * 1_024, watermark)
        .expect("compact");
    (tree, folder)
}

fn bench_relocation(c: &mut Criterion) {
    let mut group = c.benchmark_group("blob_relocation");
    group.sample_size(10);
    let watermark = 4 * KEYS;
    let codecs = [
        ("lz4", CompressionType::Lz4),
        ("zstd3", CompressionType::Zstd(3)),
    ];
    for (name, compression) in codecs {
        group.bench_function(name, |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let (tree, _folder) = build(compression, watermark);
                    let relocated = tree.metrics().blob_bytes_relocated();
                    let start = Instant::now();
                    tree.major_compact(64 * 1_024 * 1_024, watermark)
                        .expect("compact");
                    total += start.elapsed();
                    assert!(
                        tree.metrics().blob_bytes_relocated() > relocated,
                        "the timed merge relocates",
                    );
                }
                total
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_relocation);
criterion_main!(benches);

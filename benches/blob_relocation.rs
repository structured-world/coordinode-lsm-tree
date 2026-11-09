// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The relocating compaction of compressed blob files.
//!
//! A tree of compressible values, each a frame of its own, is overwritten by
//! half, so its first blob files are half garbage; a first merge learns that,
//! and the timed merge relocates their live halves. The live values are
//! copied as stored, so what the relocation reads it does not have to decode.
//! One arm per codec, for plain values and for cell rows whose one cell holds
//! the value; each iteration builds its tree outside the timer, and each arm
//! prints the P50, P99 and P999 of its timed merges.

use core::num::NonZeroU32;
use criterion::{Criterion, criterion_group, criterion_main};
use lsm_tree::blob_tree::field_row::Field;
use lsm_tree::{
    AbstractTree, AnyTree, BlobTree, CompressionType, Config, KvSeparationOptions,
    SequenceNumberCounter,
};
use std::time::{Duration, Instant};

const KEYS: u64 = 4_000;
const VALUE_LEN: usize = 8_192;
const PAYLOAD_COLUMN: u16 = 1;

/// A value that compresses as real data does: a seed-dependent header, then
/// a run of one seed-dependent byte.
fn value(i: u64, generation: u8) -> Vec<u8> {
    let mut v = Vec::with_capacity(VALUE_LEN);
    v.extend_from_slice(&i.to_be_bytes());
    v.push(generation);
    v.resize(VALUE_LEN, (i % 251) as u8);
    v
}

/// Writes the value of key `i` of `generation`: a plain value, or a cell row
/// whose payload cell holds it.
fn put(tree: &BlobTree, cells: bool, i: u64, generation: u8, seqno: u64) {
    let key = format!("key{i:08}");
    let bytes = value(i, generation);
    if cells {
        tree.insert_cells(key, &[Field::bytes(PAYLOAD_COLUMN, &bytes)], seqno)
            .expect("insert cells");
    } else {
        tree.insert(key, bytes, seqno);
    }
}

/// The tree, its first blob files half garbage and known as such: its next
/// merge under `watermark` relocates their live values.
fn build(
    compression: CompressionType,
    cells: bool,
    watermark: u64,
) -> (BlobTree, tempfile::TempDir) {
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
        put(&tree, cells, i, 1, i);
    }
    tree.flush_active_memtable(0).expect("flush");
    for i in (0..KEYS).step_by(2) {
        put(&tree, cells, i, 2, KEYS + i);
    }
    tree.flush_active_memtable(0).expect("flush");
    // Learns the garbage the next merge relocates.
    tree.major_compact(64 * 1_024 * 1_024, watermark)
        .expect("compact");
    (tree, folder)
}

/// Prints the median and the tail of `samples`, one timed merge each.
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

fn bench_relocation(c: &mut Criterion) {
    let mut group = c.benchmark_group("blob_relocation");
    group.sample_size(10);
    let watermark = 4 * KEYS;
    let arms = [
        ("lz4", CompressionType::Lz4, false),
        ("zstd3", CompressionType::Zstd(3), false),
        ("cells_lz4", CompressionType::Lz4, true),
        ("cells_zstd3", CompressionType::Zstd(3), true),
    ];
    for (name, compression, cells) in arms {
        let mut samples = Vec::new();
        group.bench_function(name, |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let (tree, _folder) = build(compression, cells, watermark);
                    let relocated = tree.metrics().blob_bytes_relocated();
                    let start = Instant::now();
                    tree.major_compact(64 * 1_024 * 1_024, watermark)
                        .expect("compact");
                    let elapsed = start.elapsed();
                    samples.push(elapsed);
                    total += elapsed;
                    assert!(
                        tree.metrics().blob_bytes_relocated() > relocated,
                        "the timed merge relocates",
                    );
                }
                total
            });
        });
        report_percentiles(name, samples);
    }
    group.finish();
}

criterion_group!(benches, bench_relocation);
criterion_main!(benches);

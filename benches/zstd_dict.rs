// Copyright (c) 2025-present, Dmitry Prudnikov
// This source code is licensed under the Apache 2.0 License
// (found in the LICENSE-APACHE file in the repository)

//! Benchmark: per-block zstd dictionary decompression latency.
//!
//! Measures the cost of `decompress_with_dict` for a single compressed block.
//! Two scenarios are covered:
//!
//! - **`warm`** — steady-state per-block cost with a long-lived `ZstdDictionary`
//!   handle, parsed before timing starts.
//! - **`switch/N`** — one frame each of `N` dictionaries, read in turn on one
//!   thread, as reads of blob files that each hold a dictionary of their own
//!   interleave: every read changes the dictionary its decoder decodes with.
//!
//! Run with:
//!
//! ```text
//! cargo bench --bench zstd_dict --features zstd
//! ```

use criterion::{Criterion, criterion_group, criterion_main};

#[cfg(zstd_any)]
use lsm_tree::compression::ZstdDictionary;

// --- test fixtures (only needed when a zstd backend is enabled) ----------

#[cfg(zstd_any)]
/// Pre-trained zstd dictionary (206 bytes).
///
/// Generated with the `zstd` C library from 100 samples × 32 bytes
/// (cycling pattern 0..4).
const DICT: &[u8] = &[
    55, 164, 48, 236, 98, 64, 12, 7, 42, 16, 120, 62, 7, 204, 192, 51, 240, 12, 60, 3, 207, 192,
    51, 240, 12, 60, 3, 207, 192, 51, 24, 17, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 48,
    165, 148, 2, 227, 76, 8, 33, 132, 16, 66, 136, 136, 136, 60, 84, 160, 64, 65, 65, 65, 65, 65,
    65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 193, 231, 162,
    40, 138, 162, 40, 138, 162, 40, 165, 148, 82, 74, 169, 170, 234, 1, 100, 160, 170, 193, 96, 48,
    24, 12, 6, 131, 193, 96, 48, 12, 195, 48, 12, 195, 48, 12, 195, 48, 198, 24, 99, 140, 153, 29,
    1, 0, 0, 0, 4, 0, 0, 0, 8, 0, 0, 0, 3, 3, 3, 3, 3, 3, 3, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2,
    2, 2, 2, 2, 2, 2,
];

#[cfg(zstd_any)]
/// Compressed frame for b"hello world hello world hello world" (35 bytes).
const COMPRESSED: &[u8] = &[
    40, 181, 47, 253, 35, 98, 64, 12, 7, 35, 149, 0, 0, 96, 104, 101, 108, 108, 111, 32, 119, 111,
    114, 108, 100, 32, 1, 0, 175, 75, 18,
];

#[cfg(zstd_any)]
const PLAINTEXT_LEN: usize = 35;

// -------------------------------------------------------------------------

#[cfg(zstd_any)]
fn bench_decompress_with_dict(c: &mut Criterion) {
    use lsm_tree::compression::{CompressionProvider, ZstdBackend as Backend};

    // Warm benchmark: cache is populated before timing starts.
    // Represents steady-state per-block decompression cost.
    let warm_dict = ZstdDictionary::new(DICT);
    Backend::decompress_with_dict(COMPRESSED, &warm_dict, PLAINTEXT_LEN + 1)
        .expect("pre-warm decompression failed");

    c.bench_function("decompress_with_dict/warm", |b| {
        b.iter(|| {
            Backend::decompress_with_dict(
                std::hint::black_box(COMPRESSED),
                std::hint::black_box(&warm_dict),
                PLAINTEXT_LEN + 1,
            )
            .expect("decompression failed")
        });
    });

    // Dictionary switches: each read decodes under another dictionary than
    // the read before it, all parsed before timing starts.
    let plaintext = b"hello world hello world hello world";
    for count in [4usize, 16] {
        let pairs: Vec<(ZstdDictionary, Vec<u8>)> = (0..count)
            .map(|i| {
                let dict = ZstdDictionary::new(
                    format!("raw content dictionary {i:02}: hello world").as_bytes(),
                );
                let frame = Backend::compress_with_dict(plaintext, 3, &dict, true)
                    .expect("compression failed");
                Backend::decompress_with_dict(&frame, &dict, PLAINTEXT_LEN)
                    .expect("pre-warm decompression failed");
                (dict, frame)
            })
            .collect();
        let mut next = pairs.iter().cycle();
        c.bench_function(&format!("decompress_with_dict/switch/{count}"), |b| {
            b.iter(|| {
                let (dict, frame) = next.next().expect("a cycle never ends");
                Backend::decompress_with_dict(
                    std::hint::black_box(frame),
                    std::hint::black_box(dict),
                    PLAINTEXT_LEN,
                )
                .expect("decompression failed")
            });
        });
    }
}

#[cfg(not(zstd_any))]
fn bench_decompress_with_dict(_c: &mut Criterion) {
    // Neither zstd nor zstd-pure feature enabled — nothing to bench.
}

criterion_group!(benches, bench_decompress_with_dict);
criterion_main!(benches);

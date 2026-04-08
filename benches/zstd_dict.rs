// Copyright (c) 2025-present, Structured World Foundation
// This source code is licensed under the Apache 2.0 License
// (found in the LICENSE-APACHE file in the repository)

//! Benchmark: per-block zstd dictionary decompression latency.
//!
//! Measures the cost of `decompress_with_dict` for a single compressed block.
//! Two scenarios are covered:
//!
//! - **`warm`** — steady-state per-block cost with a long-lived `ZstdDictionary`
//!   handle; the TLS `FrameDecoder` is pre-populated before timing starts.
//! - **`tls_hit`** — each iteration receives a *fresh* `ZstdDictionary` handle,
//!   but because all iterations share the same dictionary bytes (same xxh3 hash
//!   key), the thread-local `FrameDecoder` remains cached across iterations.
//!   This measures the steady-state per-block cost when callers reconstruct the
//!   handle on every operation.
//!
//! Run with:
//!
//! ```text
//! cargo bench --bench zstd_dict --features zstd
//! ```

use criterion::{Criterion, criterion_group, criterion_main};

#[cfg(zstd_any)]
use criterion::BatchSize;
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

    // TLS-hit benchmark: each iteration gets a fresh `ZstdDictionary` handle,
    // but the TLS decoder is keyed by the 64-bit content hash. All iterations
    // share the same DICT bytes and therefore the same hash, so the TLS entry
    // remains live across iterations — this measures the steady-state per-block
    // decompression cost with the decoder already cached.
    c.bench_function("decompress_with_dict/tls_hit", |b| {
        b.iter_batched(
            || ZstdDictionary::new(DICT),
            |d| {
                Backend::decompress_with_dict(
                    std::hint::black_box(COMPRESSED),
                    std::hint::black_box(&d),
                    PLAINTEXT_LEN + 1,
                )
                .expect("decompression failed")
            },
            BatchSize::SmallInput,
        );
    });
}

#[cfg(not(zstd_any))]
fn bench_decompress_with_dict(_c: &mut Criterion) {
    // Neither zstd nor zstd-pure feature enabled — nothing to bench.
}

criterion_group!(benches, bench_decompress_with_dict);
criterion_main!(benches);

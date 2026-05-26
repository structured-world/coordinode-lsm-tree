//! Page ECC microbenches.
//!
//! Measures the cost of the Reed-Solomon (4, 2) parity step that
//! `BlockTransform::*Ecc` writers emit and `Block::from_reader` /
//! `from_file` consume on a checksum mismatch. Two hot paths:
//!
//! 1. **encode_parity** — runs on every write when ECC is enabled.
//!    Throughput at typical block sizes (4 KiB / 16 KiB / 64 KiB)
//!    sets the write-side overhead.
//! 2. **try_recover** — runs only on checksum mismatch. Worst-case
//!    cost is 15 trial decodes (C(6, 4)); the bench measures the
//!    first-subset-succeeds case (fastest path) and the all-15-fail
//!    case (slowest path, fall through to PageEccUnrecoverable).

#![cfg(feature = "page_ecc")]

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lsm_tree::ecc::{encode_parity, try_recover};

/// Block sizes covering the typical SST data-block range. 4 KiB is
/// the default `data_block_size` in `Writer`; the larger sizes show
/// how parity scales with the on-disk payload (parity is ~50% the
/// payload size for our (4, 2) scheme).
const SIZES: &[usize] = &[4 * 1024, 16 * 1024, 64 * 1024];

fn deterministic_payload(size: usize) -> Vec<u8> {
    // Pseudo-random but reproducible: each byte = (i * 31 + 7) & 0xFF.
    // Avoids the all-zeros trivial case where the parity might be
    // suspiciously easy to compute, without dragging in `rand`.
    let mut buf = vec![0u8; size];
    for (i, b) in buf.iter_mut().enumerate() {
        *b = ((i.wrapping_mul(31).wrapping_add(7)) & 0xFF) as u8;
    }
    buf
}

fn bench_encode_parity(c: &mut Criterion) {
    let mut group = c.benchmark_group("ecc/encode_parity");
    for &size in SIZES {
        let payload = deterministic_payload(size);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &payload, |b, payload| {
            b.iter(|| {
                let parity = encode_parity(payload).expect("encode succeeds on non-empty input");
                std::hint::black_box(parity);
            });
        });
    }
    group.finish();
}

fn bench_try_recover_first_subset(c: &mut Criterion) {
    // Recovery scenario: shard 0 corrupted. The (missing_a=0,
    // missing_b=1) subset isn't the right one — the right one is
    // (missing_a=0, missing_b=any-other-good). try_recover iterates
    // 15 subsets in order; flipping shard 0 means the FIRST subset
    // that excludes shard 0 wins. Bench measures that path.
    let mut group = c.benchmark_group("ecc/try_recover/first_subset");
    for &size in SIZES {
        let payload = deterministic_payload(size);
        let parity = encode_parity(&payload).expect("parity encodes");
        let expected_xxh3 = lsm_tree::hash::hash128(&payload);

        // Corrupt the first byte of shard 0 in a COPY of the payload.
        let mut corrupt = payload.clone();
        corrupt[0] ^= 0xFF;

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &corrupt, |b, corrupt| {
            b.iter(|| {
                let recovered = try_recover(corrupt, &parity, payload.len(), |buf| {
                    lsm_tree::hash::hash128(buf) == expected_xxh3
                })
                .expect("recovery succeeds");
                std::hint::black_box(recovered);
            });
        });
    }
    group.finish();
}

fn bench_try_recover_all_subsets_fail(c: &mut Criterion) {
    // Worst-case path: try_recover walks all 15 C(6,4) subsets and
    // none reconstructs a payload whose xxh3 matches. The oracle
    // returns `false` on every candidate (we pass an unreachable
    // expected hash), so the function pays the full 15× decode cost
    // before returning Ok(None). Sets the upper bound on the
    // recovery-time CPU we accept for a genuinely-unrecoverable
    // block.
    let mut group = c.benchmark_group("ecc/try_recover/all_subsets_fail");
    for &size in SIZES {
        let payload = deterministic_payload(size);
        let parity = encode_parity(&payload).expect("parity encodes");

        // Flip enough bytes that recovery genuinely can't reconstruct
        // a matching payload — corrupt 3 data shards (more than the
        // (4, 2) scheme can recover).
        let sb = (size.div_ceil(4) + 1) & !1usize; // shard_bytes
        let mut corrupt = payload.clone();
        for i in 0..3 {
            let offset = i * sb;
            if offset < corrupt.len() {
                corrupt[offset] ^= 0xFF;
            }
        }

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &corrupt, |b, corrupt| {
            b.iter(|| {
                // Oracle always returns false — forces all 15 subsets
                // to be tried.
                let result = try_recover(corrupt, &parity, payload.len(), |_| false)
                    .expect("try_recover surfaces engine errors only");
                std::hint::black_box(result);
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_encode_parity,
    bench_try_recover_first_subset,
    bench_try_recover_all_subsets_fail,
);
criterion_main!(benches);

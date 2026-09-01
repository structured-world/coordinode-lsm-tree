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
use lsm_tree::ecc::{RS_DATA_SHARDS, RS_PARITY_SHARDS, encode_parity, try_recover};

/// Cap on retained per-read samples for the percentile report: enough volume
/// for a meaningful P999 while bounding memory (Criterion drives the
/// sub-microsecond reads through millions of iterations).
const PCT_SAMPLE_CAP: usize = 1_000_000;

/// Reports per-read tail latency (P50/P99/P999) to stderr — Criterion's summary
/// only surfaces mean/CI.
fn report_percentiles(label: &str, mut samples: Vec<std::time::Duration>) {
    if samples.is_empty() {
        return;
    }
    samples.sort_unstable();
    let pick =
        |per_mille: usize| samples[(samples.len() * per_mille / 1000).min(samples.len() - 1)];
    eprintln!(
        "  [{label}] n={} P50={:?} P99={:?} P999={:?}",
        samples.len(),
        pick(500),
        pick(990),
        pick(999),
    );
}

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
                let parity = encode_parity(payload, RS_DATA_SHARDS, RS_PARITY_SHARDS)
                    .expect("encode succeeds on non-empty input");
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
        let parity =
            encode_parity(&payload, RS_DATA_SHARDS, RS_PARITY_SHARDS).expect("parity encodes");
        let expected_xxh3 = lsm_tree::hash::hash128(&payload);

        // Corrupt the first byte of shard 0 in a COPY of the payload.
        let mut corrupt = payload.clone();
        corrupt[0] ^= 0xFF;

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &corrupt, |b, corrupt| {
            b.iter(|| {
                let recovered = try_recover(
                    corrupt,
                    &parity,
                    payload.len(),
                    RS_DATA_SHARDS,
                    RS_PARITY_SHARDS,
                    |buf| lsm_tree::hash::hash128(buf) == expected_xxh3,
                )
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
        let parity =
            encode_parity(&payload, RS_DATA_SHARDS, RS_PARITY_SHARDS).expect("parity encodes");

        // Flip enough bytes that recovery genuinely can't reconstruct a
        // matching payload — corrupt one more shard than the scheme can
        // recover. Derived from the scheme constants so this keeps targeting
        // distinct unrecoverable shards if the layout changes.
        let sb = (size.div_ceil(RS_DATA_SHARDS) + 1) & !1usize; // shard_bytes
        let mut corrupt = payload.clone();
        for i in 0..(RS_PARITY_SHARDS + 1) {
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
                let result = try_recover(
                    corrupt,
                    &parity,
                    payload.len(),
                    RS_DATA_SHARDS,
                    RS_PARITY_SHARDS,
                    |_| false,
                )
                .expect("try_recover surfaces engine errors only");
                std::hint::black_box(result);
            });
        });
    }
    group.finish();
}

fn bench_clean_read(c: &mut Criterion) {
    // The path EVERY ECC-protected block read takes: frame in, checksum
    // matches in place, payload detached and served. The parity bytes must
    // never be copied here — recovery (benched above) runs only on a mismatch.
    use lsm_tree::fs::{Fs, FsOpenOptions, MemFs};
    use lsm_tree::table::BlockHandle;
    use lsm_tree::table::block::{
        Block, BlockIdentity, BlockOffset, BlockTransform, BlockType, EccParams,
    };

    let mut group = c.benchmark_group("ecc/clean_read");
    let schemes: &[(&str, EccParams)] =
        &[("secded", EccParams::SECDED), ("rs_4_2", EccParams::RS_4_2)];
    for &(label, params) in schemes {
        for &size in SIZES {
            let payload = deterministic_payload(size);
            let transform = BlockTransform::PLAIN.with_ecc(params);
            let identity = BlockIdentity {
                table_id: 0,
                block_type: BlockType::Data,
                dict_id: 0,
                window_log: 0,
            };

            let fs = MemFs::new();
            let path = format!("/bench-{label}-{size}");
            let mut file = fs
                .open(
                    std::path::Path::new(&path),
                    &FsOpenOptions::new().write(true).create(true).read(true),
                )
                .expect("open mem file");
            let header =
                Block::write_into(&mut file, &payload, identity, &transform).expect("write block");
            // `on_disk_size_with` sizes the frame under the block's ACTUAL
            // scheme; the plain `on_disk_size` assumes the legacy RS(4,2)
            // layout for flagged blocks.
            let on_disk = header.on_disk_size_with(Some(params));
            let handle = BlockHandle::new(BlockOffset(0), on_disk);

            group.throughput(Throughput::Bytes(size as u64));
            // Per-read timing so the percentile report sees individual reads
            // (tail latency), not only Criterion's aggregate. The sample vector
            // outlives the routine closure: Criterion re-enters that closure
            // for warm-up and for every measurement sample, so a vector
            // declared inside it would be rebuilt and reported per invocation.
            let mut samples = Vec::new();
            group.bench_with_input(BenchmarkId::new(label, size), &handle, |b, &handle| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let t = std::time::Instant::now();
                        let block = Block::from_file(&*file, handle, identity, &transform)
                            .expect("clean read succeeds");
                        let elapsed = t.elapsed();
                        std::hint::black_box(block);
                        total += elapsed;
                        if samples.len() < PCT_SAMPLE_CAP {
                            samples.push(elapsed);
                        }
                    }
                    total
                });
            });
            report_percentiles(&format!("clean_read/{label}/{size}"), samples);
        }
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_encode_parity,
    bench_try_recover_first_subset,
    bench_try_recover_all_subsets_fail,
    bench_clean_read,
);
criterion_main!(benches);

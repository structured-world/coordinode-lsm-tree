//! BuRR filter microbenches (construction + probe).
//!
//! The bench file is still named `bloom.rs` for git-history continuity —
//! the standard bloom filter it used to benchmark has been replaced by
//! BuRR (Bumped Ribbon Retrieval). Compare absolute numbers against the
//! pre-BuRR runs to track migration deltas.
//!
//! # Percentile reporting
//!
//! The probe benches (BuRR + Ribbon) use criterion's `iter_custom` API
//! to record per-iteration durations into a fixed-size reservoir, then
//! print P50 / P99 / P999 tail-latency to stderr alongside criterion's
//! own mean+CI report. Criterion's default analysis surfaces mean only
//! and hides tail regressions — the percentiles are how we catch
//! pathological cases in the probe hot path.

use criterion::{Criterion, criterion_group, criterion_main};
use lsm_tree::hash::hash64;
use lsm_tree::table::filter::ribbon::burr::{
    BurrBuilder, BurrFilter, BurrFilterReader, BurrParams,
};
use rand::RngExt;
use std::time::{Duration, Instant};

/// Cap on retained per-iteration duration samples. Criterion can pick
/// very large `iters` counts for fast probe benches (millions); keeping
/// every sample would balloon memory and slow the post-loop sort. With
/// reservoir sampling the cap is also the worst-case memory budget
/// (~16 bytes × cap = ~160 KB at the default), independent of `iters`.
const MAX_SAMPLES: usize = 10_000;

/// Run `body` `iters` times under criterion, recording per-iteration
/// durations into a fixed-size reservoir (Vitter's Algorithm R) to
/// compute P50 / P99 / P999 percentiles. Returns the total elapsed
/// duration (criterion's `iter_custom` contract). Prints the
/// percentile summary to stderr so cross-run diffs can spot tail
/// regressions that the mean+CI report would hide.
fn measure_with_percentiles<F: FnMut()>(label: &str, iters: u64, mut body: F) -> Duration {
    if iters == 0 {
        // Criterion can legitimately call iter_custom with iters == 0
        // during early-warmup probing. Without this guard the
        // percentile indexing below would underflow on an empty
        // samples vec (n.max(1) returns 1 but samples[0] would OOB).
        return Duration::ZERO;
    }
    // Lossless clamp: bound `iters` by MAX_SAMPLES as u64 first, then
    // convert via try_from. Avoids `iters as usize` truncation on
    // 32-bit targets and keeps the cast infallible + lint-clean.
    let cap = usize::try_from(iters.min(MAX_SAMPLES as u64)).unwrap_or(MAX_SAMPLES);
    let mut samples: Vec<Duration> = Vec::with_capacity(cap);
    // Deterministic LCG for reservoir replacement — perf benches
    // should not depend on system RNG availability or quality.
    let mut rng_state: u64 = 0xCAFE_F00D_DEAD_BEEF_u64
        .wrapping_add(iters)
        .wrapping_mul(label.len() as u64 + 1);
    let next_rand = |state: &mut u64| -> u64 {
        *state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        *state
    };
    // Outer wall-clock for the criterion-returned value — captures
    // per-iteration Instant::now overhead AND reservoir bookkeeping.
    // Returning the sum of `elapsed` would under-report (criterion
    // divides by iters for the mean estimate).
    let outer_start = Instant::now();
    for i in 0..iters {
        let start = Instant::now();
        body();
        let elapsed = start.elapsed();
        if samples.len() < MAX_SAMPLES {
            samples.push(elapsed);
        } else {
            // Reservoir replacement: pick a random index in [0, i].
            // If that index falls within the reservoir, replace.
            // Keep math in u64 and only cast to usize AFTER the
            // bounds check. Earlier `usize::try_from(...).unwrap_or(MAX_SAMPLES)`
            // would map any out-of-range index to MAX_SAMPLES on
            // 32-bit (when `i + 1 > usize::MAX`), making the
            // `idx < MAX_SAMPLES` branch never fire and skewing
            // sampling toward early iterations.
            let idx_u64 = next_rand(&mut rng_state) % (i + 1);
            if idx_u64 < MAX_SAMPLES as u64 {
                // Lossless: idx_u64 < MAX_SAMPLES <= usize::MAX on
                // any target we care about (MAX_SAMPLES = 10_000).
                samples[idx_u64 as usize] = elapsed;
            }
        }
    }
    let total = outer_start.elapsed();
    samples.sort_unstable();
    // `iters == 0` early-returned above, so the loop ran at least
    // once and pushed at least one sample. The `.max(1)` was
    // misleading — it doesn't make the indexing safe (an empty
    // vec would still panic on samples[n/2]). A debug_assert
    // documents the invariant for future readers.
    debug_assert!(
        !samples.is_empty(),
        "iters > 0 guarantees at least one sample"
    );
    let n = samples.len();
    // Integer percentile indices — avoids f64 cast lint trips and
    // float-rounding edge cases. Base the math on `last = n - 1`
    // (last valid index) so the indices never saturate at the
    // maximum for exact-boundary sample counts (e.g., for n=100 we
    // must not pick samples[99] for p99 — that's p100, not p99).
    let p50 = samples[n / 2];
    let last = n - 1;
    let p99_idx = last.saturating_mul(99) / 100;
    let p999_idx = last.saturating_mul(999) / 1000;
    let p99 = samples[p99_idx];
    let p999 = samples[p999_idx];
    eprintln!(
        "  [{label}] P50={:>8.2?} P99={:>8.2?} P999={:>8.2?} (n={}/{})",
        p50, p99, p999, n, iters
    );
    total
}

fn fast_block_index(c: &mut Criterion) {
    pub fn fast_impl(h: u64, num_blocks: usize) -> usize {
        // https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
        (((h >> 32).wrapping_mul(num_blocks as u64)) >> 32) as usize
    }

    let mut rng = rand::rng();
    let num_blocks = 100_000;

    c.bench_function("block index - mod", |b| {
        b.iter(|| {
            let h: u64 = rng.random();
            std::hint::black_box(h % (num_blocks as u64))
        });
    });

    c.bench_function("block index - fast", |b| {
        b.iter(|| {
            let h: u64 = rng.random();
            std::hint::black_box(fast_impl(h, num_blocks))
        });
    });
}

fn burr_filter_construction(c: &mut Criterion) {
    let mut rng = rand::rng();

    for n in [100_000_usize, 1_000_000] {
        let label = format!("burr filter build, {n} keys @ FPR=1%");
        c.bench_function(&label, |b| {
            // Pre-hash a key universe so the bench measures BuRR build cost,
            // not RNG.
            let mut keys = Vec::with_capacity(n);
            for _ in 0..n {
                let mut key = [0_u8; 16];
                rng.fill(&mut key[..]);
                keys.push(hash64(&key));
            }

            b.iter(|| {
                let params = BurrParams::with_fp_rate(n, 0.01).expect("params");
                let builder = BurrBuilder::new(params).expect("builder");
                let filter: BurrFilter = builder.build_from_hashes(&keys).expect("build");
                std::hint::black_box(filter.layer_count());
            });
        });
    }
}

fn burr_filter_contains(c: &mut Criterion) {
    let keys: Vec<Vec<u8>> = (0..100_000_u128)
        .map(|x| x.to_be_bytes().to_vec())
        .collect();

    for fpr in [0.01_f32, 0.001, 0.0001] {
        let n = 1_000_000_usize;

        let hashes: Vec<u64> = keys.iter().map(|k| hash64(k)).collect();
        // Pad to n with random hashes so the filter is sized realistically.
        let mut rng = rand::rng();
        let mut padded = hashes.clone();
        while padded.len() < n {
            padded.push(rng.random::<u64>());
        }

        let params = BurrParams::with_fp_rate(n, fpr).expect("params");
        let builder = BurrBuilder::new(params).expect("builder");
        let filter = builder.build_from_hashes(&padded).expect("build");
        let filter_bytes = filter.to_wire_bytes();

        // Long-lived reader matches the table read path (FilterBlock
        // pins the parsed view); construct ONCE outside b.iter to
        // measure steady-state probe latency, not parse+probe.
        let reader = BurrFilterReader::new(&filter_bytes).unwrap();

        // Probe hashes are exactly the prefix of `hashes` covering
        // the real keys (the padding past keys.len() is random
        // fillers we don't want to probe). Slice into `hashes`
        // directly — no need to clone.
        let probe_hashes: &[u64] = &hashes[..keys.len()];

        let probe_label = format!(
            "burr filter contains (probe-only), true positive (FPR={}%)",
            fpr * 100.0
        );
        let mut probe_idx = 0_usize;
        c.bench_function(&probe_label, |b| {
            b.iter_custom(|iters| {
                measure_with_percentiles(&probe_label, iters, || {
                    let hash = probe_hashes[probe_idx];
                    // Branch wrap, not `%` — modulo compiles to a div
                    // and would skew sub-microbench timings / tails.
                    probe_idx += 1;
                    if probe_idx == probe_hashes.len() {
                        probe_idx = 0;
                    }
                    assert!(reader.contains_hash(hash));
                })
            });
        });

        // Separate decode+probe bench so the cost of parsing the wire
        // header is also visible — e.g. for callers that don't pin a
        // long-lived reader.
        let decode_label = format!(
            "burr filter contains (decode+probe), true positive (FPR={}%)",
            fpr * 100.0
        );
        let mut decode_idx = 0_usize;
        c.bench_function(&decode_label, |b| {
            b.iter_custom(|iters| {
                measure_with_percentiles(&decode_label, iters, || {
                    let reader = BurrFilterReader::new(&filter_bytes).unwrap();
                    let hash = probe_hashes[decode_idx];
                    decode_idx += 1;
                    if decode_idx == probe_hashes.len() {
                        decode_idx = 0;
                    }
                    assert!(reader.contains_hash(hash));
                })
            });
        });
    }
}

criterion_group!(
    benches,
    fast_block_index,
    burr_filter_construction,
    burr_filter_contains,
);
criterion_main!(benches);

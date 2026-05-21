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
use std::collections::hash_map::DefaultHasher;
use std::hash::BuildHasherDefault;
use std::time::{Duration, Instant};

type Hasher = BuildHasherDefault<DefaultHasher>;

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
            let idx = usize::try_from(next_rand(&mut rng_state) % (i + 1)).unwrap_or(MAX_SAMPLES);
            if idx < MAX_SAMPLES {
                samples[idx] = elapsed;
            }
        }
    }
    let total = outer_start.elapsed();
    samples.sort_unstable();
    let n = samples.len().max(1);
    // Integer percentile indices — avoids f64 cast lint trips and
    // float-rounding edge cases. p99 = floor(n * 99 / 100),
    // p999 = floor(n * 999 / 1000), both clamped to n-1.
    let p50 = samples[n / 2];
    let p99_idx = (n.saturating_mul(99) / 100).min(n - 1);
    let p999_idx = (n.saturating_mul(999) / 1000).min(n - 1);
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
                let builder = BurrBuilder::new(params, Hasher::default()).expect("builder");
                let filter: BurrFilter<Hasher> = builder.build_from_hashes(&keys).expect("build");
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
        let builder = BurrBuilder::new(params, Hasher::default()).expect("builder");
        let filter = builder.build_from_hashes(&padded).expect("build");
        let filter_bytes = filter.to_wire_bytes();

        // Long-lived reader matches the table read path (FilterBlock
        // pins the parsed view); construct ONCE outside b.iter to
        // measure steady-state probe latency, not parse+probe.
        let reader = BurrFilterReader::new(&filter_bytes).unwrap();

        // Precompute hashes outside the timed body so percentiles
        // reflect ONLY the probe work, not RNG sampling + hash64
        // overhead. Round-robin index gives deterministic, cache-
        // friendly access without rng/hash cost per iteration.
        let probe_hashes: Vec<u64> = hashes.iter().take(keys.len()).copied().collect();

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

/// Standard (single-layer) Ribbon contains_in bench — apples-to-apples
/// against BuRR's contains_hash so the BuRR multi-layer overhead vs
/// pure Ribbon stays visible.
fn ribbon_filter_contains(c: &mut Criterion) {
    use lsm_tree::table::filter::ribbon::{Mode, Params, RibbonBuilder};

    let keys: Vec<u64> = (0..100_000_u64).collect();

    for fpr in [0.01_f32, 0.001, 0.0001] {
        let n = 1_000_000_usize;
        // r = ceil(-log2(fpr)) — matches what BuRR picks internally.
        #[expect(
            clippy::cast_possible_truncation,
            reason = "r is derived from bounded FPR inputs for this benchmark"
        )]
        #[expect(
            clippy::cast_sign_loss,
            reason = "(-log2(fpr)).ceil() is non-negative for fpr in (0, 1)"
        )]
        let r = (-fpr.log2()).ceil() as usize;
        let params = Params::new(n, 64, r, Mode::Standard)
            .expect("ribbon params")
            .with_seed(0);
        let builder = RibbonBuilder::new(params, Hasher::default()).expect("builder");
        // Pad the key set to n with random fillers so the Ribbon load
        // factor matches the BuRR bench above. Without padding the
        // Ribbon body would be ~10% loaded vs BuRR's ~70-90%, skewing
        // probe-latency conclusions.
        let mut rng = rand::rng();
        let mut padded = keys.clone();
        while padded.len() < n {
            padded.push(rng.random::<u64>());
        }
        let filter = builder.build(&padded).expect("build");
        let mut scratch = filter.new_scratch();

        // Precompute the probe order outside the timed body. Round-
        // robin over the key universe — same rationale as the BuRR
        // probe benches above (keep RNG cost out of the percentile
        // measurement).
        let probe_keys: Vec<u64> = keys.clone();
        let mut probe_idx = 0_usize;

        let label = format!(
            "standard ribbon contains, true positive (FPR={}%)",
            fpr * 100.0
        );
        c.bench_function(&label, |b| {
            b.iter_custom(|iters| {
                measure_with_percentiles(&label, iters, || {
                    let sample = probe_keys[probe_idx];
                    probe_idx += 1;
                    if probe_idx == probe_keys.len() {
                        probe_idx = 0;
                    }
                    assert!(filter.contains_in(&sample, &mut scratch));
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
    ribbon_filter_contains,
);
criterion_main!(benches);

use criterion::{Criterion, criterion_group, criterion_main};
use lsm_tree::merge::{BoxedIterator, Merger};
use lsm_tree::merge_source::{CoherentIterSource, MergeSource};
use lsm_tree::seeking_merger::SeekingMerger;
use lsm_tree::{
    DefaultUserComparator, InternalValue, Memtable, SharedComparator, mvcc_stream::MvccStream,
};
use nanoid::nanoid;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Maximum number of duration samples we retain for P99/P999
/// computation. Criterion can pick very large `iters` counts; storing
/// one `Duration` per iteration would balloon memory and slow the
/// sort. With reservoir sampling the cap is also the worst-case
/// memory budget (~16 bytes × cap = ~160 KB at the default cap),
/// which is independent of the criterion iteration count.
const MAX_SAMPLES: usize = 10_000;
/// `MAX_SAMPLES` re-typed as `u64` so reservoir math can stay
/// fully in `u64` and only narrow to `usize` after a bounds check
/// via `usize::try_from`. Declared as a raw literal (not `as u64`
/// from MAX_SAMPLES) to avoid the boundary cast entirely. A
/// `const_assert!` would be ideal but adds a dev-dep; the values
/// must stay equal — keep both lines in sync if MAX_SAMPLES
/// changes.
const MAX_SAMPLES_U64: u64 = 10_000;

/// Run `body` repeatedly under criterion, recording per-iteration
/// durations into a fixed-size reservoir (Vitter's Algorithm R) to
/// compute P50/P99/P999 tail latency. Returns the total duration
/// (criterion's iter_custom contract).
fn measure_with_percentiles<F: FnMut()>(label: &str, iters: u64, mut body: F) -> Duration {
    if iters == 0 {
        // Criterion can legitimately call iter_custom with iters == 0
        // during early-warmup probing. Without this guard the
        // percentile indexing below would underflow on an empty
        // samples vec (n.max(1) returns 1 but samples[0] would OOB).
        return Duration::ZERO;
    }
    // Lossless clamp: bound `iters` by MAX_SAMPLES_U64 first, then
    // convert via try_from — avoids `iters as usize` truncation on
    // 32-bit targets, lint-clean.
    let cap = usize::try_from(iters.min(MAX_SAMPLES_U64)).unwrap_or(MAX_SAMPLES);
    let mut samples: Vec<Duration> = Vec::with_capacity(cap);
    // Deterministic LCG for reservoir replacement — perf benches
    // should not depend on system RNG availability/quality.
    // label.len() → u64 via try_from per the project conversion
    // rule. saturating_add(1) keeps the multiplier non-zero even
    // for the impossible case of label.len() == u64::MAX.
    let label_len_u64 = u64::try_from(label.len()).unwrap_or(u64::MAX);
    let mut rng_state: u64 = 0xCAFE_F00D_DEAD_BEEF_u64
        .wrapping_add(iters)
        .wrapping_mul(label_len_u64.saturating_add(1));
    let next_rand = |state: &mut u64| -> u64 {
        *state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *state
    };
    // Outer wall-clock for the value returned to criterion — this
    // is what `b.iter` would have observed. Includes per-iteration
    // `Instant::now()` overhead AND reservoir bookkeeping
    // (push / LCG / replace). The sum of `elapsed` would
    // under-report by exactly those amounts and skew criterion's
    // mean estimate.
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
    let p50 = samples[n / 2];
    // Integer percentile indices — avoids f64 cast lint trips and
    // float-rounding edge cases. Base the math on `last = n - 1`
    // (last valid index) so the indices never saturate at the
    // maximum for exact-boundary sample counts (e.g., for n=100 we
    // must not pick samples[99] for p99 — that's p100, not p99).
    let last = n - 1;
    let p99_idx = last.saturating_mul(99) / 100;
    let p999_idx = last.saturating_mul(999) / 1000;
    let p99 = samples[p99_idx];
    let p999 = samples[p999_idx];
    // Print tail-latency summary so perf comparisons can be done by
    // diffing bench output across runs. Criterion's own report only
    // surfaces mean+CI which hides tail regressions.
    eprintln!(
        "  [{label}] P50={:>8.2?} P99={:>8.2?} P999={:>8.2?} (n={}/{})",
        p50, p99, p999, n, iters
    );
    total
}

fn merger(c: &mut Criterion) {
    let cmp: SharedComparator = Arc::new(DefaultUserComparator);

    for num in [2_usize, 4, 8, 16, 30] {
        let memtables = (0..num)
            .map(|_| {
                let table = Memtable::new(0, cmp.clone());
                for _ in 0..100 {
                    table.insert(InternalValue::from_components(
                        nanoid!(),
                        vec![],
                        0,
                        lsm_tree::ValueType::Value,
                    ));
                }
                table
            })
            .collect::<Vec<_>>();

        let label_heap = format!("Merge {num} (MergeHeap)");
        c.bench_function(&label_heap, |b| {
            b.iter_custom(|iters| {
                measure_with_percentiles(&label_heap, iters, || {
                    let iters_v = memtables
                        .iter()
                        .map(|x| x.iter().map(Ok))
                        .map(|x| Box::new(x) as BoxedIterator<'_>)
                        .collect();
                    let merger = Merger::new(iters_v, cmp.clone());
                    assert_eq!(num * 100, merger.count());
                })
            })
        });

        let label_seeking = format!("Merge {num} (SeekingMerger)");
        c.bench_function(&label_seeking, |b| {
            b.iter_custom(|iters| {
                measure_with_percentiles(&label_seeking, iters, || {
                    let sources: Vec<Box<dyn MergeSource + '_>> = memtables
                        .iter()
                        .map(|x| {
                            let iter = x.iter().map(Ok);
                            Box::new(CoherentIterSource::new(iter)) as Box<dyn MergeSource + '_>
                        })
                        .collect();
                    // Pass the CONCRETE DefaultUserComparator (not the
                    // Arc<dyn> SharedComparator) so SeekingMerger's
                    // generic C parameter monomorphises to a unit
                    // struct — InternalKey::compare_with's inner
                    // `cmp.compare(...)` call then inlines flat
                    // instead of going through the dyn UserComparator
                    // vtable on every key compare.
                    let merger = SeekingMerger::new(sources, DefaultUserComparator);
                    assert_eq!(num * 100, merger.count());
                })
            })
        });
    }
}

fn mvcc_stream(c: &mut Criterion) {
    let cmp: SharedComparator = Arc::new(DefaultUserComparator);

    for num in [2_usize, 4, 8, 16, 30] {
        let memtables = (0..num)
            .map(|_| {
                let table = Memtable::new(0, cmp.clone());
                for key in 'a'..='z' {
                    table.insert(InternalValue::from_components(
                        key.to_string(),
                        vec![],
                        u64::try_from(num).unwrap_or(u64::MAX),
                        lsm_tree::ValueType::Value,
                    ));
                }
                table
            })
            .collect::<Vec<_>>();

        let label = format!("MVCC stream {num} versions (MergeHeap)");
        c.bench_function(&label, |b| {
            b.iter_custom(|iters| {
                measure_with_percentiles(&label, iters, || {
                    let iters_v = memtables
                        .iter()
                        .map(|x| x.iter().map(Ok))
                        .map(|x| Box::new(x) as BoxedIterator<'_>)
                        .collect();
                    let merger = MvccStream::new(Merger::new(iters_v, cmp.clone()), None);
                    assert_eq!(26, merger.count());
                })
            })
        });
    }
}

criterion_group!(benches, merger, mvcc_stream);
criterion_main!(benches);

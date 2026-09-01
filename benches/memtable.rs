use criterion::{Criterion, criterion_group, criterion_main};
use lsm_tree::{DefaultUserComparator, InternalValue, MAX_SEQNO, Memtable, SharedComparator};
use nanoid::nanoid;
use std::sync::Arc;

fn default_cmp() -> SharedComparator {
    Arc::new(DefaultUserComparator)
}

fn memtable_get_hit(c: &mut Criterion) {
    let memtable = Memtable::new(0, default_cmp());

    memtable.insert(InternalValue::from_components(
        "abc_w5wa35aw35naw",
        vec![1, 2, 3],
        0,
        lsm_tree::ValueType::Value,
    ));

    for _ in 0..1_000_000 {
        memtable.insert(InternalValue::from_components(
            format!("abc_{}", nanoid!()).as_bytes(),
            vec![],
            0,
            lsm_tree::ValueType::Value,
        ));
    }

    c.bench_function("memtable get", |b| {
        b.iter(|| {
            assert_eq!(
                [1, 2, 3],
                &*memtable.get(b"abc_w5wa35aw35naw", MAX_SEQNO).unwrap().value,
            )
        });
    });
}

fn memtable_get_snapshot(c: &mut Criterion) {
    let memtable = Memtable::new(0, default_cmp());

    memtable.insert(InternalValue::from_components(
        "abc_w5wa35aw35naw",
        vec![1, 2, 3],
        0,
        lsm_tree::ValueType::Value,
    ));
    memtable.insert(InternalValue::from_components(
        "abc_w5wa35aw35naw",
        vec![1, 2, 3, 4],
        1,
        lsm_tree::ValueType::Value,
    ));

    for _ in 0..1_000_000 {
        memtable.insert(InternalValue::from_components(
            format!("abc_{}", nanoid!()).as_bytes(),
            vec![],
            0,
            lsm_tree::ValueType::Value,
        ));
    }

    c.bench_function("memtable get snapshot", |b| {
        b.iter(|| {
            assert_eq!(
                [1, 2, 3],
                &*memtable.get(b"abc_w5wa35aw35naw", 1).unwrap().value,
            );
        });
    });
}

fn memtable_get_miss(c: &mut Criterion) {
    let memtable = Memtable::new(0, default_cmp());

    for _ in 0..1_000_000 {
        memtable.insert(InternalValue::from_components(
            format!("abc_{}", nanoid!()).as_bytes(),
            vec![],
            0,
            lsm_tree::ValueType::Value,
        ));
    }

    c.bench_function("memtable get miss", |b| {
        b.iter(|| assert!(memtable.get(b"abc_564321", MAX_SEQNO).is_none()));
    });
}

/// splitmix64 step — deterministic key material so the arms below measure the
/// same key population in every process (nanoid arms rebuild a different
/// random memtable per run, which swings their numbers by ±50% run to run).
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Cap on retained per-op samples for the percentile report: enough volume for
/// a meaningful P999 while bounding memory (Criterion drives these micro-ops
/// through millions of iterations).
const PCT_SAMPLE_CAP: usize = 1_000_000;

/// The most recent `PCT_SAMPLE_CAP` per-op timings. A ring, not an append-only
/// cap: Criterion's warm-up alone runs millions of these ops before measurement
/// starts, so keeping the FIRST cap-many samples would report the warm-up and
/// drop every measured op. The measurement phase comes last, so the tail of the
/// stream is what the report should see.
struct TailSamples {
    buf: Vec<std::time::Duration>,
    seen: usize,
}

impl TailSamples {
    fn new() -> Self {
        Self {
            buf: Vec::with_capacity(PCT_SAMPLE_CAP),
            seen: 0,
        }
    }

    #[inline]
    fn record(&mut self, d: std::time::Duration) {
        if self.buf.len() < PCT_SAMPLE_CAP {
            self.buf.push(d);
        } else {
            self.buf[self.seen % PCT_SAMPLE_CAP] = d;
        }
        self.seen += 1;
    }
}

/// Reports per-op tail latency (P50/P99/P999) to stderr — Criterion's summary
/// only surfaces mean/CI.
fn report_percentiles(label: &str, samples: TailSamples) {
    let mut samples = samples.buf;
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

/// Point-lookup cost averaged over 10 000 distinct resident keys, so a single
/// key's placement in the (per-process random) tower structure cannot dominate
/// the measurement the way it does in `memtable get`.
fn memtable_get_many(c: &mut Criterion) {
    let memtable = Memtable::new(0, default_cmp());
    let mut s = 0x1234_5678_9ABC_DEF0u64;
    let mut probes = Vec::new();

    for i in 0..1_000_000u64 {
        let key = format!("abc_{:016x}", splitmix64(&mut s));
        if i % 100 == 0 {
            probes.push(key.clone());
        }
        memtable.insert(InternalValue::from_components(
            key.as_bytes(),
            vec![1, 2, 3],
            0,
            lsm_tree::ValueType::Value,
        ));
    }

    let mut i = 0usize;
    // Per-op timing so the percentile report sees individual lookups, not only
    // Criterion's aggregate. The samples outlive the routine closure: Criterion
    // re-enters it for warm-up and for every measurement sample, so a
    // collection declared inside it would be rebuilt and reported per invocation.
    let mut samples = TailSamples::new();
    c.bench_function("memtable get many", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let key = &probes[i % probes.len()];
                i += 1;
                let t = std::time::Instant::now();
                let found = memtable.get(key.as_bytes(), MAX_SEQNO).is_some();
                let elapsed = t.elapsed();
                assert!(found);
                total += elapsed;
                samples.record(elapsed);
            }
            total
        });
    });
    report_percentiles("get many", samples);
}

/// Miss cost averaged over 10 000 distinct absent keys (same population trick
/// as `memtable get many`; the `zzz_` prefix guarantees a miss).
fn memtable_get_miss_many(c: &mut Criterion) {
    let memtable = Memtable::new(0, default_cmp());
    let mut s = 0x1234_5678_9ABC_DEF0u64;

    for _ in 0..1_000_000u64 {
        let key = format!("abc_{:016x}", splitmix64(&mut s));
        memtable.insert(InternalValue::from_components(
            key.as_bytes(),
            vec![],
            0,
            lsm_tree::ValueType::Value,
        ));
    }

    let probes: Vec<String> = (0..10_000)
        .map(|_| format!("abc_{:016x}", splitmix64(&mut s)))
        .collect();

    let mut i = 0usize;
    // Same sampling shape as `memtable get many` (see the note there).
    let mut samples = TailSamples::new();
    c.bench_function("memtable get miss many", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let key = &probes[i % probes.len()];
                i += 1;
                let t = std::time::Instant::now();
                let found = memtable.get(key.as_bytes(), MAX_SEQNO).is_some();
                let elapsed = t.elapsed();
                assert!(!found);
                total += elapsed;
                samples.record(elapsed);
            }
            total
        });
    });
    report_percentiles("get miss many", samples);
}

fn memtable_highest_seqno(c: &mut Criterion) {
    c.bench_function("memtable highest seqno", |b| {
        let memtable = Memtable::new(0, default_cmp());

        for x in 0..100_000 {
            memtable.insert(InternalValue::from_components(
                format!("abc_{}", nanoid!()).as_bytes(),
                vec![],
                x,
                lsm_tree::ValueType::Value,
            ));
        }

        b.iter(|| {
            assert_eq!(Some(99_999), memtable.get_highest_seqno());
        });
    });
}

criterion_group!(
    benches,
    memtable_get_hit,
    memtable_get_snapshot,
    memtable_get_miss,
    memtable_get_many,
    memtable_get_miss_many,
    memtable_highest_seqno
);
criterion_main!(benches);

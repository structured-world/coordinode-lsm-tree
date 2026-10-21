//! What building a table's filters costs: CPU, allocations and peak heap.
//!
//! Two parts. The first builds BuRR filters straight from key hashes, as a
//! table writer's `finish` does: a membership filter (the table filter) and a
//! retrieval filter (the locator), over 1k to 1M keys at several widths. The
//! second flushes a memtable into a tree on `MemFs`, where the filters are
//! built together with everything else a flush writes: under the default
//! configuration (filter and locator), without the locator, with a prefix
//! extractor, and with partitioned filters.
//!
//! Every line reports allocations and bytes allocated per build or flush, the
//! peak of live heap above what was live when it started, and the P50 of its
//! wall time with the P99 / P999 where the run has the samples for them. Run
//! it on a quiet host and compare runs of two commits interleaved, not back to
//! back.

use lsm_tree::config::{LocatorPolicy, PinningPolicy};
use lsm_tree::hash::hash64;
use lsm_tree::table::filter::ribbon::burr::{BurrBuilder, BurrParams};
use lsm_tree::{AbstractTree, Config, PrefixExtractor, SequenceNumberCounter};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// The system allocator, counting allocations, bytes, and live bytes with
/// their peak.
struct Counting;

static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every call delegates to `System` with the caller's layout and
// pointer unchanged; the counters only observe.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller upholds `alloc`'s contract, passed through.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(layout.size(), Ordering::Relaxed);
            let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller upholds `dealloc`'s contract, passed through.
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: the caller upholds `realloc`'s contract, passed through.
        let new = unsafe { System.realloc(ptr, layout, new_size) };
        if !new.is_null() {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(new_size, Ordering::Relaxed);
            if new_size >= layout.size() {
                let grown = new_size - layout.size();
                let live = LIVE.fetch_add(grown, Ordering::Relaxed) + grown;
                PEAK.fetch_max(live, Ordering::Relaxed);
            } else {
                LIVE.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        new
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// What one measured operation cost.
struct Sample {
    time: Duration,
    allocs: usize,
    bytes: usize,
    peak: usize,
}

/// Runs `op` once, counting what it allocates and the heap it peaks at above
/// what was live when it started; returns that and what `op` returned.
fn measure<T>(op: impl FnOnce() -> T) -> (Sample, T) {
    let allocs = ALLOCS.load(Ordering::Relaxed);
    let bytes = BYTES.load(Ordering::Relaxed);
    let base = LIVE.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    let start = Instant::now();
    let out = op();
    let time = start.elapsed();
    let sample = Sample {
        time,
        allocs: ALLOCS.load(Ordering::Relaxed) - allocs,
        bytes: BYTES.load(Ordering::Relaxed) - bytes,
        peak: PEAK.load(Ordering::Relaxed) - base,
    };
    (sample, out)
}

/// Prints one line: the median of the counts and the P50 of the times, with
/// the P99 / P999 when there are enough samples to have one (100 / 1000);
/// below that, the slowest sample instead.
fn report(label: &str, mut samples: Vec<Sample>) {
    samples.sort_by_key(|s| s.time);
    let n = samples.len();
    let p50 = samples[n / 2].time;
    let mut tail = String::new();
    for (name, per) in [("P99", 100), ("P999", 1_000)] {
        if n >= per {
            tail += &format!(" {name}={:?}", samples[(n - 1) * (per - 1) / per].time);
        }
    }
    if n < 100 {
        tail += &format!(" max={:?}", samples[n - 1].time);
    }
    let median = |f: fn(&Sample) -> usize| {
        let mut v: Vec<usize> = samples.iter().map(f).collect();
        v.sort_unstable();
        v[v.len() / 2]
    };
    eprintln!(
        "  [{label}] n={n} P50={p50:?}{tail} allocs={} alloc_bytes={} peak={}",
        median(|s| s.allocs),
        median(|s| s.bytes),
        median(|s| s.peak),
    );
}

/// Builds per measurement: enough that a small build is not one sample's
/// noise, few enough that a million keys stay quick.
fn rounds(keys: usize) -> usize {
    (2_000_000 / keys).clamp(10, 1_000)
}

/// `n` distinct key hashes.
fn hashes(n: usize) -> Vec<u64> {
    (0..n as u64)
        .map(|i| hash64(format!("key-{i:012}").as_bytes()))
        .collect()
}

fn filter_builds() {
    for n in [1_000_usize, 10_000, 100_000, 1_000_000] {
        let input = hashes(n);
        for bpk in [8.0_f32, 10.0, 16.0] {
            let samples = (0..rounds(n))
                .map(|_| {
                    let input = input.clone();
                    measure(|| {
                        let params = BurrParams::with_bpk(n, bpk).expect("params");
                        let builder = BurrBuilder::new(params).expect("builder");
                        let filter = builder.build_from_hashes_owned(input).expect("build");
                        std::hint::black_box(filter.to_wire_bytes());
                    })
                    .0
                })
                .collect();
            report(&format!("membership/keys={n}/bpk={bpk}"), samples);
        }

        // A locator at block precision over a table of `n / 64` blocks.
        let locators: Vec<u64> = (0..n as u64).map(|i| i / 64).collect();
        let r = 64 - (n as u64 / 64).leading_zeros().min(63);
        let samples = (0..rounds(n))
            .map(|_| {
                measure(|| {
                    let params = BurrParams::with_bpk(n, r as f32).expect("params");
                    let builder = BurrBuilder::new(params).expect("builder");
                    let filter = builder
                        .build_from_hashes_with_values(&input, &locators)
                        .expect("build");
                    std::hint::black_box(filter.to_wire_bytes());
                })
                .0
            })
            .collect();
        report(&format!("retrieval/keys={n}/r={r}"), samples);
    }
}

/// Prefixes at every `:` of the key.
struct Colon;

impl PrefixExtractor for Colon {
    fn name(&self) -> &str {
        "colon"
    }

    fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        Box::new(
            key.iter()
                .enumerate()
                .filter(|(_, b)| **b == b':')
                .map(move |(i, _)| &key[..=i]),
        )
    }
}

fn flushes() -> lsm_tree::Result<()> {
    type Variant = (&'static str, fn(Config) -> Config);
    let variants: [Variant; 4] = [
        ("default", |c| c),
        ("no-locator", |c| {
            c.locator_policy(LocatorPolicy::disabled())
        }),
        ("prefixes", |c| c.prefix_extractor(Arc::new(Colon))),
        ("partitioned", |c| {
            c.filter_block_partitioning_policy(PinningPolicy::all(true))
        }),
    ];
    for keys in [10_000_u64, 100_000] {
        for (name, variant) in variants {
            let fs = lsm_tree::fs::MemFs::new();
            let tree = variant(
                Config::new(
                    "/flush",
                    SequenceNumberCounter::default(),
                    SequenceNumberCounter::default(),
                )
                .with_fs(fs),
            )
            .open()?;
            let mut seqno = 0;
            let mut samples = Vec::new();
            for round in 0..rounds(keys as usize).min(40) {
                for i in 0..keys {
                    // Sixteen keys a group, so each group shares a prefix.
                    tree.insert(
                        format!("g{:06}:{:012}", i / 16, i),
                        format!("value-{round}-{i}"),
                        seqno,
                    );
                    seqno += 1;
                }
                let (sample, flushed) = measure(|| tree.flush_active_memtable(0));
                flushed?;
                samples.push(sample);
                // Keep the tree from growing through the run.
                tree.major_compact(u64::MAX, 0)?;
            }
            report(&format!("flush/{name}/keys={keys}"), samples);
        }
    }
    Ok(())
}

/// A table's filter built whole at its finish, as levels 0 to 2 build theirs
/// by default, against one built in partitions as the table is written, as
/// the deeper levels do: what flushing a large memtable costs with each, and
/// what a point read through each costs, of a key the tree holds and of one
/// it does not.
fn layouts() -> lsm_tree::Result<()> {
    const KEYS: u64 = 1_000_000;
    const READS: u64 = 200_000;
    for (name, partitioned) in [("full", false), ("partitioned", true)] {
        let tree = Config::new(
            "/layouts",
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_fs(lsm_tree::fs::MemFs::new())
        .filter_block_partitioning_policy(PinningPolicy::all(partitioned))
        .open()?;
        let mut seqno = 0;
        let mut flushes = Vec::new();
        for round in 0..3 {
            for i in 0..KEYS {
                tree.insert(format!("k{i:012}"), format!("value-{round}-{i}"), seqno);
                seqno += 1;
            }
            let (sample, flushed) = measure(|| tree.flush_active_memtable(0));
            flushed?;
            flushes.push(sample);
        }
        report(&format!("layout/{name}/flush/keys={KEYS}"), flushes);

        // Reads spread over the key space by a fixed multiplicative walk.
        let key_at = |n: u64| n.wrapping_mul(0x9E37_79B9_7F4A_7C15) % KEYS;
        for (kind, key_of) in [
            ("present", (|i| format!("k{i:012}")) as fn(u64) -> String),
            ("absent", |i| format!("k{i:012}~")),
        ] {
            let mut samples = Vec::new();
            for n in 0..READS {
                let key = key_of(key_at(n));
                let (sample, got) = measure(|| tree.get(&key, lsm_tree::SeqNo::MAX));
                std::hint::black_box(got?);
                samples.push(sample);
            }
            report(&format!("layout/{name}/get/{kind}"), samples);
        }
    }
    Ok(())
}

fn main() -> lsm_tree::Result<()> {
    // `cargo bench` passes `--bench`; a filter argument selects one part.
    let only: Option<String> = std::env::args().skip(1).find(|a| !a.starts_with('-'));
    if only.as_deref().is_none_or(|o| "filters".contains(o)) {
        eprintln!("[filter_build/filters]");
        filter_builds();
    }
    if only.as_deref().is_none_or(|o| "flushes".contains(o)) {
        eprintln!("[filter_build/flushes]");
        flushes()?;
    }
    if only.as_deref().is_none_or(|o| "layouts".contains(o)) {
        eprintln!("[filter_build/layouts]");
        layouts()?;
    }
    Ok(())
}

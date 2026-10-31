// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! What a row-table compaction costs per entry it merges, and how many heap
//! allocations it makes for each: the cost of turning every input row into an
//! owned entry on its way to the output table.
//!
//! Each configuration writes `fan-in` tables into L0 of a tree on an in-memory
//! file system and times one `major_compact` of all of them into one run:
//!
//! | axis | values |
//! |---|---|
//! | key length | 8, 16, 32, 64, 128 bytes (a key above 20 bytes no longer fits inline) |
//! | key shape | `shared` (every key starts with the same bytes, so most rows are stored as a suffix of their restart head) or `random` (keys differ from the first byte) |
//! | fan-in | 1, 4, 16, 32 input tables |
//! | layout | `interleaved` (key `i` in table `i % fan-in`, so the merge switches input at every key) or `disjoint` (each table a contiguous slice) |
//! | codec | `none`, and `zstd1` when built with `zstd`; `zstd22` only when `COMPACTION_MERGE_CODECS` names it |
//!
//! Per configuration it prints the median over the rounds of the compaction
//! time per entry, the heap allocations and allocated bytes per entry, and the
//! peak of bytes held at once above what the tree held before, all counted by a
//! counting global allocator over the compaction alone (the fixture is built
//! before the count starts). Compaction runs on one thread,
//! so the counts are this path's, not a worker pool's.
//!
//! `COMPACTION_MERGE_ENTRIES` (default 400000) sets the entries per
//! configuration, `COMPACTION_MERGE_ROUNDS` (default 3) the rounds,
//! `COMPACTION_MERGE_VALUE` (default 32) the bytes per value (a value above 20
//! bytes no longer fits inline, so 20 against 21 prices its reference count), and
//! `COMPACTION_MERGE_KEYS`, `COMPACTION_MERGE_SHAPES`,
//! `COMPACTION_MERGE_FANINS`, `COMPACTION_MERGE_LAYOUTS` and
//! `COMPACTION_MERGE_CODECS` (comma-separated) run only the values they name.
//!
//! `COMPACTION_MERGE_VERSIONS` (default 1) stores that many versions of each
//! key, all of which the compaction keeps, within the same entry count;
//! `COMPACTION_MERGE_BLOCK` sets the data block size in bytes; and
//! `COMPACTION_MERGE_REVERSE`, when set, orders keys by a custom comparator, the
//! path that cannot compare keys as words.
//!
//! `COMPACTION_MERGE_PERF_CTL=<ctl>,<ack>` names the fifos of a
//! `perf record --control fifo:<ctl>,<ack> -D -1`: the bench enables recording
//! around each compaction, so the profile leaves the fixture out.

#![expect(
    clippy::expect_used,
    reason = "only for invariants of the harness itself; setup I/O is propagated"
)]

use lsm_tree::config::{BlockSizePolicy, CompressionPolicy};
use lsm_tree::{
    AbstractTree, AnyTree, CompressionType, Config, SequenceNumberCounter, UserComparator,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cmp::Ordering as KeyOrder;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// The system allocator, counting allocations and allocated bytes.
struct Counting;

static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);
/// Bytes allocated and not yet freed, and the most of them seen since the last
/// reset: what a compaction holds at once, which `BYTES` cannot show.
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

/// Adds `size` to the live bytes and raises the peak to them.
fn grew(size: usize) {
    let live = LIVE.fetch_add(size, Ordering::Relaxed) + size;
    PEAK.fetch_max(live, Ordering::Relaxed);
}

// SAFETY: every call delegates to `System` with the caller's layout and
// pointer unchanged; the counters only observe.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller upholds `alloc`'s contract, passed through.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(layout.size(), Ordering::Relaxed);
            grew(layout.size());
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
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
            grew(new_size);
        }
        new
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Bytes per value by default: small, so the per-row key work is what the time
/// shows.
const VALUE_SIZE: usize = 32;
const KEY_LENGTHS: [usize; 5] = [8, 16, 32, 64, 128];
const FANINS: [usize; 4] = [1, 4, 16, 32];
/// Multiplier that scatters a `random` key's index over the key space.
const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;

/// Bytewise order backwards: a comparator the engine cannot treat as bytes.
struct Reverse;

impl UserComparator for Reverse {
    fn name(&self) -> &'static str {
        "compaction-merge-reverse"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> KeyOrder {
        b.cmp(a)
    }
}

#[derive(Clone, Copy)]
enum Shape {
    Shared,
    Random,
}

impl Shape {
    const ALL: [Self; 2] = [Self::Shared, Self::Random];

    fn label(self) -> &'static str {
        match self {
            Self::Shared => "shared",
            Self::Random => "random",
        }
    }
}

#[derive(Clone, Copy)]
enum Placement {
    Interleaved,
    Disjoint,
}

impl Placement {
    const ALL: [Self; 2] = [Self::Interleaved, Self::Disjoint];

    fn label(self) -> &'static str {
        match self {
            Self::Interleaved => "interleaved",
            Self::Disjoint => "disjoint",
        }
    }

    /// The table key `i` of `n` goes to among `fanin`.
    fn table(self, shape: Shape, i: usize, n: usize, fanin: usize) -> usize {
        match (self, shape) {
            (Self::Interleaved, _) => i % fanin,
            (Self::Disjoint, Shape::Shared) => i * fanin / n,
            // The scatter leads the key, so its high bits are the key's range.
            (Self::Disjoint, Shape::Random) => {
                let scatter = u128::from((i as u64).wrapping_mul(GOLDEN));
                usize::try_from((scatter * fanin as u128) >> 64).expect("below fanin")
            }
        }
    }
}

#[derive(Clone, Copy)]
enum Codec {
    None,
    #[cfg(feature = "zstd")]
    Zstd1,
    #[cfg(feature = "zstd")]
    Zstd22,
}

impl Codec {
    const ALL: &[Self] = &[
        Self::None,
        #[cfg(feature = "zstd")]
        Self::Zstd1,
        #[cfg(feature = "zstd")]
        Self::Zstd22,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            #[cfg(feature = "zstd")]
            Self::Zstd1 => "zstd1",
            #[cfg(feature = "zstd")]
            Self::Zstd22 => "zstd22",
        }
    }

    fn compression(self) -> CompressionType {
        match self {
            Self::None => CompressionType::None,
            #[cfg(feature = "zstd")]
            Self::Zstd1 => CompressionType::zstd(1).expect("level 1 is valid"),
            #[cfg(feature = "zstd")]
            Self::Zstd22 => CompressionType::zstd(22).expect("level 22 is valid"),
        }
    }

    /// Whether a run that names no codec includes this one: level 22 takes
    /// minutes, so it runs only when asked for.
    fn runs_unasked(self) -> bool {
        match self {
            #[cfg(feature = "zstd")]
            Self::Zstd22 => false,
            _ => true,
        }
    }
}

/// Key `i`, `len` bytes, in ascending index order: the index ends the key,
/// after the same filler for every key (`shared`) or after a scatter of the
/// index that makes keys differ from their first byte (`random`). An 8-byte
/// `random` key is the scatter alone, which stays unique: the multiplier is odd.
fn key(shape: Shape, len: usize, i: u64) -> Vec<u8> {
    if len == 8 && matches!(shape, Shape::Random) {
        return i.wrapping_mul(GOLDEN).to_be_bytes().to_vec();
    }
    let mut out = Vec::with_capacity(len);
    match shape {
        Shape::Shared => out.resize(len - 8, b'k'),
        Shape::Random => {
            out.extend_from_slice(&i.wrapping_mul(GOLDEN).to_be_bytes());
            out.resize(len - 8, b'k');
        }
    }
    out.extend_from_slice(&i.to_be_bytes());
    out
}

/// A tree holding `fanin` L0 tables of the `n` keys between them.
fn fixture(
    shape: Shape,
    len: usize,
    fanin: usize,
    placement: Placement,
    codec: Codec,
    n: usize,
) -> lsm_tree::Result<AnyTree> {
    let mut config = Config::new(
        Path::new("/bench"),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(lsm_tree::fs::MemFs::new())
    .data_block_compression_policy(CompressionPolicy::all(codec.compression()))
    .compaction_threads(1);
    let block = env_or("COMPACTION_MERGE_BLOCK", 0);
    if block > 0 {
        let block = u32::try_from(block).expect("a data block size fits u32");
        config = config.data_block_size_policy(BlockSizePolicy::all(block));
    }
    if std::env::var_os("COMPACTION_MERGE_REVERSE").is_some() {
        config = config.comparator(Arc::new(Reverse));
    }
    let tree = config.open()?;
    let value = vec![b'v'; env_or("COMPACTION_MERGE_VALUE", VALUE_SIZE)];
    let versions = env_or("COMPACTION_MERGE_VERSIONS", 1).max(1);
    let keys = (n / versions).max(1);
    let mut seqno = 0u64;
    for table in 0..fanin {
        for i in (0..keys).filter(|&i| placement.table(shape, i, keys, fanin) == table) {
            for _ in 0..versions {
                tree.insert(key(shape, len, i as u64), value.as_slice(), seqno);
                seqno += 1;
            }
        }
        tree.flush_active_memtable(0)?;
    }
    Ok(tree)
}

fn median<T: Copy + Ord>(mut samples: Vec<T>) -> T {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn env_or(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Whether the comma-separated list in `name` names `item`; every item when
/// the variable is unset.
fn picked(name: &str, item: &str) -> bool {
    match std::env::var(name) {
        Ok(list) => list.split(',').any(|v| v.trim() == item),
        Err(_) => true,
    }
}

/// The control fifos of a running `perf record`.
struct PerfControl {
    ctl: File,
    ack: File,
}

impl PerfControl {
    fn from_env() -> lsm_tree::Result<Option<Self>> {
        let Ok(paths) = std::env::var("COMPACTION_MERGE_PERF_CTL") else {
            return Ok(None);
        };
        let (ctl, ack) = paths
            .split_once(',')
            .expect("COMPACTION_MERGE_PERF_CTL is <ctl>,<ack>");
        Ok(Some(Self {
            ctl: File::options().write(true).open(ctl)?,
            ack: File::open(ack)?,
        }))
    }

    /// Sends `command` and waits for perf to apply it.
    fn send(&mut self, command: &str) -> lsm_tree::Result<()> {
        self.ctl.write_all(command.as_bytes())?;
        let mut reply = [0; 5];
        self.ack.read_exact(&mut reply)?;
        Ok(())
    }
}

fn main() -> lsm_tree::Result<()> {
    let mut perf = PerfControl::from_env()?;
    // `cargo test` runs a bench target too: one small round then.
    let measuring = std::env::args().any(|a| a == "--bench");
    let (n, rounds) = if measuring {
        (
            env_or("COMPACTION_MERGE_ENTRIES", 400_000).max(1),
            env_or("COMPACTION_MERGE_ROUNDS", 3).max(1),
        )
    } else {
        (2_000, 1)
    };
    for &codec in Codec::ALL {
        let named = std::env::var_os("COMPACTION_MERGE_CODECS").is_some();
        if !picked("COMPACTION_MERGE_CODECS", codec.label()) || !(named || codec.runs_unasked()) {
            continue;
        }
        for len in KEY_LENGTHS {
            if !picked("COMPACTION_MERGE_KEYS", &len.to_string()) {
                continue;
            }
            for shape in Shape::ALL {
                if !picked("COMPACTION_MERGE_SHAPES", shape.label()) {
                    continue;
                }
                for fanin in FANINS {
                    if !picked("COMPACTION_MERGE_FANINS", &fanin.to_string()) {
                        continue;
                    }
                    for placement in Placement::ALL {
                        if fanin == 1 && matches!(placement, Placement::Disjoint) {
                            // One table has one layout.
                            continue;
                        }
                        if !picked("COMPACTION_MERGE_LAYOUTS", placement.label()) {
                            continue;
                        }
                        let mut times = Vec::with_capacity(rounds);
                        let mut allocs = Vec::with_capacity(rounds);
                        let mut bytes = Vec::with_capacity(rounds);
                        let mut peaks = Vec::with_capacity(rounds);
                        for _ in 0..rounds {
                            let tree = fixture(shape, len, fanin, placement, codec, n)?;
                            let input_tables = tree.table_count();
                            let (a0, b0) = (
                                ALLOCS.load(Ordering::Relaxed),
                                BYTES.load(Ordering::Relaxed),
                            );
                            let live_before = LIVE.load(Ordering::Relaxed);
                            PEAK.store(live_before, Ordering::Relaxed);
                            if let Some(perf) = &mut perf {
                                perf.send("enable\n")?;
                            }
                            let start = Instant::now();
                            let result = tree.major_compact(u64::MAX, 0)?;
                            let elapsed = start.elapsed();
                            if let Some(perf) = &mut perf {
                                perf.send("disable\n")?;
                            }
                            allocs.push(ALLOCS.load(Ordering::Relaxed) - a0);
                            bytes.push(BYTES.load(Ordering::Relaxed) - b0);
                            peaks.push(PEAK.load(Ordering::Relaxed) - live_before);
                            times.push(elapsed);
                            assert_eq!(result.tables_in, input_tables, "every table is merged");
                            assert_eq!(result.tables_out, 1, "into one output table");
                            assert_eq!(tree.table_count(), 1, "which replaced them");
                            std::hint::black_box(&tree);
                        }
                        let time: Duration = median(times);
                        println!(
                            "compaction_merge/{}/k{len}/{}/f{fanin}/{}: {:.1} ns/entry, {:.2} allocs/entry, {:.1} B allocated/entry, {:.1} KiB peak",
                            codec.label(),
                            shape.label(),
                            placement.label(),
                            time.as_nanos() as f64 / n as f64,
                            median(allocs) as f64 / n as f64,
                            median(bytes) as f64 / n as f64,
                            median(peaks) as f64 / 1024.0,
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

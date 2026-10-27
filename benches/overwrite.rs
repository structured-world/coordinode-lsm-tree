// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! What rewriting every key of a tree costs, part by part: the head-to-head
//! `overwrite` scenario (a tree holding one copy of `n` keys gets every key a
//! second time and one flush), split into what each part of that path adds.
//!
//! Every arm starts from a copy of the same seeded tree and times the same
//! body, with one part switched off or measured alone:
//!
//! | arm | body | what the difference to `full` is |
//! |---|---|---|
//! | `full` | inserts, then a flush | |
//! | `insert` | inserts only | the flush |
//! | `memfs` | `full` on an in-memory file system | the file system calls and syncs |
//! | `no_filter` | `full` without a filter | building and writing the filter |
//! | `no_pool` | `full` without the table file pool | what the pool saves or costs |
//!
//! The tree is configured as the head-to-head harness configures it under its
//! `rocksdb-parity` preset, so the numbers line up with the published page.
//! Each round runs every arm once, starting one arm later than the round
//! before, and the median over the rounds is reported in nanoseconds per key.
//! A last pass counts, through a counting file system, what one flush writes:
//! the write calls, the bytes, the files it creates and every sync.
//!
//! `OVERWRITE_SIZES` (comma-separated, default `1000,10000,70000`) sets `n`,
//! `OVERWRITE_ROUNDS` (default 9) the rounds, `OVERWRITE_KEYS_PER_SAMPLE`
//! (default 100000) how many keys an arm rewrites per round, in as many
//! rewrites of the key set as that takes.

#![expect(
    clippy::expect_used,
    reason = "benchmark setup favors concise panic messages"
)]

use lsm_tree::config::{CompressionPolicy, FilterPolicy, LocatorPolicy, PinningPolicy};
use lsm_tree::fs::{CountingFs, StdFs, WriteIoStats, WriteOrigin};
use lsm_tree::runtime_config::{KvChecksumPolicy, RuntimeConfig};
use lsm_tree::{AbstractTree, AnyTree, CompressionType, Config, SequenceNumberCounter};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Bytes per value, as in the head-to-head harness.
const VALUE_SIZE: usize = 256;
/// Threads compressing a table's blocks, as in the head-to-head harness.
const COMPRESSION_THREADS: usize = 4;

/// The `i`-th key: the harness's golden-ratio scatter, so consecutive indices
/// land all over the key space.
fn key_for(i: u64) -> [u8; 16] {
    const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&i.wrapping_mul(GOLDEN).to_be_bytes());
    out[8..].copy_from_slice(&i.to_be_bytes());
    out
}

/// Values with the compressibility of RocksDB's `db_bench` defaults, as the
/// harness writes them: 100-byte pieces of 50 random printable bytes twice.
fn values(n: usize) -> Vec<Vec<u8>> {
    let mut state: u64 = 0x2545_F491_4F6C_DD1D;
    let mut pool = Vec::with_capacity((1 << 20) + 100);
    while pool.len() < 1 << 20 {
        let mut half = [0u8; 50];
        for byte in &mut half {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            // `state % 95` is below 95, so the cast cannot truncate.
            *byte = b' ' + (state % 95) as u8;
        }
        pool.extend_from_slice(&half);
        pool.extend_from_slice(&half);
    }
    let slots = pool.len() / VALUE_SIZE;
    (0..n)
        .map(|i| {
            let start = (i % slots) * VALUE_SIZE;
            pool[start..start + VALUE_SIZE].to_vec()
        })
        .collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Codec {
    None,
    #[cfg(feature = "zstd")]
    Zstd22,
}

impl Codec {
    fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            #[cfg(feature = "zstd")]
            Self::Zstd22 => "zstd22",
        }
    }

    fn compression(self) -> CompressionType {
        match self {
            Self::None => CompressionType::None,
            #[cfg(feature = "zstd")]
            Self::Zstd22 => CompressionType::zstd(22).expect("level 22 is valid"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Arm {
    Full,
    Insert,
    MemFs,
    NoFilter,
    NoPool,
}

impl Arm {
    const ALL: [Self; 5] = [
        Self::Full,
        Self::Insert,
        Self::MemFs,
        Self::NoFilter,
        Self::NoPool,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Insert => "insert",
            Self::MemFs => "memfs",
            Self::NoFilter => "no_filter",
            Self::NoPool => "no_pool",
        }
    }
}

/// The harness's tree configuration under `rocksdb-parity`, with the arm's
/// part switched off.
fn config(path: &Path, codec: Codec, arm: Arm) -> Config {
    let mut rc = RuntimeConfig::default();
    rc.manifest_footer_mirror = false;
    rc.kv_checksums = KvChecksumPolicy::Off;
    rc.seqno_in_index = false;
    rc.page_ecc = false;
    rc.disable_cow_on_sst_files = false;
    rc.use_reflink_for_checkpoint = false;
    rc.manifest_kv_checksums = true;
    let config = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_runtime_config(rc)
    .page_ecc(false)
    .locator_policy(LocatorPolicy::disabled())
    .index_block_partitioning_policy(PinningPolicy::disabled())
    .filter_block_partitioning_policy(PinningPolicy::disabled())
    .index_block_pinning_policy(PinningPolicy::all(true))
    .filter_block_pinning_policy(PinningPolicy::all(true))
    .compaction_threads(COMPRESSION_THREADS)
    .use_cache(Arc::new(lsm_tree::Cache::with_capacity_bytes(
        16 * 1024 * 1024,
    )))
    .data_block_compression_policy(CompressionPolicy::all(codec.compression()));
    match arm {
        Arm::NoFilter => config.filter_policy(FilterPolicy::disabled()),
        Arm::NoPool => config.table_file_pool(0),
        Arm::MemFs => config.with_fs(lsm_tree::fs::MemFs::new()),
        Arm::Full | Arm::Insert => config,
    }
}

/// The key set and its values.
struct Inputs {
    keys: Vec<[u8; 16]>,
    values: Vec<Vec<u8>>,
}

impl Inputs {
    fn new(n: usize) -> Self {
        Self {
            keys: (0..n as u64).map(key_for).collect(),
            values: values(n),
        }
    }

    fn n(&self) -> u64 {
        self.keys.len() as u64
    }

    /// Writes every key at seqnos from `first` up.
    fn write(&self, tree: &AnyTree, first: u64) {
        for ((key, value), seqno) in self.keys.iter().zip(&self.values).zip(first..) {
            tree.insert(key, value, seqno);
        }
    }
}

/// One copy of the key set written and flushed, then closed: the state every
/// rewrite starts from.
fn seed(dir: &Path, codec: Codec, arm: Arm, inputs: &Inputs) {
    let tree = config(dir, codec, arm).open().expect("seed: open");
    inputs.write(&tree, 0);
    tree.flush_active_memtable(0).expect("seed: flush");
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("create copy");
    for entry in std::fs::read_dir(from).expect("read seed") {
        let entry = entry.expect("seed entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("seed entry type").is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy seed file");
        }
    }
}

/// An arm's state: the seed on disk it copies, or none for `memfs`, which
/// seeds a fresh in-memory tree per rewrite.
struct ArmState {
    arm: Arm,
    seed: Option<tempfile::TempDir>,
}

impl ArmState {
    fn new(arm: Arm, codec: Codec, inputs: &Inputs) -> Self {
        let seed = (arm != Arm::MemFs).then(|| {
            let dir = tempfile::tempdir().expect("seed dir");
            seed(dir.path(), codec, arm, inputs);
            dir
        });
        Self { arm, seed }
    }

    /// One rewrite of the key set; the time of its body alone.
    fn rewrite(&self, codec: Codec, inputs: &Inputs) -> Duration {
        let (tree, _dir) = match &self.seed {
            Some(seed) => {
                let dir = tempfile::tempdir().expect("arm dir");
                copy_dir(seed.path(), dir.path());
                let tree = config(dir.path(), codec, self.arm)
                    .open()
                    .expect("arm: open");
                (tree, Some(dir))
            }
            None => {
                let tree = config(Path::new("/bench"), codec, self.arm)
                    .open()
                    .expect("memfs: open");
                inputs.write(&tree, 0);
                tree.flush_active_memtable(0).expect("memfs: seed flush");
                (tree, None)
            }
        };
        let start = Instant::now();
        inputs.write(&tree, inputs.n());
        if self.arm != Arm::Insert {
            tree.flush_active_memtable(0).expect("rewrite: flush");
        }
        let elapsed = start.elapsed();
        std::hint::black_box(&tree);
        elapsed
    }
}

fn median(mut samples: Vec<f64>) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples[samples.len() / 2]
}

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Runs the arms for one size and codec and prints each arm's median in
/// nanoseconds per key, then the parts as differences to `full`.
fn measure(codec: Codec, n: usize, rounds: usize, keys_per_sample: usize) {
    let inputs = Inputs::new(n);
    let states: Vec<ArmState> = Arm::ALL
        .iter()
        .map(|&arm| ArmState::new(arm, codec, &inputs))
        .collect();
    let rewrites = (keys_per_sample / n).max(1);
    let mut samples = vec![Vec::with_capacity(rounds); states.len()];
    for round in 0..rounds {
        for offset in 0..states.len() {
            let index = (round + offset) % states.len();
            let state = &states[index];
            let total: Duration = (0..rewrites).map(|_| state.rewrite(codec, &inputs)).sum();
            samples[index].push(total.as_nanos() as f64 / (rewrites * n) as f64);
        }
    }
    let medians: Vec<f64> = samples.into_iter().map(median).collect();
    let of = |arm: Arm| {
        medians[Arm::ALL
            .iter()
            .position(|&a| a == arm)
            .expect("every arm is measured")]
    };
    for (arm, ns) in Arm::ALL.iter().zip(&medians) {
        println!(
            "overwrite/{}/{n}/{}: {ns:.1} ns/key",
            codec.label(),
            arm.label()
        );
    }
    let full = of(Arm::Full);
    println!(
        "overwrite/{}/{n}/parts: insert {:.1}, flush {:.1}, of which fs {:.1}, filter {:.1}; pool saves {:.1} ns/key",
        codec.label(),
        of(Arm::Insert),
        full - of(Arm::Insert),
        full - of(Arm::MemFs),
        full - of(Arm::NoFilter),
        of(Arm::NoPool) - full,
    );
}

/// What one flush of the rewrite writes, counted on a counting file system.
fn count(codec: Codec, n: usize) {
    let inputs = Inputs::new(n);
    let seed_dir = tempfile::tempdir().expect("count seed dir");
    seed(seed_dir.path(), codec, Arm::Full, &inputs);
    let dir = tempfile::tempdir().expect("count dir");
    copy_dir(seed_dir.path(), dir.path());
    let stats = Arc::new(WriteIoStats::default());
    let tree = config(dir.path(), codec, Arm::Full)
        .with_fs(CountingFs::with_stats(Arc::new(StdFs), Arc::clone(&stats)))
        .open()
        .expect("count: open");
    inputs.write(&tree, inputs.n());
    let before = stats.snapshot();
    tree.flush_active_memtable(0).expect("count: flush");
    let flush = stats
        .snapshot()
        .since(&before)
        .expect("one set of counters");
    let files = flush.files();
    println!(
        "overwrite/{}/{n}/flush-io: {} writes, {} bytes, {} files created, {} syncs in all ({} of files, {} of directories, {} of devices)",
        codec.label(),
        files.writes,
        files.bytes,
        files.created,
        flush.syncs(),
        files.syncs,
        flush.directory.syncs,
        flush.directory.device_syncs,
    );
    for origin in WriteOrigin::ALL {
        let writes = flush.origin(origin);
        if writes.writes + writes.syncs + writes.opened > 0 {
            println!(
                "  {}: {} writes, {} bytes, {} opened, {} created, {} syncs",
                origin.name(),
                writes.writes,
                writes.bytes,
                writes.opened,
                writes.created,
                writes.syncs,
            );
        }
    }
}

fn main() {
    let sizes: Vec<usize> = std::env::var("OVERWRITE_SIZES")
        .unwrap_or_else(|_| "1000,10000,70000".to_owned())
        .split(',')
        .map(|s| {
            s.trim()
                .parse()
                .expect("OVERWRITE_SIZES: a list of key counts")
        })
        .collect();
    let rounds = env_or("OVERWRITE_ROUNDS", 9usize).max(1);
    let keys_per_sample = env_or("OVERWRITE_KEYS_PER_SAMPLE", 100_000usize).max(1);
    // `cargo test` runs a bench target too: one quick round then.
    let measuring = std::env::args().any(|a| a == "--bench");
    let (sizes, rounds, keys_per_sample) = if measuring {
        (sizes, rounds, keys_per_sample)
    } else {
        (vec![1_000], 1, 1)
    };
    let codecs = [
        Codec::None,
        #[cfg(feature = "zstd")]
        Codec::Zstd22,
    ];
    for codec in codecs {
        for &n in &sizes {
            measure(codec, n, rounds, keys_per_sample);
            count(codec, n);
        }
    }
}

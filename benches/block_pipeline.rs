// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Cost of the parallel block pipeline per block: flushes one memtable of at
//! least `BP_BLOCKS` data blocks (12 000 by default) and reports, per case, the
//! median wall time of the flush and the median CPU time of the whole process
//! during it (the workers included), each per MiB of rows written.
//!
//! Cases: data block size 4 KiB and 64 KiB by default, codec none, lz4, zstd 1 and zstd 19, one
//! thread (the serial path) and four. Environment knobs:
//!
//! - `BP_BLOCKS`: data blocks per flush.
//! - `BP_BLOCK_SIZES`: comma-separated data block sizes in bytes.
//! - `BP_REPS`: flushes per case; the median is reported.
//! - `BP_FILTER`: run only the cases whose label contains this text.
//! - `BP_INLINE`: comma-separated inline thresholds to sweep for the parallel
//!   cases, each `default`, `max` or a byte count (`default` only if unset).

use cpu_time::ProcessTime;
use lsm_tree::config::{BlockSizePolicy, CompressionPolicy};
use lsm_tree::{AbstractTree, AnyTree, CompressionType, Config, SequenceNumberCounter};
use std::time::{Duration, Instant};

/// Rows per data block: the value length is the block size over this.
const ROWS_PER_BLOCK: u32 = 16;

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// A compressible value of `len` bytes: words drawn from a small vocabulary
/// by a xorshift stream, so every codec has real work and a real ratio.
fn value(len: usize, state: &mut u64) -> Vec<u8> {
    const WORDS: [&[u8]; 16] = [
        b"alpha ",
        b"bravo ",
        b"charlie ",
        b"delta ",
        b"echo ",
        b"foxtrot ",
        b"golf ",
        b"hotel ",
        b"india ",
        b"juliet ",
        b"kilo ",
        b"lima ",
        b"mike ",
        b"november ",
        b"oscar ",
        b"papa ",
    ];
    let mut out = Vec::with_capacity(len + 16);
    while out.len() < len {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        let word = WORDS.get((*state % 16) as usize).copied().unwrap_or(b"x ");
        out.extend_from_slice(word);
        // A digit now and then keeps the stream from being pure dictionary.
        if state.is_multiple_of(5) {
            out.push(b'0' + (*state % 10) as u8);
        }
    }
    out.truncate(len);
    out
}

struct Case {
    block: u32,
    codec: (&'static str, CompressionType),
    threads: usize,
    inline_below: Option<u32>,
    inline_label: String,
}

impl Case {
    fn label(&self) -> String {
        format!(
            "b{}/{}/t{}/inline_{}",
            self.block, self.codec.0, self.threads, self.inline_label
        )
    }
}

/// A tree configured for `case`, holding one memtable of `rows` rows.
fn filled_tree(case: &Case, rows: u64) -> (AnyTree, tempfile::TempDir, u64) {
    let folder = tempfile::tempdir().expect("tempdir");
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(case.block))
    .data_block_compression_policy(CompressionPolicy::all(case.codec.1))
    .compaction_threads(case.threads)
    .parallel_compression_inline_below(case.inline_below)
    .open()
    .expect("open");

    let value_len = (case.block / ROWS_PER_BLOCK) as usize;
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let mut bytes = 0u64;
    for i in 0..rows {
        let key = format!("key{i:016}");
        let value = value(value_len, &mut state);
        bytes += (key.len() + value.len()) as u64;
        tree.insert(key, value, i);
    }
    (tree, folder, bytes)
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples.get(samples.len() / 2).copied().unwrap_or_default()
}

fn main() {
    let blocks: u64 = env_or("BP_BLOCKS", 12_000);
    let reps: usize = env_or("BP_REPS", 5);
    let filter = std::env::var("BP_FILTER").unwrap_or_default();
    let inline: Vec<(Option<u32>, String)> = std::env::var("BP_INLINE")
        .unwrap_or_else(|_| "default".into())
        .split(',')
        .map(|v| match v.trim() {
            "default" => (None, "default".to_owned()),
            "max" => (Some(u32::MAX), "max".to_owned()),
            n => (Some(n.parse().expect("an inline threshold")), n.to_owned()),
        })
        .collect();

    let codecs = [
        ("none", CompressionType::None),
        ("lz4", CompressionType::Lz4),
        ("zstd1", CompressionType::zstd(1).expect("level")),
        ("zstd19", CompressionType::zstd(19).expect("level")),
    ];
    let mut cases = Vec::new();
    let block_sizes: Vec<u32> = std::env::var("BP_BLOCK_SIZES")
        .unwrap_or_else(|_| "4096,65536".into())
        .split(',')
        .map(|v| v.trim().parse().expect("a block size"))
        .collect();
    for &block in &block_sizes {
        for codec in codecs {
            cases.push(Case {
                block,
                codec,
                threads: 1,
                inline_below: None,
                inline_label: "serial".to_owned(),
            });
            for (inline_below, inline_label) in &inline {
                cases.push(Case {
                    block,
                    codec,
                    threads: 4,
                    inline_below: *inline_below,
                    inline_label: inline_label.clone(),
                });
            }
        }
    }

    println!("case\tMiB\twall_ms\tcpu_ms\twall_ms_per_MiB\tcpu_ms_per_MiB");
    for case in cases.iter().filter(|c| c.label().contains(&filter)) {
        let rows = blocks * u64::from(ROWS_PER_BLOCK);
        let mut walls = Vec::with_capacity(reps);
        let mut cpus = Vec::with_capacity(reps);
        let mut mib = 0.0;
        for _ in 0..reps {
            let (tree, _folder, bytes) = filled_tree(case, rows);
            mib = bytes as f64 / (1024.0 * 1024.0);
            let cpu = ProcessTime::now();
            let wall = Instant::now();
            tree.flush_active_memtable(0).expect("flush");
            walls.push(wall.elapsed());
            cpus.push(cpu.elapsed());
            std::hint::black_box(&tree);
        }
        let (wall, cpu) = (median(walls), median(cpus));
        println!(
            "{}\t{mib:.0}\t{:.1}\t{:.1}\t{:.3}\t{:.3}",
            case.label(),
            wall.as_secs_f64() * 1e3,
            cpu.as_secs_f64() * 1e3,
            wall.as_secs_f64() * 1e3 / mib,
            cpu.as_secs_f64() * 1e3 / mib,
        );
    }
}

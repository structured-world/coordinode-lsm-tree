// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Cost of the parallel block pipeline per block: flushes one memtable of
//! `BP_BLOCKS` data blocks (12 000 by default) and reports, per case, the
//! median wall time of the flush and the median CPU time of the whole process
//! during it (the workers included), each per MiB of rows written. After each
//! flush, outside both timings, a sample of the rows is read back and checked.
//!
//! One iteration is a whole flush of 10^4 blocks, so the bench reports a
//! median over a few flushes rather than tail percentiles; the tail latency of
//! a whole compaction is measured by the `compaction` bench.
//!
//! Cases: data block size 4 KiB and 64 KiB by default; codec none, lz4, zstd 1
//! and zstd 19; block transform plain, AES-GCM or page ECC; one thread (the
//! serial path) and four. A table with no codec and no transform never uses
//! the pipeline, so it runs only as the serial baseline. Environment knobs:
//!
//! - `BP_BLOCKS`: data blocks per flush.
//! - `BP_BLOCK_SIZES`: comma-separated data block sizes in bytes.
//! - `BP_TRANSFORMS`: comma-separated transforms, each `plain`, `aes` or `ecc`
//!   (`plain` only if unset).
//! - `BP_REPS`: flushes per case; the median is reported.
//! - `BP_FILTER`: run only the cases whose label contains this text.
//! - `BP_INLINE`: comma-separated inline thresholds to sweep for the parallel
//!   cases, each `default`, `max` or a byte count (`default` only if unset).
//! - `BP_FILTER_PARTITION`: partition the filter at this many bytes, so the
//!   flush builds its filter partitions on the pipeline too (a full filter if
//!   unset).
//!
//! Besides the process's CPU time, the CPU time of the writer's own thread is
//! reported: the flush runs on the calling thread, so that is what the writer
//! spends outside the workers.

use cpu_time::{ProcessTime, ThreadTime};
use lsm_tree::config::{BlockSizePolicy, CompressionPolicy, PinningPolicy};
use lsm_tree::{AbstractTree, AnyTree, CompressionType, Config, SeqNo, SequenceNumberCounter};
use std::sync::Arc;
use std::time::{Duration, Instant};

type BenchResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Values are a block's size over this, so a block holds about as many rows.
const ROWS_PER_BLOCK: u32 = 16;

/// The key of row `i`: always `KEY_LEN` bytes.
fn key(i: u64) -> String {
    format!("key{i:016}")
}

/// Bytes of every key `key` returns.
const KEY_LEN: u64 = 19;

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
        // Both remainders fit their targets (below 16 and below 10).
        let word = usize::try_from(*state % 16)
            .ok()
            .and_then(|at| WORDS.get(at).copied())
            .unwrap_or(b"x ");
        out.extend_from_slice(word);
        // A digit now and then keeps the stream from being pure dictionary.
        if state.is_multiple_of(5) {
            out.push(b'0' + u8::try_from(*state % 10).unwrap_or(0));
        }
    }
    out.truncate(len);
    out
}

/// The per-block transform a case writes under, besides its codec.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Transform {
    Plain,
    Aes,
    Ecc,
}

impl Transform {
    fn parse(name: &str) -> BenchResult<Self> {
        match name {
            "plain" => Ok(Self::Plain),
            "aes" => Ok(Self::Aes),
            "ecc" => Ok(Self::Ecc),
            other => Err(format!("unknown transform {other:?}").into()),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Plain => "plain",
            Self::Aes => "aes",
            Self::Ecc => "ecc",
        }
    }
}

struct Case {
    block: u32,
    codec: (&'static str, CompressionType),
    transform: Transform,
    threads: usize,
    inline_below: Option<u32>,
    inline_label: String,
}

impl Case {
    fn label(&self) -> String {
        format!(
            "b{}/{}/{}/t{}/inline_{}",
            self.block,
            self.codec.0,
            self.transform.name(),
            self.threads,
            self.inline_label
        )
    }

    /// Whether a flush of this case goes through the block pipeline at all.
    fn uses_pipeline(&self) -> bool {
        self.codec.1 != CompressionType::None || self.transform != Transform::Plain
    }
}

/// A tree configured for `case`, one memtable of `blocks` data blocks' rows, the bytes of
/// those rows, and a few rows to read back after the flush.
struct Fixture {
    tree: AnyTree,
    _folder: tempfile::TempDir,
    bytes: u64,
    samples: Vec<(String, Vec<u8>)>,
}

fn filled_tree(case: &Case, blocks: u64, filter_partition: Option<u32>) -> BenchResult<Fixture> {
    let folder = tempfile::tempdir()?;
    let mut config = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(case.block))
    .data_block_compression_policy(CompressionPolicy::all(case.codec.1))
    .compaction_threads(case.threads)
    .parallel_compression_inline_below(case.inline_below);
    match case.transform {
        Transform::Plain => {}
        Transform::Aes => {
            config =
                config.with_encryption(Some(Arc::new(lsm_tree::Aes256GcmProvider::new(&[7; 32]))));
        }
        Transform::Ecc => config = config.page_ecc(true),
    }
    if let Some(size) = filter_partition {
        config = config
            .filter_block_partitioning_policy(PinningPolicy::all(true))
            .filter_block_partition_size_policy(BlockSizePolicy::all(size));
    }
    let tree = config.open()?;

    // The writer cuts a block once its keys and values reach the block size,
    // so a block holds as many rows as it takes to reach it.
    if u64::try_from(key(0).len())? != KEY_LEN {
        return Err("KEY_LEN no longer matches the key format".into());
    }
    let value_len = case.block / ROWS_PER_BLOCK;
    let entry_len = KEY_LEN + u64::from(value_len);
    let rows_per_block = u64::from(case.block).div_ceil(entry_len);
    let rows = blocks
        .checked_mul(rows_per_block)
        .ok_or("too many rows for one flush")?;
    let value_len = usize::try_from(value_len)?;
    let last = rows.checked_sub(1).ok_or("a flush of no rows")?;
    let sampled = [0, rows / 2, last];
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let mut bytes = 0u64;
    let mut samples = Vec::with_capacity(sampled.len());
    for i in 0..rows {
        let key = key(i);
        let value = value(value_len, &mut state);
        bytes += u64::try_from(key.len() + value.len())?;
        if sampled.contains(&i) {
            samples.push((key.clone(), value.clone()));
        }
        tree.insert(key, value, i);
    }
    Ok(Fixture {
        tree,
        _folder: folder,
        bytes,
        samples,
    })
}

/// Reads the sampled rows back from the flushed table.
fn verify(fixture: &Fixture, label: &str) -> BenchResult<()> {
    for (key, expected) in &fixture.samples {
        let got = fixture.tree.get(key, SeqNo::MAX)?;
        if got.as_deref() != Some(expected.as_slice()) {
            return Err(format!("{label}: {key} did not read back").into());
        }
    }
    Ok(())
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples.get(samples.len() / 2).copied().unwrap_or_default()
}

fn list(name: &str, default: &str) -> Vec<String> {
    std::env::var(name)
        .unwrap_or_else(|_| default.to_owned())
        .split(',')
        .map(|v| v.trim().to_owned())
        .collect()
}

fn main() -> BenchResult<()> {
    let blocks: u64 = env_or("BP_BLOCKS", 12_000);
    let reps: usize = env_or("BP_REPS", 5);
    let filter = std::env::var("BP_FILTER").unwrap_or_default();
    let filter_partition: Option<u32> = std::env::var("BP_FILTER_PARTITION")
        .ok()
        .map(|v| v.parse())
        .transpose()?;
    let inline = list("BP_INLINE", "default")
        .into_iter()
        .map(|v| match v.as_str() {
            "default" => Ok((None, v)),
            "max" => Ok((Some(u32::MAX), v)),
            n => Ok((Some(n.parse()?), v)),
        })
        .collect::<BenchResult<Vec<_>>>()?;
    let block_sizes = list("BP_BLOCK_SIZES", "4096,65536")
        .iter()
        .map(|v| v.parse::<u32>())
        .collect::<Result<Vec<_>, _>>()?;
    let transforms = list("BP_TRANSFORMS", "plain")
        .iter()
        .map(|v| Transform::parse(v))
        .collect::<BenchResult<Vec<_>>>()?;

    let codecs = [
        ("none", CompressionType::None),
        ("lz4", CompressionType::Lz4),
        ("zstd1", CompressionType::zstd(1)?),
        ("zstd19", CompressionType::zstd(19)?),
    ];
    let mut cases = Vec::new();
    for &block in &block_sizes {
        for &transform in &transforms {
            for codec in codecs {
                let serial = Case {
                    block,
                    codec,
                    transform,
                    threads: 1,
                    inline_below: None,
                    inline_label: "serial".to_owned(),
                };
                let uses_pipeline = serial.uses_pipeline();
                cases.push(serial);
                if !uses_pipeline {
                    continue;
                }
                for (inline_below, inline_label) in &inline {
                    cases.push(Case {
                        block,
                        codec,
                        transform,
                        threads: 4,
                        inline_below: *inline_below,
                        inline_label: inline_label.clone(),
                    });
                }
            }
        }
    }

    println!(
        "case\tMiB\twall_ms\tcpu_ms\twriter_cpu_ms\twall_ms_per_MiB\tcpu_ms_per_MiB\twriter_cpu_ms_per_MiB"
    );
    for case in cases.iter().filter(|c| c.label().contains(&filter)) {
        let mut walls = Vec::with_capacity(reps);
        let mut cpus = Vec::with_capacity(reps);
        let mut writer_cpus = Vec::with_capacity(reps);
        let mut mib = 0.0;
        for _ in 0..reps {
            let fixture = filled_tree(case, blocks, filter_partition)?;
            mib = fixture.bytes as f64 / (1024.0 * 1024.0);
            let cpu = ProcessTime::now();
            let writer_cpu = ThreadTime::now();
            let wall = Instant::now();
            fixture.tree.flush_active_memtable(0)?;
            walls.push(wall.elapsed());
            writer_cpus.push(writer_cpu.elapsed());
            cpus.push(cpu.elapsed());
            verify(&fixture, &case.label())?;
        }
        let (wall, cpu, writer_cpu) = (median(walls), median(cpus), median(writer_cpus));
        println!(
            "{}\t{mib:.0}\t{:.1}\t{:.1}\t{:.1}\t{:.3}\t{:.3}\t{:.3}",
            case.label(),
            wall.as_secs_f64() * 1e3,
            cpu.as_secs_f64() * 1e3,
            writer_cpu.as_secs_f64() * 1e3,
            wall.as_secs_f64() * 1e3 / mib,
            cpu.as_secs_f64() * 1e3 / mib,
            writer_cpu.as_secs_f64() * 1e3 / mib,
        );
    }
    Ok(())
}

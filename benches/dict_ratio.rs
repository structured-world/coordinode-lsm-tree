// Copyright (c) 2026-present, Dmitry Prudnikov
// This source code is licensed under the Apache 2.0 License
// (found in the LICENSE-APACHE file in the repository)

//! What a compression dictionary is worth, per block, measured rather than
//! assumed.
//!
//! Prints a table instead of using criterion: the question here is BYTES first
//! and time second, and criterion measures time.
//!
//! Four things are measured, one per question a dictionary policy has to
//! answer:
//!
//! 1. **Ratio by workload and block size.** A dictionary pays off by supplying
//!    the LZ77 history a small block cannot build for itself, so the win has to
//!    shrink as blocks grow, and vanish on data with no shared structure.
//!    Both directions are printed rather than only the flattering one.
//! 2. **Training cost.** What one training run costs on a corpus of a given
//!    size, which is what decides whether it can sit inside a compaction.
//! 3. **Staleness.** A dictionary trained on one generation of data and applied
//!    to a later one: the number that decides whether training is a one-off or
//!    has to repeat.
//! 4. **The cost of asking.** Compressing a small sample twice (with and
//!    without) to decide per output, which is the cheap probe a compaction
//!    could run.
//!
//! Run with:
//!
//! ```text
//! cargo bench --bench dict_ratio --features zstd
//! ```

#[cfg(not(zstd_any))]
fn main() {
    eprintln!("dict_ratio needs the `zstd` feature");
}

#[cfg(zstd_any)]
fn main() {
    measure::run();
}

#[cfg(zstd_any)]
mod measure {
    use lsm_tree::compression::{CompressionProvider as _, ZstdBackend, ZstdDictionary};
    use std::time::Instant;
    use structured_zstd::dictionary::{
        FastCoverOptions, FinalizeOptions, create_fastcover_dict_from_slice,
    };

    /// zstd level the engine's default dictionary policy would use.
    const LEVEL: i32 = 3;
    /// Levels swept in the ratio table. A dictionary changes what the match
    /// finder can reach, and the match finder is exactly what a level selects,
    /// so the two interact: the cheapest levels have the least history of their
    /// own to fall back on, and the most expensive ones find matches a
    /// dictionary would otherwise supply.
    const LEVELS: &[i32] = &[1, 3, 9, 19, 22];
    /// Dictionary size to train. 110 KiB is the zstd CLI's own default.
    const DICT_SIZE: usize = 112_640;

    /// One synthetic corpus, standing in for a shape of user data.
    struct Workload {
        name: &'static str,
        /// Builds record `i`. Records are concatenated into blocks.
        record: fn(u64) -> Vec<u8>,
    }

    /// Keys and values with heavy shared structure: the shape an LSM tree
    /// holding serialized records actually sees.
    fn structured_record(i: u64) -> Vec<u8> {
        format!(
            "{{\"tenant\":\"acme-corp\",\"table\":\"orders\",\"id\":{i},\
             \"region\":\"eu-central-1\",\"status\":\"confirmed\",\
             \"created_at\":\"2026-09-08T12:{:02}:{:02}Z\",\"total_cents\":{}}}",
            i % 60,
            (i * 7) % 60,
            1000 + (i % 90_000),
        )
        .into_bytes()
    }

    /// Timeseries-shaped records: a repeating metric name, a moving timestamp
    /// and a small float.
    fn timeseries_record(i: u64) -> Vec<u8> {
        format!(
            "host=web-{:03} metric=cpu.usage.percent ts={} value={}.{}\n",
            i % 200,
            1_757_000_000 + i,
            i % 100,
            (i * 13) % 1000,
        )
        .into_bytes()
    }

    /// Incompressible payloads: a counter-keyed record whose body is a
    /// deterministic pseudo-random blob. The floor case, where a dictionary
    /// has nothing to offer and its cost still has to be paid.
    fn random_record(i: u64) -> Vec<u8> {
        let mut out = format!("key-{i:016}|").into_bytes();
        let mut state = i.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        for _ in 0..64 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            out.extend_from_slice(&state.to_le_bytes());
        }
        out
    }

    const WORKLOADS: &[Workload] = &[
        Workload {
            name: "structured (json-ish records)",
            record: structured_record,
        },
        Workload {
            name: "timeseries (metric lines)",
            record: timeseries_record,
        },
        Workload {
            name: "random (incompressible bodies)",
            record: random_record,
        },
    ];

    /// Cuts `range` of records into blocks of about `block_size` bytes.
    fn blocks(w: &Workload, range: std::ops::Range<u64>, block_size: usize) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let mut current = Vec::with_capacity(block_size + 256);
        for i in range {
            current.extend_from_slice(&(w.record)(i));
            if current.len() >= block_size {
                out.push(std::mem::take(&mut current));
                current.reserve(block_size + 256);
            }
        }
        if !current.is_empty() {
            out.push(current);
        }
        out
    }

    fn train(corpus: &[u8]) -> (ZstdDictionary, std::time::Duration) {
        let mut raw = Vec::new();
        let started = Instant::now();
        create_fastcover_dict_from_slice(
            corpus,
            &mut raw,
            DICT_SIZE,
            &FastCoverOptions::default(),
            FinalizeOptions::default(),
        )
        .expect("training a dictionary on the sample corpus");
        let elapsed = started.elapsed();
        (ZstdDictionary::new(&raw), elapsed)
    }

    /// One measured variant: what it costs to write, what it costs to read
    /// back, and what it leaves on disk.
    struct Variant {
        bytes: usize,
        /// Compression throughput over the raw input, MiB/s.
        write_mib_s: f64,
        /// Decompression throughput over the raw output, MiB/s.
        read_mib_s: f64,
    }

    fn mib_s(raw_bytes: usize, elapsed: std::time::Duration) -> f64 {
        if elapsed.as_secs_f64() == 0.0 {
            return f64::INFINITY;
        }
        (raw_bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64()
    }

    /// Compresses and decompresses every block at `level`, with the dictionary
    /// when `dict` is given.
    fn measure_variant(blocks: &[Vec<u8>], level: i32, dict: Option<&ZstdDictionary>) -> Variant {
        let raw: usize = blocks.iter().map(Vec::len).sum();

        let started = Instant::now();
        let compressed: Vec<Vec<u8>> = blocks
            .iter()
            .map(|b| match dict {
                Some(d) => {
                    ZstdBackend::compress_with_dict(b, level, d.raw()).expect("dict compression")
                }
                None => ZstdBackend::compress(b, level).expect("plain compression"),
            })
            .collect();
        let write = started.elapsed();

        let bytes: usize = compressed.iter().map(Vec::len).sum();

        // Read back, because a policy that only counts written bytes hides
        // what it costs to serve them.
        let started = Instant::now();
        for (block, frame) in blocks.iter().zip(&compressed) {
            let out = match dict {
                Some(d) => ZstdBackend::decompress_with_dict(frame, d, block.len())
                    .expect("dict decompression"),
                None => ZstdBackend::decompress(frame, block.len()).expect("plain decompression"),
            };
            // Not `debug_assert_eq!`: the bench profile inherits `release` and
            // strips debug assertions, so the check would be absent from the
            // only build that ever runs this. The decompressed buffer has no
            // other use, and an unvalidated round-trip would report a read
            // throughput for work nothing proved correct.
            assert_eq!(out.len(), block.len(), "round-trip length mismatch");
        }
        let read = started.elapsed();

        Variant {
            bytes,
            write_mib_s: mib_s(raw, write),
            read_mib_s: mib_s(raw, read),
        }
    }

    /// Compressed size of every block at `level`, plain and with the
    /// dictionary.
    fn compare_at(blocks: &[Vec<u8>], dict: &ZstdDictionary, level: i32) -> (usize, usize, usize) {
        let raw = blocks.iter().map(Vec::len).sum();
        let plain = measure_variant(blocks, level, None).bytes;
        let with_dict = measure_variant(blocks, level, Some(dict)).bytes;
        (raw, plain, with_dict)
    }

    /// The same at the default level, for the sections that do not sweep.
    fn compare(blocks: &[Vec<u8>], dict: &ZstdDictionary) -> (usize, usize, usize) {
        compare_at(blocks, dict, LEVEL)
    }

    fn pct(from: usize, to: usize) -> f64 {
        if from == 0 {
            return 0.0;
        }
        (from as f64 - to as f64) / from as f64 * 100.0
    }

    /// Quantile `q` of `samples`, which must already be sorted ascending.
    fn quantile(samples: &[f64], q: f64) -> f64 {
        let Some(last) = samples.len().checked_sub(1) else {
            return 0.0;
        };
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "an index derived from a quantile in [0, 1] over a bounded sample count"
        )]
        let idx = (q * last as f64).round() as usize;
        samples.get(idx).copied().unwrap_or(0.0)
    }

    /// One formatted latency row in microseconds: mean, then the tail that
    /// actually bounds a per-output decision.
    fn latency_line(samples: &mut [f64]) -> String {
        let mean = if samples.is_empty() {
            0.0
        } else {
            samples.iter().sum::<f64>() / samples.len() as f64
        };
        samples.sort_unstable_by(f64::total_cmp);
        format!(
            "{:>9.1}us {:>9.1}us {:>9.1}us {:>9.1}us",
            mean,
            quantile(samples, 0.50),
            quantile(samples, 0.99),
            quantile(samples, 0.999),
        )
    }

    /// Writes the exact bytes this measurement used to `dir`, so the same
    /// corpus / dictionary / block can be run through another zstd
    /// implementation (`zstd -3 -D dict.bin block.bin`) and the two numbers
    /// compared. Enabled with `DICT_RATIO_DUMP=<dir>`.
    fn dump_artifacts(dir: &str) {
        std::fs::create_dir_all(dir).expect("dump directory");
        for w in WORKLOADS {
            let corpus: Vec<u8> = blocks(w, 0..20_000, 64 * 1024).concat();
            let (dict, _) = train(&corpus);
            let slug: String = w
                .name
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric())
                .collect();

            std::fs::write(format!("{dir}/{slug}.corpus.bin"), &corpus).expect("corpus");
            std::fs::write(format!("{dir}/{slug}.dict.bin"), dict.raw()).expect("dict");
            for block_size in [4 * 1024usize, 16 * 1024, 64 * 1024] {
                let block = blocks(w, 20_000..40_000, block_size)
                    .into_iter()
                    .next()
                    .expect("at least one block");
                std::fs::write(
                    format!("{dir}/{slug}.block{}k.bin", block_size / 1024),
                    &block,
                )
                .expect("block");
                // What THIS crate produces for the same bytes at every level,
                // so an external implementation has something to disagree with
                // per level rather than only at the default.
                for &level in LEVELS {
                    let plain = ZstdBackend::compress(&block, level).expect("plain");
                    let with_dict =
                        ZstdBackend::compress_with_dict(&block, level, dict.raw()).expect("dict");
                    println!(
                        "dump {slug} {}K L{level}: raw {} ours-plain {} ours-dict {}",
                        block_size / 1024,
                        block.len(),
                        plain.len(),
                        with_dict.len(),
                    );
                }
            }
        }
    }

    pub fn run() {
        if let Ok(dir) = std::env::var("DICT_RATIO_DUMP") {
            dump_artifacts(&dir);
            return;
        }

        println!("zstd level {LEVEL}, dictionary budget {DICT_SIZE} bytes\n");

        // The decision table: the two shapes a tree actually writes in. A
        // flush is on the write path and pays for compression immediately; a
        // compaction to a cold level is off it and can pay much more, once,
        // for bytes that then sit there being read.
        println!("=== 0. The two write modes, priced ===");
        println!(
            "{:<24} {:>5} {:>6} {:>5} {:>10} {:>8} {:>11} {:>11}",
            "workload", "block", "level", "dict", "bytes", "vs best", "write MiB/s", "read MiB/s",
        );
        for w in WORKLOADS {
            let corpus: Vec<u8> = blocks(w, 0..20_000, 64 * 1024).concat();
            let (dict, _) = train(&corpus);
            let slug = w.name.split_whitespace().next().unwrap_or(w.name);

            for block_size in [4 * 1024usize, 16 * 1024] {
                let measured = blocks(w, 20_000..40_000, block_size);

                // Candidates: the cheap no-dictionary levels a flush can
                // afford, and the expensive dictionary levels a cold-level
                // compaction can.
                let candidates: Vec<(i32, bool)> = vec![
                    (1, false),
                    (3, false),
                    (9, false),
                    (19, false),
                    (3, true),
                    (9, true),
                    (19, true),
                    (22, true),
                ];
                let mut rows = Vec::new();
                for (level, use_dict) in candidates {
                    let v = measure_variant(
                        &measured,
                        level,
                        if use_dict { Some(&dict) } else { None },
                    );
                    rows.push((level, use_dict, v));
                }
                let best = rows
                    .iter()
                    .map(|(_, _, v)| v.bytes)
                    .min()
                    .expect("at least one candidate");

                for (level, use_dict, v) in rows {
                    println!(
                        "{:<24} {:>4}K {:>6} {:>5} {:>10} {:>7.1}% {:>11.0} {:>11.0}",
                        slug,
                        block_size / 1024,
                        level,
                        if use_dict { "yes" } else { "no" },
                        v.bytes,
                        pct(best, v.bytes).abs(),
                        v.write_mib_s,
                        v.read_mib_s,
                    );
                }
            }
        }

        println!("\n=== 1. Ratio by workload, block size and level ===");
        println!(
            "{:<32} {:>6} {:>6} {:>11} {:>11} {:>9}",
            "workload", "block", "level", "zstd", "zstd+dict", "saved"
        );
        for w in WORKLOADS {
            // Train on an EARLIER generation than the one measured, so the
            // number is not the flattering "trained on exactly this data".
            let corpus: Vec<u8> = blocks(w, 0..20_000, 64 * 1024).concat();
            let (dict, _) = train(&corpus);

            for block_size in [4 * 1024usize, 16 * 1024, 64 * 1024] {
                let measured = blocks(w, 20_000..40_000, block_size);
                for &level in LEVELS {
                    let (_, plain, with_dict) = compare_at(&measured, &dict, level);
                    println!(
                        "{:<32} {:>5}K {:>6} {:>11} {:>11} {:>8.1}%",
                        w.name,
                        block_size / 1024,
                        level,
                        plain,
                        with_dict,
                        pct(plain, with_dict),
                    );
                }
            }
        }

        println!("\n=== 2. Training cost ===");
        println!("{:<32} {:>12} {:>12}", "corpus", "bytes", "train time");
        for w in WORKLOADS {
            for records in [20_000u64, 100_000] {
                let corpus: Vec<u8> = blocks(w, 0..records, 64 * 1024).concat();
                let (_, elapsed) = train(&corpus);
                println!(
                    "{:<32} {:>12} {:>10.1}ms",
                    w.name,
                    corpus.len(),
                    elapsed.as_secs_f64() * 1000.0,
                );
            }
        }

        println!("\n=== 3. Staleness: trained on generation 0, applied later ===");
        println!(
            "{:<32} {:>10} {:>10} {:>10}",
            "workload", "same gen", "10x later", "100x later"
        );
        for w in WORKLOADS {
            let corpus: Vec<u8> = blocks(w, 0..20_000, 64 * 1024).concat();
            let (dict, _) = train(&corpus);

            let mut saved = Vec::new();
            for start in [0u64, 200_000, 2_000_000] {
                let measured = blocks(w, start..(start + 20_000), 16 * 1024);
                let (_, plain, with_dict) = compare(&measured, &dict);
                saved.push(pct(plain, with_dict));
            }
            println!(
                "{:<32} {:>9.1}% {:>9.1}% {:>9.1}%",
                w.name, saved[0], saved[1], saved[2],
            );
        }

        println!("\n=== 4. The cost of asking (probe one 16K block both ways) ===");
        println!(
            "{:<32} {:>11} {:>11} {:>11} {:>11}",
            "workload / variant", "mean", "p50", "p99", "p999",
        );
        for w in WORKLOADS {
            let corpus: Vec<u8> = blocks(w, 0..20_000, 64 * 1024).concat();
            let (dict, _) = train(&corpus);
            let sample = blocks(w, 20_000..21_000, 16 * 1024)
                .into_iter()
                .next()
                .expect("at least one block");

            // Timed per probe, not as one batch mean: a compaction decides per
            // OUTPUT whether to probe, so what bounds that decision is the tail,
            // and a mean hides it.
            const PROBES: usize = 200;
            let _ = ZstdBackend::compress(&sample, LEVEL);
            let mut plain_us = Vec::with_capacity(PROBES);
            for _ in 0..PROBES {
                let started = Instant::now();
                let _ = ZstdBackend::compress(&sample, LEVEL).expect("probe");
                plain_us.push(started.elapsed().as_secs_f64() * 1e6);
            }

            let _ = ZstdBackend::compress_with_dict(&sample, LEVEL, dict.raw());
            let mut dict_us = Vec::with_capacity(PROBES);
            for _ in 0..PROBES {
                let started = Instant::now();
                let _ = ZstdBackend::compress_with_dict(&sample, LEVEL, dict.raw()).expect("probe");
                dict_us.push(started.elapsed().as_secs_f64() * 1e6);
            }

            println!("{:<32} {}", w.name, latency_line(&mut plain_us));
            println!("{:<32} {}", "  with dict", latency_line(&mut dict_us));
        }
    }
}

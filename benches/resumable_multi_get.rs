// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Multi-gets in flight per thread: the resumable read against the blocking
//! one, on `io_uring`, over a cold block cache.
//!
//! The tree is laid out as `io_uring_cold_multi_get`'s four-table level 0:
//! filters and indexes partitioned and unpinned, a block cache far smaller
//! than the tree and no row cache, so every batch reads filter partitions,
//! index partitions and data blocks. The OS page cache is left as the writes
//! warmed it, so the figures are the engine's cost and the concurrency it
//! can express, not a device's latency.
//!
//! Each thread runs its batches either blocking, one after another, or
//! resumable with `depth` of them in flight at once: every block read of
//! every read in flight goes to one ring queue per thread, the jobs (file
//! opens) run inline. Prints, per thread count and depth, the batches per
//! second per thread, the median and p99 time of a batch from its start to
//! its answer, and the mean number of block reads in flight on the ring when
//! the thread waits. Linux only.

#[cfg(target_os = "linux")]
fn run() -> Result<(), Box<dyn std::error::Error>> {
    use lsm_tree::config::PinningPolicy;
    use lsm_tree::fs::{Fs, IoUringFs, QueuedRead};
    use lsm_tree::resumable::{ReadTag, ResumableMultiGet, Step};
    use lsm_tree::{AbstractTree, AnyTree, Cache, Config, SeqNo, SequenceNumberCounter};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    const KEYS: u64 = 200_000;
    const TABLES: usize = 4;
    const BATCH: usize = 8;
    /// Batches per thread and configuration.
    const BATCHES: usize = 4_000;

    fn key(i: u64) -> String {
        format!("key{i:010}")
    }

    fn quantile(samples: &mut [Duration], q: f64) -> Duration {
        samples.sort_unstable();
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss,
            reason = "rank is clamped into 0..samples.len(), a bench-sized count"
        )]
        let rank = ((samples.len() as f64 * q).ceil() as usize).clamp(1, samples.len()) - 1;
        samples.get(rank).copied().unwrap_or_default()
    }

    fn batches(seed: u64) -> impl FnMut() -> Vec<String> {
        let mut state = seed | 1;
        move || {
            (0..BATCH)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    key(state % KEYS)
                })
                .collect()
        }
    }

    /// One thread's batches, blocking: their per-batch times.
    fn blocking(tree: &AnyTree, seed: u64) -> lsm_tree::Result<(Vec<Duration>, f64)> {
        let mut next = batches(seed);
        let mut wall = Vec::with_capacity(BATCHES);
        for _ in 0..BATCHES {
            let keys = next();
            let start = Instant::now();
            let found = tree.multi_get(&keys, SeqNo::MAX)?;
            wall.push(start.elapsed());
            assert!(found.iter().all(Option::is_some), "every key exists");
        }
        Ok((wall, 0.0))
    }

    /// One thread's batches, `depth` of them in flight: their per-batch times
    /// and the mean ring depth at each wait.
    fn resumable(
        tree: &AnyTree,
        fs: &Arc<dyn Fs>,
        seed: u64,
        depth: usize,
    ) -> lsm_tree::Result<(Vec<Duration>, f64)> {
        let mut next = batches(seed);
        let mut queue = fs.read_queue();
        let mut wall = Vec::with_capacity(BATCHES);
        // The reads in flight, with when each started.
        let mut slots: Vec<Option<(ResumableMultiGet, Instant)>> =
            (0..depth).map(|_| None).collect();
        // Which read, and which of its block reads, a ring tag stands for.
        let mut tags: Vec<(usize, ReadTag)> = Vec::new();
        let (mut started, mut waits, mut depth_sum) = (0usize, 0u64, 0u64);

        // Moves a read on: its jobs inline, its block reads onto the ring, and
        // back into its slot unless it answered.
        let settle = |slot: usize,
                      step: Step,
                      since: Instant,
                      slots: &mut Vec<Option<(ResumableMultiGet, Instant)>>,
                      tags: &mut Vec<(usize, ReadTag)>,
                      queue: &mut Box<dyn lsm_tree::fs::ReadQueue + '_>,
                      wall: &mut Vec<Duration>|
         -> lsm_tree::Result<()> {
            let mut step = step;
            loop {
                match step {
                    Step::Done(found) => {
                        wall.push(since.elapsed());
                        assert!(found?.iter().all(Option::is_some), "every key exists");
                        return Ok(());
                    }
                    Step::Pending(mut read) => {
                        let jobs = read.take_jobs();
                        let ran = !jobs.is_empty();
                        for job in jobs {
                            let outcome = job.run();
                            read.complete_job(outcome);
                        }
                        for block in read.take_reads() {
                            queue.submit(QueuedRead {
                                tag: tags.len(),
                                file: block.file,
                                offset: block.offset,
                                buf: block.buf,
                            });
                            tags.push((slot, block.tag));
                        }
                        if ran {
                            step = read.resume();
                            continue;
                        }
                        if let Some(entry) = slots.get_mut(slot) {
                            *entry = Some((read, since));
                        }
                        return Ok(());
                    }
                }
            }
        };

        while wall.len() < BATCHES {
            // Every free slot takes a new batch while batches remain.
            for slot in 0..depth {
                if started < BATCHES && slots.get(slot).is_some_and(Option::is_none) {
                    started += 1;
                    let since = Instant::now();
                    let step = tree.start_multi_get(next(), SeqNo::MAX)?;
                    settle(
                        slot, step, since, &mut slots, &mut tags, &mut queue, &mut wall,
                    )?;
                }
            }
            if wall.len() >= BATCHES {
                break;
            }
            waits += 1;
            depth_sum += u64::try_from(queue.outstanding()).unwrap_or(u64::MAX);
            let mut done = Vec::new();
            queue.wait(1, &mut |read| done.push(read));
            let mut touched: Vec<usize> = Vec::new();
            for read in done {
                let Some(&(slot, tag)) = tags.get(read.tag) else {
                    continue;
                };
                if let Some(Some((pending, _))) = slots.get_mut(slot) {
                    pending.complete_read(tag, read.result, read.buf);
                    touched.push(slot);
                }
            }
            touched.sort_unstable();
            touched.dedup();
            for slot in touched {
                if let Some((read, since)) = slots.get_mut(slot).and_then(Option::take) {
                    let step = read.resume();
                    settle(
                        slot, step, since, &mut slots, &mut tags, &mut queue, &mut wall,
                    )?;
                }
            }
        }
        #[expect(clippy::cast_precision_loss, reason = "a bench-sized mean")]
        let mean = depth_sum as f64 / waits.max(1) as f64;
        Ok((wall, mean))
    }

    let dir = tempfile::tempdir()?;
    let fs: Arc<dyn Fs> = Arc::new(IoUringFs::new()?);
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::clone(&fs))
    .with_runtime_config({
        let mut runtime = lsm_tree::runtime_config::RuntimeConfig::default();
        runtime.index_partition_spill_threshold = 0;
        runtime
    })
    .filter_block_partitioning_policy(PinningPolicy::all(true))
    .index_block_partitioning_policy(PinningPolicy::all(true))
    .filter_block_pinning_policy(PinningPolicy::all(false))
    .index_block_pinning_policy(PinningPolicy::all(false))
    .use_cache(Arc::new(
        Cache::with_capacity_bytes(256 * 1_024).with_row_cache(false),
    ))
    .open()?;
    let mut seqno = 0;
    for table in 0..TABLES {
        for i in (0..KEYS).skip(table).step_by(TABLES) {
            tree.insert(key(i), vec![0xABu8; 256], seqno);
            seqno += 1;
        }
        tree.flush_active_memtable(0)?;
    }

    for threads in [1usize, 4] {
        for depth in [0usize, 1, 8, 32] {
            let start = Instant::now();
            let results = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..threads)
                    .map(|thread| {
                        let (tree, fs) = (&tree, &fs);
                        let seed = 0x9E37_79B9_7F4A_7C15u64.wrapping_mul(thread as u64 + 1);
                        scope.spawn(move || {
                            if depth == 0 {
                                blocking(tree, seed)
                            } else {
                                resumable(tree, fs, seed, depth)
                            }
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|handle| {
                        handle
                            .join()
                            .unwrap_or_else(|_| panic!("a bench thread panicked"))
                    })
                    .collect::<Result<Vec<_>, _>>()
            })?;
            let elapsed = start.elapsed();
            let mut wall: Vec<Duration> = results
                .iter()
                .flat_map(|(w, _)| w.iter().copied())
                .collect();
            #[expect(clippy::cast_precision_loss, reason = "a bench-sized mean")]
            let ring = results.iter().map(|(_, ring)| ring).sum::<f64>() / threads as f64;
            #[expect(clippy::cast_precision_loss, reason = "a bench-sized rate")]
            let rate = BATCHES as f64 / elapsed.as_secs_f64();
            let mode = if depth == 0 {
                "blocking".to_string()
            } else {
                format!("resumable, {depth} in flight")
            };
            println!(
                "{threads} threads, {mode}: {rate:.0} batches/s per thread; \
                 batch p50 {:?} p99 {:?} p999 {:?}; ring depth at wait {ring:.1}",
                quantile(&mut wall, 0.5),
                quantile(&mut wall, 0.99),
                quantile(&mut wall, 0.999),
            );
        }
    }
    Ok(())
}

fn main() {
    #[cfg(target_os = "linux")]
    if let Err(error) = run() {
        eprintln!("resumable_multi_get failed: {error}");
        std::process::exit(1);
    }
    #[cfg(not(target_os = "linux"))]
    println!("io_uring is Linux only; nothing to measure here");
}

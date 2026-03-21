use crate::config::BenchConfig;
use crate::db::{make_random_key, make_sequential_key, make_value, prefill_sequential, read_seqno};
use crate::reporter::Reporter;
use crate::workloads::Workload;
use lsm_tree::{AbstractTree, AnyTree};
use rand::Rng;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Barrier;
use std::time::Instant;

pub struct ReadWhileWriting;

impl Workload for ReadWhileWriting {
    fn run(
        &self,
        tree: &AnyTree,
        config: &BenchConfig,
        seqno: &AtomicU64,
        reporter: &mut Reporter,
    ) -> lsm_tree::Result<()> {
        // Prefill the tree with sequential keys.
        prefill_sequential(tree, config, seqno)?;

        let threads = config.threads.max(2);
        let reader_count = threads - 1;
        // Distribute ops across readers, giving remainder to the last reader.
        let base_ops = config.num / reader_count as u64;
        let remainder = config.num % reader_count as u64;
        let barrier = Barrier::new(threads);

        reporter.start();

        std::thread::scope(|s| {
            // Spawn reader threads — borrow barrier by reference.
            let reader_handles: Vec<_> = (0..reader_count)
                .enumerate()
                .map(|(i, _)| {
                    let my_ops = base_ops + if (i as u64) < remainder { 1 } else { 0 };
                    let barrier = &barrier;
                    s.spawn(move || {
                        let mut local_reporter = Reporter::new();
                        let mut rng = rand::rng();
                        barrier.wait();

                        for _ in 0..my_ops {
                            let read_seq = read_seqno(seqno);
                            let idx: u64 = rng.random_range(0..config.num);
                            let key = make_sequential_key(idx, config.key_size);

                            let t = Instant::now();
                            if let Err(e) = tree.get(&key, read_seq) {
                                eprintln!("read error: {e}");
                            }
                            local_reporter.record_duration(t.elapsed());
                        }

                        local_reporter
                    })
                })
                .collect();

            // Writer thread — also borrows barrier by reference.
            let writer_handle = s.spawn(|| {
                barrier.wait();

                // Writer runs for the full duration (same total as readers).
                for _ in 0..config.num {
                    let key = make_random_key(config.key_size);
                    let value = make_value(config.value_size);
                    let seq = seqno.fetch_add(1, Ordering::Relaxed);
                    tree.insert(key, value, seq);
                }
            });

            // Collect reader results.
            for handle in reader_handles {
                let local_reporter = handle.join().expect("reader thread panicked");
                reporter.merge(&local_reporter);
            }

            writer_handle.join().expect("writer thread panicked");
        });

        reporter.stop();

        Ok(())
    }
}

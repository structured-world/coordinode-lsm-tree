use crate::config::BenchConfig;
use crate::db::{fill_random_key, make_value};
use crate::reporter::Reporter;
use crate::workloads::{Workload, run_threaded};
use lsm_tree::{AbstractTree, AnyTree};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

pub struct FillRandom;

impl Workload for FillRandom {
    fn run(
        &self,
        tree: &AnyTree,
        config: &BenchConfig,
        seqno: &AtomicU64,
        reporter: &mut Reporter,
    ) -> lsm_tree::Result<()> {
        // All threads insert random keys — memtable contention is intentional.
        run_threaded(config, reporter, |_t, my_ops, _start| {
            let mut local = Reporter::new();
            // One key buffer and one value per thread: the engine copies what
            // it keeps, so per-op `Vec`s would only add harness overhead.
            let mut key = vec![0u8; config.key_size];
            let value = make_value(config.value_size);

            for _ in 0..my_ops {
                // Key generation is outside the timed region (before Instant::now).
                fill_random_key(&mut key);
                let seq = seqno.fetch_add(1, Ordering::Relaxed);

                let t = Instant::now();
                tree.insert(&key[..], &value[..], seq);
                local.record_duration(t.elapsed());
            }

            Ok(local)
        })?;

        reporter.stop();
        Ok(())
    }
}

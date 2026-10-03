use crate::config::BenchConfig;
use crate::db::{ValuePool, fill_sequential_key};
use crate::reporter::Reporter;
use crate::workloads::{Workload, run_threaded};
use lsm_tree::{AbstractTree, AnyTree};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

pub struct FillSeq;

impl Workload for FillSeq {
    fn run(
        &self,
        tree: &AnyTree,
        config: &BenchConfig,
        seqno: &AtomicU64,
        reporter: &mut Reporter,
    ) -> lsm_tree::Result<()> {
        let values = ValuePool::new(config.value_size);
        // Each thread fills its own key range partition:
        // thread t writes keys [start, start + my_ops).
        run_threaded(config, reporter, |_t, my_ops, start| {
            let mut local = Reporter::new();
            // One key buffer per thread and the shared value pool (as RocksDB's
            // db_bench does): the engine copies what it keeps, so per-op `Vec`s
            // would charge two heap round-trips of harness overhead to every
            // insert.
            let mut key = vec![0u8; config.key_size];

            for i in start..(start + my_ops) {
                fill_sequential_key(&mut key, i);
                let seq = seqno.fetch_add(1, Ordering::Relaxed);

                let t = Instant::now();
                tree.insert(&key[..], values.value(i), seq);
                local.record_duration(t.elapsed());
            }

            Ok(local)
        })?;

        reporter.stop();
        Ok(())
    }
}

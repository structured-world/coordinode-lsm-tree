use crate::config::BenchConfig;
use crate::db::{prefill_sequential, read_seqno};
use crate::reporter::Reporter;
use crate::workloads::Workload;
use lsm_tree::{AbstractTree, AnyTree};
use rand::Rng;
use std::sync::atomic::AtomicU64;
use std::time::Instant;

pub struct ReadRandom;

impl Workload for ReadRandom {
    fn run(
        &self,
        tree: &AnyTree,
        config: &BenchConfig,
        seqno: &AtomicU64,
        reporter: &mut Reporter,
    ) -> lsm_tree::Result<()> {
        // Prefill the tree with sequential keys.
        prefill_sequential(tree, config, seqno)?;

        let read_seq = read_seqno(seqno);
        let mut rng = rand::rng();
        let mut found = 0u64;

        // Key is generated before Instant::now() so allocation cost is
        // excluded from the timed region. A reusable buffer avoids per-op
        // heap allocation without pre-generating all keys upfront.
        let mut key_buf = vec![0u8; config.key_size];

        reporter.start();

        for _ in 0..config.num {
            let idx: u64 = rng.random_range(0..config.num);
            // Write index into reusable buffer (trailing bytes for small keys).
            let be = idx.to_be_bytes();
            if config.key_size >= 8 {
                key_buf[..8].copy_from_slice(&be);
            } else {
                key_buf.copy_from_slice(&be[8 - config.key_size..]);
            }

            let t = Instant::now();
            let result = tree.get(&key_buf, read_seq)?;
            reporter.record_duration(t.elapsed());

            if result.is_some() {
                found += 1;
            }
        }

        reporter.stop();

        if config.num > 0 {
            let hit_rate = found as f64 / config.num as f64 * 100.0;
            eprintln!("Hit rate: {found}/{} ({hit_rate:.1}%)", config.num);
        }

        Ok(())
    }
}

use crate::config::BenchConfig;
use crate::db::{prefill_prefix_keys, read_seqno};
use crate::reporter::Reporter;
use crate::workloads::Workload;
use lsm_tree::{AbstractTree, AnyTree, Guard};
use rand::Rng;
use std::sync::atomic::AtomicU64;
use std::time::Instant;

pub struct PrefixScan;

const NUM_PREFIXES: u16 = 256;
const SCAN_LIMIT: usize = 10;

impl Workload for PrefixScan {
    fn run(
        &self,
        tree: &AnyTree,
        config: &BenchConfig,
        seqno: &AtomicU64,
        reporter: &mut Reporter,
    ) -> lsm_tree::Result<()> {
        // Prefix keys use 2-byte prefix + 2-byte suffix = 4 bytes minimum.
        assert!(
            config.key_size >= 4,
            "prefixscan requires --key-size >= 4 (2-byte prefix + 2-byte suffix)"
        );

        // Prefill with structured prefix keys.
        prefill_prefix_keys(tree, config, seqno, NUM_PREFIXES)?;

        let read_seq = read_seqno(seqno);
        let mut rng = rand::rng();

        reporter.start();

        for _ in 0..config.num {
            let prefix_idx: u16 = rng.random_range(0..NUM_PREFIXES);
            let prefix_bytes = prefix_idx.to_be_bytes();

            let t = Instant::now();
            let mut iter = tree.prefix(prefix_bytes, read_seq, None);
            for _ in 0..SCAN_LIMIT {
                let Some(item) = iter.next() else { break };
                // Force value materialization so the benchmark reflects
                // actual read I/O, especially with --use-blob-tree.
                let _ = item.size()?;
            }
            reporter.record_duration(t.elapsed());
        }

        reporter.stop();
        Ok(())
    }
}

use crate::config::BenchConfig;
use crate::db::{prefill_sequential, read_seqno};
use crate::reporter::Reporter;
use crate::workloads::Workload;
use lsm_tree::{AbstractTree, AnyTree, Guard};
use std::sync::atomic::AtomicU64;
use std::time::Instant;

pub struct ReadSeq;

impl Workload for ReadSeq {
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
        let mut count = 0u64;

        reporter.start();

        let mut iter = tree.iter(read_seq, None);
        loop {
            let t = Instant::now();
            match iter.next() {
                Some(item) => {
                    // Force value materialization so read throughput reflects
                    // actual I/O, especially with --use-blob-tree.
                    let _size = item.size()?;
                    reporter.record_duration(t.elapsed());

                    count += 1;
                    if count >= config.num {
                        break;
                    }
                }
                None => {
                    break;
                }
            }
        }

        reporter.stop();

        eprintln!("Scanned {count} entries");

        Ok(())
    }
}

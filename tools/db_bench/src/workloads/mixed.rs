use crate::config::{BenchConfig, Compression, create_tree};
use crate::db::{ValuePool, fill_sequential_key};
use crate::reporter::Reporter;
use crate::workloads::Workload;
use lsm_tree::{AbstractTree, AnyTree};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// A whole write / rewrite / delete / compact / read cycle at maximum
/// compression, timed end to end.
///
/// The other workloads each isolate one operation against a tree that was
/// prepared for it. This one is deliberately the opposite: it puts a tree
/// through the sequence a real deployment actually walks, where the interesting
/// costs are the ones that only appear when the stages meet. A compaction has
/// to decode blocks that a flush wrote and re-encode them; a read afterwards
/// goes to files no flush produced. A change that is neutral on each stage
/// alone can still move this number.
///
/// zstd level 22 is pinned here rather than taken from `--compression`, so the
/// series stays comparable across dashboard runs and always exercises the
/// codec-heavy end. That is also where the merge pays most: the blocks it
/// rewrites are re-compressed at that level, not merely copied.
pub struct Mixed;

/// The workload's own name, so the reporting path can ask about it without
/// hardcoding the string in two places. It says what the series is: the
/// whole lifecycle, at zstd-22.
pub const NAME: &str = "lifecycle-zstd22";

/// The codec a given workload actually runs with.
///
/// Everything reads `--compression`; `lifecycle-zstd22` overrides it. The reporting path
/// asks here so the JSON report names the codec that ran rather than the one on
/// the command line, which would otherwise be a quiet mislabel in exactly the
/// series that is meant to be comparable across runs.
#[must_use]
pub fn effective_compression(benchmark_name: &str, requested: Compression) -> Compression {
    if benchmark_name == NAME {
        Compression::Zstd22
    } else {
        requested
    }
}

impl Workload for Mixed {
    fn run(
        &self,
        _tree: &AnyTree,
        config: &BenchConfig,
        seqno: &AtomicU64,
        reporter: &mut Reporter,
    ) -> lsm_tree::Result<()> {
        // This workload states, per key, whether it expects a value or a
        // tombstone, and asserts it. That contract needs one key per index.
        // `fill_sequential_key` truncates the index to the key width, so a key
        // smaller than 8 bytes aliases indices past its range: two indices then
        // share a key, the later write wins, and an index-keyed expectation is
        // simply wrong rather than violated. Say so instead of failing an
        // assertion that would look like an engine fault.
        if config.key_size < 8 {
            let distinct = 1_u64 << (config.key_size * 8);
            if config.num > distinct {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "the lifecycle-zstd22 workload needs one key per index, but --key-size {} encodes \
                         only {} distinct keys for --num {}; raise --key-size or lower --num",
                        config.key_size, distinct, config.num,
                    ),
                )
                .into());
            }
        }

        // The harness hands in a tree built from --compression, which would
        // make this series mean a different thing on every invocation. Only the
        // codec is overridden, though: the tree is otherwise built from the same
        // BenchConfig as every other workload, so --cache-mb, --block-size,
        // --use-blob-tree and the metadata-partitioning switches still apply and
        // the run header keeps describing the tree that actually ran.
        let dir = tempfile::tempdir()?;
        let mut pinned = config.clone();
        pinned.compression = effective_compression(NAME, config.compression);
        let tree = create_tree(dir.path(), &pinned)?;

        let mut key = vec![0u8; config.key_size];
        let values = ValuePool::new(config.value_size);
        let n = config.num;

        // The tree is opened outside the timer: the cycle is what is being
        // measured, not the cost of creating an empty directory. Everything
        // from the first insert to the last read is inside it, which is what
        // makes the reported ops/sec cover the compaction too.
        reporter.start();

        // Stage 1: every key, then flush. One SST.
        for idx in 0..n {
            fill_sequential_key(&mut key, idx);
            let seq = seqno.fetch_add(1, Ordering::Relaxed);
            let t = Instant::now();
            tree.insert(&key[..], values.value(idx), seq);
            reporter.record_duration(t.elapsed());
        }
        tree.flush_active_memtable(0)?;

        // Stage 2: rewrite every third key and delete every fifth, then flush.
        // The second SST now disagrees with the first about those keys, which
        // is what gives the merge in stage 4 real version resolution to do.
        // A rewrite carries a different value than the key's first one, as a
        // real update does: the pool's next slice after the first pass.
        for idx in (0..n).step_by(3) {
            fill_sequential_key(&mut key, idx);
            let seq = seqno.fetch_add(1, Ordering::Relaxed);
            let t = Instant::now();
            tree.insert(&key[..], values.value(n + idx), seq);
            reporter.record_duration(t.elapsed());
        }
        for idx in (0..n).step_by(5) {
            fill_sequential_key(&mut key, idx);
            let seq = seqno.fetch_add(1, Ordering::Relaxed);
            let t = Instant::now();
            tree.remove(&key[..], seq);
            reporter.record_duration(t.elapsed());
        }
        tree.flush_active_memtable(0)?;

        // Stage 3: a third SST, overlapping both.
        for idx in (0..n).step_by(7) {
            fill_sequential_key(&mut key, idx);
            let seq = seqno.fetch_add(1, Ordering::Relaxed);
            let t = Instant::now();
            tree.insert(&key[..], values.value(2 * n + idx), seq);
            reporter.record_duration(t.elapsed());
        }
        tree.flush_active_memtable(0)?;

        // Stage 4: the merge. Timed as one operation because that is what it
        // is from a deployment's point of view, a single stall whose cost the
        // per-key stages cannot show.
        let t = Instant::now();
        tree.major_compact(u64::MAX, 0)?;
        reporter.record_duration(t.elapsed());

        // Stage 5: read every key back off the compacted files. Deleted keys
        // are read too: resolving a tombstone is work, and skipping those reads
        // would quietly drop a fifth of the keyspace from the measurement.
        for idx in 0..n {
            fill_sequential_key(&mut key, idx);
            let t = Instant::now();
            let got = tree.get(&key[..], lsm_tree::MAX_SEQNO)?;
            reporter.record_duration(t.elapsed());
            // Shape check so a build that silently stopped resolving versions
            // cannot post a fast number. Stage 2 deleted every fifth key, but
            // stage 3 ran afterwards and re-inserted every seventh, so a key
            // divisible by both is back. Last write wins.
            //
            // A plain assert, not a debug one: the dashboard builds with
            // --release, which is exactly where an unguarded number would be
            // published. It sits after the duration is recorded, so it is not
            // part of what the series measures.
            let deleted = idx % 5 == 0 && idx % 7 != 0;
            assert_eq!(got.is_none(), deleted, "key {idx}: unexpected visibility");
        }

        reporter.stop();
        Ok(())
    }
}

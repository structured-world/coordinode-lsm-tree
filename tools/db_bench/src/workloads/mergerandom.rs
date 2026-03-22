use crate::config::BenchConfig;
use crate::db::make_sequential_key;
use crate::reporter::Reporter;
use crate::workloads::Workload;
use lsm_tree::{
    config::{BlockSizePolicy, CompressionPolicy},
    AbstractTree, AnyTree, Cache, Config, MergeOperator, SequenceNumberCounter, UserValue,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Counter merge operator: sums i64 operands.
/// Used by the `mergerandom` benchmark to exercise real merge resolution.
struct CounterMerge;

impl MergeOperator for CounterMerge {
    fn merge(
        &self,
        _key: &[u8],
        base_value: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> lsm_tree::Result<UserValue> {
        let mut counter: i64 = match base_value {
            Some(bytes) if bytes.len() == 8 => {
                i64::from_le_bytes(bytes.try_into().expect("checked length"))
            }
            Some(_) => return Err(lsm_tree::Error::MergeOperator),
            None => 0,
        };

        for operand in operands {
            if operand.len() != 8 {
                return Err(lsm_tree::Error::MergeOperator);
            }
            counter += i64::from_le_bytes((*operand).try_into().expect("checked length"));
        }

        Ok(counter.to_le_bytes().to_vec().into())
    }
}

/// Writes merge operands to a small set of "hot" keys, flushing periodically
/// to create overlapping SSTs. Exercises the full merge path: operand storage,
/// lazy resolution during reads, and merge-aware compaction.
///
/// This is the lsm-tree equivalent of RocksDB's `mergerandom` benchmark.
pub struct MergeRandom;

impl Workload for MergeRandom {
    fn run(
        &self,
        _tree: &AnyTree,
        config: &BenchConfig,
        seqno: &AtomicU64,
        reporter: &mut Reporter,
    ) -> lsm_tree::Result<()> {
        let hot_keys: u64 = 1024;
        let flush_interval: u64 = 5_000;

        if config.key_size < 2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "mergerandom requires --key-size >= 2 to preserve hot-key distinctness",
            )
            .into());
        }

        // Create a dedicated tree with a merge operator — the shared tree
        // from main doesn't have one configured.
        let tmpdir =
            tempfile::tempdir().map_err(|e| std::io::Error::other(format!("tmpdir: {e}")))?;
        let cache_bytes = config.cache_mb.checked_mul(1024 * 1024).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "cache size overflows u64")
        })?;
        let cache = Arc::new(Cache::with_capacity_bytes(cache_bytes));
        let tree = Config::new(
            tmpdir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .data_block_size_policy(BlockSizePolicy::all(config.block_size))
        .data_block_compression_policy(CompressionPolicy::all(config.compression.to_lsm()))
        .use_cache(cache)
        .with_merge_operator(Some(Arc::new(CounterMerge)))
        .open()?;

        reporter.start();

        for i in 0..config.num {
            let key_idx = i % hot_keys;
            let key = make_sequential_key(key_idx, config.key_size);
            // Each operand adds 1 to the counter for this key.
            let operand = 1_i64.to_le_bytes();
            let seq = seqno.fetch_add(1, Ordering::Relaxed);

            let t = Instant::now();
            tree.merge(key, operand.as_slice(), seq);
            reporter.record_duration(t.elapsed());

            if (i + 1) % flush_interval == 0 {
                tree.flush_active_memtable(0)?;
            }
        }

        // Final flush + compaction to exercise merge resolution.
        tree.flush_active_memtable(0)?;
        let compact_seqno = seqno.load(Ordering::Relaxed);
        tree.major_compact(64 * 1024 * 1024, compact_seqno)?;

        reporter.stop();

        // Verify: each hot key should have counter = num / hot_keys.
        let expected = (config.num / hot_keys) as i64;
        let read_seqno = seqno.load(Ordering::Relaxed);
        let sample_key = make_sequential_key(0, config.key_size);
        if let Some(val) = tree.get(&sample_key, read_seqno)? {
            let actual = if val.len() >= 8 {
                i64::from_le_bytes(val[..8].try_into().expect("checked length"))
            } else {
                eprintln!("Warning: merge result too short ({} bytes)", val.len());
                0
            };
            eprintln!(
                "Merged {} operands over {} hot keys, sample counter: {actual} (expected {expected}), {} tables",
                config.num, hot_keys, tree.table_count(),
            );
        }

        Ok(())
    }
}

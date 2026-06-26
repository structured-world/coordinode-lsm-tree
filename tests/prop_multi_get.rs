// Property test: the batched multi_get path must agree with a per-key get loop.
//
// multi_get resolves a whole batch through per-table batched gets plus a
// concurrent block prewarm; it changes only HOW data blocks are fetched and
// decoded for a batch, never WHICH version is visible. So for any tree state and
// any query batch, multi_get must return exactly what calling get on each key
// returns. The per-key get path is the authoritative oracle (long-standing,
// separately tested); this fuzzes the batch path against it across random
// inserts, deletes, flushes, and compactions that spread data over the active
// memtable, L0, and deeper levels (the cross-SST case the prewarm targets).

mod common;

use lsm_tree::{AbstractTree, Cache, Config, SeqNo, SequenceNumberCounter};
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use std::sync::Arc;

/// Keys inserted live in `0..KEY_SPACE`; queries also reach a little past it so
/// some queried keys are guaranteed absent (exercising bloom-skip / no-covering
/// block in both the batch and the prewarm planner).
const KEY_SPACE: u16 = 64;

#[derive(Debug, Clone)]
enum Op {
    Insert { key_idx: u16, value: u8 },
    Remove { key_idx: u16 },
    Flush,
    Compact,
}

fn key_from_idx(idx: u16) -> String {
    format!("k{idx:04}")
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        // Weighted toward writes so most cases build real multi-block SSTs before
        // a flush/compact rather than churning empty trees.
        5 => (0..KEY_SPACE, any::<u8>())
            .prop_map(|(key_idx, value)| Op::Insert { key_idx, value }),
        2 => (0..KEY_SPACE).prop_map(|key_idx| Op::Remove { key_idx }),
        1 => Just(Op::Flush),
        1 => Just(Op::Compact),
    ]
}

fn run_test(ops: Vec<Op>, queried: Vec<u16>) -> Result<(), TestCaseError> {
    run_test_with_cache(ops, queried, None, 6)
}

/// `cache_bytes = Some(small)` shrinks the shared block cache so a level's cold
/// working set exceeds the half-cache bound, routing the resolve through the
/// chunked read-into-scratch path instead of the prewarm + serial path. `None`
/// uses the default cache (the prewarm path). `value_len` sizes each stored
/// value: larger values fill multiple data blocks per SST (needed to make a
/// level oversize against a tiny cache, since the chunked path engages only at
/// >= 2 cold blocks).
fn run_test_with_cache(
    ops: Vec<Op>,
    queried: Vec<u16>,
    cache_bytes: Option<u64>,
    value_len: usize,
) -> Result<(), TestCaseError> {
    let tmpdir = lsm_tree::get_tmp_folder();
    let mut config = Config::new(
        &tmpdir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    );
    if let Some(bytes) = cache_bytes {
        config = config.use_cache(Arc::new(Cache::with_capacity_bytes(bytes)));
    }
    let tree = config
        .open()
        .map_err(|e| TestCaseError::fail(format!("open: {e}")))?;

    let mut seqno: u64 = 1;
    for op in &ops {
        match op {
            Op::Insert { key_idx, value } => {
                tree.insert(key_from_idx(*key_idx), vec![*value; value_len], seqno);
                seqno += 1;
            }
            Op::Remove { key_idx } => {
                tree.remove(key_from_idx(*key_idx), seqno);
                seqno += 1;
            }
            Op::Flush => {
                tree.flush_active_memtable(0)
                    .map_err(|e| TestCaseError::fail(format!("flush: {e}")))?;
            }
            Op::Compact => {
                tree.major_compact(common::COMPACTION_TARGET, seqno)
                    .map_err(|e| TestCaseError::fail(format!("compact: {e}")))?;
            }
        }
    }

    // Read at MAX so every applied version is visible; the batch path and the
    // per-key path observe the same snapshot.
    let read_seqno = SeqNo::MAX;
    let query_keys: Vec<String> = queried.iter().map(|&i| key_from_idx(i)).collect();

    let batched = tree
        .multi_get(&query_keys, read_seqno)
        .map_err(|e| TestCaseError::fail(format!("multi_get: {e}")))?;

    prop_assert_eq!(
        batched.len(),
        query_keys.len(),
        "multi_get must return one result per input key"
    );

    // Per input position (duplicates and absent keys included), the batch result
    // must equal the single-key get for the same key at the same seqno.
    for (i, key) in query_keys.iter().enumerate() {
        let single = tree
            .get(key, read_seqno)
            .map_err(|e| TestCaseError::fail(format!("get {key}: {e}")))?;
        prop_assert_eq!(
            batched[i].as_ref().map(|v| v.to_vec()),
            single.as_ref().map(|v| v.to_vec()),
            "multi_get vs get mismatch for key {}",
            key,
        );
    }

    Ok(())
}

proptest! {
    // Defaults: 32 cases. Override at run time via PROPTEST_CASES, see
    // `common::proptest_config`.
    #![proptest_config(common::proptest_config())]

    #[test]
    fn prop_multi_get_matches_per_key_get(
        ops in prop::collection::vec(op_strategy(), 1..120),
        queried in prop::collection::vec(0u16..(KEY_SPACE + 8), 0..48),
    ) {
        run_test(ops, queried)?;
    }

    // Same fuzz, but ~256-byte values fill several data blocks per SST and a tiny
    // 4 KiB shared cache makes any multi-block level oversize, so the resolve goes
    // through the chunked read-into-scratch path. The oracle is identical:
    // multi_get must still equal per-key get across all the same MVCC states
    // (tombstones, merges, flushes, compactions, multi-level shadowing). This is
    // the correctness net for the chunked gather + accumulate.
    #[test]
    fn prop_multi_get_chunked_matches_per_key_get(
        ops in prop::collection::vec(op_strategy(), 1..120),
        queried in prop::collection::vec(0u16..(KEY_SPACE + 8), 0..48),
    ) {
        run_test_with_cache(ops, queried, Some(4096), 256)?;
    }
}

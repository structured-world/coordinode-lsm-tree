// Property-based model test: compare lsm-tree against BTreeMap oracle.
//
// The oracle models MVCC using (key, Reverse(seqno)) ordering, where
// None values represent tombstones. This oracle only models point
// tombstones; range tombstones are tested separately in prop_range_tombstone.rs.

mod common;

use common::guard_to_kv;
use lsm_tree::{
    AbstractTree, AnyTree, Config, MergeOperator, ScanSinceEvent, SequenceNumberCounter, Tree,
    UserValue,
};
use proptest::prelude::*;
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Commutative merge: the lexicographic max of the base and all operands. Being
/// order-independent, the oracle can model it without tracking operand order.
struct MaxMerge;

impl MergeOperator for MaxMerge {
    fn merge(
        &self,
        _key: &[u8],
        base: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> lsm_tree::Result<UserValue> {
        let mut best: &[u8] = base.unwrap_or(&[]);
        for op in operands {
            if *op > best {
                best = op;
            }
        }
        Ok(UserValue::from(best))
    }
}

// ---------------------------------------------------------------------------
// Oracle
// ---------------------------------------------------------------------------

type VersionedKey = (Vec<u8>, Reverse<u64>);

/// A point write recorded in the oracle: a value, a tombstone, or a merge
/// operand folded onto the base via the merge operator.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Entry {
    Value(Vec<u8>),
    Tombstone,
    Merge(Vec<u8>),
}

/// One change-data-capture event, the comparable form of a `ScanSinceEvent`.
#[derive(Debug, PartialEq, Eq)]
enum CdcEvent {
    /// A point write (put when `value` is `Some`, tombstone when `None`).
    Point {
        seqno: u64,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    },
    /// A range tombstone over `[start, end)`.
    Range {
        seqno: u64,
        start: Vec<u8>,
        end: Vec<u8>,
    },
    /// A merge operand for `key`.
    Merge {
        seqno: u64,
        key: Vec<u8>,
        operand: Vec<u8>,
    },
}

impl CdcEvent {
    fn seqno(&self) -> u64 {
        match self {
            Self::Point { seqno, .. } | Self::Range { seqno, .. } | Self::Merge { seqno, .. } => {
                *seqno
            }
        }
    }
}

/// MVCC oracle covering point writes and range tombstones.
#[derive(Debug, Clone)]
struct Oracle {
    /// (key, Reverse(seqno)) -> the point write at that version.
    data: BTreeMap<VersionedKey, Entry>,
    /// Range tombstones as `(start, end, seqno)` over the half-open `[start, end)`.
    range_tombstones: Vec<(Vec<u8>, Vec<u8>, u64)>,
}

impl Oracle {
    fn new() -> Self {
        Self {
            data: BTreeMap::new(),
            range_tombstones: Vec::new(),
        }
    }

    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>, seqno: u64) {
        self.data.insert((key, Reverse(seqno)), Entry::Value(value));
    }

    fn remove(&mut self, key: Vec<u8>, seqno: u64) {
        self.data.insert((key, Reverse(seqno)), Entry::Tombstone);
    }

    fn merge(&mut self, key: Vec<u8>, operand: Vec<u8>, seqno: u64) {
        self.data
            .insert((key, Reverse(seqno)), Entry::Merge(operand));
    }

    fn delete_range(&mut self, start: Vec<u8>, end: Vec<u8>, seqno: u64) {
        self.range_tombstones.push((start, end, seqno));
    }

    /// Resolves the visible value of `key` at `read_seqno`.
    ///
    /// The merge chain is folded over POINT writes only (descending until the
    /// first value or point tombstone base, gathering merge operands above it),
    /// taking the lexicographic max of the base and operands, which is
    /// commutative (matching [`MaxMerge`]) so operand order is irrelevant. A
    /// covering range tombstone is then applied as a final step: it deletes the
    /// key only when it is newer than every point write, since a later point
    /// write (value or merge) overrides it. (A range tombstone does not break the
    /// merge chain, matching the engine.)
    fn resolve(&self, key: &[u8], read_seqno: u64) -> Option<Vec<u8>> {
        if read_seqno == 0 {
            return None;
        }
        let lo = (key.to_vec(), Reverse(read_seqno - 1));
        let hi = (key.to_vec(), Reverse(0));
        let mut operands: Vec<Vec<u8>> = Vec::new();
        let mut base: Option<Vec<u8>> = None;
        let mut latest_point_seqno: Option<u64> = None;
        for ((_, Reverse(seqno)), entry) in
            self.data.range(lo..=hi).take_while(|((k, _), _)| k == key)
        {
            latest_point_seqno.get_or_insert(*seqno);
            match entry {
                Entry::Merge(op) => operands.push(op.clone()),
                Entry::Value(v) => {
                    base = Some(v.clone());
                    break;
                }
                Entry::Tombstone => {
                    base = None;
                    break;
                }
            }
        }
        let merged = if operands.is_empty() {
            base
        } else {
            let mut best: &[u8] = base.as_deref().unwrap_or(&[]);
            for op in &operands {
                if op.as_slice() > best {
                    best = op;
                }
            }
            Some(best.to_vec())
        };
        // A covering range tombstone newer than the newest point write deletes
        // the key; an older one is overridden by that later point write.
        let range_seqno = self
            .range_tombstones
            .iter()
            .filter(|(start, end, seqno)| {
                *seqno < read_seqno && key >= start.as_slice() && key < end.as_slice()
            })
            .map(|(_, _, seqno)| *seqno)
            .max();
        if let (Some(range_seqno), Some(point_seqno)) = (range_seqno, latest_point_seqno)
            && range_seqno > point_seqno
        {
            return None;
        }
        merged
    }

    /// Point read: the visible value of `key` at `read_seqno`.
    fn get(&self, key: &[u8], read_seqno: u64) -> Option<Vec<u8>> {
        self.resolve(key, read_seqno)
    }

    /// Full scan: every visible `(key, value)` at `read_seqno`, sorted by key.
    ///
    /// Only keys with a point write are considered: a key that was solely a
    /// range-tombstone target has no value (range tombstones never create one),
    /// which matches the engine, so iterating `data` keys is complete.
    fn scan(&self, read_seqno: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut result = Vec::new();
        let mut last_key: Option<&Vec<u8>> = None;
        for (key, Reverse(_)) in self.data.keys() {
            if last_key == Some(key) {
                continue;
            }
            last_key = Some(key);
            if let Some(value) = self.resolve(key, read_seqno) {
                result.push((key.clone(), value));
            }
        }
        result
    }

    /// Prefix scan: visible entries with `prefix` at `read_seqno`.
    fn prefix_scan(&self, prefix: &[u8], read_seqno: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.scan(read_seqno)
            .into_iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .collect()
    }

    /// Change-data-capture: every write at `seqno >= target`, un-collapsed (a key
    /// written N times yields N events) and ordered by ascending seqno, as
    /// [`CdcEvent`]s. This mirrors `Tree::scan_since_seqno`, whose lower bound is
    /// inclusive.
    fn scan_since(&self, target: u64) -> Vec<CdcEvent> {
        let mut events: Vec<CdcEvent> = self
            .data
            .iter()
            .filter(|((_, Reverse(seqno)), _)| *seqno >= target)
            .map(|((key, Reverse(seqno)), entry)| match entry {
                Entry::Value(v) => CdcEvent::Point {
                    seqno: *seqno,
                    key: key.clone(),
                    value: Some(v.clone()),
                },
                Entry::Tombstone => CdcEvent::Point {
                    seqno: *seqno,
                    key: key.clone(),
                    value: None,
                },
                Entry::Merge(op) => CdcEvent::Merge {
                    seqno: *seqno,
                    key: key.clone(),
                    operand: op.clone(),
                },
            })
            .collect();
        for (start, end, seqno) in &self.range_tombstones {
            if *seqno >= target {
                events.push(CdcEvent::Range {
                    seqno: *seqno,
                    start: start.clone(),
                    end: end.clone(),
                });
            }
        }
        // Each op takes a unique seqno via `seqno_counter.next()`, so sorting by
        // seqno is a total order that matches the engine's seqno-ordered output.
        events.sort_by_key(CdcEvent::seqno);
        events
    }
}

// ---------------------------------------------------------------------------
// Op generation
// ---------------------------------------------------------------------------

/// Small key space to maximize collisions and test MVCC deduplication.
const KEY_SPACE: u8 = 8;

fn key_from_idx(idx: u8) -> Vec<u8> {
    vec![idx]
}

#[derive(Debug, Clone)]
enum Op {
    Insert {
        key_idx: u8,
        value: Vec<u8>,
    },
    Remove {
        key_idx: u8,
    },
    /// Merge operand folded onto the key's base via the merge operator.
    Merge {
        key_idx: u8,
        value: Vec<u8>,
    },
    /// Range tombstone over the half-open `[start_idx, end_idx)`.
    DeleteRange {
        start_idx: u8,
        end_idx: u8,
    },
    Flush,
    Compact,
    /// Flush, drop, and recover the tree from disk mid-sequence.
    Reopen,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => (0..KEY_SPACE, prop::collection::vec(any::<u8>(), 1..32))
            .prop_map(|(key_idx, value)| Op::Insert { key_idx, value }),
        2 => (0..KEY_SPACE).prop_map(|key_idx| Op::Remove { key_idx }),
        // Merge operands stay in the lower key half; range deletes in the upper
        // half (below). A merge operand on a range-deleted key currently resolves
        // differently with vs without compaction (the range tombstone is dropped
        // during compaction while a later operand still depends on it), tracked as
        // a separate engine bug in #527. The two are kept on disjoint keys until
        // that is fixed, after which the overlap should be enabled; each op is
        // still fully exercised in isolation here.
        2 => (0..KEY_SPACE / 2, prop::collection::vec(any::<u8>(), 1..32))
            .prop_map(|(key_idx, value)| Op::Merge { key_idx, value }),
        // A non-empty half-open range within the upper key half: low..=high
        // spans low..(high + 1).
        1 => (KEY_SPACE / 2..KEY_SPACE, KEY_SPACE / 2..KEY_SPACE).prop_map(|(a, b)| {
            let (low, high) = if a <= b { (a, b) } else { (b, a) };
            Op::DeleteRange { start_idx: low, end_idx: high + 1 }
        }),
        2 => Just(Op::Flush),
        1 => Just(Op::Compact),
        1 => Just(Op::Reopen),
    ]
}

fn ops_strategy() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(op_strategy(), 5..20)
}

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

fn run_oracle_test(ops: Vec<Op>) -> Result<(), TestCaseError> {
    let tmpdir = lsm_tree::get_tmp_folder();
    let seqno_counter = SequenceNumberCounter::default();
    let visible_seqno = SequenceNumberCounter::default();
    let mut tree = open_tree(tmpdir.path(), &seqno_counter, &visible_seqno)?;

    let mut oracle = Oracle::new();
    // Highest GC watermark passed to any compaction. A compaction at watermark
    // W may drop versions shadowed below W, but preserves, for every key, all
    // versions at seqno >= W plus the single latest version below W. So a read
    // at any seqno >= W still matches the full-history oracle, while a read below
    // W may not. Snapshot reads are therefore only verified at seqnos >= gc_floor.
    let mut gc_floor = 0u64;

    // Apply all ops.
    // Data seqnos come from the shared counter (as required by the API).
    // Internal operations (flush, compact) may also advance this counter via
    // upgrade_version when they do work, keeping SV seqnos and data seqnos
    // interleaved in those cases.
    for op in &ops {
        match op {
            Op::Insert { key_idx, value } => {
                let key = key_from_idx(*key_idx);
                let seqno = seqno_counter.next();
                oracle.insert(key.clone(), value.clone(), seqno);
                tree.insert(key, value.clone(), seqno);
                visible_seqno.fetch_max(seqno + 1);
            }
            Op::Remove { key_idx } => {
                let key = key_from_idx(*key_idx);
                let seqno = seqno_counter.next();
                oracle.remove(key.clone(), seqno);
                tree.remove(key, seqno);
                visible_seqno.fetch_max(seqno + 1);
            }
            Op::Merge { key_idx, value } => {
                let key = key_from_idx(*key_idx);
                let seqno = seqno_counter.next();
                oracle.merge(key.clone(), value.clone(), seqno);
                tree.merge(key, value.clone(), seqno);
                visible_seqno.fetch_max(seqno + 1);
            }
            Op::DeleteRange { start_idx, end_idx } => {
                let start = key_from_idx(*start_idx);
                let end = key_from_idx(*end_idx);
                let seqno = seqno_counter.next();
                oracle.delete_range(start.clone(), end.clone(), seqno);
                // remove_range returns the count of range tombstones written, not
                // a Result; it is infallible, so the count is intentionally unused.
                let _ = tree.remove_range(start, end, seqno);
                visible_seqno.fetch_max(seqno + 1);
            }
            Op::Flush => {
                tree.flush_active_memtable(0)
                    .map_err(|e| TestCaseError::fail(format!("flush failed: {e}")))?;
            }
            Op::Compact => {
                let gc_watermark = seqno_counter.get();
                gc_floor = gc_floor.max(gc_watermark);
                tree.major_compact(common::COMPACTION_TARGET, gc_watermark)
                    .map_err(|e| TestCaseError::fail(format!("compact failed: {e}")))?;
            }
            Op::Reopen => {
                // Recover mid-sequence so the following ops exercise writes,
                // merges, deletes, flushes, and compactions against a recovered
                // tree, not only the final reads.
                tree = reopen_tree(tree, tmpdir.path(), &seqno_counter, &visible_seqno)?;
            }
        }
        // Verify the visible state immediately after each op, so a Flush /
        // Compact / Reopen that corrupts it is caught here rather than masked by a
        // later write before the final check.
        verify_against_oracle(&tree, &oracle, visible_seqno.get())?;
    }

    // Read seqno: the visibility watermark, which won't drift ahead of what the
    // tree considers readable.
    let read_seqno = visible_seqno.get();
    verify_against_oracle(&tree, &oracle, read_seqno)?;

    // Snapshot reads at historical seqnos within the GC-safe window
    // [gc_floor, read_seqno]: the engine and oracle agree on the visible state at
    // any past snapshot whose versions a compaction could not have dropped.
    for snapshot_seqno in [gc_floor.max(1), gc_floor.midpoint(read_seqno)] {
        // Stay within observable history: with only Flush / Compact ops,
        // read_seqno is 0 and gc_floor.max(1) would read past the end.
        if snapshot_seqno <= read_seqno {
            verify_against_oracle(&tree, &oracle, snapshot_seqno)?;
        }
    }

    // scan_since_seqno (change-data-capture): every write at seqno >= gc_floor,
    // un-collapsed and seqno-ordered. The lower bound is inclusive, so gc_floor
    // (which may be 0) covers a first op committed at seqno 0.
    verify_cdc(&tree, &oracle, gc_floor)?;

    // Checkpoint + reopen: flush everything to disk, drop the tree (releasing the
    // directory lock), recover from disk, and re-verify both the full visible
    // state and the CDC stream survive the round-trip (so a recovery that keeps
    // values but loses range tombstones / merge operands / seqno bounds is
    // caught).
    let tree = reopen_tree(tree, tmpdir.path(), &seqno_counter, &visible_seqno)?;
    verify_against_oracle(&tree, &oracle, read_seqno)?;
    verify_cdc(&tree, &oracle, gc_floor)?;

    Ok(())
}

/// Verifies the engine agrees with the oracle on every point read, the full
/// scan, and each single-byte prefix scan, at `read_seqno`.
fn verify_against_oracle(
    tree: &Tree,
    oracle: &Oracle,
    read_seqno: u64,
) -> Result<(), TestCaseError> {
    for idx in 0..KEY_SPACE {
        let key = key_from_idx(idx);
        let expected = oracle.get(&key, read_seqno);
        let actual = tree
            .get(&key, read_seqno)
            .map_err(|e| TestCaseError::fail(format!("get failed: {e}")))?;

        prop_assert_eq!(
            actual.as_ref().map(|v| v.to_vec()),
            expected,
            "Point read mismatch for key {:?} at seqno {}",
            key,
            read_seqno,
        );
    }

    let expected_scan = oracle.scan(read_seqno);
    let actual_scan: Vec<(Vec<u8>, Vec<u8>)> = tree
        .iter(read_seqno, None)
        .map(guard_to_kv)
        .collect::<lsm_tree::Result<Vec<_>>>()
        .map_err(|e| TestCaseError::fail(format!("scan: {e}")))?;

    prop_assert_eq!(
        actual_scan.len(),
        expected_scan.len(),
        "Scan length mismatch: tree={}, oracle={}",
        actual_scan.len(),
        expected_scan.len(),
    );

    for (actual, expected) in actual_scan.iter().zip(expected_scan.iter()) {
        prop_assert_eq!(actual, expected, "Scan entry mismatch");
    }

    // With single-byte keys each prefix matches exactly one key — this validates
    // the prefix() API contract and oracle agreement. Multi-key prefix grouping
    // is exercised by the db_bench prefixscan workload.
    for prefix_byte in 0..KEY_SPACE {
        let prefix = vec![prefix_byte];
        let expected_prefix = oracle.prefix_scan(&prefix, read_seqno);
        let actual_prefix: Vec<(Vec<u8>, Vec<u8>)> = tree
            .prefix(&prefix, read_seqno, None)
            .map(guard_to_kv)
            .collect::<lsm_tree::Result<Vec<_>>>()
            .map_err(|e| TestCaseError::fail(format!("prefix scan: {e}")))?;

        prop_assert_eq!(
            actual_prefix.len(),
            expected_prefix.len(),
            "Prefix scan length mismatch for prefix {:?}",
            prefix,
        );

        for (actual, expected) in actual_prefix.iter().zip(expected_prefix.iter()) {
            prop_assert_eq!(
                actual,
                expected,
                "Prefix scan entry mismatch for prefix {:?}",
                prefix,
            );
        }
    }

    Ok(())
}

/// Opens (or recovers) the standard tree with the merge operator configured.
fn open_tree(
    path: &std::path::Path,
    seqno_counter: &SequenceNumberCounter,
    visible_seqno: &SequenceNumberCounter,
) -> Result<Tree, TestCaseError> {
    let AnyTree::Standard(tree) = Config::new(path, seqno_counter.clone(), visible_seqno.clone())
        .with_merge_operator(Some(Arc::new(MaxMerge)))
        .open()
        .map_err(|e| TestCaseError::fail(format!("open tree failed: {e}")))?
    else {
        return Err(TestCaseError::fail("expected a standard tree"));
    };
    Ok(tree)
}

/// Flushes, drops (releasing the directory lock), and recovers the tree from
/// disk, returning the recovered handle.
fn reopen_tree(
    tree: Tree,
    path: &std::path::Path,
    seqno_counter: &SequenceNumberCounter,
    visible_seqno: &SequenceNumberCounter,
) -> Result<Tree, TestCaseError> {
    tree.flush_active_memtable(0)
        .map_err(|e| TestCaseError::fail(format!("flush before reopen failed: {e}")))?;
    drop(tree);
    open_tree(path, seqno_counter, visible_seqno)
}

/// Verifies the engine's change-data-capture output matches the oracle from
/// `gc_floor` (the inclusive, GC-safe lower bound).
fn verify_cdc(tree: &Tree, oracle: &Oracle, gc_floor: u64) -> Result<(), TestCaseError> {
    let expected_cdc = oracle.scan_since(gc_floor);
    // A point tombstone is a flush-materialized range deletion iff a range
    // tombstone at that exact seqno covers the key (the op at that seqno was a
    // DeleteRange, not a Remove, since seqnos are unique). Checking the key keeps
    // the filter self-validating if the engine's materialization detail changes.
    let is_materialized_range = |key: &[u8], seqno: u64| {
        oracle
            .range_tombstones
            .iter()
            .any(|(start, end, rt_seqno)| {
                *rt_seqno == seqno && key >= start.as_slice() && key < end.as_slice()
            })
    };
    let actual_cdc: Vec<CdcEvent> = tree
        .scan_since_seqno(gc_floor)
        .map_err(|e| TestCaseError::fail(format!("scan_since_seqno failed: {e}")))?
        .filter_map(|event| match event {
            ScanSinceEvent::Insert { key, value, seqno } => Some(CdcEvent::Point {
                seqno,
                key: key.to_vec(),
                value: Some(value.to_vec()),
            }),
            ScanSinceEvent::PointTombstone { key, seqno }
                if !is_materialized_range(&key, seqno) =>
            {
                Some(CdcEvent::Point {
                    seqno,
                    key: key.to_vec(),
                    value: None,
                })
            }
            ScanSinceEvent::PointTombstone { .. } => None,
            ScanSinceEvent::RangeTombstone {
                start_key,
                end_key,
                seqno,
            } => Some(CdcEvent::Range {
                seqno,
                start: start_key.to_vec(),
                end: end_key.to_vec(),
            }),
            ScanSinceEvent::MergeOperand {
                key,
                operand,
                seqno,
            } => Some(CdcEvent::Merge {
                seqno,
                key: key.to_vec(),
                operand: operand.to_vec(),
            }),
        })
        .collect();
    prop_assert_eq!(
        actual_cdc,
        expected_cdc,
        "scan_since_seqno CDC mismatch at target {}",
        gc_floor,
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// proptest
// ---------------------------------------------------------------------------

proptest! {
    // Defaults: 32 cases + 1000 shrink iters.
    // Override at run time via `PROPTEST_CASES` / `PROPTEST_MAX_SHRINK`
    // env vars — see `common::proptest_config`.
    #![proptest_config(common::proptest_config())]

    #[test]
    fn prop_btreemap_oracle_correctness(ops in ops_strategy()) {
        run_oracle_test(ops)?;
    }
}

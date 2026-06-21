// Property-based model test: compare lsm-tree against BTreeMap oracle.
//
// The oracle models MVCC using (key, Reverse(seqno)) ordering, where
// None values represent tombstones. This oracle only models point
// tombstones; range tombstones are tested separately in prop_range_tombstone.rs.

mod common;

use common::guard_to_kv;
use lsm_tree::{AbstractTree, AnyTree, Config, ScanSinceEvent, SequenceNumberCounter, Tree};
use proptest::prelude::*;
use std::cmp::Reverse;
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Oracle
// ---------------------------------------------------------------------------

type VersionedKey = (Vec<u8>, Reverse<u64>);
type VersionedValue = Option<Vec<u8>>;

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
}

impl CdcEvent {
    fn seqno(&self) -> u64 {
        match self {
            Self::Point { seqno, .. } | Self::Range { seqno, .. } => *seqno,
        }
    }
}

/// MVCC oracle covering point writes and range tombstones.
#[derive(Debug, Clone)]
struct Oracle {
    /// (key, Reverse(seqno)) -> Some(value) for puts, None for tombstones.
    data: BTreeMap<VersionedKey, VersionedValue>,
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
        self.data.insert((key, Reverse(seqno)), Some(value));
    }

    fn remove(&mut self, key: Vec<u8>, seqno: u64) {
        self.data.insert((key, Reverse(seqno)), None);
    }

    fn delete_range(&mut self, start: Vec<u8>, end: Vec<u8>, seqno: u64) {
        self.range_tombstones.push((start, end, seqno));
    }

    /// Highest seqno of a range tombstone covering `key` and visible at
    /// `read_seqno` (exclusive upper bound), or `None` if none applies.
    fn covering_range_tombstone(&self, key: &[u8], read_seqno: u64) -> Option<u64> {
        self.range_tombstones
            .iter()
            .filter(|(start, end, seqno)| {
                *seqno < read_seqno && key >= start.as_slice() && key < end.as_slice()
            })
            .map(|(_, _, seqno)| *seqno)
            .max()
    }

    /// Point read: return the latest visible value at read_seqno.
    /// lsm-tree uses exclusive upper bound: entry_seqno < read_seqno.
    fn get(&self, key: &[u8], read_seqno: u64) -> Option<Vec<u8>> {
        if read_seqno == 0 {
            return None;
        }
        // Exclusive: find entries with seqno < read_seqno (i.e., <= read_seqno - 1)
        let start = (key.to_vec(), Reverse(read_seqno - 1));
        let end_inclusive = (key.to_vec(), Reverse(0));

        // Latest point version visible at read_seqno.
        let (point_seqno, point_value) = self
            .data
            .range(start..=end_inclusive)
            .take_while(|((k, _), _)| k == key)
            .map(|((_, Reverse(seqno)), val)| (*seqno, val.clone()))
            .next()?;
        // A range tombstone newer than that point version hides the key.
        if let Some(rt_seqno) = self.covering_range_tombstone(key, read_seqno)
            && rt_seqno > point_seqno
        {
            return None;
        }
        point_value
    }

    /// Full scan: return all visible (key, value) pairs at read_seqno, sorted by key.
    /// lsm-tree uses exclusive upper bound: entry_seqno < read_seqno.
    fn scan(&self, read_seqno: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut result = Vec::new();
        let mut last_key: Option<&Vec<u8>> = None;

        for ((key, Reverse(entry_seqno)), val) in &self.data {
            if *entry_seqno >= read_seqno {
                continue;
            }
            if last_key == Some(key) {
                continue;
            }
            last_key = Some(key);

            // A range tombstone newer than this latest point version hides it.
            if let Some(rt_seqno) = self.covering_range_tombstone(key, read_seqno)
                && rt_seqno > *entry_seqno
            {
                continue;
            }
            if let Some(value) = val {
                result.push((key.clone(), value.clone()));
            }
        }

        result
    }

    /// Prefix scan: return visible entries with given prefix at read_seqno.
    fn prefix_scan(&self, prefix: &[u8], read_seqno: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.scan(read_seqno)
            .into_iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .collect()
    }

    /// Change-data-capture: every write at `seqno >= target`, un-collapsed (a key
    /// written N times yields N events) and ordered by ascending seqno, as
    /// [`CdcEvent`]s (point writes and range tombstones). This mirrors
    /// `Tree::scan_since_seqno`, whose lower bound is inclusive.
    fn scan_since(&self, target: u64) -> Vec<CdcEvent> {
        let mut events: Vec<CdcEvent> = self
            .data
            .iter()
            .filter(|((_, Reverse(seqno)), _)| *seqno >= target)
            .map(|((key, Reverse(seqno)), val)| CdcEvent::Point {
                seqno: *seqno,
                key: key.clone(),
                value: val.clone(),
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
    /// Range tombstone over the half-open `[start_idx, end_idx)`.
    DeleteRange {
        start_idx: u8,
        end_idx: u8,
    },
    Flush,
    Compact,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => (0..KEY_SPACE, prop::collection::vec(any::<u8>(), 1..32))
            .prop_map(|(key_idx, value)| Op::Insert { key_idx, value }),
        2 => (0..KEY_SPACE).prop_map(|key_idx| Op::Remove { key_idx }),
        // A non-empty half-open range: low..=high spans low..(high + 1).
        1 => (0..KEY_SPACE, 0..KEY_SPACE).prop_map(|(a, b)| {
            let (low, high) = if a <= b { (a, b) } else { (b, a) };
            Op::DeleteRange { start_idx: low, end_idx: high + 1 }
        }),
        2 => Just(Op::Flush),
        1 => Just(Op::Compact),
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
    let AnyTree::Standard(tree) =
        Config::new(&tmpdir, seqno_counter.clone(), visible_seqno.clone())
            .open()
            .map_err(|e| TestCaseError::fail(format!("failed to open tree: {e}")))?
    else {
        return Err(TestCaseError::fail("expected a standard tree"));
    };

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
            Op::DeleteRange { start_idx, end_idx } => {
                let start = key_from_idx(*start_idx);
                let end = key_from_idx(*end_idx);
                let seqno = seqno_counter.next();
                oracle.delete_range(start.clone(), end.clone(), seqno);
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
        }
    }

    // Read seqno: the visibility watermark, which won't drift ahead of what the
    // tree considers readable.
    let read_seqno = visible_seqno.get();
    verify_against_oracle(&tree, &oracle, read_seqno)?;

    // Snapshot reads at historical seqnos within the GC-safe window
    // [gc_floor, read_seqno]: the engine and oracle agree on the visible state at
    // any past snapshot whose versions a compaction could not have dropped.
    for snapshot_seqno in [gc_floor.max(1), gc_floor.midpoint(read_seqno)] {
        verify_against_oracle(&tree, &oracle, snapshot_seqno)?;
    }

    // scan_since_seqno (change-data-capture): every write at seqno >= target,
    // un-collapsed and seqno-ordered. GC-safe for target >= gc_floor, since every
    // such version is preserved. Both sides map to (seqno, key, value).
    let cdc_target = gc_floor.max(1);
    let expected_cdc = oracle.scan_since(cdc_target);
    let actual_cdc: Vec<CdcEvent> = tree
        .scan_since_seqno(cdc_target)
        .map_err(|e| TestCaseError::fail(format!("scan_since_seqno failed: {e}")))?
        .filter_map(|event| match event {
            ScanSinceEvent::Insert { key, value, seqno } => Some(CdcEvent::Point {
                seqno,
                key: key.to_vec(),
                value: Some(value.to_vec()),
            }),
            ScanSinceEvent::PointTombstone { key, seqno } => Some(CdcEvent::Point {
                seqno,
                key: key.to_vec(),
                value: None,
            }),
            ScanSinceEvent::RangeTombstone {
                start_key,
                end_key,
                seqno,
            } => Some(CdcEvent::Range {
                seqno,
                start: start_key.to_vec(),
                end: end_key.to_vec(),
            }),
            // No merge operands are produced by the ops above.
            ScanSinceEvent::MergeOperand { .. } => None,
        })
        .collect();
    prop_assert_eq!(
        actual_cdc,
        expected_cdc,
        "scan_since_seqno CDC mismatch at target {}",
        cdc_target,
    );

    // Checkpoint + reopen: flush everything to disk, drop the tree (releasing the
    // directory lock), recover from disk, and re-verify the full visible state
    // survives the round-trip.
    tree.flush_active_memtable(0)
        .map_err(|e| TestCaseError::fail(format!("flush before reopen failed: {e}")))?;
    drop(tree);
    let AnyTree::Standard(tree) =
        Config::new(&tmpdir, seqno_counter.clone(), visible_seqno.clone())
            .open()
            .map_err(|e| TestCaseError::fail(format!("reopen failed: {e}")))?
    else {
        return Err(TestCaseError::fail("expected a standard tree after reopen"));
    };
    verify_against_oracle(&tree, &oracle, read_seqno)?;

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

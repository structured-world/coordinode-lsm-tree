// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The projected columnar scan returns exactly the rows a row read returns.
//!
//! Random writes (runs of keys with values of varying width, point deletes,
//! range deletes, rewrites of a key at the seqno it already has) are flushed
//! into overlapping segments and sometimes compacted, then scanned at a random
//! snapshot over a random key range, under a random payload budget from one
//! byte up, with or without a predicate. Every scan must match a `BTreeMap`
//! oracle of the writes and the tree's own row read path, row for row, and a
//! budget of at least a row page must hold.

#![cfg(feature = "columnar")]

mod common;

use common::guard_to_kv;
use lsm_tree::table::columnar::{COL_USER_KEY, COL_VALUE, ColumnBatch};
use lsm_tree::table::columnar_predicate::{ColumnRangePredicate, PredicateApply};
use lsm_tree::{AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter, UserKey};
use proptest::prelude::*;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

/// Keys a case writes to: enough that a run fills several row pages.
const KEY_SPACE: u16 = 600;

fn key(i: u16) -> Vec<u8> {
    format!("k{i:04}").into_bytes()
}

#[derive(Debug, Clone)]
enum Op {
    /// `len` consecutive keys from `start`, each at its own seqno, each value
    /// `width` bytes of `fill`.
    Run {
        start: u16,
        len: u16,
        width: u16,
        fill: u8,
    },
    /// A point delete.
    Remove {
        at: u16,
    },
    /// A range delete of `[lo, hi)`.
    RemoveRange {
        lo: u16,
        hi: u16,
    },
    /// A flush, then the key rewritten at the seqno its last write had, so the
    /// newer segment and an older one hold the same key at the same seqno.
    Tie {
        at: u16,
        fill: u8,
    },
    Flush,
    /// A major compaction that keeps every version.
    Compact,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => (0..KEY_SPACE, 1..200u16, 0..300u16, any::<u8>())
            .prop_map(|(start, len, width, fill)| Op::Run { start, len, width, fill }),
        2 => (0..KEY_SPACE).prop_map(|at| Op::Remove { at }),
        1 => (0..KEY_SPACE, 0..KEY_SPACE).prop_map(|(a, b)| Op::RemoveRange {
            lo: a.min(b),
            hi: a.max(b),
        }),
        1 => (0..KEY_SPACE, any::<u8>()).prop_map(|(at, fill)| Op::Tie { at, fill }),
        3 => Just(Op::Flush),
        1 => Just(Op::Compact),
    ]
}

#[derive(Debug, Clone)]
enum Pred {
    None,
    /// On the value column, which the scan may not project.
    Value {
        lo: u8,
        hi: u8,
    },
    /// On the key column.
    Key {
        lo: u16,
        hi: u16,
    },
    /// On a column no row has.
    Absent,
}

fn pred() -> impl Strategy<Value = Pred> {
    prop_oneof![
        Just(Pred::None),
        (any::<u8>(), any::<u8>()).prop_map(|(a, b)| Pred::Value {
            lo: a.min(b),
            hi: a.max(b),
        }),
        (0..KEY_SPACE, 0..KEY_SPACE).prop_map(|(a, b)| Pred::Key {
            lo: a.min(b),
            hi: a.max(b),
        }),
        Just(Pred::Absent),
    ]
}

fn bound() -> impl Strategy<Value = Bound<u16>> {
    prop_oneof![
        Just(Bound::Unbounded),
        (0..KEY_SPACE).prop_map(Bound::Included),
        (0..KEY_SPACE).prop_map(Bound::Excluded),
    ]
}

#[derive(Debug, Clone)]
struct Scan {
    /// Where the snapshot falls, as a fraction of the writes, in 1/256ths.
    snapshot: u8,
    lo: Bound<u16>,
    hi: Bound<u16>,
    pred: Pred,
    /// Whether the value column is projected, or only the key.
    values: bool,
}

fn scan() -> impl Strategy<Value = Scan> {
    (any::<u8>(), bound(), bound(), pred(), any::<bool>()).prop_map(
        |(snapshot, lo, hi, pred, values)| Scan {
            snapshot,
            lo,
            hi,
            pred,
            values,
        },
    )
}

/// Every write, as the read path resolves them: a key's newest version below
/// the snapshot, unless a range delete newer than it covers it.
/// A version of a key: the key, then the newer seqno first.
type Version = (Vec<u8>, Reverse<SeqNo>);

#[derive(Default)]
struct Oracle {
    /// Each version's value, `None` for a deletion.
    rows: BTreeMap<Version, Option<Vec<u8>>>,
    range_deletes: Vec<(Vec<u8>, Vec<u8>, SeqNo)>,
}

impl Oracle {
    fn scan(&self, snapshot: SeqNo) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut out = Vec::new();
        let mut last: Option<&Vec<u8>> = None;
        for ((key, Reverse(seqno)), value) in &self.rows {
            if *seqno >= snapshot || last == Some(key) {
                continue;
            }
            last = Some(key);
            let covered = self
                .range_deletes
                .iter()
                .any(|(lo, hi, rt)| *rt < snapshot && *rt > *seqno && key >= lo && key < hi);
            if let (false, Some(value)) = (covered, value) {
                out.push((key.clone(), value.clone()));
            }
        }
        out
    }
}

fn in_bounds(key: &[u8], lo: &Bound<Vec<u8>>, hi: &Bound<Vec<u8>>) -> bool {
    let above = match lo {
        Bound::Unbounded => true,
        Bound::Included(lo) => key >= lo.as_slice(),
        Bound::Excluded(lo) => key > lo.as_slice(),
    };
    let below = match hi {
        Bound::Unbounded => true,
        Bound::Included(hi) => key <= hi.as_slice(),
        Bound::Excluded(hi) => key < hi.as_slice(),
    };
    above && below
}

/// The cells of a bytes column of `rows` rows: `rows + 1` little-endian `u32`
/// offsets, then the payload they index.
fn bytes_rows(data: &[u8], rows: u32) -> Vec<&[u8]> {
    let offsets = (rows as usize + 1) * 4;
    let at = |i: usize| {
        let word = data[i * 4..i * 4 + 4].try_into().expect("offset");
        u32::from_le_bytes(word) as usize
    };
    let payload = &data[offsets..];
    (0..rows as usize)
        .map(|i| &payload[at(i)..at(i + 1)])
        .collect()
}

/// The rows of `batch`: each key, and its value when projected.
fn rows_of(batch: &ColumnBatch, values: bool) -> Vec<(Vec<u8>, Vec<u8>)> {
    let column = |id| {
        batch
            .columns
            .iter()
            .find(|c| c.column_id == id)
            .expect("projected column")
    };
    let keys = bytes_rows(&column(COL_USER_KEY).data, batch.row_count);
    let vals = values.then(|| bytes_rows(&column(COL_VALUE).data, batch.row_count));
    (0..keys.len())
        .map(|i| {
            let value = vals.as_ref().map_or_else(Vec::new, |v| v[i].to_vec());
            (keys[i].to_vec(), value)
        })
        .collect()
}

fn run(ops: &[Op], scans: &[Scan], budget: u64) -> Result<(), TestCaseError> {
    let folder = lsm_tree::get_tmp_folder();
    let AnyTree::Standard(tree) = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .columnar_scan_budget(budget)
    .open()
    .map_err(|e| TestCaseError::fail(format!("open: {e}")))?
    else {
        return Err(TestCaseError::fail("expected a standard tree"));
    };
    tree.update_runtime_config(|cfg| cfg.columnar = true)
        .map_err(|e| TestCaseError::fail(format!("columnar: {e}")))?;

    let mut oracle = Oracle::default();
    // The seqno each key last had a value written at, for a tie to reuse.
    let mut written: BTreeMap<u16, SeqNo> = BTreeMap::new();
    // Keys two segments hold at one seqno. Which one a read returns is not
    // a matter of seqnos, so the oracle cannot say: the point read, which
    // keeps the first run of L0 that has the key, is the reference for them.
    let mut tied: BTreeSet<Vec<u8>> = BTreeSet::new();
    let mut seqno: SeqNo = 1;
    let flush = |tree: &lsm_tree::Tree| {
        tree.flush_active_memtable(0)
            .map_err(|e| TestCaseError::fail(format!("flush: {e}")))
    };
    for op in ops {
        match *op {
            Op::Run {
                start,
                len,
                width,
                fill,
            } => {
                for i in start..start.saturating_add(len).min(KEY_SPACE) {
                    let value = vec![fill.wrapping_add(i as u8); usize::from(width)];
                    tree.insert(key(i), value.clone(), seqno);
                    oracle.rows.insert((key(i), Reverse(seqno)), Some(value));
                    written.insert(i, seqno);
                    seqno += 1;
                }
            }
            Op::Remove { at } => {
                tree.remove(key(at), seqno);
                oracle.rows.insert((key(at), Reverse(seqno)), None);
                written.remove(&at);
                seqno += 1;
            }
            Op::RemoveRange { lo, hi } => {
                if lo < hi {
                    tree.remove_range(key(lo), key(hi), seqno);
                    oracle.range_deletes.push((key(lo), key(hi), seqno));
                    seqno += 1;
                }
            }
            Op::Tie { at, fill } => {
                if let Some(&at_seqno) = written.get(&at) {
                    flush(&tree)?;
                    tree.insert(key(at), vec![fill; 16], at_seqno);
                    tied.insert(key(at));
                }
            }
            Op::Flush => flush(&tree)?,
            Op::Compact => {
                flush(&tree)?;
                tree.major_compact(common::COMPACTION_TARGET, 0)
                    .map_err(|e| TestCaseError::fail(format!("compact: {e}")))?;
            }
        }
    }
    // The scan reads segments only.
    flush(&tree)?;

    for scan in scans {
        let snapshot = 1 + (seqno * SeqNo::from(scan.snapshot)).div_ceil(255);
        let lo = scan.lo.map(key);
        let hi = scan.hi.map(key);
        let predicate = match scan.pred {
            Pred::None => None,
            Pred::Value { lo, hi } => Some(ColumnRangePredicate {
                column_id: COL_VALUE,
                lower: Some(vec![lo]),
                upper: Some(vec![hi]),
                apply: PredicateApply::Filter,
            }),
            Pred::Key { lo, hi } => Some(ColumnRangePredicate {
                column_id: COL_USER_KEY,
                lower: Some(key(lo)),
                upper: Some(key(hi)),
                apply: PredicateApply::Filter,
            }),
            Pred::Absent => Some(ColumnRangePredicate {
                column_id: 999,
                lower: Some(vec![0]),
                upper: Some(vec![0]),
                apply: PredicateApply::Filter,
            }),
        };
        // A predicate on a column no row has keeps every row: nothing can
        // test it, and the scan reports it did not run.
        let matches = |key: &[u8], value: &[u8]| match scan.pred {
            Pred::None | Pred::Absent => true,
            Pred::Value { lo, hi } => value >= [lo].as_slice() && value <= [hi].as_slice(),
            Pred::Key { lo, hi } => {
                key >= self::key(lo).as_slice() && key <= self::key(hi).as_slice()
            }
        };
        let mut expected = Vec::new();
        for (k, mut v) in oracle.scan(snapshot) {
            if tied.contains(&k) {
                v = tree
                    .get(&k, snapshot)
                    .map_err(|e| TestCaseError::fail(format!("get: {e}")))?
                    .ok_or_else(|| TestCaseError::fail("a tied key reads as absent"))?
                    .to_vec();
            }
            if in_bounds(&k, &lo, &hi) && matches(&k, &v) {
                expected.push((k, if scan.values { v } else { Vec::new() }));
            }
        }

        let bounds = (lo.clone().map(UserKey::from), hi.clone().map(UserKey::from));
        let rows_read: Vec<(Vec<u8>, Vec<u8>)> = tree
            .range(bounds.clone(), snapshot, None)
            .map(guard_to_kv)
            .collect::<lsm_tree::Result<Vec<_>>>()
            .map_err(|e| TestCaseError::fail(format!("row read: {e}")))?
            .into_iter()
            .filter(|(k, v)| !tied.contains(k) && matches(k, v))
            .map(|(k, v)| (k, if scan.values { v } else { Vec::new() }))
            .collect();
        // The range read breaks a tie by its own order of sources, not the
        // point read's, so it is held to the rows without one.
        let untied: Vec<(Vec<u8>, Vec<u8>)> = expected
            .iter()
            .filter(|(k, _)| !tied.contains(k))
            .cloned()
            .collect();
        prop_assert_eq!(
            &rows_read,
            &untied,
            "the row read disagrees with the oracle"
        );
        let projection: &[u16] = if scan.values {
            &[COL_USER_KEY, COL_VALUE]
        } else {
            &[COL_USER_KEY]
        };
        let mut columnar = tree
            .columnar_scan(projection, predicate.as_ref(), snapshot, bounds)
            .map_err(|e| TestCaseError::fail(format!("scan: {e}")))?;
        let mut got = Vec::new();
        for batch in &mut columnar {
            let batch = batch.map_err(|e| TestCaseError::fail(format!("batch: {e}")))?;
            prop_assert!(batch.row_count > 0, "an empty batch was yielded");
            got.extend(rows_of(&batch, scan.values));
        }
        prop_assert_eq!(
            &got,
            &expected,
            "the columnar scan disagrees with the reads"
        );
        if columnar.oversized_reads() == 0 {
            prop_assert!(
                columnar.peak_payload_bytes() <= budget,
                "held {} B under a {budget} B budget with no read past a share",
                columnar.peak_payload_bytes(),
            );
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(common::proptest_config())]

    #[test]
    fn a_columnar_scan_returns_what_a_row_read_returns(
        ops in prop::collection::vec(op(), 1..30),
        scans in prop::collection::vec(scan(), 1..6),
        budget in prop_oneof![Just(1u64), Just(512), Just(4_096), Just(65_536), Just(1 << 20)],
    ) {
        run(&ops, &scans, budget)?;
    }
}

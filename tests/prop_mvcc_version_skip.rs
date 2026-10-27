// Differential property test for passing a resolved key's older versions.
//
// A scan resolves each key from its newest visible version and drops the
// rest; past a few versions it jumps instead of walking them, a data block
// holding nothing but the key's versions passed on the index alone, a
// memtable searched from the top. Chains here run to dozens of versions per
// key, across memtables, flushed tables and compacted runs, with point and
// range tombstones, merge operands and older snapshots, and every scan
// (forward, backward, both ends at once, bounded and not) must match a
// model that resolves every version in turn.

mod common;

use common::{COMPACTION_TARGET, proptest_config};
use lsm_tree::{
    AbstractTree, AnyTree, Config, Guard, MergeOperator, SequenceNumberCounter, UserComparator,
    UserValue,
};
use proptest::prelude::*;
use std::cmp::Ordering;
use std::ops::Bound;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Model
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum Kind {
    Put(Vec<u8>),
    Del,
    Merge(Vec<u8>),
}

/// Every version written, and every range tombstone, as written.
struct Model {
    versions: Vec<(Vec<u8>, u64, Kind)>,
    range_tombstones: Vec<(Vec<u8>, Vec<u8>, u64)>,
    reverse: bool,
}

impl Model {
    fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering {
        if self.reverse { b.cmp(a) } else { a.cmp(b) }
    }

    /// Whether a range tombstone a read at `read` sees deletes `key` at
    /// `seqno`.
    fn suppressed(&self, key: &[u8], seqno: u64, read: u64) -> bool {
        self.range_tombstones.iter().any(|(start, end, rt)| {
            *rt < read
                && seqno < *rt
                && self.cmp(start, key) != Ordering::Greater
                && self.cmp(key, end) == Ordering::Less
        })
    }

    /// What a read at `read` sees of `key`.
    fn resolve(&self, key: &[u8], read: u64) -> Option<Vec<u8>> {
        let mut versions: Vec<&(Vec<u8>, u64, Kind)> = self
            .versions
            .iter()
            .filter(|(k, seqno, _)| k == key && *seqno < read)
            .collect();
        versions.sort_by_key(|(_, seqno, _)| std::cmp::Reverse(*seqno));

        let mut operands: Vec<&[u8]> = Vec::new();
        let mut base: Option<&[u8]> = None;
        for (_, seqno, kind) in versions {
            if self.suppressed(key, *seqno, read) {
                break;
            }
            match kind {
                Kind::Merge(op) => operands.push(op),
                Kind::Put(value) => {
                    base = Some(value);
                    break;
                }
                Kind::Del => break,
            }
        }
        if operands.is_empty() {
            return base.map(<[u8]>::to_vec);
        }
        let mut out = base.map(<[u8]>::to_vec).unwrap_or_default();
        for op in operands.iter().rev() {
            out.extend_from_slice(op);
        }
        Some(out)
    }

    /// What a scan at `read` over `[lo, hi)` hands out, in the tree's order.
    fn scan(&self, read: u64, lo: &Bound<Vec<u8>>, hi: &Bound<Vec<u8>>) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut keys: Vec<Vec<u8>> = self.versions.iter().map(|(k, _, _)| k.clone()).collect();
        keys.sort_by(|a, b| self.cmp(a, b));
        keys.dedup();
        keys.into_iter()
            .filter(|k| match lo {
                Bound::Included(lo) => self.cmp(k, lo) != Ordering::Less,
                Bound::Excluded(lo) => self.cmp(k, lo) == Ordering::Greater,
                Bound::Unbounded => true,
            })
            .filter(|k| match hi {
                Bound::Included(hi) => self.cmp(k, hi) != Ordering::Greater,
                Bound::Excluded(hi) => self.cmp(k, hi) == Ordering::Less,
                Bound::Unbounded => true,
            })
            .filter_map(|k| self.resolve(&k, read).map(|v| (k, v)))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Tree
// ---------------------------------------------------------------------------

struct Reverse;

impl UserComparator for Reverse {
    fn name(&self) -> &'static str {
        "reverse"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        b.cmp(a)
    }
}

/// Appends each operand to the base, oldest first.
struct Concat;

impl MergeOperator for Concat {
    fn merge(
        &self,
        _key: &[u8],
        base: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> lsm_tree::Result<UserValue> {
        let mut out = base.map(<[u8]>::to_vec).unwrap_or_default();
        for op in operands {
            out.extend_from_slice(op);
        }
        Ok(out.into())
    }
}

#[derive(Clone, Copy, Debug)]
enum Variant {
    Lexicographic,
    Reverse,
    Merge,
}

fn open(dir: &std::path::Path, variant: Variant) -> AnyTree {
    let config = Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    // Small blocks: a chain of a few dozen versions spans several.
    .data_block_size_policy(lsm_tree::config::BlockSizePolicy::all(512));
    let config = match variant {
        Variant::Lexicographic => config,
        Variant::Reverse => config.comparator(Arc::new(Reverse)),
        Variant::Merge => config.with_merge_operator(Some(Arc::new(Concat))),
    };
    config.open().expect("open")
}

// ---------------------------------------------------------------------------
// Ops
// ---------------------------------------------------------------------------

const KEY_SPACE: u8 = 6;

fn key(idx: u8) -> Vec<u8> {
    vec![b'k', idx]
}

#[derive(Clone, Debug)]
enum Op {
    Put {
        key: u8,
        value: u8,
    },
    /// `n` versions of one key in a row: the chains the jump is for.
    Burst {
        key: u8,
        n: u8,
    },
    Del {
        key: u8,
    },
    Merge {
        key: u8,
        op: u8,
    },
    MergeBurst {
        key: u8,
        n: u8,
    },
    RemoveRange {
        a: u8,
        b: u8,
    },
    Flush,
    Compact,
}

fn op_strategy(merge: bool) -> impl Strategy<Value = Op> {
    let merges = if merge { 2 } else { 0 };
    prop_oneof![
        4 => (0..KEY_SPACE, any::<u8>()).prop_map(|(key, value)| Op::Put { key, value }),
        3 => (0..KEY_SPACE, 9u8..60).prop_map(|(key, n)| Op::Burst { key, n }),
        2 => (0..KEY_SPACE).prop_map(|key| Op::Del { key }),
        merges => (0..KEY_SPACE, any::<u8>()).prop_map(|(key, op)| Op::Merge { key, op }),
        merges => (0..KEY_SPACE, 9u8..40).prop_map(|(key, n)| Op::MergeBurst { key, n }),
        1 => (0..=KEY_SPACE, 0..=KEY_SPACE).prop_map(|(a, b)| Op::RemoveRange { a, b }),
        2 => Just(Op::Flush),
        1 => Just(Op::Compact),
    ]
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

/// Key-value pairs a scan hands out.
type Rows = Vec<(Vec<u8>, Vec<u8>)>;

/// A scan's bounds as drawn: a key index and whether it is included.
type RawBounds = (Option<(u8, bool)>, Option<(u8, bool)>);

fn pairs(items: impl Iterator<Item = impl Guard>) -> Result<Rows, TestCaseError> {
    items
        .map(|guard| {
            let (k, v) = guard
                .into_inner()
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
            Ok((k.to_vec(), v.to_vec()))
        })
        .collect()
}

/// A scan taken from both ends in turn: the front's items, then the back's
/// in reverse, which together are the scan's.
fn alternating(
    mut iter: impl DoubleEndedIterator<Item = impl Guard>,
) -> Result<Rows, TestCaseError> {
    let mut front = Vec::new();
    let mut back = Vec::new();
    let mut from_front = true;
    loop {
        let item = if from_front {
            iter.next()
        } else {
            iter.next_back()
        };
        let Some(guard) = item else { break };
        let (k, v) = guard
            .into_inner()
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        if from_front {
            front.push((k.to_vec(), v.to_vec()));
        } else {
            back.push((k.to_vec(), v.to_vec()));
        }
        from_front = !from_front;
    }
    back.reverse();
    front.extend(back);
    Ok(front)
}

fn bound(raw: Option<(u8, bool)>) -> Bound<Vec<u8>> {
    match raw {
        None => Bound::Unbounded,
        Some((idx, true)) => Bound::Included(key(idx)),
        Some((idx, false)) => Bound::Excluded(key(idx)),
    }
}

fn check(
    tree: &AnyTree,
    model: &Model,
    read: u64,
    lo: &Bound<Vec<u8>>,
    hi: &Bound<Vec<u8>>,
) -> Result<(), TestCaseError> {
    let expected = model.scan(read, lo, hi);
    let range = (lo.clone(), hi.clone());

    let forward = pairs(tree.range(range.clone(), read, None))?;
    prop_assert_eq!(&forward, &expected, "forward at {}", read);

    let mut backward = pairs(tree.range(range.clone(), read, None).rev())?;
    backward.reverse();
    prop_assert_eq!(&backward, &expected, "backward at {}", read);

    let both = alternating(tree.range(range, read, None))?;
    prop_assert_eq!(&both, &expected, "both ends at {}", read);
    Ok(())
}

fn run(
    variant: Variant,
    ops: Vec<Op>,
    reads: Vec<u64>,
    bounds: RawBounds,
) -> Result<(), TestCaseError> {
    let dir = lsm_tree::get_tmp_folder();
    let tree = open(dir.path(), variant);
    let mut model = Model {
        versions: Vec::new(),
        range_tombstones: Vec::new(),
        reverse: matches!(variant, Variant::Reverse),
    };
    let mut seqno = 0u64;
    let mut next = || {
        seqno += 1;
        seqno - 1
    };

    for op in ops {
        match op {
            Op::Put { key: k, value } => {
                let s = next();
                tree.insert(key(k), [value], s);
                model.versions.push((key(k), s, Kind::Put(vec![value])));
            }
            Op::Burst { key: k, n } => {
                for i in 0..n {
                    let s = next();
                    tree.insert(key(k), [i, k], s);
                    model.versions.push((key(k), s, Kind::Put(vec![i, k])));
                }
            }
            Op::Del { key: k } => {
                let s = next();
                tree.remove(key(k), s);
                model.versions.push((key(k), s, Kind::Del));
            }
            Op::Merge { key: k, op } => {
                let s = next();
                tree.merge(key(k), [op], s);
                model.versions.push((key(k), s, Kind::Merge(vec![op])));
            }
            Op::MergeBurst { key: k, n } => {
                for i in 0..n {
                    let s = next();
                    tree.merge(key(k), [i], s);
                    model.versions.push((key(k), s, Kind::Merge(vec![i])));
                }
            }
            Op::RemoveRange { a, b } => {
                let (start, end) = if model.cmp(&key(a), &key(b)) == Ordering::Greater {
                    (key(b), key(a))
                } else {
                    (key(a), key(b))
                };
                if start == end {
                    continue;
                }
                let s = next();
                tree.remove_range(start.clone(), end.clone(), s);
                model.range_tombstones.push((start, end, s));
            }
            Op::Flush => {
                tree.flush_active_memtable(0)
                    .map_err(|e| TestCaseError::fail(e.to_string()))?;
            }
            Op::Compact => {
                // A watermark of 0 keeps every version, so older snapshots
                // still read what they did.
                tree.major_compact(COMPACTION_TARGET, 0)
                    .map_err(|e| TestCaseError::fail(e.to_string()))?;
            }
        }
    }

    let end = next() + 1;
    let (lo, hi) = (bound(bounds.0), bound(bounds.1));
    for read in reads.into_iter().map(|r| r % end).chain([end]) {
        check(&tree, &model, read, &Bound::Unbounded, &Bound::Unbounded)?;
        check(&tree, &model, read, &lo, &hi)?;
    }
    Ok(())
}

fn bounds_strategy() -> impl Strategy<Value = RawBounds> {
    (
        proptest::option::of((0..=KEY_SPACE, any::<bool>())),
        proptest::option::of((0..=KEY_SPACE, any::<bool>())),
    )
}

proptest! {
    #![proptest_config(proptest_config())]

    #[test]
    fn long_chains_scan_as_the_model_resolves_them(
        ops in prop::collection::vec(op_strategy(false), 1..40),
        reads in prop::collection::vec(any::<u64>(), 1..4),
        bounds in bounds_strategy(),
    ) {
        run(Variant::Lexicographic, ops, reads, bounds)?;
    }

    #[test]
    fn long_chains_under_a_reverse_comparator(
        ops in prop::collection::vec(op_strategy(false), 1..40),
        reads in prop::collection::vec(any::<u64>(), 1..4),
        bounds in bounds_strategy(),
    ) {
        run(Variant::Reverse, ops, reads, bounds)?;
    }

    #[test]
    fn long_merge_chains_fold_as_the_model_folds_them(
        ops in prop::collection::vec(op_strategy(true), 1..40),
        reads in prop::collection::vec(any::<u64>(), 1..4),
        bounds in bounds_strategy(),
    ) {
        run(Variant::Merge, ops, reads, bounds)?;
    }
}

/// A reverse scan with a lower bound over one table whose chain the back
/// walks to its end: the block's back is used up when the jump asks it, and
/// must not hand out an older version again in place of the newest.
#[test]
fn a_used_up_block_back_keeps_the_newest_version() -> Result<(), TestCaseError> {
    run(
        Variant::Reverse,
        vec![
            Op::Burst { key: 2, n: 10 },
            Op::Del { key: 4 },
            Op::Flush,
            Op::Put { key: 3, value: 0 },
        ],
        vec![0],
        (Some((3, false)), None),
    )?;
    run(
        Variant::Lexicographic,
        vec![
            Op::Burst { key: 3, n: 10 },
            Op::Put { key: 0, value: 0 },
            Op::Flush,
            Op::Del { key: 0 },
            Op::Burst { key: 0, n: 11 },
            Op::Put { key: 0, value: 0 },
            Op::Put { key: 0, value: 0 },
            Op::Put { key: 0, value: 0 },
            Op::Put { key: 0, value: 0 },
        ],
        vec![0],
        (Some((1, false)), None),
    )
}

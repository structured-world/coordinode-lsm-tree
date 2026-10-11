// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A read bound longer than any key can be (65,535 bytes) is a legal search
//! needle: no key reaches it, but keys sort below and above it. Range, prefix
//! and point reads take one and answer as they would any other bound.

use lsm_tree::{AbstractTree, Config, Guard as _, SeqNo, SharedComparator, UserComparator};
use std::cmp::Ordering;
use std::ops::Bound;
use std::sync::Arc;

/// Orders keys in reverse byte order.
struct ReverseComparator;

impl UserComparator for ReverseComparator {
    fn name(&self) -> &'static str {
        "reverse-lexicographic"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        b.cmp(a)
    }
}

const LONG: usize = 70_000;

/// Keys in a table, a sealed memtable and the active one.
fn tree(
    folder: &std::path::Path,
    comparator: SharedComparator,
) -> lsm_tree::Result<lsm_tree::AnyTree> {
    let tree = Config::new(folder, Default::default(), Default::default())
        .comparator(comparator)
        .open()?;
    for (key, seqno) in ["a", "b", "c", "d", "e", "f"].into_iter().zip(1..) {
        tree.insert(key, "table", seqno);
    }
    tree.flush_active_memtable(0)?;
    tree.insert("c", "sealed", 10);
    tree.insert("bz", "sealed", 11);
    assert!(tree.rotate_memtable().is_some(), "a sealed memtable");
    tree.insert("d", "active", 12);
    tree.insert("dz", "active", 13);
    Ok(tree)
}

fn keys(iter: impl Iterator<Item = lsm_tree::IterGuardImpl>) -> Vec<Vec<u8>> {
    iter.map(|guard| guard.into_inner().expect("read").0.to_vec())
        .collect()
}

fn in_bounds(
    cmp: &dyn UserComparator,
    key: &[u8],
    (lo, hi): &(Bound<Vec<u8>>, Bound<Vec<u8>>),
) -> bool {
    (match lo {
        Bound::Included(b) => cmp.compare(key, b) != Ordering::Less,
        Bound::Excluded(b) => cmp.compare(key, b) == Ordering::Greater,
        Bound::Unbounded => true,
    }) && (match hi {
        Bound::Included(b) => cmp.compare(key, b) != Ordering::Greater,
        Bound::Excluded(b) => cmp.compare(key, b) == Ordering::Less,
        Bound::Unbounded => true,
    })
}

fn check(comparator: SharedComparator) -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree(folder.path(), comparator.clone())?;
    let all = keys(tree.iter(SeqNo::MAX, None));
    assert_eq!(all.len(), 8);

    let long_b = vec![b'b'; LONG];
    let long_d = vec![b'd'; LONG];
    let bounds = [
        (
            Bound::Included(long_b.clone()),
            Bound::Included(long_d.clone()),
        ),
        (
            Bound::Excluded(long_b.clone()),
            Bound::Excluded(long_d.clone()),
        ),
        (
            Bound::Included(long_d.clone()),
            Bound::Included(long_b.clone()),
        ),
        (Bound::Excluded(long_b.clone()), Bound::Unbounded),
        (Bound::Unbounded, Bound::Included(long_d.clone())),
        (
            Bound::Included(b"b".to_vec()),
            Bound::Excluded(long_d.clone()),
        ),
    ];
    for range in bounds {
        let expected: Vec<Vec<u8>> = all
            .iter()
            .filter(|key| in_bounds(comparator.as_ref(), key, &range))
            .cloned()
            .collect();
        let got = keys(tree.range::<Vec<u8>, _>(range.clone(), SeqNo::MAX, None));
        assert_eq!(got, expected, "{:?}", comparator.name());
        let mut back = keys(
            tree.range::<Vec<u8>, _>(range.clone(), SeqNo::MAX, None)
                .rev(),
        );
        back.reverse();
        assert_eq!(back, expected, "{:?}, backwards", comparator.name());
    }

    assert_eq!(tree.get(vec![b'c'; LONG], SeqNo::MAX)?, None);
    assert!(keys(tree.prefix(vec![b'c'; LONG], SeqNo::MAX, None)).is_empty());
    Ok(())
}

/// Range, prefix and point reads take a bound longer than any key in byte
/// order.
#[test]
fn reads_take_a_bound_longer_than_any_key() -> lsm_tree::Result<()> {
    check(Arc::new(lsm_tree::DefaultUserComparator))
}

/// The same under a comparator of another order.
#[test]
fn reads_take_a_bound_longer_than_any_key_under_a_custom_comparator() -> lsm_tree::Result<()> {
    check(Arc::new(ReverseComparator))
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::KeyBounds;
use crate::UserKey;
use core::ops::Bound;

fn bounds(lo: Bound<&str>, hi: Bound<&str>) -> KeyBounds {
    KeyBounds {
        lo: lo.map(|k| UserKey::from(k.as_bytes())),
        hi: hi.map(|k| UserKey::from(k.as_bytes())),
        comparator: crate::comparator::default_comparator(),
    }
}

/// Each bound keeps exactly the keys its kind names: an included end keeps
/// the key itself, an excluded one drops it, an unbounded one keeps all.
#[test]
fn key_bounds_contains_follows_each_bound_kind() {
    let included = bounds(Bound::Included("b"), Bound::Included("d"));
    assert!(!included.contains(b"a"));
    assert!(included.contains(b"b"));
    assert!(included.contains(b"d"));
    assert!(!included.contains(b"e"));

    let excluded = bounds(Bound::Excluded("b"), Bound::Excluded("d"));
    assert!(!excluded.contains(b"b"));
    assert!(excluded.contains(b"c"));
    assert!(!excluded.contains(b"d"));

    let open = bounds(Bound::Unbounded, Bound::Unbounded);
    assert!(open.contains(b""));
    assert!(open.contains(b"zzz"));
}

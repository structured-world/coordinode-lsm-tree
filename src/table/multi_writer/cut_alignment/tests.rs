// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::{Boundary, CutAlignment, share_of};
use crate::{UserKey, comparator::default_comparator};

fn key(i: u32) -> Vec<u8> {
    format!("{i:06}").into_bytes()
}

fn boundaries(keys: &[u32]) -> alloc::sync::Arc<[Boundary]> {
    keys.iter()
        .map(|&i| Boundary {
            key: UserKey::from(key(i)),
            after_bytes: u64::from(i),
        })
        .collect()
}

/// Boundaries below the writer's first key are passed without being counted,
/// and a key equal to a boundary does not cross it: it belongs to the table
/// whose largest key it is.
#[test]
fn boundaries_below_the_first_key_are_not_counted() {
    let cmp = default_comparator();
    let mut align = CutAlignment::new(boundaries(&[10, 20, 30, 40]), None);

    let first = align.crossing(&key(25), cmp.as_ref());
    assert_eq!((first.passed, first.counted), (2, 0));
    align.commit(first, false);

    let at_boundary = align.crossing(&key(30), cmp.as_ref());
    assert_eq!(
        at_boundary.passed, 0,
        "a key equal to a boundary is not past it"
    );

    let past = align.crossing(&key(31), cmp.as_ref());
    assert_eq!((past.passed, past.counted), (1, 1));
    assert_eq!(align.floor_percent(past), 50);
}

/// Each boundary an output passes without being cut raises its floor by five
/// percent, up to ninety; a cut starts the next output at zero.
#[test]
fn the_floor_rises_with_the_boundaries_crossed_and_resets_at_a_cut() {
    let cmp = default_comparator();
    let marks: Vec<u32> = (1..=12).map(|i| i * 10).collect();
    let mut align = CutAlignment::new(boundaries(&marks), None);
    align.commit(align.crossing(&key(5), cmp.as_ref()), false);

    let mut floors = Vec::new();
    for i in 1..=11 {
        let crossing = align.crossing(&key(i * 10 + 1), cmp.as_ref());
        floors.push(align.floor_percent(crossing));
        align.commit(crossing, false);
    }
    assert_eq!(floors, [50, 55, 60, 65, 70, 75, 80, 85, 90, 90, 90]);

    let crossing = align.crossing(&key(121), cmp.as_ref());
    align.commit(crossing, true);
    let after_cut = align.crossing(&key(200), cmp.as_ref());
    assert_eq!(after_cut.passed, 0, "every boundary is behind");
    assert_eq!(
        align.floor_percent(after_cut),
        50,
        "a new output starts at zero"
    );
}

/// Several boundaries passed at one key all count toward the output.
#[test]
fn boundaries_passed_at_one_key_all_count() {
    let cmp = default_comparator();
    let mut align = CutAlignment::new(boundaries(&[10, 20, 30]), None);
    align.commit(align.crossing(&key(1), cmp.as_ref()), false);

    let crossing = align.crossing(&key(35), cmp.as_ref());
    assert_eq!((crossing.passed, crossing.counted), (3, 3));
    assert_eq!(align.floor_percent(crossing), 60);
}

/// No boundary is ahead past the last one, nor past the writer's last key: a
/// boundary at or above it is crossed by none of the writer's keys.
#[test]
fn no_boundary_is_ahead_past_the_last_or_the_writer_s_range() {
    let cmp = default_comparator();
    let mut align = CutAlignment::new(boundaries(&[10, 20]), None);
    align.commit(align.crossing(&key(1), cmp.as_ref()), false);
    let at = align.crossing(&key(15), cmp.as_ref());
    assert!(align.ahead(at, cmp.as_ref()));
    let past = align.crossing(&key(25), cmp.as_ref());
    assert!(!align.ahead(past, cmp.as_ref()), "past the last boundary");

    let mut bounded = CutAlignment::new(boundaries(&[10, 20]), None);
    bounded.limit(UserKey::from(key(20)));
    bounded.commit(bounded.crossing(&key(1), cmp.as_ref()), false);
    let at = bounded.crossing(&key(15), cmp.as_ref());
    assert!(
        !bounded.ahead(at, cmp.as_ref()),
        "a boundary at the writer's last key is crossed by none of its keys"
    );
}

/// The boundary inside a run of keys is the first past those its first key
/// crosses, and lies below its last key.
#[cfg(feature = "columnar")]
#[test]
fn inside_finds_the_boundary_between_a_run_of_keys() {
    let cmp = default_comparator();
    let mut align = CutAlignment::new(boundaries(&[10, 20, 30]), None);
    align.commit(align.crossing(&key(1), cmp.as_ref()), false);

    let at_first = align.crossing(&key(15), cmp.as_ref());
    let inside = align.inside(at_first, &key(25), cmp.as_ref());
    assert_eq!(inside.map(|b| b.after_bytes), Some(20));
    assert!(
        align.inside(at_first, &key(20), cmp.as_ref()).is_none(),
        "a run ending on the boundary does not cross it"
    );

    align.commit(at_first, false);
    let mut below_half = align.clone();
    align.pass_through(&key(25), true, cmp.as_ref());
    let next = align.crossing(&key(35), cmp.as_ref());
    assert_eq!(next.passed, 1, "the boundary inside the run is behind");
    assert_eq!(
        align.floor_percent(next),
        60,
        "and counted toward the output"
    );

    below_half.pass_through(&key(25), false, cmp.as_ref());
    let next = below_half.crossing(&key(35), cmp.as_ref());
    assert_eq!(next.passed, 1, "passed below half the target");
    assert_eq!(below_half.floor_percent(next), 55, "but not counted");
}

/// A share of the largest target is clamped rather than wrapped.
#[test]
fn share_of_a_huge_target_is_clamped() {
    assert_eq!(share_of(1_000, 55), 550);
    assert_eq!(share_of(u64::MAX, 200), u64::MAX);
    assert_eq!(share_of(u64::MAX, 50), u64::MAX / 2);
}

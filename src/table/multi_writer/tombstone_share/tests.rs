// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::TombstoneShare;
use crate::{UserKey, comparator::default_comparator, range_tombstone::RangeTombstone};

fn key(i: u64) -> Vec<u8> {
    format!("{i:08}").into_bytes()
}

/// The tracking holds the tombstones open at the current key, not every
/// tombstone of the run: a run of disjoint ones keeps it at a few entries,
/// however many there are.
#[test]
fn the_share_tracks_only_the_open_tombstones() {
    const TOMBSTONES: u64 = 20_000;

    let comparator = default_comparator();
    let tombstones: Vec<_> = (0..TOMBSTONES)
        .map(|i| {
            let mut start = key(i);
            start.push(0);
            let mut end = key(i);
            end.push(1);
            RangeTombstone::new(UserKey::from(start), UserKey::from(end), 1)
        })
        .collect();
    let mut share = TombstoneShare::new();
    for i in 0..=TOMBSTONES {
        share.advance(&tombstones, &key(i), comparator.as_ref());
    }
    assert!(share.tracked() <= 8, "{} tracking entries", share.tracked());
}

/// A tombstone starting past the key advanced to and at or before the last
/// key of a run would open within it; one starting at the key advanced to
/// opened there already, and one starting past the run opens after it.
#[cfg(feature = "columnar")]
#[test]
fn starts_within_finds_a_tombstone_opening_inside_a_run() {
    let comparator = default_comparator();
    let tombstone = |start: u64, end: u64| {
        RangeTombstone::new(UserKey::from(key(start)), UserKey::from(key(end)), 1)
    };
    let tombstones = vec![tombstone(10, 11), tombstone(20, 21), tombstone(30, 31)];
    let mut share = TombstoneShare::new();
    share.advance(&tombstones, &key(10), comparator.as_ref());

    let within = |through: u64| {
        share.starts_within(&tombstones, (&key(10), &key(through)), comparator.as_ref())
    };
    assert!(!within(19), "the next one starts past the run");
    assert!(within(20), "the next one starts at the run's last key");
    assert!(within(25));
    // Past every tombstone, none is pending.
    share.advance(&tombstones, &key(40), comparator.as_ref());
    assert!(!share.starts_within(&tombstones, (&key(40), &key(50)), comparator.as_ref()));
}

/// Overlapping tombstones of scattered ends: at every key the share equals
/// the bytes of the pieces an output from the last boundary would hold,
/// counted directly.
#[test]
fn the_share_matches_the_pieces_counted_directly() {
    let comparator = default_comparator();
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let mut next = |bound: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % bound
    };
    let mut tombstones: Vec<_> = (0..300)
        .map(|_| {
            let start = next(1_000);
            let end = start + 1 + next(200);
            RangeTombstone::new(UserKey::from(key(start)), UserKey::from(key(end)), 1)
        })
        .collect();
    tombstones.sort_by(|a, b| a.start.cmp(&b.start));

    let mut share = TombstoneShare::new();
    let mut lower: Option<Vec<u8>> = None;
    for i in (0..1_300).step_by(7) {
        let at = key(i);
        share.advance(&tombstones, &at, comparator.as_ref());
        let direct: u64 = tombstones
            .iter()
            .filter_map(|rt| {
                let start = match &lower {
                    Some(lower) if rt.start.as_ref() < lower.as_slice() => lower.as_slice(),
                    _ => rt.start.as_ref(),
                };
                let end = if rt.end.as_ref() > at.as_slice() {
                    at.as_slice()
                } else {
                    rt.end.as_ref()
                };
                (rt.start.as_ref() < at.as_slice() && start < end)
                    .then(|| (12 + start.len() + end.len()) as u64)
            })
            .sum();
        assert_eq!(share.bytes(&at), direct, "at {i}");
        if i % 91 == 0 {
            share.open_output(&at);
            lower = Some(at);
        }
    }
}

/// The bytes follow the entries each output holds: a tombstone open across
/// a boundary counts in both outputs, cut to each.
#[test]
fn the_share_counts_the_pieces_of_the_current_output() {
    let comparator = default_comparator();
    let tombstones = vec![
        RangeTombstone::new(
            UserKey::from(b"b" as &[u8]),
            UserKey::from(b"d" as &[u8]),
            1,
        ),
        RangeTombstone::new(
            UserKey::from(b"c" as &[u8]),
            UserKey::from(b"k" as &[u8]),
            1,
        ),
    ];
    let mut share = TombstoneShare::new();
    share.advance(&tombstones, b"a", comparator.as_ref());
    assert_eq!(share.bytes(b"a"), 0);
    // Both open at "e"; [b, d) closed by then.
    share.advance(&tombstones, b"e", comparator.as_ref());
    assert_eq!(share.bytes(b"e"), (12 + 1 + 1) + (12 + 1 + 1));
    // A new output from "e": [c, k) continues as [e, ..).
    share.open_output(b"e");
    share.advance(&tombstones, b"m", comparator.as_ref());
    assert_eq!(share.bytes(b"m"), 12 + 1 + 1);
}

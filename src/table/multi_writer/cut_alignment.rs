// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Where a compaction output prefers to end: between two tables of the level
//! its outputs are merged into next.
//!
//! An output whose key range straddles such a boundary drags both tables into
//! that merge; one cut at it drags one. The rule is Pebble's output splitter
//! (`internal/compact/splitting.go`, `shouldSplitBasedOnSize`), `RocksDB`'s
//! aligned cut with one refinement. A cut is taken only before a new user key;
//! with `n` the boundaries the output crossed since it held half the target,
//! counting the one at hand:
//!
//! 1. below half the target, never;
//! 2. at twice the target, always;
//! 3. at a boundary, once the output holds `50 + 5 * min(n - 1, 8)` percent of
//!    the target;
//! 4. with no boundary ahead in the writer's range, at the target: waiting for
//!    a boundary that will not come only grows the output.
//!
//! The boundaries are walked in order with the stream, a merge against the
//! sorted list rather than a search per key.

use crate::UserKey;
use crate::comparator::UserComparator;
use alloc::sync::Arc;
use core::cmp::Ordering;

/// A boundary between two adjacent tables of one run of the level below.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Boundary {
    /// The largest key of the table before the boundary.
    pub key: UserKey,
    /// The bytes of the table after it: what an output straddling the
    /// boundary has the next merge read and write again.
    pub after_bytes: u64,
}

/// Where a key falls among the boundaries, decided before anything is
/// committed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Crossing {
    /// Boundaries the key passes.
    pub(super) passed: usize,
    /// Those that count toward the output's floor: passed after its first key,
    /// once it holds half the target.
    pub(super) counted: u32,
}

/// The boundaries of one writer and its position among them.
#[derive(Clone, Debug)]
pub(super) struct CutAlignment {
    boundaries: Arc<[Boundary]>,
    /// The first boundary not yet passed.
    next: usize,
    /// Boundaries the current output crossed since it held half the target.
    crossed: u32,
    /// Whether the writer took its first key: boundaries below it belong to no
    /// output of this writer and are not counted.
    started: bool,
    /// The last key the writer can be handed, when it is told: a boundary at or
    /// past it is crossed by none of its keys, so it is not ahead.
    upper: Option<UserKey>,
}

impl CutAlignment {
    pub(super) fn new(boundaries: Arc<[Boundary]>, upper: Option<UserKey>) -> Self {
        Self {
            boundaries,
            next: 0,
            crossed: 0,
            started: false,
            upper,
        }
    }

    /// Bounds the writer's keys by `upper`.
    pub(super) fn limit(&mut self, upper: UserKey) {
        self.upper = Some(upper);
    }

    /// The boundaries `key`, the next key written, passes: those whose key
    /// sorts below it.
    pub(super) fn crossing(&self, key: &[u8], comparator: &dyn UserComparator) -> Crossing {
        let passed = self
            .boundaries
            .get(self.next..)
            .unwrap_or_default()
            .iter()
            .take_while(|boundary| comparator.compare(&boundary.key, key) == Ordering::Less)
            .count();
        let counted = if self.started {
            u32::try_from(passed).unwrap_or(u32::MAX)
        } else {
            0
        };
        Crossing { passed, counted }
    }

    /// Commits `crossing`, the key it was decided at going into the output
    /// that took it: a new one when `cut`.
    pub(super) fn commit(&mut self, crossing: Crossing, cut: bool) {
        self.next += crossing.passed;
        self.started = true;
        self.crossed = if cut {
            0
        } else {
            self.crossed.saturating_add(crossing.counted)
        };
    }

    /// The share of the target, in percent, an output must hold to be cut at
    /// a boundary when it crosses `crossing` there.
    pub(super) fn floor_percent(&self, crossing: Crossing) -> u64 {
        let n = self.crossed.saturating_add(crossing.counted);
        50 + 5 * u64::from(n.saturating_sub(1).min(8))
    }

    /// Whether a boundary is ahead of the boundaries `crossing` passes, one a
    /// key of this writer can still cross.
    pub(super) fn ahead(&self, crossing: Crossing, comparator: &dyn UserComparator) -> bool {
        self.boundaries
            .get(self.next + crossing.passed)
            .is_some_and(|boundary| {
                self.upper
                    .as_ref()
                    .is_none_or(|upper| comparator.compare(&boundary.key, upper) == Ordering::Less)
            })
    }

    /// The first boundary a run of keys from `first` to `last` crosses after
    /// `first`, past the boundaries `crossing` passes at `first`: one between
    /// two of its keys, where an output written key by key could end.
    pub(super) fn inside(
        &self,
        crossing: Crossing,
        last: &[u8],
        comparator: &dyn UserComparator,
    ) -> Option<&Boundary> {
        self.boundaries
            .get(self.next + crossing.passed)
            .filter(|boundary| comparator.compare(&boundary.key, last) == Ordering::Less)
    }

    /// Moves past the boundaries the keys up to `last` cross, written into the
    /// current output without a cut among them; they count toward its floor
    /// when it holds half the target, `past_half`.
    pub(super) fn pass_through(
        &mut self,
        last: &[u8],
        past_half: bool,
        comparator: &dyn UserComparator,
    ) {
        let mut crossing = self.crossing(last, comparator);
        if !past_half {
            crossing.counted = 0;
        }
        self.commit(crossing, false);
    }
}

/// The share `percent` of `target`, clamped to the largest size: a share past
/// it is one no output reaches.
pub(super) fn share_of(target: u64, percent: u64) -> u64 {
    u64::try_from(u128::from(target) * u128::from(percent) / 100).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests;

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The range tombstones one scan reads under, and what they suppress.
//!
//! A scan's snapshot fixes which tombstones it sees, so for suppression each
//! stretch of keys comes down to one seqno: the newest tombstone visible over
//! it. That holds for one scan only: another snapshot sees other tombstones.

use crate::{SeqNo, UserKey, comparator::UserComparator, range_tombstone::RangeTombstone};
use alloc::{boxed::Box, collections::BinaryHeap, vec::Vec};
use core::cmp::Ordering;
use once_cell::race::OnceBox;

/// A scan's tombstones, shared by its stages and across its repositions.
pub(crate) struct ScanTombstones {
    /// Each with its source's cutoff, in [`RangeTombstone::cmp_with_comparator`]
    /// order, no duplicates.
    list: Vec<(RangeTombstone, SeqNo)>,
    /// Built by the first check: a scan without merge operands makes none.
    index: OnceBox<Floors>,
}

/// The stretches between tombstone bounds and the newest visible tombstone
/// over each.
struct Floors {
    /// Ascending. Stretch `i` runs from `bounds[i]` up to `bounds[i + 1]`.
    bounds: Vec<UserKey>,
    /// What stretch `i` suppresses: every version below this seqno. `0`
    /// suppresses nothing, no version being below it.
    floors: Vec<SeqNo>,
    /// The highest of `floors`: a version at or above it needs no bound found.
    highest: SeqNo,
}

impl ScanTombstones {
    /// `list` must be in [`RangeTombstone::cmp_with_comparator`] order.
    pub(crate) fn new(list: Vec<(RangeTombstone, SeqNo)>) -> Self {
        Self {
            list,
            index: OnceBox::new(),
        }
    }

    /// The tombstones, each with its cutoff.
    pub(crate) fn list(&self) -> &[(RangeTombstone, SeqNo)] {
        &self.list
    }

    /// Whether a tombstone visible at its cutoff suppresses `seqno` of `key`.
    pub(crate) fn suppresses(
        &self,
        key: &[u8],
        seqno: SeqNo,
        comparator: &dyn UserComparator,
    ) -> bool {
        if self.list.is_empty() {
            return false;
        }
        self.index
            .get_or_init(|| Box::new(Floors::new(&self.list, comparator)))
            .suppresses(key, seqno, comparator)
    }
}

impl Floors {
    fn new(list: &[(RangeTombstone, SeqNo)], comparator: &dyn UserComparator) -> Self {
        // `list` is sorted by start, and leaving some out keeps it so.
        let visible: Vec<&RangeTombstone> = list
            .iter()
            .filter(|(rt, cutoff)| rt.visible_at(*cutoff))
            .map(|(rt, _)| rt)
            .collect();

        let mut bounds: Vec<&UserKey> =
            visible.iter().flat_map(|rt| [&rt.start, &rt.end]).collect();
        bounds.sort_unstable_by(|a, b| comparator.compare(a, b));
        bounds.dedup_by(|a, b| comparator.compare(a, b) == Ordering::Equal);

        // A sweep: a tombstone joins at its start, and the newest one that
        // has not ended is what a stretch comes to. One that ended stays in
        // the heap until it would be that newest.
        let mut floors = Vec::with_capacity(bounds.len());
        let mut covering: BinaryHeap<(SeqNo, usize)> = BinaryHeap::new();
        let mut starting = visible.iter().enumerate().peekable();
        for bound in &bounds {
            while let Some((at, rt)) = starting
                .next_if(|(_, rt)| comparator.compare(&rt.start, bound) != Ordering::Greater)
            {
                covering.push((rt.seqno, at));
            }
            while let Some(&(_, at)) = covering.peek() {
                let ended = visible
                    .get(at)
                    .is_none_or(|rt| comparator.compare(&rt.end, bound) != Ordering::Greater);
                if !ended {
                    break;
                }
                covering.pop();
            }
            floors.push(covering.peek().map_or(0, |&(seqno, _)| seqno));
        }

        Self {
            bounds: bounds.into_iter().cloned().collect(),
            highest: floors.iter().copied().max().unwrap_or(0),
            floors,
        }
    }

    fn suppresses(&self, key: &[u8], seqno: SeqNo, comparator: &dyn UserComparator) -> bool {
        if seqno >= self.highest {
            return false;
        }
        let reached = self
            .bounds
            .partition_point(|bound| comparator.compare(bound, key) != Ordering::Greater);
        reached
            .checked_sub(1)
            .and_then(|stretch| self.floors.get(stretch))
            .is_some_and(|floor| seqno < *floor)
    }
}

#[cfg(test)]
mod tests;

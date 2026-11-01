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
    index: OnceBox<Stretches>,
}

/// The stretches between tombstone bounds and the newest visible tombstone
/// over each.
struct Stretches {
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
            .get_or_init(|| Box::new(Stretches::new(&self.list, comparator)))
            .suppresses(key, seqno, comparator)
    }
}

/// Where a tombstone begins or stops counting.
#[derive(Clone, Copy)]
struct Edge<'a> {
    key: &'a UserKey,
    starts: bool,
    seqno: SeqNo,
    /// The tombstone's place in the list.
    at: usize,
}

impl Stretches {
    fn new(list: &[(RangeTombstone, SeqNo)], comparator: &dyn UserComparator) -> Self {
        // Every start and end, a tombstone's start and end side by side: a
        // list of tombstones that do not overlap comes out already sorted,
        // which the sort sees in one pass.
        let mut edges: Vec<Edge<'_>> = Vec::with_capacity(list.len() * 2);
        for (at, (rt, cutoff)) in list.iter().enumerate() {
            if rt.visible_at(*cutoff) {
                edges.extend([
                    Edge {
                        key: &rt.start,
                        starts: true,
                        seqno: rt.seqno,
                        at,
                    },
                    Edge {
                        key: &rt.end,
                        starts: false,
                        seqno: rt.seqno,
                        at,
                    },
                ]);
            }
        }
        // An end before a start on the same key: the end excludes that key,
        // and a tombstone that ends where or before it starts never joins.
        edges.sort_unstable_by(|a, b| {
            comparator
                .compare(a.key, b.key)
                .then(a.starts.cmp(&b.starts))
        });

        // A sweep over the edges, which needs no more key comparisons than
        // telling bounds apart. A tombstone that ended stays in the heap
        // until it would be the newest.
        let mut bounds = Vec::with_capacity(edges.len());
        let mut floors = Vec::with_capacity(edges.len());
        let mut ended = vec![false; list.len()];
        let mut covering: BinaryHeap<(SeqNo, usize)> = BinaryHeap::new();
        let mut edges = edges.iter().peekable();
        while let Some(edge) = edges.next() {
            if let Some(ended) = ended.get_mut(edge.at) {
                if !edge.starts {
                    *ended = true;
                } else if !*ended {
                    covering.push((edge.seqno, edge.at));
                }
            }
            // The floor of a bound counts every edge on it.
            if edges
                .peek()
                .is_some_and(|next| comparator.compare(next.key, edge.key) == Ordering::Equal)
            {
                continue;
            }
            while let Some(&(_, at)) = covering.peek() {
                if !ended.get(at).copied().unwrap_or(true) {
                    break;
                }
                covering.pop();
            }
            bounds.push(edge.key.clone());
            floors.push(covering.peek().map_or(0, |&(seqno, _)| seqno));
        }

        Self {
            bounds,
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

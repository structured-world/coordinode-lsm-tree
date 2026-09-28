// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The bytes the current output's range-tombstone block takes, kept as the
//! keys advance, so a table can rotate before its tombstones push it past its
//! target.
//!
//! An output receives each tombstone overlapping its zone, cut to it: the
//! start raised to the zone's lower bound, the end lowered to the next output's
//! first key. A block entry takes 12 bytes (two length prefixes and the seqno)
//! plus its start and end. If the output closed at the current key, a
//! tombstone ending past it would end at it; so the share is the entries fixed
//! so far plus, for each tombstone still open, the current key's length.
//!
//! Only the tombstones open at the current key are tracked, in a heap by end:
//! the ones not yet open follow from the input's order by start, the closed
//! ones are done with. The tracking stays at the tombstones spanning one key,
//! however many the run holds.

use crate::{comparator::UserComparator, range_tombstone::RangeTombstone};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::cmp::Ordering;

/// Bytes a block entry takes besides its start and end.
const ENTRY_OVERHEAD: u64 = 2 + 2 + 8;

pub(super) struct TombstoneShare {
    /// Indices of the open tombstones, a min-heap by end under the comparator.
    open: Vec<usize>,
    /// The next tombstone, by start, not yet open.
    next_start: usize,
    /// Bytes of the current output's entries except the ends still open.
    fixed: u64,
    /// Entries of the current output, one per tombstone piece.
    pieces: u64,
}

impl TombstoneShare {
    /// Follows tombstones ordered by start.
    pub(super) fn new() -> Self {
        Self {
            open: Vec::new(),
            next_start: 0,
            fixed: 0,
            pieces: 0,
        }
    }

    /// Moves to `key`, the next key written: opens the tombstones starting
    /// before it, closes those ending at or before it.
    pub(super) fn advance(
        &mut self,
        tombstones: &[RangeTombstone],
        key: &[u8],
        comparator: &dyn UserComparator,
    ) {
        self.open_while(tombstones, key, comparator, Ordering::Less);
        while let Some(tombstone) = self.open.first().and_then(|&i| tombstones.get(i))
            && comparator.compare(&tombstone.end, key) != Ordering::Greater
        {
            self.fixed += tombstone.end.len() as u64;
            pop(&mut self.open, tombstones, comparator);
        }
    }

    /// Opens the tombstones starting at `key` too, the point last advanced to.
    pub(super) fn open_through(
        &mut self,
        tombstones: &[RangeTombstone],
        key: &[u8],
        comparator: &dyn UserComparator,
    ) {
        self.open_while(tombstones, key, comparator, Ordering::Equal);
    }

    /// Opens pending tombstones whose start compares to `key` as `ordering`
    /// or less.
    fn open_while(
        &mut self,
        tombstones: &[RangeTombstone],
        key: &[u8],
        comparator: &dyn UserComparator,
        ordering: Ordering,
    ) {
        while let Some(tombstone) = tombstones.get(self.next_start)
            && comparator.compare(&tombstone.start, key) <= ordering
        {
            self.fixed += ENTRY_OVERHEAD + tombstone.start.len() as u64;
            self.pieces += 1;
            push(&mut self.open, self.next_start, tombstones, comparator);
            self.next_start += 1;
        }
    }

    /// A new output begins at `lower`, the key last advanced to: it takes the
    /// tombstones still open, each starting at `lower`.
    pub(super) fn open_output(&mut self, lower: &[u8]) {
        self.fixed = self.open.len() as u64 * (ENTRY_OVERHEAD + lower.len() as u64);
        self.pieces = self.open.len() as u64;
    }

    /// Bytes the current output's entries take if it closes at `key`, the key
    /// last advanced to.
    pub(super) fn bytes(&self, key: &[u8]) -> u64 {
        self.fixed + self.open.len() as u64 * key.len() as u64
    }

    /// Entries the current output holds.
    pub(super) fn pieces(&self) -> u64 {
        self.pieces
    }

    /// Tombstones open at the key last advanced to: the entries an output
    /// beginning there starts with.
    pub(super) fn open_count(&self) -> u64 {
        self.open.len() as u64
    }

    /// Bytes an output beginning at `key` starts with: a piece of every
    /// tombstone still open, from `key`, its end at least as long.
    pub(super) fn carry(&self, key: &[u8]) -> u64 {
        self.open.len() as u64 * (ENTRY_OVERHEAD + 2 * key.len() as u64)
    }

    /// Bytes of the pending tombstones starting at `key`, whole.
    pub(super) fn group_bytes(
        &self,
        tombstones: &[RangeTombstone],
        key: &[u8],
        comparator: &dyn UserComparator,
    ) -> u64 {
        tombstones
            .get(self.next_start..)
            .unwrap_or_default()
            .iter()
            .take_while(|rt| comparator.compare(&rt.start, key) == Ordering::Equal)
            .map(|rt| ENTRY_OVERHEAD + (rt.start.len() + rt.end.len()) as u64)
            .sum()
    }

    /// The least end among the open tombstones.
    pub(super) fn first_open_end<'t>(
        &self,
        tombstones: &'t [RangeTombstone],
    ) -> Option<&'t crate::UserKey> {
        self.open
            .first()
            .and_then(|&i| tombstones.get(i))
            .map(|rt| &rt.end)
    }

    /// Whether a tombstone remains open or pending.
    pub(super) fn has_more(&self, tombstones: &[RangeTombstone]) -> bool {
        !self.open.is_empty() || self.next_start < tombstones.len()
    }

    /// The first tombstone, by start, not yet open.
    pub(super) fn next_pending(&self) -> usize {
        self.next_start
    }

    /// Tracking entries allocated.
    #[cfg(test)]
    fn tracked(&self) -> usize {
        self.open.capacity()
    }
}

/// Whether tombstone `a` ends before tombstone `b`.
fn ends_before(
    tombstones: &[RangeTombstone],
    a: usize,
    b: usize,
    comparator: &dyn UserComparator,
) -> bool {
    match (tombstones.get(a), tombstones.get(b)) {
        (Some(a), Some(b)) => comparator.compare(&a.end, &b.end) == Ordering::Less,
        _ => false,
    }
}

fn push(
    heap: &mut Vec<usize>,
    index: usize,
    tombstones: &[RangeTombstone],
    comparator: &dyn UserComparator,
) {
    heap.push(index);
    let mut at = heap.len() - 1;
    while at > 0 {
        let parent = (at - 1) / 2;
        match (heap.get(at), heap.get(parent)) {
            (Some(&child), Some(&up)) if ends_before(tombstones, child, up, comparator) => {
                heap.swap(at, parent);
                at = parent;
            }
            _ => break,
        }
    }
}

fn pop(heap: &mut Vec<usize>, tombstones: &[RangeTombstone], comparator: &dyn UserComparator) {
    let Some(last) = heap.pop() else {
        return;
    };
    let Some(root) = heap.first_mut() else {
        return;
    };
    *root = last;
    let mut at = 0;
    loop {
        let mut least = at;
        for child in [2 * at + 1, 2 * at + 2] {
            if let (Some(&c), Some(&l)) = (heap.get(child), heap.get(least))
                && ends_before(tombstones, c, l, comparator)
            {
                least = child;
            }
        }
        if least == at {
            break;
        }
        heap.swap(at, least);
        at = least;
    }
}

#[cfg(test)]
mod tests;

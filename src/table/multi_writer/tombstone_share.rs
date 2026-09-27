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

use crate::{comparator::UserComparator, range_tombstone::RangeTombstone};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::cmp::Ordering;

/// Bytes a block entry takes besides its start and end.
const ENTRY_OVERHEAD: u64 = 2 + 2 + 8;

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    /// Starts at or after the current key.
    Pending,
    /// Started before the current key and ends after it.
    Open,
    /// Ended at or before the current key.
    Closed,
}

pub(super) struct TombstoneShare {
    /// Tombstone indices ordered by end.
    by_end: Vec<usize>,
    state: Vec<State>,
    /// The next tombstone, by start, not yet open.
    next_start: usize,
    /// The next tombstone, by end, not yet closed.
    next_end: usize,
    /// Bytes of the current output's entries except the ends still open.
    fixed: u64,
    /// Tombstones of the current output still open.
    open: u64,
}

impl TombstoneShare {
    /// Follows `tombstones`, which must be ordered by start.
    pub(super) fn new(tombstones: &[RangeTombstone], comparator: &dyn UserComparator) -> Self {
        let mut by_end: Vec<usize> = (0..tombstones.len()).collect();
        by_end.sort_by(|&a, &b| {
            comparator.compare(
                tombstones.get(a).map_or(&[][..], |t| &t.end),
                tombstones.get(b).map_or(&[][..], |t| &t.end),
            )
        });
        Self {
            by_end,
            state: alloc::vec![State::Pending; tombstones.len()],
            next_start: 0,
            next_end: 0,
            fixed: 0,
            open: 0,
        }
    }

    /// Moves to `key`, the next key written.
    pub(super) fn advance(
        &mut self,
        tombstones: &[RangeTombstone],
        key: &[u8],
        comparator: &dyn UserComparator,
    ) {
        while let Some(tombstone) = tombstones.get(self.next_start)
            && comparator.compare(&tombstone.start, key) == Ordering::Less
        {
            if let Some(state) = self.state.get_mut(self.next_start) {
                *state = State::Open;
            }
            self.fixed += ENTRY_OVERHEAD + tombstone.start.len() as u64;
            self.open += 1;
            self.next_start += 1;
        }
        while let Some(&index) = self.by_end.get(self.next_end)
            && let Some(tombstone) = tombstones.get(index)
            && comparator.compare(&tombstone.end, key) != Ordering::Greater
        {
            // A tombstone ends after it starts, so one ending here has opened.
            if let Some(state) = self.state.get_mut(index)
                && *state == State::Open
            {
                *state = State::Closed;
                self.fixed += tombstone.end.len() as u64;
                self.open -= 1;
            }
            self.next_end += 1;
        }
    }

    /// A new output begins at `lower`, the key last advanced to: it takes the
    /// tombstones still open, each starting at `lower`.
    pub(super) fn open_output(&mut self, lower: &[u8]) {
        self.fixed = self.open * (ENTRY_OVERHEAD + lower.len() as u64);
    }

    /// Bytes the current output's entries take if it closes at `key`, the key
    /// last advanced to.
    pub(super) fn bytes(&self, key: &[u8]) -> u64 {
        self.fixed + self.open * key.len() as u64
    }

    /// The first tombstone, by start, not yet open.
    pub(super) fn next_pending(&self) -> usize {
        self.next_start
    }
}

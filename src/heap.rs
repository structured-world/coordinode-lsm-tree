// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

//! Custom merge heap backed by a sorted vector.
//!
//! Supports both min and max extraction (for forward and reverse
//! iteration) on the same data structure, unlike two separate heaps.
//!
//! The key optimisation is `replace_min` / `replace_max`: replacing the
//! extremum and sliding the replacement into its sorted position.  In
//! the common case — a sequential scan where the same source keeps
//! winning — the replacement is still the extremum and the operation
//! completes in **one comparison** (O(1)).
//!
//! For the typical merge fan-in (n = 2–30 source iterators), a sorted
//! vector is competitive with a binary heap because:
//! - Cache-friendly sequential layout
//! - No tree-pointer overhead
//! - `memmove` of ≤30 entries is negligible

use crate::InternalValue;
use crate::comparator::SharedComparator;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::cmp::Ordering;

/// A single entry in the merge heap.
///
/// Comparator is stored once in the heap, not per entry — eliminating
/// the per-item `Arc` clone that the old `HeapItem` required.
pub struct HeapEntry {
    index: usize,
    value: InternalValue,
}

impl HeapEntry {
    pub fn new(index: usize, value: InternalValue) -> Self {
        Self { index, value }
    }

    #[inline]
    pub fn index(&self) -> usize {
        self.index
    }

    pub fn into_value(self) -> InternalValue {
        self.value
    }

    /// Compares two heap entries using the given comparator.
    ///
    /// Ties (same user key + same seqno) are broken by source index,
    /// with lower indices sorting first.  This ensures deterministic
    /// merge order; callers that need "newer wins" semantics must pass
    /// sources in newest-first precedence order.
    #[inline]
    fn cmp_with(&self, other: &Self, cmp: &dyn crate::comparator::UserComparator) -> Ordering {
        self.value
            .key
            .compare_with(&other.value.key, cmp)
            .then_with(|| self.index.cmp(&other.index))
    }
}

// ---------------------------------------------------------------------------
// MergeHeap
// ---------------------------------------------------------------------------

/// Merge heap backed by a sorted vector.
///
/// Entries are stored in ascending order: `data[0]` is the minimum,
/// `data[last]` is the maximum.  This makes both `pop_min` / `pop_max`
/// and `replace_min` / `replace_max` straightforward.
pub struct MergeHeap {
    data: Vec<HeapEntry>,
    comparator: SharedComparator,
}

impl MergeHeap {
    /// Creates an empty heap pre-allocated for `cap` entries.
    pub fn with_capacity(cap: usize, comparator: SharedComparator) -> Self {
        Self {
            data: Vec::with_capacity(cap),
            comparator,
        }
    }

    #[inline]
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns a reference to the minimum (first) entry.
    #[inline]
    pub fn peek_min(&self) -> Option<&HeapEntry> {
        self.data.first()
    }

    /// Returns a reference to the maximum (last) entry.
    #[inline]
    pub fn peek_max(&self) -> Option<&HeapEntry> {
        self.data.last()
    }

    /// Inserts a new entry, maintaining sorted order.
    pub fn push(&mut self, entry: HeapEntry) {
        let cmp = self.comparator.as_ref();
        let pos = self
            .data
            .partition_point(|e| e.cmp_with(&entry, cmp) != Ordering::Greater);
        self.data.insert(pos, entry);
    }

    /// Removes and returns the minimum entry.
    pub fn pop_min(&mut self) -> Option<HeapEntry> {
        if self.data.is_empty() {
            return None;
        }
        Some(self.data.remove(0))
    }

    /// Removes and returns the maximum entry.
    pub fn pop_max(&mut self) -> Option<HeapEntry> {
        self.data.pop()
    }

    /// Replaces the minimum entry and slides the replacement into its
    /// sorted position.
    ///
    /// Returns the old minimum.  In the common case (replacement is
    /// still the minimum), this completes in **one comparison**.
    ///
    /// # Panics
    ///
    /// Panics if the heap is empty.
    #[expect(
        clippy::indexing_slicing,
        reason = "bounds checked by debug_assert and loop guard"
    )]
    pub fn replace_min(&mut self, entry: HeapEntry) -> HeapEntry {
        debug_assert!(!self.data.is_empty());

        let old = core::mem::replace(&mut self.data[0], entry);

        // Slide right until in sorted position.
        let cmp = self.comparator.as_ref();
        let mut i = 0;
        while i + 1 < self.data.len()
            && self.data[i].cmp_with(&self.data[i + 1], cmp) == Ordering::Greater
        {
            self.data.swap(i, i + 1);
            i += 1;
        }

        old
    }

    /// Replaces the maximum entry and slides the replacement into its
    /// sorted position.
    ///
    /// Returns the old maximum.  In the common case (replacement is
    /// still the maximum), this completes in **one comparison**.
    ///
    /// # Panics
    ///
    /// Panics if the heap is empty.
    #[expect(
        clippy::indexing_slicing,
        reason = "bounds checked by debug_assert and loop guard"
    )]
    pub fn replace_max(&mut self, entry: HeapEntry) -> HeapEntry {
        debug_assert!(!self.data.is_empty());

        let last = self.data.len() - 1;
        let old = core::mem::replace(&mut self.data[last], entry);

        // Slide left until in sorted position.
        let cmp = self.comparator.as_ref();
        let mut i = last;
        while i > 0 && self.data[i].cmp_with(&self.data[i - 1], cmp) == Ordering::Less {
            self.data.swap(i, i - 1);
            i -= 1;
        }

        old
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test assertions use unwrap for brevity")]
mod tests;

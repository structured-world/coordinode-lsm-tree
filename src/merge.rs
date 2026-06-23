// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::InternalValue;
use crate::comparator::SharedComparator;
use crate::heap::{HeapEntry, MergeHeap};
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};

type IterItem = crate::Result<InternalValue>;

pub type BoxedIterator<'a> = Box<dyn DoubleEndedIterator<Item = IterItem> + Send + 'a>;

/// Merges multiple KV iterators into a single sorted stream.
///
/// Uses a custom sorted-vector heap with `replace_min` / `replace_max`
/// to avoid the double O(log n) cost of `pop` + `push` in the hot
/// path.  The comparator is stored once in the heap, not per entry —
/// eliminating per-item `Arc` ref-count traffic.
///
/// When two entries have the same user key and sequence number, the
/// entry from the iterator with the **lower index** (earlier position
/// in the `iterators` vec) sorts first.  Callers that need "newer
/// wins" semantics (e.g. compaction) must pass sources in
/// newest-first order.
pub struct Merger<I> {
    iterators: Vec<I>,
    heap: MergeHeap,
    initialized_lo: bool,
    initialized_hi: bool,
}

impl<I: Iterator<Item = IterItem>> Merger<I> {
    #[must_use]
    pub fn new(iterators: Vec<I>, comparator: SharedComparator) -> Self {
        // 2× capacity: mixed forward+reverse can buffer up to 2 entries per source.
        let heap = MergeHeap::with_capacity(2 * iterators.len(), comparator);

        Self {
            iterators,
            heap,
            initialized_lo: false,
            initialized_hi: false,
        }
    }

    fn initialize_lo(&mut self) -> crate::Result<()> {
        for (idx, it) in self.iterators.iter_mut().enumerate() {
            if let Some(item) = it.next() {
                let item = item?;
                self.heap.push(HeapEntry::new(idx, item));
            }
        }
        self.initialized_lo = true;
        Ok(())
    }
}

impl<I: DoubleEndedIterator<Item = IterItem>> Merger<I> {
    fn initialize_hi(&mut self) -> crate::Result<()> {
        for (idx, it) in self.iterators.iter_mut().enumerate() {
            if let Some(item) = it.next_back() {
                let item = item?;
                self.heap.push(HeapEntry::new(idx, item));
            }
        }
        self.initialized_hi = true;
        Ok(())
    }
}

impl<I: Iterator<Item = IterItem>> Iterator for Merger<I> {
    type Item = IterItem;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.initialized_lo {
            fail_iter!(self.initialize_lo());
        }

        // Read the source index of the current minimum (borrow ends
        // at the semicolon, so we can mutably borrow iterators next).
        let top_index = self.heap.peek_min()?.index();

        #[expect(clippy::indexing_slicing, reason = "we trust the HeapEntry index")]
        if let Some(next_result) = self.iterators[top_index].next() {
            match next_result {
                Ok(next_value) => {
                    // Replace the min in-place and slide into position.
                    // Common case (same source still wins): 1 comparison.
                    let old = self.heap.replace_min(HeapEntry::new(top_index, next_value));
                    Some(Ok(old.into_value()))
                }
                Err(e) => {
                    // Pop the stale entry so the next call makes progress
                    // on a different source instead of retrying this one.
                    let _ = self.heap.pop_min();
                    Some(Err(e))
                }
            }
        } else {
            // Source iterator exhausted — just remove.
            let old = self.heap.pop_min()?;
            Some(Ok(old.into_value()))
        }
    }
}

impl<I: DoubleEndedIterator<Item = IterItem>> DoubleEndedIterator for Merger<I> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if !self.initialized_hi {
            fail_iter!(self.initialize_hi());
        }

        let top_index = self.heap.peek_max()?.index();

        #[expect(clippy::indexing_slicing, reason = "we trust the HeapEntry index")]
        if let Some(next_result) = self.iterators[top_index].next_back() {
            match next_result {
                Ok(next_value) => {
                    let old = self.heap.replace_max(HeapEntry::new(top_index, next_value));
                    Some(Ok(old.into_value()))
                }
                Err(e) => {
                    let _ = self.heap.pop_max();
                    Some(Err(e))
                }
            }
        } else {
            let old = self.heap.pop_max()?;
            Some(Ok(old.into_value()))
        }
    }
}

#[cfg(test)]
#[allow(clippy::unnecessary_wraps)]
mod tests;

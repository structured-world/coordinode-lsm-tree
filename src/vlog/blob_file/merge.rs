// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::scanner::Scanner as BlobFileScanner;
use crate::comparator::SharedComparator;
use crate::vlog::{BlobFileId, blob_file::scanner::ScanEntry};
use alloc::vec::Vec;
use core::cmp::Ordering;

/// Interleaves multiple blob file readers into a single stream, ordered by
/// key under the tree's comparator, newest version first.
///
/// A relocating compaction walks this stream alongside the merged table
/// stream, which is in the comparator's order; merging by raw key bytes
/// instead would put the two out of step under any other ordering. The heap
/// is kept here rather than in a `BinaryHeap` because its ordering needs the
/// comparator, which would otherwise have to ride along in every entry. It
/// orders the readers' indices, each reader's next entry waiting in its slot:
/// an entry is moved once, out of its slot, however deep the heap.
pub(crate) struct MergeScanner {
    readers: Vec<BlobFileScanner>,
    comparator: SharedComparator,
    /// Each reader's next entry, `None` once it ran out.
    slots: Vec<Option<ScanEntry>>,
    /// Min-heap of the readers holding an entry, by [`Self::order`].
    heap: Vec<usize>,
    started: bool,
}

impl MergeScanner {
    /// Initializes a new merging reader over `readers`, ordering keys by
    /// `comparator`.
    pub(crate) fn new(readers: Vec<BlobFileScanner>, comparator: SharedComparator) -> Self {
        let slots = core::iter::repeat_with(|| None)
            .take(readers.len())
            .collect();
        let heap = Vec::with_capacity(readers.len());
        Self {
            readers,
            comparator,
            slots,
            heap,
            started: false,
        }
    }

    /// Whether reader `a`'s entry comes before reader `b`'s: key ascending
    /// under the comparator, then seqno descending. Both hold one.
    fn before(&self, a: usize, b: usize) -> bool {
        match (self.slots.get(a), self.slots.get(b)) {
            (Some(Some(a)), Some(Some(b))) => {
                self.comparator
                    .compare(&a.key, &b.key)
                    .then_with(|| b.seqno.cmp(&a.seqno))
                    == Ordering::Less
            }
            _ => false,
        }
    }

    /// Moves the reader at heap position `at` down to its place.
    fn sift_down(&mut self, mut at: usize) {
        loop {
            let mut least = at;
            for child in [2 * at + 1, 2 * at + 2] {
                if let (Some(&c), Some(&l)) = (self.heap.get(child), self.heap.get(least))
                    && self.before(c, l)
                {
                    least = child;
                }
            }
            if least == at {
                return;
            }
            self.heap.swap(at, least);
            at = least;
        }
    }

    /// Moves the reader at heap position `at` up to its place.
    fn sift_up(&mut self, mut at: usize) {
        while at > 0 {
            let parent = (at - 1) / 2;
            match (self.heap.get(at), self.heap.get(parent)) {
                (Some(&c), Some(&p)) if self.before(c, p) => {
                    self.heap.swap(at, parent);
                    at = parent;
                }
                _ => return,
            }
        }
    }

    /// Reads reader `idx`'s next entry into its slot; `true` when it had one.
    fn refill(&mut self, idx: usize) -> crate::Result<bool> {
        let (Some(reader), Some(slot)) = (self.readers.get_mut(idx), self.slots.get_mut(idx))
        else {
            return Ok(false);
        };
        *slot = reader.next().transpose()?;
        Ok(slot.is_some())
    }

    fn start(&mut self) -> crate::Result<()> {
        for idx in 0..self.readers.len() {
            if self.refill(idx)? {
                self.heap.push(idx);
                self.sift_up(self.heap.len() - 1);
            }
        }
        Ok(())
    }
}

impl Iterator for MergeScanner {
    type Item = crate::Result<(ScanEntry, BlobFileId)>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.started {
            self.started = true;
            fail_iter!(self.start());
        }

        let &idx = self.heap.first()?;
        let entry = self.slots.get_mut(idx)?.take()?;
        let blob_file_id = self.readers.get(idx)?.blob_file_id;
        // The reader's next entry takes its place at the top, or, once the
        // reader runs out, the heap's last reader does.
        if !fail_iter!(self.refill(idx)) {
            self.heap.swap_remove(0);
        }
        self.sift_down(0);
        Some(Ok((entry, blob_file_id)))
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests;

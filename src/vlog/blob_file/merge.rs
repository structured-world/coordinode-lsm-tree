// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::scanner::Scanner as BlobFileScanner;
use crate::comparator::SharedComparator;
use crate::vlog::{BlobFileId, blob_file::scanner::ScanEntry};
use alloc::vec::Vec;
use core::cmp::Ordering;

type IteratorIndex = usize;

#[derive(Debug)]
struct IteratorValue {
    index: IteratorIndex,
    scan_entry: ScanEntry,
    blob_file_id: BlobFileId,
}

/// Interleaves multiple blob file readers into a single stream, ordered by
/// key under the tree's comparator, newest version first.
///
/// A relocating compaction walks this stream alongside the merged table
/// stream, which is in the comparator's order; merging by raw key bytes
/// instead would put the two out of step under any other ordering. The heap
/// is kept here rather than in a `BinaryHeap` because its ordering needs the
/// comparator, which would otherwise have to ride along in every entry.
pub struct MergeScanner {
    readers: Vec<BlobFileScanner>,
    comparator: SharedComparator,
    /// Min-heap by [`Self::order`].
    heap: Vec<IteratorValue>,
    started: bool,
}

impl MergeScanner {
    /// Initializes a new merging reader over `readers`, ordering keys by
    /// `comparator`.
    pub fn new(readers: Vec<BlobFileScanner>, comparator: SharedComparator) -> Self {
        let heap = Vec::with_capacity(readers.len());
        Self {
            readers,
            comparator,
            heap,
            started: false,
        }
    }

    /// Key ascending under the comparator, then seqno descending.
    fn order(&self, a: &IteratorValue, b: &IteratorValue) -> Ordering {
        self.comparator
            .compare(&a.scan_entry.key, &b.scan_entry.key)
            .then_with(|| b.scan_entry.seqno.cmp(&a.scan_entry.seqno))
    }

    fn push(&mut self, value: IteratorValue) {
        self.heap.push(value);
        let mut child = self.heap.len() - 1;
        while child > 0 {
            let parent = (child - 1) / 2;
            match (self.heap.get(child), self.heap.get(parent)) {
                (Some(c), Some(p)) if self.order(c, p) == Ordering::Less => {
                    self.heap.swap(child, parent);
                    child = parent;
                }
                _ => break,
            }
        }
    }

    fn pop(&mut self) -> Option<IteratorValue> {
        if self.heap.is_empty() {
            return None;
        }
        let head = self.heap.swap_remove(0);
        let mut parent = 0;
        loop {
            let mut least = parent;
            for child in [2 * parent + 1, 2 * parent + 2] {
                if let (Some(c), Some(l)) = (self.heap.get(child), self.heap.get(least))
                    && self.order(c, l) == Ordering::Less
                {
                    least = child;
                }
            }
            if least == parent {
                break;
            }
            self.heap.swap(parent, least);
            parent = least;
        }
        Some(head)
    }

    fn advance_reader(&mut self, idx: usize) -> crate::Result<()> {
        let Some(reader) = self.readers.get_mut(idx) else {
            return Ok(());
        };
        if let Some(value) = reader.next() {
            let scan_entry = value?;
            let blob_file_id = reader.blob_file_id;
            self.push(IteratorValue {
                index: idx,
                blob_file_id,
                scan_entry,
            });
        }
        Ok(())
    }

    fn push_next(&mut self) -> crate::Result<()> {
        for idx in 0..self.readers.len() {
            self.advance_reader(idx)?;
        }
        Ok(())
    }
}

impl Iterator for MergeScanner {
    type Item = crate::Result<(ScanEntry, BlobFileId)>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.started {
            self.started = true;
            fail_iter!(self.push_next());
        }

        let head = self.pop()?;
        fail_iter!(self.advance_reader(head.index));
        Some(Ok((head.scan_entry, head.blob_file_id)))
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests;

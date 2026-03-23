// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::comparator::SharedComparator;
use crate::heap::{HeapEntry, MergeHeap};
use crate::InternalValue;

type IterItem = crate::Result<InternalValue>;

pub type BoxedIterator<'a> = Box<dyn DoubleEndedIterator<Item = IterItem> + Send + 'a>;

/// Merges multiple KV iterators into a single sorted stream.
///
/// Uses a custom sorted-vector heap with `replace_min` / `replace_max`
/// to avoid the double O(log n) cost of `pop` + `push` in the hot
/// path.  The comparator is stored once in the heap, not per entry —
/// eliminating per-item `Arc` ref-count traffic.
pub struct Merger<I> {
    iterators: Vec<I>,
    heap: MergeHeap,
    initialized_lo: bool,
    initialized_hi: bool,
}

impl<I: Iterator<Item = IterItem>> Merger<I> {
    #[must_use]
    pub fn new(iterators: Vec<I>, comparator: SharedComparator) -> Self {
        let heap = MergeHeap::with_capacity(iterators.len(), comparator);

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
                self.heap.push(HeapEntry {
                    index: idx,
                    value: item,
                });
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
                self.heap.push(HeapEntry {
                    index: idx,
                    value: item,
                });
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
        let top_index = self.heap.peek_min()?.index;

        #[expect(clippy::indexing_slicing, reason = "we trust the HeapEntry index")]
        if let Some(next_result) = self.iterators[top_index].next() {
            let next_value = fail_iter!(next_result);
            // Replace the min in-place and slide into position.
            // Common case (same source still wins): 1 comparison.
            let old = self.heap.replace_min(HeapEntry {
                index: top_index,
                value: next_value,
            });
            Some(Ok(old.value))
        } else {
            // Source iterator exhausted — just remove.
            let old = self.heap.pop_min()?;
            Some(Ok(old.value))
        }
    }
}

impl<I: DoubleEndedIterator<Item = IterItem>> DoubleEndedIterator for Merger<I> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if !self.initialized_hi {
            fail_iter!(self.initialize_hi());
        }

        let top_index = self.heap.peek_max()?.index;

        #[expect(clippy::indexing_slicing, reason = "we trust the HeapEntry index")]
        if let Some(next_result) = self.iterators[top_index].next_back() {
            let next_value = fail_iter!(next_result);
            let old = self.heap.replace_max(HeapEntry {
                index: top_index,
                value: next_value,
            });
            Some(Ok(old.value))
        } else {
            let old = self.heap.pop_max()?;
            Some(Ok(old.value))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::comparator;
    use crate::ValueType::Value;
    use test_log::test;

    #[test]
    #[expect(clippy::unwrap_used)]
    fn merge_simple() -> crate::Result<()> {
        #[rustfmt::skip]
        let a = vec![
            Ok(InternalValue::from_components("a", b"", 0, Value)),
        ];
        #[rustfmt::skip]
        let b = vec![
            Ok(InternalValue::from_components("b", b"", 0, Value)),
        ];

        let mut iter = Merger::new(
            vec![a.into_iter(), b.into_iter()],
            comparator::default_comparator(),
        );

        assert_eq!(
            iter.next().unwrap()?,
            InternalValue::from_components("a", b"", 0, Value),
        );
        assert_eq!(
            iter.next().unwrap()?,
            InternalValue::from_components("b", b"", 0, Value),
        );
        assert!(iter.next().is_none(), "iter should be closed");

        Ok(())
    }

    #[test]
    #[ignore = "maybe not needed"]
    #[expect(clippy::unwrap_used)]
    fn merge_dup() -> crate::Result<()> {
        #[rustfmt::skip]
        let a = vec![
            Ok(InternalValue::from_components("a", b"", 0, Value)),
        ];
        #[rustfmt::skip]
        let b = vec![
            Ok(InternalValue::from_components("a", b"", 0, Value)),
        ];

        let mut iter = Merger::new(
            vec![a.into_iter(), b.into_iter()],
            comparator::default_comparator(),
        );

        assert_eq!(
            iter.next().unwrap()?,
            InternalValue::from_components("a", b"", 0, Value),
        );
        assert!(iter.next().is_none(), "iter should be closed");

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn merge_interleaved() -> crate::Result<()> {
        let a = vec![
            Ok(InternalValue::from_components("a", b"", 0, Value)),
            Ok(InternalValue::from_components("c", b"", 0, Value)),
            Ok(InternalValue::from_components("e", b"", 0, Value)),
        ];
        let b = vec![
            Ok(InternalValue::from_components("b", b"", 0, Value)),
            Ok(InternalValue::from_components("d", b"", 0, Value)),
        ];

        let iter = Merger::new(
            vec![a.into_iter(), b.into_iter()],
            comparator::default_comparator(),
        );

        let keys: Vec<String> = iter
            .map(|r| {
                let v = r.unwrap();
                String::from_utf8_lossy(&v.key.user_key).to_string()
            })
            .collect();
        assert_eq!(keys, ["a", "b", "c", "d", "e"]);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn merge_many_sources() -> crate::Result<()> {
        let sources: Vec<Vec<IterItem>> = (0..8)
            .map(|i| {
                vec![Ok(InternalValue::from_components(
                    format!("{}", (b'a' + i) as char),
                    b"",
                    0,
                    Value,
                ))]
            })
            .collect();

        let iter = Merger::new(
            sources.into_iter().map(|s| s.into_iter()).collect(),
            comparator::default_comparator(),
        );

        let keys: Vec<String> = iter
            .map(|r| {
                let v = r.unwrap();
                String::from_utf8_lossy(&v.key.user_key).to_string()
            })
            .collect();
        assert_eq!(keys, ["a", "b", "c", "d", "e", "f", "g", "h"]);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn merge_seqno_ordering() -> crate::Result<()> {
        // Same key, different seqnos — higher seqno must come first.
        let a = vec![Ok(InternalValue::from_components("k", b"v1", 3, Value))];
        let b = vec![Ok(InternalValue::from_components("k", b"v2", 7, Value))];
        let c = vec![Ok(InternalValue::from_components("k", b"v3", 1, Value))];

        let iter = Merger::new(
            vec![a.into_iter(), b.into_iter(), c.into_iter()],
            comparator::default_comparator(),
        );

        let seqnos: Vec<u64> = iter.map(|r| r.unwrap().key.seqno).collect();
        assert_eq!(seqnos, [7, 3, 1]);

        Ok(())
    }
}

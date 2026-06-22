// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::active_tombstone_set::ActiveTombstoneSet;
use crate::comparator::SharedComparator;
use crate::range_tombstone::RangeTombstone;
use crate::{InternalValue, SeqNo, UserKey, UserValue, ValueType, merge_operator::MergeOperator};
use alloc::collections::VecDeque;
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::iter::Peekable;

type Item = crate::Result<InternalValue>;

/// A callback that receives all dropped KVs
///
/// Used for counting blobs that are not referenced anymore because of
/// vHandles that are being dropped through compaction.
pub trait DroppedKvCallback {
    fn on_dropped(&mut self, kv: &InternalValue);
}

/// Verdict returned by [`StreamFilter`]
#[derive(Debug)]
pub enum StreamFilterVerdict {
    /// Keep the item as is.
    Keep,

    /// Replace the item.
    Replace((ValueType, UserValue)),

    /// Drop the item without leaving a tombstone.
    Drop,
}

/// A callback for modifying KVs in the stream
pub trait StreamFilter {
    /// Handle an item, possibly modifying it.
    fn filter_item(&mut self, item: &InternalValue) -> crate::Result<StreamFilterVerdict>;
}

/// A [`StreamFilter`] that does not modify anything
pub struct NoFilter;

impl StreamFilter for NoFilter {
    fn filter_item(&mut self, _item: &InternalValue) -> crate::Result<StreamFilterVerdict> {
        Ok(StreamFilterVerdict::Keep)
    }
}

/// Consumes a stream of KVs and emits a new stream according to GC and tombstone rules
///
/// This iterator is used during flushing & compaction.
pub struct CompactionStream<'a, I: Iterator<Item = Item>, F: StreamFilter = NoFilter> {
    /// KV stream
    inner: Peekable<I>,

    /// MVCC watermark to get rid of old versions
    gc_seqno_threshold: SeqNo,

    /// Event emitter that receives all dropped KVs
    dropped_callback: Option<&'a mut dyn DroppedKvCallback>,

    /// Stream filter
    filter: F,

    evict_tombstones: bool,

    zero_seqnos: bool,

    /// Merge operator for collapsing merge operands during compaction
    merge_operator: Option<Arc<dyn MergeOperator>>,

    /// Entries that could not be merged (e.g., Indirection base) and need
    /// to be re-emitted unchanged on subsequent `next()` calls.
    pending: VecDeque<InternalValue>,

    /// Range tombstones strictly below the watermark (`seqno <
    /// gc_seqno_threshold`) whose covered entries can be physically dropped
    /// during this (bottommost) compaction: every live snapshot (which reads at
    /// or above the watermark) sees them in effect, so the covered KVs are
    /// deleted for all readers. A tombstone exactly at the watermark is excluded
    /// — it is invisible to a read at the watermark. Empty when RT application is
    /// not enabled.
    rt_apply: Vec<RangeTombstone>,
    rt_comparator: Option<SharedComparator>,
    rt_active: Option<ActiveTombstoneSet>,
    rt_idx: usize,
    rt_sorted: bool,
}

impl<I: Iterator<Item = Item>> CompactionStream<'_, I, NoFilter> {
    /// Initializes a new merge iterator
    #[must_use]
    pub fn new(iter: I, gc_seqno_threshold: SeqNo) -> Self {
        let iter = iter.peekable();

        Self {
            inner: iter,
            gc_seqno_threshold,
            dropped_callback: None,
            filter: NoFilter,
            evict_tombstones: false,
            zero_seqnos: false,
            merge_operator: None,
            pending: VecDeque::new(),
            rt_apply: Vec::new(),
            rt_comparator: None,
            rt_active: None,
            rt_idx: 0,
            rt_sorted: false,
        }
    }
}

impl<'a, I: Iterator<Item = Item>, F: StreamFilter + 'a> CompactionStream<'a, I, F> {
    /// Installs a filter into this stream.
    pub fn with_filter<NF: StreamFilter>(self, filter: NF) -> CompactionStream<'a, I, NF> {
        CompactionStream {
            inner: self.inner,
            gc_seqno_threshold: self.gc_seqno_threshold,
            dropped_callback: self.dropped_callback,
            filter,
            evict_tombstones: self.evict_tombstones,
            zero_seqnos: self.zero_seqnos,
            merge_operator: self.merge_operator,
            pending: self.pending,
            rt_apply: self.rt_apply,
            rt_comparator: self.rt_comparator,
            rt_active: self.rt_active,
            rt_idx: self.rt_idx,
            rt_sorted: self.rt_sorted,
        }
    }

    pub fn evict_tombstones(mut self, b: bool) -> Self {
        self.evict_tombstones = b;
        self
    }

    /// Installs a callback that receives all dropped KVs.
    pub fn with_drop_callback(mut self, cb: &'a mut dyn DroppedKvCallback) -> Self {
        self.dropped_callback = Some(cb);
        self
    }

    /// Installs a merge operator for collapsing merge operands during compaction.
    #[must_use]
    pub fn with_merge_operator(mut self, op: Option<Arc<dyn MergeOperator>>) -> Self {
        self.merge_operator = op;
        self
    }

    /// Sets sequence numbers to zero if they are below the snapshot watermark.
    ///
    /// This can save a lot of space, because "0" only takes 1 byte, and sequence numbers are monotonically increasing.
    pub fn zero_seqnos(mut self, b: bool) -> Self {
        self.zero_seqnos = b;
        self
    }

    /// Enables compaction-time range-tombstone application: surviving entries
    /// covered by a tombstone whose seqno is strictly below the watermark
    /// (`seqno < gc_seqno_threshold`) and higher than the entry's seqno are
    /// physically dropped (and reported to the drop callback for blob-GC
    /// accounting) instead of being carried to the output and suppressed at read
    /// time.
    ///
    /// Only strictly-below-watermark tombstones are applied: a tombstone at or
    /// above the watermark might not be in effect for a snapshot between the
    /// entry's seqno and the tombstone's (a read at the watermark does not see a
    /// tombstone at the watermark), so those entries are preserved (PITR/MVCC
    /// safety). Pass tombstones gathered from the whole version; this filters
    /// them to the applicable set.
    #[must_use]
    pub fn with_range_tombstone_application(
        mut self,
        tombstones: Vec<RangeTombstone>,
        comparator: SharedComparator,
    ) -> Self {
        self.rt_apply = tombstones
            .into_iter()
            // Strict visibility (`seqno < threshold`), matching the read path and
            // the point-key GC: a tombstone exactly at the watermark is still
            // invisible to the oldest live snapshot (which reads at the
            // watermark), so it must NOT physically drop covered keys yet.
            .filter(|rt| rt.visible_at(self.gc_seqno_threshold))
            .collect();
        self.rt_active = Some(ActiveTombstoneSet::new_with_comparator(comparator.clone()));
        self.rt_comparator = Some(comparator);
        self
    }

    /// Returns `true` if `user_key`/`seqno` is covered by an applicable
    /// (strictly-below-watermark) range tombstone with a higher seqno — meaning
    /// the entry is deleted for every live snapshot and can be physically dropped.
    /// Entries arrive in non-decreasing `user_key` order, so the active set is
    /// swept monotonically.
    fn covered_by_applied_tombstone(&mut self, user_key: &[u8], seqno: SeqNo) -> bool {
        let (Some(comparator), Some(active)) =
            (self.rt_comparator.as_ref(), self.rt_active.as_mut())
        else {
            return false;
        };
        if !self.rt_sorted {
            self.rt_apply
                .sort_by(|a, b| a.cmp_with_comparator(b, comparator.as_ref()));
            self.rt_sorted = true;
        }
        while let Some(rt) = self.rt_apply.get(self.rt_idx) {
            if comparator.compare(&rt.start, user_key) == core::cmp::Ordering::Greater {
                break;
            }
            // cutoff = MAX: every applicable tombstone is active; `is_suppressed`
            // then drops the entry iff some active tombstone outranks its seqno.
            active.activate(rt, SeqNo::MAX);
            self.rt_idx += 1;
        }
        active.expire_until(user_key);
        active.is_suppressed(seqno)
    }

    /// Collects merge operands and resolves them via the merge operator.
    ///
    /// `head` is the first `MergeOperand` entry (highest seqno).
    /// Collects subsequent same-key entries, merges them, and returns the result.
    /// When a base value or tombstone boundary is found, the result is a `Value`
    /// (complete merge). When no boundary is found (partial merge), the result
    /// remains a `MergeOperand` so future compactions can find the real base.
    fn resolve_merge_operands(
        &mut self,
        head: InternalValue,
        merge_op: &dyn MergeOperator,
    ) -> crate::Result<InternalValue> {
        let user_key = head.key.user_key.clone();
        let head_seqno = head.key.seqno;

        // Store full entries so we can re-emit them unchanged if we hit an
        // Indirection base and cannot resolve the merge.
        let mut collected: Vec<InternalValue> = vec![head];
        let mut base_value: Option<UserValue> = None;
        let mut found_boundary = false;

        // Collect remaining same-key entries
        loop {
            let should_take = self.inner.peek().is_some_and(|peeked| {
                if let Ok(peeked) = peeked {
                    peeked.key.user_key == user_key
                } else {
                    true
                }
            });

            if !should_take {
                break;
            }

            // Check for Indirection BEFORE consuming — the indirection entry
            // stays in the stream and will be emitted normally by next().
            let is_indirection = self.inner.peek().is_some_and(
                |peeked| matches!(peeked, Ok(p) if p.key.value_type == ValueType::Indirection),
            );

            if is_indirection {
                // Cannot merge with a blob-pointer base. Re-emit all consumed
                // entries unchanged via the pending buffer to avoid data loss.
                // The first entry is returned immediately; the rest are buffered
                // for subsequent next() calls.
                let mut iter = collected.into_iter();
                #[expect(clippy::expect_used, reason = "collected always has head")]
                let first = iter
                    .next()
                    .expect("collected should contain at least one element");
                self.pending.extend(iter);
                return Ok(first);
            }

            #[expect(clippy::expect_used, reason = "we just checked peek is Some")]
            let next = self.inner.next().expect("peeked value should exist")?;

            match next.key.value_type {
                ValueType::MergeOperand => {
                    collected.push(next);
                }
                ValueType::Value => {
                    found_boundary = true;
                    // A covering applied range tombstone newer than this value
                    // deletes it, so the merge operands must fold onto an empty
                    // base instead of the value being physically dropped. Without
                    // this, a compaction resurrects a range-deleted key whenever a
                    // later merge operand exists (the read path before compaction
                    // already folds onto the empty base).
                    if self.covered_by_applied_tombstone(user_key.as_ref(), next.key.seqno) {
                        if let Some(watcher) = &mut self.dropped_callback {
                            watcher.on_dropped(&next);
                        }
                    } else {
                        base_value = Some(next.value);
                    }
                    self.drain_key(&user_key)?;
                    break;
                }
                ValueType::Indirection => {
                    // Unreachable: handled by the peek check above.
                    unreachable!("Indirection should be caught by peek check");
                }
                ValueType::Tombstone | ValueType::WeakTombstone => {
                    // Tombstone kills base — merge with no base
                    found_boundary = true;
                    if let Some(watcher) = &mut self.dropped_callback {
                        watcher.on_dropped(&next);
                    }
                    self.drain_key(&user_key)?;
                    break;
                }
            }
        }

        // Extract operand values for merge
        let operands: Vec<UserValue> = collected.into_iter().map(|e| e.value).collect();

        // Reverse to chronological order (ascending seqno)
        let mut operands_reversed = operands;
        operands_reversed.reverse();

        let operand_refs: Vec<&[u8]> = operands_reversed.iter().map(AsRef::as_ref).collect();
        let merged = merge_op.merge(&user_key, base_value.as_deref(), &operand_refs)?;

        // Complete merge (base or tombstone found): emit as Value.
        // Partial merge (no boundary in this stream — base may be in lower level):
        // emit as MergeOperand so future compactions can find the real base.
        // The MergeOperator contract requires stability across re-merging:
        // future passes may see this pre-merged output as an operand.
        let result_type = if found_boundary {
            ValueType::Value
        } else {
            ValueType::MergeOperand
        };

        Ok(InternalValue::from_components(
            user_key,
            merged,
            head_seqno,
            result_type,
        ))
    }

    /// Drains the remaining versions of the given key.
    fn drain_key(&mut self, key: &UserKey) -> crate::Result<()> {
        loop {
            let Some(next) = self.inner.next_if(|kv| {
                if let Ok(kv) = kv {
                    let expired = kv.key.user_key == key;

                    if expired && let Some(watcher) = &mut self.dropped_callback {
                        watcher.on_dropped(kv);
                    }

                    expired
                } else {
                    true
                }
            }) else {
                return Ok(());
            };

            next?;
        }
    }
}

impl<'a, I: Iterator<Item = Item>, F: StreamFilter + 'a> Iterator for CompactionStream<'a, I, F> {
    type Item = Item;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Pending entries (from Indirection bailout) go through the same pipeline.
            let next = self
                .pending
                .pop_front()
                .map_or_else(|| self.inner.next(), |e| Some(Ok(e)));
            let mut head = fail_iter!(next?);

            if !head.is_tombstone() {
                match fail_iter!(self.filter.filter_item(&head)) {
                    StreamFilterVerdict::Keep => { /* Do nothing */ }
                    StreamFilterVerdict::Replace((new_type, new_value)) => {
                        // If we are replacing this item's value, call the dropped callback for the previous item
                        if let Some(watcher) = &mut self.dropped_callback {
                            watcher.on_dropped(&head);
                        }
                        head.value = new_value;

                        // Preserve MergeOperand type only when filter replaces it
                        // with a Value: turning a MergeOperand into an Indirection
                        // would store blob-pointer bytes under MergeOperand type,
                        // confusing merge resolution or reads.
                        let preserve_merge_type =
                            head.key.value_type.is_merge_operand() && new_type == ValueType::Value;
                        if !preserve_merge_type {
                            head.key.value_type = new_type;
                        }
                    }
                    StreamFilterVerdict::Drop => {
                        if let Some(watcher) = &mut self.dropped_callback {
                            watcher.on_dropped(&head);
                        }
                        continue;
                    }
                }
            }

            if let Some(peeked) = self.inner.peek() {
                let Ok(peeked) = peeked else {
                    #[expect(
                        clippy::expect_used,
                        reason = "we just asserted, the peeked value is an error"
                    )]
                    return Some(Err(self
                        .inner
                        .next()
                        .expect("value should exist")
                        .expect_err("should be error")));
                };

                if peeked.key.user_key > head.key.user_key {
                    if head.is_tombstone() && self.evict_tombstones {
                        continue;
                    }

                    // NOTE: Only item of this key and thus latest version, so return it no matter what
                    // For a lone merge operand with a merge operator and below GC threshold,
                    // collapse via partial merge (result stays MergeOperand if no base found)
                    if head.key.value_type.is_merge_operand()
                        && head.key.seqno < self.gc_seqno_threshold
                        && let Some(merge_op) = self.merge_operator.clone()
                    {
                        let merged =
                            fail_iter!(self.resolve_merge_operands(head, merge_op.as_ref()));
                        head = merged;
                    }
                } else if peeked.key.seqno < self.gc_seqno_threshold {
                    // Merge operands below GC watermark: collapse via merge operator.
                    // Both head AND peeked must be below threshold for MVCC safety.
                    if head.key.value_type.is_merge_operand()
                        && head.key.seqno < self.gc_seqno_threshold
                    {
                        if let Some(merge_op) = self.merge_operator.clone() {
                            let mut merged =
                                fail_iter!(self.resolve_merge_operands(head, merge_op.as_ref()));
                            // Drop the merged result if an applicable tombstone
                            // outranks it (same rule as the main emit path).
                            if self.covered_by_applied_tombstone(
                                merged.key.user_key.as_ref(),
                                merged.key.seqno,
                            ) {
                                if let Some(watcher) = &mut self.dropped_callback {
                                    watcher.on_dropped(&merged);
                                }
                                continue;
                            }
                            // Skip zeroing for partial merges (MergeOperand) to avoid duplicate keys
                            if self.zero_seqnos
                                && merged.key.seqno < self.gc_seqno_threshold
                                && !merged.key.value_type.is_merge_operand()
                            {
                                merged.key.seqno = 0;
                            }
                            return Some(Ok(merged));
                        }

                        // No merge operator — read path resolves operands on-the-fly
                    } else if head.key.value_type.is_merge_operand() {
                        // Head MergeOperand above GC — preserve tail for future merge
                    } else {
                        if head.key.value_type == ValueType::Tombstone && self.evict_tombstones {
                            fail_iter!(self.drain_key(&head.key.user_key));
                            continue;
                        }

                        // Drop weak tombstone if next item is Value
                        let drop_weak_tombstone = peeked.key.value_type == ValueType::Value
                            && head.key.value_type == ValueType::WeakTombstone;
                        // Tail expired — drain (head is never MergeOperand here)
                        fail_iter!(self.drain_key(&head.key.user_key));

                        if drop_weak_tombstone {
                            continue;
                        }
                    }
                }
            } else if head.is_tombstone() && self.evict_tombstones {
                continue;
            } else if head.key.value_type.is_merge_operand()
                && head.key.seqno < self.gc_seqno_threshold
            {
                // Last stream item is a MergeOperand below GC — partial merge.
                if let Some(merge_op) = self.merge_operator.clone() {
                    let merged = fail_iter!(self.resolve_merge_operands(head, merge_op.as_ref()));
                    head = merged;
                }
            }

            // Compaction-time range-tombstone application: physically drop the
            // surviving entry when an applicable (strictly-below-watermark)
            // tombstone outranks it, accounting it to the drop callback (blob GC)
            // instead of carrying it to the output to be suppressed at every read.
            if self.covered_by_applied_tombstone(head.key.user_key.as_ref(), head.key.seqno) {
                if let Some(watcher) = &mut self.dropped_callback {
                    watcher.on_dropped(&head);
                }
                continue;
            }

            // Zero seqnos below GC, but skip MergeOperands (duplicate key risk)
            if self.zero_seqnos
                && head.key.seqno < self.gc_seqno_threshold
                && !head.key.value_type.is_merge_operand()
            {
                head.key.seqno = 0;
            }

            return Some(Ok(head));
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::useless_vec,
    clippy::doc_markdown,
    clippy::unnecessary_wraps,
    reason = "test code"
)]
mod tests {
    use super::*;
    use crate::{ValueType, value::InternalValue};
    use test_log::test;

    macro_rules! stream {
        ($($key:expr, $sub_key:expr, $value_type:expr),* $(,)?) => {{
            let mut values = Vec::new();
            let mut counters = std::collections::HashMap::new();

            $(
                #[expect(clippy::string_lit_as_bytes)]
                let key = $key.as_bytes();

                #[expect(clippy::string_lit_as_bytes)]
                let sub_key = $sub_key.as_bytes();

                let value_type = match $value_type {
                    "V" => ValueType::Value,
                    "T" => ValueType::Tombstone,
                    "W" => ValueType::WeakTombstone,
                    "M" => ValueType::MergeOperand,
                    "I" => ValueType::Indirection,
                    _ => panic!("Unknown value type"),
                };

                let counter = counters.entry($key).and_modify(|x| { *x -= 1 }).or_insert(999);
                values.push(InternalValue::from_components(key, sub_key, *counter, value_type));
            )*

            values
        }};
    }

    macro_rules! iter_closed {
        ($iter:expr) => {
            assert!($iter.next().is_none(), "iterator should be closed (done)");
        };
    }

    #[derive(Default)]
    struct TrackCallback {
        items: Vec<InternalValue>,
    }

    impl DroppedKvCallback for TrackCallback {
        fn on_dropped(&mut self, kv: &InternalValue) {
            self.items.push(kv.clone());
        }
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn compaction_stream_expired_callback_1() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "", "T",
          "a", "", "T",
          "a", "", "T",
        ];

        let mut my_watcher = TrackCallback::default();

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_000).with_drop_callback(&mut my_watcher);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"", 999, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        assert_eq!(
            [
                InternalValue::from_components("a", "", 998, ValueType::Tombstone),
                InternalValue::from_components("a", "", 997, ValueType::Tombstone),
            ],
            &*my_watcher.items,
        );

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn compaction_stream_seqno_zeroing_1() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "3", "V",
          "a", "2", "V",
          "a", "1", "V",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_000).zero_seqnos(true);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"3", 0, ValueType::Value),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        Ok(())
    }

    #[test]
    fn compaction_stream_queue_weak_tombstones() {
        #[rustfmt::skip]
        let vec = stream![
          "a", "", "W",
          "a", "old", "V",
          "b", "", "W",
          "b", "old", "V",
          "c", "", "W",
          "c", "old", "V",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_050);

        iter_closed!(iter);
    }

    /// GC should not evict tombstones, unless they are covered up
    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn compaction_stream_tombstone_no_gc() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "", "T",
          "b", "", "T",
          "c", "", "T",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_000_000);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"", 999, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"", 999, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"", 999, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn compaction_stream_old_tombstone() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "", "T",
          "a", "", "T",
          "b", "", "T",
          "b", "", "T",
          "c", "", "T",
          "c", "", "T",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 998);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"", 999, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"a", *b"", 998, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"", 999, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"", 998, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"", 999, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"", 998, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn compaction_stream_tombstone_overwrite_gc() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "val", "V",
          "a", "", "T",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 999);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"val", 999, ValueType::Value),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn compaction_stream_weak_tombstone_simple() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "", "W",
          "a", "old", "V",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 0);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"", 999, ValueType::WeakTombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"a", *b"old", 998, ValueType::Value),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn compaction_stream_weak_tombstone_no_gc() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "", "W",
          "a", "old", "V",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 998);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"", 999, ValueType::WeakTombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"a", *b"old", 998, ValueType::Value),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        Ok(())
    }

    #[test]
    fn compaction_stream_weak_tombstone_evict() {
        #[rustfmt::skip]
        let vec = stream![
          "a", "", "W",
          "a", "old", "V",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 999);

        // NOTE: Weak tombstone is consumed because value is GC'ed

        iter_closed!(iter);
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn compaction_stream_weak_tombstone_evict_next_value() -> crate::Result<()> {
        #[rustfmt::skip]
        let mut vec = stream![
          "a", "", "W",
          "a", "old", "V",
        ];
        vec.push(InternalValue::from_components(
            "b",
            "other",
            999,
            ValueType::Value,
        ));

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 999);

        // NOTE: Weak tombstone is consumed because value is GC'ed

        assert_eq!(
            InternalValue::from_components(*b"b", *b"other", 999, ValueType::Value),
            iter.next().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn compaction_stream_no_evict_simple() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "old", "V",
          "b", "old", "V",
          "c", "old", "V",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 0);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"old", 999, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"old", 999, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"old", 999, ValueType::Value),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn compaction_stream_no_evict_simple_multi_keys() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "new", "V",
          "a", "old", "V",
          "b", "new", "V",
          "b", "old", "V",
          "c", "newnew", "V",
          "c", "new", "V",
          "c", "old", "V",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 0);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"new", 999, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"a", *b"old", 998, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"new", 999, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"old", 998, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"newnew", 999, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"new", 998, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"old", 997, ValueType::Value),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        Ok(())
    }

    #[test]
    fn compaction_stream_filter_1() {
        struct Filter(&'static [u8]);
        impl StreamFilter for Filter {
            fn filter_item(&mut self, value: &InternalValue) -> crate::Result<StreamFilterVerdict> {
                if value.key.user_key == b"b" {
                    Ok(StreamFilterVerdict::Drop)
                } else if value.value < self.0 {
                    Ok(StreamFilterVerdict::Replace((
                        ValueType::Tombstone,
                        UserValue::empty(),
                    )))
                } else {
                    Ok(StreamFilterVerdict::Keep)
                }
            }
        }

        #[rustfmt::skip]
        let vec = stream![
            "a", "9", "V",
            "a", "8", "V",
            "a", "7", "V",
            // subsequent values will be filtered out
            "a", "6", "V",
            "a", "5", "V",
            // subsequent values below gc threshold after filter
            "a", "4", "V",

            // this value will be dropped without leaving a tombstone
            "b", "b", "V",
        ];

        let mut drop_cb = TrackCallback { items: vec![] };
        let iter = vec.iter().cloned().map(Ok);
        let iter = CompactionStream::new(iter, 995)
            .with_filter(Filter(b"7"))
            .with_drop_callback(&mut drop_cb);

        let out: Vec<_> = iter.map(Result::unwrap).collect();

        #[rustfmt::skip]
        assert_eq!(out, stream![
            "a", "9", "V",
            "a", "8", "V",
            "a", "7", "V",
            "a", "", "T",
            "a", "", "T",
        ]);

        let fc = InternalValue::from_components;

        #[rustfmt::skip]
        assert_eq!(drop_cb.items, [
            fc(b"a", b"6", 996, ValueType::Value),
            fc(b"a", b"5", 995, ValueType::Value),
            fc(b"a", b"4", 994, ValueType::Value),
            fc(b"b", b"b", 999, ValueType::Value),
        ]);
    }

    pub mod custom_mvcc {
        use super::*;
        use crate::io::{BE, ReadBytesExt, WriteBytesExt};
        use test_log::test;

        /// MVCC trailer size (anything but user key)
        const TRAILER_SIZE: usize = 10;

        // Our keys become a multi map of: <key>#<seqno>
        //
        // (type does not really matter for ordering, because key+seqno are unique anyway)
        fn kv(key: &[u8], seqno: SeqNo, value: &[u8], tomb: bool) -> InternalValue {
            InternalValue::from_components(
                {
                    use std::io::Write;

                    let len = key.len() + TRAILER_SIZE;

                    let mut key_builder = unsafe { UserKey::builder_unzeroed(len) };
                    let mut cursor = std::io::Cursor::new(&mut key_builder[..]);

                    cursor.write_all(key).unwrap();
                    cursor.write_u8(0).unwrap(); // Keys are variable size so we need a \0 delimiter
                    cursor
                        .write_u64::<BE>(
                            // IMPORTANT: Invert the seqno for correct descending sort
                            !seqno,
                        )
                        .unwrap();
                    cursor.write_u8(u8::from(tomb)).unwrap();

                    debug_assert_eq!(len, usize::try_from(cursor.position()).unwrap());

                    key_builder.freeze()
                },
                value,
                2_353, // does not matter for us
                ValueType::Value,
            )
        }

        struct Filter {
            /// The previous user key
            ///
            /// Note that the user key is NOT the full KV key
            /// because we embed MVCC information into the key (`user_key#seqno#type`).
            prev_user_key: Option<UserKey>,

            /// MVCC watermark we can safely delete if an item < watermark
            /// is covered by a newer version.
            mvcc_watermark: SeqNo,
        }

        impl StreamFilter for Filter {
            fn filter_item(&mut self, value: &InternalValue) -> crate::Result<StreamFilterVerdict> {
                let l = value.key.user_key.len();

                // User key len
                let ukl = l - TRAILER_SIZE;

                if let Some(prev) = &self.prev_user_key {
                    let user_key = &value.key.user_key[..ukl];

                    if prev == &user_key {
                        // We found another, older version of the previous key
                        let mut seqno = &value.key.user_key[(ukl + 1)..l - 1];
                        debug_assert_eq!(8, seqno.len());

                        // IMPORTANT: Invert the seqno back to normal value
                        let seqno = !seqno.read_u64::<BE>().unwrap();

                        if seqno < self.mvcc_watermark {
                            return Ok(StreamFilterVerdict::Drop);
                        }
                    } else {
                        let user_key = &value.key.user_key.slice(..ukl);
                        self.prev_user_key = Some(user_key.clone());
                    }
                } else {
                    let user_key = &value.key.user_key.slice(..ukl);
                    self.prev_user_key = Some(user_key.clone());
                }

                Ok(StreamFilterVerdict::Keep)
            }
        }

        #[test]
        fn compaction_filter_custom_mvcc() {
            let vec = vec![
                kv(b"abc", 4, b"c", false),
                kv(b"abc", 3, b"b", false),
                kv(b"abc", 2, b"a", false),
            ];

            let mut drop_cb = TrackCallback { items: vec![] };
            let iter = vec.iter().cloned().map(Ok);
            let iter = CompactionStream::new(iter, 995)
                .with_filter(Filter {
                    mvcc_watermark: 5,
                    prev_user_key: None,
                })
                .with_drop_callback(&mut drop_cb);

            let out: Vec<_> = iter.map(Result::unwrap).collect();

            #[rustfmt::skip]
            assert_eq!(out, vec![
                kv(b"abc", 4, b"c", false),
            ]);
        }

        #[test]
        fn compaction_filter_custom_mvcc_multi_keys() {
            let vec = vec![
                kv(b"a", 4, b"c", false),
                kv(b"a", 3, b"b", false),
                kv(b"a", 2, b"a", false),
                //
                kv(b"b", 4, b"c", false),
                kv(b"b", 3, b"b", false),
                kv(b"b", 2, b"a", false),
                //
                kv(b"c", 1, b"c", false),
                //
                kv(b"d", 0, b"c", false),
            ];

            let mut drop_cb = TrackCallback { items: vec![] };
            let iter = vec.iter().cloned().map(Ok);
            let iter = CompactionStream::new(iter, 995)
                .with_filter(Filter {
                    mvcc_watermark: 3,
                    prev_user_key: None,
                })
                .with_drop_callback(&mut drop_cb);

            let out: Vec<_> = iter.map(Result::unwrap).collect();

            #[rustfmt::skip]
            assert_eq!(out, vec![
                kv(b"a", 4, b"c", false),
                kv(b"a", 3, b"b", false),
                //
                kv(b"b", 4, b"c", false),
                kv(b"b", 3, b"b", false),
                //
                kv(b"c", 1, b"c", false),
                //
                kv(b"d", 0, b"c", false),
            ]);
        }
    }

    mod merge_operator_tests {
        use super::*;
        use std::sync::Arc;
        use test_log::test;

        /// Concatenation merge operator: joins all operands with ","
        struct ConcatMerge;

        impl crate::merge_operator::MergeOperator for ConcatMerge {
            fn merge(
                &self,
                _key: &[u8],
                base_value: Option<&[u8]>,
                operands: &[&[u8]],
            ) -> crate::Result<UserValue> {
                let mut result = match base_value {
                    Some(b) => String::from_utf8_lossy(b).to_string(),
                    None => String::new(),
                };
                for op in operands {
                    if !result.is_empty() {
                        result.push(',');
                    }
                    result.push_str(&String::from_utf8_lossy(op));
                }
                Ok(result.into_bytes().into())
            }
        }

        fn merge_op() -> Arc<dyn crate::merge_operator::MergeOperator> {
            Arc::new(ConcatMerge)
        }

        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn compaction_merge_operands_below_gc() -> crate::Result<()> {
            // All entries below gc_seqno_threshold=1000, no base → partial merge
            #[rustfmt::skip]
            let vec = stream![
                "a", "op2", "M",
                "a", "op1", "M",
            ];

            let iter = vec.iter().cloned().map(Ok);
            let mut iter = CompactionStream::new(iter, 1_000).with_merge_operator(Some(merge_op()));

            let item = iter.next().unwrap()?;
            // Partial merge (no base boundary) → stays MergeOperand
            assert_eq!(item.key.value_type, ValueType::MergeOperand);
            assert_eq!(&*item.value, b"op1,op2");
            assert!(iter.next().is_none());

            Ok(())
        }

        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn compaction_merge_with_base_below_gc() -> crate::Result<()> {
            // Merge operands + base value, all below gc threshold
            #[rustfmt::skip]
            let vec = stream![
                "a", "op2", "M",
                "a", "op1", "M",
                "a", "base", "V",
            ];

            let iter = vec.iter().cloned().map(Ok);
            let mut iter = CompactionStream::new(iter, 1_000).with_merge_operator(Some(merge_op()));

            let item = iter.next().unwrap()?;
            assert_eq!(item.key.value_type, ValueType::Value);
            assert_eq!(&*item.value, b"base,op1,op2");
            assert!(iter.next().is_none());

            Ok(())
        }

        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn compaction_merge_base_deleted_by_range_tombstone() -> crate::Result<()> {
            // op@999 and mid@998 operands above a base@997; a range tombstone over
            // the key at seqno 998 deletes the base (997 < 998), while the operands
            // survive (999, 998 are not below 998). The operands fold onto an empty
            // base, and the dropped base value reaches the callback.
            #[rustfmt::skip]
            let vec = stream![
                "a", "op", "M",
                "a", "mid", "M",
                "a", "base", "V",
            ];

            let mut callback = TrackCallback::default();
            let cmp = crate::comparator::default_comparator();
            let rt = RangeTombstone::new(
                UserKey::from(b"a".as_ref()),
                UserKey::from(b"b".as_ref()),
                998,
            );

            let iter = vec.iter().cloned().map(Ok);
            {
                let mut iter = CompactionStream::new(iter, 1_000)
                    .with_merge_operator(Some(merge_op()))
                    .with_range_tombstone_application(vec![rt], cmp)
                    .with_drop_callback(&mut callback);

                let item = iter.next().unwrap()?;
                assert_eq!(item.key.value_type, ValueType::Value);
                assert_eq!(&*item.value, b"mid,op");
                assert!(iter.next().is_none());
            }
            assert!(
                callback.items.iter().any(|kv| &*kv.value == b"base"),
                "the range-deleted base value must reach the drop callback"
            );

            Ok(())
        }

        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn compaction_merge_with_tombstone_below_gc() -> crate::Result<()> {
            // Merge operand above tombstone → merge with no base
            #[rustfmt::skip]
            let vec = stream![
                "a", "op1", "M",
                "a", "", "T",
            ];

            let iter = vec.iter().cloned().map(Ok);
            let mut iter = CompactionStream::new(iter, 1_000).with_merge_operator(Some(merge_op()));

            let item = iter.next().unwrap()?;
            assert_eq!(item.key.value_type, ValueType::Value);
            assert_eq!(&*item.value, b"op1");
            assert!(iter.next().is_none());

            Ok(())
        }

        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn compaction_merge_above_gc_preserved() -> crate::Result<()> {
            // Entries above gc_seqno_threshold → NOT merged, preserved as-is
            #[rustfmt::skip]
            let vec = stream![
                "a", "op2", "M",
                "a", "op1", "M",
            ];

            let iter = vec.iter().cloned().map(Ok);
            let mut iter = CompactionStream::new(iter, 0) // gc_threshold=0, nothing expired
                .with_merge_operator(Some(merge_op()));

            let item = iter.next().unwrap()?;
            assert_eq!(item.key.value_type, ValueType::MergeOperand);
            assert_eq!(&*item.value, b"op2");

            let item = iter.next().unwrap()?;
            assert_eq!(item.key.value_type, ValueType::MergeOperand);
            assert_eq!(&*item.value, b"op1");

            assert!(iter.next().is_none());

            Ok(())
        }

        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn compaction_merge_lone_operand_below_gc() -> crate::Result<()> {
            // Single merge operand (only entry for key) below gc → partial merge
            let vec = vec![
                InternalValue::from_components("a", "lone_op", 5, ValueType::MergeOperand),
                InternalValue::from_components("b", "regular", 6, ValueType::Value),
            ];

            let iter = vec.iter().cloned().map(Ok);
            let mut iter = CompactionStream::new(iter, 1_000).with_merge_operator(Some(merge_op()));

            let item = iter.next().unwrap()?;
            // Partial merge (no base boundary) → stays MergeOperand
            assert_eq!(item.key.value_type, ValueType::MergeOperand);
            assert_eq!(&*item.value, b"lone_op");
            assert_eq!(&*item.key.user_key, b"a");

            let item = iter.next().unwrap()?;
            assert_eq!(&*item.key.user_key, b"b");
            assert_eq!(&*item.value, b"regular");

            assert!(iter.next().is_none());

            Ok(())
        }

        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn compaction_merge_last_item_operand() -> crate::Result<()> {
            // Last item in entire stream is a merge operand below gc
            let vec = vec![InternalValue::from_components(
                "z",
                "last",
                5,
                ValueType::MergeOperand,
            )];

            let iter = vec.iter().cloned().map(Ok);
            let mut iter = CompactionStream::new(iter, 1_000).with_merge_operator(Some(merge_op()));

            let item = iter.next().unwrap()?;
            // Partial merge (no base boundary) → stays MergeOperand
            assert_eq!(item.key.value_type, ValueType::MergeOperand);
            assert_eq!(&*item.value, b"last");

            assert!(iter.next().is_none());

            Ok(())
        }

        #[test]
        fn compaction_merge_mixed_keys() -> crate::Result<()> {
            // Multiple keys, some with merge operands, some without
            let vec = vec![
                InternalValue::from_components("a", "val_a", 10, ValueType::Value),
                InternalValue::from_components("b", "op2", 9, ValueType::MergeOperand),
                InternalValue::from_components("b", "op1", 8, ValueType::MergeOperand),
                InternalValue::from_components("b", "base_b", 7, ValueType::Value),
                InternalValue::from_components("c", "val_c", 6, ValueType::Value),
            ];

            let iter = vec.iter().cloned().map(Ok);
            let iter = CompactionStream::new(iter, 1_000).with_merge_operator(Some(merge_op()));

            let out: Vec<_> = iter.map(Result::unwrap).collect();

            assert_eq!(out.len(), 3);
            assert_eq!(&*out[0].key.user_key, b"a");
            assert_eq!(&*out[0].value, b"val_a");
            assert_eq!(&*out[1].key.user_key, b"b");
            assert_eq!(&*out[1].value, b"base_b,op1,op2");
            assert_eq!(&*out[2].key.user_key, b"c");
            assert_eq!(&*out[2].value, b"val_c");

            Ok(())
        }

        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn compaction_merge_no_operator_passthrough() -> crate::Result<()> {
            // Without merge operator, MergeOperand entries pass through unchanged
            #[rustfmt::skip]
            let vec = stream![
                "a", "op1", "M",
            ];

            let iter = vec.iter().cloned().map(Ok);
            let mut iter = CompactionStream::new(iter, 1_000);

            let item = iter.next().unwrap()?;
            assert_eq!(item.key.value_type, ValueType::MergeOperand);
            assert_eq!(&*item.value, b"op1");

            Ok(())
        }

        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn compaction_merge_with_weak_tombstone() -> crate::Result<()> {
            // Merge operand above weak tombstone → merge with no base
            #[rustfmt::skip]
            let vec = stream![
                "a", "op1", "M",
                "a", "", "W",
                "a", "old_val", "V",
            ];

            let iter = vec.iter().cloned().map(Ok);
            let mut iter = CompactionStream::new(iter, 1_000).with_merge_operator(Some(merge_op()));

            let item = iter.next().unwrap()?;
            assert_eq!(item.key.value_type, ValueType::Value);
            assert_eq!(&*item.value, b"op1");
            assert!(iter.next().is_none());

            Ok(())
        }

        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn compaction_merge_seqno_zeroing() -> crate::Result<()> {
            // Merged value should get seqno zeroed when below threshold
            #[rustfmt::skip]
            let vec = stream![
                "a", "op1", "M",
                "a", "base", "V",
            ];

            let iter = vec.iter().cloned().map(Ok);
            let mut iter = CompactionStream::new(iter, 1_000)
                .with_merge_operator(Some(merge_op()))
                .zero_seqnos(true);

            let item = iter.next().unwrap()?;
            assert_eq!(item.key.seqno, 0);
            assert_eq!(&*item.value, b"base,op1");

            Ok(())
        }

        /// When merge operands sit above an Indirection base, compaction must
        /// preserve ALL entries unchanged — no operand may be dropped.
        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn compaction_merge_indirection_base_preserves_all() -> crate::Result<()> {
            #[rustfmt::skip]
            let vec = stream![
                "a", "op2", "M",
                "a", "op1", "M",
                "a", "blob_ptr", "I",
            ];

            let iter = vec.iter().cloned().map(Ok);
            let mut iter = CompactionStream::new(iter, 1_000).with_merge_operator(Some(merge_op()));

            // All three entries must be emitted unchanged
            let item = iter.next().unwrap()?;
            assert_eq!(&*item.key.user_key, b"a");
            assert_eq!(item.key.value_type, ValueType::MergeOperand);
            assert_eq!(&*item.value, b"op2");

            let item = iter.next().unwrap()?;
            assert_eq!(&*item.key.user_key, b"a");
            assert_eq!(item.key.value_type, ValueType::MergeOperand);
            assert_eq!(&*item.value, b"op1");

            let item = iter.next().unwrap()?;
            assert_eq!(&*item.key.user_key, b"a");
            assert_eq!(item.key.value_type, ValueType::Indirection);
            assert_eq!(&*item.value, b"blob_ptr");

            assert!(iter.next().is_none());
            Ok(())
        }

        /// Exact GC boundary: head.seqno == gc_seqno_threshold should NOT merge.
        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn compaction_merge_at_exact_gc_boundary() -> crate::Result<()> {
            // gc_threshold=999; head.seqno=999 (NOT below threshold)
            // Entries should be preserved as-is
            let vec = vec![
                InternalValue::from_components("a", "op2", 999, ValueType::MergeOperand),
                InternalValue::from_components("a", "op1", 998, ValueType::MergeOperand),
            ];

            let iter = vec.iter().cloned().map(Ok);
            let mut iter = CompactionStream::new(iter, 999).with_merge_operator(Some(merge_op()));

            // head.seqno == gc_threshold → NOT below → preserved as MergeOperand
            let item = iter.next().unwrap()?;
            assert_eq!(item.key.value_type, ValueType::MergeOperand);
            assert_eq!(&*item.value, b"op2");

            Ok(())
        }

        /// DroppedKvCallback receives dropped merge operands during compaction.
        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn compaction_merge_dropped_callback() -> crate::Result<()> {
            #[rustfmt::skip]
            let vec = stream![
                "a", "op2", "M",
                "a", "op1", "M",
                "a", "base", "V",
            ];

            let mut callback = TrackCallback::default();

            let iter = vec.iter().cloned().map(Ok);
            let mut iter = CompactionStream::new(iter, 1_000)
                .with_merge_operator(Some(merge_op()))
                .with_drop_callback(&mut callback);

            let item = iter.next().unwrap()?;
            assert_eq!(item.key.value_type, ValueType::Value);
            assert_eq!(&*item.value, b"base,op1,op2");
            assert!(iter.next().is_none());

            // The base Value is consumed by merge (not dropped);
            // operands are consumed by merge (not dropped via callback).
            // DroppedKvCallback fires for entries DRAINED after base is found.
            // In this case there are no entries after the base, so callback
            // should have no items.
            assert!(callback.items.is_empty());

            Ok(())
        }

        /// Head above GC, peeked below GC: head must be preserved as MergeOperand.
        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn compaction_merge_head_above_gc_peeked_below() -> crate::Result<()> {
            // head.seqno=10 (above gc=7), peeked.seqno=5 (below gc=7)
            let vec = vec![
                InternalValue::from_components("a", "op_new", 10, ValueType::MergeOperand),
                InternalValue::from_components("a", "op_old", 5, ValueType::MergeOperand),
                InternalValue::from_components("a", "base", 2, ValueType::Value),
            ];

            let iter = vec.iter().cloned().map(Ok);
            let mut iter = CompactionStream::new(iter, 7).with_merge_operator(Some(merge_op()));

            // Head is above GC → emit as-is (MergeOperand)
            let item = iter.next().unwrap()?;
            assert_eq!(item.key.value_type, ValueType::MergeOperand);
            assert_eq!(&*item.value, b"op_new");

            // Remaining entries preserved for future merge resolution
            let item = iter.next().unwrap()?;
            assert_eq!(&*item.key.user_key, b"a");

            Ok(())
        }

        /// Merge operator error propagates correctly during compaction.
        #[test]
        fn compaction_merge_error_propagation() {
            struct FailMerge;
            impl crate::merge_operator::MergeOperator for FailMerge {
                fn merge(
                    &self,
                    _key: &[u8],
                    _base_value: Option<&[u8]>,
                    _operands: &[&[u8]],
                ) -> crate::Result<crate::UserValue> {
                    Err(crate::Error::MergeOperator)
                }
            }

            #[rustfmt::skip]
            let vec = stream![
                "a", "op1", "M",
                "a", "base", "V",
            ];

            let iter = vec.iter().cloned().map(Ok);
            let fail_op: Option<Arc<dyn crate::merge_operator::MergeOperator>> =
                Some(Arc::new(FailMerge));
            let mut iter = CompactionStream::new(iter, 1_000).with_merge_operator(fail_op);

            assert!(matches!(
                iter.next(),
                Some(Err(crate::Error::MergeOperator))
            ));
        }

        /// Complete merge (with base) emits Value; partial merge emits MergeOperand.
        #[test]
        fn compaction_merge_complete_vs_partial() -> crate::Result<()> {
            // Complete merge: operand + base → Value
            #[rustfmt::skip]
            let vec = stream![
                "a", "op1", "M",
                "a", "base", "V",
                "b", "op2", "M",
                "b", "op1", "M",
            ];

            let iter = vec.iter().cloned().map(Ok);
            let iter = CompactionStream::new(iter, 1_000).with_merge_operator(Some(merge_op()));
            let out: Vec<_> = iter.map(Result::unwrap).collect();

            assert_eq!(out.len(), 2);
            // "a": base found → complete merge → Value
            assert_eq!(out[0].key.value_type, ValueType::Value);
            assert_eq!(&*out[0].value, b"base,op1");
            // "b": no base → partial merge → MergeOperand
            assert_eq!(out[1].key.value_type, ValueType::MergeOperand);
            assert_eq!(&*out[1].value, b"op1,op2");

            Ok(())
        }

        /// Stream filter that replaces values preserves MergeOperand type.
        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn compaction_filter_preserves_merge_operand_type() -> crate::Result<()> {
            struct UpperFilter;
            impl StreamFilter for UpperFilter {
                fn filter_item(
                    &mut self,
                    _item: &InternalValue,
                ) -> crate::Result<StreamFilterVerdict> {
                    Ok(StreamFilterVerdict::Replace((
                        ValueType::Value,
                        b"REPLACED".to_vec().into(),
                    )))
                }
            }

            let vec = vec![InternalValue::from_components(
                "a",
                "op1",
                5,
                ValueType::MergeOperand,
            )];

            let iter = vec.iter().cloned().map(Ok);
            let mut iter = CompactionStream::new(iter, 1_000).with_filter(UpperFilter);

            let item = iter.next().unwrap()?;
            // Filter tried to set Value, but MergeOperand type must be preserved
            assert_eq!(item.key.value_type, ValueType::MergeOperand);
            assert_eq!(&*item.value, b"REPLACED");

            Ok(())
        }
    }
}

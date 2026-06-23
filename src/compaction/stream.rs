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

        // Drop collected operands that a covering applied range tombstone deletes
        // (they are pre-delete state): only operands newer than the tombstone fold
        // onto the now-empty base. Without this, an operand below the tombstone
        // would resurrect deleted state across compaction.
        collected.retain(|e| {
            let covered = self.covered_by_applied_tombstone(e.key.user_key.as_ref(), e.key.seqno);
            if covered && let Some(watcher) = &mut self.dropped_callback {
                watcher.on_dropped(e);
            }
            !covered
        });

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
mod tests;

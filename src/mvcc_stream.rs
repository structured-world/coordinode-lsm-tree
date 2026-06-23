// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::double_ended_peekable::{DoubleEndedPeekable, DoubleEndedPeekableExt};
use crate::merge_operator::MergeOperator;
use crate::range_tombstone::RangeTombstone;
use crate::{InternalValue, SeqNo, UserKey, UserValue, ValueType, comparator::SharedComparator};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// Consumes a stream of KVs and emits a new stream according to MVCC and tombstone rules
///
/// This iterator is used for read operations.
pub struct MvccStream<I: DoubleEndedIterator<Item = crate::Result<InternalValue>>> {
    inner: DoubleEndedPeekable<crate::Result<InternalValue>, I>,
    merge_operator: Option<Arc<dyn MergeOperator>>,
    comparator: SharedComparator,

    /// Range tombstones with per-source visibility cutoffs. When set, merge
    /// resolution skips entries suppressed by an RT (treats them as a
    /// tombstone boundary). Each tuple is `(tombstone, cutoff_seqno)`.
    range_tombstones: Vec<(RangeTombstone, SeqNo)>,

    /// Reusable buffer for reverse-iteration merge resolution. Avoids
    /// allocating a fresh `Vec` on every `next_back()` call.
    key_entries_buf: Vec<InternalValue>,
}

impl<I: DoubleEndedIterator<Item = crate::Result<InternalValue>>> MvccStream<I> {
    /// Initializes a new multi-version-aware iterator.
    #[must_use]
    pub fn new(iter: I, merge_operator: Option<Arc<dyn MergeOperator>>) -> Self {
        Self::new_with_comparator(
            iter,
            merge_operator,
            crate::comparator::default_comparator(),
        )
    }

    /// Initializes a new multi-version-aware iterator with the given comparator.
    #[must_use]
    pub fn new_with_comparator(
        iter: I,
        merge_operator: Option<Arc<dyn MergeOperator>>,
        comparator: SharedComparator,
    ) -> Self {
        Self {
            inner: iter.double_ended_peekable(),
            merge_operator,
            comparator,
            range_tombstones: Vec::new(),
            key_entries_buf: Vec::new(),
        }
    }

    /// Installs range tombstones for merge-resolution awareness.
    ///
    /// When set, operands or base values suppressed by a range tombstone are
    /// treated as a deletion boundary (merge stops, base = None).
    #[must_use]
    pub fn with_range_tombstones(mut self, rts: Vec<(RangeTombstone, SeqNo)>) -> Self {
        self.range_tombstones = rts;
        self
    }

    /// Returns true if the entry is suppressed by any installed range tombstone.
    fn is_rt_suppressed(&self, entry: &InternalValue) -> bool {
        self.range_tombstones.iter().any(|(rt, cutoff)| {
            rt.should_suppress_with(
                &entry.key.user_key,
                entry.key.seqno,
                *cutoff,
                self.comparator.as_ref(),
            )
        })
    }

    /// Collects all entries for the given key and applies the merge operator (forward).
    fn resolve_merge_forward(
        &mut self,
        head: &InternalValue,
        merge_op: &dyn MergeOperator,
    ) -> crate::Result<InternalValue> {
        let user_key = &head.key.user_key;
        let mut operands: Vec<UserValue> = vec![head.value.clone()];
        let mut base_value: Option<UserValue> = None;
        let mut found_base = false;
        let mut saw_indirection_base = false;

        // Collect remaining same-key entries
        loop {
            let Some(next) = self.inner.next_if(|kv| {
                if let Ok(kv) = kv {
                    kv.key.user_key == *user_key
                } else {
                    true
                }
            }) else {
                break;
            };

            let next = next?;

            // Range tombstone suppression: an RT-suppressed entry is logically
            // deleted — treat it as a tombstone boundary (no base value).
            if self.is_rt_suppressed(&next) {
                found_base = true;
                break;
            }

            match next.key.value_type {
                ValueType::MergeOperand => {
                    operands.push(next.value);
                }
                ValueType::Value => {
                    base_value = Some(next.value);
                    found_base = true;
                    break;
                }
                ValueType::Indirection => {
                    // Indirection payloads are internal blob pointers and must not be
                    // used as a merge base user value. Remember that we saw an
                    // indirection base so we can skip merge resolution for this key.
                    found_base = true;
                    saw_indirection_base = true;
                    break;
                }
                ValueType::Tombstone | ValueType::WeakTombstone => {
                    // Tombstone kills base
                    found_base = true;
                    break;
                }
            }
        }

        // Drain any remaining same-key entries
        if found_base {
            self.drain_key_min(user_key)?;
        }

        // If the base would be an indirection, do not attempt to resolve the merge;
        // just return the newest entry unchanged.
        if saw_indirection_base {
            return Ok(head.clone());
        }

        // Reverse to chronological order (ascending seqno)
        operands.reverse();

        let operand_refs: Vec<&[u8]> = operands.iter().map(AsRef::as_ref).collect();
        let merged = merge_op.merge(user_key, base_value.as_deref(), &operand_refs)?;

        Ok(InternalValue::from_components(
            user_key.clone(),
            merged,
            head.key.seqno,
            ValueType::Value,
        ))
    }

    /// Resolves buffered entries for reverse iteration merge.
    /// `entries` are in ascending seqno order (oldest first, as collected by `next_back`).
    fn resolve_merge_buffered(&self, entries: Vec<InternalValue>) -> crate::Result<InternalValue> {
        let Some(merge_op) = &self.merge_operator else {
            // No merge operator — return newest entry (last in ascending order)
            return entries
                .into_iter()
                .last()
                .ok_or(crate::Error::Unrecoverable);
        };

        // entries are in ascending seqno order (oldest→newest)
        // The newest entry (last) has the highest seqno — that's our result seqno.
        let newest = entries.last().ok_or(crate::Error::Unrecoverable)?;
        let mut operands: Vec<UserValue> = Vec::new();
        let mut base_value: Option<UserValue> = None;
        let result_seqno = newest.key.seqno;
        let result_key = newest.key.user_key.clone();

        // Process in descending seqno order (newest first) to match forward merge semantics
        let mut saw_indirection = false;

        for entry in entries.iter().rev() {
            // RT-suppressed entries are logically deleted — treat as tombstone.
            if self.is_rt_suppressed(entry) {
                break;
            }

            match entry.key.value_type {
                ValueType::MergeOperand => {
                    operands.push(entry.value.clone());
                }
                ValueType::Value => {
                    base_value = Some(entry.value.clone());
                    break;
                }
                ValueType::Indirection => {
                    // Do not use indirection bytes as a merge base; stop scanning
                    // older versions.
                    saw_indirection = true;
                    break;
                }
                ValueType::Tombstone | ValueType::WeakTombstone => {
                    break;
                }
            }
        }

        // If the base is an indirection, return the newest entry unchanged.
        if saw_indirection {
            return entries
                .into_iter()
                .last()
                .ok_or(crate::Error::Unrecoverable);
        }

        // Reverse operands to chronological order (ascending seqno)
        operands.reverse();

        let operand_refs: Vec<&[u8]> = operands.iter().map(AsRef::as_ref).collect();
        let merged = merge_op.merge(&result_key, base_value.as_deref(), &operand_refs)?;

        Ok(InternalValue::from_components(
            result_key,
            merged,
            result_seqno,
            ValueType::Value,
        ))
    }

    // Drains all entries for the given user key from the front of the iterator.
    fn drain_key_min(&mut self, key: &UserKey) -> crate::Result<()> {
        loop {
            let Some(next) = self.inner.next_if(|kv| {
                if let Ok(kv) = kv {
                    kv.key.user_key == key
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

impl<I> crate::reseek::Reseekable for MvccStream<I>
where
    I: DoubleEndedIterator<Item = crate::Result<InternalValue>> + crate::reseek::Reseekable,
{
    /// Clear the lookahead peek buffers and the reverse-merge scratch buffer,
    /// then forward the reposition to the inner merger. The installed range
    /// tombstones and merge operator are position-independent and stay as-is.
    fn reseek(&mut self, ctx: &crate::reseek::ReseekCtx) {
        self.inner.reset_front_peeked();
        self.inner.reset_back_peeked();
        self.key_entries_buf.clear();
        self.inner.inner_mut().reseek(ctx);
    }
}

impl<I: DoubleEndedIterator<Item = crate::Result<InternalValue>>> Iterator for MvccStream<I> {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        let head = fail_iter!(self.inner.next()?);

        if head.key.value_type.is_merge_operand() {
            // Clone the Arc (not the operator) — resolve_merge_forward needs
            // &mut self which conflicts with borrowing self.merge_operator.
            if let Some(merge_op) = self.merge_operator.clone()
                && !self.is_rt_suppressed(&head)
            {
                let result = self.resolve_merge_forward(&head, merge_op.as_ref());
                return Some(result);
            }
        }

        // As long as items are the same key, ignore them
        fail_iter!(self.drain_key_min(&head.key.user_key));

        Some(Ok(head))
    }
}

impl<I: DoubleEndedIterator<Item = crate::Result<InternalValue>>> DoubleEndedIterator
    for MvccStream<I>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        // When a merge operator is configured we must buffer ALL entries
        // for a key (not just MergeOperands) because we only learn that
        // merge is needed when we reach the newest entry (last in
        // reverse order). The base Value/Tombstone seen first must be
        // preserved for the merge function.
        //
        // NOTE: Lazy allocation (only buffer after seeing MergeOperand) is
        // incorrect — reverse iteration visits the oldest (base) entry first,
        // so deferring allocation until a MergeOperand is found would lose
        // the base Value needed by the merge function.
        let has_merge_op = self.merge_operator.is_some();
        self.key_entries_buf.clear();

        loop {
            let tail = fail_iter!(self.inner.next_back()?);

            let prev = match self.inner.peek_back() {
                Some(Ok(prev)) => prev,
                Some(Err(_)) => {
                    #[expect(
                        clippy::expect_used,
                        reason = "we just asserted, the peeked value is an error"
                    )]
                    return Some(Err(self
                        .inner
                        .next_back()
                        .expect("should exist")
                        .expect_err("should be error")));
                }
                None => {
                    // Last item — resolve merge only if newest entry is a MergeOperand
                    // and not RT-suppressed.
                    if has_merge_op
                        && tail.key.value_type.is_merge_operand()
                        && !self.is_rt_suppressed(&tail)
                    {
                        self.key_entries_buf.push(tail);
                        let entries = self.key_entries_buf.drain(..).collect();
                        return Some(self.resolve_merge_buffered(entries));
                    }
                    return Some(Ok(tail));
                }
            };

            if prev.key.user_key < tail.key.user_key {
                // `tail` is the newest entry for this key — boundary reached.
                // Only merge if the newest entry is a MergeOperand.
                if has_merge_op
                    && tail.key.value_type.is_merge_operand()
                    && !self.is_rt_suppressed(&tail)
                {
                    self.key_entries_buf.push(tail);
                    let entries = core::mem::take(&mut self.key_entries_buf);
                    return Some(self.resolve_merge_buffered(entries));
                }
                return Some(Ok(tail));
            }

            // Same key — buffer entry when merge operator is configured.
            // We must buffer ALL types (including Value/Tombstone) because
            // we don't yet know if the newest entry will be a MergeOperand.
            if has_merge_op {
                self.key_entries_buf.push(tail);
            }
            // Without merge operator: skip older versions (loop continues)
        }
    }
}

#[cfg(test)]
#[allow(clippy::string_lit_as_bytes)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::useless_vec,
    reason = "test code"
)]
mod tests;

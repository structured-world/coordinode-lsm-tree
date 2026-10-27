// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use crate::double_ended_peekable::{DoubleEndedPeekable, DoubleEndedPeekableExt};
use crate::merge_operator::MergeOperator;
use crate::range_tombstone::RangeTombstone;
use crate::{InternalValue, SeqNo, UserKey, UserValue, ValueType, comparator::SharedComparator};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// Reads a merge base kept in the value log, as `RocksDB`'s merge reads a
/// blob base before merging onto it.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot read a merge base kept in the value log",
    label = "not a value log reader",
    note = "a stream that never meets such a base uses `NoValueLog`"
)]
pub trait SeparatedBase {
    /// The value the indirection `base` points to.
    ///
    /// # Errors
    ///
    /// Returns an error when the value cannot be read.
    fn read(&self, base: InternalValue) -> crate::Result<UserValue>;
}

/// The reader of a stream that is not a blob tree's and cannot read a
/// separated base: the pointer is refused rather than handed to the operator
/// as if it were the value.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default)]
pub struct NoValueLog;

impl SeparatedBase for NoValueLog {
    fn read(&self, _base: InternalValue) -> crate::Result<UserValue> {
        Err(crate::Error::FeatureUnsupported(
            "merge-onto-separated-base-without-value-log",
        ))
    }
}

/// The value log a merge reads a base from when the base is an indirection:
/// a blob tree's blob source over the version being read.
#[derive(Clone, Copy)]
pub(crate) struct ValueLog<'v> {
    pub(crate) source: &'v crate::blob_tree::BlobSource,
    pub(crate) version: &'v crate::version::Version,
}

/// A stream whose tree may or may not be a blob tree, decided when it is
/// built.
impl SeparatedBase for Option<ValueLog<'_>> {
    fn read(&self, base: InternalValue) -> crate::Result<UserValue> {
        match self {
            Some(log) => log.source.value(log.version, base),
            None => NoValueLog.read(base),
        }
    }
}

/// How a stream passes the older versions of a key it has resolved.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot pass the versions of a resolved key for a stream over `{I}`",
    label = "not a version-passing policy for this source",
    note = "`Walk` walks any source; `Jump` needs a source implementing `SkipKey`"
)]
pub trait PassVersions<I> {
    /// Moves `inner` past the rest of `key`'s versions in one step; `false`
    /// when it cannot, and the stream walks them.
    ///
    /// # Errors
    ///
    /// The error the source met on the way.
    fn jump(inner: &mut I, key: &[u8]) -> crate::Result<bool>;

    /// Moves the back of `inner` up to the newest version of `key` it holds,
    /// past the older ones; `false` when it cannot, and the stream walks them.
    ///
    /// # Errors
    ///
    /// The error the source met on the way.
    fn jump_back(inner: &mut I, key: &[u8]) -> crate::Result<bool>;
}

/// Walks every version of a resolved key: what a source that cannot jump
/// gets, and what a point read, which holds few versions, needs.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default)]
pub struct Walk;

impl<I> PassVersions<I> for Walk {
    fn jump(_inner: &mut I, _key: &[u8]) -> crate::Result<bool> {
        Ok(false)
    }

    fn jump_back(_inner: &mut I, _key: &[u8]) -> crate::Result<bool> {
        Ok(false)
    }
}

/// Walks the first [`WALKED_BEFORE_JUMP`] versions of a resolved key, then
/// jumps past the rest: a short chain costs a jump more than it saves, a long
/// one costs a step per version walked.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default)]
pub struct Jump;

impl<I: crate::skip_key::SkipKey> PassVersions<I> for Jump {
    fn jump(inner: &mut I, key: &[u8]) -> crate::Result<bool> {
        inner.skip_front_past(key)
    }

    /// The sources filter by their own snapshot; the stream sees only what
    /// they pass.
    fn jump_back(inner: &mut I, key: &[u8]) -> crate::Result<bool> {
        inner.skip_back_to_newest(key, SeqNo::MAX)
    }
}

/// The versions of a resolved key [`Jump`] walks before it jumps.
const WALKED_BEFORE_JUMP: usize = 8;

/// Consumes a stream of KVs and emits a new stream according to MVCC and tombstone rules
///
/// This iterator is used for read operations.
pub struct MvccStream<
    I: DoubleEndedIterator<Item = crate::Result<InternalValue>>,
    L = NoValueLog,
    P = Walk,
> {
    inner: DoubleEndedPeekable<crate::Result<InternalValue>, I>,
    merge_operator: Option<Arc<dyn MergeOperator>>,
    comparator: SharedComparator,

    /// Reads a base the stream finds kept in the value log. Only a blob
    /// tree's stream meets such a base.
    value_log: L,

    /// Range tombstones with per-source visibility cutoffs. When set, merge
    /// resolution skips entries suppressed by an RT (treats them as a
    /// tombstone boundary). Each tuple is `(tombstone, cutoff_seqno)`.
    range_tombstones: Vec<(RangeTombstone, SeqNo)>,

    /// Reusable buffer for reverse-iteration merge resolution. Avoids
    /// allocating a fresh `Vec` on every `next_back()` call.
    key_entries_buf: Vec<InternalValue>,

    /// The key whose resolution from the front ended in an error, and whose
    /// remaining versions the next forward call skips before anything else:
    /// they are older than the failed one and never stand for the key.
    skip_front: Option<UserKey>,

    /// The same for the back: the newer versions of a key whose older ones
    /// went with an error, which never resolve without them.
    skip_back: Option<UserKey>,

    /// How the versions of a resolved key are passed.
    pass: core::marker::PhantomData<P>,
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
            value_log: NoValueLog,
            range_tombstones: Vec::new(),
            key_entries_buf: Vec::new(),
            skip_front: None,
            skip_back: None,
            pass: core::marker::PhantomData,
        }
    }
}

impl<I: DoubleEndedIterator<Item = crate::Result<InternalValue>>, P> MvccStream<I, NoValueLog, P> {
    /// Installs the value log a merge reads a base kept there from.
    #[must_use]
    pub(crate) fn with_value_log(
        self,
        value_log: Option<ValueLog<'_>>,
    ) -> MvccStream<I, Option<ValueLog<'_>>, P> {
        MvccStream {
            inner: self.inner,
            merge_operator: self.merge_operator,
            comparator: self.comparator,
            value_log,
            range_tombstones: self.range_tombstones,
            key_entries_buf: self.key_entries_buf,
            skip_front: self.skip_front,
            skip_back: self.skip_back,
            pass: core::marker::PhantomData,
        }
    }
}

impl<I: DoubleEndedIterator<Item = crate::Result<InternalValue>>, L> MvccStream<I, L, Walk> {
    /// Jumps past the versions of a resolved key past the first few, rather
    /// than walking them all: for a source that can (see [`Jump`]).
    #[must_use]
    pub(crate) fn jumping(self) -> MvccStream<I, L, Jump>
    where
        Jump: PassVersions<I>,
    {
        MvccStream {
            inner: self.inner,
            merge_operator: self.merge_operator,
            comparator: self.comparator,
            value_log: self.value_log,
            range_tombstones: self.range_tombstones,
            key_entries_buf: self.key_entries_buf,
            skip_front: self.skip_front,
            skip_back: self.skip_back,
            pass: core::marker::PhantomData,
        }
    }
}

impl<
    I: DoubleEndedIterator<Item = crate::Result<InternalValue>>,
    L: SeparatedBase,
    P: PassVersions<I>,
> MvccStream<I, L, P>
{
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

        // Collect remaining same-key entries
        loop {
            let Some(next) = self.inner.next_if(|kv| {
                if let Ok(kv) = kv {
                    crate::comparator::same_held_key(&kv.key.user_key, user_key)
                } else {
                    true
                }
            }) else {
                break;
            };

            let next = match next {
                Ok(next) => next,
                Err(e) => {
                    self.skip_front = Some(user_key.clone());
                    return Err(e);
                }
            };

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
                ValueType::Indirection | ValueType::CellRow => {
                    // Read now, as RocksDB's iterator fetches a blob base when
                    // it lands on a merged key: the merged value is the item
                    // this stream yields, so there is no pointer left for a
                    // guard to resolve later. Only an unmerged value stays
                    // lazy. After a failed read the key's remaining versions
                    // are skipped, so a caller that goes on past the error
                    // does not get one the base shadows as the key's value.
                    match self.value_log.read(next) {
                        Ok(value) => base_value = Some(value),
                        Err(e) => {
                            self.skip_front = Some(user_key.clone());
                            return Err(e);
                        }
                    }
                    found_base = true;
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
        for entry in entries.into_iter().rev() {
            // RT-suppressed entries are logically deleted — treat as tombstone.
            if self.is_rt_suppressed(&entry) {
                break;
            }

            match entry.key.value_type {
                ValueType::MergeOperand => {
                    operands.push(entry.value);
                }
                ValueType::Value => {
                    base_value = Some(entry.value);
                    break;
                }
                ValueType::Indirection | ValueType::CellRow => {
                    base_value = Some(self.value_log.read(entry)?);
                    break;
                }
                ValueType::Tombstone | ValueType::WeakTombstone => {
                    break;
                }
            }
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
    //
    // An error ends the drain and is returned at once; the key is remembered
    // in `skip_front` so the next call skips its remaining versions instead of
    // yielding the next older one as the key's value.
    //
    // A chain longer than `WALKED_BEFORE_JUMP` is jumped past when the policy
    // and the source allow, and only while no item is peeked: a peeked item is
    // one the source has already handed over, which a jump would not see.
    fn drain_key_min(&mut self, key: &UserKey) -> crate::Result<()> {
        let mut walked = 0usize;
        loop {
            let Some(next) = self.inner.next_if(|kv| {
                if let Ok(kv) = kv {
                    crate::comparator::same_held_key(&kv.key.user_key, key)
                } else {
                    true
                }
            }) else {
                return Ok(());
            };

            if let Err(e) = next {
                self.skip_front = Some(key.clone());
                return Err(e);
            }

            walked += 1;
            if walked == WALKED_BEFORE_JUMP && self.inner.holds_no_peek() {
                match P::jump(self.inner.inner_mut(), key) {
                    Ok(true) => return Ok(()),
                    Ok(false) => {}
                    Err(e) => {
                        self.skip_front = Some(key.clone());
                        return Err(e);
                    }
                }
            }
        }
    }

    // Goes on skipping the front key a failed resolution left. Yields the
    // next error met on the way, one per call, and clears the skip once the
    // key is behind.
    fn resume_skip_front(&mut self) -> Option<crate::Error> {
        let key = self.skip_front.take()?;
        loop {
            let next = self.inner.next_if(|kv| {
                if let Ok(kv) = kv {
                    crate::comparator::same_held_key(&kv.key.user_key, &key)
                } else {
                    true
                }
            })?;
            if let Err(e) = next {
                self.skip_front = Some(key);
                return Some(e);
            }
        }
    }

    // The back counterpart of `resume_skip_front`.
    fn resume_skip_back(&mut self) -> Option<crate::Error> {
        let key = self.skip_back.take()?;
        while let Some(prev) = self.inner.peek_back() {
            if let Ok(prev) = prev
                && !crate::comparator::same_held_key(&prev.key.user_key, &key)
            {
                return None;
            }
            if let Some(Err(e)) = self.inner.next_back() {
                self.skip_back = Some(key);
                return Some(e);
            }
        }
        None
    }
}

impl<I, L, P> crate::reseek::Reseekable for MvccStream<I, L, P>
where
    I: DoubleEndedIterator<Item = crate::Result<InternalValue>> + crate::reseek::Reseekable,
{
    /// Clear the lookahead peek buffers, the reverse-merge scratch buffer and
    /// the keys left to skip after an error, then forward the reposition to
    /// the inner merger. The installed range tombstones and merge operator are
    /// position-independent and stay as-is.
    fn reseek(&mut self, ctx: &crate::reseek::ReseekCtx) {
        self.inner.reset_front_peeked();
        self.inner.reset_back_peeked();
        self.key_entries_buf.clear();
        self.skip_front = None;
        self.skip_back = None;
        self.inner.inner_mut().reseek(ctx);
    }
}

impl<
    I: DoubleEndedIterator<Item = crate::Result<InternalValue>>,
    L: SeparatedBase,
    P: PassVersions<I>,
> Iterator for MvccStream<I, L, P>
{
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        // The front's own failed key first: only once it is behind can the
        // next key be checked against the back's.
        if self.skip_front.is_some()
            && let Some(e) = self.resume_skip_front()
        {
            return Some(Err(e));
        }
        // The back failed on a key: what reaches the front of it is what the
        // failure left, never the key's value, so it is skipped before it is
        // resolved (resolving could only fail again).
        if let Some(failed) = self.skip_back.take() {
            match self.skip_key_at_front(&failed) {
                Skipped::Done => {}
                Skipped::NotReached => self.skip_back = Some(failed),
                Skipped::Error(e) => {
                    self.skip_back = Some(failed);
                    return Some(Err(e));
                }
            }
        }
        self.step_front()
    }
}

/// What skipping a failed key at one end found.
enum Skipped {
    /// The key's versions were met there and are all behind.
    Done,
    /// The key has not reached that end yet.
    NotReached,
    /// An error was met among the key's versions; more may follow.
    Error(crate::Error),
}

impl<
    I: DoubleEndedIterator<Item = crate::Result<InternalValue>>,
    L: SeparatedBase,
    P: PassVersions<I>,
> MvccStream<I, L, P>
{
    // Skips `key`'s versions at the front, up to the first error met.
    fn skip_key_at_front(&mut self, key: &UserKey) -> Skipped {
        let mut met = false;
        loop {
            let Some(next) = self.inner.next_if(|kv| {
                if let Ok(kv) = kv {
                    crate::comparator::same_held_key(&kv.key.user_key, key)
                } else {
                    // An error is taken only once the key was met: before
                    // that it belongs to whatever key comes first.
                    met
                }
            }) else {
                return if met || self.inner.peek().is_none() {
                    Skipped::Done
                } else {
                    Skipped::NotReached
                };
            };
            match next {
                Ok(_) => met = true,
                Err(e) => return Skipped::Error(e),
            }
        }
    }

    // The back counterpart of `skip_key_at_front`.
    fn skip_key_at_back(&mut self, key: &UserKey) -> Skipped {
        let mut met = false;
        while let Some(prev) = self.inner.peek_back() {
            match prev {
                Ok(prev) if !crate::comparator::same_held_key(&prev.key.user_key, key) => {
                    break;
                }
                Err(_) if !met => break,
                _ => {}
            }
            match self.inner.next_back() {
                Some(Err(e)) => return Skipped::Error(e),
                Some(Ok(_)) => met = true,
                None => break,
            }
        }
        if met || self.inner.peek_back().is_none() {
            Skipped::Done
        } else {
            Skipped::NotReached
        }
    }
}

impl<
    I: DoubleEndedIterator<Item = crate::Result<InternalValue>>,
    L: SeparatedBase,
    P: PassVersions<I>,
> MvccStream<I, L, P>
{
    // One forward item, once `next` has dealt with both ends' failed keys.
    fn step_front(&mut self) -> Option<crate::Result<InternalValue>> {
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

impl<
    I: DoubleEndedIterator<Item = crate::Result<InternalValue>>,
    L: SeparatedBase,
    P: PassVersions<I>,
> DoubleEndedIterator for MvccStream<I, L, P>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        // The mirror of `next`: the back's own failed key first, then a key
        // the front failed on is skipped here before it is resolved.
        if self.skip_back.is_some()
            && let Some(e) = self.resume_skip_back()
        {
            return Some(Err(e));
        }
        if let Some(failed) = self.skip_front.take() {
            match self.skip_key_at_back(&failed) {
                Skipped::Done => {}
                Skipped::NotReached => self.skip_front = Some(failed),
                Skipped::Error(e) => {
                    self.skip_front = Some(failed);
                    return Some(Err(e));
                }
            }
        }
        self.step_back()
    }
}

impl<
    I: DoubleEndedIterator<Item = crate::Result<InternalValue>>,
    L: SeparatedBase,
    P: PassVersions<I>,
> MvccStream<I, L, P>
{
    // One backward item, once `next_back` has dealt with both ends' failed keys.
    fn step_back(&mut self) -> Option<crate::Result<InternalValue>> {
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
        let mut walked = 0usize;

        loop {
            let tail = fail_iter!(self.inner.next_back()?);

            let prev = match self.inner.peek_back() {
                Some(Ok(prev)) => prev,
                Some(Err(_)) => {
                    #[expect(
                        clippy::expect_used,
                        reason = "we just asserted, the peeked value is an error"
                    )]
                    let error = self
                        .inner
                        .next_back()
                        .expect("should exist")
                        .expect_err("should be error");
                    // The versions of `tail`'s key buffered so far are gone
                    // with the error; its newer ones are skipped too, so none
                    // resolves without them (a merge without its base).
                    self.key_entries_buf.clear();
                    self.skip_back = Some(tail.key.user_key);
                    return Some(Err(error));
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

            // Key boundary = DIFFERENT key, by identity (byte equality), not by
            // bytewise `<`: the merger already yields comparator order, so
            // `prev` is never a later key than `tail` — but under a custom
            // comparator a different key may sort bytewise-higher, and a
            // bytewise `<` here classified it as "same key" and dropped it.
            // Byte equality is also cheaper (length short-circuit, no ordering).
            if !crate::comparator::same_held_key(&prev.key.user_key, &tail.key.user_key) {
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
                continue;
            }
            // Without merge operator: skip older versions (loop continues).
            // Every `WALKED_BEFORE_JUMP` of them the back jumps up to the
            // newest version the source holds. The peeked `prev` was handed
            // over before that, so it is older than anything the source still
            // holds and goes too. A merge keeps every operand, so it walks.
            walked += 1;
            if walked.is_multiple_of(WALKED_BEFORE_JUMP) && self.inner.front_unpeeked() {
                match P::jump_back(self.inner.inner_mut(), &tail.key.user_key) {
                    Ok(true) => self.inner.reset_back_peeked(),
                    Ok(false) => {}
                    Err(e) => {
                        // As an error met while walking: the key's newer
                        // versions are skipped, none resolves it.
                        self.skip_back = Some(tail.key.user_key);
                        return Some(Err(e));
                    }
                }
            }
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

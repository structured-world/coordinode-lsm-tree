// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

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

/// The peekable input, counting the versions that LEAVE it.
///
/// It wraps the peekable rather than sitting under it, because `peek` pulls an
/// entry from the source to fill its cache: counting there would charge the
/// stream for a version it has not taken yet, and a run abandoned mid-way (a
/// stop signal, whose partial output is still installed) would leave that
/// prefetch on the balance forever.
///
/// Counting on the way OUT keeps the by-construction property that a list of
/// call sites could not: every consumption goes through `next` or `next_if`,
/// so the fold's drain, a merge resolution taking a base inline, a
/// range-tombstone drop and an eviction all register themselves, including
/// ways of consuming that do not exist yet (see `gc_balance`).
struct CountingPeek<I: Iterator<Item = Item>> {
    inner: Peekable<I>,
    balance: Arc<portable_atomic::AtomicU64>,
}

impl<I: Iterator<Item = Item>> CountingPeek<I> {
    fn new(iter: I, balance: Arc<portable_atomic::AtomicU64>) -> Self {
        Self {
            inner: iter.peekable(),
            balance,
        }
    }

    /// Fills the cache without taking anything, so it does not count.
    fn peek(&mut self) -> Option<&Item> {
        self.inner.peek()
    }

    fn count_taken(&self, taken: Option<&Item>) {
        if matches!(taken, Some(Ok(_))) {
            self.balance
                .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        }
    }

    fn next(&mut self) -> Option<Item> {
        let taken = self.inner.next();
        self.count_taken(taken.as_ref());
        taken
    }

    fn next_if(&mut self, func: impl FnOnce(&Item) -> bool) -> Option<Item> {
        let taken = self.inner.next_if(func);
        self.count_taken(taken.as_ref());
        taken
    }
}

/// Answers whether a compaction's inputs hold every version of one key within
/// one sequence-number interval, given as `(key, oldest, newest)` inclusive.
/// See [`CompactionStream::with_input_completeness`].
///
/// Borrowed rather than owned so the stream carries no destructor that could
/// observe the caller's borrows, and so declaring it costs no allocation.
pub type InputCompleteness<'a> = &'a dyn Fn(&[u8], SeqNo, SeqNo) -> bool;

/// What a compaction may do with the range tombstones it was given.
///
/// Both uses end a merge chain, because a range tombstone between two operands
/// hides the ones below it exactly as an in-stream tombstone does. Only a
/// bottommost compaction may also delete what they cover: elsewhere the
/// tombstone is still propagating and a lower level may hold versions it hides.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RangeTombstoneUse {
    /// End merge chains only.
    Boundary,

    /// End merge chains, and physically drop the covered entries.
    Delete,
}

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

    /// Whether this filter only ever returns
    /// [`StreamFilterVerdict::Keep`].
    ///
    /// Answering `true` is a promise, and it buys the caller the right to stop
    /// asking. The merge stream uses it to decide whether a chain may be
    /// composed: every entry reaches `filter_item` exactly once on the way
    /// out, but a composed chain is written once and the operands inside it
    /// never come back, so their verdicts would have to be collected during
    /// the fold — asking a second time on the way there is not allowed, since
    /// a filter need not be idempotent or stateless. A filter that can only
    /// keep has no verdict to collect, so the question does not arise.
    ///
    /// Defaults to `false`, which costs nothing but a declined fold.
    fn keeps_everything(&self) -> bool {
        false
    }
}

/// A [`StreamFilter`] that does not modify anything
pub struct NoFilter;

impl StreamFilter for NoFilter {
    fn filter_item(&mut self, _item: &InternalValue) -> crate::Result<StreamFilterVerdict> {
        Ok(StreamFilterVerdict::Keep)
    }

    fn keeps_everything(&self) -> bool {
        true
    }
}

/// Consumes a stream of KVs and emits a new stream according to GC and tombstone rules
///
/// This iterator is used during flushing & compaction.
pub struct CompactionStream<'a, I: Iterator<Item = Item>, F: StreamFilter = NoFilter> {
    /// KV stream
    inner: CountingPeek<I>,

    /// The GC watermark: the stream keeps every version a snapshot at or above
    /// it reads, and may drop a version only snapshots below it could see.
    gc_watermark: SeqNo,

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

    /// Range tombstones strictly below the watermark (`seqno < gc_watermark`)
    /// whose covered entries can be physically dropped
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

    /// What the installed tombstones are allowed to do here.
    rt_use: RangeTombstoneUse,

    /// Answers, for one key, whether every version of it is in this
    /// compaction's inputs. `None` means the caller never declared it, and
    /// then nothing is composed. See [`Self::with_input_completeness`].
    completeness: Option<InputCompleteness<'a>>,

    /// Ticked on every VISIBILITY-CHANGING drop this merge performs itself —
    /// a bottommost tombstone elision (and the versions it drains), a
    /// range-tombstone application, a weak-tombstone annihilation. Such a
    /// drop makes the output non-derivable from its inputs exactly like a
    /// compaction-filter verdict (a lingering input published beside a
    /// partially surviving run would resurrect the deleted data), so the
    /// table writer marks the affected output's lineage transformed through
    /// the same counter the filter adapter ticks. Obsolete-version drops do
    /// NOT tick: the newer version shadowing them lives in the output.
    transform_marker: Option<Arc<portable_atomic::AtomicU64>>,

    /// Versions consumed from the input and not emitted, which answers the
    /// install's question that neither counter above can: did this run collect
    /// any history? A run that collected none must not raise the retention
    /// floor, or it refuses snapshots whose data is still there. An empty
    /// output is not that signal, since a watermark above every version
    /// collects the lot and writes no table.
    ///
    /// It is a BALANCE rather than a list of drop sites: `Counting` adds on
    /// every consumption and [`Self::settle_one`] subtracts on every emit, so
    /// any way of losing a version registers itself. The polarity is chosen so
    /// that an oversight over-reports (the floor rises, reads are refused)
    /// rather than under-reports (the floor stays put and a read is answered
    /// from data that is gone). Only the deliberately visibility-neutral drops
    /// excuse themselves, through [`Self::note_neutral_drop`].
    ///
    /// Consumption always precedes the matching emit, including for entries
    /// parked in `pending`, so this never underflows.
    gc_balance: Arc<portable_atomic::AtomicU64>,
}

impl<I: Iterator<Item = Item>> CompactionStream<'_, I, NoFilter> {
    /// Initializes a new merge iterator
    #[must_use]
    pub fn new(iter: I, gc_watermark: SeqNo) -> Self {
        let gc_balance = Arc::new(portable_atomic::AtomicU64::new(0));
        let iter = CountingPeek::new(iter, Arc::clone(&gc_balance));

        Self {
            inner: iter,
            gc_balance,
            gc_watermark,
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
            rt_use: RangeTombstoneUse::Boundary,
            completeness: None,
            transform_marker: None,
        }
    }
}

impl<'a, I: Iterator<Item = Item>, F: StreamFilter + 'a> CompactionStream<'a, I, F> {
    /// Installs a filter into this stream.
    pub fn with_filter<NF: StreamFilter>(self, filter: NF) -> CompactionStream<'a, I, NF> {
        CompactionStream {
            inner: self.inner,
            gc_watermark: self.gc_watermark,
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
            rt_use: self.rt_use,
            completeness: self.completeness,
            transform_marker: self.transform_marker,
            gc_balance: self.gc_balance,
        }
    }

    pub fn evict_tombstones(mut self, b: bool) -> Self {
        self.evict_tombstones = b;
        self
    }

    /// Wires the shared transform counter (see the `transform_marker` field);
    /// the same counter the filter adapter ticks on non-`Keep` verdicts.
    #[must_use]
    pub fn with_transform_marker(mut self, marker: Arc<portable_atomic::AtomicU64>) -> Self {
        self.transform_marker = Some(marker);
        self
    }

    /// Handle on the collected-history balance (see the `gc_balance` field),
    /// which the install reads once the stream is drained to tell a run that
    /// collected history from one that collected none. Non-zero means some
    /// version went in and did not come out.
    #[must_use]
    pub fn gc_balance(&self) -> Arc<portable_atomic::AtomicU64> {
        Arc::clone(&self.gc_balance)
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
    /// (`seqno < gc_watermark`) and higher than the entry's seqno are
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
    /// Production installs tombstones through [`Self::with_range_tombstones`],
    /// whose use is decided once by the compaction worker; this reads better in
    /// a test that is specifically about deletion.
    #[cfg(test)]
    #[must_use]
    pub fn with_range_tombstone_application(
        self,
        tombstones: Vec<RangeTombstone>,
        comparator: SharedComparator,
    ) -> Self {
        self.with_range_tombstones(tombstones, comparator, RangeTombstoneUse::Delete)
    }

    /// The same tombstones, for a compaction that may NOT delete what they
    /// cover.
    ///
    /// A range tombstone ends a merge chain the way an in-stream `Tombstone`
    /// does: operands below it are hidden and operands above it fold onto an
    /// absent base. A compaction off the last level still has to know that,
    /// because composing across the break would produce one operand newer than
    /// the tombstone, which then survives the read the tombstone should have
    /// cut. It may not physically drop the covered entries, though: the
    /// tombstone is still propagating and a lower level may hold versions it is
    /// hiding.
    ///
    /// So this installs them as boundaries only. Nothing is dropped for
    /// coverage; the sole effect is that a chain crossing one is not composed.
    ///
    /// Production goes through [`Self::with_range_tombstones`]; this is the
    /// readable form for a test that is specifically about boundaries.
    #[cfg(test)]
    #[must_use]
    pub fn with_range_tombstone_barriers(
        self,
        tombstones: Vec<RangeTombstone>,
        comparator: SharedComparator,
    ) -> Self {
        self.with_range_tombstones(tombstones, comparator, RangeTombstoneUse::Boundary)
    }

    /// Installs the tombstones for the use a caller has already decided on.
    #[must_use]
    pub fn with_range_tombstones(
        mut self,
        tombstones: Vec<RangeTombstone>,
        comparator: SharedComparator,
        use_: RangeTombstoneUse,
    ) -> Self {
        self.rt_apply = tombstones
            .into_iter()
            // Strict visibility (`seqno < threshold`), matching the read path and
            // the point-key GC: a tombstone exactly at the watermark is still
            // invisible to the oldest live snapshot (which reads at the
            // watermark), so it must NOT physically drop covered keys yet.
            .filter(|rt| rt.visible_at(self.gc_watermark))
            .collect();
        self.rt_active = Some(ActiveTombstoneSet::new_with_comparator(comparator.clone()));
        self.rt_comparator = Some(comparator);
        self.rt_use = use_;
        self
    }

    /// Declares how to tell whether a key's versions are all in this
    /// compaction's inputs.
    ///
    /// Composing a chain needs more than the operands in front of it: it needs
    /// to know they are ALL of them. A strategy may select some runs and leave
    /// others out (`SizeTiered` picks by size), and an omitted run can hold a
    /// version of the same key between two selected ones — another operand, a
    /// put, or a point tombstone. Fold across that and the result carries state
    /// the omitted entry was supposed to reset or reorder, and unlike an
    /// omitted operand no property of the operator can repair it.
    ///
    /// The question is asked per key AND per sequence-number interval, because
    /// both wider forms answer "incomplete" almost always. A neighbouring
    /// table spanning the same keys says nothing about whether it holds THIS
    /// key; and a table that does hold it, but only in versions older than the
    /// chain being folded, cannot sit between two of those operands — it is
    /// the base they will meet later, not a break in the middle.
    ///
    /// The predicate may answer conservatively, since a false "not complete"
    /// only declines a fold, but it must never claim completeness it cannot
    /// prove.
    ///
    /// Without this declaration nothing is composed.
    #[must_use]
    pub fn with_input_completeness(mut self, predicate: InputCompleteness<'a>) -> Self {
        self.completeness = Some(predicate);
        self
    }

    /// Whether this compaction's inputs hold every version of `user_key`
    /// between `oldest` and `newest`. Reached only for a chain a composing
    /// operator is about to fold, not per entry.
    fn inputs_hold_the_chain(&self, user_key: &[u8], oldest: SeqNo, newest: SeqNo) -> bool {
        self.completeness
            .as_ref()
            .is_some_and(|predicate| predicate(user_key, oldest, newest))
    }

    /// Whether a covering tombstone may physically drop what it covers, as
    /// opposed to only ending a merge chain; the use is fixed when the
    /// tombstones are installed through [`Self::with_range_tombstones`].
    fn covered_and_deletable(&mut self, user_key: &[u8], seqno: SeqNo) -> bool {
        self.rt_use == RangeTombstoneUse::Delete
            && self.covered_by_applied_tombstone(user_key, seqno)
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
    /// Collects subsequent same-key entries and folds them onto the base, which
    /// the stream has to have proven: a boundary it found, or absence at the
    /// bottom level. The result is then the key's `Value`.
    ///
    /// Without a proven base the outcome depends on the operator. By default
    /// the operands are re-emitted unchanged and the fold waits for the level
    /// that holds a base. An operator whose
    /// [`MergeOperator::composes_operands`] is set folds them here instead and
    /// the result is a `MergeOperand`, which meets the base wherever it is; if
    /// that operator refuses the composition, the operands are re-emitted as
    /// they would have been by default.
    /// [`Self::resolve_merge_operands`] with the stream's own operator. The
    /// resolver needs `&mut self` for the input stream, so the operator cannot
    /// be borrowed across the call; it is MOVED out and back instead of
    /// cloning its `Arc` per merged key (one refcount bump and drop each).
    /// Restored on every path, including an error. A stream without an
    /// operator returns `head` untouched.
    fn resolve_with_operator(&mut self, head: InternalValue) -> crate::Result<InternalValue> {
        let Some(merge_op) = self.merge_operator.take() else {
            return Ok(head);
        };
        let result = self.resolve_merge_operands(head, merge_op.as_ref());
        self.merge_operator = Some(merge_op);
        result
    }

    /// Which reads this may be invisible to: those AT OR BELOW the head's
    /// seqno `H` in the ordinary case, and up to a covering tombstone's seqno
    /// when one applies. The result carries `H`, and visibility is strict
    /// (`entry.seqno < read_seqno`), so a read at exactly `H` does not see the
    /// result either, while before the fold it saw the consumed operand or
    /// base sitting below `H`. The boundary is `R <= H`, not `R < H`; a call
    /// site preserving the wrong one would leave the read at `H` unaccounted
    /// for.
    ///
    /// An applied range tombstone at `T` with `H < T < watermark` widens it.
    /// The retain below drops the head as covered, the emit path then drops the
    /// merged result for the same reason, and the key leaves entirely: a read
    /// at `H < R <= T` resolved to the operand before (the tombstone was not
    /// visible to it yet) and resolves to nothing after. So the range is
    /// `R <= max(H, T)`, still strictly under the watermark, since applied
    /// tombstones are filtered to below it.
    ///
    /// All THREE call sites enter only with the head below the watermark (the
    /// key-boundary lone operand, the same-key merge arm, and the end-of-stream
    /// operand), so everything consumed here is below it too and so is `H`
    /// itself, which puts the whole affected range under the install's floor. A
    /// call site that folded a head at or above the watermark would break that,
    /// which is why none of them does.
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
                    crate::comparator::same_user_key(&peeked.key.user_key, &user_key)
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
                    // A covered base is not a base: the tombstone hides it from
                    // every reader. Where this compaction may delete it, it
                    // goes and the operands fold onto the empty base it leaves
                    // (below). Where it may not, the base must still not be
                    // folded onto, and it must still be emitted, so the only
                    // correct move is to stop and put everything back: the fold
                    // then happens at the level that applies the tombstone.
                    // Using it as a base here would republish deleted state
                    // under the head's seqno.
                    if self.rt_use == RangeTombstoneUse::Boundary
                        && self.covered_by_applied_tombstone(user_key.as_ref(), next.key.seqno)
                    {
                        collected.push(next);
                        let mut iter = collected.into_iter();
                        #[expect(clippy::expect_used, reason = "collected always has head")]
                        let first = iter
                            .next()
                            .expect("collected should contain at least one element");
                        self.pending.extend(iter);
                        return Ok(first);
                    }
                    // A covering applied range tombstone newer than this value
                    // deletes it, so the merge operands must fold onto an empty
                    // base instead of the value being physically dropped. Without
                    // this, a compaction resurrects a range-deleted key whenever a
                    // later merge operand exists (the read path before compaction
                    // already folds onto the empty base).
                    if self.covered_and_deletable(user_key.as_ref(), next.key.seqno) {
                        if let Some(watcher) = &mut self.dropped_callback {
                            watcher.on_dropped(&next);
                        }
                        self.note_transform();
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
                    // Tombstone kills base — merge with no base. The tombstone
                    // itself is consumed by the fold, so the output no longer
                    // carries it: a visibility transform.
                    found_boundary = true;
                    if let Some(watcher) = &mut self.dropped_callback {
                        watcher.on_dropped(&next);
                    }
                    self.note_transform();
                    let drained = self.drain_key(&user_key)?;
                    // The boundary tombstone and an all-tombstone tail under it
                    // are neutral at the bottom level, the same neutrality the
                    // plain fold excuses: every snapshot that resolved to one of
                    // them read the key as absent, and reads it absent from
                    // nothing afterwards.
                    //
                    // Off the bottom level it is not neutral, because a lower
                    // level may hold the version the tombstone was hiding. And
                    // a value drained under the tombstone is collection at any
                    // level: the merged result carries the head's seqno, so the
                    // snapshot that resolved to that value no longer can.
                    //
                    // The operands consumed into the result stay counted for the
                    // same reason, so this settles the tombstone and its tail
                    // only.
                    if self.evict_tombstones && drained.all_tombstones {
                        self.settle(drained.total + 1);
                    }
                    break;
                }
            }
        }

        // The operator is only ever handed a base this stream has PROVEN: the
        // boundary it just found, or absence. Absence is proven by a tombstone
        // boundary, or by `evict_tombstones`, which the caller sets only where
        // this compaction holds every surviving version of the key, so nothing
        // outside it can hold a base. Writing to the last level is NOT that
        // proof on its own: the level can hold several overlapping runs, and a
        // compaction rewriting part of it leaves older versions in a run it
        // never read (see `holds_every_surviving_version`). Without either
        // proof the base may sit lower down, and folding onto an assumed-empty
        // base is not a partial answer but a wrong one: a set removal becomes
        // an empty set, a patch becomes a whole record, and either one
        // overwrites the real base when they meet. So the operands are
        // re-emitted unchanged instead, each with its own seqno and order, and
        // the fold happens where the base is.
        //
        // Re-emission goes through `pending`, as the Indirection bail-out
        // above does: those entries re-enter the pipeline one at a time, so
        // each settles itself against the balance and the run reports no
        // collected history for this key. Nothing re-collects them into
        // another attempt either, since they are no longer in `inner`.
        //
        // Unless the operator composes: then folding a prefix of the chain
        // yields something that is still an operand, so the fold can happen
        // here and meet the base later, wherever it is. The result is emitted
        // as a `MergeOperand` rather than a `Value` for exactly that reason —
        // calling it a value would assert the base is empty, which is the
        // wrong answer this branch exists to avoid.
        // Composing needs the range tombstones too, and not only to delete what
        // they cover. A range tombstone between two operands ENDS the chain,
        // exactly as an in-stream `Tombstone` boundary does: the ones below it
        // are hidden and the ones above fold onto an absent base. Composing
        // across that break would produce one operand newer than the
        // still-propagating tombstone, which then survives the read the
        // tombstone should have cut. So a stream that cannot see them does not
        // compose at all, and one that can does not compose a chain reaching
        // under the newest applicable one.
        //
        // The oldest collected operand is the whole test: a tombstone covering
        // it either sits inside the chain or above all of it, and both are
        // reasons to leave the operands alone; one below it covers nothing
        // here. In a deleting compaction `retain` will drop the covered ones
        // anyway, so this only ever declines ahead of that.
        //
        // Every fold before this feature coincided with having the tombstones,
        // through either the boundary or `evict_tombstones`; requiring it here
        // keeps that invariant rather than adding a new precaution.
        //
        // And it needs the chain to be the whole chain. A strategy may select
        // some runs and not others, so the operands in front of this stream are
        // not always every version of the key; an omitted put or point
        // tombstone between two of them is a reset the fold would erase, and
        // unlike an omitted operand no property of the operator can repair it.
        // `inputs_hold_every_version_of` is what rules that out.
        //
        // And it needs the compaction filter to have nothing to say. Every
        // entry reaches `filter_item` exactly once, on its way out; the head
        // of this chain already has, and the rest would on re-emission. A
        // composition is written once and the operands inside it never come
        // back, so their verdicts would have to be collected during the fold —
        // a second visit for the head, a premature one for the tail, and a
        // filter is not required to be idempotent or stateless. Worse, a
        // verdict that turns an operand into a tombstone is a chain boundary,
        // not an operand with new bytes, so applying it mid-fold would
        // resurrect what it was meant to hide. A filter that can only keep
        // raises none of this.
        let no_proven_base = !found_boundary && !self.evict_tombstones;
        let oldest_seqno = collected.last().map_or(head_seqno, |entry| entry.key.seqno);
        let composes = merge_op.composes_operands()
            && self.filter.keeps_everything()
            && self.rt_comparator.is_some()
            && self.inputs_hold_the_chain(user_key.as_ref(), oldest_seqno, head_seqno);
        let crosses_barrier = composes
            && collected.last().is_some_and(|oldest| {
                let (key, seqno) = (oldest.key.user_key.clone(), oldest.key.seqno);
                self.covered_by_applied_tombstone(key.as_ref(), seqno)
            });
        let compose_only = no_proven_base && composes && !crosses_barrier;
        if no_proven_base && !compose_only {
            let mut iter = collected.into_iter();
            #[expect(clippy::expect_used, reason = "collected always has head")]
            let first = iter
                .next()
                .expect("collected should contain at least one element");
            self.pending.extend(iter);
            return Ok(first);
        }

        // Drop collected operands that a covering applied range tombstone deletes
        // (they are pre-delete state): only operands newer than the tombstone fold
        // onto the now-empty base. Without this, an operand below the tombstone
        // would resurrect deleted state across compaction.
        collected.retain(|e| {
            let covered = self.covered_and_deletable(e.key.user_key.as_ref(), e.key.seqno);
            if covered {
                if let Some(watcher) = &mut self.dropped_callback {
                    watcher.on_dropped(e);
                }
                self.note_transform();
            }
            !covered
        });

        // The coverage filter cannot empty a composing chain: emptying it means
        // every operand was covered, the oldest included, and a covered oldest
        // is exactly the barrier that declined the composition before any of
        // this ran. So the fold below always has at least one entry, and so
        // does the re-emit it may fall back to.
        debug_assert!(
            !compose_only || !collected.is_empty(),
            "a composing chain the barrier check passed cannot be emptied here",
        );

        // Operand values in chronological order (ascending seqno): `collected`
        // holds them newest-first, so reading it backwards is that order.
        // Borrowed rather than moved out, because the composing path below may
        // still have to re-emit the entries with their own seqnos.
        let merged = {
            let operand_refs: Vec<&[u8]> =
                collected.iter().rev().map(|e| e.value.as_ref()).collect();
            merge_op.merge(&user_key, base_value.as_deref(), &operand_refs)
        };

        let merged = match merged {
            Ok(merged) => merged,
            // A fold onto a PROVEN base is the only place the key's value can
            // be produced, so its failure is the caller's to see.
            Err(err) if !compose_only => return Err(err),
            // A mid-tree composition is an optimisation, and an optimisation
            // must never make a compaction fail that would otherwise have
            // succeeded. Composition changes which intermediate results exist,
            // so an operator with checked arithmetic can refuse a prefix whose
            // full chain against the real base is perfectly in range: with
            // `base = -1` and operands `[i64::MAX, 1]`, the chain stays in
            // range while the prefix alone overflows. The three properties an
            // operator asserts by composing are about the values it returns,
            // so they cannot rule this out, and requiring totality instead
            // would be a heavier obligation than simply declining here.
            //
            // So the operands are re-emitted exactly as the non-composing
            // branch above does, and the fold happens where the base is. That
            // is the behaviour this key had before the operator opted in, so
            // the failure surfaces at the same point, and for the same reason,
            // as it would have without the optimisation.
            Err(_) => {
                let mut iter = collected.into_iter();
                #[expect(clippy::expect_used, reason = "collected always has head")]
                let first = iter
                    .next()
                    .expect("collected should contain at least one element");
                self.pending.extend(iter);
                return Ok(first);
            }
        };

        // With a proven base this is the key's value, not a further operand: a
        // key that never had a put therefore materialises at the bottom level
        // instead of carrying its operands forever. Composed without a proven
        // base it stays an operand, and folds onto the real base when a later
        // compaction reaches it.
        let value_type = if compose_only {
            ValueType::MergeOperand
        } else {
            ValueType::Value
        };

        Ok(InternalValue::from_components(
            user_key, merged, head_seqno, value_type,
        ))
    }

    /// Records one visibility-changing drop (see the `transform_marker`
    /// field): the output no longer derives from its inputs.
    fn note_transform(&self) {
        if let Some(marker) = &self.transform_marker {
            marker.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Settles one consumed version against the balance (see `gc_balance`),
    /// either because it was emitted or because dropping it changed nothing an
    /// enabled snapshot can observe.
    fn settle_one(&self) {
        self.settle(1);
    }

    /// Settles `n` at once, for a caller that excuses a whole chain.
    fn settle(&self, n: u64) {
        if n > 0 {
            self.gc_balance
                .fetch_sub(n, core::sync::atomic::Ordering::Relaxed);
        }
    }

    /// A drop that no snapshot can tell from keeping it, so it is not
    /// collected history: a tombstone with no same-key sibling at the bottom
    /// level shadows nothing, and an absent key reads the same as a deleted
    /// one. Every OTHER way of losing a version is meant to count, which is why
    /// this is an explicit exception rather than the default.
    ///
    /// One case IS counted although it is observationally neutral, and is left
    /// that way on purpose: two inputs carrying the same key at the same seqno,
    /// which a re-registered table or an overlapping ingest can produce. One
    /// copy is emitted and both are consumed, so the balance ends positive and
    /// the floor rises for a run that changed nothing a reader can see. That
    /// errs toward refusing a read rather than answering it from data that is
    /// gone, which is the direction this counter is built to fail in, and
    /// excusing it would mean deciding two entries are identical rather than
    /// merely adjacent. Do not "fix" it by adding an exemption here.
    fn note_neutral_drop(&self) {
        self.settle_one();
    }

    /// Drains the remaining versions of the given key, reporting what went.
    ///
    /// Nothing is settled here: `CountingPeek` registered each drained version
    /// on the way out of the input and none of them is emitted, so the balance
    /// carries them. The report lets the one caller that can excuse a whole
    /// chain decide whether to.
    fn drain_key(&mut self, key: &UserKey) -> crate::Result<Drained> {
        let mut drained = Drained::default();
        loop {
            let Some(next) = self.inner.next_if(|kv| {
                if let Ok(kv) = kv {
                    let expired = crate::comparator::same_user_key(&kv.key.user_key, key);

                    if expired && let Some(watcher) = &mut self.dropped_callback {
                        watcher.on_dropped(kv);
                    }

                    expired
                } else {
                    true
                }
            }) else {
                return Ok(drained);
            };

            let next = next?;
            drained.total += 1;
            drained.all_tombstones &= next.is_tombstone();
        }
    }
}

/// What a [`CompactionStream::drain_key`] took.
#[derive(Clone, Copy)]
struct Drained {
    total: u64,
    /// Vacuously true for an empty drain, which is what the callers that drain
    /// nothing want: they have nothing to excuse.
    all_tombstones: bool,
}

impl Default for Drained {
    fn default() -> Self {
        Self {
            total: 0,
            all_tombstones: true,
        }
    }
}

impl<'a, I: Iterator<Item = Item>, F: StreamFilter + 'a> Iterator for CompactionStream<'a, I, F> {
    type Item = Item;

    /// Wraps [`Self::next_inner`] so every emitted version settles against the
    /// balance in ONE place. Counting emissions at each `return` inside the
    /// pipeline would be the same list-of-sites this design exists to avoid.
    fn next(&mut self) -> Option<Self::Item> {
        let next = self.next_inner();
        if matches!(next, Some(Ok(_))) {
            self.settle_one();
        }
        next
    }
}

impl<'a, I: Iterator<Item = Item>, F: StreamFilter + 'a> CompactionStream<'a, I, F> {
    fn next_inner(&mut self) -> Option<Item> {
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

                // Key boundary = DIFFERENT key, by identity (byte equality),
                // not by bytewise `>`: the input is comparator order, so
                // `peeked` is never an earlier key than `head` — but under a
                // custom comparator a different key may sort bytewise-lower,
                // and a bytewise `>` classified it as "same key" (a
                // WeakTombstone head then annihilated against the OTHER key's
                // value). Byte equality is also cheaper (length short-circuit).
                if !crate::comparator::same_user_key(&peeked.key.user_key, &head.key.user_key) {
                    if head.is_tombstone() && self.evict_tombstones {
                        self.note_transform();
                        self.note_neutral_drop();
                        continue;
                    }

                    // NOTE: Only item of this key and thus latest version, so return it no matter what
                    // For a lone merge operand with a merge operator and below GC threshold,
                    // collapse via partial merge (result stays MergeOperand if no base found)
                    if head.key.value_type.is_merge_operand()
                        && head.key.seqno < self.gc_watermark
                        && self.merge_operator.is_some()
                    {
                        head = fail_iter!(self.resolve_with_operator(head));
                    }
                } else if head.key.value_type == ValueType::Tombstone
                    && self.evict_tombstones
                    && head.key.seqno < self.gc_watermark
                {
                    // Bottom level, and the tombstone itself is below the
                    // watermark: it is then the newest version any servable
                    // snapshot resolves to, and it reads as an absent key. So
                    // the tombstone and every version it shadows leave together.
                    //
                    // The gate is the tombstone's OWN seqno, the same condition
                    // the fold below states. Gating on the older sibling instead
                    // discards the value a snapshot between the two still
                    // resolves to; leaving the gate out entirely does that at
                    // any watermark, including the threshold-0 contract that
                    // collects nothing.
                    //
                    // The key-boundary and end-of-stream arms need no such gate:
                    // a tombstone with no sibling shadows nothing, so dropping
                    // it answers every snapshot the way keeping it does.
                    self.note_transform();
                    let drained = fail_iter!(self.drain_key(&head.key.user_key));
                    // Same neutrality as a lone tombstone, one step further: if
                    // the whole chain was tombstones, the key read as absent at
                    // every snapshot before this drop and reads absent after, so
                    // it is not collected history and must not cost a floor.
                    // A value anywhere in the chain does make it collection, and
                    // then the balance keeps all of it.
                    if drained.all_tombstones {
                        // The head plus everything it drained.
                        self.settle(drained.total + 1);
                    }
                    continue;
                } else if head.key.value_type == ValueType::WeakTombstone
                    && peeked.key.value_type == ValueType::Value
                    && head.key.seqno < self.gc_watermark
                {
                    // The weak delete and the put it consumed leave the output
                    // together: an annihilation, a visibility transform rather
                    // than a GC fold, and it needs no bottom level because a
                    // weak delete is contracted to a key written at most once.
                    //
                    // It is bounded by the watermark for the reason above: a
                    // snapshot between the put and the delete resolves to the
                    // put, so the pair may only go once the delete itself is
                    // below the watermark.
                    fail_iter!(self.drain_key(&head.key.user_key));
                    self.note_transform();
                    continue;
                } else if peeked.key.seqno < self.gc_watermark {
                    // Merge operands below GC watermark: collapse via merge operator.
                    // Both head AND peeked must be below threshold for MVCC safety.
                    if head.key.value_type.is_merge_operand() && head.key.seqno < self.gc_watermark
                    {
                        if self.merge_operator.is_some() {
                            let mut merged = fail_iter!(self.resolve_with_operator(head));
                            // Drop the merged result if an applicable tombstone
                            // outranks it (same rule as the main emit path).
                            if self.covered_and_deletable(
                                merged.key.user_key.as_ref(),
                                merged.key.seqno,
                            ) {
                                if let Some(watcher) = &mut self.dropped_callback {
                                    watcher.on_dropped(&merged);
                                }
                                self.note_transform();
                                continue;
                            }
                            // Skip zeroing for partial merges (MergeOperand) to avoid duplicate keys
                            if self.zero_seqnos
                                && merged.key.seqno < self.gc_watermark
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
                        // The GC fold, and it needs BOTH versions below the
                        // threshold, the same condition the merge path states
                        // above. Testing only the older sibling discards the
                        // newest version BELOW the threshold whenever a version
                        // at or above it exists — and that is precisely the
                        // version a read just above the recorded floor resolves
                        // to, so the floor would promise data the output no
                        // longer holds.
                        //
                        // The floor is the only read boundary: every snapshot
                        // above it is answered from the output, so this fold
                        // keeps everything such a snapshot reads.
                        if head.key.seqno < self.gc_watermark {
                            let drained = fail_iter!(self.drain_key(&head.key.user_key));
                            // A tail that was all tombstones, under a head this
                            // fold emits, is observationally neutral ONLY at the
                            // bottom level. Every snapshot below the head read
                            // "absent" through the newest of those tombstones and
                            // reads "absent" from nothing afterwards.
                            //
                            // Off the bottom level it is not neutral and must
                            // stay counted: a lower level may hold an older
                            // version that the drained tombstone was hiding, and
                            // dropping the tombstone without raising the floor
                            // would resurrect it for exactly the snapshots the
                            // floor would otherwise refuse.
                            if self.evict_tombstones && drained.all_tombstones {
                                self.settle(drained.total);
                            }
                        }
                    }
                }
            } else if head.is_tombstone() && self.evict_tombstones {
                self.note_transform();
                self.note_neutral_drop();
                continue;
            } else if head.key.value_type.is_merge_operand() && head.key.seqno < self.gc_watermark {
                // Last stream item is a MergeOperand below GC — partial merge.
                if self.merge_operator.is_some() {
                    head = fail_iter!(self.resolve_with_operator(head));
                }
            }

            // Compaction-time range-tombstone application: physically drop the
            // surviving entry when an applicable (strictly-below-watermark)
            // tombstone outranks it, accounting it to the drop callback (blob GC)
            // instead of carrying it to the output to be suppressed at every read.
            if self.covered_and_deletable(head.key.user_key.as_ref(), head.key.seqno) {
                if let Some(watcher) = &mut self.dropped_callback {
                    watcher.on_dropped(&head);
                }
                self.note_transform();
                continue;
            }

            // Zero seqnos below GC, but skip MergeOperands (duplicate key risk)
            if self.zero_seqnos
                && head.key.seqno < self.gc_watermark
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

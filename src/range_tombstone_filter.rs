// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Bidirectional range tombstone filter for iteration.
//!
//! Wraps a sorted KV stream and suppresses entries covered by range tombstones.
//! Forward: tombstones sorted by `(start asc, seqno desc)`, activated when
//! `start <= key`, expired when `end <= key`.
//! Reverse: tombstones sorted by `(end desc, seqno desc)`, activated when
//! `end > key`, expired when `key < start`.

use crate::active_tombstone_set::{ActiveTombstoneSet, ActiveTombstoneSetReverse};
use crate::range_tombstone::RangeTombstone;
use crate::range_tombstone_index::ScanTombstones;
use crate::{InternalValue, SeqNo, comparator::SharedComparator};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// Wraps a bidirectional KV stream and suppresses entries covered by range tombstones.
///
/// Each tombstone is paired with a per-source visibility cutoff (`SeqNo`).
/// Different sources may use different cutoffs — e.g., an ephemeral memtable
/// uses its own `index_seqno` while disk segments use the outer scan seqno.
pub(crate) struct RangeTombstoneFilter<I> {
    inner: I,
    comparator: SharedComparator,

    // Forward state: the tombstones in forward order, each with its cutoff.
    fwd_tombstones: Arc<ScanTombstones>,
    fwd_idx: usize,
    fwd_active: ActiveTombstoneSet,

    // Reverse state: (tombstone, per-source cutoff)
    rev_tombstones: Vec<(RangeTombstone, SeqNo)>,
    rev_idx: usize,
    rev_active: ActiveTombstoneSetReverse,
    rev_initialized: bool,
}

impl<I> RangeTombstoneFilter<I> {
    /// Creates a new bidirectional filter over tombstones in any order.
    #[cfg(test)]
    pub(crate) fn new(inner: I, mut tombstones: Vec<(RangeTombstone, SeqNo)>) -> Self {
        let comparator = crate::comparator::default_comparator();
        tombstones.sort_by(|a, b| a.0.cmp_with_comparator(&b.0, comparator.as_ref()));
        Self::new_with_comparator(inner, Arc::new(ScanTombstones::new(tombstones)), comparator)
    }

    /// Creates a new bidirectional filter with the given comparator.
    ///
    /// The tombstones are in forward order. The reverse order is built by the
    /// first `next_back()`.
    #[must_use]
    pub(crate) fn new_with_comparator(
        inner: I,
        fwd_tombstones: Arc<ScanTombstones>,
        comparator: SharedComparator,
    ) -> Self {
        Self {
            inner,
            comparator: comparator.clone(),
            fwd_tombstones,
            fwd_idx: 0,
            fwd_active: ActiveTombstoneSet::new_with_comparator(comparator.clone()),
            rev_tombstones: Vec::new(),
            rev_idx: 0,
            rev_active: ActiveTombstoneSetReverse::new_with_comparator(comparator),
            rev_initialized: false,
        }
    }

    /// Ensures reverse tombstones are built and sorted (end desc, seqno desc).
    fn ensure_rev_initialized(&mut self) {
        if !self.rev_initialized {
            // From the forward order, so both directions share a canonical
            // base order and the stable sort below keeps its tie-breaking.
            self.rev_tombstones = self.fwd_tombstones.list().to_vec();
            let comparator = self.comparator.as_ref();
            self.rev_tombstones.sort_by(|a, b| {
                comparator
                    .compare(&b.0.end, &a.0.end)
                    .then_with(|| b.0.seqno.cmp(&a.0.seqno))
                    .then_with(|| comparator.compare(&a.0.start, &b.0.start))
            });
            self.rev_initialized = true;
        }
    }

    /// Activates forward tombstones whose start <= `current_key`.
    fn fwd_activate_up_to(&mut self, key: &[u8]) {
        while let Some((rt, cutoff)) = self.fwd_tombstones.list().get(self.fwd_idx) {
            if self.comparator.compare(&rt.start, key) == core::cmp::Ordering::Greater {
                break;
            }
            self.fwd_active.activate(rt, *cutoff);
            self.fwd_idx += 1;
        }
    }

    /// Activates reverse tombstones whose end > `current_key`.
    fn rev_activate_up_to(&mut self, key: &[u8]) {
        while let Some((rt, cutoff)) = self.rev_tombstones.get(self.rev_idx) {
            if self.comparator.compare(&rt.end, key) == core::cmp::Ordering::Greater {
                self.rev_active.activate(rt, *cutoff);
                self.rev_idx += 1;
            } else {
                break;
            }
        }
    }
}

impl<I: crate::reseek::Reseekable> crate::reseek::Reseekable for RangeTombstoneFilter<I> {
    /// Reset the forward/reverse activation cursors and active sets so the next
    /// pull re-activates tombstones from the start of the (position-independent)
    /// sorted lists, then forward the reposition to the wrapped stream.
    ///
    /// The sorted tombstone lists are kept: sort order does not depend on
    /// iteration position, so re-sorting on every reposition would be wasted
    /// work.
    fn reseek(&mut self, ctx: &crate::reseek::ReseekCtx) {
        self.fwd_idx = 0;
        self.rev_idx = 0;
        // Clear in place rather than reconstructing: a seek-then-iterate-then-
        // reseek loop that activated tombstones keeps the active sets' backing
        // storage for the next pass instead of dropping and re-allocating it.
        self.fwd_active.clear();
        self.rev_active.clear();
        self.inner.reseek(ctx);
    }
}

impl<I: Iterator<Item = crate::Result<InternalValue>>> Iterator for RangeTombstoneFilter<I> {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.inner.next()?;

            let Ok(kv) = &item else { return Some(item) };

            let key = kv.key.user_key.as_ref();
            let kv_seqno = kv.key.seqno;

            // Activate tombstones whose start <= this key
            self.fwd_activate_up_to(key);

            // Expire tombstones whose end <= this key
            self.fwd_active.expire_until(key);

            // Check suppression
            if self.fwd_active.is_suppressed(kv_seqno) {
                continue;
            }

            return Some(item);
        }
    }
}

impl<I: DoubleEndedIterator<Item = crate::Result<InternalValue>>> DoubleEndedIterator
    for RangeTombstoneFilter<I>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.ensure_rev_initialized();

        loop {
            let item = self.inner.next_back()?;

            let Ok(kv) = &item else { return Some(item) };

            let key = kv.key.user_key.as_ref();
            let kv_seqno = kv.key.seqno;

            // Activate tombstones whose end > this key (strict >)
            self.rev_activate_up_to(key);

            // Expire tombstones whose start > this key (key < start)
            self.rev_active.expire_until(key);

            // Check suppression
            if self.rev_active.is_suppressed(kv_seqno) {
                continue;
            }

            return Some(item);
        }
    }
}

#[cfg(test)]
mod tests;

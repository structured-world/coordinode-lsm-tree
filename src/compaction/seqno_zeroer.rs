// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Bottommost sequence-number zeroing for compaction output.
//!
//! At the last level an entry whose seqno is already below the GC watermark (no
//! live snapshot needs it) can have its seqno set to `0` — "0" packs to a single
//! byte, and sequence numbers grow monotonically, so this saves space on the
//! coldest, largest level.
//!
//! ## Why a range-tombstone gate is required (MVCC / PITR safety)
//!
//! Range tombstones are applied at read time by sequence-number comparison: a
//! tombstone `RT@r` suppresses an entry `K@s` iff it covers `K` and `s < r`. If
//! we zero `K@s` to `K@0`, then **any** covering tombstone with `r > 0` would
//! suppress it — including:
//!   - a tombstone older than the entry (`r < s`), which must NOT suppress it; and
//!   - a tombstone newer than the entry (`r > s`) but with `r` above the
//!     watermark, which must stay visible for snapshots in `[watermark, r)`.
//!
//! So a key is zeroed only when **no range tombstone in the whole version covers
//! it**. Tombstones are gathered from every level (not just this compaction's
//! inputs), so a tombstone in a level that is not part of this compaction still
//! blocks zeroing — the "beyond output level" case.
//!
//! ## Which reads this is allowed to be invisible to
//!
//! For KEY AND VALUE visibility: none above the watermark, which is the whole
//! of the permission. A rewritten entry answers snapshots between `0` and its
//! real seqno that it never answered before, and every entry rewritten here is
//! below the watermark, so the install's floor refuses AT LEAST those snapshots
//! after a reopen. A snapshot at or above the watermark resolves to the same
//! entry as before, since a zeroed seqno still loses to any real one.
//!
//! The seqno ITSELF is not covered by that, and deliberately so. A columnar
//! scan can project `COL_SEQNO`, which is returned in tree-global space, so a
//! scan at any snapshot reports `0` for a rewritten row where it used to report
//! the real seqno. That is the point of the rewrite (one byte instead of a
//! varint on the coldest level) and it is why the column is a physical
//! encoding detail rather than a value the engine promises to preserve. A
//! caller that needs a stable per-row seqno must store one of its own.
//!
//! At least, and usually more: the floor is `(watermark - 1)` capped at the
//! install's own seqno, derived from a boolean that says only THAT something
//! was rewritten. Zeroing one entry at seqno 10 under a watermark of 100 and
//! an install at 50 refuses every snapshot through 50, though only key/value
//! reads through 10 changed. That is the safe direction and the deliberate one;
//! the gate below is what keeps it safe, not the floor's precision.
//!
//! That coupling is why the gate is `seqno < gc_watermark` and not, say,
//! the output level's own bounds: an entry rewritten at or above the watermark
//! would change an answer the recorded floor still promises.
//!
//! Zeroing the bottom version itself is PITR-safe: it only applies to the
//! latest entry below the watermark (no snapshot reads below the watermark), and
//! a newer version (real seqno > 0) always wins the merge over the zeroed one.
//! Older-version ambiguity cannot arise at the last level for the same reason
//! [`CompactionStream::evict_tombstones`](crate::compaction::stream::CompactionStream::evict_tombstones) relies on: the last level is the
//! authoritative bottom.

use crate::active_tombstone_set::ActiveTombstoneSet;
use crate::range_tombstone::RangeTombstone;
use crate::{InternalValue, SeqNo, comparator::SharedComparator};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// Wraps a sorted compaction output stream and zeroes the seqno of entries that
/// are GC-collapsible (below the watermark) and not covered by any range
/// tombstone. See the module docs for the correctness argument.
pub(super) struct BottommostSeqnoZeroer<I> {
    inner: I,
    /// When `false` (not the last level), the stream is a pass-through —
    /// zeroing is only safe at the authoritative bottom.
    enabled: bool,
    comparator: SharedComparator,
    /// Entries with `seqno < gc_watermark` are eligible for zeroing (subject to
    /// the no-coverage rule).
    gc_watermark: SeqNo,
    /// Range tombstones from the whole version, sorted lazily by `start`.
    tombstones: Vec<RangeTombstone>,
    idx: usize,
    active: ActiveTombstoneSet,
    tombstones_sorted: bool,
    /// The merge stream's collected-history balance, shared so a zeroing can
    /// report itself: rewriting a seqno changes what a snapshot resolves to
    /// without changing how many entries came out, which is the one kind of
    /// history loss the balance cannot derive on its own.
    gc_balance: alloc::sync::Arc<portable_atomic::AtomicU64>,
    /// Whether a zeroing has already been reported, so the shared counter is
    /// touched once per run rather than once per entry.
    zeroed_any: bool,
}

impl<I> BottommostSeqnoZeroer<I> {
    pub(super) fn new(
        inner: I,
        enabled: bool,
        tombstones: Vec<RangeTombstone>,
        gc_watermark: SeqNo,
        comparator: SharedComparator,
        gc_balance: alloc::sync::Arc<portable_atomic::AtomicU64>,
    ) -> Self {
        Self {
            inner,
            enabled,
            comparator: comparator.clone(),
            gc_watermark,
            tombstones,
            idx: 0,
            active: ActiveTombstoneSet::new_with_comparator(comparator),
            tombstones_sorted: false,
            gc_balance,
            zeroed_any: false,
        }
    }

    /// Returns `true` if any range tombstone covers `key` (any seqno). Keys
    /// arrive in non-decreasing `user_key` order, so the active set is swept
    /// monotonically. The lazy sort lives here (not in `next`) so streams whose
    /// entries are all ineligible for zeroing never pay for it.
    fn covered(&mut self, key: &[u8]) -> bool {
        if !self.tombstones_sorted {
            let comparator = self.comparator.as_ref();
            self.tombstones
                .sort_by(|a, b| a.cmp_with_comparator(b, comparator));
            self.tombstones_sorted = true;
        }
        while let Some(rt) = self.tombstones.get(self.idx) {
            if self.comparator.compare(&rt.start, key) == core::cmp::Ordering::Greater {
                break;
            }
            // cutoff = MAX so every tombstone is "visible": ANY covering
            // tombstone (any seqno) blocks zeroing, since a zeroed entry would
            // be shadowed by it at read time.
            self.active.activate(rt, SeqNo::MAX);
            self.idx += 1;
        }
        self.active.expire_until(key);
        self.active.max_active_seqno().is_some()
    }
}

impl<I: Iterator<Item = crate::Result<InternalValue>>> Iterator for BottommostSeqnoZeroer<I> {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.enabled {
            return self.inner.next();
        }
        match self.inner.next()? {
            Ok(mut kv) => {
                if kv.key.seqno > 0
                    && kv.key.seqno < self.gc_watermark
                    && !self.covered(kv.key.user_key.as_ref())
                {
                    kv.key.seqno = 0;
                    // The version comes out at a seqno it never had, so a
                    // snapshot between 0 and its real seqno would now resolve
                    // to a value that did not exist at that snapshot. The entry
                    // count is unchanged, so the install's balance cannot see
                    // this: report it here. Once is enough, the install only
                    // asks whether anything happened.
                    if !self.zeroed_any {
                        self.zeroed_any = true;
                        self.gc_balance
                            .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
                    }
                }
                Some(Ok(kv))
            }
            other => Some(other),
        }
    }
}

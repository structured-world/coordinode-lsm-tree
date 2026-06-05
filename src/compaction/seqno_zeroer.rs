// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

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
//! Zeroing the bottom version itself is PITR-safe: it only applies to the
//! latest entry below the watermark (no snapshot reads below the watermark), and
//! a newer version (real seqno > 0) always wins the merge over the zeroed one.
//! Older-version ambiguity cannot arise at the last level for the same reason
//! [`CompactionStream::evict_tombstones`] relies on: the last level is the
//! authoritative bottom.

use crate::active_tombstone_set::ActiveTombstoneSet;
use crate::range_tombstone::RangeTombstone;
use crate::{InternalValue, SeqNo, comparator::SharedComparator};

/// Wraps a sorted compaction output stream and zeroes the seqno of entries that
/// are GC-collapsible (below the watermark) and not covered by any range
/// tombstone. See the module docs for the correctness argument.
pub(super) struct BottommostSeqnoZeroer<I> {
    inner: I,
    /// When `false` (not the last level), the stream is a pass-through —
    /// zeroing is only safe at the authoritative bottom.
    enabled: bool,
    comparator: SharedComparator,
    /// Entries with `seqno < gc_seqno_threshold` are below the GC watermark and
    /// eligible for zeroing (subject to the no-coverage rule).
    gc_seqno_threshold: SeqNo,
    /// Range tombstones from the whole version, sorted lazily by `start`.
    tombstones: Vec<RangeTombstone>,
    idx: usize,
    active: ActiveTombstoneSet,
    initialized: bool,
}

impl<I> BottommostSeqnoZeroer<I> {
    pub(super) fn new(
        inner: I,
        enabled: bool,
        tombstones: Vec<RangeTombstone>,
        gc_seqno_threshold: SeqNo,
        comparator: SharedComparator,
    ) -> Self {
        Self {
            inner,
            enabled,
            comparator: comparator.clone(),
            gc_seqno_threshold,
            tombstones,
            idx: 0,
            active: ActiveTombstoneSet::new_with_comparator(comparator),
            initialized: false,
        }
    }

    fn ensure_sorted(&mut self) {
        if !self.initialized {
            let comparator = self.comparator.as_ref();
            self.tombstones
                .sort_by(|a, b| a.cmp_with_comparator(b, comparator));
            self.initialized = true;
        }
    }

    /// Returns `true` if any range tombstone covers `key` (any seqno). Keys
    /// arrive in non-decreasing `user_key` order, so the active set is swept
    /// monotonically.
    fn covered(&mut self, key: &[u8]) -> bool {
        while let Some(rt) = self.tombstones.get(self.idx) {
            if self.comparator.compare(&rt.start, key) == std::cmp::Ordering::Greater {
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
        self.ensure_sorted();
        match self.inner.next()? {
            Ok(mut kv) => {
                if kv.key.seqno > 0
                    && kv.key.seqno < self.gc_seqno_threshold
                    && !self.covered(kv.key.user_key.as_ref())
                {
                    kv.key.seqno = 0;
                }
                Some(Ok(kv))
            }
            other => Some(other),
        }
    }
}

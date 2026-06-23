// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::{BoxedIterator, InternalValue, Table, UserKey, version::Run};
#[cfg(not(feature = "std"))]
use alloc::boxed::Box;
use alloc::sync::Arc;
use core::ops::{Bound, Deref, RangeBounds};

type OwnedRange = (Bound<UserKey>, Bound<UserKey>);

fn to_owned_range<R: RangeBounds<UserKey>>(range: &R) -> OwnedRange {
    (
        match range.start_bound() {
            Bound::Included(k) => Bound::Included(k.clone()),
            Bound::Excluded(k) => Bound::Excluded(k.clone()),
            Bound::Unbounded => Bound::Unbounded,
        },
        match range.end_bound() {
            Bound::Included(k) => Bound::Included(k.clone()),
            Bound::Excluded(k) => Bound::Excluded(k.clone()),
            Bound::Unbounded => Bound::Unbounded,
        },
    )
}

/// Reads through a disjoint run with lazy reader initialization.
///
/// `lo_reader` and `hi_reader` are constructed on first `next()` /
/// `next_back()` respectively, deferring the `table.range()` seek.
pub struct RunReader {
    run: Arc<Run<Table>>,
    range: OwnedRange,
    lo: usize,
    hi: usize,
    lo_reader: Option<BoxedIterator<'static>>,
    hi_reader: Option<BoxedIterator<'static>>,
    lo_initialized: bool,
    hi_initialized: bool,
}

impl RunReader {
    /// Creates a new `RunReader` using default lexicographic key ordering.
    ///
    /// For trees with a custom [`crate::comparator::UserComparator`], use [`Self::new_cmp`] instead.
    #[must_use]
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "crate-internal API — used by other modules")
    )]
    pub fn new<R: RangeBounds<UserKey> + Clone + Send + 'static>(
        run: Arc<Run<Table>>,
        range: R,
    ) -> Option<Self> {
        Self::new_cmp(run, range, &crate::comparator::DefaultUserComparator)
    }

    /// Like [`Self::new`], but uses a custom comparator for key ordering.
    #[must_use]
    pub fn new_cmp<R: RangeBounds<UserKey> + Clone + Send + 'static>(
        run: Arc<Run<Table>>,
        range: R,
        cmp: &dyn crate::comparator::UserComparator,
    ) -> Option<Self> {
        assert!(!run.is_empty(), "level reader cannot read empty level");

        let (lo, hi) = run.range_overlap_indexes_cmp(&range, cmp)?;

        Some(Self::culled(run, range, (Some(lo), Some(hi))))
    }

    #[must_use]
    pub fn culled<R: RangeBounds<UserKey> + Clone + Send + 'static>(
        run: Arc<Run<Table>>,
        range: R,
        (lo, hi): (Option<usize>, Option<usize>),
    ) -> Self {
        let lo = lo.unwrap_or_default();
        let hi = hi.unwrap_or(run.len() - 1);
        let owned_range = to_owned_range(&range);

        Self {
            run,
            range: owned_range,
            lo,
            hi,
            lo_reader: None,
            hi_reader: None,
            lo_initialized: false,
            hi_initialized: lo >= hi,
        }
    }

    /// Re-position this reader to a fresh `range`, reusing the same run `Arc`
    /// and struct instead of rebuilding. Recomputes the overlapping table window
    /// and drops the boundary readers so they re-open lazily against the new
    /// bounds on the next pull.
    ///
    /// When the new range does not overlap the run at all, the reader is left in
    /// an immediately-exhausted state (both directions yield `None`).
    pub(crate) fn reseek<R: RangeBounds<UserKey> + Clone + Send + 'static>(
        &mut self,
        range: R,
        cmp: &dyn crate::comparator::UserComparator,
    ) {
        self.range = to_owned_range(&range);
        self.lo_reader = None;
        self.hi_reader = None;

        if let Some((lo, hi)) = self.run.range_overlap_indexes_cmp(&range, cmp) {
            self.lo = lo;
            self.hi = hi;
            self.lo_initialized = false;
            self.hi_initialized = lo >= hi;
        } else {
            // No overlap: present as exhausted. Marking both sides initialized
            // with no readers makes `next` / `next_back` fall straight through
            // to `None` (see their else-branches).
            self.lo = 0;
            self.hi = 0;
            self.lo_initialized = true;
            self.hi_initialized = true;
        }
    }

    fn ensure_lo_initialized(&mut self) {
        if !self.lo_initialized {
            #[expect(
                clippy::expect_used,
                reason = "we trust the caller to pass valid indexes"
            )]
            let lo_table = self.run.deref().get(self.lo).expect("should exist");
            self.lo_reader = Some(Box::new(lo_table.range(self.range.clone())));
            self.lo_initialized = true;
        }
    }

    fn ensure_hi_initialized(&mut self) {
        if !self.hi_initialized {
            #[expect(
                clippy::expect_used,
                reason = "we trust the caller to pass valid indexes"
            )]
            let hi_table = self.run.deref().get(self.hi).expect("should exist");
            self.hi_reader = Some(Box::new(hi_table.range(self.range.clone())));
            self.hi_initialized = true;
        }
    }
}

impl Iterator for RunReader {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        self.ensure_lo_initialized();

        loop {
            if let Some(lo_reader) = &mut self.lo_reader {
                if let Some(item) = lo_reader.next() {
                    return Some(item);
                }

                // NOTE: Lo reader is empty, get next one
                self.lo_reader = None;
                self.lo += 1;

                // Strict `<`: when lo reaches hi, this branch is skipped and
                // the hi table is read via ensure_hi_initialized (which uses
                // table.range() to respect the range end bound). `.iter()` is
                // only used for middle tables that are fully consumed.
                if self.lo < self.hi {
                    self.lo_reader = Some(Box::new(
                        #[expect(
                            clippy::expect_used,
                            reason = "hi is at most equal to the last slot; so because 0 <= lo < hi, it must be a valid index"
                        )]
                        self.run.get(self.lo).expect("should exist").iter(),
                    ));
                }
            } else {
                // Lo exhausted — initialize hi reader if needed and consume from it
                self.ensure_hi_initialized();

                if let Some(hi_reader) = &mut self.hi_reader {
                    return hi_reader.next();
                }
                return None;
            }
        }
    }
}

impl DoubleEndedIterator for RunReader {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.ensure_hi_initialized();

        loop {
            if let Some(hi_reader) = &mut self.hi_reader {
                if let Some(item) = hi_reader.next_back() {
                    return Some(item);
                }

                // NOTE: Hi reader is empty, get prev one
                self.hi_reader = None;
                self.hi -= 1;

                if self.lo < self.hi {
                    self.hi_reader = Some(Box::new(
                        #[expect(
                            clippy::expect_used,
                            reason = "because 0 <= lo <= hi, and hi monotonically decreases, hi must be a valid index"
                        )]
                        self.run.get(self.hi).expect("should exist").iter(),
                    ));
                }
            } else {
                // Hi exhausted — initialize lo reader if needed and consume from it
                self.ensure_lo_initialized();

                if let Some(lo_reader) = &mut self.lo_reader {
                    return lo_reader.next_back();
                }
                return None;
            }
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests;

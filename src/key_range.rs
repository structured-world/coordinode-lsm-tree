// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::{Slice, UserKey};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::ops::Bound;

/// A key range in the format of [min, max] (inclusive on both sides)
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyRange(UserKey, UserKey);

impl KeyRange {
    /// Creates a new key range.
    #[must_use]
    pub fn new((min, max): (UserKey, UserKey)) -> Self {
        Self(min, max)
    }

    /// Creates an empty key range.
    #[must_use]
    pub fn empty() -> Self {
        Self(Slice::empty(), Slice::empty())
    }

    /// Returns the lower bound.
    #[must_use]
    pub fn min(&self) -> &UserKey {
        &self.0
    }

    /// Returns the upper bound.
    #[must_use]
    pub fn max(&self) -> &UserKey {
        &self.1
    }

    fn as_tuple(&self) -> (&UserKey, &UserKey) {
        (self.min(), self.max())
    }

    /// Returns `true` if the list of key ranges is disjoint
    #[must_use]
    pub fn is_disjoint(ranges: &[&Self]) -> bool {
        for (idx, a) in ranges.iter().enumerate() {
            for b in ranges.iter().skip(idx + 1) {
                if a.overlaps_with_key_range(b) {
                    return false;
                }
            }
        }

        true
    }

    /// Returns `true` if the key falls within this key range.
    ///
    /// Uses lexicographic ordering. See [`Self::overlaps_with_key_range_cmp`] and
    /// [`Self::contains_range_cmp`] for custom comparator support; a `contains_key_cmp`
    /// variant can be added when needed (#116).
    #[must_use]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        let (start, end) = self.as_tuple();
        key >= *start && key <= *end
    }

    /// Returns `true` if the `other` is fully contained in this range.
    #[must_use]
    pub fn contains_range(&self, other: &Self) -> bool {
        let (start1, end1) = self.as_tuple();
        let (start2, end2) = other.as_tuple();
        start1 <= start2 && end1 >= end2
    }

    /// Like [`Self::contains_range`], but uses a custom comparator for key ordering.
    #[must_use]
    pub fn contains_range_cmp(
        &self,
        other: &Self,
        cmp: &dyn crate::comparator::UserComparator,
    ) -> bool {
        let (start1, end1) = self.as_tuple();
        let (start2, end2) = other.as_tuple();
        cmp.compare(start1, start2) != core::cmp::Ordering::Greater
            && cmp.compare(end1, end2) != core::cmp::Ordering::Less
    }

    /// Returns `true` if the `other` overlaps at least partially with this range.
    #[must_use]
    pub fn overlaps_with_key_range(&self, other: &Self) -> bool {
        let (start1, end1) = self.as_tuple();
        let (start2, end2) = other.as_tuple();
        end1 >= start2 && start1 <= end2
    }

    /// Like [`Self::overlaps_with_key_range`], but uses a custom comparator for key ordering.
    #[must_use]
    pub fn overlaps_with_key_range_cmp(
        &self,
        other: &Self,
        cmp: &dyn crate::comparator::UserComparator,
    ) -> bool {
        let (start1, end1) = self.as_tuple();
        let (start2, end2) = other.as_tuple();
        cmp.compare(end1, start2) != core::cmp::Ordering::Less
            && cmp.compare(start1, end2) != core::cmp::Ordering::Greater
    }

    /// Like [`Self::overlaps_with_bounds`], but uses a custom comparator for key ordering.
    #[must_use]
    pub fn overlaps_with_bounds_cmp(
        &self,
        bounds: &(Bound<&[u8]>, Bound<&[u8]>),
        cmp: &dyn crate::comparator::UserComparator,
    ) -> bool {
        use core::cmp::Ordering;

        let (lo, hi) = bounds;
        let (my_lo, my_hi) = self.as_tuple();

        if *lo == Bound::Unbounded && *hi == Bound::Unbounded {
            return true;
        }

        if *hi == Bound::Unbounded {
            return match lo {
                Bound::Included(key) => cmp.compare(key, my_hi) != Ordering::Greater,
                Bound::Excluded(key) => cmp.compare(key, my_hi) == Ordering::Less,
                Bound::Unbounded => unreachable!(),
            };
        }

        if *lo == Bound::Unbounded {
            return match hi {
                Bound::Included(key) => cmp.compare(key, my_lo) != Ordering::Less,
                Bound::Excluded(key) => cmp.compare(key, my_lo) == Ordering::Greater,
                Bound::Unbounded => unreachable!(),
            };
        }

        let lo_included = match lo {
            Bound::Included(key) => cmp.compare(key, my_hi) != Ordering::Greater,
            Bound::Excluded(key) => cmp.compare(key, my_hi) == Ordering::Less,
            Bound::Unbounded => unreachable!(),
        };

        let hi_included = match hi {
            Bound::Included(key) => cmp.compare(key, my_lo) != Ordering::Less,
            Bound::Excluded(key) => cmp.compare(key, my_lo) == Ordering::Greater,
            Bound::Unbounded => unreachable!(),
        };

        lo_included && hi_included
    }

    /// Returns `true` if the ranges overlap partially or fully.
    #[must_use]
    pub fn overlaps_with_bounds(&self, bounds: &(Bound<&[u8]>, Bound<&[u8]>)) -> bool {
        let (lo, hi) = bounds;
        let (my_lo, my_hi) = self.as_tuple();

        if *lo == Bound::Unbounded && *hi == Bound::Unbounded {
            return true;
        }

        if *hi == Bound::Unbounded {
            return match lo {
                Bound::Included(key) => key <= my_hi,
                Bound::Excluded(key) => key < my_hi,
                Bound::Unbounded => unreachable!(),
            };
        }

        if *lo == Bound::Unbounded {
            return match hi {
                Bound::Included(key) => key >= my_lo,
                Bound::Excluded(key) => key > my_lo,
                Bound::Unbounded => unreachable!(),
            };
        }

        let lo_included = match lo {
            Bound::Included(key) => key <= my_hi,
            Bound::Excluded(key) => key < my_hi,
            Bound::Unbounded => unreachable!(),
        };

        let hi_included = match hi {
            Bound::Included(key) => key >= my_lo,
            Bound::Excluded(key) => key > my_lo,
            Bound::Unbounded => unreachable!(),
        };

        lo_included && hi_included
    }

    /// Merges sorted key ranges into disjoint intervals using a custom comparator.
    ///
    /// Input ranges must be sorted by min key (in comparator order). Overlapping
    /// or adjacent ranges are coalesced. Returns a `Vec` of non-overlapping
    /// `KeyRange`s covering exactly the union of the inputs.
    ///
    /// Used by multi-level compaction to reduce redundant L2 overlap queries
    /// when L0 tables overlap (#122 Part 2).
    #[must_use]
    pub(crate) fn merge_sorted_cmp(
        ranges: impl IntoIterator<Item = Self>,
        cmp: &dyn crate::comparator::UserComparator,
    ) -> Vec<Self> {
        let mut out: Vec<Self> = Vec::new();

        #[cfg(debug_assertions)]
        let mut prev_min: Option<UserKey> = None;

        for r in ranges {
            #[cfg(debug_assertions)]
            {
                debug_assert!(
                    prev_min
                        .as_ref()
                        .is_none_or(|pm| cmp.compare(pm, r.min()) != core::cmp::Ordering::Greater),
                    "merge_sorted_cmp: input ranges must be sorted by min key in comparator order",
                );
                prev_min = Some(r.min().clone());
            }

            if let Some(last) = out.last_mut() {
                // Ranges overlap or are adjacent when last.max >= r.min
                if cmp.compare(last.max(), r.min()) != core::cmp::Ordering::Less {
                    // Extend the current interval if r.max is beyond last.max
                    if cmp.compare(r.max(), last.max()) == core::cmp::Ordering::Greater {
                        last.1 = r.1;
                    }
                    continue;
                }
            }
            out.push(r);
        }

        out
    }

    /// Aggregates a key range.
    pub fn aggregate<'a>(mut iter: impl Iterator<Item = &'a Self>) -> Self {
        let Some(first) = iter.next() else {
            return Self::empty();
        };

        let mut min = first.min();
        let mut max = first.max();

        for other in iter {
            let x = other.min();
            if x < min {
                min = x;
            }

            let x = other.max();
            if x > max {
                max = x;
            }
        }

        Self(min.clone(), max.clone())
    }

    /// Like [`Self::aggregate`], but uses a custom comparator for key ordering.
    pub fn aggregate_cmp<'a>(
        mut iter: impl Iterator<Item = &'a Self>,
        cmp: &dyn crate::comparator::UserComparator,
    ) -> Self {
        let Some(first) = iter.next() else {
            return Self::empty();
        };

        let mut min = first.min();
        let mut max = first.max();

        for other in iter {
            let x = other.min();
            if cmp.compare(x, min) == core::cmp::Ordering::Less {
                min = x;
            }

            let x = other.max();
            if cmp.compare(x, max) == core::cmp::Ordering::Greater {
                max = x;
            }
        }

        Self(min.clone(), max.clone())
    }
}

#[cfg(test)]
mod tests;

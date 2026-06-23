// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::KeyRange;
use crate::comparator::UserComparator;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::ops::{Bound, RangeBounds};

pub trait Ranged {
    fn key_range(&self) -> &KeyRange;
}

/// Item inside a run
///
/// May point to an interval [min, max] of tables in the next run.
#[expect(dead_code, reason = "planned for cascading index optimization")]
pub struct Indexed<T: Ranged> {
    inner: T,
    // cascade_indexes: (u32, u32),
}

/* impl<T: Ranged> Indexed<T> {
    pub fn update_cascading(&mut self, next_run: &Run<T>) {
        let kr = self.key_range();
        let range = &**kr.min()..=&**kr.max();

        if let Some((lo, hi)) = next_run.range_indexes(range) {
            // NOTE: There are never 4+ billion tables in a run
            #[allow(clippy::cast_possible_truncation)]
            let interval = (lo as u32, hi as u32);

            self.cascade_indexes = interval;
        } else {
            self.cascade_indexes = (u32::MAX, u32::MAX);
        }
    }
} */

impl<T: Ranged> Ranged for Indexed<T> {
    fn key_range(&self) -> &KeyRange {
        self.inner.key_range()
    }
}

impl<T: Ranged> core::ops::Deref for Indexed<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// A disjoint run of tables
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Run<T: Ranged>(Vec<T>);

impl<T: Ranged> core::ops::Deref for Run<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Returns the span between the first and last element matching `pred`.
///
/// Note: non-matching elements *between* matches are included. This is
/// correct for `get_contained` / `get_contained_cmp` where the overlap
/// window guarantees contiguity of matching tables.
fn trim_slice<T, F>(s: &[T], pred: F) -> &[T]
where
    F: Fn(&T) -> bool,
{
    let start = s.iter().position(&pred).unwrap_or(s.len());
    let end = s.iter().rposition(&pred).map_or(start, |i| i + 1);

    #[expect(
        clippy::expect_used,
        reason = "start..end are derived from position/rposition on the same slice"
    )]
    s.get(start..end).expect("should be in range")
}

impl<T: Ranged> Run<T> {
    pub fn new(items: Vec<T>) -> Option<Self> {
        if items.is_empty() {
            None
        } else {
            Some(Self(items))
        }
    }

    pub fn inner_mut(&mut self) -> &mut Vec<T> {
        &mut self.0
    }

    /// Pushes a table into the run and re-sorts by min key using lexicographic
    /// byte ordering.
    ///
    /// Only correct when the tree uses the default (lexicographic) comparator.
    /// For custom comparators, use [`Self::push_cmp`] instead.
    pub fn push_lexicographic(&mut self, item: T) {
        self.0.push(item);

        self.0
            .sort_by(|a, b| a.key_range().min().cmp(b.key_range().min()));
    }

    /// Pushes a table and re-sorts using a custom comparator for key ordering.
    ///
    /// Re-sorts the entire run on each call (mirrors [`Self::push_lexicographic`]
    /// behavior). Acceptable for typical run sizes (<100 tables); for bulk
    /// insertion use [`Self::extend`] followed by [`Self::sort_by_cmp`].
    pub fn push_cmp(&mut self, item: T, cmp: &dyn UserComparator) {
        self.0.push(item);
        self.sort_by_cmp(cmp);
    }

    /// Sorts the run by min key using the provided user comparator.
    ///
    /// Use after [`Self::extend`] to re-establish ordering in a single pass.
    pub fn sort_by_cmp(&mut self, cmp: &dyn UserComparator) {
        self.0
            .sort_by(|a, b| cmp.compare(a.key_range().min(), b.key_range().min()));
    }

    /// Appends items without re-sorting. Callers must ensure the run remains
    /// sorted (e.g. via [`Self::sort_by_cmp`] after all items are added).
    pub fn extend(&mut self, items: Vec<T>) {
        self.0.extend(items);
    }

    pub fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&T) -> bool,
    {
        self.0.retain(f);
    }

    pub fn remove(&mut self, idx: usize) -> T {
        self.0.remove(idx)
    }

    /// Returns the table that may possibly contains the given key.
    pub fn get_for_key(&self, key: &[u8]) -> Option<&T> {
        let idx = self.partition_point(|x| x.key_range().max() < &key);

        self.0.get(idx).filter(|x| x.key_range().min() <= &key)
    }

    /// Like [`Self::get_for_key`], but uses a custom comparator for key ordering.
    ///
    /// # Precondition (guaranteed by construction)
    ///
    /// Tables within a run are sorted by `key_range` in comparator order.
    /// This holds because tables are flushed from comparator-sorted memtables
    /// and compaction preserves the ordering. The binary search here must
    /// use the same comparator to maintain the invariant.
    pub fn get_for_key_cmp(
        &self,
        key: &[u8],
        cmp: &dyn crate::comparator::UserComparator,
    ) -> Option<&T> {
        let idx = self.partition_point(|x| {
            cmp.compare(x.key_range().max(), key) == core::cmp::Ordering::Less
        });

        self.0
            .get(idx)
            .filter(|x| cmp.compare(x.key_range().min(), key) != core::cmp::Ordering::Greater)
    }

    /// Returns the run's key range.
    pub fn aggregate_key_range(&self) -> KeyRange {
        #[expect(clippy::expect_used, reason = "by definition, runs are never empty")]
        let lo = self.first().expect("run should never be empty");

        #[expect(clippy::expect_used, reason = "by definition, runs are never empty")]
        let hi = self.last().expect("run should never be empty");

        KeyRange::new((lo.key_range().min().clone(), hi.key_range().max().clone()))
    }

    /// Returns the sub slice of tables in the run that have
    /// a key range overlapping the input key range.
    ///
    /// Uses lexicographic ordering. For custom comparators, use [`Self::get_overlapping_cmp`].
    pub fn get_overlapping<'a>(&'a self, key_range: &'a KeyRange) -> &'a [T] {
        let range = key_range.min()..=key_range.max();

        let Some((lo, hi)) = self.range_overlap_indexes::<crate::Slice, _>(&range) else {
            return &[];
        };

        self.get(lo..=hi).unwrap_or_default()
    }

    /// Like [`Self::get_overlapping`], but uses a custom comparator for key ordering.
    ///
    /// Lifetime on `key_range` mirrors [`Self::get_overlapping`] for API consistency.
    pub fn get_overlapping_cmp<'a>(
        &'a self,
        key_range: &'a KeyRange,
        cmp: &dyn UserComparator,
    ) -> &'a [T] {
        let range = key_range.min()..=key_range.max();

        let Some((lo, hi)) = self.range_overlap_indexes_cmp::<crate::Slice, _>(&range, cmp) else {
            return &[];
        };

        self.get(lo..=hi).unwrap_or_default()
    }

    /// Returns the sub slice of tables of tables in the run that have
    /// a key range fully contained in the input key range.
    pub fn get_contained<'a>(&'a self, key_range: &KeyRange) -> &'a [T] {
        let range = key_range.min()..=key_range.max();

        let Some((lo, hi)) = self.range_overlap_indexes::<crate::Slice, _>(&range) else {
            return &[];
        };

        self.get(lo..=hi)
            .map(|slice| trim_slice(slice, |x| key_range.contains_range(x.key_range())))
            .unwrap_or_default()
    }

    /// Like [`Self::get_contained`], but uses a custom comparator for key ordering.
    pub fn get_contained_cmp<'a>(
        &'a self,
        key_range: &KeyRange,
        cmp: &dyn UserComparator,
    ) -> &'a [T] {
        let range = key_range.min()..=key_range.max();

        let Some((lo, hi)) = self.range_overlap_indexes_cmp::<crate::Slice, _>(&range, cmp) else {
            return &[];
        };

        self.get(lo..=hi)
            .map(|slice| trim_slice(slice, |x| key_range.contains_range_cmp(x.key_range(), cmp)))
            .unwrap_or_default()
    }

    /// Returns the indexes of the interval [min, max] of tables that overlap with a given range.
    pub fn range_overlap_indexes<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        key_range: &R,
    ) -> Option<(usize, usize)> {
        let level = &self.0;

        let lo = match key_range.start_bound() {
            Bound::Unbounded => 0,
            Bound::Included(start_key) => {
                level.partition_point(|x| x.key_range().max() < start_key)
            }
            Bound::Excluded(start_key) => {
                level.partition_point(|x| x.key_range().max() <= start_key)
            }
        };

        if lo >= level.len() {
            return None;
        }

        // NOTE: We check for level length above
        #[expect(clippy::indexing_slicing)]
        let truncated_level = &level[lo..];

        let hi = match key_range.end_bound() {
            Bound::Unbounded => level.len() - 1,
            Bound::Included(end_key) => {
                // IMPORTANT: We need to add back `lo` because we sliced it off
                let idx = lo + truncated_level.partition_point(|x| x.key_range().min() <= end_key);

                if idx == 0 {
                    return None;
                }

                idx.saturating_sub(1) // To avoid underflow
            }
            Bound::Excluded(end_key) => {
                // IMPORTANT: We need to add back `lo` because we sliced it off
                let idx = lo + truncated_level.partition_point(|x| x.key_range().min() < end_key);

                if idx == 0 {
                    return None;
                }

                idx.saturating_sub(1) // To avoid underflow
            }
        };

        if lo > hi {
            return None;
        }

        Some((lo, hi))
    }

    /// Like [`Self::range_overlap_indexes`], but uses a custom comparator for key ordering.
    pub fn range_overlap_indexes_cmp<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        key_range: &R,
        cmp: &dyn UserComparator,
    ) -> Option<(usize, usize)> {
        use core::cmp::Ordering;

        let level = &self.0;

        let lo = match key_range.start_bound() {
            Bound::Unbounded => 0,
            Bound::Included(start_key) => level.partition_point(|x| {
                cmp.compare(x.key_range().max(), start_key.as_ref()) == Ordering::Less
            }),
            Bound::Excluded(start_key) => level.partition_point(|x| {
                cmp.compare(x.key_range().max(), start_key.as_ref()) != Ordering::Greater
            }),
        };

        if lo >= level.len() {
            return None;
        }

        #[expect(clippy::indexing_slicing)]
        let truncated_level = &level[lo..];

        let hi = match key_range.end_bound() {
            Bound::Unbounded => level.len() - 1,
            Bound::Included(end_key) => {
                let idx = lo
                    + truncated_level.partition_point(|x| {
                        cmp.compare(x.key_range().min(), end_key.as_ref()) != Ordering::Greater
                    });

                if idx == 0 {
                    return None;
                }

                idx.saturating_sub(1)
            }
            Bound::Excluded(end_key) => {
                let idx = lo
                    + truncated_level.partition_point(|x| {
                        cmp.compare(x.key_range().min(), end_key.as_ref()) == Ordering::Less
                    });

                if idx == 0 {
                    return None;
                }

                idx.saturating_sub(1)
            }
        };

        if lo > hi {
            return None;
        }

        Some((lo, hi))
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::indexing_slicing, reason = "test code")]
mod tests;

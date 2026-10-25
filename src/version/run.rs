// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use crate::KeyRange;
use crate::comparator::UserComparator;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::ops::{Bound, RangeBounds};
use once_cell::race::OnceBool;

pub trait Ranged {
    fn key_range(&self) -> &KeyRange;
}

/// An item that knows whether it holds range tombstones. Kept apart from
/// [`Ranged`] with no default, so no item answers "none" without being asked:
/// a read skips a run by this answer.
pub trait HoldsRangeTombstones {
    fn holds_range_tombstones(&self) -> bool;
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
pub struct Run<T: Ranged> {
    items: Vec<T>,

    /// Whether an item holds a range tombstone, worked out on the first ask.
    /// A run is shared by every version it survives into, so the scan runs
    /// once per run rather than once per reader; every change of the items
    /// clears it.
    range_tombstones: OnceBool,
}

impl<T: Ranged + Clone> Clone for Run<T> {
    fn clone(&self) -> Self {
        Self {
            items: self.items.clone(),
            range_tombstones: OnceBool::new(),
        }
    }
}

impl<T: Ranged + PartialEq> PartialEq for Run<T> {
    fn eq(&self, other: &Self) -> bool {
        self.items == other.items
    }
}

impl<T: Ranged + Eq> Eq for Run<T> {}

impl<T: Ranged + core::fmt::Debug> core::fmt::Debug for Run<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("Run").field(&self.items).finish()
    }
}

impl<T: Ranged> core::ops::Deref for Run<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.items
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

impl<T: Ranged + HoldsRangeTombstones> Run<T> {
    /// Whether any item holds a range tombstone.
    pub fn holds_range_tombstones(&self) -> bool {
        self.range_tombstones.get_or_init(|| {
            self.items
                .iter()
                .any(HoldsRangeTombstones::holds_range_tombstones)
        })
    }
}

impl<T: Ranged> Run<T> {
    pub fn new(items: Vec<T>) -> Option<Self> {
        if items.is_empty() {
            None
        } else {
            Some(Self {
                items,
                range_tombstones: OnceBool::new(),
            })
        }
    }

    /// The items to change in place; what they hold is asked anew afterwards.
    pub fn inner_mut(&mut self) -> &mut Vec<T> {
        self.range_tombstones = OnceBool::new();
        &mut self.items
    }

    /// Pushes a table into the run and re-sorts by min key using lexicographic
    /// byte ordering.
    ///
    /// Only correct when the tree uses the default (lexicographic) comparator.
    /// For custom comparators, use [`Self::push_cmp`] instead.
    pub fn push_lexicographic(&mut self, item: T) {
        self.push_cmp(item, &crate::comparator::DefaultUserComparator);
    }

    /// Pushes a table and re-sorts using a custom comparator for key ordering.
    ///
    /// Re-sorts the entire run on each call (mirrors [`Self::push_lexicographic`]
    /// behavior). Acceptable for typical run sizes (<100 tables); for bulk
    /// insertion use [`Self::extend`] followed by [`Self::sort_by_cmp`].
    pub fn push_cmp<C: UserComparator + ?Sized>(&mut self, item: T, cmp: &C) {
        self.inner_mut().push(item);
        self.sort_by_cmp(cmp);
    }

    /// Inserts a table at its place by min key in a run already sorted so,
    /// after the tables with an equal min key: the run a [`Self::push_cmp`]
    /// would leave, without sorting it again.
    pub fn insert_sorted_cmp<C: UserComparator + ?Sized>(&mut self, item: T, cmp: &C) {
        let at = self.items.partition_point(|x| {
            cmp.compare(x.key_range().min(), item.key_range().min()) != core::cmp::Ordering::Greater
        });
        self.inner_mut().insert(at, item);
    }

    /// Sorts the run by min key using the provided user comparator.
    ///
    /// Use after [`Self::extend`] to re-establish ordering in a single pass.
    pub fn sort_by_cmp<C: UserComparator + ?Sized>(&mut self, cmp: &C) {
        self.items
            .sort_by(|a, b| cmp.compare(a.key_range().min(), b.key_range().min()));
    }

    /// Appends items without re-sorting. Callers must ensure the run remains
    /// sorted (e.g. via [`Self::sort_by_cmp`] after all items are added).
    pub fn extend(&mut self, items: Vec<T>) {
        self.inner_mut().extend(items);
    }

    pub fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&T) -> bool,
    {
        self.inner_mut().retain(f);
    }

    pub fn remove(&mut self, idx: usize) -> T {
        self.inner_mut().remove(idx)
    }

    /// Returns the table that may possibly contains the given key.
    pub fn get_for_key(&self, key: &[u8]) -> Option<&T> {
        self.get_for_key_cmp(key, &crate::comparator::DefaultUserComparator)
    }

    /// Like [`Self::get_for_key`], but uses a custom comparator for key ordering.
    ///
    /// # Precondition (guaranteed by construction)
    ///
    /// Tables within a run are sorted by `key_range` in comparator order and
    /// disjoint. This holds because a version change lays the new tables out
    /// in sorted, disjoint runs as they join a level, whatever order its
    /// writer produced them in. The binary search here must use the same
    /// comparator to maintain the invariant.
    pub fn get_for_key_cmp<C: UserComparator + ?Sized>(&self, key: &[u8], cmp: &C) -> Option<&T> {
        self.index_for_key_cmp(key, cmp)
            .and_then(|idx| self.items.get(idx))
    }

    /// The position of the table [`Self::get_for_key_cmp`] returns.
    pub fn index_for_key_cmp<C: UserComparator + ?Sized>(
        &self,
        key: &[u8],
        cmp: &C,
    ) -> Option<usize> {
        let idx = self.partition_point(|x| {
            cmp.compare(x.key_range().max(), key) == core::cmp::Ordering::Less
        });

        self.items
            .get(idx)
            .filter(|x| cmp.compare(x.key_range().min(), key) != core::cmp::Ordering::Greater)
            .map(|_| idx)
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
        self.get_overlapping_cmp(key_range, &crate::comparator::DefaultUserComparator)
    }

    /// Like [`Self::get_overlapping`], but uses a custom comparator for key ordering.
    ///
    /// Lifetime on `key_range` mirrors [`Self::get_overlapping`] for API consistency.
    pub fn get_overlapping_cmp<'a, C: UserComparator + ?Sized>(
        &'a self,
        key_range: &'a KeyRange,
        cmp: &C,
    ) -> &'a [T] {
        let range = key_range.min()..=key_range.max();

        let Some((lo, hi)) = self.range_overlap_indexes_cmp::<crate::Slice, _, C>(&range, cmp)
        else {
            return &[];
        };

        self.get(lo..=hi).unwrap_or_default()
    }

    /// Returns the sub slice of tables of tables in the run that have
    /// a key range fully contained in the input key range.
    pub fn get_contained<'a>(&'a self, key_range: &KeyRange) -> &'a [T] {
        self.get_contained_cmp(key_range, &crate::comparator::DefaultUserComparator)
    }

    /// Like [`Self::get_contained`], but uses a custom comparator for key ordering.
    pub fn get_contained_cmp<'a, C: UserComparator + ?Sized>(
        &'a self,
        key_range: &KeyRange,
        cmp: &C,
    ) -> &'a [T] {
        let range = key_range.min()..=key_range.max();

        let Some((lo, hi)) = self.range_overlap_indexes_cmp::<crate::Slice, _, C>(&range, cmp)
        else {
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
        self.range_overlap_indexes_cmp(key_range, &crate::comparator::DefaultUserComparator)
    }

    /// Like [`Self::range_overlap_indexes`], but uses a custom comparator for key ordering.
    pub fn range_overlap_indexes_cmp<K: AsRef<[u8]>, R: RangeBounds<K>, C>(
        &self,
        key_range: &R,
        cmp: &C,
    ) -> Option<(usize, usize)>
    where
        C: UserComparator + ?Sized,
    {
        use core::cmp::Ordering;

        let level = &self.items;

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

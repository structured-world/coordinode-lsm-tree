// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::run::Ranged;
use crate::comparator::UserComparator;
use crate::version::Run;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// Fuses L0 runs, given newest first, into as few runs as the overlaps allow,
/// keeping them newest first: a table never ends up behind a run holding
/// older data it overlaps, so the run order stays recency order.
pub fn optimize_runs<T: Clone + Ranged>(
    runs: Vec<Run<T>>,
    cmp: &dyn UserComparator,
) -> Vec<Run<T>> {
    if runs.len() <= 1 {
        runs
    } else {
        let mut new_runs: Vec<Run<T>> = Vec::new();

        // Newest first, so every table already placed is newer than `table`:
        // it belongs behind the last placed run it overlaps. No run after
        // that one overlaps it, so joining the next run keeps every run
        // internally disjoint.
        for run in &runs {
            for table in run.iter() {
                let last_overlap = new_runs.iter().rposition(|existing_run| {
                    existing_run.iter().any(|x| {
                        table
                            .key_range()
                            .overlaps_with_key_range_cmp(x.key_range(), cmp)
                    })
                });

                let target = match last_overlap {
                    Some(idx) => new_runs.get_mut(idx + 1),
                    None => new_runs.first_mut(),
                };

                if let Some(target) = target {
                    target.push_cmp(table.clone(), cmp);
                } else {
                    #[expect(
                        clippy::expect_used,
                        reason = "we pass in a table, so the run cannot be None"
                    )]
                    new_runs.push(Run::new(vec![table.clone()]).expect("run should not be empty"));
                }
            }
        }

        new_runs
    }
}

/// What orders L0 tables by recency: a table whose age compares greater
/// holds newer data.
pub trait Aged {
    /// The table's age; greater is newer.
    type Age: Ord;

    /// The age of the table's data.
    fn age(&self) -> Self::Age;
}

/// A table's age is its persisted L0 recency key: its own id for a flush or an
/// ingest (ids are allocated in increasing order), its newest input's for a
/// compaction output, so a flush landing while a compaction runs is newer
/// than its output. Sequence numbers are no age: a caller may assign them, so
/// two tables can hold one key at one seqno, and the newer table's must win.
/// The id breaks a tie, the higher one being the superseding copy.
impl Aged for crate::table::Table {
    type Age = (crate::table::TableId, crate::table::TableId);

    fn age(&self) -> Self::Age {
        (self.l0_recency(), self.id())
    }
}

/// Lays L0 out from its tables' ages rather than from any run order they came
/// in: every table on its own, newest first, fused by [`optimize_runs`].
/// Which run a table sat in says nothing about its age (a table joins any run
/// it does not overlap), so a flush that joined a compaction input's run, an
/// output placed among runs it did not come from, or an order a manifest
/// persisted wrongly all come out in recency order again.
pub fn order_by_age<T: Clone + Ranged + Aged>(
    tables: impl IntoIterator<Item = T>,
    cmp: &dyn UserComparator,
) -> Vec<Run<T>> {
    let mut tables: Vec<T> = tables.into_iter().collect();
    // Newest first.
    tables.sort_by_key(|table| core::cmp::Reverse(table.age()));
    let runs = tables
        .into_iter()
        .filter_map(|table| Run::new(alloc::vec![table]))
        .collect();
    optimize_runs(runs, cmp)
}

/// Whether `runs`, given front to back, keep every table ahead of each older
/// table it overlaps: the order a layout from ages produces. A layout that
/// holds it is kept as it stands, so a recovered L0 is laid out again only
/// when a manifest holds a newer table behind an older one.
///
/// O(n log n) in the table count: the tables are taken newest first, and each
/// is checked against the newer ones it overlaps through a range maximum of
/// their run positions over the key space, so an open does not compare every
/// pair of a wide L0.
pub fn in_recency_order<T: Ranged + Aged>(runs: &[&Run<T>], cmp: &dyn UserComparator) -> bool {
    let mut placed: Vec<(usize, &T)> = runs
        .iter()
        .enumerate()
        .flat_map(|(at, run)| run.iter().map(move |table| (at, table)))
        .collect();

    // Every table's bounds, in key order: two tables overlap exactly when
    // their index ranges over these points do.
    let mut points: Vec<&[u8]> = placed
        .iter()
        .flat_map(|&(_, table)| {
            let range = table.key_range();
            [range.min().as_ref(), range.max().as_ref()]
        })
        .collect();
    points.sort_by(|a, b| cmp.compare(a, b));
    points.dedup_by(|a, b| cmp.compare(a, b) == core::cmp::Ordering::Equal);
    let index = |key: &[u8]| {
        points
            .binary_search_by(|point| cmp.compare(point, key))
            .unwrap_or_else(|at| at)
    };

    placed.sort_by(|(_, a), (_, b)| b.age().cmp(&a.age()));
    // One past the furthest-back run of a newer table, per key point.
    let mut newer_runs = RangeMax::new(points.len());
    placed.iter().all(|(run, table)| {
        let range = table.key_range();
        let (lo, hi) = (index(range.min()), index(range.max()));
        // Every table recorded so far is newer than this one: none it
        // overlaps may sit in a run behind it.
        let in_order = newer_runs.max(lo, hi) <= run + 1;
        newer_runs.raise(lo, hi, run + 1);
        in_order
    })
}

/// A maximum over points `0..len` that inclusive index ranges raise and read,
/// each in O(log len). A node keeps the highest value raised over all of its
/// range (`whole`) and over any part of it (`any`).
struct RangeMax {
    len: usize,
    whole: Vec<usize>,
    any: Vec<usize>,
}

impl RangeMax {
    fn new(len: usize) -> Self {
        let nodes = 4 * len.max(1);
        Self {
            len,
            whole: alloc::vec![0; nodes],
            any: alloc::vec![0; nodes],
        }
    }

    /// Raises every point of `lo..=hi` to at least `value`.
    fn raise(&mut self, lo: usize, hi: usize, value: usize) {
        if self.len > 0 {
            self.raise_at(1, 0, self.len - 1, lo, hi, value);
        }
    }

    fn raise_at(&mut self, node: usize, l: usize, r: usize, lo: usize, hi: usize, value: usize) {
        if hi < l || r < lo {
            return;
        }
        if let Some(any) = self.any.get_mut(node) {
            *any = (*any).max(value);
        }
        if lo <= l && r <= hi {
            if let Some(whole) = self.whole.get_mut(node) {
                *whole = (*whole).max(value);
            }
            return;
        }
        let mid = l + (r - l) / 2;
        self.raise_at(2 * node, l, mid, lo, hi, value);
        self.raise_at(2 * node + 1, mid + 1, r, lo, hi, value);
    }

    /// The highest value raised over any point of `lo..=hi`.
    fn max(&self, lo: usize, hi: usize) -> usize {
        if self.len == 0 {
            return 0;
        }
        self.max_at(1, 0, self.len - 1, lo, hi)
    }

    fn max_at(&self, node: usize, l: usize, r: usize, lo: usize, hi: usize) -> usize {
        if hi < l || r < lo {
            return 0;
        }
        if lo <= l && r <= hi {
            return self.any.get(node).copied().unwrap_or(0);
        }
        let mid = l + (r - l) / 2;
        let whole = self.whole.get(node).copied().unwrap_or(0);
        whole
            .max(self.max_at(2 * node, l, mid, lo, hi))
            .max(self.max_at(2 * node + 1, mid + 1, r, lo, hi))
    }
}

/// A table reduced to what placement looks at, for the fuzz adapter below.
#[derive(Clone)]
struct RangedId {
    id: u64,
    key_range: crate::KeyRange,
}

impl Ranged for RangedId {
    fn key_range(&self) -> &crate::KeyRange {
        &self.key_range
    }
}

/// Runs `optimize_runs` over runs given newest first as `(id, key range)`
/// lists under the bytewise comparator, returning the ids per output run.
/// Exists for the `optimize_runs` fuzz target; production code calls
/// `optimize_runs` directly. An input run that is empty is skipped.
#[doc(hidden)]
#[must_use]
pub fn optimize_key_ranges(runs: Vec<Vec<(u64, crate::KeyRange)>>) -> Vec<Vec<u64>> {
    let runs = runs
        .into_iter()
        .filter_map(|run| {
            Run::new(
                run.into_iter()
                    .map(|(id, key_range)| RangedId { id, key_range })
                    .collect(),
            )
        })
        .collect();
    optimize_runs(runs, &crate::comparator::DefaultUserComparator)
        .iter()
        .map(|run| run.iter().map(|table| table.id).collect())
        .collect()
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests;

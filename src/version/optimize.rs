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

/// A table's age is its sequence numbers, the version every read resolves
/// by: of two tables that overlap, the one holding the higher sequence
/// numbers holds the newer data. The table id breaks a tie. Bounds that do
/// not fit together (damaged metadata) cannot tell an age, and sort oldest.
impl Aged for crate::table::Table {
    type Age = (Option<(crate::SeqNo, crate::SeqNo)>, crate::table::TableId);

    fn age(&self) -> Self::Age {
        (
            self.seqno_range()
                .map(|(lowest, highest)| (highest, lowest)),
            self.id(),
        )
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

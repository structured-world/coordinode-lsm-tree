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

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests;

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use super::run::Ranged;
use crate::comparator::UserComparator;
use crate::version::Run;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

pub fn optimize_runs<T: Clone + Ranged>(
    runs: Vec<Run<T>>,
    cmp: &dyn UserComparator,
) -> Vec<Run<T>> {
    if runs.len() <= 1 {
        runs
    } else {
        let mut new_runs: Vec<Run<T>> = Vec::new();

        for run in runs.iter().rev() {
            'run: for table in run.iter().rev() {
                for existing_run in new_runs.iter_mut().rev() {
                    if existing_run.iter().all(|x| {
                        !table
                            .key_range()
                            .overlaps_with_key_range_cmp(x.key_range(), cmp)
                    }) {
                        existing_run.push_cmp(table.clone(), cmp);
                        continue 'run;
                    }
                }

                #[expect(
                    clippy::expect_used,
                    reason = "we pass in a table, so the run cannot be None"
                )]
                new_runs.insert(
                    0,
                    Run::new(vec![table.clone()]).expect("run should not be empty"),
                );
            }
        }

        new_runs
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests;

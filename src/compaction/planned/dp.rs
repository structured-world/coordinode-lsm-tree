// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The merges of one compaction round that leave queries the most throughput,
//! found by dynamic programming (Wang et al., "Rethinking The Compaction
//! Policies in LSM-trees", SIGMOD 2025, Algorithm 1).
//!
//! A round starts with an empty main level and ends with the global merge of
//! the main level into the last, once `R` unit runs have come down from the
//! top level, one every `T_w` seconds. Between two arrivals the planner may
//! merge the newest runs of the main level into one, which takes `T_c`
//! seconds per unit of run merged and slows the queries running meanwhile to
//! `β` of their speed. A query's speed with `e` runs in the main level is
//! `q(e) = 1 / ((e + 2) · r + (1 − r) · (1 + f))`, `r` being the share of long
//! scans, which read every run, and `f` the filters' false positive rate a
//! point read pays per run. The plan maximises the queries served over the
//! round: an early merge serves the queries of every arrival after it, a late
//! one few, so merges grow lazier as the round goes on.

use alloc::vec::Vec;

/// What a plan is worked out from.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Inputs {
    /// Unit runs per round (`R`).
    pub round: u32,
    /// The last level's size over the main level's at the end of a round
    /// (`C`).
    pub size_ratio: u32,
    /// Seconds between two unit runs coming down (`T_w`).
    pub arrival_secs: f64,
    /// Seconds a merge in the main level takes per unit run it merges
    /// (`T_c`).
    pub merge_secs: f64,
    /// The share of their speed queries keep while a merge in the main level
    /// runs (`β`).
    pub merge_query_share: f64,
    /// The share of queries that are long scans (`r`).
    pub scan_share: f64,
    /// The filters' false positive rate (`f`).
    pub false_positive_rate: f64,
}

#[expect(
    clippy::suboptimal_flops,
    reason = "`mul_add` needs std, and the plan is solved without it too"
)]
impl Inputs {
    /// The speed of queries with `runs` runs in the main level.
    fn speed(&self, runs: u32) -> f64 {
        let r = self.scan_share;
        1.0 / ((f64::from(runs) + 2.0) * r + (1.0 - r) * (1.0 + self.false_positive_rate))
    }
}

/// The merges of a round: after the `t`-th unit run of the round has come
/// down, the newest runs of the main level adding up to `merge_at[t]` units
/// are merged into one; `1` merges nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Plan {
    merge_at: Vec<u32>,
}

impl Plan {
    /// The units the newest runs merged after the `units`-th unit run of the
    /// round add up to, `1` when nothing is merged then.
    #[must_use]
    pub fn merge_after(&self, units: u32) -> u32 {
        self.merge_at
            .get(usize::try_from(units).unwrap_or(usize::MAX))
            .copied()
            .unwrap_or(1)
    }

    /// Works out the plan for `inputs`, whose round and size ratio the
    /// strategy keeps within [`MAX_ROUND`] and [`MAX_SIZE_RATIO`].
    #[must_use]
    pub fn solve(inputs: &Inputs) -> Self {
        let round = inputs.round.clamp(1, MAX_ROUND);
        // Both bounded, so the product fits a u32 many times over.
        let last = inputs.size_ratio.min(MAX_SIZE_RATIO) * round;
        let mut solver = Solver {
            inputs,
            memo: crate::HashMap::default(),
        };
        let mut merge_at = alloc::vec![1; usize::try_from(round).unwrap_or(0) + 1];
        solver.schedule(0, round, last, 0, &mut merge_at);
        Self { merge_at }
    }
}

/// The most unit runs a round takes: the plan's work grows with its fourth
/// power.
pub const MAX_ROUND: u32 = 64;

/// The largest last-to-main size ratio a plan is worked out for.
pub const MAX_SIZE_RATIO: u32 = 64;

/// The memo of `f(e, c, m)`: the best score of placing `c` incoming unit
/// runs into a main level of `e` runs, ending in a merge of `m + c` units,
/// and the units of the first final run that score takes.
struct Solver<'a> {
    inputs: &'a Inputs,
    memo: crate::HashMap<(u32, u32, u32), (f64, u32)>,
}

#[expect(
    clippy::suboptimal_flops,
    reason = "`mul_add` needs std, and the plan is solved without it too"
)]
impl Solver<'_> {
    /// `f(e, c, m)` of Algorithm 1.
    fn best(&mut self, e: u32, c: u32, m: u32) -> f64 {
        if let Some(&(score, _)) = self.memo.get(&(e, c, m)) {
            return score;
        }
        let (tw, tc) = (self.inputs.arrival_secs, self.inputs.merge_secs);
        let solved = if c <= 1 {
            // The merge ending the problem, queries slowed meanwhile.
            let units = f64::from(m) + f64::from(c);
            (
                units * tc * self.inputs.merge_query_share * self.inputs.speed(e),
                1,
            )
        } else {
            // The first unit run stays a final run of its own.
            let mut best = (
                tw * self.inputs.speed(e + 1) + self.best(e + 1, c - 1, m + 1),
                1,
            );
            // Or the first `x` are merged into one, after the last of them.
            for x in 2..c {
                let score = self.best(e, x, 0)
                    + (tw - f64::from(x) * tc) * self.inputs.speed(e + 1)
                    + self.best(e + 1, c - x, m + x);
                // Ties keep the smaller merge, so the plan is the same on
                // every solve.
                if score > best.0 {
                    best = (score, x);
                }
            }
            best
        };
        self.memo.insert((e, c, m), solved);
        solved.0
    }

    /// Writes into `merge_at` the merges the best plan for `f(e, c, m)` makes,
    /// its first unit run being the round's `start + 1`-th.
    fn schedule(&mut self, e: u32, c: u32, m: u32, start: u32, merge_at: &mut [u32]) {
        if c <= 1 {
            return;
        }
        self.best(e, c, m);
        let x = self.memo.get(&(e, c, m)).map_or(1, |&(_, x)| x);
        if x > 1 {
            self.schedule(e, x, 0, start, merge_at);
            // The merge forming the first final run, after its last unit run;
            // it takes in every merge the left problem ends with then.
            if let Some(slot) = merge_at.get_mut(usize::try_from(start + x).unwrap_or(usize::MAX)) {
                *slot = (*slot).max(x);
            }
        }
        self.schedule(e + 1, c - x, m + x, start + x, merge_at);
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
mod tests;

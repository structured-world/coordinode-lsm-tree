// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Compaction planned for the throughput queries get over a compaction round.
//!
//! The goal is the queries served, rather than the least write or read
//! amplification (Wang et al., "Rethinking The Compaction Policies in
//! LSM-trees", SIGMOD 2025). Compactions and queries draw on the same CPU and
//! I/O, so a merge is an investment: it costs the queries that run while it
//! does, and it pays them back in a faster read for as long as its output
//! lasts. The strategy lays the tree out in three levels by what merging them
//! buys:
//!
//! - **top** (L0): flushed runs, never merged among themselves. Once they
//!   hold a unit `S` of bytes they go down together as one run of the main
//!   level.
//! - **main** (L1): runs of a unit or more. Between two units coming down, the
//!   newest runs may be merged into one, as the round's plan says.
//! - **last**: one run. Once `R` units have come down in a round, the main
//!   level's runs move as they are to L2, which ends the round, and are merged
//!   from there into the last. That merge holds neither the top nor the main
//!   level, so the next round's units come down while it runs.
//!
//! The plan of a round is worked out by dynamic programming over what the
//! merges cost and what they buy (see [`dp`]): early in the round a merge
//! serves many queries and is worth more, late in it few, so the plan merges
//! eagerly first and grows lazy. A unit is the last level's size over
//! `C · R`, so the tree grows into larger units rather than more levels.
//!
//! What the plan is worked out from is a [`Model`] of the machine and the
//! reads the tables recorded: the share of them that are range scans. The
//! same tree, model and reads give the same plan, however often and in
//! whichever process it is solved.

pub mod dp;

use super::{Choice, CompactionStrategy, Input as CompactionInput};
use crate::compaction::state::CompactionState;
use crate::config::Config;
use crate::table::Table;
use crate::version::Version;
use crate::{HashSet, TableId};
use alloc::sync::Arc;
use alloc::vec::Vec;

#[cfg(feature = "std")]
use parking_lot::Mutex;
// no-std: spin mirrors parking_lot's Mutex API without an allocator.
#[cfg(not(feature = "std"))]
use spin::Mutex;

#[doc(hidden)]
pub const NAME: &str = "PlannedCompaction";

/// The unit runs a round takes by default.
///
/// Measured on sustained writes with point reads and scans under a compaction
/// rate limit both strategies keep up with: 10 units served more queries than
/// leveled at no worse p99, where 25 left an ended round's runs to raise the
/// scan p99 by a sixth.
pub const DEFAULT_ROUND: u32 = 10;

/// The level an ended round's runs wait in to go into the last level.
const DRAIN: usize = 2;
#[expect(clippy::cast_possible_truncation, reason = "a small constant level")]
const DRAIN_LEVEL: u8 = DRAIN as u8;

/// What the planner knows of the machine a tree runs on.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Model {
    /// Bytes per second the tree takes in, which its flushes write.
    pub write_rate: u64,
    /// Bytes per second a merge in the main level encodes. The tree's
    /// compaction rate limit caps it, when one is set: what a merge may use
    /// is what the shared budget leaves it.
    pub encode_rate: u64,
    /// Bytes per second a merge copies what it does not encode.
    pub copy_rate: u64,
    /// The share of a merge's output it copies rather than encodes, as the
    /// work reports of past merges measure it.
    pub copied_share: f64,
    /// The share of their speed queries keep while a merge in the main level
    /// runs.
    pub merge_query_share: f64,
    /// The filters' false positive rate: what a point read pays per run its
    /// key is not in.
    pub false_positive_rate: f64,
}

impl Default for Model {
    /// A write stream of 16 MiB/s, merges encoding at 256 MiB/s and copying
    /// at 1 GiB/s, nothing copied, queries at half speed during a merge, and
    /// filters of 1% false positives.
    fn default() -> Self {
        Self {
            write_rate: 16 * 1_024 * 1_024,
            encode_rate: 256 * 1_024 * 1_024,
            copy_rate: 1_024 * 1_024 * 1_024,
            copied_share: 0.0,
            merge_query_share: 0.5,
            false_positive_rate: 0.01,
        }
    }
}

/// Compaction planned for query throughput over a compaction round.
///
/// # Examples
///
/// ```
/// use lsm_tree::compaction::Planned;
///
/// // Rounds of ten units; the last level holds three times the main.
/// let strategy = Planned::default().with_round(10).with_size_ratio(3);
/// # let _ = strategy;
/// ```
pub struct Strategy {
    /// Unit runs per round (`R`).
    round: u32,
    /// The last level's size over the main level's at the end of a round
    /// (`C`).
    size_ratio: u32,
    /// The smallest unit, and the target size of the tables written.
    target_size: u64,
    /// The L0 table count that sends the top level down whatever its size.
    l0_file_threshold: u16,
    model: Model,
    /// The share of reads that are scans, when set rather than counted.
    scan_share: Option<f64>,
    /// The plan last worked out, with what it was worked out from.
    plan: Mutex<Option<(dp::Inputs, Arc<dp::Plan>)>>,
}

impl Default for Strategy {
    fn default() -> Self {
        Self {
            round: DEFAULT_ROUND,
            size_ratio: 3,
            target_size: 64 * 1_024 * 1_024,
            l0_file_threshold: super::leveled::DEFAULT_L0_FILE_THRESHOLD,
            model: Model::default(),
            scan_share: None,
            plan: Mutex::new(None),
        }
    }
}

impl Strategy {
    /// Sets the unit runs a round takes before it ends in the last level
    /// (default [`DEFAULT_ROUND`], at least one and at most
    /// [`dp::MAX_ROUND`]).
    ///
    /// A round rewrites the last level once per `R` units, `C` units per unit
    /// written whatever `R` is; what `R` sets is how many runs an ended round
    /// leaves to read until it is merged into the last, and how large a unit
    /// is, the last level over `C · R`.
    #[must_use]
    pub fn with_round(mut self, units: u32) -> Self {
        self.round = units.clamp(1, dp::MAX_ROUND);
        self
    }

    /// Sets the last level's size over the main level's at the end of a
    /// round (default 3, at most [`dp::MAX_SIZE_RATIO`]).
    #[must_use]
    pub fn with_size_ratio(mut self, ratio: u32) -> Self {
        self.size_ratio = ratio.clamp(1, dp::MAX_SIZE_RATIO);
        self
    }

    /// Sets the target size of the tables written, which is also the
    /// smallest unit (default 64 MiB).
    #[must_use]
    pub fn with_table_target_size(mut self, bytes: u64) -> Self {
        self.target_size = bytes.max(1);
        self
    }

    /// Sets the L0 table count that sends the top level down whatever its
    /// size (default as [`Leveled`](super::Leveled)'s).
    #[must_use]
    pub fn with_l0_file_threshold(mut self, tables: u16) -> Self {
        self.l0_file_threshold = tables.max(1);
        self
    }

    /// Sets what the planner knows of the machine.
    #[must_use]
    pub fn with_model(mut self, model: Model) -> Self {
        self.model = model;
        self
    }

    /// Plans for `share` of the reads being range scans (clamped to `0..=1`)
    /// instead of the share the tree's tables counted: a stand-in for a
    /// workload known ahead, or not yet seen. `None` reads the tables again.
    #[must_use]
    pub fn with_scan_share(mut self, share: Option<f64>) -> Self {
        self.scan_share = share.map(|share| share.clamp(0.0, 1.0));
        self
    }

    /// Unit runs per round.
    const fn round(&self) -> u32 {
        self.round
    }

    /// The unit of a tree whose last level holds `last_bytes`.
    fn unit(&self, last_bytes: u64) -> u64 {
        (last_bytes / u64::from(self.size_ratio * self.round())).max(self.target_size)
    }

    /// The plan for `inputs`, solved once for the same inputs.
    fn plan(&self, inputs: dp::Inputs) -> Arc<dp::Plan> {
        let mut cached = self.plan.lock();
        if let Some((solved_for, plan)) = cached.as_ref()
            && *solved_for == inputs
        {
            return Arc::clone(plan);
        }
        let plan = Arc::new(dp::Plan::solve(&inputs));
        *cached = Some((inputs, Arc::clone(&plan)));
        plan
    }

    /// What the plan for `version` is worked out from, with `budget` bytes
    /// per second the tree's compactions may use, `0` for no limit.
    fn inputs(&self, version: &Version, unit: u64, budget: u64) -> dp::Inputs {
        let scan_share = self.scan_share.unwrap_or_else(|| {
            let (mut points, mut scans) = (0u64, 0u64);
            for table in version.iter_tables() {
                scans += table.scans();
                points += table.reads() - table.scans();
            }
            // A point read is counted by the one table holding its key, a scan
            // by a table of every run it reaches, which is every run of the
            // tree for a scan over a few hundred keys: the scans are its
            // counts over the runs there are.
            let runs: usize = version.iter_levels().map(|level| level.run_count()).sum();
            #[expect(
                clippy::cast_precision_loss,
                reason = "a share of read counts, approximate by nature"
            )]
            let scans = scans as f64 / runs.max(1) as f64;
            // With no read seen there is nothing to plan for: no merge in the
            // main level pays, and none runs.
            #[expect(
                clippy::cast_precision_loss,
                reason = "a share of read counts, approximate by nature"
            )]
            let points = points as f64;
            if scans + points == 0.0 {
                0.0
            } else {
                scans / (scans + points)
            }
        });
        let model = self.model;
        let encode_rate = match budget {
            0 => model.encode_rate,
            budget => model.encode_rate.min(budget),
        }
        .max(1);
        #[expect(
            clippy::cast_precision_loss,
            reason = "seconds of a unit, approximate by nature"
        )]
        let (unit, write_rate, copy_rate, encode_rate) = (
            unit as f64,
            model.write_rate.max(1) as f64,
            model.copy_rate.max(1) as f64,
            encode_rate as f64,
        );
        let copied = model.copied_share.clamp(0.0, 1.0);
        dp::Inputs {
            round: self.round(),
            size_ratio: self.size_ratio,
            arrival_secs: unit / write_rate,
            merge_secs: unit * (1.0 - copied) / encode_rate + unit * copied / copy_rate,
            merge_query_share: model.merge_query_share.clamp(0.0, 1.0),
            scan_share,
            false_positive_rate: model.false_positive_rate.clamp(0.0, 1.0),
        }
    }
}

/// The runs of the main level newest first, each with its units.
fn main_runs(main: &crate::version::Level, unit: u64) -> Vec<(Vec<TableId>, u64)> {
    main.iter()
        .map(|run| {
            let bytes: u64 = run.iter().map(Table::file_size).sum();
            // A run is at least a unit: the top level comes down a unit at a
            // time, and a merge of runs is the sum of theirs.
            let units = bytes.div_ceil(unit).max(1);
            (run.iter().map(Table::id).collect(), units)
        })
        .collect()
}

impl CompactionStrategy for Strategy {
    fn get_name(&self) -> &'static str {
        NAME
    }

    fn pending_compaction_bytes(&self, version: &Version) -> u64 {
        use crate::version::Level;

        let last = version.level_count() - 1;
        let last_bytes = version.level(last).map_or(0, Level::size);
        let unit = self.unit(last_bytes);
        let top = version.level(0).map_or(0, Level::size);
        let main = version.level(1).map_or(0, Level::size);
        let draining: u64 = (DRAIN..last)
            .filter_map(|index| version.level(index))
            .map(Level::size)
            .sum();
        // The top level past its unit, the main level past its round, and an
        // ended round still to go into the last: a level within its bound
        // owes nothing, not less than nothing.
        top.saturating_sub(unit) + main.saturating_sub(unit * u64::from(self.round())) + draining
    }

    fn next_merge_level(&self, version: &Version, dest_level: u8) -> Option<usize> {
        // A run of the main level is next merged whole with others, or into
        // the last level whole: no boundary of another level is worth an
        // output's cut.
        let _ = (version, dest_level);
        None
    }

    fn choose(&self, version: &Version, config: &Config, state: &CompactionState) -> Choice {
        let last = version.level_count() - 1;
        if last <= DRAIN {
            return Choice::DoNothing;
        }
        let hidden = state.hidden_set();
        let busy = |index: usize| version.level_is_busy(index, hidden);
        let (Some(top), Some(main), Some(last_level)) =
            (version.level(0), version.level(1), version.level(last))
        else {
            return Choice::DoNothing;
        };
        let unit = self.unit(last_level.size());
        let runs = main_runs(main, unit);
        // Bounded by the bytes the levels hold.
        let units: u64 = runs.iter().map(|(_, units)| units).sum();
        #[expect(clippy::cast_possible_truncation, reason = "level index below 255")]
        let last_index = last as u8;

        // The top level holds a unit: it comes down as one run. First, as the
        // cheapest work and the one that keeps flushes from piling up.
        if !busy(0)
            && !busy(1)
            && !top.is_empty()
            && (top.size() >= unit || top.table_count() >= usize::from(self.l0_file_threshold))
        {
            let choice = CompactionInput {
                table_ids: top.list_ids(),
                dest_level: 1,
                canonical_level: 1,
                target_size: self.target_size,
            };
            let cmp = config.comparator.as_ref();
            let key_range = top.aggregate_key_range_cmp(cmp);
            let overlaps = main
                .iter()
                .any(|run| !run.get_overlapping_cmp(&key_range, cmp).is_empty());
            if top.run_count() == 1 && !overlaps {
                return Choice::Move(choice);
            }
            return Choice::Merge(choice);
        }

        // An ended round's runs, and anything else left between the main and
        // the last level, go into the last. The merge holds neither the top
        // nor the main level, so units keep coming down while it runs.
        let draining: HashSet<TableId> = (DRAIN..last)
            .filter_map(|index| version.level(index))
            .flat_map(crate::version::Level::list_ids)
            .collect();
        if draining.is_empty() {
            // The round is over: its runs move out of the main level as they
            // are, kept apart and in age order, so the next round starts on an
            // empty one.
            if units >= u64::from(self.round()) && !busy(1) && !busy(DRAIN) {
                return Choice::Move(CompactionInput {
                    table_ids: main.list_ids(),
                    dest_level: DRAIN_LEVEL,
                    canonical_level: DRAIN_LEVEL,
                    target_size: self.target_size,
                });
            }
        } else if (DRAIN..=last).all(|index| !busy(index)) {
            let mut table_ids = draining;
            table_ids.extend(last_level.list_ids());
            return Choice::Merge(CompactionInput {
                table_ids,
                dest_level: last_index,
                canonical_level: last_index,
                target_size: self.target_size,
            });
        }

        if busy(1) {
            return Choice::DoNothing;
        }
        // The merge the plan makes after this many units came down.
        let budget = config
            .compaction_rate_limiter
            .as_ref()
            .map_or(config.compaction_rate_limit, |limiter| limiter.rate());
        let plan = self.plan(self.inputs(version, unit, budget));
        let wanted = u64::from(plan.merge_after(u32::try_from(units).unwrap_or(u32::MAX)));
        if wanted > 1 {
            let mut table_ids = HashSet::default();
            let (mut merged_units, mut merged_runs) = (0u64, 0usize);
            for (ids, run_units) in &runs {
                if merged_units >= wanted {
                    break;
                }
                table_ids.extend(ids.iter().copied());
                merged_units += run_units;
                merged_runs += 1;
            }
            if merged_runs > 1 {
                return Choice::Merge(CompactionInput {
                    table_ids,
                    dest_level: 1,
                    canonical_level: 1,
                    target_size: self.target_size,
                });
            }
        }
        Choice::DoNothing
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
mod tests;

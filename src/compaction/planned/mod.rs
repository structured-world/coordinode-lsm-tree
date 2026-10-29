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
//!
//! # Compaction budget
//!
//! The engine does not cap how fast a tree is written:
//! [`Config::write_rate_limit`](crate::Config::write_rate_limit) paces writes
//! only while the tree slows them down for compaction to catch up. What it
//! does cap, when the deployment asks, is compaction:
//! [`Config::compaction_rate_limit`](crate::Config::compaction_rate_limit), or
//! a limiter shared by the trees of one device, gives compaction its share of
//! the device. That share is the budget, and the deployment sets it: how much
//! of the device its reads and writes need is not something the tree can see.
//!
//! Every written byte is compacted again before it settles, so a budget that
//! pays for less than the write rate times the write amplification leaves
//! compaction behind and runs pile up. This strategy reads the budget and
//! plans a tree it can keep: a round costs the size ratio plus two units of
//! compaction per unit written, so the size ratio is cut to what the budget
//! over [`Model::write_rate`] pays for, down to one. Set the model's write
//! rate to what the tree takes in.
//!
//! A [write budget](crate::write_budget) on the medium of the last level caps
//! the budget too: over the long run compaction may write the allowance's rate,
//! less the write rate when L0 is on the same budget and its flushes spend
//! from it. The
//! plan then keeps a tree that budget pays for, rather than one whose merges
//! the budget's gate would hold back.
//!
//! One measurement, made with `db_bench planned-sustained` (3M rows of
//! 16-byte keys and 100-byte values, uncompressed, written at 8 MiB/s with
//! point reads and scans between flushes, two compaction threads per tree;
//! three runs, planned against [`Leveled`](crate::compaction::Leveled) at the
//! same budget):
//!
//! | Budget | Queries/s | Scan p99 | Point read p99 | Compaction thread time | Mean runs |
//! |---|---|---|---|---|---|
//! | 8× the write rate | +7% to +14% | −11% to −21% | −8% to +7% | about a third | 4.7–5.0 vs 6.6–7.0 |
//! | 2× the write rate | +6% to +13% | −5% to +10% | +7% to +18% | about half | 11.9–12.1 vs 16.0–17.1 |
//!
//! At eight times the write rate the planner wrote more compaction bytes per
//! written byte than leveled (7.6–8.0 vs 7.0–7.3), at two times fewer
//! (3.6–4.1 vs 4.5–4.6). These are figures of one machine and one workload:
//! another device, key and value size, read mix or budget can move each of
//! them either way. Measure the workload that runs, the bench above included.
//!
//! # A model calibrated from observation
//!
//! The [`Model`] is set, not learned. A model moved toward what a tree
//! observes (its merges' rate, the filters' false positive rate, the speed
//! queries keep beside a merge, a scan's cost per run against a point read's)
//! was measured against it with `db_bench planned-sustained`: 1M rows, four
//! read mixes (even points and 100-key scans, Zipf-hot points with short
//! scans, 1000-key scans, 32 runs in L0), both budgets, three runs, each
//! model judged on steps it was not fitted to. It won every run on two
//! (Zipf-hot 2× by 12% to 129%, 32 L0 runs 8× by 13% to 80%), lost every run
//! on one (Zipf-hot 8× by 24% to 46%), and went either way on the other five.
//! Its plan changed 4 to 27 times a run where the set model's never did, and
//! both models' predicted query cost erred by 40% to 110% on the held-out
//! steps: the main level's run count does not explain what a query costs.
//! Queries beside a merge kept their speed, so the share they keep fitted to
//! its bound of one. The set model is kept. With 32 runs in L0, leveled
//! served the most queries in two runs of three at either budget.

pub mod dp;

use super::{Choice, CompactionStrategy, Input as CompactionInput};
use crate::compaction::state::CompactionState;
use crate::config::Config;
use crate::table::Table;
use crate::version::Version;
use crate::{HashSet, TableId};
use alloc::sync::Arc;
use alloc::vec::Vec;
use portable_atomic::{AtomicU64, Ordering};

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
    /// Bytes per second the tree takes in, which its flushes write. With a
    /// compaction limiter shared by several trees, the rate they all take in
    /// together: the budget is theirs together, so each plans what the shared
    /// budget pays for over the shared writes.
    pub write_rate: u64,
    /// Bytes per second a merge in the main level encodes. The tree's
    /// compaction rate limit caps it, when one is set: what a merge may use
    /// is what the shared budget leaves it. Over [`Self::write_rate`] it also
    /// bounds the size ratio a round can afford: a round costs the ratio plus
    /// two units of compaction per unit written.
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
    /// The compaction budget the last choice saw, for the debt reported
    /// between choices.
    budget_seen: AtomicU64,
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
            budget_seen: AtomicU64::new(0),
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
    /// round (default 3, at most [`dp::MAX_SIZE_RATIO`]). A model whose
    /// compaction rate cannot pay for it gets a smaller one (see
    /// [`Model::encode_rate`]).
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

    /// The last level's size over the main level's at the end of a round, as
    /// far as the model's compaction rate pays for it.
    ///
    /// A round writes each unit down once and rewrites it into the last level
    /// with `C` more, `C + 2` units of compaction per unit written: a rate
    /// below that falls behind every round, and what it cannot do piles up in
    /// L0. The ratio shrinks to what the rate pays, down to 1, never past the
    /// one set. The rate is the model's, capped by `budget`, the bytes per
    /// second the tree's compactions may use (`0` for no limit).
    fn size_ratio(&self, budget: u64) -> u32 {
        let paid = within_budget(self.model.encode_rate, budget) / self.model.write_rate.max(1);
        // Below two units per unit written no ratio is paid for; the floor of
        // one is the cheapest round there is.
        let paid = u32::try_from(paid.saturating_sub(2)).unwrap_or(u32::MAX);
        paid.clamp(1, self.size_ratio)
    }

    /// The unit of a tree whose last level holds `last_bytes`, compacting
    /// within `budget`.
    fn unit(&self, last_bytes: u64, budget: u64) -> u64 {
        (last_bytes / (u64::from(self.size_ratio(budget)) * u64::from(self.round())))
            .max(self.target_size)
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
            // Queries as the tree served them: a lookup whatever the filters
            // answered, a scan, row or projected, however many runs it reached.
            let (points, scans) = version.read_mix().counts();
            #[expect(
                clippy::cast_precision_loss,
                reason = "a share of read counts, approximate by nature"
            )]
            let scans = scans as f64;
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
        // A copied group is read through the same limiter as an encoded one.
        let encode_rate = within_budget(model.encode_rate, budget).max(1);
        let copy_rate = within_budget(model.copy_rate, budget).max(1);
        #[expect(
            clippy::cast_precision_loss,
            reason = "seconds of a unit, approximate by nature"
        )]
        let (unit, write_rate, copy_rate, encode_rate) = (
            unit as f64,
            model.write_rate.max(1) as f64,
            copy_rate as f64,
            encode_rate as f64,
        );
        let copied = model.copied_share.clamp(0.0, 1.0);
        dp::Inputs {
            round: self.round(),
            size_ratio: self.size_ratio(budget),
            arrival_secs: unit / write_rate,
            merge_secs: unit * (1.0 - copied) / encode_rate + unit * copied / copy_rate,
            merge_query_share: model.merge_query_share.clamp(0.0, 1.0),
            scan_share,
            false_positive_rate: model.false_positive_rate.clamp(0.0, 1.0),
        }
    }
}

/// The plan a [`Strategy`] would follow for one version, and what it was
/// worked out from.
#[derive(Clone, Debug)]
pub struct PlanView {
    /// What the plan is worked out from.
    pub inputs: dp::Inputs,
    /// The plan of the round.
    pub plan: Arc<dp::Plan>,
    /// The unit runs that have come down in the round so far.
    pub units: u32,
}

impl Strategy {
    /// The plan this strategy would follow for `version` under `config`'s
    /// compaction budget, worked out as [`CompactionStrategy::choose`] works it
    /// out but choosing nothing: for comparing models side by side on one
    /// tree. `None` for a tree with no main level apart from the last, which
    /// has no plan.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};
    /// use lsm_tree::compaction::Planned;
    ///
    /// # let folder = tempfile::tempdir()?;
    /// # let tree = Config::new(folder.path(), SequenceNumberCounter::default(), SequenceNumberCounter::default()).open()?;
    /// let strategy = Planned::default();
    /// let view = strategy
    ///     .plan_for(&tree.current_version(), tree.tree_config())
    ///     .expect("a tree has a main level apart from the last");
    /// // Nothing has come down yet: the round is at its start.
    /// assert_eq!(view.units, 0);
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    #[must_use]
    pub fn plan_for(&self, version: &Version, config: &Config) -> Option<PlanView> {
        let last = version.level_count() - 1;
        if last < DRAIN {
            return None;
        }
        let budget = self.budget_for(config, last);
        let unit = self.unit(
            version.level(last).map_or(0, crate::version::Level::size),
            budget,
        );
        let bytes: u64 = version.level(1).map_or(0, |main| {
            main_runs(main).iter().map(|(_, bytes)| bytes).sum()
        });
        let inputs = self.inputs(version, unit, budget);
        Some(PlanView {
            inputs,
            plan: self.plan(inputs),
            units: u32::try_from(bytes / unit).unwrap_or(u32::MAX),
        })
    }

    /// The tree's compaction budget in bytes per second, `0` for no limit:
    /// the tighter of its rate limit and the write budget of the medium of
    /// `last`, its last level.
    fn budget_for(&self, config: &Config, last: usize) -> u64 {
        #[expect(clippy::cast_possible_truncation, reason = "level index below 255")]
        tighter(
            budget(config),
            endurance_budget(config, last as u8, self.model.write_rate).unwrap_or(0),
        )
    }
}

/// The bytes a merge may encode or copy per second: the model's rate, capped
/// by the tree's compaction `budget` when it has one (`0` for none).
fn within_budget(model_rate: u64, budget: u64) -> u64 {
    match budget {
        0 => model_rate,
        budget => model_rate.min(budget),
    }
}

/// The tree's compaction budget in bytes per second, `0` for no limit: the
/// live rate of its limiter. A limiter shared by several trees gives each the
/// whole rate, which is why [`Model::write_rate`] is then their writes
/// together.
fn budget(config: &Config) -> u64 {
    config
        .live_compaction_rate_limiter
        .as_ref()
        .or(config.compaction_rate_limiter.as_ref())
        .map_or(config.compaction_rate_limit, |limiter| limiter.rate())
}

/// The bytes per second compaction may write over the long run on the medium
/// of `level`, `None` without a write budget there: the budget's allowance
/// rate, less `write_rate` when L0, which the flushes write, is on the same
/// budget and spends from it. An allowance the writes alone use up leaves
/// compaction one byte a second, the least a budget can say, since `0` reads
/// as no limit.
fn endurance_budget(config: &Config, level: u8, write_rate: u64) -> Option<u64> {
    let (_, fs) = config.tables_folder_for_level(level);
    let budget = fs.write_budget()?;
    let allowance = budget.allowance();
    let per_sec = u128::from(allowance.bytes_per_period()) * 1_000_000_000
        / allowance.period().as_nanos().max(1);
    let per_sec = u64::try_from(per_sec).unwrap_or(u64::MAX);
    let (_, l0_fs) = config.tables_folder_for_level(0);
    let flushes_here = l0_fs
        .write_budget()
        .is_some_and(|l0| Arc::ptr_eq(l0, budget));
    if !flushes_here {
        return Some(per_sec.max(1));
    }
    // Below the write rate nothing is left: none, not an error.
    Some(per_sec.saturating_sub(write_rate).max(1))
}

/// The tighter of two budgets in bytes per second, `0` standing for none.
fn tighter(a: u64, b: u64) -> u64 {
    match (a, b) {
        (0, other) | (other, 0) => other,
        (a, b) => a.min(b),
    }
}

/// The runs of the main level newest first, each with its bytes.
fn main_runs(main: &crate::version::Level) -> Vec<(Vec<TableId>, u64)> {
    main.iter()
        .map(|run| {
            let bytes: u64 = run.iter().map(Table::file_size).sum();
            (run.iter().map(Table::id).collect(), bytes)
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
        // A tree of one level has no compaction to owe.
        if last == 0 {
            return 0;
        }
        let last_bytes = version.level(last).map_or(0, Level::size);
        // The budget the last choice saw: the version alone does not say it.
        let unit = self.unit(last_bytes, self.budget_seen.load(Ordering::Relaxed));
        let top_level = version.level(0);
        let top = top_level.map_or(0, Level::size);
        // The top level past its unit owes the bytes past it; one past its
        // table count owes all of it, as `choose` sends it all down then.
        let top_owed = if top_level
            .is_some_and(|level| level.table_count() >= usize::from(self.l0_file_threshold))
        {
            top
        } else {
            top.saturating_sub(unit)
        };
        // A tree of two levels owes only its top level: the main level is the
        // last, which nothing drains.
        if last < DRAIN {
            return top_owed;
        }
        let main = version.level(1).map_or(0, Level::size);
        let draining: u64 = (DRAIN..last)
            .filter_map(|index| version.level(index))
            .map(Level::size)
            .sum();
        // The top level past its unit, the main level past its round, and an
        // ended round still to go into the last: a level within its bound
        // owes nothing, not less than nothing.
        top_owed + main.saturating_sub(unit * u64::from(self.round())) + draining
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
        // A tree of one level has no level to compact into.
        if last == 0 {
            return Choice::DoNothing;
        }
        let hidden = state.hidden_set();
        let busy = |index: usize| version.level_is_busy(index, hidden);
        #[expect(
            clippy::expect_used,
            reason = "levels 0 to the last exist in every version, and the last is past 0"
        )]
        let (top, main, last_level) = (
            version.level(0).expect("L0"),
            version.level(1).expect("L1"),
            version.level(last).expect("the last level"),
        );
        let budget = self.budget_for(config, last);
        self.budget_seen.store(budget, Ordering::Relaxed);
        let unit = self.unit(last_level.size(), budget);
        let runs = main_runs(main);
        // The units come down so far, counted by bytes: runs the L0 table
        // count sent down before they held a unit count for what they hold.
        // Counted from the version, not from arrivals kept beside it, so the
        // plan is the same in every process: a round ends once the main level
        // holds a round of units, as a leveled level within its target never
        // goes down; overwrites a main-level merge collapses stay there,
        // merged as reads ask, and tombstones, which only the last level drops,
        // fill the round.
        let units = runs.iter().map(|(_, bytes)| bytes).sum::<u64>() / unit;
        #[expect(clippy::cast_possible_truncation, reason = "level index below 255")]
        let last_index = last as u8;

        let top_due = !top.is_empty()
            && (top.size() >= unit || top.table_count() >= usize::from(self.l0_file_threshold));

        // A tree of two levels has no main level apart from the last: the top
        // level goes into it whole, merged with what it holds.
        if last < DRAIN {
            if top_due && !busy(0) && !busy(1) {
                let mut table_ids = top.list_ids();
                table_ids.extend(main.list_ids());
                return Choice::Merge(CompactionInput {
                    table_ids,
                    dest_level: last_index,
                    canonical_level: last_index,
                    target_size: self.target_size,
                });
            }
            return Choice::DoNothing;
        }

        // The top level holds a unit: it comes down as one run. First, as the
        // cheapest work and the one that keeps flushes from piling up.
        if !busy(0) && !busy(1) && top_due {
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
                // A tree of three levels has no level between the main and
                // the last to wait in: the round merges into the last at once.
                if last == DRAIN {
                    let mut table_ids = main.list_ids();
                    table_ids.extend(last_level.list_ids());
                    return Choice::Merge(CompactionInput {
                        table_ids,
                        dest_level: last_index,
                        canonical_level: last_index,
                        target_size: self.target_size,
                    });
                }
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
        let plan = self.plan(self.inputs(version, unit, budget));
        let wanted = u64::from(plan.merge_after(u32::try_from(units).unwrap_or(u32::MAX)));
        if wanted > 1 {
            // At most a round of units, each at most the last level's bytes.
            let wanted_bytes = wanted * unit;
            let mut table_ids = HashSet::default();
            let (mut merged_bytes, mut merged_runs) = (0u64, 0usize);
            for (ids, run_bytes) in &runs {
                if merged_bytes >= wanted_bytes {
                    break;
                }
                table_ids.extend(ids.iter().copied());
                merged_bytes += run_bytes;
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

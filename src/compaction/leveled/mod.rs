// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::useless_vec,
    clippy::unnecessary_map_or,
    reason = "test code"
)]
mod test;

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::indexing_slicing, reason = "test code")]
mod picker_tests;

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
mod primitive_tests;

mod primitives;

pub use primitives::{Granularity, Layout, LevelPolicy, Movement, Trigger};

use super::{Choice, CompactionStrategy, Input as CompactionInput};
use crate::{
    HashSet, TableId,
    compaction::state::{CompactionState, hidden_set::HiddenSet},
    config::Config,
    table::{Table, util::aggregate_run_key_range},
    version::{Level, Version, run::Ranged},
};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// A merge candidate's cost: the bytes it moves down a level (`promoted`) and
/// every byte it reads and rewrites to do so (`total`, the promoted bytes
/// plus those it pulls in from the level below), and the reads its tables
/// served (`heat`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MergeCost {
    promoted: u64,
    total: u64,
    heat: u64,
}

impl MergeCost {
    /// Whether `self` rewrites fewer bytes per promoted byte than `other`.
    ///
    /// Cross-multiplied in `u128`: two `u64` byte counts multiply into at
    /// most 128 bits, so the comparison is exact for every size a level can
    /// hold, where dividing first would lose the low bits.
    fn cheaper_than(self, other: Self) -> bool {
        u128::from(self.total) * u128::from(other.promoted)
            < u128::from(other.total) * u128::from(self.promoted)
    }

    /// Whether `movement` prefers `self` to `other`: the least rewrite per
    /// promoted byte, or the most reads per byte rewritten with the least
    /// rewrite breaking a tie. Cross-multiplied in `u128` as
    /// [`Self::cheaper_than`] is.
    fn better_than(self, other: Self, movement: Movement) -> bool {
        match movement {
            Movement::LeastRewrite { .. } => self.cheaper_than(other),
            Movement::Hottest => {
                let ours = u128::from(self.heat) * u128::from(other.total);
                let theirs = u128::from(other.heat) * u128::from(self.total);
                ours > theirs || (ours == theirs && self.cheaper_than(other))
            }
        }
    }
}

/// Streams merge candidates and keeps the one that pays down a level's debt
/// best by the level's [`Movement`]: by default at the least rewriting per
/// promoted byte.
///
/// Ranking by the total input alone prefers a small merge that promotes little
/// into a dense level below, which has to be repeated many times to retire the
/// same debt; the ratio ranks how much each promoted byte costs. Among the
/// candidates that promote no more than `overshoot + slack`, the best one
/// wins, so a level is not asked to push down far more than it owes; when none
/// is that small, the best overall still makes progress. Ties go to the
/// earliest candidate, so the choice is the same on every call.
///
/// Only the two leaders are held, whatever the number of candidates offered.
struct MergeRanking<T> {
    movement: Movement,
    bound: u64,
    offered: usize,
    /// The cheapest candidate within the bound, with its offer index.
    within: Option<(usize, MergeCost, T)>,
    /// The cheapest candidate overall, with its offer index.
    any: Option<(usize, MergeCost, T)>,
    /// The cost of the cheapest candidate after `any`.
    second: Option<MergeCost>,
}

impl<T: Copy> MergeRanking<T> {
    fn new(overshoot: u64, slack: u64, movement: Movement) -> Self {
        Self {
            movement,
            // A bound past `u64::MAX` admits every candidate, which is what
            // clamping to `u64::MAX` means: no promoted byte count exceeds it.
            bound: overshoot.saturating_add(slack),
            offered: 0,
            within: None,
            any: None,
            second: None,
        }
    }

    fn offer(&mut self, cost: MergeCost, candidate: T) {
        let index = self.offered;
        let movement = self.movement;
        self.offered += 1;
        if cost.promoted <= self.bound
            && self
                .within
                .as_ref()
                .is_none_or(|(_, best, _)| cost.better_than(*best, movement))
        {
            self.within = Some((index, cost, candidate));
        }
        match &self.any {
            Some((_, best, _)) if !cost.better_than(*best, movement) => {
                if self
                    .second
                    .is_none_or(|second| cost.better_than(second, movement))
                {
                    self.second = Some(cost);
                }
            }
            previous => {
                self.second = previous.as_ref().map(|(_, best, _)| *best);
                self.any = Some((index, cost, candidate));
            }
        }
    }

    /// The chosen candidate with its offer index and cost, and the cost of the
    /// cheapest candidate not chosen.
    fn finish(self) -> Option<((usize, MergeCost, T), Option<MergeCost>)> {
        match (self.within, self.any) {
            (Some(within), Some(any)) if within.0 != any.0 => Some((within, Some(any.1))),
            (Some(chosen), _) | (None, Some(chosen)) => Some((chosen, self.second)),
            (None, None) => None,
        }
    }

    fn offered(&self) -> usize {
        self.offered
    }
}

/// Tries to find the most optimal compaction set from one level into the other.
///
/// Scans all runs in both levels to handle transient multi-run states from
/// multi-level compaction (#108). See #122 Part 3.
fn pick_minimal_compaction(
    curr_level: &Level,
    next_level: &Level,
    hidden_set: &HiddenSet,
    overshoot: u64,
    table_base_size: u64,
    movement: Movement,
    cmp: &dyn crate::comparator::UserComparator,
) -> Option<(HashSet<TableId>, bool)> {
    let promotion_slack = match movement {
        Movement::LeastRewrite { slack } => slack.unwrap_or(table_base_size),
        Movement::Hottest => table_base_size,
    };
    if let Some(ids) = largest_trivial_move(curr_level, next_level, hidden_set, cmp) {
        return Some((ids, true));
    }

    // NOTE: Look for merges
    // Iterate windows across all runs in next_level, pull in from all runs
    // in curr_level.
    if next_level.is_empty() {
        return None;
    }

    // Candidates are ranked as they are enumerated, holding only a window's
    // place and its cost for the leaders; the winner's tables are collected at
    // the end.
    let mut ranking = MergeRanking::new(overshoot, promotion_slack, movement);
    let cap = 50 * table_base_size;
    for (run_index, run) in next_level.iter().enumerate() {
        let window_bytes = prefix_sums(run.iter().map(Table::file_size));
        let window_hidden =
            prefix_sums(run.iter().map(|t| u64::from(hidden_set.is_hidden(t.id()))));
        let window_heat = prefix_sums(run.iter().map(Table::reads));
        let pull_ins: Vec<PullIn> = curr_level
            .iter()
            .map(|curr_run| PullIn::new(curr_run, run, hidden_set, cmp))
            .collect();
        // Windows by width, then by start, the order that decides ties; the
        // first window past the cap ends the run's candidates, whatever the
        // width of those after it.
        'run: for width in 1..=run.len() {
            for start in 0..=(run.len() - width) {
                let end = start + width;
                #[expect(
                    clippy::indexing_slicing,
                    reason = "start < end <= run.len(), and the sums hold run.len() + 1 entries"
                )]
                let (next_level_size, next_hidden, next_heat) = (
                    window_bytes[end] - window_bytes[start],
                    window_hidden[end] - window_hidden[start],
                    window_heat[end] - window_heat[start],
                );
                if next_level_size > cap {
                    break 'run;
                }
                if next_hidden > 0 {
                    continue;
                }
                let mut promoted = 0;
                let mut heat = next_heat;
                let mut blocked = false;
                for pull_in in &pull_ins {
                    let (bytes, hidden, reads) = pull_in.contained(start, end - 1);
                    promoted += bytes;
                    heat += reads;
                    blocked |= hidden > 0;
                }
                if promoted == 0 || blocked {
                    continue;
                }
                ranking.offer(
                    MergeCost {
                        promoted,
                        total: promoted + next_level_size,
                        heat,
                    },
                    (run_index, start, end),
                );
            }
        }
    }

    let offered = ranking.offered();
    let ((chosen, cost, (run_index, start, end)), runner_up) = ranking.finish()?;
    log::debug!(
        "leveled: merge candidate {chosen} of {offered} chosen at {cost:?}, runner-up \
         {runner_up:?} (overshoot {overshoot}, slack {promotion_slack})",
    );
    #[expect(
        clippy::expect_used,
        reason = "the ranking holds a window it was offered from these runs"
    )]
    let window = next_level
        .iter()
        .nth(run_index)
        .and_then(|run| run.get(start..end))
        .expect("the chosen window lies in its run");
    let key_range = aggregate_run_key_range(window);
    let contained: HashSet<_> = curr_level
        .iter()
        .flat_map(|run| run.get_contained_cmp(&key_range, cmp))
        .map(Table::id)
        .collect();
    let pulled_in = contained.len();
    let curr_ids = with_overlapping_runs(curr_level, contained, hidden_set, cmp)?;
    let mut ids: HashSet<_> = window.iter().map(Table::id).collect();
    // What the other runs added may reach past the window: what it overlaps
    // below goes into the merge too, so the level below stays one run.
    if curr_ids.len() > pulled_in {
        for table in curr_level.iter().flat_map(|run| run.iter()) {
            if !curr_ids.contains(&table.id()) {
                continue;
            }
            for below in next_level
                .iter()
                .flat_map(|run| run.get_overlapping_cmp(table.key_range(), cmp))
            {
                if ids.insert(below.id()) && hidden_set.is_hidden(below.id()) {
                    return None;
                }
            }
        }
    }
    ids.extend(curr_ids);
    Some((ids, false))
}

/// `ids`, tables of `level`, with every table of its runs that shares a key
/// with them, taken in until none is left out; `None` when one is hidden.
///
/// The runs of a level hold versions of one key in more than one of them,
/// and a read stops at the first level that holds the key: a merge that
/// took the newer version down and left the older above would have reads
/// return the older.
fn with_overlapping_runs(
    level: &Level,
    mut ids: HashSet<TableId>,
    hidden_set: &HiddenSet,
    cmp: &dyn crate::comparator::UserComparator,
) -> Option<HashSet<TableId>> {
    // The tables of one run share no key.
    if level.run_count() <= 1 {
        return Some(ids);
    }
    loop {
        let mut ranges: Vec<_> = level
            .iter()
            .flat_map(|run| run.iter())
            .filter(|table| ids.contains(&table.id()))
            .map(|table| table.key_range().clone())
            .collect();
        ranges.sort_by(|a, b| cmp.compare(a.min(), b.min()));
        let mut grew = false;
        for interval in crate::KeyRange::merge_sorted_cmp(ranges, cmp) {
            for table in level
                .iter()
                .flat_map(|run| run.get_overlapping_cmp(&interval, cmp))
            {
                if ids.insert(table.id()) {
                    if hidden_set.is_hidden(table.id()) {
                        return None;
                    }
                    grew = true;
                }
            }
        }
        if !grew {
            return Some(ids);
        }
    }
}

/// The longest window of one run of `curr_level` that overlaps no table of
/// `next_level`, shares no key with the other runs of `curr_level` and holds no
/// hidden table, the leftmost of that length and from the first run that has
/// one: a move that rewrites nothing.
///
/// A window's key range is its tables' ranges and the gaps between them, so it
/// overlaps the level below exactly when one of its tables does or one of its
/// gaps does. The windows that qualify are therefore the stretches of
/// neighbouring tables that each qualify with clear gaps between them, and
/// the longest such stretch is found in one pass. A key of a table the other
/// runs hold as well stays where it is: a read stops at the first level that
/// holds it, and the version left above may be the older one.
fn largest_trivial_move(
    curr_level: &Level,
    next_level: &Level,
    hidden_set: &HiddenSet,
    cmp: &dyn crate::comparator::UserComparator,
) -> Option<HashSet<TableId>> {
    use core::ops::Bound;

    let free_of = |runs: &[alloc::sync::Arc<crate::version::run::Run<Table>>],
                   skip: Option<usize>,
                   bounds: (Bound<&crate::Slice>, Bound<&crate::Slice>)| {
        runs.iter().enumerate().all(|(index, run)| {
            Some(index) == skip
                || run
                    .range_overlap_indexes_cmp::<crate::Slice, _, _>(&bounds, cmp)
                    .is_none()
        })
    };
    let clear = |bounds| free_of(next_level, None, bounds);
    for (run_index, curr_run) in curr_level.iter().enumerate() {
        // The longest stretch so far, as (start, length), and the current one.
        let mut best: Option<(usize, usize)> = None;
        let (mut stretch_start, mut stretch_len) = (0, 0);
        let mut previous: Option<&Table> = None;
        for (index, table) in curr_run.iter().enumerate() {
            let range = table.key_range();
            let bounds = (Bound::Included(range.min()), Bound::Included(range.max()));
            let movable = !hidden_set.is_hidden(table.id())
                && clear(bounds)
                && free_of(curr_level, Some(run_index), bounds);
            if !movable {
                stretch_len = 0;
                previous = None;
                continue;
            }
            let joins = previous.is_some_and(|previous| {
                clear((
                    Bound::Excluded(previous.key_range().max()),
                    Bound::Excluded(range.min()),
                ))
            });
            if joins {
                stretch_len += 1;
            } else {
                (stretch_start, stretch_len) = (index, 1);
            }
            previous = Some(table);
            if best.is_none_or(|(_, len)| stretch_len > len) {
                best = Some((stretch_start, stretch_len));
            }
        }
        if let Some((start, len)) = best {
            return curr_run
                .get(start..start + len)
                .map(|window| window.iter().map(Table::id).collect());
        }
    }
    None
}

/// A window of one run of `level` to go into a tiered level below, which
/// takes nothing of that level in, none of its tables hidden.
///
/// Every window rewrites only its own bytes, so least rewrite takes the
/// leading tables of the first run holding `overshoot` bytes; the hottest
/// takes the window with the most reads per byte, ranked as a merge into a
/// leveled level ranks them.
fn window_over(
    level: &Level,
    hidden_set: &HiddenSet,
    overshoot: u64,
    movement: Movement,
    table_base_size: u64,
) -> Option<HashSet<TableId>> {
    let (run, start, end) = match movement {
        Movement::LeastRewrite { .. } => {
            let run = level.iter().next()?;
            let mut bytes = 0u64;
            let mut end = 0;
            for table in run.iter() {
                if hidden_set.is_hidden(table.id()) {
                    break;
                }
                end += 1;
                bytes += table.file_size();
                if bytes >= overshoot {
                    break;
                }
            }
            (run, 0, end)
        }
        Movement::Hottest => {
            let mut ranking = MergeRanking::new(overshoot, table_base_size, movement);
            let cap = 50 * table_base_size;
            for (run_index, run) in level.iter().enumerate() {
                let bytes = prefix_sums(run.iter().map(Table::file_size));
                let hidden =
                    prefix_sums(run.iter().map(|t| u64::from(hidden_set.is_hidden(t.id()))));
                let heat = prefix_sums(run.iter().map(Table::reads));
                'run: for width in 1..=run.len() {
                    for start in 0..=(run.len() - width) {
                        let end = start + width;
                        #[expect(
                            clippy::indexing_slicing,
                            reason = "start < end <= run.len(), and the sums hold run.len() + 1 entries"
                        )]
                        let (size, hidden, heat) = (
                            bytes[end] - bytes[start],
                            hidden[end] - hidden[start],
                            heat[end] - heat[start],
                        );
                        if size > cap {
                            break 'run;
                        }
                        if hidden > 0 {
                            continue;
                        }
                        ranking.offer(
                            MergeCost {
                                promoted: size,
                                total: size,
                                heat,
                            },
                            (run_index, start, end),
                        );
                    }
                }
            }
            let ((_, _, (run_index, start, end)), _) = ranking.finish()?;
            (level.iter().nth(run_index)?, start, end)
        }
    };
    let ids: HashSet<_> = run.get(start..end)?.iter().map(Table::id).collect();
    (!ids.is_empty()).then_some(ids)
}

/// Prefix sums: entry `i` is the sum of the first `i` items.
fn prefix_sums(items: impl Iterator<Item = u64>) -> Vec<u64> {
    let mut sums = Vec::with_capacity(items.size_hint().0 + 1);
    sums.push(0);
    let mut total = 0;
    for item in items {
        // Byte sizes or counts of one run's tables, bounded by the disk.
        total += item;
        sums.push(total);
    }
    sums
}

/// The tables of one run of the level a merge promotes from that each window
/// of one run of the level below would pull in, priced in constant time.
///
/// A window pulls in the tables its key range contains. In a run, sorted by
/// key with disjoint ranges, those are the tables starting at or after the
/// window's min key and ending at or before its max key: a contiguous stretch
/// whose ends move only forward as the window's ends do.
struct PullIn {
    /// For each table of the window run, the first table of this run starting
    /// at or after its min key.
    first_from: Vec<usize>,
    /// For each table of the window run, the number of tables of this run
    /// ending at or before its max key.
    end_through: Vec<usize>,
    /// Prefix sums of this run's table sizes, of its hidden tables and of the
    /// reads its tables served.
    bytes: Vec<u64>,
    hidden: Vec<u64>,
    reads: Vec<u64>,
}

impl PullIn {
    fn new(
        run: &[Table],
        window_run: &[Table],
        hidden_set: &HiddenSet,
        cmp: &dyn crate::comparator::UserComparator,
    ) -> Self {
        use core::cmp::Ordering;

        let mut first_from = Vec::with_capacity(window_run.len());
        let mut at = 0;
        for table in window_run {
            let min = table.key_range().min();
            while run
                .get(at)
                .is_some_and(|t| cmp.compare(t.key_range().min(), min) == Ordering::Less)
            {
                at += 1;
            }
            first_from.push(at);
        }
        let mut end_through = Vec::with_capacity(window_run.len());
        at = 0;
        for table in window_run {
            let max = table.key_range().max();
            while run
                .get(at)
                .is_some_and(|t| cmp.compare(t.key_range().max(), max) != Ordering::Greater)
            {
                at += 1;
            }
            end_through.push(at);
        }
        Self {
            first_from,
            end_through,
            bytes: prefix_sums(run.iter().map(Table::file_size)),
            hidden: prefix_sums(run.iter().map(|t| u64::from(hidden_set.is_hidden(t.id())))),
            reads: prefix_sums(run.iter().map(Table::reads)),
        }
    }

    /// The bytes, the hidden tables and the reads of what the window from
    /// the window run's table `first` to its table `last` pulls in.
    #[expect(
        clippy::indexing_slicing,
        reason = "first and last index the window run, and the stretch ends lie in 0..=run.len()"
    )]
    fn contained(&self, first: usize, last: usize) -> (u64, u64, u64) {
        let (from, through) = (self.first_from[first], self.end_through[last]);
        if from < through {
            (
                self.bytes[through] - self.bytes[from],
                self.hidden[through] - self.hidden[from],
                self.reads[through] - self.reads[from],
            )
        } else {
            (0, 0, 0)
        }
    }
}

#[doc(hidden)]
pub const NAME: &str = "LeveledCompaction";

/// The default of [`Strategy::with_l0_file_threshold`].
pub const DEFAULT_L0_FILE_THRESHOLD: u16 = 500;

/// Leveled compaction strategy (LCS)
///
/// When a level reaches some threshold size, parts of it are merged into overlapping tables in the next level.
///
/// Each level Ln for n >= 2 can have up to `level_base_size * ratio^(n - 1)` tables.
///
/// LCS suffers from comparatively high write amplification, but has decent read amplification and great space amplification (~1.1x).
///
/// LCS is the recommended compaction strategy to use.
///
/// More info here: <https://fjall-rs.github.io/post/lsm-leveling/>
#[derive(Clone)]
pub struct Strategy {
    /// L0 run count that triggers a merge into L1.
    l0_threshold: u8,

    /// L0 table count that triggers a merge into L1 whatever the run count.
    l0_file_threshold: u16,

    /// The target table size as disk (possibly compressed).
    target_size: u64,

    /// Size ratio between levels of the LSM tree (a.k.a fanout, growth rate)
    level_ratio_policy: Vec<f32>,

    /// When true, dynamically sizes levels based on the actual data in
    /// the last non-empty level, reducing space amplification to ~1.1x.
    ///
    /// Same as `level_compaction_dynamic_level_bytes` in `RocksDB`.
    ///
    /// Default = false (static leveling).
    dynamic: bool,

    /// When true, enables multi-level compaction: if L0→L1 is chosen but
    /// L1 is already oversized, compacts L0+L1→L2 directly in one pass
    /// to avoid a write-then-rewrite cycle.
    ///
    /// Default = false.
    multi_level: bool,

    /// How many bytes past a level's overshoot a merge may promote and still
    /// be preferred on its cost per promoted byte. `None` = one target table
    /// size, the smallest step a merge can take.
    promotion_slack: Option<u64>,

    /// The choices set for a level in place of the leveled ones, by level.
    policies: [Option<LevelPolicy>; 7],
}

impl Default for Strategy {
    fn default() -> Self {
        Self {
            l0_threshold: 4,
            l0_file_threshold: DEFAULT_L0_FILE_THRESHOLD,
            target_size:/* 64 MiB */ 64 * 1_024 * 1_024,
            level_ratio_policy: vec![10.0],
            dynamic: false,
            multi_level: false,
            promotion_slack: None,
            policies: [None; 7],
        }
    }
}

impl Strategy {
    /// Sets the growth ratio between levels.
    ///
    /// Same as `set_max_bytes_for_level_multiplier` in `RocksDB`.
    ///
    /// Default = [10.0]
    #[must_use]
    pub fn with_level_ratio_policy(mut self, policy: Vec<f32>) -> Self {
        self.level_ratio_policy = policy;
        self
    }

    /// Sets the L0 threshold.
    ///
    /// When the number of runs in L0 reaches this threshold, L0 is merged
    /// into L1. A run's tables have disjoint key ranges, so a read pays at
    /// most one table per run: the run count bounds L0's read amplification,
    /// where the table count does not. A flush larger than
    /// [`Config::table_target_size`](crate::Config::table_target_size) writes
    /// one run of several tables and counts once.
    ///
    /// Plays the role of `level0_file_num_compaction_trigger` in `RocksDB`,
    /// whose flushes write one table each, so files and runs coincide there.
    ///
    /// Default = 4; `0` is taken as `1`, since no run count is below zero.
    #[must_use]
    pub fn with_l0_threshold(mut self, threshold: u8) -> Self {
        self.l0_threshold = threshold.max(1);
        self
    }

    /// Sets the L0 table count that triggers a merge into L1 whatever the run
    /// count.
    ///
    /// A guard against fan-out: every L0 table costs a file handle, a
    /// manifest entry and per-table metadata in memory, however few runs
    /// hold them.
    ///
    /// Keep it above the run threshold times the tables one flush writes
    /// (memtable size over
    /// [`Config::table_target_size`](crate::Config::table_target_size)):
    /// below that it fires before the run trigger, on every few flushes,
    /// and L0 merges into L1 as often as a file-count trigger would make it.
    /// The default, with the default run threshold of 4, leaves room for a
    /// memtable of 125 tables of the target (about 8 GiB at 64 MiB).
    ///
    /// Default = 500; `0` is taken as `1`, since no table count is below zero.
    ///
    /// # Examples
    ///
    /// ```
    /// use lsm_tree::compaction::Leveled;
    ///
    /// let strategy = Leveled::default().with_l0_file_threshold(200);
    /// # let _ = strategy;
    /// ```
    #[must_use]
    pub fn with_l0_file_threshold(mut self, threshold: u16) -> Self {
        self.l0_file_threshold = threshold.max(1);
        self
    }

    /// Whether L0's shape alone calls for a merge into L1: its run count or
    /// its table count has reached its threshold.
    fn l0_triggered(&self, l0: &Level) -> bool {
        // L0 is never the last level of a tree that compacts.
        shape_triggered(self.policy(0, 0, usize::MAX).trigger, l0)
    }
}

/// Whether a level's shape alone calls for compacting it: it is triggered by
/// its shape, and its run count or its table count has reached its count.
/// A count of `0` is taken as `1`, since no count is below zero.
fn shape_triggered(trigger: Trigger, level: &Level) -> bool {
    match trigger {
        Trigger::Shape { runs, tables } => {
            level.run_count() >= usize::from(runs.max(1))
                || level.table_count() >= usize::from(tables.max(1))
        }
        Trigger::Size | Trigger::Never => false,
    }
}

/// A level's score under a shape trigger of `runs` and `tables`: how far past
/// the nearer of the two counts it is.
fn shape_score((runs, tables): (u8, u16), level: &Level) -> f64 {
    #[expect(
        clippy::cast_precision_loss,
        reason = "counts of runs and tables are far below 2^52"
    )]
    let (run_count, table_count) = (level.run_count() as f64, level.table_count() as f64);
    (run_count / f64::from(runs.max(1))).max(table_count / f64::from(tables.max(1)))
}

impl Strategy {
    /// Sets the table target size on disk (possibly compressed).
    ///
    /// Same as `target_file_size_base` in `RocksDB`.
    ///
    /// Default = 64 MiB
    #[must_use]
    pub fn with_table_target_size(mut self, bytes: u64) -> Self {
        self.target_size = bytes;
        self
    }

    /// Enables dynamic level sizing based on actual data in the last level.
    ///
    /// When enabled, level target sizes are computed top-down from the actual
    /// size of the last non-empty level, divided by the ratio at each step.
    /// This reduces space amplification to ~1.1x while keeping write
    /// amplification comparable to static leveling.
    ///
    /// Same as `level_compaction_dynamic_level_bytes` in `RocksDB`.
    ///
    /// Default = false
    #[must_use]
    pub fn with_dynamic_level_bytes(mut self, enabled: bool) -> Self {
        self.dynamic = enabled;
        self
    }

    /// Enables multi-level compaction optimization.
    ///
    /// When L0→L1 compaction is selected but L1 already exceeds its target
    /// size, this option allows compacting L0+L1 directly into L2 in one
    /// pass, avoiding the write-then-rewrite cycle that would otherwise
    /// occur.
    ///
    /// Default = false
    #[must_use]
    pub fn with_multi_level(mut self, enabled: bool) -> Self {
        self.multi_level = enabled;
        self
    }

    /// Sets how many bytes past a level's overshoot a merge may promote and
    /// still be preferred.
    ///
    /// A leveled merge is chosen by the bytes it rewrites per byte it moves
    /// down a level, among the candidates that promote no more than the
    /// level's overshoot plus this slack; when none is that small, the
    /// cheapest candidate overall is taken, so the level still makes progress.
    ///
    /// Default = the table target size.
    ///
    /// # Examples
    ///
    /// ```
    /// use lsm_tree::compaction::Leveled;
    ///
    /// let strategy = Leveled::default().with_promotion_slack(128 * 1_024 * 1_024);
    /// # let _ = strategy;
    /// ```
    #[must_use]
    pub fn with_promotion_slack(mut self, bytes: u64) -> Self {
        self.promotion_slack = Some(bytes);
        self
    }

    /// Sets the four choices for level `level` in place of the leveled ones:
    /// when it compacts, how it holds its data, how much one compaction out
    /// of it takes and which data that is. A level index past the tree's last
    /// level is ignored.
    ///
    /// The leveled defaults are: L0 triggered by its shape (see
    /// [`Self::with_l0_threshold`] and [`Self::with_l0_file_threshold`]),
    /// tiered and compacted whole (through L1 when
    /// [`Self::with_multi_level`] is on); every other level leveled,
    /// triggered by its size, and compacted a window at a time chosen by the
    /// least rewrite per promoted byte (see [`Self::with_promotion_slack`]);
    /// the last level never triggered.
    ///
    /// # Examples
    ///
    /// ```
    /// use lsm_tree::compaction::{Granularity, Layout, LevelPolicy, Leveled, Movement, Trigger};
    ///
    /// // L1 accumulates runs and goes down whole once it holds four.
    /// let strategy = Leveled::default().with_level_policy(
    ///     1,
    ///     LevelPolicy {
    ///         trigger: Trigger::Shape { runs: 4, tables: u16::MAX },
    ///         layout: Layout::Tiered,
    ///         granularity: Granularity::Level { through_full_next: false },
    ///         movement: Movement::LeastRewrite { slack: None },
    ///     },
    /// );
    /// # let _ = strategy;
    /// ```
    #[must_use]
    pub fn with_level_policy(mut self, level: u8, policy: LevelPolicy) -> Self {
        if let Some(slot) = self.policies.get_mut(usize::from(level)) {
            *slot = Some(policy);
        }
        self
    }

    /// The choices for the level at `physical`, `level_shift` levels below
    /// its place while the tree is small, of a tree whose last level is
    /// `last`: the ones set for its place, or the leveled ones. A level is
    /// set by its place, L1 being the first level holding data below L0.
    fn policy(&self, physical: usize, level_shift: usize, last: usize) -> LevelPolicy {
        let level = if physical == 0 {
            0
        } else {
            physical.saturating_sub(level_shift).max(1)
        };
        let policy = self
            .policies
            .get(level)
            .copied()
            .flatten()
            .unwrap_or_else(|| self.leveled_policy(level));
        // The last level only receives compactions, whatever its place.
        if physical == last && physical > 0 {
            return LevelPolicy {
                trigger: Trigger::Never,
                ..policy
            };
        }
        policy
    }

    /// The leveled choices for the level at place `level`.
    fn leveled_policy(&self, level: usize) -> LevelPolicy {
        LevelPolicy {
            trigger: if level == 0 {
                Trigger::Shape {
                    runs: self.l0_threshold,
                    tables: self.l0_file_threshold,
                }
            } else {
                Trigger::Size
            },
            layout: if level == 0 {
                Layout::Tiered
            } else {
                Layout::Leveled
            },
            granularity: if level == 0 {
                Granularity::Level {
                    through_full_next: self.multi_level,
                }
            } else {
                Granularity::Window
            },
            movement: Movement::LeastRewrite {
                slack: self.promotion_slack,
            },
        }
    }

    /// Calculates the size of L1.
    fn level_base_size(&self) -> u64 {
        self.target_size * u64::from(self.l0_threshold)
    }

    /// Calculates the level target size.
    ///
    /// L1 = `level_base_size`
    ///
    /// L2 = `level_base_size * ratio`
    ///
    /// L3 = `level_base_size * ratio * ratio`
    ///
    /// ...
    fn level_target_size(&self, canonical_level_idx: u8) -> u64 {
        assert!(
            canonical_level_idx >= 1,
            "level_target_size does not apply to L0",
        );

        if canonical_level_idx == 1 {
            // u64::from(self.target_size)
            self.level_base_size()
        } else {
            #[expect(
                clippy::cast_precision_loss,
                reason = "precision loss is acceptable for level size calculations"
            )]
            let mut size = self.level_base_size() as f32;

            // NOTE: Minus 2 because |{L0, L1}|
            for idx in 0..=(canonical_level_idx - 2) {
                let ratio = self
                    .level_ratio_policy
                    .get(usize::from(idx))
                    .copied()
                    .unwrap_or_else(|| self.level_ratio_policy.last().copied().unwrap_or(10.0));

                size *= ratio;
            }

            #[expect(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "size is always positive and will never even come close to u64::MAX"
            )]
            {
                size as u64
            }
        }
    }

    /// Computes level target sizes for all 7 levels.
    ///
    /// In static mode, uses the standard exponential formula.
    /// In dynamic mode, derives targets from the actual size of the last
    /// non-empty level, dividing backwards by the ratio at each step.
    /// Falls back to static mode when the tree is small (dynamic L1 target
    /// would be less than `level_base_size`).
    fn compute_level_targets(
        &self,
        version: &Version,
        level_shift: usize,
        state: &CompactionState,
    ) -> [u64; 7] {
        let mut targets = [u64::MAX; 7];

        // L0 target is not size-based (it's count-based), so leave at MAX
        targets[0] = u64::MAX;

        if self.dynamic {
            // Find the last non-empty level (Lmax) and its actual size.
            // Iterate forward and keep track of the last non-empty level
            // since iter_levels() does not support DoubleEndedIterator.
            let mut lmax_idx = None;

            for (idx, lvl) in version.iter_levels().enumerate().skip(1) {
                if !lvl.is_empty() {
                    lmax_idx = Some(idx);
                }
            }

            if let Some(lmax_idx) = lmax_idx {
                #[expect(
                    clippy::expect_used,
                    reason = "lmax_idx was found by iterating levels, so it must exist"
                )]
                let lmax_level = version.level(lmax_idx).expect("level should exist");

                let lmax_size: u64 = lmax_level
                    .iter()
                    .flat_map(|run| run.iter())
                    .filter(|table| !state.hidden_set().is_hidden(table.id()))
                    .map(Table::file_size)
                    .sum();

                // Work backwards from Lmax
                if let Some(slot) = targets.get_mut(lmax_idx) {
                    *slot = lmax_size;
                }

                #[expect(
                    clippy::cast_precision_loss,
                    reason = "precision loss is acceptable for level size calculations"
                )]
                let mut current_target = lmax_size as f64;

                // Only backfill down to the effective L1 (accounting for
                // level_shift), not to physical level 1, so we don't
                // overwrite slots below the shifted canonical L1.
                let dynamic_l1_idx = level_shift + 1;

                for idx in (dynamic_l1_idx..lmax_idx).rev() {
                    let canonical = idx - level_shift;
                    // In the forward formula, target(k+1)/target(k) = ratio[k-1],
                    // so backwards: target(k) = target(k+1) / ratio[k-1]
                    // Clamp-to-zero: the ratio for the first dynamic level reuses
                    // index 0 rather than underflowing to a phantom level.
                    let ratio_idx = canonical.saturating_sub(1);
                    let ratio = f64::from(
                        self.level_ratio_policy
                            .get(ratio_idx)
                            .copied()
                            .unwrap_or_else(|| {
                                self.level_ratio_policy.last().copied().unwrap_or(10.0)
                            }),
                    );

                    // Guard against invalid ratios (zero, negative, NaN, infinite).
                    // Fall back to static targets instead of leaving partial
                    // dynamic targets with u64::MAX in lower-level slots.
                    if !ratio.is_finite() || ratio <= 0.0 {
                        return self.compute_static_targets(level_shift);
                    }

                    current_target /= ratio;

                    #[expect(
                        clippy::cast_possible_truncation,
                        clippy::cast_sign_loss,
                        reason = "target is always positive"
                    )]
                    if let Some(slot) = targets.get_mut(idx) {
                        *slot = current_target as u64;
                    }
                }

                // Fallback: if dynamic L1 target is too small, use static.
                // Compare the shifted L1 slot, not physical slot 1.
                let static_l1 = self.level_base_size();
                if targets.get(dynamic_l1_idx).copied().unwrap_or(0) < static_l1 {
                    return self.compute_static_targets(level_shift);
                }

                return targets;
            }
        }

        self.compute_static_targets(level_shift)
    }

    /// Computes static (exponential) level targets.
    fn compute_static_targets(&self, level_shift: usize) -> [u64; 7] {
        let mut targets = [u64::MAX; 7];

        for (idx, slot) in targets.iter_mut().enumerate().skip(1) {
            if idx <= level_shift {
                continue; // stays at u64::MAX
            }
            #[expect(
                clippy::cast_possible_truncation,
                reason = "level index is bounded by level count (7)"
            )]
            {
                *slot = self.level_target_size((idx - level_shift) as u8);
            }
        }

        targets
    }
}

impl CompactionStrategy for Strategy {
    fn get_name(&self) -> &'static str {
        NAME
    }

    fn get_config(&self) -> Vec<crate::KvPair> {
        vec![
            (
                crate::UserKey::from("leveled_l0_threshold"),
                crate::UserValue::from(self.l0_threshold.to_le_bytes()),
            ),
            (
                crate::UserKey::from("leveled_l0_file_threshold"),
                crate::UserValue::from(self.l0_file_threshold.to_le_bytes()),
            ),
            (
                crate::UserKey::from("leveled_target_size"),
                crate::UserValue::from(self.target_size.to_le_bytes()),
            ),
            (
                crate::UserKey::from("leveled_level_ratio_policy"),
                crate::UserValue::from({
                    use crate::io::{LittleEndian, WriteBytesExt};

                    let mut v = vec![];

                    #[expect(
                        clippy::expect_used,
                        clippy::cast_possible_truncation,
                        reason = "writing into Vec should not fail; policies have length of 255 max"
                    )]
                    v.write_u8(self.level_ratio_policy.len() as u8)
                        .expect("cannot fail");

                    for &f in &self.level_ratio_policy {
                        #[expect(clippy::expect_used, reason = "writing into Vec should not fail")]
                        v.write_f32::<LittleEndian>(f).expect("cannot fail");
                    }

                    v
                }),
            ),
            (
                crate::UserKey::from("leveled_dynamic"),
                crate::UserValue::from([u8::from(self.dynamic)]),
            ),
            (
                crate::UserKey::from("leveled_multi_level"),
                crate::UserValue::from([u8::from(self.multi_level)]),
            ),
        ]
    }

    fn pending_compaction_bytes(&self, version: &Version) -> u64 {
        // Read-only snapshot: an empty compaction state (nothing hidden) so the
        // targets reflect the full current footprint; no level shift.
        let targets = self.compute_level_targets(version, 0, &CompactionState::default());
        let last = version.level_count() - 1;
        let mut debt = 0u64;
        for (idx, level) in version.iter_levels().enumerate() {
            // Each term is at most one level's on-disk size, so the running sum is
            // bounded by the tree's total footprint (disk capacity) and cannot
            // overflow u64 — plain addition.
            // The figure is read with no level shift, as the targets are.
            match self.policy(idx, 0, last).trigger {
                // A shape-triggered level: once its runs or its tables reach
                // their count, its whole size is pending, the same predicate
                // `choose` acts on.
                trigger @ Trigger::Shape { .. } => {
                    if shape_triggered(trigger, level) {
                        debt += level.size();
                    }
                }
                // A level never triggered still owes the bytes past its
                // target, as the last level does in a static tree.
                Trigger::Size | Trigger::Never => {
                    if let Some(&target) = targets.get(idx)
                        && idx > 0
                    {
                        // max(0, size - target): the bytes above this level's
                        // target are the work that must be rewritten downward.
                        debt += level.size().saturating_sub(target);
                    }
                }
            }
        }
        debt
    }

    #[expect(clippy::too_many_lines)]
    fn choose(&self, version: &Version, config: &Config, state: &CompactionState) -> Choice {
        // Tie the invariant to the caller-supplied `Config::level_count`
        // rather than a literal so a tree explicitly configured with a
        // non-default level count does not panic here. The assertion still
        // fires when a `Version` carries a different count than the
        // `Config` driving this compactor invocation — that mismatch is a
        // real bug (Version and Config disagree).
        assert!(
            version.level_count() == usize::from(config.level_count),
            "leveled compaction requires version.level_count() == config.level_count (got version={}, config={})",
            version.level_count(),
            config.level_count,
        );
        let cmp = config.comparator.as_ref();
        let last = version.level_count() - 1;

        // Trivial move into Lmax
        'trivial_lmax: {
            #[expect(
                clippy::expect_used,
                reason = "level 0 is guaranteed to exist in a valid version"
            )]
            let l0 = version.level(0).expect("first level should exist");

            if !l0.is_empty() && l0.is_disjoint() {
                let lmax_index = version.level_count() - 1;

                if (1..lmax_index).any(|idx| {
                    #[expect(
                        clippy::expect_used,
                        reason = "levels within level_count are guaranteed to exist"
                    )]
                    let level = version.level(idx).expect("level should exist");
                    !level.is_empty()
                }) {
                    // There are intermediary levels with data, cannot trivially move to Lmax
                    break 'trivial_lmax;
                }

                #[expect(
                    clippy::expect_used,
                    reason = "lmax_index is derived from level_count so level is guaranteed to exist"
                )]
                let lmax = version.level(lmax_index).expect("last level should exist");

                if !lmax
                    .aggregate_key_range_cmp(cmp)
                    .overlaps_with_key_range_cmp(&l0.aggregate_key_range_cmp(cmp), cmp)
                {
                    return Choice::Move(CompactionInput {
                        table_ids: l0.list_ids(),
                        #[expect(
                            clippy::cast_possible_truncation,
                            reason = "level count is at most 7, fits in u8"
                        )]
                        dest_level: lmax_index as u8,
                        canonical_level: 1,
                        target_size: self.target_size,
                    });
                }
            }
        }

        // Find the level that corresponds to L1
        #[expect(clippy::map_unwrap_or)]
        let first_non_empty_level = version
            .iter_levels()
            .enumerate()
            .skip(1)
            .find(|(_, lvl)| !lvl.is_empty())
            .map(|(idx, _)| idx)
            .unwrap_or_else(|| version.level_count() - 1);

        let mut canonical_l1_idx = first_non_empty_level;

        // Number of levels we have to shift to get from the actual level idx to the canonical
        let mut level_shift = canonical_l1_idx - 1;

        if canonical_l1_idx > 1 && version.iter_levels().skip(1).any(|lvl| !lvl.is_empty()) {
            let need_new_l1 = version
                .iter_levels()
                .enumerate()
                .skip(1)
                .filter(|(_, lvl)| !lvl.is_empty())
                .all(|(idx, level)| {
                    let level_size = level
                        .iter()
                        .flat_map(|x| x.iter())
                        // NOTE: Take bytes that are already being compacted into account,
                        // otherwise we may be overcompensating
                        .filter(|x| !state.hidden_set().is_hidden(x.id()))
                        .map(Table::file_size)
                        .sum::<u64>();

                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "level index is bounded by level count (7, technically 255)"
                    )]
                    let target_size = self.level_target_size((idx - level_shift) as u8);

                    level_size > target_size
                });

            // Move up L1 one level if all current levels are at capacity
            if need_new_l1 {
                canonical_l1_idx -= 1;
                level_shift -= 1;
            }
        }

        // Trivial move into L1: free, so taken whatever L0's trigger says,
        // unless L0 is set never to start a compaction.
        'trivial: {
            let first_level = version.l0();
            let target_level_idx = first_non_empty_level.min(canonical_l1_idx);

            if self.policy(0, level_shift, last).trigger == Trigger::Never {
                break 'trivial;
            }

            if first_level.run_count() == 1 {
                if version.level_is_busy(0, state.hidden_set())
                    || version.level_is_busy(target_level_idx, state.hidden_set())
                {
                    break 'trivial;
                }

                let Some(target_level) = &version.level(target_level_idx) else {
                    break 'trivial;
                };

                if target_level.run_count() != 1 {
                    break 'trivial;
                }

                let key_range = first_level.aggregate_key_range_cmp(cmp);

                // Get overlapping tables in next level
                let get_overlapping = target_level
                    .iter()
                    .flat_map(|run| run.get_overlapping_cmp(&key_range, cmp))
                    .map(Table::id)
                    .next();

                if get_overlapping.is_none() && first_level.is_disjoint() {
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "level index is bounded by level count (7)"
                    )]
                    return Choice::Move(CompactionInput {
                        table_ids: first_level.list_ids(),
                        dest_level: target_level_idx as u8,
                        canonical_level: 1,
                        target_size: self.target_size,
                    });
                }
            }
        }

        // Intra-L0 compaction: merge multiple L0 runs into a single run within
        // L0 while L0 is a few tables. It rewrites the whole of L0, so it pays
        // only on a small L0: on runs of many tables it would fire after every
        // flush, keep the run count under the trigger and never let L0 go
        // down, rewriting an ever larger L0 each time.
        // Only an L0 triggered by its run count compacts within itself: the
        // merge keeps its runs few.
        if let Trigger::Shape { runs, .. } = self.policy(0, level_shift, last).trigger {
            let first_level = version.l0();

            // A triggered L0 goes down instead: rewriting it here would clear
            // the debt (a file guard below the run threshold) without moving
            // anything to L1. `l0_triggered` reads L0's policy, the one set
            // for it when one is, so a table count set lower than the
            // defaults is the one this tests.
            if first_level.run_count() > 1
                && first_level.table_count() < usize::from(runs.max(1))
                && !self.l0_triggered(first_level)
                && !version.level_is_busy(0, state.hidden_set())
            {
                return Choice::Merge(CompactionInput {
                    table_ids: first_level.list_ids(),
                    dest_level: 0,
                    canonical_level: 0,
                    target_size: self.target_size,
                });
            }
        }

        // Compute level targets (supports both static and dynamic modes)
        let level_targets = self.compute_level_targets(version, level_shift, state);

        // Scoring
        let mut scores = [(/* score */ 0.0, /* overshoot */ 0u64); 7];

        {
            // TODO(weak-tombstone-rewrite): incorporate `Table::weak_tombstone_count` and
            // `Table::weak_tombstone_reclaimable` when computing level scores so rewrite
            // decisions can prioritize tables that would free the most reclaimable values.

            for (idx, level) in version.iter_levels().enumerate() {
                let trigger = self.policy(idx, level_shift, last).trigger;
                // A level triggered by its shape scores by it, whether or not
                // it is empty, as L0 always has.
                if let Trigger::Shape { runs, tables } = trigger {
                    if shape_triggered(trigger, level)
                        && let Some(slot) = scores.get_mut(idx)
                    {
                        *slot = (shape_score((runs, tables), level), 0);
                    }
                    continue;
                }
                // The last level, and any level set so, receives compactions
                // and never starts one.
                if trigger == Trigger::Never || level.is_empty() || idx == 0 {
                    continue;
                }

                let level_size = level
                    .iter()
                    .flat_map(|x| x.iter())
                    // NOTE: Take bytes that are already being compacted into account,
                    // otherwise we may be overcompensating
                    .filter(|x| !state.hidden_set().is_hidden(x.id()))
                    .map(Table::file_size)
                    .sum::<u64>();

                // NOTE: We check for level length above
                #[expect(clippy::indexing_slicing)]
                let target_size = level_targets[idx];

                #[expect(
                    clippy::indexing_slicing,
                    reason = "idx is from iter_levels().enumerate() so always < 7 = scores.len()"
                )]
                if level_size > target_size {
                    #[expect(
                        clippy::cast_precision_loss,
                        reason = "precision loss is acceptable for scoring calculations"
                    )]
                    let score = level_size as f64 / target_size as f64;
                    scores[idx] = (score, level_size - target_size);

                    // NOTE: Force a trivial move
                    if version
                        .level(idx + 1)
                        .is_some_and(|next_level| next_level.is_empty())
                    {
                        scores[idx] = (99.99, 999);
                    }
                }
            }
        }

        // Choose compaction
        #[expect(clippy::expect_used, reason = "highest score is expected to exist")]
        let (level_idx_with_highest_score, (score, overshoot_bytes)) = scores
            .into_iter()
            .enumerate()
            .max_by(|(_, (score_a, _)), (_, (score_b, _))| {
                score_a
                    .partial_cmp(score_b)
                    .unwrap_or(core::cmp::Ordering::Equal)
            })
            .expect("should have highest score somewhere");

        if score < 1.0 {
            return Choice::DoNothing;
        }

        // We choose L0->L1 compaction, the whole of L0
        let l0_policy = self.policy(0, level_shift, last);
        if let (0, Granularity::Level { through_full_next }) =
            (level_idx_with_highest_score, l0_policy.granularity)
        {
            let Some(first_level) = version.level(0) else {
                return Choice::DoNothing;
            };

            if version.level_is_busy(0, state.hidden_set())
                || version.level_is_busy(canonical_l1_idx, state.hidden_set())
            {
                return Choice::DoNothing;
            }

            let Some(target_level) = &version.level(canonical_l1_idx) else {
                return Choice::DoNothing;
            };

            let mut table_ids = first_level.list_ids();

            let key_range = first_level.aggregate_key_range_cmp(cmp);

            // Get overlapping tables in next level
            let target_level_overlapping_table_ids: Vec<_> = target_level
                .iter()
                .flat_map(|run| run.get_overlapping_cmp(&key_range, cmp))
                .map(Table::id)
                .collect();

            // A leveled L1 takes the merge into its run; a tiered one takes
            // its output as a run of its own.
            let l1_tiered =
                self.policy(canonical_l1_idx, level_shift, last).layout == Layout::Tiered;
            if !l1_tiered {
                table_ids.extend(&target_level_overlapping_table_ids);
            }

            // Multi-level compaction: if L1 is already oversized, skip it
            // and compact L0+L1 directly into L2 in one pass.
            // NOTE: Currently triggers on pre-compaction L1 score. A future
            // improvement could use projected post-compaction bytes to also
            // catch cases where L1 is close to its target and this batch
            // would push it over.
            if through_full_next && !l1_tiered {
                let l1_score = scores.get(canonical_l1_idx).map_or(0.0, |(s, _)| *s);
                let l2_idx = canonical_l1_idx + 1;

                if l1_score > 1.0
                    && l2_idx < version.level_count()
                    && !version.level_is_busy(l2_idx, state.hidden_set())
                    && let Some(l2) = version.level(l2_idx)
                {
                    // Include ALL L1 tables (we're emptying L1 into L2)
                    table_ids.extend(target_level.list_ids());

                    // Include overlapping L2 tables — query per merged
                    // interval instead of one coarse aggregate (#72).
                    // An aggregate across disjoint tables (e.g. [a,d] and
                    // [x,z] → [a,z]) covers gaps and pulls in L2 tables
                    // that don't actually overlap any input table.
                    //
                    // Merge input key ranges into disjoint intervals first
                    // to reduce redundant queries when L0 tables overlap
                    // (#122 Part 2). Sort by comparator-min, then coalesce.
                    {
                        let mut input_ranges: Vec<_> = target_level
                            .iter()
                            .chain(first_level.iter())
                            .flat_map(|run| run.iter())
                            .map(|t| t.key_range().clone())
                            .collect();
                        input_ranges.sort_by(|a, b| cmp.compare(a.min(), b.min()));

                        let merged = crate::KeyRange::merge_sorted_cmp(input_ranges, cmp);

                        for run in l2.iter() {
                            for interval in &merged {
                                for l2t in run.get_overlapping_cmp(interval, cmp) {
                                    table_ids.insert(Table::id(l2t));
                                }
                            }
                        }
                    }

                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "level index is bounded by level count (7)"
                    )]
                    return Choice::Merge(CompactionInput {
                        table_ids,
                        dest_level: l2_idx as u8,
                        canonical_level: 2,
                        target_size: self.target_size,
                    });
                }
            }

            #[expect(
                clippy::cast_possible_truncation,
                reason = "level index is bounded by level count (7, technically 255)"
            )]
            let choice = CompactionInput {
                table_ids,
                dest_level: canonical_l1_idx as u8,
                canonical_level: 1,
                target_size: self.target_size,
            };

            if target_level_overlapping_table_ids.is_empty() && first_level.is_disjoint() {
                return Choice::Move(choice);
            }
            return Choice::Merge(choice);
        }

        // We choose L1+ compaction instead, or a window of L0

        // L0 goes into the canonical L1, a deeper level into the one below.
        let next_level_idx = if level_idx_with_highest_score == 0 {
            canonical_l1_idx
        } else {
            level_idx_with_highest_score + 1
        };
        // NOTE: Level count is 255 max
        #[expect(clippy::cast_possible_truncation)]
        let (next_level_index, canonical_level) =
            (next_level_idx as u8, (next_level_idx - level_shift) as u8);

        let Some(level) = version.level(level_idx_with_highest_score) else {
            return Choice::DoNothing;
        };

        let Some(next_level) = version.level(next_level_idx) else {
            return Choice::DoNothing;
        };

        let policy = self.policy(level_idx_with_highest_score, level_shift, last);
        let next_tiered = self.policy(next_level_idx, level_shift, last).layout == Layout::Tiered;
        // A window into a tiered level below goes down as a run of its own,
        // nothing of that level taken in: the window of this one that moves
        // as it is, when one does.
        let tiered_window = if policy.granularity == Granularity::Window && next_tiered {
            match window_over(
                level,
                state.hidden_set(),
                overshoot_bytes,
                policy.movement,
                self.target_size,
            ) {
                Some(ids) => {
                    let Some(ids) = with_overlapping_runs(level, ids, state.hidden_set(), cmp)
                    else {
                        return Choice::DoNothing;
                    };
                    Some(ids)
                }
                None => None,
            }
        } else {
            None
        };
        let (table_ids, can_trivial_move) = match (tiered_window, policy.granularity) {
            // Moved only where nothing below overlaps it, which a move may not
            // pass under; merged into a run of its own otherwise.
            (Some(ids), _) => {
                if version.level_is_busy(next_level_idx, state.hidden_set()) {
                    return Choice::DoNothing;
                }
                let overlaps = level
                    .iter()
                    .flat_map(|run| run.iter())
                    .filter(|table| ids.contains(&table.id()))
                    .any(|table| {
                        next_level
                            .iter()
                            .any(|run| !run.get_overlapping_cmp(table.key_range(), cmp).is_empty())
                    });
                (ids, !overlaps)
            }
            // A window of a leveled level below: pick_minimal_compaction scans
            // all runs in both levels, handling transient multi-run states from
            // multi-level compaction (#108, #122).
            (None, Granularity::Window) if !next_tiered => {
                let Some(picked) = pick_minimal_compaction(
                    level,
                    next_level,
                    state.hidden_set(),
                    overshoot_bytes,
                    self.target_size,
                    policy.movement,
                    cmp,
                ) else {
                    return Choice::DoNothing;
                };
                picked
            }
            // The whole level, with what it overlaps of a leveled level
            // below; into a tiered one it goes as a run of its own, which
            // joins a run there it overlaps nothing of: the shape trigger
            // counts runs as a read meets them. A window
            // into a tiered level comes here only when no part of this one
            // moves as it is.
            (None, Granularity::Window | Granularity::Level { .. }) => {
                if version.level_is_busy(level_idx_with_highest_score, state.hidden_set())
                    || version.level_is_busy(next_level_idx, state.hidden_set())
                {
                    return Choice::DoNothing;
                }
                // The level below already past its target goes on down with
                // this one in the same merge, as L0 does into a full L1,
                // rather than being rewritten here and again when it is
                // chosen.
                if policy.granularity
                    == (Granularity::Level {
                        through_full_next: true,
                    })
                    && !next_tiered
                    && scores
                        .get(next_level_idx)
                        .is_some_and(|(score, _)| *score > 1.0)
                    && let Some(below) = version.level(next_level_idx + 1)
                    && !version.level_is_busy(next_level_idx + 1, state.hidden_set())
                    // An empty level below takes the next one as a trivial
                    // move instead, which rewrites nothing.
                    && !below.is_empty()
                {
                    let mut ids = level.list_ids();
                    ids.extend(next_level.list_ids());
                    let mut ranges: Vec<_> = level
                        .iter()
                        .chain(next_level.iter())
                        .flat_map(|run| run.iter())
                        .map(|table| table.key_range().clone())
                        .collect();
                    ranges.sort_by(|a, b| cmp.compare(a.min(), b.min()));
                    for interval in &crate::KeyRange::merge_sorted_cmp(ranges, cmp) {
                        for run in below.iter() {
                            ids.extend(
                                run.get_overlapping_cmp(interval, cmp).iter().map(Table::id),
                            );
                        }
                    }
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "level index is bounded by level count (7, technically 255)"
                    )]
                    return Choice::Merge(CompactionInput {
                        table_ids: ids,
                        dest_level: (next_level_idx + 1) as u8,
                        canonical_level: canonical_level + 1,
                        target_size: self.target_size,
                    });
                }
                let key_range = level.aggregate_key_range_cmp(cmp);
                let overlapping: Vec<TableId> = next_level
                    .iter()
                    .flat_map(|run| run.get_overlapping_cmp(&key_range, cmp))
                    .map(Table::id)
                    .collect();
                let mut ids = level.list_ids();
                if !next_tiered {
                    ids.extend(&overlapping);
                }
                (ids, overlapping.is_empty())
            }
        };

        let choice = CompactionInput {
            table_ids,
            dest_level: next_level_index,
            canonical_level,
            target_size: self.target_size,
        };

        if can_trivial_move && level.is_disjoint() {
            return Choice::Move(choice);
        }
        Choice::Merge(choice)
    }
}

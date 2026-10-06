// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::{Choice, CompactionStrategy, Input as CompactionInput};
use crate::{
    KvPair, TableId, compaction::state::CompactionState, config::Config, table::Table,
    version::Version,
};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[cfg(test)]
mod tests;

#[doc(hidden)]
pub const NAME: &str = "SizeTieredCompaction";

/// Size-tiered compaction strategy (STCS), also known as Universal compaction.
///
/// All sorted runs live in L0. When enough similarly-sized runs accumulate,
/// they are merged into a single larger run (still in L0). This minimizes
/// write amplification at the cost of higher read and space amplification.
///
/// Best for write-heavy workloads: posting list merges, counters, time-series
/// append-only data.
///
/// # Algorithm
///
/// Only runs that are contiguous in L0 order (newest to oldest), and that no
/// run ahead of them overlaps, are merged together, so the merged run is older
/// than every run in front of it that shares a key with it and newer than
/// every run behind it.
///
/// 1. **Space amplification check:** if `total_size / largest_run_size - 1`
///    exceeds [`max_space_amplification_percent`](Strategy::with_max_space_amplification_percent),
///    all runs are merged (full compaction). While a compaction holds a run
///    between two others, this waits for it to land.
/// 2. **Size-ratio merge:** walking L0 from the newest run, the first stretch
///    of consecutive runs where each neighbouring pair satisfies
///    `larger / smaller <= 1.0 + size_ratio`, that no run ahead of it
///    overlaps, and that is at least
///    [`min_merge_width`](Strategy::with_min_merge_width) long is merged (its
///    newest [`max_merge_width`](Strategy::with_max_merge_width) runs). A
///    stretch a run ahead overlaps is cut short there, and the runs behind
///    are tried as stretches of their own.
///
/// # Trade-offs vs Leveled
///
/// | Metric | STCS | Leveled |
/// |--------|------|---------|
/// | Write amplification | ~O(N/T) | ~O(T×L) |
/// | Read amplification | Higher (more runs) | Lower (1 run per level) |
/// | Space amplification | Up to 2× temporary | ~1.1× |
#[derive(Clone)]
pub struct Strategy {
    /// Maximum allowed size ratio between adjacent sorted runs (by size) for
    /// them to be considered "similar" and eligible for merging together.
    ///
    /// For two adjacent runs sorted by size, if `larger / smaller <= 1.0 + size_ratio`,
    /// they are considered similar.
    ///
    /// Default = 1.0 (adjacent run can be up to 2× the previous).
    size_ratio: f64,

    /// Minimum number of similarly-sized sorted runs required before
    /// triggering a merge.
    ///
    /// Default = 4.
    min_merge_width: usize,

    /// Maximum number of sorted runs to merge at once.
    ///
    /// Default = `usize::MAX` (unlimited).
    max_merge_width: usize,

    /// When space amplification exceeds this percentage, a full compaction
    /// of all runs is triggered.
    ///
    /// Space amplification is computed as `(total_size / largest_run_size - 1) × 100`.
    ///
    /// Default = 200 (i.e. 200%, meaning total data can be up to 3× the largest run).
    max_space_amplification_percent: u64,

    /// Target table size on disk (possibly compressed) for output tables.
    ///
    /// Default = 64 MiB.
    target_size: u64,
}

impl Default for Strategy {
    fn default() -> Self {
        Self {
            size_ratio: 1.0,
            min_merge_width: 4,
            max_merge_width: usize::MAX,
            max_space_amplification_percent: 200,
            target_size: 64 * 1_024 * 1_024,
        }
    }
}

impl Strategy {
    /// Sets the size ratio threshold for considering runs "similar".
    ///
    /// Two adjacent runs (sorted by size) are similar if
    /// `larger / smaller <= 1.0 + size_ratio`.
    ///
    /// Same as `compaction_options_universal.size_ratio` in `RocksDB`.
    ///
    /// Default = 1.0
    #[must_use]
    pub fn with_size_ratio(mut self, ratio: f64) -> Self {
        // Clamp invalid values: NaN, negative, and infinite are replaced
        // with the default (1.0). Zero is allowed (exact-size-match only).
        self.size_ratio = if ratio.is_finite() && ratio >= 0.0 {
            ratio
        } else {
            1.0
        };
        self
    }

    /// Sets the minimum number of runs to merge at once.
    ///
    /// Same as `compaction_options_universal.min_merge_width` in `RocksDB`.
    ///
    /// Default = 4
    #[must_use]
    pub fn with_min_merge_width(mut self, width: usize) -> Self {
        self.min_merge_width = width.max(2);
        self
    }

    /// Sets the maximum number of runs to merge at once.
    ///
    /// Same as `compaction_options_universal.max_merge_width` in `RocksDB`.
    ///
    /// Default = `usize::MAX`
    #[must_use]
    pub fn with_max_merge_width(mut self, width: usize) -> Self {
        self.max_merge_width = width.max(2);
        self
    }

    /// Sets the space amplification threshold (in percent) that triggers
    /// a full compaction of all runs.
    ///
    /// Same as `compaction_options_universal.max_size_amplification_percent` in `RocksDB`.
    ///
    /// Default = 200
    #[must_use]
    pub fn with_max_space_amplification_percent(mut self, percent: u64) -> Self {
        self.max_space_amplification_percent = percent;
        self
    }

    /// Sets the target table size on disk (possibly compressed).
    ///
    /// Default = 64 MiB
    #[must_use]
    pub fn with_table_target_size(mut self, bytes: u64) -> Self {
        self.target_size = bytes;
        self
    }
}

/// Per-run metadata for compaction decisions.
struct RunInfo {
    /// Total on-disk size of all tables in this run.
    size: u64,

    /// Table IDs belonging to this run.
    table_ids: Vec<TableId>,
}

/// Collects L0 runs in L0 order (newest first), `None` for a run with a
/// table in the hidden set (being compacted).
fn collect_runs(version: &Version, state: &CompactionState) -> Vec<Option<RunInfo>> {
    version
        .l0()
        .iter()
        .map(|run| {
            if run
                .iter()
                .any(|table| state.hidden_set().is_hidden(table.id()))
            {
                return None;
            }

            let size = run.iter().map(Table::file_size).sum::<u64>();
            let table_ids = run.iter().map(Table::id).collect();

            Some(RunInfo { size, table_ids })
        })
        .collect()
}

/// Whether two runs neighbouring in L0 are close enough in size to merge.
fn similar(a: &RunInfo, b: &RunInfo, size_ratio: f64) -> bool {
    let (smaller, larger) = if a.size <= b.size {
        (a.size, b.size)
    } else {
        (b.size, a.size)
    };
    if smaller == 0 {
        // A zero-size run is similar to anything.
        return true;
    }
    #[expect(
        clippy::cast_precision_loss,
        reason = "precision loss is acceptable for ratio comparison"
    )]
    let ratio = larger as f64 / smaller as f64;
    ratio <= 1.0 + size_ratio
}

/// For each L0 run, the frontmost run ahead of it (L0 is newest first, so the
/// lowest index) that holds a table overlapping one of its tables, if any.
/// Computed once per choice: every candidate stretch is then checked against
/// it without looking at a table again.
fn frontmost_overlap_ahead(
    version: &Version,
    cmp: &dyn crate::comparator::UserComparator,
) -> Vec<Option<usize>> {
    let l0 = version.l0();
    l0.iter()
        .enumerate()
        .map(|(at, run)| {
            l0.iter().take(at).position(|ahead| {
                ahead.iter().any(|table| {
                    !run.get_overlapping_cmp(&table.metadata.key_range, cmp)
                        .is_empty()
                })
            })
        })
        .collect()
}

/// Whether L0 run `run` may be merged in a stretch that starts at run `start`:
/// no run ahead of the stretch overlaps it. A table ahead that overlaps a
/// merged one is newer than the data the two share, so the output, which
/// takes that data, would belong behind it while belonging ahead of the older
/// tables behind its inputs, and the output's single recency cannot say both.
/// This is Pebble's rule for intra-L0 compactions: a merge that takes an older
/// version of a key takes every newer version of it L0 holds.
fn clear_of_runs_ahead(ahead: &[Option<usize>], run: usize, start: usize) -> bool {
    ahead
        .get(run)
        .copied()
        .flatten()
        .is_none_or(|overlap| overlap >= start)
}

fn merge_runs<'a>(runs: impl Iterator<Item = &'a RunInfo>, target_size: u64) -> Choice {
    Choice::Merge(CompactionInput {
        table_ids: runs.flat_map(|r| r.table_ids.iter().copied()).collect(),
        dest_level: 0,
        canonical_level: 0,
        target_size,
    })
}

impl CompactionStrategy for Strategy {
    fn get_name(&self) -> &'static str {
        NAME
    }

    fn get_config(&self) -> Vec<KvPair> {
        use crate::io::{LittleEndian, WriteBytesExt};

        let mut size_ratio_bytes = vec![];
        #[expect(clippy::expect_used, reason = "writing into Vec should not fail")]
        size_ratio_bytes
            .write_f64::<LittleEndian>(self.size_ratio)
            .expect("cannot fail");

        vec![
            (
                crate::UserKey::from("tiered_size_ratio"),
                crate::UserValue::from(size_ratio_bytes),
            ),
            (
                crate::UserKey::from("tiered_min_merge_width"),
                crate::UserValue::from(
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "min_merge_width fits in u32 for persistence; usize::MAX maps to u32::MAX"
                    )]
                    (self.min_merge_width.min(u32::MAX as usize) as u32).to_le_bytes(),
                ),
            ),
            (
                crate::UserKey::from("tiered_max_merge_width"),
                crate::UserValue::from(
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "max_merge_width fits in u32 for persistence; usize::MAX maps to u32::MAX"
                    )]
                    (self.max_merge_width.min(u32::MAX as usize) as u32).to_le_bytes(),
                ),
            ),
            (
                crate::UserKey::from("tiered_max_space_amp_pct"),
                crate::UserValue::from(self.max_space_amplification_percent.to_le_bytes()),
            ),
            (
                crate::UserKey::from("tiered_target_size"),
                crate::UserValue::from(self.target_size.to_le_bytes()),
            ),
        ]
    }

    fn pending_compaction_bytes(&self, version: &Version) -> u64 {
        // Tiered debt mirrors the space-amplification trigger in `choose`: the
        // bytes by which total L0 size exceeds the budget
        // `largest_run * (1 + max_space_amplification_percent / 100)`. This makes
        // the bytes-axis backpressure (bytes_slowdown / bytes_stop) engage for
        // size-tiered trees, not only leveled. All runs are counted, including
        // any already being compacted: the redundancy is on disk until the full
        // compaction lands. Zero with fewer than two runs (nothing to reclaim).
        let l0 = version.l0();
        let mut total: u128 = 0;
        let mut largest: u128 = 0;
        let mut run_count = 0usize;
        for run in l0.iter() {
            let size: u128 = run.iter().map(|t| u128::from(Table::file_size(t))).sum();
            total += size;
            largest = largest.max(size);
            run_count += 1;
        }
        if run_count < 2 || largest == 0 {
            return 0;
        }
        // Same *100-scaled comparison as `choose` (avoids f64 precision loss);
        // `rhs` saturates so an effectively unbounded threshold yields zero debt.
        let lhs = total * 100;
        let rhs = largest.saturating_mul(100 + u128::from(self.max_space_amplification_percent));
        // saturating_sub: debt floors at zero by definition (none within budget).
        let debt = lhs.saturating_sub(rhs) / 100;
        u64::try_from(debt).unwrap_or(u64::MAX)
    }

    fn choose(&self, version: &Version, config: &Config, state: &CompactionState) -> Choice {
        let cmp = config.comparator.as_ref();
        let all_runs = collect_runs(version, state);
        let runs: Vec<&RunInfo> = all_runs.iter().flatten().collect();

        if runs.len() < 2 {
            return Choice::DoNothing;
        }

        let ahead = frontmost_overlap_ahead(version, cmp);

        // The available runs can merge as one unless a busy run sits between
        // two of them, or a busy run ahead of them overlaps one.
        let first_available = all_runs.iter().position(Option::is_some);
        let last_available = all_runs.iter().rposition(Option::is_some);
        let available_contiguous = match (first_available, last_available) {
            (Some(first), Some(last)) => all_runs.get(first..=last).is_some_and(|span| {
                span.iter().all(Option::is_some)
                    && (first..=last).all(|run| clear_of_runs_ahead(&ahead, run, first))
            }),
            _ => false,
        };

        // --- Space amplification check ---
        //
        // The largest run is treated as the "base" data set. Everything else
        // is overhead. If overhead exceeds the threshold, compact everything.
        let total_size: u64 = runs.iter().map(|r| r.size).sum();
        let largest_run_size = runs.iter().map(|r| r.size).max().unwrap_or(0);

        // A busy run between available ones would split the merge into two
        // ranges of different age; wait for that compaction to land instead.
        if largest_run_size > 0 && available_contiguous {
            // Integer arithmetic to avoid f64 precision loss on large sizes.
            //   (total / largest - 1) * 100 >= threshold
            // is equivalent to:
            //   total * 100 >= largest * (100 + threshold)
            // `lhs` multiplies a u64-derived u128 by the constant 100, which
            // cannot overflow u128 — plain multiply. `rhs` multiplies by
            // `100 + max_space_amplification_percent`, and the percentage is an
            // unvalidated u64 (tests pass u64::MAX), so that product CAN exceed
            // u128 — saturate. When `rhs` saturates to u128::MAX the `lhs >= rhs`
            // check below cannot fire, so a huge amplification bound simply never
            // triggers a space-amplification compaction, matching the intent of a
            // very high threshold (tolerate maximum amplification).
            let lhs = u128::from(total_size) * 100;
            let rhs = u128::from(largest_run_size)
                .saturating_mul(100 + u128::from(self.max_space_amplification_percent));

            if lhs >= rhs {
                return merge_runs(runs.iter().copied(), self.target_size);
            }
        }

        // --- Size-ratio triggered merge ---
        //
        // Walk L0 from the newest run and take the first stretch of
        // consecutive available runs whose neighbours have similar sizes and
        // that no run ahead of it overlaps, as `RocksDB` universal compaction
        // picks from its newest sorted run
        // (`UniversalCompactionBuilder::PickCompactionToReduceSortedRuns`).
        // Merging by size alone could join runs around a differently sized one
        // between them in age.
        // Cap at max_merge_width, but still meet min_merge_width (guards
        // against a misconfigured max < min).
        let merge_count_for = |stretch: usize| {
            let count = stretch.min(self.max_merge_width);
            (count >= self.min_merge_width).then_some(count)
        };
        let mut start = 0;
        while start < all_runs.len() {
            let mut len = 0;
            if let Some(Some(first)) = all_runs.get(start)
                && clear_of_runs_ahead(&ahead, start, start)
            {
                // The stretch ends at the first run that is busy, differs in
                // size from its neighbour, or is overlapped by a run ahead of
                // `start`: a run the stretch's own runs overlap stays mergeable
                // with them.
                let mut prev = first;
                len = 1;
                while let Some(Some(next)) = all_runs.get(start + len)
                    && similar(prev, next, self.size_ratio)
                    && clear_of_runs_ahead(&ahead, start + len, start)
                {
                    prev = next;
                    len += 1;
                }
                if let Some(count) = merge_count_for(len) {
                    return merge_runs(
                        all_runs.iter().skip(start).take(count).flatten(),
                        self.target_size,
                    );
                }
            }
            // A stretch starting inside this one ends where this one ended,
            // or sooner, so it is shorter still. A run that could not start a
            // stretch is passed over alone: the run behind it may.
            start += len.max(1);
        }

        Choice::DoNothing
    }
}

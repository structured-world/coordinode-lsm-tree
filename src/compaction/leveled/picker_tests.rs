// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The picker prices each window in constant time and must choose what pricing
//! every window from scratch chooses: the reference below does that, window by
//! window, and the two are compared on randomized level layouts.

use super::*;
use crate::{
    AbstractTree, Config, SequenceNumberCounter,
    slice_windows::{GrowingWindowsExt, ShrinkingWindowsExt},
};
use std::sync::Arc;
use test_log::test;

/// The picker as it reads: every window's overlap, pull-in, sizes and hidden
/// tables found anew.
fn reference_pick(
    curr_level: &Level,
    next_level: &Level,
    hidden_set: &HiddenSet,
    overshoot: u64,
    table_base_size: u64,
    promotion_slack: u64,
    cmp: &dyn crate::comparator::UserComparator,
) -> Option<(HashSet<TableId>, bool)> {
    for curr_run in curr_level.iter() {
        if let Some(window) = curr_run.shrinking_windows().find(|window| {
            if hidden_set.is_blocked(window.iter().map(Table::id)) {
                return false;
            }
            if next_level.is_empty() {
                return true;
            }
            let key_range = aggregate_run_key_range(window);
            next_level
                .iter()
                .all(|run| run.get_overlapping_cmp(&key_range, cmp).is_empty())
        }) {
            return Some((window.iter().map(Table::id).collect(), true));
        }
    }
    if next_level.is_empty() {
        return None;
    }
    let pull_in = |window: &[Table]| {
        let key_range = aggregate_run_key_range(window);
        curr_level
            .iter()
            .flat_map(|run| run.get_contained_cmp(&key_range, cmp))
            .collect::<Vec<&Table>>()
    };
    let mut ranking = MergeRanking::new(overshoot, promotion_slack);
    let windows = next_level.iter().flat_map(|run| {
        run.growing_windows().take_while(|window| {
            window.iter().map(Table::file_size).sum::<u64>() <= 50 * table_base_size
        })
    });
    for window in windows {
        if hidden_set.is_blocked(window.iter().map(Table::id)) {
            continue;
        }
        let pulled = pull_in(window);
        let promoted = pulled.iter().map(|t| t.file_size()).sum::<u64>();
        if promoted == 0 || hidden_set.is_blocked(pulled.iter().map(|t| t.id())) {
            continue;
        }
        let next_level_size = window.iter().map(Table::file_size).sum::<u64>();
        ranking.offer(
            MergeCost {
                promoted,
                total: promoted + next_level_size,
            },
            window,
        );
    }
    let ((_, _, window), _) = ranking.finish()?;
    let mut ids: HashSet<_> = window.iter().map(Table::id).collect();
    ids.extend(pull_in(window).iter().map(|t| t.id()));
    Some((ids, false))
}

/// A deterministic stream of numbers for the layouts.
struct Draws(u64);

impl Draws {
    fn next(&mut self) -> u64 {
        // splitmix64
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

/// Fills levels 1 to 3 of a fresh tree, and level 0 with one run, with flushes
/// over random key ranges and value sizes; a level moved into more than once
/// may hold several runs.
fn random_layout(dir: &std::path::Path, draws: &mut Draws) -> crate::Result<crate::AnyTree> {
    let tree = Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .table_target_size(4 * 1_024)
    .data_block_compression_policy(crate::config::CompressionPolicy::disabled())
    .open()?;
    let mut seqno = 1;
    for level in [3u8, 2, 1, 0] {
        for _ in 0..=draws.below(3) {
            let start = draws.below(2_000);
            let keys = 20 + draws.below(400);
            let step = 1 + draws.below(4);
            for i in 0..keys {
                let key = format!("k{:06}", start + i * step);
                let value_len = 50 + draws.below(400);
                tree.insert(key, vec![b'v'; usize::try_from(value_len).unwrap()], seqno);
                seqno += 1;
            }
            tree.flush_active_memtable(0)?;
            if level > 0 {
                tree.compact(Arc::new(crate::compaction::MoveDown(0, level)), 0)?;
            }
        }
    }
    Ok(tree)
}

/// On random layouts of up to four levels, with random hidden tables, targets
/// and overshoots, the picker chooses what the window-by-window reference
/// chooses: the same tables, and a move where the reference finds one.
#[test]
fn the_picker_chooses_what_pricing_every_window_anew_chooses() -> crate::Result<()> {
    let cmp = crate::comparator::DefaultUserComparator;
    // Moves, merges, and choices of nothing.
    let mut outcomes = [0u32; 3];
    for seed in 0..12 {
        let mut draws = Draws(seed);
        let dir = tempfile::tempdir()?;
        let tree = random_layout(dir.path(), &mut draws)?;
        let version = tree.current_version();
        let empty = Level::empty();
        let level = |index: usize| version.level(index).unwrap_or(&empty);
        let ids: Vec<TableId> = version.iter_tables().map(Table::id).collect();
        for curr in 0..4 {
            for round in 0..6 {
                let mut hidden_set = HiddenSet::default();
                // Rounds 0 and 1 hide nothing; later ones hide up to a third.
                if round >= 2 {
                    let share = 1 + draws.below(3);
                    hidden_set.hide(ids.iter().copied().filter(|_| draws.below(10) < share));
                }
                let base = [1_024, 4 * 1_024, 16 * 1_024, 64 * 1_024 * 1_024]
                    [usize::try_from(draws.below(4)).unwrap()];
                let overshoot = [0, u64::MAX, draws.below(200 * 1_024)]
                    [usize::try_from(draws.below(3)).unwrap()];
                let slack = draws.below(64 * 1_024);
                let (curr_level, next_level) = (level(curr), level(curr + 1));
                let chosen = pick_minimal_compaction(
                    curr_level,
                    next_level,
                    &hidden_set,
                    overshoot,
                    base,
                    slack,
                    &cmp,
                );
                assert_eq!(
                    chosen,
                    reference_pick(
                        curr_level,
                        next_level,
                        &hidden_set,
                        overshoot,
                        base,
                        slack,
                        &cmp
                    ),
                    "seed {seed}, L{curr} into L{}, round {round}",
                    curr + 1,
                );
                match chosen {
                    Some((_, true)) => outcomes[0] += 1,
                    Some((_, false)) => outcomes[1] += 1,
                    None => outcomes[2] += 1,
                }
            }
        }
    }
    // Both kinds of choice were compared, moves and merges.
    assert!(outcomes[0] > 0 && outcomes[1] > 0, "{outcomes:?}");
    Ok(())
}

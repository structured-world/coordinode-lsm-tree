// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The four choices set apart from the leveled ones.

use super::*;
use crate::compaction::MoveDown;
use crate::{AbstractTree, Config, SeqNo, SequenceNumberCounter};
use std::sync::Arc;
use test_log::test;

fn tree() -> crate::Result<(tempfile::TempDir, crate::AnyTree)> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    Ok((dir, tree))
}

/// Writes keys `0..100` at round `round` and flushes them into one table.
fn write_round(tree: &crate::AnyTree, round: u64) -> crate::Result<()> {
    for i in 0..100u64 {
        tree.insert(format!("k{i:03}"), format!("v{round}"), round * 100 + i);
    }
    tree.flush_active_memtable(0)?;
    Ok(())
}

/// Compacts with `strategy` until it has nothing to do.
fn settle(tree: &crate::AnyTree, strategy: &Arc<Strategy>) -> crate::Result<()> {
    loop {
        let strategy: Arc<dyn CompactionStrategy> = strategy.clone();
        if tree.compact(strategy, 0)?.action == crate::compaction::CompactionAction::Nothing {
            return Ok(());
        }
    }
}

/// A tiered L1 takes each merge out of L0 as a run of its own, without
/// rewriting what it holds, and goes down whole once its runs reach their
/// count; every key reads its newest value throughout.
#[test]
fn a_tiered_l1_accumulates_runs_then_goes_down_whole() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    let strategy = Arc::new(Strategy::default().with_l0_threshold(1).with_level_policy(
        1,
        LevelPolicy {
            trigger: Trigger::Shape {
                runs: 3,
                tables: u16::MAX,
            },
            layout: Layout::Tiered,
            granularity: Granularity::Level {
                through_full_next: false,
            },
            movement: Movement::LeastRewrite { slack: None },
        },
    ));
    write_round(&tree, 0)?;
    tree.compact(Arc::new(MoveDown(0, 1)), 0)?;

    write_round(&tree, 1)?;
    settle(&tree, &strategy)?;
    let version = tree.current_version();
    let l1 = version.level(1).expect("L1");
    assert_eq!(l1.run_count(), 2, "the merge out of L0 is a run of its own");
    assert!(version.level(2).expect("L2").is_empty());

    write_round(&tree, 2)?;
    settle(&tree, &strategy)?;
    let version = tree.current_version();
    assert!(
        version.level(1).expect("L1").is_empty(),
        "three runs send L1 down whole"
    );
    assert_eq!(version.level(2).expect("L2").run_count(), 1);
    for i in 0..100u64 {
        assert_eq!(
            tree.get(format!("k{i:03}"), SeqNo::MAX)?.as_deref(),
            Some(b"v2".as_slice()),
            "key {i}",
        );
    }
    Ok(())
}

/// Merges exactly the L0 tables into a level, as a run of their own beside
/// what the level holds.
struct MergeL0(u8);

impl CompactionStrategy for MergeL0 {
    fn get_name(&self) -> &'static str {
        "MergeL0Test"
    }

    fn choose(&self, version: &Version, _: &Config, _: &CompactionState) -> Choice {
        Choice::Merge(CompactionInput {
            table_ids: version.l0().list_ids(),
            dest_level: self.0,
            canonical_level: self.0,
            target_size: u64::MAX,
        })
    }
}

/// A deeper level compacted whole, set to go through a full level below,
/// takes that level on down with it in one merge, as L0 does into a full
/// L1, instead of rewriting it now and again when it is chosen.
#[test]
fn a_whole_level_goes_through_a_full_level_below() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    let shape = |granularity| LevelPolicy {
        trigger: Trigger::Shape {
            runs: 1,
            tables: u16::MAX,
        },
        layout: Layout::Leveled,
        granularity,
        movement: Movement::LeastRewrite { slack: None },
    };
    let strategy = Strategy::default()
        .with_level_policy(
            1,
            shape(Granularity::Level {
                through_full_next: true,
            }),
        )
        .with_level_policy(2, shape(Granularity::Window));
    // L2 two runs (past its one), L1 three (further past): L1 is chosen.
    for (round, level) in [(0, 2), (1, 2), (2, 1), (3, 1), (4, 1)] {
        write_round(&tree, round)?;
        tree.compact(Arc::new(MergeL0(level)), 0)?;
    }
    let version = tree.current_version();
    assert_eq!(version.level(1).expect("L1").run_count(), 3);
    assert_eq!(version.level(2).expect("L2").run_count(), 2);

    let Choice::Merge(input) =
        strategy.choose(&version, tree.tree_config(), &CompactionState::default())
    else {
        panic!("a merge is chosen");
    };
    assert_eq!(input.dest_level, 3, "through L2 into L3");
    let mut expected = version.level(1).expect("L1").list_ids();
    expected.extend(version.level(2).expect("L2").list_ids());
    assert_eq!(input.table_ids, expected, "every table of L1 and L2");
    Ok(())
}

/// A level set never to trigger starts no compaction however far past its
/// target it grows.
#[test]
fn a_level_set_never_to_trigger_starts_no_compaction() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    let never = LevelPolicy {
        trigger: Trigger::Never,
        layout: Layout::Leveled,
        granularity: Granularity::Window,
        movement: Movement::LeastRewrite { slack: None },
    };
    // A level target of a single table: one round already overshoots it.
    let strategy = Strategy::default()
        .with_table_target_size(64)
        .with_l0_threshold(1);
    write_round(&tree, 0)?;
    tree.compact(Arc::new(MoveDown(0, 1)), 0)?;
    let version = tree.current_version();
    let choice = strategy.clone().with_level_policy(1, never).choose(
        &version,
        tree.tree_config(),
        &CompactionState::default(),
    );
    assert!(
        matches!(choice, Choice::DoNothing),
        "nothing triggers: {choice:?}"
    );
    assert!(
        !matches!(
            strategy.choose(&version, tree.tree_config(), &CompactionState::default()),
            Choice::DoNothing
        ),
        "the leveled L1 is past its target",
    );
    Ok(())
}

/// The hottest movement takes the window its reads touch most per byte it
/// rewrites, where the leveled one takes the cheapest per promoted byte.
#[test]
fn the_hottest_movement_prefers_reads_to_a_cheap_rewrite() {
    let cheap_cold = MergeCost {
        promoted: 100,
        total: 150,
        heat: 0,
    };
    let dear_hot = MergeCost {
        promoted: 100,
        total: 1_000,
        heat: 5_000,
    };
    let pick = |movement| {
        let mut ranking = MergeRanking::new(u64::MAX, 0, movement);
        ranking.offer(cheap_cold, 0);
        ranking.offer(dear_hot, 1);
        ranking.finish().map(|((_, _, index), _)| index)
    };
    assert_eq!(pick(Movement::LeastRewrite { slack: None }), Some(0));
    assert_eq!(pick(Movement::Hottest), Some(1));

    // Equally hot per byte, the cheaper rewrite wins.
    let tie = MergeCost {
        promoted: 200,
        total: 1_000,
        heat: 5_000,
    };
    let mut ranking = MergeRanking::new(u64::MAX, 0, Movement::Hottest);
    ranking.offer(dear_hot, 0);
    ranking.offer(tie, 1);
    assert_eq!(ranking.finish().map(|((_, _, index), _)| index), Some(1));
}

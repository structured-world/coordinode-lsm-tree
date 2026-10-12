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

/// L1 three runs and L2 two, both past their one, L1 further; L3 holds a run
/// first when `with_l3`. L1 goes whole and through a full level below.
fn full_l1_over_full_l2(
    with_l3: bool,
) -> crate::Result<(tempfile::TempDir, crate::AnyTree, Strategy)> {
    let (dir, tree) = tree()?;
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
    let below = with_l3.then_some((5, 3));
    for (round, level) in below
        .into_iter()
        .chain([(0, 2), (1, 2), (2, 1), (3, 1), (4, 1)])
    {
        write_round(&tree, round)?;
        tree.compact(Arc::new(MergeL0(level)), 0)?;
    }
    let version = tree.current_version();
    assert_eq!(version.level(1).expect("L1").run_count(), 3);
    assert_eq!(version.level(2).expect("L2").run_count(), 2);
    Ok((dir, tree, strategy))
}

/// A deeper level compacted whole, set to go through a full level below,
/// takes that level on down with it in one merge, as L0 does into a full
/// L1, instead of rewriting it now and again when it is chosen.
#[test]
fn a_whole_level_goes_through_a_full_level_below() -> crate::Result<()> {
    let (_dir, tree, strategy) = full_l1_over_full_l2(true)?;
    let version = tree.current_version();
    let Choice::Merge(input) =
        strategy.choose(&version, tree.tree_config(), &CompactionState::default())
    else {
        panic!("a merge is chosen");
    };
    assert_eq!(input.dest_level, 3, "through L2 into L3");
    let mut expected = version.level(1).expect("L1").list_ids();
    expected.extend(version.level(2).expect("L2").list_ids());
    expected.extend(version.level(3).expect("L3").list_ids());
    assert_eq!(
        input.table_ids, expected,
        "every table of L1 and L2, and what they overlap of L3"
    );
    Ok(())
}

/// Over an empty level below, the full level is not rewritten on its way
/// through: it goes there as a trivial move of its own, and the level above
/// merges into it alone.
#[test]
fn a_whole_level_does_not_go_through_into_an_empty_level() -> crate::Result<()> {
    let (_dir, tree, strategy) = full_l1_over_full_l2(false)?;
    let version = tree.current_version();
    let Choice::Merge(input) =
        strategy.choose(&version, tree.tree_config(), &CompactionState::default())
    else {
        panic!("a merge is chosen");
    };
    assert_eq!(input.dest_level, 2, "L1 into L2 alone");
    Ok(())
}

/// Writes keys `from..from + 100` and flushes them into one table.
fn write_keys(tree: &crate::AnyTree, from: u64) -> crate::Result<()> {
    for i in from..from + 100 {
        tree.insert(format!("k{i:04}"), "v", i);
    }
    tree.flush_active_memtable(0)?;
    Ok(())
}

/// L0 set never to trigger starts no compaction, not even the free trivial
/// move into L1 the leveled strategy otherwise takes.
#[test]
fn an_l0_set_never_to_trigger_makes_no_trivial_move() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    write_keys(&tree, 0)?;
    tree.compact(Arc::new(MoveDown(0, 1)), 0)?;
    write_keys(&tree, 1_000)?;
    let version = tree.current_version();
    let never = LevelPolicy {
        trigger: Trigger::Never,
        layout: Layout::Leveled,
        granularity: Granularity::Level {
            through_full_next: false,
        },
        movement: Movement::LeastRewrite { slack: None },
    };
    assert!(
        matches!(
            Strategy::default().choose(&version, tree.tree_config(), &CompactionState::default()),
            Choice::Move(_)
        ),
        "the leveled L0 moves its disjoint run down",
    );
    let choice = Strategy::default().with_level_policy(0, never).choose(
        &version,
        tree.tree_config(),
        &CompactionState::default(),
    );
    assert!(matches!(choice, Choice::DoNothing), "{choice:?}");
    Ok(())
}

/// A window into a tiered level below is a window of this level: what is past
/// its target, not the whole level, and nothing of the level below.
#[test]
fn a_window_into_a_tiered_level_takes_a_window() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    for from in [0, 1_000, 2_000] {
        write_keys(&tree, from)?;
        tree.compact(Arc::new(MoveDown(0, 1)), 0)?;
    }
    let version = tree.current_version();
    assert_eq!(version.level(1).expect("L1").table_count(), 3);
    let strategy = Strategy::default()
        .with_level_policy(
            1,
            LevelPolicy {
                trigger: Trigger::Shape {
                    runs: u8::MAX,
                    tables: 1,
                },
                layout: Layout::Leveled,
                granularity: Granularity::Window,
                movement: Movement::LeastRewrite { slack: None },
            },
        )
        .with_level_policy(
            2,
            LevelPolicy {
                trigger: Trigger::Never,
                layout: Layout::Tiered,
                granularity: Granularity::Level {
                    through_full_next: false,
                },
                movement: Movement::LeastRewrite { slack: None },
            },
        );
    let choice = strategy.choose(&version, tree.tree_config(), &CompactionState::default());
    let Choice::Move(input) = choice else {
        panic!("a move of a window: {choice:?}");
    };
    assert_eq!(input.dest_level, 2);
    assert_eq!(input.table_ids.len(), 1, "one table of three");
    Ok(())
}

/// A window into a tiered level, set to move the hottest data, takes the
/// table reads touch, not the first one.
#[test]
fn a_hottest_window_into_a_tiered_level_takes_the_read_table() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    for from in [0, 1_000, 2_000] {
        write_keys(&tree, from)?;
        tree.compact(Arc::new(MoveDown(0, 1)), 0)?;
    }
    for _ in 0..3 {
        for i in 2_000..2_100u64 {
            tree.get(format!("k{i:04}"), SeqNo::MAX)?;
        }
    }
    let version = tree.current_version();
    let hot = version
        .level(1)
        .expect("L1")
        .iter()
        .flat_map(|run| run.iter())
        .find(|table| table.key_range().min().as_ref() == b"k2000")
        .expect("the table of k2000..")
        .id();
    let strategy = Strategy::default()
        .with_level_policy(
            1,
            LevelPolicy {
                trigger: Trigger::Shape {
                    runs: u8::MAX,
                    tables: 1,
                },
                layout: Layout::Leveled,
                granularity: Granularity::Window,
                movement: Movement::Hottest,
            },
        )
        .with_level_policy(
            2,
            LevelPolicy {
                trigger: Trigger::Never,
                layout: Layout::Tiered,
                granularity: Granularity::Level {
                    through_full_next: false,
                },
                movement: Movement::LeastRewrite { slack: None },
            },
        );
    let choice = strategy.choose(&version, tree.tree_config(), &CompactionState::default());
    let Choice::Move(input) = choice else {
        panic!("a move of a window: {choice:?}");
    };
    assert_eq!(input.table_ids, core::iter::once(hot).collect());
    Ok(())
}

/// Writes keys `from..to` at `round` and flushes them into one table.
fn write_span(tree: &crate::AnyTree, (from, to): (u64, u64), round: u64) -> crate::Result<()> {
    for i in from..to {
        tree.insert(format!("k{i:04}"), format!("v{round}"), round * 1_000 + i);
    }
    tree.flush_active_memtable(0)?;
    Ok(())
}

/// A window taken out of a level of several runs takes every table of them
/// that shares a key with it: a newer narrow table goes down only with the
/// older wide one under it, else reads would stop on the older version left
/// above.
#[test]
fn a_window_of_several_runs_takes_the_older_versions_along() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    write_span(&tree, (0, 50), 0)?;
    write_span(&tree, (50, 100), 0)?;
    tree.compact(Arc::new(MoveDown(0, 1)), 0)?;
    write_span(&tree, (0, 100), 1)?;
    write_span(&tree, (10, 20), 2)?;
    let version = tree.current_version();
    let l0 = version.level(0).expect("L0");
    assert_eq!(l0.run_count(), 2);
    let id_of = |min: &[u8], max: &[u8]| {
        l0.iter()
            .flat_map(|run| run.iter())
            .find(|t| t.key_range().min().as_ref() == min && t.key_range().max().as_ref() == max)
            .expect("the table")
            .id()
    };
    let (wide, narrow) = (id_of(b"k0000", b"k0099"), id_of(b"k0010", b"k0019"));
    let narrow_size = l0
        .iter()
        .flat_map(|run| run.iter())
        .find(|t| t.id() == narrow)
        .expect("narrow")
        .file_size();

    // The bound admits only the narrow table promoted on its own.
    let (ids, _) = pick_minimal_compaction(
        l0,
        version.level(1).expect("L1"),
        &HiddenSet::default(),
        0,
        u64::MAX / 64,
        Movement::LeastRewrite {
            slack: Some(narrow_size),
        },
        &crate::comparator::DefaultUserComparator,
    )
    .expect("a merge");
    assert!(ids.contains(&narrow), "the narrow window is the cheapest");
    assert!(ids.contains(&wide), "the older wide version goes along");
    Ok(())
}

/// L1 of three disjoint tables, `k0000..`, `k1000..` and `k2000..`.
fn three_tables_in_l1(tree: &crate::AnyTree) -> crate::Result<()> {
    for from in [0, 1_000, 2_000] {
        write_keys(tree, from)?;
        tree.compact(Arc::new(MoveDown(0, 1)), 0)?;
    }
    Ok(())
}

/// L1 compacting a window as soon as it holds a table, into a tiered L2.
fn window_into_tiered_l2() -> Strategy {
    Strategy::default()
        .with_level_policy(
            1,
            LevelPolicy {
                trigger: Trigger::Shape {
                    runs: u8::MAX,
                    tables: 1,
                },
                layout: Layout::Leveled,
                granularity: Granularity::Window,
                movement: Movement::LeastRewrite { slack: None },
            },
        )
        .with_level_policy(2, tiered_never())
}

fn tiered_never() -> LevelPolicy {
    LevelPolicy {
        trigger: Trigger::Never,
        layout: Layout::Tiered,
        granularity: Granularity::Level {
            through_full_next: false,
        },
        movement: Movement::LeastRewrite { slack: None },
    }
}

/// A window into a tiered level that holds the same keys is merged into a run
/// of its own there: nothing of that level is taken in.
#[test]
fn a_window_over_a_tiered_level_holding_its_keys_is_merged_alone() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    write_keys(&tree, 0)?;
    tree.compact(Arc::new(MoveDown(0, 2)), 0)?;
    three_tables_in_l1(&tree)?;
    let version = tree.current_version();
    let choice =
        window_into_tiered_l2().choose(&version, tree.tree_config(), &CompactionState::default());
    let Choice::Merge(input) = choice else {
        panic!("a merge of a window: {choice:?}");
    };
    assert_eq!(input.dest_level, 2);
    let l1 = version.level(1).expect("L1").list_ids();
    assert!(
        input.table_ids.iter().all(|id| l1.contains(id)),
        "nothing of L2 taken in"
    );
    Ok(())
}

/// A window does not go into a tiered level being compacted, nor take a table
/// being compacted.
#[test]
fn a_window_into_a_tiered_level_waits_for_busy_tables() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    write_keys(&tree, 5_000)?;
    tree.compact(Arc::new(MoveDown(0, 2)), 0)?;
    three_tables_in_l1(&tree)?;
    let version = tree.current_version();
    let strategy = window_into_tiered_l2();

    let mut busy_below = CompactionState::default();
    busy_below
        .hidden_set_mut()
        .hide(version.level(2).expect("L2").list_ids());
    let choice = strategy.choose(&version, tree.tree_config(), &busy_below);
    assert!(matches!(choice, Choice::DoNothing), "{choice:?}");

    let first = version
        .level(1)
        .expect("L1")
        .iter()
        .flat_map(|run| run.iter())
        .map(Table::id)
        .next()
        .expect("a table");
    let mut busy_first = CompactionState::default();
    busy_first.hidden_set_mut().hide([first]);
    let choice = strategy.choose(&version, tree.tree_config(), &busy_first);
    assert!(
        match &choice {
            Choice::Move(input) | Choice::Merge(input) => !input.table_ids.contains(&first),
            Choice::DoNothing => true,
            Choice::Drop(_) => false,
        },
        "{choice:?}"
    );
    Ok(())
}

/// A whole level into a tiered level goes as a run of its own, taking in
/// nothing of what that level holds.
#[test]
fn a_whole_level_into_a_tiered_level_takes_nothing_of_it() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    write_keys(&tree, 0)?;
    tree.compact(Arc::new(MoveDown(0, 2)), 0)?;
    three_tables_in_l1(&tree)?;
    let version = tree.current_version();
    let strategy = Strategy::default()
        .with_level_policy(
            1,
            LevelPolicy {
                trigger: Trigger::Shape {
                    runs: u8::MAX,
                    tables: 1,
                },
                layout: Layout::Leveled,
                granularity: Granularity::Level {
                    through_full_next: false,
                },
                movement: Movement::LeastRewrite { slack: None },
            },
        )
        .with_level_policy(2, tiered_never());
    let choice = strategy.choose(&version, tree.tree_config(), &CompactionState::default());
    let Choice::Merge(input) = choice else {
        panic!("a merge of L1: {choice:?}");
    };
    assert_eq!(input.dest_level, 2);
    assert_eq!(input.table_ids, version.level(1).expect("L1").list_ids());
    Ok(())
}

/// L0 compacting a window goes into L1, by a window of what it overlaps there.
#[test]
fn a_window_of_l0_goes_into_l1() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    three_tables_in_l1(&tree)?;
    write_keys(&tree, 1_000)?;
    let version = tree.current_version();
    let strategy = Strategy::default().with_level_policy(
        0,
        LevelPolicy {
            trigger: Trigger::Shape { runs: 1, tables: 1 },
            layout: Layout::Tiered,
            granularity: Granularity::Window,
            movement: Movement::LeastRewrite { slack: None },
        },
    );
    let choice = strategy.choose(&version, tree.tree_config(), &CompactionState::default());
    let Choice::Merge(input) = choice else {
        panic!("a merge into L1: {choice:?}");
    };
    assert_eq!(input.dest_level, 1);
    assert_eq!(
        input.table_ids.len(),
        2,
        "the L0 table and the one under it"
    );
    Ok(())
}

/// The hottest movement into a leveled level promotes past the overshoot by
/// at most a table's size, as least rewrite does by default.
#[test]
fn the_hottest_merge_picks_a_window_into_a_leveled_level() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    three_tables_in_l1(&tree)?;
    write_keys(&tree, 1_000)?;
    let version = tree.current_version();
    let picked = pick_minimal_compaction(
        version.level(0).expect("L0"),
        version.level(1).expect("L1"),
        &HiddenSet::default(),
        0,
        64 * 1_024 * 1_024,
        Movement::Hottest,
        &crate::comparator::DefaultUserComparator,
    );
    let (ids, moved) = picked.expect("a merge");
    assert!(!moved);
    assert_eq!(ids.len(), 2);
    Ok(())
}

/// A trigger by size or never is no trigger by shape, however many tables
/// the level holds; past the last level, a policy set is ignored.
#[test]
fn only_a_shape_trigger_triggers_by_shape() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    for from in [0, 1_000, 2_000] {
        write_keys(&tree, from)?;
    }
    let version = tree.current_version();
    let l0 = version.level(0).expect("L0");
    assert!(shape_triggered(Trigger::Shape { runs: 1, tables: 1 }, l0));
    assert!(!shape_triggered(Trigger::Size, l0));
    assert!(!shape_triggered(Trigger::Never, l0));

    let never = LevelPolicy {
        trigger: Trigger::Never,
        ..tiered_never()
    };
    assert_eq!(
        Strategy::default().with_level_policy(7, never).policies,
        [None; 7]
    );
    // L0 set never to trigger owes nothing however many tables it holds.
    assert_eq!(
        Strategy::default()
            .with_level_policy(0, never)
            .pending_compaction_bytes(&version),
        0
    );
    Ok(())
}

/// An L0 policy set with a lower table count than the strategy's defaults is
/// the one that decides: L0 past it goes down into L1, not into a merge within
/// L0 that would coalesce its tables back below the count.
#[test]
fn a_configured_l0_shape_sends_l0_down_instead_of_within() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    write_keys(&tree, 0)?;
    write_keys(&tree, 50)?;
    let version = tree.current_version();
    let l0 = version.level(0).expect("L0");
    assert_eq!((l0.run_count(), l0.table_count()), (2, 2));
    let strategy = Strategy::default().with_level_policy(
        0,
        LevelPolicy {
            trigger: Trigger::Shape { runs: 4, tables: 2 },
            layout: Layout::Tiered,
            granularity: Granularity::Level {
                through_full_next: false,
            },
            movement: Movement::LeastRewrite { slack: None },
        },
    );
    let choice = strategy.choose(&version, tree.tree_config(), &CompactionState::default());
    let (Choice::Merge(input) | Choice::Move(input)) = choice else {
        panic!("L0 goes down: {choice:?}");
    };
    // L1's place on a tree this small is further down: anywhere but L0.
    assert_ne!(input.dest_level, 0);
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

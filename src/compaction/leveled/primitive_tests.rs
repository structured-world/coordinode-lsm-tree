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
fn tiered_l1_with_runs_at_its_count_goes_down_whole() -> crate::Result<()> {
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
fn whole_level_over_a_full_level_goes_through_it() -> crate::Result<()> {
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
fn whole_level_over_an_empty_level_stops_at_the_next() -> crate::Result<()> {
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
fn l0_set_never_to_trigger_makes_no_trivial_move() -> crate::Result<()> {
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
fn window_into_a_tiered_level_takes_one_table_of_three() -> crate::Result<()> {
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
fn hottest_window_into_a_tiered_level_takes_the_read_table() -> crate::Result<()> {
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

/// The hottest window is the one read most often, not the one read most since
/// it was written: a table written ten seconds ago and read a hundred times
/// outranks one written a thousand seconds ago and read three hundred.
#[test]
fn hottest_window_with_an_old_well_read_table_takes_the_newly_hot_one() -> crate::Result<()> {
    crate::time::with_test_clock(|clock| {
        let (_dir, tree) = tree()?;
        clock.set_secs(1_000);
        for from in [0, 1_000] {
            write_keys(&tree, from)?;
            tree.compact(Arc::new(MoveDown(0, 1)), 0)?;
        }
        for _ in 0..3 {
            for i in 0..100u64 {
                tree.get(format!("k{i:04}"), SeqNo::MAX)?;
            }
        }
        clock.set_secs(1_990);
        write_keys(&tree, 2_000)?;
        tree.compact(Arc::new(MoveDown(0, 1)), 0)?;
        for i in 2_000..2_100u64 {
            tree.get(format!("k{i:04}"), SeqNo::MAX)?;
        }
        clock.set_secs(2_000);

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
            .with_level_policy(2, tiered_never());
        let choice = strategy.choose(&version, tree.tree_config(), &CompactionState::default());
        let Choice::Move(input) = choice else {
            panic!("a move of a window: {choice:?}");
        };
        assert_eq!(input.table_ids, core::iter::once(hot).collect());
        Ok(())
    })
}

/// A window over a tiered level: least rewrite takes the whole first run when
/// the overshoot is past it, the hottest skips a hidden table and takes no
/// window past fifty tables' worth of bytes.
#[test]
fn window_over_with_bounds_and_hidden_tables_stays_within_them() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    three_tables_in_l1(&tree)?;
    let version = tree.current_version();
    let l1 = version.level(1).expect("L1");
    let ids: Vec<TableId> = l1
        .iter()
        .flat_map(|run| run.iter())
        .map(Table::id)
        .collect();
    let none = HiddenSet::default();

    let whole = window_over(
        l1,
        &none,
        u64::MAX,
        Movement::LeastRewrite { slack: None },
        1,
    )
    .expect("the first run");
    assert_eq!(whole.len(), 3, "an overshoot past the run takes all of it");

    let first = *ids.first().expect("a table");
    let mut first_hidden = HiddenSet::default();
    first_hidden.hide([first]);
    let hottest = window_over(l1, &first_hidden, 0, Movement::Hottest, u64::MAX / 64)
        .expect("a window of the visible tables");
    assert!(!hottest.contains(&first), "the hidden table is not taken");

    assert_eq!(
        window_over(l1, &none, 0, Movement::Hottest, 1),
        None,
        "every window is past fifty one-byte tables"
    );
    Ok(())
}

/// While the tree is small its data lies deeper than its place, and the
/// choices of its place decide its debt, as they decide what `choose` does: a
/// shape trigger set for L5 is not the one of data playing L1 there.
#[test]
fn debt_of_data_below_its_place_follows_the_policy_of_its_place() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    write_keys(&tree, 0)?;
    tree.compact(Arc::new(MoveDown(0, 5)), 0)?;
    let version = tree.current_version();
    assert!(!version.level(5).expect("L5").is_empty());
    let strategy = Strategy::default().with_level_policy(
        5,
        LevelPolicy {
            trigger: Trigger::Shape { runs: 1, tables: 1 },
            ..tiered_never()
        },
    );
    assert!(
        matches!(
            strategy.choose(&version, tree.tree_config(), &CompactionState::default()),
            Choice::DoNothing
        ),
        "the data plays L1, which is within its target"
    );
    assert_eq!(strategy.pending_compaction_bytes(&version), 0);
    Ok(())
}

/// A least-rewrite window over a run whose first table is being compacted
/// takes the tables after it, not none.
#[test]
fn least_rewrite_window_past_a_hidden_first_table_takes_the_rest() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    three_tables_in_l1(&tree)?;
    let version = tree.current_version();
    let l1 = version.level(1).expect("L1");
    assert_eq!(l1.run_count(), 1, "the fixture needs the tables in one run");
    let ids: Vec<TableId> = l1
        .iter()
        .flat_map(|run| run.iter())
        .map(Table::id)
        .collect();
    let [first, second, third] = ids[..] else {
        panic!("three tables in L1: {ids:?}");
    };
    let mut first_hidden = HiddenSet::default();
    first_hidden.hide([first]);
    let window = window_over(
        l1,
        &first_hidden,
        u64::MAX,
        Movement::LeastRewrite { slack: None },
        1,
    );
    assert_eq!(window, Some([second, third].into_iter().collect()));
    Ok(())
}

/// One run in L1: a table of incompressible values over `k0000..`, then two
/// tables of short ones over `k1000..` and `k2000..`; the last is read. The
/// first table's size, a fiftieth of it less one, puts the cap below it and
/// above the others.
fn a_large_table_before_a_hot_small_one(
    tree: &crate::AnyTree,
) -> crate::Result<(Version, TableId, u64)> {
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    for i in 0..100u64 {
        let value: Vec<u8> = (0..2_000)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state.to_le_bytes()[0]
            })
            .collect();
        tree.insert(format!("k{i:04}"), value, i);
    }
    tree.flush_active_memtable(0)?;
    tree.compact(Arc::new(MoveDown(0, 1)), 0)?;
    for from in [1_000, 2_000] {
        write_keys(tree, from)?;
        tree.compact(Arc::new(MoveDown(0, 1)), 0)?;
    }
    for i in 2_000..2_100u64 {
        tree.get(format!("k{i:04}"), SeqNo::MAX)?;
    }
    let version = tree.current_version();
    let l1 = version.level(1).expect("L1");
    assert_eq!(l1.run_count(), 1, "the fixture needs the tables in one run");
    let tables: Vec<&Table> = l1.iter().flat_map(|run| run.iter()).collect();
    let [large, small, hot] = tables[..] else {
        panic!("three tables in L1: {}", tables.len());
    };
    let base = large.file_size() / 50 - 1;
    assert!(
        small.file_size() + hot.file_size() <= 50 * base,
        "the fixture needs the small tables under the cap",
    );
    let hot = hot.id();
    Ok((version, hot, base))
}

/// A window past the cap leaves the rest of its width to be ranked: the
/// hottest window over a tiered level whose first table is past the cap is
/// the small table reads touch, not none.
#[test]
fn hottest_window_past_a_large_first_table_takes_the_read_one() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    let (version, hot, base) = a_large_table_before_a_hot_small_one(&tree)?;
    let window = window_over(
        version.level(1).expect("L1"),
        &HiddenSet::default(),
        0,
        Movement::Hottest,
        base,
    );
    assert_eq!(window, Some(core::iter::once(hot).collect()));
    Ok(())
}

/// A merge into a leveled level ranks the windows of its width past one over
/// the cap: L0 over the keys of the small table merges with it, though the
/// level's first table is past the cap.
#[test]
fn merge_past_a_large_first_table_takes_the_window_it_overlaps() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    let (_, hot, base) = a_large_table_before_a_hot_small_one(&tree)?;
    write_keys(&tree, 2_000)?;
    let version = tree.current_version();
    let picked = pick_minimal_compaction(
        version.level(0).expect("L0"),
        version.level(1).expect("L1"),
        &HiddenSet::default(),
        0,
        base,
        Movement::LeastRewrite { slack: None },
        &crate::comparator::DefaultUserComparator,
    );
    let (ids, moved) = picked.expect("a merge with the small table");
    assert!(!moved);
    assert!(ids.contains(&hot), "{ids:?}");
    assert_eq!(ids.len(), 2);
    Ok(())
}

/// The tables of a run taken along for the keys they share lie side by side:
/// a run whose two tables share keys with the ends of another run takes the
/// table between those ends too, which a merge reads as it reads the run.
#[test]
fn overlapping_runs_take_the_tables_between_the_ones_they_share_keys_with() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    three_tables_in_l1(&tree)?;
    for from in [50, 2_050] {
        for i in from..from + 10 {
            tree.insert(format!("k{i:04}"), "w", 10_000 + i);
        }
        tree.flush_active_memtable(0)?;
        tree.compact(Arc::new(MergeL0(1)), 0)?;
    }
    let version = tree.current_version();
    let l1 = version.level(1).expect("L1");
    let runs: Vec<Vec<TableId>> = l1
        .iter()
        .map(|run| run.iter().map(Table::id).collect())
        .collect();
    assert_eq!(runs.len(), 2, "the fixture needs two runs in L1: {runs:?}");
    let newer = runs
        .iter()
        .find(|run| run.len() == 2)
        .expect("the fixture needs a run of the two new tables");
    let older = runs
        .iter()
        .find(|run| run.len() == 3)
        .expect("the fixture needs the run of the three first tables");

    let ids = with_overlapping_runs(
        l1,
        newer.iter().copied().collect(),
        &HiddenSet::default(),
        &crate::comparator::DefaultUserComparator,
    )
    .expect("nothing hidden");
    assert!(
        older.iter().all(|id| ids.contains(id)),
        "the older run's middle table is taken too: {ids:?} of {older:?}"
    );
    Ok(())
}

/// A window into a tiered level out of a level of two runs takes the other
/// run's versions of its keys along, and waits while one of them is being
/// compacted.
#[test]
fn window_into_a_tiered_level_from_two_runs_takes_both_or_waits() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    write_round(&tree, 0)?;
    tree.compact(Arc::new(MoveDown(0, 1)), 0)?;
    write_round(&tree, 1)?;
    tree.compact(Arc::new(MergeL0(1)), 0)?;
    let version = tree.current_version();
    let l1 = version.level(1).expect("L1");
    assert_eq!(l1.run_count(), 2);
    let ids = l1.list_ids();
    let strategy = window_into_tiered_l2();

    let choice = strategy.choose(&version, tree.tree_config(), &CompactionState::default());
    let (Choice::Move(input) | Choice::Merge(input)) = choice else {
        panic!("a window goes down: {choice:?}");
    };
    assert_eq!(input.table_ids, ids, "both versions of every key");

    let older = l1
        .iter()
        .last()
        .and_then(|run| run.iter().next())
        .expect("a table of the older run")
        .id();
    let mut busy = CompactionState::default();
    busy.hidden_set_mut().hide([older]);
    let choice = strategy.choose(&version, tree.tree_config(), &busy);
    assert!(matches!(choice, Choice::DoNothing), "{choice:?}");
    Ok(())
}

/// L0 set never to trigger takes no free move to the last level of an empty
/// tree either.
#[test]
fn l0_set_never_to_trigger_over_an_empty_tree_makes_no_move() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    write_keys(&tree, 0)?;
    let version = tree.current_version();
    assert!(
        matches!(
            Strategy::default().choose(&version, tree.tree_config(), &CompactionState::default()),
            Choice::Move(_)
        ),
        "the leveled L0 moves its run to the last level",
    );
    let never = LevelPolicy {
        trigger: Trigger::Never,
        ..tiered_never()
    };
    let choice = Strategy::default().with_level_policy(0, never).choose(
        &version,
        tree.tree_config(),
        &CompactionState::default(),
    );
    assert!(matches!(choice, Choice::DoNothing), "{choice:?}");
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
fn window_of_several_runs_takes_the_older_versions_along() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    write_span(&tree, (0, 50), 0)?;
    write_span(&tree, (50, 100), 0)?;
    write_span(&tree, (200, 250), 0)?;
    tree.compact(Arc::new(MoveDown(0, 1)), 0)?;
    write_span(&tree, (0, 100), 1)?;
    write_span(&tree, (10, 20), 2)?;
    // Far from the window, over a table of L1 so it cannot move as it is,
    // and dearer per byte than the narrow window: no table the merge takes
    // shares a key with it.
    write_span(&tree, (200, 205), 3)?;
    let version = tree.current_version();
    let l0 = version.level(0).expect("L0");
    assert!(l0.run_count() >= 2);
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

    let far = id_of(b"k0200", b"k0204");
    let l1 = version.level(1).expect("L1");
    let upper = l1
        .iter()
        .flat_map(|run| run.iter())
        .find(|t| t.key_range().min().as_ref() == b"k0050")
        .expect("the upper L1 table")
        .id();

    // The bound admits only the narrow table promoted on its own.
    let pick = |hidden: &HiddenSet| {
        pick_minimal_compaction(
            l0,
            l1,
            hidden,
            0,
            u64::MAX / 64,
            Movement::LeastRewrite {
                slack: Some(narrow_size),
            },
            &crate::comparator::DefaultUserComparator,
        )
    };
    let (ids, _) = pick(&HiddenSet::default()).expect("a merge");
    assert!(ids.contains(&narrow), "the narrow window is the cheapest");
    assert!(ids.contains(&wide), "the older wide version goes along");
    assert!(ids.contains(&upper), "with what it overlaps below");
    assert!(!ids.contains(&far), "nothing that shares no key with them");

    // A table the closure needs being compacted holds the merge back, in
    // the level and below it.
    for busy in [wide, upper] {
        let mut hidden = HiddenSet::default();
        hidden.hide([busy]);
        assert_eq!(pick(&hidden), None);
    }
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
fn window_over_a_tiered_level_holding_its_keys_merges_alone() -> crate::Result<()> {
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
fn window_into_a_tiered_level_with_busy_tables_waits() -> crate::Result<()> {
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
fn whole_level_into_a_tiered_level_takes_nothing_of_it() -> crate::Result<()> {
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
fn window_of_l0_over_l1_goes_into_l1() -> crate::Result<()> {
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
fn hottest_merge_into_a_leveled_level_picks_a_window() -> crate::Result<()> {
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
fn shape_trigger_by_size_or_never_never_fires() -> crate::Result<()> {
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
fn l0_past_a_configured_shape_goes_down_not_within() -> crate::Result<()> {
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

/// A level set never to trigger owes no compaction debt however far past its
/// target it grows: nothing this strategy runs would pay it, and a debt
/// nothing pays would hold writes back for good.
#[test]
fn pending_bytes_of_a_level_set_never_to_trigger_are_none() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    write_round(&tree, 0)?;
    tree.compact(Arc::new(MoveDown(0, 1)), 0)?;
    let version = tree.current_version();
    let strategy = Strategy::default().with_table_target_size(64);
    assert!(
        strategy.pending_compaction_bytes(&version) > 0,
        "the leveled L1 is past its target"
    );
    let never = LevelPolicy {
        trigger: Trigger::Never,
        layout: Layout::Leveled,
        granularity: Granularity::Window,
        movement: Movement::LeastRewrite { slack: None },
    };
    assert_eq!(
        strategy
            .with_level_policy(1, never)
            .pending_compaction_bytes(&version),
        0
    );
    Ok(())
}

/// L0 set to trigger by size goes down once it holds more than L1's base
/// size, overlapping runs and all, and owes what it holds past it.
#[test]
fn l0_set_to_trigger_by_size_past_its_target_goes_down() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    write_keys(&tree, 0)?;
    write_keys(&tree, 50)?;
    let version = tree.current_version();
    let size = LevelPolicy {
        trigger: Trigger::Size,
        layout: Layout::Tiered,
        granularity: Granularity::Level {
            through_full_next: false,
        },
        movement: Movement::LeastRewrite { slack: None },
    };
    let strategy = Strategy::default()
        .with_table_target_size(64)
        .with_level_policy(0, size);
    let choice = strategy.choose(&version, tree.tree_config(), &CompactionState::default());
    let (Choice::Merge(input) | Choice::Move(input)) = choice else {
        panic!("L0 goes down: {choice:?}");
    };
    assert_ne!(input.dest_level, 0);
    assert!(strategy.pending_compaction_bytes(&version) > 0);
    Ok(())
}

/// A level set never to trigger starts no compaction however far past its
/// target it grows.
#[test]
fn level_set_never_to_trigger_past_its_target_starts_nothing() -> crate::Result<()> {
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
fn hottest_movement_with_a_cheap_cold_rewrite_prefers_reads() {
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

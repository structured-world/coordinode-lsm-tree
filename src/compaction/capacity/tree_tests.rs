// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Compactions of real trees reserving their room in a pool.

use super::*;
use crate::compaction::CompactionAction;
use crate::compaction::filter::{CompactionFilter, Context, Factory, ItemAccessor, Verdict};
use crate::config::LevelRoute;
use crate::fs::MemFs;
use crate::{AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter, Table};

const MIB: u64 = 1024 * 1024;

fn demand<'a>(fs: &'a MemFs, path: &'a std::path::Path, bytes: u64) -> Demand<'a> {
    Demand { fs, path, bytes }
}

fn key(i: u32) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

/// A value of `len` bytes no codec shrinks, the same for the same `seed`.
fn noise(seed: u32, len: usize) -> Vec<u8> {
    let mut state = u64::from(seed).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

fn standard(config: Config) -> crate::Tree {
    let AnyTree::Standard(tree) = config.open().expect("open") else {
        panic!("expected a standard tree");
    };
    tree
}

fn config(dir: &std::path::Path, fs: &MemFs, pool: &Arc<CapacityPool>) -> Config {
    Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(fs.clone()))
    .compaction_capacity(Arc::clone(pool))
}

/// Writes keys `range` with values of `len` bytes into one table.
fn flush(tree: &crate::Tree, range: core::ops::Range<u32>, len: usize, seqno: &mut u64) {
    for i in range {
        tree.insert(key(i), noise(i, len), *seqno);
        *seqno += 1;
    }
    tree.flush_active_memtable(0).expect("flush");
}

fn table_ids(tree: &crate::Tree) -> Vec<crate::TableId> {
    let mut ids: Vec<_> = tree
        .current_version()
        .iter_tables()
        .map(Table::id)
        .collect();
    ids.sort_unstable();
    ids
}

fn admit(tree: &crate::Tree) {
    tree.update_runtime_config(|c| c.storage_admission_check = true)
        .expect("admission");
}

/// A tree that leaves a shared pool takes its account along: what its
/// compactions gave back of its quota, and what an output that could not be
/// removed kept held, end with the tree, so trees opened and dropped over a
/// long-lived pool leave nothing behind in it.
#[test]
fn a_dropped_tree_leaves_nothing_in_the_shared_pool() {
    let dir = tempfile::tempdir().expect("dir");
    let fs = MemFs::with_capacity(64 * MIB);
    let pool = Arc::new(CapacityPool::default());
    let tree = standard(config(dir.path(), &fs, &pool));
    let mut seqno = 1;
    flush(&tree, 0..500, 512, &mut seqno);
    flush(&tree, 250..750, 512, &mut seqno);
    tree.major_compact(64 * MIB, 0).expect("compact");
    let id = tree.id;
    // An output left on disk keeps part of the quota held past its
    // reservation.
    let stranded = pool.hold(id, &[demand(&fs, dir.path(), 4_096)]);
    stranded.strand(None);
    drop(stranded);
    assert!(pool.accounts(id), "the tree's compaction gave quota back");
    assert_eq!(pool.quota_held(id), 4_096);

    drop(tree);
    assert!(!pool.accounts(id), "nothing of the dropped tree is kept");
    assert_eq!(pool.quota_held(id), 0);
}

/// A tree's merge does not count on the free bytes another tree's compaction
/// in the same pool has still to write: it is skipped while they are held and
/// runs once they are given back, and it gives its own back as it returns.
#[test]
fn a_merge_skips_while_another_tree_holds_the_volumes_room() {
    let dir = tempfile::tempdir().expect("dir");
    let fs = MemFs::with_capacity(64 * MIB);
    let pool = Arc::new(CapacityPool::default());
    let tree = standard(config(dir.path(), &fs, &pool));
    let mut seqno = 1;
    flush(&tree, 0..500, 512, &mut seqno);
    flush(&tree, 250..750, 512, &mut seqno);
    admit(&tree);
    let before = table_ids(&tree);

    // Another tree's compaction holds all but half a MiB of the volume.
    let free = fs.available_space(dir.path()).expect("free");
    let other = pool
        .reserve(u64::MAX, &[demand(&fs, dir.path(), free - MIB / 2)], None)
        .expect("unchecked");

    let result = tree.major_compact(64 * MIB, 0).expect("compact");
    assert_eq!(result.action, CompactionAction::Nothing);
    assert_eq!(table_ids(&tree), before, "the inputs are as they were");

    drop(other);
    let result = tree.major_compact(64 * MIB, 0).expect("compact");
    assert_eq!(result.action, CompactionAction::Merged);
    assert_eq!(
        pool.quota_held(tree.id()),
        0,
        "the merge gave its room back"
    );
    assert_eq!(
        pool.available(&fs, dir.path()),
        fs.available_space(dir.path()).expect("free")
    );
}

/// A move into a level another volume holds rewrites its tables there and
/// asks for that room as a merge does; a move in place writes nothing and
/// asks for none.
#[test]
fn a_move_across_volumes_reserves_as_a_merge_and_a_move_in_place_does_not() {
    let dir = tempfile::tempdir().expect("dir");
    let cold = MemFs::with_capacity(64 * MIB);
    let hot = MemFs::with_capacity(64 * MIB);
    let pool = Arc::new(CapacityPool::default());
    let tree = standard(
        config(dir.path(), &cold, &pool).level_routes(vec![LevelRoute {
            levels: 0..2,
            path: dir.path().join("hot"),
            fs: Arc::new(hot),
        }]),
    );
    let mut seqno = 1;
    flush(&tree, 0..500, 512, &mut seqno);
    admit(&tree);

    // The cold volume, which level 2 lives on, has no room left to give.
    let free = cold.available_space(dir.path()).expect("free");
    let _held = pool
        .reserve(u64::MAX, &[demand(&cold, dir.path(), free)], None)
        .expect("unchecked");

    let across = tree
        .compact(Arc::new(crate::compaction::MoveDown(0, 2)), 0)
        .expect("compact");
    assert_eq!(
        across.action,
        CompactionAction::Nothing,
        "no room on the cold volume"
    );

    let in_place = tree
        .compact(Arc::new(crate::compaction::MoveDown(0, 1)), 0)
        .expect("compact");
    assert_eq!(in_place.action, CompactionAction::Moved);
}

/// A merge whose filter grows its rows past the room it reserved takes the
/// excess on as long as the volume keeps the flush floor, then stops: its
/// inputs are as they were, its outputs are gone, and so is its room.
#[test]
fn a_merge_a_filter_grows_past_its_room_stops_and_leaves_its_inputs() {
    struct Grow;
    impl CompactionFilter for Grow {
        fn filter_item(&mut self, item: ItemAccessor<'_>, _: &Context) -> crate::Result<Verdict> {
            Ok(Verdict::ReplaceValue(
                noise(7, item.value()?.len() * 16).into(),
            ))
        }
    }
    struct GrowFactory;
    impl Factory for GrowFactory {
        fn name(&self) -> &'static str {
            "grow"
        }
        fn make_filter(&self, _: &Context) -> Box<dyn CompactionFilter> {
            Box::new(Grow)
        }
    }

    let dir = tempfile::tempdir().expect("dir");
    let fs = MemFs::with_capacity(u64::MAX);
    let pool = Arc::new(CapacityPool::default());
    let tree = standard(
        config(dir.path(), &fs, &pool).with_compaction_filter_factory(Some(Arc::new(GrowFactory))),
    );
    let mut seqno = 1;
    flush(&tree, 0..1_000, 1_024, &mut seqno);
    flush(&tree, 500..1_500, 1_024, &mut seqno);
    admit(&tree);
    let before = table_ids(&tree);

    // Room for the inputs and some, far from the sixteenfold the filter writes.
    let probe = 1u64 << 40;
    fs.set_capacity(probe);
    let stored = probe - fs.available_space(dir.path()).expect("free");
    fs.set_capacity(stored + 6 * MIB);
    let free_before = fs.available_space(dir.path()).expect("free");

    // Small outputs, so each lands and is weighed before the next is written.
    let result = tree.major_compact(256 * 1024, 0).expect("compact");

    assert_eq!(result.action, CompactionAction::Nothing);
    assert_eq!(table_ids(&tree), before, "the inputs are as they were");
    for i in 0..1_500 {
        assert!(
            tree.get(key(i), SeqNo::MAX).expect("get").is_some(),
            "key {i}"
        );
    }
    assert_eq!(
        fs.available_space(dir.path()).expect("free"),
        free_before,
        "the outputs were removed",
    );
    assert_eq!(pool.quota_held(tree.id()), 0, "the room went back");
}

/// A merge published while a snapshot still reads its inputs gives its room
/// back, and the inputs' bytes, still on disk, are not counted as free: the
/// volume's report keeps them until they go.
#[test]
fn a_merge_under_a_pinned_snapshot_credits_nothing_for_its_inputs() {
    let dir = tempfile::tempdir().expect("dir");
    let fs = MemFs::with_capacity(64 * MIB);
    let pool = Arc::new(CapacityPool::default());
    let tree = standard(config(dir.path(), &fs, &pool));
    let mut seqno = 1;
    flush(&tree, 0..500, 512, &mut seqno);
    flush(&tree, 250..750, 512, &mut seqno);
    admit(&tree);

    let pinned = tree.current_version();
    let inputs: Vec<_> = pinned.iter_tables().map(|t| t.path.clone()).collect();
    let result = tree.major_compact(64 * MIB, 0).expect("compact");
    assert_eq!(result.action, CompactionAction::Merged);

    for path in &inputs {
        assert!(
            crate::fs::Fs::exists(&fs, path).expect("exists"),
            "a pinned input stays on disk"
        );
    }
    assert_eq!(pool.quota_held(tree.id()), 0);
    assert_eq!(
        pool.available(&fs, dir.path()),
        fs.available_space(dir.path()).expect("free")
    );
    drop(pinned);
}

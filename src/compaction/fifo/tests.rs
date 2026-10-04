use super::Strategy;
use crate::{AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter};
use std::sync::Arc;

#[test]
fn fifo_empty_levels() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let fifo = Arc::new(Strategy::new(1, None));
    tree.compact(fifo, 0)?;

    assert_eq!(0, tree.table_count());
    Ok(())
}

#[test]
fn fifo_below_limit() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    for i in 0..4u8 {
        tree.insert([b'k', i].as_slice(), "v", u64::from(i));
        tree.flush_active_memtable(u64::from(i))?;
    }

    let before = tree.table_count();
    let fifo = Arc::new(Strategy::new(u64::MAX, None));
    tree.compact(fifo, 4)?;

    assert_eq!(before, tree.table_count());
    Ok(())
}

#[test]
fn fifo_more_than_limit() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    for i in 0..4u8 {
        tree.insert([b'k', i].as_slice(), "v", u64::from(i));
        tree.flush_active_memtable(u64::from(i))?;
    }

    let before = tree.table_count();
    // Very small limit forces dropping oldest tables
    let fifo = Arc::new(Strategy::new(1, None));
    tree.compact(fifo, 4)?;

    assert!(tree.table_count() < before);
    Ok(())
}

#[test]
fn fifo_more_than_limit_blobs() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    .open()?;

    for i in 0..3u8 {
        tree.insert([b'k', i].as_slice(), "$", u64::from(i));
        tree.flush_active_memtable(u64::from(i))?;
    }

    let before = tree.table_count();
    let fifo = Arc::new(Strategy::new(1, None));
    tree.compact(fifo, 3)?;

    assert!(tree.table_count() < before);
    Ok(())
}

#[test]
fn fifo_ttl() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Freeze time and create first (older) table at t=1000s
    crate::time::set_unix_timestamp_for_test(Some(std::time::Duration::from_secs(1_000)));
    tree.insert("a", "1", 0);
    tree.flush_active_memtable(0)?;

    // Advance time and create second (newer) table at t=1005s
    crate::time::set_unix_timestamp_for_test(Some(std::time::Duration::from_secs(1_005)));
    tree.insert("b", "2", 1);
    tree.flush_active_memtable(1)?;

    // Now set current time to t=1011s; with TTL=10s, cutoff=1001s => drop first only
    crate::time::set_unix_timestamp_for_test(Some(std::time::Duration::from_secs(1_011)));

    assert_eq!(2, tree.table_count());

    let fifo = Arc::new(Strategy::new(u64::MAX, Some(10)));
    tree.compact(fifo, 2)?;

    assert_eq!(1, tree.table_count());

    // Reset override
    crate::time::set_unix_timestamp_for_test(None);
    Ok(())
}

/// Two flushes whose key ranges overlap (`a..c`, then `b`) leave L0 not
/// disjoint, which non-monotonic keys produce routinely. FIFO must compact
/// such a tree instead of panicking the worker, and enforce its limit by
/// dropping the oldest table first.
#[test]
fn fifo_overlapping_l0_compacts_without_panic_and_drops_oldest_first() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    crate::time::set_unix_timestamp_for_test(Some(std::time::Duration::from_secs(1_000)));
    tree.insert("a", "old", 0);
    tree.insert("c", "old", 1);
    tree.flush_active_memtable(1)?;
    crate::time::set_unix_timestamp_for_test(Some(std::time::Duration::from_secs(1_001)));
    tree.insert("b", "new", 2);
    tree.flush_active_memtable(2)?;
    crate::time::set_unix_timestamp_for_test(None);

    // Nothing to drop: the tree is left as it is.
    tree.compact(Arc::new(Strategy::new(u64::MAX, None)), 3)?;
    assert_eq!(2, tree.table_count());

    // A limit just below the total keeps the newer table and drops the older.
    let newest_size = tree
        .current_version()
        .iter_tables()
        .max_by_key(|t| t.metadata.created_at)
        .map(crate::table::Table::file_size)
        .unwrap_or_default();
    tree.compact(Arc::new(Strategy::new(newest_size, None)), 3)?;
    assert_eq!(1, tree.table_count());
    assert!(tree.get("b", 3)?.is_some(), "the newer table must survive");
    assert!(tree.get("a", 3)?.is_none(), "the older table goes first");
    Ok(())
}

/// After `major_compact` the tables sit in the last level, not L0. The size
/// limit must still apply to them.
#[test]
fn fifo_limit_applies_after_major_compaction() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    for i in 0..4u8 {
        tree.insert([b'k', i].as_slice(), "v", u64::from(i));
        tree.flush_active_memtable(u64::from(i))?;
    }
    tree.major_compact(u64::MAX, 0)?;
    assert_eq!(0, tree.current_version().l0().table_count());
    assert!(tree.table_count() > 0);

    tree.compact(Arc::new(Strategy::new(1, None)), 4)?;
    assert_eq!(
        0,
        tree.table_count(),
        "tables below L0 must count against the limit"
    );
    Ok(())
}

/// After `major_compact` the TTL must still expire tables below L0.
#[test]
fn fifo_ttl_applies_after_major_compaction() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    crate::time::set_unix_timestamp_for_test(Some(std::time::Duration::from_secs(1_000)));
    for i in 0..3u8 {
        tree.insert([b'k', i].as_slice(), "v", u64::from(i));
        tree.flush_active_memtable(u64::from(i))?;
    }
    tree.major_compact(u64::MAX, 0)?;

    crate::time::set_unix_timestamp_for_test(Some(std::time::Duration::from_secs(1_011)));
    tree.compact(Arc::new(Strategy::new(u64::MAX, Some(10))), 3)?;
    crate::time::set_unix_timestamp_for_test(None);

    assert_eq!(0, tree.table_count(), "expired tables below L0 must drop");
    Ok(())
}

/// A table another compaction holds is not FIFO's to drop, and holding it
/// must not panic: `choose` skips hidden tables and drops only the others.
#[test]
fn fifo_choose_skips_hidden_tables_instead_of_panicking() -> crate::Result<()> {
    use super::super::{Choice, CompactionStrategy, state::CompactionState};

    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    for i in 0..3u8 {
        tree.insert([b'k', i].as_slice(), "v", u64::from(i));
        tree.flush_active_memtable(u64::from(i))?;
    }
    let version = tree.current_version();
    let ids: Vec<_> = version.iter_tables().map(crate::table::Table::id).collect();
    let Some(&hidden) = ids.first() else {
        panic!("three tables were flushed");
    };

    let mut state = CompactionState::default();
    state.hidden_set_mut().hide([hidden]);
    let choice = Strategy::new(1, None).choose(&version, &Config::default(), &state);
    let Choice::Drop(dropped) = choice else {
        panic!("expected a drop of the tables not held");
    };
    assert!(
        !dropped.contains(&hidden),
        "a held table must not be dropped"
    );
    assert!(!dropped.is_empty());

    let mut all_held = CompactionState::default();
    all_held.hidden_set_mut().hide(ids.iter().copied());
    assert!(matches!(
        Strategy::new(1, None).choose(&version, &Config::default(), &all_held),
        Choice::DoNothing
    ));
    Ok(())
}

#[test]
fn fifo_ttl_then_limit_additional_drops_blob_unit() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    .open()?;

    // Create two tables; we will expire them via time override and force additional drops via limit.
    tree.insert("a", "$", 0);
    tree.flush_active_memtable(0)?;
    tree.insert("b", "$", 1);
    tree.flush_active_memtable(1)?;

    crate::time::set_unix_timestamp_for_test(Some(std::time::Duration::from_secs(10_000_000)));

    // TTL=1s will mark both expired; very small limit ensures size-based collection path is also exercised.
    let fifo = Arc::new(Strategy::new(1, Some(1)));
    tree.compact(fifo, 2)?;

    assert_eq!(0, tree.table_count());

    crate::time::set_unix_timestamp_for_test(None);
    Ok(())
}

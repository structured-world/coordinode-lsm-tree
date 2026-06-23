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

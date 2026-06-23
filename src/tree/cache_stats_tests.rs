use crate::{AbstractTree, Config, MAX_SEQNO, SequenceNumberCounter};
use test_log::test;

#[test]
fn cache_stats_reports_capacity_and_counts_after_reads() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Empty cache: a configured capacity, but nothing loaded yet.
    let before = tree.cache_stats();
    assert!(
        before.capacity_bytes > 0,
        "the default block cache has a capacity",
    );
    assert_eq!(
        before.hits + before.misses,
        0,
        "no block has been loaded yet",
    );
    assert!(
        (before.hit_rate - 1.0).abs() < f64::EPSILON,
        "no miss yet, so the rate is 1.0",
    );

    // Flush an SST and read every key back so data / index / filter blocks flow
    // through the cache.
    for i in 0..200u32 {
        tree.insert(format!("k{i:04}"), "v", 0);
    }
    tree.flush_active_memtable(0)?;
    for i in 0..200u32 {
        assert!(tree.get(format!("k{i:04}"), MAX_SEQNO)?.is_some());
    }

    let after = tree.cache_stats();
    assert!(
        after.hits + after.misses > 0,
        "reads should have loaded blocks through the cache",
    );
    assert!(
        after.size_bytes <= after.capacity_bytes,
        "resident size stays within capacity",
    );
    assert_eq!(
        after.capacity_bytes, before.capacity_bytes,
        "capacity is stable across reads",
    );

    Ok(())
}

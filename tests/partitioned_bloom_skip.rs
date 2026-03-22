/// Tests that partitioned bloom filters correctly skip non-matching keys
/// via the bloom_may_contain_key path (issue #83).
///
/// Before #83, bloom_may_contain_key_hash returned Ok(true) conservatively
/// for partitioned filters, so the merge pipeline could not skip tables.
/// After #83, bloom_may_contain_key seeks the partition index and checks
/// only the matching partition's bloom filter.
#[test_log::test]
#[cfg(feature = "metrics")]
fn partitioned_bloom_skip_for_point_reads() -> lsm_tree::Result<()> {
    use lsm_tree::{
        config::PinningPolicy, get_tmp_folder, AbstractTree, Config, SeqNo, SequenceNumberCounter,
    };

    let folder = get_tmp_folder();
    let path = folder.path();

    let seqno = SequenceNumberCounter::default();

    let tree = Config::new(path, seqno.clone(), SequenceNumberCounter::default())
        // Force partitioned filters on all levels (including L0)
        .filter_block_partitioning_policy(PinningPolicy::all(true))
        .open()?;

    // Insert keys "a" and "c" into a single table
    tree.insert("a", "val_a", seqno.next());
    tree.insert("c", "val_c", seqno.next());
    tree.flush_active_memtable(0)?;

    // Query for "b" which does NOT exist — bloom should reject
    assert!(tree.get("b", SeqNo::MAX)?.is_none());

    // With partitioned bloom skip working, the filter should have
    // rejected the table and recorded a skip.
    assert_eq!(
        1,
        tree.metrics().io_skipped_by_filter(),
        "partitioned bloom filter should skip the table for non-matching key"
    );
    assert_eq!(1, tree.metrics().filter_queries());

    // Verify that existing keys are still found correctly
    assert!(tree.get("a", SeqNo::MAX)?.is_some());
    assert!(tree.get("c", SeqNo::MAX)?.is_some());

    Ok(())
}

/// Tests that bloom_may_contain_key returns Ok(false) for a key beyond all
/// partition boundaries (i.e. greater than the last partition's end key).
#[test_log::test]
fn partitioned_bloom_skip_beyond_partitions() -> lsm_tree::Result<()> {
    use lsm_tree::{
        config::PinningPolicy, get_tmp_folder, AbstractTree, Config, SeqNo, SequenceNumberCounter,
    };

    let folder = get_tmp_folder();
    let path = folder.path();

    let seqno = SequenceNumberCounter::default();

    let tree = Config::new(path, seqno.clone(), SequenceNumberCounter::default())
        .filter_block_partitioning_policy(PinningPolicy::all(true))
        .open()?;

    tree.insert("a", "val_a", seqno.next());
    tree.insert("b", "val_b", seqno.next());
    tree.flush_active_memtable(0)?;

    // Key "z" is beyond all partition boundaries
    assert!(tree.get("z", SeqNo::MAX)?.is_none());

    // Key "a" should still be found
    assert!(tree.get("a", SeqNo::MAX)?.is_some());

    Ok(())
}

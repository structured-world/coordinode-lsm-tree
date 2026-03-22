/// Tests that partitioned bloom filters correctly skip non-matching keys
/// via the Table::get path (which already has partition-aware bloom seeking).
///
/// This exercises the existing partition-aware bloom in Table::get — the
/// metrics confirm the filter rejects the table for a non-matching key.
#[test_log::test]
#[cfg(feature = "metrics")]
fn partitioned_bloom_skip_for_point_reads() -> lsm_tree::Result<()> {
    use lsm_tree::{
        config::PinningPolicy, get_tmp_folder, AbstractTree, Config, SequenceNumberCounter,
        MAX_SEQNO,
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
    assert!(tree.get("b", MAX_SEQNO)?.is_none());

    // With partitioned bloom skip working, the filter should have
    // rejected the table and recorded a skip.
    assert_eq!(
        1,
        tree.metrics().io_skipped_by_filter(),
        "partitioned bloom filter should skip the table for non-matching key"
    );
    assert!(
        tree.metrics().filter_queries() >= 1,
        "expected at least one filter query for non-matching key, got {}",
        tree.metrics().filter_queries()
    );

    // Verify that existing keys are still found correctly
    assert!(tree.get("a", MAX_SEQNO)?.is_some());
    assert!(tree.get("c", MAX_SEQNO)?.is_some());

    Ok(())
}

/// Tests that bloom_may_contain_key returns Ok(false) for a key beyond all
/// partition boundaries (i.e. greater than the last partition's end key).
#[test_log::test]
fn partitioned_bloom_skip_beyond_partitions() -> lsm_tree::Result<()> {
    use lsm_tree::{
        config::PinningPolicy, get_tmp_folder, AbstractTree, Config, SequenceNumberCounter,
        MAX_SEQNO,
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
    assert!(tree.get("z", MAX_SEQNO)?.is_none());

    // Key "a" should still be found
    assert!(tree.get("a", MAX_SEQNO)?.is_some());

    Ok(())
}

/// Exercises the bloom_may_contain_key path through the merge pipeline
/// (resolve_merge_via_pipeline → TreeIter → bloom_passes → bloom_may_contain_key).
///
/// This is the primary path that issue #83 enables: when a merge operator is
/// configured, point reads go through the iterator pipeline where bloom_key
/// allows partition-aware filtering instead of the conservative Ok(true).
#[test_log::test]
fn partitioned_bloom_skip_merge_pipeline() -> lsm_tree::Result<()> {
    use lsm_tree::{
        config::PinningPolicy, get_tmp_folder, AbstractTree, Config, MergeOperator,
        SequenceNumberCounter, MAX_SEQNO,
    };

    struct SumMerge;
    impl MergeOperator for SumMerge {
        fn merge(
            &self,
            _key: &[u8],
            base_value: Option<&[u8]>,
            operands: &[&[u8]],
        ) -> lsm_tree::Result<lsm_tree::Slice> {
            let mut sum: i64 = base_value
                .map(|b| {
                    i64::from_le_bytes(
                        b.try_into()
                            .expect("invalid base value length for i64 in SumMerge"),
                    )
                })
                .unwrap_or(0);
            for op in operands {
                sum += i64::from_le_bytes(
                    (*op)
                        .try_into()
                        .expect("invalid operand length for i64 in SumMerge"),
                );
            }
            Ok(sum.to_le_bytes().to_vec().into())
        }
    }

    let folder = get_tmp_folder();
    let path = folder.path();

    let seqno = SequenceNumberCounter::default();

    let tree = Config::new(path, seqno.clone(), SequenceNumberCounter::default())
        .filter_block_partitioning_policy(PinningPolicy::all(true))
        .with_merge_operator(Some(std::sync::Arc::new(SumMerge)))
        .open()?;

    // Table 1: base value for "counter"
    tree.insert("counter", &100_i64.to_le_bytes(), seqno.next());
    tree.flush_active_memtable(0)?;

    // Table 2: unrelated key — bloom should skip this table for "counter"
    tree.insert("zzz_other", &999_i64.to_le_bytes(), seqno.next());
    tree.flush_active_memtable(0)?;

    // Merge operand in active memtable — triggers resolve_merge_via_pipeline
    tree.merge("counter", 10_i64.to_le_bytes(), seqno.next());

    // The pipeline scans disk tables with bloom pre-filter.
    // Table 2 should be skipped by bloom_may_contain_key("counter", hash)
    // because "counter" is not in table 2's partitioned bloom filter.
    let result = tree.get("counter", MAX_SEQNO)?;
    assert!(result.is_some());

    let value = i64::from_le_bytes(result.unwrap().as_ref().try_into().unwrap());
    assert_eq!(110, value, "merge(100, [10]) = 110");

    Ok(())
}

/// Exercises bloom_may_contain_key with a full (non-partitioned) filter
/// through the merge pipeline — covers the delegation to bloom_may_contain_hash.
#[test_log::test]
fn full_filter_bloom_skip_merge_pipeline() -> lsm_tree::Result<()> {
    use lsm_tree::{
        config::PinningPolicy, get_tmp_folder, AbstractTree, Config, MergeOperator,
        SequenceNumberCounter, MAX_SEQNO,
    };

    struct SumMerge;
    impl MergeOperator for SumMerge {
        fn merge(
            &self,
            _key: &[u8],
            base_value: Option<&[u8]>,
            operands: &[&[u8]],
        ) -> lsm_tree::Result<lsm_tree::Slice> {
            let mut sum: i64 = base_value
                .map(|b| {
                    i64::from_le_bytes(
                        b.try_into()
                            .expect("invalid base value length for i64 in SumMerge"),
                    )
                })
                .unwrap_or(0);
            for op in operands {
                sum += i64::from_le_bytes(
                    (*op)
                        .try_into()
                        .expect("invalid operand length for i64 in SumMerge"),
                );
            }
            Ok(sum.to_le_bytes().to_vec().into())
        }
    }

    let folder = get_tmp_folder();
    let path = folder.path();

    let seqno = SequenceNumberCounter::default();

    // Full (non-partitioned) filters — bloom_may_contain_key delegates to hash path
    let tree = Config::new(path, seqno.clone(), SequenceNumberCounter::default())
        .filter_block_partitioning_policy(PinningPolicy::all(false))
        .with_merge_operator(Some(std::sync::Arc::new(SumMerge)))
        .open()?;

    tree.insert("counter", &100_i64.to_le_bytes(), seqno.next());
    tree.flush_active_memtable(0)?;

    tree.insert("zzz_other", &999_i64.to_le_bytes(), seqno.next());
    tree.flush_active_memtable(0)?;

    tree.merge("counter", 10_i64.to_le_bytes(), seqno.next());

    let result = tree.get("counter", MAX_SEQNO)?;
    assert!(result.is_some());

    let value = i64::from_le_bytes(result.unwrap().as_ref().try_into().unwrap());
    assert_eq!(110, value, "merge(100, [10]) = 110");

    Ok(())
}

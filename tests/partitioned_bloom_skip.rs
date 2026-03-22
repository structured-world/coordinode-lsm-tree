/// Tests that partitioned bloom filters correctly skip non-matching keys
/// via the Table::get path (which has partition-aware bloom seeking).
///
/// Metrics confirm the filter rejects the table for a non-matching key.
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
        .filter_block_partitioning_policy(PinningPolicy::all(true))
        .open()?;

    tree.insert("a", "val_a", seqno.next());
    tree.insert("c", "val_c", seqno.next());
    tree.flush_active_memtable(0)?;

    assert!(tree.get("b", MAX_SEQNO)?.is_none());

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

    assert!(tree.get("a", MAX_SEQNO)?.is_some());
    assert!(tree.get("c", MAX_SEQNO)?.is_some());

    Ok(())
}

/// Tests that a key beyond all partition boundaries is correctly rejected.
///
/// For keys beyond the table's key range, Table::get skips via key-range
/// overlap check (before bloom). The unit test in table::tests covers the
/// bloom_may_contain_key Ok(false) path directly.
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

    assert!(tree.get("z", MAX_SEQNO)?.is_none());
    assert!(tree.get("a", MAX_SEQNO)?.is_some());

    Ok(())
}

/// Exercises bloom_may_contain_key through the merge pipeline
/// (resolve_merge_via_pipeline → TreeIter → bloom_passes → bloom_may_contain_key).
///
/// With a merge operator, point reads go through the iterator pipeline where
/// bloom_key enables partition-aware filtering. Correctness of the merge
/// result (110 = merge(100, [10])) confirms the pipeline executes without
/// errors through the new bloom_may_contain_key code path.
///
/// Note: io_skipped_by_filter is only incremented by Table::get, not by
/// bloom_passes in the pipeline path, so we assert correctness not metrics.
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

    // Table 2: unrelated key — bloom_may_contain_key should reject this
    tree.insert("zzz_other", &999_i64.to_le_bytes(), seqno.next());
    tree.flush_active_memtable(0)?;

    // Merge operand in active memtable — triggers resolve_merge_via_pipeline
    tree.merge("counter", 10_i64.to_le_bytes(), seqno.next());

    let result = tree.get("counter", MAX_SEQNO)?;
    assert!(result.is_some());

    let value = i64::from_le_bytes(result.unwrap().as_ref().try_into().unwrap());
    assert_eq!(110, value, "merge(100, [10]) = 110");

    Ok(())
}

/// Exercises bloom_may_contain_key with a full (non-partitioned) filter
/// through the merge pipeline — covers the delegation to bloom_may_contain_hash.
///
/// Same note as above: pipeline bloom skips don't increment io_skipped_by_filter.
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

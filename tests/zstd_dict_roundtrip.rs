// Integration test: zstd dictionary compression roundtrip
//
// Verifies that data written with zstd dictionary compression can be read back
// correctly through the full Tree API (write → flush → read) and that the
// various read paths continue to work correctly when a zstd dictionary is used.

#[cfg(feature = "zstd")]
mod zstd_dict {
    use lsm_tree::{
        AbstractTree,
        CompressionType,
        Config,
        Guard, // trait import — required for IterGuardImpl::into_inner()
        SequenceNumberCounter,
        ZstdDictionary,
        config::CompressionPolicy,
    };
    use std::sync::Arc;

    /// Build a synthetic dictionary from repetitive sample data.
    /// Real workloads would use `zstd --train` or `zstd::dict::from_continuous`.
    fn make_test_dictionary() -> ZstdDictionary {
        // Repetitive data that mirrors the key/value patterns we'll write.
        let mut samples = Vec::new();
        for i in 0u32..500 {
            let key = format!("key-{i:05}");
            let val = format!("value-{i:05}-padding-to-make-it-longer");
            samples.extend_from_slice(key.as_bytes());
            samples.extend_from_slice(val.as_bytes());
        }
        ZstdDictionary::new(&samples)
    }

    fn make_config(dir: &std::path::Path) -> Config {
        Config::new(
            dir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
    }

    #[test]
    fn tree_write_flush_read_zstd_dict() -> lsm_tree::Result<()> {
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let compression = CompressionType::zstd_dict(3, dict.id())?;

        let tree = make_config(dir.path())
            .data_block_compression_policy(CompressionPolicy::all(compression))
            .zstd_dictionary(Some(Arc::new(dict)))
            .open()?;

        for i in 0u32..200 {
            let key = format!("key-{i:05}");
            let val = format!("value-{i:05}-padding-to-make-it-longer");
            tree.insert(key.as_bytes(), val.as_bytes(), i.into());
        }

        tree.flush_active_memtable(0)?;

        // Verify all data reads back correctly
        for i in 0u32..200 {
            let key = format!("key-{i:05}");
            let expected = format!("value-{i:05}-padding-to-make-it-longer");
            let got = tree
                .get(key.as_bytes(), lsm_tree::MAX_SEQNO)?
                .expect("key should exist");
            assert_eq!(got.as_ref(), expected.as_bytes(), "mismatch at key {key}");
        }

        assert!(tree.get(b"nonexistent", lsm_tree::MAX_SEQNO)?.is_none());
        Ok(())
    }

    #[test]
    fn tree_range_scan_with_zstd_dict() -> lsm_tree::Result<()> {
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let compression = CompressionType::zstd_dict(3, dict.id())?;

        let tree = make_config(dir.path())
            .data_block_compression_policy(CompressionPolicy::all(compression))
            .zstd_dictionary(Some(Arc::new(dict)))
            .open()?;

        for i in 0u32..100 {
            let key = format!("key-{i:05}");
            let val = format!("value-{i:05}");
            tree.insert(key.as_bytes(), val.as_bytes(), i.into());
        }

        tree.flush_active_memtable(0)?;

        // Range scan should work correctly with dictionary compression.
        let items: Vec<_> = tree
            .range(
                "key-00010".as_bytes()..="key-00020".as_bytes(),
                lsm_tree::MAX_SEQNO,
                None,
            )
            .collect();
        assert_eq!(
            items.len(),
            11,
            "range scan should return 11 items (inclusive)"
        );

        // Verify actual key-value content, not just count
        let pairs: Vec<_> = items.into_iter().map(|g| g.into_inner().unwrap()).collect();
        assert_eq!(pairs.first().unwrap().0.as_ref(), b"key-00010");
        assert_eq!(pairs.last().unwrap().0.as_ref(), b"key-00020");

        Ok(())
    }

    #[test]
    fn zstd_dict_with_per_level_policy() -> lsm_tree::Result<()> {
        // Per-level policy: ZstdDict for L0 (exercised by flush), None for deeper.
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let compression = CompressionType::zstd_dict(3, dict.id())?;

        let tree = make_config(dir.path())
            .data_block_compression_policy(CompressionPolicy::new([
                compression,
                CompressionType::None,
            ]))
            .zstd_dictionary(Some(Arc::new(dict)))
            .open()?;

        for i in 0u32..50 {
            let key = format!("key-{i:05}");
            let val = format!("value-{i:05}");
            tree.insert(key.as_bytes(), val.as_bytes(), i.into());
        }

        tree.flush_active_memtable(0)?;

        for i in 0u32..50 {
            let key = format!("key-{i:05}");
            let expected = format!("value-{i:05}");
            let got = tree
                .get(key.as_bytes(), lsm_tree::MAX_SEQNO)?
                .expect("key should exist");
            assert_eq!(got.as_ref(), expected.as_bytes(), "mismatch at key {key}");
        }

        Ok(())
    }

    #[test]
    fn zstd_dict_mismatch_returns_error() -> lsm_tree::Result<()> {
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let wrong_dict = ZstdDictionary::new(b"completely different dictionary content");

        // dict_id in compression type matches wrong_dict, but we provide dict
        let compression = CompressionType::zstd_dict(3, wrong_dict.id())?;

        // Config validation catches the mismatch at open() time
        let result = make_config(dir.path())
            .data_block_compression_policy(CompressionPolicy::all(compression))
            .zstd_dictionary(Some(Arc::new(dict)))
            .open();

        assert!(
            matches!(result, Err(lsm_tree::Error::ZstdDictMismatch { .. })),
            "expected ZstdDictMismatch",
        );

        Ok(())
    }

    #[test]
    fn zstd_dict_missing_returns_error() -> lsm_tree::Result<()> {
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let compression = CompressionType::zstd_dict(3, dict.id())?;

        // ZstdDict compression configured but no dictionary provided
        let result = make_config(dir.path())
            .data_block_compression_policy(CompressionPolicy::all(compression))
            .open();

        assert!(
            matches!(
                result,
                Err(lsm_tree::Error::ZstdDictMismatch { got: None, .. })
            ),
            "expected ZstdDictMismatch with got=None",
        );

        Ok(())
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn zstd_dict_with_encryption() -> lsm_tree::Result<()> {
        use lsm_tree::Aes256GcmProvider;

        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let compression = CompressionType::zstd_dict(3, dict.id())?;
        let encryption = Arc::new(Aes256GcmProvider::new(&[0x42; 32]));

        let tree = make_config(dir.path())
            .data_block_compression_policy(CompressionPolicy::all(compression))
            .zstd_dictionary(Some(Arc::new(dict)))
            .with_encryption(Some(encryption))
            .open()?;

        for i in 0u32..100 {
            let key = format!("key-{i:05}");
            let val = format!("value-{i:05}-encrypted-and-dict-compressed");
            tree.insert(key.as_bytes(), val.as_bytes(), i.into());
        }

        tree.flush_active_memtable(0)?;

        for i in 0u32..100 {
            let key = format!("key-{i:05}");
            let expected = format!("value-{i:05}-encrypted-and-dict-compressed");
            let got = tree
                .get(key.as_bytes(), lsm_tree::MAX_SEQNO)?
                .expect("key should exist");
            assert_eq!(got.as_ref(), expected.as_bytes(), "mismatch at key {key}");
        }

        Ok(())
    }

    #[test]
    fn zstd_dict_survives_major_compaction() -> lsm_tree::Result<()> {
        // Verifies that dictionary-compressed data is correctly preserved through
        // the full compaction cycle: three L0 SSTs are flushed, then major_compact
        // merges them into L1, decompressing source blocks and re-compressing the
        // output with the same ZstdDict policy.  Both compress_with_dict and
        // decompress_with_dict are exercised on the compaction hot path.
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let compression = CompressionType::zstd_dict(3, dict.id())?;

        let tree = make_config(dir.path())
            .data_block_compression_policy(CompressionPolicy::all(compression))
            .zstd_dictionary(Some(Arc::new(dict)))
            .open()?;

        // Three separate flushes → three L0 SSTs
        for batch in 0u32..3 {
            for i in 0u32..100 {
                let key = format!("key-{batch:02}-{i:04}");
                let val = format!("value-{batch:02}-{i:04}-padding-to-make-it-longer");
                tree.insert(key.as_bytes(), val.as_bytes(), (batch * 100 + i).into());
            }
            tree.flush_active_memtable(0)?;
        }

        assert!(
            tree.table_count() >= 3,
            "expected at least 3 tables before compaction; got {}",
            tree.table_count()
        );

        tree.major_compact(u64::MAX, 0)?;

        // Verify compaction actually ran: L0 must be empty after major compaction.
        // If major_compact() ever regresses to a no-op, this guard catches it before
        // the read assertions, which would otherwise pass against the original L0 tables.
        assert_eq!(
            Some(0),
            tree.level_table_count(0),
            "L0 must be empty after major_compact — compaction may not have run"
        );

        // All 300 keys must be readable after compaction.
        for batch in 0u32..3 {
            for i in 0u32..100 {
                let key = format!("key-{batch:02}-{i:04}");
                let expected = format!("value-{batch:02}-{i:04}-padding-to-make-it-longer");
                let got = tree
                    .get(key.as_bytes(), lsm_tree::MAX_SEQNO)?
                    .unwrap_or_else(|| panic!("key {key} missing after compaction"));
                assert_eq!(
                    got.as_ref(),
                    expected.as_bytes(),
                    "value mismatch for {key} after compaction"
                );
            }
        }

        // Range scan across the compacted SST must also work.
        let items: Vec<_> = tree
            .range(
                "key-01-0000".as_bytes()..="key-01-0009".as_bytes(),
                lsm_tree::MAX_SEQNO,
                None,
            )
            .collect();
        assert_eq!(
            items.len(),
            10,
            "range scan after compaction should return 10 items"
        );

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Blob-file (KV-separation) tests
    // -------------------------------------------------------------------------

    /// Build KvSeparationOptions that force every value into a blob file,
    /// compress blobs with ZstdDict, and attach the matching dictionary.
    fn make_blob_opts(
        compression: lsm_tree::CompressionType,
        dict: Arc<lsm_tree::ZstdDictionary>,
    ) -> lsm_tree::KvSeparationOptions {
        lsm_tree::KvSeparationOptions::default()
            .compression(compression)
            // separation_threshold = 1 forces every non-empty value into a blob file
            .separation_threshold(1)
            .dict(dict)
    }

    #[test]
    fn blob_zstd_dict_roundtrip_write_flush_read() -> lsm_tree::Result<()> {
        // Round-trip: write blobs compressed with ZstdDict, flush to disk, read back.
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let compression = lsm_tree::CompressionType::zstd_dict(3, dict.id())?;
        let dict_arc = Arc::new(dict);

        let tree = make_config(dir.path())
            .with_kv_separation(Some(make_blob_opts(compression, dict_arc)))
            .open()?;

        let big_value = b"blob-value-".repeat(20);

        for i in 0u32..50 {
            let key = format!("key-{i:04}");
            tree.insert(key.as_bytes(), &big_value, i.into());
        }

        tree.flush_active_memtable(0)?;

        assert!(
            tree.blob_file_count() >= 1,
            "at least one blob file should exist after flush"
        );

        for i in 0u32..50 {
            let key = format!("key-{i:04}");
            let got = tree
                .get(key.as_bytes(), lsm_tree::MAX_SEQNO)?
                .unwrap_or_else(|| panic!("key {key} missing"));
            assert_eq!(
                got.as_ref(),
                big_value.as_slice(),
                "value mismatch for key {key}",
            );
        }

        Ok(())
    }

    #[test]
    fn blob_zstd_dict_roundtrip_survives_major_compact() -> lsm_tree::Result<()> {
        // Verifies that blob files compressed with ZstdDict survive major compaction
        // (relocation path reads with dict, writes with dict).
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let compression = lsm_tree::CompressionType::zstd_dict(3, dict.id())?;
        let dict_arc = Arc::new(dict);

        let tree = make_config(dir.path())
            .with_kv_separation(Some(make_blob_opts(compression, dict_arc)))
            .open()?;

        let big_value = b"compacted-blob-value-".repeat(15);

        // Two flushes to have multiple tables/blob files
        for i in 0u32..30 {
            let key = format!("key-{i:04}");
            tree.insert(key.as_bytes(), &big_value, i.into());
        }
        tree.flush_active_memtable(0)?;

        for i in 30u32..60 {
            let key = format!("key-{i:04}");
            tree.insert(key.as_bytes(), &big_value, i.into());
        }
        tree.flush_active_memtable(0)?;

        tree.major_compact(u64::MAX, 0)?;
        assert_eq!(
            Some(0),
            tree.level_table_count(0),
            "L0 must be empty after major_compact — compaction may not have run",
        );

        for i in 0u32..60 {
            let key = format!("key-{i:04}");
            let got = tree
                .get(key.as_bytes(), lsm_tree::MAX_SEQNO)?
                .unwrap_or_else(|| panic!("key {key} missing after major_compact"));
            assert_eq!(
                got.as_ref(),
                big_value.as_slice(),
                "value mismatch for {key} after major_compact",
            );
        }

        Ok(())
    }

    #[test]
    fn blob_zstd_dict_missing_at_open_is_rejected() -> lsm_tree::Result<()> {
        // ZstdDict compression configured for blobs, but no dictionary provided at open.
        // Config::validate_zstd_dictionary must catch this before any I/O.
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let compression = lsm_tree::CompressionType::zstd_dict(3, dict.id())?;

        let result = make_config(dir.path())
            .with_kv_separation(Some(
                lsm_tree::KvSeparationOptions::default()
                    .compression(compression)
                    .separation_threshold(1),
                // deliberately omit .dict(...)
            ))
            .open();

        let expected_id = dict.id();
        assert!(
            matches!(
                result,
                Err(lsm_tree::Error::ZstdDictMismatch { expected, got: None })
                    if expected == expected_id
            ),
            "expected ZstdDictMismatch{{expected: {expected_id}, got: None}} when dict is missing",
        );

        Ok(())
    }

    #[test]
    fn blob_zstd_dict_id_mismatch_at_open_is_rejected() -> lsm_tree::Result<()> {
        // dict_id in CompressionType does not match the actual dictionary provided.
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let wrong_dict = ZstdDictionary::new(b"entirely different content for wrong dict");
        // compression claims to need wrong_dict.id(), but we provide dict
        let expected_id = wrong_dict.id();
        let provided_id = dict.id();
        let compression = lsm_tree::CompressionType::zstd_dict(3, expected_id)?;

        let result = make_config(dir.path())
            .with_kv_separation(Some(
                lsm_tree::KvSeparationOptions::default()
                    .compression(compression)
                    .separation_threshold(1)
                    .dict(Arc::new(dict)),
            ))
            .open();

        assert!(
            matches!(
                result,
                Err(lsm_tree::Error::ZstdDictMismatch {
                    expected,
                    got: Some(actual),
                }) if expected == expected_id && actual == provided_id
            ),
            "expected ZstdDictMismatch{{expected: {expected_id}, got: Some({provided_id})}}",
        );

        Ok(())
    }

    #[test]
    fn blob_zstd_dict_range_scan() -> lsm_tree::Result<()> {
        // Range-scan and prefix-scan resolve blob indirections through the
        // iterator path (Guard::value → resolve_value_handle).  Verify that
        // the dict is threaded correctly through that path.
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let compression = lsm_tree::CompressionType::zstd_dict(3, dict.id())?;
        let dict_arc = Arc::new(dict);

        let tree = make_config(dir.path())
            .with_kv_separation(Some(make_blob_opts(compression, dict_arc)))
            .open()?;

        let big_value = b"range-blob-value-".repeat(10);

        for i in 0u32..40 {
            let key = format!("key-{i:04}");
            tree.insert(key.as_bytes(), &big_value, i.into());
        }
        tree.flush_active_memtable(0)?;

        // Inclusive range scan
        let items: Vec<_> = tree
            .range(
                "key-0010".as_bytes()..="key-0019".as_bytes(),
                lsm_tree::MAX_SEQNO,
                None,
            )
            .collect();
        assert_eq!(items.len(), 10, "range scan should return 10 blob items");

        // Consume items via into_inner to resolve blob indirections
        for g in items {
            let (_, val) = g.into_inner()?;
            assert_eq!(val.as_ref(), big_value.as_slice());
        }

        // Prefix scan — also resolve blob indirections via into_inner to exercise
        // the zstd-dict decompression path through the prefix iterator.
        let prefix_items: Vec<_> = tree.prefix("key-002", lsm_tree::MAX_SEQNO, None).collect();
        assert_eq!(
            prefix_items.len(),
            10,
            "prefix scan should return 10 blob items"
        );
        for g in prefix_items {
            let (_, val) = g.into_inner()?;
            assert_eq!(val.as_ref(), big_value.as_slice());
        }

        Ok(())
    }

    #[test]
    fn blob_zstd_dict_multi_get() -> lsm_tree::Result<()> {
        // multi_get resolves blob indirections via a separate code path
        // (blob_tree::multi_get → resolve_value_handle).  Verify dict threads correctly.
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let compression = lsm_tree::CompressionType::zstd_dict(3, dict.id())?;
        let dict_arc = Arc::new(dict);

        let tree = make_config(dir.path())
            .with_kv_separation(Some(make_blob_opts(compression, dict_arc)))
            .open()?;

        let big_value = b"multi-get-blob-value-".repeat(10);

        tree.insert(b"alpha", &big_value, 0);
        tree.insert(b"beta", &big_value, 1);
        tree.insert(b"gamma", &big_value, 2);
        tree.flush_active_memtable(0)?;

        let results = tree.multi_get(["alpha", "beta", "gamma", "missing"], lsm_tree::MAX_SEQNO)?;

        assert_eq!(results.len(), 4);
        assert_eq!(results[0].as_deref(), Some(big_value.as_slice()), "alpha");
        assert_eq!(results[1].as_deref(), Some(big_value.as_slice()), "beta");
        assert_eq!(results[2].as_deref(), Some(big_value.as_slice()), "gamma");
        assert!(results[3].is_none(), "missing key should return None");

        Ok(())
    }

    #[test]
    fn reopen_with_wrong_dict_fails_at_recovery() -> lsm_tree::Result<()> {
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let compression = CompressionType::zstd_dict(3, dict.id())?;

        // Write data with dict A
        {
            let tree = make_config(dir.path())
                .data_block_compression_policy(CompressionPolicy::all(compression))
                .zstd_dictionary(Some(Arc::new(dict.clone())))
                .open()?;

            tree.insert(b"key", b"value", 0);
            tree.flush_active_memtable(0)?;
        }

        // Reopen with dict B → should fail at recovery
        let wrong_dict = ZstdDictionary::new(b"completely different dictionary bytes");
        let wrong_compression = CompressionType::zstd_dict(3, wrong_dict.id())?;
        let result = make_config(dir.path())
            .data_block_compression_policy(CompressionPolicy::all(wrong_compression))
            .zstd_dictionary(Some(Arc::new(wrong_dict)))
            .open();

        assert!(
            matches!(result, Err(lsm_tree::Error::ZstdDictMismatch { .. })),
            "expected ZstdDictMismatch on reopen with wrong dict",
        );

        Ok(())
    }
}

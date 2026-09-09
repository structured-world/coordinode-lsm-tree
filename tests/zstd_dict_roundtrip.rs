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

    /// Removes the manifest and its version pointer, so the next open has to
    /// rebuild from the tables on disk.
    ///
    /// Asserts that it removed something: matching by name means a change to the
    /// naming would silently leave the manifest intact, and every test that
    /// depends on this would then quietly assert only that a repair over a
    /// HEALTHY manifest works.
    fn lose_the_manifest(dir: &std::path::Path) -> lsm_tree::Result<()> {
        let mut removed = 0;
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let is_version = name
                .strip_prefix('v')
                .is_some_and(|rest| rest.parse::<u64>().is_ok());
            if is_version || name == "current" {
                std::fs::remove_file(entry.path())?;
                removed += 1;
            }
        }
        assert!(
            removed > 0,
            "no manifest file matched: the naming changed and this helper stopped losing anything",
        );
        Ok(())
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
    fn a_blob_trees_dictionary_becomes_tree_state_like_a_tables() -> lsm_tree::Result<()> {
        // A KV-separated tree compresses its blob files against a dictionary of
        // their own. It has to be stored and registered exactly like the table
        // one, or the blob files are readable only while the caller keeps
        // supplying the bytes — and a checkpoint of such a tree cannot be opened
        // at all.
        let dir = tempfile::tempdir()?;
        let target = tempfile::tempdir()?;
        let checkpoint = target.path().join("snapshot");
        let dict = make_test_dictionary();
        let dict_id = dict.id();
        let compression = CompressionType::zstd_dict(3, dict_id)?;
        let big_value = b"blob-value-".repeat(20);

        {
            let tree = make_config(dir.path())
                .with_kv_separation(Some(make_blob_opts(compression, Arc::new(dict))))
                .open()?;
            for i in 0u32..50 {
                let key = format!("key-{i:04}");
                tree.insert(key.as_bytes(), &big_value, i.into());
            }
            tree.flush_active_memtable(0)?;
            assert!(tree.blob_file_count() >= 1, "a blob file must exist");

            tree.create_checkpoint(&checkpoint)?;
        }

        assert!(
            dir.path().join("dicts").join(dict_id.to_string()).exists(),
            "the blob dictionary is stored in the tree, not only in the config",
        );
        assert!(
            checkpoint.join("dicts").join(dict_id.to_string()).exists(),
            "and travels into a checkpoint with the blob files it decodes",
        );

        // The checkpoint opens on its own: the same blob compression policy, but
        // NO dictionary supplied — it is resolved from the copy beside the blob
        // files.
        let restored = make_config(&checkpoint)
            .with_kv_separation(Some(
                lsm_tree::KvSeparationOptions::default()
                    .separation_threshold(1)
                    .compression(compression),
            ))
            .open()?;
        for i in 0u32..50 {
            let key = format!("key-{i:04}");
            assert_eq!(
                restored
                    .get(key.as_bytes(), lsm_tree::MAX_SEQNO)?
                    .as_deref(),
                Some(big_value.as_slice()),
                "blob value for {key} must survive the checkpoint",
            );
        }
        Ok(())
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
    fn a_tree_reopens_with_no_dictionary_in_the_config() -> lsm_tree::Result<()> {
        // The defect this whole mechanism exists for: the dictionary bytes used
        // to live only in the caller's config, so losing that file (or simply
        // not passing it again) made every dictionary-compressed table
        // unreadable. The tree keeps them now.
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let compression = CompressionType::zstd_dict(3, dict.id())?;

        {
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
        }

        // Not one dictionary supplied, and no compression policy either: this
        // is a caller that has forgotten the dictionary ever existed.
        let reopened = make_config(dir.path()).open()?;

        for i in 0u32..200 {
            let key = format!("key-{i:05}");
            let expected = format!("value-{i:05}-padding-to-make-it-longer");
            let got = reopened
                .get(key.as_bytes(), lsm_tree::MAX_SEQNO)?
                .unwrap_or_else(|| panic!("key {key} unreadable after reopen without the dict"));
            assert_eq!(got.as_ref(), expected.as_bytes());
        }

        Ok(())
    }

    #[test]
    fn a_dictionary_can_be_introduced_on_a_tree_that_already_holds_data() -> lsm_tree::Result<()> {
        // A tree is created empty, so there is nothing to train on at creation
        // time. Introducing a dictionary later has to leave the tables written
        // before it readable, or the realistic flow (run unconditioned, then
        // train on what accumulated) is impossible.
        let dir = tempfile::tempdir()?;

        {
            let tree = make_config(dir.path()).open()?;
            for i in 0u32..100 {
                let key = format!("plain-{i:05}");
                tree.insert(key.as_bytes(), b"written-before-any-dictionary", i.into());
            }
            tree.flush_active_memtable(0)?;
        }

        let dict = make_test_dictionary();
        let compression = CompressionType::zstd_dict(3, dict.id())?;
        {
            let tree = make_config(dir.path())
                .data_block_compression_policy(CompressionPolicy::all(compression))
                .zstd_dictionary(Some(Arc::new(dict)))
                .open()?;

            for i in 0u32..100 {
                let key = format!("dict-{i:05}");
                let val = format!("value-{i:05}-padding-to-make-it-longer");
                tree.insert(key.as_bytes(), val.as_bytes(), (1000 + i).into());
            }
            tree.flush_active_memtable(0)?;

            // Both generations readable in the same tree.
            assert!(tree.get(b"plain-00000", lsm_tree::MAX_SEQNO)?.is_some());
            assert!(tree.get(b"dict-00000", lsm_tree::MAX_SEQNO)?.is_some());
        }

        // And after a reopen with nothing supplied.
        let reopened = make_config(dir.path()).open()?;
        assert_eq!(
            reopened
                .get(b"plain-00042", lsm_tree::MAX_SEQNO)?
                .as_deref(),
            Some(b"written-before-any-dictionary".as_slice()),
        );
        assert_eq!(
            reopened.get(b"dict-00042", lsm_tree::MAX_SEQNO)?.as_deref(),
            Some(b"value-00042-padding-to-make-it-longer".as_slice()),
        );

        Ok(())
    }

    #[test]
    fn a_second_dictionary_leaves_the_first_generation_readable() -> lsm_tree::Result<()> {
        // Replacing a dictionary is the same operation as introducing one, and
        // has the same requirement: the tables written under the previous one
        // keep resolving to it. This is what a single `Config` slot could not
        // express — it held exactly one dictionary for everything.
        let dir = tempfile::tempdir()?;
        let first = make_test_dictionary();
        let second = ZstdDictionary::new(&b"a second dictionary with different content".repeat(40));
        assert_ne!(first.id(), second.id());

        {
            let tree = make_config(dir.path())
                .data_block_compression_policy(CompressionPolicy::all(CompressionType::zstd_dict(
                    3,
                    first.id(),
                )?))
                .zstd_dictionary(Some(Arc::new(first)))
                .open()?;
            for i in 0u32..100 {
                let key = format!("first-{i:05}");
                let val = format!("value-{i:05}-padding-to-make-it-longer");
                tree.insert(key.as_bytes(), val.as_bytes(), i.into());
            }
            tree.flush_active_memtable(0)?;
        }

        let second_id = second.id();
        {
            // Opened under a DIFFERENT dictionary than the data on disk was
            // written with. Before the tree owned its dictionaries this was a
            // hard failure; now the new one is what new blocks are written
            // against and the old one is still what the old blocks resolve to.
            let tree = make_config(dir.path())
                .data_block_compression_policy(CompressionPolicy::all(CompressionType::zstd_dict(
                    3, second_id,
                )?))
                .zstd_dictionary(Some(Arc::new(second)))
                .open()?;

            for i in 0u32..100 {
                let key = format!("second-{i:05}");
                let val = format!("value-{i:05}-padding-to-make-it-longer");
                tree.insert(key.as_bytes(), val.as_bytes(), (1000 + i).into());
            }
            tree.flush_active_memtable(0)?;

            for i in 0u32..100 {
                let expected = format!("value-{i:05}-padding-to-make-it-longer");
                let first_key = format!("first-{i:05}");
                assert_eq!(
                    tree.get(first_key.as_bytes(), lsm_tree::MAX_SEQNO)?
                        .as_deref(),
                    Some(expected.as_bytes()),
                    "data written under the first dictionary must stay readable",
                );
                let second_key = format!("second-{i:05}");
                assert_eq!(
                    tree.get(second_key.as_bytes(), lsm_tree::MAX_SEQNO)?
                        .as_deref(),
                    Some(expected.as_bytes()),
                );
            }
        }

        // Both survive a reopen with nothing supplied.
        let reopened = make_config(dir.path()).open()?;
        assert!(reopened.get(b"first-00007", lsm_tree::MAX_SEQNO)?.is_some());
        assert!(
            reopened
                .get(b"second-00007", lsm_tree::MAX_SEQNO)?
                .is_some()
        );

        Ok(())
    }

    #[test]
    fn a_second_blob_dictionary_leaves_the_first_generation_readable() -> lsm_tree::Result<()> {
        // The blob twin of the test above. A blob file records the dictionary it
        // was written with exactly as a table does, so rotating the KV
        // dictionary must leave the previous generation readable: the read has
        // to resolve each blob file's own recorded id against the tree's set,
        // not hand every file whatever the current write policy happens to name.
        let dir = tempfile::tempdir()?;
        let first = make_test_dictionary();
        let second =
            ZstdDictionary::new(&b"a second blob dictionary with different content".repeat(40));
        assert_ne!(first.id(), second.id());
        let big_value = b"blob-value-".repeat(20);

        {
            let tree = make_config(dir.path())
                .with_kv_separation(Some(make_blob_opts(
                    CompressionType::zstd_dict(3, first.id())?,
                    Arc::new(first),
                )))
                .open()?;
            for i in 0u32..50 {
                let key = format!("first-{i:04}");
                tree.insert(key.as_bytes(), &big_value, i.into());
            }
            tree.flush_active_memtable(0)?;
            assert!(tree.blob_file_count() >= 1, "a blob file must exist");
        }

        // Reopened under a DIFFERENT dictionary: the new one is what new blob
        // files are written against, the old one is still what the old ones
        // resolve to. Both are in the tree's set, read from its own folder.
        let tree = make_config(dir.path())
            .with_kv_separation(Some(make_blob_opts(
                CompressionType::zstd_dict(3, second.id())?,
                Arc::new(second),
            )))
            .open()?;
        for i in 0u32..50 {
            let key = format!("second-{i:04}");
            tree.insert(key.as_bytes(), &big_value, (1000 + i).into());
        }
        tree.flush_active_memtable(0)?;

        for i in 0u32..50 {
            let first_key = format!("first-{i:04}");
            assert_eq!(
                tree.get(first_key.as_bytes(), lsm_tree::MAX_SEQNO)?
                    .as_deref(),
                Some(big_value.as_slice()),
                "a blob written under the first dictionary must stay readable",
            );
            let second_key = format!("second-{i:04}");
            assert_eq!(
                tree.get(second_key.as_bytes(), lsm_tree::MAX_SEQNO)?
                    .as_deref(),
                Some(big_value.as_slice()),
            );
        }

        // And through the scan, which resolves handles on a different path (the
        // guard, plus the coalescing prefetch that warms the cache ahead of it).
        let mut scanned = 0;
        for guard in tree.range(
            "first-0000".as_bytes().."first-9999".as_bytes(),
            lsm_tree::MAX_SEQNO,
            None,
        ) {
            let (_, value) = guard.into_inner()?;
            assert_eq!(value.as_ref(), big_value.as_slice());
            scanned += 1;
        }
        assert_eq!(scanned, 50, "every first-generation blob must scan back");

        Ok(())
    }

    #[test]
    fn a_live_reader_keeps_the_dictionary_its_blob_files_need() -> lsm_tree::Result<()> {
        // A reader that captured a version keeps its blob files alive even
        // after `clear` drains the history out from under it. The dictionary
        // those files decode with has to be pinned by the same hold: what the
        // REGISTRY holds is a moving target, and the collection that follows a
        // clear takes an id no retained version names any more.
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let dict_id = dict.id();
        let big_value = b"blob-value-".repeat(20);

        {
            let tree = make_config(dir.path())
                .with_kv_separation(Some(make_blob_opts(
                    CompressionType::zstd_dict(3, dict_id)?,
                    Arc::new(dict),
                )))
                .open()?;
            for i in 0u32..50 {
                let key = format!("key-{i:04}");
                tree.insert(key.as_bytes(), &big_value, i.into());
            }
            tree.flush_active_memtable(0)?;
        }

        // Reopened WITHOUT that dictionary in the write policy, so the only
        // thing that still needs it is the blob files already written under it.
        let tree = make_config(dir.path())
            .with_kv_separation(Some(
                lsm_tree::KvSeparationOptions::default().separation_threshold(1),
            ))
            .open()?;
        let lsm_tree::AnyTree::Blob(blob) = &tree else {
            panic!("a blob tree");
        };

        // Captured, not yet resolved: each guard holds its own version, which
        // is what defers the deletion of the blob files under it.
        let pending: Vec<_> = tree
            .range(
                "key-0000".as_bytes().."key-9999".as_bytes(),
                lsm_tree::MAX_SEQNO,
                None,
            )
            .collect();
        assert_eq!(pending.len(), 50, "the whole generation is captured");

        tree.clear()?;
        assert_eq!(
            blob.index.collect_unreferenced_dictionaries()?,
            1,
            "no retained version names it once the history is drained",
        );
        assert!(
            !dir.path().join("dicts").join(dict_id.to_string()).exists(),
            "its file is unlinked",
        );

        // The reader is still owed its values, and the bytes it needs to
        // decode them are held by the blob files it pinned.
        for guard in pending {
            let (_, value) = guard.into_inner()?;
            assert_eq!(value.as_ref(), big_value.as_slice());
        }
        Ok(())
    }

    #[test]
    fn relocation_keeps_the_codec_its_frames_were_written_under() -> lsm_tree::Result<()> {
        // A relocation copies blob frames VERBATIM: it never re-encodes them.
        // The file it produces must therefore record the codec those frames
        // actually carry, not whatever the tree's blob policy says today, or
        // the bytes and the descriptor disagree and the file stops decoding.
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let big_value = b"blob-value-".repeat(20);
        let replacement = b"second-generation-value-".repeat(15);

        {
            let tree = make_config(dir.path())
                .with_kv_separation(Some(
                    make_blob_opts(CompressionType::zstd_dict(3, dict.id())?, Arc::new(dict))
                        .staleness_threshold(0.0)
                        .age_cutoff(1.0),
                ))
                .open()?;
            for i in 0u32..50 {
                let key = format!("key-{i:04}");
                tree.insert(key.as_bytes(), &big_value, i.into());
            }
            tree.flush_active_memtable(0)?;
            // Overwrite half, so the first blob file carries dead bytes and a
            // relocation pass has a reason to rewrite it.
            for i in 0u32..25 {
                let key = format!("key-{i:04}");
                tree.insert(key.as_bytes(), &replacement, (100 + i).into());
            }
            tree.flush_active_memtable(0)?;
            // Above every seqno written, so the superseded versions are
            // actually dropped: that is what records dead bytes in the
            // version's fragmentation map and makes the file relocatable.
            tree.major_compact(u64::MAX, 100_000)?;
        }

        let blob_ids = |dir: &std::path::Path| -> lsm_tree::Result<Vec<String>> {
            let mut names = Vec::new();
            for entry in std::fs::read_dir(dir.join("blobs"))? {
                names.push(entry?.file_name().to_string_lossy().into_owned());
            }
            names.sort();
            Ok(names)
        };
        let before = blob_ids(dir.path())?;

        // The blob policy changes. Files already written keep THEIR codec.
        let tree = make_config(dir.path())
            .with_kv_separation(Some(
                lsm_tree::KvSeparationOptions::default()
                    .separation_threshold(1)
                    .staleness_threshold(0.0)
                    .age_cutoff(1.0),
            ))
            .open()?;
        // Which compaction picks the stale file up is a scheduling decision, so
        // drive it until it happens rather than assuming one pass suffices.
        // The test is worthless if the relocation never runs, so failing to
        // provoke it is a failure, not a silent pass.
        let mut relocated = false;
        for round in 0u32..5 {
            let key = format!("filler-{round:04}");
            tree.insert(key.as_bytes(), &replacement, (200 + round).into());
            tree.flush_active_memtable(0)?;
            tree.major_compact(u64::MAX, 100_000)?;

            let after = blob_ids(dir.path())?;
            if before.iter().any(|id| !after.contains(id)) {
                relocated = true;
                break;
            }
        }
        assert!(
            relocated,
            "no generation-A blob file was relocated: before={before:?} after={:?}",
            blob_ids(dir.path())?,
        );

        for i in 25u32..50 {
            let key = format!("key-{i:04}");
            assert_eq!(
                tree.get(key.as_bytes(), lsm_tree::MAX_SEQNO)?.as_deref(),
                Some(big_value.as_slice()),
                "a relocated blob must still decode under its own codec",
            );
        }
        for i in 0u32..25 {
            let key = format!("key-{i:04}");
            assert_eq!(
                tree.get(key.as_bytes(), lsm_tree::MAX_SEQNO)?.as_deref(),
                Some(replacement.as_slice()),
            );
        }
        Ok(())
    }

    #[test]
    fn a_blob_file_naming_a_dictionary_the_tree_lost_reports_that_id() -> lsm_tree::Result<()> {
        // The blob twin of the table rule. A table whose recorded id the tree
        // no longer holds refuses to open and names the id; a blob file has to
        // do the same, or the open succeeds and every value in that file turns
        // into a failed read later, far from the cause. Reaching the tree's
        // dictionary folder is what makes this recoverable: the operator can
        // put the file back.
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let dict_id = dict.id();
        let big_value = b"blob-value-".repeat(20);

        {
            let tree = make_config(dir.path())
                .with_kv_separation(Some(make_blob_opts(
                    CompressionType::zstd_dict(3, dict_id)?,
                    Arc::new(dict),
                )))
                .open()?;
            for i in 0u32..20 {
                let key = format!("key-{i:04}");
                tree.insert(key.as_bytes(), &big_value, i.into());
            }
            tree.flush_active_memtable(0)?;
        }

        // The dictionary goes missing while the blob files that name it stay.
        std::fs::remove_file(dir.path().join("dicts").join(dict_id.to_string()))?;

        let err = make_config(dir.path())
            .with_kv_separation(Some(
                lsm_tree::KvSeparationOptions::default().separation_threshold(1),
            ))
            .open()
            .err()
            .expect("a blob file whose dictionary is gone must refuse the open");
        match err {
            lsm_tree::Error::ZstdDictMismatch { expected, .. } => assert_eq!(expected, dict_id),
            other => panic!("expected ZstdDictMismatch naming {dict_id}, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn a_dictionary_still_in_use_is_never_collected() -> lsm_tree::Result<()> {
        // The direction that matters: collection must not take a dictionary
        // the tables still need, or the tree loses the ability to read itself.
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let compression = CompressionType::zstd_dict(3, dict.id())?;

        let tree = make_config(dir.path())
            .data_block_compression_policy(CompressionPolicy::all(compression))
            .zstd_dictionary(Some(Arc::new(dict)))
            .open()?;

        for i in 0u32..100 {
            let key = format!("key-{i:05}");
            let val = format!("value-{i:05}-padding-to-make-it-longer");
            tree.insert(key.as_bytes(), val.as_bytes(), i.into());
        }
        tree.flush_active_memtable(0)?;

        let lsm_tree::AnyTree::Standard(standard) = &tree else {
            panic!("a standard tree");
        };
        assert_eq!(
            standard.collect_unreferenced_dictionaries()?,
            0,
            "the dictionary every table is compressed against is still referenced",
        );

        for i in 0u32..100 {
            let key = format!("key-{i:05}");
            assert!(
                tree.get(key.as_bytes(), lsm_tree::MAX_SEQNO)?.is_some(),
                "the tree must still read itself after a collection pass",
            );
        }
        Ok(())
    }

    #[test]
    fn the_dictionary_new_blocks_are_written_against_is_never_collected() -> lsm_tree::Result<()> {
        // "No table uses it" is not the same as "nothing will". A collection
        // that runs before the first dictionary-compressed flush would take the
        // very dictionary the write policy is about to compress against, and
        // the table written next names an id the tree no longer holds.
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let dict_id = dict.id();
        let compression = CompressionType::zstd_dict(3, dict_id)?;

        {
            let tree = make_config(dir.path())
                .data_block_compression_policy(CompressionPolicy::all(compression))
                .zstd_dictionary(Some(Arc::new(dict)))
                .open()?;
            let lsm_tree::AnyTree::Standard(standard) = &tree else {
                panic!("a standard tree");
            };

            // Nothing is written yet, so no table references the dictionary.
            // Unregistering it here is what goes wrong: the write policy names
            // it, the tables written next are compressed against it, and
            // nothing puts the id back into the version.
            assert_eq!(
                standard.collect_unreferenced_dictionaries()?,
                0,
                "the write policy's dictionary is referenced by what comes next",
            );

            for i in 0u32..100 {
                let key = format!("key-{i:05}");
                let val = format!("value-{i:05}-padding-to-make-it-longer");
                tree.insert(key.as_bytes(), val.as_bytes(), i.into());
            }
            tree.flush_active_memtable(0)?;

            // Churn the history so the versions written before the collection
            // above are pruned: what keeps the file alive from here on is the
            // LATEST version's registration, and that is what the first pass
            // dropped.
            tree.major_compact(u64::MAX, 1_000)?;

            // A second pass, now that the tables DO reference it. The version
            // must still name the id, or this is the pass that unlinks the file
            // out from under them.
            assert_eq!(standard.collect_unreferenced_dictionaries()?, 0);
            assert!(
                dir.path().join("dicts").join(dict_id.to_string()).exists(),
                "the dictionary the live tables are compressed against is still there",
            );
        }

        // The tables written after the collection still resolve on a reopen
        // that supplies nothing.
        let reopened = make_config(dir.path()).open()?;
        assert_eq!(
            reopened.get(b"key-00042", lsm_tree::MAX_SEQNO)?.as_deref(),
            Some(b"value-00042-padding-to-make-it-longer".as_slice()),
        );
        Ok(())
    }

    #[test]
    fn a_dictionary_no_table_uses_is_collected() -> lsm_tree::Result<()> {
        // Write under the dictionary, then rewrite every table WITHOUT it: the
        // dictionary becomes dead weight and the tree should stop carrying it.
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let dict_id = dict.id();
        let compression = CompressionType::zstd_dict(3, dict_id)?;

        {
            let tree = make_config(dir.path())
                .data_block_compression_policy(CompressionPolicy::all(compression))
                .zstd_dictionary(Some(Arc::new(dict)))
                .open()?;
            for i in 0u32..100 {
                let key = format!("key-{i:05}");
                tree.insert(
                    key.as_bytes(),
                    b"value-written-under-the-dictionary",
                    i.into(),
                );
            }
            tree.flush_active_memtable(0)?;
        }

        // The file is there while a table references it.
        assert!(dir.path().join("dicts").join(dict_id.to_string()).exists());

        {
            let tree = make_config(dir.path())
                .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
                .open()?;
            // Rewrite everything under the no-dictionary policy.
            tree.major_compact(u64::MAX, 0)?;

            let lsm_tree::AnyTree::Standard(standard) = &tree else {
                panic!("a standard tree");
            };

            // Stage 1 only: the latest version stops registering the id, but
            // the version that named it is STILL RETAINED and can still be read
            // from, so unlinking the file now would break exactly that read.
            assert_eq!(
                standard.collect_unreferenced_dictionaries()?,
                0,
                "a dictionary a retained version still names is not unlinked",
            );
            assert!(
                dir.path().join("dicts").join(dict_id.to_string()).exists(),
                "its file survives while a retained version names it",
            );
        }

        // Stage 2: after a reopen the history is one version, and that version
        // no longer registers the id, so the file is finally collectable.
        let tree = make_config(dir.path())
            .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
            .open()?;
        let lsm_tree::AnyTree::Standard(standard) = &tree else {
            panic!("a standard tree");
        };
        assert_eq!(
            standard.collect_unreferenced_dictionaries()?,
            1,
            "the dictionary no version names any more is collected",
        );
        assert!(
            !dir.path().join("dicts").join(dict_id.to_string()).exists(),
            "its file is gone",
        );

        // And the data is still readable: it was rewritten, not lost.
        for i in 0u32..100 {
            let key = format!("key-{i:05}");
            assert_eq!(
                tree.get(key.as_bytes(), lsm_tree::MAX_SEQNO)?.as_deref(),
                Some(b"value-written-under-the-dictionary".as_slice()),
            );
        }
        Ok(())
    }

    #[test]
    fn two_trees_from_one_cloned_config_keep_separate_dictionary_sets() -> lsm_tree::Result<()> {
        // A `Config` is `Clone`, and a keyspace clones one base config per
        // partition. The registry a tree loads at open therefore has to belong
        // to THAT tree: sharing it would let the second open replace the first
        // tree's set, and the first tree would stop resolving its own tables.
        let first_dir = tempfile::tempdir()?;
        let second_dir = tempfile::tempdir()?;

        let first_dict = make_test_dictionary();
        let second_dict =
            ZstdDictionary::new(&b"an unrelated corpus for the second tree".repeat(40));
        assert_ne!(first_dict.id(), second_dict.id());

        let base = make_config(first_dir.path());

        let first = base
            .clone()
            .data_block_compression_policy(CompressionPolicy::all(CompressionType::zstd_dict(
                3,
                first_dict.id(),
            )?))
            .zstd_dictionary(Some(Arc::new(first_dict)))
            .open()?;
        for i in 0u32..100 {
            let key = format!("first-{i:05}");
            let val = format!("value-{i:05}-padding-to-make-it-longer");
            first.insert(key.as_bytes(), val.as_bytes(), i.into());
        }

        // The second tree is opened from a CLONE of the same config, pointed at
        // its own directory and its own dictionary.
        let mut second_config = base;
        second_config.path = second_dir.path().into();
        let second = second_config
            .data_block_compression_policy(CompressionPolicy::all(CompressionType::zstd_dict(
                3,
                second_dict.id(),
            )?))
            .zstd_dictionary(Some(Arc::new(second_dict)))
            .open()?;
        for i in 0u32..100 {
            let key = format!("second-{i:05}");
            let val = format!("value-{i:05}-padding-to-make-it-longer");
            second.insert(key.as_bytes(), val.as_bytes(), (1000 + i).into());
        }
        second.flush_active_memtable(0)?;

        // The first tree flushes AFTER the second opened: the table it writes is
        // opened right there, and its dictionary is resolved against whatever
        // registry the first tree is still holding.
        first.flush_active_memtable(0)?;

        // The first tree must read the tables it wrote, from blocks only its own
        // dictionary decodes.
        for i in 0u32..100 {
            let expected = format!("value-{i:05}-padding-to-make-it-longer");
            let key = format!("first-{i:05}");
            assert_eq!(
                first.get(key.as_bytes(), lsm_tree::MAX_SEQNO)?.as_deref(),
                Some(expected.as_bytes()),
                "the second open must not take over the first tree's registry",
            );
            let key = format!("second-{i:05}");
            assert_eq!(
                second.get(key.as_bytes(), lsm_tree::MAX_SEQNO)?.as_deref(),
                Some(expected.as_bytes()),
            );
        }
        Ok(())
    }

    #[test]
    fn a_tree_keeps_writing_under_its_own_dictionary_without_the_config() -> lsm_tree::Result<()> {
        // Supplying the dictionary once is the point of storing it, and that has
        // to hold for WRITING too: a reopen that keeps the same compression
        // policy but no longer carries the bytes must resolve the id the policy
        // names from the tree's own folder.
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let compression = CompressionType::zstd_dict(3, dict.id())?;

        {
            let tree = make_config(dir.path())
                .data_block_compression_policy(CompressionPolicy::all(compression))
                .zstd_dictionary(Some(Arc::new(dict)))
                .open()?;
            for i in 0u32..100 {
                let key = format!("first-{i:05}");
                let val = format!("value-{i:05}-padding-to-make-it-longer");
                tree.insert(key.as_bytes(), val.as_bytes(), i.into());
            }
            tree.flush_active_memtable(0)?;
        }

        // Same policy, no dictionary in the config at all.
        {
            let tree = make_config(dir.path())
                .data_block_compression_policy(CompressionPolicy::all(compression))
                .open()?;
            for i in 0u32..100 {
                let key = format!("second-{i:05}");
                let val = format!("value-{i:05}-padding-to-make-it-longer");
                tree.insert(key.as_bytes(), val.as_bytes(), (1000 + i).into());
            }
            tree.flush_active_memtable(0)?;
        }

        // Both generations read back, and the second was written under the
        // dictionary the tree resolved for itself.
        let reopened = make_config(dir.path()).open()?;
        for i in 0u32..100 {
            let expected = format!("value-{i:05}-padding-to-make-it-longer");
            for prefix in ["first", "second"] {
                let key = format!("{prefix}-{i:05}");
                assert_eq!(
                    reopened
                        .get(key.as_bytes(), lsm_tree::MAX_SEQNO)?
                        .as_deref(),
                    Some(expected.as_bytes()),
                    "{key} must read back",
                );
            }
        }
        Ok(())
    }

    #[test]
    fn a_repair_resolves_the_dictionaries_the_tree_stores() -> lsm_tree::Result<()> {
        // A repair opens every SST it finds, so it needs the same dictionaries a
        // normal open does. It does not go through `Tree::open`, so it has to
        // load the tree's `dicts/` folder itself; without that every
        // dictionary-compressed table is graded unreadable and the rebuilt
        // manifest leaves the whole tree behind.
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let compression = CompressionType::zstd_dict(3, dict.id())?;

        {
            let tree = make_config(dir.path())
                .data_block_compression_policy(CompressionPolicy::all(compression))
                .zstd_dictionary(Some(Arc::new(dict)))
                .open()?;
            for i in 0u32..100 {
                let key = format!("key-{i:05}");
                let val = format!("value-{i:05}-padding-to-make-it-longer");
                tree.insert(key.as_bytes(), val.as_bytes(), i.into());
            }
            tree.flush_active_memtable(0)?;
        }

        // Lose the manifest, then rebuild it with NOTHING supplied: the tree
        // owns its dictionary, and that is the whole point of storing it.
        lose_the_manifest(dir.path())?;

        let report = make_config(dir.path()).repair()?;
        assert_eq!(report.unreadable, 0, "no table should be unreadable");
        assert!(report.recovered >= 1, "the table must be recovered");

        let reopened = make_config(dir.path()).open()?;
        for i in 0u32..100 {
            let key = format!("key-{i:05}");
            let expected = format!("value-{i:05}-padding-to-make-it-longer");
            assert_eq!(
                reopened
                    .get(key.as_bytes(), lsm_tree::MAX_SEQNO)?
                    .as_deref(),
                Some(expected.as_bytes()),
                "every key must survive a repair of a dictionary-compressed tree",
            );
        }
        Ok(())
    }

    /// The tree's first SST, by its numeric name.
    fn a_table_to_corrupt(dir: &std::path::Path) -> lsm_tree::Result<std::path::PathBuf> {
        Ok(std::fs::read_dir(dir.join("tables"))?
            .filter_map(Result::ok)
            .map(|e| e.path())
            .find(|p| {
                p.file_name()
                    .is_some_and(|n| n.to_string_lossy().parse::<u64>().is_ok())
            })
            .expect("an SST to corrupt"))
    }

    /// Flips a byte inside the SST's data section, so the file still opens but
    /// one data block fails its checksum: the shape block salvage exists for.
    fn corrupt_a_data_block(path: &std::path::Path) -> std::io::Result<()> {
        const DEPTH: u64 = 512;
        let pos = {
            let mut f = std::fs::File::open(path)?;
            let reader = lsm_tree::sfa::Reader::from_reader(&mut f)
                .map_err(|e| std::io::Error::other(format!("read SFA TOC: {e}")))?;
            let entry = reader
                .toc()
                .iter()
                .find(|e| e.name() == b"data")
                .expect("the SST carries a data section");
            assert!(entry.len() > DEPTH, "data section too small to corrupt");
            usize::try_from(entry.pos() + DEPTH).expect("position fits usize")
        };
        let mut bytes = std::fs::read(path)?;
        *bytes.get_mut(pos).expect("offset within the SST") ^= 0xFF;
        std::fs::write(path, &bytes)
    }

    #[test]
    fn a_repaired_version_registers_the_dictionaries_its_tables_name() -> lsm_tree::Result<()> {
        // A repair rebuilds the version from what is on disk, and the rebuilt
        // one has to REGISTER the dictionaries its recovered tables reference.
        // Everything downstream reads that list: a checkpoint copies exactly
        // it, so a version that forgot the ids produces a snapshot with the
        // tables and none of the dictionaries they need.
        let dir = tempfile::tempdir()?;
        let target = tempfile::tempdir()?;
        let checkpoint = target.path().join("snapshot");
        let dict = make_test_dictionary();
        let dict_id = dict.id();
        let compression = CompressionType::zstd_dict(3, dict_id)?;

        {
            let tree = make_config(dir.path())
                .data_block_compression_policy(CompressionPolicy::all(compression))
                .zstd_dictionary(Some(Arc::new(dict)))
                .open()?;
            for i in 0u32..100 {
                let key = format!("key-{i:05}");
                let val = format!("value-{i:05}-padding-to-make-it-longer");
                tree.insert(key.as_bytes(), val.as_bytes(), i.into());
            }
            tree.flush_active_memtable(0)?;
        }

        lose_the_manifest(dir.path())?;
        let report = make_config(dir.path()).repair()?;
        assert_eq!(report.unreadable, 0);

        // Checkpoint the REPAIRED tree under a policy that no longer names the
        // dictionary, so nothing re-registers it: what the rebuilt version
        // recorded is all there is. The tables on disk are still compressed
        // against it and still need it.
        {
            let tree = make_config(dir.path())
                .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
                .open()?;
            tree.create_checkpoint(&checkpoint)?;
        }

        assert!(
            checkpoint.join("dicts").join(dict_id.to_string()).exists(),
            "the rebuilt version must name the dictionary its tables were written against",
        );

        let restored = make_config(&checkpoint).open()?;
        assert_eq!(
            restored.get(b"key-00042", lsm_tree::MAX_SEQNO)?.as_deref(),
            Some(b"value-00042-padding-to-make-it-longer".as_slice()),
        );
        Ok(())
    }

    #[test]
    fn a_repair_stores_the_dictionary_it_was_handed() -> lsm_tree::Result<()> {
        // Repairing a tree written before dictionaries were stored: the tables
        // name an id, but `dicts/` does not exist, so the caller supplies the
        // bytes to the repair. Recording that id in the rebuilt manifest without
        // ALSO writing the file leaves a manifest naming a dictionary the tree
        // does not have, and the next open — the whole point of the repair —
        // fails.
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let dict_id = dict.id();
        let compression = CompressionType::zstd_dict(3, dict_id)?;
        let dict = Arc::new(dict);

        {
            let tree = make_config(dir.path())
                .data_block_compression_policy(CompressionPolicy::all(compression))
                .zstd_dictionary(Some(Arc::clone(&dict)))
                .open()?;
            for i in 0u32..100 {
                let key = format!("key-{i:05}");
                let val = format!("value-{i:05}-padding-to-make-it-longer");
                tree.insert(key.as_bytes(), val.as_bytes(), i.into());
            }
            tree.flush_active_memtable(0)?;
        }

        // Take the tree back to the pre-storage world: the tables still name the
        // dictionary, nothing on disk answers for it.
        std::fs::remove_dir_all(dir.path().join("dicts"))?;
        lose_the_manifest(dir.path())?;

        let report = make_config(dir.path())
            .zstd_dictionary(Some(dict))
            .repair()?;
        assert_eq!(report.unreadable, 0, "the supplied dictionary reads them");

        assert!(
            dir.path().join("dicts").join(dict_id.to_string()).exists(),
            "a repair that records the id must also store the bytes behind it",
        );

        // And the repaired tree opens with nothing supplied.
        let reopened = make_config(dir.path()).open()?;
        assert_eq!(
            reopened.get(b"key-00042", lsm_tree::MAX_SEQNO)?.as_deref(),
            Some(b"value-00042-padding-to-make-it-longer".as_slice()),
        );
        Ok(())
    }

    #[test]
    fn a_salvage_resolves_the_dictionaries_the_tree_stores() -> lsm_tree::Result<()> {
        // Salvage reads the source's blocks and rewrites them, so it needs the
        // dictionary the source was written against just as a plain open does.
        // Taking only the CONFIGURED one leaves a tree that stores its
        // dictionary unsalvageable, which is the one case salvage exists for.
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let compression = CompressionType::zstd_dict(3, dict.id())?;

        {
            let tree = make_config(dir.path())
                .data_block_compression_policy(CompressionPolicy::all(compression))
                .zstd_dictionary(Some(Arc::new(dict)))
                .open()?;
            for i in 0u32..500 {
                let key = format!("key-{i:05}");
                let val = format!("value-{i:05}-padding-to-make-it-longer");
                tree.insert(key.as_bytes(), val.as_bytes(), i.into());
            }
            tree.flush_active_memtable(0)?;
        }

        let victim = a_table_to_corrupt(dir.path())?;
        corrupt_a_data_block(&victim)?;
        lose_the_manifest(dir.path())?;

        let report = make_config(dir.path()).repair_with_salvage(true)?;
        assert_eq!(
            report.salvaged, 1,
            "the block-corrupt table is salvaged, not dropped: {:?}",
            report.unreadable_files,
        );

        // The blocks the corruption did not touch are back, still readable.
        let reopened = make_config(dir.path()).open()?;
        let present = (0u32..500)
            .filter(|i| {
                let key = format!("key-{i:05}");
                reopened
                    .get(key.as_bytes(), lsm_tree::MAX_SEQNO)
                    .expect("read")
                    .is_some()
            })
            .count();
        assert!(
            present > 400,
            "only the corrupt block's keys may be lost, got {present}/500",
        );
        Ok(())
    }

    #[test]
    fn a_checkpoint_carries_the_dictionaries_its_tables_need() -> lsm_tree::Result<()> {
        // A checkpoint is meant to open on its own. Linking the tables without
        // the dictionaries they name produces a directory that cannot be
        // opened at all, which is the one thing a checkpoint must never be.
        let dir = tempfile::tempdir()?;
        let target = tempfile::tempdir()?;
        let checkpoint = target.path().join("snapshot");
        let dict = make_test_dictionary();
        let dict_id = dict.id();
        let compression = CompressionType::zstd_dict(3, dict_id)?;

        {
            let tree = make_config(dir.path())
                .data_block_compression_policy(CompressionPolicy::all(compression))
                .zstd_dictionary(Some(Arc::new(dict)))
                .open()?;
            for i in 0u32..100 {
                let key = format!("key-{i:05}");
                let val = format!("value-{i:05}-padding-to-make-it-longer");
                tree.insert(key.as_bytes(), val.as_bytes(), i.into());
            }
            tree.flush_active_memtable(0)?;

            tree.create_checkpoint(&checkpoint)?;
        }

        assert!(
            checkpoint.join("dicts").join(dict_id.to_string()).exists(),
            "the checkpoint holds the dictionary its tables were written against",
        );

        // Opened with nothing supplied, exactly as the source tree reopens.
        let restored = make_config(&checkpoint).open()?;
        for i in 0u32..100 {
            let key = format!("key-{i:05}");
            let expected = format!("value-{i:05}-padding-to-make-it-longer");
            assert_eq!(
                restored
                    .get(key.as_bytes(), lsm_tree::MAX_SEQNO)?
                    .as_deref(),
                Some(expected.as_bytes()),
            );
        }
        Ok(())
    }

    #[test]
    fn an_ingested_table_resolves_its_dictionary_from_the_tree() -> lsm_tree::Result<()> {
        // Ingestion builds a table and opens it, so it goes through the same
        // dictionary resolution a flush does. It is a separate entry point and
        // is covered separately.
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let compression = CompressionType::zstd_dict(3, dict.id())?;

        {
            let tree = make_config(dir.path())
                .data_block_compression_policy(CompressionPolicy::all(compression))
                .zstd_dictionary(Some(Arc::new(dict)))
                .open()?;

            let mut ingestion = tree.ingestion()?;
            for i in 0u32..100 {
                let key = format!("key-{i:05}");
                let val = format!("value-{i:05}-padding-to-make-it-longer");
                ingestion.write(key.as_bytes(), val.as_bytes())?;
            }
            ingestion.finish()?;

            assert_eq!(
                tree.get(b"key-00042", lsm_tree::MAX_SEQNO)?.as_deref(),
                Some(b"value-00042-padding-to-make-it-longer".as_slice()),
            );
        }

        // And after a reopen that supplies nothing, from the tree's own folder.
        let reopened = make_config(dir.path()).open()?;
        assert_eq!(
            reopened.get(b"key-00042", lsm_tree::MAX_SEQNO)?.as_deref(),
            Some(b"value-00042-padding-to-make-it-longer".as_slice()),
        );
        Ok(())
    }

    #[test]
    fn an_ingested_table_whose_dictionary_is_gone_reports_that_id() -> lsm_tree::Result<()> {
        // The other direction on the ingestion path: the id it cannot resolve
        // is named, exactly as on the flush path.
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let dict_id = dict.id();
        let compression = CompressionType::zstd_dict(3, dict_id)?;

        {
            let tree = make_config(dir.path())
                .data_block_compression_policy(CompressionPolicy::all(compression))
                .zstd_dictionary(Some(Arc::new(dict)))
                .open()?;
            let mut ingestion = tree.ingestion()?;
            ingestion.write(b"key", b"value")?;
            ingestion.finish()?;
        }

        std::fs::remove_dir_all(dir.path().join("dicts"))?;

        let err = make_config(dir.path())
            .open()
            .err()
            .expect("opening a tree whose dictionary is gone must fail");
        assert!(
            matches!(
                err,
                lsm_tree::Error::ZstdDictMismatch { expected, got: None } if expected == dict_id
            ),
            "expected the missing id to be named, got {err:?}",
        );
        Ok(())
    }

    #[test]
    fn a_salvage_rewrites_an_older_generation_under_its_own_dictionary() -> lsm_tree::Result<()> {
        // The recovered copy mirrors the SOURCE's compression descriptor, so it
        // has to be written with the dictionary that descriptor names. Handing
        // the writer the tree's CURRENT dictionary instead compresses the blocks
        // against bytes the stamped id does not describe, and the copy fails on
        // its first read — the multi-generation salvage would be impossible.
        let dir = tempfile::tempdir()?;
        let first = make_test_dictionary();
        let first_id = first.id();
        let second = ZstdDictionary::new(&b"a second dictionary with different content".repeat(40));
        let second_id = second.id();
        assert_ne!(first_id, second_id);

        // Generation one, under the first dictionary.
        {
            let tree = make_config(dir.path())
                .data_block_compression_policy(CompressionPolicy::all(CompressionType::zstd_dict(
                    3, first_id,
                )?))
                .zstd_dictionary(Some(Arc::new(first)))
                .open()?;
            for i in 0u32..500 {
                let key = format!("key-{i:05}");
                let val = format!("value-{i:05}-padding-to-make-it-longer");
                tree.insert(key.as_bytes(), val.as_bytes(), i.into());
            }
            tree.flush_active_memtable(0)?;
        }

        // Corrupt that table, then repair under a policy that has MOVED ON to a
        // second dictionary: the salvage must still rewrite the old table under
        // the dictionary it names.
        let victim = a_table_to_corrupt(dir.path())?;
        corrupt_a_data_block(&victim)?;
        lose_the_manifest(dir.path())?;

        let report = make_config(dir.path())
            .data_block_compression_policy(CompressionPolicy::all(CompressionType::zstd_dict(
                3, second_id,
            )?))
            .zstd_dictionary(Some(Arc::new(second)))
            .repair_with_salvage(true)?;
        assert_eq!(
            report.salvaged, 1,
            "the older generation is salvaged, not dropped: {:?}",
            report.unreadable_files,
        );

        // The salvaged copy reads back: it was written under the dictionary its
        // own descriptor names.
        let reopened = make_config(dir.path()).open()?;
        let present = (0u32..500)
            .filter(|i| {
                let key = format!("key-{i:05}");
                reopened
                    .get(key.as_bytes(), lsm_tree::MAX_SEQNO)
                    .expect("read")
                    .is_some()
            })
            .count();
        assert!(
            present > 400,
            "only the corrupt block's keys may be lost, got {present}/500",
        );
        Ok(())
    }

    #[test]
    fn a_table_naming_a_dictionary_the_tree_lost_reports_that_id() -> lsm_tree::Result<()> {
        // The other direction: when the bytes genuinely are not there, the
        // failure has to name the id the table asked for, so an operator can
        // tell WHICH dictionary is missing rather than only that one is.
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let dict_id = dict.id();
        let compression = CompressionType::zstd_dict(3, dict_id)?;

        {
            let tree = make_config(dir.path())
                .data_block_compression_policy(CompressionPolicy::all(compression))
                .zstd_dictionary(Some(Arc::new(dict)))
                .open()?;
            tree.insert(b"key", b"value", 0);
            tree.flush_active_memtable(0)?;
        }

        // Remove the tree's copy: the operator deleted the folder, a backup
        // restored without it, a bad migration.
        std::fs::remove_dir_all(dir.path().join("dicts"))?;

        let err = make_config(dir.path())
            .open()
            .err()
            .expect("opening a tree whose dictionary is gone must fail");
        assert!(
            matches!(
                err,
                lsm_tree::Error::ZstdDictMismatch { expected, got: None } if expected == dict_id
            ),
            "expected the missing id to be named, got {err:?}",
        );

        Ok(())
    }
}

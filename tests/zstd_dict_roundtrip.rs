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

// Integration tests for dictionary compression using the pure Rust zstd backend
// (`structured-zstd`). These mirror the `zstd_dict` tests above to confirm that
// `compress_with_dict` in the pure backend produces RFC 8878-compliant frames that
// round-trip correctly through the full Tree API.
#[cfg(all(feature = "zstd-pure", not(feature = "zstd")))]
mod zstd_pure_dict {
    use lsm_tree::{
        AbstractTree, CompressionType, Config, Guard, SequenceNumberCounter, ZstdDictionary,
        config::CompressionPolicy,
    };
    use std::sync::Arc;

    /// Build a raw-content dictionary from repetitive sample data.
    ///
    /// Uses a corpus similar to the C-backend tests so the dictionary content is
    /// meaningful (repeated key/value patterns). `ZstdDictionary::new(&samples)`
    /// creates a raw-content dictionary here (no finalized-dictionary magic
    /// header), so this helper covers the raw-content compression path in the
    /// pure Rust backend. For the finalized-dictionary path see
    /// `pure_finalized_dict_roundtrip`.
    fn make_test_dictionary() -> ZstdDictionary {
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
    fn pure_tree_write_flush_read_zstd_dict() -> lsm_tree::Result<()> {
        // Full round-trip via the Tree API: write → flush (compress with dict) →
        // read (decompress with dict). This exercises the block writer/reader path
        // end-to-end with the pure backend.
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
    fn pure_tree_range_scan_with_zstd_dict() -> lsm_tree::Result<()> {
        // Range scan must return correct key-value pairs when blocks were
        // compressed with a dictionary by the pure backend.
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

        let pairs: Vec<_> = items.into_iter().map(|g| g.into_inner().unwrap()).collect();
        assert_eq!(pairs.first().unwrap().0.as_ref(), b"key-00010");
        assert_eq!(pairs.last().unwrap().0.as_ref(), b"key-00020");

        Ok(())
    }

    #[test]
    fn pure_tree_reopen_with_dict_reads_back_correctly() -> lsm_tree::Result<()> {
        // Data written and flushed in one session must be readable after the
        // tree is closed and reopened with the same dictionary.
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let compression = CompressionType::zstd_dict(3, dict.id())?;

        {
            let tree = make_config(dir.path())
                .data_block_compression_policy(CompressionPolicy::all(compression))
                .zstd_dictionary(Some(Arc::new(dict.clone())))
                .open()?;

            for i in 0u32..50 {
                let key = format!("key-{i:05}");
                let val = format!("value-{i:05}");
                tree.insert(key.as_bytes(), val.as_bytes(), i.into());
            }
            tree.flush_active_memtable(0)?;
        }

        // Reopen — the on-disk blocks were compressed by the pure backend and must
        // decompress correctly via the same pure `decompress_with_dict` path.
        let tree = make_config(dir.path())
            .data_block_compression_policy(CompressionPolicy::all(CompressionType::zstd_dict(
                3,
                dict.id(),
            )?))
            .zstd_dictionary(Some(Arc::new(dict)))
            .open()?;

        for i in 0u32..50 {
            let key = format!("key-{i:05}");
            let expected = format!("value-{i:05}");
            let got = tree
                .get(key.as_bytes(), lsm_tree::MAX_SEQNO)?
                .expect("key should exist after reopen");
            assert_eq!(got.as_ref(), expected.as_bytes(), "mismatch at key {key}");
        }

        Ok(())
    }

    #[test]
    fn pure_zstd_dict_missing_returns_error() -> lsm_tree::Result<()> {
        // Config validation must reject `ZstdDict` compression without a dictionary.
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let compression = CompressionType::zstd_dict(3, dict.id())?;

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
    fn pure_finalized_dict_roundtrip() -> lsm_tree::Result<()> {
        // Exercises the `Dictionary::decode_dict` path in the pure Rust backend.
        //
        // A finalized zstd dictionary (magic `0x37A430EC`, entropy tables,
        // content) is passed to `ZstdDictionary::new`. The pure backend detects
        // the magic prefix and calls `decode_dict` rather than `from_raw_content`,
        // exercising a different code path from the raw-content tests above.
        //
        // Dict bytes: produced by `zstd::dict::from_continuous` on a small corpus
        // (the same fixture used by the unit tests in `src/compression/zstd_pure.rs`).
        const FINALIZED_DICT: &[u8] = &[
            55, 164, 48, 236, 98, 64, 12, 7, 42, 16, 120, 62, 7, 204, 192, 51, 240, 12, 60, 3, 207,
            192, 51, 240, 12, 60, 3, 207, 192, 51, 24, 17, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 128, 48, 165, 148, 2, 227, 76, 8, 33, 132, 16, 66, 136, 136, 136, 60, 84, 160, 64,
            65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65,
            65, 65, 65, 193, 231, 162, 40, 138, 162, 40, 138, 162, 40, 165, 148, 82, 74, 169, 170,
            234, 1, 100, 160, 170, 193, 96, 48, 24, 12, 6, 131, 193, 96, 48, 12, 195, 48, 12, 195,
            48, 12, 195, 48, 198, 24, 99, 140, 153, 29, 1, 0, 0, 0, 4, 0, 0, 0, 8, 0, 0, 0, 3, 3,
            3, 3, 3, 3, 3, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2,
        ];

        let dir = tempfile::tempdir()?;
        let dict = ZstdDictionary::new(FINALIZED_DICT);
        let compression = CompressionType::zstd_dict(3, dict.id())?;

        let tree = make_config(dir.path())
            .data_block_compression_policy(CompressionPolicy::all(compression))
            .zstd_dictionary(Some(Arc::new(dict.clone())))
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
}

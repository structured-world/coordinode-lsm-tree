// Integration test: zstd dictionary compression roundtrip
//
// Verifies that data written with zstd dictionary compression can be read back
// correctly through the full Tree API (write → flush → read) and that the
// various read paths continue to work correctly when a zstd dictionary is used.

#[cfg(feature = "zstd")]
mod zstd_dict {
    use lsm_tree::{
        config::CompressionPolicy, AbstractTree, CompressionType, Config, Guard,
        SequenceNumberCounter, ZstdDictionary,
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
    fn zstd_dict_with_multiple_levels() -> lsm_tree::Result<()> {
        // Use per-level compression: None for L0, ZstdDict for deeper levels
        let dir = tempfile::tempdir()?;
        let dict = make_test_dictionary();
        let compression = CompressionType::zstd_dict(3, dict.id())?;

        let tree = make_config(dir.path())
            .data_block_compression_policy(CompressionPolicy::new([
                CompressionType::None,
                compression,
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

        // dict_id in compression type matches the wrong dictionary
        let compression = CompressionType::zstd_dict(3, wrong_dict.id())?;

        let tree = make_config(dir.path())
            .data_block_compression_policy(CompressionPolicy::all(compression))
            // Provide a dictionary whose ID does NOT match the one in CompressionType
            .zstd_dictionary(Some(Arc::new(dict)))
            .open()?;

        tree.insert(b"key", b"value", 0);

        // Flushing should fail with ZstdDictMismatch because the dict_id
        // in the compression type doesn't match the provided dictionary
        let result = tree.flush_active_memtable(0);
        assert!(
            matches!(result, Err(lsm_tree::Error::ZstdDictMismatch { .. })),
            "expected ZstdDictMismatch, got: {result:?}",
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
}

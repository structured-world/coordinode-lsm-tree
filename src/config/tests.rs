use super::*;
use crate::{CompressionType, SequenceNumberCounter, compression::ZstdDictionary};
use alloc::sync::Arc;

fn blob_config(folder: &tempfile::TempDir) -> Config {
    Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default()))
}

#[test]
fn blob_zstd_dict_no_dict_is_rejected() {
    // A blob compression naming a dictionary the tree does not hold must fail
    // the open, before anything is written under it.
    let folder = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir failed: {err}"));
    let result = blob_config(&folder)
        .blob_compression(CompressionType::ZstdDict {
            level: 3,
            dict_id: 7,
        })
        .open();

    assert!(
        matches!(
            result,
            Err(crate::Error::ZstdDictMismatch {
                expected: 7,
                got: None
            })
        ),
        "expected ZstdDictMismatch when no dictionary is supplied",
    );
}

#[test]
fn blob_zstd_dict_id_mismatch_is_rejected() {
    // The supplied dictionary is registered, but the compression names a
    // different id, which the tree still does not hold.
    let folder = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir failed: {err}"));
    let dict = Arc::new(ZstdDictionary::new(b"sample training data for test"));
    let wrong_dict_id = dict.id().wrapping_add(1);
    let result = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default().dict(Arc::clone(&dict))))
    .blob_compression(CompressionType::ZstdDict {
        level: 3,
        dict_id: wrong_dict_id,
    })
    .open();

    assert!(
        matches!(
            result,
            Err(crate::Error::ZstdDictMismatch { expected, got: None }) if expected == wrong_dict_id
        ),
        "expected ZstdDictMismatch naming the id the compression asks for",
    );
}

#[test]
fn blob_zstd_dict_matching_dict_is_accepted() {
    // A compression naming the dictionary supplied with it opens.
    let folder = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir failed: {err}"));
    let dict = Arc::new(ZstdDictionary::new(b"sample training data for test"));
    let result = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default().dict(Arc::clone(&dict))))
    .blob_compression(CompressionType::ZstdDict {
        level: 3,
        dict_id: dict.id(),
    })
    .open();

    assert!(result.is_ok(), "matching dictionary must be accepted");
}

#[test]
fn compression_builders_on_a_config_seed_the_initial_runtime_config() {
    // The three compression settings live in the runtime config; the builders
    // are how a caller sets what the tree starts with.
    let data = CompressionPolicy::new([CompressionType::None, CompressionType::Zstd(9)]);
    let index = CompressionPolicy::all(CompressionType::Zstd(1));
    let blob = CompressionType::Zstd(5);
    let config = Config::new(
        "unused",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(data.clone())
    .index_block_compression_policy(index.clone())
    .blob_compression(blob);

    assert_eq!(
        config.initial_runtime_config.data_block_compression_policy,
        data
    );
    assert_eq!(
        config.initial_runtime_config.index_block_compression_policy,
        index
    );
    assert_eq!(config.initial_runtime_config.blob_compression, blob);
}

#[test]
fn with_runtime_config_after_compression_builders_keeps_their_policies() {
    // The compression settings moved into the runtime config, where
    // `with_runtime_config` replaces the snapshot. A caller that set its
    // compression through the builders BEFORE handing over a runtime config
    // must not have it reset to the defaults that config carries, so the
    // replacement leaves the three policies alone, in either order.
    let data = CompressionPolicy::all(CompressionType::Zstd(9));
    let index = CompressionPolicy::all(CompressionType::Zstd(1));
    let blob = CompressionType::Zstd(5);
    let runtime = crate::runtime_config::RuntimeConfig {
        seqno_in_index: true,
        ..Default::default()
    };

    let before = Config::new(
        "unused",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(data.clone())
    .index_block_compression_policy(index.clone())
    .blob_compression(blob)
    .with_runtime_config(runtime.clone());

    let after = Config::new(
        "unused",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_runtime_config(runtime)
    .data_block_compression_policy(data.clone())
    .index_block_compression_policy(index.clone())
    .blob_compression(blob);

    for config in [before, after] {
        let rc = &config.initial_runtime_config;
        assert_eq!(rc.data_block_compression_policy, data);
        assert_eq!(rc.index_block_compression_policy, index);
        assert_eq!(rc.blob_compression, blob);
        assert!(rc.seqno_in_index, "the rest of the snapshot is replaced");
    }
}

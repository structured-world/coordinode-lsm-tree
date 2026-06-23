use super::*;
use crate::{CompressionType, SequenceNumberCounter, compression::ZstdDictionary};
use alloc::sync::Arc;

#[test]
fn blob_zstd_dict_no_dict_is_rejected() {
    // ZstdDict compression for blobs without providing a dictionary must fail.
    let folder = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir failed: {err}"));
    let cfg = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default().compression(
        CompressionType::ZstdDict {
            level: 3,
            dict_id: 7,
        },
    )));

    assert!(
        matches!(
            cfg.validate_zstd_dictionary(),
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
    // ZstdDict compression with a dictionary whose id doesn't match the
    // compression type's dict_id must fail.
    let folder = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir failed: {err}"));
    let dict = Arc::new(ZstdDictionary::new(b"sample training data for test"));
    let wrong_dict_id = dict.id().wrapping_add(1);
    let cfg = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default()
            .compression(CompressionType::ZstdDict {
                level: 3,
                dict_id: wrong_dict_id,
            })
            .dict(Arc::clone(&dict)),
    ));

    assert!(
        matches!(
            cfg.validate_zstd_dictionary(),
            Err(crate::Error::ZstdDictMismatch { .. })
        ),
        "expected ZstdDictMismatch when dict_id doesn't match dictionary",
    );
}

#[test]
fn blob_zstd_dict_matching_dict_is_accepted() {
    // ZstdDict compression with a correctly matching dictionary must succeed.
    let folder = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir failed: {err}"));
    let dict = Arc::new(ZstdDictionary::new(b"sample training data for test"));
    let cfg = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default()
            .compression(CompressionType::ZstdDict {
                level: 3,
                dict_id: dict.id(),
            })
            .dict(Arc::clone(&dict)),
    ));

    assert!(
        cfg.validate_zstd_dictionary().is_ok(),
        "matching dictionary must be accepted",
    );
}

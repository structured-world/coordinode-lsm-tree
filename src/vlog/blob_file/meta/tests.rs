use super::*;
use test_log::test;

#[test]
fn test_blob_file_meta_truncated_returns_err() {
    // Truncated metadata (just the magic header) must return Err, not panic
    let buf = Slice::from(METADATA_HEADER_MAGIC.to_vec());
    assert!(Metadata::from_slice(&buf).is_err());
}

/// Build a metadata block that is structurally valid but omits a required
/// property (`compression`).  `from_slice` must return `Err`, not panic.
#[test]
#[expect(clippy::unwrap_used)]
fn test_blob_file_meta_missing_field_returns_err() {
    use crate::table::block::BlockType;
    use std::io::Write;

    fn meta(key: &str, value: &[u8]) -> InternalValue {
        InternalValue::from_components(key, value, 0, crate::ValueType::Value)
    }

    // Include all required fields EXCEPT `compression`
    #[rustfmt::skip]
    let meta_items = [
        meta("blob_file_version", &[4u8]),
        meta("checksum_type", &[u8::from(ChecksumType::Xxh3)]),
        // "compression" intentionally omitted
        meta("crate_version", env!("CARGO_PKG_VERSION").as_bytes()),
        meta("created_at", &1_234_567_890u128.to_le_bytes()),
        meta("file_size", &1024u64.to_le_bytes()),
        meta("id", &0u64.to_le_bytes()),
        meta("item_count", &100u64.to_le_bytes()),
        meta("key#max", b"z"),
        meta("key#min", b"a"),
        meta("uncompressed_size", &2048u64.to_le_bytes()),
    ];

    let encoded = DataBlock::encode_into_vec(&meta_items, 1, 0.0).unwrap();

    let mut buf = Vec::new();
    buf.write_all(METADATA_HEADER_MAGIC).unwrap();
    Block::write_into(
        &mut buf,
        &encoded,
        crate::table::block::BlockIdentity::for_test(0, BlockType::Meta),
        &crate::table::block::BlockTransform::PLAIN,
    )
    .unwrap();

    let buf = Slice::from(buf);
    let result = Metadata::from_slice(&buf);
    assert!(
        matches!(result, Err(crate::Error::InvalidHeader("BlobFileMeta"))),
        "expected Err(InvalidHeader(\"BlobFileMeta\")), got {result:?}",
    );
}

/// Regression test for #195: corrupt the block trailer (last bytes) of a
/// valid blob file metadata block.  `from_slice` must return `Err`, not
/// panic.  The checksum layer catches byte-level corruption before trailer
/// parsing; the `point_read` → `ok_or` error path for missing/malformed
/// fields is exercised by `test_blob_file_meta_missing_field_returns_err`.
#[test]
#[expect(clippy::unwrap_used)]
fn test_blob_file_meta_corrupted_trailer_returns_err() {
    let meta = Metadata {
        id: 0,
        version: 4,
        created_at: 1_234_567_890,
        compression: CompressionType::None,
        item_count: 100,
        total_compressed_bytes: 1024,
        total_uncompressed_bytes: 2048,
        key_range: KeyRange::new((b"a".into(), b"z".into())),
    };

    let mut buf = Vec::new();
    meta.encode_into(&mut buf).unwrap();

    // Corrupt the last 4 bytes of the block (trailer region).
    // This triggers a ChecksumMismatch in `Block::from_reader` — the
    // first defense layer.  The deeper point_read → ok_or path (which
    // previously could panic) is exercised separately by
    // `test_blob_file_meta_missing_field_returns_err`, where the block
    // is structurally valid but omits a required property.
    let len = buf.len();
    assert!(len >= 4, "buffer too small for corruption");
    #[expect(clippy::indexing_slicing, reason = "length checked above")]
    for b in &mut buf[len - 4..] {
        *b ^= 0xFF;
    }

    let buf = Slice::from(buf);
    let result = Metadata::from_slice(&buf);
    assert!(
        result.is_err(),
        "corrupted trailer must produce Err, got {result:?}",
    );
}

#[test]
#[expect(clippy::unwrap_used)]
fn test_blob_file_meta_roundtrip() {
    let meta = Metadata {
        id: 0,
        version: 4,
        created_at: 1_234_567_890,
        compression: CompressionType::None,
        item_count: 100,
        total_compressed_bytes: 1024,
        total_uncompressed_bytes: 2048,
        key_range: KeyRange::new((b"a".into(), b"z".into())),
    };

    let mut buf = Vec::new();
    meta.encode_into(&mut buf).unwrap();
    let buf = Slice::from(buf);

    let meta2 = Metadata::from_slice(&buf).unwrap();
    assert_eq!(meta, meta2);
}

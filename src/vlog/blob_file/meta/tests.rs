use super::*;
use test_log::test;

#[test]
fn test_blob_file_meta_truncated_returns_err() {
    // Truncated metadata (just the magic header) must return Err, not panic
    let buf = Slice::from(METADATA_HEADER_MAGIC.to_vec());
    assert!(Metadata::from_slice(&buf).is_err());
}

/// A blob file of an earlier format, its metadata serialized through the
/// PRODUCTION encoder, is refused by the production decoder: the engine reads
/// exactly one blob-file format, and an earlier one is a format to convert,
/// reported as such (so repair keeps the file rather than grading it damaged),
/// never a compat case and never damage.
#[test]
#[expect(clippy::unwrap_used)]
fn test_blob_file_meta_retired_version_rejected() {
    for version in [3, META_VERSION - 1] {
        let metadata = Metadata {
            id: 0,
            version,
            created_at: 0,
            item_count: 1,
            total_compressed_bytes: 8,
            total_uncompressed_bytes: 8,
            block_entry_bytes: 0,
            block_frame_bytes: 0,
            key_range: KeyRange::new((b"a"[..].into(), b"z"[..].into())),
            compression: CompressionType::None,
            lifetime_class: 0,
        };
        let mut buf = Vec::new();
        metadata.encode_into(&mut buf).unwrap();

        let result = Metadata::from_slice(&Slice::from(buf));
        assert!(
            matches!(
                result,
                Err(crate::Error::UnsupportedFormat {
                    part: crate::FormatPart::BlobFiles,
                    found: Some(found),
                    expected: META_VERSION,
                }) if found == version
            ),
            "version {version}: got {result:?}",
        );
    }
}

/// The refusal of a blob file of an earlier layout names a remedy an operator
/// can follow: the offline converter does not rewrite blob files, so it names
/// the release that wrote the store rather than the converter.
#[test]
fn an_earlier_blob_layout_names_the_release_that_wrote_it() {
    let rendered = crate::Error::UnsupportedFormat {
        part: crate::FormatPart::BlobFiles,
        found: Some(META_VERSION - 1),
        expected: META_VERSION,
    }
    .to_string();
    assert!(
        rendered.contains("release that wrote it"),
        "the remedy: {rendered}"
    );
    assert!(
        !rendered.contains("run the offline converter"),
        "the converter does not rewrite blob files: {rendered}"
    );
    // A table part of an earlier layout, which the converter does rewrite.
    let table = crate::Error::UnsupportedFormat {
        part: crate::FormatPart::Columnar,
        found: None,
        expected: 2,
    }
    .to_string();
    assert!(
        table.contains("unstamped") && table.contains("run the offline converter"),
        "{table}"
    );
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
        meta("blob_file_version", &[META_VERSION]),
        meta("block_entry_bytes", &0u64.to_le_bytes()),
        meta("block_frame_bytes", &0u64.to_le_bytes()),
        meta("checksum_type", &[u8::from(ChecksumType::Xxh3)]),
        // "compression" intentionally omitted
        meta("crate_version", env!("CARGO_PKG_VERSION").as_bytes()),
        meta("created_at", &1_234_567_890u128.to_le_bytes()),
        meta("file_size", &1024u64.to_le_bytes()),
        meta("id", &0u64.to_le_bytes()),
        meta("item_count", &100u64.to_le_bytes()),
        meta("key#max", b"z"),
        meta("key#min", b"a"),
        meta("lifetime_class", &[0u8]),
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
        crate::table::block::ChecksumAt::Unbound,
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
        version: META_VERSION,
        created_at: 1_234_567_890,
        compression: CompressionType::None,
        item_count: 100,
        total_compressed_bytes: 1024,
        total_uncompressed_bytes: 2048,
        block_entry_bytes: 0,
        block_frame_bytes: 0,
        key_range: KeyRange::new((b"a".into(), b"z".into())),
        lifetime_class: 0,
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
        version: META_VERSION,
        created_at: 1_234_567_890,
        compression: CompressionType::None,
        item_count: 100,
        total_compressed_bytes: 1024,
        total_uncompressed_bytes: 2048,
        // Distinct non-zero values so the roundtrip proves each is encoded in
        // its own item.
        block_entry_bytes: 900,
        block_frame_bytes: 300,
        key_range: KeyRange::new((b"a".into(), b"z".into())),
        // Non-zero so the roundtrip proves the class is encoded, not defaulted.
        lifetime_class: 2,
    };

    let mut buf = Vec::new();
    meta.encode_into(&mut buf).unwrap();
    let buf = Slice::from(buf);

    let meta2 = Metadata::from_slice(&buf).unwrap();
    assert_eq!(meta, meta2);
}

fn with_blocks(total_compressed_bytes: u64, entries: u64, frames: u64) -> Metadata {
    Metadata {
        id: 0,
        version: META_VERSION,
        created_at: 0,
        compression: CompressionType::None,
        item_count: 1,
        total_compressed_bytes,
        total_uncompressed_bytes: 1,
        block_entry_bytes: entries,
        block_frame_bytes: frames,
        key_range: KeyRange::new((b"a".into(), b"z".into())),
        lifetime_class: 0,
    }
}

/// Block entries take their part of the file's blocks on disk: a third of the
/// entries of blocks compressed to a third take a ninth, and a file with no
/// block holds no block entry to charge.
#[test]
fn block_share_is_the_entries_part_of_the_blocks_on_disk() {
    let meta = with_blocks(1_000, 900, 300);
    assert_eq!(meta.block_share(300), 100);
    assert_eq!(meta.block_share(900), 300);
    assert_eq!(meta.block_share(0), 0);
    assert_eq!(meta.own_frame_bytes(), 700);
    assert_eq!(with_blocks(1_000, 0, 0).block_share(50), 0);
}

/// Metadata whose blocks take more than all of its frames describes no file
/// the writer could have written: it is refused, so the frame bytes of values
/// with frames of their own never go below zero.
#[test]
#[expect(clippy::unwrap_used)]
fn metadata_whose_blocks_exceed_its_frames_is_refused() {
    let mut buf = Vec::new();
    with_blocks(100, 50, 101).encode_into(&mut buf).unwrap();
    let result = Metadata::from_slice(&Slice::from(buf));
    assert!(
        matches!(result, Err(crate::Error::InvalidHeader("BlobFileMeta"))),
        "got {result:?}"
    );
}

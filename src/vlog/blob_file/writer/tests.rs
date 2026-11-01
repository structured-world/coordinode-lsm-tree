use super::*;
use crate::fs::StdFs;

/// The file format is the streaming digest: the one-call path has to give it
/// on both sides of the size that selects it, or written blobs stop verifying.
#[test]
fn record_checksum_matches_streaming_digest_around_one_call_limit() {
    let key = [7_u8; 16];
    let crc = 0xDEAD_BEEF_u32;
    let limit = ONE_CALL_HASH_BYTES - key.len() - 4;
    for value_len in [0, 1, 200, 256, limit - 1, limit, limit + 1, 4096] {
        let value = vec![0x5A_u8; value_len];
        let mut hasher = xxhash_rust::xxh3::Xxh3::default();
        hasher.update(&key);
        hasher.update(&value);
        hasher.update(&crc.to_le_bytes());
        assert_eq!(
            record_checksum(&key, &value, crc),
            hasher.digest128(),
            "value of {value_len} bytes",
        );
    }
}

/// The header CRC is stored in every record, so it has to stay the digest of
/// the same 18 field bytes.
#[test]
fn header_crc_matches_streaming_digest() {
    let (seqno, key_len, real, on_disk) = (0x0123_4567_89AB_CDEF_u64, 16_u16, 256_u32, 300_u32);
    let mut hasher = xxhash_rust::xxh3::Xxh3::default();
    hasher.update(&seqno.to_le_bytes());
    hasher.update(&key_len.to_le_bytes());
    hasher.update(&real.to_le_bytes());
    hasher.update(&on_disk.to_le_bytes());
    #[expect(clippy::cast_possible_truncation, reason = "the CRC is 4 bytes")]
    let streaming = hasher.digest() as u32;
    assert_eq!(compute_header_crc(seqno, key_len, real, on_disk), streaming);
}

#[test]
fn blob_write_rejects_oversized_value() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path().join("test.blob");
    let mut writer = Writer::new(&path, 0, 0, &StdFs)?;

    #[expect(
        clippy::cast_possible_truncation,
        reason = "MAX_DECOMPRESSION_SIZE fits in u32"
    )]
    let oversize = MAX_DECOMPRESSION_SIZE as u32 + 1;
    let result = writer.write_raw(b"key", 0, b"small-on-disk", oversize);
    assert!(
        matches!(result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
        "expected DecompressedSizeTooLarge, got: {result:?}",
    );
    Ok(())
}

#[test]
fn blob_write_accepts_max_size_value() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path().join("test.blob");
    let mut writer = Writer::new(&path, 0, 0, &StdFs)?;

    #[expect(
        clippy::cast_possible_truncation,
        reason = "MAX_DECOMPRESSION_SIZE fits in u32"
    )]
    let at_limit = MAX_DECOMPRESSION_SIZE as u32;
    let result = writer.write_raw(b"key", 0, b"small-on-disk", at_limit);
    assert!(result.is_ok(), "expected Ok, got: {result:?}");
    Ok(())
}

#[test]
fn blob_write_rejects_oversized_value_none_compression() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path().join("test.blob");
    let mut writer = Writer::new(&path, 0, 0, &StdFs)?;

    let oversize_value = vec![0u8; MAX_DECOMPRESSION_SIZE + 1];
    #[expect(
        clippy::cast_possible_truncation,
        reason = "MAX_DECOMPRESSION_SIZE fits in u32"
    )]
    let result = writer.write_raw(b"key", 0, &oversize_value, MAX_DECOMPRESSION_SIZE as u32);
    assert!(
        matches!(result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
        "expected DecompressedSizeTooLarge, got: {result:?}",
    );
    Ok(())
}

#[test]
#[cfg(feature = "lz4")]
fn blob_write_lz4_accepts_small_value() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path().join("test.blob");
    let mut writer = Writer::new(&path, 0, 0, &StdFs)?.use_compression(CompressionType::Lz4);

    // Exercise the LZ4 compression arm with a value that passes
    // the pre-compression check and compresses successfully.
    let value = b"hello world lz4 test data";
    #[expect(clippy::cast_possible_truncation, reason = "test value is 25 bytes")]
    let result = writer.write_raw(b"key", 0, value, value.len() as u32);
    assert!(result.is_ok(), "expected Ok, got: {result:?}");
    Ok(())
}

#[test]
fn check_size_cap_rejects_over_limit() {
    let result = super::check_size_cap(MAX_DECOMPRESSION_SIZE + 1);
    assert!(
        matches!(result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
        "expected DecompressedSizeTooLarge, got: {result:?}",
    );
}

#[test]
fn check_size_cap_accepts_at_limit() {
    assert!(super::check_size_cap(MAX_DECOMPRESSION_SIZE).is_ok());
    assert!(super::check_size_cap(0).is_ok());
}

#[test]
#[cfg(zstd_any)]
fn blob_write_zstd_dict_no_dict_returns_mismatch() -> crate::Result<()> {
    // ZstdDict compression without a dictionary must return ZstdDictMismatch.
    let folder = tempfile::tempdir()?;
    let path = folder.path().join("test.blob");
    let dict = crate::compression::ZstdDictionary::new(b"test dictionary content for blob");
    let compression = CompressionType::ZstdDict {
        level: 3,
        dict_id: dict.id(),
    };
    // Intentionally do NOT call use_zstd_dictionary — no dict supplied.
    let mut writer = Writer::new(&path, 0, 0, &StdFs)?.use_compression(compression);

    let result = writer.write(b"key", 0, b"value");
    assert!(
        matches!(result, Err(crate::Error::ZstdDictMismatch { .. })),
        "expected ZstdDictMismatch when no dictionary is supplied, got: {result:?}",
    );
    Ok(())
}

#[test]
#[cfg(zstd_any)]
fn blob_write_zstd_dict_with_dict_succeeds() -> crate::Result<()> {
    // ZstdDict compression with a matching dictionary must succeed.
    let folder = tempfile::tempdir()?;
    let path = folder.path().join("test.blob");
    let dict = crate::compression::ZstdDictionary::new(b"test dictionary content for blob");
    let compression = CompressionType::ZstdDict {
        level: 3,
        dict_id: dict.id(),
    };
    let mut writer = Writer::new(&path, 0, 0, &StdFs)?
        .use_compression(compression)
        .use_zstd_dictionary(Some(alloc::sync::Arc::new(dict)));

    let result = writer.write(b"key", 0, b"hello world blob value");
    assert!(
        result.is_ok(),
        "expected Ok with matching dictionary, got: {result:?}"
    );
    Ok(())
}

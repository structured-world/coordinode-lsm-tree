use super::*;
use crate::fs::StdFs;

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

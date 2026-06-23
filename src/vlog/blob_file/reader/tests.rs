use super::*;
use crate::SequenceNumberCounter;
use crate::fs::StdFs;
use crate::vlog::blob_file::writer::BLOB_HEADER_LEN_V3;
use std::fs::File;
use std::sync::Arc;
use test_log::test;

#[test]
fn blob_reader_roundtrip() -> crate::Result<()> {
    let id_generator = SequenceNumberCounter::default();

    let folder = tempfile::tempdir()?;
    let mut writer =
        crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None, Arc::new(StdFs))?
            .use_target_size(u64::MAX);

    let handle = writer.write(b"a", 0, b"abcdef")?;

    let blob_file = writer.finish()?;
    let blob_file = blob_file.first().unwrap();

    let file = File::open(&blob_file.0.path)?;
    let reader = Reader::new(blob_file, &file);

    assert_eq!(reader.get(b"a", &handle)?, b"abcdef");

    Ok(())
}

#[test]
#[cfg(feature = "lz4")]
fn blob_reader_roundtrip_lz4() -> crate::Result<()> {
    let id_generator = SequenceNumberCounter::default();

    let folder = tempfile::tempdir()?;
    let mut writer =
        crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None, Arc::new(StdFs))?
            .use_target_size(u64::MAX)
            .use_compression(CompressionType::Lz4);

    let handle0 = writer.write(b"a", 0, b"abcdef")?;
    let handle1 = writer.write(b"b", 0, b"ghi")?;

    let blob_file = writer.finish()?;
    let blob_file = blob_file.first().unwrap();

    let file = File::open(&blob_file.0.path)?;
    let reader = Reader::new(blob_file, &file);

    assert_eq!(reader.get(b"a", &handle0)?, b"abcdef");
    assert_eq!(reader.get(b"b", &handle1)?, b"ghi");

    Ok(())
}

/// Tamper `real_val_len` to an absurd value: V4 header CRC catches the
/// corruption before the size-cap check is even reached.
#[test]
#[cfg(feature = "lz4")]
fn blob_reader_reject_absurd_real_val_len() {
    let id_generator = SequenceNumberCounter::default();

    let folder = tempfile::tempdir().unwrap();
    let mut writer =
        crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None, Arc::new(StdFs))
            .unwrap()
            .use_target_size(u64::MAX)
            .use_compression(CompressionType::Lz4);

    let handle = writer.write_raw(b"k", 0, b"value", 5).unwrap();

    let blob_file = writer.finish().unwrap();
    let blob_file = blob_file.first().unwrap();

    // Patch real_val_len at handle.offset + magic(4) + checksum(16) + seqno(8) + key_len(2) = +30
    let mut raw = std::fs::read(&blob_file.0.path).unwrap();
    let real_val_len_offset = usize::try_from(handle.offset).unwrap() + 30;
    raw[real_val_len_offset..real_val_len_offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    std::fs::write(&blob_file.0.path, &raw).unwrap();

    let file = File::open(&blob_file.0.path).unwrap();
    let reader = Reader::new(blob_file, &file);

    let result = reader.get(b"k", &handle);
    assert!(
        matches!(result, Err(crate::Error::HeaderCrcMismatch { .. })),
        "expected HeaderCrcMismatch, got: {result:?}",
    );
}

#[test]
#[cfg(feature = "lz4")]
fn blob_reader_zero_real_val_len_with_data_fails_decompress() {
    let id_generator = SequenceNumberCounter::default();

    let folder = tempfile::tempdir().unwrap();
    let mut writer =
        crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None, Arc::new(StdFs))
            .unwrap()
            .use_target_size(u64::MAX)
            .use_compression(CompressionType::Lz4);

    // Zero real_val_len is allowed (valid for empty values), but when
    // compressed data is present, lz4 decompression fails on the mismatch.
    let handle = writer.write_raw(b"k", 0, b"value", 0).unwrap();

    let blob_file = writer.finish().unwrap();
    let blob_file = blob_file.first().unwrap();

    let file = File::open(&blob_file.0.path).unwrap();
    let reader = Reader::new(blob_file, &file);

    let result = reader.get(b"k", &handle);
    assert!(
        matches!(result, Err(crate::Error::Decompress(_))),
        "expected Decompress error, got: {result:?}",
    );
}

/// Tamper `real_val_len` in lz4 blob: V4 header CRC catches the
/// corruption before decompression is attempted.
#[test]
#[cfg(feature = "lz4")]
fn blob_reader_lz4_corrupted_real_val_len_triggers_header_crc_mismatch() -> crate::Result<()> {
    use crate::io::WriteBytesExt;

    let id_generator = SequenceNumberCounter::default();

    let folder = tempfile::tempdir()?;
    let mut writer =
        crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None, Arc::new(StdFs))?
            .use_target_size(u64::MAX)
            .use_compression(CompressionType::Lz4);

    let handle = writer.write(b"a", 0, b"abcdef")?;

    let blob_file = writer.finish()?;
    let blob_file = blob_file.first().unwrap();

    // RealValLen is at offset 30 from the blob start.
    let real_val_len_offset = handle.offset + 4 + 16 + 8 + 2;

    {
        use std::io::{Seek, Write};
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&blob_file.0.path)?;
        file.seek(std::io::SeekFrom::Start(real_val_len_offset))?;
        file.write_u32::<LittleEndian>(u32::try_from(b"abcdef".len()).unwrap() + 1)?;
        file.flush()?;
    }

    let file = File::open(&blob_file.0.path)?;
    let reader = Reader::new(blob_file, &file);

    match reader.get(b"a", &handle) {
        Err(crate::Error::HeaderCrcMismatch { .. }) => { /* header CRC catches it */ }
        Ok(_) => panic!("expected HeaderCrcMismatch, but got Ok"),
        Err(other) => panic!("expected HeaderCrcMismatch, got: {other:?}"),
    }

    Ok(())
}

#[test]
fn blob_reader_reject_oversized_on_disk_size() {
    let id_generator = SequenceNumberCounter::default();

    let folder = tempfile::tempdir().unwrap();
    let mut writer =
        crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None, Arc::new(StdFs))
            .unwrap()
            .use_target_size(u64::MAX);

    let mut handle = writer.write(b"a", 0, b"hello").unwrap();

    let blob_file = writer.finish().unwrap();
    let blob_file = blob_file.first().unwrap();

    // Tamper the handle to declare an absurd on_disk_size
    handle.on_disk_size = u32::MAX;

    let file = File::open(&blob_file.0.path).unwrap();
    let reader = Reader::new(blob_file, &file);

    let result = reader.get(b"a", &handle);
    assert!(
        matches!(result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
        "expected DecompressedSizeTooLarge, got: {result:?}",
    );
}

/// Tamper `real_val_len` in zstd blob: V4 header CRC catches the
/// corruption before decompression is attempted.
#[test]
#[cfg(zstd_any)]
fn blob_reader_zstd_corrupted_real_val_len_triggers_header_crc_mismatch() -> crate::Result<()> {
    use crate::io::WriteBytesExt;

    let id_generator = SequenceNumberCounter::default();

    let folder = tempfile::tempdir()?;
    let mut writer =
        crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None, Arc::new(StdFs))?
            .use_target_size(u64::MAX)
            .use_compression(CompressionType::Zstd(3));

    let handle = writer.write(b"a", 0, b"abcdef")?;

    let blob_file = writer.finish()?;
    let blob_file = blob_file.first().unwrap();

    // RealValLen is at offset 30 from the blob start.
    let real_val_len_offset = handle.offset + 4 + 16 + 8 + 2;

    {
        use std::io::{Seek, Write};
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&blob_file.0.path)?;
        file.seek(std::io::SeekFrom::Start(real_val_len_offset))?;
        file.write_u32::<LittleEndian>(u32::try_from(b"abcdef".len()).unwrap() + 1)?;
        file.flush()?;
    }

    let file = File::open(&blob_file.0.path)?;
    let reader = Reader::new(blob_file, &file);

    match reader.get(b"a", &handle) {
        Err(crate::Error::HeaderCrcMismatch { .. }) => { /* header CRC catches it */ }
        Ok(_) => panic!("expected HeaderCrcMismatch, but got Ok"),
        Err(other) => panic!("expected HeaderCrcMismatch, got: {other:?}"),
    }

    Ok(())
}

/// Tamper `real_val_len` to exceed size cap: V4 header CRC catches the
/// corruption before the size-cap check is reached.
#[test]
fn blob_reader_rejects_oversized_real_val_len() -> crate::Result<()> {
    let id_generator = SequenceNumberCounter::default();

    let folder = tempfile::tempdir()?;
    let mut writer =
        crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None, Arc::new(StdFs))?
            .use_target_size(u64::MAX);

    let handle = writer.write(b"a", 0, b"abcdef")?;

    let blob_file = writer.finish()?;
    let blob_file = blob_file.first().unwrap();

    // Byte-patch real_val_len in the blob header
    let mut raw = std::fs::read(&blob_file.0.path)?;
    let real_val_len_offset = usize::try_from(handle.offset).unwrap() + 4 + 16 + 8 + 2;
    let oversize = u32::try_from(MAX_DECOMPRESSION_SIZE).unwrap() + 1;
    raw[real_val_len_offset..real_val_len_offset + 4].copy_from_slice(&oversize.to_le_bytes());
    std::fs::write(&blob_file.0.path, &raw)?;

    let file = File::open(&blob_file.0.path)?;
    let reader = Reader::new(blob_file, &file);

    let result = reader.get(b"a", &handle);
    assert!(
        matches!(result, Err(crate::Error::HeaderCrcMismatch { .. })),
        "expected HeaderCrcMismatch, got: {result:?}",
    );
    Ok(())
}

#[test]
#[cfg(zstd_any)]
fn blob_reader_roundtrip_zstd() -> crate::Result<()> {
    let id_generator = SequenceNumberCounter::default();

    let folder = tempfile::tempdir()?;
    let mut writer =
        crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None, Arc::new(StdFs))?
            .use_target_size(u64::MAX)
            .use_compression(CompressionType::Zstd(3));

    let handle0 = writer.write(b"a", 0, b"abcdef")?;
    let handle1 = writer.write(b"b", 0, b"ghi")?;

    let blob_file = writer.finish()?;
    let blob_file = blob_file.first().unwrap();

    let file = File::open(&blob_file.0.path)?;
    let reader = Reader::new(blob_file, &file);

    assert_eq!(reader.get(b"a", &handle0)?, b"abcdef");
    assert_eq!(reader.get(b"b", &handle1)?, b"ghi");

    Ok(())
}

/// Tamper on-disk key bytes and verify two detection layers:
/// 1. Original caller key → `InvalidHeader` from cross-check (fast path)
/// 2. Tampered key as caller → `ChecksumMismatch` (checksum path, upstream #277)
#[test]
fn blob_reader_corrupted_on_disk_key_detected_by_cross_check_and_checksum() -> crate::Result<()> {
    let id_generator = SequenceNumberCounter::default();

    let folder = tempfile::tempdir()?;
    let mut writer =
        crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None, Arc::new(StdFs))?
            .use_target_size(u64::MAX);

    let handle = writer.write(b"abc", 0, b"value")?;

    let blob_file = writer.finish()?;
    let blob_file = blob_file.first().unwrap();

    // Tamper on-disk key bytes.
    // V4 header layout: MAGIC(4) + Checksum(16) + SeqNo(8) + KeyLen(2) + RealValLen(4) + OnDiskValLen(4) + HeaderCrc(4) = 42
    // Key starts at offset 42 from blob start (BLOB_HEADER_LEN_V4).
    let key_offset = usize::try_from(handle.offset).unwrap() + BLOB_HEADER_LEN_V4;
    let mut raw = std::fs::read(&blob_file.0.path)?;
    raw[key_offset] ^= 0xFF; // flip bits in first key byte
    let corrupted_key = raw[key_offset..key_offset + 3].to_vec();
    std::fs::write(&blob_file.0.path, &raw)?;

    let file = File::open(&blob_file.0.path)?;
    let reader = Reader::new(blob_file, &file);

    // Layer 1: original caller key vs tampered on-disk key → InvalidHeader
    let result = reader.get(b"abc", &handle);
    assert!(
        matches!(result, Err(crate::Error::InvalidHeader("Blob"))),
        "expected InvalidHeader(Blob) from key cross-check, got: {result:?}",
    );

    // Layer 2: pass the tampered key as caller so cross-check passes,
    // but checksum (computed over tampered key + value) won't match the
    // stored checksum (computed over original key + value).
    let result = reader.get(&corrupted_key, &handle);
    assert!(
        matches!(result, Err(crate::Error::ChecksumMismatch { .. })),
        "expected ChecksumMismatch for tampered on-disk key, got: {result:?}",
    );

    Ok(())
}

/// Verify that reading a blob with a caller key that differs from the
/// stored key (same length, different bytes) is rejected.
#[test]
fn blob_reader_wrong_caller_key_same_length_returns_invalid_header() -> crate::Result<()> {
    let id_generator = SequenceNumberCounter::default();

    let folder = tempfile::tempdir()?;
    let mut writer =
        crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None, Arc::new(StdFs))?
            .use_target_size(u64::MAX);

    let handle = writer.write(b"aaa", 0, b"value")?;

    let blob_file = writer.finish()?;
    let blob_file = blob_file.first().unwrap();

    let file = File::open(&blob_file.0.path)?;
    let reader = Reader::new(blob_file, &file);

    // Correct key works
    assert_eq!(reader.get(b"aaa", &handle)?, b"value");

    // Wrong key with same length → InvalidHeader from cross-check
    let result = reader.get(b"bbb", &handle);
    assert!(
        matches!(result, Err(crate::Error::InvalidHeader("Blob"))),
        "expected InvalidHeader(Blob) for wrong caller key, got: {result:?}",
    );

    Ok(())
}

/// Wrong caller key with different length is caught by the `key_len`
/// cross-check (header field vs caller key length) before the on-disk
/// key bytes are even read.
#[test]
fn blob_reader_wrong_caller_key_different_length_returns_invalid_header() -> crate::Result<()> {
    let id_generator = SequenceNumberCounter::default();

    let folder = tempfile::tempdir()?;
    let mut writer =
        crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None, Arc::new(StdFs))?
            .use_target_size(u64::MAX);

    let handle = writer.write(b"abc", 0, b"value")?;

    let blob_file = writer.finish()?;
    let blob_file = blob_file.first().unwrap();

    let file = File::open(&blob_file.0.path)?;
    let reader = Reader::new(blob_file, &file);

    // Shorter key
    let result = reader.get(b"ab", &handle);
    assert!(
        matches!(result, Err(crate::Error::InvalidHeader("Blob"))),
        "expected InvalidHeader for shorter key, got: {result:?}",
    );

    // Longer key
    let result = reader.get(b"abcd", &handle);
    assert!(
        matches!(result, Err(crate::Error::InvalidHeader("Blob"))),
        "expected InvalidHeader for longer key, got: {result:?}",
    );

    Ok(())
}

/// Tamper the value payload bytes (after the key) and verify the checksum
/// catches the corruption. This validates the end-to-end checksum path
/// for uncompressed blobs.
#[test]
fn blob_reader_corrupted_value_payload_triggers_checksum_mismatch() -> crate::Result<()> {
    let id_generator = SequenceNumberCounter::default();

    let folder = tempfile::tempdir()?;
    let mut writer =
        crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None, Arc::new(StdFs))?
            .use_target_size(u64::MAX);

    let handle = writer.write(b"key", 0, b"payload_data")?;

    let blob_file = writer.finish()?;
    let blob_file = blob_file.first().unwrap();

    // Value payload starts after header + key: offset + BLOB_HEADER_LEN_V4 + key_len
    let payload_offset =
        usize::try_from(handle.offset).unwrap() + BLOB_HEADER_LEN_V4 + b"key".len();
    let mut raw = std::fs::read(&blob_file.0.path)?;
    raw[payload_offset] ^= 0xFF; // flip bits in first value byte
    std::fs::write(&blob_file.0.path, &raw)?;

    let file = File::open(&blob_file.0.path)?;
    let reader = Reader::new(blob_file, &file);

    let result = reader.get(b"key", &handle);
    assert!(
        matches!(result, Err(crate::Error::ChecksumMismatch { .. })),
        "expected ChecksumMismatch for corrupted value, got: {result:?}",
    );

    Ok(())
}

/// Tamper on-disk key bytes in an lz4-compressed blob and verify the
/// cross-check catches the corruption before decompression runs.
#[test]
#[cfg(feature = "lz4")]
fn blob_reader_corrupted_on_disk_key_lz4_returns_invalid_header() -> crate::Result<()> {
    let id_generator = SequenceNumberCounter::default();

    let folder = tempfile::tempdir()?;
    let mut writer =
        crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None, Arc::new(StdFs))?
            .use_target_size(u64::MAX)
            .use_compression(CompressionType::Lz4);

    let handle = writer.write(b"abc", 0, b"value")?;

    let blob_file = writer.finish()?;
    let blob_file = blob_file.first().unwrap();

    let key_offset = usize::try_from(handle.offset).unwrap() + BLOB_HEADER_LEN_V4;
    let mut raw = std::fs::read(&blob_file.0.path)?;
    raw[key_offset] ^= 0xFF;
    std::fs::write(&blob_file.0.path, &raw)?;

    let file = File::open(&blob_file.0.path)?;
    let reader = Reader::new(blob_file, &file);

    let result = reader.get(b"abc", &handle);
    assert!(
        matches!(result, Err(crate::Error::InvalidHeader("Blob"))),
        "expected InvalidHeader for corrupted lz4 key, got: {result:?}",
    );

    Ok(())
}

/// Tamper on-disk key bytes in a zstd-compressed blob and verify the
/// cross-check catches the corruption before decompression runs.
#[test]
#[cfg(zstd_any)]
fn blob_reader_corrupted_on_disk_key_zstd_returns_invalid_header() -> crate::Result<()> {
    let id_generator = SequenceNumberCounter::default();

    let folder = tempfile::tempdir()?;
    let mut writer =
        crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None, Arc::new(StdFs))?
            .use_target_size(u64::MAX)
            .use_compression(CompressionType::Zstd(3));

    let handle = writer.write(b"abc", 0, b"value")?;

    let blob_file = writer.finish()?;
    let blob_file = blob_file.first().unwrap();

    let key_offset = usize::try_from(handle.offset).unwrap() + BLOB_HEADER_LEN_V4;
    let mut raw = std::fs::read(&blob_file.0.path)?;
    raw[key_offset] ^= 0xFF;
    std::fs::write(&blob_file.0.path, &raw)?;

    let file = File::open(&blob_file.0.path)?;
    let reader = Reader::new(blob_file, &file);

    let result = reader.get(b"abc", &handle);
    assert!(
        matches!(result, Err(crate::Error::InvalidHeader("Blob"))),
        "expected InvalidHeader for corrupted zstd key, got: {result:?}",
    );

    Ok(())
}

/// V4 header CRC detects seqno corruption — the primary motivating
/// case for upstream #278. A corrupted seqno could cause MVCC
/// time-travel returning wrong versions.
#[test]
fn blob_reader_v4_corrupted_seqno_detected_by_header_crc() -> crate::Result<()> {
    let id_generator = SequenceNumberCounter::default();

    let folder = tempfile::tempdir()?;
    let mut writer =
        crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None, Arc::new(StdFs))?
            .use_target_size(u64::MAX);

    let handle = writer.write(b"key", 42, b"value")?;

    let blob_file = writer.finish()?;
    let blob_file = blob_file.first().unwrap();

    // Tamper seqno: offset + magic(4) + checksum(16) = 20
    let seqno_offset = usize::try_from(handle.offset).unwrap() + 20;
    let mut raw = std::fs::read(&blob_file.0.path)?;
    // Change seqno from 42 to 99
    raw[seqno_offset..seqno_offset + 8].copy_from_slice(&99u64.to_le_bytes());
    std::fs::write(&blob_file.0.path, &raw)?;

    let file = File::open(&blob_file.0.path)?;
    let reader = Reader::new(blob_file, &file);

    let result = reader.get(b"key", &handle);
    assert!(
        matches!(result, Err(crate::Error::HeaderCrcMismatch { .. })),
        "expected HeaderCrcMismatch for corrupted seqno, got: {result:?}",
    );

    Ok(())
}

/// V4 header CRC field itself corrupted (header fields intact) is
/// detected before the data checksum check.
#[test]
fn blob_reader_v4_corrupted_header_crc_field_detected() -> crate::Result<()> {
    let id_generator = SequenceNumberCounter::default();

    let folder = tempfile::tempdir()?;
    let mut writer =
        crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None, Arc::new(StdFs))?
            .use_target_size(u64::MAX);

    let handle = writer.write(b"key", 0, b"value")?;

    let blob_file = writer.finish()?;
    let blob_file = blob_file.first().unwrap();

    // header_crc is at offset 38 (after magic+checksum+seqno+key_len+real_val_len+on_disk_val_len)
    let header_crc_offset = usize::try_from(handle.offset).unwrap() + 4 + 16 + 8 + 2 + 4 + 4;
    let mut raw = std::fs::read(&blob_file.0.path)?;
    raw[header_crc_offset] ^= 0xFF; // flip bits
    std::fs::write(&blob_file.0.path, &raw)?;

    let file = File::open(&blob_file.0.path)?;
    let reader = Reader::new(blob_file, &file);

    let result = reader.get(b"key", &handle);
    assert!(
        matches!(result, Err(crate::Error::HeaderCrcMismatch { .. })),
        "expected HeaderCrcMismatch for corrupted header_crc field, got: {result:?}",
    );

    Ok(())
}

/// Verify V4 header layout: `BLOB_HEADER_LEN_V4` = 42 bytes
/// (`magic:4` + `checksum:16` + `seqno:8` + `key_len:2` + `real_val_len:4` + `on_disk_val_len:4` + `header_crc:4`).
#[test]
fn blob_header_len_v4_is_42() {
    assert_eq!(BLOB_HEADER_LEN_V4, 42);
    assert_eq!(BLOB_HEADER_LEN_V3, 38);
}

/// Write a V3 blob file manually and verify the reader handles it
/// via the V3 backward compat path (no `header_crc` validation).
#[test]
fn blob_reader_v3_backward_compat_roundtrip() -> crate::Result<()> {
    use crate::file_accessor::FileAccessor;
    use crate::io::WriteBytesExt;
    use crate::vlog::{ValueHandle, blob_file::Inner as BlobFileInner};
    use std::io::Write;
    use std::sync::{Arc, atomic::AtomicBool};

    let folder = tempfile::tempdir()?;
    let blob_file_path = folder.path().join("0");

    let key = b"abc";
    let value = b"hello_v3";

    // V3 data checksum: xxh3_128(key + value) — no header_crc
    let checksum = {
        let mut hasher = xxhash_rust::xxh3::Xxh3::default();
        hasher.update(key);
        hasher.update(value);
        hasher.digest128()
    };

    // Write V3 blob file manually using sfa framing
    {
        let file = std::fs::File::create(&blob_file_path)?;
        let mut sfa_writer = crate::sfa::Writer::from_writer(file);
        sfa_writer.start("data")?;

        // V3 frame: BLOB magic, no header_crc
        sfa_writer.write_all(b"BLOB")?;
        sfa_writer.write_u128::<crate::io::LittleEndian>(checksum)?;
        sfa_writer.write_u64::<crate::io::LittleEndian>(42)?; // seqno
        #[expect(
            clippy::cast_possible_truncation,
            reason = "test key length fits in u16"
        )]
        sfa_writer.write_u16::<crate::io::LittleEndian>(key.len() as u16)?;
        #[expect(
            clippy::cast_possible_truncation,
            reason = "test value length fits in u32"
        )]
        sfa_writer.write_u32::<crate::io::LittleEndian>(value.len() as u32)?;
        #[expect(
            clippy::cast_possible_truncation,
            reason = "test value length fits in u32"
        )]
        sfa_writer.write_u32::<crate::io::LittleEndian>(value.len() as u32)?;
        sfa_writer.write_all(key)?;
        sfa_writer.write_all(value)?;

        // Write metadata
        sfa_writer.start("meta")?;
        let metadata = crate::vlog::blob_file::meta::Metadata {
            id: 0,
            version: 3,
            created_at: 0,
            item_count: 1,
            total_compressed_bytes: value.len() as u64,
            total_uncompressed_bytes: value.len() as u64,
            key_range: crate::KeyRange::new((key[..].into(), key[..].into())),
            compression: CompressionType::None,
        };
        metadata.encode_into(&mut sfa_writer)?;
        let inner = sfa_writer.into_inner()?;
        inner.sync_all()?;
    }

    // Construct a BlobFile with V3 metadata for the reader
    let file = File::open(&blob_file_path)?;
    let file2 = File::open(&blob_file_path)?;
    let blob_file = crate::BlobFile(Arc::new(BlobFileInner {
        id: 0,
        tree_id: 0,
        path: blob_file_path,
        meta: crate::vlog::blob_file::meta::Metadata {
            id: 0,
            version: 3,
            created_at: 0,
            item_count: 1,
            total_compressed_bytes: value.len() as u64,
            total_uncompressed_bytes: value.len() as u64,
            key_range: crate::KeyRange::new((key[..].into(), key[..].into())),
            compression: CompressionType::None,
        },
        is_deleted: AtomicBool::new(false),
        punch_on_drop: portable_atomic::AtomicU64::new(u64::MAX),
        checksum: crate::Checksum::from_raw(0),
        file_accessor: FileAccessor::File(Arc::new(file2)),
        fs: Arc::new(crate::fs::StdFs),
        deletion_pause: once_cell::race::OnceBox::new(),

        #[cfg(feature = "std")]
        background_deleter: once_cell::race::OnceBox::new(),
    }));

    let reader = Reader::new(&blob_file, &file);

    // V3 frame offset: sfa "data" segment header comes first.
    // Find actual data start via sfa reader.
    let sfa_reader = crate::sfa::Reader::new(&blob_file.0.path)?;
    let data_section = sfa_reader.toc().section(b"data").unwrap();
    let data_start = data_section.pos();

    let handle = ValueHandle {
        blob_file_id: 0,
        offset: data_start,
        #[expect(
            clippy::cast_possible_truncation,
            reason = "test value length fits in u32"
        )]
        on_disk_size: value.len() as u32,
    };

    let result = reader.get(key, &handle)?;
    assert_eq!(result, value);

    Ok(())
}

/// Write a blob with `ZstdDict`, then read it back without supplying a
/// dictionary.  Expect `ZstdDictMismatch { got: None }`.
#[test]
#[cfg(zstd_any)]
fn blob_reader_zstd_dict_missing_dict_returns_mismatch() -> crate::Result<()> {
    use crate::compression::ZstdDictionary;

    let id_generator = SequenceNumberCounter::default();
    let folder = tempfile::tempdir()?;

    let dict = ZstdDictionary::new(b"some_dictionary_content_for_testing");
    let compression = CompressionType::ZstdDict {
        level: 3,
        dict_id: dict.id(),
    };
    let dict_arc = Arc::new(dict);

    let mut writer =
        crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None, Arc::new(StdFs))?
            .use_target_size(u64::MAX)
            .use_compression(compression)
            .use_zstd_dictionary(Some(dict_arc));

    let handle = writer.write(b"key", 0, b"value-to-compress-with-dict")?;
    let blob_file = writer.finish()?;
    let blob_file = blob_file.first().unwrap();

    let file = File::open(&blob_file.0.path)?;
    // Reader created WITHOUT a dictionary
    let reader = Reader::new(blob_file, &file);

    let result = reader.get(b"key", &handle);
    assert!(
        matches!(
            result,
            Err(crate::Error::ZstdDictMismatch { got: None, .. })
        ),
        "expected ZstdDictMismatch{{got: None}} when dict is absent; got: {result:?}",
    );

    Ok(())
}

/// Write a blob with dict A, then read it back with dict B (different id).
/// Expect `ZstdDictMismatch { got: Some(B.id()) }`.
#[test]
#[cfg(zstd_any)]
fn blob_reader_zstd_dict_wrong_dict_id_returns_mismatch() -> crate::Result<()> {
    use crate::compression::ZstdDictionary;

    let id_generator = SequenceNumberCounter::default();
    let folder = tempfile::tempdir()?;

    let dict_a = ZstdDictionary::new(b"dictionary_a_content_for_testing");
    let dict_b = ZstdDictionary::new(b"dictionary_b_entirely_different_content");
    let compression = CompressionType::ZstdDict {
        level: 3,
        dict_id: dict_a.id(),
    };
    let dict_a_arc = Arc::new(dict_a);
    let dict_b_arc = Arc::new(dict_b);

    let mut writer =
        crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None, Arc::new(StdFs))?
            .use_target_size(u64::MAX)
            .use_compression(compression)
            .use_zstd_dictionary(Some(dict_a_arc));

    let handle = writer.write(b"key", 0, b"value-compressed-with-dict-a")?;
    let blob_file = writer.finish()?;
    let blob_file = blob_file.first().unwrap();

    let file = File::open(&blob_file.0.path)?;
    // Reader supplied with dict B (wrong id)
    let reader = Reader::new(blob_file, &file).with_dict(Some(&dict_b_arc));

    let result = reader.get(b"key", &handle);
    assert!(
        matches!(result, Err(crate::Error::ZstdDictMismatch { .. })),
        "expected ZstdDictMismatch when wrong dict id provided; got: {result:?}",
    );

    Ok(())
}

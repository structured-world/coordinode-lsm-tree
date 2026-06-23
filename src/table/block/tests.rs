use super::*;
use test_log::test;

/// A pathological-but-valid shard config (1 data shard, 255 parity shards)
/// over a large block makes `shard_bytes * parity_shards` exceed u32. The
/// parity length must saturate to `u32::MAX` (it is rejected against the actual
/// block downstream), never wrap or panic.
#[test]
fn expected_parity_len_saturates_on_huge_parity_product() {
    let data_length = 100 * 1024 * 1024; // 100 MiB
    let params = EccParams::Shard {
        data_shards: 1,
        parity_shards: 255,
    };
    assert_eq!(expected_parity_len(data_length, params), u32::MAX);
}

/// A `u32::MAX` data length with a single data shard makes `ceil` equal an
/// odd `u32::MAX`; rounding it up to an even shard size must saturate, not
/// overflow `u32`. A corrupt header reaching this path is rejected
/// downstream, so the clamp must not panic here.
#[test]
fn expected_parity_len_saturates_on_max_data_length_even_rounding() {
    let params = EccParams::Shard {
        data_shards: 1,
        parity_shards: 2,
    };
    assert_eq!(expected_parity_len(u32::MAX, params), u32::MAX);
}

/// Result of [`write_block_to_tempfile`]. Bundles the open
/// file, the pre-computed [`crate::table::BlockHandle`], and
/// the owning [`tempfile::TempDir`].
///
/// **Drop-order safety lives entirely in the struct field
/// order** (Windows portability constraint). Rust drops
/// struct fields in declaration order, so `file` is first
/// and `dir` LAST: when a `TempBlock` value goes out of
/// scope, the open file handle closes before the `TempDir`
/// removes the directory. Windows rejects directory
/// removal while a file inside it is still open.
///
/// **Callers SHOULD keep the result as a single binding**
/// — borrow `&tmp.file` and copy `tmp.handle` (it's `Copy`)
/// instead of destructuring. Destructuring opens a
/// foot-gun: local bindings drop in REVERSE declaration
/// order, so a pattern like
/// `let TempBlock { file, handle, dir: _dir } = ...?;`
/// would close `dir` before `file` and break the
/// invariant. Holding the whole struct as one local
/// (`let tmp = ...?;`) makes the struct field order the
/// SOLE source of truth — no pattern can break it.
struct TempBlock {
    /// Open read-only handle on the persisted block.
    /// Declared first so it drops before `dir`.
    file: std::fs::File,
    /// Pre-computed handle: offset 0, length = header +
    /// payload, ready to pass straight into `Block::from_file`.
    handle: crate::table::BlockHandle,
    /// Drop guard for the tempdir. Kept bound for the test
    /// lifetime so the file inside the directory survives
    /// long enough to be reopened by the test reader, then
    /// dropped at end-of-scope to reclaim the directory.
    /// Declared LAST so the file handle above closes before
    /// this removes the directory.
    ///
    /// Feature-matrix-gated suppression: under `--all-features`
    /// the ECC-corruption tests read `.path()` to flip on-disk
    /// bytes, so the field IS used and an unconditional
    /// `#[allow(dead_code)]` would clash with
    /// `clippy::used_underscore_binding` if we tried to prefix
    /// the field name with `_`. Under default features the
    /// `page_ecc` tests are not compiled, the field genuinely
    /// IS dead, and the suppression silences the warning.
    #[cfg_attr(
        not(feature = "page_ecc"),
        expect(dead_code, reason = "drop guard; only read by page_ecc-gated tests")
    )]
    dir: tempfile::TempDir,
}

/// Shared scaffold for `Block::from_file` roundtrip tests: writes
/// `data` through `Block::write_into` directly into a fresh
/// tempdir-backed file, reopens it read-only, and returns a
/// [`TempBlock`] bundling the open file, the pre-computed
/// [`crate::table::BlockHandle`], and the owning [`tempfile::TempDir`]
/// (kept bound for the test's lifetime). The streaming write
/// avoids an intermediate `Vec<u8>` — relevant for the 32 KiB
/// large-payload encryption test.
///
/// Centralises the ~10× write/sync/reopen/handle boilerplate that
/// the `from_file` tests below would otherwise duplicate.
fn write_block_to_tempfile(
    data: &[u8],
    identity: BlockIdentity,
    transform: &BlockTransform<'_>,
) -> crate::Result<TempBlock> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("block");
    // Scope the write-side file handle so the read-side
    // `File::open` below sees a fully-flushed file. Dropping
    // closes; sync_all flushes before close.
    let header = {
        let mut file = std::fs::File::create(&path)?;
        let header = Block::write_into(&mut file, data, identity, transform)?;
        file.sync_all()?;
        header
    };
    let file = std::fs::File::open(&path)?;
    // Size the handle under the transform's actual scheme so the helper
    // works for any ECC layout, not just RS(4,2). `on_disk_size()` hardcodes
    // RS(4,2) and would mis-size a block written with a different scheme.
    let handle = crate::table::BlockHandle::new(
        BlockOffset(0),
        header.on_disk_size_with(transform.ecc_params()),
    );
    Ok(TempBlock { file, handle, dir })
}

#[test]
fn block_from_file_roundtrip_uncompressed() -> crate::Result<()> {
    let data = b"abcdefabcdefabcdef";
    let tmp = write_block_to_tempfile(
        data,
        crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
        &crate::table::block::BlockTransform::from_parts(
            CompressionType::None,
            None,
            #[cfg(zstd_any)]
            None,
        )?,
    )?;
    let block = Block::from_file(
        &tmp.file,
        tmp.handle,
        crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
        &crate::table::block::BlockTransform::from_parts(
            CompressionType::None,
            None,
            #[cfg(zstd_any)]
            None,
        )?,
    )?;
    assert_eq!(data, &*block.data);
    Ok(())
}

#[cfg(feature = "zstd")]
#[test]
fn read_data_frame_returns_decompressible_zstd_frame() -> crate::Result<()> {
    // Repetitive payload so zstd produces a real (smaller) compressed frame.
    let data: Vec<u8> = (0..40_000u32).map(|i| (i % 64) as u8).collect();
    let transform =
        crate::table::block::BlockTransform::from_parts(CompressionType::Zstd(3), None, None)?;
    let tmp = write_block_to_tempfile(
        &data,
        crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
        &transform,
    )?;

    let (header, frame, _corrected) = Block::read_data_frame(&tmp.file, tmp.handle, &transform)?;
    // The returned bytes are the COMPRESSED frame: it must be smaller than
    // the payload and must decompress back to the original (proving
    // read_data_frame skipped decode yet returned a valid frame).
    assert!(
        frame.len() < data.len(),
        "frame must be compressed (got {} for {} bytes)",
        frame.len(),
        data.len(),
    );
    let decompressed = crate::compression::ZstdBackend::decompress(
        &frame,
        header.uncompressed_length as usize + 1,
    )?;
    assert_eq!(decompressed, data, "frame must decompress to the original");
    Ok(())
}

#[cfg(feature = "zstd")]
#[test]
fn read_data_frame_rejects_oversized_handle() -> crate::Result<()> {
    // A handle declaring an absurd on-disk size must be rejected by the
    // pre-allocation cap before any read / allocation, mirroring
    // `from_file_with_status`.
    let data: Vec<u8> = (0..1_000u32).map(|i| (i % 64) as u8).collect();
    let transform =
        crate::table::block::BlockTransform::from_parts(CompressionType::Zstd(3), None, None)?;
    let tmp = write_block_to_tempfile(
        &data,
        crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
        &transform,
    )?;
    let oversized = BlockHandle::new(tmp.handle.offset(), u32::MAX);
    let err = Block::read_data_frame(&tmp.file, oversized, &transform)
        .expect_err("oversized handle must be rejected");
    assert!(
        matches!(err, crate::Error::DecompressedSizeTooLarge { .. }),
        "expected DecompressedSizeTooLarge, got {err:?}",
    );
    Ok(())
}

#[test]
#[cfg(feature = "lz4")]
fn block_from_file_roundtrip_lz4() -> crate::Result<()> {
    let data = b"abcdefabcdefabcdef";
    let tmp = write_block_to_tempfile(
        data,
        crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
        &crate::table::block::BlockTransform::from_parts(
            CompressionType::Lz4,
            None,
            #[cfg(zstd_any)]
            None,
        )?,
    )?;
    let block = Block::from_file(
        &tmp.file,
        tmp.handle,
        crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
        &crate::table::block::BlockTransform::from_parts(
            CompressionType::Lz4,
            None,
            #[cfg(zstd_any)]
            None,
        )?,
    )?;
    assert_eq!(data, &*block.data);
    Ok(())
}

#[test]
#[cfg(zstd_any)]
fn block_from_file_roundtrip_zstd() -> crate::Result<()> {
    let data = b"abcdefabcdefabcdef";
    let tmp = write_block_to_tempfile(
        data,
        BlockIdentity::for_test(0, BlockType::Data),
        &crate::table::block::BlockTransform::from_parts(
            CompressionType::Zstd(3),
            None,
            #[cfg(zstd_any)]
            None,
        )?,
    )?;
    let block = Block::from_file(
        &tmp.file,
        tmp.handle,
        BlockIdentity::for_test(0, BlockType::Data),
        &crate::table::block::BlockTransform::from_parts(
            CompressionType::Zstd(3),
            None,
            #[cfg(zstd_any)]
            None,
        )?,
    )?;
    assert_eq!(data, &*block.data);
    Ok(())
}

#[test]
fn block_roundtrip_uncompressed() -> crate::Result<()> {
    let mut writer = vec![];

    Block::write_into(
        &mut writer,
        b"abcdefabcdefabcdef",
        crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
        &crate::table::block::BlockTransform::from_parts(
            CompressionType::None,
            None,
            #[cfg(zstd_any)]
            None,
        )?,
    )?;

    {
        let mut reader = &writer[..];
        let block = Block::from_reader(
            &mut reader,
            crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::None,
                None,
                #[cfg(zstd_any)]
                None,
            )?,
        )?;
        assert_eq!(b"abcdefabcdefabcdef", &*block.data);
    }

    Ok(())
}
#[test]
#[cfg(feature = "lz4")]
fn block_roundtrip_lz4() -> crate::Result<()> {
    let mut writer = vec![];

    Block::write_into(
        &mut writer,
        b"abcdefabcdefabcdef",
        crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
        &crate::table::block::BlockTransform::from_parts(
            CompressionType::Lz4,
            None,
            #[cfg(zstd_any)]
            None,
        )?,
    )?;

    {
        let mut reader = &writer[..];
        let block = Block::from_reader(
            &mut reader,
            crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Lz4,
                None,
                #[cfg(zstd_any)]
                None,
            )?,
        )?;
        assert_eq!(b"abcdefabcdefabcdef", &*block.data);
    }

    Ok(())
}

#[test]
#[cfg(feature = "lz4")]
fn block_reject_absurd_uncompressed_length() {
    use crate::coding::Encode;

    // Write a valid lz4-compressed block first so we get the right header format
    let mut buf = vec![];
    Block::write_into(
        &mut buf,
        b"hello",
        crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
        &crate::table::block::BlockTransform::from_parts(
            CompressionType::Lz4,
            None,
            #[cfg(zstd_any)]
            None,
        )
        .unwrap(),
    )
    .unwrap();

    // Tamper the header: set uncompressed_length to u32::MAX.
    // The block checksum only covers the compressed payload bytes; it does not include
    // header fields. The header itself has its own checksum, which we recompute below
    // by re-encoding the modified header, so the tampered block remains internally
    // consistent while exercising the DecompressedSizeTooLarge path.
    let mut reader = &buf[..];
    let mut header = Header::decode_from(&mut reader).unwrap();
    let compressed_payload: Vec<u8> = reader.to_vec();

    header.uncompressed_length = u32::MAX;
    let mut tampered = header.encode_into_vec();
    tampered.extend_from_slice(&compressed_payload);

    let mut r = &tampered[..];
    let result = Block::from_reader(
        &mut r,
        crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
        &crate::table::block::BlockTransform::from_parts(
            CompressionType::Lz4,
            None,
            #[cfg(zstd_any)]
            None,
        )
        .unwrap(),
    );

    assert!(
        matches!(&result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
        "expected DecompressedSizeTooLarge, got: {:?}",
        result.err(),
    );
}

#[test]
#[cfg(feature = "lz4")]
fn block_zero_uncompressed_length_with_data_fails_decompress() {
    use crate::coding::Encode;

    // Zero uncompressed_length is allowed (valid for empty blocks), but when
    // the compressed payload is non-empty, lz4 decompression will fail because
    // the output buffer is zero-sized.
    let mut buf = vec![];
    Block::write_into(
        &mut buf,
        b"hello",
        crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
        &crate::table::block::BlockTransform::from_parts(
            CompressionType::Lz4,
            None,
            #[cfg(zstd_any)]
            None,
        )
        .unwrap(),
    )
    .unwrap();

    let mut reader = &buf[..];
    let mut header = Header::decode_from(&mut reader).unwrap();
    let compressed_payload: Vec<u8> = reader.to_vec();

    header.uncompressed_length = 0;
    let mut tampered = header.encode_into_vec();
    tampered.extend_from_slice(&compressed_payload);

    let mut r = &tampered[..];
    let result = Block::from_reader(
        &mut r,
        crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
        &crate::table::block::BlockTransform::from_parts(
            CompressionType::Lz4,
            None,
            #[cfg(zstd_any)]
            None,
        )
        .unwrap(),
    );

    assert!(
        matches!(&result, Err(crate::Error::Decompress(_))),
        "expected Decompress error, got: {:?}",
        result.err(),
    );
}

#[test]
#[cfg(feature = "lz4")]
fn lz4_corrupted_uncompressed_length_triggers_decompress_error() {
    use crate::coding::Encode;
    use std::io::Cursor;

    let payload: &[u8] = b"hello world";

    // Compress with lz4 using the block format
    let compressed = lz4_flex::compress(payload);

    // Build a header with corrupted uncompressed_length (1 byte too large)
    let data_length = compressed.len() as u32;
    let uncompressed_length_correct = payload.len() as u32;
    let uncompressed_length_corrupted = uncompressed_length_correct + 1;

    let checksum = Checksum::from_raw(crate::hash::hash128(&compressed));

    let header = Header {
        data_length,
        uncompressed_length: uncompressed_length_corrupted,
        checksum,
        ..Header::test_dummy(BlockType::Data)
    };

    let mut buf = header.encode_into_vec();
    buf.extend_from_slice(&compressed);

    let mut cursor = Cursor::new(buf);
    let result = Block::from_reader(
        &mut cursor,
        crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
        &crate::table::block::BlockTransform::from_parts(
            CompressionType::Lz4,
            None,
            #[cfg(zstd_any)]
            None,
        )
        .unwrap(),
    );

    match result {
        Err(crate::Error::Decompress(CompressionType::Lz4)) => { /* expected */ }
        Ok(_) => panic!("expected Error::Decompress, but got Ok(Block)"),
        Err(other) => panic!("expected Error::Decompress, got different error: {other:?}"),
    }
}

#[test]
#[cfg(feature = "lz4")]
fn block_from_file_reject_absurd_uncompressed_length() {
    use crate::coding::Encode;
    use std::io::Write;

    let mut buf = vec![];
    Block::write_into(
        &mut buf,
        b"hello",
        crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
        &crate::table::block::BlockTransform::from_parts(
            CompressionType::Lz4,
            None,
            #[cfg(zstd_any)]
            None,
        )
        .unwrap(),
    )
    .unwrap();

    // Tamper: set uncompressed_length to u32::MAX.
    // The block checksum only covers the compressed payload bytes; it does not include
    // header fields. The header itself has its own checksum, which we recompute below
    // by re-encoding the modified header, so the tampered block remains internally
    // consistent while exercising the DecompressedSizeTooLarge path.
    let mut reader = &buf[..];
    let mut header = Header::decode_from(&mut reader).unwrap();
    let compressed_payload: Vec<u8> = reader.to_vec();

    header.uncompressed_length = u32::MAX;
    let mut tampered = header.encode_into_vec();
    tampered.extend_from_slice(&compressed_payload);

    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    tmp.write_all(&tampered).unwrap();
    tmp.flush().unwrap();
    let file = std::fs::File::open(tmp.path()).unwrap();

    let handle = crate::table::BlockHandle::new(BlockOffset(0), tampered.len() as u32);
    let result = Block::from_file(
        &file,
        handle,
        crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
        &crate::table::block::BlockTransform::from_parts(
            CompressionType::Lz4,
            None,
            #[cfg(zstd_any)]
            None,
        )
        .unwrap(),
    );

    assert!(
        matches!(&result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
        "expected DecompressedSizeTooLarge, got: {:?}",
        result.err(),
    );
}

#[test]
#[cfg(feature = "lz4")]
fn block_from_file_zero_uncompressed_length_with_data_fails_decompress() {
    use crate::coding::Encode;
    use std::io::Write;

    let mut buf = vec![];
    Block::write_into(
        &mut buf,
        b"hello",
        crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
        &crate::table::block::BlockTransform::from_parts(
            CompressionType::Lz4,
            None,
            #[cfg(zstd_any)]
            None,
        )
        .unwrap(),
    )
    .unwrap();

    let mut reader = &buf[..];
    let mut header = Header::decode_from(&mut reader).unwrap();
    let compressed_payload: Vec<u8> = reader.to_vec();

    header.uncompressed_length = 0;
    let mut tampered = header.encode_into_vec();
    tampered.extend_from_slice(&compressed_payload);

    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    tmp.write_all(&tampered).unwrap();
    tmp.flush().unwrap();
    let file = std::fs::File::open(tmp.path()).unwrap();

    let handle = crate::table::BlockHandle::new(BlockOffset(0), tampered.len() as u32);
    let result = Block::from_file(
        &file,
        handle,
        crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
        &crate::table::block::BlockTransform::from_parts(
            CompressionType::Lz4,
            None,
            #[cfg(zstd_any)]
            None,
        )
        .unwrap(),
    );

    assert!(
        matches!(&result, Err(crate::Error::Decompress(_))),
        "expected Decompress error, got: {:?}",
        result.err(),
    );
}

#[test]
fn block_from_reader_reject_absurd_data_length() {
    use crate::coding::Encode;

    let mut buf = vec![];
    Block::write_into(
        &mut buf,
        b"hello",
        crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
        &crate::table::block::BlockTransform::from_parts(
            CompressionType::None,
            None,
            #[cfg(zstd_any)]
            None,
        )
        .unwrap(),
    )
    .unwrap();

    let mut reader = &buf[..];
    let mut header = Header::decode_from(&mut reader).unwrap();
    let payload: Vec<u8> = reader.to_vec();

    // Set data_length past the limit (no encryption → overhead is 0)
    header.data_length = MAX_DECOMPRESSION_SIZE + 1;
    let mut tampered = header.encode_into_vec();
    tampered.extend_from_slice(&payload);

    let mut r = &tampered[..];
    let result = Block::from_reader(
        &mut r,
        crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
        &crate::table::block::BlockTransform::from_parts(
            CompressionType::None,
            None,
            #[cfg(zstd_any)]
            None,
        )
        .unwrap(),
    );

    assert!(
        matches!(&result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
        "expected DecompressedSizeTooLarge, got: {:?}",
        result.err(),
    );
}

#[test]
fn block_from_file_reject_oversized_handle() {
    use std::io::Write;

    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    tmp.write_all(b"dummy").unwrap();
    tmp.flush().unwrap();
    let file = std::fs::File::open(tmp.path()).unwrap();

    let handle = crate::table::BlockHandle::new(BlockOffset(0), u32::MAX);
    let result = Block::from_file(
        &file,
        handle,
        crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
        &crate::table::block::BlockTransform::from_parts(
            CompressionType::None,
            None,
            #[cfg(zstd_any)]
            None,
        )
        .unwrap(),
    );

    assert!(
        matches!(&result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
        "expected DecompressedSizeTooLarge, got: {:?}",
        result.err(),
    );
}

#[test]
#[cfg(zstd_any)]
fn zstd_corrupted_uncompressed_length_triggers_decompress_error() {
    use crate::coding::Encode;
    use std::io::Cursor;

    let payload: &[u8] = b"hello world";

    // Fully-qualified path resolves the trait method unambiguously without
    // needing `use CompressionProvider` in this test module scope.
    let compressed =
        crate::compression::ZstdBackend::compress(payload, 3).expect("zstd compress failed");

    let data_length = compressed.len() as u32;
    let uncompressed_length_corrupted = payload.len() as u32 + 1;

    let checksum = Checksum::from_raw(crate::hash::hash128(&compressed));

    let header = Header {
        data_length,
        uncompressed_length: uncompressed_length_corrupted,
        checksum,
        ..Header::test_dummy(BlockType::Data)
    };

    let mut buf = header.encode_into_vec();
    buf.extend_from_slice(&compressed);

    let mut cursor = Cursor::new(buf);
    let result = Block::from_reader(
        &mut cursor,
        crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
        &crate::table::block::BlockTransform::from_parts(
            CompressionType::Zstd(3),
            None,
            #[cfg(zstd_any)]
            None,
        )
        .unwrap(),
    );

    match result {
        Err(crate::Error::Decompress(CompressionType::Zstd(_))) => { /* expected */ }
        Ok(_) => panic!("expected Error::Decompress, but got Ok(Block)"),
        Err(other) => panic!("expected Error::Decompress, got different error: {other:?}"),
    }
}

#[test]
#[cfg(zstd_any)]
fn zstd_decreased_uncompressed_length_triggers_decompress_error() {
    use crate::coding::Encode;
    use std::io::Cursor;

    let payload: &[u8] = b"hello world hello world hello world";

    let compressed =
        crate::compression::ZstdBackend::compress(payload, 3).expect("zstd compress failed");

    let data_length = compressed.len() as u32;
    // Set uncompressed_length smaller than real decompressed size.
    // The backend decompresses into a buffer of this size; the real output
    // exceeds it, triggering the capacity/length mismatch error.
    let uncompressed_length_too_small = payload.len() as u32 - 1;

    let checksum = Checksum::from_raw(crate::hash::hash128(&compressed));

    let header = Header {
        data_length,
        uncompressed_length: uncompressed_length_too_small,
        checksum,
        ..Header::test_dummy(BlockType::Data)
    };

    let mut buf = header.encode_into_vec();
    buf.extend_from_slice(&compressed);

    let mut cursor = Cursor::new(buf);
    let result = Block::from_reader(
        &mut cursor,
        crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
        &crate::table::block::BlockTransform::from_parts(
            CompressionType::Zstd(3),
            None,
            #[cfg(zstd_any)]
            None,
        )
        .unwrap(),
    );

    match result {
        Err(crate::Error::Decompress(CompressionType::Zstd(_))) => { /* expected */ }
        Ok(_) => panic!("expected Error::Decompress, but got Ok(Block)"),
        Err(other) => panic!("expected Error::Decompress, got different error: {other:?}"),
    }
}

#[test]
#[cfg(zstd_any)]
fn block_roundtrip_zstd() -> crate::Result<()> {
    let mut writer = vec![];

    Block::write_into(
        &mut writer,
        b"abcdefabcdefabcdef",
        crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
        &crate::table::block::BlockTransform::from_parts(
            CompressionType::Zstd(3),
            None,
            #[cfg(zstd_any)]
            None,
        )?,
    )?;

    {
        let mut reader = &writer[..];
        let block = Block::from_reader(
            &mut reader,
            crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Zstd(3),
                None,
                #[cfg(zstd_any)]
                None,
            )?,
        )?;
        assert_eq!(b"abcdefabcdefabcdef", &*block.data);
    }

    Ok(())
}

#[test]
fn block_write_rejects_oversized_payload() {
    let oversized = vec![0u8; MAX_DECOMPRESSION_SIZE as usize + 1];
    let mut sink = std::io::sink();
    let result = Block::write_into(
        &mut sink,
        &oversized,
        crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
        &crate::table::block::BlockTransform::from_parts(
            CompressionType::None,
            None,
            #[cfg(zstd_any)]
            None,
        )
        .unwrap(),
    );
    assert!(
        matches!(result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
        "expected DecompressedSizeTooLarge, got: {result:?}",
    );
}

#[test]
#[cfg(zstd_any)]
fn block_roundtrip_zstd_large_data() -> crate::Result<()> {
    let data = vec![0xABu8; 64 * 1024]; // 64KB
    let mut writer = vec![];

    Block::write_into(
        &mut writer,
        &data,
        crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
        &crate::table::block::BlockTransform::from_parts(
            CompressionType::Zstd(3),
            None,
            #[cfg(zstd_any)]
            None,
        )?,
    )?;

    // Verify compression actually reduced size
    assert!(
        writer.len() < data.len(),
        "zstd should compress repeated data"
    );

    {
        let mut reader = &writer[..];
        let block = Block::from_reader(
            &mut reader,
            crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Zstd(3),
                None,
                #[cfg(zstd_any)]
                None,
            )?,
        )?;
        assert_eq!(&*block.data, &data[..]);
    }

    Ok(())
}

// --- Encrypted block roundtrip tests ---
// These exercise the encrypt_vec/decrypt_vec code paths in write_into,
// from_reader, and from_file that are untouched by the non-encrypted tests.
//
// NOTE: The tempfile + write + reopen + handle pattern is duplicated across
// from_file tests (both encrypted and non-encrypted). Tracked in #127.

#[cfg(feature = "encryption")]
mod encrypted {
    use crate::table::block::*;

    fn test_provider() -> crate::encryption::Aes256GcmProvider {
        crate::encryption::Aes256GcmProvider::new(&[0x42; 32])
    }

    #[test]
    fn block_roundtrip_encrypted_uncompressed() -> crate::Result<()> {
        let enc = test_provider();
        let data = b"plaintext block data for encryption test";
        let mut writer = vec![];

        Block::write_into(
            &mut writer,
            data,
            crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::None,
                Some(&enc),
                #[cfg(zstd_any)]
                None,
            )?,
        )?;

        let mut reader = &writer[..];
        let block = Block::from_reader(
            &mut reader,
            crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::None,
                Some(&enc),
                #[cfg(zstd_any)]
                None,
            )?,
        )?;
        assert_eq!(data, &*block.data);
        Ok(())
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn block_roundtrip_encrypted_lz4() -> crate::Result<()> {
        let enc = test_provider();
        let data = b"abcdefabcdefabcdef";
        let mut writer = vec![];

        Block::write_into(
            &mut writer,
            data,
            crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Lz4,
                Some(&enc),
                #[cfg(zstd_any)]
                None,
            )?,
        )?;

        let mut reader = &writer[..];
        let block = Block::from_reader(
            &mut reader,
            crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Lz4,
                Some(&enc),
                #[cfg(zstd_any)]
                None,
            )?,
        )?;
        assert_eq!(data, &*block.data);
        Ok(())
    }

    #[test]
    #[cfg(zstd_any)]
    fn block_roundtrip_encrypted_zstd() -> crate::Result<()> {
        let enc = test_provider();
        let data = b"abcdefabcdefabcdef";
        let mut writer = vec![];

        Block::write_into(
            &mut writer,
            data,
            crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Zstd(3),
                Some(&enc),
                #[cfg(zstd_any)]
                None,
            )?,
        )?;

        let mut reader = &writer[..];
        let block = Block::from_reader(
            &mut reader,
            crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Zstd(3),
                Some(&enc),
                #[cfg(zstd_any)]
                None,
            )?,
        )?;
        assert_eq!(data, &*block.data);
        Ok(())
    }

    #[test]
    fn block_from_file_encrypted_uncompressed() -> crate::Result<()> {
        let enc = test_provider();
        let data = b"plaintext block data for from_file encryption test";
        let tmp = super::write_block_to_tempfile(
            data,
            crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::None,
                Some(&enc),
                #[cfg(zstd_any)]
                None,
            )?,
        )?;
        let block = Block::from_file(
            &tmp.file,
            tmp.handle,
            crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::None,
                Some(&enc),
                #[cfg(zstd_any)]
                None,
            )?,
        )?;
        assert_eq!(data, &*block.data);
        Ok(())
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn block_from_file_encrypted_lz4() -> crate::Result<()> {
        let enc = test_provider();
        let data = b"abcdefabcdefabcdef";
        let tmp = super::write_block_to_tempfile(
            data,
            crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Lz4,
                Some(&enc),
                #[cfg(zstd_any)]
                None,
            )?,
        )?;
        let block = Block::from_file(
            &tmp.file,
            tmp.handle,
            crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Lz4,
                Some(&enc),
                #[cfg(zstd_any)]
                None,
            )?,
        )?;
        assert_eq!(data, &*block.data);
        Ok(())
    }

    #[test]
    #[cfg(zstd_any)]
    fn block_from_file_encrypted_zstd() -> crate::Result<()> {
        let enc = test_provider();
        let data = b"abcdefabcdefabcdef";
        let tmp = super::write_block_to_tempfile(
            data,
            crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Zstd(3),
                Some(&enc),
                #[cfg(zstd_any)]
                None,
            )?,
        )?;
        let block = Block::from_file(
            &tmp.file,
            tmp.handle,
            crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Zstd(3),
                Some(&enc),
                #[cfg(zstd_any)]
                None,
            )?,
        )?;
        assert_eq!(data, &*block.data);
        Ok(())
    }

    #[test]
    fn block_from_file_encrypted_wrong_key_fails() -> crate::Result<()> {
        let enc_write = test_provider();
        let enc_read = crate::encryption::Aes256GcmProvider::new(&[0x99; 32]);
        let data = b"encrypted block data";
        let tmp = super::write_block_to_tempfile(
            data,
            crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::None,
                Some(&enc_write),
                #[cfg(zstd_any)]
                None,
            )?,
        )?;
        let result = Block::from_file(
            &tmp.file,
            tmp.handle,
            crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::None,
                Some(&enc_read),
                #[cfg(zstd_any)]
                None,
            )?,
        );
        assert!(
            matches!(result, Err(crate::Error::Decrypt(_))),
            "expected Decrypt error for wrong key, got: {:?}",
            result.err(),
        );
        Ok(())
    }

    #[test]
    fn block_from_reader_encrypted_wrong_key_fails() -> crate::Result<()> {
        let enc_write = test_provider();
        let enc_read = crate::encryption::Aes256GcmProvider::new(&[0x99; 32]);
        let data = b"encrypted block data";
        let mut writer = vec![];

        Block::write_into(
            &mut writer,
            data,
            crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::None,
                Some(&enc_write),
                #[cfg(zstd_any)]
                None,
            )?,
        )?;

        let mut reader = &writer[..];
        let result = Block::from_reader(
            &mut reader,
            crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::None,
                Some(&enc_read),
                #[cfg(zstd_any)]
                None,
            )?,
        );
        assert!(
            matches!(result, Err(crate::Error::Decrypt(_))),
            "expected Decrypt error for wrong key, got: {:?}",
            result.err(),
        );
        Ok(())
    }

    #[test]
    fn block_from_file_encrypted_checksum_tamper_detected() -> crate::Result<()> {
        use std::io::Write;

        let enc = test_provider();
        let data = b"data for tamper test";
        let mut buf = vec![];
        let header = Block::write_into(
            &mut buf,
            data,
            crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::None,
                Some(&enc),
                #[cfg(zstd_any)]
                None,
            )?,
        )?;

        // Tamper a byte in the encrypted payload (after header)
        let mid = Header::MIN_LEN + 1;
        if mid < buf.len() {
            #[expect(clippy::indexing_slicing, reason = "mid < buf.len() checked above")]
            {
                buf[mid] ^= 0xFF;
            }
        }

        let dir = tempfile::tempdir()?;
        let path = dir.path().join("block");
        let mut file = std::fs::File::create(&path)?;
        file.write_all(&buf)?;
        file.sync_all()?;
        drop(file);

        let file = std::fs::File::open(&path)?;
        let handle = crate::table::BlockHandle::new(BlockOffset(0), header.on_disk_size());
        let result = Block::from_file(
            &file,
            handle,
            crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::None,
                Some(&enc),
                #[cfg(zstd_any)]
                None,
            )?,
        );
        assert!(
            matches!(result, Err(crate::Error::ChecksumMismatch { .. })),
            "expected ChecksumMismatch for tampered data, got: {:?}",
            result.err(),
        );
        Ok(())
    }

    #[test]
    fn block_from_file_encrypted_undersized_handle_rejected() -> crate::Result<()> {
        use std::io::Write;

        let enc = test_provider();
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("block");
        let mut file = std::fs::File::create(&path)?;
        file.write_all(b"tiny")?;
        file.sync_all()?;
        drop(file);

        let file = std::fs::File::open(&path)?;
        // Handle size smaller than Header::MIN_LEN
        let handle = crate::table::BlockHandle::new(BlockOffset(0), 2);
        let result = Block::from_file(
            &file,
            handle,
            crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::None,
                Some(&enc),
                #[cfg(zstd_any)]
                None,
            )?,
        );

        assert!(
            matches!(result, Err(crate::Error::InvalidHeader(_))),
            "expected InvalidHeader for undersized handle, got: {:?}",
            result.err(),
        );
        Ok(())
    }

    #[test]
    fn block_from_file_encrypted_uncompressed_large_payload() -> crate::Result<()> {
        let enc = test_provider();
        let data = vec![0xBB_u8; 32 * 1024]; // 32 KiB
        let tmp = super::write_block_to_tempfile(
            &data,
            crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::None,
                Some(&enc),
                #[cfg(zstd_any)]
                None,
            )?,
        )?;
        let block = Block::from_file(
            &tmp.file,
            tmp.handle,
            crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::None,
                Some(&enc),
                #[cfg(zstd_any)]
                None,
            )?,
        )?;
        assert_eq!(&*block.data, &data[..]);
        Ok(())
    }

    #[test]
    fn block_roundtrip_encrypted_uncompressed_large() -> crate::Result<()> {
        let enc = test_provider();
        let data = vec![0xCC_u8; 32 * 1024]; // 32 KiB
        let mut writer = vec![];

        Block::write_into(
            &mut writer,
            &data,
            crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::None,
                Some(&enc),
                #[cfg(zstd_any)]
                None,
            )?,
        )?;

        let mut reader = &writer[..];
        let block = Block::from_reader(
            &mut reader,
            crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::None,
                Some(&enc),
                #[cfg(zstd_any)]
                None,
            )?,
        )?;
        assert_eq!(&*block.data, &data[..]);
        Ok(())
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn block_roundtrip_encrypted_lz4_large() -> crate::Result<()> {
        let enc = test_provider();
        let data = vec![0xDD_u8; 32 * 1024]; // 32 KiB
        let mut writer = vec![];

        Block::write_into(
            &mut writer,
            &data,
            crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Lz4,
                Some(&enc),
                #[cfg(zstd_any)]
                None,
            )?,
        )?;

        let mut reader = &writer[..];
        let block = Block::from_reader(
            &mut reader,
            crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Lz4,
                Some(&enc),
                #[cfg(zstd_any)]
                None,
            )?,
        )?;
        assert_eq!(&*block.data, &data[..]);
        Ok(())
    }

    #[test]
    #[cfg(zstd_any)]
    fn block_roundtrip_encrypted_zstd_large() -> crate::Result<()> {
        let enc = test_provider();
        let data = vec![0xEE_u8; 32 * 1024]; // 32 KiB
        let mut writer = vec![];

        Block::write_into(
            &mut writer,
            &data,
            crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Zstd(3),
                Some(&enc),
                #[cfg(zstd_any)]
                None,
            )?,
        )?;

        let mut reader = &writer[..];
        let block = Block::from_reader(
            &mut reader,
            crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Zstd(3),
                Some(&enc),
                #[cfg(zstd_any)]
                None,
            )?,
        )?;
        assert_eq!(&*block.data, &data[..]);
        Ok(())
    }
}

#[cfg(feature = "zstd")]
mod zstd_dict {
    use super::*;
    use crate::compression::ZstdDictionary;
    use test_log::test;

    fn test_dict() -> ZstdDictionary {
        let mut samples = Vec::new();
        for i in 0u32..500 {
            samples.extend_from_slice(format!("key-{i:05}val-{i:05}").as_bytes());
        }
        ZstdDictionary::new(&samples)
    }

    fn test_compression(dict: &ZstdDictionary) -> CompressionType {
        CompressionType::ZstdDict {
            level: 3,
            dict_id: dict.id(),
        }
    }

    #[test]
    fn block_roundtrip_zstd_dict_reader() -> crate::Result<()> {
        let dict = test_dict();
        let compression = test_compression(&dict);
        let data = b"abcdefabcdefabcdef";
        let mut writer = vec![];

        Block::write_into(
            &mut writer,
            data,
            crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                compression,
                None,
                #[cfg(zstd_any)]
                Some(&dict),
            )?,
        )?;

        let mut reader = &writer[..];
        let block = Block::from_reader(
            &mut reader,
            crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                compression,
                None,
                #[cfg(zstd_any)]
                Some(&dict),
            )?,
        )?;
        assert_eq!(data, &*block.data);
        Ok(())
    }

    #[test]
    fn block_roundtrip_zstd_dict_file() -> crate::Result<()> {
        use std::io::Write;

        let dict = test_dict();
        let compression = test_compression(&dict);
        let data = b"abcdefabcdefabcdef";
        let mut buf = vec![];
        let header = Block::write_into(
            &mut buf,
            data,
            crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                compression,
                None,
                #[cfg(zstd_any)]
                Some(&dict),
            )?,
        )?;

        let dir = tempfile::tempdir()?;
        let path = dir.path().join("block");
        let mut file = std::fs::File::create(&path)?;
        file.write_all(&buf)?;
        file.sync_all()?;
        drop(file);

        let file = std::fs::File::open(&path)?;
        let handle = crate::table::BlockHandle::new(BlockOffset(0), header.on_disk_size());
        let block = Block::from_file(
            &file,
            handle,
            crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                compression,
                None,
                #[cfg(zstd_any)]
                Some(&dict),
            )?,
        )?;
        assert_eq!(data, &*block.data);
        Ok(())
    }

    #[test]
    fn block_roundtrip_zstd_dict_large_data() -> crate::Result<()> {
        let dict = test_dict();
        let compression = test_compression(&dict);
        let data = vec![0xAB_u8; 64 * 1024]; // 64 KiB
        let mut writer = vec![];

        Block::write_into(
            &mut writer,
            &data,
            crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                compression,
                None,
                #[cfg(zstd_any)]
                Some(&dict),
            )?,
        )?;

        assert!(
            writer.len() < data.len(),
            "dict compression should reduce size"
        );

        let mut reader = &writer[..];
        let block = Block::from_reader(
            &mut reader,
            crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                compression,
                None,
                #[cfg(zstd_any)]
                Some(&dict),
            )?,
        )?;
        assert_eq!(&*block.data, &data[..]);
        Ok(())
    }

    #[test]
    fn block_zstd_dict_wrong_dict_returns_error() {
        // Companion test to
        // `block_transform_from_parts_zstd_dict_missing_returns_error`
        // (below): both assert the BlockTransform::from_parts
        // check that used to live inside Block::write_into /
        // from_reader for the ZstdDict codec. The dict-missing
        // case exercises the `None` half of the dict argument;
        // this one exercises the cross-check between the
        // supplied dictionary id and the
        // ZstdDict { dict_id } discriminator. Both assert
        // directly on the transform-construction result; no
        // Block I/O call is needed to exercise the mismatch
        // path.
        let dict = test_dict();
        let compression = test_compression(&dict);
        let wrong_dict = ZstdDictionary::new(b"completely different dictionary bytes");

        let result =
            crate::table::block::BlockTransform::from_parts(compression, None, Some(&wrong_dict));
        assert!(
            matches!(
                result,
                Err(crate::Error::ZstdDictMismatch { got: Some(_), .. })
            ),
            "expected ZstdDictMismatch with got=Some",
        );
    }

    #[test]
    fn block_transform_from_parts_zstd_dict_missing_returns_error() {
        // The runtime dict-presence check that used to live inside
        // Block::write_into / from_reader for the ZstdDict codec
        // is now centralised in BlockTransform::from_parts. The
        // error therefore surfaces at transform-construction time
        // instead of at the Block I/O call; this test verifies
        // that earlier surface — it no longer exercises
        // Block::write_into / from_reader at all, hence the test
        // name describes from_parts rather than block_write_*.
        // (See block_zstd_dict_wrong_dict_returns_error above for
        // the matching "wrong dict id" path.)
        let dict = test_dict();
        let compression = test_compression(&dict);

        // Try to construct the read-side transform without
        // providing the dict the codec needs.
        let result = crate::table::block::BlockTransform::from_parts(compression, None, None);
        // BlockTransform holds `&dyn EncryptionProvider` which
        // doesn't impl Debug, so we can't print the whole result;
        // surface just the Err side (which IS Debug) on mismatch.
        // Match on `&result` + `.as_ref().err()` so the variant
        // check and the formatter borrow the same value — no
        // need to reason about whether the matches! patterns
        // bind by value.
        assert!(
            matches!(
                &result,
                Err(crate::Error::ZstdDictMismatch { got: None, .. })
            ),
            "expected ZstdDictMismatch, got: {:?}",
            result.as_ref().err(),
        );
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn block_roundtrip_zstd_dict_encrypted_reader() -> crate::Result<()> {
        let enc = crate::Aes256GcmProvider::new(&[0x42; 32]);
        let dict = test_dict();
        let compression = test_compression(&dict);
        let data = b"encrypted-dict-compressed-data-for-test";
        let mut writer = vec![];

        Block::write_into(
            &mut writer,
            data,
            crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                compression,
                Some(&enc),
                #[cfg(zstd_any)]
                Some(&dict),
            )?,
        )?;

        let mut reader = &writer[..];
        let block = Block::from_reader(
            &mut reader,
            crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                compression,
                Some(&enc),
                #[cfg(zstd_any)]
                Some(&dict),
            )?,
        )?;
        assert_eq!(data, &*block.data);
        Ok(())
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn block_roundtrip_zstd_dict_encrypted_file() -> crate::Result<()> {
        use std::io::Write;

        let enc = crate::Aes256GcmProvider::new(&[0x42; 32]);
        let dict = test_dict();
        let compression = test_compression(&dict);
        let data = vec![0xCC_u8; 16 * 1024]; // 16 KiB
        let mut buf = vec![];
        let header = Block::write_into(
            &mut buf,
            &data,
            crate::table::block::BlockIdentity::for_test(0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                compression,
                Some(&enc),
                #[cfg(zstd_any)]
                Some(&dict),
            )?,
        )?;

        let dir = tempfile::tempdir()?;
        let path = dir.path().join("block");
        let mut file = std::fs::File::create(&path)?;
        file.write_all(&buf)?;
        file.sync_all()?;
        drop(file);

        let file = std::fs::File::open(&path)?;
        let handle = crate::table::BlockHandle::new(BlockOffset(0), header.on_disk_size());
        let block = Block::from_file(
            &file,
            handle,
            crate::table::block::BlockIdentity::for_test(0, crate::table::block::BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                compression,
                Some(&enc),
                #[cfg(zstd_any)]
                Some(&dict),
            )?,
        )?;
        assert_eq!(&*block.data, &data[..]);
        Ok(())
    }
}

/// Page ECC integration tests — write a block with the
/// `BlockTransform::*Ecc` variant, verify the on-disk layout
/// round-trips through `Block::from_reader`, and verify that
/// Reed-Solomon recovery kicks in when the payload bytes are
/// corrupted between write and read.
#[cfg(feature = "page_ecc")]
mod page_ecc {
    use super::*;
    use test_log::test;

    const PAYLOAD: &[u8] = b"the quick brown fox jumps over the lazy dog \
                             0123456789 the quick brown fox jumps over \
                             the lazy dog 0123456789";

    #[test]
    fn block_roundtrip_plain_ecc_clean_read() -> crate::Result<()> {
        let mut writer = vec![];
        let header = Block::write_into(
            &mut writer,
            PAYLOAD,
            BlockIdentity::for_test(0, BlockType::Data),
            &BlockTransform::PlainEcc(EccParams::RS_4_2),
        )?;

        assert!(
            header.block_flags & crate::table::block::header::block_flags::ECC_PARITY != 0,
            "PlainEcc writer must set the ECC_PARITY flag",
        );
        assert_eq!(
            writer.len(),
            header.on_disk_size() as usize,
            "on-disk size must equal header + payload + derived parity length",
        );

        let mut reader = &writer[..];
        let block = Block::from_reader(
            &mut reader,
            BlockIdentity::for_test(0, BlockType::Data),
            &BlockTransform::PlainEcc(EccParams::RS_4_2),
        )?;
        assert_eq!(&*block.data, PAYLOAD);
        Ok(())
    }

    #[test]
    fn block_roundtrip_plain_ecc_recovers_from_single_byte_flip() -> crate::Result<()> {
        let mut writer = vec![];
        let header = Block::write_into(
            &mut writer,
            PAYLOAD,
            BlockIdentity::for_test(0, BlockType::Data),
            &BlockTransform::PlainEcc(EccParams::RS_4_2),
        )?;

        // Flip a single byte inside the payload region (after
        // the header, before the parity trailer) so the on-disk
        // bytes' XXH3 no longer matches header.checksum but the
        // recoverable shape (1 of 6 shards corrupted) holds.
        let header_len = Header::MIN_LEN;
        let flip_at = header_len + (header.data_length as usize) / 2;
        writer[flip_at] ^= 0xFF;

        let mut reader = &writer[..];
        let block = Block::from_reader(
            &mut reader,
            BlockIdentity::for_test(0, BlockType::Data),
            &BlockTransform::PlainEcc(EccParams::RS_4_2),
        )?;
        // ECC recovery reconstructs the original payload despite
        // the in-flight bit-flip.
        assert_eq!(
            &*block.data, PAYLOAD,
            "Reed-Solomon recovery must reconstruct the original \
             payload from a single-byte data-shard flip",
        );
        Ok(())
    }

    #[test]
    fn block_roundtrip_secded_clean_read() -> crate::Result<()> {
        let mut writer = vec![];
        let header = Block::write_into(
            &mut writer,
            PAYLOAD,
            BlockIdentity::for_test(0, BlockType::Data),
            &BlockTransform::PlainEcc(EccParams::SECDED),
        )?;

        assert!(
            header.block_flags & crate::table::block::header::block_flags::ECC_PARITY != 0,
            "SECDED writer must set the ECC_PARITY flag",
        );
        // One parity byte per 8-byte word: header + payload + ceil(N/8).
        // `on_disk_size()` hardcodes the RS(4,2) default; the scheme-aware
        // `on_disk_size_with` sizes the SECDED trailer.
        assert_eq!(
            writer.len(),
            header.on_disk_size_with(Some(EccParams::SECDED)) as usize,
            "on-disk size must equal header + payload + SECDED parity length",
        );

        let mut reader = &writer[..];
        let block = Block::from_reader(
            &mut reader,
            BlockIdentity::for_test(0, BlockType::Data),
            &BlockTransform::PlainEcc(EccParams::SECDED),
        )?;
        assert_eq!(&*block.data, PAYLOAD);
        Ok(())
    }

    #[test]
    fn block_roundtrip_secded_recovers_from_single_bit_flip() -> crate::Result<()> {
        let mut writer = vec![];
        let header = Block::write_into(
            &mut writer,
            PAYLOAD,
            BlockIdentity::for_test(0, BlockType::Data),
            &BlockTransform::PlainEcc(EccParams::SECDED),
        )?;

        // Flip a SINGLE bit inside the payload region: SECDED heals one bit
        // per 8-byte word, so a single-bit flip is recoverable (a whole-byte
        // flip would be 8 bit-errors in one word — uncorrectable).
        let flip_at = Header::MIN_LEN + (header.data_length as usize) / 2;
        writer[flip_at] ^= 0x01;

        let mut reader = &writer[..];
        let block = Block::from_reader(
            &mut reader,
            BlockIdentity::for_test(0, BlockType::Data),
            &BlockTransform::PlainEcc(EccParams::SECDED),
        )?;
        assert_eq!(
            &*block.data, PAYLOAD,
            "SECDED must heal a single-bit payload flip",
        );
        Ok(())
    }

    #[test]
    fn block_roundtrip_secded_unrecoverable_on_double_bit_flip() -> crate::Result<()> {
        let mut writer = vec![];
        let header = Block::write_into(
            &mut writer,
            PAYLOAD,
            BlockIdentity::for_test(0, BlockType::Data),
            &BlockTransform::PlainEcc(EccParams::SECDED),
        )?;

        // Two bit errors in the same byte (hence the same 8-byte word):
        // SECDED detects but cannot correct — the read must fail rather
        // than return miscorrected data.
        let flip_at = Header::MIN_LEN + (header.data_length as usize) / 2;
        writer[flip_at] ^= 0x03;

        let mut reader = &writer[..];
        let result = Block::from_reader(
            &mut reader,
            BlockIdentity::for_test(0, BlockType::Data),
            &BlockTransform::PlainEcc(EccParams::SECDED),
        );
        assert!(
            matches!(&result, Err(crate::Error::PageEccUnrecoverable { .. })),
            "a double-bit error in one word must be detected as unrecoverable \
             (got ok={})",
            result.is_ok(),
        );
        Ok(())
    }

    #[test]
    fn block_from_file_plain_ecc_recovers_from_single_byte_flip() -> crate::Result<()> {
        let tmp = super::write_block_to_tempfile(
            PAYLOAD,
            BlockIdentity::for_test(0, BlockType::Data),
            &BlockTransform::PlainEcc(EccParams::RS_4_2),
        )?;
        let path = tmp.dir.path().join("block");

        // Flip one byte inside the payload region (after the
        // header, before the parity trailer).
        let mut bytes = std::fs::read(&path)?;
        let payload_start = Header::MIN_LEN;
        bytes[payload_start + 3] ^= 0x80;
        std::fs::write(&path, &bytes)?;

        // Re-open and read via from_file. ECC recovery should
        // reconstruct the original payload.
        let file = std::fs::File::open(&path)?;
        let block = Block::from_file(
            &file,
            tmp.handle,
            BlockIdentity::for_test(0, BlockType::Data),
            &BlockTransform::PlainEcc(EccParams::RS_4_2),
        )?;
        assert_eq!(&*block.data, PAYLOAD);
        Ok(())
    }

    /// `from_file_with_status` reports `EccStatus::Corrected` when a read
    /// repaired the block via ECC, and `EccStatus::Ok` for a clean read.
    /// This is the auto-heal (#411) signal: a healed read returns the
    /// correct bytes AND flags that the on-disk copy still holds the fault.
    #[test]
    fn from_file_with_status_reports_corrected_after_ecc_repair() -> crate::Result<()> {
        let transform = BlockTransform::PlainEcc(EccParams::RS_4_2);
        let tmp = super::write_block_to_tempfile(
            PAYLOAD,
            BlockIdentity::for_test(0, BlockType::Data),
            &transform,
        )?;
        let path = tmp.dir.path().join("block");

        // Clean read first: no repair → EccStatus::Ok.
        {
            let file = std::fs::File::open(&path)?;
            let (block, status) = Block::from_file_with_status(
                &file,
                tmp.handle,
                BlockIdentity::for_test(0, BlockType::Data),
                &transform,
            )?;
            assert_eq!(&*block.data, PAYLOAD);
            assert_eq!(status, EccStatus::Ok, "clean read must not flag a repair");
        }

        // Flip one payload byte so the read must repair via RS parity.
        let mut bytes = std::fs::read(&path)?;
        bytes[Header::MIN_LEN + 3] ^= 0x80;
        std::fs::write(&path, &bytes)?;

        let file = std::fs::File::open(&path)?;
        let (block, status, recovery) = Block::from_file_with_recovery(
            &file,
            tmp.handle,
            BlockIdentity::for_test(0, BlockType::Data),
            &transform,
        )?;
        assert_eq!(&*block.data, PAYLOAD, "repaired bytes must equal original");
        assert_eq!(
            status,
            EccStatus::Corrected,
            "a repaired read reports Corrected"
        );
        assert_eq!(
            recovery,
            Some(EccRecoveryKind::Shard),
            "an RS repair is attributed to the shard mechanism",
        );
        Ok(())
    }

    /// A single-bit flip in a SEC-DED-protected block is healed by the
    /// SEC-DED fast path and reported as `Corrected(Secded)`, distinct from
    /// the RS shard path. This is the kind-attribution the unified recovery
    /// metric relies on (the per-kind counter is bumped from this status at
    /// the `load_block` / scrub / partial-decode call sites).
    #[test]
    fn from_file_with_status_reports_secded_kind_after_single_bit_heal() -> crate::Result<()> {
        let transform = BlockTransform::PlainEcc(EccParams::SECDED);
        let tmp = super::write_block_to_tempfile(
            PAYLOAD,
            BlockIdentity::for_test(0, BlockType::Data),
            &transform,
        )?;
        let path = tmp.dir.path().join("block");

        // Flip a SINGLE bit so the SEC-DED per-word code heals it.
        let mut bytes = std::fs::read(&path)?;
        bytes[Header::MIN_LEN + 3] ^= 0x01;
        std::fs::write(&path, &bytes)?;

        let file = std::fs::File::open(&path)?;
        let (block, status, recovery) = Block::from_file_with_recovery(
            &file,
            tmp.handle,
            BlockIdentity::for_test(0, BlockType::Data),
            &transform,
        )?;
        assert_eq!(
            &*block.data, PAYLOAD,
            "SEC-DED must heal the single-bit flip"
        );
        assert_eq!(
            status,
            EccStatus::Corrected,
            "a repaired read reports Corrected"
        );
        assert_eq!(
            recovery,
            Some(EccRecoveryKind::Secded),
            "a SEC-DED single-bit heal is attributed to the SEC-DED mechanism",
        );
        Ok(())
    }

    /// Three-state read contract: a block written WITH a parity trailer,
    /// read back by a reader that does NOT recognize the scheme (a `Plain`
    /// transform — as happens when the per-SST descriptor decodes to an
    /// unsupported scheme), must still return the payload (framed by
    /// `data_length`, checksum-verified) and report
    /// `EccStatus::Unrecognized` — a soft warning, not a read error. Read
    /// with the matching scheme, it reports `EccStatus::Ok`.
    #[test]
    fn from_file_with_status_soft_warns_on_unrecognized_trailer() -> crate::Result<()> {
        // A non-default scheme on purpose: the read path is descriptor-
        // driven, never an implicit RS(4,2).
        let scheme = EccParams::try_new(8, 2).expect("valid shards");
        let tmp = super::write_block_to_tempfile(
            PAYLOAD,
            BlockIdentity::for_test(0, BlockType::Data),
            &BlockTransform::PlainEcc(scheme),
        )?;

        // Reader recognizes the scheme → clean read.
        let (block, status) = Block::from_file_with_status(
            &tmp.file,
            tmp.handle,
            BlockIdentity::for_test(0, BlockType::Data),
            &BlockTransform::PlainEcc(scheme),
        )?;
        assert_eq!(&*block.data, PAYLOAD);
        assert_eq!(status, EccStatus::Ok);

        // Reader does NOT recognize the trailer (Plain transform): the
        // opaque trailer is excluded from the payload, the checksum still
        // verifies, and the status is a warning rather than an error.
        let (block, status) = Block::from_file_with_status(
            &tmp.file,
            tmp.handle,
            BlockIdentity::for_test(0, BlockType::Data),
            &BlockTransform::PLAIN,
        )?;
        assert_eq!(
            &*block.data, PAYLOAD,
            "payload reads despite unknown trailer"
        );
        assert_eq!(status, EccStatus::Unrecognized);
        Ok(())
    }

    /// Regression: a RECOGNIZED ECC layout must require an exact trailer
    /// length — including the empty-payload case where the parity length is
    /// zero. Extra trailing bytes on such a block are corruption and must
    /// FAIL, not be softened to `EccStatus::Unrecognized` (which keying the
    /// decision off `ecc_length == 0` instead of "is the layout recognized"
    /// would wrongly do).
    #[test]
    fn from_file_recognized_empty_block_rejects_extra_trailer() -> crate::Result<()> {
        // Empty payload under a (non-default) recognized scheme →
        // data_length 0, parity length 0, so the on-disk block is just the
        // header (no trailer).
        let scheme = EccParams::try_new(8, 2).expect("valid shards");
        let tmp = super::write_block_to_tempfile(
            b"",
            BlockIdentity::for_test(0, BlockType::Data),
            &BlockTransform::PlainEcc(scheme),
        )?;
        let path = tmp.dir.path().join("block");
        let base = tmp.handle.size();

        // Append junk so the handle covers a 4-byte trailer that the
        // recognized (zero-parity) layout does not expect.
        let mut bytes = std::fs::read(&path)?;
        bytes.extend_from_slice(&[0xAB, 0xCD, 0xEF, 0x01]);
        std::fs::write(&path, &bytes)?;

        let file = std::fs::File::open(&path)?;
        let handle = crate::table::BlockHandle::new(crate::table::BlockOffset(0), base + 4);
        // `.err()` drops the `Ok((Block, _))` payload (Block is not Debug)
        // so the assert can format the error variant.
        let err = Block::from_file_with_status(
            &file,
            handle,
            BlockIdentity::for_test(0, BlockType::Data),
            &BlockTransform::PlainEcc(scheme),
        )
        .err();
        assert!(
            matches!(err, Some(crate::Error::InvalidHeader("Block"))),
            "recognized zero-parity layout + extra trailer must fail, got {err:?}",
        );
        Ok(())
    }

    /// Same recovery story as `PlainEcc` but with lz4 compression
    /// stacked on top: parity is computed over the
    /// post-compression payload, recovery happens BEFORE
    /// decompression. Catches a regression where the read path
    /// would try to decompress corrupt bytes (lz4 would fail
    /// before recovery had a chance to fire).
    #[cfg(feature = "lz4")]
    #[test]
    fn block_roundtrip_compressed_ecc_recovers_from_byte_flip() -> crate::Result<()> {
        let mut writer = vec![];
        let header = Block::write_into(
            &mut writer,
            PAYLOAD,
            BlockIdentity::for_test(0, BlockType::Data),
            &BlockTransform::CompressedEcc(
                CompressionContext::new(CompressionType::Lz4)?,
                EccParams::RS_4_2,
            ),
        )?;
        assert!(header.block_flags & crate::table::block::header::block_flags::ECC_PARITY != 0);

        // Flip a byte in the compressed-payload region.
        let header_len = Header::MIN_LEN;
        let flip_at = header_len + (header.data_length as usize) / 2;
        writer[flip_at] ^= 0x55;

        let mut reader = &writer[..];
        let block = Block::from_reader(
            &mut reader,
            BlockIdentity::for_test(0, BlockType::Data),
            &BlockTransform::CompressedEcc(
                CompressionContext::new(CompressionType::Lz4)?,
                EccParams::RS_4_2,
            ),
        )?;
        assert_eq!(
            &*block.data, PAYLOAD,
            "ECC must recover the compressed bytes BEFORE lz4 \
             decompression, otherwise lz4 would fail on corrupt input",
        );
        Ok(())
    }

    /// ECC-protected encrypted roundtrip with a single-byte
    /// ciphertext flip. Parity is computed over the ciphertext,
    /// so recovery must produce byte-exact reconstruction —
    /// AEAD authentication fails on even a one-bit mismatch.
    /// This catches a regression where ECC recovery rebuilds an
    /// arithmetically valid Reed-Solomon shard that doesn't
    /// bit-identically reproduce the original ciphertext.
    #[cfg(feature = "encryption")]
    #[test]
    fn block_roundtrip_encrypted_ecc_recovers_from_byte_flip() -> crate::Result<()> {
        let enc = crate::encryption::Aes256GcmProvider::new(&[0x42; 32]);
        let mut writer = vec![];
        let header = Block::write_into(
            &mut writer,
            PAYLOAD,
            BlockIdentity::for_test(0, BlockType::Data),
            &BlockTransform::EncryptedEcc(&enc, EccParams::RS_4_2),
        )?;
        assert!(header.block_flags & crate::table::block::header::block_flags::ECC_PARITY != 0);

        // Flip one byte in the ciphertext region.
        let header_len = Header::MIN_LEN;
        let flip_at = header_len + (header.data_length as usize) / 2;
        writer[flip_at] ^= 0x21;

        let mut reader = &writer[..];
        let block = Block::from_reader(
            &mut reader,
            BlockIdentity::for_test(0, BlockType::Data),
            &BlockTransform::EncryptedEcc(&enc, EccParams::RS_4_2),
        )?;
        assert_eq!(
            &*block.data, PAYLOAD,
            "ECC must reconstruct ciphertext byte-exactly so AEAD \
             authentication succeeds on the recovered bytes",
        );
        Ok(())
    }

    /// Asserts the unrecoverable path surfaces
    /// `Error::PageEccUnrecoverable`. Corrupts enough on-disk
    /// bytes to take out more than the RS(4, 2) scheme can
    /// recover (≥ 3 data shards damaged), so every C(6, 4)
    /// subset trial decode fails the xxh3 oracle and
    /// `try_recover` exhausts all 15 candidates.
    #[test]
    fn block_roundtrip_plain_ecc_unrecoverable_when_too_many_shards_corrupt() -> crate::Result<()> {
        let mut writer = vec![];
        let header = Block::write_into(
            &mut writer,
            PAYLOAD,
            BlockIdentity::for_test(0, BlockType::Data),
            &BlockTransform::PlainEcc(EccParams::RS_4_2),
        )?;

        // Shard size in bytes — same formula as crate::ecc::shard_bytes
        // (ceil(payload_len / 4) rounded up to even).
        let payload_len = header.data_length as usize;
        let shard_bytes = ((payload_len.div_ceil(4)) + 1) & !1;

        // Flip one byte in EACH of the first 3 data shards.
        // RS(4, 2) recovers up to 2 missing shards; with 3
        // corrupted data shards no subset of 4 intact shards
        // reconstructs the original.
        let payload_start = Header::MIN_LEN;
        for shard_idx in 0..3 {
            let pos = payload_start + shard_idx * shard_bytes;
            if pos < writer.len() {
                writer[pos] ^= 0xFF;
            }
        }

        let mut reader = &writer[..];
        let result = Block::from_reader(
            &mut reader,
            BlockIdentity::for_test(0, BlockType::Data),
            &BlockTransform::PlainEcc(EccParams::RS_4_2),
        );
        match result {
            Ok(_) => panic!(
                "3-shard corruption must exceed RS(4,2) recovery capacity, \
                 but from_reader returned Ok"
            ),
            Err(crate::Error::PageEccUnrecoverable { .. }) => {}
            Err(e) => panic!("expected PageEccUnrecoverable, got {e:?}"),
        }
        Ok(())
    }

    #[test]
    fn ecc_parity_bit_agrees_with_emitted_parity_length() -> crate::Result<()> {
        use crate::table::block::header::block_flags;

        // Empty payload: the Reed-Solomon encoder short-circuits to a
        // zero-length parity trailer even under PlainEcc, so the
        // presence-authoritative ECC_PARITY bit must stay CLEAR — and the
        // derived on-disk size carries no parity (header + 0 payload).
        let mut empty_buf = vec![];
        let empty = Block::write_into(
            &mut empty_buf,
            &[],
            BlockIdentity::for_test(0, BlockType::Data),
            &BlockTransform::PlainEcc(EccParams::RS_4_2),
        )?;
        assert_eq!(
            empty.block_flags & block_flags::ECC_PARITY,
            0,
            "ECC_PARITY must be clear when no parity trailer is emitted",
        );
        assert_eq!(
            empty_buf.len(),
            empty.on_disk_size() as usize,
            "on-disk size matches the derived (zero) parity length",
        );
        assert_eq!(
            empty.on_disk_size() as usize,
            Header::MIN_LEN,
            "empty payload emits no parity, so on-disk size is just the header",
        );

        // Non-empty payload: parity is emitted, so the bit is set and the
        // derived on-disk size includes the parity trailer.
        let mut full_buf = vec![];
        let full = Block::write_into(
            &mut full_buf,
            PAYLOAD,
            BlockIdentity::for_test(0, BlockType::Data),
            &BlockTransform::PlainEcc(EccParams::RS_4_2),
        )?;
        assert_ne!(
            full.block_flags & block_flags::ECC_PARITY,
            0,
            "ECC_PARITY must be set when a parity trailer is emitted",
        );
        assert_eq!(
            full_buf.len(),
            full.on_disk_size() as usize,
            "on-disk size matches header + payload + derived parity",
        );
        assert!(
            full.on_disk_size() as usize > Header::MIN_LEN + full.data_length as usize,
            "non-empty payload emits a parity trailer beyond header + payload",
        );
        Ok(())
    }
}

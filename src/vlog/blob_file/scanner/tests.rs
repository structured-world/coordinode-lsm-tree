use super::*;
use crate::{Slice, fs::StdFs, vlog::blob_file::writer::Writer as BlobFileWriter};
use tempfile::tempdir;
use test_log::test;

#[test]
fn blob_scanner() -> crate::Result<()> {
    let dir = tempdir()?;
    let blob_file_path = dir.path().join("0");

    let keys = [b"a", b"b", b"c", b"d", b"e"];

    {
        let mut writer = BlobFileWriter::new(&blob_file_path, 0, 0, &StdFs)?;

        for key in keys {
            writer.write(key, 0, &key.repeat(100))?;
        }

        writer.finish()?;
    }

    {
        let mut scanner = Scanner::new(&blob_file_path, &StdFs, 0)?;

        for key in keys {
            assert_eq!(
                (Slice::from(key), Slice::from(key.repeat(100))),
                scanner
                    .next()
                    .map(|result| result.map(|entry| { (entry.key, entry.value) }))
                    .unwrap()?,
            );
        }

        assert!(scanner.next().is_none());
    }

    Ok(())
}

/// `Scanner::resume` re-opens at a carried frame boundary and reads only the
/// suffix (the tight-space relocation loop's per-slice resume), and rejects an
/// offset outside the data section.
#[test]
fn blob_scanner_resume_reads_suffix_and_rejects_bad_offset() -> crate::Result<()> {
    let dir = tempdir()?;
    let blob_file_path = dir.path().join("0");

    let keys = [b"a", b"b", b"c", b"d", b"e"];
    {
        let mut writer = BlobFileWriter::new(&blob_file_path, 0, 0, &StdFs)?;
        for key in keys {
            writer.write(key, 0, &key.repeat(100))?;
        }
        writer.finish()?;
    }

    // Scan the first two frames and capture the frame boundary after "b".
    let resume_at = {
        let mut scanner = Scanner::new(&blob_file_path, &StdFs, 0)?;
        let _a = scanner.next().unwrap()?;
        let b = scanner.next().unwrap()?;
        b.frame_end
    };

    // Resuming at that boundary yields exactly the suffix c, d, e.
    {
        let mut scanner = Scanner::resume(&blob_file_path, &StdFs, 0, resume_at)?;
        for key in [b"c", b"d", b"e"] {
            assert_eq!(
                Slice::from(&key[..]),
                scanner.next().map(|r| r.map(|e| e.key)).unwrap()?,
            );
        }
        assert!(scanner.next().is_none());
    }

    // An offset past the data section is rejected, never silently mis-seeked.
    assert!(
        matches!(
            Scanner::resume(&blob_file_path, &StdFs, 0, u64::MAX),
            Err(crate::Error::InvalidHeader("BlobFile")),
        ),
        "resume offset past the data section must error",
    );
    Ok(())
}

/// Tamper seqno in first blob frame and verify the scanner's header
/// CRC catches the corruption.
#[test]
fn blob_scanner_corrupted_seqno_detected_by_header_crc() -> crate::Result<()> {
    use crate::vlog::blob_file::writer::BLOB_HEADER_MAGIC;

    let dir = tempdir()?;
    let blob_file_path = dir.path().join("0");

    {
        let mut writer = BlobFileWriter::new(&blob_file_path, 0, 0, &StdFs)?;
        writer.write(b"key", 42, &b"v".repeat(100))?;
        writer.finish()?;
    }

    // BlobFileWriter writes the first frame at file offset 0
    // (sfa has no inline section headers), so use deterministic offset.
    let mut raw = std::fs::read(&blob_file_path)?;
    let frame_start = 0usize;

    // Tamper seqno: header layout is [magic][checksum][seqno]...
    let seqno_offset = frame_start + BLOB_HEADER_MAGIC.len() + std::mem::size_of::<u128>();
    let seqno_len = std::mem::size_of::<u64>();
    raw[seqno_offset..seqno_offset + seqno_len].copy_from_slice(&99u64.to_le_bytes()[..seqno_len]);
    std::fs::write(&blob_file_path, &raw)?;

    let mut scanner = Scanner::new(&blob_file_path, &StdFs, 0)?;
    let result = scanner.next().unwrap();
    assert!(
        matches!(result, Err(crate::Error::HeaderCrcMismatch { .. })),
        "expected HeaderCrcMismatch for corrupted seqno, got: {result:?}",
    );

    Ok(())
}

/// Tamper value payload in blob frame and verify scanner's data
/// checksum catches the corruption.
#[test]
fn blob_scanner_corrupted_value_detected_by_data_checksum() -> crate::Result<()> {
    use crate::vlog::blob_file::writer::BLOB_HEADER_LEN;

    let dir = tempdir()?;
    let blob_file_path = dir.path().join("0");

    {
        let mut writer = BlobFileWriter::new(&blob_file_path, 0, 0, &StdFs)?;
        writer.write(b"key", 0, &b"v".repeat(100))?;
        writer.finish()?;
    }

    // BlobFileWriter writes the first frame at file offset 0
    // (sfa has no inline section headers), so use deterministic offset.
    let mut raw = std::fs::read(&blob_file_path)?;
    let frame_start = 0usize;

    // Tamper value payload: frame_start + header + key
    let key = b"key";
    let value_offset = frame_start + BLOB_HEADER_LEN + key.len();
    raw[value_offset] ^= 0xFF;
    std::fs::write(&blob_file_path, &raw)?;

    let mut scanner = Scanner::new(&blob_file_path, &StdFs, 0)?;
    let result = scanner.next().unwrap();
    assert!(
        matches!(result, Err(crate::Error::ChecksumMismatch { .. })),
        "expected ChecksumMismatch for corrupted value, got: {result:?}",
    );

    Ok(())
}

/// A frame under the retired pre-V5 `b"BLOB"` magic (no header CRC) is NOT
/// readable: the engine supports exactly one on-disk format, so the scanner
/// reports it as corruption (and, finding no other frame, terminates).
#[test]
fn blob_scanner_rejects_retired_blob_magic_frame() -> crate::Result<()> {
    use crate::io::{LittleEndian, WriteBytesExt};
    use std::io::Write;

    let dir = tempdir()?;
    let blob_file_path = dir.path().join("0");

    let key = b"abc";
    let value = b"hello_v3";

    // Retired-format data checksum: xxh3_128(key + value), no header_crc.
    let checksum = {
        let mut hasher = xxhash_rust::xxh3::Xxh3::default();
        hasher.update(key);
        hasher.update(value);
        hasher.digest128()
    };

    // Manually write a retired-format blob file using sfa framing.
    {
        let file = std::fs::File::create(&blob_file_path)?;
        let mut sfa_writer = crate::sfa::Writer::from_writer(file);
        sfa_writer.start("data")?;

        // Retired frame: BLOB magic, no header_crc.
        sfa_writer.write_all(b"BLOB")?;
        sfa_writer.write_u128::<LittleEndian>(checksum)?;
        sfa_writer.write_u64::<LittleEndian>(42)?; // seqno
        #[expect(
            clippy::cast_possible_truncation,
            reason = "test key length fits in u16"
        )]
        sfa_writer.write_u16::<LittleEndian>(key.len() as u16)?;
        #[expect(
            clippy::cast_possible_truncation,
            reason = "test value length fits in u32"
        )]
        sfa_writer.write_u32::<LittleEndian>(value.len() as u32)?; // real_val_len
        #[expect(
            clippy::cast_possible_truncation,
            reason = "test value length fits in u32"
        )]
        sfa_writer.write_u32::<LittleEndian>(value.len() as u32)?; // on_disk_val_len
        sfa_writer.write_all(key)?;
        sfa_writer.write_all(value)?;

        // Write metadata section
        sfa_writer.start("meta")?;
        let metadata = crate::vlog::blob_file::meta::Metadata {
            id: 0,
            version: 4,
            created_at: 0,
            item_count: 1,
            total_compressed_bytes: value.len() as u64,
            total_uncompressed_bytes: value.len() as u64,
            key_range: crate::KeyRange::new((key[..].into(), key[..].into())),
            compression: crate::CompressionType::None,
        };
        metadata.encode_into(&mut sfa_writer)?;
        let inner = sfa_writer.into_inner()?;
        inner.sync_all()?;
    }

    let mut scanner = Scanner::new(&blob_file_path, &StdFs, 0)?;
    let result = scanner.next().unwrap();
    assert!(
        matches!(result, Err(crate::Error::InvalidHeader("Blob"))),
        "a retired-format frame must be rejected, got: {result:?}",
    );
    assert!(
        scanner.next().is_none(),
        "no readable frame exists past the rejected one"
    );

    Ok(())
}

/// A frame whose declared `on_disk_val_len` runs past the data section must
/// be rejected by the checked frame-fit bound, not read past the section.
/// The header CRC is forged CONSISTENT with the oversized length so the
/// declaration survives CRC validation and reaches the fit check — the shape
/// of a truncated file, whose remaining declared bytes simply do not exist.
#[test]
fn blob_scanner_rejects_oversized_on_disk_len() -> crate::Result<()> {
    use crate::io::{LittleEndian, WriteBytesExt};
    use std::io::Write;

    let dir = tempdir()?;
    let blob_file_path = dir.path().join("0");

    let key = b"abc";
    let value = b"hi";
    {
        let file = std::fs::File::create(&blob_file_path)?;
        let mut sfa_writer = crate::sfa::Writer::from_writer(file);
        sfa_writer.start("data")?;
        sfa_writer.write_all(crate::vlog::blob_file::writer::BLOB_HEADER_MAGIC)?;
        sfa_writer.write_u128::<LittleEndian>(0)?; // checksum (unreached)
        sfa_writer.write_u64::<LittleEndian>(1)?; // seqno
        #[expect(clippy::cast_possible_truncation, reason = "test key fits u16")]
        sfa_writer.write_u16::<LittleEndian>(key.len() as u16)?;
        sfa_writer.write_u32::<LittleEndian>(2)?; // real_val_len
        // on_disk_val_len far exceeds the data section → frame-fit reject.
        sfa_writer.write_u32::<LittleEndian>(u32::MAX)?;
        #[expect(clippy::cast_possible_truncation, reason = "test key fits u16")]
        let crc =
            crate::vlog::blob_file::writer::compute_header_crc(1, key.len() as u16, 2, u32::MAX);
        sfa_writer.write_u32::<LittleEndian>(crc)?;
        sfa_writer.write_all(key)?;
        sfa_writer.write_all(value)?;

        sfa_writer.start("meta")?;
        let metadata = crate::vlog::blob_file::meta::Metadata {
            id: 0,
            version: 4,
            created_at: 0,
            item_count: 1,
            total_compressed_bytes: 2,
            total_uncompressed_bytes: 2,
            key_range: crate::KeyRange::new((key[..].into(), key[..].into())),
            compression: crate::CompressionType::None,
        };
        metadata.encode_into(&mut sfa_writer)?;
        sfa_writer.into_inner()?.sync_all()?;
    }

    let mut scanner = Scanner::new(&blob_file_path, &StdFs, 0)?;
    let result = scanner.next().unwrap();
    assert!(
        matches!(result, Err(crate::Error::InvalidHeader("Blob"))),
        "an oversized on_disk_val_len must be rejected, got: {result:?}",
    );
    assert!(
        scanner.next().is_none(),
        "a CRC-vouched oversized declaration means truncation: the scan terminates"
    );
    Ok(())
}

/// Writes two frames with the real writer and returns the path. Frame 1
/// starts at data-section offset 0 (sfa has no inline section headers).
fn write_two_frames(dir: &std::path::Path) -> crate::Result<std::path::PathBuf> {
    let blob_file_path = dir.join("0");
    let mut writer = BlobFileWriter::new(&blob_file_path, 0, 0, &StdFs)?;
    writer.write(b"aaa", 7, b"first_value")?;
    writer.write(b"bbb", 7, b"second_value")?;
    writer.finish()?;
    Ok(blob_file_path)
}

/// Rot in a frame's LENGTH field (caught by the header CRC) must not cost
/// the readable tail: the consumed lengths are untrusted, so the scanner
/// resynchronizes at the next frame magic instead of terminating —
/// otherwise blob salvage drops every intact later record over one rotted
/// header field.
#[test]
fn blob_scanner_header_crc_rot_resyncs_to_next_frame() -> crate::Result<()> {
    let dir = tempdir()?;
    let blob_file_path = write_two_frames(dir.path())?;

    // Inflate frame 1's on_disk_val_len field (frame offset 34..38: magic 4 +
    // checksum 16 + seqno 8 + key_len 2 + real_val_len 4). The header CRC no
    // longer matches, so the lengths are untrusted.
    {
        let mut bytes = std::fs::read(&blob_file_path)?;
        bytes[34..38].copy_from_slice(&u32::MAX.to_le_bytes());
        std::fs::write(&blob_file_path, bytes)?;
    }

    let mut scanner = Scanner::new(&blob_file_path, &StdFs, 0)?;
    let first = scanner.next().unwrap();
    assert!(
        matches!(first, Err(crate::Error::HeaderCrcMismatch { .. })),
        "the rotted length field fails the header CRC: {first:?}",
    );
    let second = scanner
        .next()
        .expect("the intact second frame must survive the rotted first")?;
    assert_eq!(second.key, Slice::from(&b"bbb"[..]));
    assert_eq!(second.value, Slice::from(&b"second_value"[..]));
    assert!(scanner.next().is_none());

    Ok(())
}

/// Rot in a frame's MAGIC bytes must not cost the readable tail either: the
/// frame's lengths are unreachable (nothing vouches for the header), so the
/// scanner resynchronizes at the next frame magic.
#[test]
fn blob_scanner_magic_rot_resyncs_to_next_frame() -> crate::Result<()> {
    let dir = tempdir()?;
    let blob_file_path = write_two_frames(dir.path())?;

    // Rot frame 1's magic (frame offset 0..4).
    {
        let mut bytes = std::fs::read(&blob_file_path)?;
        bytes[0] ^= 0xFF;
        std::fs::write(&blob_file_path, bytes)?;
    }

    let mut scanner = Scanner::new(&blob_file_path, &StdFs, 0)?;
    let first = scanner.next().unwrap();
    assert!(
        matches!(first, Err(crate::Error::InvalidHeader("Blob"))),
        "the rotted magic is rejected: {first:?}",
    );
    let second = scanner
        .next()
        .expect("the intact second frame must survive the rotted first")?;
    assert_eq!(second.key, Slice::from(&b"bbb"[..]));
    assert_eq!(second.value, Slice::from(&b"second_value"[..]));
    assert!(scanner.next().is_none());

    Ok(())
}

/// Scanner rejects frames with invalid magic (neither V3 nor V4).
#[test]
fn blob_scanner_rejects_invalid_magic() -> crate::Result<()> {
    let dir = tempdir()?;
    let blob_file_path = dir.path().join("0");

    {
        let mut writer = BlobFileWriter::new(&blob_file_path, 0, 0, &StdFs)?;
        writer.write(b"key", 0, b"value")?;
        writer.finish()?;
    }

    // Corrupt magic bytes at offset 0 (start of first frame).
    let mut raw = std::fs::read(&blob_file_path)?;
    // First frame starts at offset 0 because sfa has no inline headers.
    raw[0..4].copy_from_slice(b"XXXX");
    std::fs::write(&blob_file_path, &raw)?;

    let mut scanner = Scanner::new(&blob_file_path, &StdFs, 0)?;
    let result = scanner.next().unwrap();
    assert!(
        matches!(result, Err(crate::Error::InvalidHeader("Blob"))),
        "expected InvalidHeader for bad magic, got: {result:?}",
    );

    // Scanner must be terminated — subsequent next() returns None,
    // not garbage parsed from an invalid stream position.
    assert!(scanner.next().is_none());

    Ok(())
}

/// Corruption that produces META bytes at a frame boundary must
/// surface as an error, not silently terminate iteration.
///
/// Regression test for #50: the old scanner checked for `b"META"`
/// magic to detect the metadata section boundary, which meant
/// corruption matching those bytes caused silent data loss.
#[test]
fn blob_scanner_meta_corruption_is_not_silent_eof() -> crate::Result<()> {
    use crate::vlog::blob_file::writer::BLOB_HEADER_LEN;

    let dir = tempdir()?;
    let blob_file_path = dir.path().join("0");

    {
        let mut writer = BlobFileWriter::new(&blob_file_path, 0, 0, &StdFs)?;
        writer.write(b"a", 0, &b"v".repeat(50))?;
        writer.write(b"b", 1, &b"w".repeat(50))?;
        writer.finish()?;
    }

    // Get data section start from SFA TOC so the offset calculation
    // stays correct even if SFA ever places data at non-zero offset.
    let data_start = {
        let sfa_reader = crate::sfa::Reader::new(&blob_file_path)?;
        let section = sfa_reader.toc().section(b"data").unwrap();
        #[expect(
            clippy::cast_possible_truncation,
            reason = "test blob file is tiny, pos fits in usize"
        )]
        {
            section.pos() as usize
        }
    };

    let mut raw = std::fs::read(&blob_file_path)?;
    // Second frame offset: data_start + first frame (header + key + value).
    let second_frame_offset = data_start + BLOB_HEADER_LEN + 1 + 50;

    // Corrupt the second frame's magic to b"META".
    raw.get_mut(second_frame_offset..second_frame_offset + 4)
        .unwrap()
        .copy_from_slice(b"META");
    std::fs::write(&blob_file_path, &raw)?;

    let mut scanner = Scanner::new(&blob_file_path, &StdFs, 0)?;

    // First frame should still be readable (it's intact).
    let first = scanner.next().unwrap();
    assert!(first.is_ok(), "first frame should be OK: {first:?}");

    // Second frame has corrupted magic — scanner must return an
    // error, NOT silently terminate.
    let second = scanner.next().unwrap();
    assert!(
        matches!(second, Err(crate::Error::InvalidHeader("Blob"))),
        "expected InvalidHeader for META-corrupted magic, got: {second:?}",
    );

    Ok(())
}

/// Scanner rejects blob files that have no SFA "data" section.
#[test]
fn blob_scanner_rejects_missing_data_section() -> crate::Result<()> {
    use std::io::Write;

    let dir = tempdir()?;
    let blob_file_path = dir.path().join("0");

    // Write an SFA file with only a "meta" section (no "data").
    {
        let file = std::fs::File::create(&blob_file_path)?;
        let mut sfa_writer = crate::sfa::Writer::from_writer(file);
        sfa_writer.start("meta")?;
        sfa_writer.write_all(b"dummy")?;
        sfa_writer.finish()?;
    }

    let result = Scanner::new(&blob_file_path, &StdFs, 0);
    assert!(result.is_err(), "expected error for missing data section");
    let err = result.err().unwrap();
    assert!(
        matches!(err, crate::Error::InvalidHeader("BlobFile")),
        "expected InvalidHeader for missing data section, got: {err:?}",
    );

    Ok(())
}

/// Scanner rejects blob files where the SFA TOC reports a data
/// section whose pos + len overflows u64.
#[test]
fn blob_scanner_rejects_data_section_offset_overflow() -> crate::Result<()> {
    use crate::io::{LittleEndian, WriteBytesExt};
    use std::io::Write;

    let dir = tempdir()?;
    let blob_file_path = dir.path().join("0");

    // Craft a valid SFA file with a "data" TOC entry where
    // pos=1 and len=u64::MAX, causing pos+len to overflow.
    //
    // Hand-encoding is intentional: the sfa crate derives TOC
    // values from real stream positions, so it cannot produce
    // overflowing entries through its public API. The binary
    // format below matches sfa 1.x's stable on-disk layout.
    //
    // SFA layout: [section data...] [TOC] [Trailer]
    // TOC entry:  [pos: u64 LE] [len: u64 LE] [name_len: u16 LE] [name]
    // TOC header: [magic: "TOC!"] [entry_count: u32 LE] [entries...]
    // Trailer:    [magic: "SFA!"] [version: u8] [checksum_type: u8]
    //             [toc_checksum: u128 LE] [toc_pos: u64 LE] [toc_len: u64 LE]
    {
        let mut file = std::fs::File::create(&blob_file_path)?;

        // Write 1 byte of dummy data so toc_pos > 0.
        file.write_all(b"\x00")?;
        let toc_pos: u64 = 1;

        // Build TOC bytes: one entry named "data" with pos=1, len=u64::MAX.
        let mut toc_buf = Vec::new();
        toc_buf.write_all(b"TOC!")?;
        toc_buf.write_u32::<LittleEndian>(1)?; // 1 entry
        toc_buf.write_u64::<LittleEndian>(1)?; // pos = 1
        toc_buf.write_u64::<LittleEndian>(u64::MAX)?; // len = u64::MAX → overflow
        toc_buf.write_u16::<LittleEndian>(4)?; // name len
        toc_buf.write_all(b"data")?; // name

        // Compute TOC checksum (xxh3-128 over the raw TOC bytes).
        let toc_checksum = xxhash_rust::xxh3::xxh3_128(&toc_buf);

        let toc_len = toc_buf.len() as u64;
        file.write_all(&toc_buf)?;

        // Write trailer.
        file.write_all(b"SFA!")?;
        file.write_u8(0x1)?; // version
        file.write_u8(0x0)?; // checksum type (xxh3)
        file.write_u128::<LittleEndian>(toc_checksum)?;
        file.write_u64::<LittleEndian>(toc_pos)?;
        file.write_u64::<LittleEndian>(toc_len)?;

        file.sync_all()?;
    }

    let result = Scanner::new(&blob_file_path, &StdFs, 0);
    assert!(
        result.is_err(),
        "expected error for overflowing data section"
    );
    let err = result.err().unwrap();
    assert!(
        matches!(err, crate::Error::InvalidHeader("BlobFile")),
        "expected InvalidHeader(\"BlobFile\") for overflow, got: {err:?}",
    );

    Ok(())
}

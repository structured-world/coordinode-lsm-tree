use super::*;
use crate::vlog::blob_file::frame::{forged_header, restamp_header_crc};
use crate::vlog::blob_file::writer::{
    FrameSizes, MAX_DECOMPRESSION_SIZE, Writer as BlobFileWriter,
};
use crate::{Slice, fs::StdFs};
use tempfile::tempdir;
use test_log::test;

/// Small sizes, so a test reaches every class with a few bytes: values of
/// 100 to 300 bytes are frames of their own.
const SIZES: FrameSizes = FrameSizes {
    block_target: 100,
    chunk_size: 300,
};

/// Writes `values` into a blob file at `dir/0` and returns its path.
fn write(dir: &std::path::Path, values: &[(&[u8], &[u8])]) -> crate::Result<std::path::PathBuf> {
    let path = dir.join("0");
    let mut writer = BlobFileWriter::new(&path, 0, 0, &StdFs)?.use_frame_sizes(SIZES);
    for (key, value) in values {
        writer.write(key, 7, value)?;
    }
    writer.finish()?;
    Ok(path)
}

/// Where the file's `name` section lies.
fn section(path: &std::path::Path, name: &[u8]) -> crate::Result<(usize, usize)> {
    let reader = crate::sfa::Reader::new(path)?;
    let section = reader.toc().section(name).expect("the section exists");
    Ok((
        usize::try_from(section.pos()).expect("small file"),
        usize::try_from(section.len()).expect("small file"),
    ))
}

/// The starts of the file's frames, then its data end, as its index lists them.
fn index_of(path: &std::path::Path) -> crate::Result<Vec<usize>> {
    let (pos, len) = section(path, b"frames")?;
    let bytes = std::fs::read(path)?;
    let index = crate::vlog::blob_file::writer::decode_frame_index(&bytes[pos..pos + len])?;
    Ok(index.iter().map(|&at| at as usize).collect())
}

/// Damages the file's frame index, so a scan walks the frames' own lengths.
fn break_index(path: &std::path::Path) -> crate::Result<()> {
    let (pos, _) = section(path, b"frames")?;
    let mut bytes = std::fs::read(path)?;
    bytes[pos] ^= 0xFF;
    std::fs::write(path, bytes)?;
    Ok(())
}

/// Applies `edit` to the file's bytes.
fn edit(path: &std::path::Path, edit: impl FnOnce(&mut Vec<u8>)) -> crate::Result<()> {
    let mut bytes = std::fs::read(path)?;
    edit(&mut bytes);
    std::fs::write(path, bytes)?;
    Ok(())
}

/// Three values of frames of their own.
fn three_singles(dir: &std::path::Path) -> crate::Result<std::path::PathBuf> {
    write(
        dir,
        &[
            (b"aaa", &[b'1'; 120]),
            (b"bbb", &[b'2'; 130]),
            (b"ccc", &[b'3'; 140]),
        ],
    )
}

/// Every value comes back in order with its frame and index: a block's
/// values share their frame, and the block is consumed only with its last
/// value; a value with frames of its own hands them over as stored; a
/// chunked value comes back whole.
#[test]
fn a_scan_yields_every_class() -> crate::Result<()> {
    let dir = tempdir()?;
    let chunked: Vec<u8> = (0..700_u32).map(|i| (i % 251) as u8).collect();
    let path = write(
        dir.path(),
        &[
            (b"a", b"x1"),
            (b"b", b"x2"),
            (b"c", &[b'c'; 150]),
            (b"d", &chunked),
            (b"e", b"x3"),
        ],
    )?;
    let index = index_of(&path)?;
    let entries = Scanner::new(&path, &StdFs, 0)?.collect::<crate::Result<Vec<_>>>()?;

    let places: Vec<_> = entries.iter().map(|e| (e.frame, e.index)).collect();
    assert_eq!(places, [(0, 0), (0, 1), (1, 0), (2, 0), (6, 0)]);
    assert_eq!(
        entries[0].frame_end, index[0] as u64,
        "the block is not consumed yet"
    );
    assert_eq!(
        entries[1].frame_end, index[1] as u64,
        "its last value consumes it"
    );
    assert!(entries[0].frames.is_none() && entries[1].frames.is_none());
    assert_eq!(entries[2].frames.as_ref().map(Vec::len), Some(1));
    assert_eq!(
        entries[3].frames.as_ref().map(Vec::len),
        Some(4),
        "a head and its chunks"
    );
    assert_eq!(entries[3].frame_end, index[6] as u64, "past its last chunk");
    assert_eq!(entries[3].value, Slice::from(chunked));
    assert_eq!(entries[2].value, Slice::from(&[b'c'; 150][..]));
    assert!(entries.iter().all(|e| !e.resynced && e.seqno == 7));
    Ok(())
}

/// A scan resumed at a frame boundary a previous scan reported yields the
/// suffix; an offset outside the data section, or inside a frame, is refused.
#[test]
fn a_resumed_scan_reads_the_suffix_and_refuses_a_bad_offset() -> crate::Result<()> {
    let dir = tempdir()?;
    let path = three_singles(dir.path())?;
    let resume_at = {
        let mut scanner = Scanner::new(&path, &StdFs, 0)?;
        scanner.next().expect("a")?.frame_end
    };
    let keys = Scanner::resume(&path, &StdFs, 0, resume_at)?
        .map(|entry| entry.map(|e| e.key))
        .collect::<crate::Result<Vec<_>>>()?;
    assert_eq!(keys, [Slice::from(&b"bbb"[..]), Slice::from(&b"ccc"[..])]);

    for offset in [u64::MAX, resume_at + 1] {
        assert!(
            matches!(
                Scanner::resume(&path, &StdFs, 0, offset),
                Err(crate::Error::InvalidHeader("BlobFile"))
            ),
            "offset {offset} must be refused"
        );
    }
    Ok(())
}

/// Opening a scanner reads the file's trailer and table of contents before
/// the first frame: a paced scanner times those reads too, so a relocation
/// that opens many cold blob files up front tells its pacer about every one.
#[cfg(feature = "std")]
#[test]
fn a_paced_scanner_times_the_reads_that_open_it() -> crate::Result<()> {
    use crate::table::util::{Pacer, ReadPacer};
    use core::sync::atomic::{AtomicU64, Ordering};

    struct Timed(AtomicU64);
    impl ReadPacer for Timed {
        fn active(&self) -> bool {
            true
        }
        fn pace(&self, _offset: u64, _len: u64) {}
        fn read_took(&self, _elapsed: core::time::Duration) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let dir = tempdir()?;
    let mut writer = crate::vlog::BlobFileWriter::new(
        crate::SequenceNumberCounter::default(),
        dir.path(),
        0,
        None,
        alloc::sync::Arc::new(StdFs),
    )?;
    writer.write(b"a", 0, &[b'v'; 100])?;
    let files = writer.finish()?;

    let timed = alloc::sync::Arc::new(Timed(AtomicU64::new(0)));
    let pace: Pacer = timed.clone();
    let _scanner = Scanner::of_paced(&files[0], None, Some(pace))?;
    assert!(
        timed.0.load(Ordering::Relaxed) > 0,
        "opening the scanner read its trailer and table of contents untimed",
    );
    Ok(())
}

/// By the index, a damaged frame costs that frame alone, wherever the damage
/// is: the next frame is where the index puts it, and its boundary is the
/// index's, so nothing after it is tainted.
#[test]
fn by_the_index_a_damaged_frame_costs_only_itself() -> crate::Result<()> {
    let dir = tempdir()?;
    let path = three_singles(dir.path())?;
    let index = index_of(&path)?;
    let pristine = std::fs::read(&path)?;
    // The magic, a length field, the header CRC, a key byte, a value byte.
    for at in [0, 12, 20, 40 + 14, index[2] - index[1] - 1] {
        std::fs::write(&path, &pristine)?;
        edit(&path, |bytes| bytes[index[1] + at] ^= 0x40)?;
        let mut scanner = Scanner::new(&path, &StdFs, 0)?;
        assert_eq!(scanner.next().expect("aaa")?.key, Slice::from(&b"aaa"[..]));
        assert!(
            scanner.next().expect("bbb").is_err(),
            "byte {at} of frame 1"
        );
        let third = scanner.next().expect("ccc")?;
        assert_eq!(third.key, Slice::from(&b"ccc"[..]));
        assert!(!third.resynced, "a boundary from the index is not tainted");
        assert!(scanner.next().is_none());
    }
    Ok(())
}

/// A damaged chunk costs its value alone: the walk goes on after the value's
/// last chunk, so the chunks left are not reported one by one.
#[test]
fn a_damaged_chunk_costs_its_value_alone() -> crate::Result<()> {
    let dir = tempdir()?;
    let path = write(dir.path(), &[(b"big", &[9; 700]), (b"next", &[1; 120])])?;
    let index = index_of(&path)?;
    // The first chunk is frame 1.
    edit(&path, |bytes| bytes[index[2] - 1] ^= 0x40)?;
    let mut scanner = Scanner::new(&path, &StdFs, 0)?;
    assert!(matches!(
        scanner.next(),
        Some(Err(crate::Error::ChecksumMismatch { .. }))
    ));
    let next = scanner.next().expect("the value after it")?;
    assert_eq!(next.key, Slice::from(&b"next"[..]));
    assert!(scanner.next().is_none());
    Ok(())
}

/// A field past the decompression cap under a header CRC that holds is
/// refused before anything is allocated for it.
#[test]
fn an_over_cap_length_is_refused() -> crate::Result<()> {
    let dir = tempdir()?;
    let path = three_singles(dir.path())?;
    let over_cap = u32::try_from(MAX_DECOMPRESSION_SIZE).expect("fits") + 1;
    edit(&path, |bytes| {
        bytes[12..16].copy_from_slice(&over_cap.to_le_bytes());
        restamp_header_crc(&mut bytes[..]);
    })?;
    let mut scanner = Scanner::new(&path, &StdFs, 0)?;
    let result = scanner.next().expect("a result");
    assert!(
        matches!(result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
        "got: {result:?}"
    );
    assert_eq!(scanner.next().expect("bbb")?.key, Slice::from(&b"bbb"[..]));
    Ok(())
}

/// The earlier record layout is not readable: a frame under its magic is
/// refused, and the walk goes on.
#[test]
fn an_earlier_record_magic_is_refused() -> crate::Result<()> {
    let dir = tempdir()?;
    let path = three_singles(dir.path())?;
    edit(&path, |bytes| bytes[..4].copy_from_slice(b"BLO4"))?;
    let mut scanner = Scanner::new(&path, &StdFs, 0)?;
    assert!(matches!(
        scanner.next(),
        Some(Err(crate::Error::InvalidHeader("BlobFrame")))
    ));
    assert_eq!(scanner.next().expect("bbb")?.key, Slice::from(&b"bbb"[..]));
    Ok(())
}

/// Without a readable index the walk follows the frames' own lengths, and
/// reads the file as the index would.
#[test]
fn without_its_index_a_file_is_walked_by_its_frames() -> crate::Result<()> {
    let dir = tempdir()?;
    let path = write(
        dir.path(),
        &[(b"a", b"x1"), (b"b", b"x2"), (b"c", &[9; 700])],
    )?;
    let indexed = Scanner::new(&path, &StdFs, 0)?.collect::<crate::Result<Vec<_>>>()?;
    break_index(&path)?;
    let chained = Scanner::new(&path, &StdFs, 0)?.collect::<crate::Result<Vec<_>>>()?;
    assert_eq!(chained, indexed);
    Ok(())
}

/// Without an index, a resumed walk could not number its frames: it is
/// refused rather than handing out wrong frame numbers.
#[test]
fn without_its_index_a_scan_cannot_resume() -> crate::Result<()> {
    let dir = tempdir()?;
    let path = three_singles(dir.path())?;
    let at = index_of(&path)?[1] as u64;
    break_index(&path)?;
    assert!(matches!(
        Scanner::resume(&path, &StdFs, 0, at),
        Err(crate::Error::InvalidHeader("BlobFile"))
    ));
    Ok(())
}

/// In a walk by the frames' lengths, a rotted length field fails the header
/// CRC and the walk searches for the next magic instead of ending: the
/// readable tail survives, and everything past the search is tainted.
#[test]
fn without_its_index_header_rot_resyncs_to_the_next_frame() -> crate::Result<()> {
    let dir = tempdir()?;
    let path = three_singles(dir.path())?;
    edit(&path, |bytes| {
        bytes[16..20].copy_from_slice(&u32::MAX.to_le_bytes())
    })?;
    break_index(&path)?;
    let mut scanner = Scanner::new(&path, &StdFs, 0)?;
    assert!(matches!(
        scanner.next(),
        Some(Err(crate::Error::HeaderCrcMismatch { .. }))
    ));
    let second = scanner.next().expect("bbb")?;
    assert_eq!(second.key, Slice::from(&b"bbb"[..]));
    assert!(second.resynced, "a boundary found by search is tainted");
    let third = scanner.next().expect("ccc")?;
    assert!(third.resynced, "and the taint is sticky");
    assert!(scanner.next().is_none());
    Ok(())
}

/// In a walk by the frames' lengths, a rotted magic is refused and the walk
/// searches on.
#[test]
fn without_its_index_magic_rot_resyncs_to_the_next_frame() -> crate::Result<()> {
    let dir = tempdir()?;
    let path = three_singles(dir.path())?;
    edit(&path, |bytes| bytes[0] ^= 0xFF)?;
    break_index(&path)?;
    let mut scanner = Scanner::new(&path, &StdFs, 0)?;
    assert!(matches!(
        scanner.next(),
        Some(Err(crate::Error::InvalidHeader("BlobFrame")))
    ));
    assert_eq!(scanner.next().expect("bbb")?.key, Slice::from(&b"bbb"[..]));
    Ok(())
}

/// Three frames, frame 0's value carrying a forged frame header 10 bytes in
/// whose declared length ends `past` bytes after frame 2's start, and frame
/// 0's own magic rotted: a search from it lands on the forgery first.
fn write_with_a_forged_header(
    dir: &std::path::Path,
    past: u32,
) -> crate::Result<std::path::PathBuf> {
    // A first pass places the frames: the value's bytes do not move them.
    let filler = [0xAA_u8; 150];
    let path = write(
        dir,
        &[(b"aaa", &filler), (b"bbb", &[2; 130]), (b"ccc", &[3; 140])],
    )?;
    let index = index_of(&path)?;
    std::fs::remove_file(&path)?;

    // The forgery sits in frame 0's value, which starts after its header,
    // its entry and the key.
    let forged_at = 40 + 14 + 3 + 10;
    let entries_len = 14 + 3;
    let disk_len = u32::try_from(index[2] - forged_at - 40 - entries_len).expect("small") + past;
    let header = forged_header(FrameKind::Single, 1, entries_len as u32, disk_len, disk_len);
    let mut value = filler.to_vec();
    value[10..10 + header.len()].copy_from_slice(&header);

    let path = write(
        dir,
        &[(b"aaa", &value), (b"bbb", &[2; 130]), (b"ccc", &[3; 140])],
    )?;
    edit(&path, |bytes| bytes[0] ^= 0xFF)?;
    break_index(&path)?;
    Ok(path)
}

/// A search candidate whose checksum fails does not have its declared length
/// trusted: it came from a value's bytes, and a header CRC that holds can
/// declare an end past intact frames. The search goes on strictly after it,
/// so frame 1 is recovered, not skipped.
#[test]
fn a_forged_candidate_failing_its_checksum_is_searched_past() -> crate::Result<()> {
    let dir = tempdir()?;
    let path = write_with_a_forged_header(dir.path(), 0)?;
    let mut scanner = Scanner::new(&path, &StdFs, 0)?;
    assert!(scanner.next().expect("the rotted frame").is_err());
    assert!(
        matches!(
            scanner.next(),
            Some(Err(crate::Error::ChecksumMismatch { .. }))
        ),
        "the forgery fails its checksum"
    );
    let second = scanner.next().expect("bbb")?;
    assert_eq!(
        second.key,
        Slice::from(&b"bbb"[..]),
        "recovered, not skipped"
    );
    assert!(second.resynced);
    assert_eq!(scanner.next().expect("ccc")?.key, Slice::from(&b"ccc"[..]));
    assert!(scanner.next().is_none());
    Ok(())
}

/// A search candidate declaring an end past the data section does not end
/// the walk: that would hand one crafted value the whole readable tail.
#[test]
fn a_forged_candidate_declaring_past_the_section_is_searched_past() -> crate::Result<()> {
    let dir = tempdir()?;
    let path = write_with_a_forged_header(dir.path(), 1_000_000)?;
    let mut scanner = Scanner::new(&path, &StdFs, 0)?;
    assert!(scanner.next().expect("the rotted frame").is_err());
    assert!(scanner.next().expect("the forgery").is_err());
    assert_eq!(scanner.next().expect("bbb")?.key, Slice::from(&b"bbb"[..]));
    assert_eq!(scanner.next().expect("ccc")?.key, Slice::from(&b"ccc"[..]));
    assert!(scanner.next().is_none());
    Ok(())
}

/// A frame at a chained position whose length was rewritten, under a header
/// CRC that holds, to swallow the next frame fails its checksum, and the walk
/// searches on rather than resuming past what it swallowed.
#[test]
fn a_chained_frame_swallowing_the_next_is_searched_past() -> crate::Result<()> {
    let dir = tempdir()?;
    let path = three_singles(dir.path())?;
    let index = index_of(&path)?;
    let swallowing = u32::try_from(index[2] - 40 - 14 - 3).expect("small");
    edit(&path, |bytes| {
        bytes[16..20].copy_from_slice(&swallowing.to_le_bytes());
        restamp_header_crc(&mut bytes[..]);
    })?;
    break_index(&path)?;
    let mut scanner = Scanner::new(&path, &StdFs, 0)?;
    assert!(scanner.next().expect("the swallowing frame").is_err());
    assert_eq!(
        scanner.next().expect("bbb")?.key,
        Slice::from(&b"bbb"[..]),
        "not skipped by the rewritten length"
    );
    assert_eq!(scanner.next().expect("ccc")?.key, Slice::from(&b"ccc"[..]));
    assert!(scanner.next().is_none());
    Ok(())
}

/// A frame at a chained position declaring an end past the data section is
/// refused and searched past, not taken for the end of the file.
#[test]
fn a_chained_frame_declaring_past_the_section_is_searched_past() -> crate::Result<()> {
    let dir = tempdir()?;
    let path = three_singles(dir.path())?;
    edit(&path, |bytes| {
        let huge = u32::try_from(bytes.len()).expect("small");
        bytes[16..20].copy_from_slice(&huge.to_le_bytes());
        restamp_header_crc(&mut bytes[..]);
    })?;
    break_index(&path)?;
    let mut scanner = Scanner::new(&path, &StdFs, 0)?;
    assert!(matches!(
        scanner.next(),
        Some(Err(crate::Error::InvalidHeader("BlobFrame")))
    ));
    assert_eq!(scanner.next().expect("bbb")?.key, Slice::from(&b"bbb"[..]));
    Ok(())
}

/// Bytes that spell another section's magic where a frame starts are damage,
/// reported as such, never read as the end of the data.
#[test]
fn a_frame_rotted_into_meta_is_not_a_silent_end() -> crate::Result<()> {
    let dir = tempdir()?;
    let path = three_singles(dir.path())?;
    let index = index_of(&path)?;
    edit(&path, |bytes| {
        bytes[index[1]..index[1] + 4].copy_from_slice(b"META")
    })?;
    let mut scanner = Scanner::new(&path, &StdFs, 0)?;
    assert!(scanner.next().expect("aaa").is_ok());
    assert!(matches!(
        scanner.next(),
        Some(Err(crate::Error::InvalidHeader("BlobFrame")))
    ));
    assert!(scanner.next().expect("ccc").is_ok());
    Ok(())
}

/// A data section ending inside a frame header is refused before the header
/// is read, so the read never takes bytes of the section after it.
#[test]
fn a_partial_header_at_the_section_end_is_refused() -> crate::Result<()> {
    use std::io::Write;

    let dir = tempdir()?;
    let path = dir.path().join("0");
    {
        let file = std::fs::File::create(&path)?;
        let mut sfa_writer = crate::sfa::Writer::from_writer(file);
        sfa_writer.start("data")?;
        sfa_writer.write_all(&super::super::frame::FRAME_MAGIC)?;
        sfa_writer.write_all(&[0; 8])?;
        sfa_writer.start("meta")?;
        sfa_writer.write_all(b"unread")?;
        sfa_writer.into_inner()?.sync_all()?;
    }
    let codec = ScanCodec {
        compression: CompressionType::None,
        #[cfg(zstd_any)]
        dictionary: None,
    };
    let mut scanner = Scanner::open(&path, &StdFs, 0, codec, None)?;
    assert!(matches!(
        scanner.next(),
        Some(Err(crate::Error::InvalidHeader("BlobFrame")))
    ));
    assert!(scanner.next().is_none());
    Ok(())
}

fn plain() -> ScanCodec {
    ScanCodec {
        compression: CompressionType::None,
        #[cfg(zstd_any)]
        dictionary: None,
    }
}

/// A file without a data section is refused at open.
#[test]
fn a_file_without_a_data_section_is_refused() -> crate::Result<()> {
    use std::io::Write;

    let dir = tempdir()?;
    let path = dir.path().join("0");
    {
        let file = std::fs::File::create(&path)?;
        let mut sfa_writer = crate::sfa::Writer::from_writer(file);
        sfa_writer.start("meta")?;
        sfa_writer.write_all(b"dummy")?;
        sfa_writer.finish()?;
    }
    assert!(matches!(
        Scanner::open(&path, &StdFs, 0, plain(), None),
        Err(crate::Error::InvalidHeader("BlobFile"))
    ));
    Ok(())
}

/// A data section whose end does not fit a `u64` is refused at open.
#[test]
fn a_data_section_past_u64_is_refused() -> crate::Result<()> {
    use crate::io::{LittleEndian, WriteBytesExt};
    use std::io::Write;

    let dir = tempdir()?;
    let path = dir.path().join("0");
    // Hand-encoded: the sfa crate derives table-of-contents entries from real
    // stream positions, so it cannot produce an overflowing one.
    {
        let mut file = std::fs::File::create(&path)?;
        file.write_all(b"\x00")?;
        let mut toc = Vec::new();
        toc.write_all(b"TOC!")?;
        toc.write_u32::<LittleEndian>(1)?;
        toc.write_u64::<LittleEndian>(1)?;
        toc.write_u64::<LittleEndian>(u64::MAX)?;
        toc.write_u16::<LittleEndian>(4)?;
        toc.write_all(b"data")?;
        let checksum = xxhash_rust::xxh3::xxh3_128(&toc);
        file.write_all(&toc)?;
        file.write_all(b"SFA!")?;
        file.write_u8(0x1)?;
        file.write_u8(0x0)?;
        file.write_u128::<LittleEndian>(checksum)?;
        file.write_u64::<LittleEndian>(1)?;
        file.write_u64::<LittleEndian>(toc.len() as u64)?;
        file.sync_all()?;
    }
    assert!(matches!(
        Scanner::open(&path, &StdFs, 0, plain(), None),
        Err(crate::Error::InvalidHeader("BlobFile"))
    ));
    Ok(())
}

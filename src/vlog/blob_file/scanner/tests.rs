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
/// chunked value comes back as its checked frames, not gathered.
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

    // The index a handle names each by is checked against the writes in
    // `a_scan_names_each_value_as_its_handle_does`.
    let frames: Vec<_> = entries.iter().map(|e| e.frame).collect();
    assert_eq!(frames, [0, 0, 1, 2, 6]);
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
    assert_eq!(entries[3].value, None, "a chunked value is not gathered");
    assert_eq!(
        entries[3].whole_value(CompressionType::None)?,
        Slice::from(chunked),
        "its frames hold it whole"
    );
    assert_eq!(entries[2].value, Some(Slice::from(&[b'c'; 150][..])));
    assert!(entries.iter().all(|e| !e.resynced && e.seqno == 7));
    Ok(())
}

/// A value comes back at the frame and index its write returned, the place a
/// handle names it by: in a packed block that is its record's offset, not its
/// place in the block, or a compaction matching the scan against the tables'
/// handles drains every record after the first as unreferenced.
#[test]
fn a_scan_names_each_value_as_its_handle_does() -> crate::Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("0");
    let mut writer = BlobFileWriter::new(&path, 0, 0, &StdFs)?.use_frame_sizes(SIZES);
    let values: [(&[u8], &[u8]); 4] = [
        (b"a", b"x1"),
        (b"b", b"x22"),
        (b"c", &[b'c'; 150]),
        (b"d", b"x333"),
    ];
    let mut written = Vec::new();
    for (key, value) in values {
        let at = writer.write(key, 7, value)?;
        written.push((at.frame, at.index));
    }
    writer.finish()?;
    let entries = Scanner::new(&path, &StdFs, 0)?.collect::<crate::Result<Vec<_>>>()?;
    let scanned: Vec<_> = entries.iter().map(|e| (e.frame, e.index)).collect();
    assert_eq!(scanned, written);
    assert!(written[1].1 > 0, "the second record starts past the first");
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

/// Loading a file's frame index: an environmental read failure is returned
/// for a retry, never taken for damage, since the index may be intact and the
/// chain walk would give up frames past damage elsewhere; a read the bytes
/// fail falls back to the chain; an intact index is read.
#[cfg(feature = "std")]
#[test]
fn an_environmental_index_read_failure_is_returned_not_walked_around() -> crate::Result<()> {
    use std::io::{Cursor, Read, Seek, SeekFrom};

    struct Failing(std::io::ErrorKind);
    impl Read for Failing {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            Err(self.0.into())
        }
    }
    // Seeks succeed and the file is long enough to hold the index: only its
    // reads fail.
    impl Seek for Failing {
        fn seek(&mut self, to: SeekFrom) -> std::io::Result<u64> {
            Ok(match to {
                SeekFrom::End(_) => 1 << 20,
                _ => 0,
            })
        }
    }

    let dir = tempdir()?;
    let path = three_singles(dir.path())?;
    let (pos, len) = section(&path, b"frames")?;
    let (data_pos, data_len) = section(&path, b"data")?;
    let at = (pos as u64, len as u64);
    let data = data_pos as u64..(data_pos + data_len) as u64;

    let mut file = Cursor::new(std::fs::read(&path)?);
    let index = read_frame_index(&mut file, at, data.clone())?;
    assert_eq!(index.map(|i| i.len()), Some(4), "three frames and the end");

    assert!(matches!(
        read_frame_index(&mut Failing(std::io::ErrorKind::PermissionDenied), at, data.clone()),
        Err(crate::Error::Io(e)) if e.kind() == crate::io::ErrorKind::PermissionDenied
    ));
    assert!(
        read_frame_index(
            &mut Failing(std::io::ErrorKind::InvalidData),
            at,
            data.clone()
        )?
        .is_none(),
        "a read the bytes fail walks the chain"
    );
    // A file whose length cannot be asked is handled as its reads are.
    struct NoLength(std::io::ErrorKind);
    impl Read for NoLength {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            Ok(0)
        }
    }
    impl Seek for NoLength {
        fn seek(&mut self, _: SeekFrom) -> std::io::Result<u64> {
            Err(self.0.into())
        }
    }
    assert!(matches!(
        read_frame_index(
            &mut NoLength(std::io::ErrorKind::PermissionDenied),
            at,
            data.clone()
        ),
        Err(crate::Error::Io(e)) if e.kind() == crate::io::ErrorKind::PermissionDenied
    ));
    assert!(
        read_frame_index(
            &mut NoLength(std::io::ErrorKind::InvalidData),
            at,
            data.clone()
        )?
        .is_none()
    );
    // An index named as running past the file is damage, refused before
    // anything is sized from the length it names.
    assert!(
        read_frame_index(&mut file, (pos as u64, u64::MAX / 2), data)?.is_none(),
        "an index past the file walks the chain"
    );
    Ok(())
}

/// A copying scan checks a frame a value has to itself as stored and leaves
/// it undecoded: its stored payload, garbage to the codec but sealed, is
/// handed over as its frames, which a scan proving every value refuses; a
/// block is still decoded, its values taken one by one; and stored bytes
/// that no longer match the checksum are refused either way.
#[cfg(all(feature = "std", feature = "lz4"))]
#[test]
fn a_copying_scan_leaves_frames_of_their_own_undecoded() -> crate::Result<()> {
    let dir = tempdir()?;
    let chunked: Vec<u8> = (0..700_u32).map(|i| (i % 7) as u8).collect();
    let mut writer = crate::vlog::BlobFileWriter::new(
        crate::SequenceNumberCounter::default(),
        dir.path(),
        0,
        None,
        alloc::sync::Arc::new(StdFs),
    )?
    .use_target_size(u64::MAX)
    .use_frame_sizes(SIZES)
    .use_compression(CompressionType::Lz4);
    for (key, value) in [
        (&b"a"[..], &b"x1x1x1x1"[..]),
        (b"b", b"x2x2x2x2"),
        (b"c", &[b'c'; 150]),
        (b"d", &chunked),
    ] {
        writer.write(key, 0, value)?;
    }
    let file = writer.finish()?.remove(0);
    let path = file.path().to_path_buf();
    let index = index_of(&path)?;
    // Frame 1 is the single frame, frame 2 the head, frame 3 its first chunk:
    // the single frame's payload garbled and the frame resealed, the chunk's
    // bytes garbled and its row's checksum set to match them.
    edit(&path, |bytes| {
        let single = &mut bytes[index[1]..index[2]];
        let header = parse_header(single).expect("a frame");
        assert!(!header.is_raw(), "lz4 compressed single frame");
        single[FRAME_HEADER_LEN + header.entries_len as usize..].fill(0xFF);
        crate::vlog::blob_file::frame::reseal(single);

        let chunk = &mut bytes[index[3]..index[4]];
        chunk.fill(0xFF);
        let checksum = crate::vlog::blob_file::frame::chunk_checksum(chunk);
        let head = &mut bytes[index[2]..index[3]];
        let header = parse_header(head).expect("a head");
        let row = FRAME_HEADER_LEN + header.entries_len as usize + 8;
        assert_eq!(head[row + 4], 0, "lz4 compressed first chunk");
        head[row + 5..row + 21].copy_from_slice(&checksum.to_le_bytes());
        crate::vlog::blob_file::frame::reseal(head);
    })?;
    let on_disk = std::fs::read(&path)?;

    let copied = Scanner::of_paced(&file, None, None)?.collect::<crate::Result<Vec<_>>>()?;
    assert_eq!(copied.len(), 4, "every value, none refused");
    assert_eq!(
        copied[0].value.as_deref(),
        Some(&b"x1x1x1x1"[..]),
        "a block decodes"
    );
    assert_eq!(copied[2].value, None, "the single frame is not decoded");
    assert_eq!(
        copied[2].frames.as_deref(),
        Some(&[Slice::from(&on_disk[index[1]..index[2]])][..]),
        "it goes over as stored",
    );
    assert_eq!(copied[3].frames.as_ref().map(Vec::len), Some(4));

    let proven: Vec<_> = Scanner::new(&path, &StdFs, 0)?.collect();
    assert!(
        matches!(proven[2], Err(crate::Error::Decompress(_))),
        "a scan proving every value decodes the single frame: {:?}",
        proven[2],
    );

    edit(&path, |bytes| bytes[index[2] - 1] ^= 1)?;
    let damaged: Vec<_> = Scanner::of_paced(&file, None, None)?.collect();
    assert!(
        matches!(damaged[2], Err(crate::Error::ChecksumMismatch { .. })),
        "the stored bytes are still checked: {:?}",
        damaged[2],
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

/// A chunk whose stored bytes check but do not decode is refused by a scan
/// that decodes, as a read of the value would refuse it: the checksum proves
/// the bytes are the ones written, not that they decode.
#[cfg(feature = "lz4")]
#[test]
fn a_decoding_scan_refuses_a_chunk_that_checks_but_does_not_decode() -> crate::Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("0");
    let mut writer = BlobFileWriter::new(&path, 0, 0, &StdFs)?
        .use_frame_sizes(SIZES)
        .use_compression(crate::CompressionType::Lz4);
    writer.write(b"big", 7, &[9; 700])?;
    writer.write(b"next", 7, &[1; 120])?;
    writer.finish()?;
    let index = index_of(&path)?;
    edit(&path, |bytes| {
        // The first chunk's stored bytes, frame 1, turned to bytes no LZ4
        // block decodes, then named in its row by their checksum.
        let chunk = &mut bytes[index[1]..index[2]];
        chunk.fill(0xFF);
        let checksum = crate::vlog::blob_file::frame::chunk_checksum(chunk);
        let head = &mut bytes[index[0]..index[1]];
        let entries_len = parse_header(head).expect("a head").entries_len as usize;
        // The first row's checksum, after the chunk size, the count, the
        // row's stored length and its flags.
        let row = FRAME_HEADER_LEN + entries_len + 8;
        head[row + 5..row + 21].copy_from_slice(&checksum.to_le_bytes());
        crate::vlog::blob_file::frame::reseal(head);
    })?;
    let mut scanner = Scanner::new(&path, &StdFs, 0)?;
    assert!(scanner.next().expect("the big value").is_err());
    let next = scanner.next().expect("the value after it")?;
    assert_eq!(next.key, Slice::from(&b"next"[..]));
    Ok(())
}

/// Along the chain, where only the head's table placed its chunks, a value
/// whose chunks do not read is refused and the walk searches on from the
/// head for the next frame: a damaged chunk, and a row naming a stored length
/// a chunk of an uncompressed file cannot have, each cost the value alone,
/// and what the search finds is tainted.
#[test]
fn along_the_chain_a_value_whose_chunks_do_not_read_is_searched_past() -> crate::Result<()> {
    let damaged_chunk = |path: &std::path::Path| -> crate::Result<()> {
        let index = index_of(path)?;
        edit(path, |bytes| bytes[index[2] - 1] ^= 0x40)
    };
    let short_row = |path: &std::path::Path| -> crate::Result<()> {
        let index = index_of(path)?;
        edit(path, |bytes| {
            let head = &mut bytes[index[0]..index[1]];
            let entries_len = parse_header(head).expect("a head").entries_len as usize;
            // The first row's stored length, after the chunk size and count.
            let row = FRAME_HEADER_LEN + entries_len + 8;
            head[row..row + 4].copy_from_slice(&299_u32.to_le_bytes());
            crate::vlog::blob_file::frame::reseal(head);
        })
    };
    type Damage<'a> = &'a dyn Fn(&std::path::Path) -> crate::Result<()>;
    let cases: [(&str, Damage<'_>); 2] = [
        ("a damaged chunk", &damaged_chunk),
        ("a row it cannot hold", &short_row),
    ];
    for (case, damage) in cases {
        let dir = tempdir()?;
        let path = write(dir.path(), &[(b"big", &[9; 700]), (b"next", &[1; 120])])?;
        damage(&path)?;
        break_index(&path)?;
        let results: Vec<_> = Scanner::new(&path, &StdFs, 0)?.collect();
        assert!(results[0].is_err(), "{case}: {:?}", results[0]);
        let next = results
            .iter()
            .filter_map(|r| r.as_ref().ok())
            .next_back()
            .unwrap_or_else(|| panic!("{case}: the value after it"));
        assert_eq!(next.key, Slice::from(&b"next"[..]), "{case}");
        assert!(next.resynced, "{case}: found by search");
    }
    Ok(())
}

/// A head lost to damage takes its chunks with it: by the index, which marks
/// them, the walk passes them without reading one as a frame and goes on to
/// the value after them, untainted, since the index placed it.
#[test]
fn chunks_whose_head_is_damaged_are_passed_with_it() -> crate::Result<()> {
    let dir = tempdir()?;
    let path = write(dir.path(), &[(b"big", &[9; 700]), (b"next", &[1; 120])])?;
    let index = index_of(&path)?;
    // A key byte of the head, frame 0.
    edit(&path, |bytes| bytes[index[0] + 40 + 14] ^= 0x40)?;
    let results: Vec<_> = Scanner::new(&path, &StdFs, 0)?.collect();
    assert_eq!(results.len(), 2, "the head, then the next value");
    assert!(matches!(
        results[0],
        Err(crate::Error::ChecksumMismatch { .. })
    ));
    let next = results[1].as_ref().expect("the next value");
    assert_eq!(next.key, Slice::from(&b"next"[..]));
    assert_eq!(next.frame, 4, "after the head and its three chunks");
    assert!(!next.resynced);
    Ok(())
}

/// The index marks the chunks of a chunked value and no other frame, and a
/// mark on the first frame or on the data end is refused: a chunk follows a
/// head.
#[test]
fn the_frame_index_marks_chunks_alone() -> crate::Result<()> {
    use crate::vlog::blob_file::writer::{
        CHUNK_MARK, decode_frame_index, encode_frame_index_marked,
    };

    let dir = tempdir()?;
    let path = write(
        dir.path(),
        &[
            (b"one", &[1; 120]),
            (b"big", &[9; 700]),
            (b"next", &[1; 120]),
        ],
    )?;
    let (pos, len) = section(&path, b"frames")?;
    let bytes = std::fs::read(&path)?;
    let index = decode_frame_index(&bytes[pos..pos + len])?;
    // A single, a head, three chunks, a single, the data end.
    assert_eq!(index.len(), 7);
    let marked: Vec<bool> = (0..index.len()).map(|at| index.is_chunk(at)).collect();
    assert_eq!(marked, [false, false, true, true, true, false, false]);
    assert!(index.iter().all(|start| start & CHUNK_MARK == 0));

    let first = encode_frame_index_marked(&[10, 20], 30, &[0b01]);
    assert!(decode_frame_index(&first).is_err(), "the first frame");
    // The writer marks starts alone, so the data end is marked by hand, the
    // index sealed again.
    let mut end = encode_frame_index_marked(&[10, 20], 30, &[]);
    let body = end.len() - 8;
    end[body - 1] |= 0x80;
    let digest = xxhash_rust::xxh3::xxh3_64(&end[..body]);
    end[body..].copy_from_slice(&digest.to_le_bytes());
    assert!(decode_frame_index(&end).is_err(), "the data end");
    let second = encode_frame_index_marked(&[10, 20], 30, &[0b10]);
    assert!(decode_frame_index(&second).is_ok_and(|index| index.is_chunk(1)));
    Ok(())
}

/// A chunk is the value's own bytes with no header, so a value can hold a
/// whole frame that checks. When its head is lost to damage the walk by the
/// index still knows the frames after it for chunks, and yields none of them
/// as a value: the frame inside the chunk is the writer's payload, never a
/// record of the file.
#[test]
fn a_frame_inside_a_chunk_whose_head_is_damaged_is_not_a_value() -> crate::Result<()> {
    // A frame of one value that takes exactly a chunk's 300 bytes.
    let forged = (100..300_usize)
        .find_map(|len| {
            let dir = tempdir().ok()?;
            let path = write(dir.path(), &[(b"forged", &vec![b'f'; len])]).ok()?;
            let index = index_of(&path).ok()?;
            let frame = std::fs::read(&path).ok()?[index[0]..index[1]].to_vec();
            (frame.len() == 300).then_some(frame)
        })
        .expect("a single frame of 300 bytes");

    let mut big = forged;
    big.extend_from_slice(&[9; 400]);
    let dir = tempdir()?;
    let path = write(dir.path(), &[(b"big", &big), (b"next", &[1; 120])])?;
    let index = index_of(&path)?;
    // A key byte of the head, frame 0.
    edit(&path, |bytes| bytes[index[0] + 40 + 14] ^= 0x40)?;

    let results: Vec<_> = Scanner::new(&path, &StdFs, 0)?.collect();
    let keys: Vec<_> = results
        .iter()
        .filter_map(|r| r.as_ref().ok())
        .map(|entry| entry.key.clone())
        .collect();
    assert_eq!(keys, [Slice::from(&b"next"[..])], "{results:?}");
    Ok(())
}

/// Rewrites the chunked value's head, frame 0 of a file written by `write`:
/// its value length and its chunk count, sealed again so the head checks.
fn rewrite_head(path: &std::path::Path, value_len: u32, chunks: u32) -> crate::Result<()> {
    rewrite_head_table(path, value_len, None, chunks)
}

/// [`rewrite_head`], the chunk size rewritten too when given.
fn rewrite_head_table(
    path: &std::path::Path,
    value_len: u32,
    chunk_size: Option<u32>,
    chunks: u32,
) -> crate::Result<()> {
    let index = index_of(path)?;
    edit(path, |bytes| {
        let head = &mut bytes[index[0]..index[1]];
        // The entry's value length, after its seqno and key length.
        head[40 + 10..40 + 14].copy_from_slice(&value_len.to_le_bytes());
        // The chunk table after the entry: the chunk size, then the count,
        // then a row per chunk, left as written.
        let entries_len = parse_header(head).expect("a head").entries_len as usize;
        let table = FRAME_HEADER_LEN + entries_len;
        if let Some(size) = chunk_size {
            head[table..table + 4].copy_from_slice(&size.to_le_bytes());
        }
        head[table + 4..table + 8].copy_from_slice(&chunks.to_le_bytes());
        crate::vlog::blob_file::frame::reseal(head);
    })
}

/// A head whose chunks are all their size but run into a frame that is no
/// chunk is refused: the frame after its last true chunk is another value's.
#[test]
fn a_head_whose_chunk_run_meets_another_frame_is_refused() -> crate::Result<()> {
    let dir = tempdir()?;
    // Two chunks of 300 bytes, then a value of its own.
    let path = write(dir.path(), &[(b"big", &[9; 600]), (b"next", &[1; 120])])?;
    // Three full chunks named: the third is the next value's frame.
    rewrite_head(&path, 900, 3)?;
    let first = Scanner::new(&path, &StdFs, 0)?.next().expect("the head");
    assert!(matches!(
        first,
        Err(crate::Error::InvalidHeader("BlobFrame"))
    ));
    Ok(())
}

/// A packed block of values that compress far below their size closes before
/// its values together decode past the cap: its records stay small while what
/// they decode to grows, and a block whose declared size passes the cap is one
/// no reader takes.
#[cfg(zstd_any)]
#[test]
fn packed_blocks_close_before_their_values_decode_past_the_cap() -> crate::Result<()> {
    const VALUES: usize = 320;
    let dir = tempdir()?;
    let path = dir.path().join("0");
    let value = vec![0_u8; 900 * 1024];
    let mut writer = BlobFileWriter::new(&path, 0, 0, &StdFs)?
        .use_compression(crate::CompressionType::Zstd(3))
        .use_frame_sizes(FrameSizes {
            block_target: 1 << 20,
            chunk_size: 1 << 20,
        })
        .use_file_dictionary(64 * 1024);
    for i in 0..VALUES {
        writer.write(format!("k{i:04}").as_bytes(), 7, &value)?;
    }
    writer.finish()?;
    let bytes = std::fs::read(&path)?;
    let index = index_of(&path)?;
    let mut values = 0;
    for frame in index.windows(2) {
        let header = crate::vlog::blob_file::frame::parse_header(&bytes[frame[0]..frame[1]])?;
        values += usize::from(header.count);
    }
    assert_eq!(values, VALUES, "every value lies in a frame a reader takes");
    Ok(())
}

/// A head that checks but names far more chunks than the file holds, a cap's
/// worth of one-byte chunks, is damage refused before room is reserved for
/// them, by the index or along the chain, and the walk goes on after it.
#[test]
fn a_head_naming_more_chunks_than_the_file_holds_is_refused() -> crate::Result<()> {
    let dir = tempdir()?;
    let path = write(dir.path(), &[(b"big", &[9; 700]), (b"next", &[1; 120])])?;
    let cap = MAX_DECOMPRESSION_SIZE as u32;
    rewrite_head_table(&path, cap, Some(1), cap)?;
    let indexed = std::fs::read(&path)?;
    break_index(&path)?;
    let chained = std::fs::read(&path)?;
    for bytes in [indexed, chained] {
        std::fs::write(&path, &bytes)?;
        let results: Vec<_> = Scanner::new(&path, &StdFs, 0)?.collect();
        assert!(matches!(
            results[0],
            Err(crate::Error::InvalidHeader("BlobFrame"))
        ));
        let next = results.iter().filter_map(|r| r.as_ref().ok()).next_back();
        assert_eq!(
            next.map(|e| &*e.key),
            Some(&b"next"[..]),
            "the walk goes on"
        );
    }
    Ok(())
}

/// A head that checks but names chunks its value length does not need, or
/// chunks the file does not hold where they should lie, is damage: the walk
/// reports it for the value instead of taking another frame, or the end of the
/// data, for a chunk.
#[test]
fn a_head_naming_chunks_that_are_not_there_is_refused() -> crate::Result<()> {
    let dir = tempdir()?;
    let value = [9; 700];

    // Four chunks of 300 bytes for 700: the table does not cover the value.
    let path = write(dir.path(), &[(b"big", &value), (b"next", &[1; 120])])?;
    rewrite_head(&path, 700, 4)?;
    let first = Scanner::new(&path, &StdFs, 0)?.next().expect("the head");
    assert!(matches!(
        first,
        Err(crate::Error::InvalidHeader("BlobFrame"))
    ));

    // A thousand bytes in four chunks: the fourth is where the next value lies.
    rewrite_head(&path, 1000, 4)?;
    let first = Scanner::new(&path, &StdFs, 0)?.next().expect("the head");
    assert!(matches!(
        first,
        Err(crate::Error::InvalidHeader("BlobFrame"))
    ));

    // The same with nothing after the value: the fourth is past the data.
    let alone = tempdir()?;
    let path = write(alone.path(), &[(b"big", &value)])?;
    rewrite_head(&path, 1000, 4)?;
    let first = Scanner::new(&path, &StdFs, 0)?.next().expect("the head");
    assert!(matches!(
        first,
        Err(crate::Error::InvalidHeader("BlobFrame"))
    ));
    Ok(())
}

/// An index that checks but ends past the data section is not the file's:
/// the scan walks the frame chain instead and reads every value, none past
/// the section.
#[test]
fn an_index_ending_past_the_data_is_walked_around() -> crate::Result<()> {
    let dir = tempdir()?;
    let path = three_singles(dir.path())?;
    let index = index_of(&path)?;
    let (pos, len) = section(&path, b"frames")?;
    let starts: Vec<u64> = index[..index.len() - 1]
        .iter()
        .map(|&at| at as u64)
        .collect();
    let past = index[index.len() - 1] as u64 + 1;
    let encoded = crate::vlog::blob_file::writer::encode_frame_index(&starts, past);
    assert_eq!(encoded.len(), len, "the same count of frames");
    edit(&path, |bytes| {
        bytes[pos..pos + len].copy_from_slice(&encoded);
    })?;

    let keys: Vec<_> = Scanner::new(&path, &StdFs, 0)?
        .map(|entry| entry.map(|entry| entry.key))
        .collect::<crate::Result<_>>()?;
    assert_eq!(keys.len(), 3, "every value, along the chain");
    Ok(())
}

/// A read that fails anywhere in a scan, by the index or along the frame
/// chain, surfaces as an error and ends the scan: it never reads as a short
/// file or an empty value.
#[test]
fn a_failed_read_ends_the_scan_with_its_error() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};

    let dir = tempdir()?;
    // More than the scanner's buffer, so the frames take reads of their own.
    let values: Vec<(Vec<u8>, Vec<u8>)> =
        (0..40_u8).map(|i| (vec![b'k', i], vec![i; 280])).collect();
    let borrowed: Vec<(&[u8], &[u8])> = values.iter().map(|(k, v)| (&k[..], &v[..])).collect();
    let path = write(dir.path(), &borrowed)?;
    let indexed = std::fs::read(&path)?;
    break_index(&path)?;
    let chained = std::fs::read(&path)?;

    for bytes in [indexed, chained] {
        std::fs::write(&path, &bytes)?;
        let mut failed_any = false;
        for skip in 0..64 {
            let fs = FaultFs::new(StdFs);
            let mut scanner = Scanner::new(&path, &fs, 0)?;
            fs.injector().arm(
                FaultRule::new(FaultOp::Read, Fault::Error(crate::io::ErrorKind::Other))
                    .skip(skip)
                    .once(),
            );
            let results: Vec<_> = scanner.by_ref().collect();
            if let Some(at) = results.iter().position(Result::is_err) {
                failed_any = true;
                assert_eq!(at, results.len() - 1, "the scan ends at the failure");
            } else {
                assert_eq!(results.len(), values.len(), "an untouched scan reads all");
                break;
            }
        }
        assert!(failed_any, "some read of the scan was faulted");
    }
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
        bytes[16..20].copy_from_slice(&u32::MAX.to_le_bytes());
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
        bytes[index[1]..index[1] + 4].copy_from_slice(b"META");
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

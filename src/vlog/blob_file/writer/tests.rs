use super::*;
use crate::fs::StdFs;
use crate::vlog::blob_file::frame::{DecodedFrame, decode_frame};

/// Small sizes, so a test reaches every class with a few bytes.
const SIZES: FrameSizes = FrameSizes {
    block_target: 100,
    chunk_size: 300,
};

/// A frame index decodes only whole and consistent: one too short for its
/// digest, one failing its digest, one whose count disagrees with its length
/// or whose starts do not rise, each under a digest that holds, are refused.
#[test]
fn a_frame_index_that_does_not_hold_is_refused() {
    let refused = |bytes: &[u8]| {
        matches!(
            decode_frame_index(bytes),
            Err(crate::Error::InvalidHeader(_))
        )
    };
    let sealed = |body: &[u8]| {
        let mut out = body.to_vec();
        out.extend_from_slice(&xxhash_rust::xxh3::xxh3_64(body).to_le_bytes());
        out
    };
    let index = encode_frame_index(&[0, 40, 90], 120);
    assert_eq!(
        &*decode_frame_index(&index).expect("whole"),
        &[0, 40, 90, 120]
    );

    assert!(refused(&index[..4]), "shorter than its digest");
    let mut flipped = index.clone();
    flipped[5] ^= 1;
    assert!(refused(&flipped), "a digest that fails");
    let mut count = index[..index.len() - 8].to_vec();
    count[0] = 4;
    assert!(refused(&sealed(&count)), "a count its length does not hold");
    let falling = encode_frame_index(&[0, 90, 40], 120);
    assert!(refused(&falling), "starts that do not rise");
    assert!(refused(&sealed(&[1, 2])), "no room for a count");
}

/// The frames of the finished file at `path`, decoded, by the index `frames`.
fn frames_of(path: &Path, frames: &[u64]) -> Vec<DecodedFrame> {
    let bytes = std::fs::read(path).expect("the file reads");
    decode_all(
        &bytes,
        frames,
        CompressionType::None,
        #[cfg(zstd_any)]
        None,
    )
    .expect("every frame decodes")
}

/// Every frame of a file's `bytes` by its index `frames`, decoded under
/// `compression`: a chunk by the row the head before it lists for it.
fn decode_all(
    bytes: &[u8],
    frames: &[u64],
    compression: CompressionType,
    #[cfg(zstd_any)] dictionary: Option<&crate::compression::ZstdDictionary>,
) -> crate::Result<Vec<DecodedFrame>> {
    let mut decoded: Vec<DecodedFrame> = Vec::new();
    // The head the chunks being read belong to, its place, and how many of
    // its chunks are still to come.
    let mut head: Option<(usize, u32, u32)> = None;
    for pair in frames.windows(2) {
        let stored = crate::Slice::from(&bytes[pair[0] as usize..pair[1] as usize]);
        if let Some((at, next, chunks)) = head
            && next < chunks
        {
            let frame = super::super::frame::decode_chunk(
                &decoded[at].chunk(next)?,
                &stored,
                compression,
                #[cfg(zstd_any)]
                dictionary,
                &mut 0,
            )?;
            decoded.push(frame);
            head = Some((at, next + 1, chunks));
            continue;
        }
        let frame = decode_frame(
            &stored,
            compression,
            #[cfg(zstd_any)]
            dictionary,
            &mut 0,
        )?;
        head = match frame.header.kind {
            FrameKind::Head => Some((decoded.len(), 0, frame.chunk_table()?.1)),
            _ => None,
        };
        decoded.push(frame);
    }
    Ok(decoded)
}

fn writer(folder: &tempfile::TempDir) -> crate::Result<Writer> {
    Ok(Writer::new(folder.path().join("blob"), 0, 0, &StdFs)?.use_frame_sizes(SIZES))
}

/// A writer of `sizes` that compresses, so its small values share blocks: a
/// file that compresses nothing packs them instead.
#[cfg(feature = "lz4")]
fn block_writer(folder: &tempfile::TempDir, sizes: FrameSizes) -> crate::Result<Writer> {
    Ok(Writer::new(folder.path().join("blob"), 0, 0, &StdFs)?
        .use_compression(CompressionType::Lz4)
        .use_frame_sizes(sizes))
}

/// A value below the block target joins a block, one up to the chunk size is
/// a frame of its own, a larger one a head and its chunks; each is found at
/// the frame and index its write returned (in this packed block, the second
/// small value's record offset).
#[test]
fn a_value_goes_to_the_frame_its_size_calls_for() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let mut w = writer(&folder)?;
    let small = [w.write(b"a", 0, b"x1")?, w.write(b"b", 0, b"x2")?];
    let single = w.write(b"c", 0, &[7; 150])?;
    let chunked = w.write(b"d", 0, &[9; 700])?;
    let after = w.write(b"e", 0, b"x3")?;
    let path = w.path.clone();
    let (meta, _, frames) = w.finish()?;

    assert_eq!((small[0].frame, small[0].index), (0, 0));
    assert_eq!(small[1].frame, 0);
    assert_eq!((single.frame, single.index), (1, 0));
    assert_eq!((chunked.frame, chunked.index), (2, 0));
    // The head's three chunks are frames 3 to 5: 300, 300, 100 bytes.
    assert_eq!((after.frame, after.index), (6, 0));
    assert_eq!(meta.item_count, 5);
    assert_eq!(meta.total_uncompressed_bytes, 2 + 2 + 150 + 700 + 2);

    let decoded = frames_of(&path, &frames);
    let kinds: Vec<_> = decoded.iter().map(|f| f.header.kind).collect();
    assert_eq!(
        kinds,
        [
            FrameKind::Block,
            FrameKind::Single,
            FrameKind::Head,
            FrameKind::Chunk,
            FrameKind::Chunk,
            FrameKind::Chunk,
            FrameKind::Block,
        ]
    );
    assert_eq!(decoded[0].place_of(small[1].index), Some(1));
    assert_eq!(decoded[0].key(1), Some(&b"b"[..]));
    assert_eq!(decoded[2].chunk_table()?, (300, 3));
    let chunk_lens: Vec<_> = decoded[3..6].iter().map(|f| f.payload.len()).collect();
    assert_eq!(chunk_lens, [300, 300, 100]);
    assert_eq!(
        meta.total_compressed_bytes,
        frames.last().copied().unwrap_or(0) - frames.first().copied().unwrap_or(0),
        "the metadata counts every frame byte",
    );
    Ok(())
}

/// A block closes for the next value only once it is 90% full: below that a
/// value joins it even past the target, so no block closes small. A value
/// counts as its entry: 14 bytes, its one-byte key and its bytes.
#[cfg(feature = "lz4")]
#[test]
fn a_block_closes_only_once_nearly_full() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let mut w = block_writer(
        &folder,
        FrameSizes {
            block_target: 1_000,
            chunk_size: 3_000,
        },
    )?;
    // 515 + 315 = 830 bytes, under 90% of 1000: the next value joins.
    w.write(b"a", 0, &[1; 500])?;
    w.write(b"b", 0, &[1; 300])?;
    let joined = w.write(b"c", 0, &[1; 300])?;
    assert_eq!(
        (joined.frame, joined.index),
        (0, 2),
        "joins past the target"
    );
    // The block holds 1145 bytes, full: one that would pass the target
    // closes it.
    let next = w.write(b"d", 0, &[1; 10])?;
    assert_eq!((next.frame, next.index), (1, 0));
    // 25 + 890 = 915, full, but 75 more stay within the target.
    w.write(b"e", 0, &[1; 875])?;
    let fits = w.write(b"f", 0, &[1; 60])?;
    assert_eq!(
        (fits.frame, fits.index),
        (1, 2),
        "within the target, it joins"
    );
    w.finish()?;
    Ok(())
}

/// A block is sized by what it holds, entries and keys included: small
/// values under long keys fill it as their entries do, so a block of them
/// stays near the target rather than holding a target's worth of value bytes
/// and several times that in keys.
#[cfg(feature = "lz4")]
#[test]
fn a_block_of_small_values_is_sized_by_its_entries() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let mut w = block_writer(&folder, FrameSizes::DEFAULT)?;
    let target = u64::from(FrameSizes::DEFAULT.block_target);
    let mut first_block = 0u64;
    for i in 0..4_000u32 {
        let at = w.write(format!("key{i:06}").as_bytes(), 0, &i.to_le_bytes())?;
        if at.frame == 0 {
            first_block += 1;
        }
    }
    let (_, _, frames) = w.finish()?;
    // 14 + 9 + 4 bytes a value.
    let entry = 27;
    assert!(
        first_block * entry <= target + entry,
        "{first_block} values of {entry} bytes in a block of {target}"
    );
    assert!(
        frames[1] - frames[0] <= target + entry + 64,
        "the block takes {} bytes",
        frames[1] - frames[0]
    );
    Ok(())
}

/// A block holds at most `u16::MAX` values, the most an index names.
#[cfg(feature = "lz4")]
#[test]
fn a_block_closes_at_the_most_values_an_index_names() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let mut w = block_writer(
        &folder,
        FrameSizes {
            block_target: u32::MAX,
            chunk_size: u32::MAX,
        },
    )?;
    let mut last = None;
    for i in 0..=u32::from(u16::MAX) {
        last = Some(w.write(&i.to_be_bytes(), 0, b"v")?);
    }
    let last = last.expect("written");
    assert_eq!(
        (last.frame, last.index),
        (1, 0),
        "the 65 536th value opens a block"
    );
    w.finish()?;
    Ok(())
}

/// A file that compresses nothing gains nothing from a block sharing its
/// compression: its small values are packed records, each read alone, with
/// or without a dictionary configured.
#[test]
fn a_file_that_compresses_nothing_packs_its_small_values() -> crate::Result<()> {
    use crate::blob_tree::handle::Stored;

    let folder = tempfile::tempdir()?;
    let mut w = writer(&folder)?;
    let first = w.write(b"key", 0, b"value")?;
    let second = w.write(b"kez", 0, b"v")?;
    let (meta, _, _) = w.finish()?;
    let (Stored::Packed(a), Stored::Packed(b)) = (first.stored, second.stored) else {
        panic!("{:?} {:?}", first.stored, second.stored);
    };
    assert_eq!(
        meta.block_entry_bytes,
        u64::from(a + b),
        "a record counts as a block entry, at its length on disk"
    );
    assert!(
        matches!(first.stored, Stored::Packed(_)),
        "{:?}",
        first.stored
    );
    assert!(
        matches!(second.stored, Stored::Packed(_)),
        "{:?}",
        second.stored
    );
    assert_eq!(first.frame, second.frame, "one packed block holds both");
    Ok(())
}

/// What a value takes: its entry uncompressed in a block, whose compressed
/// share is not known when it is written; its frames whole otherwise. The
/// file's metadata sums the block entries and records what its blocks take,
/// the two figures a block entry's share on disk is read from.
#[cfg(feature = "lz4")]
#[test]
fn a_write_reports_what_the_value_takes() -> crate::Result<()> {
    use crate::blob_tree::handle::Stored;

    let folder = tempfile::tempdir()?;
    let mut w = block_writer(&folder, SIZES)?;
    let small = w.write(b"key", 0, b"value")?;
    let other = w.write(b"kez", 0, b"v")?;
    let single = w.write(b"one", 0, &[7; 150])?;
    let chunked = w.write(b"two", 0, &[9; 700])?;
    let (meta, _, frames) = w.finish()?;

    let entry = |key: &[u8], value: &[u8]| {
        (super::super::frame::ENTRY_HEADER_LEN + key.len() + value.len()) as u32
    };
    assert_eq!(small.stored, Stored::InBlock(entry(b"key", b"value")));
    assert_eq!(other.stored, Stored::InBlock(entry(b"kez", b"v")));
    assert_eq!(
        single.stored,
        Stored::Frames((frames[2] - frames[1]) as u32)
    );
    assert_eq!(
        chunked.stored,
        Stored::Frames((frames[6] - frames[2]) as u32),
        "a head and its chunks"
    );
    assert_eq!(
        meta.block_entry_bytes,
        u64::from(entry(b"key", b"value") + entry(b"kez", b"v"))
    );
    assert_eq!(
        meta.block_frame_bytes,
        frames[1] - frames[0],
        "the one block"
    );
    assert_eq!(meta.own_frame_bytes(), frames[6] - frames[1]);
    Ok(())
}

/// A value's frames written as they are land at the next frame, are found by
/// the key they list, and take what they take in the source.
#[test]
fn verbatim_frames_are_written_as_they_are() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let mut source =
        Writer::new(folder.path().join("source"), 0, 0, &StdFs)?.use_frame_sizes(SIZES);
    source.write(b"big", 3, &[9; 700])?;
    let source_path = source.path.clone();
    let (_, _, source_frames) = source.finish()?;
    let bytes = std::fs::read(&source_path)?;
    let stored: Vec<crate::Slice> = source_frames
        .windows(2)
        .map(|pair| crate::Slice::from(&bytes[pair[0] as usize..pair[1] as usize]))
        .collect();

    let mut w = writer(&folder)?;
    w.write(b"a", 0, b"small")?;
    let copied = w.write_verbatim(b"big", &stored, 700)?;
    let path = w.path.clone();
    let (meta, _, frames) = w.finish()?;

    // The small value's block closed before the copy.
    assert_eq!((copied.frame, copied.index), (1, 0));
    assert_eq!(
        copied.stored,
        crate::blob_tree::handle::Stored::Frames(stored.iter().map(|f| f.len() as u32).sum())
    );
    assert_eq!(meta.item_count, 2);
    let decoded = frames_of(&path, &frames);
    assert_eq!(decoded[1].key(0), Some(&b"big"[..]));
    assert_eq!(decoded[1].list[0].seqno, 3, "the source's seqno is kept");
    Ok(())
}

/// A file that compresses nothing hands no value to be compressed ahead, of
/// any class.
#[cfg(feature = "std")]
#[test]
fn an_uncompressed_file_compresses_no_value_ahead() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let w = Writer::new(folder.path().join("plain"), 0, 0, &StdFs)?.use_frame_sizes(SIZES);
    for len in [10, 150, 700] {
        assert!(w.value_codec(len, 0).is_none(), "{len} bytes");
    }
    Ok(())
}

/// A block copied whole is taken only as the block it says it is: a frame of
/// another kind, or a block of another count of values, is refused before a
/// byte of it is written.
#[cfg(feature = "lz4")]
#[test]
fn a_block_copied_whole_must_be_the_block_it_names() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let mut source = block_writer(&folder, SIZES)?;
    source.write(b"a", 0, b"value")?;
    source.write(b"b", 0, b"v")?;
    source.write(b"one", 0, &[7; 150])?;
    let source_path = source.path.clone();
    let (_, _, frames) = source.finish()?;
    let bytes = std::fs::read(&source_path)?;
    let frame = |at: usize| &bytes[frames[at] as usize..frames[at + 1] as usize];
    let copied = |stored, values| CopiedBlock {
        stored,
        first_key: b"a",
        last_key: b"b",
        values,
        value_bytes: 6,
        entry_bytes: 0,
    };

    let target = tempfile::tempdir()?;
    let mut w = writer(&target)?;
    let before = w.len_written();
    for (stored, values) in [(frame(1), 1), (frame(0), 3)] {
        assert!(matches!(
            w.write_block_verbatim(&copied(stored, values)),
            Err(crate::Error::InvalidHeader("verbatim blob block"))
        ));
    }
    assert_eq!(w.len_written(), before, "nothing was written");
    assert_eq!(w.write_block_verbatim(&copied(frame(0), 2))?, 0);
    Ok(())
}

/// A file gathering its own dictionary names the codec of a value to
/// compress ahead from what it has gathered and what is queued before the
/// value: without the dictionary while those leave room in it, none once
/// they fill it, and the dictionary's once it is built.
#[cfg(all(zstd_any, feature = "std"))]
#[test]
fn a_value_ahead_of_a_gathering_file_is_compressed_without_the_dictionary() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let mut w = Writer::new(folder.path().join("blob"), 0, 0, &StdFs)?
        .use_compression(CompressionType::Zstd(3))
        .use_file_dictionary(1_024);
    let value = [7_u8; 100];
    let stamp = |codec: Option<OwnedCodec>| codec.map(|c| CodecStamp::of(&c.as_codec()));

    let gathering = stamp(w.value_codec(100, 0));
    assert!(
        gathering.is_some_and(|s| s.plain && s.dictionary.is_none()),
        "nothing gathered: written without the dictionary"
    );
    assert!(stamp(w.value_codec(100, 1_023)) == gathering, "room left");
    assert!(w.value_codec(100, 1_024).is_none(), "the queue fills it");

    for i in 0..5_u8 {
        w.write(&[b'k', i], 0, &value)?;
    }
    assert!(stamp(w.value_codec(100, 500)) == gathering, "500 + 500");
    assert!(w.value_codec(100, 600).is_none(), "500 + 600");

    for i in 5..11_u8 {
        w.write(&[b'k', i], 0, &value)?;
    }
    assert!(w.dictionary_in_file);
    assert!(
        stamp(w.value_codec(100, 0)).is_some_and(|s| !s.plain && s.dictionary.is_some()),
        "built: written under it"
    );
    Ok(())
}

/// A file's own dictionary is the first values it wrote, as they are: values
/// beginning with the magic of a trained zstd dictionary still make a raw one,
/// so the values after it compress, the file finishes, and every value reads
/// back under the dictionary the file stores.
#[cfg(zstd_any)]
#[test]
fn values_beginning_like_a_trained_dictionary_still_build_a_raw_one() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path().join("blob");
    let mut w = Writer::new(&path, 0, 0, &StdFs)?
        .use_compression(CompressionType::Zstd(3))
        .use_file_dictionary(4 * 1024);
    let mut first = vec![0_u8; 4 * 1024];
    first[..4].copy_from_slice(&[0x37, 0xA4, 0x30, 0xEC]);
    let values: Vec<Vec<u8>> = core::iter::once(first)
        .chain((0..20_u8).map(|i| vec![i; 1_000]))
        .collect();
    for (i, value) in values.iter().enumerate() {
        w.write(format!("k{i:03}").as_bytes(), 0, value)?;
    }
    let (meta, _, frames) = w.finish()?;
    assert!(
        matches!(meta.compression, CompressionType::ZstdDict { .. }),
        "{:?}",
        meta.compression
    );

    let mut file = crate::fs::Fs::open(&StdFs, &path, &crate::fs::FsOpenOptions::new().read(true))?;
    let dict = crate::vlog::read_file_dictionary(&mut file, &meta)?;
    let decoded = decode_all(
        &std::fs::read(&path)?,
        &frames,
        meta.compression,
        dict.as_deref(),
    )?;
    let mut got = Vec::new();
    for frame in &decoded {
        got.extend((0..).map_while(|at| frame.value(at)));
    }
    assert_eq!(got.len(), values.len());
    for (got, value) in got.iter().zip(&values) {
        assert_eq!(&got[..], &value[..]);
    }
    Ok(())
}

/// A file's own dictionary is built from its first values whatever frame they
/// take: a file of single-frame values, or of chunked ones, comes to hold a
/// dictionary, and every value reads back.
#[cfg(zstd_any)]
#[test]
fn single_and_chunked_values_build_a_file_dictionary() -> crate::Result<()> {
    // 200-byte values are single frames under SIZES, 1 000-byte ones chunked.
    for value_len in [200_usize, 1_000] {
        let folder = tempfile::tempdir()?;
        let path = folder.path().join("blob");
        let mut w = Writer::new(&path, 0, 0, &StdFs)?
            .use_compression(CompressionType::Zstd(3))
            .use_frame_sizes(SIZES)
            .use_file_dictionary(1_024);
        let values: Vec<Vec<u8>> = (0..40_u8)
            .map(|i| (0..value_len).map(|at| (at % 7) as u8 ^ i).collect())
            .collect();
        for (i, value) in values.iter().enumerate() {
            w.write(format!("k{i:03}").as_bytes(), 0, value)?;
        }
        let (meta, _, _) = w.finish()?;
        assert!(
            matches!(meta.compression, CompressionType::ZstdDict { .. }),
            "{value_len}-byte values: {:?}",
            meta.compression
        );

        let mut file =
            crate::fs::Fs::open(&StdFs, &path, &crate::fs::FsOpenOptions::new().read(true))?;
        let dict = crate::vlog::read_file_dictionary(&mut file, &meta)?;
        assert!(
            dict.is_some(),
            "{value_len}-byte values: the file stores no dictionary"
        );
        // The scan checks and decodes every frame and chunk under the file's
        // codec, the values written before the dictionary and after it alike.
        let scanner = crate::vlog::blob_file::scanner::Scanner::open(
            &path,
            &StdFs,
            0,
            crate::vlog::blob_file::scanner::ScanCodec {
                compression: meta.compression,
                dictionary: dict.clone(),
            },
            None,
        )?;
        let entries = scanner.collect::<crate::Result<Vec<_>>>()?;
        assert_eq!(entries.len(), values.len(), "{value_len}-byte values");
        // Each value read back whole, a chunked one gathered out of its
        // frames under the file's dictionary.
        for (entry, value) in entries.iter().zip(&values) {
            let got = entry.whole_value_with(meta.compression, dict.as_deref())?;
            assert_eq!(&got[..], &value[..], "{value_len}-byte values");
        }
    }
    Ok(())
}

/// A file set to build a dictionary of its own gives it up for frames copied
/// in as they are, which name the codec they were written in: before it holds
/// a value it records their codec, and once it has written values toward the
/// dictionary it refuses them.
#[cfg(zstd_any)]
#[test]
fn verbatim_frames_end_a_dictionary_only_before_any_value() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let zstd = CompressionType::Zstd(3);
    let mut source = Writer::new(folder.path().join("source"), 0, 0, &StdFs)?
        .use_compression(zstd)
        .use_frame_sizes(SIZES);
    source.write(b"big", 3, &[9; 700])?;
    let source_path = source.path.clone();
    let (_, _, source_frames) = source.finish()?;
    let bytes = std::fs::read(&source_path)?;
    let stored: Vec<crate::Slice> = source_frames
        .windows(2)
        .map(|pair| crate::Slice::from(&bytes[pair[0] as usize..pair[1] as usize]))
        .collect();
    let target = |name: &str| -> crate::Result<Writer> {
        Ok(Writer::new(folder.path().join(name), 1, 0, &StdFs)?
            .use_compression(zstd)
            .use_frame_sizes(SIZES)
            .use_file_dictionary(64 * 1024))
    };

    let mut fresh = target("fresh")?;
    fresh.write_verbatim(b"big", &stored, 700)?;
    fresh.write(b"next", 4, &[5; 50])?;
    let path = fresh.path.clone();
    let (meta, _, frames) = fresh.finish()?;
    assert_eq!(meta.compression, zstd, "the copied frames' codec");
    let decoded = decode_all(&std::fs::read(&path)?, &frames, zstd, None)?;
    assert_eq!(decoded[0].key(0), Some(&b"big"[..]));

    let mut started = target("started")?;
    started.write(b"a", 0, &[5; 50])?;
    assert!(matches!(
        started.write_verbatim(b"big", &stored, 700),
        Err(crate::Error::InvalidHeader(_))
    ));
    Ok(())
}

/// Frames written as they are but listed at another seqno take the bytes the
/// source's do, list the value at that seqno, and check; the chunks after the
/// head are the source's bytes.
#[test]
fn verbatim_frames_listed_at_another_seqno_check() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let mut source =
        Writer::new(folder.path().join("source"), 0, 0, &StdFs)?.use_frame_sizes(SIZES);
    source.write(b"big", 3, &[9; 700])?;
    let source_path = source.path.clone();
    let (_, _, source_frames) = source.finish()?;
    let bytes = std::fs::read(&source_path)?;
    let stored: Vec<crate::Slice> = source_frames
        .windows(2)
        .map(|pair| crate::Slice::from(&bytes[pair[0] as usize..pair[1] as usize]))
        .collect();

    let mut w = writer(&folder)?;
    let copied = w.write_verbatim_at(b"big", &stored, 700, 8)?;
    let path = w.path.clone();
    let (_, _, frames) = w.finish()?;

    assert_eq!(
        copied.stored,
        crate::blob_tree::handle::Stored::Frames(stored.iter().map(|f| f.len() as u32).sum())
    );
    let decoded = frames_of(&path, &frames);
    assert_eq!(decoded[0].list[0].seqno, 8);
    let written = std::fs::read(&path)?;
    for (at, chunk) in stored.iter().enumerate().skip(1) {
        assert_eq!(
            written[frames[at] as usize..frames[at + 1] as usize],
            chunk[..],
            "chunk {at} as stored"
        );
    }
    Ok(())
}

/// Frames that are not one whole value under the key are refused before
/// anything is written: a block, a chunk without its head, another key's
/// value, a head followed by something other than chunks or by another
/// number of chunks than it names, a length its entry does not list, a slice
/// that is not one whole frame.
#[test]
fn verbatim_frames_that_are_not_one_value_are_refused() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let mut source =
        Writer::new(folder.path().join("source"), 0, 0, &StdFs)?.use_frame_sizes(SIZES);
    source.write(b"a", 0, b"small")?;
    source.write(b"big", 0, &[9; 700])?;
    let source_path = source.path.clone();
    let (_, _, frames) = source.finish()?;
    let bytes = std::fs::read(&source_path)?;
    let frame =
        |at: usize| crate::Slice::from(&bytes[frames[at] as usize..frames[at + 1] as usize]);
    let (block, head, chunk) = (frame(0), frame(1), frame(2));
    let chunks = [head.clone(), chunk.clone(), frame(3), frame(4)];

    let mut w = writer(&folder)?;
    let refused =
        |result: crate::Result<FrameRef>| matches!(result, Err(crate::Error::InvalidHeader(_)));
    assert!(
        refused(w.write_verbatim(b"big", &chunks[..3], 700)),
        "a head short of its chunks"
    );
    assert!(
        refused(w.write_verbatim(b"big", &[&chunks[..], &[frame(4)]].concat(), 700)),
        "a head with a chunk past its own"
    );
    assert!(
        refused(w.write_verbatim(b"big", &chunks, 600)),
        "a length the head does not list"
    );
    assert!(
        refused(w.write_verbatim(b"a", std::slice::from_ref(&block), 5)),
        "a block"
    );
    assert!(
        refused(w.write_verbatim(b"big", std::slice::from_ref(&chunk), 300)),
        "a chunk alone"
    );
    assert!(
        refused(w.write_verbatim(b"other", &chunks, 700)),
        "another key"
    );
    assert!(
        refused(w.write_verbatim(b"big", &[head.clone(), chunk.clone(), frame(3), block], 700)),
        "a head and a block"
    );
    // Each slice is one whole frame: a chunk cut short or carrying a byte
    // past its frame would put the index out of step with the frames.
    let cut = chunk.slice(..chunk.len() - 1);
    let mut longer = chunk.to_vec();
    longer.push(0);
    for bad in [cut, crate::Slice::from(longer)] {
        assert!(
            refused(w.write_verbatim(b"big", &[head.clone(), bad, frame(3), frame(4)], 700)),
            "a chunk that is not one whole frame"
        );
    }
    // A byte past the head leaves its chunk count readable, so only the
    // length check refuses it.
    let mut longer_head = head.to_vec();
    longer_head.push(0);
    assert!(
        refused(w.write_verbatim(
            b"big",
            &[
                crate::Slice::from(longer_head),
                chunk.clone(),
                frame(3),
                frame(4)
            ],
            700
        )),
        "a head with a byte past its frame"
    );
    assert!(
        refused(w.write_verbatim(
            b"big",
            &[head.slice(..head.len() - 1), chunk, frame(3), frame(4)],
            700
        )),
        "a head that is not one whole frame"
    );
    assert!(
        refused(w.write_verbatim(b"big", std::slice::from_ref(&head), 700)),
        "a head without chunks"
    );
    assert!(refused(w.write_verbatim(b"big", &[], 700)), "nothing");
    assert_eq!(w.len_written(), 0, "nothing was written");
    Ok(())
}

/// A value past the decompression cap is refused.
#[test]
fn blob_write_rejects_oversized_value() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let mut w = Writer::new(folder.path().join("blob"), 0, 0, &StdFs)?;
    let oversize = vec![0_u8; MAX_DECOMPRESSION_SIZE + 1];
    let result = w.write(b"key", 0, &oversize);
    assert!(
        matches!(result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
        "expected DecompressedSizeTooLarge, got: {result:?}",
    );
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

/// A file compressed under a codec decodes back to what was written, frames
/// compression shrank stored compressed, the rest raw.
#[test]
#[cfg(feature = "lz4")]
fn an_lz4_file_decodes_to_what_was_written() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let mut w = writer(&folder)?.use_compression(CompressionType::Lz4);
    w.write(b"a", 0, &[b'x'; 60])?;
    w.write(b"b", 0, b"q8#vZ")?;
    let path = w.path.clone();
    let (meta, _, frames) = w.finish()?;
    assert_eq!(meta.compression, CompressionType::Lz4);
    let bytes = std::fs::read(&path)?;
    let block = decode_frame(
        &crate::Slice::from(&bytes[frames[0] as usize..frames[1] as usize]),
        CompressionType::Lz4,
        #[cfg(zstd_any)]
        None,
        &mut 0,
    )?;
    assert!(!block.header.is_raw(), "repetitive bytes compress");
    assert_eq!(&*block.value(0).expect("listed"), &[b'x'; 60][..]);
    assert_eq!(&*block.value(1).expect("listed"), b"q8#vZ");
    Ok(())
}

/// A dictionary codec without its dictionary fails where the frame is
/// compressed: at the write of a value with a frame of its own, at the close
/// of the block a small value joined.
#[test]
#[cfg(zstd_any)]
fn blob_write_zstd_dict_no_dict_returns_mismatch() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let dict = crate::compression::ZstdDictionary::new(b"test dictionary content for blob");
    let compression = CompressionType::ZstdDict {
        level: 3,
        dict_id: dict.id(),
    };
    let mismatch =
        |result: crate::Result<()>| matches!(result, Err(crate::Error::ZstdDictMismatch { .. }));

    let mut w = Writer::new(folder.path().join("single"), 0, 0, &StdFs)?
        .use_frame_sizes(SIZES)
        .use_compression(compression);
    assert!(
        mismatch(w.write(b"key", 0, &[1; 150]).map(|_| ())),
        "a frame of its own"
    );

    let mut w = Writer::new(folder.path().join("block"), 0, 0, &StdFs)?
        .use_frame_sizes(SIZES)
        .use_compression(compression);
    w.write(b"key", 0, b"value")?;
    assert!(
        mismatch(w.finish().map(|_| ())),
        "the block, when it closes"
    );
    Ok(())
}

#[test]
#[cfg(zstd_any)]
fn blob_write_zstd_dict_with_dict_succeeds() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let dict = crate::compression::ZstdDictionary::new(b"test dictionary content for blob");
    let compression = CompressionType::ZstdDict {
        level: 3,
        dict_id: dict.id(),
    };
    let mut w = Writer::new(folder.path().join("blob"), 0, 0, &StdFs)?
        .use_compression(compression)
        .use_zstd_dictionary(Some(alloc::sync::Arc::new(dict)));
    w.write(b"key", 0, b"hello world blob value")?;
    w.finish()?;
    Ok(())
}

/// Under a dictionary codec only the classes the dictionary applies to are
/// compressed with it; the frames of the others say they are compressed
/// without it, and every frame decodes under the file's codec and dictionary.
#[cfg(zstd_any)]
#[test]
fn a_dictionary_compresses_only_its_classes() -> crate::Result<()> {
    use crate::vlog::blob_file::frame::FLAG_PLAIN;

    let folder = tempfile::tempdir()?;
    let dict = alloc::sync::Arc::new(crate::compression::ZstdDictionary::new(
        &b"abcde".repeat(200),
    ));
    let compression = CompressionType::ZstdDict {
        level: 3,
        dict_id: dict.id(),
    };
    let mut w = writer(&folder)?
        .use_compression(compression)
        .use_zstd_dictionary(Some(alloc::sync::Arc::clone(&dict)))
        .use_dictionary_classes(crate::config::DictionaryClasses {
            blocks: true,
            singles: false,
            chunks: false,
        });
    let value = |len: usize| b"abcde".repeat(len / 5);
    w.write(b"a", 0, &value(40))?;
    w.write(b"b", 0, &value(150))?;
    w.write(b"c", 0, &value(700))?;
    let path = w.path.clone();
    let (_, _, frames) = w.finish()?;

    let bytes = std::fs::read(path)?;
    let decoded = decode_all(&bytes, &frames, compression, Some(&dict))?;
    let plain: Vec<_> = decoded
        .iter()
        .map(|f| (f.header.kind, f.header.flags & FLAG_PLAIN != 0))
        .collect();
    assert_eq!(
        plain,
        [
            (FrameKind::Block, false),
            (FrameKind::Single, true),
            (FrameKind::Head, false),
            (FrameKind::Chunk, true),
            (FrameKind::Chunk, true),
            (FrameKind::Chunk, true),
        ]
    );
    assert_eq!(&*decoded[1].payload, &value(150)[..]);
    Ok(())
}

/// The frame index decodes to the starts and the end it was written with,
/// and refuses a tampered, short or non-increasing one.
#[test]
fn the_frame_index_round_trips_and_refuses_damage() {
    let encoded = encode_frame_index(&[10, 60, 200], 450);
    assert_eq!(
        &*decode_frame_index(&encoded).expect("decodes"),
        &[10, 60, 200, 450]
    );

    let mut flipped = encoded.clone();
    flipped[6] ^= 1;
    assert!(decode_frame_index(&flipped).is_err(), "a flipped bit");
    assert!(
        decode_frame_index(&encoded[..encoded.len() - 1]).is_err(),
        "short"
    );
    assert!(
        decode_frame_index(&encode_frame_index(&[10, 10], 20)).is_err(),
        "starts that do not increase"
    );
    assert!(
        decode_frame_index(&encode_frame_index(&[10, 60], 60)).is_err(),
        "an end at the last start"
    );
    assert_eq!(
        &*decode_frame_index(&encode_frame_index(&[], 0)).expect("empty"),
        &[0]
    );
}

/// Runs every task on a thread of its own.
#[cfg(any(feature = "lz4", zstd_any))]
struct ThreadSpawner;

#[cfg(any(feature = "lz4", zstd_any))]
impl crate::table::writer::CompactionSpawner for ThreadSpawner {
    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        std::thread::spawn(task);
    }
}

/// Chunks compressed on workers come out as the writer's thread writes them
/// one after another: the same frames in the same order, so the file's data
/// is the same bytes.
#[cfg(any(feature = "lz4", zstd_any))]
#[test]
fn chunks_compressed_on_workers_are_the_frames_written_serially() -> crate::Result<()> {
    #[cfg(feature = "lz4")]
    let codec = CompressionType::Lz4;
    #[cfg(not(feature = "lz4"))]
    let codec = CompressionType::Zstd(3);
    // Twenty chunks of varied bytes, more than the workers hold in flight.
    let value: Vec<u8> = (0..20 * 300_u32).map(|i| (i * 7 % 13) as u8).collect();
    let data = |parallel: Option<crate::table::writer::ParallelCompression>| {
        let folder = tempfile::tempdir()?;
        let mut w = writer(&folder)?
            .use_compression(codec)
            .use_parallel_compression(parallel);
        w.write(b"a", 0, b"small")?;
        w.write(b"b", 0, &value)?;
        w.write(b"c", 0, &value[..900])?;
        let path = w.path.clone();
        let (_, _, frames) = w.finish()?;
        let bytes = std::fs::read(path)?;
        let end = frames.last().copied().unwrap_or(0) as usize;
        Ok::<_, crate::Error>((bytes[..end].to_vec(), frames))
    };
    let serial = data(None)?;
    let parallel = data(Some(crate::table::writer::ParallelCompression {
        spawner: alloc::sync::Arc::new(ThreadSpawner),
        threads: 3,
        inline_below: None,
    }))?;
    assert_eq!(serial.1, parallel.1, "the same frames at the same offsets");
    assert!(serial.0 == parallel.0, "the same data bytes");
    Ok(())
}

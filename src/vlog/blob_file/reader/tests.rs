use super::*;
use crate::vlog::blob_file::writer::FrameSizes;
use crate::{CompressionType, SequenceNumberCounter, fs::StdFs};
use std::fs::File;
use std::sync::Arc;
use test_log::test;

/// Small sizes, so a test reaches every class with a few bytes.
const SIZES: FrameSizes = FrameSizes {
    block_target: 100,
    chunk_size: 300,
};

/// Writes `values` into one blob file under `compression` and returns the
/// file with the handle of each value.
fn write(
    folder: &tempfile::TempDir,
    compression: CompressionType,
    values: &[(&[u8], &[u8])],
) -> crate::Result<(BlobFile, Vec<ValueHandle>)> {
    let mut writer = crate::vlog::BlobFileWriter::new(
        SequenceNumberCounter::default(),
        folder.path(),
        0,
        None,
        Arc::new(StdFs),
    )?
    .use_target_size(u64::MAX)
    .use_frame_sizes(SIZES)
    .use_compression(compression);
    let handles = values
        .iter()
        .map(|(key, value)| writer.write(key, 0, value).map(|ind| ind.vhandle))
        .collect::<crate::Result<Vec<_>>>()?;
    let mut files = writer.finish()?;
    Ok((files.remove(0), handles))
}

/// One value of each class.
fn one_of_each() -> [(&'static [u8], Vec<u8>); 4] {
    [
        (b"a", b"small".to_vec()),
        (b"b", b"also small".to_vec()),
        (b"c", vec![b'c'; 150]),
        (b"d", (0..700_u32).map(|i| (i % 251) as u8).collect()),
    ]
}

fn round_trip(compression: CompressionType) -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let values = one_of_each();
    let refs: Vec<(&[u8], &[u8])> = values.iter().map(|(k, v)| (*k, v.as_slice())).collect();
    let (blob_file, handles) = write(&folder, compression, &refs)?;
    let file = File::open(&blob_file.0.path)?;
    let reader = Reader::new(&blob_file, &file);
    for ((key, value), handle) in values.iter().zip(&handles) {
        assert_eq!(&*reader.get(key, handle)?, value.as_slice(), "{key:?}");
    }
    Ok(())
}

/// Every class reads back through its handle, uncompressed.
#[test]
fn every_class_round_trips() -> crate::Result<()> {
    round_trip(CompressionType::None)
}

/// Every class reads back through its handle under lz4.
#[test]
#[cfg(feature = "lz4")]
fn every_class_round_trips_under_lz4() -> crate::Result<()> {
    round_trip(CompressionType::Lz4)
}

/// Every class reads back through its handle under zstd.
#[test]
#[cfg(zstd_any)]
fn every_class_round_trips_under_zstd() -> crate::Result<()> {
    round_trip(CompressionType::Zstd(3))
}

/// A handle checks its key against the one its frame lists at its index: a
/// different key, of the same or another length, or an index past the list
/// is refused, never served the value there.
#[test]
fn a_value_is_served_only_to_its_key() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let (blob_file, handles) = write(
        &folder,
        CompressionType::None,
        &[(b"aaa", b"one"), (b"aab", b"two")],
    )?;
    let file = File::open(&blob_file.0.path)?;
    let reader = Reader::new(&blob_file, &file);
    assert_eq!(&*reader.get(b"aab", &handles[1])?, b"two");
    for key in [&b"aaa"[..], b"aa", b"aabb"] {
        let result = reader.get(key, &handles[1]);
        assert!(
            matches!(result, Err(crate::Error::InvalidHeader("Blob"))),
            "{key:?}: {result:?}"
        );
    }
    let past = ValueHandle {
        index: 2,
        ..handles[1]
    };
    assert!(matches!(
        reader.get(b"aab", &past),
        Err(crate::Error::InvalidHeader("Blob"))
    ));
    Ok(())
}

/// A handle naming a frame the file does not have is refused.
#[test]
fn a_frame_the_file_lacks_is_refused() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let (blob_file, handles) = write(&folder, CompressionType::None, &[(b"k", b"v")])?;
    let file = File::open(&blob_file.0.path)?;
    let reader = Reader::new(&blob_file, &file);
    let missing = ValueHandle {
        frame: 9,
        ..handles[0]
    };
    assert!(matches!(
        reader.get(b"k", &missing),
        Err(crate::Error::InvalidHeader("Blob"))
    ));
    Ok(())
}

/// A byte flipped on disk in a frame, wherever it is, is caught before any
/// value is served: by the header CRC in the header, by the checksum past it.
#[test]
fn damage_on_disk_is_caught() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let (blob_file, handles) = write(&folder, CompressionType::None, &[(b"key", b"payload_data")])?;
    let (start, end) = blob_file.frame_span(0).expect("one frame");
    let pristine = std::fs::read(&blob_file.0.path)?;
    for at in start..end {
        let mut raw = pristine.clone();
        raw[at as usize] ^= 0x40;
        std::fs::write(&blob_file.0.path, &raw)?;
        let file = File::open(&blob_file.0.path)?;
        let result = Reader::new(&blob_file, &file).get(b"key", &handles[0]);
        assert!(
            matches!(
                result,
                Err(crate::Error::HeaderCrcMismatch { .. }
                    | crate::Error::ChecksumMismatch { .. }
                    | crate::Error::InvalidHeader(_))
            ),
            "byte {} of the frame: {result:?}",
            at - start
        );
    }
    Ok(())
}

/// The chunks a range of a chunked value covers, and where it starts in the
/// first of them.
#[test]
fn a_range_covers_only_its_chunks() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let value: Vec<u8> = (0..700_u32).map(|i| (i % 251) as u8).collect();
    let (blob_file, handles) = write(&folder, CompressionType::None, &[(b"big", &value)])?;
    let file = File::open(&blob_file.0.path)?;
    let reader = Reader::new(&blob_file, &file);
    let head = reader.read_frame(handles[0].frame, &mut 0)?;

    assert_eq!(covering_chunks(&head, &(0..700))?, (0..3, 0));
    assert_eq!(covering_chunks(&head, &(310..320))?, (1..2, 10));
    assert_eq!(covering_chunks(&head, &(299..301))?, (0..2, 299));
    assert_eq!(covering_chunks(&head, &(600..700))?, (2..3, 0));

    let chunks = chunk_frames(&head, handles[0].frame)?;
    let second = reader.read_frame(chunks.start + 1, &mut 0)?;
    let part = assemble(&head, &[Box::new(second)], Some(&(310..320)))?;
    assert_eq!(&*part, &value[310..320]);
    Ok(())
}

/// Chunks that are not the head's are refused: of another size than its
/// table gives them, or not chunks at all.
#[test]
fn chunks_must_be_the_heads() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let value = vec![9_u8; 700];
    let (blob_file, handles) = write(
        &folder,
        CompressionType::None,
        &[(b"big", &value), (b"z", b"tail")],
    )?;
    let file = File::open(&blob_file.0.path)?;
    let reader = Reader::new(&blob_file, &file);
    let head = reader.read_frame(handles[0].frame, &mut 0)?;
    let first = reader.read_frame(handles[0].frame + 1, &mut 0)?;
    let last = reader.read_frame(handles[0].frame + 3, &mut 0)?;
    let tail = reader.read_frame(handles[1].frame, &mut 0)?;

    // The last chunk (100 bytes) where the first (300) belongs.
    assert!(assemble(&head, &[Box::new(last)], Some(&(0..10))).is_err());
    // A block where a chunk belongs.
    assert!(assemble(&head, &[Box::new(tail)], Some(&(0..10))).is_err());
    // Too few chunks for the range.
    assert!(assemble(&head, &[Box::new(first)], Some(&(0..400))).is_err());
    Ok(())
}

/// A copy takes a value with frames of its own as those frames, each one
/// checked, and a value out of a block as the value; the bytes it reports
/// read are what it read.
#[test]
fn a_copy_takes_frames_or_the_value() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let values = one_of_each();
    let refs: Vec<(&[u8], &[u8])> = values.iter().map(|(k, v)| (*k, v.as_slice())).collect();
    let (blob_file, handles) = write(&folder, CompressionType::None, &refs)?;
    let file = File::open(&blob_file.0.path)?;
    let reader = Reader::new(&blob_file, &file);

    let (small, read) = reader.copy_source(b"a", &handles[0])?;
    assert!(matches!(&small, CopySource::Value(v) if &**v == b"small"));
    let (start, end) = blob_file.frame_span(handles[0].frame).expect("frame");
    assert_eq!(read, end - start, "the block was read");

    let (single, _) = reader.copy_source(b"c", &handles[2])?;
    assert!(matches!(&single, CopySource::Frames { frames, value_len: 150 } if frames.len() == 1));

    let (chunked, read) = reader.copy_source(b"d", &handles[3])?;
    let CopySource::Frames { frames, value_len } = chunked else {
        panic!("a chunked value is copied by its frames");
    };
    assert_eq!(
        (frames.len(), value_len),
        (4, 700),
        "a head and three chunks"
    );
    assert_eq!(read, frames.iter().map(|f| f.len() as u64).sum::<u64>());

    assert!(
        reader.copy_source(b"x", &handles[3]).is_err(),
        "another key"
    );
    Ok(())
}

/// A writer that compressed against a dictionary produces a handle carrying it,
/// so the read needs nothing supplied: it just used those bytes, and they are
/// the file's by construction.
#[test]
#[cfg(zstd_any)]
fn blob_reader_reads_a_written_dictionary_blob_with_nothing_supplied() -> crate::Result<()> {
    use crate::compression::ZstdDictionary;

    let folder = tempfile::tempdir()?;
    let dict = ZstdDictionary::new(b"some_dictionary_content_for_testing");
    let compression = CompressionType::ZstdDict {
        level: 3,
        dict_id: dict.id(),
    };
    let mut writer = crate::vlog::BlobFileWriter::new(
        SequenceNumberCounter::default(),
        folder.path(),
        0,
        None,
        Arc::new(StdFs),
    )?
    .use_target_size(u64::MAX)
    .use_compression(compression)
    .use_zstd_dictionary(Some(Arc::new(dict)));

    let handle = writer
        .write(b"key", 0, b"value-to-compress-with-dict")?
        .vhandle;
    let blob_file = writer.finish()?;
    let blob_file = blob_file.first().expect("one blob file written");

    let file = File::open(&blob_file.0.path)?;
    // The reader takes no dictionary at all: the file's handle carries it.
    let value = Reader::new(blob_file, &file).get(b"key", &handle)?;
    assert_eq!(&*value, b"value-to-compress-with-dict");
    Ok(())
}

/// The reader takes no dictionary: a blob file carries the one its own
/// descriptor names, pinned when the handle was built. A handle built while the
/// tree held that dictionary decodes; one built while it did not reports the id
/// as missing, and neither answer can change afterwards.
#[test]
#[cfg(zstd_any)]
fn blob_reader_uses_the_dictionary_pinned_on_its_file() -> crate::Result<()> {
    use crate::compression::{ZstdDictionaries, ZstdDictionary};

    let folder = tempfile::tempdir()?;

    let dict_a = ZstdDictionary::new(b"dictionary_a_content_for_testing");
    let dict_b = ZstdDictionary::new(b"dictionary_b_entirely_different_content");
    let compression = CompressionType::ZstdDict {
        level: 3,
        dict_id: dict_a.id(),
    };
    let dict_a_arc = Arc::new(dict_a);
    let dict_b_arc = Arc::new(dict_b);

    // Written by a tree that holds only the newer B alongside the A it
    // compresses with: the handle pins A, because A is what the file records.
    let both = ZstdDictionaries::new()
        .with(dict_b_arc.clone())
        .with(dict_a_arc.clone());
    let mut writer = crate::vlog::BlobFileWriter::new(
        SequenceNumberCounter::default(),
        folder.path(),
        0,
        None,
        Arc::new(StdFs),
    )?
    .use_target_size(u64::MAX)
    .use_compression(compression)
    .use_zstd_dictionary(Some(dict_a_arc))
    .use_zstd_dictionaries(both);

    // Compressible, so its frame is stored compressed: a frame compression
    // left raw decodes without any dictionary, rightly.
    let value = b"value-compressed-with-dict-a ".repeat(20);
    let handle = writer.write(b"key", 0, &value)?.vhandle;
    let blob_file = writer.finish()?;
    let blob_file = blob_file.first().expect("one blob file written");

    let file = File::open(&blob_file.0.path)?;
    assert_eq!(
        &*Reader::new(blob_file, &file).get(b"key", &handle)?,
        value.as_slice()
    );

    // The same file recovered by a tree that does NOT hold A pins nothing, and
    // says which id it wanted rather than decoding against something else.
    let only_b = ZstdDictionaries::new().with(dict_b_arc);
    let orphaned = crate::vlog::recover_blob_file(
        &blob_file.0.path,
        blob_file.id(),
        crate::Checksum::from_raw(0),
        0,
        &(Arc::new(StdFs) as Arc<dyn crate::fs::Fs>),
        &only_b,
    )?;
    let result = Reader::new(&orphaned, &file).get(b"key", &handle);
    assert!(
        matches!(
            result,
            Err(crate::Error::ZstdDictMismatch { got: None, .. })
        ),
        "a tree without the recorded dictionary must report it missing; got: {result:?}",
    );
    Ok(())
}

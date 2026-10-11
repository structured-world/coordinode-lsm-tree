// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::BlockStream;
use crate::fs::{Fs, FsOpenOptions, StdFs};
use crate::table::util::TimedFile;
use alloc::sync::Arc;
use std::io::Read;

/// Byte `i` of the stream fixtures.
fn pattern(len: usize) -> Vec<u8> {
    (0..=250u8).cycle().take(len).collect()
}

/// A stream over a file holding `bytes`, read `readahead` bytes at a time.
fn stream_over(bytes: &[u8], readahead: usize) -> crate::Result<(tempfile::TempDir, BlockStream)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("table");
    std::fs::write(&path, bytes)?;
    let fs: Arc<dyn Fs> = Arc::new(StdFs);
    let file = fs.open(&path, &FsOpenOptions::new().read(true))?;
    let stream = BlockStream::new(TimedFile::new(file), 0, bytes.len() as u64, readahead);
    Ok((dir, stream))
}

/// Blocks inside one chunk are views of it, laid out as in the file, not
/// copies; a block reaching past its chunk starts the next one, and a block
/// longer than the readahead gets a chunk of its own length.
#[test]
fn views_share_their_chunk_and_a_straddling_block_starts_the_next() -> crate::Result<()> {
    let bytes = pattern(1_000);
    let (_dir, mut stream) = stream_over(&bytes, 300)?;

    let first = stream.view(100)?;
    let second = stream.view(100)?;
    assert_eq!(&*first, &bytes[..100]);
    assert_eq!(&*second, &bytes[100..200]);
    assert_eq!(
        second.as_ptr(),
        first.as_ptr().wrapping_add(100),
        "two blocks of one chunk are views of it, not copies"
    );

    let straddling = stream.view(150)?;
    assert_eq!(&*straddling, &bytes[200..350]);
    assert_eq!(
        stream.chunk.len(),
        300,
        "the next chunk, read at the readahead"
    );

    let long = stream.view(500)?;
    assert_eq!(&*long, &bytes[350..850]);

    let mut rest = vec![0; 150];
    stream.read_exact(&mut rest)?;
    assert_eq!(rest, &bytes[850..]);
    assert!(stream.view(1).is_err(), "nothing is left to read");
    Ok(())
}

/// A file shorter than the readahead is read whole and no further: a small
/// table pays for its own length, not the readahead's.
#[test]
fn a_chunk_is_no_longer_than_the_file() -> crate::Result<()> {
    let bytes = pattern(50);
    let (_dir, mut stream) = stream_over(&bytes, 1_000)?;
    assert_eq!(&*stream.view(10)?, &bytes[..10]);
    assert_eq!(stream.chunk.len(), 50);
    assert_eq!(&*stream.view(40)?, &bytes[10..]);
    Ok(())
}

/// Small reads, as a block header is read, cross chunks without losing or
/// repeating a byte.
#[test]
fn small_reads_cross_chunks() -> crate::Result<()> {
    let bytes = pattern(1_000);
    let (_dir, mut stream) = stream_over(&bytes, 64)?;
    let mut read = Vec::new();
    let mut piece = [0u8; 7];
    while read.len() + piece.len() <= bytes.len() {
        stream.read_exact(&mut piece)?;
        read.extend_from_slice(&piece);
    }
    assert_eq!(read, &bytes[..read.len()]);
    Ok(())
}

/// A `len`-byte value drawn from `seed` that does not compress: a compressed
/// table then stays as large as an uncompressed one.
fn noise(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed;
    core::iter::repeat_with(|| {
        // splitmix64
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        (z ^ (z >> 31)).to_le_bytes()
    })
    .flatten()
    .take(len)
    .collect()
}

/// A pacer that paces every read.
struct EveryRead;

impl crate::table::util::ReadPacer for EveryRead {
    fn active(&self) -> bool {
        true
    }

    fn pace(&self, _offset: u64, _len: u64) {}
}

/// A table past the readahead, its blocks crossing chunk boundaries, scans to
/// the same entries in the same order as an indexed read, compressed or not,
/// with reads paced or not.
#[test]
fn a_scan_returns_what_an_indexed_read_does() -> crate::Result<()> {
    use crate::config::{BlockSizePolicy, CompressionPolicy};
    use crate::{AbstractTree, AnyTree, Config, SequenceNumberCounter};

    // About 5 MB of entries: more than two chunks.
    const ENTRIES: u32 = 16_000;

    let codecs = [
        crate::CompressionType::None,
        #[cfg(feature = "lz4")]
        crate::CompressionType::Lz4,
    ];
    for codec in codecs {
        let folder = tempfile::tempdir()?;
        let AnyTree::Standard(tree) = Config::new(
            folder.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .data_block_compression_policy(CompressionPolicy::all(codec))
        // An odd block size: the blocks cross the readahead's boundaries.
        .data_block_size_policy(BlockSizePolicy::all(3_001))
        .table_target_size(64 * 1_024 * 1_024)
        .open()?
        else {
            panic!("expected a standard tree");
        };
        for i in 0..ENTRIES {
            let value = noise(u64::from(i), 300);
            tree.insert(format!("key{i:06}"), value, u64::from(i));
        }
        tree.flush_active_memtable(0)?;

        let version = tree.current_version();
        let tables: Vec<_> = version.iter_tables().cloned().collect();
        assert_eq!(tables.len(), 1, "one table");
        let table = &tables[0];
        assert!(
            table.file_size() > 2 * super::SCANNER_READAHEAD_BYTES as u64,
            "the table spans several chunks: {} bytes",
            table.file_size()
        );

        // Keys and values both: an entry's equality looks at its key only.
        let entries = |iter: &mut dyn Iterator<Item = crate::Result<crate::InternalValue>>| {
            iter.map(|entry| entry.map(|entry| (entry.key, entry.value)))
                .collect::<crate::Result<Vec<_>>>()
        };
        let indexed = entries(&mut table.iter())?;
        assert_eq!(indexed.len(), ENTRIES as usize);
        assert_eq!(entries(&mut table.scan()?)?, indexed, "{codec:?}");
        let pacer: crate::table::util::Pacer = Arc::new(EveryRead);
        assert_eq!(
            entries(&mut table.scan_paced(Some(&pacer))?)?,
            indexed,
            "{codec:?}, paced"
        );
    }
    Ok(())
}

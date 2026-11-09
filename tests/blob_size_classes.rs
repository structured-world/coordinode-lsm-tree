// What each size class of a separated value costs to store and to read: a
// small value shares a block, a medium one has a frame of its own, a large one
// is split into chunks.

use lsm_tree::{
    AbstractTree, AnyTree, BlobTree, CompressionType, Config, KvSeparationOptions, SeqNo,
    SequenceNumberCounter, config::BlobCacheFill, get_tmp_folder,
};

/// `len` bytes of an xorshift stream seeded by `seed`, which no codec shrinks.
fn random(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

/// A blob tree separating values from 1 KiB, its blob files compressed with
/// `compression` and its blob frames cached as `fill` says.
fn open(
    path: &std::path::Path,
    compression: CompressionType,
    fill: BlobCacheFill,
) -> lsm_tree::Result<BlobTree> {
    let tree = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(1_024),
    ))
    .blob_compression(compression)
    .blob_cache_fill(fill)
    .use_cache(std::sync::Arc::new(lsm_tree::Cache::with_capacity_bytes(
        64 << 20,
    )))
    .open()?;
    let AnyTree::Blob(tree) = tree else {
        panic!("a tree with kv separation opens as a blob tree");
    };
    Ok(tree)
}

/// Bytes of every blob file of `tree`.
fn blob_bytes(tree: &BlobTree) -> u64 {
    tree.current_version()
        .blob_files
        .iter()
        .map(|file| {
            std::fs::metadata(file.path())
                .expect("blob file metadata")
                .len()
        })
        .sum()
}

/// The codecs compiled in, each a compressed blob arm.
fn codecs() -> Vec<CompressionType> {
    vec![
        #[cfg(feature = "lz4")]
        CompressionType::Lz4,
        #[cfg(feature = "zstd")]
        CompressionType::Zstd(3),
    ]
}

/// Values a codec cannot shrink take the space they take uncompressed, in
/// every class: compression that saves nothing stores the frame raw, so a
/// compressed tree holds what an uncompressed one does, its frame headers
/// included, give or take the file's codec field.
#[test]
fn incompressible_values_are_stored_at_their_raw_size() -> lsm_tree::Result<()> {
    let write = |compression| -> lsm_tree::Result<u64> {
        let folder = get_tmp_folder();
        let tree = open(folder.path(), compression, BlobCacheFill::Never)?;
        for i in 0..8 {
            tree.insert(format!("block-{i}"), random(i, 2_000), 0);
        }
        tree.insert("single", random(100, 100_000), 0);
        tree.insert("chunked", random(200, 600_000), 0);
        tree.flush_active_memtable(0)?;
        Ok(blob_bytes(&tree))
    };
    let raw = write(CompressionType::None)?;
    let values = 8 * 2_000 + 100_000 + 600_000;
    assert!(raw >= values, "{raw} bytes hold {values} bytes of values");
    for codec in codecs() {
        let stored = write(codec)?;
        assert!(
            stored <= raw + 16,
            "{codec:?}: {stored} bytes for values stored raw in {raw}"
        );
    }
    Ok(())
}

/// A medium value has a frame of its own, so reading it decodes that value
/// and nothing beside it, compressed or not.
#[cfg(feature = "metrics")]
#[test]
fn a_medium_value_decodes_only_itself() -> lsm_tree::Result<()> {
    let len = 100_000;
    for compression in std::iter::once(CompressionType::None).chain(codecs()) {
        let folder = get_tmp_folder();
        let tree = open(folder.path(), compression, BlobCacheFill::Never)?;
        // Compressible, so a compressed frame decodes to more than it reads.
        let value: Vec<u8> = (0..len).map(|i| (i % 7) as u8).collect();
        tree.insert("a", &value, 0);
        tree.insert("b", &value, 0);
        tree.flush_active_memtable(0)?;
        // The first read loads the table's blocks, so the second decodes its
        // value alone.
        assert_eq!(tree.get("a", SeqNo::MAX)?.as_deref(), Some(&*value));

        let m = tree.metrics();
        let before = m.bytes_decoded();
        assert_eq!(tree.get("b", SeqNo::MAX)?.as_deref(), Some(&*value));
        assert_eq!(
            m.bytes_decoded() - before,
            len as u64,
            "{compression:?}: a medium value of {len} bytes"
        );
    }
    Ok(())
}

/// A large value read with the blob cache filling is served from it the next
/// time: its chunks are cached one by one, each small enough for a shard.
#[cfg(feature = "metrics")]
#[test]
fn a_large_value_read_through_the_cache_is_served_from_it_again() -> lsm_tree::Result<()> {
    for compression in std::iter::once(CompressionType::None).chain(codecs()) {
        let folder = get_tmp_folder();
        let tree = open(folder.path(), compression, BlobCacheFill::Always)?;
        // Four chunks of the default 256 KiB.
        let value = random(7, 1 << 20);
        tree.insert("large", &value, 0);
        tree.flush_active_memtable(0)?;

        let m = tree.metrics();
        assert_eq!(tree.get("large", SeqNo::MAX)?.as_deref(), Some(&*value));
        let (read, requests) = (m.blob_bytes_read(), m.blob_read_count());
        assert!(
            read >= 1 << 20,
            "{compression:?}: the first read read {read}"
        );

        assert_eq!(tree.get("large", SeqNo::MAX)?.as_deref(), Some(&*value));
        assert_eq!(
            m.blob_bytes_read(),
            read,
            "{compression:?}: the repeat read asked the filesystem again"
        );
        assert_eq!(m.blob_read_count(), requests, "{compression:?}");
    }
    Ok(())
}

/// The byte at the middle of the only blob file, flipped.
fn damage_the_middle(tree: &BlobTree) -> lsm_tree::Result<()> {
    let version = tree.current_version();
    let file = version.blob_files.iter().next().expect("one blob file");
    let mut bytes = std::fs::read(file.path())?;
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xFF;
    std::fs::write(file.path(), &bytes)?;
    Ok(())
}

/// A damaged block costs the values it holds and no other: the block is one
/// frame with one checksum, and the values of the other blocks still read.
#[test]
fn a_damaged_block_costs_its_values_alone() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), CompressionType::None, BlobCacheFill::Never)?;
    let n = 400_u64;
    for i in 0..n {
        tree.insert(format!("k{i:04}"), random(i, 2_000), 0);
    }
    tree.flush_active_memtable(0)?;
    damage_the_middle(&tree)?;

    let mut failed = 0;
    for i in 0..n {
        match tree.get(format!("k{i:04}"), SeqNo::MAX) {
            Ok(got) => assert_eq!(got.as_deref(), Some(&*random(i, 2_000)), "k{i:04}"),
            Err(_) => failed += 1,
        }
    }
    // A 16 KiB block holds 7 values of 2,000 bytes and their keys.
    assert!(
        (1..=8).contains(&failed),
        "{failed} of {n} values failed for one damaged block"
    );
    let report = lsm_tree::verify::verify_integrity(&tree);
    assert!(
        matches!(
            report.errors.as_slice(),
            [lsm_tree::verify::IntegrityError::BlobFileCorrupted { .. }]
        ),
        "{:?}",
        report.errors
    );
    Ok(())
}

/// A damaged chunk fails the read of its value, and a range of the value
/// outside it still reads: each chunk is a frame with its own checksum.
#[test]
fn a_damaged_chunk_fails_its_value_and_spares_the_other_chunks() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), CompressionType::None, BlobCacheFill::Never)?;
    // Eight chunks of the default 256 KiB: the middle of the file is in one
    // of the middle chunks, away from the first and the last.
    let value = random(3, 8 * 256 * 1_024);
    tree.insert("large", &value, 0);
    tree.flush_active_memtable(0)?;
    damage_the_middle(&tree)?;

    assert!(tree.get("large", SeqNo::MAX).is_err());
    let head = tree
        .get_range("large", 0..1_000, SeqNo::MAX)?
        .expect("the key has a value");
    assert_eq!(&*head, &value[..1_000]);
    let tail = value.len() as u64 - 1_000..value.len() as u64;
    let got = tree
        .get_range("large", tail, SeqNo::MAX)?
        .expect("the key has a value");
    assert_eq!(&*got, &value[value.len() - 1_000..]);

    let report = lsm_tree::verify::verify_integrity(&tree);
    assert!(
        matches!(
            report.errors.as_slice(),
            [lsm_tree::verify::IntegrityError::BlobFileCorrupted { .. }]
        ),
        "{:?}",
        report.errors
    );
    Ok(())
}

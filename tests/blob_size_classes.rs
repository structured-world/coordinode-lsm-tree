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
/// `compression` and its blob frames cached as `fill` says. Its zstd files
/// build no dictionary of their own, so small values share blocks under
/// every codec; packing under a file's dictionary is tested apart.
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
        KvSeparationOptions::default()
            .separation_threshold(1_024)
            .file_dictionary(0),
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

/// A large value cached while the fill was `Always` is still served from the
/// cache after the fill is set to `Never` on the live tree: the setting stops
/// new fills, not lookups of what the cache holds.
#[cfg(feature = "metrics")]
#[test]
fn a_cached_large_value_is_served_from_the_cache_after_fills_stop() -> lsm_tree::Result<()> {
    for compression in std::iter::once(CompressionType::None).chain(codecs()) {
        let folder = get_tmp_folder();
        let tree = open(folder.path(), compression, BlobCacheFill::Always)?;
        let value = random(13, 1 << 20);
        tree.insert("large", &value, 0);
        tree.flush_active_memtable(0)?;
        assert_eq!(tree.get("large", SeqNo::MAX)?.as_deref(), Some(&*value));

        tree.update_runtime_config(|c| c.blob_cache_fill = BlobCacheFill::Never)?;
        let m = tree.metrics();
        let requests = m.blob_read_count();
        assert_eq!(tree.get("large", SeqNo::MAX)?.as_deref(), Some(&*value));
        assert_eq!(
            m.blob_read_count(),
            requests,
            "{compression:?}: the cached value was read from the filesystem again"
        );
    }
    Ok(())
}

/// A large value read whole without the cache comes back as written under
/// every codec, compressible chunks decompressed into place and incompressible
/// ones read straight in, each byte decoded.
#[cfg(feature = "metrics")]
#[test]
fn a_large_value_read_whole_is_built_in_place() -> lsm_tree::Result<()> {
    for compression in std::iter::once(CompressionType::None).chain(codecs()) {
        let folder = get_tmp_folder();
        let tree = open(folder.path(), compression, BlobCacheFill::Never)?;
        // Half compressible, half not: both kinds of chunk in one value.
        let mut value: Vec<u8> = (0..600_000_u32).map(|i| (i % 7) as u8).collect();
        value.extend(random(5, 600_000));
        tree.insert("large", &value, 0);
        tree.flush_active_memtable(0)?;

        let m = tree.metrics();
        let before = m.bytes_decoded();
        assert_eq!(
            tree.get("large", SeqNo::MAX)?.as_deref(),
            Some(&*value),
            "{compression:?}"
        );
        assert!(
            m.bytes_decoded() - before >= value.len() as u64,
            "{compression:?}: every byte of the value was decoded once"
        );
    }
    Ok(())
}

/// A large value read alone without the cache is one request of the
/// filesystem, its head and its chunks together, under every codec: read
/// apart, the head's small read starts the kernel's readahead small and the
/// chunks after it come in one growing window after another, which made a
/// cold 4 MiB value three times slower than a frame of its own.
#[cfg(feature = "metrics")]
#[test]
fn a_large_value_read_alone_is_one_request() -> lsm_tree::Result<()> {
    for compression in std::iter::once(CompressionType::None).chain(codecs()) {
        let folder = get_tmp_folder();
        let tree = open(folder.path(), compression, BlobCacheFill::Never)?;
        let mut value: Vec<u8> = (0..600_000_u32).map(|i| (i % 7) as u8).collect();
        value.extend(random(9, 600_000));
        tree.insert("large", &value, 0);
        tree.flush_active_memtable(0)?;

        let m = tree.metrics();
        let before = m.blob_read_count();
        assert_eq!(
            tree.get("large", SeqNo::MAX)?.as_deref(),
            Some(&*value),
            "{compression:?}"
        );
        assert_eq!(
            m.blob_read_count() - before,
            1,
            "{compression:?}: the head and the chunks in one request"
        );
    }
    Ok(())
}

/// A block of small values a point read decodes is cached like a data block,
/// whatever the blob cache fill says: the values beside the one read are read
/// from the cache, by `get` and by `multi_get` alike, not decoded again per
/// value. A value of a frame of its own still follows the fill.
#[cfg(feature = "metrics")]
#[test]
fn a_block_a_point_read_decodes_serves_its_other_values() -> lsm_tree::Result<()> {
    for compression in codecs() {
        let folder = get_tmp_folder();
        let tree = open(folder.path(), compression, BlobCacheFill::Never)?;
        // Twenty values of 2,000 bytes, four to an 8 KiB block: five blocks.
        for i in 0..20 {
            tree.insert(format!("k{i:02}"), random(i, 2_000), 0);
        }
        tree.insert("single", random(99, 100_000), 0);
        tree.flush_active_memtable(0)?;

        let m = tree.metrics();
        for i in 0..20 {
            assert_eq!(
                tree.get(format!("k{i:02}"), SeqNo::MAX)?.as_deref(),
                Some(&*random(i, 2_000))
            );
        }
        let requests = m.blob_read_count();
        assert!(
            requests <= 5,
            "{compression:?}: {requests} reads for five blocks"
        );
        let keys: Vec<String> = (0..20).map(|i| format!("k{i:02}")).collect();
        let got = tree.multi_get(&keys, SeqNo::MAX)?;
        assert!(got.iter().all(Option::is_some));
        assert_eq!(
            m.blob_read_count(),
            requests,
            "{compression:?}: the cached blocks serve the batch"
        );

        let _ = tree.get("single", SeqNo::MAX)?;
        let once = m.blob_read_count();
        let _ = tree.get("single", SeqNo::MAX)?;
        assert_eq!(
            m.blob_read_count(),
            once + 1,
            "{compression:?}: a value of its own is not cached under Never"
        );
    }
    Ok(())
}

/// Under zstd and the default options a blob file builds its own dictionary
/// and packs its small values: a cold point read of a value reads its record
/// alone, and not the 8 KiB block of its neighbours a file without a
/// dictionary would read and decode. Incompressible values keep the block
/// and the record at their raw sizes, so the bytes read tell them apart.
#[cfg(all(feature = "metrics", feature = "zstd"))]
#[test]
fn a_zstd_file_reads_a_small_value_alone_by_default() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let AnyTree::Blob(tree) = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(1_024),
    ))
    .blob_compression(CompressionType::Zstd(3))
    .blob_cache_fill(BlobCacheFill::Never)
    .use_cache(std::sync::Arc::new(lsm_tree::Cache::with_capacity_bytes(
        64 << 20,
    )))
    .open()?
    else {
        panic!("a tree with kv separation opens as a blob tree");
    };
    // 400 KB, well past the default dictionary's 64 KiB of samples.
    for i in 0..200 {
        tree.insert(format!("k{i:03}"), random(i, 2_000), 0);
    }
    tree.flush_active_memtable(0)?;

    let m = tree.metrics();
    let (reads, bytes) = (m.blob_read_count(), m.blob_bytes_read());
    assert_eq!(
        tree.get("k150", SeqNo::MAX)?.as_deref(),
        Some(&*random(150, 2_000))
    );
    assert_eq!(m.blob_read_count(), reads + 1, "one read for the value");
    let read = m.blob_bytes_read() - bytes;
    assert!(
        read < 2 * 2_000,
        "{read} bytes read for a value of 2000: its record alone"
    );
    Ok(())
}

/// In a file that compresses nothing a small value is a packed record: a
/// batch and a point read read it alone, a request of its record's bytes.
/// Under `Never` a cache that has not filled keeps none of them, so a read
/// again reads its record again.
#[cfg(feature = "metrics")]
#[test]
fn a_packed_value_is_read_as_its_record_alone() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let cache = std::sync::Arc::new(lsm_tree::Cache::with_capacity_bytes(64 << 20));
    let AnyTree::Blob(tree) = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(1_024),
    ))
    .blob_compression(CompressionType::None)
    .blob_cache_fill(BlobCacheFill::Never)
    .use_cache(std::sync::Arc::clone(&cache))
    .open()?
    else {
        panic!("a tree with kv separation opens as a blob tree");
    };
    for i in 0..20 {
        tree.insert(format!("k{i:02}"), random(i, 2_000), 0);
    }
    tree.flush_active_memtable(0)?;
    let keys: Vec<String> = (0..20).map(|i| format!("k{i:02}")).collect();
    let m = tree.metrics();

    let got = tree.multi_get(&keys, SeqNo::MAX)?;
    for (i, value) in got.iter().enumerate() {
        assert_eq!(value.as_deref(), Some(&*random(i as u64, 2_000)));
    }
    assert_eq!(m.blob_read_count(), 20, "one record read per value");
    // The record's fixed part and its three-byte key beside the value.
    assert_eq!(m.blob_bytes_read(), 20 * (27 + 3 + 2_000));

    for (i, key) in keys.iter().enumerate() {
        assert_eq!(
            tree.get(key, SeqNo::MAX)?.as_deref(),
            Some(&*random(i as u64, 2_000))
        );
    }
    assert_eq!(
        m.blob_read_count(),
        40,
        "each read again reads its record again"
    );
    assert_eq!(m.blob_bytes_read(), 40 * (27 + 3 + 2_000));
    Ok(())
}

/// Under `Always` a packed value a batch read is cached as its record, and a
/// batch again takes it from the cache: no request of the filesystem.
#[cfg(feature = "metrics")]
#[test]
fn a_cached_packed_value_serves_a_batch_again() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), CompressionType::None, BlobCacheFill::Always)?;
    for i in 0..20 {
        tree.insert(format!("k{i:02}"), random(i, 2_000), 0);
    }
    tree.flush_active_memtable(0)?;
    let keys: Vec<String> = (0..20).map(|i| format!("k{i:02}")).collect();
    let m = tree.metrics();

    for _ in 0..2 {
        let got = tree.multi_get(&keys, SeqNo::MAX)?;
        for (i, value) in got.iter().enumerate() {
            assert_eq!(value.as_deref(), Some(&*random(i as u64, 2_000)));
        }
    }
    assert_eq!(
        m.blob_read_count(),
        20,
        "the second batch is served from the cached records"
    );
    Ok(())
}

/// A batch whose values share blocks reads each block once, even when none
/// is cached yet.
#[cfg(feature = "metrics")]
#[test]
fn a_batch_reads_each_block_it_needs_once() -> lsm_tree::Result<()> {
    for compression in codecs() {
        let folder = get_tmp_folder();
        let tree = open(folder.path(), compression, BlobCacheFill::Never)?;
        for i in 0..20 {
            tree.insert(format!("k{i:02}"), random(i, 2_000), 0);
        }
        tree.flush_active_memtable(0)?;

        let m = tree.metrics();
        let keys: Vec<String> = (0..20).map(|i| format!("k{i:02}")).collect();
        let got = tree.multi_get(&keys, SeqNo::MAX)?;
        for (i, value) in got.iter().enumerate() {
            assert_eq!(value.as_deref(), Some(&*random(i as u64, 2_000)));
        }
        let decoded_blocks = m.blob_read_count();
        assert!(
            decoded_blocks <= 5,
            "{compression:?}: {decoded_blocks} reads for five blocks"
        );
    }
    Ok(())
}

/// A dictionary that applies to blocks only leaves the other classes
/// compressed without it, and every value reads back, after a reopen too,
/// whichever way its frame was compressed.
#[cfg(feature = "zstd")]
#[test]
fn a_dictionary_of_one_class_reads_back_every_class() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let dictionary = std::sync::Arc::new(lsm_tree::ZstdDictionary::new(
        &b"a sample of the values a dictionary learns from ".repeat(64),
    ));
    let compression = CompressionType::ZstdDict {
        level: 3,
        dict_id: dictionary.id(),
    };
    let open = || {
        Config::new(
            folder.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(
            KvSeparationOptions::default()
                .separation_threshold(1_024)
                .dict(std::sync::Arc::clone(&dictionary))
                .dictionary_classes(lsm_tree::DictionaryClasses {
                    blocks: true,
                    singles: false,
                    chunks: false,
                }),
        ))
        .blob_compression(compression)
        .open()
    };
    let value = |len: usize| b"the values a dictionary learns from ".repeat(len / 36);
    let values = [
        ("block", value(2_000)),
        ("single", value(100_000)),
        ("chunked", value(600_000)),
    ];
    {
        let tree = open()?;
        for (key, value) in &values {
            tree.insert(*key, value, 0);
        }
        tree.flush_active_memtable(0)?;
        for (key, value) in &values {
            assert_eq!(
                tree.get(*key, SeqNo::MAX)?.as_deref(),
                Some(&**value),
                "{key}"
            );
        }
    }
    let tree = open()?;
    for (key, value) in &values {
        assert_eq!(
            tree.get(*key, SeqNo::MAX)?.as_deref(),
            Some(&**value),
            "{key}"
        );
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
    // An 8 KiB block holds 4 values of 2,000 bytes and their keys.
    assert!(
        (1..=4).contains(&failed),
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

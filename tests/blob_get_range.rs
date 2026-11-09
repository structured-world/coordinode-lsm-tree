// A byte range of a value is read without the rest of it: of a value split
// into chunks, only the chunks the range covers.

use lsm_tree::{
    AbstractTree, AnyTree, BlobTree, CompressionType, Config, KvSeparationOptions, SeqNo,
    SequenceNumberCounter, get_tmp_folder,
};

/// Bytes that do not repeat within a chunk, so a range read from the wrong
/// place cannot match by chance.
fn pattern(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| u8::try_from((i * 7 + i / 251) % 251).expect("below 251"))
        .collect()
}

/// A blob tree reading its values without the blob cache, so every range is
/// read from the file.
fn open(path: &std::path::Path) -> lsm_tree::Result<BlobTree> {
    let tree = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(1_024),
    ))
    .blob_compression(CompressionType::None)
    .blob_cache_fill(lsm_tree::config::BlobCacheFill::Never)
    .open()?;
    let AnyTree::Blob(tree) = tree else {
        panic!("a tree with kv separation opens as a blob tree");
    };
    Ok(tree)
}

/// A range within one chunk, one across a chunk boundary, the value's tail
/// and an empty range all come back as those bytes of the value.
#[test]
fn a_range_of_a_chunked_value_is_those_bytes() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path())?;
    // Four 256 KiB chunks and a part of a fifth.
    let value = pattern(4 * 256 * 1_024 + 1_000);
    tree.insert("big", &value, 0);
    tree.flush_active_memtable(0)?;

    let len = value.len() as u64;
    let chunk = 256 * 1_024_u64;
    for range in [
        0..100,
        chunk - 10..chunk + 10,
        chunk + 5..3 * chunk + 7,
        len - 1_000..len,
        77..77,
    ] {
        let got = tree
            .get_range("big", range.clone(), SeqNo::MAX)?
            .expect("the key has a value");
        let (start, end) = (range.start as usize, range.end as usize);
        assert_eq!(&*got, &value[start..end], "{range:?}");
    }
    Ok(())
}

/// Of a chunked value, a range reads the chunks it covers and no other: a
/// range inside one chunk reads about one chunk, far from the whole value.
#[cfg(feature = "metrics")]
#[test]
fn a_range_of_a_chunked_value_reads_only_its_chunks() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path())?;
    let value = pattern(8 * 256 * 1_024);
    tree.insert("big", &value, 0);
    tree.flush_active_memtable(0)?;

    let m = tree.metrics();
    let before = m.blob_bytes_read();
    let chunk = 256 * 1_024_u64;
    let got = tree
        .get_range("big", 3 * chunk + 10..3 * chunk + 20, SeqNo::MAX)?
        .expect("the key has a value");
    assert_eq!(&*got, &value[3 * 262_144 + 10..3 * 262_144 + 20]);
    let read = m.blob_bytes_read() - before;
    assert!(
        read < 2 * chunk,
        "{read} bytes read for a range inside one chunk of a {} byte value",
        value.len()
    );
    Ok(())
}

/// An empty range inside a chunk is the empty value, and reads no chunk:
/// only the head that says where the value lies.
#[cfg(feature = "metrics")]
#[test]
fn an_empty_range_of_a_chunked_value_reads_no_chunk() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path())?;
    let value = pattern(4 * 256 * 1_024);
    tree.insert("big", &value, 0);
    tree.flush_active_memtable(0)?;

    let m = tree.metrics();
    let before = m.blob_bytes_read();
    let got = tree
        .get_range("big", 77..77, SeqNo::MAX)?
        .expect("the key has a value");
    assert!(got.is_empty());
    let read = m.blob_bytes_read() - before;
    assert!(read < 1_024, "{read} bytes read for an empty range");
    Ok(())
}

/// The chunk size is the tree's setting: under 64 KiB chunks, a range inside
/// one chunk reads about 64 KiB, a quarter of a default chunk.
#[cfg(feature = "metrics")]
#[test]
fn a_range_reads_the_chunks_the_tree_was_set_to() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let chunk = 64 * 1_024_u32;
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default()
            .separation_threshold(1_024)
            .chunk_size(core::num::NonZeroU32::new(chunk).expect("not zero")),
    ))
    .blob_compression(CompressionType::None)
    .blob_cache_fill(lsm_tree::config::BlobCacheFill::Never)
    .open()?;
    let AnyTree::Blob(tree) = tree else {
        panic!("a tree with kv separation opens as a blob tree");
    };
    let value = pattern(1 << 20);
    tree.insert("big", &value, 0);
    tree.flush_active_memtable(0)?;

    let m = tree.metrics();
    let before = m.blob_bytes_read();
    let at = u64::from(5 * chunk + 100);
    let got = tree
        .get_range("big", at..at + 50, SeqNo::MAX)?
        .expect("the key has a value");
    assert_eq!(&*got, &value[at as usize..at as usize + 50]);
    let read = m.blob_bytes_read() - before;
    assert!(
        read < 2 * u64::from(chunk),
        "{read} bytes read for a range inside one chunk of {chunk} bytes"
    );
    Ok(())
}

/// A value that shares a block, a value of a frame of its own and a value
/// still in the memtable each answer a range as their bytes.
#[test]
fn a_range_of_any_value_is_those_bytes() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path())?;
    let in_block = pattern(2_000);
    let single = pattern(100_000);
    tree.insert("a-block", &in_block, 0);
    tree.insert("b-single", &single, 0);
    tree.flush_active_memtable(0)?;
    let inline = pattern(300);
    tree.insert("c-inline", &inline, 1);

    for (key, value) in [
        ("a-block", &in_block),
        ("b-single", &single),
        ("c-inline", &inline),
    ] {
        let got = tree
            .get_range(key, 10..250, SeqNo::MAX)?
            .expect("the key has a value");
        assert_eq!(&*got, &value[10..250], "{key}");
    }
    Ok(())
}

/// A range past the value's end, or one that ends before it starts, is
/// refused as such; a key with no value has no range either.
#[test]
fn a_range_outside_the_value_is_refused() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path())?;
    tree.insert("big", pattern(600_000), 0);
    tree.insert("small", pattern(2_000), 0);
    tree.flush_active_memtable(0)?;

    for key in ["big", "small"] {
        let past = tree.get_range(key, 0..700_000, SeqNo::MAX);
        assert!(
            matches!(past, Err(lsm_tree::Error::ValueRange { .. })),
            "{key}: {past:?}"
        );
        #[expect(clippy::reversed_empty_ranges, reason = "the range under test")]
        let backwards = tree.get_range(key, 20..10, SeqNo::MAX);
        assert!(
            matches!(backwards, Err(lsm_tree::Error::ValueRange { .. })),
            "{key}: {backwards:?}"
        );
    }
    assert!(tree.get_range("absent", 0..1, SeqNo::MAX)?.is_none());
    Ok(())
}

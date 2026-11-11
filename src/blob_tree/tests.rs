use super::*;
use crate::coding::Encode;
use crate::vlog::ValueHandle;
use test_log::test;

/// A blob tree of `keys` keys with 4 KiB values, every one in a frame of its
/// own in a blob file, over a cache of its own, and reading with `fill`. Frames
/// of their own are what the fill governs: a block a point read decodes is
/// cached whatever it says.
fn blob_tree_with_cache(
    folder: &tempfile::TempDir,
    keys: u32,
    fill: crate::config::BlobCacheFill,
) -> crate::Result<(BlobTree, Arc<crate::Cache>)> {
    use crate::AbstractTree;

    let cache = Arc::new(crate::Cache::with_capacity_bytes(64 * 1_024 * 1_024));
    let crate::AnyTree::Blob(tree) = crate::Config::new(
        folder.path(),
        crate::SequenceNumberCounter::default(),
        crate::SequenceNumberCounter::default(),
    )
    .use_cache(Arc::clone(&cache))
    .with_kv_separation(Some(crate::KvSeparationOptions::default().block_size(0)))
    .blob_cache_fill(fill)
    .open()?
    else {
        panic!("a tree with kv separation opens as a blob tree");
    };
    for i in 0..keys {
        tree.insert(format!("k{i:04}"), vec![b'v'; 4 * 1_024], u64::from(i));
    }
    tree.flush_active_memtable(0)?;
    Ok((tree, cache))
}

/// Whether every data block of the tree's tables is in `cache`.
fn every_data_block_cached(tree: &BlobTree, cache: &crate::Cache) -> crate::Result<bool> {
    for table in tree.index.current_version().iter_tables() {
        for handle in table.data_block_handles() {
            if !cache.has_block(table.global_id(), handle?.as_ref().offset()) {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// Reads every one of `keys` keys by point read, then all of them by scan.
fn read_everything(tree: &BlobTree, keys: u32) -> crate::Result<()> {
    use crate::AbstractTree;

    for i in 0..keys {
        assert_eq!(
            tree.get(format!("k{i:04}"), crate::SeqNo::MAX)?
                .map(|v| v.len()),
            Some(4 * 1_024)
        );
    }
    for item in tree.iter(crate::SeqNo::MAX, None) {
        assert_eq!(item.into_inner()?.1.len(), 4 * 1_024);
    }
    Ok(())
}

/// A tree set never to cache what it reads from blob files reads every value
/// as it did, puts none of them in the cache, and leaves the table blocks
/// cached before the reads where they were; the default caches them all.
#[test]
fn blob_reads_never_cached_leave_the_cache_as_it_was() -> crate::Result<()> {
    const KEYS: u32 = 64;

    let folder = crate::get_tmp_folder();
    let (tree, cache) = blob_tree_with_cache(&folder, KEYS, crate::config::BlobCacheFill::Never)?;
    // The first pass brings the table blocks in; the blobs stay out.
    read_everything(&tree, KEYS)?;
    assert!(every_data_block_cached(&tree, &cache)?);
    let warm = cache.size();
    read_everything(&tree, KEYS)?;
    assert_eq!(
        cache.size(),
        warm,
        "no value read from a blob file was cached"
    );
    assert!(
        every_data_block_cached(&tree, &cache)?,
        "the table blocks cached before stay"
    );

    let folder = crate::get_tmp_folder();
    let (tree, cached) = blob_tree_with_cache(&folder, KEYS, crate::config::BlobCacheFill::Always)?;
    read_everything(&tree, KEYS)?;
    assert!(
        cached.size() >= warm + u64::from(KEYS) * 4 * 1_024,
        "the default caches every value it reads: {} against {warm}",
        cached.size()
    );
    Ok(())
}

/// The policy changes on a live tree: values read after it is set never to
/// cache stay out, and values read after it is set back go in.
#[test]
fn blob_cache_fill_flips_on_a_live_tree() -> crate::Result<()> {
    const KEYS: u32 = 32;

    let folder = crate::get_tmp_folder();
    let (tree, cache) = blob_tree_with_cache(&folder, KEYS, crate::config::BlobCacheFill::Always)?;
    tree.update_runtime_config(|c| c.blob_cache_fill = crate::config::BlobCacheFill::Never)?;
    read_everything(&tree, KEYS)?;
    let warm = cache.size();
    read_everything(&tree, KEYS)?;
    assert_eq!(cache.size(), warm, "nothing cached while set never to");

    tree.update_runtime_config(|c| c.blob_cache_fill = crate::config::BlobCacheFill::Always)?;
    read_everything(&tree, KEYS)?;
    assert!(
        cache.size() >= warm + u64::from(KEYS) * 4 * 1_024,
        "cached again once set back: {} against {warm}",
        cache.size()
    );
    Ok(())
}

/// By default a scan caches no value it reads from blob files, yet still
/// reads them ahead, into a store of its own: the values come back as
/// written, in far fewer reads than values, and the cache holds none of them.
#[cfg(feature = "metrics")]
#[test]
fn a_scan_not_caching_reads_ahead_into_its_own_store() -> crate::Result<()> {
    use crate::{AbstractTree, Guard as _};

    const KEYS: u32 = 256;

    let folder = crate::get_tmp_folder();
    let cache = Arc::new(crate::Cache::with_capacity_bytes(64 * 1_024 * 1_024));
    let crate::AnyTree::Blob(tree) = crate::Config::new(
        folder.path(),
        crate::SequenceNumberCounter::default(),
        crate::SequenceNumberCounter::default(),
    )
    .use_cache(Arc::clone(&cache))
    .with_kv_separation(Some(
        crate::KvSeparationOptions::default().separation_threshold(1),
    ))
    .open()?
    else {
        panic!("a tree with kv separation opens as a blob tree");
    };
    assert_eq!(
        tree.index.runtime_config.load().blob_cache_fill,
        crate::config::BlobCacheFill::Never,
        "values are not cached by default"
    );
    let value = |i: u32| format!("value-{i:06}-{}", "x".repeat(400)).into_bytes();
    for i in 0..KEYS {
        tree.insert(format!("k{i:04}"), value(i), u64::from(i));
    }
    tree.flush_active_memtable(0)?;

    let scan = |tree: &BlobTree| -> crate::Result<()> {
        for (i, item) in (0..KEYS).zip(tree.iter(crate::SeqNo::MAX, None)) {
            assert_eq!(&*item.into_inner()?.1, value(i).as_slice());
        }
        Ok(())
    };
    scan(&tree)?;
    let warm = cache.size();
    let reads = tree.metrics().blob_read_count();
    scan(&tree)?;
    assert_eq!(cache.size(), warm, "the scan cached no value");
    let second = tree.metrics().blob_read_count() - reads;
    assert!(
        second * 8 < KEYS as usize,
        "{second} reads for {KEYS} values: the read-ahead coalesced them"
    );
    assert!(tree.metrics().blob_bytes_prefetched() > 0);
    Ok(())
}

/// A scan set not to cache what it reads still serves the frames the cache
/// holds from it: its read-ahead plans no read of a frame the cache has, so
/// values cached while the tree cached them are read again from nowhere.
#[cfg(feature = "metrics")]
#[test]
fn a_scan_not_caching_reads_nothing_the_cache_holds() -> crate::Result<()> {
    const KEYS: u32 = 64;

    let folder = crate::get_tmp_folder();
    let (tree, _cache) = blob_tree_with_cache(&folder, KEYS, crate::config::BlobCacheFill::Always)?;
    read_everything(&tree, KEYS)?;
    tree.update_runtime_config(|c| c.blob_cache_fill = crate::config::BlobCacheFill::Never)?;

    let reads = tree.metrics().blob_read_count();
    let prefetched = tree.metrics().blob_bytes_prefetched();
    for item in tree.iter(crate::SeqNo::MAX, None) {
        assert_eq!(item.into_inner()?.1.len(), 4 * 1_024);
    }
    assert_eq!(
        tree.metrics().blob_read_count(),
        reads,
        "no frame read again"
    );
    assert_eq!(
        tree.metrics().blob_bytes_prefetched(),
        prefetched,
        "nothing read ahead"
    );
    Ok(())
}

/// A read-ahead whose reads fit its bound but whose frames decode past it
/// stops admitting frames where the decoded bytes would exceed it: a scan of
/// values that compress well still returns each as written, the ones past
/// the bound read on their own.
#[cfg(feature = "lz4")]
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn a_read_ahead_stops_where_its_decoded_frames_reach_its_bound() -> crate::Result<()> {
    use crate::{AbstractTree, Guard as _};

    const KEYS: u32 = 24;
    const LEN: usize = 512 * 1_024;
    let folder = crate::get_tmp_folder();
    let crate::AnyTree::Blob(tree) = crate::Config::new(
        folder.path(),
        crate::SequenceNumberCounter::default(),
        crate::SequenceNumberCounter::default(),
    )
    .blob_compression(crate::CompressionType::Lz4)
    .with_kv_separation(Some(
        crate::KvSeparationOptions::default()
            .separation_threshold(1)
            .chunk_size(core::num::NonZeroU32::new(1_024 * 1_024).expect("non-zero")),
    ))
    .open()?
    else {
        panic!("a tree with kv separation opens as a blob tree");
    };
    // A frame each, a few hundred bytes stored for half a MiB decoded: one
    // coalesced read holds them all, their frames decode to three times the
    // bound.
    let value = |i: u32| vec![u8::try_from(i).expect("small"); LEN];
    for i in 0..KEYS {
        tree.insert(format!("k{i:04}"), value(i), u64::from(i));
    }
    tree.flush_active_memtable(0)?;

    for (i, item) in (0..KEYS).zip(tree.iter(crate::SeqNo::MAX, None)) {
        assert!(*item.into_inner()?.1 == *value(i), "value {i}");
    }
    Ok(())
}

/// A row's guard takes out of the scan's store exactly the values its row
/// names: an indirection takes its value, and a row whose indirection or cell
/// references do not decode takes nothing, leaving its resolution, and the
/// report of the damage, to the read of the row itself.
#[test]
fn a_row_takes_only_what_its_handles_name_from_the_scan_store() {
    let vhandle = ValueHandle {
        blob_file_id: 3,
        frame: 12,
        index: 0,
    };
    let indirection = BlobIndirection {
        vhandle,
        size: 5,
        stored: crate::blob_tree::handle::Stored::Frames(5),
    };
    let mut store = crate::vlog::Prefetched::default();
    store.hold(
        vhandle.frame_at(),
        crate::vlog::blob_file::frame::decoded_frame(&[(b"k", b"value")]),
    );

    let damaged = |value_type| InternalValue::from_components("k", vec![0xff_u8; 3], 0, value_type);
    for value_type in [crate::ValueType::Indirection, crate::ValueType::CellRow] {
        assert!(
            take_prefetched(&store, &damaged(value_type))
                .as_slice()
                .is_empty(),
            "{value_type:?} that does not decode takes nothing"
        );
    }
    assert!(!store.is_empty(), "the value stays for its own row");

    let row = InternalValue::from_components(
        "k",
        indirection.encode_into_vec(),
        0,
        crate::ValueType::Indirection,
    );
    let taken = take_prefetched(&store, &row);
    let [(handle, value)] = taken.as_slice() else {
        panic!("one value taken, got {}", taken.as_slice().len());
    };
    assert_eq!(*handle, vhandle);
    assert_eq!(&**value, b"value");

    // The frame lists its key: a row of another key naming the same place
    // takes nothing out of it.
    let other = InternalValue::from_components(
        "other",
        indirection.encode_into_vec(),
        0,
        crate::ValueType::Indirection,
    );
    assert!(matches!(take_prefetched(&store, &other), AheadValues::None));
}

/// A frame the scan's store lets go still counts against the read-ahead bound
/// while a value taken out of it keeps its bytes, and stops counting once the
/// last such value is dropped: what a kept guard pins is its frame's payload.
#[test]
fn a_released_frame_counts_while_a_taken_value_keeps_it() {
    let vhandle = ValueHandle {
        blob_file_id: 3,
        frame: 12,
        index: 0,
    };
    // Longer than a slice holds inline, so the value is a view, not a copy.
    let payload = vec![7_u8; 64];
    let mut store = crate::vlog::Prefetched::default();
    store.hold(
        vhandle.frame_at(),
        crate::vlog::blob_file::frame::decoded_frame(&[(b"k", &payload)]),
    );
    assert_eq!(store.held_bytes(), payload.len(), "the held frame counts");

    let Some(value) = store.get(b"k", &vhandle) else {
        panic!("the value is held");
    };
    store.retain_for(&[]);
    assert!(store.is_empty(), "the frame is let go");
    assert_eq!(
        store.held_bytes(),
        payload.len(),
        "the value taken out of it still keeps its payload"
    );

    drop(value);
    assert_eq!(store.held_bytes(), 0, "nothing keeps the payload any more");
}

/// A scan that does not cache hands each guard the values read ahead for it.
/// A caller that keeps the guards without resolving them (collecting the rest
/// of a scan, say) must not make the scan read the whole value log into them:
/// once the guards hold a read's worth, the scan stops reading ahead, and the
/// guards it hands out after that read their values when resolved.
#[cfg(feature = "metrics")]
#[test]
fn guards_kept_unresolved_hold_no_more_than_the_read_ahead_bound() -> crate::Result<()> {
    use crate::{AbstractTree, Guard as _};

    const KEYS: u32 = 512;
    const VALUE: usize = 64 * 1_024;

    let folder = crate::get_tmp_folder();
    let crate::AnyTree::Blob(tree) = crate::Config::new(
        folder.path(),
        crate::SequenceNumberCounter::default(),
        crate::SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(crate::KvSeparationOptions::default()))
    .blob_compression(crate::CompressionType::None)
    .blob_cache_fill(crate::config::BlobCacheFill::Never)
    .open()?
    else {
        panic!("a tree with kv separation opens as a blob tree");
    };
    for i in 0..KEYS {
        tree.insert(format!("k{i:04}"), vec![b'v'; VALUE], u64::from(i));
    }
    tree.flush_active_memtable(0)?;

    let mut iter = tree.iter(crate::SeqNo::MAX, None);
    // Resolving one value is what arms the read-ahead.
    let Some(first) = iter.next() else {
        panic!("the scan yields a row");
    };
    let first = first.into_inner()?;
    assert_eq!(first.1.len(), VALUE);
    let before = tree.metrics().blob_bytes_prefetched();
    let kept: Vec<_> = iter.collect();
    let read_ahead = tree.metrics().blob_bytes_prefetched() - before;
    assert_eq!(kept.len(), KEYS as usize - 1);
    assert!(
        read_ahead < 3 * PREFETCH_MAX_READ as u64,
        "{read_ahead} bytes read ahead into guards nobody resolved, of {} in the scan",
        u64::from(KEYS) * VALUE as u64,
    );
    for guard in kept {
        assert_eq!(guard.into_inner()?.1.len(), VALUE);
    }
    Ok(())
}

/// A value taken out of a shared block keeps the whole block: a caller keeping
/// one guard per block pins a block's bytes for each, and the bound counts
/// those, not the values. Counted by the values, keeping every block's first
/// guard read the whole value log ahead into blocks nobody resolved.
#[cfg(feature = "metrics")]
#[test]
fn guards_kept_one_per_block_hold_no_more_than_the_read_ahead_bound() -> crate::Result<()> {
    use crate::{AbstractTree, Guard as _};

    const KEYS: u32 = 32 * 1_024;
    const VALUE: usize = 1_024;
    /// Fewer values than a 16 KiB block holds, so a kept guard per stride
    /// lands in a block of its own most of the time.
    const STRIDE: usize = 14;

    let folder = crate::get_tmp_folder();
    let crate::AnyTree::Blob(tree) = crate::Config::new(
        folder.path(),
        crate::SequenceNumberCounter::default(),
        crate::SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        crate::KvSeparationOptions::default().separation_threshold(512),
    ))
    .blob_compression(crate::CompressionType::None)
    .blob_cache_fill(crate::config::BlobCacheFill::Never)
    .open()?
    else {
        panic!("a tree with kv separation opens as a blob tree");
    };
    for i in 0..KEYS {
        tree.insert(format!("k{i:06}"), vec![b'v'; VALUE], u64::from(i));
    }
    tree.flush_active_memtable(0)?;

    let mut iter = tree.iter(crate::SeqNo::MAX, None);
    let Some(first) = iter.next() else {
        panic!("the scan yields a row");
    };
    assert_eq!(first.into_inner()?.1.len(), VALUE);
    let before = tree.metrics().blob_bytes_prefetched();
    let kept: Vec<_> = iter.step_by(STRIDE).collect();
    let read_ahead = tree.metrics().blob_bytes_prefetched() - before;
    assert!(
        read_ahead < 3 * PREFETCH_MAX_READ as u64,
        "{read_ahead} bytes read ahead into blocks {} guards keep, of {} in the scan",
        kept.len(),
        u64::from(KEYS) * VALUE as u64,
    );
    for guard in kept {
        assert_eq!(guard.into_inner()?.1.len(), VALUE);
    }
    Ok(())
}

/// An indirection naming a blob file the version does not hold is damage:
/// resolving it is an error the caller can handle, never a panic and never
/// an empty value.
#[test]
fn a_dangling_indirection_resolves_to_an_error() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let crate::AnyTree::Blob(tree) = crate::Config::new(
        folder.path(),
        crate::SequenceNumberCounter::default(),
        crate::SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(crate::KvSeparationOptions::default()))
    .open()?
    else {
        panic!("a tree with kv separation opens as a blob tree");
    };
    let dangling = BlobIndirection {
        vhandle: ValueHandle {
            blob_file_id: 999,
            frame: 0,
            index: 0,
        },
        size: 10,
        stored: crate::blob_tree::handle::Stored::Frames(10),
    };
    let item = InternalValue::from_components(
        "k",
        dangling.encode_into_vec(),
        0,
        crate::ValueType::Indirection,
    );
    let version = tree.current_version();
    let result = resolve_value_handle(
        tree.id(),
        &tree.index.config.cache,
        crate::config::BlobCacheFill::Always,
        &[],
        &version,
        #[cfg(feature = "metrics")]
        tree.metrics(),
        item,
    );
    assert!(
        matches!(result, Err(crate::Error::InvalidHeader(_))),
        "{result:?}"
    );
    // A batched read leaves it to the value's own path, which reports it.
    assert!(matches!(
        tree.blob_source().plan_value(&version, &item_of(&dangling)),
        ValuePlan::Alone
    ));
    Ok(())
}

/// The entry for key `k` pointing at `indirection`.
fn item_of(indirection: &BlobIndirection) -> InternalValue {
    InternalValue::from_components(
        "k",
        indirection.encode_into_vec(),
        0,
        crate::ValueType::Indirection,
    )
}

/// A batched read plans a chunked value's read through its head: a head the
/// cache holds with every chunk sends the value on its own path, which
/// assembles it from them; a read that reaches the head alone hands back no
/// value for that path to read the chunks; a read shorter than the head's
/// frame is refused.
#[test]
#[expect(clippy::expect_used, clippy::indexing_slicing, reason = "test code")]
fn a_batched_read_of_a_chunked_value_falls_back_to_its_own_path() -> crate::Result<()> {
    use crate::{AbstractTree, coding::Decode};

    let folder = crate::get_tmp_folder();
    let open = |fill| {
        crate::Config::new(
            folder.path(),
            crate::SequenceNumberCounter::default(),
            crate::SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(crate::KvSeparationOptions::default()))
        .blob_cache_fill(fill)
        .open()
    };
    {
        let crate::AnyTree::Blob(tree) = open(crate::config::BlobCacheFill::Never)? else {
            panic!("a tree with kv separation opens as a blob tree");
        };
        tree.insert("k", vec![b'v'; 1024 * 1024], 0);
        tree.flush_active_memtable(0)?;
    }

    let crate::AnyTree::Blob(tree) = open(crate::config::BlobCacheFill::Never)? else {
        panic!("a tree with kv separation opens as a blob tree");
    };
    let version = tree.current_version();
    let item = tree
        .index
        .get_internal_entry(b"k", crate::SeqNo::MAX)?
        .expect("the value");
    let indirection = BlobIndirection::decode_from(&mut &item.value[..])?;

    // A head charged only its own bytes is read alone.
    let head_only = BlobIndirection {
        stored: crate::blob_tree::handle::Stored::Frames(1),
        ..indirection
    };
    let source = tree.blob_source();
    let ValuePlan::Read(read) = source.plan_value(&version, &item_of(&head_only)) else {
        panic!("an uncached value is read");
    };
    let file = std::fs::read(&read.blob_file.0.path)?;
    let at = usize::try_from(read.offset).expect("fits");
    let head = crate::Slice::from(&file[at..at + read.len]);
    let (value, frame) = source.finish_value(&version, b"k", &read, &head)?;
    assert!(
        value.is_none() && frame.is_none(),
        "the chunks are read on the value's own path"
    );
    let short = crate::Slice::from(&file[at..at + read.len - 1]);
    assert!(source.finish_value(&version, b"k", &read, &short).is_err());

    // A point read caching what it reads leaves the head and every chunk
    // cached: the value's own path assembles it from them, with no read.
    drop(tree);
    let crate::AnyTree::Blob(tree) = open(crate::config::BlobCacheFill::Always)? else {
        panic!("a tree with kv separation opens as a blob tree");
    };
    assert_eq!(
        tree.get("k", crate::SeqNo::MAX)?.map(|v| v.len()),
        Some(1024 * 1024)
    );
    let version = tree.current_version();
    assert!(matches!(
        tree.blob_source()
            .plan_value(&version, &item_of(&indirection)),
        ValuePlan::Alone
    ));
    Ok(())
}

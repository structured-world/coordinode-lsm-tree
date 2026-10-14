use super::*;
use crate::coding::Encode;
use crate::vlog::ValueHandle;
use test_log::test;

/// A blob tree of `keys` keys with 4 KiB values, every one in a blob file,
/// over a cache of its own, and reading with `fill`.
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
    .with_kv_separation(Some(crate::KvSeparationOptions::default()))
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

/// A row's guard takes out of the scan's store exactly the values its row
/// names: an indirection takes its value, and a row whose indirection or cell
/// references do not decode takes nothing, leaving its resolution, and the
/// report of the damage, to the read of the row itself.
#[test]
fn a_row_takes_only_what_its_handles_name_from_the_scan_store() {
    let vhandle = ValueHandle {
        blob_file_id: 3,
        offset: 128,
        on_disk_size: 40,
    };
    let indirection = BlobIndirection { vhandle, size: 32 };
    let mut store = crate::vlog::Prefetched::default();
    store.hold(b"k", &vhandle, crate::UserValue::from(b"value".as_slice()));

    let damaged = |value_type| InternalValue::from_components("k", vec![0xff_u8; 3], 0, value_type);
    for value_type in [crate::ValueType::Indirection, crate::ValueType::CellRow] {
        assert!(
            take_prefetched(&mut store, &damaged(value_type)).is_empty(),
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
    let taken = take_prefetched(&mut store, &row);
    let [(handle, value)] = taken.as_slice() else {
        panic!("one value taken, got {}", taken.len());
    };
    assert_eq!(*handle, vhandle);
    assert_eq!(&**value, b"value");
    assert!(store.is_empty(), "taken out, not copied");
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
            offset: 0,
            on_disk_size: 10,
        },
        size: 10,
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
    Ok(())
}

use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, StdFs};
use crate::{AbstractTree, AnyTree, Cache, Config, SeqNo, SequenceNumberCounter};
use alloc::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const READERS: u8 = 8;

/// The value written under `key` by [`write_keys`].
fn value(key: u8) -> [u8; 32] {
    [key; 32]
}

/// Writes keys `0..count` and flushes them into one table.
fn write_keys(config: Config, count: u8) -> crate::Result<()> {
    let tree = config.open()?;
    for key in 0..count {
        tree.insert([key], value(key), u64::from(key));
    }
    tree.flush_active_memtable(0)
}

/// A cache of `bytes` without a row cache whose misses share every cold load,
/// however quick: these tests exercise the sharing itself.
fn sharing_cache(bytes: u64) -> Arc<Cache> {
    Arc::new(
        Cache::with_capacity_bytes(bytes)
            .with_row_cache(false)
            .with_load_coalescing_after(Duration::ZERO),
    )
}

fn config(folder: &std::path::Path) -> Config {
    Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
}

/// The tree in `folder` reopened over `fs` with `cache`, so nothing it holds
/// is cached.
fn reopen_cold(
    folder: &std::path::Path,
    fs: FaultFs<StdFs>,
    cache: &Arc<Cache>,
    separated: bool,
) -> crate::Result<AnyTree> {
    let config = config(folder).with_fs(fs).use_cache(Arc::clone(cache));
    if separated {
        config
            .with_kv_separation(Some(
                crate::KvSeparationOptions::default().separation_threshold(1),
            ))
            .open()
    } else {
        config.open()
    }
}

/// The path and file offset of the table's first data block.
fn first_data_block(tree: &AnyTree) -> (String, u64) {
    use crate::table::block_index::BlockIndex as _;
    let version = tree.current_version();
    let Some(table) = version.iter_tables().next() else {
        panic!("the flush wrote a table");
    };
    let Some(handle) = table.block_index.iter().find_map(Result::ok) else {
        panic!("a non-empty table has a data block");
    };
    (table.path.to_string_lossy().into_owned(), *handle.offset())
}

/// A hook that holds the first read it sees until `waiters` requests wait on
/// cold loads (or ten seconds pass), and counts every read it sees.
fn hold_until_joined(
    cache: &Arc<Cache>,
    waiters: usize,
) -> (Arc<AtomicUsize>, crate::fs::ReadHook) {
    let reads = Arc::new(AtomicUsize::new(0));
    let hook: crate::fs::ReadHook = {
        let (reads, cache) = (Arc::clone(&reads), Arc::clone(cache));
        Arc::new(move || {
            if reads.fetch_add(1, Ordering::SeqCst) == 0 {
                let deadline = Instant::now() + Duration::from_secs(10);
                while cache.load_waiters() < waiters && Instant::now() < deadline {
                    std::thread::yield_now();
                }
            }
        })
    };
    (reads, hook)
}

/// Reads keys `0..READERS` from `tree`, one thread each, all released at once.
#[expect(
    clippy::needless_collect,
    reason = "every reader is spawned before any is joined, or the barrier never opens"
)]
fn read_at_once(tree: &AnyTree) -> Vec<crate::Result<Option<crate::UserValue>>> {
    let barrier = Arc::new(Barrier::new(usize::from(READERS)));
    let readers: Vec<_> = (0..READERS)
        .map(|key| {
            let (tree, barrier) = (tree.clone(), Arc::clone(&barrier));
            std::thread::spawn(move || {
                barrier.wait();
                tree.get([key], SeqNo::MAX)
            })
        })
        .collect();
    readers
        .into_iter()
        .map(|reader| reader.join().expect("reader"))
        .collect()
}

/// Point reads that miss the same cold block at once read and decode it once:
/// one reader loads it, the others wait and take its block, every one gets the
/// right value, and no load stays registered.
#[test]
fn simultaneous_cold_point_reads_of_one_block_read_it_once() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    write_keys(config(folder.path()), 64)?;
    let fs = FaultFs::new(StdFs);
    let injector = fs.injector();
    let cache = sharing_cache(64 << 20);
    let tree = reopen_cold(folder.path(), fs, &cache, false)?;
    let (path, offset) = first_data_block(&tree);
    let (reads, hook) = hold_until_joined(&cache, usize::from(READERS) - 1);
    injector.hook_read_at(path, offset, hook);

    for (key, got) in (0..READERS).zip(read_at_once(&tree)) {
        assert_eq!(got?.as_deref(), Some(&value(key)[..]), "key {key}");
    }
    assert_eq!(reads.load(Ordering::SeqCst), 1, "one read of the block");
    assert_eq!(cache.loads_in_flight(), 0, "no load stays registered");
    #[cfg(feature = "metrics")]
    assert_eq!(
        tree.metrics().block_load_shared_count(),
        usize::from(READERS) - 1
    );
    Ok(())
}

/// A block the cache will not keep (heavier than a cache shard, as one the
/// cache evicts at once) still reaches the readers waiting on its load: they
/// hold the decoded block itself, not a cache entry, so the block is read once.
#[test]
fn a_block_the_cache_refuses_still_reaches_its_waiters() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    write_keys(config(folder.path()), 64)?;
    let fs = FaultFs::new(StdFs);
    let injector = fs.injector();
    // 64 bytes over 64 shards: no block fits a shard, so none is kept.
    let cache = sharing_cache(64);
    let tree = reopen_cold(folder.path(), fs, &cache, false)?;
    let (path, offset) = first_data_block(&tree);
    let (reads, hook) = hold_until_joined(&cache, usize::from(READERS) - 1);
    injector.hook_read_at(path, offset, hook);

    for (key, got) in (0..READERS).zip(read_at_once(&tree)) {
        assert_eq!(got?.as_deref(), Some(&value(key)[..]), "key {key}");
    }
    assert_eq!(reads.load(Ordering::SeqCst), 1, "one read of the block");
    assert_eq!(cache.size(), 0, "the cache kept nothing");
    assert_eq!(cache.loads_in_flight(), 0);
    Ok(())
}

/// A load that fails releases its waiters to read on their own, so each meets
/// the failure itself, and the next read after the fault clears succeeds: no
/// error is remembered and no waiter is stranded.
#[test]
fn failed_cold_load_releases_waiters_and_is_not_remembered() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    write_keys(config(folder.path()), 64)?;
    let fs = FaultFs::new(StdFs);
    let injector = fs.injector();
    let cache = sharing_cache(64 << 20);
    let tree = reopen_cold(folder.path(), fs, &cache, false)?;
    let (path, offset) = first_data_block(&tree);
    let (reads, hook) = hold_until_joined(&cache, usize::from(READERS) - 1);
    injector.hook_read_at(path.clone(), offset, hook);
    injector.arm(
        FaultRule::new(FaultOp::ReadAt, Fault::Error(crate::io::ErrorKind::Other))
            .on_path(path)
            .at_offset(offset),
    );

    for got in read_at_once(&tree) {
        assert!(got.is_err(), "every reader meets the read failure");
    }
    assert_eq!(
        reads.load(Ordering::SeqCst),
        usize::from(READERS),
        "a failed load is not shared: each waiter read for itself"
    );
    assert_eq!(cache.loads_in_flight(), 0, "no load stays registered");

    injector.clear();
    assert_eq!(
        tree.get([0], SeqNo::MAX)?.as_deref(),
        Some(&value(0)[..]),
        "the failure was not remembered"
    );
    Ok(())
}

/// Loads of different blocks do not wait on each other: while one block's
/// read is held, a read of another block completes.
#[test]
fn held_cold_load_does_not_block_another_block() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    write_keys(
        config(folder.path()).data_block_size_policy(crate::config::BlockSizePolicy::all(256)),
        64,
    )?;
    let fs = FaultFs::new(StdFs);
    let injector = fs.injector();
    let cache = sharing_cache(64 << 20);
    let tree = reopen_cold(folder.path(), fs, &cache, false)?;
    let (path, offset) = first_data_block(&tree);

    let held = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    {
        let (held, release) = (Arc::clone(&held), Arc::clone(&release));
        injector.hook_read_at(
            path,
            offset,
            Arc::new(move || {
                held.wait();
                release.wait();
            }),
        );
    }
    let first = {
        let tree = tree.clone();
        std::thread::spawn(move || tree.get([0], SeqNo::MAX))
    };
    held.wait();
    // The last key lives in another block, whose load runs while the first
    // block's read is held.
    assert_eq!(tree.get([63], SeqNo::MAX)?.as_deref(), Some(&value(63)[..]));
    release.wait();
    assert_eq!(
        first.join().expect("reader")?.as_deref(),
        Some(&value(0)[..])
    );
    Ok(())
}

/// Point reads that miss the same cold columnar row group at once read its
/// directory once: one reader reads it with the pages it needs, the others
/// take the directory from it and find those pages cached.
#[cfg(feature = "columnar")]
#[test]
fn simultaneous_cold_reads_of_one_row_group_read_its_directory_once() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    {
        let crate::AnyTree::Standard(tree) = config(folder.path()).open()? else {
            unreachable!("standard tree configured");
        };
        tree.update_runtime_config(|cfg| cfg.columnar = true)?;
        for key in 0..64u8 {
            tree.insert([key], value(key), u64::from(key));
        }
        tree.flush_active_memtable(0)?;
    }
    let fs = FaultFs::new(StdFs);
    let injector = fs.injector();
    let cache = sharing_cache(64 << 20);
    let tree = reopen_cold(folder.path(), fs, &cache, false)?;
    let (path, offset) = first_data_block(&tree);
    let (reads, hook) = hold_until_joined(&cache, usize::from(READERS) - 1);
    injector.hook_read_at(path, offset, hook);

    for (key, got) in (0..READERS).zip(read_at_once(&tree)) {
        assert_eq!(got?.as_deref(), Some(&value(key)[..]), "key {key}");
    }
    assert_eq!(reads.load(Ordering::SeqCst), 1, "one read of the directory");
    assert_eq!(cache.loads_in_flight(), 0, "no load stays registered");
    Ok(())
}

/// A point read that misses a block a blocking multi-get is reading waits for
/// the multi-get's read instead of reading it again, and both answer right.
#[test]
fn point_read_waits_for_a_block_a_multi_get_reads() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    write_keys(config(folder.path()), 64)?;
    let fs = FaultFs::new(StdFs);
    let injector = fs.injector();
    let cache = sharing_cache(64 << 20);
    let tree = reopen_cold(folder.path(), fs, &cache, false)?;
    let (path, offset) = first_data_block(&tree);

    let entered = Arc::new(Barrier::new(2));
    let reads = Arc::new(AtomicUsize::new(0));
    {
        let (entered, reads, cache) =
            (Arc::clone(&entered), Arc::clone(&reads), Arc::clone(&cache));
        injector.hook_read_at(
            path,
            offset,
            Arc::new(move || {
                if reads.fetch_add(1, Ordering::SeqCst) == 0 {
                    entered.wait();
                    let deadline = Instant::now() + Duration::from_secs(10);
                    while cache.load_waiters() < 1 && Instant::now() < deadline {
                        std::thread::yield_now();
                    }
                }
            }),
        );
    }
    let batch = {
        let tree = tree.clone();
        std::thread::spawn(move || tree.multi_get((0..64u8).map(|key| [key]), SeqNo::MAX))
    };
    entered.wait();
    let point = std::thread::spawn(move || tree.get([0], SeqNo::MAX));
    assert_eq!(
        point.join().expect("reader")?.as_deref(),
        Some(&value(0)[..])
    );
    let values = batch.join().expect("reader")?;
    for (key, got) in (0..64u8).zip(values) {
        assert_eq!(got.as_deref(), Some(&value(key)[..]), "key {key}");
    }
    assert_eq!(reads.load(Ordering::SeqCst), 1, "one read of the block");
    assert_eq!(cache.loads_in_flight(), 0);
    Ok(())
}

/// A multi-get its caller drives leads no load: the caller can hold it
/// suspended as long as it likes, so a point read missing one of its blocks
/// meanwhile reads the block itself rather than waiting on it.
#[test]
fn a_suspended_resumable_multi_get_holds_no_load() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    write_keys(config(folder.path()), 64)?;
    let cache = sharing_cache(64 << 20);
    let tree = reopen_cold(folder.path(), FaultFs::new(StdFs), &cache, false)?;
    let crate::resumable::Step::Pending(mut read) = tree.start_multi_get(
        (0..64u8).map(|key| crate::UserKey::from(&[key][..])),
        SeqNo::MAX,
    )?
    else {
        panic!("a cold multi-get suspends on its reads");
    };
    // Suspended with work handed out and not done.
    let jobs = read.take_jobs();
    let reads = read.take_reads();
    assert_eq!(cache.loads_in_flight(), 0, "a suspended read leads no load");
    assert_eq!(tree.get([0], SeqNo::MAX)?.as_deref(), Some(&value(0)[..]));

    for job in jobs {
        let outcome = job.run();
        read.complete_job(outcome);
    }
    for mut block in reads {
        let result = block.file.read_at(&mut block.buf, block.offset).map(|_| ());
        read.complete_read(block.tag, result, block.buf);
    }
    let mut step = read.resume();
    let values = loop {
        match step {
            crate::resumable::Step::Done(values) => break values?,
            crate::resumable::Step::Pending(mut read) => {
                for job in read.take_jobs() {
                    let outcome = job.run();
                    read.complete_job(outcome);
                }
                for mut block in read.take_reads() {
                    let result = block.file.read_at(&mut block.buf, block.offset).map(|_| ());
                    read.complete_read(block.tag, result, block.buf);
                }
                step = read.resume();
            }
        }
    };
    for (key, got) in (0..64u8).zip(values) {
        assert_eq!(got.as_deref(), Some(&value(key)[..]), "key {key}");
    }
    Ok(())
}

/// Multi-gets reading through the `io_uring` ring's own queue lead the loads
/// of their blocks and publish them from the ring's completions, while point
/// reads of the same keys run at once and wait on those loads: every read
/// answers right, none is left waiting, and no load stays registered.
#[cfg(all(target_os = "linux", feature = "io-uring"))]
#[test]
fn point_reads_wait_on_multi_gets_reading_through_io_uring() -> crate::Result<()> {
    if !crate::fs::is_io_uring_available() {
        return Ok(());
    }
    let folder = tempfile::tempdir()?;
    write_keys(
        config(folder.path()).data_block_size_policy(crate::config::BlockSizePolicy::all(256)),
        200,
    )?;
    for _ in 0..4 {
        let cache = sharing_cache(64 << 20);
        let tree = config(folder.path())
            .with_fs(crate::fs::IoUringFs::new()?)
            .use_cache(Arc::clone(&cache))
            .open()?;
        let barrier = Arc::new(Barrier::new(usize::from(READERS)));
        let readers: Vec<_> = (0..READERS)
            .map(|reader| {
                let (tree, barrier) = (tree.clone(), Arc::clone(&barrier));
                std::thread::spawn(move || -> crate::Result<()> {
                    barrier.wait();
                    if reader % 2 == 0 {
                        let values = tree.multi_get((0..200u8).map(|key| [key]), SeqNo::MAX)?;
                        for (key, got) in (0..200u8).zip(values) {
                            assert_eq!(got.as_deref(), Some(&value(key)[..]), "key {key}");
                        }
                    } else {
                        for key in 0..200u8 {
                            assert_eq!(
                                tree.get([key], SeqNo::MAX)?.as_deref(),
                                Some(&value(key)[..]),
                                "key {key}"
                            );
                        }
                    }
                    Ok(())
                })
            })
            .collect();
        for reader in readers {
            reader.join().expect("reader")?;
        }
        assert_eq!(cache.loads_in_flight(), 0, "no load stays registered");
    }
    Ok(())
}

/// A table of one large zstd data block that bounded ranges decode only in
/// part, with its file and the block's offset.
#[cfg(feature = "zstd")]
fn partial_table(
    folder: &std::path::Path,
    fs: FaultFs<StdFs>,
) -> crate::Result<(crate::table::Table, String, crate::table::BlockOffset)> {
    use crate::config::{BlockSizePolicy, CompressionPolicy};
    let crate::AnyTree::Standard(tree) = config(folder)
        .with_fs(fs)
        .use_cache(Arc::new(
            Cache::with_capacity_bytes(64 << 20).with_load_coalescing_after(Duration::ZERO),
        ))
        .data_block_compression_policy(CompressionPolicy::all(crate::CompressionType::zstd(19)?))
        .data_block_size_policy(BlockSizePolicy::all(512 * 1024))
        .open()?
    else {
        unreachable!("standard tree configured");
    };
    for i in 0u64..20_000 {
        tree.insert(
            format!("key-{i:08}"),
            format!("value-{i:08}-padding-padding"),
            0,
        );
    }
    tree.flush_active_memtable(0)?;
    let binding = tree.version_history.read().latest_version();
    let Some(table) = binding.version.iter_tables().next().cloned() else {
        unreachable!("flush produced one table");
    };
    let offset = table
        .untraced_index_walk()
        .next()
        .expect("the table has a data block")?
        .offset();
    let path = table.path.to_string_lossy().into_owned();
    Ok((table, path, offset))
}

/// The keys of `table` below `upper`, read through the partial tier on the
/// calling thread.
#[cfg(feature = "zstd")]
fn keys_below(table: &crate::table::Table, upper: u64) -> crate::Result<Vec<crate::UserKey>> {
    crate::table::iter::force_partial_decode_on_this_thread();
    let upper = crate::UserKey::from(format!("key-{upper:08}"));
    table
        .range_iter(crate::UserKey::from("key-")..upper)
        .map(|kv| kv.map(|kv| kv.key.user_key))
        .collect()
}

/// The keys `keys_below` must return.
#[cfg(feature = "zstd")]
fn expected_below(upper: u64) -> Vec<crate::UserKey> {
    (0..upper)
        .map(|i| crate::UserKey::from(format!("key-{i:08}")))
        .collect()
}

/// Two bounded ranges that need the same cold block's prefix grown do it one
/// after the other, in either completion order: with the wider read first the
/// narrower one is served from its prefix without reading the block, with the
/// narrower first the wider one resumes from it; every read returns exactly
/// its rows, and the cached prefix never shrinks.
#[cfg(feature = "zstd")]
#[test]
fn concurrent_partial_growth_is_serialized_in_either_order() -> crate::Result<()> {
    const NARROW: u64 = 30;
    const WIDE: u64 = 3_000;
    for (first, second, reads_expected) in [(WIDE, NARROW, 1), (NARROW, WIDE, 2)] {
        let folder = tempfile::tempdir()?;
        let fs = FaultFs::new(StdFs);
        let injector = fs.injector();
        let (table, path, offset) = partial_table(folder.path(), fs)?;
        let cache = Arc::clone(&table.cache);

        // Holds the first read of the block until the second range waits on
        // its growth.
        let entered = Arc::new(Barrier::new(2));
        let reads = Arc::new(AtomicUsize::new(0));
        {
            let (entered, reads, cache) =
                (Arc::clone(&entered), Arc::clone(&reads), Arc::clone(&cache));
            injector.hook_read_at(
                path,
                *offset,
                Arc::new(move || {
                    if reads.fetch_add(1, Ordering::SeqCst) == 0 {
                        entered.wait();
                        let deadline = Instant::now() + Duration::from_secs(10);
                        while cache.load_waiters() < 1 && Instant::now() < deadline {
                            std::thread::yield_now();
                        }
                    }
                }),
            );
        }
        let leader = {
            let table = table.clone();
            std::thread::spawn(move || keys_below(&table, first))
        };
        entered.wait();
        let follower = {
            let table = table.clone();
            std::thread::spawn(move || keys_below(&table, second))
        };
        assert_eq!(follower.join().expect("reader")?, expected_below(second));
        assert_eq!(leader.join().expect("reader")?, expected_below(first));

        assert_eq!(
            reads.load(Ordering::SeqCst),
            reads_expected,
            "{first} then {second}: the block is read once per growth"
        );
        let Some(entry) = cache.peek_partial_block(table.global_id(), offset) else {
            panic!("the bounded reads left a partial prefix");
        };
        assert!(
            &*entry.covered_upper >= format!("key-{:08}", WIDE - 1).as_bytes(),
            "{first} then {second}: the prefix covers the wider read"
        );
        assert_eq!(cache.loads_in_flight(), 0);
    }
    Ok(())
}

/// A read waiting on a prefix growth that ends by promoting the block to a
/// full load does not grow a prefix of its own: it takes the full block from
/// the promoting read's load instead of reading the frame again.
#[cfg(feature = "zstd")]
#[test]
fn a_read_waiting_on_a_growth_that_promotes_shares_the_full_load() -> crate::Result<()> {
    // Wide enough to decode most of the block, which promotes it.
    const WIDE: u64 = 19_500;
    const NARROW: u64 = 30;
    let folder = tempfile::tempdir()?;
    let fs = FaultFs::new(StdFs);
    let injector = fs.injector();
    let (table, path, offset) = partial_table(folder.path(), fs)?;
    let cache = Arc::clone(&table.cache);

    // The first read (the wide range's frame) is held until the narrow range
    // waits on its growth; the second (its full load after promoting) until a
    // read waits on that load, or ten seconds pass.
    let entered = Arc::new(Barrier::new(2));
    let reads = Arc::new(AtomicUsize::new(0));
    {
        let (entered, reads, cache) =
            (Arc::clone(&entered), Arc::clone(&reads), Arc::clone(&cache));
        injector.hook_read_at(
            path,
            *offset,
            Arc::new(move || {
                let n = reads.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    entered.wait();
                }
                if n <= 1 {
                    let deadline = Instant::now() + Duration::from_secs(10);
                    while cache.load_waiters() < 1 && Instant::now() < deadline {
                        std::thread::yield_now();
                    }
                }
            }),
        );
    }
    let wide = {
        let table = table.clone();
        std::thread::spawn(move || keys_below(&table, WIDE))
    };
    entered.wait();
    let narrow = std::thread::spawn(move || keys_below(&table, NARROW));
    assert_eq!(narrow.join().expect("reader")?, expected_below(NARROW));
    assert_eq!(wide.join().expect("reader")?, expected_below(WIDE));
    assert_eq!(
        reads.load(Ordering::SeqCst),
        2,
        "the wide read's frame and its full load, and nothing for the narrow one"
    );
    assert_eq!(cache.loads_in_flight(), 0);
    Ok(())
}

/// A read that caches the values it reads, waiting on a read of the same
/// value that does not, caches the shared value itself: whether the value
/// lands in the cache follows the policy of the read that asked, not of the
/// one that happened to lead.
#[cfg(feature = "metrics")]
#[test]
fn a_caching_read_caches_a_value_it_took_from_a_read_that_does_not() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    write_keys(
        config(folder.path()).with_kv_separation(Some(
            crate::KvSeparationOptions::default().separation_threshold(1),
        )),
        1,
    )?;
    let fs = FaultFs::new(StdFs);
    let injector = fs.injector();
    let cache = sharing_cache(64 << 20);
    let tree = reopen_cold(folder.path(), fs, &cache, true)?;
    let crate::AnyTree::Blob(blob) = &tree else {
        unreachable!("a separated tree opens as a blob tree");
    };
    assert!(tree.contains_key([0], SeqNo::MAX)?);

    // The first read of the record (by a read that does not cache) is held
    // until the caching read waits on it.
    let entered = Arc::new(Barrier::new(2));
    {
        let (entered, cache) = (Arc::clone(&entered), Arc::clone(&cache));
        let first = Arc::new(AtomicUsize::new(0));
        injector.hook_read_at(
            "blobs",
            0,
            Arc::new(move || {
                if first.fetch_add(1, Ordering::SeqCst) == 0 {
                    entered.wait();
                    let deadline = Instant::now() + Duration::from_secs(10);
                    while cache.load_waiters() < 1 && Instant::now() < deadline {
                        std::thread::yield_now();
                    }
                }
            }),
        );
    }
    let leader = {
        let tree = tree.clone();
        std::thread::spawn(move || tree.get([0], SeqNo::MAX))
    };
    entered.wait();
    blob.index
        .update_runtime_config(|cfg| cfg.blob_cache_fill = crate::config::BlobCacheFill::Always)?;
    let caching = {
        let tree = tree.clone();
        std::thread::spawn(move || tree.get([0], SeqNo::MAX))
    };
    assert_eq!(
        caching.join().expect("reader")?.as_deref(),
        Some(&value(0)[..])
    );
    assert_eq!(
        leader.join().expect("reader")?.as_deref(),
        Some(&value(0)[..])
    );

    let reads = tree.metrics().blob_read_count();
    assert_eq!(tree.get([0], SeqNo::MAX)?.as_deref(), Some(&value(0)[..]));
    assert_eq!(
        tree.metrics().blob_read_count(),
        reads,
        "the caching read cached the value it shared"
    );
    Ok(())
}

/// Reads that miss the same cold separated value at once read its record once
/// and every one gets the value.
#[test]
fn simultaneous_cold_reads_of_one_blob_read_it_once() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    // One key, so its record is the blob file's first, at offset 0.
    write_keys(
        config(folder.path()).with_kv_separation(Some(
            crate::KvSeparationOptions::default().separation_threshold(1),
        )),
        1,
    )?;
    let fs = FaultFs::new(StdFs);
    let injector = fs.injector();
    let cache = sharing_cache(64 << 20);
    let tree = reopen_cold(folder.path(), fs, &cache, true)?;
    // Warm the index side, which resolves no value, so the readers meet only
    // at the blob read.
    assert!(tree.contains_key([0], SeqNo::MAX)?);
    let (reads, hook) = hold_until_joined(&cache, usize::from(READERS) - 1);
    injector.hook_read_at("blobs", 0, hook);

    let barrier = Arc::new(Barrier::new(usize::from(READERS)));
    let readers: Vec<_> = (0..READERS)
        .map(|_| {
            let (tree, barrier) = (tree.clone(), Arc::clone(&barrier));
            std::thread::spawn(move || {
                barrier.wait();
                tree.get([0], SeqNo::MAX)
            })
        })
        .collect();
    for reader in readers {
        assert_eq!(
            reader.join().expect("reader")?.as_deref(),
            Some(&value(0)[..])
        );
    }
    assert_eq!(reads.load(Ordering::SeqCst), 1, "one read of the record");
    assert_eq!(cache.loads_in_flight(), 0, "no load stays registered");
    #[cfg(feature = "metrics")]
    assert_eq!(
        tree.metrics().blob_load_shared_count(),
        usize::from(READERS) - 1
    );
    Ok(())
}

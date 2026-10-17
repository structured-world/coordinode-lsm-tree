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

/// A blocking multi-get over a table whose filter and index blocks are not
/// pinned leads the loads of those blocks as well, and publishes each one as
/// it is supplied: every value is right and no load stays registered.
#[test]
fn a_multi_get_leads_and_publishes_its_filter_and_index_loads() -> crate::Result<()> {
    use crate::config::PinningPolicy;
    let folder = tempfile::tempdir()?;
    write_keys(config(folder.path()), 64)?;
    let cache = sharing_cache(64 << 20);
    let tree = config(folder.path())
        .with_fs(FaultFs::new(StdFs))
        .use_cache(Arc::clone(&cache))
        .filter_block_pinning_policy(PinningPolicy::all(false))
        .index_block_pinning_policy(PinningPolicy::all(false))
        .open()?;
    let values = tree.multi_get((0..64u8).map(|key| [key]), SeqNo::MAX)?;
    for (key, got) in (0..64u8).zip(values) {
        assert_eq!(got.as_deref(), Some(&value(key)[..]), "key {key}");
    }
    assert_eq!(cache.loads_in_flight(), 0, "no load stays registered");
    Ok(())
}

/// A point read waiting on a block a multi-get decodes past the room it
/// keeps for its blocks takes the block and caches it, as it would have
/// reading the block itself: a later read of the block finds it cached.
#[test]
fn a_waiting_point_read_caches_a_block_a_multi_get_had_no_room_for() -> crate::Result<()> {
    use crate::table::block::BlockType;
    let folder = tempfile::tempdir()?;
    write_keys(config(folder.path()), 64)?;
    let cache = sharing_cache(64 << 20);
    let tree = reopen_cold(folder.path(), FaultFs::new(StdFs), &cache, false)?;
    let table = {
        let version = tree.current_version();
        let Some(table) = version.iter_tables().next().cloned() else {
            panic!("the flush wrote a table");
        };
        table
    };
    let keyed = {
        use crate::table::block_index::BlockIndex as _;
        table
            .block_index
            .iter()
            .find_map(Result::ok)
            .expect("a non-empty table has a data block")
    };
    let handle = crate::table::BlockHandle::new(keyed.offset(), keyed.size());
    let bytes = {
        let file = std::fs::read(&*table.path)?;
        let start = usize::try_from(*handle.offset()).expect("offset fits");
        file.get(start..start + handle.size() as usize)
            .expect("the block lies in the table file")
            .to_vec()
    };
    let Some(lead) = table.claim_batched_block(&handle, BlockType::Data, true) else {
        panic!("the multi-get leads the block's load");
    };
    let point = std::thread::spawn(move || tree.get([0], SeqNo::MAX));
    let deadline = Instant::now() + Duration::from_secs(10);
    while cache.load_waiters() < 1 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert_eq!(cache.load_waiters(), 1, "the point read waits on the load");
    // No room left: the multi-get does not cache the block itself.
    table.decode_data_block_keeping(&bytes, &handle, &mut 0, Some(lead))?;
    assert_eq!(
        point.join().expect("reader")?.as_deref(),
        Some(&value(0)[..])
    );
    assert!(
        cache.has_block(table.global_id(), handle.offset()),
        "the waiting point read cached the block"
    );
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
    const NARROW: u64 = 30;
    let folder = tempfile::tempdir()?;
    let fs = FaultFs::new(StdFs);
    let injector = fs.injector();
    let (table, path, offset) = partial_table(folder.path(), fs)?;
    let cache = Arc::clone(&table.cache);
    // Inside the first block yet past most of it, so decoding up to it
    // promotes the block: a range reaching past the block is not decoded in
    // part at all.
    let wide: u64 = {
        let handle = table
            .untraced_index_walk()
            .next()
            .expect("the table has a data block")?;
        let last = std::str::from_utf8(&handle.end_key()[..])
            .ok()
            .and_then(|key| key.strip_prefix("key-"))
            .and_then(|number| number.parse::<u64>().ok())
            .expect("the fixture's keys are numbered");
        last * 97 / 100
    };

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
    let wide_read = {
        let table = table.clone();
        std::thread::spawn(move || keys_below(&table, wide))
    };
    entered.wait();
    let narrow = std::thread::spawn(move || keys_below(&table, NARROW));
    assert_eq!(narrow.join().expect("reader")?, expected_below(NARROW));
    assert_eq!(wide_read.join().expect("reader")?, expected_below(wide));
    assert_eq!(
        reads.load(Ordering::SeqCst),
        2,
        "the wide read's frame and its full load, and nothing for the narrow one"
    );
    assert_eq!(cache.loads_in_flight(), 0);
    Ok(())
}

/// The growth of a block's partial prefix is timed into the shard its
/// growths coordinate in, so bounded scans alone switch their coordination
/// on once their loads are slow.
#[cfg(feature = "zstd")]
#[test]
fn a_partial_prefix_growth_is_timed_in_its_own_shard() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let (table, _, offset) = partial_table(folder.path(), FaultFs::new(StdFs))?;
    let (cache, id) = (Arc::clone(&table.cache), table.global_id());
    let keys = std::thread::spawn(move || {
        crate::inflight::override_load_sampling_on_this_thread(Some(true));
        keys_below(&table, 30)
    })
    .join()
    .expect("reader")?;
    assert_eq!(keys, expected_below(30));
    assert!(
        cache.partial_growth_estimate(id, offset) > 0,
        "the growth's load was timed"
    );
    Ok(())
}

/// A bounded range missing a cold block that another read is loading whole
/// takes that load's block, which covers any query, rather than reading the
/// frame again for a prefix of it.
#[cfg(feature = "zstd")]
#[test]
fn a_bounded_range_takes_a_full_load_in_flight_rather_than_a_prefix() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let fs = FaultFs::new(StdFs);
    let injector = fs.injector();
    let (table, path, offset) = partial_table(folder.path(), fs)?;
    let cache = Arc::clone(&table.cache);

    // The full load's read is held until the bounded range waits on it.
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
    // An unbounded scan loads its first block whole.
    let full = {
        let table = table.clone();
        std::thread::spawn(move || {
            table
                .range_iter::<core::ops::RangeFull>(..)
                .next()
                .transpose()
                .map(|kv| kv.map(|kv| kv.key.user_key))
        })
    };
    entered.wait();
    let bounded = std::thread::spawn(move || keys_below(&table, 30));
    assert_eq!(bounded.join().expect("reader")?, expected_below(30));
    assert_eq!(
        full.join().expect("reader")?,
        expected_below(1).into_iter().next()
    );
    assert_eq!(
        reads.load(Ordering::SeqCst),
        1,
        "one read of the block, the full load's"
    );
    assert_eq!(cache.loads_in_flight(), 0);
    Ok(())
}

/// The pages a read fetches under a cached directory are timed into the
/// shards their own loads coordinate in, so reads of a group whose directory
/// is warm switch their coordination on once its page loads are slow.
#[cfg(feature = "columnar")]
#[test]
fn page_loads_under_a_cached_directory_are_timed() -> crate::Result<()> {
    use crate::config::BlockSizePolicy;
    let key = |i: u16| i.to_be_bytes();
    let folder = tempfile::tempdir()?;
    {
        let crate::AnyTree::Standard(tree) = config(folder.path())
            .filter_policy(crate::config::FilterPolicy::disabled())
            .columnar_row_group_size_policy(BlockSizePolicy::all(128 * 1_024))
            .columnar_page_size_policy(BlockSizePolicy::all(4 * 1_024))
            .open()?
        else {
            unreachable!("standard tree configured");
        };
        tree.update_runtime_config(|cfg| cfg.columnar = true)?;
        for i in 0..200u16 {
            tree.insert(key(i), [b'v'; 4_096], u64::from(i));
        }
        tree.flush_active_memtable(0)?;
    }
    let cache = Arc::new(Cache::with_capacity_bytes(64 << 20).with_row_cache(false));
    let tree = reopen_cold(folder.path(), FaultFs::new(StdFs), &cache, false)?;
    let read = |i: u16, timed: bool| {
        let tree = tree.clone();
        std::thread::spawn(move || {
            crate::inflight::override_load_sampling_on_this_thread(Some(timed));
            tree.get(key(i), SeqNo::MAX)
        })
        .join()
        .expect("reader")
    };
    // Untimed: the directory, the index and the key page are cached.
    assert_eq!(read(7, false)?.as_deref(), Some(&[b'v'; 4_096][..]));
    assert_eq!(cache.load_estimates_total(), 0);
    // A row of the same group on another row page: only that page loads.
    assert_eq!(read(20, true)?.as_deref(), Some(&[b'v'; 4_096][..]));
    assert!(
        cache.load_estimates_total() > 0,
        "the page's load was timed"
    );
    Ok(())
}

/// A maintenance read paced by a rate budget may wait on it in the middle of
/// its read: it leads no load a foreground miss of the same block would wait
/// behind, and the wait feeds no load-time estimate.
#[cfg(feature = "std")]
#[test]
fn a_paced_maintenance_load_holds_up_no_foreground_miss() -> crate::Result<()> {
    use crate::table::util::ReadPacer;
    use std::sync::atomic::AtomicBool;

    /// Stands for a rate budget: holds the read until it is released.
    struct Budget {
        entered: AtomicBool,
        released: AtomicBool,
    }
    impl ReadPacer for Budget {
        fn active(&self) -> bool {
            true
        }
        fn pace(&self, _offset: u64, _len: u64) {
            self.entered.store(true, Ordering::SeqCst);
            let deadline = Instant::now() + Duration::from_secs(10);
            while !self.released.load(Ordering::SeqCst) && Instant::now() < deadline {
                std::thread::yield_now();
            }
        }
    }

    let folder = tempfile::tempdir()?;
    write_keys(config(folder.path()), 64)?;
    let cache = sharing_cache(64 << 20);
    let tree = reopen_cold(folder.path(), FaultFs::new(StdFs), &cache, false)?;
    let version = tree.current_version();
    let Some(table) = version.iter_tables().next() else {
        panic!("the flush wrote a table");
    };
    let handle = {
        use crate::table::block_index::BlockIndex as _;
        let Some(keyed) = table.block_index.iter().find_map(Result::ok) else {
            panic!("a non-empty table has a data block");
        };
        *keyed.as_ref()
    };
    let budget = Budget {
        entered: AtomicBool::new(false),
        released: AtomicBool::new(false),
    };
    let (held_up, foreground, paced) = std::thread::scope(|scope| {
        let paced = scope.spawn(|| {
            crate::inflight::override_load_sampling_on_this_thread(Some(true));
            table.load_data_block_for_maintenance(&handle, &budget)
        });
        let deadline = Instant::now() + Duration::from_secs(10);
        while !budget.entered.load(Ordering::SeqCst) && Instant::now() < deadline {
            std::thread::yield_now();
        }
        let foreground = scope.spawn(|| {
            crate::inflight::override_load_sampling_on_this_thread(Some(false));
            tree.get([0], SeqNo::MAX)
        });
        while !foreground.is_finished() && cache.load_waiters() == 0 && Instant::now() < deadline {
            std::thread::yield_now();
        }
        let held_up = cache.load_waiters() > 0;
        budget.released.store(true, Ordering::SeqCst);
        (
            held_up,
            foreground.join().expect("foreground"),
            paced.join().expect("maintenance"),
        )
    });
    assert!(!held_up, "the foreground miss waited on the paced read");
    assert_eq!(foreground?.as_deref(), Some(&value(0)[..]));
    assert!(paced?.is_some(), "the paced read loads the block");
    assert_eq!(cache.load_estimates_total(), 0, "the paced wait was timed");
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

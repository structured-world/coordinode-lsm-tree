// A multi-get reads a level stage by stage across its tables: the filter blocks
// of every table in one batch, then their index blocks, then their data
// blocks, instead of walking each table's filter and index before the next.

use lsm_tree::{
    AbstractTree, AnyTree, Cache, Config, SeqNo, SequenceNumberCounter, Slice,
    config::{BlockSizePolicy, CompressionPolicy, LocatorPolicy, PinningPolicy},
    fs::{
        BlockRead, Fs, FsDirEntry, FsFile, FsMetadata, FsOpenOptions, QueuedRead, ReadDone,
        ReadQueue, StdFs,
    },
    io,
};
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

/// The batched reads a backend was asked for, per call: its request count and
/// the bytes those requests asked for.
#[derive(Default)]
struct Batches {
    calls: Vec<(usize, u64)>,
    /// The call, counted from the last reset, the backend refuses.
    refuse: Option<usize>,
}

/// A shared handle on a [`StageFs`]'s record.
#[derive(Clone, Default)]
struct Record(Arc<Mutex<Batches>>);

impl Record {
    fn lock(&self) -> std::sync::MutexGuard<'_, Batches> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Forgets the calls so far and sets which call from now on is refused.
    fn reset(&self, refuse: Option<usize>) {
        let mut batches = self.lock();
        batches.calls.clear();
        batches.refuse = refuse;
    }

    fn calls(&self) -> Vec<(usize, u64)> {
        self.lock().calls.clone()
    }
}

/// A backend recording every batched read, delegating to [`StdFs`].
struct StageFs(Record);

impl Fs for StageFs {
    fn read_blocks_batched(&self, reqs: &mut [BlockRead<'_>]) -> io::Result<()> {
        let refused = {
            let mut batches = self.0.lock();
            let bytes = reqs.iter().map(|r| r.buf.capacity() as u64).sum();
            batches.calls.push((reqs.len(), bytes));
            batches.refuse == Some(batches.calls.len() - 1)
        };
        if refused {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "batched read refused by the test backend",
            ));
        }
        StdFs.read_blocks_batched(reqs)
    }

    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
        StdFs.open(path, opts)
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        StdFs.create_dir_all(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<FsDirEntry>> {
        StdFs.read_dir(path)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        StdFs.remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        StdFs.remove_dir_all(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        StdFs.rename(from, to)
    }

    fn metadata(&self, path: &Path) -> io::Result<FsMetadata> {
        StdFs.metadata(path)
    }

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        StdFs.sync_directory(path)
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        StdFs.exists(path)
    }
}

/// How a table lays out the blocks a point read goes through.
#[derive(Clone, Copy, Debug)]
enum Shape {
    /// One filter block and one index block per table.
    Whole,
    /// Partitioned filter and index, with small partitions and no locator, so
    /// a read goes through a filter partition and an index partition.
    Partitioned,
}

const SHAPES: [Shape; 2] = [Shape::Whole, Shape::Partitioned];

const LARGE_CACHE: u64 = 64 * 1_024 * 1_024;

/// A tree of `tables` level-0 tables, each holding its own key range, with
/// filters and indexes read on demand rather than pinned, reopened cold with a
/// cache of `cache_bytes`; and the keys a batch reads: two present and one
/// absent per table.
fn tree(
    dir: &Path,
    shape: Shape,
    tables: u32,
    cache_bytes: u64,
    record: &Record,
) -> lsm_tree::Result<(AnyTree, Vec<String>)> {
    tree_on(
        dir,
        shape,
        tables,
        cache_bytes,
        &(Arc::new(StageFs(record.clone())) as Arc<dyn Fs>),
    )
}

/// [`tree`] on the backend `fs`.
fn tree_on(
    dir: &Path,
    shape: Shape,
    tables: u32,
    cache_bytes: u64,
    fs: &Arc<dyn Fs>,
) -> lsm_tree::Result<(AnyTree, Vec<String>)> {
    let config = || {
        let config = Config::new(
            dir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(Arc::clone(fs))
        .use_cache(Arc::new(Cache::with_capacity_bytes(cache_bytes)))
        .filter_block_pinning_policy(PinningPolicy::all(false))
        .index_block_pinning_policy(PinningPolicy::all(false))
        .data_block_compression_policy(CompressionPolicy::all(lsm_tree::CompressionType::None))
        .index_block_compression_policy(CompressionPolicy::all(lsm_tree::CompressionType::None));
        match shape {
            Shape::Whole => config,
            Shape::Partitioned => config
                .filter_block_partitioning_policy(PinningPolicy::all(true))
                .index_block_partitioning_policy(PinningPolicy::all(true))
                .filter_block_partition_size_policy(BlockSizePolicy::all(128))
                .index_block_partition_size_policy(BlockSizePolicy::all(128))
                .data_block_size_policy(BlockSizePolicy::all(1_024))
                .locator_policy(LocatorPolicy::disabled()),
        }
    };
    {
        let tree = config().open()?;
        let mut seqno = 0;
        for table in 0..tables {
            for row in 0..200u32 {
                tree.insert(format!("t{table:03}r{row:04}"), vec![b'v'; 64], seqno);
                seqno += 1;
            }
            tree.flush_active_memtable(0)?;
        }
    }
    let keys = (0..tables)
        .flat_map(|table| {
            [
                format!("t{table:03}r0010"),
                format!("t{table:03}r0150"),
                format!("t{table:03}r0150x"),
            ]
        })
        .collect();
    Ok((config().open()?, keys))
}

/// The values a key-by-key read returns for `keys`.
fn one_by_one(tree: &AnyTree, keys: &[String]) -> lsm_tree::Result<Vec<Option<Slice>>> {
    keys.iter().map(|key| tree.get(key, SeqNo::MAX)).collect()
}

/// Every block byte the read charged went through a batch: nothing was read
/// table by table on the side, and no block twice.
#[cfg(feature = "metrics")]
fn assert_batches_carry_every_read(tree: &AnyTree, calls: &[(usize, u64)], what: &str) {
    let metrics = tree.metrics();
    let charged = metrics.filter_block_io() + metrics.index_block_io() + metrics.data_block_io();
    let batched: u64 = calls.iter().map(|&(_, bytes)| bytes).sum();
    assert_eq!(charged, batched, "{what}: block bytes read outside a batch");
}

/// A cold level is read in as many batches whatever its table count: each
/// stage asks for the blocks of every table in one batch. A per-table walk
/// makes its filter and index reads one table after another, so its count
/// grows with the tables.
#[test]
fn a_cold_level_is_read_in_as_many_batches_whatever_its_table_count() -> lsm_tree::Result<()> {
    for shape in SHAPES {
        let mut depth = None;
        for tables in [1u32, 4, 16] {
            let dir = tempfile::tempdir()?;
            let record = Record::default();
            let (tree, keys) = tree(dir.path(), shape, tables, LARGE_CACHE, &record)?;
            record.reset(None);

            let values = tree.multi_get(&keys, SeqNo::MAX)?;
            let calls = record.calls();
            let what = format!("{shape:?}, {tables} tables");
            #[cfg(feature = "metrics")]
            {
                assert_batches_carry_every_read(&tree, &calls, &what);
                assert!(tree.metrics().filter_block_io() > 0, "{what}: filters read");
                if let Shape::Partitioned = shape {
                    assert!(tree.metrics().index_block_io() > 0, "{what}: indexes read");
                }
            }
            assert!(
                calls.len() >= 2,
                "{what}: a metadata stage and data, got {calls:?}"
            );
            assert!(
                calls[0].0 >= tables as usize,
                "{what}: the first stage asks every table at once, got {calls:?}"
            );
            match depth {
                None => depth = Some(calls.len()),
                Some(depth) => assert_eq!(calls.len(), depth, "{what}: {calls:?}"),
            }

            assert_eq!(values, one_by_one(&tree, &keys)?, "{what}");
            assert_eq!(
                values.iter().filter(|v| v.is_some()).count(),
                2 * tables as usize,
                "{what}: the present keys resolve, the absent ones do not"
            );
        }
    }
    Ok(())
}

/// A warm level reads nothing: the filter and index blocks the first read
/// fetched, and the data blocks it kept, are all taken from the cache.
#[test]
fn a_warm_level_reads_nothing() -> lsm_tree::Result<()> {
    for shape in SHAPES {
        let dir = tempfile::tempdir()?;
        let record = Record::default();
        let (tree, keys) = tree(dir.path(), shape, 4, LARGE_CACHE, &record)?;
        let cold = tree.multi_get(&keys, SeqNo::MAX)?;
        record.reset(None);

        let warm = tree.multi_get(&keys, SeqNo::MAX)?;
        let calls = record.calls();
        assert!(
            calls.is_empty(),
            "{shape:?}: a warm level asked for {calls:?}"
        );
        assert_eq!(cold, warm, "{shape:?}");
    }
    Ok(())
}

/// With a cache that keeps nothing, the read answers from the blocks its
/// stages fetched, each read once: going back to the cache for a block the
/// read just fetched would find it gone and read it again.
#[test]
fn a_level_is_answered_from_what_it_read_when_the_cache_keeps_nothing() -> lsm_tree::Result<()> {
    for shape in SHAPES {
        let dir = tempfile::tempdir()?;
        let record = Record::default();
        let (tree, keys) = tree(dir.path(), shape, 4, 0, &record)?;
        record.reset(None);

        let values = tree.multi_get(&keys, SeqNo::MAX)?;
        let calls = record.calls();
        assert!(
            calls.first().is_some_and(|&(requests, _)| requests >= 4),
            "{shape:?}: the first stage asks every table at once, got {calls:?}"
        );
        #[cfg(feature = "metrics")]
        assert_batches_carry_every_read(&tree, &calls, &format!("{shape:?}"));

        assert_eq!(values, one_by_one(&tree, &keys)?, "{shape:?}");
    }
    Ok(())
}

/// A batch the backend refuses, at any stage, leaves the tables it was for to
/// the serial path, which reads the same blocks one by one: the answer is
/// unchanged.
#[test]
fn a_refused_stage_still_answers_the_query() -> lsm_tree::Result<()> {
    for shape in SHAPES {
        let depth = {
            let dir = tempfile::tempdir()?;
            let record = Record::default();
            let (tree, keys) = tree(dir.path(), shape, 4, LARGE_CACHE, &record)?;
            record.reset(None);
            tree.multi_get(&keys, SeqNo::MAX)?;
            record.calls().len()
        };
        for refused in 0..depth {
            let dir = tempfile::tempdir()?;
            let record = Record::default();
            let (tree, keys) = tree(dir.path(), shape, 4, LARGE_CACHE, &record)?;
            record.reset(Some(refused));

            let values = tree.multi_get(&keys, SeqNo::MAX)?;
            let what = format!("{shape:?}, batch {refused} of {depth} refused");
            assert!(
                record.calls().len() > refused,
                "{what}: the batch was asked"
            );
            assert_eq!(values, one_by_one(&tree, &keys)?, "{what}");
            assert_eq!(
                values.iter().filter(|v| v.is_some()).count(),
                8,
                "{what}: every present key resolves"
            );
        }
    }
    Ok(())
}

/// What a [`SlowFirstFs`] saw: the reads submitted before its queue was first
/// waited on, and the reads submitted by the time the first of them finished.
#[derive(Default)]
struct SlowFirst {
    initial: Option<usize>,
    submitted_when_first_finished: Option<usize>,
}

/// A backend whose read queue holds the first read it is given until no other
/// read is left, and finishes the others one per wait, newest first: a slow
/// file in the same stage as fast ones.
struct SlowFirstFs(Arc<Mutex<SlowFirst>>);

/// The queue of a [`SlowFirstFs`].
struct SlowFirstQueue {
    seen: Arc<Mutex<SlowFirst>>,
    /// Reads not yet finished, each with its submission number.
    pending: Vec<(usize, QueuedRead)>,
    submitted: usize,
}

impl ReadQueue for SlowFirstQueue {
    fn submit(&mut self, read: QueuedRead) {
        self.pending.push((self.submitted, read));
        self.submitted += 1;
    }

    fn outstanding(&self) -> usize {
        self.pending.len()
    }

    fn wait(&mut self, min: usize, on_done: &mut dyn FnMut(ReadDone)) {
        {
            let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
            seen.initial.get_or_insert(self.submitted);
        }
        let mut handed = 0;
        while handed < min && !self.pending.is_empty() {
            let pick = self
                .pending
                .iter()
                .rposition(|&(number, _)| number != 0)
                .unwrap_or(0);
            let (number, mut read) = self.pending.remove(pick);
            if number == 0 {
                let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
                seen.submitted_when_first_finished = Some(self.submitted);
            }
            let want = read.buf.len();
            let result = match read.file.read_at(&mut read.buf, read.offset) {
                Ok(n) if n == want => Ok(()),
                Ok(_) => Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read")),
                Err(error) => Err(error),
            };
            on_done(ReadDone {
                tag: read.tag,
                buf: read.buf,
                result,
            });
            handed += 1;
        }
    }
}

impl Fs for SlowFirstFs {
    fn read_queue(&self) -> Box<dyn ReadQueue + '_> {
        Box::new(SlowFirstQueue {
            seen: Arc::clone(&self.0),
            pending: Vec::new(),
            submitted: 0,
        })
    }

    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
        StdFs.open(path, opts)
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        StdFs.create_dir_all(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<FsDirEntry>> {
        StdFs.read_dir(path)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        StdFs.remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        StdFs.remove_dir_all(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        StdFs.rename(from, to)
    }

    fn metadata(&self, path: &Path) -> io::Result<FsMetadata> {
        StdFs.metadata(path)
    }

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        StdFs.sync_directory(path)
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        StdFs.exists(path)
    }
}

/// A table whose blocks are back asks for its next stage while a slower file
/// of the same stage is still being read: the first stage's reads are not a
/// barrier the whole level waits behind.
#[test]
fn a_table_moves_on_while_a_slower_file_of_its_stage_is_read() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let seen = Arc::new(Mutex::new(SlowFirst::default()));
    let fs: Arc<dyn Fs> = Arc::new(SlowFirstFs(Arc::clone(&seen)));
    // No cache, so every table's index is read in a stage after its filter.
    let (tree, keys) = tree_on(dir.path(), Shape::Whole, 4, 0, &fs)?;
    *seen.lock().unwrap_or_else(PoisonError::into_inner) = SlowFirst::default();

    let values = tree.multi_get(&keys, SeqNo::MAX)?;
    let seen = seen.lock().unwrap_or_else(PoisonError::into_inner);
    let initial = seen.initial.expect("the level was read through the queue");
    let submitted = seen
        .submitted_when_first_finished
        .expect("the held read finished");
    assert!(
        submitted > initial,
        "the tables whose first stage was back waited for the slow file: \
         {initial} reads before the first wait, still {submitted} when it finished"
    );
    drop(seen);
    assert_eq!(values, one_by_one(&tree, &keys)?);
    Ok(())
}

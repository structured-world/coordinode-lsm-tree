// A resumable multi-get never does I/O on the thread that drives it: every
// file it opens and every block it reads is opened and read by whoever runs
// its jobs and its block reads. With a cache that keeps nothing it reads no
// block twice, and it answers what the blocking multi-get answers, for every
// table format a level can hold.

use lsm_tree::fs::{Fs, FsDirEntry, FsFile, FsMetadata, FsOpenOptions, StdFs};
use lsm_tree::resumable::Step;
use lsm_tree::{
    AbstractTree, AnyTree, Cache, Config, MergeOperator, SeqNo, SequenceNumberCounter, UserValue,
    config::{BlockSizePolicy, LocatorPolicy, PinningPolicy},
    io,
    runtime_config::RuntimeConfig,
};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::ThreadId;

/// What the backend saw, while recording.
#[derive(Default)]
struct Trace {
    on: bool,
    /// The thread of every open and every read.
    threads: Vec<ThreadId>,
    /// `(file, offset, length)` of every positioned read.
    reads: Vec<(PathBuf, u64, usize)>,
}

#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Trace>>);

impl Recorder {
    fn lock(&self) -> std::sync::MutexGuard<'_, Trace> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn start(&self) {
        let mut trace = self.lock();
        *trace = Trace::default();
        trace.on = true;
    }

    fn stop(&self) -> Trace {
        let mut trace = self.lock();
        trace.on = false;
        core::mem::take(&mut *trace)
    }

    fn saw(&self, read: Option<(PathBuf, u64, usize)>) {
        let mut trace = self.lock();
        if trace.on {
            trace.threads.push(std::thread::current().id());
            trace.reads.extend(read);
        }
    }
}

/// [`StdFs`], recording every open and read while its recorder is on.
struct RecordingFs(Recorder);

/// A [`StdFs`] file whose reads its backend records.
struct RecordingFile {
    inner: Box<dyn FsFile>,
    path: PathBuf,
    recorder: Recorder,
}

impl std::io::Read for RecordingFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.recorder.saw(None);
        self.inner.read(buf)
    }
}

impl std::io::Write for RecordingFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl std::io::Seek for RecordingFile {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}

impl FsFile for RecordingFile {
    fn sync_all(&self) -> io::Result<()> {
        self.inner.sync_all()
    }

    fn sync_data(&self) -> io::Result<()> {
        self.inner.sync_data()
    }

    fn metadata(&self) -> io::Result<FsMetadata> {
        self.inner.metadata()
    }

    fn hard_link_count(&self) -> io::Result<u64> {
        self.inner.hard_link_count()
    }

    fn set_len(&self, size: u64) -> io::Result<()> {
        self.inner.set_len(size)
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.recorder
            .saw(Some((self.path.clone(), offset, buf.len())));
        self.inner.read_at(buf, offset)
    }

    fn lock_exclusive(&self) -> io::Result<()> {
        self.inner.lock_exclusive()
    }

    fn try_lock_exclusive(&self) -> io::Result<bool> {
        self.inner.try_lock_exclusive()
    }
}

impl Fs for RecordingFs {
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
        self.0.saw(None);
        Ok(Box::new(RecordingFile {
            inner: StdFs.open(path, opts)?,
            path: path.to_path_buf(),
            recorder: self.0.clone(),
        }))
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

/// Drives a read from the calling thread, running every job and every block
/// read it hands out on another thread, as a caller with a pool would.
fn drive_on_a_pool(mut step: Step) -> lsm_tree::Result<Vec<Option<UserValue>>> {
    loop {
        let mut read = match step {
            Step::Done(values) => return values,
            Step::Pending(read) => read,
        };
        let jobs = read.take_jobs();
        let reads = read.take_reads();
        let (outcomes, blocks) = std::thread::scope(|pool| {
            pool.spawn(move || {
                let outcomes: Vec<_> = jobs
                    .into_iter()
                    .map(lsm_tree::resumable::ReadJob::run)
                    .collect();
                let blocks: Vec<_> = reads
                    .into_iter()
                    .map(|mut block| {
                        let result =
                            block
                                .file
                                .read_at(&mut block.buf, block.offset)
                                .and_then(|n| {
                                    if n == block.buf.len() {
                                        Ok(())
                                    } else {
                                        Err(io::ErrorKind::UnexpectedEof.into())
                                    }
                                });
                        (block.tag, result, block.buf)
                    })
                    .collect();
                (outcomes, blocks)
            })
            .join()
            .unwrap_or_else(|_| panic!("the pool thread panicked"))
        });
        for outcome in outcomes {
            read.complete_job(outcome);
        }
        for (tag, result, buf) in blocks {
            read.complete_read(tag, result, buf);
        }
        step = read.resume();
    }
}

/// Sums 8-byte little-endian operands onto an 8-byte base.
struct CounterMerge;

impl MergeOperator for CounterMerge {
    fn merge(
        &self,
        _key: &[u8],
        base_value: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> lsm_tree::Result<UserValue> {
        let read = |bytes: &[u8]| -> lsm_tree::Result<i64> {
            Ok(i64::from_le_bytes(
                bytes
                    .try_into()
                    .map_err(|_| lsm_tree::Error::MergeOperator)?,
            ))
        };
        let mut total = base_value.map_or(Ok(0), read)?;
        for operand in operands {
            total += read(operand)?;
        }
        Ok(total.to_le_bytes().to_vec().into())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    /// Row tables over two levels.
    Plain,
    /// Partitioned filters and indexes.
    Partitioned,
    /// Page-ECC tables.
    #[cfg(feature = "page_ecc")]
    Ecc,
    /// Columnar tables.
    #[cfg(feature = "columnar")]
    Columnar,
    /// One level holding a row, a Page-ECC and a columnar table.
    #[cfg(all(feature = "page_ecc", feature = "columnar"))]
    Mixed,
    /// Merge operands over base values.
    Merge,
}

const SHAPES: &[Shape] = &[
    Shape::Plain,
    Shape::Partitioned,
    #[cfg(feature = "page_ecc")]
    Shape::Ecc,
    #[cfg(feature = "columnar")]
    Shape::Columnar,
    #[cfg(all(feature = "page_ecc", feature = "columnar"))]
    Shape::Mixed,
    Shape::Merge,
];

/// The table formats each flush of a shape writes.
fn formats(shape: Shape) -> Vec<(bool, bool)> {
    match shape {
        #[cfg(feature = "page_ecc")]
        Shape::Ecc => vec![(true, false), (true, false)],
        #[cfg(feature = "columnar")]
        Shape::Columnar => vec![(false, true), (false, true)],
        #[cfg(all(feature = "page_ecc", feature = "columnar"))]
        Shape::Mixed => vec![(false, false), (true, false), (false, true)],
        _ => vec![(false, false), (false, false)],
    }
}

/// A tree of `shape` on a recording backend with a cache that keeps
/// nothing, and the batch to read from it: present keys, absent ones, a
/// key asked twice.
fn tree(
    folder: &Path,
    shape: Shape,
    recorder: &Recorder,
) -> lsm_tree::Result<(AnyTree, Vec<String>)> {
    let mut config = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(RecordingFs(recorder.clone())))
    .use_cache(Arc::new(Cache::with_capacity_bytes(0)))
    .filter_block_pinning_policy(PinningPolicy::all(false))
    .index_block_pinning_policy(PinningPolicy::all(false));
    if shape == Shape::Partitioned {
        config = config
            .with_runtime_config({
                let mut runtime = RuntimeConfig::default();
                runtime.index_partition_spill_threshold = 0;
                runtime
            })
            .filter_block_partitioning_policy(PinningPolicy::all(true))
            .index_block_partitioning_policy(PinningPolicy::all(true))
            .filter_block_partition_size_policy(BlockSizePolicy::all(128))
            .index_block_partition_size_policy(BlockSizePolicy::all(128))
            .data_block_size_policy(BlockSizePolicy::all(1_024))
            .locator_policy(LocatorPolicy::disabled());
    }
    if shape == Shape::Merge {
        config = config.with_merge_operator(Some(Arc::new(CounterMerge)));
    }
    let any = config.open()?;
    let AnyTree::Standard(tree) = &any else {
        panic!("a standard tree");
    };

    let mut seqno = 0;
    for (flush, (ecc, columnar)) in formats(shape).into_iter().enumerate() {
        tree.update_runtime_config(|runtime| {
            #[cfg(feature = "page_ecc")]
            {
                runtime.page_ecc = ecc;
            }
            #[cfg(feature = "columnar")]
            {
                runtime.columnar = columnar;
            }
            let _ = (ecc, columnar, &runtime);
        })?;
        for i in (flush..1_500).step_by(2) {
            let key = format!("k{i:05}");
            if shape == Shape::Merge {
                if flush == 0 {
                    tree.insert(key, 10i64.to_le_bytes(), seqno);
                } else {
                    tree.merge(key, 1i64.to_le_bytes(), seqno);
                }
            } else {
                tree.insert(key, format!("v{flush}-{i}"), seqno);
            }
            seqno += 1;
        }
        tree.flush_active_memtable(0)?;
        if flush == 0 && shape != Shape::Partitioned {
            tree.major_compact(u64::MAX, 0)?;
        }
    }
    let mut keys: Vec<String> = (0..1_700).step_by(7).map(|i| format!("k{i:05}")).collect();
    keys.push("k00014".into());
    Ok((any, keys))
}

/// For every table format a level can hold, the resumable read answers what
/// the blocking read answers, and the thread that drives it opens and reads
/// nothing: every open and every read is the pool's.
#[test]
fn the_driving_thread_does_no_io_for_any_table_format() -> lsm_tree::Result<()> {
    for &shape in SHAPES {
        let folder = tempfile::tempdir()?;
        let recorder = Recorder::default();
        let (tree, keys) = tree(folder.path(), shape, &recorder)?;
        let blocking = tree.multi_get(&keys, SeqNo::MAX)?;

        let driver = std::thread::current().id();
        recorder.start();
        let resumable =
            drive_on_a_pool(tree.start_multi_get(keys.iter().map(String::as_str), SeqNo::MAX)?)?;
        let trace = recorder.stop();

        assert_eq!(resumable, blocking, "{shape:?}");
        assert!(resumable.iter().any(Option::is_some), "{shape:?}");
        assert!(!trace.threads.is_empty(), "{shape:?}: a cold read reads");
        assert!(
            trace.threads.iter().all(|thread| *thread != driver),
            "{shape:?}: the driving thread did I/O"
        );
    }
    Ok(())
}

/// With a cache that keeps nothing, so every block fetched is gone from it by
/// the next resume, the read answers from what it fetched: no block is read
/// twice.
#[test]
fn no_block_is_read_twice_when_the_cache_keeps_nothing() -> lsm_tree::Result<()> {
    for &shape in SHAPES {
        if shape == Shape::Merge {
            // A merge reads every version below the newest through a merging
            // iterator of its own, a read of the key's history rather than a
            // block the multi-get fetched.
            continue;
        }
        let folder = tempfile::tempdir()?;
        let recorder = Recorder::default();
        let (tree, keys) = tree(folder.path(), shape, &recorder)?;

        recorder.start();
        drive_on_a_pool(tree.start_multi_get(keys.iter().map(String::as_str), SeqNo::MAX)?)?;
        let mut reads = recorder.stop().reads;
        reads.sort();
        let mut twice: Vec<(u64, usize, usize)> = Vec::new();
        for group in reads.chunk_by(|a, b| a == b) {
            if let [(_, offset, len), _, ..] = group {
                twice.push((*offset, *len, group.len()));
            }
        }
        assert!(
            twice.is_empty(),
            "{shape:?}: blocks read more than once, (offset, length, times): {twice:?}"
        );
    }
    Ok(())
}

/// Reads under injected read failures, some dropped while their block reads
/// are still running on another thread: a dropped read never blocks the
/// thread dropping it, the reads still running finish into the buffers they
/// own, and a read that is not dropped answers what the tree holds, the
/// failure taken around rather than reported as a wrong answer.
#[test]
fn reads_dropped_mid_flight_under_injected_faults_stay_sound() -> lsm_tree::Result<()> {
    use lsm_tree::fs::{Fault, FaultFs, FaultOp, FaultRule};

    let folder = tempfile::tempdir()?;
    let fs = FaultFs::new(StdFs);
    let injector = fs.injector();
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(fs))
    .use_cache(Arc::new(Cache::with_capacity_bytes(0)))
    .filter_block_pinning_policy(PinningPolicy::all(false))
    .index_block_pinning_policy(PinningPolicy::all(false))
    .open()?;
    for table in 0..4u32 {
        for i in (table..1_200).step_by(4) {
            tree.insert(format!("k{i:05}"), format!("v{i}"), u64::from(i));
        }
        tree.flush_active_memtable(0)?;
    }
    let keys: Vec<String> = (0..1_300).step_by(9).map(|i| format!("k{i:05}")).collect();
    let expected = tree.multi_get(&keys, SeqNo::MAX)?;

    // A fixed xorshift sequence, so a failure replays.
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let mut next = move |bound: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % bound
    };
    let (mut dropped, mut answered) = (0, 0);
    for _ in 0..200 {
        injector.clear();
        injector.arm(
            FaultRule::new(FaultOp::ReadAt, Fault::Error(io::ErrorKind::Other))
                .on_path("tables")
                .skip(next(40))
                .once(),
        );
        // Half the reads run to their answer; the others are given up at a
        // round of their own, each with a chance of one in eight.
        let may_drop = next(2) == 0;
        let mut step = tree.start_multi_get(keys.iter().map(String::as_str), SeqNo::MAX)?;
        let answer = loop {
            let mut read = match step {
                Step::Done(values) => break Some(values),
                Step::Pending(read) => read,
            };
            for job in read.take_jobs() {
                let outcome = job.run();
                read.complete_job(outcome);
            }
            let reads = read.take_reads();
            let pool = std::thread::spawn(move || {
                reads
                    .into_iter()
                    .map(|mut block| {
                        let result =
                            block
                                .file
                                .read_at(&mut block.buf, block.offset)
                                .and_then(|n| {
                                    if n == block.buf.len() {
                                        Ok(())
                                    } else {
                                        Err(io::ErrorKind::UnexpectedEof.into())
                                    }
                                });
                        (block.tag, result, block.buf)
                    })
                    .collect::<Vec<_>>()
            });
            if may_drop && next(8) == 0 {
                // Given up while its reads are still running.
                drop(read);
                let finished = pool
                    .join()
                    .unwrap_or_else(|_| panic!("the pool thread panicked"));
                assert!(
                    finished
                        .iter()
                        .all(|(_, result, buf)| result.is_err() || !buf.is_empty()),
                    "a read still running finished into its own buffer"
                );
                break None;
            }
            for (tag, result, buf) in pool
                .join()
                .unwrap_or_else(|_| panic!("the pool thread panicked"))
            {
                read.complete_read(tag, result, buf);
            }
            step = read.resume();
        };
        if answer.is_none() {
            dropped += 1;
        }
        if let Some(answer) = answer {
            answered += 1;
            match answer {
                Ok(values) => assert_eq!(values, expected),
                // A fault a key-by-key resolve meets too is reported, not
                // turned into a wrong answer.
                Err(lsm_tree::Error::Io(_)) => {}
                Err(error) => panic!("an injected read failure surfaced as {error:?}"),
            }
        }
    }
    injector.clear();
    assert!(
        dropped > 0 && answered > 0,
        "both outcomes were exercised: {dropped} dropped, {answered} answered"
    );
    Ok(())
}

/// Dropping a read with its work out neither blocks nor takes back the
/// buffers it handed out: they are the caller's, and reading into them after
/// the drop is sound.
#[test]
fn dropping_a_suspended_read_leaves_its_buffers_to_the_caller() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let recorder = Recorder::default();
    let (tree, keys) = tree(folder.path(), Shape::Plain, &recorder)?;
    let Step::Pending(mut read) =
        tree.start_multi_get(keys.iter().map(String::as_str), SeqNo::MAX)?
    else {
        panic!("a cold read has work to hand out");
    };
    let mut reads = read.take_reads();
    let jobs = read.take_jobs();
    assert!(!reads.is_empty() || !jobs.is_empty());
    drop(read);
    for job in jobs {
        let _ = job.run();
    }
    for block in &mut reads {
        let read = block.file.read_at(&mut block.buf, block.offset)?;
        assert_eq!(read, block.buf.len());
    }
    Ok(())
}

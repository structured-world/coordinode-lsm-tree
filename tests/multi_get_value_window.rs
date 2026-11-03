// A multi-get of separated values keeps the bytes of the value reads it has
// out at once within a window, so each value is checked while what its read
// copied is still in the processor's cache; one value always goes out, so a
// value larger than the window is read alone rather than never.

use lsm_tree::{
    AbstractTree, Cache, CompressionType, Config, SeqNo, SequenceNumberCounter,
    config::{BlobCacheFill, CompressionPolicy, KvSeparationOptions},
    fs::{
        Fs, FsDirEntry, FsFile, FsMetadata, FsOpenOptions, QueuedRead, ReadDone, ReadQueue,
        ReadWake, StdFs,
    },
    io,
};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// A backend over [`StdFs`] whose queue reads what it holds when waited on,
/// recording the most bytes it held at once. With `fail_value` set, the first
/// read of a value (at least [`VALUE_READ`] bytes) fails instead; with
/// `short_block` set, the first read of a table block is reported done
/// without a byte read, as a backend breaking its contract would.
struct PeakFs {
    peak: Arc<AtomicUsize>,
    fail_value: Arc<AtomicBool>,
    short_block: Arc<AtomicBool>,
}

/// The queue of a [`PeakFs`].
struct PeakQueue {
    held: Vec<QueuedRead>,
    peak: Arc<AtomicUsize>,
    fail_value: Arc<AtomicBool>,
    short_block: Arc<AtomicBool>,
}

/// What a [`PeakFs`] does wrong once, if anything.
#[derive(Clone, Copy)]
enum Inject {
    Nothing,
    FailValue,
    ShortBlock,
}

/// The size from which a queued read is a value's: table blocks are smaller.
const VALUE_READ: usize = 1 << 20;

impl ReadQueue for PeakQueue {
    fn submit(&mut self, read: QueuedRead) {
        self.held.push(read);
    }

    fn outstanding(&self) -> usize {
        self.held.len()
    }

    fn wait(&mut self, min: usize, on_done: &mut dyn FnMut(ReadDone)) {
        if min == 0 {
            return;
        }
        let bytes: usize = self.held.iter().map(|read| read.buf.capacity()).sum();
        self.peak.fetch_max(bytes, Ordering::SeqCst);
        for mut read in core::mem::take(&mut self.held) {
            let value = read.buf.capacity() >= VALUE_READ;
            let result = if value && self.fail_value.swap(false, Ordering::SeqCst) {
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "injected value read failure",
                ))
            } else if !value && self.short_block.swap(false, Ordering::SeqCst) {
                Ok(())
            } else {
                read.buf.read_from(read.file.as_ref(), read.offset)
            };
            on_done(ReadDone {
                tag: read.tag,
                buf: read.buf,
                result,
            });
        }
    }

    fn set_wake(&mut self, _wake: Arc<dyn ReadWake>) -> bool {
        // Reads only while waited on, so a wake never comes.
        false
    }
}

impl Fs for PeakFs {
    fn read_queue(&self) -> Box<dyn ReadQueue + '_> {
        Box::new(PeakQueue {
            held: Vec::new(),
            peak: Arc::clone(&self.peak),
            fail_value: Arc::clone(&self.fail_value),
            short_block: Arc::clone(&self.short_block),
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

/// The window the read keeps its value reads out within.
const WINDOW: usize = 8 << 20;

/// The value of row `i`, `len` bytes.
fn value(i: usize, len: usize) -> Vec<u8> {
    let byte = u8::try_from(i % 251).unwrap_or_default();
    vec![byte; len]
}

/// Writes `rows` separated values of `len` bytes, multi-gets them all with no
/// cache, checks every value, and returns the most bytes the queue held at
/// once. `inject` is what the queue does wrong once, after the writes.
fn peak_of_multi_get(rows: usize, len: usize, inject: Inject) -> lsm_tree::Result<usize> {
    let dir = tempfile::tempdir()?;
    let peak = Arc::new(AtomicUsize::new(0));
    let fail_value = Arc::new(AtomicBool::new(false));
    let short_block = Arc::new(AtomicBool::new(false));
    let fs: Arc<dyn Fs> = Arc::new(PeakFs {
        peak: Arc::clone(&peak),
        fail_value: Arc::clone(&fail_value),
        short_block: Arc::clone(&short_block),
    });
    let config = || {
        Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(Arc::clone(&fs))
        .use_cache(Arc::new(Cache::with_capacity_bytes(0)))
        .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
        .blob_compression(CompressionType::None)
        .blob_cache_fill(BlobCacheFill::Never)
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    };
    {
        let tree = config().open()?;
        for i in 0..rows {
            tree.insert(format!("k{i:04}"), value(i, len), i as u64);
        }
        tree.flush_active_memtable(0)?;
    }
    let tree = config().open()?;
    peak.store(0, Ordering::SeqCst);
    fail_value.store(matches!(inject, Inject::FailValue), Ordering::SeqCst);
    short_block.store(matches!(inject, Inject::ShortBlock), Ordering::SeqCst);
    let names: Vec<String> = (0..rows).map(|i| format!("k{i:04}")).collect();

    let values = tree.multi_get(&names, SeqNo::MAX)?;

    assert!(
        !fail_value.load(Ordering::SeqCst) && !short_block.load(Ordering::SeqCst),
        "the injected fault, when asked for, was taken"
    );
    for (i, got) in values.iter().enumerate() {
        let Some(got) = got else {
            panic!("k{i:04} was written");
        };
        assert!(got[..] == value(i, len)[..], "k{i:04} holds its value");
    }
    Ok(peak.load(Ordering::SeqCst))
}

/// Six values of 3 MiB, 18 MiB in all, are read two at a time: never more
/// than the window out at once.
#[test]
fn a_multi_get_keeps_its_value_reads_within_the_window() -> lsm_tree::Result<()> {
    let len = 3 << 20;
    let peak = peak_of_multi_get(6, len, Inject::Nothing)?;
    assert!(peak <= WINDOW, "{peak} bytes out at once, above the window");
    assert!(
        peak >= 2 * len,
        "two values fit the window and go out together"
    );
    Ok(())
}

/// Values two of which do not fit the window are read one at a time, each
/// still read.
#[test]
fn values_two_of_which_overflow_the_window_are_read_one_at_a_time() -> lsm_tree::Result<()> {
    let len = 5 << 20;
    let peak = peak_of_multi_get(3, len, Inject::Nothing)?;
    // A read is the value's record: the value and its header.
    assert!(
        (len..2 * len).contains(&peak),
        "{peak} bytes out at once: each value goes out on its own"
    );
    Ok(())
}

/// A value whose batched read fails is read again on its own path: the
/// multi-get still hands back every value, the failed one included.
#[test]
fn a_value_whose_batched_read_fails_is_read_on_its_own() -> lsm_tree::Result<()> {
    peak_of_multi_get(4, 2 << 20, Inject::FailValue)?;
    Ok(())
}

/// A table block a backend reports read without a byte in it sends the level
/// to the serial read, which still finds every key.
#[test]
fn a_block_read_reported_done_short_reads_the_level_serially() -> lsm_tree::Result<()> {
    peak_of_multi_get(4, 2 << 20, Inject::ShortBlock)?;
    Ok(())
}

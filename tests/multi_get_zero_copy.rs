// A multi-get reads its blocks and separated values into buffers it hands the
// backend's read queue, and answers from those buffers: a value it returns
// points into the memory a read filled, not into a copy of it.

use lsm_tree::{
    AbstractTree, AnyTree, Cache, CompressionType, Config, SeqNo, SequenceNumberCounter, Slice,
    config::{BlobCacheFill, CompressionPolicy, KvSeparationOptions},
    fs::{
        Fs, FsDirEntry, FsFile, FsMetadata, FsOpenOptions, QueuedRead, ReadDone, ReadQueue,
        ReadWake, StdFs,
    },
    io,
};
use std::ops::Range;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

/// The memory every read through a [`RecordingFs`] filled.
#[derive(Clone, Default)]
struct Filled(Arc<Mutex<Vec<Range<usize>>>>);

impl Filled {
    fn ranges(&self) -> Vec<Range<usize>> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn clear(&self) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }
}

/// A backend over [`StdFs`] whose queue reads what it holds when waited on,
/// recording the memory each read filled.
struct RecordingFs(Filled);

/// The queue of a [`RecordingFs`].
struct RecordingQueue {
    held: Vec<QueuedRead>,
    filled: Filled,
}

impl ReadQueue for RecordingQueue {
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
        for mut read in core::mem::take(&mut self.held) {
            let result = read.buf.read_from(read.file.as_ref(), read.offset);
            let bytes = read.buf.filled_bytes();
            let start = bytes.as_ptr() as usize;
            self.filled
                .0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(start..start + bytes.len());
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

impl Fs for RecordingFs {
    fn read_queue(&self) -> Box<dyn ReadQueue + '_> {
        Box::new(RecordingQueue {
            held: Vec::new(),
            filled: self.0.clone(),
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

/// Values large enough to live on the heap, so a copy of one would land at
/// an address of its own.
const VALUE_LEN: usize = 4_096;

/// The value of row `i`.
fn value(i: u32) -> Vec<u8> {
    let byte = u8::try_from(i % 251).unwrap_or_default();
    vec![byte; VALUE_LEN]
}

/// A tree of `rows` uncompressed rows on a [`RecordingFs`], values separated
/// into blob files when `separated`, reopened with no cache so a batch reads
/// everything it answers; and the record of what its reads filled.
fn tree(dir: &Path, rows: u32, separated: bool) -> lsm_tree::Result<(AnyTree, Filled)> {
    let filled = Filled::default();
    let fs: Arc<dyn Fs> = Arc::new(RecordingFs(filled.clone()));
    let config = || {
        let config = Config::new(
            dir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(Arc::clone(&fs))
        .use_cache(Arc::new(Cache::with_capacity_bytes(0)))
        .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
        .blob_compression(CompressionType::None)
        .blob_cache_fill(BlobCacheFill::Never);
        if separated {
            config.with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        } else {
            config
        }
    };
    {
        let tree = config().open()?;
        for i in 0..rows {
            tree.insert(format!("k{i:04}"), value(i), u64::from(i));
        }
        tree.flush_active_memtable(0)?;
    }
    let tree = config().open()?;
    filled.clear();
    Ok((tree, filled))
}

/// Asserts every value is the one written and lies inside memory a read of
/// the batch filled.
fn assert_answered_in_place(values: &[Option<Slice>], filled: &Filled, keys: &[u32]) {
    let ranges = filled.ranges();
    assert!(!ranges.is_empty(), "the batch read through the queue");
    for (value_at, &i) in values.iter().zip(keys) {
        let Some(got) = value_at else {
            panic!("k{i:04} was written");
        };
        assert_eq!(&got[..], &value(i)[..], "k{i:04}");
        let start = got.as_ptr() as usize;
        let end = start + got.len();
        assert!(
            ranges.iter().any(|r| r.start <= start && end <= r.end),
            "k{i:04} was copied out of the buffer its read filled"
        );
    }
}

/// A separated value a batch reads is answered from the buffer the blob read
/// filled.
#[test]
fn a_batched_separated_value_points_into_its_read_buffer() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let (tree, filled) = tree(dir.path(), 64, true)?;
    let keys: Vec<u32> = (0..64).step_by(3).collect();
    let names: Vec<String> = keys.iter().map(|i| format!("k{i:04}")).collect();

    let values = tree.multi_get(&names, SeqNo::MAX)?;

    assert_answered_in_place(&values, &filled, &keys);
    Ok(())
}

/// A value stored in its data block is answered from the buffer the block's
/// read filled, the block decoded over that buffer rather than a copy.
#[test]
fn a_batched_inline_value_points_into_its_block_read_buffer() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let (tree, filled) = tree(dir.path(), 64, false)?;
    let keys: Vec<u32> = (0..64).step_by(3).collect();
    let names: Vec<String> = keys.iter().map(|i| format!("k{i:04}")).collect();

    let values = tree.multi_get(&names, SeqNo::MAX)?;

    assert_answered_in_place(&values, &filled, &keys);
    Ok(())
}

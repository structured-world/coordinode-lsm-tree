//! One run: a corpus written into a blob tree under one layout and codec, and
//! what that costs.

use crate::corpus::{Corpus, read};
use crate::{Error, HeapProbe};
use lsm_tree::{
    AbstractTree, AnyTree, Cache, CompressionType, Config, DictionaryClasses, Guard as _,
    KvSeparationOptions, SeqNo, SequenceNumberCounter, ZstdDictionary, config::BlobCacheFill,
};
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// How a run's blob files hold their values.
#[derive(Clone, Debug)]
pub(crate) struct Layout {
    pub(crate) name: String,
    pub(crate) block_size: u32,
    pub(crate) chunk_size: NonZeroU32,
    /// The classes the dictionary compresses, for a dictionary codec.
    pub(crate) dictionary: DictionaryClasses,
}

impl Layout {
    /// Every value a frame of its own, none split: how a blob file held its
    /// values before size classes.
    pub(crate) fn per_value() -> Self {
        Self {
            name: "per-value".to_owned(),
            block_size: 0,
            chunk_size: NonZeroU32::new(256 << 20).expect("not zero"),
            dictionary: DictionaryClasses::ALL,
        }
    }

    /// Blocks of `block_size` and chunks of `chunk_size`.
    pub(crate) fn sized(block_size: u32, chunk_size: u32) -> Self {
        Self {
            name: format!("block {} chunk {}", kib(block_size), kib(chunk_size)),
            block_size,
            chunk_size: NonZeroU32::new(chunk_size).expect("a chunk size is not zero"),
            dictionary: DictionaryClasses::ALL,
        }
    }
}

fn kib(bytes: u32) -> String {
    if bytes >= 1 << 20 {
        format!("{}M", bytes >> 20)
    } else {
        format!("{}K", bytes >> 10)
    }
}

/// What a run compresses with.
#[derive(Clone)]
pub(crate) enum Codec {
    Lz4,
    Zstd(i32),
    /// zstd at the level with this dictionary, trained on the corpus.
    ZstdDict(i32, Arc<ZstdDictionary>),
}

impl Codec {
    pub(crate) fn name(&self) -> String {
        match self {
            Self::Lz4 => "lz4".to_owned(),
            Self::Zstd(level) => format!("zstd {level}"),
            Self::ZstdDict(level, _) => format!("zstd {level} dict"),
        }
    }

    fn compression(&self) -> CompressionType {
        match self {
            Self::Lz4 => CompressionType::Lz4,
            Self::Zstd(level) => CompressionType::Zstd(*level),
            Self::ZstdDict(level, dict) => CompressionType::ZstdDict {
                level: *level,
                dict_id: dict.id(),
            },
        }
    }
}

/// A dictionary of `size` bytes trained on the first values of `corpus` up to
/// `sample` bytes of them.
///
/// # Errors
///
/// A value cannot be read, or the trainer refuses the samples.
pub(crate) fn train(
    corpus: &Corpus,
    sample: usize,
    size: usize,
) -> Result<Arc<ZstdDictionary>, Error> {
    use structured_zstd::dictionary::{FastCoverOptions, FinalizeOptions, optimize_fastcover_dict};
    let mut samples = Vec::new();
    let mut sizes = Vec::new();
    // A sample is a block's worth, and a value gives at most 64 of them, so a
    // few large values do not make up the whole sample.
    'values: for (_, source) in &corpus.values {
        for piece in read(source)?.chunks(16 << 10).take(64) {
            if samples.len() >= sample {
                break 'values;
            }
            samples.extend_from_slice(piece);
            sizes.push(piece.len());
        }
    }
    // The parameter search `zstd --train` runs.
    let (dict, _) = optimize_fastcover_dict(
        &samples,
        &sizes,
        size,
        &FastCoverOptions::default(),
        FinalizeOptions::default(),
    )?;
    Ok(Arc::new(ZstdDictionary::new(&dict)))
}

/// What one run measured.
#[derive(Debug, Default)]
pub(crate) struct Measured {
    pub(crate) value_bytes: u64,
    pub(crate) blob_bytes: u64,
    pub(crate) write: Duration,
    pub(crate) write_peak: Option<u64>,
    pub(crate) scan: Duration,
    /// Point reads with a cache smaller than the data, then with one it fits.
    pub(crate) get_small_cache: Duration,
    pub(crate) get_large_cache: Duration,
    pub(crate) get_bytes_read: u64,
    pub(crate) get_bytes_decoded: u64,
    pub(crate) gets: u64,
    pub(crate) range_reads: u64,
    pub(crate) range_time: Duration,
    pub(crate) range_bytes_decoded: u64,
    pub(crate) large_value: u64,
    pub(crate) large_read_peak: Option<u64>,
    pub(crate) range_read_peak: Option<u64>,
    pub(crate) relocated: u64,
    pub(crate) reclaimed: i128,
}

impl Measured {
    pub(crate) fn ratio(&self) -> f64 {
        self.blob_bytes as f64 / self.value_bytes.max(1) as f64
    }

    pub(crate) fn to_json(
        &self,
        corpus: &str,
        layout: &Layout,
        codec: &Codec,
    ) -> serde_json::Value {
        let per = |d: Duration, n: u64| d.as_nanos() as f64 / n.max(1) as f64;
        serde_json::json!({
            "corpus": corpus,
            "layout": layout.name,
            "block_size": layout.block_size,
            "chunk_size": layout.chunk_size.get(),
            "dictionary": {
                "blocks": layout.dictionary.blocks,
                "singles": layout.dictionary.singles,
                "chunks": layout.dictionary.chunks,
            },
            "codec": codec.name(),
            "values": self.gets,
            "value_bytes": self.value_bytes,
            "blob_bytes": self.blob_bytes,
            "ratio": self.ratio(),
            "write_ms": self.write.as_secs_f64() * 1e3,
            "write_peak_bytes": self.write_peak,
            "scan_ms": self.scan.as_secs_f64() * 1e3,
            "get_ns_small_cache": per(self.get_small_cache, self.gets),
            "get_ns_large_cache": per(self.get_large_cache, self.gets),
            "get_bytes_read": self.get_bytes_read as f64 / self.gets.max(1) as f64,
            "get_bytes_decoded": self.get_bytes_decoded as f64 / self.gets.max(1) as f64,
            "range_reads": self.range_reads,
            "range_ns": per(self.range_time, self.range_reads),
            "range_bytes_decoded": self.range_bytes_decoded as f64 / self.range_reads.max(1) as f64,
            "large_value_bytes": self.large_value,
            "large_read_peak_bytes": self.large_read_peak,
            "range_read_peak_bytes": self.range_read_peak,
            "gc_relocated_bytes": self.relocated,
            "gc_reclaimed_bytes": self.reclaimed,
            "gc_write_amplification": if self.reclaimed > 0 {
                Some(self.relocated as f64 / self.reclaimed as f64)
            } else {
                None
            },
        })
    }
}

/// Values at or above this many bytes go to blob files: every value of every
/// corpus, so the runs measure the blob files alone.
const SEPARATION: u32 = 64;
/// The cache the data does not fit in, and one it fits in.
const SMALL_CACHE: u64 = 16 << 20;
const LARGE_CACHE: u64 = 4 << 30;
/// A memtable is flushed once it holds this much.
const MEMTABLE: u64 = 64 << 20;
const RANGE: u64 = 4 << 10;

fn open(
    dir: &Path,
    layout: &Layout,
    codec: &Codec,
    cache: u64,
    fill: BlobCacheFill,
) -> lsm_tree::Result<AnyTree> {
    let mut kv = KvSeparationOptions::default()
        .separation_threshold(SEPARATION)
        .block_size(layout.block_size)
        .chunk_size(layout.chunk_size)
        .dictionary_classes(layout.dictionary)
        // The layouts compare blocks, chunks and a trained dictionary; a
        // dictionary each zstd file builds of its own would blur them.
        .file_dictionary(0)
        .age_cutoff(1.0);
    if let Codec::ZstdDict(_, dict) = codec {
        kv = kv.dict(Arc::clone(dict));
    }
    Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(kv))
    .blob_compression(codec.compression())
    .blob_cache_fill(fill)
    .use_cache(Arc::new(Cache::with_capacity_bytes(cache)))
    .open()
}

/// The bytes on disk of `tree`'s blob files.
///
/// # Errors
///
/// As [`blob_files`].
fn blob_bytes(tree: &AnyTree) -> Result<u64, Error> {
    Ok(blob_files(tree)?.iter().map(|(_, bytes)| bytes).sum())
}

/// A pseudo-random permutation of `0..n`, the same on every run.
fn shuffled(n: usize) -> Vec<usize> {
    let mut order: Vec<usize> = (0..n).collect();
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    for i in (1..n).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        order.swap(i, (state % (i as u64 + 1)) as usize);
    }
    order
}

/// Writes `corpus` into a blob tree under `work` with `layout` and `codec`,
/// and measures it.
///
/// # Errors
///
/// A value cannot be read, or the tree fails.
pub(crate) fn measure(
    corpus: &Corpus,
    layout: &Layout,
    codec: &Codec,
    work: &Path,
    heap: &dyn HeapProbe,
) -> Result<Measured, Error> {
    let folder = tempfile::tempdir_in(work)?;
    let mut measured = Measured {
        gets: corpus.values.len() as u64,
        ..Measured::default()
    };

    // Write, flushing as the memtable fills.
    {
        let tree = open(
            folder.path(),
            layout,
            codec,
            SMALL_CACHE,
            BlobCacheFill::Never,
        )?;
        heap.reset();
        // Only the writes are timed: reading the corpus is not the engine's.
        let mut write = std::time::Duration::ZERO;
        for (seqno, (key, source)) in (0..).zip(&corpus.values) {
            let value = read(source)?;
            measured.value_bytes += value.len() as u64;
            let start = Instant::now();
            tree.insert(key, value, seqno);
            if tree.active_memtable().size() >= MEMTABLE {
                tree.flush_active_memtable(0)?;
            }
            write += start.elapsed();
        }
        let start = Instant::now();
        tree.flush_active_memtable(0)?;
        measured.write = write + start.elapsed();
        measured.write_peak = heap.peak();
        measured.blob_bytes = blob_bytes(&tree)?;
    }

    let order = shuffled(corpus.values.len());
    // Reads with a cache smaller than the data: the scan, the point reads and
    // the range reads.
    {
        let tree = open(
            folder.path(),
            layout,
            codec,
            SMALL_CACHE,
            BlobCacheFill::Never,
        )?;
        let start = Instant::now();
        let mut scanned = 0_u64;
        for guard in tree.iter(SeqNo::MAX, None) {
            let (_, value) = guard.into_inner()?;
            scanned += value.len() as u64;
        }
        measured.scan = start.elapsed();
        assert_eq!(
            scanned, measured.value_bytes,
            "the scan returned every value"
        );

        let metrics = tree.metrics();
        let (read_before, decoded_before) = (metrics.bytes_read(), metrics.bytes_decoded());
        let start = Instant::now();
        for &i in &order {
            let got = tree.get(&corpus.values[i].0, SeqNo::MAX)?;
            assert!(got.is_some(), "every key reads back");
        }
        measured.get_small_cache = start.elapsed();
        measured.get_bytes_read = metrics.bytes_read() - read_before;
        measured.get_bytes_decoded = metrics.bytes_decoded() - decoded_before;

        measure_ranges(&tree, corpus, &mut measured, heap)?;
    }
    // Point reads with a cache the data fits in, warmed by a first pass.
    {
        let tree = open(
            folder.path(),
            layout,
            codec,
            LARGE_CACHE,
            BlobCacheFill::Always,
        )?;
        // The warming pass is not timed, so it also checks every value reads
        // back as written.
        for (key, source) in &corpus.values {
            let got = tree.get(key, SeqNo::MAX)?;
            assert_eq!(
                got.as_deref(),
                Some(read(source)?.as_slice()),
                "a value reads back as written"
            );
        }
        let start = Instant::now();
        for &i in &order {
            let _ = tree.get(&corpus.values[i].0, SeqNo::MAX)?;
        }
        measured.get_large_cache = start.elapsed();
    }

    collect(folder.path(), corpus, layout, codec, &mut measured)?;
    Ok(measured)
}

/// 4 KiB range reads of every value above 1 MiB, and the heap a whole read
/// and a range read of the largest one take.
fn measure_ranges(
    tree: &AnyTree,
    corpus: &Corpus,
    measured: &mut Measured,
    heap: &dyn HeapProbe,
) -> Result<(), Error> {
    let AnyTree::Blob(blob) = tree else {
        return Ok(());
    };
    let mut largest: Option<(&[u8], u64)> = None;
    let metrics = tree.metrics();
    let mut state = 0x2545_F491_4F6C_DD1D_u64;
    for (key, source) in &corpus.values {
        let len = match source {
            crate::corpus::Source::File(path) => std::fs::metadata(path)?.len(),
            crate::corpus::Source::Line { len, .. } => u64::from(*len),
        };
        if len <= 1 << 20 {
            continue;
        }
        if largest.is_none_or(|(_, l)| len > l) {
            largest = Some((key, len));
        }
        let decoded_before = metrics.bytes_decoded();
        let start = Instant::now();
        for _ in 0..16 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let at = state % (len - RANGE);
            let got = blob
                .get_range(key, at..at + RANGE, SeqNo::MAX)?
                .ok_or("a range of a stored value")?;
            assert_eq!(got.len() as u64, RANGE);
        }
        measured.range_time += start.elapsed();
        measured.range_reads += 16;
        measured.range_bytes_decoded += metrics.bytes_decoded() - decoded_before;
    }
    if let Some((key, len)) = largest {
        measured.large_value = len;
        heap.reset();
        let value = tree.get(key, SeqNo::MAX)?.ok_or("the largest value")?;
        measured.large_read_peak = heap.peak();
        drop(value);
        heap.reset();
        let _ = blob.get_range(key, len / 2..len / 2 + RANGE, SeqNo::MAX)?;
        measured.range_read_peak = heap.peak();
    }
    Ok(())
}

/// Three rounds, each overwriting a third of the keys with another key's
/// value, flushed and compacted: the blob bytes the compactions relocated and
/// those they removed.
fn collect(
    dir: &Path,
    corpus: &Corpus,
    layout: &Layout,
    codec: &Codec,
    measured: &mut Measured,
) -> Result<(), Error> {
    let tree = open(dir, layout, codec, SMALL_CACHE, BlobCacheFill::Never)?;
    let n = corpus.values.len();
    let mut seqno = n as SeqNo;
    for round in 0..3 {
        for i in (round..n).step_by(3) {
            let value = read(&corpus.values[(i + 1) % n].1)?;
            tree.insert(&corpus.values[i].0, value, seqno);
            seqno += 1;
            if tree.active_memtable().size() >= MEMTABLE {
                tree.flush_active_memtable(seqno)?;
            }
        }
        tree.flush_active_memtable(seqno)?;
        let before = blob_files(&tree)?;
        // The first counts the dead values, the second collects the files
        // they made stale.
        tree.major_compact(64 << 20, seqno)?;
        tree.major_compact(64 << 20, seqno)?;
        let after = blob_files(&tree)?;
        // What the relocation wrote, as the files it made take on disk: the
        // relocation counter charges a value in a block its entry uncompressed,
        // which the file lengths below cannot be set against.
        let relocated: u64 = after
            .iter()
            .filter(|(id, _)| !before.iter().any(|(old, _)| old == id))
            .map(|(_, bytes)| bytes)
            .sum();
        let total = |files: &[(u64, u64)]| files.iter().map(|(_, bytes)| bytes).sum::<u64>();
        measured.relocated += relocated;
        measured.reclaimed +=
            i128::from(total(&before)) + i128::from(relocated) - i128::from(total(&after));
    }
    Ok(())
}

/// Every blob file of `tree`'s current version, by id, with its length on disk.
///
/// # Errors
///
/// A file whose length cannot be read: counted as nothing, it would skew
/// what the sweep reports relocated and reclaimed.
fn blob_files(tree: &AnyTree) -> Result<Vec<(u64, u64)>, Error> {
    tree.current_version()
        .blob_files
        .iter()
        .map(|file| Ok((file.id(), std::fs::metadata(file.path())?.len())))
        .collect()
}

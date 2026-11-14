// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};

use super::frame::{
    ChunkSpec, Codec, EntrySpec, FrameKind, encode_chunk, encode_frame_parts,
    encode_frame_parts_compressed, head_payload,
};
use super::meta::{META_VERSION, Metadata};
use crate::io::FillingBufWriter;
#[cfg(not(feature = "std"))]
use crate::io::Write;
use crate::path::{Path, PathBuf};
use crate::{
    Checksum, CompressionType, KeyRange, SeqNo, TreeId, UserKey,
    checksum::ChecksummedWriter,
    fs::{Fs, FsFile, FsOpenOptions, SyncMode},
    vlog::BlobFileId,
};
#[cfg(feature = "std")]
use std::io::Write;

/// Safety cap on blob value size (256 MiB).
///
/// Enforced on the write path to prevent producing blobs that are
/// unreasonably large. The reader applies its own copy of this limit.
///
/// The blob-format definition site: `reader` and `scanner` import this rather
/// than duplicate it. Kept SEPARATE from `table::block`'s cap (a `u32`) on
/// purpose, since blocks and blobs are independent storage formats that may
/// diverge.
pub(crate) const MAX_DECOMPRESSION_SIZE: usize = 256 * 1024 * 1024;

/// The smallest chunk a value is split into: a page. Below that, a value at
/// the cap splits into so many chunks that their frame headers pass the u32
/// a value's stored bytes are recorded in, and each chunk costs a page read
/// anyway. It also bounds how many frames, and frame index entries, a value
/// takes.
pub(crate) const MIN_CHUNK_SIZE: u32 = 4 * 1024;

/// Returns `Err(DecompressedSizeTooLarge)` if `len > MAX_DECOMPRESSION_SIZE`.
pub(crate) fn check_size_cap(len: usize) -> crate::Result<u32> {
    if len > MAX_DECOMPRESSION_SIZE {
        return Err(crate::Error::DecompressedSizeTooLarge {
            declared: len as u64,
            limit: MAX_DECOMPRESSION_SIZE as u64,
        });
    }
    #[expect(
        clippy::cast_possible_truncation,
        reason = "the cap, 256 MiB, fits u32"
    )]
    Ok(len as u32)
}

/// How a blob file sizes its frames: set through
/// [`KvSeparationOptions::block_size`](crate::KvSeparationOptions::block_size)
/// and [`KvSeparationOptions::chunk_size`](crate::KvSeparationOptions::chunk_size).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FrameSizes {
    /// Values below it share a block, which closes once it reaches
    /// [`BLOCK_FILL_PERCENT`] of it and the next value would take it past.
    pub(crate) block_target: u32,
    /// Values above it are split into chunks of this many bytes; never zero.
    pub(crate) chunk_size: u32,
}

/// A block copied as it was stored, with what it adds to the file that
/// copies it, as the scan that read and checked it counted it.
pub(crate) struct CopiedBlock<'a> {
    /// The block's frame as stored.
    pub(crate) stored: &'a [u8],
    /// The keys of its first and last values.
    pub(crate) first_key: &'a [u8],
    pub(crate) last_key: &'a [u8],
    /// How many values it holds, and their decoded bytes.
    pub(crate) values: u16,
    pub(crate) value_bytes: u64,
    /// The block entry bytes its values are charged, as its writer counted
    /// them.
    pub(crate) entry_bytes: u64,
}

impl FrameSizes {
    /// A block of 8 KiB, chunks of 256 KiB, chosen from the blob file sweep in
    /// `docs/BENCHMARKING.md`. Against a frame per value an 8 KiB block takes
    /// a fifth to two fifths less room, and a cold point read of documents
    /// costs about what it did; 16 KiB saves 4 to 8% more for a third more per
    /// read.
    /// A 256 KiB chunk reads a 4 KiB range of a large value in under 0.5 ms
    /// where a frame of the whole value took 60 to 120 ms.
    pub(crate) const DEFAULT: Self = Self {
        block_target: 8 * 1024,
        chunk_size: 256 * 1024,
    };
}

/// How full a block must be before the next value may close it, in percent of
/// the target. Below it, a value joins the block even past the target, so a
/// block never closes small for want of room for one more value.
pub(crate) const BLOCK_FILL_PERCENT: u64 = 90;

/// Where a written value lies: the frame that holds it (a head for a chunked
/// value) and its place in the frame's list, and the bytes it takes (see
/// [`crate::blob_tree::handle::BlobIndirection::stored`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FrameRef {
    pub(crate) frame: u32,
    pub(crate) index: u16,
    pub(crate) stored: crate::blob_tree::handle::Stored,
}

/// The values waiting for the block they join.
#[derive(Default)]
struct PendingBlock {
    /// Every key, back to back.
    keys: Vec<u8>,
    /// Each value's sequence number, the end of its key in `keys` and its
    /// length.
    entries: Vec<(SeqNo, usize, u32)>,
    /// Every value, back to back.
    payload: Vec<u8>,
}

impl PendingBlock {
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn clear(&mut self) {
        self.keys.clear();
        self.entries.clear();
        self.payload.clear();
    }

    /// What the block holds so far, uncompressed: its entries, keys and
    /// values.
    fn len(&self) -> u64 {
        (self.entries.len() * super::frame::ENTRY_HEADER_LEN + self.keys.len() + self.payload.len())
            as u64
    }
}

/// The bit of a frame's start in the index that says the frame is a chunk of
/// the head before it. A chunk is its value's bytes with no header, so
/// nothing in it tells it from a frame: a value may hold a whole frame that
/// checks. The index is what knows, and a walk that lost a head to damage
/// passes its chunks by this bit instead of reading them as frames. A file
/// offset never reaches it.
pub(crate) const CHUNK_MARK: u64 = 1 << 63;

/// Marks frame `at` in a bit set of frames.
fn mark(marks: &mut Vec<u64>, at: usize) {
    let word = at / 64;
    if marks.len() <= word {
        marks.resize(word + 1, 0);
    }
    if let Some(bits) = marks.get_mut(word) {
        *bits |= 1 << (at % 64);
    }
}

/// Whether frame `at` is marked in a bit set of frames.
fn is_marked(marks: &[u64], at: usize) -> bool {
    marks
        .get(at / 64)
        .is_some_and(|bits| bits & (1 << (at % 64)) != 0)
}

/// The frame index a blob file ends its data with: where each frame starts,
/// then where the data ends, then a checksum over both. A chunk's start
/// carries [`CHUNK_MARK`]; `chunks` is the bit set of the frames that are.
///
/// `[count; 4][offset; 8] * count [end; 8][xxh3; 8]`
pub(crate) fn encode_frame_index_marked(starts: &[u64], end: u64, chunks: &[u64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + (starts.len() + 2) * 8);
    #[expect(
        clippy::cast_possible_truncation,
        reason = "a frame number is a u32, so a file holds at most u32::MAX frames"
    )]
    out.extend_from_slice(&(starts.len() as u32).to_le_bytes());
    for start in starts {
        out.extend_from_slice(&start.to_le_bytes());
    }
    // The marks go onto the starts already laid down, a chunk at a time: most
    // files hold none, and their index costs what it did without the marks.
    for (word, &bits) in chunks.iter().enumerate() {
        let mut bits = bits;
        while bits != 0 {
            let at = word * 64 + bits.trailing_zeros() as usize;
            // The top byte of the little-endian start of frame `at`.
            if at < starts.len()
                && let Some(top) = out.get_mut(4 + at * 8 + 7)
            {
                *top |= 0x80;
            }
            bits &= bits - 1;
        }
    }
    out.extend_from_slice(&end.to_le_bytes());
    let digest = xxhash_rust::xxh3::xxh3_64(&out);
    out.extend_from_slice(&digest.to_le_bytes());
    out
}

/// [`encode_frame_index_marked`] of a file that holds no chunk.
#[cfg(test)]
pub(crate) fn encode_frame_index(starts: &[u64], end: u64) -> Vec<u8> {
    encode_frame_index_marked(starts, end, &[])
}

/// A blob file's frame index as a reader holds it: where each frame starts,
/// then where the data ends (it reads as that slice), and which frames are
/// chunks. Cloning it shares both.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FrameIndex {
    starts: alloc::sync::Arc<[u64]>,
    chunks: alloc::sync::Arc<[u64]>,
}

impl FrameIndex {
    /// The index of frames starting at `starts`, the last of which is the
    /// data end, `chunks` the bit set of the frames that are chunks.
    pub(crate) fn new(starts: Vec<u64>, chunks: Vec<u64>) -> Self {
        Self {
            starts: starts.into(),
            chunks: chunks.into(),
        }
    }

    /// Whether `frame` is a chunk of the head before it.
    pub(crate) fn is_chunk(&self, frame: usize) -> bool {
        is_marked(&self.chunks, frame)
    }
}

impl core::ops::Deref for FrameIndex {
    type Target = [u64];

    fn deref(&self) -> &[u64] {
        &self.starts
    }
}

/// Decodes a frame index: each frame's start followed by the data end, one
/// more entry than there are frames, and the frames it marks as chunks.
///
/// # Errors
///
/// [`crate::Error::InvalidHeader`] for an index that is short, long, fails its
/// checksum, lists starts that do not increase within the data, or marks its
/// first frame or its data end a chunk: a chunk follows a head.
pub(crate) fn decode_frame_index(bytes: &[u8]) -> crate::Result<FrameIndex> {
    let malformed = || crate::Error::InvalidHeader("BlobFrameIndex");
    let (body, digest) = bytes.split_last_chunk::<8>().ok_or_else(malformed)?;
    if xxhash_rust::xxh3::xxh3_64(body) != u64::from_le_bytes(*digest) {
        return Err(malformed());
    }
    let (count, rest) = body.split_first_chunk::<4>().ok_or_else(malformed)?;
    let words = (u32::from_le_bytes(*count) as usize)
        .checked_add(1)
        .and_then(|words| words.checked_mul(8));
    if words != Some(rest.len()) {
        return Err(malformed());
    }
    let mut chunks = Vec::new();
    let offsets: Vec<u64> = rest
        .as_chunks::<8>()
        .0
        .iter()
        .enumerate()
        .map(|(at, word)| {
            let word = u64::from_le_bytes(*word);
            if word & CHUNK_MARK != 0 {
                mark(&mut chunks, at);
            }
            word & !CHUNK_MARK
        })
        .collect();
    if offsets.windows(2).any(|pair| pair.first() >= pair.get(1))
        || is_marked(&chunks, 0)
        || is_marked(&chunks, offsets.len() - 1)
    {
        return Err(malformed());
    }
    Ok(FrameIndex::new(offsets, chunks))
}

/// Blob file writer
pub(crate) struct Writer {
    pub(crate) tree_id: TreeId,
    pub path: PathBuf,
    pub(crate) blob_file_id: BlobFileId,

    #[expect(clippy::struct_field_names)]
    writer: crate::sfa::Writer<ChecksummedWriter<FillingBufWriter<Box<dyn FsFile>>>>,

    offset: u64,

    pub(crate) item_count: u64,
    pub(crate) written_blob_bytes: u64,
    pub(crate) uncompressed_bytes: u64,

    /// What the values written into blocks were charged, their entries
    /// uncompressed, and what those blocks take on disk.
    block_entry_bytes: u64,
    block_frame_bytes: u64,

    pub(crate) first_key: Option<UserKey>,
    /// Rewritten by every value, so it reuses its buffer rather than keeping a
    /// key of its own per value.
    pub(crate) last_key: Vec<u8>,

    pub(crate) compression: CompressionType,

    /// The lifetime class this file holds, recorded in its metadata.
    pub(crate) lifetime_class: u8,

    /// The tree's clock, read for the file's age; `None` reads the process
    /// clock.
    clock: Option<alloc::sync::Arc<dyn crate::Clock>>,

    /// Durability level for the final blob-file fsync. Default
    /// [`SyncMode::Normal`]; wired from `Config::sync_mode` via
    /// [`Self::use_sync_mode`].
    pub(crate) sync_mode: SyncMode,

    /// Whether `finish` syncs the file; a deferred one is synced by the
    /// install that persists it.
    pub(crate) durability: crate::config::FlushDurability,

    /// Bytes gathered between writebacks the writer starts while writing; `0`
    /// starts none. Set via [`Self::use_writeback_bytes`].
    writeback_bytes: u64,

    /// Where the last writeback ended: every byte before it was handed over.
    written_back: u64,

    /// Dictionary for `ZstdDict` compression.  Must be supplied when
    /// `compression` is [`CompressionType::ZstdDict`].
    #[cfg(zstd_any)]
    pub(crate) zstd_dictionary: Option<alloc::sync::Arc<crate::compression::ZstdDictionary>>,

    /// The bytes of the dictionary a zstd file builds from its own first
    /// values, `0` for none: see [`Self::use_file_dictionary`].
    #[cfg(zstd_any)]
    file_dictionary: u32,

    /// The values collected toward the file's dictionary while it does not
    /// exist yet, back to back.
    #[cfg(zstd_any)]
    dictionary_samples: Vec<u8>,

    /// Whether [`Self::zstd_dictionary`] is the file's own, built from its
    /// values and written into it.
    #[cfg(zstd_any)]
    pub(crate) dictionary_in_file: bool,

    /// Whether zstd levels 19-22 run the `btultra2` two-pass seed. Defaults to
    /// `true`, the codec's own behaviour; set from the live runtime config via
    /// [`Self::use_zstd_two_pass_seed`].
    #[cfg(zstd_any)]
    pub(crate) zstd_two_pass_seed: bool,

    /// How this file sizes its frames.
    pub(crate) sizes: FrameSizes,

    /// The frame classes a dictionary codec compresses with its dictionary.
    pub(crate) dictionary_classes: crate::config::DictionaryClasses,

    /// Where each frame written so far starts.
    frames: Vec<u64>,

    /// Which of `frames` are chunks, a bit a frame: see [`CHUNK_MARK`].
    chunk_marks: Vec<u64>,

    /// The block the next small value joins.
    block: PendingBlock,

    /// The records of the packed block the next small value joins (see
    /// [`Self::packs`]), back to back, how many, and their values' bytes.
    packed: Vec<u8>,
    packed_count: u16,
    packed_raw: u64,

    /// A frame being encoded, kept so a writer allocates it once.
    scratch: Vec<u8>,

    /// Where the chunks of a large value are compressed, when not on the
    /// writer's thread one after another.
    #[cfg(feature = "std")]
    parallel: Option<crate::table::writer::ParallelCompression>,
}

/// One chunk of a large value to encode on a worker. The chunk is the job's
/// own copy: a worker outlives the borrow of the value written.
#[cfg(feature = "std")]
struct ChunkJob(Vec<u8>);

/// A chunk encoded: compressed bytes when compression shrank it, `None` when
/// it is stored as it is, and its row in the head's table.
type EncodedChunk = (Option<Vec<u8>>, ChunkSpec);

/// A writer's codec as a worker holds it, its dictionary shared rather than
/// borrowed: a worker outlives the borrow of the writer.
#[cfg(feature = "std")]
pub(crate) struct OwnedCodec {
    compression: CompressionType,
    #[cfg(zstd_any)]
    dictionary: Option<alloc::sync::Arc<crate::compression::ZstdDictionary>>,
    #[cfg(zstd_any)]
    two_pass_seed: bool,
    plain: bool,
}

#[cfg(feature = "std")]
impl OwnedCodec {
    fn as_codec(&self) -> Codec<'_> {
        Codec {
            compression: self.compression,
            #[cfg(zstd_any)]
            dictionary: self.dictionary.as_deref(),
            #[cfg(zstd_any)]
            two_pass_seed: self.two_pass_seed,
            #[cfg(not(zstd_any))]
            dictionary: core::marker::PhantomData,
            plain: self.plain,
        }
    }

    /// `value` compressed under this codec off the writer's thread, for the
    /// writer to take when it writes the value under the same codec.
    ///
    /// # Errors
    ///
    /// Propagates the compression.
    pub(crate) fn precompress(&self, value: &[u8]) -> crate::Result<PreCompressed> {
        let codec = self.as_codec();
        Ok(PreCompressed {
            stamp: CodecStamp::of(&codec),
            compressed: super::frame::compress_payload(value, codec)?,
        })
    }
}

#[cfg(feature = "std")]
impl crate::table::writer::ordered_pipeline::OrderedJob for ChunkJob {
    type Context = OwnedCodec;
    type Output = crate::Result<EncodedChunk>;

    fn run(self, codec: &OwnedCodec) -> Self::Output {
        // The chunk is stored as it is from the value written, not from
        // this copy: only what compression made is handed back.
        encode_chunk(&self.0, codec.as_codec())
    }
}

/// What a codec compresses with, down to the dictionary itself: two codecs
/// with one stamp produce the same bytes.
#[derive(Clone, Copy, PartialEq, Eq)]
struct CodecStamp {
    compression: CompressionType,
    plain: bool,
    /// The full fingerprint of the dictionary: a dictionary id is 32 bits,
    /// and two files' dictionaries sharing it would decode each other's
    /// values into garbage.
    #[cfg(zstd_any)]
    dictionary: Option<u64>,
}

impl CodecStamp {
    #[cfg_attr(
        not(zstd_any),
        expect(
            clippy::trivially_copy_pass_by_ref,
            reason = "without zstd a codec carries no dictionary and fits a byte"
        )
    )]
    fn of(codec: &Codec<'_>) -> Self {
        Self {
            compression: codec.compression,
            plain: codec.plain,
            #[cfg(zstd_any)]
            dictionary: codec
                .dictionary
                .map(crate::compression::ZstdDictionary::id64),
        }
    }
}

/// A value compressed ahead of its write, under the codec its stamp names:
/// taken by the write only when that is still the codec the value is written
/// under, and compressed again by the write otherwise.
pub(crate) struct PreCompressed {
    stamp: CodecStamp,
    /// What compression made, `None` when it saved nothing.
    compressed: Option<Vec<u8>>,
}

impl PreCompressed {
    /// What compressing `value` under `codec` makes: this, when it was made
    /// under `codec`, else made now.
    fn compressed_for(
        pre: Option<Self>,
        value: &[u8],
        codec: Codec<'_>,
    ) -> crate::Result<Option<Vec<u8>>> {
        match pre {
            Some(pre) if pre.stamp == CodecStamp::of(&codec) => Ok(pre.compressed),
            _ => super::frame::compress_payload(value, codec),
        }
    }
}

impl Writer {
    /// A blob file writer whose file carries no origin.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[cfg(test)]
    pub(crate) fn new<P: AsRef<Path>>(
        path: P,
        blob_file_id: BlobFileId,
        tree_id: TreeId,
        fs: &dyn Fs,
    ) -> crate::Result<Self> {
        Self::open(path, blob_file_id, tree_id, fs, None)
    }

    /// [`Self::new`] writing every value up to the default chunk size into a
    /// frame of its own, none into a block: a fixture whose damage to one
    /// value is damage to that value alone.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[cfg(test)]
    pub(crate) fn one_frame_each<P: AsRef<Path>>(
        path: P,
        blob_file_id: BlobFileId,
        tree_id: TreeId,
        fs: &dyn Fs,
    ) -> crate::Result<Self> {
        Ok(
            Self::new(path, blob_file_id, tree_id, fs)?.use_frame_sizes(FrameSizes {
                block_target: 0,
                chunk_size: FrameSizes::DEFAULT.chunk_size,
            }),
        )
    }

    /// Initializes a new blob file writer, its file written as `origin`'s
    /// when given.
    ///
    /// Uses `create_new` (not `create+truncate`) because blob file IDs are
    /// monotonically unique: a path collision indicates a bug, not a retry.
    /// Orphaned files from crashes are cleaned up during recovery.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(crate) fn open<P: AsRef<Path>>(
        path: P,
        blob_file_id: BlobFileId,
        tree_id: TreeId,
        fs: &dyn Fs,
        origin: Option<crate::fs::WriteOrigin>,
    ) -> crate::Result<Self> {
        let path = path.as_ref();

        let mut opts = FsOpenOptions::new().write(true).create_new(true);
        opts.write_origin = origin;
        let file = fs.open(path, &opts)?;
        // From here the file exists and is OURS (`create_new` proved it), so
        // this constructor owns cleanup: an init failure below must not leak a
        // partial file, and a CALLER must never remove `path` on a constructor
        // error: an open failure above created nothing, so caller-side
        // cleanup would race a concurrent creator and delete a file it does
        // not own.
        let mut writer = crate::sfa::Writer::from_writer(ChecksummedWriter::new(
            FillingBufWriter::with_capacity(u16::MAX.into(), file),
        ));
        if let Err(e) = writer.start("data") {
            drop(writer);
            // Best-effort cleanup of the partial file we own, but do not swallow
            // a removal failure silently: log it with context so a leaked
            // partial blob file is diagnosable. The original start error is still
            // what the caller sees.
            if let Err(remove_err) = fs.remove_file(path) {
                log::warn!(
                    "failed to remove partial blob file {} after a writer-init error: {remove_err}",
                    path.display(),
                );
            }
            return Err(e.into());
        }

        Ok(Self {
            tree_id,
            path: path.into(),
            blob_file_id,

            writer,

            offset: 0,
            item_count: 0,
            written_blob_bytes: 0,
            uncompressed_bytes: 0,
            block_entry_bytes: 0,
            block_frame_bytes: 0,

            first_key: None,
            last_key: Vec::new(),

            compression: CompressionType::None,
            lifetime_class: 0,
            clock: None,
            sync_mode: SyncMode::Normal,
            durability: crate::config::FlushDurability::Synced,
            writeback_bytes: 0,
            written_back: 0,

            #[cfg(zstd_any)]
            zstd_dictionary: None,
            #[cfg(zstd_any)]
            file_dictionary: 0,
            #[cfg(zstd_any)]
            dictionary_samples: Vec::new(),
            #[cfg(zstd_any)]
            dictionary_in_file: false,
            #[cfg(zstd_any)]
            zstd_two_pass_seed: true,

            sizes: FrameSizes::DEFAULT,
            dictionary_classes: crate::config::DictionaryClasses::DEFAULT,
            frames: Vec::new(),
            chunk_marks: Vec::new(),
            block: PendingBlock::default(),
            packed: Vec::new(),
            packed_count: 0,
            packed_raw: 0,
            scratch: Vec::new(),
            #[cfg(feature = "std")]
            parallel: None,
        })
    }

    /// Compresses the chunks of a large value on `parallel`'s workers, or one
    /// after another on the writer's thread without it.
    #[cfg(feature = "std")]
    #[must_use]
    pub(crate) fn use_parallel_compression(
        mut self,
        parallel: Option<crate::table::writer::ParallelCompression>,
    ) -> Self {
        self.parallel = parallel;
        self
    }

    pub(crate) fn use_compression(mut self, compressor: CompressionType) -> Self {
        self.compression = compressor;
        self
    }

    /// Sizes this file's frames by `sizes`.
    #[must_use]
    pub(crate) fn use_frame_sizes(mut self, sizes: FrameSizes) -> Self {
        self.sizes = sizes;
        self
    }

    /// Compresses only the frames of `classes` with a dictionary codec's
    /// dictionary, the others at its level without one.
    #[must_use]
    pub(crate) fn use_dictionary_classes(
        mut self,
        classes: crate::config::DictionaryClasses,
    ) -> Self {
        self.dictionary_classes = classes;
        self
    }

    /// Wires the tree's `Config::sync_mode` into the final blob-file fsync.
    #[must_use]
    pub(crate) fn use_sync_mode(mut self, sync_mode: SyncMode) -> Self {
        self.sync_mode = sync_mode;
        self
    }

    /// Reads the time on `clock`, the tree's own, instead of the process clock.
    #[must_use]
    pub(crate) fn use_clock(mut self, clock: Option<alloc::sync::Arc<dyn crate::Clock>>) -> Self {
        self.clock = clock;
        self
    }

    /// Starts writing back every `bytes` the file gathers while it is written
    /// ([`FsFile::start_writeback`]), so its final sync is short; `0` starts
    /// none.
    #[must_use]
    pub(crate) fn use_writeback_bytes(mut self, bytes: u64) -> Self {
        self.writeback_bytes = bytes;
        self
    }

    /// Records `class` as the lifetime class of this file and tells the device
    /// how long its data is expected to live, when `lifetime` says.
    ///
    /// The device hint is advisory: a filesystem or kernel that refuses it
    /// leaves the file written exactly as without it.
    pub(crate) fn set_lifetime_class(
        &mut self,
        class: u8,
        lifetime: Option<crate::fs::WriteLifetime>,
    ) {
        self.lifetime_class = class;
        if let Some(lifetime) = lifetime {
            let file = self.writer.get_mut().get_ref().get_ref();
            if let Err(error) = file.set_write_lifetime(lifetime) {
                log::debug!(
                    "write lifetime hint refused for {}: {error}",
                    self.path.display()
                );
            }
        }
    }

    /// Starts writing back what the file wrote since the last writeback, once
    /// at least `writeback_bytes` of it gathered. Checked once per value
    /// written, not per frame: a range hint per chunk of a large value cost its
    /// write a fifth of its time.
    fn writeback_if_due(&mut self) -> crate::Result<()> {
        // `written_back` only ever takes a value of `offset`, which grows.
        let pending = self.offset - self.written_back;
        if self.writeback_bytes == 0 || pending < self.writeback_bytes {
            return Ok(());
        }
        // The range is in the buffer until it is flushed to the file.
        let checksummed = self.writer.get_mut();
        checksummed.flush()?;
        crate::fs::hint_writeback(
            &**checksummed.get_ref().get_ref(),
            self.written_back,
            pending,
            &mut self.writeback_bytes,
        );
        self.written_back = self.offset;
        Ok(())
    }

    /// Selects whether zstd levels 19-22 run the `btultra2` two-pass seed.
    ///
    /// See [`crate::runtime_config::RuntimeConfig::zstd_two_pass_seed`]. Has no
    /// effect below level 19, where another strategy applies.
    #[cfg(zstd_any)]
    #[must_use]
    pub(crate) fn use_zstd_two_pass_seed(mut self, enabled: bool) -> Self {
        self.zstd_two_pass_seed = enabled;
        self
    }

    /// Provides the zstd dictionary for [`CompressionType::ZstdDict`] writes.
    ///
    /// Must be called before writing any blobs when the compression type is
    /// `ZstdDict`.  Passing `None` clears a previously set dictionary.
    #[cfg(zstd_any)]
    #[must_use]
    pub(crate) fn use_zstd_dictionary(
        mut self,
        dict: Option<alloc::sync::Arc<crate::compression::ZstdDictionary>>,
    ) -> Self {
        self.zstd_dictionary = dict;
        self
    }

    /// Stores the dictionary [`Self::use_zstd_dictionary`] gave in the file, as
    /// one of its own, when `own`: frames copied from a file that held it need
    /// it next to them, as no tree's set holds it.
    #[cfg(zstd_any)]
    #[must_use]
    pub(crate) fn keep_dictionary_in_file(mut self, own: bool) -> Self {
        self.dictionary_in_file = own;
        self
    }

    /// Builds a dictionary of `bytes` from this file's own first values when
    /// it compresses with zstd and no dictionary: every value up to the chunk
    /// size then gets a frame of its own, none shares a block, and a value is
    /// compressed with the file's dictionary once the file holds one, so a
    /// read of a value decodes that value alone. The values written before
    /// the dictionary exists are compressed without it. `0` builds none.
    #[cfg(zstd_any)]
    #[must_use]
    pub(crate) fn use_file_dictionary(mut self, bytes: u32) -> Self {
        self.set_file_dictionary(bytes);
        self
    }

    /// [`Self::use_file_dictionary`] on a writer that holds nothing yet: a
    /// file that wrote a value keeps the dictionary it began with.
    #[cfg(zstd_any)]
    pub(crate) fn set_file_dictionary(&mut self, bytes: u32) {
        if self.item_count == 0 && !self.dictionary_in_file {
            self.file_dictionary = bytes;
        }
    }

    /// Whether this file is collecting values toward a dictionary of its own.
    #[cfg(zstd_any)]
    fn collecting_dictionary(&self) -> bool {
        self.file_dictionary > 0
            && !self.dictionary_in_file
            && matches!(self.compression, CompressionType::Zstd(_))
    }

    /// Whether small values are packed records rather than block entries:
    /// under the file's own dictionary, and in a file that compresses
    /// nothing, where a block gains nothing from sharing.
    fn packs(&self) -> bool {
        #[cfg(zstd_any)]
        let own_dictionary = self.file_dictionary_on();
        #[cfg(not(zstd_any))]
        let own_dictionary = false;
        own_dictionary || self.compression == CompressionType::None
    }

    /// Whether values are written a frame each under the file's own
    /// dictionary, built or being built.
    #[cfg(zstd_any)]
    fn file_dictionary_on(&self) -> bool {
        self.dictionary_in_file || self.collecting_dictionary()
    }

    /// Takes `value` toward the file's dictionary, and builds it once the
    /// samples fill it: the dictionary is the samples as they are, raw
    /// content a frame's matches reach back into, the latest last.
    #[cfg(zstd_any)]
    fn sample_for_dictionary(&mut self, value: &[u8]) {
        let CompressionType::Zstd(level) = self.compression else {
            return;
        };
        let want = self.file_dictionary as usize;
        let room = want - self.dictionary_samples.len();
        self.dictionary_samples
            .extend_from_slice(value.get(..room.min(value.len())).unwrap_or_default());
        if self.dictionary_samples.len() < want {
            return;
        }
        // Bytes that open with a trained dictionary's magic are read as one,
        // and these are raw content: the first byte goes, and with it the
        // magic, here once and so in the dictionary the file stores.
        if self
            .dictionary_samples
            .starts_with(&structured_zstd::decoding::DICTIONARY_MAGIC)
        {
            self.dictionary_samples.remove(0);
        }
        let dict = crate::compression::ZstdDictionary::new(&self.dictionary_samples);
        self.compression = CompressionType::ZstdDict {
            level,
            dict_id: dict.id(),
        };
        self.zstd_dictionary = Some(alloc::sync::Arc::new(dict));
        self.dictionary_in_file = true;
        self.dictionary_samples = Vec::new();
    }

    /// Stops building a dictionary of the file's own before frames encoded
    /// elsewhere are copied in as they are: a file records one codec, theirs.
    ///
    /// # Errors
    ///
    /// [`crate::Error::InvalidHeader`] for a file that already wrote values
    /// toward its own dictionary.
    #[cfg_attr(
        not(zstd_any),
        expect(
            clippy::unused_self,
            clippy::unnecessary_wraps,
            clippy::needless_pass_by_ref_mut,
            reason = "without zstd no file builds a dictionary of its own"
        )
    )]
    fn leave_file_dictionary(&mut self) -> crate::Result<()> {
        // A file that holds its dictionary takes frames of that codec, its
        // own; only one still gathering values toward it cannot.
        #[cfg(zstd_any)]
        if self.collecting_dictionary() {
            if self.item_count > 0 {
                return Err(crate::Error::InvalidHeader(
                    "verbatim frames into a file building its own dictionary",
                ));
            }
            self.file_dictionary = 0;
        }
        Ok(())
    }

    /// Returns the blob file ID.
    #[must_use]
    pub(crate) fn blob_file_id(&self) -> BlobFileId {
        self.blob_file_id
    }

    /// Bytes the file holds so far, the values waiting for their block counted.
    #[must_use]
    pub(crate) fn len_written(&self) -> u64 {
        self.offset + self.block.len() + self.packed.len() as u64
    }

    /// The codec a frame of `kind` is compressed under: the file's, without
    /// its dictionary for a class the dictionary does not apply to.
    fn codec(&self, kind: FrameKind) -> Codec<'_> {
        // Before the file's own dictionary exists: its level, no dictionary,
        // said so by the frame, as the file will record the dictionary codec.
        #[cfg(zstd_any)]
        if self.collecting_dictionary() {
            return Codec {
                compression: self.compression,
                dictionary: None,
                two_pass_seed: self.zstd_two_pass_seed,
                plain: true,
            };
        }
        #[cfg(zstd_any)]
        if let CompressionType::ZstdDict { level, .. } = self.compression
            && !self.dictionary_classes.covers(kind)
        {
            return Codec {
                compression: CompressionType::Zstd(level),
                dictionary: None,
                two_pass_seed: self.zstd_two_pass_seed,
                plain: true,
            };
        }
        #[cfg(not(zstd_any))]
        let _ = kind;
        Codec {
            compression: self.compression,
            #[cfg(zstd_any)]
            dictionary: self.zstd_dictionary.as_deref(),
            #[cfg(zstd_any)]
            two_pass_seed: self.zstd_two_pass_seed,
            #[cfg(not(zstd_any))]
            dictionary: core::marker::PhantomData,
            plain: false,
        }
    }

    /// [`Self::codec`] for a worker to hold.
    #[cfg(feature = "std")]
    fn owned_codec(&self, kind: FrameKind) -> OwnedCodec {
        let codec = self.codec(kind);
        OwnedCodec {
            compression: codec.compression,
            #[cfg(zstd_any)]
            dictionary: codec
                .dictionary
                .and(self.zstd_dictionary.as_ref())
                .map(alloc::sync::Arc::clone),
            #[cfg(zstd_any)]
            two_pass_seed: codec.two_pass_seed,
            plain: codec.plain,
        }
    }

    /// The codec a value of `value_len` bytes written next would be
    /// compressed under as a whole, for a worker to compress it ahead: a
    /// packed record or a frame of its own. `None` for a value that shares a
    /// block, whose compression is the block's, one that is chunked, whose
    /// chunks compress on the workers already, and in a file that compresses
    /// nothing.
    ///
    /// `queued` is the bytes of the values handed out to be compressed ahead
    /// and not written yet, which this value follows into the file. A file
    /// gathering its own dictionary writes its values without it until the
    /// values written fill it, so the codec of this one is known from what is
    /// gathered and queued: the codec without the dictionary while they leave
    /// room in it, and `None` once they fill it, since the dictionary this
    /// value is written under does not exist yet.
    #[cfg(feature = "std")]
    #[cfg_attr(
        not(zstd_any),
        expect(unused_variables, reason = "only a zstd file gathers a dictionary")
    )]
    pub(crate) fn value_codec(&self, value_len: u32, queued: u64) -> Option<OwnedCodec> {
        if self.compression == CompressionType::None {
            return None;
        }
        #[cfg(zstd_any)]
        if self.collecting_dictionary()
            && self.dictionary_samples.len() as u64 + queued >= u64::from(self.file_dictionary)
        {
            return None;
        }
        let FrameSizes {
            block_target,
            chunk_size,
        } = self.sizes;
        if self.packs() && value_len < block_target {
            return Some(self.owned_codec(FrameKind::Block));
        }
        (value_len >= block_target && value_len <= chunk_size)
            .then(|| self.owned_codec(FrameKind::Single))
    }

    /// The number the next frame written gets.
    fn next_frame(&self) -> crate::Result<u32> {
        u32::try_from(self.frames.len())
            .map_err(|_| crate::Error::InvalidHeader("blob file holds u32::MAX frames"))
    }

    /// Writes `bytes`, one encoded frame, at the current offset.
    fn put_frame(&mut self, bytes: &[u8]) -> crate::Result<()> {
        self.put_frame_parts(bytes, &[])
    }

    /// Writes one encoded frame given as `head` followed by `tail`, at the
    /// current offset, so a payload borrowed from elsewhere is not staged
    /// first.
    fn put_frame_parts(&mut self, head: &[u8], tail: &[u8]) -> crate::Result<()> {
        self.frames.push(self.offset);
        self.writer.write_all(head)?;
        self.writer.write_all(tail)?;
        let len = (head.len() + tail.len()) as u64;
        self.offset += len;
        self.written_blob_bytes += len;
        Ok(())
    }

    /// Encodes and writes one frame. A payload stored as it is goes to the
    /// file from where it lies, not through the frame buffer.
    fn emit<'e, E: core::borrow::Borrow<EntrySpec<'e>>>(
        &mut self,
        kind: FrameKind,
        entries: impl IntoIterator<Item = E, IntoIter: ExactSizeIterator>,
        payload: &[u8],
    ) -> crate::Result<()> {
        let mut scratch = core::mem::take(&mut self.scratch);
        let encoded = encode_frame_parts(kind, entries, payload, self.codec(kind), &mut scratch);
        let written = encoded
            .and_then(|as_is| self.put_frame_parts(&scratch, if as_is { payload } else { &[] }));
        self.scratch = scratch;
        written
    }

    /// [`Self::emit`] for a frame of one whole value, compressed ahead as
    /// `pre` when it was (see [`PreCompressed`]).
    fn emit_pre(
        &mut self,
        kind: FrameKind,
        entries: &[EntrySpec<'_>],
        payload: &[u8],
        pre: Option<PreCompressed>,
    ) -> crate::Result<()> {
        if pre.is_none() {
            return self.emit(kind, entries, payload);
        }
        let mut scratch = core::mem::take(&mut self.scratch);
        let codec = self.codec(kind);
        let encoded = PreCompressed::compressed_for(pre, payload, codec).and_then(|compressed| {
            encode_frame_parts_compressed(
                kind,
                entries,
                payload,
                codec,
                compressed.as_deref(),
                &mut scratch,
            )
        });
        let written = encoded
            .and_then(|as_is| self.put_frame_parts(&scratch, if as_is { payload } else { &[] }));
        self.scratch = scratch;
        written
    }

    /// Encodes `value` as chunks of `chunk_size` bytes, in order: all of them
    /// before any is written, since the head that precedes them lists how
    /// each is stored. Compressed chunks are encoded on the parallel workers
    /// when the writer has them and the value has more than one chunk;
    /// without a codec an encode is a checksum, which a worker would only add
    /// a copy to.
    fn encode_chunks(&self, value: &[u8], chunk_size: usize) -> crate::Result<Vec<EncodedChunk>> {
        let mut encoded = Vec::with_capacity(value.len().div_ceil(chunk_size));
        #[cfg(feature = "std")]
        if let Some(parallel) = &self.parallel
            && parallel.threads > 1
            && self.compression != CompressionType::None
            && value.len() > chunk_size
        {
            let codec = self.owned_codec(FrameKind::Chunk);
            // Two chunks in flight per worker keep every worker fed.
            let in_flight = parallel.threads * 2;
            let mut pipeline = crate::table::writer::ordered_pipeline::OrderedPipeline::new(
                alloc::sync::Arc::clone(&parallel.spawner),
                codec,
                parallel.threads,
                in_flight,
            );
            #[expect(
                clippy::expect_used,
                reason = "taken only while a chunk is pending, which take_next then returns"
            )]
            for chunk in value.chunks(chunk_size) {
                if pipeline.pending() == in_flight {
                    encoded.push(pipeline.take_next().expect("a chunk is pending")?);
                }
                pipeline.submit(ChunkJob(chunk.to_vec()));
            }
            #[expect(
                clippy::expect_used,
                reason = "taken only while a chunk is pending, which take_next then returns"
            )]
            while pipeline.pending() > 0 {
                encoded.push(pipeline.take_next().expect("a chunk is pending")?);
            }
            return Ok(encoded);
        }
        let codec = self.codec(FrameKind::Chunk);
        for chunk in value.chunks(chunk_size) {
            encoded.push(encode_chunk(chunk, codec)?);
        }
        Ok(encoded)
    }

    /// Writes `chunks`, the chunks of `value` of `chunk_size` bytes each as
    /// encoded, back to back, each where the frame index places it. A run of
    /// chunks stored as they are is one range of the value, written from it
    /// in one call, as a frame of the whole value would be.
    fn put_chunks(
        &mut self,
        value: &[u8],
        chunk_size: usize,
        chunks: &[EncodedChunk],
    ) -> crate::Result<()> {
        let mut run_start: Option<usize> = None;
        for (at, (compressed, _)) in chunks.iter().enumerate() {
            let from = at * chunk_size;
            match compressed {
                None => {
                    let start = *run_start.get_or_insert(from);
                    mark(&mut self.chunk_marks, self.frames.len());
                    self.frames.push(self.offset + (from - start) as u64);
                }
                Some(bytes) => {
                    if let Some(start) = run_start.take() {
                        self.put_bytes(value.get(start..from).unwrap_or_default())?;
                    }
                    mark(&mut self.chunk_marks, self.frames.len());
                    self.frames.push(self.offset);
                    self.put_bytes(bytes)?;
                }
            }
        }
        if let Some(start) = run_start {
            self.put_bytes(value.get(start..).unwrap_or_default())?;
        }
        Ok(())
    }

    /// Writes `bytes` at the current offset, as part of the frames placed so
    /// far.
    fn put_bytes(&mut self, bytes: &[u8]) -> crate::Result<()> {
        self.writer.write_all(bytes)?;
        self.offset += bytes.len() as u64;
        self.written_blob_bytes += bytes.len() as u64;
        Ok(())
    }

    /// Writes the block waiting for its values, packed or not, if any.
    fn close_block(&mut self) -> crate::Result<()> {
        self.close_packed()?;
        if self.block.is_empty() {
            return Ok(());
        }
        let block = core::mem::take(&mut self.block);
        let mut key_start = 0;
        // Taken out of the block's own storage as they are encoded, with no
        // list built for them.
        let entries = block.entries.iter().map(|&(seqno, key_end, value_len)| {
            let key = block.keys.get(key_start..key_end).unwrap_or_default();
            key_start = key_end;
            EntrySpec {
                key,
                seqno,
                value_len,
            }
        });
        let before = self.offset;
        let written = self.emit(FrameKind::Block, entries, &block.payload);
        self.block_frame_bytes += self.offset - before;
        self.block = block;
        self.block.clear();
        written
    }

    fn note_key(&mut self, key: &[u8], value_len: u32) {
        if self.first_key.is_none() {
            self.first_key = Some(key.into());
        }
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.item_count += 1;
        self.uncompressed_bytes += u64::from(value_len);
    }

    /// [`Self::note_key`] for a run of values written at once: `count` of
    /// them from `first` to `last`, `value_bytes` decoded.
    fn note_keys(&mut self, first: &[u8], last: &[u8], count: u64, value_bytes: u64) {
        if self.first_key.is_none() {
            self.first_key = Some(first.into());
        }
        self.last_key.clear();
        self.last_key.extend_from_slice(last);
        self.item_count += count;
        self.uncompressed_bytes += value_bytes;
    }

    /// Writes an item into the file, into the frame its size calls for, and
    /// returns where it lies.
    ///
    /// Items need to be written in key order.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs, or
    /// `Err(Error::DecompressedSizeTooLarge { .. })` if the value exceeds the
    /// 256 MiB limit.
    ///
    /// # Panics
    ///
    /// Panics if the key is empty or longer than `u16::MAX`.
    pub(crate) fn write(
        &mut self,
        key: &[u8],
        seqno: SeqNo,
        value: &[u8],
    ) -> crate::Result<FrameRef> {
        self.write_pre(key, seqno, value, None)
    }

    /// [`Self::write`], the value compressed ahead as `pre` when it was (see
    /// [`Self::value_codec`]): taken when that is still the codec the value
    /// is written under, compressed again otherwise.
    ///
    /// # Errors
    ///
    /// As [`Self::write`].
    ///
    /// # Panics
    ///
    /// As [`Self::write`].
    pub(crate) fn write_pre(
        &mut self,
        key: &[u8],
        seqno: SeqNo,
        value: &[u8],
        pre: Option<PreCompressed>,
    ) -> crate::Result<FrameRef> {
        assert_ne!(key.len(), 0, "a blob is written under a non-empty key");
        assert!(u16::try_from(key.len()).is_ok(), "keys are u16 length max");
        let value_len = check_size_cap(value.len())?;

        let FrameSizes {
            block_target,
            chunk_size,
        } = self.sizes;
        // A small value that packs is a record of its own in a packed block,
        // read without its neighbours, and under the file's own dictionary a
        // sample until the dictionary exists.
        if self.packs() && value_len < block_target {
            #[cfg(zstd_any)]
            let sample = self.collecting_dictionary();
            let at = self.write_packed(key, seqno, value, pre)?;
            self.note_key(key, value_len);
            #[cfg(zstd_any)]
            if sample {
                self.sample_for_dictionary(value);
            }
            self.writeback_if_due()?;
            return Ok(at);
        }
        // A value of any frame is a sample of the file's own dictionary until
        // the dictionary exists, taken after the value is written without it.
        #[cfg(zstd_any)]
        let sample = self.collecting_dictionary();
        let at = if value_len < block_target {
            // A block is sized by its entries, keys and values alike: small
            // values under long keys would otherwise fill it with keys.
            let pending = self.block.len();
            let entry = (super::frame::ENTRY_HEADER_LEN + key.len()) as u64 + u64::from(value_len);
            let full = pending * 100 >= u64::from(block_target) * BLOCK_FILL_PERCENT;
            if !self.block.is_empty()
                && ((full && pending + entry > u64::from(block_target))
                    || self.block.entries.len() == usize::from(u16::MAX))
            {
                self.close_block()?;
            }
            let frame = self.next_frame()?;
            #[expect(
                clippy::cast_possible_truncation,
                reason = "a block closes at u16::MAX values"
            )]
            let index = self.block.entries.len() as u16;
            self.block.keys.extend_from_slice(key);
            self.block
                .entries
                .push((seqno, self.block.keys.len(), value_len));
            self.block.payload.extend_from_slice(value);
            // The value's entry as the block holds it, uncompressed: its share
            // of the block once compressed is not known yet. A key is at most
            // u16::MAX bytes and the value below the block target, a u32.
            #[expect(clippy::cast_possible_truncation, reason = "bounded as above")]
            let entry = (super::frame::ENTRY_HEADER_LEN + key.len()) as u32 + value_len;
            self.block_entry_bytes += u64::from(entry);
            FrameRef {
                frame,
                index,
                stored: crate::blob_tree::handle::Stored::InBlock(entry),
            }
        } else if value_len <= chunk_size {
            self.close_block()?;
            let frame = self.next_frame()?;
            let entry = EntrySpec {
                key,
                seqno,
                value_len,
            };
            let before = self.offset;
            self.emit_pre(FrameKind::Single, &[entry], value, pre)?;
            FrameRef {
                frame,
                index: 0,
                stored: self.stored_since(before),
            }
        } else {
            self.close_block()?;
            let frame = self.next_frame()?;
            let chunks = value_len.div_ceil(chunk_size);
            let entry = EntrySpec {
                key,
                seqno,
                value_len,
            };
            let before = self.offset;
            let encoded = self.encode_chunks(value, chunk_size as usize)?;
            if encoded.len() != chunks as usize {
                return Err(crate::Error::InvalidHeader("blob chunk count"));
            }
            let specs: Vec<ChunkSpec> = encoded.iter().map(|(_, spec)| *spec).collect();
            self.emit(FrameKind::Head, &[entry], &head_payload(chunk_size, &specs))?;
            self.put_chunks(value, chunk_size as usize, &encoded)?;
            FrameRef {
                frame,
                index: 0,
                stored: self.stored_since(before),
            }
        };
        self.note_key(key, value_len);
        #[cfg(zstd_any)]
        if sample {
            self.sample_for_dictionary(value);
        }
        self.writeback_if_due()?;
        Ok(at)
    }

    /// Writes `value` as a record of the packed block being gathered, closing
    /// it first when it holds the block target or a record starting past it
    /// would not be named by a handle's index. A read of one value reads its
    /// record alone whatever the block holds; the block's size is what a
    /// scan, a collection and the frame index see, as of a block of entries.
    fn write_packed(
        &mut self,
        key: &[u8],
        seqno: SeqNo,
        value: &[u8],
        pre: Option<PreCompressed>,
    ) -> crate::Result<FrameRef> {
        use super::packed::{PACKED_REACH, encode_record};

        // A record can be far smaller than its value, so the block is bounded
        // by what its values decode to as well as by its records' bytes.
        if !self.packed.is_empty()
            && (self.packed.len() as u64 >= u64::from(self.sizes.block_target)
                || self.packed.len() > PACKED_REACH
                || self.packed_count == u16::MAX
                || self.packed_raw + value.len() as u64 > MAX_DECOMPRESSION_SIZE as u64)
        {
            self.close_packed()?;
        }
        let frame = self.next_frame()?;
        #[expect(
            clippy::cast_possible_truncation,
            reason = "a packed block closes before a record would start past u16::MAX"
        )]
        let index = self.packed.len() as u16;
        // Compressed before the record goes into the block, which nothing
        // takes out of the writer meanwhile: a compression that fails, or
        // unwinds, leaves the block's records as they were.
        let codec = self.codec(FrameKind::Block);
        let (compression, plain) = (codec.compression, codec.plain);
        let compressed = PreCompressed::compressed_for(pre, value, codec)?;
        let len = encode_record(
            &mut self.packed,
            key,
            seqno,
            value,
            (compression, plain),
            compressed.as_deref(),
        )?;
        self.packed_count += 1;
        self.packed_raw += value.len() as u64;
        #[expect(
            clippy::cast_possible_truncation,
            reason = "a record holds a value below the block target and a u16 key"
        )]
        let len = len as u32;
        self.block_entry_bytes += u64::from(len);
        Ok(FrameRef {
            frame,
            index,
            stored: crate::blob_tree::handle::Stored::Packed(len),
        })
    }

    /// Writes the packed block being gathered, if any.
    fn close_packed(&mut self) -> crate::Result<()> {
        if self.packed.is_empty() {
            return Ok(());
        }
        #[expect(
            clippy::cast_possible_truncation,
            reason = "a packed block closes before its values decode past MAX_DECOMPRESSION_SIZE"
        )]
        let raw_len = self.packed_raw as u32;
        let header = super::packed::packed_header(self.packed_count, raw_len, &self.packed);
        let records = core::mem::take(&mut self.packed);
        let before = self.offset;
        let written = self.put_frame_parts(&header, &records);
        self.block_frame_bytes += self.offset - before;
        self.packed = records;
        self.packed.clear();
        self.packed_count = 0;
        self.packed_raw = 0;
        written
    }

    /// Writes `frames`, the encoded frames of one whole value (a single frame,
    /// or a head and its chunks) under the codec this file records, without
    /// encoding them again, and returns where the value lies. `value_len` is
    /// the value's decoded length.
    ///
    /// The scan that read the frames checked each against its checksum, so
    /// only their shape is checked here: one value under `key`, a head's
    /// chunks where its table puts them.
    ///
    /// # Errors
    ///
    /// [`crate::Error::InvalidHeader`] when the frames are not one whole value
    /// under `key`, and any IO error.
    pub(crate) fn write_verbatim(
        &mut self,
        key: &[u8],
        frames: &[crate::Slice],
        value_len: u32,
    ) -> crate::Result<FrameRef> {
        self.write_frames(key, frames, value_len, None)
    }

    /// [`Self::write_verbatim`] with the value listed at `seqno`: only the
    /// first frame's header and entry are rewritten, its payload written from
    /// `frames` as stored.
    ///
    /// # Errors
    ///
    /// As [`Self::write_verbatim`].
    pub(crate) fn write_verbatim_at(
        &mut self,
        key: &[u8],
        frames: &[crate::Slice],
        value_len: u32,
        seqno: crate::SeqNo,
    ) -> crate::Result<FrameRef> {
        self.write_frames(key, frames, value_len, Some(seqno))
    }

    fn write_frames(
        &mut self,
        key: &[u8],
        frames: &[crate::Slice],
        value_len: u32,
        relist: Option<crate::SeqNo>,
    ) -> crate::Result<FrameRef> {
        let malformed = || crate::Error::InvalidHeader("verbatim blob frames");
        self.leave_file_dictionary()?;
        let first = frames.first().ok_or_else(malformed)?;
        let header = super::frame::parse_header(first)?;
        let chunks = frames.len() - 1;
        let shape_ok = match header.kind {
            FrameKind::Single => chunks == 0,
            FrameKind::Head => chunks > 0,
            FrameKind::Block | FrameKind::Chunk => false,
        };
        use super::frame::{ENTRY_HEADER_LEN, FRAME_HEADER_LEN};

        let entries_end = FRAME_HEADER_LEN + header.entries_len as usize;
        let listed_key = first.get(FRAME_HEADER_LEN + ENTRY_HEADER_LEN..entries_end);
        // The entry's value length follows its seqno and key length; a head's
        // payload, stored as it is, is its chunk size then its chunk count.
        let u32_at = |at: usize| {
            first
                .get(at..at + 4)
                .and_then(|b| <[u8; 4]>::try_from(b).ok())
                .map(u32::from_le_bytes)
        };
        let listed_len = u32_at(FRAME_HEADER_LEN + 10);
        let listed_chunks = match header.kind {
            FrameKind::Head => u32_at(entries_end + 4).map(|n| n as usize),
            _ => Some(0),
        };
        // Each slice is one whole frame: the index records one offset per
        // slice, so a slice cut short or running past its frame would put the
        // index out of step with the frames.
        if !shape_ok
            || first.len() as u64 != header.frame_len()
            || listed_key != Some(key)
            || listed_len != Some(value_len)
            || listed_chunks != Some(chunks)
        {
            return Err(malformed());
        }
        // Each chunk takes what the head's table says it takes, so the frames
        // the copy places are where the head's readers look.
        if header.kind == FrameKind::Head
            && !super::frame::chunks_fit_head(
                first,
                entries_end,
                u64::from(value_len),
                frames.get(1..).unwrap_or_default(),
                self.compression,
            )?
        {
            return Err(malformed());
        }
        // Relisted before anything is written, so a refusal leaves the file
        // as it was.
        let mut prefix = core::mem::take(&mut self.scratch);
        let relisted = match relist {
            Some(seqno) => super::frame::relist_prefix(first, seqno, &mut prefix),
            None => Ok(None),
        };
        let written = relisted.and_then(|relisted| {
            self.close_block()?;
            let frame = self.next_frame()?;
            let before = self.offset;
            match relisted {
                Some(payload_at) => {
                    self.put_frame_parts(&prefix, first.get(payload_at..).unwrap_or_default())?;
                }
                None => self.put_frame(first)?,
            }
            // The frames after the first are the chunks of a head.
            for bytes in frames.get(1..).unwrap_or_default() {
                mark(&mut self.chunk_marks, self.frames.len());
                self.put_frame(bytes)?;
            }
            Ok((frame, before))
        });
        self.scratch = prefix;
        let (frame, before) = written?;
        self.note_key(key, value_len);
        self.writeback_if_due()?;
        Ok(FrameRef {
            frame,
            index: 0,
            stored: self.stored_since(before),
        })
    }

    /// Writes `block.stored`, a block frame as stored under the codec this
    /// file records, without encoding it again, and returns its frame number:
    /// each of its values keeps its place in it.
    ///
    /// The scan that read the block checked it whole and counted what it
    /// holds into `block`, so it is neither checked nor parsed again here:
    /// only its header is read, to refuse a frame that is no such block.
    ///
    /// # Errors
    ///
    /// [`crate::Error::InvalidHeader`] when `block.stored` is not one whole
    /// block of `block.values` values, and any IO error.
    pub(crate) fn write_block_verbatim(&mut self, block: &CopiedBlock<'_>) -> crate::Result<u32> {
        self.leave_file_dictionary()?;
        let header = super::frame::parse_header(block.stored)?;
        if header.kind != FrameKind::Block
            || header.count != block.values
            || header.frame_len() != block.stored.len() as u64
        {
            return Err(crate::Error::InvalidHeader("verbatim blob block"));
        }
        self.close_block()?;
        let frame = self.next_frame()?;
        let before = self.offset;
        self.put_frame(block.stored)?;
        self.block_frame_bytes += self.offset - before;
        self.block_entry_bytes += block.entry_bytes;
        self.note_keys(
            block.first_key,
            block.last_key,
            u64::from(block.values),
            block.value_bytes,
        );
        self.writeback_if_due()?;
        Ok(frame)
    }

    /// The bytes written since the offset was `before`: what the frames of
    /// one value take.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "a frame never stores more than its raw bytes, so one value takes at most \
                  the 256 MiB cap plus its frames' headers, and a chunk of at least 4 KiB \
                  keeps those headers within the rest of a u32"
    )]
    fn stored_since(&self, before: u64) -> crate::blob_tree::handle::Stored {
        crate::blob_tree::handle::Stored::Frames((self.offset - before) as u32)
    }

    /// Writes the pending block, the frame index and the metadata, and returns
    /// the metadata, the file's checksum and its frame index: where each frame
    /// starts, then where the data ends.
    ///
    /// # Errors
    ///
    /// Any IO error.
    pub(crate) fn finish(mut self) -> crate::Result<(Metadata, Checksum, FrameIndex)> {
        self.close_block()?;

        // The file's own dictionary, which its reader decodes with: what the
        // codec it records names, since a dictionary's id is its content's.
        #[cfg(zstd_any)]
        if self.dictionary_in_file
            && let Some(dict) = &self.zstd_dictionary
        {
            self.writer.start("dict")?;
            self.writer.write_all(dict.raw())?;
        }

        self.writer.start("frames")?;
        let index = encode_frame_index_marked(&self.frames, self.offset, &self.chunk_marks);
        self.writer.write_all(&index)?;
        self.frames.push(self.offset);
        let frames = FrameIndex::new(
            core::mem::take(&mut self.frames),
            core::mem::take(&mut self.chunk_marks),
        );

        self.writer.start("meta")?;

        // Write metadata
        let metadata = Metadata {
            id: self.blob_file_id,
            version: META_VERSION,
            created_at: crate::time::now_on(self.clock.as_ref()).as_nanos(),
            item_count: self.item_count,
            total_compressed_bytes: self.written_blob_bytes,
            total_uncompressed_bytes: self.uncompressed_bytes,
            block_entry_bytes: self.block_entry_bytes,
            block_frame_bytes: self.block_frame_bytes,
            #[expect(clippy::expect_used, reason = "should have written at least 1 item")]
            key_range: KeyRange::new((
                self.first_key
                    .clone()
                    .expect("should have written at least 1 item"),
                UserKey::from(self.last_key.as_slice()),
            )),
            // The codec every frame of the file is under: the writer's own,
            // which verbatim frames written into it share.
            compression: self.compression,
            lifetime_class: self.lifetime_class,
        };
        metadata.encode_into(&mut self.writer)?;

        let checksum = self.writer.into_inner()?;
        // Data and size are all a read of the file needs, so `sync_data`. A
        // deferred file is synced by the install that persists it.
        if self.durability == crate::config::FlushDurability::Synced {
            FsFile::sync_data_with(&**checksum.get_ref().get_ref(), self.sync_mode)?;
        }
        let checksum = checksum.checksum();

        Ok((metadata, checksum, frames))
    }
}

#[cfg(test)]
#[expect(
    clippy::indexing_slicing,
    clippy::expect_used,
    clippy::cast_possible_truncation,
    reason = "test code: fixture files are small enough for every cast"
)]
mod tests;

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};

use super::frame::{Codec, EntrySpec, FrameKind, encode_frame, head_payload};
use super::meta::{META_VERSION, Metadata};
use crate::io::BufWriter;
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

impl FrameSizes {
    /// A block of 16 KiB, chunks of 256 KiB.
    pub(crate) const DEFAULT: Self = Self {
        block_target: 16 * 1024,
        chunk_size: 256 * 1024,
    };
}

impl Default for FrameSizes {
    fn default() -> Self {
        Self::DEFAULT
    }
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

/// The frame index a blob file ends its data with: where each frame starts,
/// then where the data ends, then a checksum over both.
///
/// `[count; 4][offset; 8] * count [end; 8][xxh3; 8]`
pub(crate) fn encode_frame_index(starts: &[u64], end: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + (starts.len() + 2) * 8);
    #[expect(
        clippy::cast_possible_truncation,
        reason = "a frame number is a u32, so a file holds at most u32::MAX frames"
    )]
    out.extend_from_slice(&(starts.len() as u32).to_le_bytes());
    for start in starts {
        out.extend_from_slice(&start.to_le_bytes());
    }
    out.extend_from_slice(&end.to_le_bytes());
    let digest = xxhash_rust::xxh3::xxh3_64(&out);
    out.extend_from_slice(&digest.to_le_bytes());
    out
}

/// Decodes a frame index: each frame's start followed by the data end, one
/// more entry than there are frames.
///
/// # Errors
///
/// [`crate::Error::InvalidHeader`] for an index that is short, long, fails its
/// checksum, or lists starts that do not increase within the data.
pub(crate) fn decode_frame_index(bytes: &[u8]) -> crate::Result<alloc::sync::Arc<[u64]>> {
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
    let offsets: Vec<u64> = rest
        .chunks_exact(8)
        .filter_map(|word| word.try_into().ok().map(u64::from_le_bytes))
        .collect();
    if offsets.windows(2).any(|pair| pair.first() >= pair.get(1)) {
        return Err(malformed());
    }
    Ok(offsets.into())
}

/// Blob file writer
pub(crate) struct Writer {
    pub(crate) tree_id: TreeId,
    pub path: PathBuf,
    pub(crate) blob_file_id: BlobFileId,

    #[expect(clippy::struct_field_names)]
    writer: crate::sfa::Writer<ChecksummedWriter<BufWriter<Box<dyn FsFile>>>>,

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

    /// Whether zstd levels 19-22 run the `btultra2` two-pass seed. Defaults to
    /// `true`, the codec's own behaviour; set from the live runtime config via
    /// [`Self::use_zstd_two_pass_seed`].
    #[cfg(zstd_any)]
    pub(crate) zstd_two_pass_seed: bool,

    /// How this file sizes its frames.
    pub(crate) sizes: FrameSizes,

    /// Where each frame written so far starts.
    frames: Vec<u64>,

    /// The block the next small value joins.
    block: PendingBlock,

    /// A frame being encoded, kept so a writer allocates it once.
    scratch: Vec<u8>,
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
            BufWriter::with_capacity(u16::MAX.into(), file),
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
            zstd_two_pass_seed: true,

            sizes: FrameSizes::DEFAULT,
            frames: Vec::new(),
            block: PendingBlock::default(),
            scratch: Vec::new(),
        })
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
    /// at least `writeback_bytes` of it gathered.
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

    /// Returns the blob file ID.
    #[must_use]
    pub(crate) fn blob_file_id(&self) -> BlobFileId {
        self.blob_file_id
    }

    /// Bytes the file holds so far, the values waiting for their block counted.
    #[must_use]
    pub(crate) fn len_written(&self) -> u64 {
        self.offset + self.block.len()
    }

    fn codec(&self) -> Codec<'_> {
        Codec {
            compression: self.compression,
            #[cfg(zstd_any)]
            dictionary: self.zstd_dictionary.as_deref(),
            #[cfg(zstd_any)]
            two_pass_seed: self.zstd_two_pass_seed,
            #[cfg(not(zstd_any))]
            dictionary: core::marker::PhantomData,
        }
    }

    /// The number the next frame written gets.
    fn next_frame(&self) -> crate::Result<u32> {
        u32::try_from(self.frames.len())
            .map_err(|_| crate::Error::InvalidHeader("blob file holds u32::MAX frames"))
    }

    /// Writes `bytes`, one encoded frame, at the current offset.
    fn put_frame(&mut self, bytes: &[u8]) -> crate::Result<()> {
        self.frames.push(self.offset);
        self.writer.write_all(bytes)?;
        self.offset += bytes.len() as u64;
        self.written_blob_bytes += bytes.len() as u64;
        self.writeback_if_due()
    }

    /// Encodes and writes one frame.
    fn emit(
        &mut self,
        kind: FrameKind,
        entries: &[EntrySpec<'_>],
        payload: &[u8],
    ) -> crate::Result<()> {
        let mut scratch = core::mem::take(&mut self.scratch);
        let encoded = encode_frame(kind, entries, payload, self.codec(), &mut scratch);
        let written = encoded.and_then(|()| self.put_frame(&scratch));
        self.scratch = scratch;
        written
    }

    /// Writes the block waiting for its values, if any.
    fn close_block(&mut self) -> crate::Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }
        let block = core::mem::take(&mut self.block);
        let mut key_start = 0;
        let entries: Vec<EntrySpec<'_>> = block
            .entries
            .iter()
            .map(|&(seqno, key_end, value_len)| {
                let key = block.keys.get(key_start..key_end).unwrap_or_default();
                key_start = key_end;
                EntrySpec {
                    key,
                    seqno,
                    value_len,
                }
            })
            .collect();
        let before = self.offset;
        let written = self.emit(FrameKind::Block, &entries, &block.payload);
        drop(entries);
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
        assert_ne!(key.len(), 0, "a blob is written under a non-empty key");
        assert!(u16::try_from(key.len()).is_ok(), "keys are u16 length max");
        let value_len = check_size_cap(value.len())?;

        let FrameSizes {
            block_target,
            chunk_size,
        } = self.sizes;
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
            self.emit(FrameKind::Single, &[entry], value)?;
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
            self.emit(FrameKind::Head, &[entry], &head_payload(chunk_size, chunks))?;
            for chunk in value.chunks(chunk_size as usize) {
                self.emit(FrameKind::Chunk, &[], chunk)?;
            }
            FrameRef {
                frame,
                index: 0,
                stored: self.stored_since(before),
            }
        };
        self.note_key(key, value_len);
        Ok(at)
    }

    /// Writes `frames`, the encoded frames of one whole value (a single frame,
    /// or a head and its chunks) under the codec this file records, without
    /// encoding them again, and returns where the value lies. `value_len` is
    /// the value's decoded length.
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
        let malformed = || crate::Error::InvalidHeader("verbatim blob frames");
        let first = frames.first().ok_or_else(malformed)?;
        let header = super::frame::parse_header(first)?;
        let chunks = frames.len() - 1;
        let shape_ok = match header.kind {
            FrameKind::Single => chunks == 0,
            FrameKind::Head => chunks > 0,
            FrameKind::Block | FrameKind::Chunk => false,
        };
        let listed_key = first.get(
            super::frame::FRAME_HEADER_LEN + super::frame::ENTRY_HEADER_LEN
                ..super::frame::FRAME_HEADER_LEN + header.entries_len as usize,
        );
        if !shape_ok || listed_key != Some(key) {
            return Err(malformed());
        }
        for chunk in frames.get(1..).unwrap_or_default() {
            if super::frame::parse_header(chunk)?.kind != FrameKind::Chunk {
                return Err(malformed());
            }
        }
        self.close_block()?;
        let frame = self.next_frame()?;
        let before = self.offset;
        for bytes in frames {
            self.put_frame(bytes)?;
        }
        self.note_key(key, value_len);
        Ok(FrameRef {
            frame,
            index: 0,
            stored: self.stored_since(before),
        })
    }

    /// The bytes written since the offset was `before`: what the frames of
    /// one value take.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "a frame never stores more than its raw bytes, so one value takes at most \
                  the 256 MiB cap plus its frames' headers"
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
    pub(crate) fn finish(mut self) -> crate::Result<(Metadata, Checksum, alloc::sync::Arc<[u64]>)> {
        self.close_block()?;

        self.writer.start("frames")?;
        let index = encode_frame_index(&self.frames, self.offset);
        self.writer.write_all(&index)?;
        self.frames.push(self.offset);
        let frames: alloc::sync::Arc<[u64]> = core::mem::take(&mut self.frames).into();

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
#[expect(clippy::indexing_slicing, clippy::expect_used, reason = "test code")]
mod tests;

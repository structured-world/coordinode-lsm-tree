// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::frame::{
    DecodedFrame, FRAME_HEADER_LEN, FRAME_MAGIC, FrameKind, decode_frame, parse_header,
    undecoded_frame,
};
use crate::fs::{Fs, FsOpenOptions};
use crate::io::BufReader;
#[cfg(not(feature = "std"))]
use crate::io::{Read, Seek, SeekFrom};
use crate::path::Path;
use crate::{CompressionType, SeqNo, Slice, UserKey, UserValue, vlog::BlobFileId};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
#[cfg(feature = "std")]
use std::io::{Read, Seek, SeekFrom};

/// What a scanner decodes frames with: the file's codec and, for a dictionary
/// codec, the dictionary.
#[derive(Clone)]
pub(crate) struct ScanCodec {
    pub(crate) compression: CompressionType,
    #[cfg(zstd_any)]
    pub(crate) dictionary: Option<Arc<crate::compression::ZstdDictionary>>,
}

impl ScanCodec {
    /// The codec `blob_file` records, with its pinned dictionary.
    pub(crate) fn of(blob_file: &crate::BlobFile) -> Self {
        Self {
            compression: blob_file.compression(),
            #[cfg(zstd_any)]
            dictionary: blob_file.0.zstd_dictionary.clone(),
        }
    }
}

/// How the scanner finds where each frame starts.
enum Bounds {
    /// From the file's frame index: each frame's start, then the data end.
    /// A damaged frame costs that frame alone: the next one is where the
    /// index puts it.
    Index(Arc<[u64]>),
    /// Without a readable index: each frame from the length the one before
    /// declares, and past damage from a byte-wise search for the next magic.
    Chain,
}

/// Which frames a scan decodes.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Decode {
    /// Every frame, so a scan proves each value round-trips (repair, salvage).
    All,
    /// The blocks alone, whose values are taken one by one: a frame a value
    /// has to itself, single or chunk, is checked as stored, the form a copy
    /// writes, without decompressing it for nothing.
    Blocks,
}

/// Reads through a blob file in order, one value at a time.
///
/// Termination is determined by the SFA table-of-contents: the scanner stops
/// when the read position reaches the end of the "data" section, not when it
/// meets particular bytes.
pub(crate) struct Scanner {
    pub(crate) blob_file_id: BlobFileId,
    inner: BufReader<crate::table::util::TimedFile>,
    is_terminated: bool,

    /// Byte offset where the "data" section ends (from the SFA TOC).
    data_end: u64,

    bounds: Bounds,

    /// The number of the next frame to read.
    next_frame: u32,

    codec: ScanCodec,

    decode: Decode,

    /// The single frame or block read last, whose values are yielded from
    /// it one by one.
    values: Option<FrameValues>,

    /// Set when the chain walk resynchronized at a magic found by a byte-wise
    /// search after a damaged frame, and STICKY thereafter: every value read
    /// once this is set is marked `resynced`. The magic may be an original
    /// boundary OR a checksum-valid frame nested inside the damaged frame's
    /// user-controlled bytes; the two are byte-for-byte indistinguishable, and
    /// every frame chained past it inherits that unproven anchor, so the taint
    /// never lifts. Callers that must not fabricate data (salvage) treat every
    /// `resynced` entry as untrusted. An index walk never sets it: its
    /// boundaries come from the checksummed index, not from the damaged bytes.
    resync_tainted: bool,
}

impl Scanner {
    /// Scans `blob_file` from the frame starting at `start` (a frame boundary a
    /// previous scan's [`ScanEntry::frame_end`] gave, or the file's frontier),
    /// or from its first live frame, `pace` told how long each read takes:
    /// for a copy, which yields a value with frames of its own undecoded.
    ///
    /// # Errors
    ///
    /// [`crate::Error::InvalidHeader`] for a `start` that is not a frame
    /// boundary, and any IO error.
    pub(crate) fn of_paced(
        blob_file: &crate::BlobFile,
        start: Option<u64>,
        pace: Option<crate::table::util::Pacer>,
    ) -> crate::Result<Self> {
        let live = blob_file.live_data_start();
        let start = start.map_or(live, |start| start.max(live));
        let mut scanner = Self::open_with(
            &blob_file.0.path,
            &*blob_file.0.fs,
            blob_file.id(),
            ScanCodec::of(blob_file),
            Some(Arc::clone(&blob_file.0.frames)),
            (start != 0).then_some(start),
            pace,
        )?;
        scanner.decode = Decode::Blocks;
        Ok(scanner)
    }

    /// Scans the blob file at `path` from `start`, or from the data start,
    /// reading its frame index from the file and walking the frames' own
    /// lengths when the index is unreadable: for a file that may be damaged
    /// (salvage, repair), whose index is not taken on trust.
    ///
    /// # Errors
    ///
    /// The file lacks a "data" section, `start` lies outside it, or an IO
    /// error occurs.
    #[cfg(any(feature = "std", test))]
    pub(crate) fn open<P: AsRef<Path>>(
        path: P,
        fs: &dyn Fs,
        blob_file_id: BlobFileId,
        codec: ScanCodec,
        start: Option<u64>,
    ) -> crate::Result<Self> {
        Self::open_with(path.as_ref(), fs, blob_file_id, codec, None, start, None)
    }

    /// [`Self::open`] from the data start, under the codec the file's
    /// metadata records and no dictionary.
    #[cfg(test)]
    pub(crate) fn new<P: AsRef<Path>>(
        path: P,
        fs: &dyn Fs,
        blob_file_id: BlobFileId,
    ) -> crate::Result<Self> {
        Self::resume_at(path, fs, blob_file_id, None)
    }

    /// [`Self::open`] from the frame at `start`, under the codec the file's
    /// metadata records and no dictionary.
    #[cfg(test)]
    pub(crate) fn resume<P: AsRef<Path>>(
        path: P,
        fs: &dyn Fs,
        blob_file_id: BlobFileId,
        start: u64,
    ) -> crate::Result<Self> {
        Self::resume_at(path, fs, blob_file_id, Some(start))
    }

    #[cfg(test)]
    fn resume_at<P: AsRef<Path>>(
        path: P,
        fs: &dyn Fs,
        blob_file_id: BlobFileId,
        start: Option<u64>,
    ) -> crate::Result<Self> {
        // The metadata alone: the frame index may be what a test damaged.
        let meta = crate::vlog::read_blob_meta(path.as_ref(), fs, blob_file_id)?;
        let codec = ScanCodec {
            compression: meta.compression,
            #[cfg(zstd_any)]
            dictionary: None,
        };
        Self::open(path, fs, blob_file_id, codec, start)
    }

    fn open_with(
        path: &Path,
        fs: &dyn Fs,
        blob_file_id: BlobFileId,
        codec: ScanCodec,
        frames: Option<Arc<[u64]>>,
        start: Option<u64>,
        pace: Option<crate::table::util::Pacer>,
    ) -> crate::Result<Self> {
        let mut file =
            crate::table::util::TimedFile::new(fs.open(path, &FsOpenOptions::new().read(true))?);
        if let Some(pace) = pace {
            file.set_pace(pace);
        }
        let (data_start, data_end, index_at) = {
            let sfa_reader = crate::sfa::Reader::from_reader(&mut file)?;
            let toc = sfa_reader.toc();
            let data = toc.section(b"data").ok_or_else(|| {
                log::error!("BlobFile: SFA TOC has no \"data\" section");
                crate::Error::InvalidHeader("BlobFile")
            })?;
            let data_start = data.pos();
            let data_end = data_start.checked_add(data.len()).ok_or_else(|| {
                log::error!(
                    "BlobFile: data section offset overflow (pos={data_start}, len={})",
                    data.len()
                );
                crate::Error::InvalidHeader("BlobFile")
            })?;
            let index_at = toc
                .section(b"frames")
                .map(|entry| (entry.pos(), entry.len()));
            (data_start, data_end, index_at)
        };

        // The index the caller trusts, else the one the file holds if it
        // reads and fits the data, else the chain.
        let frames = match (frames, index_at) {
            (Some(frames), _) => Some(frames),
            (None, Some(at)) => read_frame_index(&mut file, at, data_start..data_end)?,
            (None, None) => None,
        };

        let seek_to = start.unwrap_or(data_start);
        if seek_to < data_start || seek_to > data_end {
            log::error!(
                "BlobFile: resume offset {seek_to} outside data section [{data_start}, {data_end}]"
            );
            return Err(crate::Error::InvalidHeader("BlobFile"));
        }
        let (bounds, next_frame) = match frames {
            Some(frames) => {
                let starts = frames.get(..frames.len() - 1).unwrap_or_default();
                // At the data end there is nothing left; elsewhere `start` must
                // be a frame's own start.
                let next = match starts.binary_search(&seek_to) {
                    Ok(at) => at,
                    Err(at) if seek_to == data_end || (start.is_none() && at == 0) => at,
                    Err(_) => return Err(crate::Error::InvalidHeader("BlobFile")),
                };
                let next =
                    u32::try_from(next).map_err(|_| crate::Error::InvalidHeader("BlobFile"))?;
                (Bounds::Index(frames), next)
            }
            // Without an index a frame's number is its place from the data
            // start, which a resumed chain walk cannot know.
            None if seek_to == data_start => (Bounds::Chain, 0),
            None => return Err(crate::Error::InvalidHeader("BlobFile")),
        };
        let seek_to = match &bounds {
            Bounds::Index(frames) => frames.get(next_frame as usize).copied().unwrap_or(data_end),
            Bounds::Chain => seek_to,
        };

        file.seek(SeekFrom::Start(seek_to))?;
        Ok(Self {
            blob_file_id,
            inner: BufReader::with_capacity(32_000, file),
            is_terminated: false,
            data_end,
            bounds,
            next_frame,
            codec,
            decode: Decode::All,
            values: None,
            resync_tainted: false,
        })
    }

    /// Repositions the reader at the next frame magic strictly AFTER
    /// `frame_offset`, or at the data-section end when none remains: the
    /// chain walk's way past a frame whose lengths cannot be trusted.
    fn resync_to_next_frame(&mut self, frame_offset: u64) -> crate::Result<()> {
        const MAGIC_LEN: usize = FRAME_MAGIC.len();

        // Scan in chunks, overlapping by MAGIC_LEN - 1 bytes so a magic
        // straddling two chunks is still found.
        let mut buf = alloc::vec![0u8; 64 * 1024];
        let mut pos = frame_offset + 1;
        while pos < self.data_end {
            self.inner.seek(SeekFrom::Start(pos))?;
            // `#[allow]`, not `#[expect]`: this is a target-width-dependent lint
            // (`u64 as usize`), so a target where Clippy proves the `min()` bound
            // fits usize would leave an `#[expect]` unfulfilled under `-D warnings`.
            #[allow(
                clippy::cast_possible_truncation,
                reason = "min() bounds the window by the buffer length, which fits usize"
            )]
            let want = (self.data_end - pos).min(buf.len() as u64) as usize;
            let Some(window) = buf.get_mut(..want) else {
                break;
            };
            self.inner.read_exact(window)?;
            if let Some(hit) = window.windows(MAGIC_LEN).position(|w| w == FRAME_MAGIC) {
                self.inner.seek(SeekFrom::Start(pos + hit as u64))?;
                self.resync_tainted = true;
                return Ok(());
            }
            if want < MAGIC_LEN {
                break;
            }
            pos += (want - (MAGIC_LEN - 1)) as u64;
        }
        // No further frame: park at the section end so the next call
        // terminates cleanly.
        self.inner.seek(SeekFrom::Start(self.data_end))?;
        Ok(())
    }

    /// Where the frame at `offset` ends, by the index, or `None` in a chain
    /// walk.
    fn indexed_end(&self, frame: u32) -> Option<u64> {
        match &self.bounds {
            Bounds::Index(frames) => frames.get(frame as usize + 1).copied(),
            Bounds::Chain => None,
        }
    }

    /// Moves to frame `frame` when the index places it, past the frames before
    /// it; in a chain walk, which knows no frame's place ahead, stays where
    /// the walk already is.
    fn skip_to_frame(&mut self, frame: u32) -> crate::Result<()> {
        if let Bounds::Index(frames) = &self.bounds {
            let at = frames.get(frame as usize).copied().unwrap_or(self.data_end);
            self.next_frame = frame;
            self.inner.seek(SeekFrom::Start(at))?;
        }
        Ok(())
    }

    /// Moves past a frame that did not check: to where the index says the next
    /// one starts, or by a byte-wise search in a chain walk.
    fn skip_damaged(&mut self, frame: u32, offset: u64) -> crate::Result<()> {
        self.next_frame = frame + 1;
        match self.indexed_end(frame) {
            Some(end) => {
                self.inner.seek(SeekFrom::Start(end))?;
                Ok(())
            }
            None => self.resync_to_next_frame(offset),
        }
    }

    /// Reads the frame at the current position whole: its bytes as stored and
    /// its number, checked and decoded.
    fn read_one(&mut self) -> Option<crate::Result<(u32, u64, Slice, DecodedFrame)>> {
        let offset = fail_iter!(self.inner.stream_position());
        if offset >= self.data_end {
            self.is_terminated = true;
            return None;
        }
        let frame = self.next_frame;
        let damaged = |this: &mut Self, e: crate::Error| {
            if let Err(seek) = this.skip_damaged(frame, offset) {
                this.is_terminated = true;
                return Some(Err(seek));
            }
            Some(Err(e))
        };

        // The span to read: the index's, else the header's declared length,
        // bounded by the section either way. An index is taken only when it
        // rises strictly within the data section and ends at its end, and a
        // walk by it only ever stands at a frame's start, so a frame's end
        // lies past where the walk stands and within the section.
        let len = if let Some(end) = self.indexed_end(frame) {
            end - offset
        } else {
            if offset + FRAME_HEADER_LEN as u64 > self.data_end {
                self.is_terminated = true;
                return Some(Err(crate::Error::InvalidHeader("BlobFrame")));
            }
            let mut header = [0_u8; FRAME_HEADER_LEN];
            if let Err(e) = self.inner.read_exact(&mut header) {
                self.is_terminated = true;
                return Some(Err(e.into()));
            }
            let declared = match parse_header(&header) {
                Ok(header) => header.frame_len(),
                Err(e) => return damaged(self, e),
            };
            if offset + declared > self.data_end {
                return damaged(self, crate::Error::InvalidHeader("BlobFrame"));
            }
            if let Err(e) = self.inner.seek(SeekFrom::Start(offset)) {
                self.is_terminated = true;
                return Some(Err(e.into()));
            }
            declared
        };
        let Ok(len) = usize::try_from(len) else {
            return damaged(self, crate::Error::InvalidHeader("BlobFrame"));
        };
        // Read into an uninitialized buffer: zero-filling a frame only to
        // overwrite every byte would cost a memory write per byte relocated.
        let bytes = match Slice::from_reader(&mut self.inner, len) {
            Ok(bytes) => bytes,
            Err(e) => {
                self.is_terminated = true;
                return Some(Err(e.into()));
            }
        };
        // The kind byte follows the magic; one that does not name a frame a
        // value has to itself is decoded, damage included, and refused there.
        let own = matches!(
            bytes
                .get(FRAME_MAGIC.len())
                .copied()
                .map(FrameKind::try_from),
            Some(Ok(FrameKind::Single))
        );
        let parsed = if own && self.decode == Decode::Blocks {
            undecoded_frame(&bytes, self.codec.compression)
        } else {
            decode_frame(
                &bytes,
                self.codec.compression,
                #[cfg(zstd_any)]
                self.codec.dictionary.as_deref(),
                &mut 0,
            )
        };
        match parsed {
            Ok(decoded) => {
                self.next_frame = frame + 1;
                Some(Ok((frame, offset, bytes, decoded)))
            }
            Err(e) => damaged(self, e),
        }
    }

    /// Reads the next frame: a chunked value is yielded whole, the values of
    /// a single frame or a block one by one from [`Self::values`]. `None` at
    /// the data end.
    fn fill(&mut self) -> Option<crate::Result<Option<ScanEntry>>> {
        let (frame, offset, bytes, decoded) = match self.read_one()? {
            Ok(read) => read,
            Err(e) => return Some(Err(e)),
        };
        let resynced = self.resync_tainted;
        let frame_end = offset + bytes.len() as u64;
        match decoded.header.kind {
            FrameKind::Single | FrameKind::Block => {
                // A single frame left undecoded is yielded by its frame alone.
                let decoded_value =
                    decoded.header.kind == FrameKind::Block || self.decode == Decode::All;
                self.values = Some(FrameValues {
                    frame,
                    offset,
                    frame_end,
                    resynced,
                    decoded_value,
                    bytes,
                    decoded,
                    at: 0,
                });
                Some(Ok(None))
            }
            FrameKind::Head => {
                // The frames the file can hold bound a head's chunks before
                // room is reserved for them: the index's count, or, along the
                // chain, the bytes the data has left, as every chunk takes at
                // least one. `frame_end` lies within the section: the frame
                // was read whole in it.
                let room = match &self.bounds {
                    Bounds::Index(frames) => frames.len() - 1,
                    Bounds::Chain => {
                        let fit = self.data_end - frame_end;
                        // An upper bound: one past `usize::MAX` bounds nothing more.
                        usize::try_from(fit)
                            .map_or(usize::MAX, |fit| fit.saturating_add(frame as usize + 1))
                    }
                };
                let Ok(chunks) = super::reader::chunk_frames(&decoded, frame, room) else {
                    return Some(Err(crate::Error::InvalidHeader("BlobFrame")));
                };
                // A value whose chunks do not read is damage that ends with
                // them: by the index, the walk goes on at the frame after
                // them, so the chunks left are not read as frames; a chain
                // walk, which only the head's table placed, searches on.
                let failed = |this: &mut Self, e: crate::Error| {
                    let moved = match this.bounds {
                        Bounds::Index(_) => this.skip_to_frame(chunks.end),
                        Bounds::Chain => this.resync_to_next_frame(offset),
                    };
                    if let Err(seek) = moved {
                        this.is_terminated = true;
                        return Some(Err(seek));
                    }
                    Some(Err(e))
                };
                // One pass over the head's table, as many rows as `chunks`:
                // both are counted from it.
                let specs = match decoded.chunk_specs() {
                    Ok(specs) => specs,
                    Err(e) => return failed(self, e),
                };
                let area_len: u64 = specs.iter().map(|spec| u64::from(spec.stored_len)).sum();
                let end = frame_end + area_len;
                let placed = chunks.end > chunks.start
                    && self
                        .indexed_end(chunks.end - 1)
                        .is_none_or(|indexed| indexed == end);
                let Ok(area_len) = usize::try_from(area_len) else {
                    return failed(self, crate::Error::InvalidHeader("BlobFrame"));
                };
                if !placed || end > self.data_end {
                    return failed(self, crate::Error::InvalidHeader("BlobFrame"));
                }
                // The chunks follow the head back to back, read into an
                // uninitialized buffer as a frame is.
                let area = match Slice::from_reader(&mut self.inner, area_len) {
                    Ok(area) => area,
                    Err(e) => {
                        self.is_terminated = true;
                        return Some(Err(e.into()));
                    }
                };
                let (Some(entry), Some(_)) = (decoded.list.first(), decoded.key(0)) else {
                    return failed(self, crate::Error::InvalidHeader("BlobFrame"));
                };
                // Each chunk checked against its row as it lies: every consumer
                // copies the value as its stored frames. A scan that decodes
                // (repair, salvage) decodes each chunk too, as a read of the
                // value would: a checksum proves the bytes are the ones
                // written, not that they decode.
                let mut stored = Vec::with_capacity(specs.len() + 1);
                stored.push(bytes);
                let mut decoded_chunk = Vec::new();
                let mut from = 0;
                for spec in &specs {
                    let to = from + spec.stored_len as usize;
                    let chunk = area.slice(from..to);
                    if !spec.fits(self.codec.compression) {
                        return failed(self, crate::Error::InvalidHeader("BlobFrame"));
                    }
                    let checked = if self.decode == Decode::All {
                        decoded_chunk.resize(spec.raw_len as usize, 0);
                        super::frame::decode_chunk_into(
                            spec,
                            &chunk,
                            self.codec.compression,
                            #[cfg(zstd_any)]
                            self.codec.dictionary.as_deref(),
                            &mut decoded_chunk,
                            &mut 0,
                        )
                    } else {
                        super::frame::check_chunk(spec, &chunk)
                    };
                    if let Err(e) = checked {
                        return failed(self, e);
                    }
                    stored.push(chunk);
                    from = to;
                }
                self.next_frame = chunks.end;
                // `key` read in range, so the view is too.
                let key = decoded.entries.slice(entry.key.clone());
                Some(Ok(Some(ScanEntry {
                    key,
                    seqno: entry.seqno,
                    value: None,
                    frame,
                    index: 0,
                    offset,
                    uncompressed_len: entry.value_len,
                    frame_end: end,
                    resynced: self.resync_tainted,
                    frames: Some(stored),
                    block: None,
                })))
            }
            // A chunk without its head is damage: the head before it was lost.
            FrameKind::Chunk => Some(Err(crate::Error::InvalidHeader("BlobFrame"))),
        }
    }
}

/// The frame index of a file whose data section spans `data`, read from the
/// `(position, length)` its table of contents gives it, or `None` when the
/// index is damaged: it does not read whole, decode or fit the data, and the
/// scan walks the frames' chain instead.
///
/// # Errors
///
/// An environmental read failure (a transient one, a permission or a
/// resource the host ran out of): the index may well be intact, and walking
/// the chain past damage elsewhere would give up the frames it can still
/// place, so the caller retries instead.
pub(crate) fn read_frame_index<F: Read + Seek>(
    file: &mut F,
    (pos, len): (u64, u64),
    data: core::ops::Range<u64>,
) -> crate::Result<Option<Arc<[u64]>>> {
    // The index lies within the file: a length past it is damage, refused
    // before a buffer is sized from it.
    let file_len = match file.seek(SeekFrom::End(0)) {
        Ok(file_len) => file_len,
        Err(e) => return environmental_or_none(e),
    };
    let Some(len) = pos
        .checked_add(len)
        .filter(|&end| end <= file_len)
        .and_then(|_| usize::try_from(len).ok())
    else {
        return Ok(None);
    };
    let mut bytes = alloc::vec![0_u8; len];
    let read = file
        .seek(SeekFrom::Start(pos))
        .and_then(|_| file.read_exact(&mut bytes));
    if let Err(e) = read {
        return environmental_or_none(e);
    }
    let Ok(frames) = super::writer::decode_frame_index(&bytes) else {
        return Ok(None);
    };
    let fits = frames.first().is_some_and(|&first| first >= data.start)
        && frames.last() == Some(&data.end);
    Ok(fits.then_some(frames))
}

/// A failure reading the frame index: returned when environmental, so the
/// caller retries, else taken as damage and answered with the chain.
fn environmental_or_none<T, E>(e: E) -> crate::Result<Option<T>>
where
    crate::Error: From<E>,
{
    let e = crate::Error::from(e);
    if e.is_environmental() {
        Err(e)
    } else {
        Ok(None)
    }
}

/// One value a scan reads.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ScanEntry {
    pub key: UserKey,
    pub seqno: SeqNo,
    /// The value, decoded: a view into its frame. `None` for a chunked value,
    /// whose frames, checked, are what every consumer copies: assembling it
    /// would copy a value of up to the cap for nothing; and for a single
    /// frame a copying scan checked as stored without decoding it.
    pub value: Option<UserValue>,
    /// The frame that holds the value (a chunked value's head).
    pub frame: u32,
    /// The value's place in its frame.
    pub index: u16,
    /// Where that frame starts.
    pub offset: u64,
    pub uncompressed_len: u32,
    /// Where the data this entry consumes ends: the start of the next frame
    /// once the value's frames are consumed whole, its frame's start for a
    /// value a block still shares with later ones. The tight-space relocation
    /// loop uses it as the exact punch / resume boundary.
    pub frame_end: u64,
    /// Whether the chain walk had resynchronized at or before this value's
    /// frame: its boundary is unproven, and so is every one after it. Salvage
    /// must NOT re-emit such a value as a genuine record.
    pub resynced: bool,
    /// The value's frames as stored, for a value that has frames of its own (a
    /// single frame, or a head and its chunks): what a copy under the same
    /// codec writes as it is. `None` for a value that shares a block.
    pub frames: Option<Vec<Slice>>,
    /// The block the value shares, for a value in one.
    pub block: Option<ScanBlock>,
}

/// The block a scanned value shares with its neighbours.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ScanBlock {
    /// The block's frame as stored, which a copy under the same codec writes
    /// as it is; every value of the block holds the same bytes.
    pub stored: Slice,
    /// How many values the block holds.
    pub values: u16,
    /// What this value adds to its file's block entry bytes, as its writer
    /// counted it (see [`DecodedFrame::block_entry_bytes`]).
    pub entry_bytes: u64,
}

#[cfg(test)]
impl ScanEntry {
    /// The value whole: decoded, or, for a chunked value of a file under
    /// `compression` (with no dictionary), gathered out of its frames.
    pub(crate) fn whole_value(
        &self,
        compression: crate::CompressionType,
    ) -> crate::Result<UserValue> {
        self.whole_value_with(
            compression,
            #[cfg(zstd_any)]
            None,
        )
    }

    /// [`Self::whole_value`] of a file whose codec decodes with `dictionary`.
    pub(crate) fn whole_value_with(
        &self,
        compression: crate::CompressionType,
        #[cfg(zstd_any)] dictionary: Option<&crate::compression::ZstdDictionary>,
    ) -> crate::Result<UserValue> {
        if let Some(value) = &self.value {
            return Ok(value.clone());
        }
        let (head, chunks) = self
            .frames
            .as_deref()
            .and_then(<[Slice]>::split_first)
            .ok_or(crate::Error::InvalidHeader("Blob"))?;
        let head = super::frame::decode_frame(
            head,
            compression,
            #[cfg(zstd_any)]
            dictionary,
            &mut 0,
        )?;
        let area: Vec<u8> = chunks
            .iter()
            .flat_map(|chunk| chunk.iter().copied())
            .collect();
        super::reader::assemble_stored(
            &head,
            &Slice::from(area),
            compression,
            #[cfg(zstd_any)]
            dictionary,
            &mut 0,
        )
    }
}

/// The values of a single frame or a block a scan read, yielded one by one.
struct FrameValues {
    frame: u32,
    offset: u64,
    frame_end: u64,
    resynced: bool,
    /// Whether values are yielded decoded: a block's always, a single
    /// frame's when the scan decodes every frame.
    decoded_value: bool,
    /// The frame as stored, and decoded.
    bytes: Slice,
    decoded: DecodedFrame,
    /// The next value to yield.
    at: usize,
}

impl FrameValues {
    /// The next value; `None` past the last one.
    fn next_value(&mut self) -> Option<crate::Result<ScanEntry>> {
        let at = self.at;
        let decoded = &self.decoded;
        let entry = decoded.list.get(at)?;
        self.at += 1;
        let in_block = decoded.header.kind == FrameKind::Block;
        // The index a handle names the value by: in a packed block its
        // record's offset.
        let (Some(_), Some(index), Some(entry_bytes)) = (
            decoded.key(at),
            decoded.index_of(at),
            decoded.block_entry_bytes(at),
        ) else {
            return Some(Err(crate::Error::InvalidHeader("BlobFrame")));
        };
        let value = if self.decoded_value {
            let Some(value) = decoded.value(at) else {
                return Some(Err(crate::Error::InvalidHeader("BlobFrame")));
            };
            Some(value)
        } else {
            None
        };
        Some(Ok(ScanEntry {
            // A view into the frame's entries, as the value is into its
            // payload: `key` read in range, so the view is too.
            key: decoded.entries.slice(entry.key.clone()),
            seqno: entry.seqno,
            value,
            frame: self.frame,
            index,
            offset: self.offset,
            uncompressed_len: entry.value_len,
            // A block is consumed once its last value is: until then the
            // frontier stays at its start.
            frame_end: if at + 1 == decoded.list.len() {
                self.frame_end
            } else {
                self.offset
            },
            resynced: self.resynced,
            frames: (!in_block).then(|| alloc::vec![self.bytes.clone()]),
            block: in_block.then(|| ScanBlock {
                stored: self.bytes.clone(),
                values: decoded.header.count,
                entry_bytes,
            }),
        }))
    }
}

impl Iterator for Scanner {
    type Item = crate::Result<ScanEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(values) = &mut self.values {
                match values.next_value() {
                    Some(Ok(entry)) => return Some(Ok(entry)),
                    // A value that does not decode ends its frame's values.
                    Some(Err(e)) => {
                        self.values = None;
                        return Some(Err(e));
                    }
                    None => self.values = None,
                }
            }
            if self.is_terminated {
                return None;
            }
            match self.fill()? {
                Ok(Some(entry)) => return Some(Ok(entry)),
                Ok(None) => {}
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

#[cfg(test)]
#[expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    reason = "test code: fixture files are small enough for every cast"
)]
mod tests;

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::frame::{
    DecodedFrame, FRAME_HEADER_LEN, FRAME_MAGIC, FrameKind, decode_frame, parse_header,
};
use super::reader::assemble;
use crate::fs::{Fs, FsOpenOptions};
use crate::io::BufReader;
#[cfg(not(feature = "std"))]
use crate::io::{Read, Seek, SeekFrom};
use crate::path::Path;
use crate::{CompressionType, SeqNo, Slice, UserKey, UserValue, vlog::BlobFileId};
use alloc::collections::VecDeque;
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

    /// Values of the block read last that are still to be yielded.
    pending: VecDeque<ScanEntry>,

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
    /// or from its first live frame, `pace` told how long each read takes.
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
        Self::open_with(
            &blob_file.0.path,
            &*blob_file.0.fs,
            blob_file.id(),
            ScanCodec::of(blob_file),
            Some(Arc::clone(&blob_file.0.frames)),
            (start != 0).then_some(start),
            pace,
        )
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
        let frames = match frames {
            Some(frames) => Some(frames),
            None => index_at.and_then(|(pos, len)| {
                let len = usize::try_from(len).ok()?;
                let mut bytes = alloc::vec![0_u8; len];
                file.seek(SeekFrom::Start(pos)).ok()?;
                file.read_exact(&mut bytes).ok()?;
                let frames = super::writer::decode_frame_index(&bytes).ok()?;
                let fits = frames.first().is_some_and(|&first| first >= data_start)
                    && frames.last() == Some(&data_end);
                fits.then_some(frames)
            }),
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
            pending: VecDeque::new(),
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
        // bounded by the section either way.
        let len = match self.indexed_end(frame) {
            Some(end) if end > offset && end <= self.data_end => end - offset,
            Some(_) => return damaged(self, crate::Error::InvalidHeader("BlobFrame")),
            None => {
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
            }
        };
        let Ok(len) = usize::try_from(len) else {
            return damaged(self, crate::Error::InvalidHeader("BlobFrame"));
        };
        let mut bytes = alloc::vec![0_u8; len];
        if let Err(e) = self.inner.read_exact(&mut bytes) {
            self.is_terminated = true;
            return Some(Err(e.into()));
        }
        let bytes = Slice::from(bytes);
        let mut decoded = 0;
        let parsed = decode_frame(
            &bytes,
            self.codec.compression,
            #[cfg(zstd_any)]
            self.codec.dictionary.as_deref(),
            &mut decoded,
        );
        match parsed {
            Ok(decoded) => {
                self.next_frame = frame + 1;
                Some(Ok((frame, offset, bytes, decoded)))
            }
            Err(e) => damaged(self, e),
        }
    }

    /// The values of the next frame, queued; `None` at the data end.
    fn fill(&mut self) -> Option<crate::Result<()>> {
        let (frame, offset, bytes, decoded) = match self.read_one()? {
            Ok(read) => read,
            Err(e) => return Some(Err(e)),
        };
        let resynced = self.resync_tainted;
        let frame_end = offset + bytes.len() as u64;
        match decoded.header.kind {
            FrameKind::Single | FrameKind::Block => {
                let last = decoded.list.len() - 1;
                let block = (decoded.header.kind == FrameKind::Block).then(|| ScanBlock {
                    stored: bytes.clone(),
                    values: decoded.header.count,
                });
                for (at, entry) in decoded.list.iter().enumerate() {
                    let (Some(key), Some(value)) = (decoded.key(at), decoded.value(at)) else {
                        return Some(Err(crate::Error::InvalidHeader("BlobFrame")));
                    };
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "a frame lists at most u16::MAX values"
                    )]
                    let index = at as u16;
                    let whole = decoded.header.kind == FrameKind::Single;
                    self.pending.push_back(ScanEntry {
                        key: UserKey::from(key),
                        seqno: entry.seqno,
                        // A view into the frame: a block's values are taken
                        // together, as the scan yields them in a row.
                        value,
                        frame,
                        index,
                        offset,
                        uncompressed_len: entry.value_len,
                        // A block is consumed once its last value is: until
                        // then the frontier stays at its start.
                        frame_end: if at == last { frame_end } else { offset },
                        resynced,
                        frames: whole.then(|| alloc::vec![bytes.clone()]),
                        block: block.clone(),
                    });
                }
                Some(Ok(()))
            }
            FrameKind::Head => {
                let Ok(chunks) = super::reader::chunk_frames(&decoded, frame) else {
                    return Some(Err(crate::Error::InvalidHeader("BlobFrame")));
                };
                let mut stored = alloc::vec![bytes];
                let mut parts = Vec::with_capacity((chunks.end - chunks.start) as usize);
                let mut end = frame_end;
                for _ in chunks.clone() {
                    let failure = match self.read_one() {
                        Some(Ok((_, at, chunk_bytes, chunk)))
                            if chunk.header.kind == FrameKind::Chunk =>
                        {
                            end = at + chunk_bytes.len() as u64;
                            stored.push(chunk_bytes);
                            parts.push(alloc::boxed::Box::new(chunk));
                            continue;
                        }
                        Some(Ok(_)) | None => crate::Error::InvalidHeader("BlobFrame"),
                        Some(Err(e)) => e,
                    };
                    // By the index, the value's damage ends with its chunks:
                    // the walk goes on at the frame after them, so the chunks
                    // left do not each read as a chunk without its head.
                    if let Err(e) = self.skip_to_frame(chunks.end) {
                        self.is_terminated = true;
                        return Some(Err(e));
                    }
                    return Some(Err(failure));
                }
                let (Some(entry), Some(key)) = (decoded.list.first(), decoded.key(0)) else {
                    return Some(Err(crate::Error::InvalidHeader("BlobFrame")));
                };
                let value = match assemble(&decoded, &parts, None) {
                    Ok(value) => value,
                    Err(e) => return Some(Err(e)),
                };
                self.pending.push_back(ScanEntry {
                    key: UserKey::from(key),
                    seqno: entry.seqno,
                    value,
                    frame,
                    index: 0,
                    offset,
                    uncompressed_len: entry.value_len,
                    frame_end: end,
                    resynced: self.resync_tainted,
                    frames: Some(stored),
                    block: None,
                });
                Some(Ok(()))
            }
            // A chunk without its head is damage: the head before it was lost.
            FrameKind::Chunk => Some(Err(crate::Error::InvalidHeader("BlobFrame"))),
        }
    }
}

/// One value a scan reads.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ScanEntry {
    pub key: UserKey,
    pub seqno: SeqNo,
    /// The value, decoded.
    pub value: UserValue,
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
}

impl Iterator for Scanner {
    type Item = crate::Result<ScanEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(entry) = self.pending.pop_front() {
                return Some(Ok(entry));
            }
            if self.is_terminated {
                return None;
            }
            match self.fill()? {
                Ok(()) => {}
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

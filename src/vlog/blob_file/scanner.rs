// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

// Format constants live in writer (the format definition site).
// Extracting to a shared module is an upstream structural decision.
use super::writer::{BLOB_HEADER_MAGIC, validate_header_crc};

/// Safety cap on blob value size (256 MiB), matching the writer and the
/// ordinary reader. Intentionally duplicated (see the writer's copy):
/// blocks and blobs are independent formats that may diverge. The scanner
/// enforces it BEFORE allocating: a CRC-valid fake header inside a damaged
/// record's user-controlled bytes can declare a near-`u32::MAX` length,
/// and in a sufficiently large source the declared frame still fits the
/// data section — without the cap the salvage walk would attempt a
/// multi-gigabyte allocation before the candidate's checksum rejection.
const MAX_DECOMPRESSION_SIZE: usize = 256 * 1024 * 1024;
use crate::fs::{Fs, FsFile, FsOpenOptions};
use crate::io::BufReader;
use crate::io::{LittleEndian, ReadBytesExt};
#[cfg(not(feature = "std"))]
use crate::io::{Read, Seek, SeekFrom};
use crate::path::Path;
use crate::{Checksum, SeqNo, UserKey, UserValue, vlog::BlobFileId};
#[cfg(not(feature = "std"))]
use alloc::boxed::Box;
#[cfg(feature = "std")]
use std::io::{Read, Seek, SeekFrom};

/// Reads through a blob file in order.
///
/// Termination is determined by the SFA table-of-contents: the scanner
/// stops when the read position reaches the end of the "data" section,
/// not when it encounters specific magic bytes. This avoids silent data
/// loss if corrupted frame bytes happen to match the metadata header
/// magic (`META`).
pub struct Scanner {
    pub(crate) blob_file_id: BlobFileId, // TODO: remove unused?
    inner: BufReader<Box<dyn FsFile>>,
    is_terminated: bool,

    /// Byte offset where the "data" section ends (from the SFA TOC).
    data_end: u64,

    /// Whether the CURRENT read position came from a forward magic
    /// resynchronization rather than a writer-chained frame end. A resync
    /// candidate's magic may sit inside a damaged record's user-controlled
    /// value bytes, so until the candidate fully validates its declared
    /// lengths are untrusted: a rejection (bounds or payload checksum)
    /// resynchronizes again strictly past the candidate instead of trusting
    /// its declared end or terminating. Cleared once a frame validates.
    resynced: bool,
}

impl Scanner {
    /// Initializes a new blob file reader.
    ///
    /// Reads the SFA table-of-contents to determine the "data" section
    /// boundary, then positions the reader at the start of the data
    /// section.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs or the blob file lacks
    /// a "data" section.
    pub fn new<P: AsRef<Path>>(
        path: P,
        fs: &dyn Fs,
        blob_file_id: BlobFileId,
    ) -> crate::Result<Self> {
        Self::open(path, fs, blob_file_id, None)
    }

    /// Re-opens a blob file mid-stream, positioning the reader at `start_offset`
    /// (an absolute data-section frame boundary captured from a previous scan's
    /// [`ScanEntry::frame_end`]). Used by the tight-space blob relocation loop so
    /// each slice resumes the stale-file scan where the prior slice stopped,
    /// instead of re-reading a prefix that has already been hole-punched.
    ///
    /// # Errors
    ///
    /// Returns `Err` if an IO error occurs, the blob file lacks a "data" section,
    /// or `start_offset` falls outside the data section.
    #[cfg(feature = "std")]
    pub fn resume<P: AsRef<Path>>(
        path: P,
        fs: &dyn Fs,
        blob_file_id: BlobFileId,
        start_offset: u64,
    ) -> crate::Result<Self> {
        Self::open(path, fs, blob_file_id, Some(start_offset))
    }

    /// Reads the SFA TOC to bound the "data" section, then positions the reader
    /// at `start` if given (validated to lie within `[data_start, data_end]`) or
    /// at the data-section start otherwise.
    fn open<P: AsRef<Path>>(
        path: P,
        fs: &dyn Fs,
        blob_file_id: BlobFileId,
        start: Option<u64>,
    ) -> crate::Result<Self> {
        let path = path.as_ref();

        let mut file = fs.open(path, &FsOpenOptions::new().read(true))?;
        let sfa_reader = crate::sfa::Reader::from_reader(&mut file)?;
        let data_section = sfa_reader.toc().section(b"data").ok_or_else(|| {
            log::error!("BlobFile: SFA TOC has no \"data\" section");
            crate::Error::InvalidHeader("BlobFile")
        })?;
        let data_start = data_section.pos();
        let data_end = data_start.checked_add(data_section.len()).ok_or_else(|| {
            log::error!(
                "BlobFile: data section offset overflow (pos={data_start}, len={})",
                data_section.len()
            );
            crate::Error::InvalidHeader("BlobFile")
        })?;

        let seek_to = match start {
            None => data_start,
            Some(off) if off >= data_start && off <= data_end => off,
            Some(off) => {
                log::error!(
                    "BlobFile: resume offset {off} outside data section [{data_start}, {data_end}]"
                );
                return Err(crate::Error::InvalidHeader("BlobFile"));
            }
        };

        file.seek(SeekFrom::Start(seek_to))?;
        let file_reader = BufReader::with_capacity(32_000, file);

        Ok(Self {
            blob_file_id,
            inner: file_reader,
            is_terminated: false,
            data_end,
            // The opening position is writer-chained (data start or a carried
            // frame boundary), never a magic-scan candidate.
            resynced: false,
        })
    }
    // No `with_reader` constructor: Scanner is crate-private (parent
    // `vlog` module is not re-exported from lib.rs), so there are no
    // external callers. All internal usage goes through `new()` / `resume()`.
}

impl Scanner {
    /// Repositions the reader at the next frame magic strictly AFTER
    /// `frame_offset`, or at the data-section end when none remains. Used
    /// after HEADER rot (bad magic, header-CRC mismatch), where the frame's
    /// lengths cannot be trusted to locate the next frame — without the
    /// forward magic scan one rotted header would cost every readable later
    /// frame. A false match inside a value payload fails its own header CRC
    /// or payload checksum and resynchronizes again, strictly forward.
    fn resync_to_next_frame(&mut self, frame_offset: u64) -> crate::Result<()> {
        const MAGIC_LEN: usize = BLOB_HEADER_MAGIC.len();

        // Scan in chunks, overlapping by MAGIC_LEN - 1 bytes so a magic
        // straddling two chunks is still found.
        let mut buf = alloc::vec![0u8; 64 * 1024];
        let mut pos = frame_offset + 1;
        while pos < self.data_end {
            self.inner.seek(SeekFrom::Start(pos))?;
            #[expect(
                clippy::cast_possible_truncation,
                reason = "min() bounds the window by the buffer length, which fits usize"
            )]
            let want = (self.data_end - pos).min(buf.len() as u64) as usize;
            let Some(window) = buf.get_mut(..want) else {
                break;
            };
            self.inner.read_exact(window)?;
            if let Some(hit) = window
                .windows(MAGIC_LEN)
                .position(|w| w == BLOB_HEADER_MAGIC)
            {
                self.inner.seek(SeekFrom::Start(pos + hit as u64))?;
                // The next frame is a magic-scan CANDIDATE: its lengths stay
                // untrusted until the frame fully validates.
                self.resynced = true;
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
}

#[derive(Debug, PartialEq, Eq)]
pub struct ScanEntry {
    pub key: UserKey,
    pub seqno: SeqNo,
    pub value: UserValue,
    pub offset: u64,
    pub uncompressed_len: u32,
    /// Absolute data-section position immediately AFTER this frame (the start of
    /// the next frame, or the data-section end for the last frame). The
    /// tight-space relocation loop uses it as the exact punch / resume boundary:
    /// once an entry is consumed, `[data_start, frame_end)` is reclaimable and a
    /// resumed scan opens here.
    pub frame_end: u64,
}

impl Iterator for Scanner {
    type Item = crate::Result<ScanEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_terminated {
            return None;
        }

        let offset = fail_iter!(self.inner.stream_position());

        // Terminate when the read position reaches the end of the "data"
        // section (from the SFA TOC), not when magic bytes match META.
        if offset >= self.data_end {
            self.is_terminated = true;
            return None;
        }

        {
            let mut buf = [0; BLOB_HEADER_MAGIC.len()];
            fail_iter!(self.inner.read_exact(&mut buf));

            if buf != BLOB_HEADER_MAGIC {
                // Header rot: the frame's lengths are unreadable, so the
                // next frame cannot be located from this one. Resynchronize
                // at the next frame magic so one rotted header does not
                // cost every readable later frame (record-granular
                // salvage); the frame itself is lost either way.
                if let Err(e) = self.resync_to_next_frame(offset) {
                    self.is_terminated = true;
                    return Some(Err(e));
                }
                return Some(Err(crate::Error::InvalidHeader("Blob")));
            }
        }

        let expected_checksum = fail_iter!(self.inner.read_u128::<LittleEndian>());
        let seqno = fail_iter!(self.inner.read_u64::<LittleEndian>());

        let key_len = fail_iter!(self.inner.read_u16::<LittleEndian>());

        let real_val_len = fail_iter!(self.inner.read_u32::<LittleEndian>());

        let on_disk_val_len = fail_iter!(self.inner.read_u32::<LittleEndian>());

        // Read and validate the header CRC. On a mismatch the consumed
        // lengths are untrusted (any of them may be the rotted field), so
        // resynchronize at the next frame magic instead of terminating —
        // continuing from a length-derived position could desynchronize,
        // and stopping would drop every readable later frame.
        let stored_header_crc = {
            let crc = fail_iter!(self.inner.read_u32::<LittleEndian>());
            if let Err(e) = validate_header_crc(seqno, key_len, real_val_len, on_disk_val_len, crc)
            {
                if let Err(e2) = self.resync_to_next_frame(offset) {
                    self.is_terminated = true;
                    return Some(Err(e2));
                }
                return Some(Err(e));
            }
            crc
        };

        // Verify the declared frame payload fits within the data section
        // before allocating buffers. A declared payload that overruns the
        // data section or the 256 MiB cap makes the span UNTRUSTWORTHY, so
        // resynchronize at the next
        // real frame regardless of how this position was reached. A resync
        // candidate's magic may sit inside a damaged record's
        // user-controlled bytes; a writer-chained frame's header CRC vouches
        // for its lengths at parse time, but a re-stamped length that
        // recomputes the header CRC passes that check and can declare past
        // the section — terminating would then leave every intact later
        // frame uninspected. Resync parks at the section end when no further
        // magic exists, so a genuine truncation still terminates cleanly on
        // the next call.
        {
            let header_len = super::writer::BLOB_HEADER_LEN as u64;
            // `key_len` / `on_disk_val_len` come from the on-disk frame header and
            // may be corrupt. Use checked adds so a value that overflows u64 fails
            // loudly here (treated as "does not fit") instead of saturating to
            // u64::MAX and relying on the `> data_end` compare to reject it.
            let frame_end = offset
                .checked_add(header_len)
                .and_then(|x| x.checked_add(u64::from(key_len)))
                .and_then(|x| x.checked_add(u64::from(on_disk_val_len)));
            // Same 256 MiB value cap as the writer / ordinary reader,
            // checked BEFORE the buffers below are allocated: no
            // legitimate frame exceeds it, so an over-cap declared length
            // is corruption regardless of whether it fits the section.
            let over_cap = u64::from(real_val_len) > MAX_DECOMPRESSION_SIZE as u64
                || u64::from(on_disk_val_len) > MAX_DECOMPRESSION_SIZE as u64;
            if over_cap || frame_end.is_none_or(|end| end > self.data_end) {
                if let Err(e) = self.resync_to_next_frame(offset) {
                    self.is_terminated = true;
                    return Some(Err(e));
                }
                return Some(Err(crate::Error::InvalidHeader("Blob")));
            }
        }

        let key = fail_iter!(UserKey::from_reader(&mut self.inner, key_len as usize));

        let value = fail_iter!(UserValue::from_reader(
            &mut self.inner,
            on_disk_val_len as usize
        ));

        {
            let checksum = {
                let mut hasher = xxhash_rust::xxh3::Xxh3::default();
                hasher.update(&key);
                hasher.update(&value);
                hasher.update(&stored_header_crc.to_le_bytes());
                hasher.digest128()
            };

            if expected_checksum != checksum {
                log::error!(
                    "Checksum mismatch for blob>{}@{offset}, got={checksum}, expected={expected_checksum}",
                    self.blob_file_id,
                );

                // A checksum rejection means the DECLARED SPAN is not
                // trustworthy, so resume the magic search strictly past this
                // frame's start regardless of how the position was reached.
                // A resync candidate's magic came from user-controlled value
                // bytes (a CRC-valid fake header can declare an end past
                // intact records); a WRITER-CHAINED frame's header CRC
                // vouches for its lengths at parse time, but a re-stamped
                // length that recomputes the header CRC passes that check
                // and can consume one or more intact later frames before the
                // payload checksum fails — trusting its declared end would
                // then skip those frames without reporting the loss. Both
                // resynchronize at the next real frame instead.
                if let Err(e) = self.resync_to_next_frame(offset) {
                    self.is_terminated = true;
                    return Some(Err(e));
                }
                return Some(Err(crate::Error::ChecksumMismatch {
                    got: Checksum::from_raw(checksum),
                    expected: Checksum::from_raw(expected_checksum),
                }));
            }
        }

        // The frame fully validated: later positions are writer-chained again.
        self.resynced = false;

        // The reader is now positioned at the next frame: capture it as the exact
        // punch / resume boundary for this frame.
        let frame_end = fail_iter!(self.inner.stream_position());

        Some(Ok(ScanEntry {
            key,
            seqno,
            value,
            offset,
            uncompressed_len: real_val_len,
            frame_end,
        }))
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::indexing_slicing, reason = "test code")]
mod tests;

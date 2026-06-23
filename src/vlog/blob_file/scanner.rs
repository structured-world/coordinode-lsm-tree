// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

// Format constants live in writer (the format definition site).
// Extracting to a shared module is an upstream structural decision.
use super::writer::{BLOB_HEADER_MAGIC_V3, BLOB_HEADER_MAGIC_V4, validate_header_crc};
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
        })
    }
    // No `with_reader` constructor: Scanner is crate-private (parent
    // `vlog` module is not re-exported from lib.rs), so there are no
    // external callers. All internal usage goes through `new()` / `resume()`.
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

        let frame_is_v4;

        {
            let mut buf = [0; BLOB_HEADER_MAGIC_V4.len()];
            fail_iter!(self.inner.read_exact(&mut buf));

            frame_is_v4 = buf == BLOB_HEADER_MAGIC_V4;
            if !frame_is_v4 && buf != BLOB_HEADER_MAGIC_V3 {
                self.is_terminated = true;
                return Some(Err(crate::Error::InvalidHeader("Blob")));
            }
        }

        let expected_checksum = fail_iter!(self.inner.read_u128::<LittleEndian>());
        let seqno = fail_iter!(self.inner.read_u64::<LittleEndian>());

        let key_len = fail_iter!(self.inner.read_u16::<LittleEndian>());

        let real_val_len = fail_iter!(self.inner.read_u32::<LittleEndian>());

        let on_disk_val_len = fail_iter!(self.inner.read_u32::<LittleEndian>());

        // V4: read and validate header CRC using shared validator.
        // On CRC failure, terminate the scanner so subsequent next() calls
        // don't read from a mid-frame stream position.
        let stored_header_crc = if frame_is_v4 {
            let crc = fail_iter!(self.inner.read_u32::<LittleEndian>());
            if let Err(e) = validate_header_crc(seqno, key_len, real_val_len, on_disk_val_len, crc)
            {
                self.is_terminated = true;
                return Some(Err(e));
            }
            Some(crc)
        } else {
            None
        };

        // Verify the declared frame payload fits within the data section
        // before allocating buffers. Without this, a corrupted key_len or
        // on_disk_val_len could cause a huge allocation or read past
        // data_end into the TOC/trailer region.
        {
            let header_len = if frame_is_v4 {
                super::writer::BLOB_HEADER_LEN_V4 as u64
            } else {
                super::writer::BLOB_HEADER_LEN_V3 as u64
            };
            // `key_len` / `on_disk_val_len` come from the on-disk frame header and
            // may be corrupt. Use checked adds so a value that overflows u64 fails
            // loudly here (treated as "does not fit") instead of saturating to
            // u64::MAX and relying on the `> data_end` compare to reject it.
            let frame_end = offset
                .checked_add(header_len)
                .and_then(|x| x.checked_add(u64::from(key_len)))
                .and_then(|x| x.checked_add(u64::from(on_disk_val_len)));
            if frame_end.is_none_or(|end| end > self.data_end) {
                self.is_terminated = true;
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
                if let Some(hcrc) = stored_header_crc {
                    hasher.update(&hcrc.to_le_bytes());
                }
                hasher.digest128()
            };

            if expected_checksum != checksum {
                log::error!(
                    "Checksum mismatch for blob>{}@{offset}, got={checksum}, expected={expected_checksum}",
                    self.blob_file_id,
                );

                return Some(Err(crate::Error::ChecksumMismatch {
                    got: Checksum::from_raw(checksum),
                    expected: Checksum::from_raw(expected_checksum),
                }));
            }
        }

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

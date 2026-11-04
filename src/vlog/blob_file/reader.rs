// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::frame::{DecodedFrame, FrameKind, decode_frame};
use crate::{BlobFile, Slice, UserValue, fs::FsFile, vlog::ValueHandle};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::ops::Range;

/// Reads frames, and the values in them, out of one blob file.
pub(crate) struct Reader<'a> {
    blob_file: &'a BlobFile,
    file: &'a dyn FsFile,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(blob_file: &'a BlobFile, file: &'a dyn FsFile) -> Self {
        Self { blob_file, file }
    }

    /// Where frame `frame` lies: its first byte and its length.
    ///
    /// # Errors
    ///
    /// [`crate::Error::InvalidHeader`] for a frame the file does not have, or
    /// one longer than any frame can be.
    pub(crate) fn frame_span(blob_file: &BlobFile, frame: u32) -> crate::Result<(u64, usize)> {
        let (start, end) = blob_file
            .frame_span(frame)
            .ok_or(crate::Error::InvalidHeader("Blob"))?;
        let len = usize::try_from(end - start).map_err(|_| crate::Error::InvalidHeader("Blob"))?;
        Ok((start, len))
    }

    /// The bytes of frame `frame`, as stored and unchecked: a caller checks
    /// them with [`Self::decode`].
    ///
    /// # Errors
    ///
    /// The frame is not in the file, or the read fails.
    pub(crate) fn read_frame_bytes(&self, frame: u32) -> crate::Result<Slice> {
        let (start, len) = Self::frame_span(self.blob_file, frame)?;
        Ok(crate::file::read_exact(self.file, start, len)?)
    }

    /// Checks and decodes `bytes`, one frame of this file, under its codec.
    ///
    /// `decoded` receives the length the decompressor produced, as soon as it
    /// produced it: a frame refused afterwards was still decompressed.
    ///
    /// # Errors
    ///
    /// The frame does not check or does not decode.
    pub(crate) fn decode(
        blob_file: &BlobFile,
        bytes: &Slice,
        decoded: &mut usize,
    ) -> crate::Result<DecodedFrame> {
        decode_frame(
            bytes,
            blob_file.0.meta.compression,
            #[cfg(zstd_any)]
            blob_file.0.zstd_dictionary.as_deref(),
            decoded,
        )
    }

    /// Reads, checks and decodes frame `frame`.
    ///
    /// # Errors
    ///
    /// As [`Self::read_frame_bytes`] and [`Self::decode`].
    #[cfg(test)]
    pub(crate) fn read_frame(
        &self,
        frame: u32,
        decoded: &mut usize,
    ) -> crate::Result<DecodedFrame> {
        let bytes = self.read_frame_bytes(frame)?;
        Self::decode(self.blob_file, &bytes, decoded)
    }

    /// Reads the value `vhandle` names under `key`, whole: out of its frame,
    /// or out of its head's chunks for a chunked value.
    ///
    /// # Errors
    ///
    /// The frames do not check, or the entry `vhandle` names is not `key`'s.
    #[cfg(test)]
    pub(crate) fn get(&self, key: &[u8], vhandle: &ValueHandle) -> crate::Result<UserValue> {
        let mut decoded = 0;
        let frame = self.read_frame(vhandle.frame, &mut decoded)?;
        match frame.header.kind {
            FrameKind::Head => {
                check_key(&frame, vhandle.index, key)?;
                let mut chunks = Vec::new();
                for chunk in chunk_frames(&frame, vhandle.frame)? {
                    chunks.push(alloc::boxed::Box::new(
                        self.read_frame(chunk, &mut decoded)?,
                    ));
                }
                assemble(&frame, &chunks, None)
            }
            _ => value_in(&frame, vhandle.index, key),
        }
    }

    /// What a copy of the value `vhandle` names under `key` into another file
    /// writes, each frame read checked, and the bytes read for it.
    ///
    /// # Errors
    ///
    /// The frames do not check, or the entry is not `key`'s.
    pub(crate) fn copy_source(
        &self,
        key: &[u8],
        vhandle: &ValueHandle,
    ) -> crate::Result<(CopySource, u64)> {
        let mut decoded = 0;
        let first = self.read_frame_bytes(vhandle.frame)?;
        let mut read = first.len() as u64;
        let frame = Self::decode(self.blob_file, &first, &mut decoded)?;
        check_key(&frame, vhandle.index, key)?;
        let value_len = frame.list.first().map_or(0, |e| e.value_len);
        let source = match frame.header.kind {
            FrameKind::Block => CopySource::Value(value_in(&frame, vhandle.index, key)?),
            FrameKind::Single => CopySource::Frames {
                frames: alloc::vec![first],
                value_len,
            },
            FrameKind::Head => {
                let mut frames = alloc::vec![first];
                for chunk in chunk_frames(&frame, vhandle.frame)? {
                    let bytes = self.read_frame_bytes(chunk)?;
                    read += bytes.len() as u64;
                    let decoded_chunk = Self::decode(self.blob_file, &bytes, &mut decoded)?;
                    if decoded_chunk.header.kind != FrameKind::Chunk {
                        return Err(crate::Error::InvalidHeader("Blob"));
                    }
                    frames.push(bytes);
                }
                CopySource::Frames { frames, value_len }
            }
            FrameKind::Chunk => return Err(crate::Error::InvalidHeader("Blob")),
        };
        Ok((source, read))
    }
}

/// What a copy of one value into another file writes.
pub(crate) enum CopySource {
    /// The frames of a value that has frames of its own, as stored: written
    /// as they are into a file under the same codec. `value_len` is the
    /// value's decoded length.
    Frames { frames: Vec<Slice>, value_len: u32 },
    /// A value that shares a block, out of it: written as a value.
    Value(UserValue),
}

/// Checks that entry `index` of `frame` is `key`'s.
///
/// # Errors
///
/// [`crate::Error::InvalidHeader`] when it is not, or the frame lists no such
/// entry: a handle that names another value is damage, never that value.
pub(crate) fn check_key(frame: &DecodedFrame, index: u16, key: &[u8]) -> crate::Result<()> {
    if frame.key(usize::from(index)) == Some(key) {
        Ok(())
    } else {
        Err(crate::Error::InvalidHeader("Blob"))
    }
}

/// The value at `index` of `frame`, a single or block frame, under `key`.
///
/// Handed on as a view into the frame, with no copy: a value out of a block
/// keeps that block alive while it is held, as a value read out of a table's
/// data block keeps the data block.
///
/// # Errors
///
/// The entry is not `key`'s, or the frame is not a single or block frame.
pub(crate) fn value_in(frame: &DecodedFrame, index: u16, key: &[u8]) -> crate::Result<UserValue> {
    check_key(frame, index, key)?;
    match frame.header.kind {
        FrameKind::Single | FrameKind::Block => frame
            .value(usize::from(index))
            .ok_or(crate::Error::InvalidHeader("Blob")),
        FrameKind::Head | FrameKind::Chunk => Err(crate::Error::InvalidHeader("Blob")),
    }
}

/// The frame numbers of a head's chunks: the frames right after it.
///
/// # Errors
///
/// The head's chunk table is malformed, or the chunks would run past frame
/// number `u32::MAX`.
pub(crate) fn chunk_frames(head: &DecodedFrame, frame: u32) -> crate::Result<Range<u32>> {
    let (_, chunks) = head.chunk_table()?;
    let first = frame
        .checked_add(1)
        .ok_or(crate::Error::InvalidHeader("Blob"))?;
    let end = first
        .checked_add(chunks)
        .ok_or(crate::Error::InvalidHeader("Blob"))?;
    Ok(first..end)
}

/// The chunks a byte range of a chunked value covers, by their place among
/// the head's chunks, and where the range starts in the first of them.
///
/// # Errors
///
/// [`crate::Error::InvalidHeader`] for a malformed head; an empty or
/// out-of-bounds range is the caller's to refuse.
pub(crate) fn covering_chunks(
    head: &DecodedFrame,
    range: &Range<u64>,
) -> crate::Result<(Range<u32>, u64)> {
    let (size, _) = head.chunk_table()?;
    let size = u64::from(size);
    #[expect(
        clippy::cast_possible_truncation,
        reason = "a chunk number fits u32: the head's table holds u32 chunks"
    )]
    let chunks = (range.start / size) as u32..range.end.div_ceil(size) as u32;
    Ok((chunks, range.start % size))
}

/// Assembles a chunked value out of its head and `chunks`, the chunks of
/// `range` (all of them when `None`), each decoded.
///
/// # Errors
///
/// A chunk is not a chunk, or is not the size the head's table gives it.
pub(crate) fn assemble<C: core::ops::Deref<Target = DecodedFrame>>(
    head: &DecodedFrame,
    chunks: &[C],
    range: Option<&Range<u64>>,
) -> crate::Result<UserValue> {
    let malformed = || crate::Error::InvalidHeader("Blob");
    let (size, count) = head.chunk_table()?;
    let total = head.list.first().map_or(0, |e| u64::from(e.value_len));
    let range = range.cloned().unwrap_or(0..total);
    let (covered, skip) = covering_chunks(head, &range)?;
    if chunks.len() != (covered.end - covered.start) as usize || covered.end > count {
        return Err(malformed());
    }
    let len = usize::try_from(range.end - range.start).map_err(|_| malformed())?;
    let mut out = Vec::with_capacity(len);
    for (at, chunk) in covered.clone().zip(chunks) {
        let expected = if at + 1 == count {
            total - u64::from(at) * u64::from(size)
        } else {
            u64::from(size)
        };
        if chunk.header.kind != FrameKind::Chunk || chunk.payload.len() as u64 != expected {
            return Err(malformed());
        }
        let from = if at == covered.start {
            usize::try_from(skip).map_err(|_| malformed())?
        } else {
            0
        };
        let wanted = len - out.len();
        let bytes = chunk.payload.get(from..).ok_or_else(malformed)?;
        out.extend_from_slice(bytes.get(..wanted.min(bytes.len())).ok_or_else(malformed)?);
    }
    if out.len() != len {
        return Err(malformed());
    }
    Ok(UserValue::from(out))
}

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test code"
)]
mod tests;

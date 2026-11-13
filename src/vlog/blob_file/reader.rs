// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::frame::{DecodedFrame, FRAME_MAGIC, FrameKind, decode_frame, undecoded_frame};
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

    /// Checks and decodes `stored`, chunk `at` of the value `head` names in
    /// this file, under its codec.
    ///
    /// # Errors
    ///
    /// The head lists no such chunk, or the chunk does not check or decode.
    pub(crate) fn decode_chunk(
        blob_file: &BlobFile,
        head: &DecodedFrame,
        at: u32,
        stored: &Slice,
        decoded: &mut usize,
    ) -> crate::Result<DecodedFrame> {
        super::frame::decode_chunk(
            &head.chunk(at)?,
            stored,
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
                let all = chunk_frames(&frame, vhandle.frame, self.blob_file.frame_count())?;
                for (at, chunk) in (0_u32..).zip(all) {
                    let stored = self.read_frame_bytes(chunk)?;
                    chunks.push(alloc::boxed::Box::new(Self::decode_chunk(
                        self.blob_file,
                        &frame,
                        at,
                        &stored,
                        &mut decoded,
                    )?));
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
        let compression = self.blob_file.0.meta.compression;
        let first = self.read_frame_bytes(vhandle.frame)?;
        let mut read = first.len() as u64;
        // A single frame is copied as it lies, so it is checked as stored and
        // not decoded; a block is decoded for the value, and a head's payload
        // is its chunk table, stored as it is. The kind byte follows the
        // magic; one naming no single frame is decoded and refused there.
        let single = matches!(
            first
                .get(FRAME_MAGIC.len())
                .copied()
                .map(FrameKind::try_from),
            Some(Ok(FrameKind::Single))
        );
        let frame = if single {
            undecoded_frame(&first, compression)?
        } else {
            Self::decode(self.blob_file, &first, &mut 0)?
        };
        check_key(&frame, vhandle.index, key)?;
        let value_len = frame.list.first().map_or(0, |e| e.value_len);
        let source = match frame.header.kind {
            // A chunk lists no entry, so the key check above already refused
            // one; `value_in` refuses it again for any other caller.
            FrameKind::Block | FrameKind::Chunk => {
                CopySource::Value(value_in(&frame, vhandle.index, key)?)
            }
            FrameKind::Single => CopySource::Frames {
                frames: alloc::vec![first],
                value_len,
            },
            FrameKind::Head => {
                let all = chunk_frames(&frame, vhandle.frame, self.blob_file.frame_count())?;
                let mut frames = Vec::with_capacity(all.len() + 1);
                frames.push(first);
                // As many rows as chunks: both are counted from the table.
                for (spec, chunk) in frame.chunks()?.zip(all) {
                    let bytes = self.read_frame_bytes(chunk)?;
                    read += bytes.len() as u64;
                    // Copied as stored: the row's length and checksum prove
                    // it, no decode.
                    let spec = spec?;
                    if !spec.fits(compression) {
                        return Err(crate::Error::InvalidHeader("Blob"));
                    }
                    super::frame::check_chunk(&spec, &bytes)?;
                    frames.push(bytes);
                }
                CopySource::Frames { frames, value_len }
            }
        };
        Ok((source, read))
    }
}

/// The value `frames` of `blob_file` hold, the frames of its own a copy source
/// holds (a single frame, or a head and its chunks), decoded: for a value
/// written anew rather than copied as stored.
///
/// # Errors
///
/// The frames are no value's own, or one does not check or decode.
pub(crate) fn decode_copied(blob_file: &BlobFile, frames: &[Slice]) -> crate::Result<UserValue> {
    let malformed = || crate::Error::InvalidHeader("Blob");
    let (first, chunks) = frames.split_first().ok_or_else(malformed)?;
    let mut decoded = 0;
    let frame = Reader::decode(blob_file, first, &mut decoded)?;
    match frame.header.kind {
        FrameKind::Single if chunks.is_empty() => frame.value(0).ok_or_else(malformed),
        FrameKind::Head => {
            let mut parts = Vec::with_capacity(chunks.len());
            for (at, stored) in (0_u32..).zip(chunks) {
                parts.push(Reader::decode_chunk(
                    blob_file,
                    &frame,
                    at,
                    stored,
                    &mut decoded,
                )?);
            }
            assemble(&frame, &parts, None)
        }
        _ => Err(malformed()),
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
    if frame.place_of(index).and_then(|at| frame.key(at)) == Some(key) {
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
    let malformed = || crate::Error::InvalidHeader("Blob");
    // The entry is placed once for its key and its value: in a packed block
    // placing it is a search.
    let at = frame.place_of(index).ok_or_else(malformed)?;
    if frame.key(at) != Some(key) {
        return Err(malformed());
    }
    match frame.header.kind {
        FrameKind::Single | FrameKind::Block => frame.value(at).ok_or_else(malformed),
        FrameKind::Head | FrameKind::Chunk => Err(malformed()),
    }
}

/// The frame numbers of a head's chunks, the frames right after it, in a file
/// of `frames` frames.
///
/// # Errors
///
/// The head's chunk table is malformed, or its chunks run past the file's
/// last frame: a damaged head that checks, which must not size the buffers
/// its callers reserve for the chunks.
pub(crate) fn chunk_frames(
    head: &DecodedFrame,
    frame: u32,
    frames: usize,
) -> crate::Result<Range<u32>> {
    let (_, chunks) = head.chunk_table()?;
    let first = frame
        .checked_add(1)
        .ok_or(crate::Error::InvalidHeader("Blob"))?;
    let end = first
        .checked_add(chunks)
        .filter(|&end| end as usize <= frames)
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

/// The whole value `head` names out of `area`, its chunks as stored back to
/// back, each checked and decoded under `compression`. A value whose chunks
/// are all stored as they are is handed out of `area` as it is, a view with
/// no copy; any other is decoded into one buffer, each chunk into its place.
///
/// `decoded` receives what the chunks decoded to, as [`Reader::decode`] does.
///
/// # Errors
///
/// `area` is not the chunks the head lists, or a chunk does not check or
/// decode.
pub(crate) fn assemble_stored(
    head: &DecodedFrame,
    area: &Slice,
    compression: crate::CompressionType,
    #[cfg(zstd_any)] dictionary: Option<&crate::compression::ZstdDictionary>,
    decoded: &mut usize,
) -> crate::Result<UserValue> {
    use super::frame::{check_chunk, decode_chunk_into};

    let malformed = || crate::Error::InvalidHeader("Blob");
    let total = head.list.first().map_or(0, |e| u64::from(e.value_len));
    let len = usize::try_from(total).map_err(|_| malformed())?;
    let specs = head.chunks()?.collect::<crate::Result<Vec<_>>>()?;
    let stored_len: u64 = specs.iter().map(|spec| u64::from(spec.stored_len)).sum();
    if stored_len != area.len() as u64 {
        return Err(malformed());
    }
    *decoded = 0;
    if specs.iter().all(|spec| spec.is_stored_as_is(compression)) {
        let mut from = 0;
        for spec in &specs {
            let to = from + spec.stored_len as usize;
            check_chunk(spec, area.get(from..to).ok_or_else(malformed)?)?;
            from = to;
        }
        *decoded = area.len();
        if area.len() != len {
            return Err(malformed());
        }
        return Ok(area.clone());
    }
    let mut value = crate::slice::UnfilledSlice::new(len);
    let mut target = value.target();
    let mut from = 0;
    for spec in &specs {
        let to = from + spec.stored_len as usize;
        let stored = area.get(from..to).ok_or_else(malformed)?;
        let mut part = target.portion(target.filled(), spec.raw_len as usize);
        if spec.is_stored_as_is(compression) {
            // Copied into the unwritten memory as it is: only a
            // decompression's target is zeroed first.
            check_chunk(spec, stored)?;
            if stored.len() != part.capacity() {
                return Err(malformed());
            }
            part.append(stored);
            *decoded += stored.len();
        } else {
            let mut produced = 0;
            let checked = decode_chunk_into(
                spec,
                stored,
                compression,
                #[cfg(zstd_any)]
                dictionary,
                part.init_mut(),
                &mut produced,
            );
            *decoded += produced;
            checked?;
            part.set_filled(spec.raw_len as usize)?;
        }
        target.extend_from_portion(spec.raw_len as usize);
        from = to;
    }
    if target.filled() != len {
        return Err(malformed());
    }
    // SAFETY: every byte of the value was filled above, chunk by chunk, as
    // `target` counted them.
    #[expect(unsafe_code, reason = "see safety")]
    Ok(unsafe { value.assume_filled() })
}

/// Assembles a chunked value out of its head and `chunks`, the chunks of
/// `range` (all of them when `None`), each decoded.
///
/// # Errors
///
/// A chunk is not a chunk, or is not the size the head's table gives it.
pub(crate) fn assemble<C: core::borrow::Borrow<DecodedFrame>>(
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
    // A range within one chunk is a view into that chunk, as a range of a
    // value with one frame is: no buffer, no copy.
    if let [chunk] = chunks {
        let chunk = chunk.borrow();
        let from = usize::try_from(skip).map_err(|_| malformed())?;
        let to = from.checked_add(len).ok_or_else(malformed)?;
        if !chunk_fits(chunk, covered.start, (size, count), total) || to > chunk.payload.len() {
            return Err(malformed());
        }
        return Ok(chunk.payload.slice(from..to));
    }
    // Gathered straight into the value's own allocation: a buffer of its own
    // would be copied into a slice once more.
    let mut out = crate::slice::UnfilledSlice::new(len);
    let mut target = out.target();
    for (at, chunk) in covered.clone().zip(chunks) {
        let chunk = chunk.borrow();
        if !chunk_fits(chunk, at, (size, count), total) {
            return Err(malformed());
        }
        let from = if at == covered.start {
            usize::try_from(skip).map_err(|_| malformed())?
        } else {
            0
        };
        let wanted = len - target.filled();
        let bytes = chunk.payload.get(from..).ok_or_else(malformed)?;
        // Within what is left: `append` takes no more than the buffer holds.
        target.append(bytes.get(..wanted.min(bytes.len())).ok_or_else(malformed)?);
    }
    if target.filled() != len {
        return Err(malformed());
    }
    // SAFETY: the target counted every one of the `len` bytes written.
    #[expect(unsafe_code, reason = "see safety")]
    Ok(unsafe { out.assume_filled() })
}

/// Whether `chunk` is chunk `at` of a value of `total` bytes split by the
/// table `(size, count)`: a chunk, holding the chunk size, the last what is
/// left.
fn chunk_fits(chunk: &DecodedFrame, at: u32, (size, count): (u32, u32), total: u64) -> bool {
    let expected = if at + 1 == count {
        total - u64::from(at) * u64::from(size)
    } else {
        u64::from(size)
    };
    // The declared length: what a decoded payload was checked to be, and the
    // only one an undecoded chunk has.
    chunk.header.kind == FrameKind::Chunk && u64::from(chunk.header.raw_len) == expected
}

#[cfg(test)]
#[expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    reason = "test code: fixture files are small enough for every cast"
)]
mod tests;

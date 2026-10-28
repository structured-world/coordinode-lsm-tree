// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use super::writer::TOC_MAGIC;
use crate::io::ReadBytesExt;
use crate::sfa::{
    Result,
    checksum::Checksum,
    toc::{Toc, entry::TocEntry},
    trailer::reader::ParsedTrailer,
};
// `Read`/`Seek`/`SeekFrom` resolve to std under `std` and to the native
// `crate::io` types under `no_std`; the `take`/`seek` calls and the `SeekFrom`
// value below track whichever is in play.
#[cfg(not(feature = "std"))]
use crate::io::{Read, Seek, SeekFrom};
#[cfg(feature = "std")]
use std::io::{Read, Seek, SeekFrom};

struct ChecksummedReader<R: crate::io::Read> {
    inner: R,
    hasher: xxhash_rust::xxh3::Xxh3Default,
}

impl<R: crate::io::Read> ChecksummedReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            inner: reader,
            hasher: xxhash_rust::xxh3::Xxh3Default::new(),
        }
    }

    pub fn checksum(&self) -> Checksum {
        Checksum::from_raw(self.hasher.digest128())
    }
}

// `crate::io::Read` is the std trait (via the std-mode supertrait blanket)
// under `std` and the native trait under `no_std`; gated impls differ only
// in the trait path and `Result` type.
#[cfg(feature = "std")]
impl<R: crate::io::Read> std::io::Read for ChecksummedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;

        #[expect(clippy::indexing_slicing)]
        self.hasher.update(&buf[..n]);

        Ok(n)
    }
}

#[cfg(not(feature = "std"))]
impl<R: crate::io::Read> crate::io::Read for ChecksummedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> crate::io::Result<usize> {
        let n = self.inner.read(buf)?;

        #[expect(clippy::indexing_slicing)]
        self.hasher.update(&buf[..n]);

        Ok(n)
    }
}

/// Bytes the TOC is read in at a time: a TOC of a few dozen sections fits one
/// read.
const TOC_CHUNK: usize = 4_096;

/// Reads its source `TOC_CHUNK` bytes at a time, so parsing the TOC field by
/// field costs one read per chunk rather than one per field, and no more
/// memory than a chunk whatever length the trailer claims.
struct ChunkedReader<R> {
    inner: R,
    chunk: [u8; TOC_CHUNK],
    pos: usize,
    filled: usize,
}

impl<R> ChunkedReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            chunk: [0; TOC_CHUNK],
            pos: 0,
            filled: 0,
        }
    }

    /// Copies what the current chunk still holds into `out`.
    #[expect(
        clippy::indexing_slicing,
        reason = "pos <= filled <= TOC_CHUNK, and n is at most either length"
    )]
    fn drain_into(&mut self, out: &mut [u8]) -> usize {
        let left = &self.chunk[self.pos..self.filled];
        let n = out.len().min(left.len());
        out[..n].copy_from_slice(&left[..n]);
        self.pos += n;
        n
    }
}

#[cfg(feature = "std")]
impl<R: crate::io::Read> std::io::Read for ChunkedReader<R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.pos == self.filled {
            self.filled = self.inner.read(&mut self.chunk)?;
            self.pos = 0;
        }
        Ok(self.drain_into(out))
    }
}

#[cfg(not(feature = "std"))]
impl<R: crate::io::Read> crate::io::Read for ChunkedReader<R> {
    fn read(&mut self, out: &mut [u8]) -> crate::io::Result<usize> {
        if self.pos == self.filled {
            self.filled = self.inner.read(&mut self.chunk)?;
            self.pos = 0;
        }
        Ok(self.drain_into(out))
    }
}

pub struct TocReader;

impl TocReader {
    pub fn from_reader<R: Read + Seek>(reader: &mut R, trailer: &ParsedTrailer) -> Result<Toc> {
        use crate::io::LE;

        log::trace!("Reading ToC");

        // The TOC lies between its position and the trailer.
        if trailer
            .toc_pos
            .checked_add(trailer.toc_len)
            .is_none_or(|end| end > trailer.trailer_pos)
        {
            return Err(crate::io::Error::new(
                crate::io::ErrorKind::UnexpectedEof,
                "the TOC runs past the trailer",
            )
            .into());
        }

        reader.seek(SeekFrom::Start(trailer.toc_pos))?;
        // Bounded to `toc_len`, so a forged entry length inside the TOC fails
        // as an early end rather than reading what follows; read in chunks,
        // so a damaged `toc_len` costs no buffer of its size.
        let mut reader = ChecksummedReader::new(ChunkedReader::new(reader.take(trailer.toc_len)));

        {
            let mut buf = [0u8; TOC_MAGIC.len()];
            reader.read_exact(&mut buf)?;

            if buf != TOC_MAGIC {
                log::error!("Invalid TOC magic header");
                return Err(crate::sfa::Error::InvalidHeader);
            }
        }

        let len = reader.read_u32::<LE>()?;

        // Don't pre-allocate from a length field that has not yet
        // been checksum-verified — a corrupted / forged TOC could
        // force a multi-GiB Vec allocation before the per-entry
        // reads fail. Grow the Vec amortized as entries are read;
        // the realloc cost is small next to parsing. The `toc_len` bound
        // above caps the total work even if `len` itself is forged.
        let mut entries = Vec::new();

        for _ in 0..len {
            entries.push(TocEntry::read_from_file(&mut reader)?);
        }

        reader.checksum().check(trailer.toc_checksum)?;

        Ok(Toc(entries))
    }
}

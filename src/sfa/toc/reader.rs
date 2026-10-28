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

pub struct TocReader;

impl TocReader {
    pub fn from_reader<R: Read + Seek>(reader: &mut R, trailer: &ParsedTrailer) -> Result<Toc> {
        use crate::io::LE;

        log::trace!("Reading ToC");

        // The TOC lies between its position and the trailer. A length that
        // runs past the trailer is refused here, before a buffer that long
        // is allocated: a forged length cannot ask for more than the file.
        let toc_end = trailer
            .toc_pos
            .checked_add(trailer.toc_len)
            .filter(|&end| end <= trailer.trailer_pos);
        let Some(toc_len) = toc_end.and_then(|_| usize::try_from(trailer.toc_len).ok()) else {
            return Err(crate::io::Error::new(
                crate::io::ErrorKind::UnexpectedEof,
                "the TOC runs past the trailer",
            )
            .into());
        };

        reader.seek(SeekFrom::Start(trailer.toc_pos))?;
        // One read for the whole TOC, parsed from memory: a read per entry
        // field is a system call per field on an unbuffered file. Parsing a
        // buffer exactly `toc_len` long bounds every entry to the TOC region,
        // so a forged entry length inside it fails as an early end rather than
        // reading what follows.
        let mut toc = alloc::vec![0u8; toc_len];
        reader.read_exact(&mut toc)?;
        let mut reader = ChecksummedReader::new(&toc[..]);

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
        // the realloc cost is small next to parsing. The buffer above
        // bounds the total work even if `len` itself is forged.
        let mut entries = Vec::new();

        for _ in 0..len {
            entries.push(TocEntry::read_from_file(&mut reader)?);
        }

        reader.checksum().check(trailer.toc_checksum)?;

        Ok(Toc(entries))
    }
}

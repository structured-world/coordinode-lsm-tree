// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::writer::TOC_MAGIC;
use crate::sfa::{
    Result,
    checksum::Checksum,
    toc::{Toc, entry::TocEntry},
};
use byteorder::ReadBytesExt;
use std::io::{Read, Seek, SeekFrom};

struct ChecksummedReader<R: std::io::Read> {
    inner: R,
    hasher: xxhash_rust::xxh3::Xxh3Default,
}

impl<R: std::io::Read> ChecksummedReader<R> {
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

impl<R: std::io::Read> std::io::Read for ChecksummedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;

        #[expect(clippy::indexing_slicing)]
        self.hasher.update(&buf[..n]);

        Ok(n)
    }
}

pub struct TocReader;

impl TocReader {
    pub fn from_reader<R: Read + Seek>(
        reader: &mut R,
        toc_pos: u64,
        toc_checksum: Checksum,
    ) -> Result<Toc> {
        use byteorder::LE;

        log::trace!("Reading ToC");

        reader.seek(SeekFrom::Start(toc_pos))?;

        let mut reader = ChecksummedReader::new(reader);

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
        // the per-entry I/O cost dwarfs the realloc cost.
        let mut entries = Vec::new();

        for _ in 0..len {
            entries.push(TocEntry::read_from_file(&mut reader)?);
        }

        reader.checksum().check(toc_checksum)?;

        Ok(Toc(entries))
    }
}

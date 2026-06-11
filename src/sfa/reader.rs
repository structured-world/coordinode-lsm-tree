// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

#[cfg(not(feature = "std"))]
use crate::io::{Read, Seek};
use crate::sfa::{
    toc::{Toc, reader::TocReader},
    trailer::reader::TrailerReader,
};
#[cfg(feature = "std")]
use std::io::{BufReader, Read, Seek};

/// Archive reader
pub struct Reader {
    toc: Toc,
}

impl Reader {
    /// Creates a new [`Reader`] from a file path.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    // std-only: opens the path via std::fs. no_std callers use
    // [`Reader::from_reader`] with a caller-provided Fs-backed reader.
    #[cfg(feature = "std")]
    pub fn new(path: impl AsRef<std::path::Path>) -> crate::sfa::Result<Self> {
        let file = std::fs::File::open(path)?;
        let mut file = BufReader::with_capacity(4_096, file);
        let trailer = TrailerReader::from_reader(&mut file)?;
        let toc = TocReader::from_reader(
            &mut file,
            trailer.toc_pos,
            trailer.toc_len,
            trailer.toc_checksum,
        )?;
        Ok(Self { toc })
    }

    /// Creates a new [`Reader`] from a reader.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    pub fn from_reader<R: Read + Seek>(reader: &mut R) -> crate::sfa::Result<Self> {
        let trailer = TrailerReader::from_reader(reader)?;
        let toc = TocReader::from_reader(
            reader,
            trailer.toc_pos,
            trailer.toc_len,
            trailer.toc_checksum,
        )?;
        Ok(Self { toc })
    }

    /// Lists the table of contents.
    #[must_use]
    pub fn toc(&self) -> &Toc {
        &self.toc
    }
}

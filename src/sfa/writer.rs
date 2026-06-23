// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use crate::io::{Seek, Write};
use crate::sfa::{
    toc::{
        entry::{SectionName, TocEntry},
        writer::TocWriter,
    },
    trailer::writer::TrailerWriter,
};

/// Archive writer
#[expect(
    clippy::struct_field_names,
    reason = "upstream sfa struct: `writer` field matches struct name; \
              kept verbatim so the upstream-sync diff stays mechanical"
)]
pub struct Writer<W: Write + Seek> {
    writer: W,
    last_section_pos: u64,
    section_name: SectionName,
    toc: Vec<TocEntry>,
}

impl<W: Write + Seek> Writer<W> {
    /// Returns a mutable reference to the underlying writer.
    pub fn get_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    /// Creates a new writer with the given I/O writer.
    #[must_use]
    pub fn from_writer(writer: W) -> Self {
        Self {
            writer,
            last_section_pos: 0,
            section_name: SectionName::new(),
            toc: Vec::new(),
        }
    }
}

// `crate::io::Write` is the std trait (via the std-mode supertrait blanket)
// under `std` and the native trait under `no_std`; gated impls differ only in
// the trait path and `Result` type.
#[cfg(feature = "std")]
impl<W: Write + Seek> std::io::Write for Writer<W> {
    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }

    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.writer.write(buf)
    }
}

#[cfg(not(feature = "std"))]
impl<W: Write + Seek> crate::io::Write for Writer<W> {
    fn flush(&mut self) -> crate::io::Result<()> {
        self.writer.flush()
    }

    fn write(&mut self, buf: &[u8]) -> crate::io::Result<usize> {
        self.writer.write(buf)
    }
}

impl<W: Write + Seek> Writer<W> {
    /// Starts the first named section.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    pub fn start(&mut self, name: impl Into<SectionName>) -> crate::io::Result<()> {
        self.append_toc_entry()?;
        self.section_name = name.into();
        Ok(())
    }

    fn append_toc_entry(&mut self) -> crate::io::Result<()> {
        let file_pos = self.writer.stream_position()?;

        if file_pos > 0 {
            let name = core::mem::take(&mut self.section_name);
            self.toc.push(TocEntry {
                name,
                pos: self.last_section_pos,
                len: file_pos - self.last_section_pos,
            });
        }

        self.last_section_pos = file_pos;

        Ok(())
    }

    fn append_trailer(writer: &mut W, toc: &[TocEntry]) -> crate::sfa::Result<()> {
        // Write ToC
        let toc_pos = writer.stream_position()?;
        let toc_checksum = TocWriter::write_into(&mut *writer, toc)?;

        let after_toc_pos = writer.stream_position()?;
        let toc_len = after_toc_pos - toc_pos;

        // Write trailer
        TrailerWriter::write_into(writer, toc_checksum, toc_pos, toc_len)
    }

    /// Finishes the file.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    pub fn finish(mut self) -> crate::sfa::Result<()> {
        log::trace!("Finishing archive");

        self.append_toc_entry()?;
        Self::append_trailer(&mut self.writer, &self.toc)?;
        self.writer.flush()?;

        Ok(())
    }

    /// Finishes the file.
    ///
    /// Returns the inner writer.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    pub fn into_inner(mut self) -> crate::sfa::Result<W> {
        log::trace!("Finishing archive");

        self.append_toc_entry()?;
        Self::append_trailer(&mut self.writer, &self.toc)?;
        self.writer.flush()?;

        Ok(self.writer)
    }
}

#[cfg(test)]
#[expect(
    clippy::indexing_slicing,
    reason = "vendored upstream tests use `toc[i]` indexing freely; \
              keep verbatim so the upstream-sync diff stays mechanical"
)]
mod tests;

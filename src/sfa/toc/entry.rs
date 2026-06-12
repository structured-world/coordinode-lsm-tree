// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use crate::io::{ReadBytesExt, WriteBytesExt};
// Codec `impl Read`/`impl Write` bounds resolve to std under `std` and to the
// native `crate::io` traits under `no_std`. The `reader`/`buf_reader` helpers
// are std-only (they open a file), so `File`/`BufReader`/`Path` plus the
// `Seek` trait for `file.seek` live behind the std gate.
#[cfg(not(feature = "std"))]
use crate::io::{Read, Write};
#[cfg(feature = "std")]
use std::io::{Read, Seek, Write};
#[cfg(feature = "std")]
use std::{fs::File, io::BufReader, path::Path};

pub type SectionName = Vec<u8>;

/// Entry in the table of contents (a section in the archive)
#[derive(Debug)]
pub struct TocEntry {
    pub(crate) name: SectionName,
    pub(crate) pos: u64,
    pub(crate) len: u64,
}

impl TocEntry {
    /// Returns the section name.
    #[must_use]
    pub fn name(&self) -> &[u8] {
        &self.name
    }

    /// Returns the section position.
    #[must_use]
    pub fn pos(&self) -> u64 {
        self.pos
    }

    /// Returns the section length in bytes.
    #[must_use]
    #[expect(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        self.len
    }

    #[doc(hidden)]
    #[cfg(feature = "std")]
    pub fn reader(&self, path: &Path) -> std::io::Result<impl std::io::Read> {
        let mut file = File::open(path)?;
        file.seek(std::io::SeekFrom::Start(self.pos))?;
        Ok(file.take(self.len))
    }

    #[doc(hidden)]
    #[cfg(feature = "std")]
    pub fn buf_reader(&self, path: &Path) -> std::io::Result<impl std::io::BufRead> {
        let mut file = BufReader::new(File::open(path)?);
        file.seek(std::io::SeekFrom::Start(self.pos))?;
        Ok(file.take(self.len))
    }

    pub(crate) fn write_into(&self, mut writer: impl Write) -> crate::sfa::Result<()> {
        use crate::io::LE;

        writer.write_u64::<LE>(self.pos())?;
        writer.write_u64::<LE>(self.len())?;

        // Section names longer than u16::MAX cannot fit the on-disk length
        // prefix. Return a structural error instead of panicking: the writer
        // path is a Result and a forged / programmatically generated section
        // name is a recoverable caller error, not an invariant violation.
        let name_len =
            u16::try_from(self.name().len()).map_err(|_| crate::sfa::Error::InvalidHeader)?;
        writer.write_u16::<LE>(name_len)?;
        writer.write_all(self.name())?;

        Ok(())
    }

    pub(crate) fn read_from_file(reader: &mut impl Read) -> crate::sfa::Result<Self> {
        use crate::io::LE;

        let pos = reader.read_u64::<LE>()?;
        let len = reader.read_u64::<LE>()?;
        let section_name_len = reader.read_u16::<LE>()?;

        let mut name = vec![0; section_name_len as usize];
        reader.read_exact(&mut name)?;

        Ok(Self { name, pos, len })
    }
}

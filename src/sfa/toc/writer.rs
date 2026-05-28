// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::sfa::{checksum::Checksum, checksum_writer::ChecksummedWriter, toc::entry::TocEntry};
use byteorder::WriteBytesExt;
use std::io::Write;

pub const TOC_MAGIC: &[u8] = b"TOC!";

pub struct TocWriter;

impl TocWriter {
    pub fn write_into(
        mut writer: impl Write,
        entries: &[TocEntry],
    ) -> crate::sfa::Result<Checksum> {
        use byteorder::LE;

        log::trace!("Writing ToC");
        log::trace!("ToC: {entries:#?}");

        let mut writer = ChecksummedWriter::new(&mut writer);

        writer.write_all(TOC_MAGIC)?;
        writer.write_u32::<LE>(
            #[expect(
                clippy::expect_used,
                reason = "expect message documents the invariant: a TOC with >=2^32 entries is impossible by construction; panicking on the unreachable overflow is preferred over silent truncation"
            )]
            u32::try_from(entries.len())
                .expect("table of contents should not have 4 billion or more entries"),
        )?;

        for entry in entries {
            entry.write_into(&mut writer)?;
        }

        Ok(writer.checksum())
    }
}

// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::writer::TRAILER_MAGIC;
use crate::sfa::{Result, checksum::Checksum};
use byteorder::ReadBytesExt;
use std::io::{Read, Seek, SeekFrom};

#[expect(clippy::cast_possible_wrap)]
const TRAILER_SIZE: i64 = TRAILER_MAGIC.len() as i64 + 1 + 1 + 16 + 8 + 8;

#[derive(Debug, Eq, PartialEq)]
#[expect(
    clippy::struct_field_names,
    reason = "all three fields describe TOC metadata recorded in the trailer; \
              the `toc_` prefix is part of the upstream sfa format wording \
              and keeps the diff vs vendored upstream mechanical"
)]
pub struct ParsedTrailer {
    pub toc_checksum: Checksum,
    pub toc_pos: u64,
    /// On-disk byte length of the TOC region, copied from the
    /// trailer. The `TocReader` uses this to bound its own reads
    /// via `Read::take(toc_len)` so a forged TOC `len` prefix
    /// can't force unbounded work / memory before the checksum
    /// catches it.
    pub toc_len: u64,
}

pub struct TrailerReader;

impl TrailerReader {
    pub fn from_reader<R: Read + Seek>(reader: &mut R) -> Result<ParsedTrailer> {
        use byteorder::LE;

        log::trace!("Reading trailer");

        reader.seek(SeekFrom::End(-TRAILER_SIZE))?;

        {
            let mut buf = [0u8; TRAILER_MAGIC.len()];
            reader.read_exact(&mut buf)?;

            if buf != TRAILER_MAGIC {
                log::error!("Invalid trailer header");
                return Err(crate::sfa::Error::InvalidHeader);
            }
        }

        {
            let version = reader.read_u8()?;
            if version != 0x1 {
                log::error!("Invalid version");
                return Err(crate::sfa::Error::InvalidVersion);
            }
        }

        {
            let checksum_type = reader.read_u8()?;
            if checksum_type != 0x0 {
                log::error!("Invalid checksum type");
                return Err(crate::sfa::Error::UnsupportedChecksumType);
            }
        }

        let toc_checksum = Checksum::from_raw(reader.read_u128::<LE>()?);
        let toc_pos = reader.read_u64::<LE>()?;
        let toc_len = reader.read_u64::<LE>()?;

        Ok(ParsedTrailer {
            toc_checksum,
            toc_pos,
            toc_len,
        })
    }
}

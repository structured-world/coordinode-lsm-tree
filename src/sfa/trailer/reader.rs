// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::writer::TRAILER_MAGIC;
use crate::io::ReadBytesExt;
use crate::sfa::{Result, checksum::Checksum};
// `Read`/`Seek`/`SeekFrom` resolve to std under `std` and to the native
// `crate::io` types under `no_std`; gated together so the `SeekFrom` value
// matches whichever `Seek::seek` is in play (crate::io::SeekFrom is a distinct
// type from std's, not a re-export).
#[cfg(not(feature = "std"))]
use crate::io::{Read, Seek, SeekFrom};
#[cfg(feature = "std")]
use std::io::{Read, Seek, SeekFrom};

#[expect(
    clippy::cast_possible_wrap,
    reason = "TRAILER_LEN is a fixed small constant from the upstream sfa format (well under i64::MAX); the cast to i64 here matches the SeekFrom::End(-TRAILER_SIZE) call site and cannot wrap"
)]
const TRAILER_SIZE: i64 = super::TRAILER_LEN as i64;

#[derive(Debug, Eq, PartialEq)]
pub struct ParsedTrailer {
    pub toc_checksum: Checksum,
    pub toc_pos: u64,
    /// On-disk byte length of the TOC region, copied from the
    /// trailer. The `TocReader` reads exactly this many bytes, so a
    /// forged TOC `len` prefix can't force unbounded work / memory
    /// before the checksum catches it.
    pub toc_len: u64,
    /// Where the trailer starts: the end of everything before it, the TOC
    /// included.
    pub trailer_pos: u64,
}

pub struct TrailerReader;

impl TrailerReader {
    pub fn from_reader<R: Read + Seek>(reader: &mut R) -> Result<ParsedTrailer> {
        use crate::io::LE;

        log::trace!("Reading trailer");

        let trailer_pos = reader.seek(SeekFrom::End(-TRAILER_SIZE))?;
        // One read for the whole trailer, parsed from memory: a read per
        // field is a system call per field on an unbuffered file.
        let mut trailer = [0u8; super::TRAILER_LEN];
        reader.read_exact(&mut trailer)?;
        let reader = &mut &trailer[..];

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
            trailer_pos,
        })
    }
}

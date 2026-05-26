// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use super::Checksum;
use crate::checksum::ChecksummedWriter;
use crate::coding::{Decode, Encode};
use crate::file::MAGIC_BYTES;
use crate::table::block::BlockType;
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

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

    /// Optionally expose the inner reader if needed
    pub fn into_inner(self) -> R {
        self.inner
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

/// Header of a disk-based block
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Header {
    pub block_type: BlockType,

    /// Checksum value to verify integrity of data
    pub checksum: Checksum,

    /// On-disk size of data segment
    pub data_length: u32,

    /// Uncompressed size of data segment
    pub uncompressed_length: u32,

    /// Length in bytes of the Reed-Solomon parity trailer that follows
    /// the `data_length` payload bytes on disk. `0` when the block was
    /// written without Page ECC (`Config::page_ecc(false)`, the
    /// default), in which case no parity bytes follow — the *payload
    /// region* stays V5-shaped (header + payload, no trailer). The
    /// *header* itself is always V6: 4 extra bytes for this field
    /// plus the bumped magic `[L,S,M,4]` (V5 was `[L,S,M,3]`), so a
    /// pre-V6 reader rejects every V6 block at header decode
    /// regardless of `ecc_length`'s value. Non-zero when ECC is
    /// enabled — the reader uses it to read the parity bytes and
    /// attempt Reed-Solomon recovery on `data` XXH3 mismatch.
    pub ecc_length: u32,
}

impl Header {
    #[must_use]
    pub const fn serialized_len() -> usize {
        MAGIC_BYTES.len()
            // Block type
            + std::mem::size_of::<BlockType>()
            // Data checksum
            + std::mem::size_of::<Checksum>()
            // On-disk size
            + std::mem::size_of::<u32>()
            // Uncompressed data length
            + std::mem::size_of::<u32>()
            // Reed-Solomon parity trailer length (0 when ECC off)
            + std::mem::size_of::<u32>()
            // Checksum
            + std::mem::size_of::<u32>()
    }

    /// Total bytes this block occupies on disk: header + payload +
    /// optional ECC parity trailer. Use this when computing block
    /// handles instead of manually summing `serialized_len() +
    /// data_length` — that older form silently underflows the
    /// on-disk size when `ecc_length > 0`.
    #[must_use]
    pub fn on_disk_size(&self) -> u32 {
        // serialized_len is a small constant (28 bytes); cast to
        // u32 is safe by construction. data_length + ecc_length
        // is already bounded by the writer's MAX_DECOMPRESSION_SIZE
        // checks, well within u32.
        #[expect(
            clippy::cast_possible_truncation,
            reason = "Header::serialized_len() is a small const"
        )]
        let header = Self::serialized_len() as u32;
        header
            .saturating_add(self.data_length)
            .saturating_add(self.ecc_length)
    }
}

impl Encode for Header {
    fn encode_into<W: Write>(&self, mut writer: &mut W) -> Result<(), crate::Error> {
        use byteorder::LE;

        let checksum = {
            let mut writer = ChecksummedWriter::new(&mut writer);

            // Write header
            writer.write_all(&MAGIC_BYTES)?;

            // Write block type
            writer.write_u8(self.block_type.into())?;

            // Write data checksum
            writer.write_u128::<LE>(self.checksum.into_u128())?;

            // Write on-disk size length
            writer.write_u32::<LE>(self.data_length)?;

            // Write uncompressed data length
            writer.write_u32::<LE>(self.uncompressed_length)?;

            // Write Reed-Solomon parity trailer length (V6+); 0 when
            // Page ECC is disabled.
            writer.write_u32::<LE>(self.ecc_length)?;

            writer.checksum()
        };

        #[expect(
            clippy::cast_possible_truncation,
            reason = "we purposefully only use the lower 4 bytes as checksum"
        )]
        // Write 4-byte checksum
        writer.write_u32::<LE>(checksum.into_u128() as u32)?;

        Ok(())
    }
}

impl Decode for Header {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, crate::Error> {
        use byteorder::LE;

        let mut protected_reader = ChecksummedReader::new(reader);

        // Check header
        let mut magic = [0u8; MAGIC_BYTES.len()];
        protected_reader.read_exact(&mut magic)?;

        if magic != MAGIC_BYTES {
            return Err(crate::Error::InvalidHeader("Block"));
        }

        // Read block type
        let block_type = protected_reader.read_u8()?;
        let block_type = BlockType::try_from(block_type)?;

        // Read data checksum
        let checksum = protected_reader.read_u128::<LE>()?;

        // Read data length
        let data_length = protected_reader.read_u32::<LE>()?;

        // Read data length
        let uncompressed_length = protected_reader.read_u32::<LE>()?;

        // Read Reed-Solomon parity trailer length (V6+)
        let ecc_length = protected_reader.read_u32::<LE>()?;

        #[expect(
            clippy::cast_possible_truncation,
            reason = "we purposefully only use the lower 4 bytes as checksum"
        )]
        // Get header checksum
        let got_checksum = protected_reader.checksum().into_u128() as u32;
        let got_checksum = Checksum::from_raw(u128::from(got_checksum));

        let reader = protected_reader.into_inner();

        // Read & check checksum
        let header_checksum: u128 = reader.read_u32::<LE>()?.into();
        let header_checksum = Checksum::from_raw(header_checksum);

        if header_checksum != got_checksum {
            return Err(crate::Error::ChecksumMismatch {
                got: got_checksum,
                expected: header_checksum,
            });
        }

        Ok(Self {
            block_type,
            checksum: Checksum::from_raw(checksum),
            data_length,
            uncompressed_length,
            ecc_length,
        })
    }
}

#[cfg(test)]
impl Header {
    /// Test-only constructor for placeholder Header values used in
    /// unit tests that don't care about checksum / lengths. All
    /// numeric fields are zero. Callers that need specific lengths
    /// or a non-zero checksum override via struct-update syntax:
    ///
    /// ```ignore
    /// // All fields zero, just the block_type:
    /// Header::test_dummy(BlockType::Data)
    ///
    /// // Override only the data_length / uncompressed_length:
    /// Header {
    ///     data_length: 42,
    ///     uncompressed_length: 42,
    ///     ..Header::test_dummy(BlockType::Index)
    /// }
    /// ```
    ///
    /// The whole point of this helper is to keep test sites
    /// future-proof: adding a new field to `Header` only needs the
    /// new default wired in here, not at every test literal across
    /// the crate.
    pub(crate) fn test_dummy(block_type: BlockType) -> Self {
        Self {
            block_type,
            checksum: Checksum::from_raw(0),
            data_length: 0,
            uncompressed_length: 0,
            ecc_length: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn block_header_serde_roundtrip() -> crate::Result<()> {
        let header = Header {
            block_type: BlockType::Data,
            checksum: Checksum::from_raw(5),
            data_length: 252_356,
            uncompressed_length: 124_124_124,
            ecc_length: 0,
        };

        let bytes = header.encode_into_vec();

        assert_eq!(bytes.len(), Header::serialized_len());
        assert_eq!(header, Header::decode_from(&mut &bytes[..])?);

        Ok(())
    }

    #[test]
    #[expect(clippy::indexing_slicing)]
    fn block_header_detect_corruption() {
        let header = Header {
            block_type: BlockType::Data,
            checksum: Checksum::from_raw(5),
            data_length: 252_356,
            uncompressed_length: 124_124_124,
            ecc_length: 0,
        };

        let mut bytes = header.encode_into_vec();
        bytes[5] += 1; // mutate block type enum tag

        assert!(
            matches!(
                Header::decode_from(&mut &bytes[..]),
                Err(crate::Error::ChecksumMismatch { .. }),
            ),
            "did not detect header corruption",
        );
    }
}

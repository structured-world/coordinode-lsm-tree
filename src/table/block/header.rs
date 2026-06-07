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

/// Per-block transform-layer presence flags carried in the block
/// header (`Header::block_flags`).
///
/// Each bit records that one orthogonal, composable transform layer
/// was applied to the block payload. This is the single home for the
/// "which layers are present" question; the per-layer PARAMETERS live
/// elsewhere (the parity length is DERIVED from `data_length` via the
/// Reed-Solomon scheme, the per-KV footer's algorithm + count
/// self-describe in the footer tail, the compression codec + zstd dict
/// come from the owning SST's compression policy, the encryption
/// scheme/key from its encryption config). A presence bit can't carry
/// those parameters, so it doesn't try to — it answers presence, the
/// parameter homes answer "how".
///
/// `BlockType` stays orthogonal to these flags: it is the block's
/// mutually-exclusive ROLE (Data / Index / Filter / …), not a
/// transform layer. A compressed, encrypted, per-KV-checked data
/// block is still `BlockType::Data` with three bits set — the layers
/// compose, the role does not.
pub mod block_flags {
    /// The payload is followed by a per-entry checksum footer
    /// (per-KV integrity). The footer's algorithm + entry count
    /// self-describe in its tail; this bit says only that it is
    /// present and must be split off before the inner payload is
    /// decoded as a plain block of its role.
    pub const KV_CHECKSUM_FOOTER: u8 = 1 << 0;

    /// The payload is followed by a Reed-Solomon parity trailer.
    /// Its byte length is NOT stored: the reader derives it as
    /// `expected_parity_len(data_length)` (the RS(4, 2) scheme is
    /// deterministic). This bit is the canonical, presence-authoritative
    /// signal that a trailer follows; a block whose payload is empty
    /// emits a zero-length trailer and leaves this bit clear.
    pub const ECC_PARITY: u8 = 1 << 1;

    /// The payload was compressed. The codec (and any zstd dict) is
    /// not stored per block — it comes from the owning SST's
    /// compression policy, which the reader supplies via the
    /// `BlockTransform`. This bit is a presence-only self-describing
    /// signal; the read path decodes using the caller-supplied
    /// transform and does not currently validate this bit against it.
    pub const COMPRESSED: u8 = 1 << 2;

    /// The payload was encrypted. The scheme/key comes from the
    /// owning SST's encryption config (caller-supplied via the
    /// `BlockTransform`); this bit is a presence-only self-describing
    /// signal, not validated against the transform on the read path.
    pub const ENCRYPTED: u8 = 1 << 3;

    /// Mask of every defined transform-layer bit. The header decoder
    /// rejects any byte with a bit set outside this mask: `block_flags`
    /// is a persisted transform field, so an unknown high bit means a
    /// newer writer or a forged header is trying to declare a layer this
    /// build does not understand. Failing fast beats silently treating
    /// it as a partially-known block.
    pub const KNOWN: u8 = KV_CHECKSUM_FOOTER | ECC_PARITY | COMPRESSED | ENCRYPTED;
}

/// Header of a disk-based block
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Header {
    pub block_type: BlockType,

    /// Transform-layer presence bitfield — see the `block_flags` module in
    /// this file for the bit definitions. `0` means a plain block of its role
    /// with no compression / encryption / ECC / per-KV footer.
    pub block_flags: u8,

    /// Checksum value to verify integrity of data
    pub checksum: Checksum,

    /// On-disk size of data segment
    pub data_length: u32,

    /// Uncompressed size of data segment
    pub uncompressed_length: u32,
}

impl Header {
    /// Header size WITHOUT the optional `block_flags` byte: the fixed part
    /// every block carries (magic + `block_type` + checksum + `data_length` +
    /// `uncompressed_length` + header checksum). Pre-decode lower bound; the
    /// actual size for a given block is [`Self::header_len`].
    pub const MIN_LEN: usize = MAGIC_BYTES.len()
        // Block type — encoded as a single u8 by encode_into, not as
        // size_of::<BlockType>(). BlockType is a fieldless enum without
        // `#[repr(u8)]`, so its in-memory size is implementation-defined;
        // the wire format is the contract and that contract is 1 byte.
        + 1
        // Data checksum
        + std::mem::size_of::<Checksum>()
        // On-disk size
        + std::mem::size_of::<u32>()
        // Uncompressed data length
        + std::mem::size_of::<u32>()
        // Header checksum
        + std::mem::size_of::<u32>();

    /// Largest possible header size: [`Self::MIN_LEN`] plus the optional
    /// `block_flags` byte. Used as a pre-decode upper bound where the
    /// `block_type` (and thus exact header length) is not yet known.
    pub const MAX_LEN: usize = Self::MIN_LEN + 1;

    /// Whether a block of `block_type` carries the `block_flags` byte.
    ///
    /// Bootstrap / out-of-band blocks keep the self-describing byte:
    /// `Meta` is read before its own per-SST descriptor exists (the
    /// descriptor lives inside it); `Manifest` / `ManifestFooter` belong to
    /// the manifest subsystem, which has no per-SST descriptor at all.
    ///
    /// Ordinary SST blocks (`Data` / `Index` / `Filter` / `RangeTombstone`)
    /// are read AFTER their table's `ParsedMeta` descriptor is loaded, so
    /// their transform presence is sourced from that descriptor and the byte
    /// is omitted — saving one byte per block.
    #[must_use]
    pub const fn has_block_flags(block_type: BlockType) -> bool {
        matches!(
            block_type,
            BlockType::Meta | BlockType::Manifest | BlockType::ManifestFooter
        )
    }

    /// On-disk header length for `block_type`: [`Self::MIN_LEN`] plus the
    /// `block_flags` byte when [`Self::has_block_flags`] is true.
    #[must_use]
    pub const fn header_len(block_type: BlockType) -> usize {
        if Self::has_block_flags(block_type) {
            Self::MIN_LEN + 1
        } else {
            Self::MIN_LEN
        }
    }

    /// Total bytes this block occupies on disk: header + payload +
    /// optional ECC parity trailer. Use this when computing block
    /// handles instead of manually summing `header_len(block_type) +
    /// data_length` — that form silently underflows the on-disk size
    /// when a parity trailer is present.
    ///
    /// The parity-trailer length is derived from `data_length` (the
    /// Reed-Solomon scheme is deterministic) and the `ECC_PARITY`
    /// presence bit, not stored: a block that set the bit at write time
    /// carries `expected_parity_len(data_length)` parity bytes.
    ///
    /// # Parity accounting is only valid when `block_flags` is authoritative
    ///
    /// This reads parity presence from [`Self::block_flags`]. That field is
    /// authoritative for headers freshly built by the writer (the on-disk-size
    /// call sites that compute block handles) and for the block types that
    /// serialize the byte (`Meta` / `Manifest` / `ManifestFooter` — see
    /// [`Self::has_block_flags`]). It is NOT authoritative on a header obtained
    /// from [`Decode::decode_from`] for an SST block type (`Data` / `Index` /
    /// `Filter` / `RangeTombstone`): those omit the byte on disk, so it decodes
    /// as `0` and this method UNDERCOUNTS an ECC-bearing SST block by its parity
    /// trailer. A caller holding a decoded SST header must derive the trailer
    /// from the per-SST `page_ecc` descriptor and
    /// [`expected_parity_len`](super::expected_parity_len), as the block scrub
    /// walker does — do not call `on_disk_size` for that.
    #[must_use]
    pub fn on_disk_size(&self) -> u32 {
        // header_len is a small constant (33 or 34 bytes: 4 magic + 1
        // block_type + [1 block_flags for meta/manifest] + 16 checksum +
        // 4 data_length + 4 uncompressed + 4 header checksum); cast to u32
        // is safe by construction. `data_length` is bounded by the writer's
        // `MAX_DECOMPRESSION_SIZE` cap and the parity length is bounded
        // by `expected_parity_len(data_length)`, so the sum stays well
        // within u32.
        #[expect(
            clippy::cast_possible_truncation,
            reason = "Header::header_len() is a small const"
        )]
        let header = Self::header_len(self.block_type) as u32;
        let parity = if self.block_flags & block_flags::ECC_PARITY != 0 {
            // Self-describing blocks (Meta / Manifest) carry only the
            // ECC_PARITY bit, not a scheme, so they use the fixed default
            // layout; the configurable per-SST scheme applies to SST data
            // blocks (whose scheme comes from the table descriptor).
            super::expected_parity_len(self.data_length, super::EccParams::RS_4_2)
        } else {
            0
        };
        header
            .saturating_add(self.data_length)
            .saturating_add(parity)
    }

    /// On-disk byte size of this block under a CALLER-supplied ECC scheme,
    /// rather than the fixed RS(4,2) [`Self::on_disk_size`] assumes.
    ///
    /// SST data / index / filter blocks carry a configurable scheme (the
    /// writer's resolved [`EccParams`](super::EccParams)); their on-disk
    /// size — and therefore the index block-handle size — must be computed
    /// with the SAME scheme the parity trailer was written under, not the
    /// RS(4,2) default. `ecc = None` means no parity trailer.
    #[must_use]
    pub fn on_disk_size_with(&self, ecc: Option<super::EccParams>) -> u32 {
        #[expect(
            clippy::cast_possible_truncation,
            reason = "Header::header_len() is a small const"
        )]
        let header = Self::header_len(self.block_type) as u32;
        let parity = ecc.map_or(0, |p| super::expected_parity_len(self.data_length, p));
        header
            .saturating_add(self.data_length)
            .saturating_add(parity)
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

            // Write transform-layer presence flags — only for block types
            // that carry the byte (Meta / Manifest / ManifestFooter). SST
            // blocks omit it and derive transform presence from the per-SST
            // descriptor on read.
            if Self::has_block_flags(self.block_type) {
                writer.write_u8(self.block_flags)?;
            }

            // Write data checksum
            writer.write_u128::<LE>(self.checksum.into_u128())?;

            // Write on-disk size length
            writer.write_u32::<LE>(self.data_length)?;

            // Write uncompressed data length
            writer.write_u32::<LE>(self.uncompressed_length)?;

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

        // Read transform-layer presence flags — only for block types that
        // carry the byte (Meta / Manifest / ManifestFooter). SST blocks omit
        // it (transform presence comes from the per-SST descriptor), so their
        // in-memory `block_flags` is 0 and callers consult the descriptor.
        // Reject any byte with a bit outside the defined set: an unknown high
        // bit means a newer writer or a forged header is declaring a layer
        // this build can't honor.
        let block_flags = if Self::has_block_flags(block_type) {
            let flags = protected_reader.read_u8()?;
            if flags & !block_flags::KNOWN != 0 {
                return Err(crate::Error::InvalidTag(("block_flags", flags)));
            }
            flags
        } else {
            0
        };

        // Read data checksum
        let checksum = protected_reader.read_u128::<LE>()?;

        // Read data length
        let data_length = protected_reader.read_u32::<LE>()?;

        // Read data length
        let uncompressed_length = protected_reader.read_u32::<LE>()?;

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
            block_flags,
            checksum: Checksum::from_raw(checksum),
            data_length,
            uncompressed_length,
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
            block_flags: 0,
            checksum: Checksum::from_raw(0),
            data_length: 0,
            uncompressed_length: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn block_header_serde_roundtrip() -> crate::Result<()> {
        // Manifest carries the block_flags byte, so this exercises the
        // round-trip of a non-zero flags byte (SST block types omit it).
        let header = Header {
            block_type: BlockType::Manifest,
            block_flags: block_flags::KV_CHECKSUM_FOOTER | block_flags::COMPRESSED,
            checksum: Checksum::from_raw(5),
            data_length: 252_356,
            uncompressed_length: 124_124_124,
        };

        let bytes = header.encode_into_vec();

        assert_eq!(bytes.len(), Header::header_len(BlockType::Manifest));
        assert_eq!(header, Header::decode_from(&mut &bytes[..])?);

        Ok(())
    }

    #[test]
    fn block_header_serde_roundtrip_sst_omits_flags_byte() -> crate::Result<()> {
        // SST block types omit the block_flags byte: a Data header encodes to
        // MIN_LEN bytes and decodes back with block_flags == 0 regardless of
        // the in-memory value (which the writer leaves at 0 for SST blocks).
        let header = Header {
            block_type: BlockType::Data,
            block_flags: 0,
            checksum: Checksum::from_raw(7),
            data_length: 42,
            uncompressed_length: 42,
        };
        let bytes = header.encode_into_vec();
        assert_eq!(bytes.len(), Header::MIN_LEN);
        assert_eq!(header, Header::decode_from(&mut &bytes[..])?);
        Ok(())
    }

    #[test]
    fn block_header_rejects_unknown_block_flags_bit() {
        // `block_flags` is a persisted transform field. A header carrying a
        // bit this build does not define (here the reserved 1 << 4) must be
        // rejected at decode, not silently accepted as a partially-known
        // block. The header + checksum are otherwise valid, so this isolates
        // the flag-mask check from checksum validation. Uses Manifest, which
        // carries the block_flags byte (SST types omit it entirely).
        let header = Header {
            block_type: BlockType::Manifest,
            block_flags: 1 << 4,
            checksum: Checksum::from_raw(5),
            data_length: 10,
            uncompressed_length: 10,
        };
        let bytes = header.encode_into_vec();
        assert!(
            matches!(
                Header::decode_from(&mut &bytes[..]),
                Err(crate::Error::InvalidTag(("block_flags", _))),
            ),
            "decode must reject an unknown block_flags bit",
        );
    }

    #[test]
    #[expect(clippy::indexing_slicing)]
    fn block_header_detect_corruption() {
        let header = Header {
            block_type: BlockType::Data,
            block_flags: 0,
            checksum: Checksum::from_raw(5),
            data_length: 252_356,
            uncompressed_length: 124_124_124,
        };

        let mut bytes = header.encode_into_vec();
        // Mutate a header byte (offset 5 is the first checksum byte for a
        // Data header, which omits the block_flags byte). Any header byte flip
        // must be caught by the header checksum.
        bytes[5] += 1;

        assert!(
            matches!(
                Header::decode_from(&mut &bytes[..]),
                Err(crate::Error::ChecksumMismatch { .. }),
            ),
            "did not detect header corruption",
        );
    }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

#[cfg(not(feature = "std"))]
use crate::io::{Read, Write};
#[cfg(not(feature = "std"))]
use crate::io::{VarintReader, VarintWriter};
use crate::{
    coding::{Decode, Encode},
    vlog::ValueHandle,
};
#[cfg(feature = "std")]
use std::io::{Read, Write};
#[cfg(feature = "std")]
use varint_rs::{VarintReader, VarintWriter};

/// What a separated value takes in its blob file, what collecting it frees.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Stored {
    /// The exact bytes of the frames the value has of its own.
    Frames(u32),
    /// The value's entry in a block it shares, uncompressed: the block is
    /// compressed after the value is written, so its share on disk is this
    /// entry's part of the bytes the file's blocks take.
    InBlock(u32),
    /// The value's record in a packed block, its exact length: the record
    /// starts where the handle's index says, so a read of it reads it alone.
    /// It shares the block's header with its neighbours, so it is charged as
    /// a share of the file's blocks, as a block entry is.
    Packed(u32),
}

impl Stored {
    /// The bytes any kind counts: exact frame bytes, the uncompressed entry,
    /// or the record.
    pub(crate) fn bytes(self) -> u32 {
        match self {
            Self::Frames(bytes) | Self::InBlock(bytes) | Self::Packed(bytes) => bytes,
        }
    }

    /// The exact frame bytes, and the block bytes, an entry's or a record's.
    pub(crate) fn split(self) -> (u64, u64) {
        match self {
            Self::Frames(bytes) => (u64::from(bytes), 0),
            Self::InBlock(bytes) | Self::Packed(bytes) => (0, u64::from(bytes)),
        }
    }
}

/// What a table stores for a separated value: where it lies, its length, and
/// what it takes in its blob file.
#[derive(Copy, Clone, Debug)]
pub struct BlobIndirection {
    pub(crate) vhandle: ValueHandle,
    /// The value's decoded length.
    pub(crate) size: u32,
    /// The bytes the value takes in its blob file.
    pub(crate) stored: Stored,
}

impl Encode for BlobIndirection {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), crate::Error> {
        self.vhandle.encode_into(writer)?;
        writer.write_u32_varint(self.size)?;
        // The kind rides in the low bits, so it costs no byte of its own.
        let stored = match self.stored {
            Stored::Frames(bytes) => u64::from(bytes) << 2,
            Stored::InBlock(bytes) => (u64::from(bytes) << 2) | 1,
            Stored::Packed(bytes) => (u64::from(bytes) << 2) | 2,
        };
        writer.write_u64_varint(stored)?;
        Ok(())
    }
}

impl BlobIndirection {
    /// The most bytes an encoded indirection takes: the varints of its file id
    /// (10), frame (5), index (3), size (5) and stored word (a `u32` shifted by
    /// two bits, 5).
    #[cfg(feature = "std")]
    pub(crate) const MAX_ENCODED_LEN: u64 = Self::ENCODED_CAP as u64;

    /// Encodes this indirection into `buf` and returns the bytes written:
    /// what [`Encode`] writes, a varint at a time straight into the caller's
    /// buffer, for the writers that encode one for every value.
    #[inline]
    pub(crate) fn encode_in<'b>(&self, buf: &'b mut [u8; Self::ENCODED_CAP]) -> &'b [u8] {
        #[inline]
        fn put(buf: &mut [u8], at: &mut usize, mut value: u64) {
            loop {
                let low = value.to_le_bytes()[0] & 0x7f;
                value >>= 7;
                // The buffer holds the longest encoding, so every byte lands.
                if let Some(slot) = buf.get_mut(*at) {
                    *slot = if value == 0 { low } else { low | 0x80 };
                }
                *at += 1;
                if value == 0 {
                    return;
                }
            }
        }
        let stored = match self.stored {
            Stored::Frames(bytes) => u64::from(bytes) << 2,
            Stored::InBlock(bytes) => (u64::from(bytes) << 2) | 1,
            Stored::Packed(bytes) => (u64::from(bytes) << 2) | 2,
        };
        let mut at = 0;
        put(buf, &mut at, self.vhandle.blob_file_id);
        put(buf, &mut at, u64::from(self.vhandle.frame));
        put(buf, &mut at, u64::from(self.vhandle.index));
        put(buf, &mut at, u64::from(self.size));
        put(buf, &mut at, stored);
        buf.get(..at).unwrap_or_default()
    }

    /// Room for the longest encoded indirection.
    pub(crate) const ENCODED_CAP: usize = 10 + 5 + 3 + 5 + 5;

    /// The most bytes the indirection of this value takes once the value is
    /// moved: its length stays, so that field keeps its bytes, and its file,
    /// frame, index and stored word may each take the most theirs can.
    #[cfg(feature = "std")]
    pub(crate) fn longest_moved(&self) -> u64 {
        Self::MAX_ENCODED_LEN - 5 + crate::table::writer::varint_len(u64::from(self.size))
    }

    /// The bytes this indirection takes encoded.
    #[cfg(feature = "std")]
    pub(crate) fn encoded_len(&self) -> u64 {
        use crate::table::writer::varint_len;

        let stored = match self.stored {
            Stored::Frames(bytes) | Stored::InBlock(bytes) | Stored::Packed(bytes) => bytes,
        };
        varint_len(self.vhandle.blob_file_id)
            + varint_len(u64::from(self.vhandle.frame))
            + varint_len(u64::from(self.vhandle.index))
            + varint_len(u64::from(self.size))
            + varint_len(u64::from(stored) << 2)
    }

    /// Decodes the indirection a table row stores as `value`, straight off the
    /// slice: what [`Decode`] reads of a well-formed one.
    ///
    /// # Errors
    ///
    /// [`crate::Error::InvalidHeader`] for a value that does not hold one: a
    /// varint cut short, longer than its field allows or past its width, or
    /// an unknown stored kind.
    #[inline]
    pub(crate) fn decode_slice(value: &[u8]) -> Result<Self, crate::Error> {
        let mut bytes = value;
        Self::take_from(&mut bytes).ok_or(crate::Error::InvalidHeader("BlobIndirection"))
    }

    /// The indirection off the front of `bytes`; `None` for one that does not
    /// decode.
    #[inline]
    fn take_from(bytes: &mut &[u8]) -> Option<Self> {
        use crate::coding::take_varint;

        let vhandle = ValueHandle::take_from(bytes)?;
        let size = u32::try_from(take_varint(bytes, 5)?).ok()?;
        let word = take_varint(bytes, 10)?;
        let stored_bytes = u32::try_from(word >> 2).ok()?;
        let stored = match word & 3 {
            0 => Stored::Frames(stored_bytes),
            1 => Stored::InBlock(stored_bytes),
            2 => Stored::Packed(stored_bytes),
            _ => return None,
        };
        Some(Self {
            vhandle,
            size,
            stored,
        })
    }
}

impl Decode for BlobIndirection {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, crate::Error> {
        let vhandle = ValueHandle::decode_from(reader)?;
        let size = reader.read_u32_varint()?;
        let word = reader.read_u64_varint()?;
        let malformed = || crate::Error::InvalidHeader("BlobIndirection");
        let bytes = u32::try_from(word >> 2).map_err(|_| malformed())?;
        let stored = match word & 3 {
            0 => Stored::Frames(bytes),
            1 => Stored::InBlock(bytes),
            2 => Stored::Packed(bytes),
            _ => return Err(malformed()),
        };
        Ok(Self {
            vhandle,
            size,
            stored,
        })
    }
}

#[cfg(test)]
mod tests;

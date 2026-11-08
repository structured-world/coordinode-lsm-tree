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
}

impl Stored {
    /// The bytes either kind counts: exact frame bytes, or the uncompressed
    /// entry.
    pub(crate) fn bytes(self) -> u32 {
        match self {
            Self::Frames(bytes) | Self::InBlock(bytes) => bytes,
        }
    }

    /// The exact frame bytes, and the uncompressed block entry bytes.
    pub(crate) fn split(self) -> (u64, u64) {
        match self {
            Self::Frames(bytes) => (u64::from(bytes), 0),
            Self::InBlock(bytes) => (0, u64::from(bytes)),
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
        // The kind rides in the low bit, so the common case costs no byte of
        // its own.
        let stored = match self.stored {
            Stored::Frames(bytes) => u64::from(bytes) << 1,
            Stored::InBlock(bytes) => (u64::from(bytes) << 1) | 1,
        };
        writer.write_u64_varint(stored)?;
        Ok(())
    }
}

impl Decode for BlobIndirection {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, crate::Error> {
        let vhandle = ValueHandle::decode_from(reader)?;
        let size = reader.read_u32_varint()?;
        let word = reader.read_u64_varint()?;
        let bytes =
            u32::try_from(word >> 1).map_err(|_| crate::Error::InvalidHeader("BlobIndirection"))?;
        let stored = if word & 1 == 0 {
            Stored::Frames(bytes)
        } else {
            Stored::InBlock(bytes)
        };
        Ok(Self {
            vhandle,
            size,
            stored,
        })
    }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

#[cfg(not(feature = "std"))]
use crate::io::{Read, Write};
#[cfg(not(feature = "std"))]
use crate::io::{VarintReader, VarintWriter};
use crate::{
    coding::{Decode, Encode},
    vlog::BlobFileId,
};
use core::hash::Hash;
#[cfg(feature = "std")]
use std::io::{Read, Write};
#[cfg(feature = "std")]
use varint_rs::{VarintReader, VarintWriter};

/// A value handle points into the value log: the blob file, the frame that
/// holds the value (a chunked value's head) and the value's place in the
/// frame's list.
///
/// A frame number, not a byte offset, so the handle of a value is known when
/// the value is written, before the block it joins is compressed; the file's
/// frame index maps numbers to offsets.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ValueHandle {
    /// Blob file ID
    pub(crate) blob_file_id: BlobFileId,

    /// The frame that holds the value
    pub(crate) frame: u32,

    /// The value's place in the frame's list
    pub(crate) index: u16,
}

impl ValueHandle {
    /// The frame this handle names.
    #[must_use]
    pub(crate) fn frame_at(&self) -> crate::cache::BlobFrameAt {
        crate::cache::BlobFrameAt {
            blob_file_id: self.blob_file_id,
            frame: self.frame,
        }
    }

    /// The value's place in its blob file as one number, ordered as the
    /// values lie: the frame above the index. What names an object a cell
    /// row owns.
    #[must_use]
    pub(crate) fn object(&self) -> u64 {
        (u64::from(self.frame) << 16) | u64::from(self.index)
    }
}

impl Encode for ValueHandle {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), crate::Error> {
        writer.write_u64_varint(self.blob_file_id)?;
        writer.write_u32_varint(self.frame)?;
        writer.write_u16_varint(self.index)?;
        Ok(())
    }
}

impl ValueHandle {
    /// Takes the handle off the front of `bytes` as [`Decode`] reads it,
    /// straight off the slice; `None` where that read would fail.
    #[inline]
    pub(crate) fn take_from(bytes: &mut &[u8]) -> Option<Self> {
        use crate::coding::take_varint;

        let blob_file_id = take_varint(bytes, 10)?;
        let frame = u32::try_from(take_varint(bytes, 5)?).ok()?;
        let index = u16::try_from(take_varint(bytes, 3)?).ok()?;
        Some(Self {
            blob_file_id,
            frame,
            index,
        })
    }
}

impl Decode for ValueHandle {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, crate::Error> {
        let blob_file_id = reader.read_u64_varint()?;
        let frame = reader.read_u32_varint()?;
        let index = reader.read_u16_varint()?;

        Ok(Self {
            blob_file_id,
            frame,
            index,
        })
    }
}

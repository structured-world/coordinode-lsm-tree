// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

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

/// A value handle points into the value log
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct ValueHandle {
    /// Blob file ID
    pub blob_file_id: BlobFileId,

    /// Offset in file
    pub offset: u64,

    /// On-disk size
    pub on_disk_size: u32,
}

impl Encode for ValueHandle {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), crate::Error> {
        writer.write_u64_varint(self.offset)?;
        writer.write_u64_varint(self.blob_file_id)?;
        writer.write_u32_varint(self.on_disk_size)?;
        Ok(())
    }
}

impl Decode for ValueHandle {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, crate::Error> {
        let offset = reader.read_u64_varint()?;
        let blob_file_id = reader.read_u64_varint()?;
        let on_disk_size = reader.read_u32_varint()?;

        Ok(Self {
            blob_file_id,
            offset,
            on_disk_size,
        })
    }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

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

#[derive(Copy, Clone, Debug)]
pub struct BlobIndirection {
    pub(crate) vhandle: ValueHandle,
    pub(crate) size: u32,
}

impl Encode for BlobIndirection {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), crate::Error> {
        self.vhandle.encode_into(writer)?;
        writer.write_u32_varint(self.size)?;
        Ok(())
    }
}

impl Decode for BlobIndirection {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, crate::Error> {
        let vhandle = ValueHandle::decode_from(reader)?;
        let size = reader.read_u32_varint()?;
        Ok(Self { vhandle, size })
    }
}

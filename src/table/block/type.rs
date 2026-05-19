// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BlockType {
    Data,
    Index,
    Filter,
    Meta,
    RangeTombstone,
}

impl From<BlockType> for u8 {
    fn from(val: BlockType) -> Self {
        match val {
            BlockType::Data => 0,
            BlockType::Index => 1,
            BlockType::Filter => 2,
            BlockType::Meta => 3,
            BlockType::RangeTombstone => 4,
        }
    }
}

impl TryFrom<u8> for BlockType {
    type Error = crate::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Data),
            1 => Ok(Self::Index),
            2 => Ok(Self::Filter),
            3 => Ok(Self::Meta),
            4 => Ok(Self::RangeTombstone),
            _ => Err(crate::Error::InvalidTag(("BlockType", value))),
        }
    }
}

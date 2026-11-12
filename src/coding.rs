// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use crate::io::{Read, Write};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// Trait to serialize stuff
pub trait Encode {
    /// Serializes into writer.
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), crate::Error>;

    /// Serializes into vector.
    fn encode_into_vec(&self) -> Vec<u8> {
        let mut v = vec![];
        #[expect(
            clippy::expect_used,
            reason = "encoding into a vec is not expected to fail"
        )]
        self.encode_into(&mut v).expect("cannot fail");
        v
    }
}

/// Trait to deserialize stuff
pub trait Decode {
    /// Deserializes from reader.
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, crate::Error>
    where
        Self: Sized;
}

/// The longest varint a `u64` field takes.
pub(crate) const VAR_U64_MAX_LEN: usize = 10;

/// Takes a LEB128 varint of at most `max_len` bytes off the front of `bytes`;
/// `None` when it is truncated, runs longer, or carries bits past a `u64`.
#[inline]
pub(crate) fn take_varint(bytes: &mut &[u8], max_len: usize) -> Option<u64> {
    let mut value = 0u64;
    for (index, &byte) in bytes.iter().take(max_len.min(VAR_U64_MAX_LEN)).enumerate() {
        // The tenth byte holds bit 63 alone; anything above it names a value
        // no `u64` has, which only a damaged or forged varint writes.
        if index == VAR_U64_MAX_LEN - 1 && byte > 1 {
            return None;
        }
        value |= u64::from(byte & 0x7f) << (7 * index);
        if byte & 0x80 == 0 {
            *bytes = bytes.get(index + 1..)?;
            return Some(value);
        }
    }
    None
}

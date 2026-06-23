// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

//! The hash index is a compact (typically <=1 byte per KV) index
//! embeddeded into a block to speed up point reads.
//!
//! The index is initialized with `hash_ratio * item_count` buckets.
//!
//! Each bucket is initialized as 254 (`FREE`).
//!
//! During block building, each key is hashed into a bucket.
//! If the bucket is FREE, it is set to the index of the binary index pointer
//! pointing to the item's restart interval.
//!
//! If the given bucket is already non-`FREE`, it is set to `CONFLICT`.
//!
//! During a point read, `CONFLICT`ed buckets are skipped, and the binary index
//! is consulted instead.

mod builder;
mod reader;

pub use builder::{Builder, MAX_POINTERS_FOR_HASH_INDEX};
pub use reader::Reader;

pub(crate) const MARKER_FREE: u8 = u8::MAX - 1; // 254
pub(crate) const MARKER_CONFLICT: u8 = u8::MAX; // 255

/// Calculates the bucket index for the given key.
#[expect(
    clippy::cast_possible_truncation,
    reason = "the hash index has a bucket count <= u32"
)]
fn calculate_bucket_position(key: &[u8], bucket_count: u32) -> usize {
    use crate::hash::hash64;

    let hash = hash64(key);

    (hash % u64::from(bucket_count)) as usize
}

#[cfg(test)]
mod tests;

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use crate::{SeqNo, UserKey, table::BlockOffset};

pub struct Metadata {
    /// Written data block count
    pub data_block_count: usize,

    /// Written item count
    pub item_count: usize,

    /// Tombstone count
    pub tombstone_count: usize,

    /// Weak tombstone (single delete) count
    pub weak_tombstone_count: usize,

    /// Weak tombstone + value pairs that become reclaimable when GC watermark advances
    pub weak_tombstone_reclaimable_count: usize,

    /// Written key count (unique keys)
    pub key_count: usize,

    /// Hashes the table's filter holds, set when the filter is written; zero
    /// without one.
    pub filter_hashes: u64,

    /// Hashes of the filter's largest partition, set when a partitioned
    /// filter is written; zero for a full filter or none.
    pub filter_partition_hashes: u64,

    /// Sum of user-key byte lengths across all written entries (every version,
    /// pairs with `item_count`). Drives the average-entry-shape introspection.
    pub sum_user_key_bytes: u64,

    /// Sum of value byte lengths across all written entries (every version,
    /// pairs with `item_count`).
    pub sum_value_bytes: u64,

    /// Current file position of writer
    pub file_pos: BlockOffset,

    /// Only takes user data into account
    pub uncompressed_size: u64,

    /// First encountered key
    pub first_key: Option<UserKey>,

    /// Last encountered key
    pub last_key: Option<UserKey>,

    /// Lowest encountered seqno
    pub lowest_seqno: SeqNo,

    /// Highest encountered seqno (includes both KV and RT)
    pub highest_seqno: SeqNo,

    /// Highest encountered seqno from KV entries only (excludes range tombstones).
    ///
    /// Used for table-skip decisions: a covering RT stored in the same table
    /// can now trigger skip because `rt.seqno > highest_kv_seqno` may be true
    /// even when `rt.seqno <= highest_seqno`.
    pub highest_kv_seqno: SeqNo,
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            data_block_count: 0,

            item_count: 0,
            tombstone_count: 0,
            weak_tombstone_count: 0,
            weak_tombstone_reclaimable_count: 0,
            key_count: 0,
            filter_hashes: 0,
            filter_partition_hashes: 0,
            sum_user_key_bytes: 0,
            sum_value_bytes: 0,
            file_pos: BlockOffset(0),
            uncompressed_size: 0,

            first_key: None,
            last_key: None,

            lowest_seqno: SeqNo::MAX,
            highest_seqno: 0,
            highest_kv_seqno: 0,
        }
    }
}

// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::table::{Block, filter::ribbon::burr::contains_hash_from_bytes};

#[derive(Clone)]
pub struct FilterBlock(Block);

impl FilterBlock {
    #[must_use]
    pub fn new(block: Block) -> Self {
        Self(block)
    }

    pub fn maybe_contains_hash(&self, hash: u64) -> crate::Result<bool> {
        // Single-pass parse + probe — no per-call heap allocation. The
        // alternative `BurrFilterReader::new(bytes)?.contains_hash(hash)`
        // builds a `Vec<LayerView>` inside `wire::decode`; we are on
        // the table read hot path (`Table::check_bloom` calls this per
        // candidate table) so amortising that allocation matters.
        contains_hash_from_bytes(&self.0.data, hash)
    }

    /// Returns the block size in bytes.
    #[must_use]
    pub fn size(&self) -> usize {
        self.0.size()
    }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::table::{Block, filter::ribbon::burr::contains_hash_from_bytes};

#[derive(Clone)]
pub struct FilterBlock(Block);

impl FilterBlock {
    #[must_use]
    pub fn new(block: Block) -> Self {
        Self(block)
    }

    pub fn maybe_contains_hash(&self, hash: u64) -> crate::Result<bool> {
        // Empty payload is the "no filter installed" sentinel produced
        // by build_burr_filter_bytes for empty key sets and by
        // BurrFilter::to_wire_bytes for zero-layer filters. Probing
        // such a buffer must report Ok(true) (permissive) so the
        // caller falls through to the data block lookup; forwarding
        // it to contains_hash_from_bytes would fail the magic check
        // and surface InvalidHeader on every read of a filter-less
        // partition.
        if self.0.data.is_empty() {
            return Ok(true);
        }
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

    /// Whether the decoded payload is the empty "no filter installed" sentinel
    /// (see [`maybe_contains_hash`](Self::maybe_contains_hash)). The read path
    /// treats this permissively, but a verifier deciding whether to trust the
    /// bytes must distinguish it from a real filter — a present-but-empty full
    /// filter on a table with keys is anomalous (the writer omits the section
    /// rather than emitting an empty one).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.data.is_empty()
    }
}

#[cfg(test)]
mod tests;

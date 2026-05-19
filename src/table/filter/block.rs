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

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;
    use crate::Slice;
    use crate::checksum::Checksum;
    use crate::table::Block;
    use crate::table::block::{BlockType, Header};

    fn empty_filter_block() -> FilterBlock {
        // Sentinel "no filter" payload: empty data slice. Matches what
        // build_burr_filter_bytes returns for an empty key set and what
        // BurrFilter::to_wire_bytes returns for a zero-layer filter.
        let block = Block {
            header: Header {
                block_type: BlockType::Filter,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
            data: Slice::empty(),
        };
        FilterBlock::new(block)
    }

    #[test]
    fn maybe_contains_hash_empty_payload_returns_true() {
        // Empty payload is the sentinel for "no filter installed for this
        // table" — probes must report Ok(true) (permissive) so the caller
        // falls through to the actual data block lookup. Forwarding the
        // empty buffer to contains_hash_from_bytes returns InvalidHeader,
        // which turns every read on a filter-less partition into a hard
        // error.
        let fb = empty_filter_block();
        let result = fb.maybe_contains_hash(0xDEAD_BEEF_CAFE_F00D);
        assert!(
            matches!(result, Ok(true)),
            "expected Ok(true) for empty filter payload, got {result:?}",
        );
    }
}

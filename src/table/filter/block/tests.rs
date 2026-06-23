use super::*;
use crate::Slice;
use crate::table::Block;
use crate::table::block::{BlockType, Header};

fn empty_filter_block() -> FilterBlock {
    // Sentinel "no filter" payload: empty data slice. Matches what
    // build_burr_filter_bytes returns for an empty key set and what
    // BurrFilter::to_wire_bytes returns for a zero-layer filter.
    let block = Block {
        header: Header::test_dummy(BlockType::Filter),
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

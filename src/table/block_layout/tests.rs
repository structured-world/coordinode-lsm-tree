use super::*;

#[test]
fn encode_decode_roundtrips_entries() {
    let layouts = vec![
        (BlockOffset(0), vec![100, 250, 400]),
        (BlockOffset(512), vec![80, 160]),
    ];
    let mut buf = Vec::new();
    encode_block_layouts(&mut buf, &layouts);
    let map = BlockLayoutMap::decode(&buf).expect("decode");
    assert_eq!(map.len(), 2);
    // `ends_for` is only compiled for the zstd partial-decode read path.
    #[cfg(feature = "zstd")]
    {
        assert_eq!(map.ends_for(0), Some([100u32, 250, 400].as_slice()));
        assert_eq!(map.ends_for(512), Some([80u32, 160].as_slice()));
        assert_eq!(map.ends_for(999), None, "unrecorded offset → None");
    }
}

#[test]
fn decode_rejects_inner_count_less_than_two() {
    // A recorded block must always have >= 2 inner blocks; a hand-crafted
    // payload with inner_count = 1 must be rejected (corruption guard).
    let mut buf = Vec::new();
    buf.extend_from_slice(&1u32.to_le_bytes()); // entry_count = 1
    buf.extend_from_slice(&0u64.to_le_bytes()); // block_offset = 0
    buf.extend_from_slice(&1u32.to_le_bytes()); // inner_count = 1 (invalid)
    buf.extend_from_slice(&100u32.to_le_bytes()); // one end offset
    assert!(
        matches!(
            BlockLayoutMap::decode(&buf),
            Err(crate::Error::InvalidHeader("BlockLayout"))
        ),
        "inner_count < 2 must surface as InvalidHeader(\"BlockLayout\")",
    );
}

#[test]
fn decode_rejects_non_ascending_offsets() {
    let layouts = vec![
        (BlockOffset(512), vec![80, 160]),
        (BlockOffset(0), vec![100, 250]),
    ];
    let mut buf = Vec::new();
    encode_block_layouts(&mut buf, &layouts);
    assert!(
        BlockLayoutMap::decode(&buf).is_err(),
        "non-ascending offsets must be rejected",
    );
}

#[test]
fn empty_layouts_decode_to_empty_map() {
    let mut buf = Vec::new();
    encode_block_layouts(&mut buf, &[]);
    let map = BlockLayoutMap::decode(&buf).expect("decode empty");
    assert_eq!(map.len(), 0);
    #[cfg(feature = "zstd")]
    assert_eq!(map.ends_for(0), None);
}

#[test]
fn map_byte_range_single_block_subset() {
    // ends: block0=[0,100) block1=[100,250) block2=[250,400)
    let ends = [100u32, 250, 400];
    // Range fully inside block 0 → [0, 1).
    assert_eq!(map_byte_range_to_blocks(&ends, 10, 50), Some((0, 1)));
    // Range inside block 1 → [1, 2).
    assert_eq!(map_byte_range_to_blocks(&ends, 120, 200), Some((1, 2)));
    // Range spanning block 0 and 1 → [0, 2).
    assert_eq!(map_byte_range_to_blocks(&ends, 50, 150), Some((0, 2)));
    // Range reaching the tail → [1, 3).
    assert_eq!(map_byte_range_to_blocks(&ends, 120, 400), Some((1, 3)));
    // Whole block → [0, 3).
    assert_eq!(map_byte_range_to_blocks(&ends, 0, 400), Some((0, 3)));
}

#[test]
fn map_byte_range_past_end_returns_none() {
    let ends = [100u32, 250, 400];
    assert_eq!(map_byte_range_to_blocks(&ends, 400, 500), None);
    assert_eq!(map_byte_range_to_blocks(&[], 0, 10), None);
}

#[test]
fn map_byte_range_boundary_is_exclusive_end() {
    // upper exactly on a block boundary must NOT pull in the next block.
    let ends = [100u32, 250, 400];
    // [0, 100) is exactly block 0.
    assert_eq!(map_byte_range_to_blocks(&ends, 0, 100), Some((0, 1)));
    // [100, 250) is exactly block 1.
    assert_eq!(map_byte_range_to_blocks(&ends, 100, 250), Some((1, 2)));
}

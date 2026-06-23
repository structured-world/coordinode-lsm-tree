use super::*;

#[test]
fn encode_decode_round_trips() {
    let bounds = [
        (BlockOffset(0), (10u64, 20u64)),
        (BlockOffset(4096), (5, 5)),
        (BlockOffset(9000), (0, 1_000_000)),
    ];
    let mut buf = Vec::new();
    encode_seqno_bounds(&mut buf, &bounds).expect("encode");
    let map = SeqnoBoundsMap::decode(&buf).expect("decode");
    assert_eq!(map.len(), 3);
    assert_eq!(map.bounds_for(0), Some((10, 20)));
    assert_eq!(map.bounds_for(4096), Some((5, 5)));
    assert_eq!(map.bounds_for(9000), Some((0, 1_000_000)));
    // An offset not present (e.g. between recorded blocks) returns None.
    assert_eq!(map.bounds_for(1), None);
    assert_eq!(map.bounds_for(5000), None);
}

#[test]
fn decode_empty_section_is_empty_map() {
    let mut buf = Vec::new();
    encode_seqno_bounds(&mut buf, &[]).expect("encode");
    let map = SeqnoBoundsMap::decode(&buf).expect("decode empty");
    assert!(map.is_empty());
    assert_eq!(map.bounds_for(0), None);
}

#[test]
fn decode_rejects_non_ascending_offsets() {
    // count=2 with offsets 100 then 50 (descending) must be rejected.
    let mut buf = Vec::new();
    buf.extend_from_slice(&2u32.to_le_bytes());
    for (off, lo, hi) in [(100u64, 1u64, 2u64), (50, 1, 2)] {
        buf.extend_from_slice(&off.to_le_bytes());
        buf.extend_from_slice(&lo.to_le_bytes());
        buf.extend_from_slice(&hi.to_le_bytes());
    }
    assert!(SeqnoBoundsMap::decode(&buf).is_err());
}

#[test]
fn decode_rejects_inverted_bounds() {
    let mut buf = Vec::new();
    buf.extend_from_slice(&1u32.to_le_bytes());
    buf.extend_from_slice(&0u64.to_le_bytes());
    buf.extend_from_slice(&9u64.to_le_bytes()); // min
    buf.extend_from_slice(&3u64.to_le_bytes()); // max < min
    assert!(SeqnoBoundsMap::decode(&buf).is_err());
}

#[test]
fn decode_rejects_truncated_payload() {
    // count=1 but no entry bytes.
    let buf = 1u32.to_le_bytes().to_vec();
    assert!(SeqnoBoundsMap::decode(&buf).is_err());
}

#[test]
fn decode_rejects_trailing_bytes_after_last_entry() {
    // A section with bytes past the declared entries must be rejected, not
    // silently parsed with the wrong length: leftover data means a wrong
    // count or a corrupt / padded section, so it must surface as an error.
    let mut buf = Vec::new();
    encode_seqno_bounds(&mut buf, &[(BlockOffset(0), (1u64, 2u64))]).expect("encode");
    buf.push(0xAB); // one stray trailing byte
    assert!(SeqnoBoundsMap::decode(&buf).is_err());
}

#[test]
fn decode_rejects_count_larger_than_payload() {
    // A corrupt count must be rejected up front, before any speculative
    // allocation sized by that count: each entry is exactly 24 bytes, so a
    // count claiming far more entries than the payload can hold is invalid.
    // (The pathological multi-GB count that would OOM the old `with_capacity`
    // cannot itself be unit-tested without risking an allocator abort; this
    // guards the same contract with a count the decoder must reject fast.)
    let mut buf = Vec::new();
    buf.extend_from_slice(&100_000u32.to_le_bytes()); // claims 100k entries
    buf.extend_from_slice(&0u64.to_le_bytes()); // ...but only 8 stray bytes
    assert!(SeqnoBoundsMap::decode(&buf).is_err());
}

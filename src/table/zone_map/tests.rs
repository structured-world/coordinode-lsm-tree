use super::*;

fn col(id: u32, min: &[u8], max: &[u8], nulls: u32, rows: u32) -> ColumnStats {
    ColumnStats {
        column_id: id,
        type_tag: 1,
        codec_id: 0,
        null_count: nulls,
        row_count: rows,
        min: min.to_vec(),
        max: max.to_vec(),
    }
}

fn sample() -> Vec<(BlockOffset, Vec<ColumnStats>)> {
    vec![
        (BlockOffset(0), vec![col(0, b"aaa", b"mmm", 0, 100)]),
        (
            BlockOffset(4096),
            vec![col(0, b"n", b"z", 2, 50), col(1, &[], b"\xff\xff", 5, 50)],
        ),
        (BlockOffset(9000), vec![col(0, b"", b"", 0, 1)]),
    ]
}

#[test]
fn encode_decode_round_trips() {
    let blocks = sample();
    let mut buf = Vec::new();
    encode_zone_map(&mut buf, &blocks).expect("encode");
    let map = ZoneMap::decode(&buf).expect("decode");
    assert_eq!(map.len(), 3);
    for (offset, columns) in &blocks {
        assert_eq!(map.columns_for(offset.0), Some(columns.as_slice()));
    }
    // Whole-block synthetic column (row block) carries the value range.
    let first = map.columns_for(0).expect("present");
    let c = first.first().expect("one column");
    assert_eq!(c.column_id, 0);
    assert_eq!(c.min, b"aaa");
    assert_eq!(c.max, b"mmm");
    assert_eq!(c.row_count, 100);
    // An offset between recorded blocks is absent.
    assert_eq!(map.columns_for(1), None);
    assert_eq!(map.columns_for(5000), None);
}

#[test]
fn empty_section_round_trips() {
    let mut buf = Vec::new();
    encode_zone_map(&mut buf, &[]).expect("encode");
    let map = ZoneMap::decode(&buf).expect("decode");
    assert!(map.is_empty());
    assert_eq!(map.columns_for(0), None);
}

#[test]
fn cursor_matches_binary_search_forward_and_backward() {
    let blocks = sample();
    let mut buf = Vec::new();
    encode_zone_map(&mut buf, &blocks).expect("encode");
    let map = ZoneMap::decode(&buf).expect("decode");

    // Forward in-order iteration: cursor agrees with the binary-search lookup.
    let mut cur = map.cursor();
    for off in [0u64, 4096, 9000] {
        assert_eq!(cur.columns_for(off), map.columns_for(off));
    }
    // A missed offset advances without matching, then the next real one hits.
    let mut cur = map.cursor();
    assert_eq!(cur.columns_for(100), None);
    assert_eq!(cur.columns_for(4096), map.columns_for(4096));
    // Backward jump falls back to binary search.
    assert_eq!(cur.columns_for(0), map.columns_for(0));
}

#[test]
fn decode_rejects_non_ascending_offsets() {
    // Two entries with descending offsets must be rejected.
    let blocks = vec![
        (BlockOffset(9000), vec![col(0, b"a", b"b", 0, 1)]),
        (BlockOffset(0), vec![col(0, b"a", b"b", 0, 1)]),
    ];
    let mut buf = Vec::new();
    encode_zone_map(&mut buf, &blocks).expect("encode");
    assert!(ZoneMap::decode(&buf).is_err());
}

#[test]
fn decode_rejects_trailing_bytes() {
    let blocks = sample();
    let mut buf = Vec::new();
    encode_zone_map(&mut buf, &blocks).expect("encode");
    buf.push(0xAB); // one stray byte past the declared count
    assert!(ZoneMap::decode(&buf).is_err());
}

#[test]
fn decode_rejects_truncated_payload() {
    let blocks = sample();
    let mut buf = Vec::new();
    encode_zone_map(&mut buf, &blocks).expect("encode");
    buf.truncate(buf.len() - 1); // drop a byte from the last max field
    assert!(ZoneMap::decode(&buf).is_err());
}

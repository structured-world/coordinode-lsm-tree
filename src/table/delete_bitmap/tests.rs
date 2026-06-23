use super::*;

/// Asserts that decoding `bytes` fails with exactly the expected
/// `InvalidHeader` message, proving the intended validation path fired rather
/// than some other error a generic `is_err()` would also accept.
fn assert_decode_rejects(bytes: &[u8], expected: &str) {
    match DeleteBitmap::decode(bytes) {
        Err(Error::InvalidHeader(msg)) => {
            assert_eq!(msg, expected, "decode failed on the wrong validation path");
        }
        other => panic!("expected InvalidHeader({expected:?}), got {other:?}"),
    }
}

#[test]
fn insert_and_contains_across_chunks() {
    let mut dv = DeleteBitmap::new();
    assert!(dv.is_empty());
    assert!(dv.insert(0));
    assert!(dv.insert(CHUNK_ROWS - 1)); // last row of chunk 0
    assert!(dv.insert(CHUNK_ROWS)); // first row of chunk 1
    assert!(dv.insert(100_000));
    assert!(!dv.insert(0)); // already present

    assert!(dv.contains(0));
    assert!(dv.contains(CHUNK_ROWS - 1));
    assert!(dv.contains(CHUNK_ROWS));
    assert!(dv.contains(100_000));
    assert!(!dv.contains(1));
    assert!(!dv.contains(CHUNK_ROWS + 1));
    assert_eq!(dv.len(), 4);
    assert!(!dv.is_empty());
}

#[test]
fn sparse_promotes_to_dense_and_stays_correct() {
    let mut dv = DeleteBitmap::new();
    // Fill one chunk past the sparse-to-dense threshold with even rows.
    let n = (SPARSE_MAX as u32 + 50).min(CHUNK_ROWS / 2);
    for i in 0..n {
        assert!(dv.insert(i * 2));
    }
    assert_eq!(dv.len(), u64::from(n));
    for i in 0..n {
        assert!(dv.contains(i * 2), "even row {} missing", i * 2);
        assert!(!dv.contains(i * 2 + 1), "odd row {} present", i * 2 + 1);
    }
}

#[test]
fn chunk_has_deletes_enables_block_skip() {
    let mut dv = DeleteBitmap::new();
    dv.insert(3 * CHUNK_ROWS + 7);
    assert!(!dv.chunk_has_deletes(0));
    assert!(!dv.chunk_has_deletes(2));
    assert!(dv.chunk_has_deletes(3));
    assert!(!dv.chunk_has_deletes(4));
}

#[test]
fn full_chunk_bulk_delete_is_dense_and_compact() {
    // A bulk delete of a whole chunk is O(1) bitmap work: the chunk collapses
    // to a single dense bitset (O(1) space, not O(rows) sparse offsets) and is
    // skipped / queried in O(1).
    let mut dv = DeleteBitmap::new();
    for off in 0..CHUNK_ROWS {
        dv.insert(CHUNK_ROWS + off);
    }
    assert_eq!(
        dv.len(),
        u64::from(CHUNK_ROWS),
        "every row of the chunk marked"
    );
    assert!(dv.chunk_has_deletes(1), "the full chunk reports deletes");
    assert!(
        !dv.chunk_has_deletes(0),
        "an untouched neighbour skips in O(1)"
    );
    assert!(!dv.chunk_has_deletes(2));
    // Boundary rows present; their neighbours in adjacent chunks are not.
    assert!(dv.contains(CHUNK_ROWS));
    assert!(dv.contains(2 * CHUNK_ROWS - 1));
    assert!(!dv.contains(CHUNK_ROWS - 1));
    assert!(!dv.contains(2 * CHUNK_ROWS));
    // Dense storage: a full chunk encodes to one bounded bitset, far below the
    // ~2 bytes/row a sparse array of CHUNK_ROWS offsets would need.
    let encoded = dv.encode();
    assert!(
        encoded.len() < CHUNK_ROWS as usize,
        "a full chunk must store densely (O(1) space), got {} bytes for {CHUNK_ROWS} rows",
        encoded.len(),
    );
}

#[test]
fn union_is_set_union() {
    let mut a = DeleteBitmap::new();
    a.insert(1);
    a.insert(CHUNK_ROWS + 1);
    let mut b = DeleteBitmap::new();
    b.insert(1); // overlap
    b.insert(2);
    b.insert(5 * CHUNK_ROWS); // new chunk
    a.union(&b);

    for row in [1, 2, CHUNK_ROWS + 1, 5 * CHUNK_ROWS] {
        assert!(a.contains(row), "row {row} missing after union");
    }
    assert_eq!(a.len(), 4);
}

#[test]
fn iter_yields_ascending_rows() {
    let mut dv = DeleteBitmap::new();
    let rows = [9_u32, 1, CHUNK_ROWS + 3, 2, CHUNK_ROWS];
    for &r in &rows {
        dv.insert(r);
    }
    let got: Vec<u32> = dv.iter().collect();
    assert_eq!(got, [1, 2, 9, CHUNK_ROWS, CHUNK_ROWS + 3]);
}

#[test]
fn encode_decode_round_trip_sparse_and_dense() {
    let mut dv = DeleteBitmap::new();
    // Sparse chunk.
    dv.insert(5);
    dv.insert(900);
    // Dense chunk (cross the threshold).
    for i in 0..=(SPARSE_MAX as u32) {
        dv.insert(CHUNK_ROWS + i);
    }
    let bytes = dv.encode();
    let decoded = DeleteBitmap::decode(&bytes).unwrap();
    assert_eq!(decoded, dv);
}

#[test]
fn decode_empty_round_trips() {
    let dv = DeleteBitmap::new();
    let decoded = DeleteBitmap::decode(&dv.encode()).unwrap();
    assert!(decoded.is_empty());
    assert_eq!(decoded, dv);
}

#[test]
fn decode_truncated_buffer_errors() {
    let mut dv = DeleteBitmap::new();
    dv.insert(7);
    let bytes = dv.encode();
    assert!(DeleteBitmap::decode(&bytes[..bytes.len() - 1]).is_err());
    assert!(DeleteBitmap::decode(&[]).is_err());
}

#[test]
fn decode_rejects_unknown_kind() {
    // 1 chunk, index 0, kind 99. Pad to the minimum per-chunk length so the
    // payload-size guard passes and the unknown-kind check is what fires.
    let bytes = [1, 0, 0, 0, 0, 0, 0, 0, 99, 0, 0];
    assert_decode_rejects(&bytes, "delete_bitmap: unknown container kind");
}

#[test]
fn decode_rejects_out_of_range_offset() {
    // 1 chunk, index 0, sparse, count 1, offset = CHUNK_ROWS (out of range).
    let mut bytes = alloc::vec![1, 0, 0, 0, 0, 0, 0, 0, KIND_SPARSE, 1, 0];
    bytes.extend_from_slice(&(CHUNK_ROWS as u16).to_le_bytes());
    assert_decode_rejects(&bytes, "delete_bitmap: sparse offset out of range");
}

#[test]
fn decode_rejects_non_ascending_chunks() {
    // 2 chunks both index 0 (not strictly ascending). Each is a non-empty
    // sparse chunk (count 1, offset 0) so the count check passes and the
    // ascending check is the one that fires.
    let bytes = [
        2,
        0,
        0,
        0, // chunk_count = 2
        0,
        0,
        0,
        0,
        KIND_SPARSE,
        1,
        0,
        0,
        0, // chunk 0: count 1, offset 0
        0,
        0,
        0,
        0,
        KIND_SPARSE,
        1,
        0,
        0,
        0, // chunk 0 again
    ];
    assert_decode_rejects(
        &bytes,
        "delete_bitmap: chunk indices not strictly ascending",
    );
}

#[test]
fn decode_rejects_chunk_count_exceeding_payload() {
    // chunk_count claims a huge number of chunks but the buffer holds none,
    // so the header is rejected before any large allocation.
    let bytes = u32::MAX.to_le_bytes();
    assert_decode_rejects(&bytes, "delete_bitmap: chunk count exceeds encoded payload");
}

#[test]
fn decode_rejects_empty_sparse_container() {
    // 1 chunk, index 0, sparse, count 0 (an empty container `encode` never
    // emits).
    let bytes = [1, 0, 0, 0, 0, 0, 0, 0, KIND_SPARSE, 0, 0];
    assert_decode_rejects(&bytes, "delete_bitmap: sparse count out of range");
}

#[test]
fn decode_rejects_chunk_index_out_of_range() {
    // 1 chunk whose index * CHUNK_ROWS would overflow the u32 position space.
    let mut bytes = alloc::vec![1, 0, 0, 0];
    bytes.extend_from_slice(&u32::MAX.to_le_bytes()); // chunk index = u32::MAX
    bytes.extend_from_slice(&[KIND_SPARSE, 1, 0, 0, 0]);
    assert_decode_rejects(
        &bytes,
        "delete_bitmap: chunk index out of row-position range",
    );
}

#[test]
fn decode_rejects_trailing_bytes() {
    // A valid encoding with one extra byte appended must fail (the section
    // decodes exactly).
    let mut dv = DeleteBitmap::new();
    dv.insert(7);
    let mut bytes = dv.encode();
    bytes.push(0);
    assert_decode_rejects(&bytes, "delete_bitmap: trailing bytes after chunks");
}

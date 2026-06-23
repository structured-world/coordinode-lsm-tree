use super::*;
use crate::table::block::{Decodable, Encodable};

fn make_truncated_entry(shared_prefix_len: usize) -> Vec<u8> {
    let handle = KeyedBlockHandle::new(
        b"abcdef".to_vec().into(),
        0,
        BlockHandle::new(BlockOffset(0), 1),
    );
    let mut bytes = Vec::new();
    let mut state = BlockOffset(0);
    handle
        .encode_truncated_into(&mut bytes, &mut state, shared_prefix_len)
        .unwrap();
    bytes
}

fn make_full_entry() -> Vec<u8> {
    let handle = KeyedBlockHandle::new(
        b"abcdef".to_vec().into(),
        0,
        BlockHandle::new(BlockOffset(0), 1),
    );
    let mut bytes = Vec::new();
    let mut state = BlockOffset(0);
    handle.encode_full_into(&mut bytes, &mut state).unwrap();
    bytes
}

fn shared_prefix_len_offset(bytes: &[u8]) -> usize {
    let mut cursor = Cursor::new(bytes);
    let marker = cursor.read_u8().unwrap();
    assert_eq!(marker, 1);
    let _ = BlockHandle::decode_from(&mut cursor).unwrap();
    let _ = cursor.read_u64_varint().unwrap();
    usize::try_from(cursor.position()).unwrap()
}

fn rest_key_len_offset(bytes: &[u8]) -> usize {
    let mut cursor = Cursor::new(bytes);
    let marker = cursor.read_u8().unwrap();
    assert_eq!(marker, 1);
    let _ = BlockHandle::decode_from(&mut cursor).unwrap();
    let _ = cursor.read_u64_varint().unwrap();
    let _ = cursor.read_u16_varint().unwrap();
    usize::try_from(cursor.position()).unwrap()
}

fn full_key_len_offset(bytes: &[u8]) -> usize {
    let mut cursor = Cursor::new(bytes);
    let marker = cursor.read_u8().unwrap();
    assert_eq!(marker, 0);
    let _ = BlockHandle::decode_from(&mut cursor).unwrap();
    let _ = cursor.read_u64_varint().unwrap();
    usize::try_from(cursor.position()).unwrap()
}

#[test]
fn parse_full_rejects_restart_key_span_overlapping_trailer_region() {
    let mut bytes = make_full_entry();
    let offset = 16;
    let entries_end = offset + bytes.len();
    let key_len_pos = full_key_len_offset(&bytes);
    *bytes.get_mut(key_len_pos).unwrap() = 8;
    bytes.extend_from_slice(&[0u8; 32]);

    let mut cursor = Cursor::new(bytes.as_slice());
    let parsed = <KeyedBlockHandle as Decodable<IndexBlockParsedItem>>::parse_full(
        &mut cursor,
        offset,
        entries_end,
    );
    assert!(parsed.is_none());
}

#[test]
fn parse_truncated_rejects_suffix_len_beyond_remaining_bytes() {
    let mut bytes = make_truncated_entry(2);
    let rest_len_pos = rest_key_len_offset(&bytes);
    *bytes.get_mut(rest_len_pos).unwrap() = 100;
    let offset = 16;
    let entries_end = offset + bytes.len();
    let mut cursor = Cursor::new(bytes.as_slice());
    let parsed = <KeyedBlockHandle as Decodable<IndexBlockParsedItem>>::parse_truncated(
        &mut cursor,
        offset,
        12,
        16,
        entries_end,
    );
    assert!(parsed.is_none());
}

#[test]
fn parse_truncated_rejects_prefix_span_crossing_entry_boundary() {
    let mut bytes = make_truncated_entry(2);
    let shared_len_pos = shared_prefix_len_offset(&bytes);
    *bytes.get_mut(shared_len_pos).unwrap() = 100;
    let offset = 16;
    let entries_end = offset + bytes.len();
    let mut cursor = Cursor::new(bytes.as_slice());
    let parsed = <KeyedBlockHandle as Decodable<IndexBlockParsedItem>>::parse_truncated(
        &mut cursor,
        offset,
        13,
        16,
        entries_end,
    );
    assert!(parsed.is_none());
}

#[test]
fn parse_truncated_rejects_prefix_span_crossing_restart_key_boundary() {
    let mut bytes = make_truncated_entry(2);
    let shared_len_pos = shared_prefix_len_offset(&bytes);
    *bytes.get_mut(shared_len_pos).unwrap() = 7;
    let offset = 16;
    let entries_end = offset + bytes.len();
    let mut cursor = Cursor::new(bytes.as_slice());
    let parsed = <KeyedBlockHandle as Decodable<IndexBlockParsedItem>>::parse_truncated(
        &mut cursor,
        offset,
        8,
        14,
        entries_end,
    );
    assert!(parsed.is_none());
}

#[test]
fn parse_truncated_rejects_base_key_offset_past_entry_start() {
    let bytes = make_truncated_entry(1);
    let offset = 16;
    let entries_end = offset + bytes.len();
    let mut cursor = Cursor::new(bytes.as_slice());
    let parsed = <KeyedBlockHandle as Decodable<IndexBlockParsedItem>>::parse_truncated(
        &mut cursor,
        offset,
        17,
        16,
        entries_end,
    );
    assert!(parsed.is_none());
}

#[test]
fn parse_truncated_rejects_invalid_marker() {
    let mut bytes = make_truncated_entry(1);
    // 99 is outside the valid marker set {0 full, 1 truncated, 255 trailer},
    // so parse_truncated must reject it.
    let invalid_marker = 99u8;
    *bytes.get_mut(0).unwrap() = invalid_marker;
    let offset = 16;
    let entries_end = offset + bytes.len();
    let mut cursor = Cursor::new(bytes.as_slice());
    let parsed = <KeyedBlockHandle as Decodable<IndexBlockParsedItem>>::parse_truncated(
        &mut cursor,
        offset,
        12,
        16,
        entries_end,
    );
    assert!(parsed.is_none());
}

#[test]
fn parse_truncated_rejects_suffix_span_overlapping_trailer_region() {
    let mut bytes = make_truncated_entry(2);
    let offset = 16;
    let entries_end = offset + bytes.len();
    let rest_len_pos = rest_key_len_offset(&bytes);
    *bytes.get_mut(rest_len_pos).unwrap() = 6;
    bytes.extend_from_slice(&[0u8; 32]);

    let mut cursor = Cursor::new(bytes.as_slice());
    let parsed = <KeyedBlockHandle as Decodable<IndexBlockParsedItem>>::parse_truncated(
        &mut cursor,
        offset,
        12,
        16,
        entries_end,
    );
    assert!(parsed.is_none());
}

#[test]
fn parse_restart_key_rejects_truncated_entry_marker() {
    let bytes = make_truncated_entry(1);
    let mut cursor = Cursor::new(bytes.as_slice());
    let parsed = <KeyedBlockHandle as Decodable<IndexBlockParsedItem>>::parse_restart_key(
        &mut cursor,
        0,
        bytes.as_slice(),
        bytes.len(),
    );
    assert!(parsed.is_none());
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "stale inline seqno-bounds marker")]
fn parse_full_asserts_on_stale_inline_marker() {
    // Marker 2 was the pre-release inline seqno-bounds full entry. This
    // build never writes it and has no on-disk compat obligation, so a 2
    // must fire the debug assertion rather than silently ending iteration
    // like the trailer (the silent-stop a stale-format SST would otherwise
    // cause). Release builds compile the assert out and reject it as `None`;
    // genuine corruption is caught upstream by the index block checksum.
    let mut bytes = make_full_entry();
    *bytes.get_mut(0).unwrap() = 2;
    let entries_end = bytes.len();
    let mut cursor = Cursor::new(bytes.as_slice());
    let _ = <KeyedBlockHandle as Decodable<IndexBlockParsedItem>>::parse_full(
        &mut cursor,
        0,
        entries_end,
    );
}

#[test]
fn full_entry_encodes_with_legacy_marker() {
    // A full index entry encodes with marker 0; seqno bounds are never
    // inline (they live in the parallel `seqno_bounds` section).
    let bytes = make_full_entry();
    assert_eq!(bytes.first().copied(), Some(0));

    let mut cursor = Cursor::new(bytes.as_slice());
    let parsed = <KeyedBlockHandle as Decodable<IndexBlockParsedItem>>::parse_full(
        &mut cursor,
        0,
        bytes.len(),
    )
    .unwrap();
    assert_eq!(parsed.seqno, 0);
}

#[test]
fn parse_restart_key_rejects_oversized_block_handle_size() {
    // A restart head whose block-handle size varint encodes a value beyond
    // u32::MAX is corrupt: BlockHandle::size is u32. parse_restart_key must
    // reject it (like parse_full / parse_truncated validate their handle
    // fields) instead of skipping the field and feeding a forged entry into
    // restart-table navigation. Built by hand because the encoder cannot
    // emit an out-of-range size.
    let mut bytes = Vec::new();
    bytes.push(0u8); // marker 0 (legacy full restart head)
    bytes.push(0u8); // file offset varint = 0
    // size varint = u32::MAX + 1 = 4_294_967_296 (5-byte LEB128).
    bytes.extend_from_slice(&[0x80, 0x80, 0x80, 0x80, 0x10]);
    bytes.push(7u8); // seqno varint = 7
    bytes.push(6u8); // key_len u16 varint = 6
    bytes.extend_from_slice(b"abcdef"); // key

    let mut cursor = Cursor::new(bytes.as_slice());
    let parsed = <KeyedBlockHandle as Decodable<IndexBlockParsedItem>>::parse_restart_key(
        &mut cursor,
        0,
        bytes.as_slice(),
        bytes.len(),
    );
    assert!(
        parsed.is_none(),
        "block-handle size beyond u32::MAX must be rejected by parse_restart_key",
    );
}

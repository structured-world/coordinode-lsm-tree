use super::{DataBlockParsedItem, SeqnoCoding};
use crate::comparator::default_comparator;
use crate::io::ReadBytesExt;
use crate::{
    InternalValue, SeqNo, Slice,
    ValueType::{Tombstone, Value},
    table::{
        Block, DataBlock,
        block::{BlockType, Decodable, Encodable, Encoder, Header, ParsedItem},
    },
};
use std::io::{Cursor, Seek};
use test_log::test;
use varint_rs::VarintReader;

fn make_truncated_data_entry(shared_prefix_len: usize) -> Vec<u8> {
    let value = InternalValue::from_components("abcdef", "payload", 0, Value);
    let mut bytes = Vec::new();
    value
        .encode_truncated_into(&mut bytes, &mut SeqnoCoding::default(), shared_prefix_len)
        .expect("encoding test InternalValue into truncated form must succeed");
    bytes
}

fn make_full_data_entry() -> Vec<u8> {
    let value = InternalValue::from_components("abcdef", "payload", 0, Value);
    let mut bytes = Vec::new();
    value
        .encode_full_into(&mut bytes, &mut SeqnoCoding::default())
        .expect("encoding full test InternalValue must succeed");
    bytes
}

fn make_full_tombstone_entry() -> Vec<u8> {
    let value = InternalValue::from_components("abcdef", "", 0, Tombstone);
    let mut bytes = Vec::new();
    value
        .encode_full_into(&mut bytes, &mut SeqnoCoding::default())
        .expect("encoding full tombstone InternalValue must succeed");
    bytes
}

fn make_truncated_tombstone_entry(shared_prefix_len: usize) -> Vec<u8> {
    let value = InternalValue::from_components("abcdef", "", 0, Tombstone);
    let mut bytes = Vec::new();
    value
        .encode_truncated_into(&mut bytes, &mut SeqnoCoding::default(), shared_prefix_len)
        .expect("encoding tombstone InternalValue into truncated form must succeed");
    bytes
}

fn data_shared_prefix_len_offset(bytes: &[u8]) -> usize {
    let mut cursor = Cursor::new(bytes);
    let _value_type = cursor
        .read_u8()
        .expect("test fixture encoding must contain a value type byte");
    let _seqno = cursor
        .read_u64_varint()
        .expect("test fixture encoding must contain a seqno varint");
    usize::try_from(cursor.position())
        .expect("cursor position must fit into usize in test environment")
}

fn data_rest_key_len_offset(bytes: &[u8]) -> usize {
    let mut cursor = Cursor::new(bytes);
    let _value_type = cursor
        .read_u8()
        .expect("test fixture encoding must contain a value type byte");
    let _seqno = cursor
        .read_u64_varint()
        .expect("test fixture encoding must contain a seqno varint");
    let _shared_prefix_len = cursor
        .read_u16_varint()
        .expect("test fixture encoding must contain a shared-prefix varint");
    usize::try_from(cursor.position())
        .expect("cursor position must fit into usize in test environment")
}

fn data_value_len_offset(bytes: &[u8]) -> usize {
    let mut cursor = Cursor::new(bytes);
    let _value_type = cursor
        .read_u8()
        .expect("test fixture encoding must contain a value type byte");
    let _seqno = cursor
        .read_u64_varint()
        .expect("test fixture encoding must contain a seqno varint");
    let _shared_prefix_len = cursor
        .read_u16_varint()
        .expect("test fixture encoding must contain a shared-prefix varint");
    let rest_key_len: usize = cursor
        .read_u16_varint()
        .expect("test fixture encoding must contain a rest-key-length varint")
        .into();
    #[expect(
        clippy::cast_possible_wrap,
        reason = "rest_key_len is encoded as u16 in test fixture"
    )]
    cursor
        .seek_relative(rest_key_len as i64)
        .expect("rest key skip in test fixture should succeed");
    usize::try_from(cursor.position())
        .expect("cursor position must fit into usize in test environment")
}

fn data_full_key_len_offset(bytes: &[u8]) -> usize {
    let mut cursor = Cursor::new(bytes);
    let _value_type = cursor
        .read_u8()
        .expect("test fixture encoding must contain a value type byte");
    let _seqno = cursor
        .read_u64_varint()
        .expect("test fixture encoding must contain a seqno varint");
    usize::try_from(cursor.position())
        .expect("cursor position must fit into usize in test environment")
}

fn data_full_value_len_offset(bytes: &[u8]) -> usize {
    let mut cursor = Cursor::new(bytes);
    let _value_type = cursor
        .read_u8()
        .expect("test fixture encoding must contain a value type byte");
    let _seqno = cursor
        .read_u64_varint()
        .expect("test fixture encoding must contain a seqno varint");
    let key_len: usize = cursor
        .read_u16_varint()
        .expect("test fixture encoding must contain a key-length varint")
        .into();
    #[expect(
        clippy::cast_possible_wrap,
        reason = "key_len is encoded as u16 in test fixture"
    )]
    cursor
        .seek_relative(key_len as i64)
        .expect("key skip in test fixture should succeed");
    usize::try_from(cursor.position())
        .expect("cursor position must fit into usize in test environment")
}

#[test]
fn parse_full_rejects_restart_key_span_overlapping_trailer_region() {
    let mut bytes = make_full_tombstone_entry();
    let offset = 16;
    let entries_end = offset + bytes.len();
    let key_len_pos = data_full_key_len_offset(&bytes);
    *bytes
        .get_mut(key_len_pos)
        .expect("key_len_pos must point to an existing byte in test fixture") = 8;
    bytes.extend_from_slice(&[0u8; 32]);

    let mut cursor = Cursor::new(bytes.as_slice());
    let parsed = <InternalValue as Decodable<DataBlockParsedItem>>::parse_full(
        &mut cursor,
        offset,
        entries_end,
    );
    assert!(parsed.is_none());
}

#[test]
fn parse_full_rejects_restart_value_span_overlapping_trailer_region() {
    let mut bytes = make_full_data_entry();
    let offset = 16;
    let entries_end = offset + bytes.len();
    let val_len_pos = data_full_value_len_offset(&bytes);
    *bytes
        .get_mut(val_len_pos)
        .expect("val_len_pos must point to an existing byte in test fixture") = 8;
    bytes.extend_from_slice(&[0u8; 32]);

    let mut cursor = Cursor::new(bytes.as_slice());
    let parsed = <InternalValue as Decodable<DataBlockParsedItem>>::parse_full(
        &mut cursor,
        offset,
        entries_end,
    );
    assert!(parsed.is_none());
}

#[test]
fn parse_full_returns_none_for_unknown_value_type_byte() {
    let mut bytes = make_full_tombstone_entry();
    let value_type = bytes
        .get_mut(0)
        .expect("full entry fixture must contain value_type byte");
    *value_type = 5;

    let offset = 16;
    let entries_end = offset + bytes.len();
    let mut cursor = Cursor::new(bytes.as_slice());
    let parsed = <InternalValue as Decodable<DataBlockParsedItem>>::parse_full(
        &mut cursor,
        offset,
        entries_end,
    );
    assert!(parsed.is_none());
}

#[test]
fn parse_truncated_rejects_prefix_span_crossing_restart_key_boundary() {
    let mut bytes = make_truncated_data_entry(2);
    let shared_len_pos = data_shared_prefix_len_offset(&bytes);
    *bytes
        .get_mut(shared_len_pos)
        .expect("shared_len_pos must point to an existing byte in test fixture") = 7;

    let offset = 16;
    let entries_end = offset + bytes.len();
    let mut cursor = Cursor::new(bytes.as_slice());
    let parsed = <InternalValue as Decodable<DataBlockParsedItem>>::parse_truncated(
        &mut cursor,
        offset,
        8,
        14,
        Some(0),
        entries_end,
    );
    assert!(parsed.is_none());
}

#[test]
fn parse_truncated_returns_none_for_unknown_value_type_byte() {
    let mut bytes = make_truncated_tombstone_entry(2);
    let value_type = bytes
        .get_mut(0)
        .expect("truncated entry fixture must contain value_type byte");
    *value_type = 5;

    let offset = 16;
    let entries_end = offset + bytes.len();
    let mut cursor = Cursor::new(bytes.as_slice());
    let parsed = <InternalValue as Decodable<DataBlockParsedItem>>::parse_truncated(
        &mut cursor,
        offset,
        8,
        14,
        Some(0),
        entries_end,
    );
    assert!(parsed.is_none());
}

#[test]
fn parse_truncated_rejects_key_span_crossing_block_boundary() {
    let mut bytes = make_truncated_tombstone_entry(2);
    let rest_len_pos = data_rest_key_len_offset(&bytes);
    *bytes
        .get_mut(rest_len_pos)
        .expect("rest_len_pos must point to an existing byte in test fixture") = 64;

    let offset = 16;
    let entries_end = offset + bytes.len();
    let mut cursor = Cursor::new(bytes.as_slice());
    let parsed = <InternalValue as Decodable<DataBlockParsedItem>>::parse_truncated(
        &mut cursor,
        offset,
        8,
        14,
        Some(0),
        entries_end,
    );
    assert!(parsed.is_none());
}

#[test]
fn parse_truncated_rejects_value_span_crossing_block_boundary() {
    let mut bytes = make_truncated_data_entry(2);
    let val_len_pos = data_value_len_offset(&bytes);
    *bytes
        .get_mut(val_len_pos)
        .expect("val_len_pos must point to an existing byte in test fixture") = 127;

    let offset = 16;
    let entries_end = offset + bytes.len();
    let mut cursor = Cursor::new(bytes.as_slice());
    let parsed = <InternalValue as Decodable<DataBlockParsedItem>>::parse_truncated(
        &mut cursor,
        offset,
        8,
        14,
        Some(0),
        entries_end,
    );
    assert!(parsed.is_none());
}

#[test]
fn parse_truncated_rejects_key_span_overlapping_trailer_region() {
    let mut bytes = make_truncated_tombstone_entry(2);
    let offset = 16;
    let entries_end = offset + bytes.len();
    let rest_len_pos = data_rest_key_len_offset(&bytes);
    *bytes
        .get_mut(rest_len_pos)
        .expect("rest_len_pos must point to an existing byte in test fixture") = 6;
    bytes.extend_from_slice(&[0u8; 32]);

    let mut cursor = Cursor::new(bytes.as_slice());
    let parsed = <InternalValue as Decodable<DataBlockParsedItem>>::parse_truncated(
        &mut cursor,
        offset,
        8,
        14,
        Some(0),
        entries_end,
    );
    assert!(parsed.is_none());
}

#[test]
fn parse_truncated_rejects_value_span_overlapping_trailer_region() {
    let mut bytes = make_truncated_data_entry(2);
    let offset = 16;
    let entries_end = offset + bytes.len();
    let val_len_pos = data_value_len_offset(&bytes);
    *bytes
        .get_mut(val_len_pos)
        .expect("val_len_pos must point to an existing byte in test fixture") = 8;
    bytes.extend_from_slice(&[0u8; 32]);

    let mut cursor = Cursor::new(bytes.as_slice());
    let parsed = <InternalValue as Decodable<DataBlockParsedItem>>::parse_truncated(
        &mut cursor,
        offset,
        8,
        14,
        Some(0),
        entries_end,
    );
    assert!(parsed.is_none());
}

#[test]
fn kv_checked_block_round_trips_through_reader() -> crate::Result<()> {
    use crate::runtime_config::ChecksumAlgorithm;
    use crate::table::block::kv_checksum::kv_digest;

    // The write path (encode_kv_checked_into) followed by the read path
    // (from_loaded → try_iter) must recover the exact items: from_loaded
    // strips the per-KV footer and the standard decoder reads the inner
    // payload unchanged.
    let items = [
        InternalValue::from_components(b"alpha".to_vec(), b"first".to_vec(), 30, Value),
        InternalValue::from_components(b"bravo".to_vec(), b"second".to_vec(), 20, Value),
        InternalValue::from_components(b"delta".to_vec(), Slice::from([]), 10, Tombstone),
    ];
    let algo = ChecksumAlgorithm::Xxh3_64;
    let digests: Vec<u64> = items
        .iter()
        .map(|it| kv_digest(it, algo).expect("xxh3 always available"))
        .collect();

    let mut bytes = Vec::new();
    DataBlock::encode_kv_checked_into(&mut bytes, &items, &digests, algo, 2, 0.0)?;

    // The SST carries per-KV footers (descriptor → has_kv_footer=true), so
    // from_loaded strips the footer. Data block headers omit the
    // block_flags byte; footer presence is supplied out-of-band.
    let loaded = DataBlock::from_loaded(
        Block {
            data: bytes.into(),
            header: Header::test_dummy(BlockType::Data),
        },
        true,
    )?;
    assert_eq!(loaded.inner.header.block_type, BlockType::Data);
    assert_eq!(loaded.inner.header.block_flags, 0);

    let recovered: Vec<InternalValue> = loaded
        .try_iter(default_comparator())?
        .map(|parsed| parsed.materialize(loaded.as_slice()))
        .collect();
    assert_eq!(recovered.len(), items.len());
    for (got, want) in recovered.iter().zip(items.iter()) {
        assert_eq!(got.key.user_key, want.key.user_key);
        assert_eq!(got.key.seqno, want.key.seqno);
        assert_eq!(got.key.value_type, want.key.value_type);
        assert_eq!(got.value, want.value);
    }
    Ok(())
}

/// The stored seqno delta is the zigzag of the wrapping difference: a step
/// either way costs one small varint, and every pair round-trips, the ends of
/// the u64 range next to each other included.
#[test]
fn seqno_delta_round_trips_both_ways_and_across_the_range_ends() {
    use super::{seqno_delta, seqno_from_delta};

    assert_eq!(seqno_delta(10, 10), 0);
    assert_eq!(seqno_delta(9, 10), 1, "one older version");
    assert_eq!(seqno_delta(11, 10), 2, "one later write");
    let edges = [
        0,
        1,
        2,
        63,
        64,
        1 << 32,
        u64::MAX >> 1,
        u64::MAX - 1,
        u64::MAX,
    ];
    for head in edges {
        for seqno in edges {
            assert_eq!(
                seqno_from_delta(seqno_delta(seqno, head), head),
                seqno,
                "{seqno} under {head}"
            );
        }
    }
}

/// Items whose seqnos rise, fall and jump across the whole range inside one
/// restart interval: older versions of a key below its head (negative
/// deltas), later keys above it (positive), and a key whose versions sit at
/// both ends of the range (deltas that wrap).
fn mixed_seqno_items() -> Vec<InternalValue> {
    let rows: [(&str, SeqNo); 13] = [
        ("a", 100),
        ("a", 99),
        ("a", 7),
        ("b", 1_000_000),
        ("b", 0),
        ("c", SeqNo::MAX >> 1),
        ("c", 3),
        ("d", 5),
        ("e", 1 << 40),
        ("e", 1 << 39),
        ("e", 2),
        ("f", 0),
        ("g", 64),
    ];
    rows.into_iter()
        .map(|(key, seqno)| {
            if seqno % 3 == 0 {
                InternalValue::from_components(key, "", seqno, Tombstone)
            } else {
                InternalValue::from_components(key, format!("{key}@{seqno}"), seqno, Value)
            }
        })
        .collect()
}

/// A block over `items` with its seqno coding chosen by the caller rather
/// than by length, so both codings run on the same items.
fn block_with_coding(
    items: &[InternalValue],
    restart_interval: u8,
    deltas: bool,
) -> crate::Result<DataBlock> {
    let mut bytes = Vec::new();
    let mut encoder = Encoder::<'_, SeqnoCoding, InternalValue>::new(
        &mut bytes,
        items.len(),
        restart_interval,
        1.33,
        &items.first().expect("non-empty").key.user_key,
    );
    encoder.state = SeqnoCoding::new(deltas);
    for item in items {
        encoder.write(item)?;
    }
    encoder.finish()?;
    Ok(DataBlock::new(Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Data),
    }))
}

/// A block over those items reads every entry back with its seqno, forward
/// and backward, at every restart interval and in both seqno codings: each
/// delta decodes against the head of its own interval.
#[test]
fn seqnos_relative_to_the_restart_head_read_back_both_directions() -> crate::Result<()> {
    let items = mixed_seqno_items();
    for (restart_interval, deltas) in (1..=16).flat_map(|ri| [(ri, false), (ri, true)]) {
        let block = block_with_coding(&items, restart_interval, deltas)?;
        let forward: Vec<InternalValue> = block
            .iter(default_comparator())
            .map(|item| item.materialize(block.as_slice()))
            .collect();
        assert_eq!(
            forward, items,
            "forward, interval {restart_interval}, deltas {deltas}"
        );
        let backward: Vec<InternalValue> = block
            .iter(default_comparator())
            .rev()
            .map(|item| item.materialize(block.as_slice()))
            .collect();
        let mut reversed = items.clone();
        reversed.reverse();
        assert_eq!(
            backward, reversed,
            "backward, interval {restart_interval}, deltas {deltas}"
        );
    }
    Ok(())
}

/// A point read at any snapshot finds the version the snapshot sees, also
/// when that version is stored below its interval's head, and finds nothing
/// when the snapshot sees none.
#[test]
fn point_reads_see_the_right_version_under_seqno_deltas() -> crate::Result<()> {
    let items = mixed_seqno_items();
    let snapshots = [
        0,
        1,
        3,
        4,
        7,
        8,
        99,
        100,
        101,
        1_000_000,
        1_000_001,
        1 << 40,
    ];
    for (restart_interval, deltas) in [1, 2, 3, 4, 16]
        .into_iter()
        .flat_map(|ri| [(ri, false), (ri, true)])
    {
        let block = block_with_coding(&items, restart_interval, deltas)?;
        for key in ["a", "b", "c", "d", "e", "f", "g", "h"] {
            for snapshot in snapshots.into_iter().chain([SeqNo::MAX]) {
                let expected = items
                    .iter()
                    .find(|item| &*item.key.user_key == key.as_bytes() && item.key.seqno < snapshot)
                    .cloned();
                assert_eq!(
                    block.point_read(key.as_bytes(), snapshot, &default_comparator())?,
                    expected,
                    "{key} at {snapshot}, interval {restart_interval}, deltas {deltas}"
                );
            }
        }
    }
    Ok(())
}

/// Items whose keys ascend, one per seqno.
fn items_at(seqnos: &[SeqNo]) -> Vec<InternalValue> {
    seqnos
        .iter()
        .enumerate()
        .map(|(idx, &seqno)| {
            InternalValue::from_components(format!("k{idx:02}"), format!("v{idx}"), seqno, Value)
        })
        .collect()
}

/// Reads `block` forward, backward and by point reads, against `items`.
fn assert_block_reads_back(block: &DataBlock, items: &[InternalValue]) -> crate::Result<()> {
    let forward: Vec<InternalValue> = block
        .iter(default_comparator())
        .map(|item| item.materialize(block.as_slice()))
        .collect();
    assert_eq!(forward, items);
    let backward: Vec<InternalValue> = block
        .iter(default_comparator())
        .rev()
        .map(|item| item.materialize(block.as_slice()))
        .collect();
    let mut reversed = items.to_vec();
    reversed.reverse();
    assert_eq!(backward, reversed);
    for item in items {
        assert_eq!(
            block.point_read(&item.key.user_key, SeqNo::MAX, &default_comparator())?,
            Some(item.clone()),
        );
    }
    Ok(())
}

/// A block stores its seqnos in whichever form is shorter, and says which on
/// its first entry: a run of writes relative to the restart heads, keys
/// written at scattered times in full. Both read back in either direction and
/// under point reads.
#[test]
fn each_block_stores_seqnos_in_the_shorter_form() -> crate::Result<()> {
    use super::SEQNO_DELTAS;

    // Close to each other: deltas take a byte, full seqnos six.
    let run = items_at(&[
        (1 << 40) + 3,
        (1 << 40) + 4,
        (1 << 40) + 1,
        (1 << 40) + 9,
        (1 << 40) + 20,
        (1 << 40) + 22,
    ]);
    // Far from their heads: full seqnos take a byte, deltas six.
    let scattered = items_at(&[1 << 40, 3, 1, 2, 1 << 41, 5]);

    for (items, deltas) in [(run, true), (scattered, false)] {
        let bytes = DataBlock::encode_into_vec(&items, 3, 1.33)?;
        assert_eq!(
            bytes.first().copied().map(|byte| byte & SEQNO_DELTAS != 0),
            Some(deltas),
        );
        let block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header::test_dummy(BlockType::Data),
        });
        assert_block_reads_back(&block, &items)?;
    }
    Ok(())
}

/// The choice compares encoded lengths over every interval of the block and
/// keeps full seqnos on a tie: deltas of the same length as the seqnos
/// compress worse than them.
#[test]
fn block_coding_keeps_full_seqnos_unless_deltas_are_shorter() {
    use super::block_uses_deltas;

    // A single entry has nothing to store either way.
    assert!(!block_uses_deltas(&items_at(&[5]), 16));
    // A run of writes: one byte per delta against three per seqno.
    assert!(block_uses_deltas(
        &items_at(&[1_000_000, 1_000_001, 1_000_002, 1_000_003]),
        2
    ));
    // Both one byte: the tie keeps full seqnos.
    assert!(!block_uses_deltas(&items_at(&[10, 11, 12]), 16));
    // Seqnos spread over the range: a delta carries a sign bit on top of the
    // seqno's width, a byte more here.
    assert!(!block_uses_deltas(
        &items_at(&[1 << 20, (1 << 21) - 1, 1]),
        16
    ));
    // Each restart head starts afresh: a run per interval stays a run even
    // when the intervals sit far apart.
    assert!(block_uses_deltas(
        &items_at(&[1 << 40, (1 << 40) + 1, 7 << 50, (7 << 50) + 1]),
        2
    ));
}

/// The seqno-coding bit belongs to the block's first entry only: there it
/// parses as the value type it rides on, and on any later head or on a
/// truncated entry it is rejected like any other unknown type byte.
#[test]
fn seqno_coding_bit_is_read_on_the_first_entry_and_refused_elsewhere() {
    use super::SEQNO_DELTAS;

    let mut head = make_full_data_entry();
    *head.get_mut(0).expect("type byte") |= SEQNO_DELTAS;
    assert!(<InternalValue as Decodable<DataBlockParsedItem>>::seqno_deltas(&head));
    let parsed = <InternalValue as Decodable<DataBlockParsedItem>>::parse_full(
        &mut Cursor::new(head.as_slice()),
        0,
        head.len(),
    )
    .expect("a flagged first entry parses");
    assert_eq!(parsed.value_type, Value);

    let later = 16;
    assert!(
        <InternalValue as Decodable<DataBlockParsedItem>>::parse_full(
            &mut Cursor::new(head.as_slice()),
            later,
            later + head.len(),
        )
        .is_none()
    );

    let mut entry = make_truncated_data_entry(2);
    *entry.get_mut(0).expect("type byte") |= SEQNO_DELTAS;
    let offset = 16;
    let parsed = <InternalValue as Decodable<DataBlockParsedItem>>::parse_truncated(
        &mut Cursor::new(entry.as_slice()),
        offset,
        8,
        14,
        Some(0),
        offset + entry.len(),
    );
    assert!(parsed.is_none());
}

#[test]
fn point_read_at_slot_positions_by_restart_and_matches_point_read() -> crate::Result<()> {
    // restart_interval = 4 → 25 restart intervals over 100 sorted keys, so
    // the locator slot positioning is meaningfully exercised.
    let ri: u8 = 4;
    let items: Vec<InternalValue> = (0..100u32)
        .map(|i| {
            InternalValue::from_components(
                format!("key{i:04}").into_bytes(),
                format!("val{i}").into_bytes(),
                0,
                Value,
            )
        })
        .collect();
    let bytes = DataBlock::encode_into_vec(&items, ri, 0.0)?;
    let block = DataBlock::new(Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Data),
    });
    let cmp = default_comparator();

    for (idx, it) in items.iter().enumerate() {
        let key = &it.key.user_key;
        let expected = block.point_read(key, crate::SeqNo::MAX, &cmp)?;
        assert!(expected.is_some(), "key {idx} must be present");

        let restart_idx = (idx / usize::from(ri)) as u64;
        // Restart precision: slot IS the key's restart index → exact hit.
        assert_eq!(
            block.point_read_at_slot(restart_idx, false, key, crate::SeqNo::MAX, &cmp)?,
            expected,
            "restart-slot read mismatch at key {idx}",
        );
        // Entry precision: slot is the exact entry index, mapped to its
        // restart via restart_interval internally.
        assert_eq!(
            block.point_read_at_slot(idx as u64, true, key, crate::SeqNo::MAX, &cmp)?,
            expected,
            "entry-slot read mismatch at key {idx}",
        );
        // Restart 0 scans the whole block → still finds every key.
        assert_eq!(
            block.point_read_at_slot(0, false, key, crate::SeqNo::MAX, &cmp)?,
            expected,
            "restart-0 read mismatch at key {idx}",
        );
        // An out-of-range restart falls back to the seqno seek → same answer.
        assert_eq!(
            block.point_read_at_slot(9_999, false, key, crate::SeqNo::MAX, &cmp)?,
            expected,
            "out-of-range-slot read mismatch at key {idx}",
        );
    }
    Ok(())
}

#[test]
#[expect(
    clippy::cast_possible_truncation,
    reason = "test footered block is tiny"
)]
fn from_loaded_shrinks_uncompressed_length_to_stripped_inner() -> crate::Result<()> {
    use crate::runtime_config::ChecksumAlgorithm;
    use crate::table::block::kv_checksum::kv_digest;

    // from_loaded strips the per-KV footer from `data`; it must also
    // shrink `uncompressed_length` to the stripped inner length.
    // Leaving it at the footered length makes the in-memory Block
    // internally inconsistent (header size > slice length) and makes
    // cache weighing over-count footer bytes for kv-checked blocks.
    let items = [
        InternalValue::from_components(b"alpha".to_vec(), b"first".to_vec(), 30, Value),
        InternalValue::from_components(b"bravo".to_vec(), b"second".to_vec(), 20, Value),
    ];
    let algo = ChecksumAlgorithm::Xxh3_64;
    let digests: Vec<u64> = items
        .iter()
        .map(|it| kv_digest(it, algo).expect("xxh3 always available"))
        .collect();

    let mut bytes = Vec::new();
    DataBlock::encode_kv_checked_into(&mut bytes, &items, &digests, algo, 2, 0.0)?;
    let footered_len = bytes.len();

    let loaded = DataBlock::from_loaded(
        Block {
            data: bytes.into(),
            header: Header {
                // uncompressed_length includes the footer, as on disk.
                uncompressed_length: footered_len as u32,
                ..Header::test_dummy(BlockType::Data)
            },
        },
        true,
    )?;

    let inner_len = loaded.as_slice().len();
    assert!(
        inner_len < footered_len,
        "footer must have been stripped: inner {inner_len} vs footered {footered_len}",
    );
    assert_eq!(
        loaded.inner.header.uncompressed_length as usize, inner_len,
        "uncompressed_length must match the stripped inner slice length",
    );
    Ok(())
}

#[test]
fn encode_kv_checked_into_rejects_digest_count_mismatch() {
    use crate::runtime_config::ChecksumAlgorithm;

    // A digest array whose length disagrees with the item count would
    // serialize a footer whose `count` field does not match the stored
    // digests — structurally corrupt on-disk data. The writer must fail
    // at write time, not defer the failure to a later read/scrub (and
    // not rely on a debug_assert that vanishes in release builds).
    let items = [
        InternalValue::from_components(b"alpha".to_vec(), b"first".to_vec(), 30, Value),
        InternalValue::from_components(b"bravo".to_vec(), b"second".to_vec(), 20, Value),
    ];
    let digests = [0u64]; // one digest for two items: mismatch
    let mut buf = Vec::new();
    let result = DataBlock::encode_kv_checked_into(
        &mut buf,
        &items,
        &digests,
        ChecksumAlgorithm::Xxh3_64,
        2,
        0.0,
    );
    assert!(
        matches!(result, Err(crate::Error::InvalidTrailer)),
        "digest/item count mismatch must fail the write, got {result:?}",
    );
}

#[test]
fn verify_kv_checked_rejects_footer_algo_diverging_from_descriptor() {
    use crate::runtime_config::ChecksumAlgorithm;
    use crate::table::block::header::block_flags::KV_CHECKSUM_FOOTER;
    use crate::table::block::kv_checksum::kv_digest;

    // A footer-bearing block written under Xxh3_64.
    let items = [InternalValue::from_components(
        b"a".to_vec(),
        b"v".to_vec(),
        0,
        Value,
    )];
    let algo = ChecksumAlgorithm::Xxh3_64;
    let digests: Vec<u64> = items
        .iter()
        .map(|it| kv_digest(it, algo).expect("xxh3 always available"))
        .collect();
    let mut bytes = Vec::new();
    DataBlock::encode_kv_checked_into(&mut bytes, &items, &digests, algo, 2, 0.0)
        .expect("encode kv-checked block");
    let bytes = Slice::from(bytes);
    let header = Header {
        block_flags: KV_CHECKSUM_FOOTER,
        ..Header::test_dummy(BlockType::Data)
    };

    // Matching per-SST descriptor algorithm → verifies.
    DataBlock::verify_kv_checked(&bytes, header, default_comparator(), Some(algo))
        .expect("matching descriptor algorithm must verify");
    // No descriptor cross-check → footer's own algorithm is authoritative.
    DataBlock::verify_kv_checked(&bytes, header, default_comparator(), None)
        .expect("None skips the cross-check and verifies");
    // Descriptor algorithm diverges from the footer tag → reject, even
    // though the digests are internally consistent with the footer's tag.
    let err = DataBlock::verify_kv_checked(
        &bytes,
        header,
        default_comparator(),
        Some(ChecksumAlgorithm::Xxh3Low32),
    )
    .expect_err("descriptor/footer algorithm divergence must be rejected");
    assert!(
        matches!(err, crate::Error::InvalidTrailer),
        "expected InvalidTrailer for algorithm divergence, got {err:?}",
    );
}

#[test]
fn data_block_ping_pong_fuzz_1() -> crate::Result<()> {
    let items = [
        InternalValue::from_components(
            Slice::from([111]),
            Slice::from([119]),
            8_602_264_972_526_186_597,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([121, 120, 99]),
            Slice::from([101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101]),
            11_426_548_769_907,
            Value,
        ),
    ];

    let ping_pong_code = [1, 0];

    let bytes: Vec<u8> = DataBlock::encode_into_vec(&items, 1, 0.0)?;

    let data_block = DataBlock::new(Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Data),
    });

    let expected_ping_ponged_items = {
        let mut iter = items.iter();
        let mut v = vec![];

        for &x in &ping_pong_code {
            if x == 0 {
                v.push(iter.next().cloned().expect("should have item"));
            } else {
                v.push(iter.next_back().cloned().expect("should have item"));
            }
        }

        v
    };

    let real_ping_ponged_items = {
        let mut iter = data_block
            .iter(default_comparator())
            .map(|x| x.materialize(data_block.as_slice()));

        let mut v = vec![];

        for &x in &ping_pong_code {
            if x == 0 {
                v.push(iter.next().expect("should have item"));
            } else {
                v.push(iter.next_back().expect("should have item"));
            }
        }

        v
    };

    assert_eq!(expected_ping_ponged_items, real_ping_ponged_items);

    Ok(())
}

#[test]
fn data_block_point_read_simple() -> crate::Result<()> {
    let items = [
        InternalValue::from_components("b", "b", 0, Value),
        InternalValue::from_components("c", "c", 0, Value),
        InternalValue::from_components("d", "d", 1, Tombstone),
        InternalValue::from_components("e", "e", 0, Value),
        InternalValue::from_components("f", "f", 0, Value),
    ];

    for restart_interval in 1..=16 {
        let bytes: Vec<u8> = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header::test_dummy(BlockType::Data),
        });

        assert!(
            data_block
                .point_read(b"a", SeqNo::MAX, &default_comparator())?
                .is_none(),
            "should return None because a does not exist",
        );

        assert!(
            data_block
                .point_read(b"b", SeqNo::MAX, &default_comparator())?
                .is_some(),
            "should return Some because b exists",
        );

        assert!(
            data_block
                .point_read(b"z", SeqNo::MAX, &default_comparator())?
                .is_none(),
            "should return Some because z does not exist",
        );
    }

    Ok(())
}

#[test]
fn data_block_point_read_one() -> crate::Result<()> {
    let items = [InternalValue::from_components(
        "pla:earth:fact",
        "eaaaaaaaaarth",
        0,
        crate::ValueType::Value,
    )];

    let bytes = DataBlock::encode_into_vec(&items, 16, 0.0)?;
    let serialized_len = bytes.len();

    let data_block = DataBlock::new(Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Data),
    });

    assert_eq!(data_block.len(), items.len());
    assert_eq!(data_block.inner.size(), serialized_len);
    assert_eq!(1, data_block.binary_index_len());

    for needle in items {
        assert_eq!(
            Some(needle.clone()),
            data_block.point_read(&needle.key.user_key, SeqNo::MAX, &default_comparator())?,
        );
    }

    assert_eq!(
        None,
        data_block.point_read(b"yyy", SeqNo::MAX, &default_comparator())?
    );

    Ok(())
}

#[test]
fn data_block_vhandle() -> crate::Result<()> {
    let items = [InternalValue::from_components(
        "abc",
        "world",
        1,
        crate::ValueType::Indirection,
    )];

    for restart_interval in 1..=16 {
        let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;
        let serialized_len = bytes.len();

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header::test_dummy(BlockType::Data),
        });

        assert_eq!(data_block.len(), items.len());
        assert_eq!(data_block.inner.size(), serialized_len);

        assert_eq!(
            Some(items[0].clone()),
            data_block.point_read(b"abc", 777, &default_comparator())?
        );
        assert!(
            data_block
                .point_read(b"abc", 1, &default_comparator())?
                .is_none()
        );
    }

    Ok(())
}

#[test]
fn data_block_mvcc_read_first() -> crate::Result<()> {
    let items = [InternalValue::from_components(
        "hello",
        "world",
        0,
        crate::ValueType::Value,
    )];

    for restart_interval in 1..=16 {
        let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;
        let serialized_len = bytes.len();

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header::test_dummy(BlockType::Data),
        });

        assert_eq!(data_block.len(), items.len());
        assert_eq!(data_block.inner.size(), serialized_len);

        assert_eq!(
            Some(items[0].clone()),
            data_block.point_read(b"hello", 777, &default_comparator())?
        );
    }

    Ok(())
}

#[test]
fn data_block_point_read_fuzz_1() -> crate::Result<()> {
    let items = [
        InternalValue::from_components([0], b"", 23_523_531_241_241_242, Value),
        InternalValue::from_components([0], b"", 0, Value),
    ];

    let bytes = DataBlock::encode_into_vec(&items, 16, 1.33)?;

    let data_block = DataBlock::new(Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Data),
    });

    assert_eq!(data_block.len(), items.len());
    assert!(
        data_block
            .hash_bucket_count()
            .expect("should have built hash index")
            > 0,
    );

    for needle in items {
        assert_eq!(
            Some(needle.clone()),
            data_block.point_read(
                &needle.key.user_key,
                needle.key.seqno + 1,
                &default_comparator()
            )?,
        );
    }

    assert_eq!(
        None,
        data_block.point_read(b"yyy", SeqNo::MAX, &default_comparator())?
    );

    Ok(())
}

#[test]
fn data_block_point_read_fuzz_2() -> crate::Result<()> {
    let items = [
        InternalValue::from_components([0], [], 5, Value),
        InternalValue::from_components([0], [], 4, Tombstone),
        InternalValue::from_components([0], [], 3, Value),
        InternalValue::from_components([0], [], 0, Value),
    ];

    let bytes = DataBlock::encode_into_vec(&items, 2, 0.0)?;

    let data_block = DataBlock::new(Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Data),
    });

    assert_eq!(data_block.len(), items.len());
    assert!(data_block.hash_bucket_count().is_none());

    for needle in items {
        assert_eq!(
            Some(needle.clone()),
            data_block.point_read(
                &needle.key.user_key,
                needle.key.seqno + 1,
                &default_comparator()
            )?,
        );
    }

    assert_eq!(
        None,
        data_block.point_read(b"yyy", SeqNo::MAX, &default_comparator())?
    );

    Ok(())
}

#[test]
fn data_block_point_read_dense() -> crate::Result<()> {
    let items = [
        InternalValue::from_components(b"a", b"a", 3, Value),
        InternalValue::from_components(b"b", b"b", 2, Value),
        InternalValue::from_components(b"c", b"c", 1, Value),
        InternalValue::from_components(b"d", b"d", 65, Value),
    ];

    let bytes = DataBlock::encode_into_vec(&items, 1, 0.0)?;

    let data_block = DataBlock::new(Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Data),
    });

    assert_eq!(data_block.len(), items.len());
    assert_eq!(4, data_block.binary_index_len());

    for needle in items {
        assert_eq!(
            Some(needle.clone()),
            data_block.point_read(&needle.key.user_key, SeqNo::MAX, &default_comparator())?,
        );
    }

    assert_eq!(
        None,
        data_block.point_read(b"yyy", SeqNo::MAX, &default_comparator())?
    );

    Ok(())
}

#[test]
fn data_block_point_read_dense_mvcc_with_hash() -> crate::Result<()> {
    let items = [
        InternalValue::from_components(b"a", b"a", 3, Value),
        InternalValue::from_components(b"a", b"a", 2, Value),
        InternalValue::from_components(b"a", b"a", 1, Value),
        InternalValue::from_components(b"b", b"b", 65, Value),
    ];

    let bytes = DataBlock::encode_into_vec(&items, 1, 1.33)?;

    let data_block = DataBlock::new(Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Data),
    });

    assert_eq!(data_block.len(), items.len());
    assert!(
        data_block
            .hash_bucket_count()
            .expect("should have built hash index")
            > 0,
    );

    for needle in items {
        assert_eq!(
            Some(needle.clone()),
            data_block.point_read(
                &needle.key.user_key,
                needle.key.seqno + 1,
                &default_comparator()
            )?,
        );
    }

    assert_eq!(
        None,
        data_block.point_read(b"yyy", SeqNo::MAX, &default_comparator())?
    );

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn data_block_point_read_mvcc_latest_fuzz_1() -> crate::Result<()> {
    let items = [
        InternalValue::from_components(Slice::from([0]), Slice::from([]), 0, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 0, Value),
        InternalValue::from_components(
            Slice::from([255, 255, 0]),
            Slice::from([]),
            127_886_946_205_696,
            Tombstone,
        ),
    ];

    let bytes = DataBlock::encode_into_vec(&items, 2, 0.0)?;

    let data_block = DataBlock::new(Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Data),
    });

    assert_eq!(data_block.len(), items.len());
    assert!(data_block.get_hash_index_reader().is_none());

    assert_eq!(
        Some(items.get(1).cloned().unwrap()),
        data_block.point_read(&[233, 233], SeqNo::MAX, &default_comparator())?
    );
    assert_eq!(
        None,
        data_block.point_read(b"yyy", SeqNo::MAX, &default_comparator())?
    );

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn data_block_point_read_mvcc_latest_fuzz_2() -> crate::Result<()> {
    let items = [
        InternalValue::from_components(Slice::from([0]), Slice::from([]), 0, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 8, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 7, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 6, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 5, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 4, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 3, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 2, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 1, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 0, Value),
        InternalValue::from_components(
            Slice::from([255, 255, 0]),
            Slice::from([]),
            127_886_946_205_696,
            Tombstone,
        ),
    ];

    let bytes = DataBlock::encode_into_vec(&items, 2, 0.0)?;

    let data_block = DataBlock::new(Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Data),
    });

    assert_eq!(data_block.len(), items.len());

    assert_eq!(
        Some(items.get(1).cloned().unwrap()),
        data_block.point_read(&[233, 233], SeqNo::MAX, &default_comparator())?
    );
    assert_eq!(
        Some(items.last().cloned().unwrap()),
        data_block.point_read(&[255, 255, 0], SeqNo::MAX, &default_comparator())?
    );
    assert_eq!(
        None,
        data_block.point_read(b"yyy", SeqNo::MAX, &default_comparator())?
    );

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn data_block_point_read_mvcc_latest_fuzz_3() -> crate::Result<()> {
    let items = [
        InternalValue::from_components(Slice::from([0]), Slice::from([]), 0, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 8, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 7, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 6, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 5, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 4, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 3, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 2, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 1, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 0, Value),
        InternalValue::from_components(
            Slice::from([255, 255, 0]),
            Slice::from([]),
            127_886_946_205_696,
            Tombstone,
        ),
    ];

    let bytes = DataBlock::encode_into_vec(&items, 2, 0.0)?;

    let data_block = DataBlock::new(Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Data),
    });

    assert_eq!(data_block.len(), items.len());

    assert_eq!(
        Some(items.get(1).cloned().unwrap()),
        data_block.point_read(&[233, 233], SeqNo::MAX, &default_comparator())?
    );
    assert_eq!(
        Some(items.last().cloned().unwrap()),
        data_block.point_read(&[255, 255, 0], SeqNo::MAX, &default_comparator())?
    );
    assert_eq!(
        None,
        data_block.point_read(b"yyy", SeqNo::MAX, &default_comparator())?
    );

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn data_block_point_read_mvcc_latest_fuzz_3_dense() -> crate::Result<()> {
    let items = [
        InternalValue::from_components(Slice::from([0]), Slice::from([]), 0, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 8, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 7, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 6, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 5, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 4, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 3, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 2, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 1, Value),
        InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 0, Value),
        InternalValue::from_components(
            Slice::from([255, 255, 0]),
            Slice::from([]),
            127_886_946_205_696,
            Tombstone,
        ),
    ];

    let bytes = DataBlock::encode_into_vec(&items, 1, 0.0)?;

    let data_block = DataBlock::new(Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Data),
    });

    assert_eq!(data_block.len(), items.len());

    assert_eq!(
        Some(items.get(1).cloned().unwrap()),
        data_block.point_read(&[233, 233], SeqNo::MAX, &default_comparator())?
    );
    assert_eq!(
        Some(items.last().cloned().unwrap()),
        data_block.point_read(&[255, 255, 0], SeqNo::MAX, &default_comparator())?
    );
    assert_eq!(
        None,
        data_block.point_read(b"yyy", SeqNo::MAX, &default_comparator())?
    );

    Ok(())
}

#[test]
fn data_block_point_read_dense_mvcc_no_hash() -> crate::Result<()> {
    let items = [
        InternalValue::from_components(b"a", b"a", 3, Value),
        InternalValue::from_components(b"a", b"a", 2, Value),
        InternalValue::from_components(b"a", b"a", 1, Value),
        InternalValue::from_components(b"b", b"b", 65, Value),
    ];

    let bytes = DataBlock::encode_into_vec(&items, 1, 0.0)?;

    let data_block = DataBlock::new(Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Data),
    });

    assert_eq!(data_block.len(), items.len());
    assert!(data_block.hash_bucket_count().is_none());

    for needle in items {
        assert_eq!(
            Some(needle.clone()),
            data_block.point_read(
                &needle.key.user_key,
                needle.key.seqno + 1,
                &default_comparator()
            )?,
        );
    }

    assert_eq!(
        None,
        data_block.point_read(b"yyy", SeqNo::MAX, &default_comparator())?
    );

    Ok(())
}

#[test]
fn data_block_point_read_shadowing() -> crate::Result<()> {
    let items = [
        InternalValue::from_components("pla:saturn:fact", "Saturn is pretty big", 0, Value),
        InternalValue::from_components("pla:saturn:name", "Saturn", 0, Value),
        InternalValue::from_components("pla:venus:fact", "", 1, Tombstone),
        InternalValue::from_components("pla:venus:fact", "Venus exists", 0, Value),
        InternalValue::from_components("pla:venus:name", "Venus", 0, Value),
    ];

    let bytes = DataBlock::encode_into_vec(&items, 16, 1.33)?;

    let data_block = DataBlock::new(Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Data),
    });

    assert_eq!(data_block.len(), items.len());
    assert!(
        data_block
            .hash_bucket_count()
            .expect("should have built hash index")
            > 0,
    );

    assert!(
        data_block
            .point_read(b"pla:venus:fact", SeqNo::MAX, &default_comparator())?
            .expect("should exist")
            .is_tombstone()
    );

    Ok(())
}

#[test]
fn data_block_point_read_dense_2() -> crate::Result<()> {
    let items = [
        InternalValue::from_components("pla:earth:fact", "eaaaaaaaaarth", 0, Value),
        InternalValue::from_components("pla:jupiter:fact", "Jupiter is big", 0, Value),
        InternalValue::from_components("pla:jupiter:mass", "Massive", 0, Value),
        InternalValue::from_components("pla:jupiter:name", "Jupiter", 0, Value),
        InternalValue::from_components("pla:jupiter:radius", "Big", 0, Value),
        InternalValue::from_components("pla:saturn:fact", "Saturn is pretty big", 0, Value),
        InternalValue::from_components("pla:saturn:name", "Saturn", 0, Value),
        InternalValue::from_components("pla:venus:fact", "", 1, Tombstone),
        InternalValue::from_components("pla:venus:fact", "Venus exists", 0, Value),
        InternalValue::from_components("pla:venus:name", "Venus", 0, Value),
    ];

    let bytes = DataBlock::encode_into_vec(&items, 1, 1.33)?;

    let data_block = DataBlock::new(Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Data),
    });

    assert_eq!(data_block.len(), items.len());
    assert!(
        data_block
            .hash_bucket_count()
            .expect("should have built hash index")
            > 0,
    );

    for needle in items {
        assert_eq!(
            Some(needle.clone()),
            data_block.point_read(
                &needle.key.user_key,
                needle.key.seqno + 1,
                &default_comparator()
            )?,
        );
    }

    assert_eq!(
        None,
        data_block.point_read(b"yyy", SeqNo::MAX, &default_comparator())?
    );

    Ok(())
}

#[test]
fn data_block_point_read_seqno_aware_seek() -> crate::Result<()> {
    // Key "a" with seqno 5,4,3,2,1 — point_read("a", seqno=3)
    // returns the first version with seqno < 3, i.e., v2 ("a2")
    let items = [
        InternalValue::from_components(b"a", b"a5", 5, Value),
        InternalValue::from_components(b"a", b"a4", 4, Value),
        InternalValue::from_components(b"a", b"a3", 3, Value),
        InternalValue::from_components(b"a", b"a2", 2, Value),
        InternalValue::from_components(b"a", b"a1", 1, Value),
    ];

    // Test across various restart intervals: at restart_interval=1 every item
    // is a restart head so binary search lands exactly; at larger intervals it
    // may scan within the restart range but must still return the correct version.
    for restart_interval in 1..=4 {
        let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header::test_dummy(BlockType::Data),
        });

        // seqno=4 → should see version with seqno=3 (first with seqno < 4)
        assert_eq!(
            Some(items[2].clone()),
            data_block.point_read(b"a", 4, &default_comparator())?,
            "restart_interval={restart_interval}: seqno=4 should return v3",
        );

        // seqno=3 → should see version with seqno=2
        assert_eq!(
            Some(items[3].clone()),
            data_block.point_read(b"a", 3, &default_comparator())?,
            "restart_interval={restart_interval}: seqno=3 should return v2",
        );

        // seqno=6 → should see latest version (seqno=5)
        assert_eq!(
            Some(items[0].clone()),
            data_block.point_read(b"a", 6, &default_comparator())?,
            "restart_interval={restart_interval}: seqno=6 should return v5",
        );

        // seqno=1 → no visible version (all seqno >= 1)
        assert!(
            data_block
                .point_read(b"a", 1, &default_comparator())?
                .is_none(),
            "restart_interval={restart_interval}: seqno=1 should return None",
        );

        // Non-existent key
        assert!(
            data_block
                .point_read(b"b", SeqNo::MAX, &default_comparator())?
                .is_none(),
            "restart_interval={restart_interval}: key 'b' should not exist",
        );
    }

    Ok(())
}

#[test]
fn data_block_point_read_seqno_aware_seek_mixed_keys() -> crate::Result<()> {
    // Multiple keys with multiple versions
    let items = [
        InternalValue::from_components(b"a", b"a3", 3, Value),
        InternalValue::from_components(b"a", b"a2", 2, Value),
        InternalValue::from_components(b"a", b"a1", 1, Value),
        InternalValue::from_components(b"b", b"b5", 5, Value),
        InternalValue::from_components(b"b", b"b4", 4, Value),
        InternalValue::from_components(b"b", b"b3", 3, Value),
        InternalValue::from_components(b"b", b"b2", 2, Value),
        InternalValue::from_components(b"b", b"b1", 1, Value),
        InternalValue::from_components(b"c", b"c1", 1, Value),
    ];

    for restart_interval in 1..=4 {
        let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header::test_dummy(BlockType::Data),
        });

        // Read "b" at seqno=4 → should return version with seqno=3
        assert_eq!(
            Some(items[5].clone()),
            data_block.point_read(b"b", 4, &default_comparator())?,
            "restart_interval={restart_interval}: b@4 should return b3",
        );

        // Read "a" at seqno=2 → should return version with seqno=1
        assert_eq!(
            Some(items[2].clone()),
            data_block.point_read(b"a", 2, &default_comparator())?,
            "restart_interval={restart_interval}: a@2 should return a1",
        );

        // Read "c" at seqno=2 → should return version with seqno=1
        assert_eq!(
            Some(items[8].clone()),
            data_block.point_read(b"c", 2, &default_comparator())?,
            "restart_interval={restart_interval}: c@2 should return c1",
        );
    }

    Ok(())
}

#[test]
fn try_iter_zero_restart_interval_returns_invalid_trailer() -> crate::Result<()> {
    use crate::table::block::Trailer;

    let items = [InternalValue::from_components(b"a", b"v", 0, Value)];
    let mut bytes = DataBlock::encode_into_vec(&items, 1, 0.0)?;

    let block = Block {
        data: bytes.clone().into(),
        header: Header::test_dummy(BlockType::Data),
    };
    let trailer_offset = Trailer::new(&block).trailer_offset();
    bytes[trailer_offset] = 0;

    let corrupt_block = DataBlock::new(Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Data),
    });

    assert!(
        matches!(
            corrupt_block.try_iter(default_comparator()),
            Err(crate::Error::InvalidTrailer)
        ),
        "zero restart_interval must return InvalidTrailer",
    );

    Ok(())
}

#[test]
fn point_read_zero_restart_interval_returns_invalid_trailer() -> crate::Result<()> {
    use crate::table::block::Trailer;

    let items = [InternalValue::from_components(b"a", b"v", 0, Value)];
    let mut bytes = DataBlock::encode_into_vec(&items, 1, 0.0)?;

    let block = Block {
        data: bytes.clone().into(),
        header: Header::test_dummy(BlockType::Data),
    };
    let trailer_offset = Trailer::new(&block).trailer_offset();
    bytes[trailer_offset] = 0;

    let corrupt_block = DataBlock::new(Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Data),
    });

    assert!(
        matches!(
            corrupt_block.point_read(b"a", SeqNo::MAX, &default_comparator()),
            Err(crate::Error::InvalidTrailer)
        ),
        "point_read on corrupt block must return InvalidTrailer",
    );

    Ok(())
}

#[test]
fn point_read_zero_restart_interval_hash_path_returns_invalid_trailer() -> crate::Result<()> {
    use crate::table::block::Trailer;

    // Use hash_index_ratio > 0 so the hash-index fast path is exercised.
    let items = [InternalValue::from_components(b"a", b"v", 0, Value)];
    let mut bytes = DataBlock::encode_into_vec(&items, 1, 1.33)?;

    let original_block = DataBlock::new(Block {
        data: bytes.clone().into(),
        header: Header::test_dummy(BlockType::Data),
    });
    assert!(
        original_block.hash_bucket_count().is_some(),
        "test fixture must build a hash index so point_read takes the hash fast path",
    );
    let trailer_offset = Trailer::new(&original_block.inner).trailer_offset();
    bytes[trailer_offset] = 0;

    let corrupt_block = DataBlock::new(Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Data),
    });

    // The upfront Decoder::try_new guard must reject the block before
    // get_hash_index_reader() / get_binary_index_reader() touch the
    // corrupt trailer.
    assert!(
        matches!(
            corrupt_block.point_read(b"a", SeqNo::MAX, &default_comparator()),
            Err(crate::Error::InvalidTrailer)
        ),
        "point_read with hash index on corrupt block must return InvalidTrailer",
    );

    Ok(())
}

#[cfg(feature = "columnar")]
#[test]
#[expect(clippy::unwrap_used)]
fn from_column_batch_masked_drops_deleted_positions() {
    use crate::table::columnar::entries_to_column_batch;
    use crate::table::delete_bitmap::DeleteBitmap;

    // A 4-row block (in-block positions 0..4): keys a, b, c, d.
    let entries = [
        InternalValue::from_components(Slice::from(b"a".as_slice()), Slice::from([]), 1, Value),
        InternalValue::from_components(Slice::from(b"b".as_slice()), Slice::from([]), 1, Value),
        InternalValue::from_components(Slice::from(b"c".as_slice()), Slice::from([]), 1, Value),
        InternalValue::from_components(Slice::from(b"d".as_slice()), Slice::from([]), 1, Value),
    ];
    // Two row pages, [a] and [b, c, d]: the second page's rows keep counting
    // positions from where the first page ended.
    let (first, rest) = entries.split_at(1);
    let pages = [
        entries_to_column_batch(first).unwrap(),
        entries_to_column_batch(rest).unwrap(),
    ];

    // The block starts at global row 10, so delete global positions 10 (a)
    // and 12 (c); b and d must survive.
    let mut dv = DeleteBitmap::new();
    dv.insert(10);
    dv.insert(12);
    let block = DataBlock::from_column_batch_masked(pages, 16, &dv, 10, &mut 0)
        .unwrap()
        .expect("not all rows deleted");

    assert_eq!(block.len(), 2);
    assert!(
        block
            .point_read(b"a", SeqNo::MAX, &default_comparator())
            .unwrap()
            .is_none()
    );
    assert!(
        block
            .point_read(b"b", SeqNo::MAX, &default_comparator())
            .unwrap()
            .is_some()
    );
    assert!(
        block
            .point_read(b"c", SeqNo::MAX, &default_comparator())
            .unwrap()
            .is_none()
    );
    assert!(
        block
            .point_read(b"d", SeqNo::MAX, &default_comparator())
            .unwrap()
            .is_some()
    );
}

#[cfg(feature = "columnar")]
#[test]
#[expect(clippy::unwrap_used)]
fn from_column_batch_masked_returns_none_when_all_deleted() {
    use crate::table::columnar::entries_to_column_batch;
    use crate::table::delete_bitmap::DeleteBitmap;

    let entries = [
        InternalValue::from_components(Slice::from(b"a".as_slice()), Slice::from([]), 1, Value),
        InternalValue::from_components(Slice::from(b"b".as_slice()), Slice::from([]), 1, Value),
    ];
    let batch = entries_to_column_batch(&entries).unwrap();

    let mut dv = DeleteBitmap::new();
    dv.insert(0);
    dv.insert(1);
    assert!(
        DataBlock::from_column_batch_masked([batch], 16, &dv, 0, &mut 0)
            .unwrap()
            .is_none()
    );
}

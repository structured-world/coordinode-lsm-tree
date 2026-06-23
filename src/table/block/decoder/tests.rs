use super::Decoder;
use crate::io::{ByteOrder, LittleEndian};
use crate::{
    InternalValue,
    table::{
        Block, BlockHandle, BlockOffset, DataBlock, IndexBlock, KeyedBlockHandle,
        block::{BlockType, Header, Trailer},
        data_block::DataBlockParsedItem,
        index_block::IndexBlockParsedItem,
    },
};

#[test]
fn read_leb128_rejects_overlong_10th_byte() {
    use super::read_leb128_fn;
    // A 10-byte varint whose final payload byte exceeds 1 encodes a value
    // wider than u64. It must be rejected (None), not silently truncated
    // into a Some(_), or corruption in on-disk seqnos/lengths that every
    // slice-based parse path now decodes through read_leb128 goes
    // undetected. Nine continuation bytes (payload 0) + a terminating 10th
    // byte with payload 2. `read_leb128_fn` is the function form of the
    // `read_leb128!` macro (same body), exercised here.
    let overlong = [0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02];
    assert!(
        read_leb128_fn(&overlong, 0).is_none(),
        "10-byte varint with 10th-byte payload > 1 overflows u64 and must be rejected",
    );
    // The boundary-valid case (10th-byte payload 1 → sets bit 63) is still
    // accepted and round-trips to 1 << 63, so the reject is not over-broad.
    let max_bit = [0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01];
    assert_eq!(read_leb128_fn(&max_bit, 0), Some((1u64 << 63, 10)));
}

fn make_handles(count: usize) -> Vec<KeyedBlockHandle> {
    (0..count)
        .map(|i| {
            let key = format!("adj:out:vertex-0001:edge-{i:04}");
            KeyedBlockHandle::new(
                key.into(),
                i as u64,
                BlockHandle::new(BlockOffset((i as u64) * 4096), 4096),
            )
        })
        .collect()
}

fn binary_index_offset_field_pos(bytes: &[u8]) -> usize {
    let trailer_probe = IndexBlock::new(Block {
        data: bytes.to_vec().into(),
        header: Header::test_dummy(BlockType::Index),
    });
    let trailer_offset = Trailer::new(&trailer_probe.inner).trailer_offset();
    trailer_offset + 1 + 1 + core::mem::size_of::<u32>()
}

fn binary_index_len_field_pos(bytes: &[u8]) -> usize {
    let trailer_probe = IndexBlock::new(Block {
        data: bytes.to_vec().into(),
        header: Header::test_dummy(BlockType::Index),
    });
    let trailer_offset = Trailer::new(&trailer_probe.inner).trailer_offset();
    trailer_offset + 1 + 1
}

fn binary_index_step_size_field_pos(bytes: &[u8]) -> usize {
    let trailer_probe = IndexBlock::new(Block {
        data: bytes.to_vec().into(),
        header: Header::test_dummy(BlockType::Index),
    });
    let trailer_offset = Trailer::new(&trailer_probe.inner).trailer_offset();
    trailer_offset + 1
}

fn hash_index_len_field_pos(bytes: &[u8]) -> usize {
    let trailer_probe = IndexBlock::new(Block {
        data: bytes.to_vec().into(),
        header: Header::test_dummy(BlockType::Index),
    });
    let trailer_offset = Trailer::new(&trailer_probe.inner).trailer_offset();
    trailer_offset + 1 + 1 + (2 * core::mem::size_of::<u32>())
}

fn hash_index_offset_field_pos(bytes: &[u8]) -> usize {
    hash_index_len_field_pos(bytes) + core::mem::size_of::<u32>()
}

fn first_restart_key_len_field_pos(bytes: &[u8]) -> usize {
    use crate::coding::Decode;
    use crate::io::Cursor;
    use crate::io::ReadBytesExt;
    use crate::table::BlockHandle;
    use varint_rs::VarintReader;

    let mut cursor = Cursor::new(bytes);

    let marker = cursor.read_u8().expect("restart head marker");
    assert_eq!(marker, 0, "first entry in index block must be restart");

    let _ = BlockHandle::decode_from(&mut cursor).expect("block handle");
    let _ = cursor.read_u64_varint().expect("seqno");

    usize::try_from(cursor.position()).expect("position should fit usize")
}

fn first_truncated_rest_key_len_field_pos(bytes: &[u8]) -> usize {
    use crate::coding::Decode;
    use crate::io::Cursor;
    use crate::io::ReadBytesExt;
    use crate::table::BlockHandle;
    use varint_rs::VarintReader;

    let mut cursor = Cursor::new(bytes);

    let marker = cursor.read_u8().expect("restart head marker");
    assert_eq!(marker, 0, "first entry in index block must be restart");

    let _ = BlockHandle::decode_from(&mut cursor).expect("block handle");
    let _ = cursor.read_u64_varint().expect("seqno");
    let key_len = cursor.read_u16_varint().expect("key len");
    cursor.set_position(cursor.position() + u64::from(key_len));

    let truncated_marker = cursor.read_u8().expect("truncated marker");
    assert_eq!(truncated_marker, 1, "second entry should be truncated");

    let _ = cursor.read_u16_varint().expect("shared prefix len");

    usize::try_from(cursor.position()).expect("position should fit usize")
}

fn nth_truncated_rest_key_len_field_pos(bytes: &[u8], ordinal: usize) -> usize {
    use crate::coding::Decode;
    use crate::io::Cursor;
    use crate::io::ReadBytesExt;
    use crate::table::BlockHandle;
    use varint_rs::VarintReader;

    let mut cursor = Cursor::new(bytes);
    let mut seen = 0usize;

    while usize::try_from(cursor.position()).expect("position should fit usize") < bytes.len() {
        let marker = cursor.read_u8().expect("entry marker");
        match marker {
            0 => {
                let _ = BlockHandle::decode_from(&mut cursor).expect("block handle");
                let _ = cursor.read_u64_varint().expect("seqno");
                let key_len = cursor.read_u16_varint().expect("key len");
                cursor.set_position(cursor.position() + u64::from(key_len));
            }
            1 => {
                let _ = BlockHandle::decode_from(&mut cursor).expect("block handle");
                let _ = cursor.read_u64_varint().expect("seqno");
                let _ = cursor.read_u16_varint().expect("shared prefix len");
                let pos = usize::try_from(cursor.position()).expect("position should fit usize");
                if seen == ordinal {
                    return pos;
                }
                seen += 1;
                let rest_key_len = cursor.read_u16_varint().expect("rest key len");
                cursor.set_position(cursor.position() + u64::from(rest_key_len));
            }
            crate::table::block::TRAILER_START_MARKER => break,
            _ => panic!("unexpected entry marker in fixture"),
        }
    }

    panic!("truncated entry ordinal out of range");
}

#[test]
fn entries_end_rejects_binary_index_offset_past_block_end() {
    let handles = make_handles(8);
    let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 8).unwrap();
    let binary_index_offset_pos = binary_index_offset_field_pos(&bytes);
    let invalid_binary_index_offset = (bytes.len() as u32).saturating_add(1);
    LittleEndian::write_u32(
        &mut bytes[binary_index_offset_pos..binary_index_offset_pos + core::mem::size_of::<u32>()],
        invalid_binary_index_offset,
    );

    let block = Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Index),
    };
    assert!(
        matches!(
            Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::try_new(&block),
            Err(crate::Error::InvalidTrailer)
        ),
        "bogus binary_index_offset must be rejected by try_new",
    );
}

#[test]
fn entries_end_rejects_zero_binary_index_offset() {
    let handles = make_handles(8);
    let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 8).unwrap();
    let binary_index_offset_pos = binary_index_offset_field_pos(&bytes);
    LittleEndian::write_u32(
        &mut bytes[binary_index_offset_pos..binary_index_offset_pos + core::mem::size_of::<u32>()],
        0,
    );

    let block = Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Index),
    };
    assert!(
        matches!(
            Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::try_new(&block),
            Err(crate::Error::InvalidTrailer)
        ),
        "zero binary_index_offset must be rejected as InvalidTrailer",
    );
}

#[test]
fn entries_end_rejects_when_marker_before_binary_index_is_missing() {
    let handles = make_handles(8);
    let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 8).unwrap();
    let binary_index_offset_pos = binary_index_offset_field_pos(&bytes);
    let binary_index_offset = LittleEndian::read_u32(
        &bytes[binary_index_offset_pos..binary_index_offset_pos + core::mem::size_of::<u32>()],
    ) as usize;
    bytes[binary_index_offset - 1] = 0;

    let block = Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Index),
    };
    assert!(
        matches!(
            Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::try_new(&block),
            Err(crate::Error::InvalidTrailer)
        ),
        "missing marker before binary index must be rejected as InvalidTrailer",
    );
}

#[test]
fn seek_rejects_binary_index_slice_past_block_end() {
    let handles = make_handles(8);
    let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 8).unwrap();
    let binary_index_len_pos = binary_index_len_field_pos(&bytes);
    let current_binary_index_len = LittleEndian::read_u32(
        &bytes[binary_index_len_pos..binary_index_len_pos + core::mem::size_of::<u32>()],
    );
    let inflated_binary_index_len = current_binary_index_len.saturating_add(10_000);
    LittleEndian::write_u32(
        &mut bytes[binary_index_len_pos..binary_index_len_pos + core::mem::size_of::<u32>()],
        inflated_binary_index_len,
    );

    let block = Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Index),
    };
    assert!(
        matches!(
            Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::try_new(&block),
            Err(crate::Error::InvalidTrailer)
        ),
        "binary index slice past block end must be rejected as InvalidTrailer",
    );
}

#[test]
fn seek_rejects_binary_index_slice_spilling_into_trailer() {
    let handles = make_handles(8);
    let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 8).unwrap();
    let binary_index_len_pos = binary_index_len_field_pos(&bytes);
    let binary_index_offset_pos = binary_index_offset_field_pos(&bytes);
    let binary_index_step_size_pos = binary_index_step_size_field_pos(&bytes);
    let trailer_probe = IndexBlock::new(Block {
        data: bytes.clone().into(),
        header: Header::test_dummy(BlockType::Index),
    });
    let trailer_offset = Trailer::new(&trailer_probe.inner).trailer_offset();

    let binary_index_offset = LittleEndian::read_u32(
        &bytes[binary_index_offset_pos..binary_index_offset_pos + core::mem::size_of::<u32>()],
    ) as usize;
    let step_size = usize::from(bytes[binary_index_step_size_pos]);
    let entries_before_trailer = trailer_offset - binary_index_offset;
    let inflated_binary_index_len = u32::try_from((entries_before_trailer / step_size) + 1)
        .expect("test fixture expects u32 binary index len");
    LittleEndian::write_u32(
        &mut bytes[binary_index_len_pos..binary_index_len_pos + core::mem::size_of::<u32>()],
        inflated_binary_index_len,
    );

    let block = Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Index),
    };
    assert!(
        matches!(
            Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::try_new(&block),
            Err(crate::Error::InvalidTrailer)
        ),
        "binary index slice spilling into trailer must be rejected as InvalidTrailer",
    );
}

#[test]
fn seek_rejects_binary_index_slice_shorter_than_metadata_boundary() {
    let handles = make_handles(8);
    let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 8).unwrap();
    let binary_index_len_pos = binary_index_len_field_pos(&bytes);
    let current_binary_index_len = LittleEndian::read_u32(
        &bytes[binary_index_len_pos..binary_index_len_pos + core::mem::size_of::<u32>()],
    );
    let shortened_binary_index_len = current_binary_index_len.saturating_sub(1);
    LittleEndian::write_u32(
        &mut bytes[binary_index_len_pos..binary_index_len_pos + core::mem::size_of::<u32>()],
        shortened_binary_index_len,
    );

    let block = Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Index),
    };
    assert!(
        matches!(
            Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::try_new(&block),
            Err(crate::Error::InvalidTrailer)
        ),
        "binary index slice shorter than metadata boundary must be rejected as InvalidTrailer",
    );
}

#[test]
fn seek_rejects_restart_head_key_crossing_entries_end() {
    let handles = make_handles(8);
    let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 8).unwrap();

    let binary_index_offset_pos = binary_index_offset_field_pos(&bytes);
    let binary_index_offset = LittleEndian::read_u32(
        &bytes[binary_index_offset_pos..binary_index_offset_pos + core::mem::size_of::<u32>()],
    ) as usize;
    let entries_end = binary_index_offset - 1;

    let key_len_pos = first_restart_key_len_field_pos(&bytes);
    let key_start = key_len_pos + 1;
    let overlapping_key_len = u8::try_from(entries_end - key_start + 1)
        .expect("test fixture expects one-byte key varint");
    bytes[key_len_pos] = overlapping_key_len;

    let block = Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Index),
    };
    let mut decoder = Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::new(&block);

    assert!(!decoder.seek(|_, _| true, false));
}

#[test]
fn advance_while_respects_active_upper_bound() {
    let handles = make_handles(16);
    let bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 4).unwrap();
    let block = Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Index),
    };
    let mut decoder = Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::new(&block);
    decoder.hi_scanner.offset = 1;
    decoder.hi_scanner.base_key_offset = Some(0);
    decoder.hi_scanner.base_key_end = Some(0);
    let upper_bound = decoder.hi_scanner.offset;

    decoder.advance_while(|_, _| true);

    assert!(
        decoder.lo_scanner.offset <= upper_bound,
        "advance_while should not move beyond active upper bound"
    );
}

#[test]
fn fill_stack_clears_partial_interval_when_truncated_parse_fails() {
    let handles = make_handles(8);
    let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 8).unwrap();
    let rest_key_len_pos = first_truncated_rest_key_len_field_pos(&bytes);
    bytes[rest_key_len_pos] = 0xFF;
    bytes[rest_key_len_pos + 1] = 0xFF;
    bytes[rest_key_len_pos + 2] = 0x03;

    let block = Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Index),
    };
    let mut decoder = Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::new(&block);
    decoder.hi_scanner.ptr_idx = 0;
    decoder.hi_scanner.stack.clear();
    decoder.hi_scanner.base_key_offset = None;
    decoder.hi_scanner.base_key_end = None;

    decoder.fill_stack();

    assert!(
        decoder.hi_scanner.stack.is_empty(),
        "partial reverse interval must be discarded on parse failure"
    );
    // poison_back_cursor delegates to clamp_upper_to_lo which preserves
    // the hard-stop sentinel as Some(0) so next() also stops.
    assert_eq!(decoder.hi_scanner.base_key_offset, Some(0));
    assert_eq!(decoder.hi_scanner.base_key_end, Some(0));
}

#[test]
fn seek_upper_fails_closed_when_selected_interval_is_corrupted() {
    let handles = make_handles(8);
    let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 8).unwrap();
    let rest_key_len_pos = first_truncated_rest_key_len_field_pos(&bytes);
    bytes[rest_key_len_pos] = 0xFF;
    bytes[rest_key_len_pos + 1] = 0xFF;
    bytes[rest_key_len_pos + 2] = 0x03;

    let block = Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Index),
    };
    let mut decoder = Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::new(&block);

    assert!(
        !decoder.seek_upper(|_, _| false, false),
        "seek_upper should fail when the selected upper interval is malformed"
    );
    assert_eq!(
        decoder.hi_scanner.offset, decoder.lo_scanner.offset,
        "failed upper seek must clamp the forward bound to current lo offset"
    );
    assert_eq!(decoder.hi_scanner.base_key_offset, Some(0));
    assert_eq!(decoder.hi_scanner.base_key_end, Some(0));
    assert!(
        decoder.next().is_none(),
        "zero-width upper cursor must prevent fail-open forward scans"
    );
}

#[test]
fn seek_exhausts_existing_cursor_when_binary_search_fails() {
    let handles = make_handles(16);
    let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 2).unwrap();
    let binary_index_offset_pos = binary_index_offset_field_pos(&bytes);
    let binary_index_len_pos = binary_index_len_field_pos(&bytes);
    let binary_index_step_size_pos = binary_index_step_size_field_pos(&bytes);
    let binary_index_offset = LittleEndian::read_u32(
        &bytes[binary_index_offset_pos..binary_index_offset_pos + core::mem::size_of::<u32>()],
    ) as usize;
    let binary_index_len = LittleEndian::read_u32(
        &bytes[binary_index_len_pos..binary_index_len_pos + core::mem::size_of::<u32>()],
    ) as usize;
    let binary_index_step_size = usize::from(bytes[binary_index_step_size_pos]);
    let mid = binary_index_len / 2;
    let mid_offset = binary_index_offset + (mid * binary_index_step_size);
    let trailer_start = binary_index_offset - 1;
    #[expect(
        clippy::cast_possible_truncation,
        reason = "test fixture keeps trailer_start within u32"
    )]
    let trailer_start_u32 = trailer_start as u32;
    match binary_index_step_size {
        2 => {
            #[expect(
                clippy::cast_possible_truncation,
                reason = "u16 step stores test offset"
            )]
            let trailer_start_u16 = trailer_start_u32 as u16;
            LittleEndian::write_u16(
                &mut bytes[mid_offset..mid_offset + core::mem::size_of::<u16>()],
                trailer_start_u16,
            );
        }
        4 => {
            LittleEndian::write_u32(
                &mut bytes[mid_offset..mid_offset + core::mem::size_of::<u32>()],
                trailer_start_u32,
            );
        }
        _ => panic!("unexpected binary index step size"),
    }

    let block = Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Index),
    };
    let mut decoder = Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::new(&block);

    decoder.lo_scanner.offset = 0;
    decoder.lo_scanner.remaining_in_interval = 0;
    decoder.lo_scanner.base_key_offset = None;
    decoder.lo_scanner.base_key_end = None;

    assert!(
        !decoder.seek(|_, _| false, false),
        "seek should fail when binary-index search probes a malformed restart head"
    );
    assert!(
        decoder.next().is_none(),
        "failed seek must exhaust the old forward cursor state"
    );
}

#[test]
fn seek_upper_exhausts_existing_cursor_when_binary_search_fails() {
    let handles = make_handles(16);
    let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 2).unwrap();
    let binary_index_offset_pos = binary_index_offset_field_pos(&bytes);
    let binary_index_len_pos = binary_index_len_field_pos(&bytes);
    let binary_index_step_size_pos = binary_index_step_size_field_pos(&bytes);
    let binary_index_offset = LittleEndian::read_u32(
        &bytes[binary_index_offset_pos..binary_index_offset_pos + core::mem::size_of::<u32>()],
    ) as usize;
    let binary_index_len = LittleEndian::read_u32(
        &bytes[binary_index_len_pos..binary_index_len_pos + core::mem::size_of::<u32>()],
    ) as usize;
    let binary_index_step_size = usize::from(bytes[binary_index_step_size_pos]);
    let mid = binary_index_len / 2;
    let mid_offset = binary_index_offset + (mid * binary_index_step_size);
    let trailer_start = binary_index_offset - 1;
    #[expect(
        clippy::cast_possible_truncation,
        reason = "test fixture keeps trailer_start within u32"
    )]
    let trailer_start_u32 = trailer_start as u32;
    match binary_index_step_size {
        2 => {
            #[expect(
                clippy::cast_possible_truncation,
                reason = "u16 step stores test offset"
            )]
            let trailer_start_u16 = trailer_start_u32 as u16;
            LittleEndian::write_u16(
                &mut bytes[mid_offset..mid_offset + core::mem::size_of::<u16>()],
                trailer_start_u16,
            );
        }
        4 => {
            LittleEndian::write_u32(
                &mut bytes[mid_offset..mid_offset + core::mem::size_of::<u32>()],
                trailer_start_u32,
            );
        }
        _ => panic!("unexpected binary index step size"),
    }

    let block = Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Index),
    };
    let mut decoder = Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::new(&block);

    decoder.hi_scanner.offset = 0;
    decoder.hi_scanner.ptr_idx = 0;
    decoder.hi_scanner.stack.push(0);
    decoder.hi_scanner.base_key_offset = None;
    decoder.hi_scanner.base_key_end = None;

    assert!(
        !decoder.seek_upper(|_, _| false, false),
        "seek_upper should fail when binary-index search probes a malformed restart head"
    );
    assert!(
        decoder.next_back().is_none(),
        "failed seek_upper must exhaust the old reverse cursor state"
    );
}

#[test]
fn advance_upper_restart_interval_preserves_hard_upper_bound_on_corruption() {
    let handles = make_handles(16);
    let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 2).unwrap();
    let second_interval_rest_key_len_pos = nth_truncated_rest_key_len_field_pos(&bytes, 1);
    bytes[second_interval_rest_key_len_pos] = 0xFF;
    bytes[second_interval_rest_key_len_pos + 1] = 0xFF;
    bytes[second_interval_rest_key_len_pos + 2] = 0x03;

    let block = Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Index),
    };
    let mut decoder = Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::new(&block);
    decoder.hi_scanner.ptr_idx = 0;
    decoder.hi_scanner.stack.clear();
    decoder.hi_scanner.base_key_offset = None;
    decoder.hi_scanner.base_key_end = None;
    decoder.fill_stack();
    assert!(
        !decoder.hi_scanner.stack.is_empty(),
        "fixture should seed initial upper interval"
    );

    decoder.lo_scanner.offset = 0;
    decoder.lo_scanner.remaining_in_interval = 0;
    decoder.lo_scanner.base_key_offset = None;
    decoder.lo_scanner.base_key_end = None;

    assert!(
        !decoder.advance_upper_restart_interval(),
        "advancing into a malformed interval must fail"
    );
    assert_eq!(
        decoder.hi_scanner.base_key_offset,
        Some(0),
        "failed advance must keep an active hard upper bound"
    );
    assert_eq!(decoder.hi_scanner.base_key_end, Some(0));
    assert!(
        decoder.next().is_none(),
        "forward scan must not continue past a failed upper-bound advance"
    );
}

#[test]
fn poison_back_cursor_also_stops_forward_next() {
    let handles = make_handles(8);
    let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 4).unwrap();

    // Corrupt the second interval's truncated rest_key_len so fill_stack fails
    let rest_key_pos = first_truncated_rest_key_len_field_pos(&bytes);
    bytes[rest_key_pos] = 0xFF;
    bytes[rest_key_pos + 1] = 0xFF;
    bytes[rest_key_pos + 2] = 0x03;

    let block = Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Index),
    };
    let mut decoder = Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::new(&block);

    // Seed the hi_scanner at interval 0 so next_back can try to advance
    decoder.hi_scanner.ptr_idx = 0;
    decoder.hi_scanner.stack.clear();
    decoder.hi_scanner.base_key_offset = None;
    decoder.hi_scanner.base_key_end = None;
    decoder.fill_stack();

    // Consume the stack, then next_back tries the previous interval which
    // is corrupted — fill_stack calls poison_back_cursor
    while decoder.consume_stack_top().is_some() {}
    let back = decoder.next_back();
    assert!(
        back.is_none(),
        "next_back must return None on corrupted interval"
    );

    // The critical check: next() must ALSO stop — poison_back_cursor
    // now clamps the upper bound so forward iteration cannot continue
    assert!(
        decoder.next().is_none(),
        "next() must not yield items after poison_back_cursor clamped the upper bound"
    );
}

#[test]
fn binary_index_bounds_accepts_data_block_with_hash_index() {
    let items = [
        InternalValue::from_components(b"a", b"a", 3, crate::ValueType::Value),
        InternalValue::from_components(b"b", b"b", 2, crate::ValueType::Value),
        InternalValue::from_components(b"c", b"c", 1, crate::ValueType::Value),
        InternalValue::from_components(b"d", b"d", 0, crate::ValueType::Value),
    ];
    let bytes = DataBlock::encode_into_vec(&items, 1, 1.33).expect("encode data block");
    let block = Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Data),
    };

    let trailer_offset = Trailer::new(&block).trailer_offset();
    let hash_index_offset_pos =
        trailer_offset + 1 + 1 + (2 * core::mem::size_of::<u32>()) + core::mem::size_of::<u32>();
    let hash_index_offset = LittleEndian::read_u32(
        &block.data[hash_index_offset_pos..hash_index_offset_pos + core::mem::size_of::<u32>()],
    );
    assert!(hash_index_offset > 0, "fixture must encode a hash index");

    let mut decoder = Decoder::<InternalValue, DataBlockParsedItem>::new(&block);
    assert!(decoder.binary_index_bounds().is_some());
    assert!(decoder.seek(|_, _| true, false));
}

#[test]
fn binary_index_bounds_rejects_hash_index_spilling_past_trailer() {
    let items = [
        InternalValue::from_components(b"a", b"a", 3, crate::ValueType::Value),
        InternalValue::from_components(b"b", b"b", 2, crate::ValueType::Value),
        InternalValue::from_components(b"c", b"c", 1, crate::ValueType::Value),
        InternalValue::from_components(b"d", b"d", 0, crate::ValueType::Value),
    ];
    let mut bytes = DataBlock::encode_into_vec(&items, 1, 1.33).expect("encode data block");
    let hash_index_offset_pos = hash_index_offset_field_pos(&bytes);
    let hash_index_offset = LittleEndian::read_u32(
        &bytes[hash_index_offset_pos..hash_index_offset_pos + core::mem::size_of::<u32>()],
    );
    assert!(hash_index_offset > 0, "fixture must encode a hash index");

    let hash_index_len_pos = hash_index_len_field_pos(&bytes);
    LittleEndian::write_u32(
        &mut bytes[hash_index_len_pos..hash_index_len_pos + core::mem::size_of::<u32>()],
        u32::MAX,
    );

    let block = Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Data),
    };
    assert!(
        matches!(
            Decoder::<InternalValue, DataBlockParsedItem>::try_new(&block),
            Err(crate::Error::InvalidTrailer)
        ),
        "hash index spilling past trailer must be rejected as InvalidTrailer",
    );
}

#[test]
fn binary_index_bounds_rejects_hash_index_ending_before_trailer() {
    let items = [
        InternalValue::from_components(b"a", b"a", 3, crate::ValueType::Value),
        InternalValue::from_components(b"b", b"b", 2, crate::ValueType::Value),
        InternalValue::from_components(b"c", b"c", 1, crate::ValueType::Value),
        InternalValue::from_components(b"d", b"d", 0, crate::ValueType::Value),
    ];
    let mut bytes = DataBlock::encode_into_vec(&items, 1, 1.33).expect("encode data block");
    let hash_index_len_pos = hash_index_len_field_pos(&bytes);
    let hash_index_len = LittleEndian::read_u32(
        &bytes[hash_index_len_pos..hash_index_len_pos + core::mem::size_of::<u32>()],
    );
    assert!(
        hash_index_len > 0,
        "fixture must encode a non-empty hash index"
    );
    LittleEndian::write_u32(
        &mut bytes[hash_index_len_pos..hash_index_len_pos + core::mem::size_of::<u32>()],
        hash_index_len - 1,
    );

    let block = Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Data),
    };
    assert!(
        matches!(
            Decoder::<InternalValue, DataBlockParsedItem>::try_new(&block),
            Err(crate::Error::InvalidTrailer)
        ),
        "hash index ending before trailer must be rejected as InvalidTrailer",
    );
}

#[test]
fn binary_index_bounds_rejects_zero_length() {
    let handles = make_handles(4);
    let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 2).unwrap();

    // Zero out binary_index_len in the trailer
    let bi_len_pos = binary_index_len_field_pos(&bytes);
    LittleEndian::write_u32(
        &mut bytes[bi_len_pos..bi_len_pos + core::mem::size_of::<u32>()],
        0,
    );

    let block = Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Index),
    };
    assert!(
        matches!(
            Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::try_new(&block),
            Err(crate::Error::InvalidTrailer)
        ),
        "zero-length binary index must be rejected as InvalidTrailer",
    );
}

#[test]
fn seek_rejects_eof_binary_index_offset() {
    let handles = make_handles(4);
    let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 2).unwrap();

    // Find the binary index region and tamper the first entry to point at data.len()
    let bi_offset_pos = binary_index_offset_field_pos(&bytes);
    let bi_len_pos = binary_index_len_field_pos(&bytes);
    let bi_step_pos = binary_index_step_size_field_pos(&bytes);
    let bi_offset =
        LittleEndian::read_u32(&bytes[bi_offset_pos..bi_offset_pos + core::mem::size_of::<u32>()])
            as usize;
    let step = usize::from(bytes[bi_step_pos]);
    let bi_len =
        LittleEndian::read_u32(&bytes[bi_len_pos..bi_len_pos + core::mem::size_of::<u32>()])
            as usize;

    // Write data.len() into every binary index slot so seek lands at EOF
    let data_len = bytes.len();
    for i in 0..bi_len {
        let slot = bi_offset + i * step;
        match step {
            2 => {
                #[expect(clippy::cast_possible_truncation, reason = "test block is small")]
                let val = data_len as u16;
                LittleEndian::write_u16(&mut bytes[slot..slot + 2], val);
            }
            4 => {
                #[expect(clippy::cast_possible_truncation, reason = "test block is small")]
                let val = data_len as u32;
                LittleEndian::write_u32(&mut bytes[slot..slot + 4], val);
            }
            _ => panic!("unexpected step size"),
        }
    }

    let block = Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Index),
    };
    let mut decoder = Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::new(&block);

    // Must not panic — reader_at rejects EOF offsets
    let result = decoder.seek(
        |key, _| key >= b"adj:out:vertex-0001:edge-0002".as_slice(),
        false,
    );
    assert!(
        !result,
        "seek must fail when every binary index entry points at data.len()"
    );
}

#[test]
fn try_new_zero_restart_interval_returns_invalid_trailer() {
    let handles = make_handles(4);
    let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 2).unwrap();

    // Locate trailer and zero out restart_interval (first trailer byte).
    let block = Block {
        data: bytes.clone().into(),
        header: Header::test_dummy(BlockType::Index),
    };
    let trailer_offset = Trailer::new(&block).trailer_offset();
    bytes[trailer_offset] = 0;

    let corrupt_block = Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Index),
    };

    assert!(
        matches!(
            Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::try_new(&corrupt_block),
            Err(crate::Error::InvalidTrailer)
        ),
        "zero restart_interval must return InvalidTrailer",
    );
}

#[test]
fn try_new_invalid_binary_index_step_size_returns_invalid_trailer() {
    let handles = make_handles(4);
    let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 2).unwrap();

    let block = Block {
        data: bytes.clone().into(),
        header: Header::test_dummy(BlockType::Index),
    };
    let trailer_offset = Trailer::new(&block).trailer_offset();
    // Corrupt binary_index_step_size (second trailer byte) to an invalid value.
    bytes[trailer_offset + 1] = 3;

    let corrupt_block = Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Index),
    };

    assert!(
        matches!(
            Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::try_new(&corrupt_block),
            Err(crate::Error::InvalidTrailer)
        ),
        "invalid step size must return InvalidTrailer",
    );
}

#[test]
#[should_panic(expected = "valid block trailer")]
fn new_panics_on_zero_restart_interval() {
    let handles = make_handles(4);
    let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 2).unwrap();

    let block = Block {
        data: bytes.clone().into(),
        header: Header::test_dummy(BlockType::Index),
    };
    let trailer_offset = Trailer::new(&block).trailer_offset();
    bytes[trailer_offset] = 0;

    let corrupt_block = Block {
        data: bytes.into(),
        header: Header::test_dummy(BlockType::Index),
    };

    // Must panic, not silently succeed.
    let _ = Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::new(&corrupt_block);
}

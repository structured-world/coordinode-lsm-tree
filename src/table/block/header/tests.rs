use super::*;
use test_log::test;

#[test]
fn block_header_serde_roundtrip() -> crate::Result<()> {
    // Manifest carries the block_flags byte, so this exercises the
    // round-trip of a non-zero flags byte (SST block types omit it).
    let header = Header {
        block_type: BlockType::Manifest,
        block_flags: block_flags::KV_CHECKSUM_FOOTER | block_flags::COMPRESSED,
        stored_checksum: Checksum::from_raw(5),
        data_length: 252_356,
        uncompressed_length: 124_124_124,
    };

    let bytes = header.encode_into_vec();

    assert_eq!(bytes.len(), Header::header_len(BlockType::Manifest));
    assert_eq!(header, Header::decode_from(&mut &bytes[..])?);

    Ok(())
}

#[test]
fn block_header_serde_roundtrip_sst_omits_flags_byte() -> crate::Result<()> {
    // SST block types omit the block_flags byte: a Data header encodes to
    // MIN_LEN bytes and decodes back with block_flags == 0 regardless of
    // the in-memory value (which the writer leaves at 0 for SST blocks).
    let header = Header {
        block_type: BlockType::Data,
        block_flags: 0,
        stored_checksum: Checksum::from_raw(7),
        data_length: 42,
        uncompressed_length: 42,
    };
    let bytes = header.encode_into_vec();
    assert_eq!(bytes.len(), Header::MIN_LEN);
    assert_eq!(header, Header::decode_from(&mut &bytes[..])?);
    Ok(())
}

#[test]
fn block_header_rejects_unknown_block_flags_bit() {
    // `block_flags` is a persisted transform field. A header carrying a
    // bit this build does not define (here the reserved 1 << 4) must be
    // rejected at decode, not silently accepted as a partially-known
    // block. The header + checksum are otherwise valid, so this isolates
    // the flag-mask check from checksum validation. Uses Manifest, which
    // carries the block_flags byte (SST types omit it entirely).
    let header = Header {
        block_type: BlockType::Manifest,
        block_flags: 1 << 4,
        stored_checksum: Checksum::from_raw(5),
        data_length: 10,
        uncompressed_length: 10,
    };
    let bytes = header.encode_into_vec();
    assert!(
        matches!(
            Header::decode_from(&mut &bytes[..]),
            Err(crate::Error::InvalidTag(("block_flags", _))),
        ),
        "decode must reject an unknown block_flags bit",
    );
}

#[test]
#[expect(clippy::indexing_slicing)]
fn block_header_detect_corruption() {
    let header = Header {
        block_type: BlockType::Data,
        block_flags: 0,
        stored_checksum: Checksum::from_raw(5),
        data_length: 252_356,
        uncompressed_length: 124_124_124,
    };

    let mut bytes = header.encode_into_vec();
    // Mutate a header byte (offset 5 is the first checksum byte for a
    // Data header, which omits the block_flags byte). Any header byte flip
    // must be caught by the header checksum.
    bytes[5] += 1;

    assert!(
        matches!(
            Header::decode_from(&mut &bytes[..]),
            Err(crate::Error::ChecksumMismatch { .. }),
        ),
        "did not detect header corruption",
    );
}

/// A binding is undone only at the place it was made: one table's offset,
/// another offset of it, or the same offset of another table all read back a
/// different payload checksum, and an unbound checksum is the payload's own.
#[test]
fn a_bound_checksum_reads_back_only_where_it_was_bound() {
    let payload = Checksum::from_raw(0x0123_4567_89ab_cdef);
    let mut header = Header::test_dummy(BlockType::Data);
    header.bind_checksum(payload, ChecksumAt::table(7, 4096));
    assert_eq!(header.payload_checksum(ChecksumAt::table(7, 4096)), payload);
    assert_ne!(header.payload_checksum(ChecksumAt::table(7, 8192)), payload);
    assert_ne!(header.payload_checksum(ChecksumAt::table(8, 4096)), payload);
    assert_ne!(header.payload_checksum(ChecksumAt::Unbound), payload);

    header.bind_checksum(payload, ChecksumAt::Unbound);
    assert_eq!(header.stored_checksum, payload);
}

/// The binding refuses a misplaced block only because no two places share a
/// modifier, so the places a packing could confuse are pinned apart: the first
/// block of table 0 against an unbound block, a table id against an offset,
/// and neighbouring places at the extremes of both halves.
#[test]
fn distinct_places_never_share_a_modifier() {
    let places = [
        ChecksumAt::Unbound,
        ChecksumAt::table(0, 0),
        ChecksumAt::table(0, 1),
        ChecksumAt::table(1, 0),
        ChecksumAt::table(1, 1),
        ChecksumAt::table(0, u64::MAX - 1),
        ChecksumAt::table(u64::MAX, 0),
        ChecksumAt::table(u64::MAX, u64::MAX - 1),
    ];
    for (i, a) in places.iter().enumerate() {
        for b in places.iter().skip(i + 1) {
            assert_ne!(
                a.modifier(),
                b.modifier(),
                "{a:?} and {b:?} share a modifier"
            );
        }
    }
}

/// Re-stamping a frame moves its binding and leaves its length and payload as
/// they were, so a verbatim copy verifies at the place it lands.
#[test]
fn a_rebound_frame_verifies_at_its_new_place() -> crate::Result<()> {
    let payload = Checksum::from_raw(42);
    let mut header = Header {
        data_length: 3,
        uncompressed_length: 3,
        ..Header::test_dummy(BlockType::Data)
    };
    header.bind_checksum(payload, ChecksumAt::table(1, 100));
    let mut frame = header.encode_into_vec();
    frame.extend_from_slice(b"abc");
    let len = frame.len();

    Header::rebind_frame(
        &mut frame,
        ChecksumAt::table(1, 100),
        ChecksumAt::table(2, 900),
    )?;
    assert_eq!(frame.len(), len);
    assert_eq!(frame.get(len - 3..), Some(&b"abc"[..]));
    let moved = Header::decode_from(&mut &frame[..])?;
    assert_eq!(moved.payload_checksum(ChecksumAt::table(2, 900)), payload);
    assert_ne!(moved.payload_checksum(ChecksumAt::table(1, 100)), payload);
    Ok(())
}

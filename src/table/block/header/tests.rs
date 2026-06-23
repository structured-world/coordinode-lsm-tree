use super::*;
use test_log::test;

#[test]
fn block_header_serde_roundtrip() -> crate::Result<()> {
    // Manifest carries the block_flags byte, so this exercises the
    // round-trip of a non-zero flags byte (SST block types omit it).
    let header = Header {
        block_type: BlockType::Manifest,
        block_flags: block_flags::KV_CHECKSUM_FOOTER | block_flags::COMPRESSED,
        checksum: Checksum::from_raw(5),
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
        checksum: Checksum::from_raw(7),
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
        checksum: Checksum::from_raw(5),
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
        checksum: Checksum::from_raw(5),
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

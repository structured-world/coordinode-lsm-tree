use super::*;

fn plain<'d>() -> Codec<'d> {
    codec(CompressionType::None)
}

fn codec<'d>(compression: CompressionType) -> Codec<'d> {
    Codec {
        compression,
        #[cfg(zstd_any)]
        dictionary: None,
        #[cfg(zstd_any)]
        two_pass_seed: true,
        #[cfg(not(zstd_any))]
        dictionary: core::marker::PhantomData,
    }
}

fn decode(bytes: &[u8], compression: CompressionType) -> crate::Result<DecodedFrame> {
    let mut decoded = 0;
    decode_frame(
        &Slice::from(bytes),
        compression,
        #[cfg(zstd_any)]
        None,
        &mut decoded,
    )
}

/// A block lists every value with its key and sequence number, and each value
/// is read back as its own view of the payload, in the order written.
#[test]
fn block_frame_round_trips_every_value() {
    let values: [&[u8]; 3] = [b"first", b"", b"third value"];
    let keys: [&[u8]; 3] = [b"a", b"bb", b"ccc"];
    let entries: Vec<EntrySpec<'_>> = keys
        .iter()
        .zip(values)
        .enumerate()
        .map(|(i, (key, value))| EntrySpec {
            key,
            seqno: 10 + i as u64,
            value_len: value.len() as u32,
        })
        .collect();
    let payload = values.concat();
    let mut out = Vec::new();
    encode_frame(FrameKind::Block, &entries, &payload, plain(), &mut out).unwrap();

    let frame = decode(&out, CompressionType::None).unwrap();
    assert_eq!(frame.header.kind, FrameKind::Block);
    assert_eq!(frame.header.frame_len(), out.len() as u64);
    assert_eq!(frame.list.len(), 3);
    for (at, (key, value)) in keys.iter().zip(values).enumerate() {
        assert_eq!(frame.key(at), Some(*key));
        assert_eq!(frame.value(at).as_deref(), Some(value));
        assert_eq!(frame.list[at].seqno, 10 + at as u64);
    }
    assert_eq!(frame.key(3), None);
    assert_eq!(frame.value(3), None);
}

/// Compression that does not shrink the payload leaves it stored as it is,
/// flagged raw, so an incompressible value costs its own size plus the header,
/// and still reads back under the file's codec.
#[cfg(feature = "lz4")]
#[test]
fn incompressible_payload_is_stored_raw() {
    // xorshift bytes: nothing for lz4 to match.
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let payload: Vec<u8> = (0..4096)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect();
    let entry = EntrySpec {
        key: b"k",
        seqno: 1,
        value_len: payload.len() as u32,
    };
    let mut out = Vec::new();
    encode_frame(
        FrameKind::Single,
        &[entry],
        &payload,
        codec(CompressionType::Lz4),
        &mut out,
    )
    .unwrap();

    let header = parse_header(&out).unwrap();
    assert!(header.is_raw());
    assert_eq!(header.disk_len as usize, payload.len());
    assert_eq!(
        out.len(),
        FRAME_HEADER_LEN + ENTRY_HEADER_LEN + 1 + payload.len()
    );
    let frame = decode(&out, CompressionType::Lz4).unwrap();
    assert_eq!(frame.value(0).as_deref(), Some(payload.as_slice()));
}

/// A compressible payload is stored compressed, smaller than it is, and decodes
/// back to the same bytes.
#[cfg(feature = "lz4")]
#[test]
fn compressible_payload_is_stored_compressed() {
    let payload = vec![b'x'; 8192];
    let entry = EntrySpec {
        key: b"k",
        seqno: 1,
        value_len: payload.len() as u32,
    };
    let mut out = Vec::new();
    encode_frame(
        FrameKind::Single,
        &[entry],
        &payload,
        codec(CompressionType::Lz4),
        &mut out,
    )
    .unwrap();

    let header = parse_header(&out).unwrap();
    assert!(!header.is_raw());
    assert!((header.disk_len as usize) < payload.len());
    let frame = decode(&out, CompressionType::Lz4).unwrap();
    assert_eq!(frame.value(0).as_deref(), Some(payload.as_slice()));
}

/// A head carries its chunk table uncompressed whatever the file's codec, and
/// the table must cover the value's length exactly.
#[test]
fn head_frame_names_its_chunk_table() {
    let entry = EntrySpec {
        key: b"big",
        seqno: 7,
        value_len: 10_000,
    };
    let mut out = Vec::new();
    encode_frame(
        FrameKind::Head,
        &[entry],
        &head_payload(4096, 3),
        plain(),
        &mut out,
    )
    .unwrap();
    let frame = decode(&out, CompressionType::None).unwrap();
    assert!(frame.header.is_raw());
    assert_eq!(frame.chunk_table().unwrap(), (4096, 3));
    assert_eq!(frame.list[0].value_len, 10_000);

    // Two chunks of 4096 do not cover 10000 bytes.
    encode_frame(
        FrameKind::Head,
        &[entry],
        &head_payload(4096, 2),
        plain(),
        &mut out,
    )
    .unwrap();
    let frame = decode(&out, CompressionType::None).unwrap();
    assert!(matches!(
        frame.chunk_table(),
        Err(crate::Error::InvalidHeader("BlobFrame"))
    ));
}

/// A chunk lists no entries; any other frame's table is not a chunk table.
#[test]
fn chunk_frame_has_no_entries() {
    let mut out = Vec::new();
    encode_frame(FrameKind::Chunk, &[], b"chunk bytes", plain(), &mut out).unwrap();
    let frame = decode(&out, CompressionType::None).unwrap();
    assert_eq!(frame.header.kind, FrameKind::Chunk);
    assert!(frame.list.is_empty());
    assert_eq!(&*frame.payload, b"chunk bytes");
    assert!(frame.chunk_table().is_err());
}

fn sample_block() -> Vec<u8> {
    let entries = [
        EntrySpec {
            key: b"a",
            seqno: 1,
            value_len: 3,
        },
        EntrySpec {
            key: b"b",
            seqno: 2,
            value_len: 3,
        },
    ];
    let mut out = Vec::new();
    encode_frame(FrameKind::Block, &entries, b"onetwo", plain(), &mut out).unwrap();
    out
}

/// A flipped bit in a value is caught by the frame checksum, not served.
#[test]
fn damaged_payload_fails_the_checksum() {
    let mut out = sample_block();
    let last = out.len() - 1;
    out[last] ^= 1;
    assert!(matches!(
        decode(&out, CompressionType::None),
        Err(crate::Error::ChecksumMismatch { .. })
    ));
}

/// A flipped bit in a key is caught by the same checksum.
#[test]
fn damaged_key_fails_the_checksum() {
    let mut out = sample_block();
    out[FRAME_HEADER_LEN + ENTRY_HEADER_LEN] ^= 1;
    assert!(matches!(
        decode(&out, CompressionType::None),
        Err(crate::Error::ChecksumMismatch { .. })
    ));
}

/// A rotted length field fails the header CRC before it can size a read.
#[test]
fn damaged_header_fails_its_crc() {
    let mut out = sample_block();
    out[12] ^= 1; // raw_len
    assert!(matches!(
        parse_header(&out),
        Err(crate::Error::HeaderCrcMismatch { .. })
    ));
}

/// The earlier record layout, and anything else that is not this frame's
/// magic, is not a frame.
#[test]
fn earlier_record_magic_is_refused() {
    let mut out = sample_block();
    out[..4].copy_from_slice(b"BLO4");
    assert!(matches!(
        parse_header(&out),
        Err(crate::Error::InvalidHeader("BlobFrame"))
    ));
}

/// A buffer shorter or longer than the frame its header declares is refused.
#[test]
fn frame_must_be_the_whole_buffer() {
    let out = sample_block();
    assert!(parse_header(&out[..FRAME_HEADER_LEN - 1]).is_err());
    assert!(decode(&out[..out.len() - 1], CompressionType::None).is_err());
    let mut longer = out.clone();
    longer.push(0);
    assert!(decode(&longer, CompressionType::None).is_err());
}

/// An unknown kind is refused, and so is a kind with a count it cannot have.
#[test]
fn kind_and_count_must_agree() {
    assert!(FrameKind::try_from(4).is_err());

    let entry = EntrySpec {
        key: b"a",
        seqno: 1,
        value_len: 1,
    };
    let mut out = Vec::new();
    // A single value frame listing two values.
    encode_frame(FrameKind::Single, &[entry, entry], b"xy", plain(), &mut out).unwrap();
    assert!(matches!(
        parse_header(&out),
        Err(crate::Error::InvalidHeader("BlobFrame"))
    ));
}

/// Values whose declared lengths do not sum to the payload are refused, so no
/// value is served out of another's bytes.
#[test]
fn value_lengths_must_cover_the_payload() {
    let entry = EntrySpec {
        key: b"a",
        seqno: 1,
        value_len: 2,
    };
    let mut out = Vec::new();
    encode_frame(FrameKind::Block, &[entry], b"abc", plain(), &mut out).unwrap();
    assert!(matches!(
        decode(&out, CompressionType::None),
        Err(crate::Error::InvalidHeader("BlobFrame"))
    ));
}

/// A frame of `kind` holding one value under `b"k"` whose payload is `stored`
/// as compressed, declaring `raw_len` decoded bytes, under checksums that
/// hold: what damage or a hostile file the checksums cannot catch looks like.
fn forge(kind: FrameKind, stored: &[u8], raw_len: u32) -> Vec<u8> {
    let mut entries = Vec::new();
    entries.extend_from_slice(&0_u64.to_le_bytes());
    entries.extend_from_slice(&1_u16.to_le_bytes());
    entries.extend_from_slice(&raw_len.to_le_bytes());
    entries.push(b'k');
    let (count, entries_len, disk_len) = (1_u16, entries.len() as u32, stored.len() as u32);
    let crc = header_crc(kind, 0, count, entries_len, raw_len, disk_len);
    let checksum = frame_checksum(&entries, stored, crc);
    let mut out = Vec::new();
    out.extend_from_slice(&FRAME_MAGIC);
    out.extend_from_slice(&[kind as u8, 0]);
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&entries_len.to_le_bytes());
    out.extend_from_slice(&raw_len.to_le_bytes());
    out.extend_from_slice(&disk_len.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&checksum.to_le_bytes());
    out.extend_from_slice(&entries);
    out.extend_from_slice(stored);
    out
}

/// A match reaching 16 bytes back with one byte written: the decoder alone
/// refuses it, which is what keeps it off the value's unwritten bytes.
#[test]
#[cfg(feature = "lz4")]
fn an_lz4_match_before_the_value_is_refused() {
    let stored = [0x10, b'a', 0x10, 0x00, 0x10, b'b'];
    let result = decode(&forge(FrameKind::Single, &stored, 6), CompressionType::Lz4);
    assert!(
        matches!(result, Err(crate::Error::Decompress(_))),
        "expected Decompress error, got: {result:?}",
    );
}

/// One sequence with a match reaching 29 bytes back with one byte written: RLE
/// tables, so the bitstream is the offset's five extra bits and its end marker.
#[test]
#[cfg(zstd_any)]
fn a_zstd_match_before_the_value_is_refused() {
    #[rustfmt::skip]
    let stored = [
        0x28, 0xB5, 0x2F, 0xFD, // magic
        0x20, 0x05,             // single segment, 5 bytes
        0x45, 0x00, 0x00,       // last compressed block of 8 bytes
        0x08, b'a',             // one raw literal
        0x01, 0x54,             // one sequence, three RLE tables
        0x01, 0x05, 0x01,       // literal, offset and match length codes
        0x20,                   // bitstream
    ];
    let result = decode(
        &forge(FrameKind::Single, &stored, 5),
        CompressionType::Zstd(3),
    );
    assert!(
        matches!(result, Err(crate::Error::Decompress(_))),
        "expected Decompress error, got: {result:?}",
    );
}

/// A payload that decompresses to fewer bytes than its header declares is
/// refused after the decompressor ran, and the length it produced is
/// reported all the same: a read that fails there is not counted as having
/// decoded nothing.
#[test]
#[cfg(feature = "lz4")]
fn a_short_decompression_is_refused_and_still_counted() {
    let value = b"0123456789abcdef";
    let stored = lz4_flex::block::compress(value);
    let forged = forge(FrameKind::Single, &stored, 32);
    let mut decoded = 0;
    let result = decode_frame(
        &Slice::from(forged),
        CompressionType::Lz4,
        #[cfg(zstd_any)]
        None,
        &mut decoded,
    );
    assert!(
        matches!(result, Err(crate::Error::Decompress(_))),
        "a value shorter than its declared length must be refused, got: {result:?}",
    );
    assert_eq!(
        decoded,
        value.len(),
        "the decompressor produced the whole value"
    );
}

/// The uncompressed twin: a raw payload of another length than declared is
/// refused, its length reported like a decompressor's output.
#[test]
fn a_raw_payload_of_another_length_is_refused_and_still_counted() {
    let forged = forge(FrameKind::Single, b"0123456789abcdef", 32);
    let mut decoded = 0;
    let result = decode_frame(
        &Slice::from(forged),
        CompressionType::None,
        #[cfg(zstd_any)]
        None,
        &mut decoded,
    );
    assert!(
        matches!(result, Err(crate::Error::InvalidHeader("BlobFrame"))),
        "got: {result:?}"
    );
    assert_eq!(decoded, 16, "the whole stored payload was inspected");
}

/// A flipped bit in the stored header CRC itself is caught, fields intact.
#[test]
fn a_damaged_header_crc_field_is_caught() {
    let mut out = sample_block();
    out[20] ^= 1; // header_crc
    assert!(matches!(
        parse_header(&out),
        Err(crate::Error::HeaderCrcMismatch { .. })
    ));
}

/// A sequence number is under the checksum: a rotted one cannot make an old
/// version read as a newer one.
#[test]
fn a_damaged_seqno_fails_the_checksum() {
    let mut out = sample_block();
    out[FRAME_HEADER_LEN] ^= 1;
    assert!(matches!(
        decode(&out, CompressionType::None),
        Err(crate::Error::ChecksumMismatch { .. })
    ));
}

/// The codec-free check proves what decoding proves short of decompression:
/// it passes a frame as stored, compressed or not, and refuses the damage
/// decoding refuses.
#[test]
fn verify_frame_checks_a_frame_without_its_codec() {
    let out = sample_block();
    assert_eq!(verify_frame(&out).unwrap().kind, FrameKind::Block);
    let mut damaged = out.clone();
    let last = damaged.len() - 1;
    damaged[last] ^= 1;
    assert!(matches!(
        verify_frame(&damaged),
        Err(crate::Error::ChecksumMismatch { .. })
    ));
    assert!(
        verify_frame(&out[..out.len() - 1]).is_err(),
        "a short buffer"
    );
    #[cfg(feature = "lz4")]
    {
        let payload = vec![b'z'; 4096];
        let entry = EntrySpec {
            key: b"k",
            seqno: 1,
            value_len: payload.len() as u32,
        };
        let mut compressed = Vec::new();
        encode_frame(
            FrameKind::Single,
            &[entry],
            &payload,
            codec(CompressionType::Lz4),
            &mut compressed,
        )
        .unwrap();
        assert!(verify_frame(&compressed).is_ok(), "no codec needed");
    }
}

/// A raw frame read under no codec and a compressed frame read under none are
/// told apart: a compressed payload needs its codec to decode.
#[cfg(feature = "lz4")]
#[test]
fn compressed_frame_needs_its_codec() {
    let payload = vec![b'y'; 4096];
    let entry = EntrySpec {
        key: b"k",
        seqno: 1,
        value_len: payload.len() as u32,
    };
    let mut out = Vec::new();
    encode_frame(
        FrameKind::Single,
        &[entry],
        &payload,
        codec(CompressionType::Lz4),
        &mut out,
    )
    .unwrap();
    assert!(decode(&out, CompressionType::None).is_err());
}

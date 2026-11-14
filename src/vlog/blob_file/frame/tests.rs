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
        plain: false,
    }
}

/// The writer sums a frame's entries and payload as two parts, the reader as
/// the one range they make: the two agree at every length, across the input
/// sizes where xxh3 changes how it hashes, or a frame the writer stamps would
/// fail its read.
#[test]
fn a_checksum_in_two_parts_is_the_checksum_of_the_whole() {
    let bytes: Vec<u8> = (0..4_096_u32).map(|at| (at * 31 + 7) as u8).collect();
    for len in [0, 1, 16, 17, 128, 129, 240, 241, 1_024, 4_096] {
        let whole = &bytes[..len];
        for split in [0, len / 3, len] {
            let (entries, payload) = whole.split_at(split);
            assert_eq!(
                frame_checksum_parts(entries, payload, 0xdead_beef),
                frame_checksum(whole, 0xdead_beef),
                "{len} bytes split at {split}"
            );
        }
    }
}

/// A single frame lists its one entry in place: decoding a frame of one value
/// allocates no list for it.
#[test]
fn a_single_frame_holds_its_entry_in_place() {
    let entry = EntrySpec {
        key: b"k",
        seqno: 3,
        value_len: 3,
    };
    let mut out = Vec::new();
    encode_frame(FrameKind::Single, &[entry], b"abc", plain(), &mut out).unwrap();
    let frame = decode(&out, CompressionType::None).unwrap();
    assert!(matches!(frame.list, FrameEntries::One(_)));
    assert_eq!(frame.value(0).as_deref(), Some(&b"abc"[..]));
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

/// A compressed frame's decoded payload is its own buffer, so its entries
/// must not stay a view into the encoded read: a frame in the cache would
/// otherwise keep the whole encoded buffer alive, which its weight, its
/// entries and decoded payload, does not count.
#[cfg(feature = "lz4")]
#[test]
fn a_decoded_compressed_frame_keeps_nothing_of_its_encoded_bytes() {
    let key = vec![b'k'; 200];
    let payload = vec![b'x'; 8192];
    let entry = EntrySpec {
        key: &key,
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
    let encoded = Slice::from(out);
    let mut decoded = 0;
    let frame = decode_frame(
        &encoded,
        CompressionType::Lz4,
        #[cfg(zstd_any)]
        None,
        &mut decoded,
    )
    .unwrap();
    assert!(!frame.header.is_raw(), "the payload is stored compressed");
    let within = |bytes: &[u8]| {
        let range = encoded.as_ptr_range();
        range.contains(&bytes.as_ptr())
    };
    assert!(!within(&frame.entries), "the entries are their own bytes");
    assert!(!within(&frame.payload), "the payload is its own bytes");
    assert_eq!(frame.key(0), Some(&key[..]));
}

/// A frame compressed without a dictionary codec's dictionary says so, and
/// decodes under that codec with no dictionary at hand, and under plain zstd
/// too: a file building its own dictionary writes such frames before it has
/// one, and records plain zstd when it never does. Under a codec that is not
/// zstd the flag names nothing and the frame is refused.
#[cfg(zstd_any)]
#[test]
fn a_plain_frame_decodes_under_its_dictionary_codec_without_the_dictionary() {
    let payload = vec![b'x'; 8192];
    let entry = EntrySpec {
        key: b"k",
        seqno: 1,
        value_len: payload.len() as u32,
    };
    let mut out = Vec::new();
    let mut plain_zstd = codec(CompressionType::Zstd(3));
    plain_zstd.plain = true;
    encode_frame(FrameKind::Single, &[entry], &payload, plain_zstd, &mut out).unwrap();
    assert_eq!(parse_header(&out).unwrap().flags, FLAG_PLAIN);

    let dict_codec = CompressionType::ZstdDict {
        level: 3,
        dict_id: 7,
    };
    let frame = decode(&out, dict_codec).unwrap();
    assert_eq!(frame.value(0).as_deref(), Some(payload.as_slice()));
    let frame = decode(&out, CompressionType::Zstd(3)).unwrap();
    assert_eq!(frame.value(0).as_deref(), Some(payload.as_slice()));
    assert!(decode(&out, CompressionType::None).is_err());
}

/// A flag the format does not define, or a payload flagged both raw and
/// compressed without the dictionary, is refused at the header.
#[test]
fn a_frame_with_flags_it_cannot_hold_is_refused() {
    let entry = EntrySpec {
        key: b"k",
        seqno: 1,
        value_len: 3,
    };
    let mut out = Vec::new();
    encode_frame(FrameKind::Single, &[entry], b"abc", plain(), &mut out).unwrap();
    for flags in [4, FLAG_RAW | FLAG_PLAIN, 0x80] {
        let mut frame = out.clone();
        frame[5] = flags;
        restamp_header_crc(&mut frame);
        assert!(parse_header(&frame).is_err(), "flags {flags:#x}");
    }
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
    let value = vec![b'x'; 10_000];
    let specs: Vec<ChunkSpec> = value
        .chunks(4096)
        .map(|chunk| encode_chunk(chunk, plain()).unwrap().1)
        .collect();
    let mut out = Vec::new();
    encode_frame(
        FrameKind::Head,
        &[entry],
        &head_payload(4096, &specs),
        plain(),
        &mut out,
    )
    .unwrap();
    let frame = decode(&out, CompressionType::None).unwrap();
    assert!(frame.header.is_raw());
    assert_eq!(frame.chunk_table().unwrap(), (4096, 3));
    assert_eq!(frame.list[0].value_len, 10_000);
    assert_eq!(frame.chunk(2).unwrap().raw_len, 10_000 - 2 * 4096);
    assert_eq!(head_chunks(&out).unwrap(), specs);
    assert!(frame.chunk(3).is_err(), "no fourth chunk");

    // Two chunks of 4096 do not cover 10000 bytes.
    encode_frame(
        FrameKind::Head,
        &[entry],
        &head_payload(4096, &specs[..2]),
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

/// A frame of `kind` over these entries and stored payload, its header CRC and
/// checksum holding: what a damaged frame that still checks looks like.
fn sealed(
    kind: FrameKind,
    count: u16,
    flags: u8,
    raw_len: u32,
    entries: &[u8],
    stored: &[u8],
) -> Vec<u8> {
    let entries_len = entries.len() as u32;
    let disk_len = stored.len() as u32;
    let crc = header_crc(kind, flags, count, entries_len, raw_len, disk_len);
    let checksum = frame_checksum_parts(entries, stored, crc);
    let mut out = Vec::new();
    out.extend_from_slice(&FRAME_MAGIC);
    out.extend_from_slice(&[kind as u8, flags]);
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&entries_len.to_le_bytes());
    out.extend_from_slice(&raw_len.to_le_bytes());
    out.extend_from_slice(&disk_len.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&checksum.to_le_bytes());
    out.extend_from_slice(entries);
    out.extend_from_slice(stored);
    out
}

/// One entry: its seqno, key length, value length and key.
fn entry_bytes(key_len: u16, value_len: u32, key: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&7_u64.to_le_bytes());
    out.extend_from_slice(&key_len.to_le_bytes());
    out.extend_from_slice(&value_len.to_le_bytes());
    out.extend_from_slice(key);
    out
}

/// Entries that disagree with the header, though checksummed, are damage: a
/// count that leaves the entries too short, a key of no bytes or past the
/// entries, bytes left past the listed entries, or entries that do not fit
/// the header's length bounds at all.
#[test]
fn malformed_entries_are_refused() {
    let malformed = |bytes: &[u8]| {
        matches!(
            decode(bytes, CompressionType::None),
            Err(crate::Error::InvalidHeader("BlobFrame"))
        )
    };
    let one = entry_bytes(1, 3, b"k");
    let fill = |len: usize| vec![0xAB_u8; len];

    // Two entries announced in the bytes two may take, the second cut short:
    // a first entry with a long key leaves six bytes for the second.
    let mut short = entry_bytes(10, 3, b"0123456789");
    short.extend_from_slice(&fill(6));
    assert!(malformed(&sealed(
        FrameKind::Block,
        2,
        0,
        3,
        &short,
        b"abc"
    )));
    // A key of no bytes, padded to the smallest entry the header allows.
    let mut empty_key = entry_bytes(0, 3, b"");
    empty_key.push(0);
    assert!(malformed(&sealed(
        FrameKind::Single,
        1,
        0,
        3,
        &empty_key,
        b"abc"
    )));
    // A key running past the entries.
    let past = entry_bytes(9, 3, b"k");
    assert!(malformed(&sealed(
        FrameKind::Single,
        1,
        0,
        3,
        &past,
        b"abc"
    )));
    // A byte left past the one entry the header lists.
    let mut trailing = one.clone();
    trailing.push(0);
    assert!(malformed(&sealed(
        FrameKind::Single,
        1,
        0,
        3,
        &trailing,
        b"abc"
    )));
    // Entries shorter than the one entry a single frame lists can take.
    assert!(malformed(&sealed(
        FrameKind::Single,
        1,
        0,
        3,
        &fill(4),
        b"abc"
    )));
    // The frame cut short, and one byte longer than its header says.
    let whole = sealed(FrameKind::Single, 1, 0, 3, &one, b"abc");
    assert!(malformed(&whole[..FRAME_HEADER_LEN + 5]));
    let mut longer = whole.clone();
    longer.push(0);
    assert!(malformed(&longer));
    assert!(decode(&whole, CompressionType::None).is_ok());
}

/// A decoded frame whose list names more than its payload holds has no value
/// there, and a head whose table is cut short has no chunk table: neither
/// panics, both read as nothing.
#[test]
fn a_decoded_frame_inconsistent_with_its_payload_yields_nothing() {
    let header = parse_header(&sealed(
        FrameKind::Single,
        1,
        0,
        3,
        &entry_bytes(1, 3, b"k"),
        b"abc",
    ))
    .unwrap();
    let frame = DecodedFrame {
        header,
        entries: Slice::from(&b"k"[..]),
        list: FrameEntries::One(FrameEntry {
            seqno: 7,
            key: 0..1,
            value_len: 3,
            value: 0..3,
            at: 0,
        }),
        payload: Slice::from(&b"ab"[..]),
    };
    assert_eq!(frame.value(0), None, "a value past the payload");

    // Ten bytes in chunks of four: three rows.
    let row = |raw_len| ChunkSpec {
        raw_len,
        stored_len: raw_len,
        flags: FLAG_RAW,
        checksum: 0,
    };
    let table = head_payload(4, &[row(4), row(4), row(2)]);
    let head_header = parse_header(&sealed(
        FrameKind::Head,
        1,
        FLAG_RAW,
        table.len() as u32,
        &entry_bytes(1, 10, b"k"),
        &table,
    ))
    .unwrap();
    let head = DecodedFrame {
        header: head_header,
        payload: Slice::from(&table[..4]),
        ..frame
    };
    assert!(head.chunk_table().is_err(), "a table cut short");
}

/// Relisting a value at another seqno is for a frame listing one value of its
/// own: a block, which lists many, is refused.
#[test]
fn only_a_frame_of_one_value_is_relisted() {
    let mut out = Vec::new();
    let entries = [
        EntrySpec {
            key: b"a",
            seqno: 1,
            value_len: 1,
        },
        EntrySpec {
            key: b"b",
            seqno: 1,
            value_len: 1,
        },
    ];
    encode_frame(FrameKind::Block, &entries, b"xy", plain(), &mut out).unwrap();
    assert!(matches!(
        relist_prefix(&out, 9, &mut Vec::new()),
        Err(crate::Error::InvalidHeader("BlobFrame"))
    ));
}

/// The decompressor is never reached for a payload stored as it is; asked to
/// decompress under no codec, it refuses rather than hand the bytes back.
#[test]
fn decompressing_under_no_codec_is_refused() {
    assert!(matches!(
        decompress(
            b"abc",
            3,
            CompressionType::None,
            #[cfg(zstd_any)]
            None,
            &mut 0,
        ),
        Err(crate::Error::InvalidHeader("BlobFrame"))
    ));
}

/// A payload that decompresses to another length than the header declares,
/// or does not decompress at all, is damage under every codec, a dictionary
/// one too; compressing under a dictionary of another id is refused.
#[cfg(zstd_any)]
#[test]
fn a_payload_that_decodes_to_the_wrong_length_is_refused() {
    use crate::compression::ZstdDictionary;

    let payload = vec![b'z'; 4096];
    let decode_as = |stored: &[u8], raw_len: usize, compression, dictionary| {
        decompress(stored, raw_len, compression, dictionary, &mut 0)
    };

    let mut out = Vec::new();
    let entry = EntrySpec {
        key: b"k",
        seqno: 1,
        value_len: payload.len() as u32,
    };
    encode_frame(
        FrameKind::Single,
        &[entry],
        &payload,
        codec(CompressionType::Zstd(3)),
        &mut out,
    )
    .unwrap();
    let header = parse_header(&out).unwrap();
    let stored = &out[FRAME_HEADER_LEN + header.entries_len as usize..];
    assert!(matches!(
        decode_as(stored, payload.len() + 1, CompressionType::Zstd(3), None),
        Err(crate::Error::Decompress(_))
    ));

    let dict = ZstdDictionary::new(b"a dictionary of z bytes zzzzzzzzzzzzzzzz");
    let with_dict = CompressionType::ZstdDict {
        level: 3,
        dict_id: dict.id(),
    };
    let dict_codec = Codec {
        compression: with_dict,
        dictionary: Some(&dict),
        two_pass_seed: true,
        plain: false,
    };
    encode_frame(FrameKind::Single, &[entry], &payload, dict_codec, &mut out).unwrap();
    let header = parse_header(&out).unwrap();
    let stored = out[FRAME_HEADER_LEN + header.entries_len as usize..].to_vec();
    assert!(matches!(
        decode_as(&stored, payload.len() + 1, with_dict, Some(&dict)),
        Err(crate::Error::Decompress(_))
    ));
    assert!(matches!(
        decode_as(b"not a zstd frame", payload.len(), with_dict, Some(&dict)),
        Err(crate::Error::Decompress(_))
    ));

    let other = ZstdDictionary::new(b"another dictionary entirely, its id differs");
    let mismatched = Codec {
        compression: with_dict,
        dictionary: Some(&other),
        two_pass_seed: true,
        plain: false,
    };
    assert!(matches!(
        encode_frame(FrameKind::Single, &[entry], &payload, mismatched, &mut out),
        Err(crate::Error::ZstdDictMismatch { got: Some(_), .. })
    ));
}

/// A head naming a value above the size a value may take is malformed, though
/// its table covers that value: a caller sizes its chunk list and its buffer
/// from the table, and a tiny damaged file would otherwise ask for gigabytes.
#[test]
fn a_head_naming_a_value_above_the_cap_is_malformed() {
    let over = u32::try_from(crate::vlog::blob_file::writer::MAX_DECOMPRESSION_SIZE + 1)
        .unwrap_or(u32::MAX);
    let entry = EntrySpec {
        key: b"big",
        seqno: 7,
        value_len: over,
    };
    // One chunk the size of the whole value: the table covers it.
    let table = head_payload(
        over,
        &[ChunkSpec {
            raw_len: over,
            stored_len: over,
            flags: FLAG_RAW,
            checksum: 0,
        }],
    );
    let mut out = Vec::new();
    encode_frame(FrameKind::Head, &[entry], &table, plain(), &mut out).unwrap();
    let frame = decode(&out, CompressionType::None).unwrap();
    assert!(matches!(
        frame.chunk_table(),
        Err(crate::Error::InvalidHeader("BlobFrame"))
    ));
}

/// A chunk carries no header: no frame on disk is a chunk, and a chunk is
/// decoded by the row of its head's table, which its bytes must match.
#[test]
fn a_chunk_decodes_by_its_heads_row() {
    let mut out = Vec::new();
    encode_frame(FrameKind::Chunk, &[], b"chunk bytes", plain(), &mut out).unwrap();
    assert!(
        decode(&out, CompressionType::None).is_err(),
        "a chunk frame on disk is refused"
    );

    let (stored, spec) = encode_chunk(b"chunk bytes", plain()).unwrap();
    assert!(
        stored.is_none(),
        "without a codec the chunk is stored as it is"
    );
    let chunk = decode_chunk(
        &spec,
        &Slice::from(&b"chunk bytes"[..]),
        CompressionType::None,
        #[cfg(zstd_any)]
        None,
        &mut 0,
    )
    .unwrap();
    assert_eq!(chunk.header.kind, FrameKind::Chunk);
    assert_eq!(&*chunk.payload, b"chunk bytes");
    assert!(chunk.chunk_table().is_err());
    assert!(matches!(
        check_chunk(&spec, b"chunk bytez"),
        Err(crate::Error::ChecksumMismatch { .. })
    ));
    assert!(check_chunk(&spec, b"chunk").is_err(), "another length");
}

/// `stored` decoded under `spec` into a buffer of `out_len` bytes, as a whole
/// read of a chunked value decodes each chunk into its place.
fn chunk_into(
    spec: &ChunkSpec,
    stored: &[u8],
    compression: CompressionType,
    #[cfg(zstd_any)] dictionary: Option<&crate::compression::ZstdDictionary>,
    out_len: usize,
) -> crate::Result<Vec<u8>> {
    let mut out = vec![0; out_len];
    decode_chunk_into(
        spec,
        stored,
        compression,
        #[cfg(zstd_any)]
        dictionary,
        &mut out,
        &mut 0,
    )?;
    Ok(out)
}

/// `spec` over other stored bytes: a chunk damaged in a way its checksum
/// does not catch, so the decoder behind it is what refuses it.
fn resealed(spec: ChunkSpec, stored: &[u8]) -> ChunkSpec {
    ChunkSpec {
        stored_len: stored.len() as u32,
        checksum: chunk_checksum(stored),
        ..spec
    }
}

/// A chunk decoded into its place in a whole value takes exactly the place
/// its row gives it, and one stored as it is under no codec must be that
/// long too, though its row names another stored length.
#[test]
fn a_chunk_decoded_into_place_takes_exactly_its_row() {
    let (_, spec) = encode_chunk(b"chunk bytes", plain()).unwrap();
    let decode = |spec: &ChunkSpec, stored: &[u8], out_len| {
        chunk_into(
            spec,
            stored,
            CompressionType::None,
            #[cfg(zstd_any)]
            None,
            out_len,
        )
    };
    assert_eq!(decode(&spec, b"chunk bytes", 11).unwrap(), b"chunk bytes");
    assert!(matches!(
        decode(&spec, b"chunk bytes", 12),
        Err(crate::Error::InvalidHeader("BlobFrame"))
    ));
    // A row of a raw length its stored bytes do not have, the checksum
    // resealed over them.
    let short = ChunkSpec {
        raw_len: 12,
        ..resealed(spec, b"chunk bytes")
    };
    assert!(matches!(
        decode(&short, b"chunk bytes", 12),
        Err(crate::Error::InvalidHeader("BlobFrame"))
    ));
    let mut decoded = 0;
    assert!(matches!(
        decode_chunk(
            &short,
            &Slice::from(&b"chunk bytes"[..]),
            CompressionType::None,
            #[cfg(zstd_any)]
            None,
            &mut decoded,
        ),
        Err(crate::Error::InvalidHeader("BlobFrame"))
    ));
    assert_eq!(decoded, 11, "the stored bytes were inspected");
}

/// An lz4 chunk decodes into its place; one that does not decompress, or
/// decompresses short of its row, is refused, though its checksum holds.
#[cfg(feature = "lz4")]
#[test]
fn an_lz4_chunk_decodes_into_place_and_refuses_damage() {
    let chunk = vec![b'q'; 4_096];
    let (stored, spec) = encode_chunk(&chunk, codec(CompressionType::Lz4)).unwrap();
    // A run of one byte compresses.
    let stored = stored.unwrap();
    let decode = |spec: &ChunkSpec, stored: &[u8]| {
        chunk_into(
            spec,
            stored,
            CompressionType::Lz4,
            #[cfg(zstd_any)]
            None,
            spec.raw_len as usize,
        )
    };
    assert_eq!(decode(&spec, &stored).unwrap(), chunk);

    let garbage = [0xF0_u8; 8];
    assert!(matches!(
        decode(&resealed(spec, &garbage), &garbage),
        Err(crate::Error::Decompress(CompressionType::Lz4))
    ));
    let long = ChunkSpec {
        raw_len: spec.raw_len + 1,
        ..spec
    };
    assert!(matches!(
        decode(&long, &stored),
        Err(crate::Error::Decompress(CompressionType::Lz4))
    ));
}

/// A zstd chunk decodes into its place, with and without a dictionary; one
/// that does not decompress is refused, and a dictionary chunk read without
/// its dictionary names the dictionary it needs.
#[cfg(zstd_any)]
#[test]
fn a_zstd_chunk_decodes_into_place_and_refuses_damage() {
    use crate::compression::ZstdDictionary;

    let chunk = b"the chunk of a value, said over and over. ".repeat(100);
    let decode = |spec: &ChunkSpec, stored: &[u8], compression, dictionary| {
        chunk_into(spec, stored, compression, dictionary, spec.raw_len as usize)
    };
    let garbage = b"not a zstd frame at all";

    let zstd = CompressionType::Zstd(3);
    // Repeated text compresses, with and without the dictionary.
    let (stored, spec) = encode_chunk(&chunk, codec(zstd)).unwrap();
    let stored = stored.unwrap();
    assert_eq!(decode(&spec, &stored, zstd, None).unwrap(), chunk);
    assert!(matches!(
        decode(&resealed(spec, garbage), garbage, zstd, None),
        Err(crate::Error::Decompress(_))
    ));

    let dict = ZstdDictionary::new(&b"the chunk of a value, said over and over. ".repeat(8));
    let with_dict = CompressionType::ZstdDict {
        level: 3,
        dict_id: dict.id(),
    };
    let dict_codec = Codec {
        compression: with_dict,
        dictionary: Some(&dict),
        two_pass_seed: true,
        plain: false,
    };
    let (stored, spec) = encode_chunk(&chunk, dict_codec).unwrap();
    let stored = stored.unwrap();
    assert_eq!(
        decode(&spec, &stored, with_dict, Some(&dict)).unwrap(),
        chunk
    );
    assert!(matches!(
        decode(&spec, &stored, with_dict, None),
        Err(crate::Error::ZstdDictMismatch { got: None, .. })
    ));
    assert!(matches!(
        decode(&resealed(spec, garbage), garbage, with_dict, Some(&dict)),
        Err(crate::Error::Decompress(_))
    ));
}

/// A plain chunk is zstd without the dictionary, which only a zstd file
/// writes: under another codec its row is refused.
#[cfg(all(feature = "lz4", zstd_any))]
#[test]
fn a_plain_chunk_under_a_codec_without_zstd_is_refused() {
    let (_, spec) = encode_chunk(b"chunk bytes", plain()).unwrap();
    let spec = ChunkSpec {
        flags: FLAG_PLAIN,
        ..spec
    };
    assert!(matches!(
        chunk_into(&spec, b"chunk bytes", CompressionType::Lz4, None, 11),
        Err(crate::Error::InvalidHeader("BlobFrame"))
    ));
}

/// A head's table is refused row by row: a chunk stored two ways at once,
/// and a chunk stored as it is that names another length than its own.
#[test]
fn a_heads_table_refuses_a_row_it_cannot_hold() {
    let entry = EntrySpec {
        key: b"big",
        seqno: 7,
        value_len: 8,
    };
    let row = |flags, stored_len| ChunkSpec {
        raw_len: 8,
        stored_len,
        flags,
        checksum: 0,
    };
    for spec in [row(FLAG_RAW | FLAG_PLAIN, 8), row(FLAG_RAW, 7)] {
        let mut out = Vec::new();
        encode_frame(
            FrameKind::Head,
            &[entry],
            &head_payload(8, &[spec]),
            plain(),
            &mut out,
        )
        .unwrap();
        assert!(
            matches!(
                head_chunks(&out),
                Err(crate::Error::InvalidHeader("BlobFrame"))
            ),
            "{spec:?}"
        );
    }

    // A frame of one value has no table at all.
    let mut out = Vec::new();
    encode_frame(FrameKind::Single, &[entry], b"8 bytes!", plain(), &mut out).unwrap();
    assert!(matches!(
        head_chunks(&out),
        Err(crate::Error::InvalidHeader("BlobFrame"))
    ));
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
    let mut longer = out;
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
    let checksum = frame_checksum_parts(&entries, stored, crc);
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

/// A frame checked without decoding, stored as it is, must store the length
/// it declares: its checksums hold, yet a payload shorter than the value is
/// refused, as a decode refuses it.
#[test]
fn an_undecoded_frame_stored_short_of_its_length_is_refused() {
    let short = Slice::from(forge(FrameKind::Single, b"abc", 5));
    assert!(matches!(
        undecoded_frame(&short, CompressionType::None),
        Err(crate::Error::InvalidHeader("BlobFrame"))
    ));
    let whole = Slice::from(forge(FrameKind::Single, b"abcde", 5));
    assert!(undecoded_frame(&whole, CompressionType::None).is_ok());
}

/// Relisting takes one whole frame: bytes past it, or a frame cut short, are
/// refused rather than relisted with the payload out of step.
#[test]
fn only_one_whole_frame_is_relisted() {
    let frame = forge(FrameKind::Single, b"abcde", 5);
    let mut longer = frame.clone();
    longer.push(0);
    for bad in [&longer[..], &frame[..frame.len() - 1]] {
        assert!(matches!(
            relist_prefix(bad, 9, &mut Vec::new()),
            Err(crate::Error::InvalidHeader("BlobFrame"))
        ));
    }
    assert!(relist_prefix(&frame, 9, &mut Vec::new()).unwrap().is_some());
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

/// A frame relisted at another sequence number, its new header and entries
/// followed by its payload as stored, reads back at it with its value and
/// checksum intact; a frame listed at that seqno already needs no prefix.
#[test]
fn a_frame_relisted_at_another_seqno_reads_back_at_it() {
    let entry = EntrySpec {
        key: b"doc",
        seqno: 1,
        value_len: 5,
    };
    let mut out = Vec::new();
    encode_frame(FrameKind::Single, &[entry], b"value", plain(), &mut out).unwrap();

    let mut relisted = Vec::new();
    let payload_at = relist_prefix(&out, 10, &mut relisted).unwrap().unwrap();
    assert_eq!(relisted.len(), payload_at, "the prefix holds no payload");
    relisted.extend_from_slice(&out[payload_at..]);
    let frame = decode(&relisted, CompressionType::None).unwrap();
    assert_eq!(frame.list[0].seqno, 10);
    assert_eq!(frame.key(0), Some(&b"doc"[..]));
    assert_eq!(frame.value(0).as_deref(), Some(&b"value"[..]));

    assert_eq!(
        relist_prefix(&out, 1, &mut Vec::new()).unwrap(),
        None,
        "listed at its seqno already"
    );
}

/// Only a frame listing one value of its own is relisted: a block's values
/// keep the sequence numbers they were written at.
#[test]
fn a_block_is_not_relisted() {
    assert!(matches!(
        relist_prefix(&sample_block(), 10, &mut Vec::new()),
        Err(crate::Error::InvalidHeader(_))
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

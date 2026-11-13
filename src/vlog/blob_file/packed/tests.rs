// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::*;
use crate::vlog::blob_file::frame::{check_frame, decode_frame};

/// A file that compresses nothing.
const UNCOMPRESSED: (CompressionType, bool) = (CompressionType::None, false);

/// The records of `values` under keys `k0`, `k1`, ... and where each starts.
fn records(values: &[&[u8]]) -> (Vec<u8>, Vec<usize>) {
    let mut out = Vec::new();
    let mut starts = Vec::new();
    for (i, value) in values.iter().enumerate() {
        starts.push(out.len());
        let key = alloc::format!("k{i}");
        encode_record(
            &mut out,
            key.as_bytes(),
            i as SeqNo,
            value,
            UNCOMPRESSED,
            None,
        )
        .unwrap();
    }
    (out, starts)
}

/// The packed block over `records`, whose records start at `starts`.
fn frame(records: &[u8], starts: &[usize], raw_len: u32) -> Vec<u8> {
    let mut sums = Vec::new();
    for &at in starts {
        sums.extend_from_slice(&records[at..at + 8]);
    }
    let header = packed_header(
        u16::try_from(starts.len()).unwrap(),
        raw_len,
        u32::try_from(records.len()).unwrap(),
        &sums,
    );
    [header.as_slice(), records].concat()
}

/// A packed block decodes to its values in order, each named by its record's
/// offset.
#[test]
fn a_packed_block_decodes_every_value_in_order() {
    let values: [&[u8]; 3] = [b"first value", b"second", b"the third one"];
    let (bytes, starts) = records(&values);
    let raw = values.iter().map(|v| v.len()).sum::<usize>();
    let frame = Slice::from(frame(&bytes, &starts, u32::try_from(raw).unwrap()));
    let mut decoded = 0;
    let frame = decode_frame(
        &frame,
        CompressionType::None,
        #[cfg(zstd_any)]
        None,
        &mut decoded,
    )
    .unwrap();
    assert_eq!(frame.list.len(), 3);
    assert_eq!(decoded, raw);
    // Stored as they are, the values stay where they lie in the frame read:
    // nothing is gathered into a payload of its own.
    assert_eq!(frame.payload.as_ptr_range(), frame.entries.as_ptr_range());
    for (place, (value, at)) in values.iter().zip(&starts).enumerate() {
        assert_eq!(frame.place_of(u16::try_from(*at).unwrap()), Some(place));
        assert_eq!(&*frame.value(place).unwrap(), *value);
        let key = alloc::format!("k{place}");
        assert_eq!(frame.key(place), Some(key.as_bytes()));
    }
}

/// A record is placed by its offset whether its block's records are of one
/// length, where the offset's share of the block is the place, or of many,
/// where that share names another record; an offset inside a record, or past
/// the block, places nothing. A handle damaged to such an offset must not
/// read a neighbour's value.
#[test]
fn a_record_is_placed_by_its_offset_alone() {
    let even: [&[u8]; 5] = [b"aaaa", b"bbbb", b"cccc", b"dddd", b"eeee"];
    let uneven: [&[u8]; 5] = [
        b"a",
        b"a much longer value than the others",
        b"cc",
        b"",
        b"eeeeeeeeeeee",
    ];
    for values in [even, uneven] {
        let (bytes, starts) = records(&values);
        let raw = values.iter().map(|v| v.len()).sum::<usize>();
        let frame = Slice::from(frame(&bytes, &starts, u32::try_from(raw).unwrap()));
        let mut decoded = 0;
        let frame = decode_frame(
            &frame,
            CompressionType::None,
            #[cfg(zstd_any)]
            None,
            &mut decoded,
        )
        .unwrap();
        for offset in 0..=bytes.len() + 1 {
            let expected = starts.iter().position(|&at| at == offset);
            assert_eq!(
                frame.place_of(u16::try_from(offset).unwrap()),
                expected,
                "offset {offset}"
            );
        }
    }
}

/// A damaged record byte is caught by that record's own checksum on a read of
/// the frame whole, which no longer hashes the records a second time.
#[test]
fn a_damaged_record_fails_the_frame_check() {
    let (bytes, starts) = records(&[b"one value", b"another value"]);
    let mut frame = frame(&bytes, &starts, 22);
    let last = frame.len() - 1;
    frame[last] ^= 1;
    assert!(matches!(
        check_frame(&frame),
        Err(crate::Error::ChecksumMismatch { .. })
    ));
}

/// A record whose value is stored as it is caches as the very bytes read, so
/// caching it allocates nothing, and a hit hands back that value.
#[test]
fn a_record_stored_as_is_caches_as_its_bytes() {
    let (bytes, _) = records(&[b"a value stored as it is"]);
    let bytes = Slice::from(bytes);
    let record = parse_record(&bytes).unwrap();
    let mut decoded = 0;
    let (cached, value) = cached_record(
        &record,
        &bytes,
        CompressionType::None,
        #[cfg(zstd_any)]
        None,
        &mut decoded,
    )
    .unwrap();
    assert_eq!(cached.as_ptr(), bytes.as_ptr(), "the bytes read are kept");
    assert_eq!(cached.len(), cached_record_len(&record));
    assert_eq!(&*cached.slice(value), b"a value stored as it is");
    assert_eq!(
        &*cached_value(&cached, b"k0").unwrap(),
        b"a value stored as it is"
    );
}

/// A compressed value is decoded straight into a record of its own beside its
/// key, of the length announced before the decode, which no longer holds the
/// bytes read: the value the read hands out is a view of that record, not a
/// second buffer copied into it, and a hit hands back the decoded value.
#[test]
#[cfg(feature = "lz4")]
fn a_compressed_record_decodes_into_the_record_it_caches() {
    let value: Vec<u8> = b"the decoded value, ".repeat(16);
    let compressed = lz4_flex::block::compress(&value);
    assert!(compressed.len() < value.len(), "the fixture compresses");
    let mut bytes = Vec::new();
    encode_record(
        &mut bytes,
        b"k0",
        1,
        &value,
        (CompressionType::Lz4, false),
        Some(&compressed),
    )
    .unwrap();
    let bytes = Slice::from(bytes);
    let record = parse_record(&bytes).unwrap();
    let mut decoded = 0;
    let (cached, at) = cached_record(
        &record,
        &bytes,
        CompressionType::Lz4,
        #[cfg(zstd_any)]
        None,
        &mut decoded,
    )
    .unwrap();
    assert_ne!(cached.as_ptr(), bytes.as_ptr(), "a record of its own");
    assert_eq!(cached.len(), cached_record_len(&record));
    assert_eq!(decoded, value.len());
    let handed_out = cached.slice(at);
    assert_eq!(&*handed_out, value.as_slice());
    assert!(
        cached.as_ptr_range().contains(&handed_out.as_ptr()),
        "the value handed out lies in the cached record"
    );
    assert_eq!(&*cached_value(&cached, b"k0").unwrap(), value.as_slice());
}

/// A cached record answers only for its own key: a hash collision on the cache
/// key, or a handle into another value, is refused rather than served.
#[test]
fn a_cached_record_refuses_another_key() {
    let (bytes, _) = records(&[b"value"]);
    let bytes = Slice::from(bytes);
    let record = parse_record(&bytes).unwrap();
    let (cached, _) = cached_record(
        &record,
        &bytes,
        CompressionType::None,
        #[cfg(zstd_any)]
        None,
        &mut 0,
    )
    .unwrap();
    assert!(matches!(
        cached_value(&cached, b"k1"),
        Err(crate::Error::InvalidHeader("BlobRecord"))
    ));
}

/// A packed block whose header names another count of records, or another
/// decoded length, than its records add up to is malformed, though every
/// checksum holds.
#[test]
fn a_header_its_records_do_not_add_up_to_is_refused() {
    let (bytes, starts) = records(&[b"one value", b"another value"]);
    assert!(check_frame(&frame(&bytes, &starts, 22)).is_ok());
    assert!(matches!(
        check_frame(&frame(&bytes, &starts, 21)),
        Err(crate::Error::InvalidHeader("BlobFrame"))
    ));
    assert!(matches!(
        check_frame(&frame(&bytes, &starts[..1], 22)),
        Err(crate::Error::InvalidHeader("BlobFrame"))
    ));
}

/// In a file that compresses nothing, a record stored in another length than
/// its value's is refused as it is read, not handed out short.
#[test]
fn an_uncompressed_record_of_another_length_is_refused() {
    let mut bytes = Vec::new();
    encode_record(
        &mut bytes,
        b"k0",
        0,
        b"the value",
        UNCOMPRESSED,
        Some(b"short"),
    )
    .unwrap();
    let bytes = Slice::from(bytes);
    let record = parse_record(&bytes).unwrap();
    let mut decoded = 0;
    assert!(matches!(
        record_value(
            &record,
            &bytes,
            CompressionType::None,
            #[cfg(zstd_any)]
            None,
            &mut decoded,
        ),
        Err(crate::Error::InvalidHeader("BlobRecord"))
    ));
    assert_eq!(decoded, 5, "the stored bytes were inspected");
}

/// Two records of the same length swapped each still check by their own
/// checksums; the frame's checksum over their checksums in order catches it.
#[test]
fn records_swapped_fail_the_frame_checksum() {
    let (bytes, starts) = records(&[b"aaaa", b"bbbb"]);
    let frame_bytes = frame(&bytes, &starts, 8);
    assert!(
        check_frame(&frame_bytes).is_ok(),
        "the frame as written checks"
    );
    let split = starts[1];
    let swapped = [&bytes[split..], &bytes[..split]].concat();
    let header = &frame_bytes[..crate::vlog::blob_file::frame::FRAME_HEADER_LEN];
    let tampered = [header, swapped.as_slice()].concat();
    assert!(matches!(
        check_frame(&tampered),
        Err(crate::Error::ChecksumMismatch { .. })
    ));
}

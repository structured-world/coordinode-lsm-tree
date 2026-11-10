// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::*;
use crate::vlog::blob_file::frame::{Codec, check_frame, decode_frame};

const UNCOMPRESSED: Codec<'static> = Codec {
    compression: CompressionType::None,
    #[cfg(zstd_any)]
    dictionary: None,
    #[cfg(zstd_any)]
    two_pass_seed: false,
    #[cfg(not(zstd_any))]
    dictionary: core::marker::PhantomData,
    plain: false,
};

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
    for (place, (value, at)) in values.iter().zip(&starts).enumerate() {
        assert_eq!(frame.place_of(u16::try_from(*at).unwrap()), Some(place));
        assert_eq!(&*frame.value(place).unwrap(), *value);
        let key = alloc::format!("k{place}");
        assert_eq!(frame.key(place), Some(key.as_bytes()));
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
    let value = record_value(
        &record,
        &bytes,
        CompressionType::None,
        #[cfg(zstd_any)]
        None,
        &mut decoded,
    )
    .unwrap();
    let cached = cached_record(&record, &bytes, &value);
    assert_eq!(cached.as_ptr(), bytes.as_ptr(), "the bytes read are kept");
    assert_eq!(cached.len(), cached_record_len(&record, &value));
    assert_eq!(
        &*cached_value(&cached, b"k0").unwrap(),
        b"a value stored as it is"
    );
}

/// A value decoded into a buffer of its own is copied beside its key into one
/// record of the length announced, which no longer holds the bytes read, and
/// a hit hands back the decoded value.
#[test]
fn a_decoded_record_caches_as_a_record_of_its_own() {
    let (bytes, _) = records(&[b"the decoded value"]);
    let bytes = Slice::from(bytes);
    let record = parse_record(&bytes).unwrap();
    let value = Slice::from(b"the decoded value".to_vec());
    let cached = cached_record(&record, &bytes, &value);
    assert_ne!(cached.as_ptr(), bytes.as_ptr(), "a record of its own");
    assert_eq!(cached.len(), cached_record_len(&record, &value));
    assert_eq!(
        &*cached_value(&cached, b"k0").unwrap(),
        b"the decoded value"
    );
}

/// A cached record answers only for its own key: a hash collision on the cache
/// key, or a handle into another value, is refused rather than served.
#[test]
fn a_cached_record_refuses_another_key() {
    let (bytes, _) = records(&[b"value"]);
    let bytes = Slice::from(bytes);
    let record = parse_record(&bytes).unwrap();
    let cached = cached_record(&record, &bytes, &bytes.slice(record.stored.clone()));
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

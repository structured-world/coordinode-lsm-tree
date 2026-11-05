// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

#![cfg(zstd_any)]

use super::*;
use crate::vlog::blob_file::frame::{Codec, check_frame, decode_frame};

const UNCOMPRESSED: Codec<'static> = Codec {
    compression: CompressionType::None,
    dictionary: None,
    two_pass_seed: false,
    plain: false,
};

/// The records of `values` under keys `k0`, `k1`, ... and where each starts.
fn records(values: &[&[u8]]) -> (Vec<u8>, Vec<usize>) {
    let mut out = Vec::new();
    let mut starts = Vec::new();
    for (i, value) in values.iter().enumerate() {
        starts.push(out.len());
        let key = alloc::format!("k{i}");
        encode_record(&mut out, key.as_bytes(), i as SeqNo, value, UNCOMPRESSED).unwrap();
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
    let frame = decode_frame(&frame, CompressionType::None, None, &mut decoded).unwrap();
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

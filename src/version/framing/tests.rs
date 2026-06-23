use super::*;
use std::io::Cursor;
// `use test_log::test;` SHADOWS the built-in `#[test]` attribute
// — every `#[test]` in this module is rewritten to
// `#[test_log::test]` by the macro re-export, which wires
// `env_logger` so a failing test prints the per-test log lines.
// This is the same idiom every other test module in the crate
// uses; rustc does NOT flag it as unused.
use test_log::test;

fn roundtrip(payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut scratch = Vec::new();
    write_framed_record(&mut buf, &mut scratch, |out| {
        out.extend_from_slice(payload);
        Ok(())
    })
    .expect("write");
    buf
}

#[test]
fn framed_record_ok_roundtrip() {
    let payload = b"hello world";
    let bytes = roundtrip(payload);

    let mut cursor = Cursor::new(&bytes);
    let mut scratch = Vec::new();
    let outcome = read_framed_record(&mut cursor, u64::MAX, None, &mut scratch).expect("read");
    match outcome {
        FramedRecordOutcome::Ok => assert_eq!(scratch.as_slice(), payload),
        other => panic!("expected Ok, got {other:?}"),
    }
}

#[test]
fn framed_record_checksum_mismatch_detected() {
    let payload = b"hello world";
    let mut bytes = roundtrip(payload);

    // Flip a payload byte (after the 12-byte header).
    bytes[FRAME_HEADER_LEN] ^= 0x01;

    let mut cursor = Cursor::new(&bytes);
    let outcome = read_framed_record(&mut cursor, u64::MAX, None, &mut Vec::new()).expect("read");
    match outcome {
        FramedRecordOutcome::ChecksumMismatch {
            bytes_consumed,
            expected,
            got,
        } => {
            assert_eq!(bytes_consumed, (FRAME_HEADER_LEN + payload.len()) as u64);
            // The header carried the digest of the un-flipped
            // payload; the reader recomputed over the flipped
            // bytes. They must differ — that's the whole point
            // of the test — and the variant must surface both.
            assert_ne!(expected, got);
        }
        other => panic!("expected ChecksumMismatch, got {other:?}"),
    }
}

#[test]
fn framed_record_oversized_len_rejected_as_bad_header() {
    // Hand-craft a header claiming `len = MAX_FRAME_PAYLOAD + 1`.
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(MAX_FRAME_PAYLOAD + 1).to_le_bytes());
    bytes.extend_from_slice(&0u64.to_le_bytes());

    let mut cursor = Cursor::new(&bytes);
    let outcome = read_framed_record(&mut cursor, u64::MAX, None, &mut Vec::new()).expect("read");
    assert!(
        matches!(outcome, FramedRecordOutcome::BadHeader),
        "expected BadHeader for oversized len, got {outcome:?}",
    );
}

#[test]
fn framed_record_len_exceeding_section_bound_classified_as_tail_truncation() {
    // Header claims a 100-byte payload, but the section says only
    // 8 bytes + 1 remain after `len`. That's "len plausible but
    // payload doesn't fit" — a clean tail truncation, NOT a
    // forged header. The reader should report TailTruncation so
    // tolerant modes can keep the prefix; only truly implausible
    // `len` (above MAX_FRAME_PAYLOAD) earns the BadHeader tag.
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&100u32.to_le_bytes());
    bytes.extend_from_slice(&0u64.to_le_bytes());

    let mut cursor = Cursor::new(&bytes);
    let outcome = read_framed_record(&mut cursor, 8 + 1, None, &mut Vec::new()).expect("read");
    assert!(
        matches!(outcome, FramedRecordOutcome::TailTruncation),
        "expected TailTruncation for len > remaining, got {outcome:?}",
    );
}

#[test]
fn framed_record_tail_truncation_at_header() {
    // Empty input → reading the `len` field hits EOF immediately.
    let mut cursor = Cursor::new(Vec::<u8>::new());
    let outcome = read_framed_record(&mut cursor, u64::MAX, None, &mut Vec::new()).expect("read");
    assert!(
        matches!(outcome, FramedRecordOutcome::TailTruncation),
        "expected TailTruncation, got {outcome:?}",
    );
}

#[test]
fn framed_record_tail_truncation_mid_payload() {
    let payload = b"hello world";
    let mut bytes = roundtrip(payload);

    // Truncate to header + half the payload — the reader will
    // get UnexpectedEof inside read_exact and report it as
    // TailTruncation, not ChecksumMismatch.
    bytes.truncate(FRAME_HEADER_LEN + payload.len() / 2);

    let mut cursor = Cursor::new(&bytes);
    let outcome = read_framed_record(&mut cursor, u64::MAX, None, &mut Vec::new()).expect("read");
    assert!(
        matches!(outcome, FramedRecordOutcome::TailTruncation),
        "expected TailTruncation, got {outcome:?}",
    );
}

#[test]
fn framed_record_fixed_len_mismatch_surfaces_lenmismatch() {
    // Writer emitted a 10-byte payload; reader pins the
    // schema to a fixed 33-byte record. `len` is internally
    // consistent (within MAX_FRAME_PAYLOAD, fits the section),
    // but the sizes disagree — that could be either schema
    // drift or a corrupted length field that still fits within
    // plausible bounds; the reader cannot tell the two apart.
    // The reader must surface this as `LenMismatch` (carrying
    // both lengths for diagnostics) rather than the generic
    // `BadHeader`, so the caller can hard-abort regardless of
    // recovery mode.
    let payload = b"ten-byte!!"; // 10 bytes
    let bytes = roundtrip(payload);

    let mut cursor = Cursor::new(&bytes);
    let outcome =
        read_framed_record(&mut cursor, u64::MAX, Some(33), &mut Vec::new()).expect("read");
    match outcome {
        FramedRecordOutcome::LenMismatch { got, expected } => {
            assert_eq!(got, 10, "got should carry the on-disk len");
            assert_eq!(expected, 33, "expected should carry the caller's pin");
        }
        other => panic!("expected LenMismatch {{ got: 10, expected: 33 }}, got {other:?}"),
    }
}

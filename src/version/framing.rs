// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Per-record length-prefixed framing for manifest sections.
//!
//! ## Why framing
//!
//! The pre-framing manifest format wrote each `tables` / `blob_files`
//! record back-to-back with no per-record header. A single corrupt
//! byte anywhere inside the section invalidated every record that
//! followed: the reader had no way to locate the start of the next
//! valid record, so recovery had to choose between (a) aborting the
//! open ([`ManifestRecoveryMode::AbsoluteConsistency`]) or
//! (b) accepting only a clean tail-truncation ([`ManifestRecoveryMode::TolerateCorruptedTailRecords`]).
//!
//! [`ManifestRecoveryMode::PointInTimeRecovery`] and
//! [`ManifestRecoveryMode::SkipAnyCorruptedRecords`] both need to do
//! more than that: PIT wants to stop at the last consistent
//! record-group boundary and accept the prefix; SkipAny wants to
//! skip one bad record and keep reading. Both modes need to know
//! the exact byte length of each record so they can step past one
//! without losing sync with the rest.
//!
//! ## Wire format
//!
//! Each framed record is:
//!
//! ```text
//! +----------------+----------------+-------------------+
//! | len: u32 LE    | xxh3_64: u64 LE | payload: [u8; len] |
//! +----------------+----------------+-------------------+
//! ```
//!
//! - `len` is the size of `payload` (does NOT include the 12-byte
//!   header itself). A `len` value larger than the section's
//!   remaining capacity is treated as corruption — the reader does
//!   not trust it for skipping.
//! - `xxh3_64` is `xxh3_64(payload)`. The 64-bit variant gives a
//!   ≈ 2⁻⁶⁴ false-positive collision rate per record, matching the
//!   integrity bar of the rest of the on-disk format.
//! - `payload` is the same bytes the pre-framing writer emitted
//!   for that record. Migration cost is zero on the payload schema;
//!   only the surrounding 12 bytes are new.
//!
//! ## Trade-off
//!
//! 12 bytes of header per record. For a `tables` section's 33-byte
//! table record this is ~36% overhead; for a `blob_files` section's
//! 25-byte record it is ~48%. The manifest is small (KiB-scale even
//! for trees with tens of thousands of tables), so the absolute
//! cost is negligible. The recovery flexibility — per-record skip,
//! exact record-group boundaries — is worth the overhead.
//!
//! [`ManifestRecoveryMode::AbsoluteConsistency`]: crate::config::ManifestRecoveryMode::AbsoluteConsistency
//! [`ManifestRecoveryMode::TolerateCorruptedTailRecords`]: crate::config::ManifestRecoveryMode::TolerateCorruptedTailRecords
//! [`ManifestRecoveryMode::PointInTimeRecovery`]: crate::config::ManifestRecoveryMode::PointInTimeRecovery
//! [`ManifestRecoveryMode::SkipAnyCorruptedRecords`]: crate::config::ManifestRecoveryMode::SkipAnyCorruptedRecords

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

/// Size of the framing header in bytes (4 B `len` + 8 B `xxh3_64`).
pub(crate) const FRAME_HEADER_LEN: usize = 4 + 8;

/// Hard cap on `len` — keeps an obviously-forged value from
/// triggering an allocation that exceeds reasonable manifest
/// record size. The largest legitimate record today is the
/// `tables` per-table entry at 33 bytes; even a hypothetical
/// future record with a comparator name string is bounded by the
/// `comparator_name` length cap upstream. 64 KiB is a generous
/// ceiling that still cuts off any `len` that would otherwise
/// trigger a multi-megabyte allocation on a corrupt header.
pub(crate) const MAX_FRAME_PAYLOAD: u32 = 64 * 1024;

/// Writes a framed record: writes the 12-byte header, then the
/// closure-provided payload, then patches the header with the
/// actual payload length and XXH3-64 digest.
///
/// `payload_fn` is invoked once with a temporary buffer; on
/// return, the buffer's contents become the record body. This
/// keeps the call site readable (no manual buf-then-len bookkeeping)
/// while still allowing the writer to compute `len` + checksum
/// from the actual emitted bytes rather than a caller-declared
/// size.
///
/// # Errors
///
/// Returns the I/O error from `writer` if any write fails, or
/// surfaces any error returned by `payload_fn`.
pub(crate) fn write_framed_record<W, F>(writer: &mut W, payload_fn: F) -> crate::Result<()>
where
    W: Write,
    F: FnOnce(&mut Vec<u8>) -> crate::Result<()>,
{
    let mut payload: Vec<u8> = Vec::new();
    payload_fn(&mut payload)?;

    #[expect(
        clippy::cast_possible_truncation,
        reason = "manifest records are bounded by MAX_FRAME_PAYLOAD = 64 KiB, which fits in u32"
    )]
    let len = payload.len() as u32;
    let digest = xxhash_rust::xxh3::xxh3_64(&payload);

    writer.write_u32::<LittleEndian>(len)?;
    writer.write_u64::<LittleEndian>(digest)?;
    writer.write_all(&payload)?;

    Ok(())
}

/// Result of [`read_framed_record`] — exposes the per-record
/// outcome so the caller can apply mode-specific recovery policy.
#[derive(Debug)]
pub(crate) enum FramedRecordOutcome {
    /// The header decoded, payload was read in full, and the
    /// XXH3-64 digest matched. The contained `Vec<u8>` is the
    /// record's payload bytes — caller decodes it into its
    /// per-section type.
    Ok(Vec<u8>),

    /// The header decoded with a plausible `len`, the payload was
    /// read in full, but the XXH3-64 digest disagreed with
    /// `xxh3_64(payload)`. The header itself stays internally
    /// consistent (len fits the section), so callers operating in
    /// SkipAny mode know how many bytes were consumed and can
    /// continue reading after the skip. The `bytes_consumed` field
    /// is `FRAME_HEADER_LEN + len`.
    ChecksumMismatch { bytes_consumed: u64 },

    /// The header's `len` field cannot be trusted (it exceeds
    /// [`MAX_FRAME_PAYLOAD`] or the section's remaining bytes). The
    /// reader has NOT advanced past the header; callers must
    /// surrender per-record granularity for the rest of the
    /// section and fall back to a section-level recovery strategy
    /// (typically: drop the rest of the section under SkipAny,
    /// abort under stricter modes).
    BadHeader,

    /// EOF was hit before the header or payload could be read in
    /// full. Always tail-truncation; recovery policy is the same
    /// as a count-overrun in the pre-framing format.
    TailTruncation,
}

/// Reads one framed record. Never panics or aborts on bad bytes —
/// the outcome variant tells the caller what happened and how
/// many bytes (if any) were consumed.
///
/// `remaining_in_section` lets the reader reject a `len` value
/// that exceeds the section payload bound, catching the case
/// where the header itself is corrupt and the length field points
/// well past the legitimate end. Pass `u64::MAX` if the section
/// boundary is not known.
///
/// # Errors
///
/// Returns the underlying [`std::io::Error`] when a read fails for
/// reasons other than EOF (the EOF case maps to
/// [`FramedRecordOutcome::TailTruncation`]). Decode-time errors
/// (checksum mismatch, oversized header) are surfaced via the
/// returned [`FramedRecordOutcome`] variant rather than `Err`.
pub(crate) fn read_framed_record<R: Read>(
    reader: &mut R,
    remaining_in_section: u64,
) -> crate::Result<FramedRecordOutcome> {
    let len = match reader.read_u32::<LittleEndian>() {
        Ok(n) => n,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Ok(FramedRecordOutcome::TailTruncation);
        }
        Err(e) => return Err(e.into()),
    };

    if len > MAX_FRAME_PAYLOAD || u64::from(len) + 8 > remaining_in_section {
        // The header `len` is implausible: it would either point
        // past the end of the section, or claim a payload larger
        // than any legitimate manifest record. We cannot trust it
        // to skip — anything that follows might or might not be
        // the next record header. The caller is told to fall
        // back to section-level recovery; we have only consumed
        // the 4 bytes of `len` so far, but advancing the reader
        // further on a forged header would compound the damage.
        return Ok(FramedRecordOutcome::BadHeader);
    }

    let digest_expected = match reader.read_u64::<LittleEndian>() {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Ok(FramedRecordOutcome::TailTruncation);
        }
        Err(e) => return Err(e.into()),
    };

    let mut payload = vec![0u8; len as usize];
    match reader.read_exact(&mut payload) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Ok(FramedRecordOutcome::TailTruncation);
        }
        Err(e) => return Err(e.into()),
    }

    let digest_actual = xxhash_rust::xxh3::xxh3_64(&payload);
    if digest_actual == digest_expected {
        Ok(FramedRecordOutcome::Ok(payload))
    } else {
        #[expect(
            clippy::cast_lossless,
            reason = "FRAME_HEADER_LEN is a small const, the cast is a tag-along"
        )]
        let bytes_consumed = FRAME_HEADER_LEN as u64 + u64::from(len);
        Ok(FramedRecordOutcome::ChecksumMismatch { bytes_consumed })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use test_log::test;

    fn roundtrip(payload: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        write_framed_record(&mut buf, |out| {
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
        let outcome = read_framed_record(&mut cursor, u64::MAX).expect("read");
        match outcome {
            FramedRecordOutcome::Ok(decoded) => assert_eq!(decoded, payload),
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
        let outcome = read_framed_record(&mut cursor, u64::MAX).expect("read");
        match outcome {
            FramedRecordOutcome::ChecksumMismatch { bytes_consumed } => {
                assert_eq!(bytes_consumed, (FRAME_HEADER_LEN + payload.len()) as u64);
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
        let outcome = read_framed_record(&mut cursor, u64::MAX).expect("read");
        assert!(
            matches!(outcome, FramedRecordOutcome::BadHeader),
            "expected BadHeader for oversized len, got {outcome:?}",
        );
    }

    #[test]
    fn framed_record_len_exceeding_section_bound_rejected_as_bad_header() {
        // Header claims a 100-byte payload, but the section says only
        // 8 bytes remain after `len`. The header should be flagged
        // before the reader advances to the digest field.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&100u32.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());

        let mut cursor = Cursor::new(&bytes);
        // Remaining section bound = 8 (the digest) + 1, so a
        // 100-byte payload definitely doesn't fit.
        let outcome = read_framed_record(&mut cursor, 8 + 1).expect("read");
        assert!(
            matches!(outcome, FramedRecordOutcome::BadHeader),
            "expected BadHeader for len > remaining, got {outcome:?}",
        );
    }

    #[test]
    fn framed_record_tail_truncation_at_header() {
        // Empty input → reading the `len` field hits EOF immediately.
        let mut cursor = Cursor::new(Vec::<u8>::new());
        let outcome = read_framed_record(&mut cursor, u64::MAX).expect("read");
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
        let outcome = read_framed_record(&mut cursor, u64::MAX).expect("read");
        assert!(
            matches!(outcome, FramedRecordOutcome::TailTruncation),
            "expected TailTruncation, got {outcome:?}",
        );
    }
}

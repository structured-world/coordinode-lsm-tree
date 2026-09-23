// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Per-record length-prefixed framing for manifest sections.
//!
//! ## Why framing
//!
//! The edit log is appended to, so its last record can be cut short by
//! a power loss mid-append. A length and a digest per record are what
//! tell that torn tail (the record runs past the end of the file) apart
//! from damage to a record already committed (it is whole but does not
//! verify): only the first may be rolled back, under
//! [`ManifestRecoveryMode::TolerateCorruptedTailRecords`]; the second
//! fails the open in every mode, [`ManifestRecoveryMode::AbsoluteConsistency`]
//! included. The snapshot's `tables` / `blob_files` records use the same
//! framing, which pins each record's size.
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
//!   header itself). A `len` value larger than the remaining
//!   capacity is treated as `TailTruncation`, not as corruption:
//!   that is the shape a power loss mid-append leaves. Use the
//!   `expected_payload_len` parameter on [`read_framed_record`]
//!   when the record schema has a fixed payload size (table / blob
//!   entries) to pin the `len` structurally.
//! - `xxh3_64` is `xxh3_64(payload)`. The 64-bit variant gives a
//!   ≈ 2⁻⁶⁴ false-positive collision rate per record, matching the
//!   integrity bar of the rest of the on-disk format.
//! - `payload` is the same bytes the pre-framing writer emitted
//!   for that record. Migration cost is zero on the payload schema;
//!   only the surrounding 12 bytes are new.
//!
//! ## Trade-off
//!
//! 12 bytes of header per record. For a `tables` section's 38-byte
//! table record this is ~32% overhead; for a `blob_files` section's
//! 25-byte record it is ~48%. The manifest is small (KiB-scale even
//! for trees with tens of thousands of tables), so the absolute
//! cost is negligible.
//!
//! [`ManifestRecoveryMode::AbsoluteConsistency`]: crate::config::ManifestRecoveryMode::AbsoluteConsistency
//! [`ManifestRecoveryMode::TolerateCorruptedTailRecords`]: crate::config::ManifestRecoveryMode::TolerateCorruptedTailRecords

use crate::io::{LittleEndian, ReadBytesExt, WriteBytesExt};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use crate::io::{Read, Write};

/// Size of the framing header in bytes (4 B `len` + 8 B `xxh3_64`).
pub const FRAME_HEADER_LEN: usize = 4 + 8;

/// Hard cap on `len` — keeps an obviously-forged value from
/// triggering an allocation that exceeds reasonable manifest
/// record size. The largest fixed-size record today is the
/// `tables` per-table entry at 38 bytes; even a hypothetical
/// future record with a comparator name string is bounded by the
/// `comparator_name` length cap upstream. 64 KiB is a generous
/// ceiling that still cuts off any `len` that would otherwise
/// trigger a multi-megabyte allocation on a corrupt header.
pub const MAX_FRAME_PAYLOAD: u32 = 64 * 1024;

/// Writes a framed record. The closure-provided payload is
/// assembled in a temporary `Vec<u8>` first so the `len` and
/// XXH3-64 digest can be computed from the actual emitted bytes;
/// the 12-byte header is then written in a single pass before
/// the payload (no seek/backpatch is involved — both header
/// fields are known by the time the first byte of the header
/// reaches `writer`).
///
/// # Errors
///
/// Returns the I/O error from `writer` if any write fails,
/// surfaces any error returned by `payload_fn`, or returns
/// [`crate::Error::Unrecoverable`] when the payload exceeds
/// [`MAX_FRAME_PAYLOAD`] — emitting an oversized record would
/// produce a frame the reader always rejects as
/// [`FramedRecordOutcome::BadHeader`], silently bricking
/// recovery for that section.
pub fn write_framed_record<W, F>(
    writer: &mut W,
    scratch: &mut Vec<u8>,
    payload_fn: F,
) -> crate::Result<()>
where
    W: Write,
    F: FnOnce(&mut Vec<u8>) -> crate::Result<()>,
{
    // Reuse the caller-provided scratch buffer instead of allocating
    // a fresh `Vec` per record. For a manifest with thousands of
    // tables the previous Vec::new() pattern produced one small heap
    // allocation per record on the hot write path; threading a
    // single scratch through the level/run/table loop drops that to
    // one allocation (the buffer's initial growth) for the entire
    // section. Callers create one Vec at the top of the section
    // and pass the same `&mut` reference to every framed write.
    scratch.clear();
    payload_fn(scratch)?;

    if scratch.len() > MAX_FRAME_PAYLOAD as usize {
        log::error!(
            "write_framed_record refusing to emit oversized payload \
             ({} bytes; MAX_FRAME_PAYLOAD = {})",
            scratch.len(),
            MAX_FRAME_PAYLOAD,
        );
        return Err(crate::Error::Unrecoverable);
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "the explicit MAX_FRAME_PAYLOAD guard above ensures scratch.len() fits in u32"
    )]
    let len = scratch.len() as u32;
    let digest = xxhash_rust::xxh3::xxh3_64(scratch);

    writer.write_u32::<LittleEndian>(len)?;
    writer.write_u64::<LittleEndian>(digest)?;
    writer.write_all(scratch)?;

    Ok(())
}

/// Result of [`read_framed_record`] — exposes the per-record
/// outcome so the caller can apply mode-specific recovery policy.
#[derive(Debug)]
pub enum FramedRecordOutcome {
    /// The header decoded, payload was read in full into the
    /// caller-provided `payload_scratch`, and the XXH3-64 digest
    /// matched. The caller reads the payload bytes by slicing
    /// the scratch buffer they passed in (its length is the
    /// payload length). The scratch buffer is reused across
    /// iterations of the recovery loop, so per-record heap
    /// allocations are zero after its initial growth.
    Ok,

    /// The header decoded with a plausible `len`, the payload was
    /// read in full, but the XXH3-64 digest disagreed with
    /// `xxh3_64(payload)`: a whole record that does not verify, which
    /// is damage to committed bytes rather than a torn write. The
    /// `expected` / `got` digest fields carry the actual XXH3-64
    /// values so callers can surface them in the error.
    ChecksumMismatch { expected: u64, got: u64 },

    /// The header's `len` field is truly implausible — exceeds
    /// [`MAX_FRAME_PAYLOAD`] (64 KiB). By the time this variant is
    /// returned the reader HAS consumed the 4-byte `len` field; the
    /// digest and payload have not been read. The cursor position
    /// is therefore unaligned with both this record and the next,
    /// and callers abort.
    ///
    /// `len` plausible but exceeding the section's remaining bytes
    /// is NOT a `BadHeader` — it's a clean tail truncation and is
    /// surfaced as [`Self::TailTruncation`] instead, so
    /// [`crate::config::ManifestRecoveryMode::TolerateCorruptedTailRecords`]
    /// recovers from a power-loss-mid-record without aborting.
    BadHeader,

    /// The header decoded with a `len` that fits
    /// [`MAX_FRAME_PAYLOAD`] but disagrees with the caller's
    /// fixed-length pin (`expected_payload_len = Some(n)` and
    /// `len != n`). This indicates either writer / reader schema
    /// drift OR corruption in the framed `len` field that happens
    /// to stay within `MAX_FRAME_PAYLOAD`; the reader cannot
    /// distinguish the two from the bytes alone. Either way callers
    /// must hard-abort: silently dropping the section would let
    /// genuine schema drift slip through, and the corrupt-len case
    /// is unrecoverable mid-record anyway. By the time
    /// this variant is returned the reader has consumed the 4-byte
    /// `len` field; the cursor is mid-record and the caller cannot
    /// resume reading after the mismatch.
    LenMismatch {
        /// The `len` value that was declared on disk.
        got: u32,
        /// The `len` the caller required via
        /// `expected_payload_len`.
        expected: u32,
    },

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
/// `expected_payload_len`, when `Some(n)`, pins the record to a
/// fixed payload size: any `len != n` is treated as
/// [`FramedRecordOutcome::LenMismatch`] BEFORE the payload is
/// consumed, so a corrupted-but-plausible `len` (still within
/// `MAX_FRAME_PAYLOAD` and the section bound) is reported as what it
/// is rather than as a checksum failure over the wrong bytes.
///
/// Pass `None` for variable-size records (none currently exist
/// in the manifest, but the parameter is kept open-ended for
/// future record types).
///
/// # Errors
///
/// Returns the underlying [`std::io::Error`] when a read fails for
/// reasons other than EOF (the EOF case maps to
/// [`FramedRecordOutcome::TailTruncation`]). Decode-time errors
/// (checksum mismatch, oversized header) are surfaced via the
/// returned [`FramedRecordOutcome`] variant rather than `Err`.
pub fn read_framed_record<R: Read>(
    reader: &mut R,
    remaining_in_section: u64,
    expected_payload_len: Option<u32>,
    payload_scratch: &mut Vec<u8>,
) -> crate::Result<FramedRecordOutcome> {
    let len = match reader.read_u32::<LittleEndian>() {
        Ok(n) => n,
        Err(e) if e.kind() == crate::io::ErrorKind::UnexpectedEof => {
            return Ok(FramedRecordOutcome::TailTruncation);
        }
        Err(e) => return Err(e.into()),
    };

    if len > MAX_FRAME_PAYLOAD {
        // `len` exceeds the sanity bound (64 KiB). This is a truly
        // implausible value — no legitimate record approaches it, so
        // the header itself is forged. We cannot trust `len` to skip
        // past this record; the caller is told to fall back to
        // section-level recovery. By this point we have already
        // consumed the 4 bytes of `len`, but that is acceptable
        // because a BadHeader signal tells the caller to surrender
        // per-record granularity for the rest of the section.
        return Ok(FramedRecordOutcome::BadHeader);
    }

    // Fixed-size record pin: caller said the payload MUST be
    // exactly `expected` bytes, but the on-disk `len` says
    // otherwise. The on-disk `len` fits MAX_FRAME_PAYLOAD and the
    // section bound, so it could be either schema drift (writer /
    // reader format disagreement) or in-record corruption of the
    // length field within plausible bounds — the reader cannot
    // tell the two apart. Surface it as a distinct `LenMismatch`
    // variant (rather than `BadHeader`) so callers hard-abort
    // regardless of recovery mode, while truly forged headers
    // (len > MAX_FRAME_PAYLOAD) still go through the tolerant-mode
    // policy.
    if let Some(expected) = expected_payload_len
        && len != expected
    {
        return Ok(FramedRecordOutcome::LenMismatch { got: len, expected });
    }

    // `len` is plausible but doesn't fit in the remaining section
    // bytes: this is a clean tail truncation, not a forged header.
    // A power-loss between the writer committing `len` and the
    // payload landing produces exactly this shape; calling it
    // `BadHeader` would make `TolerateCorruptedTailRecords`
    // wrongly abort. Surface as `TailTruncation` instead — only
    // truly implausible `len` (above) gets the `BadHeader` tag.
    //
    // `remaining_in_section` is the caller's "bytes available
    // starting at the record's first byte", inclusive of the
    // 4-byte `len` we just read. So the full record needs
    // `FRAME_HEADER_LEN (12) + len` bytes to fit, not `8 + len`.
    if u64::from(len) + FRAME_HEADER_LEN as u64 > remaining_in_section {
        return Ok(FramedRecordOutcome::TailTruncation);
    }

    let digest_expected = match reader.read_u64::<LittleEndian>() {
        Ok(d) => d,
        Err(e) if e.kind() == crate::io::ErrorKind::UnexpectedEof => {
            return Ok(FramedRecordOutcome::TailTruncation);
        }
        Err(e) => return Err(e.into()),
    };

    // Resize the caller's scratch buffer to `len` bytes and read
    // the payload directly into it. Avoids the per-record
    // Vec::new() allocation that the previous shape paid — for a
    // recovery of N records this drops from N allocs to one
    // (the scratch buffer's initial growth).
    payload_scratch.resize(len as usize, 0);
    match reader.read_exact(payload_scratch) {
        Ok(()) => {}
        // Two cfg arms, not one: under `std`, `crate::io::Read` is a method-less
        // supertrait alias, so `read_exact` resolves to `std::io::Read` and
        // `e.kind()` is `std::io::ErrorKind`; under no_std it is
        // `crate::io::ErrorKind`. A single arm would not type-check on both.
        #[cfg(feature = "std")]
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Ok(FramedRecordOutcome::TailTruncation);
        }
        #[cfg(not(feature = "std"))]
        Err(e) if e.kind() == crate::io::ErrorKind::UnexpectedEof => {
            return Ok(FramedRecordOutcome::TailTruncation);
        }
        Err(e) => return Err(e.into()),
    }

    let digest_actual = xxhash_rust::xxh3::xxh3_64(payload_scratch);
    if digest_actual == digest_expected {
        Ok(FramedRecordOutcome::Ok)
    } else {
        Ok(FramedRecordOutcome::ChecksumMismatch {
            expected: digest_expected,
            got: digest_actual,
        })
    }
}

#[cfg(test)]
#[expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test code: explicit panics and direct indexing keep test \
              setup readable; values are inline-known"
)]
mod tests;

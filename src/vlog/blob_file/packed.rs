// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A packed block: small values compressed one by one under the file's codec
//! (with the file's own dictionary once it has one), sharing one frame.
//!
//! The frame is a [`FrameKind::Block`] flagged [`FLAG_PACKED`], with no
//! entries; its payload is records back to back:
//!
//! ```text
//! [checksum; 8][seqno; 8][key len; 2][value len; 4][stored len; 4][flags; 1]
//! [key][value as stored]
//! ```
//!
//! A value's handle names its frame and its record's offset in the payload,
//! and the table its record's length, so a point read reads that record
//! alone and decodes that value alone: the neighbours a block compresses a
//! value with are what the file's dictionary gives it here. `checksum` is
//! xxh3-64 over the rest of the record. The frame's own checksum is xxh3-128
//! over its records' checksums in order, seeded with its header's CRC: the
//! records' bytes are each hashed once, by their own checksums, and a read of
//! the frame whole checks every record and that they are the frame's.

use super::frame::{
    Codec, DecodedFrame, FLAG_PLAIN, FLAG_RAW, FRAME_HEADER_LEN, FrameEntries, FrameEntry,
    FrameHeader, FrameKind,
};
use crate::{CompressionType, SeqNo, Slice};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::ops::Range;

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::indexing_slicing, reason = "test code")]
mod tests;

/// A block whose values are records compressed one by one.
pub(crate) const FLAG_PACKED: u8 = 4;

/// The fixed part of a record, before its key.
pub(crate) const RECORD_HEADER_LEN: usize = 8 + 8 + 2 + 4 + 4 + 1;

/// The most a packed block's payload holds before its last record starts: a
/// record's offset is a `u16`.
pub(crate) const PACKED_REACH: usize = u16::MAX as usize;

/// One record of a packed block, parsed and checked.
#[derive(Clone, Debug)]
pub(crate) struct Record {
    /// The record's own checksum, as stored.
    pub(crate) checksum: u64,
    pub(crate) seqno: SeqNo,
    /// Where the key lies in the bytes the record was parsed from.
    pub(crate) key: Range<usize>,
    /// The value's decoded length.
    pub(crate) value_len: u32,
    pub(crate) flags: u8,
    /// Where the value as stored lies in those bytes.
    pub(crate) stored: Range<usize>,
    /// The record's length.
    pub(crate) len: usize,
}

/// Appends the record of `value` under `key` and `seqno` to `out`, stored as
/// `compressed`, what compressing it under `codec` made (`None` when that
/// saved nothing), and returns the record's length.
///
/// # Errors
///
/// [`crate::Error::DecompressedSizeTooLarge`] for a value past the cap.
///
/// # Panics
///
/// A key is empty or longer than `u16::MAX`: the writer refuses one first.
pub(crate) fn encode_record(
    out: &mut Vec<u8>,
    key: &[u8],
    seqno: SeqNo,
    value: &[u8],
    codec: Codec<'_>,
    compressed: Option<&[u8]>,
) -> crate::Result<usize> {
    let value_len = super::writer::check_size_cap(value.len())?;
    let flags = if compressed.is_none() && codec.compression != CompressionType::None {
        FLAG_RAW
    } else if compressed.is_some() && codec.plain {
        FLAG_PLAIN
    } else {
        0
    };
    let stored: &[u8] = compressed.unwrap_or(value);
    #[expect(clippy::expect_used, reason = "the writer refuses longer keys first")]
    let key_len = u16::try_from(key.len()).expect("keys are u16 length max");
    #[expect(
        clippy::cast_possible_truncation,
        reason = "compression keeps only what shrank the value, itself within the cap"
    )]
    let stored_len = stored.len() as u32;
    let start = out.len();
    out.extend_from_slice(&[0; 8]);
    out.extend_from_slice(&seqno.to_le_bytes());
    out.extend_from_slice(&key_len.to_le_bytes());
    out.extend_from_slice(&value_len.to_le_bytes());
    out.extend_from_slice(&stored_len.to_le_bytes());
    out.push(flags);
    out.extend_from_slice(key);
    out.extend_from_slice(stored);
    let checksum = xxhash_rust::xxh3::xxh3_64(out.get(start + 8..).unwrap_or_default());
    if let Some(slot) = out.get_mut(start..start + 8) {
        slot.copy_from_slice(&checksum.to_le_bytes());
    }
    Ok(out.len() - start)
}

/// Parses and checks the record at the start of `bytes`.
///
/// # Errors
///
/// [`crate::Error::InvalidHeader`] for a record that runs past `bytes` or is
/// malformed, [`crate::Error::ChecksumMismatch`] for one that does not check.
pub(crate) fn parse_record(bytes: &[u8]) -> crate::Result<Record> {
    let record = record_fields(bytes)?;
    let rest = bytes.get(8..record.len).unwrap_or_default();
    let got = xxhash_rust::xxh3::xxh3_64(rest);
    if got != record.checksum {
        return Err(crate::Error::ChecksumMismatch {
            got: crate::Checksum::from_raw(u128::from(got)),
            expected: crate::Checksum::from_raw(u128::from(record.checksum)),
        });
    }
    Ok(record)
}

/// The fields of the record at the start of `bytes`, its checksum not
/// checked: for a frame whose records were checked whole.
///
/// # Errors
///
/// [`crate::Error::InvalidHeader`] for a record that runs past `bytes` or is
/// malformed.
fn record_fields(bytes: &[u8]) -> crate::Result<Record> {
    let malformed = || crate::Error::InvalidHeader("BlobRecord");
    let field = |at: usize, len: usize| bytes.get(at..at + len).ok_or_else(malformed);
    let u64_at = |at| -> crate::Result<u64> {
        Ok(u64::from_le_bytes(
            field(at, 8)?.try_into().map_err(|_| malformed())?,
        ))
    };
    let u32_at = |at| -> crate::Result<u32> {
        Ok(u32::from_le_bytes(
            field(at, 4)?.try_into().map_err(|_| malformed())?,
        ))
    };
    let checksum = u64_at(0)?;
    let seqno = u64_at(8)?;
    let key_len = usize::from(u16::from_le_bytes(
        field(16, 2)?.try_into().map_err(|_| malformed())?,
    ));
    let value_len = u32_at(18)?;
    let stored_len = u32_at(22)? as usize;
    let flags = *field(26, 1)?.first().ok_or_else(malformed)?;
    let key = RECORD_HEADER_LEN..RECORD_HEADER_LEN + key_len;
    // A stored length off the disk can pass a 32-bit `usize` once added to
    // the key's end: past it, the record is damage, not a wrapped range.
    let stored = key.end..key.end.checked_add(stored_len).ok_or_else(malformed)?;
    let len = stored.end;
    let flags_ok = flags & !(FLAG_RAW | FLAG_PLAIN) == 0 && flags != FLAG_RAW | FLAG_PLAIN;
    let raw_ok = flags & FLAG_RAW == 0 || stored_len == value_len as usize;
    if key_len == 0
        || !flags_ok
        || !raw_ok
        || value_len as usize > super::writer::MAX_DECOMPRESSION_SIZE
    {
        return Err(malformed());
    }
    if bytes.len() < len {
        return Err(malformed());
    }
    Ok(Record {
        checksum,
        seqno,
        key,
        value_len,
        flags,
        stored,
        len,
    })
}

/// The value of `record`, parsed out of `bytes`, decoded under the file's
/// `compression`: a value stored as it is stays a view into `bytes`.
///
/// # Errors
///
/// A decompression that fails or produces another length than the record
/// declares.
pub(crate) fn record_value(
    record: &Record,
    bytes: &Slice,
    compression: CompressionType,
    #[cfg(zstd_any)] dictionary: Option<&crate::compression::ZstdDictionary>,
    decoded: &mut usize,
) -> crate::Result<Slice> {
    #[cfg(zstd_any)]
    let (compression, dictionary) =
        super::frame::frame_codec(record.flags, compression, dictionary)?;
    #[cfg(not(zstd_any))]
    if record.flags & FLAG_PLAIN != 0 {
        return Err(crate::Error::InvalidHeader("BlobRecord"));
    }
    let stored = bytes.slice(record.stored.clone());
    if record.flags & FLAG_RAW != 0 || compression == CompressionType::None {
        *decoded = stored.len();
        if stored.len() != record.value_len as usize {
            return Err(crate::Error::InvalidHeader("BlobRecord"));
        }
        return Ok(stored);
    }
    super::frame::decompress_payload(
        &stored,
        record.value_len as usize,
        compression,
        #[cfg(zstd_any)]
        dictionary,
        decoded,
    )
}

/// The value of `record`, parsed out of `bytes`, decoded under the file's
/// `compression` into `out`, which takes exactly the value and is filled
/// whole: what a read of a whole packed block writes in place, with no
/// buffer of the value's own. A value stored as it is is copied into the
/// unwritten memory as it is; only a decompression's target is zeroed first.
///
/// # Errors
///
/// An `out` of another length than the value, and a decompression that fails
/// or produces another length.
fn record_value_into(
    record: &Record,
    bytes: &[u8],
    compression: CompressionType,
    #[cfg(zstd_any)] dictionary: Option<&crate::compression::ZstdDictionary>,
    out: &mut crate::fs::ReadBuf<'_>,
    decoded: &mut usize,
) -> crate::Result<()> {
    #[cfg(zstd_any)]
    let (compression, dictionary) =
        super::frame::frame_codec(record.flags, compression, dictionary)?;
    #[cfg(not(zstd_any))]
    if record.flags & FLAG_PLAIN != 0 {
        return Err(crate::Error::InvalidHeader("BlobRecord"));
    }
    let stored = bytes.get(record.stored.clone()).unwrap_or_default();
    if record.flags & FLAG_RAW != 0 || compression == CompressionType::None {
        *decoded = stored.len();
        if stored.len() != out.capacity() {
            return Err(crate::Error::InvalidHeader("BlobRecord"));
        }
        out.append(stored);
        return Ok(());
    }
    super::frame::decompress_to(
        stored,
        out.init_mut(),
        compression,
        #[cfg(zstd_any)]
        dictionary,
        decoded,
    )?;
    Ok(out.set_filled(out.capacity())?)
}

/// The length of the record [`cached_record`] makes of `record` and its
/// decoded `value`, which is what it weighs in the cache: known before it is
/// made, so a value the cache would refuse is not copied.
#[must_use]
pub(crate) fn cached_record_len(record: &Record, value: &Slice) -> usize {
    RECORD_HEADER_LEN + record.key.len() + value.len()
}

/// `record`, parsed out of `bytes`, as the cache holds it: a record whose
/// value is stored decoded, so a hit hands out a view of it with nothing to
/// decode and nothing else to allocate. A value stored as it is already is
/// that record, `bytes` itself; a decompressed one is copied into a record of
/// its own beside its key, one allocation that frees `bytes`.
#[must_use]
pub(crate) fn cached_record(record: &Record, bytes: &Slice, value: &Slice) -> Slice {
    let stored = bytes.get(record.stored.clone()).unwrap_or_default();
    if record.len == bytes.len() && core::ptr::eq(stored.as_ptr(), value.as_ptr()) {
        return bytes.clone();
    }
    let key = bytes.get(record.key.clone()).unwrap_or_default();
    // Written straight into the record's own allocation: a `Vec` turned into
    // a `Slice` is copied once more.
    let len = cached_record_len(record, value);
    let mut cached = crate::slice::UnfilledSlice::new(len);
    let mut out = cached.target();
    // The checksum is the record's as read: a cached record is not checked
    // again, its bytes never left memory.
    out.append(&record.checksum.to_le_bytes());
    out.append(&record.seqno.to_le_bytes());
    #[expect(
        clippy::cast_possible_truncation,
        reason = "the key was read through a u16 length"
    )]
    out.append(&(key.len() as u16).to_le_bytes());
    out.append(&record.value_len.to_le_bytes());
    out.append(&record.value_len.to_le_bytes());
    out.append(&[FLAG_RAW]);
    out.append(key);
    out.append(value);
    debug_assert_eq!(
        out.filled(),
        len,
        "a cached record is its header, key and value"
    );
    // SAFETY: the appends above write the header, key and value back to back
    // from the start, `cached_record_len` bytes in all, the slice's length.
    #[expect(unsafe_code, reason = "see safety")]
    unsafe {
        cached.assume_filled()
    }
}

/// The value of `cached`, a record [`cached_record`] made, under `key`.
///
/// # Errors
///
/// [`crate::Error::InvalidHeader`] for a record that is not `key`'s or whose
/// value is not stored decoded.
pub(crate) fn cached_value(cached: &Slice, key: &[u8]) -> crate::Result<Slice> {
    let record = record_fields(cached)?;
    if cached.get(record.key.clone()) != Some(key)
        || record.stored.len() != record.value_len as usize
    {
        return Err(crate::Error::InvalidHeader("BlobRecord"));
    }
    Ok(cached.slice(record.stored))
}

/// Checks the records of the packed block whose header is `header`, each
/// against its own checksum and all of them against the frame's, and lists
/// its values: keys as views into `records`, values where they lie once
/// decoded back to back.
///
/// # Errors
///
/// A record that does not check, records that are not the header's, or a
/// frame checksum that does not match.
pub(crate) fn check_packed(header: &FrameHeader, records: &[u8]) -> crate::Result<FrameEntries> {
    let malformed = || crate::Error::InvalidHeader("BlobFrame");
    let count = usize::from(header.count);
    let mut list = Vec::with_capacity(count);
    // Gathered and hashed in one pass: a seeded xxh3 state fed per record
    // measured 0.6-0.7% more instructions per scan, its secret derived and
    // its stripe buffer kept per frame costing more than this allocation.
    let mut sums = Vec::with_capacity(count * 8);
    let mut at = 0;
    let mut decoded = 0_usize;
    while at < records.len() {
        let record = parse_record(records.get(at..).unwrap_or_default())?;
        sums.extend_from_slice(&record.checksum.to_le_bytes());
        let value_len = record.value_len as usize;
        list.push(FrameEntry {
            seqno: record.seqno,
            key: at + record.key.start..at + record.key.end,
            value_len: record.value_len,
            value: decoded..decoded + value_len,
            at: u32::try_from(at).map_err(|_| malformed())?,
        });
        decoded += value_len;
        at += record.len;
    }
    if list.len() != count || decoded != header.raw_len as usize {
        return Err(malformed());
    }
    let checksum = super::frame::frame_checksum(&sums, header.header_crc);
    if checksum != header.checksum {
        return Err(crate::Error::ChecksumMismatch {
            got: crate::Checksum::from_raw(checksum),
            expected: crate::Checksum::from_raw(header.checksum),
        });
    }
    Ok(FrameEntries::Many(list))
}

/// The packed block whose header is `header`, whose payload, its records, is
/// `records` and whose values `list` lists ([`check_packed`] checked them),
/// decoded whole: every value, for a read of the frame whole (a scan, a
/// collection, a cache fill).
///
/// Each value is decoded straight into its place in the payload, one
/// allocation for the block.
///
/// # Errors
///
/// A record that does not decode to the length it declares, or values that
/// do not add up to the header's decoded length.
pub(crate) fn decode_packed(
    header: FrameHeader,
    records: &Slice,
    list: FrameEntries,
    compression: CompressionType,
    #[cfg(zstd_any)] dictionary: Option<&crate::compression::ZstdDictionary>,
    decoded: &mut usize,
) -> crate::Result<DecodedFrame> {
    let malformed = || crate::Error::InvalidHeader("BlobFrame");
    if compression == CompressionType::None {
        return view_packed(header, records, list, decoded);
    }
    let raw_len = header.raw_len as usize;
    let mut payload = crate::slice::UnfilledSlice::new(raw_len);
    let mut target = payload.target();
    *decoded = 0;
    for entry in list.iter() {
        let rest = records.get(entry.at as usize..).unwrap_or_default();
        let record = record_fields(rest)?;
        let len = record.value_len as usize;
        if target.filled() + len > raw_len {
            return Err(malformed());
        }
        let mut part = target.portion(target.filled(), len);
        let mut produced = 0;
        let written = record_value_into(
            &record,
            rest,
            compression,
            #[cfg(zstd_any)]
            dictionary,
            &mut part,
            &mut produced,
        );
        *decoded += produced;
        written?;
        target.extend_from_portion(len);
    }
    if target.filled() != raw_len {
        return Err(malformed());
    }
    // SAFETY: every byte of the payload was written above, value by value,
    // as `target` counted them, and they add up to its length.
    #[expect(unsafe_code, reason = "see safety")]
    let payload = unsafe { payload.assume_filled() };
    Ok(DecodedFrame {
        header,
        entries: records.clone(),
        list,
        payload,
    })
}

/// [`decode_packed`] of a block of a file that compresses nothing: every value
/// lies in its record as it is, so the payload is `records` itself and each
/// value a view where it lies, nothing gathered.
///
/// # Errors
///
/// A record stored otherwise than as it is.
fn view_packed(
    header: FrameHeader,
    records: &Slice,
    list: FrameEntries,
    decoded: &mut usize,
) -> crate::Result<DecodedFrame> {
    let malformed = || crate::Error::InvalidHeader("BlobFrame");
    let FrameEntries::Many(mut list) = list else {
        return Err(malformed());
    };
    *decoded = 0;
    for entry in &mut list {
        let at = entry.at as usize;
        let record = record_fields(records.get(at..).unwrap_or_default())?;
        if record.flags & FLAG_PLAIN != 0 || record.stored.len() != record.value_len as usize {
            return Err(crate::Error::InvalidHeader("BlobRecord"));
        }
        entry.value = at + record.stored.start..at + record.stored.end;
        *decoded += record.stored.len();
    }
    Ok(DecodedFrame {
        header,
        entries: records.clone(),
        list: FrameEntries::Many(list),
        payload: records.clone(),
    })
}

/// The header of a packed block of `count` records, `raw_len` value bytes
/// decoded, `records_len` bytes long, whose records' checksums are `sums` in
/// order.
pub(crate) fn packed_header(
    count: u16,
    raw_len: u32,
    records_len: u32,
    sums: &[u8],
) -> [u8; FRAME_HEADER_LEN] {
    super::frame::stamp_header(
        FrameKind::Block,
        FLAG_PACKED,
        count,
        0,
        raw_len,
        records_len,
        |crc| super::frame::frame_checksum(sums, crc),
    )
}

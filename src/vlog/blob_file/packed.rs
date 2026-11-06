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

/// Appends the record of `value` under `key` and `seqno` to `out`, the value
/// compressed under `codec` unless that saves nothing, and returns the
/// record's length.
///
/// # Errors
///
/// Propagates the compression; [`crate::Error::DecompressedSizeTooLarge`] for
/// a value past the cap.
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
) -> crate::Result<usize> {
    let value_len = super::writer::check_size_cap(value.len())?;
    let compressed = super::frame::compress_payload(value, codec)?;
    let flags = if compressed.is_none() && codec.compression != CompressionType::None {
        FLAG_RAW
    } else if compressed.is_some() && codec.plain {
        FLAG_PLAIN
    } else {
        0
    };
    let stored: &[u8] = compressed.as_deref().unwrap_or(value);
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
    let stored = key.end..key.end + stored_len;
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

/// What the frame [`record_frame`] builds of `record` and its decoded
/// `value` weighs in the cache, known before it is built.
#[must_use]
pub(crate) fn record_frame_weight(record: &Record, value: &Slice) -> u64 {
    (record.key.len() + value.len() + size_of::<FrameEntry>()) as u64
}

/// The value `value` of `record`, parsed out of `bytes` at offset `index` of
/// its packed block, held as a frame of that one value for the cache: its
/// key, its value decoded, and the place a handle names it by.
#[must_use]
pub(crate) fn record_frame(
    record: &Record,
    bytes: &Slice,
    value: Slice,
    index: u16,
) -> DecodedFrame {
    // Copied out, so a value decompressed out of the record does not keep the
    // record's bytes alive behind a cache weight that leaves them out.
    let key = Slice::from(bytes.get(record.key.clone()).unwrap_or_default());
    #[expect(
        clippy::cast_possible_truncation,
        reason = "a record is a value below the block target and a u16 key"
    )]
    let disk_len = record.len as u32;
    DecodedFrame {
        header: FrameHeader {
            kind: FrameKind::Block,
            flags: FLAG_PACKED,
            count: 1,
            entries_len: 0,
            raw_len: record.value_len,
            disk_len,
            header_crc: 0,
            checksum: u128::from(record.checksum),
        },
        list: FrameEntries::One(FrameEntry {
            seqno: record.seqno,
            key: 0..key.len(),
            value_len: record.value_len,
            value: 0..value.len(),
            at: u32::from(index),
        }),
        entries: key,
        payload: value,
    }
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
/// # Errors
///
/// A record that does not decode to the length it declares.
pub(crate) fn decode_packed(
    header: FrameHeader,
    records: &Slice,
    list: FrameEntries,
    compression: CompressionType,
    #[cfg(zstd_any)] dictionary: Option<&crate::compression::ZstdDictionary>,
    decoded: &mut usize,
) -> crate::Result<DecodedFrame> {
    let mut payload = Vec::with_capacity(header.raw_len as usize);
    *decoded = 0;
    for entry in list.iter() {
        let rest = records.slice(entry.at as usize..records.len());
        let record = record_fields(&rest)?;
        let mut produced = 0;
        let value = record_value(
            &record,
            &rest,
            compression,
            #[cfg(zstd_any)]
            dictionary,
            &mut produced,
        );
        *decoded += produced;
        payload.extend_from_slice(&value?);
    }
    Ok(DecodedFrame {
        header,
        entries: records.clone(),
        list,
        payload: Slice::from(payload),
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

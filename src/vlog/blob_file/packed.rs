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
//! xxh3-64 over the rest of the record, which a read of the record alone
//! checks. The frame's own checksum is xxh3-128 over its records as they lie,
//! seeded with its header's CRC, as any frame's is over its payload: a read
//! of the frame whole checks that one, in one pass over the block, and reads
//! the records' fields without hashing each again. A hash per record of a few
//! hundred bytes cost a scan two and a half times what the block hashed whole
//! does, its setup and finish paid per value.

use super::frame::{
    DecodedFrame, FLAG_PLAIN, FLAG_RAW, FRAME_HEADER_LEN, FrameEntries, FrameEntry, FrameHeader,
    FrameKind,
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
/// `compressed`, what compressing it under a codec of `compression`, `plain`
/// when that is a dictionary codec's level without its dictionary, made
/// (`None` when that saved nothing), and returns the record's length. `out`
/// is left as it was on an error.
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
    (compression, plain): (CompressionType, bool),
    compressed: Option<&[u8]>,
) -> crate::Result<usize> {
    let value_len = super::writer::check_size_cap(value.len())?;
    let flags = if compressed.is_none() && compression != CompressionType::None {
        FLAG_RAW
    } else if compressed.is_some() && plain {
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
    // Room for the whole record at once: the appends below grow nothing, and
    // a growth that fails leaves `out` without a part of a record.
    out.reserve(8 + 8 + 2 + 4 + 4 + 1 + key.len() + stored.len());
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

/// The length of the record [`cached_record`] makes of `record`, which is
/// what it weighs in the cache: known from the record's header before its
/// value is decoded, so a value the cache would refuse is decoded into a
/// buffer of its own and one it takes straight into the cached record.
#[must_use]
pub(crate) fn cached_record_len(record: &Record) -> usize {
    RECORD_HEADER_LEN + record.key.len() + record.value_len as usize
}

/// `record`, parsed out of `bytes`, as the cache holds it, and where its value
/// lies in it: a record whose value is stored decoded, so a hit hands out a
/// view of it with nothing to decode and nothing else to allocate. A value
/// stored as it is already is that record, `bytes` itself; a compressed one
/// is decoded under the file's `compression` straight into a record of its
/// own beside its key, one allocation, which frees `bytes` and which the
/// read's value is a view of as well.
///
/// # Errors
///
/// As [`record_value`].
pub(crate) fn cached_record(
    record: &Record,
    bytes: &Slice,
    compression: CompressionType,
    #[cfg(zstd_any)] dictionary: Option<&crate::compression::ZstdDictionary>,
    decoded: &mut usize,
) -> crate::Result<(Slice, Range<usize>)> {
    #[cfg(zstd_any)]
    let (codec, _) = super::frame::frame_codec(record.flags, compression, dictionary)?;
    #[cfg(not(zstd_any))]
    let codec = compression;
    if record.len == bytes.len() && (record.flags & FLAG_RAW != 0 || codec == CompressionType::None)
    {
        *decoded = record.stored.len();
        if record.stored.len() != record.value_len as usize {
            return Err(crate::Error::InvalidHeader("BlobRecord"));
        }
        return Ok((bytes.clone(), record.stored.clone()));
    }
    let key = bytes.get(record.key.clone()).unwrap_or_default();
    let len = cached_record_len(record);
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
    let at = out.filled();
    let mut value = out.portion(at, record.value_len as usize);
    record_value_into(
        record,
        bytes,
        compression,
        #[cfg(zstd_any)]
        dictionary,
        &mut value,
        decoded,
    )?;
    let filled = value.filled();
    out.extend_from_portion(filled);
    if out.filled() != len {
        return Err(crate::Error::InvalidHeader("BlobRecord"));
    }
    // SAFETY: the header and key were appended from the start and the value
    // decoded right after them, `len` bytes in all as checked above, the
    // slice's length.
    #[expect(unsafe_code, reason = "see safety")]
    let cached = unsafe { cached.assume_filled() };
    Ok((cached, at..len))
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

/// Fails unless `records` are the packed block's whose header is `header`,
/// by the frame's checksum over them.
fn check_records(header: &FrameHeader, records: &[u8]) -> crate::Result<()> {
    let checksum = super::frame::frame_checksum(records, header.header_crc);
    if checksum != header.checksum {
        return Err(crate::Error::ChecksumMismatch {
            got: crate::Checksum::from_raw(checksum),
            expected: crate::Checksum::from_raw(header.checksum),
        });
    }
    Ok(())
}

/// Checks the records of the packed block whose header is `header` against
/// the frame's checksum, and lists its values: keys as views into `records`,
/// values where they lie once decoded back to back.
///
/// # Errors
///
/// A frame checksum that does not match, or records that are not the
/// header's.
pub(crate) fn check_packed(header: &FrameHeader, records: &[u8]) -> crate::Result<FrameEntries> {
    let malformed = || crate::Error::InvalidHeader("BlobFrame");
    check_records(header, records)?;
    let count = usize::from(header.count);
    let mut list = Vec::with_capacity(count);
    let mut at = 0;
    let mut decoded = 0_usize;
    while at < records.len() {
        let record = record_fields(records.get(at..).unwrap_or_default())?;
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
    Ok(FrameEntries::Many(list))
}

/// The packed block whose header is `header` and whose payload, its records,
/// is `records`, checked as [`check_packed`] checks it and decoded whole,
/// each record parsed once: every value, for a read of the frame whole (a
/// scan, a collection, a cache fill).
///
/// Each value is decoded straight into its place in the payload, one
/// allocation for the block. In a file that compresses nothing every value
/// lies in its record as it is: the payload is `records` itself and each
/// value a view where it lies, nothing gathered.
///
/// `decoded` receives what the decoding produced, as soon as it produced it:
/// a block refused afterwards was still decoded.
///
/// # Errors
///
/// A frame checksum that does not match, records that are not the header's,
/// or a record that does not decode to the length it declares.
pub(crate) fn decode_packed(
    header: FrameHeader,
    records: &Slice,
    compression: CompressionType,
    #[cfg(zstd_any)] dictionary: Option<&crate::compression::ZstdDictionary>,
    decoded: &mut usize,
) -> crate::Result<DecodedFrame> {
    let malformed = || crate::Error::InvalidHeader("BlobFrame");
    let count = usize::from(header.count);
    let raw_len = header.raw_len as usize;
    // The header's CRC held, but its decoded length is taken before the
    // records add up to it: a block closes before its values decode past
    // what a frame may declare, so no more is allocated for it.
    if raw_len > super::writer::MAX_DECOMPRESSION_SIZE {
        return Err(malformed());
    }
    *decoded = 0;
    // Checked before anything is decoded: a block that is not the frame's
    // decodes nothing.
    check_records(&header, records)?;
    let in_place = compression == CompressionType::None;
    let mut payload = crate::slice::UnfilledSlice::new(if in_place { 0 } else { raw_len });
    let mut target = payload.target();
    let mut list = Vec::with_capacity(count);
    let mut at = 0;
    let mut placed = 0_usize;
    while at < records.len() {
        let rest = records.get(at..).unwrap_or_default();
        let record = record_fields(rest)?;
        let len = record.value_len as usize;
        if placed + len > raw_len {
            return Err(malformed());
        }
        let value = if in_place {
            if record.flags & FLAG_PLAIN != 0 || record.stored.len() != len {
                return Err(crate::Error::InvalidHeader("BlobRecord"));
            }
            *decoded += len;
            at + record.stored.start..at + record.stored.end
        } else {
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
            placed..placed + len
        };
        list.push(FrameEntry {
            seqno: record.seqno,
            key: at + record.key.start..at + record.key.end,
            value_len: record.value_len,
            value,
            at: u32::try_from(at).map_err(|_| malformed())?,
        });
        placed += len;
        at += record.len;
    }
    if list.len() != count || placed != raw_len {
        return Err(malformed());
    }
    let payload = if in_place {
        records.clone()
    } else {
        // SAFETY: every byte of the payload was written above, value by
        // value, as `target` counted them, and they add up to its length.
        #[expect(unsafe_code, reason = "see safety")]
        unsafe {
            payload.assume_filled()
        }
    };
    Ok(DecodedFrame {
        header,
        entries: records.clone(),
        list: FrameEntries::Many(list),
        payload,
    })
}

/// The header of a packed block of `count` records, `raw_len` value bytes
/// decoded, whose records are `records`.
///
/// # Panics
///
/// `records` longer than a frame holds: the writer closes a block first.
pub(crate) fn packed_header(count: u16, raw_len: u32, records: &[u8]) -> [u8; FRAME_HEADER_LEN] {
    #[expect(clippy::expect_used, reason = "the writer closes a block first")]
    let records_len = u32::try_from(records.len()).expect("a packed block fits a frame");
    super::frame::stamp_header(
        FrameKind::Block,
        FLAG_PACKED,
        count,
        0,
        raw_len,
        records_len,
        |crc| super::frame::frame_checksum(records, crc),
    )
}

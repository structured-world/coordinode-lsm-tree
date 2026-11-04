// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The unit a blob file stores: a frame, one compression and one checksum over
//! what it holds.
//!
//! A value's size decides the frame it goes to:
//!
//! - a value below the file's block target joins a [`FrameKind::Block`] with
//!   its neighbours, compressed together;
//! - a value from the block target up to the chunk size is a
//!   [`FrameKind::Single`] of its own;
//! - a larger value is a [`FrameKind::Head`] naming its length and chunk size,
//!   followed by one [`FrameKind::Chunk`] per chunk, each compressed on its own.
//!
//! ```text
//! [magic "BLO5"; 4][kind; 1][flags; 1][count; 2]
//! [entries_len; 4][raw_len; 4][disk_len; 4][header_crc; 4][checksum; 16]
//! [entries; entries_len][payload; disk_len]
//! ```
//!
//! `entries` lists the frame's values, each `[seqno; 8][key len; 2][value len;
//! 4][key]`, uncompressed, so a scan, a collection and a salvage read keys
//! without decoding values. `payload` is the values back to back (a head's is
//! its chunk table), compressed under the file's codec unless [`FLAG_RAW`]
//! says compression saved nothing and the bytes are stored as they are.
//! `header_crc` covers the fixed fields after the magic; `checksum` is
//! xxh3-128 over the entries, the payload as stored and `header_crc`.

#[cfg(zstd_any)]
use crate::compression::CompressionProvider as _;
use crate::{CompressionType, SeqNo, Slice};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::ops::Range;

use super::writer::MAX_DECOMPRESSION_SIZE;

/// The magic every frame starts with. The only readable frame shape: a frame
/// under any other magic, the earlier `BLO4` record among them, is not one.
pub(crate) const FRAME_MAGIC: [u8; 4] = *b"BLO5";

/// The fixed part of a frame, magic to checksum.
pub(crate) const FRAME_HEADER_LEN: usize = 4 + 1 + 1 + 2 + 4 + 4 + 4 + 4 + 16;

/// The fixed part of an entry, before its key.
pub(crate) const ENTRY_HEADER_LEN: usize = 8 + 2 + 4;

/// The payload is stored as it is: compression did not make it smaller.
pub(crate) const FLAG_RAW: u8 = 1;

/// The length of a head's payload: its chunk size and chunk count.
const HEAD_PAYLOAD_LEN: usize = 8;

/// What a frame holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum FrameKind {
    /// One value, from the block target up to the chunk size.
    Single = 0,
    /// Small values, back to back.
    Block = 1,
    /// A value split into chunks: its key, length and chunk size, the chunks
    /// following as frames of their own.
    Head = 2,
    /// One chunk of the value the head before it names.
    Chunk = 3,
}

impl TryFrom<u8> for FrameKind {
    type Error = crate::Error;

    fn try_from(kind: u8) -> crate::Result<Self> {
        match kind {
            0 => Ok(Self::Single),
            1 => Ok(Self::Block),
            2 => Ok(Self::Head),
            3 => Ok(Self::Chunk),
            _ => Err(crate::Error::InvalidHeader("BlobFrame")),
        }
    }
}

/// Little-endian fields read off a byte slice in order, `None` past its end.
struct Fields<'b> {
    bytes: &'b [u8],
    at: usize,
}

impl<'b> Fields<'b> {
    fn new(bytes: &'b [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    fn take<const N: usize>(&mut self) -> Option<[u8; N]> {
        let field = self.bytes.get(self.at..self.at + N)?;
        self.at += N;
        field.try_into().ok()
    }

    fn u8(&mut self) -> Option<u8> {
        self.take::<1>().map(|[b]| b)
    }

    fn u16(&mut self) -> Option<u16> {
        self.take().map(u16::from_le_bytes)
    }

    fn u32(&mut self) -> Option<u32> {
        self.take().map(u32::from_le_bytes)
    }

    fn u64(&mut self) -> Option<u64> {
        self.take().map(u64::from_le_bytes)
    }

    fn u128(&mut self) -> Option<u128> {
        self.take().map(u128::from_le_bytes)
    }
}

/// A frame's fixed fields, read and checked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FrameHeader {
    pub(crate) kind: FrameKind,
    pub(crate) flags: u8,
    pub(crate) count: u16,
    pub(crate) entries_len: u32,
    pub(crate) raw_len: u32,
    pub(crate) disk_len: u32,
    pub(crate) header_crc: u32,
    pub(crate) checksum: u128,
}

impl FrameHeader {
    /// The frame's length on disk: header, entries and payload.
    #[must_use]
    pub(crate) fn frame_len(&self) -> u64 {
        FRAME_HEADER_LEN as u64 + u64::from(self.entries_len) + u64::from(self.disk_len)
    }

    /// Whether the payload is stored uncompressed.
    #[must_use]
    pub(crate) fn is_raw(&self) -> bool {
        self.flags & FLAG_RAW != 0
    }
}

/// The CRC of a header's fixed fields: a truncated xxh3 of them.
#[expect(
    clippy::cast_possible_truncation,
    reason = "intentionally truncated to a 4-byte CRC"
)]
fn header_crc(
    kind: FrameKind,
    flags: u8,
    count: u16,
    entries_len: u32,
    raw_len: u32,
    disk_len: u32,
) -> u32 {
    let mut fields = [0_u8; 16];
    let mut out = Put::new(&mut fields);
    out.bytes(&[kind as u8, flags]);
    out.bytes(&count.to_le_bytes());
    out.bytes(&entries_len.to_le_bytes());
    out.bytes(&raw_len.to_le_bytes());
    out.bytes(&disk_len.to_le_bytes());
    xxhash_rust::xxh3::xxh3_64(&fields) as u32
}

/// Bytes written into a fixed buffer in order.
struct Put<'b> {
    buf: &'b mut [u8],
    at: usize,
}

impl<'b> Put<'b> {
    fn new(buf: &'b mut [u8]) -> Self {
        Self { buf, at: 0 }
    }

    fn bytes(&mut self, field: &[u8]) {
        if let Some(slot) = self.buf.get_mut(self.at..self.at + field.len()) {
            slot.copy_from_slice(field);
        }
        self.at += field.len();
    }
}

/// The checksum of a frame's entries and stored payload, `header_crc` folded
/// in so a header recomputed after tampering is still caught.
fn frame_checksum(entries: &[u8], payload: &[u8], header_crc: u32) -> u128 {
    let mut hasher = xxhash_rust::xxh3::Xxh3::default();
    hasher.update(entries);
    hasher.update(payload);
    hasher.update(&header_crc.to_le_bytes());
    hasher.digest128()
}

/// Reads and checks a frame's fixed fields from the start of `bytes`.
///
/// # Errors
///
/// [`crate::Error::InvalidHeader`] for a short buffer, another magic, an
/// unknown kind or a shape the kind does not allow;
/// [`crate::Error::HeaderCrcMismatch`] for fields that do not match their CRC;
/// [`crate::Error::DecompressedSizeTooLarge`] for lengths past the value cap.
pub(crate) fn parse_header(bytes: &[u8]) -> crate::Result<FrameHeader> {
    let short = crate::Error::InvalidHeader("BlobFrame");
    let mut fields = Fields::new(bytes);
    if fields.take::<4>() != Some(FRAME_MAGIC) {
        return Err(short);
    }
    let (
        Some(kind),
        Some(flags),
        Some(count),
        Some(entries_len),
        Some(raw_len),
        Some(disk_len),
        Some(stored_crc),
        Some(checksum),
    ) = (
        fields.u8(),
        fields.u8(),
        fields.u16(),
        fields.u32(),
        fields.u32(),
        fields.u32(),
        fields.u32(),
        fields.u128(),
    )
    else {
        return Err(short);
    };
    let kind = FrameKind::try_from(kind)?;

    let crc = header_crc(kind, flags, count, entries_len, raw_len, disk_len);
    if crc != stored_crc {
        return Err(crate::Error::HeaderCrcMismatch {
            recomputed: crc,
            stored: stored_crc,
        });
    }
    let cap = MAX_DECOMPRESSION_SIZE as u64;
    for len in [raw_len, disk_len] {
        if u64::from(len) > cap {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: u64::from(len),
                limit: cap,
            });
        }
    }
    // Every entry is its fixed part and a key of one to `u16::MAX` bytes.
    let count64 = u64::from(count);
    let entries_max = count64 * (ENTRY_HEADER_LEN as u64 + u64::from(u16::MAX));
    let entries_min = count64 * (ENTRY_HEADER_LEN as u64 + 1);
    if !(entries_min..=entries_max).contains(&u64::from(entries_len)) {
        return Err(short);
    }
    let shape_ok = match kind {
        FrameKind::Single | FrameKind::Head => count == 1,
        FrameKind::Block => count >= 1,
        FrameKind::Chunk => count == 0,
    };
    // A head's payload is its chunk table, never compressed.
    let head_ok =
        kind != FrameKind::Head || (flags & FLAG_RAW != 0 && raw_len as usize == HEAD_PAYLOAD_LEN);
    if !shape_ok || !head_ok {
        return Err(short);
    }
    Ok(FrameHeader {
        kind,
        flags,
        count,
        entries_len,
        raw_len,
        disk_len,
        header_crc: stored_crc,
        checksum,
    })
}

/// One value of a decoded frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FrameEntry {
    pub(crate) seqno: SeqNo,
    /// Where the key lies in [`DecodedFrame::entries`].
    pub(crate) key: Range<usize>,
    /// The value's length: the whole value's for a head.
    pub(crate) value_len: u32,
    /// Where the value lies in the payload (single and block frames).
    pub(crate) value: Range<usize>,
}

/// A frame checked and decoded.
#[derive(Clone, Debug)]
pub(crate) struct DecodedFrame {
    pub(crate) header: FrameHeader,
    /// The entries as stored: keys are views into it.
    pub(crate) entries: Slice,
    pub(crate) list: Vec<FrameEntry>,
    /// The payload decoded.
    pub(crate) payload: Slice,
}

impl DecodedFrame {
    /// The key of entry `at`.
    #[must_use]
    pub(crate) fn key(&self, at: usize) -> Option<&[u8]> {
        let entry = self.list.get(at)?;
        self.entries.get(entry.key.clone())
    }

    /// The value of entry `at`, a view into the payload.
    #[must_use]
    pub(crate) fn value(&self, at: usize) -> Option<Slice> {
        let entry = self.list.get(at)?;
        if entry.value.end > self.payload.len() {
            return None;
        }
        Some(self.payload.slice(entry.value.clone()))
    }

    /// The chunk size and chunk count a head names.
    ///
    /// # Errors
    ///
    /// The frame is not a head, or its table does not cover its value.
    pub(crate) fn chunk_table(&self) -> crate::Result<(u32, u32)> {
        let malformed = crate::Error::InvalidHeader("BlobFrame");
        if self.header.kind != FrameKind::Head {
            return Err(malformed);
        }
        let mut fields = Fields::new(&self.payload);
        let (Some(size), Some(chunks)) = (fields.u32(), fields.u32()) else {
            return Err(malformed);
        };
        let total = self.list.first().map_or(0, |e| u64::from(e.value_len));
        if size == 0 || u64::from(chunks) != total.div_ceil(u64::from(size)) {
            return Err(malformed);
        }
        Ok((size, chunks))
    }

    /// Bytes this frame takes in a cache: what it decoded, its entries and
    /// their list.
    #[must_use]
    pub(crate) fn weight(&self) -> u64 {
        (self.entries.len() + self.payload.len() + self.list.len() * size_of::<FrameEntry>()) as u64
    }
}

/// Parses the entries of a frame whose header is `header`.
fn parse_entries(header: &FrameHeader, entries: &[u8]) -> crate::Result<Vec<FrameEntry>> {
    let malformed = || crate::Error::InvalidHeader("BlobFrame");
    let mut list = Vec::with_capacity(usize::from(header.count));
    let mut fields = Fields::new(entries);
    let mut value_at = 0_usize;
    for _ in 0..header.count {
        let (Some(seqno), Some(key_len), Some(value_len)) =
            (fields.u64(), fields.u16(), fields.u32())
        else {
            return Err(malformed());
        };
        let key_start = fields.at;
        let key_end = key_start + usize::from(key_len);
        if key_len == 0 || key_end > entries.len() {
            return Err(malformed());
        }
        fields.at = key_end;
        let value_end = value_at
            .checked_add(value_len as usize)
            .ok_or_else(malformed)?;
        list.push(FrameEntry {
            seqno,
            key: key_start..key_end,
            value_len,
            value: value_at..value_end,
        });
        value_at = value_end;
    }
    if fields.at != entries.len() {
        return Err(malformed());
    }
    // A single or block frame's payload is its values back to back.
    if matches!(header.kind, FrameKind::Single | FrameKind::Block)
        && value_at != header.raw_len as usize
    {
        return Err(malformed());
    }
    Ok(list)
}

/// Checks the frame `bytes` holds whole as stored, without decoding its
/// payload: its header, its entries and its checksum. What proves a frame
/// boundary where the codec is not at hand.
///
/// # Errors
///
/// As [`decode_frame`], short of decompression.
pub(crate) fn verify_frame(bytes: &[u8]) -> crate::Result<FrameHeader> {
    check_frame(bytes).map(|(header, _)| header)
}

/// [`verify_frame`]'s checks, the entries parsed kept.
fn check_frame(bytes: &[u8]) -> crate::Result<(FrameHeader, Vec<FrameEntry>)> {
    let header = parse_header(bytes)?;
    if header.frame_len() != bytes.len() as u64 {
        return Err(crate::Error::InvalidHeader("BlobFrame"));
    }
    let entries_end = FRAME_HEADER_LEN + header.entries_len as usize;
    let (Some(entries), Some(stored)) = (
        bytes.get(FRAME_HEADER_LEN..entries_end),
        bytes.get(entries_end..),
    ) else {
        return Err(crate::Error::InvalidHeader("BlobFrame"));
    };
    let checksum = frame_checksum(entries, stored, header.header_crc);
    if checksum != header.checksum {
        return Err(crate::Error::ChecksumMismatch {
            got: crate::Checksum::from_raw(checksum),
            expected: crate::Checksum::from_raw(header.checksum),
        });
    }
    let list = parse_entries(&header, entries)?;
    Ok((header, list))
}

/// Checks and decodes the frame `bytes` holds whole, under `compression` and
/// with `dictionary` for a dictionary codec.
///
/// `decoded` receives the length the decompressor produced, as soon as it
/// produced it: a frame refused afterwards was still decompressed.
///
/// # Errors
///
/// The header, entries or checksum do not check, `bytes` is not the frame
/// whole, or decompression fails or produces another length than declared.
pub(crate) fn decode_frame(
    bytes: &Slice,
    compression: CompressionType,
    #[cfg(zstd_any)] dictionary: Option<&crate::compression::ZstdDictionary>,
    decoded: &mut usize,
) -> crate::Result<DecodedFrame> {
    let (header, list) = check_frame(bytes)?;
    let entries_end = FRAME_HEADER_LEN + header.entries_len as usize;
    let entries = bytes.slice(FRAME_HEADER_LEN..entries_end);
    let stored = bytes.slice(entries_end..bytes.len());
    let raw_len = header.raw_len as usize;
    let payload = if header.is_raw() || compression == CompressionType::None {
        *decoded = stored.len();
        if stored.len() != raw_len {
            return Err(crate::Error::InvalidHeader("BlobFrame"));
        }
        stored
    } else {
        decompress(
            &stored,
            raw_len,
            compression,
            #[cfg(zstd_any)]
            dictionary,
            decoded,
        )?
    };
    Ok(DecodedFrame {
        header,
        entries,
        list,
        payload,
    })
}

/// `stored` decompressed under `compression` into exactly `raw_len` bytes.
#[cfg_attr(
    not(any(feature = "lz4", zstd_any)),
    expect(
        unused_variables,
        clippy::needless_pass_by_ref_mut,
        reason = "without a codec built in, nothing is compressed"
    )
)]
fn decompress(
    stored: &[u8],
    raw_len: usize,
    compression: CompressionType,
    #[cfg(zstd_any)] dictionary: Option<&crate::compression::ZstdDictionary>,
    decoded: &mut usize,
) -> crate::Result<Slice> {
    match compression {
        CompressionType::None => Err(crate::Error::InvalidHeader("BlobFrame")),

        #[cfg(feature = "lz4")]
        CompressionType::Lz4 => {
            // Straight into the payload's allocation, unzeroed: zeroing bytes
            // about to be written buys nothing. SAFETY: the slice goes to this
            // one decoder and no open trait, and it refuses a match reaching
            // before the output, so it never reads an unwritten position
            // whatever the stream holds; the length check below rejects a
            // short fill.
            #[expect(unsafe_code, reason = "fill an uninitialized Slice via decompress")]
            let mut builder = unsafe { Slice::builder_unzeroed(raw_len) };
            let written = lz4_flex::block::decompress_into(stored, &mut builder)
                .map_err(|_| crate::Error::Decompress(compression))?;
            *decoded = written;
            if written != raw_len {
                return Err(crate::Error::Decompress(compression));
            }
            Ok(builder.freeze().into())
        }

        #[cfg(zstd_any)]
        CompressionType::Zstd(_) => {
            // SAFETY: as for lz4 above; the decoder resolves matches in a
            // window of its own and refuses an offset past what it wrote.
            #[expect(unsafe_code, reason = "fill an uninitialized Slice via decompress")]
            let mut builder = unsafe { Slice::builder_unzeroed(raw_len) };
            let written = crate::compression::ZstdBackend::decompress_into(stored, &mut builder)
                .map_err(|_| crate::Error::Decompress(compression))?;
            *decoded = written;
            if written != raw_len {
                return Err(crate::Error::Decompress(compression));
            }
            Ok(builder.freeze().into())
        }

        #[cfg(zstd_any)]
        CompressionType::ZstdDict { dict_id, .. } => {
            let dict = dictionary.ok_or(crate::Error::ZstdDictMismatch {
                expected: dict_id,
                got: None,
            })?;
            let out = crate::compression::ZstdBackend::decompress_with_dict(stored, dict, raw_len)
                .map_err(|_| crate::Error::Decompress(compression))?;
            *decoded = out.len();
            if out.len() != raw_len {
                return Err(crate::Error::Decompress(compression));
            }
            Ok(Slice::from(out))
        }
    }
}

/// What a frame's payload is compressed with.
#[derive(Clone, Copy)]
pub(crate) struct Codec<'d> {
    pub(crate) compression: CompressionType,
    #[cfg(zstd_any)]
    pub(crate) dictionary: Option<&'d crate::compression::ZstdDictionary>,
    #[cfg(zstd_any)]
    pub(crate) two_pass_seed: bool,
    #[cfg(not(zstd_any))]
    pub(crate) dictionary: core::marker::PhantomData<&'d ()>,
}

/// `payload` compressed under `codec`, or `None` when that saves nothing and
/// the payload is stored as it is.
#[cfg_attr(
    not(any(feature = "lz4", zstd_any)),
    expect(
        clippy::unnecessary_wraps,
        reason = "without a codec built in, nothing is compressed and nothing fails"
    )
)]
fn compress(payload: &[u8], codec: Codec<'_>) -> crate::Result<Option<Vec<u8>>> {
    let compressed = match codec.compression {
        CompressionType::None => return Ok(None),

        #[cfg(feature = "lz4")]
        CompressionType::Lz4 => lz4_flex::block::compress(payload),

        #[cfg(zstd_any)]
        CompressionType::Zstd(level) => {
            crate::compression::ZstdBackend::compress(payload, level, codec.two_pass_seed)?
        }

        #[cfg(zstd_any)]
        CompressionType::ZstdDict { level, dict_id } => {
            let dict = codec.dictionary.ok_or(crate::Error::ZstdDictMismatch {
                expected: dict_id,
                got: None,
            })?;
            if dict.id() != dict_id {
                return Err(crate::Error::ZstdDictMismatch {
                    expected: dict_id,
                    got: Some(dict.id()),
                });
            }
            crate::compression::ZstdBackend::compress_with_dict(
                payload,
                level,
                dict,
                codec.two_pass_seed,
            )?
        }
    };
    Ok((compressed.len() < payload.len()).then_some(compressed))
}

/// One value a frame lists: its key, sequence number and length.
#[derive(Clone, Copy, Debug)]
pub(crate) struct EntrySpec<'k> {
    pub(crate) key: &'k [u8],
    pub(crate) seqno: SeqNo,
    pub(crate) value_len: u32,
}

/// Encodes a frame of `kind` listing `entries` over `payload` into `out`, the
/// payload compressed under `codec` unless that saves nothing (a head's chunk
/// table never is).
///
/// # Errors
///
/// Propagates the compression; [`crate::Error::DecompressedSizeTooLarge`] for
/// a payload past the value cap.
///
/// # Panics
///
/// A key is empty or longer than `u16::MAX`, or a frame lists more than
/// `u16::MAX` entries: the writer never builds one.
pub(crate) fn encode_frame(
    kind: FrameKind,
    entries: &[EntrySpec<'_>],
    payload: &[u8],
    codec: Codec<'_>,
    out: &mut Vec<u8>,
) -> crate::Result<()> {
    super::writer::check_size_cap(payload.len())?;
    #[expect(
        clippy::expect_used,
        reason = "the writer closes a block before u16::MAX entries"
    )]
    let count = u16::try_from(entries.len()).expect("a frame lists at most u16::MAX entries");
    let compressed = if kind == FrameKind::Head {
        None
    } else {
        compress(payload, codec)?
    };
    // Raw when nothing compressed it although the file names a codec, and for
    // a head always: the flag tells a reader not to decode.
    let flags = if kind == FrameKind::Head
        || (compressed.is_none() && codec.compression != CompressionType::None)
    {
        FLAG_RAW
    } else {
        0
    };
    let stored: &[u8] = compressed.as_deref().unwrap_or(payload);

    out.clear();
    out.resize(FRAME_HEADER_LEN, 0);
    for entry in entries {
        assert!(
            !entry.key.is_empty(),
            "a blob is written under a non-empty key"
        );
        #[expect(clippy::expect_used, reason = "the writer refuses longer keys first")]
        let key_len = u16::try_from(entry.key.len()).expect("keys are u16 length max");
        out.extend_from_slice(&entry.seqno.to_le_bytes());
        out.extend_from_slice(&key_len.to_le_bytes());
        out.extend_from_slice(&entry.value_len.to_le_bytes());
        out.extend_from_slice(entry.key);
    }
    let entries_end = out.len();
    out.extend_from_slice(stored);

    #[expect(
        clippy::cast_possible_truncation,
        reason = "entries are bounded by u16::MAX keys of u16::MAX bytes, the payload by the value cap"
    )]
    let (entries_len, raw_len, disk_len) = (
        (entries_end - FRAME_HEADER_LEN) as u32,
        payload.len() as u32,
        stored.len() as u32,
    );
    let crc = header_crc(kind, flags, count, entries_len, raw_len, disk_len);
    let checksum = frame_checksum(
        out.get(FRAME_HEADER_LEN..entries_end).unwrap_or_default(),
        stored,
        crc,
    );

    let mut header = [0_u8; FRAME_HEADER_LEN];
    let mut put = Put::new(&mut header);
    put.bytes(&FRAME_MAGIC);
    put.bytes(&[kind as u8, flags]);
    put.bytes(&count.to_le_bytes());
    put.bytes(&entries_len.to_le_bytes());
    put.bytes(&raw_len.to_le_bytes());
    put.bytes(&disk_len.to_le_bytes());
    put.bytes(&crc.to_le_bytes());
    put.bytes(&checksum.to_le_bytes());
    if let Some(slot) = out.get_mut(..FRAME_HEADER_LEN) {
        slot.copy_from_slice(&header);
    }
    Ok(())
}

/// The payload of a head naming chunks of `chunk_size` bytes, `chunks` of them.
#[must_use]
pub(crate) fn head_payload(chunk_size: u32, chunks: u32) -> [u8; HEAD_PAYLOAD_LEN] {
    let mut payload = [0_u8; HEAD_PAYLOAD_LEN];
    let mut put = Put::new(&mut payload);
    put.bytes(&chunk_size.to_le_bytes());
    put.bytes(&chunks.to_le_bytes());
    payload
}

/// A frame header of `kind` with these fields under a header CRC that holds
/// and a zero checksum: what a forged frame inside a value's bytes looks like.
#[cfg(test)]
pub(crate) fn forged_header(
    kind: FrameKind,
    count: u16,
    entries_len: u32,
    raw_len: u32,
    disk_len: u32,
) -> [u8; FRAME_HEADER_LEN] {
    let crc = header_crc(kind, 0, count, entries_len, raw_len, disk_len);
    let mut header = [0_u8; FRAME_HEADER_LEN];
    let mut put = Put::new(&mut header);
    put.bytes(&FRAME_MAGIC);
    put.bytes(&[kind as u8, 0]);
    put.bytes(&count.to_le_bytes());
    put.bytes(&entries_len.to_le_bytes());
    put.bytes(&raw_len.to_le_bytes());
    put.bytes(&disk_len.to_le_bytes());
    put.bytes(&crc.to_le_bytes());
    header
}

/// Recomputes the header CRC of the frame starting at `frame` from its
/// fields as they now are, so a test's rewritten field passes the CRC and
/// reaches the check behind it.
#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test code")]
pub(crate) fn restamp_header_crc(frame: &mut [u8]) {
    let mut fields = Fields::new(frame);
    fields.take::<4>().unwrap();
    let kind = FrameKind::try_from(fields.u8().unwrap()).unwrap();
    let (flags, count) = (fields.u8().unwrap(), fields.u16().unwrap());
    let (entries_len, raw_len, disk_len) = (
        fields.u32().unwrap(),
        fields.u32().unwrap(),
        fields.u32().unwrap(),
    );
    let crc = header_crc(kind, flags, count, entries_len, raw_len, disk_len);
    frame
        .get_mut(20..24)
        .unwrap()
        .copy_from_slice(&crc.to_le_bytes());
}

/// A frame holding `values` under `keys` at seqno 0, uncompressed and
/// decoded: a single frame for one value, a block for several. What a test
/// seeds a cache or a read's store with.
#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test code")]
pub(crate) fn decoded_frame(values: &[(&[u8], &[u8])]) -> DecodedFrame {
    let entries: Vec<_> = values
        .iter()
        .map(|(key, value)| EntrySpec {
            key,
            seqno: 0,
            value_len: u32::try_from(value.len()).unwrap(),
        })
        .collect();
    let payload: Vec<u8> = values
        .iter()
        .flat_map(|(_, value)| value.iter().copied())
        .collect();
    let kind = if values.len() == 1 {
        FrameKind::Single
    } else {
        FrameKind::Block
    };
    let codec = Codec {
        compression: CompressionType::None,
        #[cfg(zstd_any)]
        dictionary: None,
        #[cfg(zstd_any)]
        two_pass_seed: false,
        #[cfg(not(zstd_any))]
        dictionary: core::marker::PhantomData,
    };
    let mut out = Vec::new();
    encode_frame(kind, &entries, &payload, codec, &mut out).unwrap();
    decode_frame(
        &Slice::from(out),
        CompressionType::None,
        #[cfg(zstd_any)]
        None,
        &mut 0,
    )
    .unwrap()
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::indexing_slicing, reason = "test code")]
mod tests;

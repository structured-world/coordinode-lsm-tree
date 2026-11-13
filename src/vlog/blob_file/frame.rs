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
//! - a larger value is a [`FrameKind::Head`] naming its length, its chunk size
//!   and how each chunk is stored, followed by its chunks back to back, each
//!   compressed on its own and carrying no header: a value whose chunks are
//!   stored as they are lies in one range after its head.
//!
//! ```text
//! [magic "BLO5"; 4][kind; 1][flags; 1][count; 2]
//! [entries_len; 4][raw_len; 4][disk_len; 4][header_crc; 4][checksum; 16]
//! [entries; entries_len][payload; disk_len]
//! ```
//!
//! `entries` lists the frame's values, each `[seqno; 8][key len; 2][value len;
//! 4][key]`, uncompressed, so a scan, a collection and a salvage read keys
//! without decoding values. `payload` is the values back to back, compressed
//! under the file's codec unless [`FLAG_RAW`] says compression saved nothing
//! and the bytes are stored as they are, or [`FLAG_PLAIN`] says a dictionary
//! codec compressed it without its dictionary.
//!
//! A head's payload, stored as it is, is its chunk table:
//!
//! ```text
//! [chunk size; 4][chunk count; 4]
//! ([stored len; 4][flags; 1][checksum; 16]) * chunk count
//! ```
//!
//! Each chunk's flags read as a frame's do, and its checksum is xxh3-128 over
//! the chunk as stored; a chunk holds the chunk size of the value, the last
//! what is left.
//! `header_crc` covers the fixed fields after the magic; `checksum` is
//! xxh3-128 over the entries and the payload as stored, which lie back to
//! back, seeded with `header_crc`; a packed block's is over its records'
//! checksums instead (see [`super::packed`]).

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

/// Under a dictionary codec, the payload is compressed without the dictionary,
/// at the codec's level: the frame's class is one the dictionary does not
/// apply to (see [`crate::config::DictionaryClasses`]).
pub(crate) const FLAG_PLAIN: u8 = 2;

/// The fixed part of a head's payload: its chunk size and chunk count.
const HEAD_TABLE_LEN: usize = 8;

/// One chunk's row of a head's table: its stored length, flags and checksum.
pub(crate) const CHUNK_ROW_LEN: usize = 4 + 1 + 16;

/// What a frame holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum FrameKind {
    /// One value, from the block target up to the chunk size.
    Single = 0,
    /// Small values, back to back.
    Block = 1,
    /// A value split into chunks: its key, length, chunk size and the table of
    /// its chunks, which follow it back to back.
    Head = 2,
    /// One chunk of the value a head names, decoded by the head's table: a
    /// chunk carries no header of its own, so no frame on disk is of this
    /// kind.
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

    /// Writes `field` next. Every buffer is sized for its fields, so the
    /// fields always fit.
    fn bytes(&mut self, field: &[u8]) {
        let (_, rest) = self.buf.split_at_mut(self.at);
        let (slot, _) = rest.split_at_mut(field.len());
        slot.copy_from_slice(field);
        self.at += field.len();
    }
}

/// The checksum of a frame's entries and stored payload, `stored` the two as
/// they lie back to back, seeded with `header_crc` so a header recomputed
/// after tampering is still caught.
///
/// Blob layout 5 is in no release yet, so changing how it is checksummed keeps
/// its version: no supported store holds frames checksummed the other way.
pub(crate) fn frame_checksum(stored: &[u8], header_crc: u32) -> u128 {
    xxhash_rust::xxh3::xxh3_128_with_seed(stored, u64::from(header_crc))
}

/// [`frame_checksum`] over `entries` followed by `payload`, where the writer
/// holds them apart.
fn frame_checksum_parts(entries: &[u8], payload: &[u8], header_crc: u32) -> u128 {
    let mut hasher = xxhash_rust::xxh3::Xxh3::with_seed(u64::from(header_crc));
    hasher.update(entries);
    hasher.update(payload);
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
    // Built only on the way out: an error made up front is dropped, glue and
    // all, on every frame that checks.
    let short = || crate::Error::InvalidHeader("BlobFrame");
    let mut fields = Fields::new(bytes);
    if fields.take::<4>() != Some(FRAME_MAGIC) {
        return Err(short());
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
        return Err(short());
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
    // A packed block lists its values in its records, its entries empty.
    let packed = flags == super::packed::FLAG_PACKED;
    let entries_ok = if packed {
        kind == FrameKind::Block && count >= 1 && entries_len == 0
    } else {
        (entries_min..=entries_max).contains(&u64::from(entries_len))
    };
    if !entries_ok {
        return Err(short());
    }
    let shape_ok = match kind {
        FrameKind::Single | FrameKind::Head => count == 1,
        FrameKind::Block => count >= 1,
        FrameKind::Chunk => false,
    };
    // A head's payload is its chunk table, never compressed.
    let head_ok = kind != FrameKind::Head
        || (flags & FLAG_RAW != 0
            && raw_len == disk_len
            && (raw_len as usize)
                .checked_sub(HEAD_TABLE_LEN)
                .is_some_and(|rows| rows % CHUNK_ROW_LEN == 0));
    // A stored payload is compressed one way or not at all.
    let flags_ok =
        packed || (flags & !(FLAG_RAW | FLAG_PLAIN) == 0 && flags != FLAG_RAW | FLAG_PLAIN);
    if !shape_ok || !head_ok || !flags_ok {
        return Err(short());
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
    /// Where the value's record starts in a packed block, which its handle
    /// names it by; `0` in any other frame.
    pub(crate) at: u32,
}

/// The entries a frame lists: the one of a single frame or a head held in
/// place, so decoding a frame of one value allocates no list for it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FrameEntries {
    One(FrameEntry),
    Many(Vec<FrameEntry>),
}

impl core::ops::Deref for FrameEntries {
    type Target = [FrameEntry];

    fn deref(&self) -> &[FrameEntry] {
        match self {
            Self::One(entry) => core::slice::from_ref(entry),
            Self::Many(list) => list,
        }
    }
}

/// A frame checked and decoded.
#[derive(Clone, Debug)]
pub(crate) struct DecodedFrame {
    pub(crate) header: FrameHeader,
    /// The entries as stored: keys are views into it.
    pub(crate) entries: Slice,
    pub(crate) list: FrameEntries,
    /// The payload decoded.
    pub(crate) payload: Slice,
}

impl DecodedFrame {
    /// Whether the frame is a packed block, whose values a handle names by
    /// their records' offsets.
    #[must_use]
    pub(crate) fn is_packed(&self) -> bool {
        self.header.flags & super::packed::FLAG_PACKED != 0
    }

    /// The place in the list of the value a handle's `index` names: the
    /// place itself, or in a packed block the value whose record starts there.
    #[must_use]
    pub(crate) fn place_of(&self, index: u16) -> Option<usize> {
        if !self.is_packed() {
            return Some(usize::from(index));
        }
        let index = u32::from(index);
        // Records of one length lie at even steps, so the offset's share of
        // the block is the place: tried first, it spares the search a scan
        // would otherwise run for every value. The product fits: an offset
        // and a block's count are both within `u16`.
        let guess = (index as usize * self.list.len())
            .checked_div(self.entries.len())
            .unwrap_or(0);
        if self.list.get(guess).is_some_and(|entry| entry.at == index) {
            return Some(guess);
        }
        self.list
            .binary_search_by_key(&index, |entry| entry.at)
            .ok()
    }

    /// The index a handle names entry `at` by, as [`Self::place_of`] reads
    /// it back: the place itself, or in a packed block its record's offset.
    /// `None` past what a handle's index holds.
    #[must_use]
    pub(crate) fn index_of(&self, at: usize) -> Option<u16> {
        if self.is_packed() {
            return self
                .list
                .get(at)
                .and_then(|entry| u16::try_from(entry.at).ok());
        }
        u16::try_from(at).ok()
    }

    /// What entry `at` adds to its file's block entry bytes, as its writer
    /// counted it: a block's entry uncompressed, or a packed block's record
    /// as it lies.
    #[must_use]
    pub(crate) fn block_entry_bytes(&self, at: usize) -> Option<u64> {
        let entry = self.list.get(at)?;
        if self.is_packed() {
            let end = self
                .list
                .get(at + 1)
                .map_or(self.entries.len() as u64, |next| u64::from(next.at));
            return end.checked_sub(u64::from(entry.at));
        }
        Some((ENTRY_HEADER_LEN + entry.key.len()) as u64 + u64::from(entry.value_len))
    }

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
        if self.header.kind != FrameKind::Head {
            return Err(crate::Error::InvalidHeader("BlobFrame"));
        }
        let total = self.list.first().map_or(0, |e| u64::from(e.value_len));
        head_table(&self.payload, total)
    }

    /// Chunk `at` of a head, as its table lists it.
    ///
    /// # Errors
    ///
    /// As [`Self::chunk_table`], and a chunk the table does not list.
    pub(crate) fn chunk(&self, at: u32) -> crate::Result<ChunkSpec> {
        let (size, chunks) = self.chunk_table()?;
        let total = self.list.first().map_or(0, |e| u64::from(e.value_len));
        chunk_row(&self.payload, size, chunks, total, at)
    }

    /// Bytes this frame takes in a cache: what it decoded, its entries and
    /// their list.
    #[must_use]
    pub(crate) fn weight(&self) -> u64 {
        // A packed block stored as it is: its payload is its records.
        let payload = if self.payload.as_ptr_range() == self.entries.as_ptr_range() {
            0
        } else {
            self.payload.len()
        };
        (self.entries.len() + payload + self.list.len() * size_of::<FrameEntry>()) as u64
    }
}

/// One chunk of a chunked value as its head's table lists it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ChunkSpec {
    /// The chunk's decoded length: the chunk size, the last chunk's what is
    /// left of the value.
    pub(crate) raw_len: u32,
    /// What the chunk takes on disk.
    pub(crate) stored_len: u32,
    /// How it is stored, as a frame's flags say it.
    pub(crate) flags: u8,
    /// xxh3-128 over the chunk as stored.
    pub(crate) checksum: u128,
}

impl ChunkSpec {
    /// Whether the chunk is stored as it is under a file of `compression`.
    #[must_use]
    pub(crate) fn is_stored_as_is(&self, compression: CompressionType) -> bool {
        self.flags & FLAG_RAW != 0 || compression == CompressionType::None
    }

    /// Whether the row is one a file of `compression` can hold: a chunk
    /// stored as it is takes its own length. A row breaking it names a value
    /// no read assembles, so a copy refuses it before carrying its bytes.
    #[must_use]
    pub(crate) fn fits(&self, compression: CompressionType) -> bool {
        !self.is_stored_as_is(compression) || self.stored_len == self.raw_len
    }
}

/// The chunk size and chunk count of a head's `table`, for a value of
/// `total` bytes.
fn head_table(table: &[u8], total: u64) -> crate::Result<(u32, u32)> {
    let malformed = || crate::Error::InvalidHeader("BlobFrame");
    let mut fields = Fields::new(table);
    let (Some(size), Some(chunks)) = (fields.u32(), fields.u32()) else {
        return Err(malformed());
    };
    let rows_ok = (chunks as usize)
        .checked_mul(CHUNK_ROW_LEN)
        .and_then(|rows| rows.checked_add(HEAD_TABLE_LEN))
        == Some(table.len());
    // A value is written within the cap, and callers size their chunk list
    // and buffer from this table: a larger one is damage.
    if size == 0
        || total > super::writer::MAX_DECOMPRESSION_SIZE as u64
        || u64::from(chunks) != total.div_ceil(u64::from(size))
        || !rows_ok
    {
        return Err(malformed());
    }
    Ok((size, chunks))
}

/// Row `at` of a head's `table` of `chunks` chunks of `size` bytes over a
/// value of `total` bytes.
fn chunk_row(
    table: &[u8],
    size: u32,
    chunks: u32,
    total: u64,
    at: u32,
) -> crate::Result<ChunkSpec> {
    let malformed = || crate::Error::InvalidHeader("BlobFrame");
    if at >= chunks {
        return Err(malformed());
    }
    let row = HEAD_TABLE_LEN + at as usize * CHUNK_ROW_LEN;
    let mut fields = Fields::new(table.get(row..row + CHUNK_ROW_LEN).ok_or_else(malformed)?);
    let (Some(stored_len), Some(flags), Some(checksum)) =
        (fields.u32(), fields.u8(), fields.u128())
    else {
        return Err(malformed());
    };
    // A chunk is stored one way, as a frame is.
    if flags & !(FLAG_RAW | FLAG_PLAIN) != 0 || flags == FLAG_RAW | FLAG_PLAIN {
        return Err(malformed());
    }
    let raw_len = if at + 1 == chunks {
        total - u64::from(at) * u64::from(size)
    } else {
        u64::from(size)
    };
    #[expect(
        clippy::cast_possible_truncation,
        reason = "a chunk holds at most the chunk size, a u32"
    )]
    let raw_len = raw_len as u32;
    // Stored as it is, a chunk takes its own length.
    if flags & FLAG_RAW != 0 && stored_len != raw_len {
        return Err(malformed());
    }
    Ok(ChunkSpec {
        raw_len,
        stored_len,
        flags,
        checksum,
    })
}

/// The table of a head `head` as stored, a frame that checks: each of its
/// chunks as listed.
///
/// # Errors
///
/// The frame does not check, is not a head, or its table is malformed.
#[cfg(feature = "std")]
pub(crate) fn head_chunks(head: &[u8]) -> crate::Result<Vec<ChunkSpec>> {
    let (header, list) = check_frame(head)?;
    let table = head
        .get(FRAME_HEADER_LEN + header.entries_len as usize..)
        .unwrap_or_default();
    if header.kind != FrameKind::Head {
        return Err(crate::Error::InvalidHeader("BlobFrame"));
    }
    let total = list.first().map_or(0, |e| u64::from(e.value_len));
    let (size, chunks) = head_table(table, total)?;
    (0..chunks)
        .map(|at| chunk_row(table, size, chunks, total, at))
        .collect()
}

/// Whether `chunks` are, in order, the chunks the table of `head` lists for a
/// value of `total` bytes, each one whole frame in its listed length and
/// stored as `compression` can hold. `head` is a head frame a scan already
/// checked, whose entries end at `entries_end`: its table is read as it lies,
/// its checksum not taken again, nothing gathered.
///
/// # Errors
///
/// A table that does not hold.
pub(crate) fn chunks_fit_head(
    head: &[u8],
    entries_end: usize,
    total: u64,
    chunks: &[Slice],
    compression: CompressionType,
) -> crate::Result<bool> {
    let table = head.get(entries_end..).unwrap_or_default();
    let (size, count) = head_table(table, total)?;
    if count as usize != chunks.len() {
        return Ok(false);
    }
    for (at, chunk) in (0..count).zip(chunks) {
        let spec = chunk_row(table, size, count, total, at)?;
        if chunk.len() != spec.stored_len as usize || !spec.fits(compression) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The table a head of a value split into `chunks` writes.
#[must_use]
pub(crate) fn head_payload(chunk_size: u32, chunks: &[ChunkSpec]) -> Vec<u8> {
    let mut table = Vec::with_capacity(HEAD_TABLE_LEN + chunks.len() * CHUNK_ROW_LEN);
    table.extend_from_slice(&chunk_size.to_le_bytes());
    #[expect(
        clippy::cast_possible_truncation,
        reason = "a value of at most the 256 MiB cap holds fewer than u32::MAX chunks"
    )]
    table.extend_from_slice(&(chunks.len() as u32).to_le_bytes());
    for chunk in chunks {
        table.extend_from_slice(&chunk.stored_len.to_le_bytes());
        table.push(chunk.flags);
        table.extend_from_slice(&chunk.checksum.to_le_bytes());
    }
    table
}

/// The checksum a chunk carries in its head's table.
#[must_use]
pub(crate) fn chunk_checksum(stored: &[u8]) -> u128 {
    xxhash_rust::xxh3::xxh3_128(stored)
}

/// Checks `stored` against `spec`: its length and its checksum.
///
/// # Errors
///
/// [`crate::Error::InvalidHeader`] for another length,
/// [`crate::Error::ChecksumMismatch`] when the bytes do not check.
pub(crate) fn check_chunk(spec: &ChunkSpec, stored: &[u8]) -> crate::Result<()> {
    if stored.len() != spec.stored_len as usize {
        return Err(crate::Error::InvalidHeader("BlobFrame"));
    }
    let checksum = chunk_checksum(stored);
    if checksum != spec.checksum {
        return Err(crate::Error::ChecksumMismatch {
            got: crate::Checksum::from_raw(checksum),
            expected: crate::Checksum::from_raw(spec.checksum),
        });
    }
    Ok(())
}

/// `chunk` encoded under `codec`: its stored bytes when compression shrank
/// it (`None` when it is stored as it is) and its row in the head's table.
///
/// # Errors
///
/// Propagates the compression.
pub(crate) fn encode_chunk(
    chunk: &[u8],
    codec: Codec<'_>,
) -> crate::Result<(Option<Vec<u8>>, ChunkSpec)> {
    let raw_len = super::writer::check_size_cap(chunk.len())?;
    let compressed = compress(chunk, codec)?;
    let flags = if compressed.is_none() && codec.compression != CompressionType::None {
        FLAG_RAW
    } else if compressed.is_some() && codec.plain {
        FLAG_PLAIN
    } else {
        0
    };
    let stored: &[u8] = compressed.as_deref().unwrap_or(chunk);
    #[expect(
        clippy::cast_possible_truncation,
        reason = "compression keeps only what shrank the chunk, itself within the cap"
    )]
    let spec = ChunkSpec {
        raw_len,
        stored_len: stored.len() as u32,
        flags,
        checksum: chunk_checksum(stored),
    };
    Ok((compressed, spec))
}

/// Parses the entries of a frame whose header is `header`.
fn parse_entries(header: &FrameHeader, entries: &[u8]) -> crate::Result<FrameEntries> {
    let malformed = || crate::Error::InvalidHeader("BlobFrame");
    // A list only for a block: a frame of one value holds its entry in place.
    let mut list = if header.count > 1 {
        Vec::with_capacity(usize::from(header.count))
    } else {
        Vec::new()
    };
    let mut only = None;
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
        let entry = FrameEntry {
            seqno,
            key: key_start..key_end,
            value_len,
            value: value_at..value_end,
            at: 0,
        };
        if header.count == 1 {
            only = Some(entry);
        } else {
            list.push(entry);
        }
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
    Ok(match only {
        Some(entry) => FrameEntries::One(entry),
        None => FrameEntries::Many(list),
    })
}

/// Checks the frame `bytes` holds whole as stored, without decoding its
/// payload: its header, its entries and its checksum. What proves a frame
/// boundary where the codec is not at hand.
///
/// # Errors
///
/// As [`decode_frame`], short of decompression.
#[cfg(feature = "std")]
pub(crate) fn verify_frame(bytes: &[u8]) -> crate::Result<FrameHeader> {
    check_frame(bytes).map(|(header, _)| header)
}

/// [`verify_frame`]'s checks, the frame kept with its payload not decoded:
/// `payload` is empty, so [`DecodedFrame::value`] has nothing to give and the
/// header's `raw_len` is the payload's length. For a frame a consumer copies
/// as stored, whose decompression would buy nothing; a payload stored as it
/// is under `compression` is checked to be the length it declares, as a
/// decode checks it.
///
/// # Errors
///
/// As [`verify_frame`], and a payload stored as it is whose length is not
/// the one declared.
pub(crate) fn undecoded_frame(
    bytes: &Slice,
    compression: CompressionType,
) -> crate::Result<DecodedFrame> {
    let (header, list) = check_frame(bytes)?;
    if (header.is_raw() || compression == CompressionType::None)
        && header.disk_len != header.raw_len
    {
        return Err(crate::Error::InvalidHeader("BlobFrame"));
    }
    let entries_end = FRAME_HEADER_LEN + header.entries_len as usize;
    Ok(DecodedFrame {
        header,
        entries: bytes.slice(FRAME_HEADER_LEN..entries_end),
        list,
        payload: Slice::empty(),
    })
}

/// [`verify_frame`]'s checks, the entries parsed kept.
pub(crate) fn check_frame(bytes: &[u8]) -> crate::Result<(FrameHeader, FrameEntries)> {
    check_frame_with(parse_header(bytes)?, bytes)
}

/// [`check_frame`] of `bytes` whose header, `header`, is parsed already.
fn check_frame_with(
    header: FrameHeader,
    bytes: &[u8],
) -> crate::Result<(FrameHeader, FrameEntries)> {
    if header.frame_len() != bytes.len() as u64 {
        return Err(crate::Error::InvalidHeader("BlobFrame"));
    }
    if header.flags & super::packed::FLAG_PACKED != 0 {
        let records = bytes.get(FRAME_HEADER_LEN..).unwrap_or_default();
        let list = super::packed::check_packed(&header, records)?;
        return Ok((header, list));
    }
    let entries_end = FRAME_HEADER_LEN + header.entries_len as usize;
    let (Some(entries), Some(checked)) = (
        bytes.get(FRAME_HEADER_LEN..entries_end),
        bytes.get(FRAME_HEADER_LEN..),
    ) else {
        return Err(crate::Error::InvalidHeader("BlobFrame"));
    };
    let checksum = frame_checksum(checked, header.header_crc);
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
    // A packed block is checked as it is decoded, each record parsed once.
    let header = parse_header(bytes)?;
    if header.flags & super::packed::FLAG_PACKED != 0 {
        if header.frame_len() != bytes.len() as u64 {
            return Err(crate::Error::InvalidHeader("BlobFrame"));
        }
        return super::packed::decode_packed(
            header,
            &bytes.slice(FRAME_HEADER_LEN..bytes.len()),
            compression,
            #[cfg(zstd_any)]
            dictionary,
            decoded,
        );
    }
    let (header, list) = check_frame_with(header, bytes)?;
    let entries_end = FRAME_HEADER_LEN + header.entries_len as usize;
    let entries = bytes.slice(FRAME_HEADER_LEN..entries_end);
    let stored = bytes.slice(entries_end..bytes.len());
    let raw_len = header.raw_len as usize;
    #[cfg(zstd_any)]
    let (compression, dictionary) = frame_codec(header.flags, compression, dictionary)?;
    #[cfg(not(zstd_any))]
    if header.flags & FLAG_PLAIN != 0 {
        // Only a zstd codec writes a plain frame.
        return Err(crate::Error::InvalidHeader("BlobFrame"));
    }
    let (entries, payload) = if header.is_raw() || compression == CompressionType::None {
        *decoded = stored.len();
        if stored.len() != raw_len {
            return Err(crate::Error::InvalidHeader("BlobFrame"));
        }
        (entries, stored)
    } else {
        let payload = decompress(
            &stored,
            raw_len,
            compression,
            #[cfg(zstd_any)]
            dictionary,
            decoded,
        )?;
        // The payload is its own buffer now: a copy of the entries, a few
        // keys, lets the encoded read go instead of keeping it alive in the
        // cache uncounted.
        (Slice::from(&entries[..]), payload)
    };
    Ok(DecodedFrame {
        header,
        entries,
        list,
        payload,
    })
}

/// The codec a frame of `flags` under a file of `compression` decodes with: a
/// plain frame is zstd at the file's level without a dictionary, which a file
/// training its own dictionary writes before the dictionary exists, whether
/// or not it ends up with one.
#[cfg(zstd_any)]
pub(crate) fn frame_codec(
    flags: u8,
    compression: CompressionType,
    dictionary: Option<&crate::compression::ZstdDictionary>,
) -> crate::Result<(CompressionType, Option<&crate::compression::ZstdDictionary>)> {
    if flags & FLAG_PLAIN == 0 {
        return Ok((compression, dictionary));
    }
    match compression {
        CompressionType::ZstdDict { level, .. } | CompressionType::Zstd(level) => {
            Ok((CompressionType::Zstd(level), None))
        }
        _ => Err(crate::Error::InvalidHeader("BlobFrame")),
    }
}

/// The codec a chunk of `spec` under a file of `compression` decodes with.
#[cfg(zstd_any)]
fn chunk_codec<'d>(
    spec: &ChunkSpec,
    compression: CompressionType,
    dictionary: Option<&'d crate::compression::ZstdDictionary>,
) -> crate::Result<(
    CompressionType,
    Option<&'d crate::compression::ZstdDictionary>,
)> {
    frame_codec(spec.flags, compression, dictionary)
}

/// The codec a chunk of `spec` under a file of `compression` decodes with:
/// only a dictionary codec writes a plain chunk.
#[cfg(not(zstd_any))]
fn chunk_codec(spec: &ChunkSpec, compression: CompressionType) -> crate::Result<CompressionType> {
    if spec.flags & FLAG_PLAIN == 0 {
        Ok(compression)
    } else {
        Err(crate::Error::InvalidHeader("BlobFrame"))
    }
}

/// Checks the chunk `stored` against `spec` and writes it, decoded under
/// `compression`, into `out`, which takes exactly the chunk.
///
/// `decoded` receives the length the decompressor produced, as
/// [`decode_frame`] does.
///
/// # Errors
///
/// As [`check_chunk`], an `out` of another length than the chunk, and a
/// decompression that fails or produces another length.
pub(crate) fn decode_chunk_into(
    spec: &ChunkSpec,
    stored: &[u8],
    compression: CompressionType,
    #[cfg(zstd_any)] dictionary: Option<&crate::compression::ZstdDictionary>,
    out: &mut [u8],
    decoded: &mut usize,
) -> crate::Result<()> {
    check_chunk(spec, stored)?;
    if spec.raw_len as usize != out.len() {
        return Err(crate::Error::InvalidHeader("BlobFrame"));
    }
    #[cfg(zstd_any)]
    let (compression, dictionary) = chunk_codec(spec, compression, dictionary)?;
    #[cfg(not(zstd_any))]
    let compression = chunk_codec(spec, compression)?;
    if spec.is_stored_as_is(compression) {
        *decoded = stored.len();
        if stored.len() != out.len() {
            return Err(crate::Error::InvalidHeader("BlobFrame"));
        }
        out.copy_from_slice(stored);
        return Ok(());
    }
    decompress_to(
        stored,
        out,
        compression,
        #[cfg(zstd_any)]
        dictionary,
        decoded,
    )
}

/// Checks the chunk `stored` against `spec` and decodes it under
/// `compression` into a frame of its own: a chunk stored as it is stays a
/// view into `stored`.
///
/// # Errors
///
/// As [`decode_chunk_into`].
pub(crate) fn decode_chunk(
    spec: &ChunkSpec,
    stored: &Slice,
    compression: CompressionType,
    #[cfg(zstd_any)] dictionary: Option<&crate::compression::ZstdDictionary>,
    decoded: &mut usize,
) -> crate::Result<DecodedFrame> {
    check_chunk(spec, stored)?;
    #[cfg(zstd_any)]
    let (compression, dictionary) = chunk_codec(spec, compression, dictionary)?;
    #[cfg(not(zstd_any))]
    let compression = chunk_codec(spec, compression)?;
    let raw_len = spec.raw_len as usize;
    let payload = if spec.is_stored_as_is(compression) {
        *decoded = stored.len();
        if stored.len() != raw_len {
            return Err(crate::Error::InvalidHeader("BlobFrame"));
        }
        stored.clone()
    } else {
        decompress(
            stored,
            raw_len,
            compression,
            #[cfg(zstd_any)]
            dictionary,
            decoded,
        )?
    };
    Ok(DecodedFrame {
        header: FrameHeader {
            kind: FrameKind::Chunk,
            flags: spec.flags,
            count: 0,
            entries_len: 0,
            raw_len: spec.raw_len,
            disk_len: spec.stored_len,
            header_crc: 0,
            checksum: spec.checksum,
        },
        entries: Slice::empty(),
        list: FrameEntries::Many(Vec::new()),
        payload,
    })
}

/// `stored` decompressed under `compression` into `out`: without a codec
/// built in, nothing is compressed, so nothing decompresses.
#[cfg(not(any(feature = "lz4", zstd_any)))]
pub(super) fn decompress_to(
    _stored: &[u8],
    _out: &mut [u8],
    _compression: CompressionType,
    _decoded: &mut usize,
) -> crate::Result<()> {
    Err(crate::Error::InvalidHeader("BlobFrame"))
}

/// `stored` decompressed under `compression` into `out`, exactly its length.
#[cfg(any(feature = "lz4", zstd_any))]
pub(super) fn decompress_to(
    stored: &[u8],
    out: &mut [u8],
    compression: CompressionType,
    #[cfg(zstd_any)] dictionary: Option<&crate::compression::ZstdDictionary>,
    decoded: &mut usize,
) -> crate::Result<()> {
    let written = match compression {
        CompressionType::None => return Err(crate::Error::InvalidHeader("BlobFrame")),

        #[cfg(feature = "lz4")]
        CompressionType::Lz4 => lz4_flex::block::decompress_into(stored, out)
            .map_err(|_| crate::Error::Decompress(compression))?,

        #[cfg(zstd_any)]
        CompressionType::Zstd(_) => crate::compression::ZstdBackend::decompress_into(stored, out)
            .map_err(|_| crate::Error::Decompress(compression))?,

        #[cfg(zstd_any)]
        CompressionType::ZstdDict { dict_id, .. } => {
            let dict = dictionary.ok_or(crate::Error::ZstdDictMismatch {
                expected: dict_id,
                got: None,
            })?;
            crate::compression::ZstdBackend::decompress_with_dict_into(stored, dict, out)
                .map_err(|_| crate::Error::Decompress(compression))?
        }
    };
    *decoded = written;
    if written != out.len() {
        return Err(crate::Error::Decompress(compression));
    }
    Ok(())
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
            // SAFETY: as for zstd above: the decoder refuses an offset reaching
            // past what it wrote and the dictionary's history, so it never
            // reads an unwritten position; the length check below rejects a
            // short fill.
            #[expect(unsafe_code, reason = "fill an uninitialized Slice via decompress")]
            let mut builder = unsafe { Slice::builder_unzeroed(raw_len) };
            let written = crate::compression::ZstdBackend::decompress_with_dict_into(
                stored,
                dict,
                &mut builder,
            )
            .map_err(|_| crate::Error::Decompress(compression))?;
            *decoded = written;
            if written != raw_len {
                return Err(crate::Error::Decompress(compression));
            }
            Ok(builder.freeze().into())
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
    /// `compression` is a dictionary codec's level without its dictionary,
    /// which the frame says by [`FLAG_PLAIN`] once it is compressed.
    pub(crate) plain: bool,
}

/// `payload` compressed under `codec`, or `None` when that saves nothing and
/// the payload is stored as it is: without a codec built in, always `None`.
#[cfg(not(any(feature = "lz4", zstd_any)))]
#[expect(
    clippy::unnecessary_wraps,
    reason = "without a codec built in, nothing is compressed and nothing fails"
)]
fn compress(_payload: &[u8], _codec: Codec<'_>) -> crate::Result<Option<Vec<u8>>> {
    Ok(None)
}

/// `payload` compressed under `codec`, or `None` when that saves nothing and
/// the payload is stored as it is.
#[cfg(any(feature = "lz4", zstd_any))]
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

/// `payload` compressed under `codec`, or `None` when that saves nothing.
///
/// # Errors
///
/// Propagates the compression.
pub(crate) fn compress_payload(payload: &[u8], codec: Codec<'_>) -> crate::Result<Option<Vec<u8>>> {
    compress(payload, codec)
}

/// `stored` decompressed under `compression` into exactly `raw_len` bytes.
///
/// # Errors
///
/// A decompression that fails or produces another length.
pub(crate) fn decompress_payload(
    stored: &[u8],
    raw_len: usize,
    compression: CompressionType,
    #[cfg(zstd_any)] dictionary: Option<&crate::compression::ZstdDictionary>,
    decoded: &mut usize,
) -> crate::Result<Slice> {
    decompress(
        stored,
        raw_len,
        compression,
        #[cfg(zstd_any)]
        dictionary,
        decoded,
    )
}

/// The header of a frame of `kind` and `flags` listing `count` values whose
/// entries take `entries_len` bytes, `raw_len` decoded, its payload
/// `disk_len` stored, its checksum what `checksum` computes from the header's
/// CRC.
#[must_use]
pub(crate) fn stamp_header(
    kind: FrameKind,
    flags: u8,
    count: u16,
    entries_len: u32,
    raw_len: u32,
    disk_len: u32,
    checksum: impl FnOnce(u32) -> u128,
) -> [u8; FRAME_HEADER_LEN] {
    let crc = header_crc(kind, flags, count, entries_len, raw_len, disk_len);
    let checksum = checksum(crc);
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
    header
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
#[cfg(test)]
pub(crate) fn encode_frame<'e, E: core::borrow::Borrow<EntrySpec<'e>>>(
    kind: FrameKind,
    entries: impl IntoIterator<Item = E, IntoIter: ExactSizeIterator>,
    payload: &[u8],
    codec: Codec<'_>,
    out: &mut Vec<u8>,
) -> crate::Result<()> {
    if encode_frame_parts(kind, entries, payload, codec, out)? {
        out.extend_from_slice(payload);
    }
    Ok(())
}

/// [`encode_frame`], the payload left out of `out` when it is stored as it is
/// (`true`): the frame is `out` followed by `payload`, which a writer then
/// writes from where it lies rather than copying it in first.
///
/// # Errors
///
/// As [`encode_frame`].
///
/// # Panics
///
/// As [`encode_frame`].
pub(crate) fn encode_frame_parts<'e, E: core::borrow::Borrow<EntrySpec<'e>>>(
    kind: FrameKind,
    entries: impl IntoIterator<Item = E, IntoIter: ExactSizeIterator>,
    payload: &[u8],
    codec: Codec<'_>,
    out: &mut Vec<u8>,
) -> crate::Result<bool> {
    super::writer::check_size_cap(payload.len())?;
    let compressed = if kind == FrameKind::Head {
        None
    } else {
        compress(payload, codec)?
    };
    encode_frame_parts_compressed(kind, entries, payload, codec, compressed.as_deref(), out)
}

/// [`encode_frame_parts`] with the payload compressed already as
/// `compressed`, what compressing it under `codec` made (`None` when that
/// saved nothing).
///
/// # Errors
///
/// [`crate::Error::DecompressedSizeTooLarge`] for a payload past the value
/// cap.
///
/// # Panics
///
/// As [`encode_frame`].
pub(crate) fn encode_frame_parts_compressed<'e, E: core::borrow::Borrow<EntrySpec<'e>>>(
    kind: FrameKind,
    entries: impl IntoIterator<Item = E, IntoIter: ExactSizeIterator>,
    payload: &[u8],
    codec: Codec<'_>,
    compressed: Option<&[u8]>,
    out: &mut Vec<u8>,
) -> crate::Result<bool> {
    let entries = entries.into_iter();
    super::writer::check_size_cap(payload.len())?;
    #[expect(
        clippy::expect_used,
        reason = "the writer closes a block before u16::MAX entries"
    )]
    let count = u16::try_from(entries.len()).expect("a frame lists at most u16::MAX entries");
    // Raw when nothing compressed it although the file names a codec, and for
    // a head always: the flag tells a reader not to decode.
    let flags = if kind == FrameKind::Head
        || (compressed.is_none() && codec.compression != CompressionType::None)
    {
        FLAG_RAW
    } else if codec.plain {
        FLAG_PLAIN
    } else {
        0
    };
    let stored: &[u8] = compressed.unwrap_or(payload);

    out.clear();
    out.resize(FRAME_HEADER_LEN, 0);
    for entry in entries {
        let entry = entry.borrow();
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
    if let Some(compressed) = compressed {
        out.extend_from_slice(compressed);
    }

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
    let checksum = frame_checksum_parts(
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
    // `out` was laid out with room for the header first.
    let (slot, _) = out.split_at_mut(FRAME_HEADER_LEN);
    slot.copy_from_slice(&header);
    Ok(compressed.is_none())
}

/// Relists the value `frame` lists, a single or head frame listing one, at
/// `seqno`: `prefix` is filled with the frame's header and entries, the entry
/// rewritten and the checksum recomputed over the same stored payload, and
/// the offset the payload starts at in `frame` is returned. The relisted
/// frame is `prefix` followed by `frame` from that offset, so the payload is
/// neither decoded nor copied. `None` for a frame listed at `seqno` already,
/// which is written as it is.
///
/// # Errors
///
/// [`crate::Error::InvalidHeader`] when `frame` is not one whole frame listing
/// a single value; the header errors of [`parse_header`].
pub(crate) fn relist_prefix(
    frame: &[u8],
    seqno: SeqNo,
    prefix: &mut Vec<u8>,
) -> crate::Result<Option<usize>> {
    let malformed = || crate::Error::InvalidHeader("BlobFrame");
    let header = parse_header(frame)?;
    if header.count != 1 || !matches!(header.kind, FrameKind::Single | FrameKind::Head) {
        return Err(malformed());
    }
    let entries_end = FRAME_HEADER_LEN + header.entries_len as usize;
    let frame_end = entries_end + header.disk_len as usize;
    if frame.len() != frame_end || entries_end < FRAME_HEADER_LEN + ENTRY_HEADER_LEN {
        return Err(malformed());
    }
    let listed = frame
        .get(FRAME_HEADER_LEN..FRAME_HEADER_LEN + 8)
        .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
        .map(SeqNo::from_le_bytes);
    if listed == Some(seqno) {
        return Ok(None);
    }
    // The lengths were checked above: the header and the entries lie within
    // `frame`, the entry's seqno within the entries.
    let (head_and_entries, payload) = frame.split_at(entries_end);
    prefix.clear();
    prefix.extend_from_slice(head_and_entries);
    let (head, entries) = prefix.split_at_mut(FRAME_HEADER_LEN);
    let (listed_seqno, _) = entries.split_at_mut(8);
    listed_seqno.copy_from_slice(&seqno.to_le_bytes());
    let checksum = frame_checksum_parts(entries, payload, header.header_crc);
    // The checksum is the header's last field.
    let (_, stored_checksum) = head.split_at_mut(FRAME_HEADER_LEN - 16);
    stored_checksum.copy_from_slice(&checksum.to_le_bytes());
    Ok(Some(entries_end))
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

/// Seals `frame`, its entries or payload edited in place, as a writer would:
/// its header CRC and its checksum over what it now holds.
#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test code")]
pub(crate) fn reseal(frame: &mut [u8]) {
    restamp_header_crc(frame);
    let header = parse_header(frame).unwrap();
    let checksum = frame_checksum(frame.get(FRAME_HEADER_LEN..).unwrap(), header.header_crc);
    frame
        .get_mut(FRAME_HEADER_LEN - 16..FRAME_HEADER_LEN)
        .unwrap()
        .copy_from_slice(&checksum.to_le_bytes());
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
        plain: false,
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
#[expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    reason = "test code: fixture frames are small enough for every cast"
)]
mod tests;

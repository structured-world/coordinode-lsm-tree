// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Columnar (PAX / rowgroup) block format and the [`ColumnBatch`] read unit.
//!
//! A columnar block holds a row-group laid out column-by-column: each column is
//! an opaque, typed, codec-tagged byte array plus an optional validity bitmap.
//! The engine attaches no relational or graph meaning to a column; it knows
//! only the physical [`TypeTag`], the [`CodecId`] used to encode the bytes, and
//! a caller-assigned `column_id`. This is the payload of a
//! [`BlockType::Columnar`](crate::table::block::BlockType::Columnar) block. The
//! intrinsic-field transpose ([`entries_to_column_batch`] and back), the
//! encode, and the decode path are all `core` + `alloc` and live here; wiring
//! the transpose into the flush / compaction writer is std-only and lands
//! separately.
//!
//! # Examples
//!
//! ```
//! use lsm_tree::table::columnar::{CodecId, Column, ColumnBatch, TypeTag};
//!
//! // Two rows, one fixed-width u32 column with the second row null.
//! let batch = ColumnBatch {
//!     row_count: 2,
//!     columns: vec![Column {
//!         column_id: 7,
//!         type_tag: TypeTag::Fixed(4),
//!         validity: Some(vec![0b0000_0001]), // row 0 valid, row 1 null
//!         data: vec![1, 0, 0, 0, 0, 0, 0, 0],
//!     }],
//! };
//! let bytes = batch.encode(CodecId::Plain).unwrap();
//! assert_eq!(ColumnBatch::decode(&bytes).unwrap(), batch);
//! ```

use crate::{Error, Result, Slice, ValueType, key::InternalKey, value::InternalValue};
use alloc::vec::Vec;

/// Physical layout category of a column's values. Drives codec selection and
/// decode framing; it carries no logical (schema) meaning.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TypeTag {
    /// Fixed-width values: every row occupies exactly `N` bytes (`N > 0`).
    Fixed(u8),
    /// Variable-width opaque byte arrays. The column data is a
    /// `(row_count + 1)`-entry little-endian `u32` offset array followed by the
    /// concatenated value bytes; row `i` spans `offset[i]..offset[i + 1]`.
    Bytes,
}

impl TypeTag {
    /// Wire form: a `(tag, width)` pair. `width` is the fixed byte width, or `0`
    /// for the variable-width [`TypeTag::Bytes`].
    const fn to_wire(self) -> (u8, u8) {
        match self {
            Self::Fixed(width) => (0, width),
            Self::Bytes => (1, 0),
        }
    }

    fn from_wire(tag: u8, width: u8) -> Result<Self> {
        match tag {
            0 => {
                if width == 0 {
                    return Err(Error::InvalidHeader("columnar: fixed column width is zero"));
                }
                Ok(Self::Fixed(width))
            }
            1 => {
                if width != 0 {
                    return Err(Error::InvalidHeader(
                        "columnar: bytes column width must be zero",
                    ));
                }
                Ok(Self::Bytes)
            }
            _ => Err(Error::InvalidTag(("ColumnTypeTag", tag))),
        }
    }
}

/// The codec used to encode a column chunk's bytes.
///
/// [`CodecId::Plain`] is the identity codec (decoded bytes equal encoded
/// bytes); structure-aware codecs (delta, dictionary, ...) join this tag space
/// in later slices.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum CodecId {
    /// Identity: the column bytes are stored verbatim.
    Plain,
    /// Delta: each fixed-width integer is stored as its wrapping difference from
    /// the previous one. A near-monotonic column (e.g. the seqno column) becomes
    /// small deltas that the terminal byte compressor squeezes far better.
    Delta,
}

impl From<CodecId> for u8 {
    fn from(c: CodecId) -> Self {
        match c {
            CodecId::Plain => 0,
            CodecId::Delta => 1,
        }
    }
}

impl TryFrom<u8> for CodecId {
    type Error = Error;

    fn try_from(v: u8) -> Result<Self> {
        match v {
            0 => Ok(Self::Plain),
            1 => Ok(Self::Delta),
            _ => Err(Error::InvalidTag(("ColumnCodecId", v))),
        }
    }
}

/// Picks a logical codec for a column from its physical type. A fixed 8-byte
/// column (the seqno column) delta-encodes well; other columns get the caller's
/// default. More type-directed choices (dictionary on keys, FOR on small
/// widths) join here as they land.
const fn auto_codec(column_id: u16, type_tag: TypeTag) -> Option<CodecId> {
    // Delta suits the near-monotonic seqno column specifically. Selecting by
    // column id (not just the 8-byte width) keeps a future unrelated Fixed(8)
    // column (hashes, random ids) from being delta-encoded by accident, which
    // would only inflate its size.
    match (column_id, type_tag) {
        (COL_SEQNO, TypeTag::Fixed(8)) => Some(CodecId::Delta),
        _ => None,
    }
}

/// Reads an 8-byte little-endian chunk as a `u64` (the caller passes a
/// `chunks_exact(8)` slice, so no indexing or fallible conversion is needed).
fn read_u64_le(chunk: &[u8]) -> u64 {
    let mut arr = [0u8; 8];
    for (dst, &src) in arr.iter_mut().zip(chunk) {
        *dst = src;
    }
    u64::from_le_bytes(arr)
}

/// Delta-encodes a fixed 8-byte integer column: each value becomes its wrapping
/// difference from the previous one (the first from zero). Same length.
fn delta_encode_u64(data: &[u8]) -> Vec<u8> {
    // A Fixed(8) column's length is a multiple of 8 (Column::validate enforces
    // it before encode). Assert it so a future caller that bypasses validation
    // fails loudly instead of silently dropping a trailing partial chunk (the
    // decoder already rejects the same condition).
    debug_assert!(
        data.len().is_multiple_of(8),
        "delta_encode_u64: input length is not a multiple of 8"
    );
    let mut out = Vec::with_capacity(data.len());
    let mut prev = 0u64;
    for chunk in data.chunks_exact(8) {
        let v = read_u64_le(chunk);
        out.extend_from_slice(&v.wrapping_sub(prev).to_le_bytes());
        prev = v;
    }
    out
}

/// Inverse of [`delta_encode_u64`]: a running wrapping sum of the deltas.
fn delta_decode_u64(data: &[u8]) -> Result<Vec<u8>> {
    if !data.len().is_multiple_of(8) {
        return Err(Error::InvalidHeader(
            "columnar: delta column length is not a multiple of 8",
        ));
    }
    let mut out = Vec::with_capacity(data.len());
    let mut acc = 0u64;
    for chunk in data.chunks_exact(8) {
        acc = acc.wrapping_add(read_u64_le(chunk));
        out.extend_from_slice(&acc.to_le_bytes());
    }
    Ok(out)
}

/// Encodes a column's decoded bytes under `codec`. Delta requires a `Fixed(8)`
/// column (it processes whole 8-byte values); applying it to any other type is
/// rejected rather than silently truncating a partial trailing value.
fn codec_encode(codec: CodecId, type_tag: TypeTag, data: &[u8]) -> Result<Vec<u8>> {
    match codec {
        CodecId::Plain => Ok(data.to_vec()),
        CodecId::Delta if matches!(type_tag, TypeTag::Fixed(8)) => Ok(delta_encode_u64(data)),
        CodecId::Delta => Err(Error::InvalidHeader(
            "columnar: delta codec requires a fixed-8 column",
        )),
    }
}

/// Decodes a column's stored bytes under `codec` back to the logical bytes,
/// rejecting Delta on a non-`Fixed(8)` column (the inverse of [`codec_encode`]).
fn codec_decode(codec: CodecId, type_tag: TypeTag, data: &[u8]) -> Result<Vec<u8>> {
    match codec {
        CodecId::Plain => Ok(data.to_vec()),
        CodecId::Delta if matches!(type_tag, TypeTag::Fixed(8)) => delta_decode_u64(data),
        CodecId::Delta => Err(Error::InvalidHeader(
            "columnar: delta codec requires a fixed-8 column",
        )),
    }
}

/// One decoded column of a [`ColumnBatch`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Column {
    /// Caller-assigned logical column identifier (opaque to the engine).
    pub column_id: u16,
    /// Physical layout of the values.
    pub type_tag: TypeTag,
    /// Per-row validity. `None` means every row is valid; otherwise one bit per
    /// row, LSB-first, `row_count` bits padded to whole bytes (a set bit = the
    /// row is valid / non-null).
    pub validity: Option<Vec<u8>>,
    /// Decoded column bytes, framed per [`TypeTag`].
    pub data: Vec<u8>,
}

/// A decoded columnar row-group: the read unit obtained by decoding a
/// [`BlockType::Columnar`](crate::table::block::BlockType::Columnar) block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnBatch {
    /// Number of rows every column in the batch describes.
    pub row_count: u32,
    /// The columns, in write order.
    pub columns: Vec<Column>,
}

/// Number of validity-bitmap bytes for `row_count` rows (one bit per row).
const fn validity_len(row_count: u32) -> usize {
    (row_count as usize).div_ceil(8)
}

/// Validates a validity bitmap: it must be exactly `ceil(row_count / 8)` bytes,
/// and every padding bit above `row_count` in the final byte must be zero (so a
/// consumer that pop-counts the byte cannot read an impossible row count).
fn check_validity(v: &[u8], row_count: u32) -> Result<()> {
    if v.len() != validity_len(row_count) {
        return Err(Error::InvalidHeader(
            "columnar: validity bitmap length is not ceil(row_count / 8)",
        ));
    }
    let used = row_count % 8;
    if used != 0 {
        // The length check above guarantees a final byte exists here.
        let last = v.last().copied().unwrap_or(0);
        let valid_mask = (1u8 << used) - 1;
        if last & !valid_mask != 0 {
            return Err(Error::InvalidHeader(
                "columnar: validity padding bits above row_count must be zero",
            ));
        }
    }
    Ok(())
}

/// Validates the framing of a [`TypeTag::Bytes`] column: a `(row_count + 1)`
/// little-endian `u32` offset table followed by the payload, where the first
/// offset is `0`, offsets are non-decreasing, and the last offset equals the
/// payload length (so `offset[i]..offset[i + 1]` slicing by a consumer is
/// always in bounds).
fn check_bytes_framing(data: &[u8], row_count: u32) -> Result<()> {
    let off_count = (row_count as usize)
        .checked_add(1)
        .ok_or(Error::InvalidHeader(
            "columnar: bytes offset count overflow",
        ))?;
    let off_bytes = off_count.checked_mul(4).ok_or(Error::InvalidHeader(
        "columnar: bytes offset table overflow",
    ))?;
    let table = data.get(..off_bytes).ok_or(Error::InvalidHeader(
        "columnar: bytes column shorter than its offset table",
    ))?;
    let payload_len = data.len() - off_bytes;
    let mut prev = 0usize;
    for (i, chunk) in table.chunks_exact(4).enumerate() {
        let off = u32::from_le_bytes(
            chunk
                .try_into()
                .map_err(|_| Error::InvalidHeader("columnar: short bytes offset"))?,
        ) as usize;
        if i == 0 && off != 0 {
            return Err(Error::InvalidHeader(
                "columnar: first bytes offset must be zero",
            ));
        }
        if off < prev {
            return Err(Error::InvalidHeader(
                "columnar: bytes offsets must be non-decreasing",
            ));
        }
        if off > payload_len {
            return Err(Error::InvalidHeader(
                "columnar: bytes offset past payload end",
            ));
        }
        prev = off;
    }
    // The last offset must reach exactly the payload end (no trailing payload).
    if prev != payload_len {
        return Err(Error::InvalidHeader(
            "columnar: final bytes offset must equal the payload length",
        ));
    }
    Ok(())
}

impl Column {
    /// Validates that the column is well-formed for `row_count` rows: a nonzero
    /// fixed width with `row_count * width` data bytes, a correctly framed
    /// `Bytes` offset table, and a correctly sized / padded validity bitmap.
    /// `encode` and `decode` both run this so a payload is accepted by one iff
    /// it is accepted by the other.
    fn validate(&self, row_count: u32) -> Result<()> {
        match self.type_tag {
            TypeTag::Fixed(0) => {
                return Err(Error::InvalidHeader("columnar: fixed column width is zero"));
            }
            TypeTag::Fixed(w) => {
                let expected =
                    (row_count as usize)
                        .checked_mul(w as usize)
                        .ok_or(Error::InvalidHeader(
                            "columnar: fixed column length overflow",
                        ))?;
                if self.data.len() != expected {
                    return Err(Error::InvalidHeader(
                        "columnar: fixed column byte length is not row_count * width",
                    ));
                }
            }
            TypeTag::Bytes => check_bytes_framing(&self.data, row_count)?,
        }
        if let Some(v) = &self.validity {
            check_validity(v, row_count)?;
        }
        Ok(())
    }
}

impl ColumnBatch {
    /// The first row's user key, or `None` for an empty batch. Reads only the
    /// intrinsic key column, so the ingest ordering guard can check a batch
    /// against the previously written key without decoding the whole batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the first column is not the intrinsic user-key column
    /// or its row framing is malformed.
    pub(crate) fn first_user_key(&self) -> Result<Option<&[u8]>> {
        if self.row_count == 0 {
            return Ok(None);
        }
        let key_col = self
            .columns
            .first()
            .filter(|c| c.column_id == COL_USER_KEY)
            .ok_or(Error::InvalidHeader(
                "columnar: first column is not the user-key column",
            ))?;
        // This runs before the full `column_batch_to_entries` validation, so
        // confirm the intrinsic key column's shape (non-null `Bytes`, correctly
        // framed for `row_count`) before the low-level offset read.
        if key_col.type_tag != TypeTag::Bytes || key_col.validity.is_some() {
            return Err(Error::InvalidHeader(
                "columnar: first column is not the non-null user-key column",
            ));
        }
        key_col.validate(self.row_count)?;
        bytes_column_row(&key_col.data, self.row_count, 0).map(Some)
    }

    /// Encodes the batch into a columnar block payload using `codec` for every
    /// column. The returned bytes are the block payload (without the surrounding
    /// block header / checksum, which the writer adds).
    ///
    /// # Errors
    ///
    /// Returns an error if any column is malformed for the batch's `row_count`:
    /// a zero fixed width, a fixed column whose byte length is not
    /// `row_count * width`, a mis-framed `Bytes` offset table, or a validity
    /// bitmap of the wrong length or with non-zero padding bits. This makes the
    /// encoder's accepted set exactly match the decoder's, so every produced
    /// payload round-trips.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "column count and per-column chunk length are bounded by the block size policy, far below u32::MAX"
    )]
    pub fn encode(&self, codec: CodecId) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        out.extend_from_slice(&self.row_count.to_le_bytes());
        out.extend_from_slice(&(self.columns.len() as u32).to_le_bytes());
        for col in &self.columns {
            col.validate(self.row_count)?;
            // Pick a type-directed codec (e.g. Delta on the seqno column),
            // falling back to the caller's default; record it per column.
            let col_codec = auto_codec(col.column_id, col.type_tag).unwrap_or(codec);
            let encoded = codec_encode(col_codec, col.type_tag, &col.data)?;
            let (type_tag, width) = col.type_tag.to_wire();
            out.extend_from_slice(&col.column_id.to_le_bytes());
            out.push(type_tag);
            out.push(width);
            out.push(col_codec.into());
            out.push(u8::from(col.validity.is_some()));
            out.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
            if let Some(v) = &col.validity {
                out.extend_from_slice(v);
            }
            out.extend_from_slice(&encoded);
        }
        Ok(out)
    }

    /// Decodes a columnar block payload produced by [`ColumnBatch::encode`].
    ///
    /// # Errors
    ///
    /// Returns an error if the payload is truncated, has trailing bytes after
    /// the last declared column, declares more columns than the remaining bytes
    /// could hold, carries an unknown type / codec tag, a non-canonical width /
    /// validity flag, or any column that fails [`Column::validate`] (fixed-width
    /// length, `Bytes` offset framing, validity bitmap length / padding).
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Self::decode_inner(bytes, None)
    }

    /// Decodes only the columns whose id is in `wanted`, advancing past every
    /// other column's bytes without allocating or running its codec. This is the
    /// projection read: a scan asking for a subset of the columns never pays to
    /// decode the rest (e.g. a key-only scan does not decode the value column).
    /// The returned batch carries only the projected columns, in write order.
    ///
    /// # Errors
    ///
    /// As [`ColumnBatch::decode`], evaluated only for the projected columns
    /// (the headers of skipped columns are still framing-checked).
    pub fn decode_projected(bytes: &[u8], wanted: &[u16]) -> Result<Self> {
        Self::decode_inner(bytes, Some(wanted))
    }

    /// Shared decode body. `wanted == None` decodes every column; `Some(ids)`
    /// decodes only the listed columns and skips the rest (their validity + data
    /// bytes are stepped over, never allocated or codec-decoded).
    fn decode_inner(bytes: &[u8], wanted: Option<&[u16]>) -> Result<Self> {
        // Smallest possible column: id(2) + type(1) + width(1) + codec(1) +
        // has_validity(1) + data_len(4), with empty validity + data.
        const MIN_COLUMN_BYTES: usize = 10;
        let mut cur = Cursor::new(bytes);
        let row_count = cur.read_u32()?;
        let column_count = cur.read_u32()? as usize;
        // Bound the declared column count by the bytes that remain before
        // reserving, so a tiny payload claiming a huge count cannot trigger a
        // giant allocation ahead of the per-column truncation checks.
        if column_count > cur.remaining() / MIN_COLUMN_BYTES {
            return Err(Error::InvalidHeader(
                "columnar: declared column count exceeds payload size",
            ));
        }
        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            let column_id = cur.read_u16()?;
            let type_tag = cur.read_u8()?;
            let width = cur.read_u8()?;
            let codec = CodecId::try_from(cur.read_u8()?)?;
            let has_validity = match cur.read_u8()? {
                0 => false,
                1 => true,
                _ => {
                    return Err(Error::InvalidHeader(
                        "columnar: validity flag must be 0 or 1",
                    ));
                }
            };
            let data_len = cur.read_u32()? as usize;
            let type_tag = TypeTag::from_wire(type_tag, width)?;
            // A skipped column's validity + data are stepped over (the cursor
            // still bounds-checks the lengths) but never copied or codec-decoded.
            let want = wanted.is_none_or(|w| w.contains(&column_id));
            let validity = if has_validity {
                let v = cur.read_bytes(validity_len(row_count))?;
                if want { Some(v.to_vec()) } else { None }
            } else {
                None
            };
            let raw = cur.read_bytes(data_len)?;
            if !want {
                continue;
            }
            // Restore the column's logical bytes from its stored codec form
            // (Plain is identity; Delta runs the prefix-sum).
            let data = codec_decode(codec, type_tag, raw)?;
            let column = Column {
                column_id,
                type_tag,
                validity,
                data,
            };
            // Same well-formedness gate the encoder runs, so a payload decodes
            // iff it could have been produced by `encode`.
            column.validate(row_count)?;
            columns.push(column);
        }
        if !cur.is_empty() {
            return Err(Error::InvalidHeader(
                "columnar: trailing bytes after the last column",
            ));
        }
        Ok(Self { row_count, columns })
    }
}

/// A bounds-checked little-endian read cursor over a byte slice.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    const fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    /// Bytes not yet consumed.
    const fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// Whether every byte has been consumed.
    const fn is_empty(&self) -> bool {
        self.pos == self.buf.len()
    }

    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or(Error::InvalidHeader("columnar: read length overflow"))?;
        let slice = self
            .buf
            .get(self.pos..end)
            .ok_or(Error::InvalidHeader("columnar: truncated block payload"))?;
        self.pos = end;
        Ok(slice)
    }

    /// Reads exactly `N` bytes as a fixed array (no slice indexing, so it stays
    /// clear of the panic-on-index lint).
    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let arr: [u8; N] = self
            .read_bytes(N)?
            .try_into()
            .map_err(|_| Error::InvalidHeader("columnar: short fixed-width read"))?;
        Ok(arr)
    }

    fn read_u8(&mut self) -> Result<u8> {
        let [b] = self.read_array::<1>()?;
        Ok(b)
    }

    fn read_u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.read_array()?))
    }

    fn read_u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.read_array()?))
    }
}

// --- Intrinsic-field transpose (engine-side, schema-free) ------------------
//
// The engine's columnar foundation lays each entry's intrinsic fields out as a
// PAX row-group: one column for the user key, the seqno, the value type, and
// the (opaque) value. This is schema-free and works for any tree; splitting the
// value into per-field sub-columns is the consumer's concern and lives behind a
// separate columnar ingest path, not here.

/// Column id of the user-key column in the intrinsic transpose.
pub const COL_USER_KEY: u16 = 0;
/// Column id of the seqno column.
pub const COL_SEQNO: u16 = 1;
/// Column id of the value-type column.
pub const COL_VALUE_TYPE: u16 = 2;
/// Column id of the (opaque) value column.
pub const COL_VALUE: u16 = 3;

/// Builds a [`TypeTag::Bytes`] column body from per-row byte slices: a
/// `(rows + 1)` little-endian `u32` offset table followed by the concatenated
/// payload.
fn build_bytes_column<'a>(rows: impl Iterator<Item = &'a [u8]>) -> Result<Vec<u8>> {
    let mut offsets = Vec::new();
    let mut payload = Vec::new();
    let mut off: u32 = 0;
    offsets.extend_from_slice(&off.to_le_bytes());
    for r in rows {
        let len = u32::try_from(r.len())
            .map_err(|_| Error::InvalidHeader("columnar: column value exceeds u32"))?;
        off = off
            .checked_add(len)
            .ok_or(Error::InvalidHeader("columnar: column payload exceeds u32"))?;
        payload.extend_from_slice(r);
        offsets.extend_from_slice(&off.to_le_bytes());
    }
    offsets.extend_from_slice(&payload);
    Ok(offsets)
}

/// Reads row `i` of a [`TypeTag::Bytes`] column body (offset table + payload),
/// bounds-checked.
fn bytes_column_row(data: &[u8], row_count: u32, i: u32) -> Result<&[u8]> {
    let off_bytes = (row_count as usize + 1) * 4;
    let read_off = |idx: u32| -> Result<usize> {
        let base = idx as usize * 4;
        let b = data
            .get(base..base + 4)
            .ok_or(Error::InvalidHeader("columnar: bytes offset truncated"))?;
        let arr: [u8; 4] = b
            .try_into()
            .map_err(|_| Error::InvalidHeader("columnar: short bytes offset"))?;
        Ok(u32::from_le_bytes(arr) as usize)
    };
    let start = read_off(i)?;
    let end = read_off(i + 1)?;
    let payload = data
        .get(off_bytes..)
        .ok_or(Error::InvalidHeader("columnar: bytes payload truncated"))?;
    payload
        .get(start..end)
        .ok_or(Error::InvalidHeader("columnar: bytes row out of range"))
}

/// Reads row `i` of a `Fixed(8)` column body as a little-endian `u64`.
fn fixed_u64_row(data: &[u8], i: u32) -> Result<u64> {
    let base = i as usize * 8;
    let b = data
        .get(base..base + 8)
        .ok_or(Error::InvalidHeader("columnar: fixed8 row truncated"))?;
    let arr: [u8; 8] = b
        .try_into()
        .map_err(|_| Error::InvalidHeader("columnar: short fixed8 row"))?;
    Ok(u64::from_le_bytes(arr))
}

/// Transposes a run of entries into the engine's intrinsic columnar layout: one
/// column each for `user_key`, `seqno`, `value_type`, and the opaque `value`.
///
/// # Errors
///
/// Returns an error if a column's total byte length or the row count exceeds the
/// `u32` wire limits.
pub fn entries_to_column_batch(entries: &[InternalValue]) -> Result<ColumnBatch> {
    let row_count = u32::try_from(entries.len())
        .map_err(|_| Error::InvalidHeader("columnar: row count exceeds u32"))?;
    let key_data = build_bytes_column(entries.iter().map(|e| e.key.user_key.as_ref()))?;
    let value_data = build_bytes_column(entries.iter().map(|e| e.value.as_ref()))?;
    let mut seqno_data = Vec::with_capacity(entries.len() * 8);
    let mut vt_data = Vec::with_capacity(entries.len());
    for e in entries {
        seqno_data.extend_from_slice(&e.key.seqno.to_le_bytes());
        vt_data.push(u8::from(e.key.value_type));
    }
    let columns = alloc::vec![
        Column {
            column_id: COL_USER_KEY,
            type_tag: TypeTag::Bytes,
            validity: None,
            data: key_data,
        },
        Column {
            column_id: COL_SEQNO,
            type_tag: TypeTag::Fixed(8),
            validity: None,
            data: seqno_data,
        },
        Column {
            column_id: COL_VALUE_TYPE,
            type_tag: TypeTag::Fixed(1),
            validity: None,
            data: vt_data,
        },
        Column {
            column_id: COL_VALUE,
            type_tag: TypeTag::Bytes,
            validity: None,
            data: value_data,
        },
    ];
    Ok(ColumnBatch { row_count, columns })
}

/// Reconstructs the entries from an intrinsic columnar batch produced by
/// [`entries_to_column_batch`].
///
/// # Errors
///
/// Returns an error if the batch does not carry exactly the four intrinsic
/// columns in order with the expected type tags, or if a row is truncated or
/// carries an unknown value type.
/// Returns row `row`'s `width`-byte cell from a fixed-width column's data.
fn fixed_column_row(data: &[u8], width: u8, row: u32) -> Result<&[u8]> {
    let w = width as usize;
    let start = (row as usize).checked_mul(w).ok_or(Error::InvalidHeader(
        "columnar: fixed column offset overflow",
    ))?;
    let end = start.checked_add(w).ok_or(Error::InvalidHeader(
        "columnar: fixed column offset overflow",
    ))?;
    data.get(start..end)
        .ok_or(Error::InvalidHeader("columnar: fixed column row truncated"))
}

/// Returns row `row`'s cell bytes from `col`, dispatching on the column's type.
fn column_cell(col: &Column, row_count: u32, row: u32) -> Result<&[u8]> {
    match col.type_tag {
        TypeTag::Fixed(width) => fixed_column_row(&col.data, width, row),
        TypeTag::Bytes => bytes_column_row(&col.data, row_count, row),
    }
}

/// Reconstructs a row's value from its value sub-columns.
///
/// With no nullable sub-column: a single sub-column yields the cell verbatim (the
/// intrinsic opaque value, or a degenerate one-column consumer value), and two or
/// more are joined by [`frame_value_cells`]. When any sub-column carries a
/// validity bitmap, the row is framed with [`frame_value_cells_nullable`] (a
/// presence bitmap plus the present cells), which the consumer reverses with
/// [`unframe_value_cells_nullable`] / [`unframe_value_cells_with_defaults`].
fn reconstruct_row_value(value_cols: &[Column], row_count: u32, row: u32) -> Result<Slice> {
    if value_cols.iter().any(|c| c.validity.is_some()) {
        let mut cells = Vec::with_capacity(value_cols.len());
        for col in value_cols {
            cells.push((col.type_tag, column_value_cell(col, row_count, row)?));
        }
        return Ok(Slice::from(frame_value_cells_nullable(&cells)?));
    }
    if let [single] = value_cols {
        return Ok(Slice::from(column_cell(single, row_count, row)?));
    }
    let mut cells = Vec::with_capacity(value_cols.len());
    for col in value_cols {
        cells.push((col.type_tag, column_cell(col, row_count, row)?));
    }
    Ok(Slice::from(frame_value_cells(&cells)?))
}

pub fn column_batch_to_entries(batch: &ColumnBatch) -> Result<Vec<InternalValue>> {
    let [key_col, seqno_col, vt_col, value_cols @ ..] = batch.columns.as_slice() else {
        return Err(Error::InvalidHeader(
            "columnar: batch missing the intrinsic columns",
        ));
    };
    if value_cols.is_empty() {
        return Err(Error::InvalidHeader(
            "columnar: batch carries no value column",
        ));
    }
    if key_col.column_id != COL_USER_KEY
        || key_col.type_tag != TypeTag::Bytes
        || seqno_col.column_id != COL_SEQNO
        || seqno_col.type_tag != TypeTag::Fixed(8)
        || vt_col.column_id != COL_VALUE_TYPE
        || vt_col.type_tag != TypeTag::Fixed(1)
    {
        return Err(Error::InvalidHeader(
            "columnar: unexpected intrinsic column layout",
        ));
    }
    // The three intrinsic fields are never null. Running `validate` also bounds
    // `row_count` against the fixed-width column lengths before the allocation
    // below, so a malformed batch claiming a huge row count is rejected instead
    // of reserving for billions of rows.
    for col in [key_col, seqno_col, vt_col] {
        if col.validity.is_some() {
            return Err(Error::InvalidHeader(
                "columnar: intrinsic columns must not be nullable",
            ));
        }
        col.validate(batch.row_count)?;
    }
    // Value sub-columns are consumer-defined (id / type / count) and may be
    // nullable. Their ids must be unique and must not overlap the intrinsic
    // columns (`< COL_VALUE`), since projection selects columns by id and a
    // collision would make the result ambiguous. `validate` bounds each against
    // `row_count` and checks the validity bitmap (length + zero padding) like the
    // intrinsics.
    let mut seen_value_column_ids = Vec::with_capacity(value_cols.len());
    for col in value_cols {
        if col.column_id < COL_VALUE || seen_value_column_ids.contains(&col.column_id) {
            return Err(Error::InvalidHeader(
                "columnar: value sub-column ids must be unique and must not overlap intrinsic columns",
            ));
        }
        seen_value_column_ids.push(col.column_id);
        col.validate(batch.row_count)?;
    }
    let mut out = Vec::with_capacity(batch.row_count as usize);
    for i in 0..batch.row_count {
        let user_key = bytes_column_row(&key_col.data, batch.row_count, i)?;
        // Match the engine's key invariants (non-empty, fits the u16 length the
        // table encoder casts to) so a malformed row cannot become an entry that
        // corrupts later block encoding.
        if user_key.is_empty() || user_key.len() > u16::MAX as usize {
            return Err(Error::InvalidHeader(
                "columnar: user key is empty or longer than u16::MAX",
            ));
        }
        let seqno = fixed_u64_row(&seqno_col.data, i)?;
        let vt_byte = vt_col
            .data
            .get(i as usize)
            .copied()
            .ok_or(Error::InvalidHeader("columnar: value-type row truncated"))?;
        let value_type =
            ValueType::try_from(vt_byte).map_err(|()| Error::InvalidTag(("ValueType", vt_byte)))?;
        let value = reconstruct_row_value(value_cols, batch.row_count, i)?;
        out.push(InternalValue {
            key: InternalKey {
                user_key: Slice::from(user_key),
                seqno,
                value_type,
            },
            value,
        });
    }
    Ok(out)
}

/// Frames one row's value sub-column cells into a single self-describing value
/// blob.
///
/// This is the form the row read paths (point / range / merge-on-read) return for
/// a row whose value the consumer split into sub-columns (the read-path model:
/// reconstruct from sub-columns on read, no opaque copy).
///
/// A fixed-width cell is stored verbatim: its width is recoverable from the
/// column's [`TypeTag`], so a fixed sub-column (e.g. a vector dimension) carries
/// no per-cell framing overhead. A variable-width ([`TypeTag::Bytes`]) cell is
/// length-prefixed (`u32` little-endian). The consumer recovers the sub-columns
/// with [`unframe_value_cells`], replaying the value sub-columns' type tags; the
/// engine never interprets the cell bytes.
///
/// # Errors
///
/// Returns an error if a variable-width cell is longer than `u32::MAX` (a cell is
/// block-bounded to at most a few MiB, so this is a structural impossibility, not
/// an expected case).
///
/// # Examples
///
/// ```
/// use lsm_tree::table::columnar::{frame_value_cells, unframe_value_cells, TypeTag};
///
/// let tags = [TypeTag::Fixed(4), TypeTag::Bytes, TypeTag::Fixed(2)];
/// let blob = frame_value_cells(&[
///     (TypeTag::Fixed(4), &[1, 2, 3, 4][..]),
///     (TypeTag::Bytes, b"hello"),
///     (TypeTag::Fixed(2), &[9, 9][..]),
/// ])
/// .unwrap();
/// let cells = unframe_value_cells(&blob, &tags).unwrap();
/// assert_eq!(cells, vec![&[1, 2, 3, 4][..], b"hello", &[9, 9][..]]);
/// ```
pub fn frame_value_cells(cells: &[(TypeTag, &[u8])]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    for (tag, cell) in cells {
        match tag {
            // Width recoverable from the tag: append verbatim, no length prefix.
            // The cell length must equal the tag width, or the blob would not
            // un-frame with the same tags (and would shift later cells).
            TypeTag::Fixed(width) => {
                if cell.len() != usize::from(*width) {
                    return Err(Error::InvalidHeader(
                        "columnar: fixed value sub-cell length does not match its type tag",
                    ));
                }
                out.extend_from_slice(cell);
            }
            TypeTag::Bytes => {
                let len = u32::try_from(cell.len()).map_err(|_| {
                    Error::InvalidHeader("columnar: framed value sub-cell exceeds u32")
                })?;
                out.extend_from_slice(&len.to_le_bytes());
                out.extend_from_slice(cell);
            }
        }
    }
    Ok(out)
}

/// Splits a value blob produced by [`frame_value_cells`] back into its sub-column
/// cells.
///
/// Given the value sub-columns' [`TypeTag`]s in column order; the returned slices
/// borrow from `blob`. Inverse of [`frame_value_cells`].
///
/// # Errors
///
/// Returns an error if the blob is truncated relative to `type_tags` (a fixed
/// cell or a length-prefixed cell runs past the end), or if bytes remain after
/// the last cell (the blob and the tag list disagree).
pub fn unframe_value_cells<'a>(blob: &'a [u8], type_tags: &[TypeTag]) -> Result<Vec<&'a [u8]>> {
    let mut out = Vec::with_capacity(type_tags.len());
    let mut pos = 0usize;
    for tag in type_tags {
        match tag {
            TypeTag::Fixed(width) => {
                let end = pos
                    .checked_add(*width as usize)
                    .ok_or(Error::InvalidHeader("columnar: framed value overflow"))?;
                let cell = blob.get(pos..end).ok_or(Error::InvalidHeader(
                    "columnar: framed value truncated (fixed)",
                ))?;
                out.push(cell);
                pos = end;
            }
            TypeTag::Bytes => {
                let len_end = pos
                    .checked_add(4)
                    .ok_or(Error::InvalidHeader("columnar: framed value overflow"))?;
                let len_bytes = blob.get(pos..len_end).ok_or(Error::InvalidHeader(
                    "columnar: framed value truncated (length)",
                ))?;
                let len = u32::from_le_bytes(
                    <[u8; 4]>::try_from(len_bytes)
                        .map_err(|_| Error::InvalidHeader("columnar: framed value length"))?,
                ) as usize;
                let end = len_end
                    .checked_add(len)
                    .ok_or(Error::InvalidHeader("columnar: framed value overflow"))?;
                let cell = blob.get(len_end..end).ok_or(Error::InvalidHeader(
                    "columnar: framed value truncated (bytes)",
                ))?;
                out.push(cell);
                pos = end;
            }
        }
    }
    if pos != blob.len() {
        return Err(Error::InvalidHeader(
            "columnar: framed value has trailing bytes",
        ));
    }
    Ok(out)
}

/// Frames a row's value sub-column cells where any cell may be absent (null),
/// into one self-describing blob.
///
/// Like [`frame_value_cells`] but each cell is `Option`: a `None` sub-cell is
/// absent for this row. The blob starts with a `ceil(N / 8)`-byte presence
/// bitmap (bit `i` set means cell `i` is present), followed by only the present
/// cells (fixed verbatim, variable-width length-prefixed). The consumer reverses
/// it with [`unframe_value_cells_nullable`], replaying the value sub-columns'
/// type tags.
///
/// # Errors
///
/// Returns an error if a fixed-width present cell's length does not match its tag
/// width, or a variable-width cell is longer than `u32::MAX`.
///
/// # Examples
///
/// ```
/// use lsm_tree::table::columnar::{
///     frame_value_cells_nullable, unframe_value_cells_nullable, TypeTag,
/// };
///
/// let tags = [TypeTag::Fixed(4), TypeTag::Bytes];
/// let blob = frame_value_cells_nullable(&[
///     (TypeTag::Fixed(4), Some(&[1, 2, 3, 4][..])),
///     (TypeTag::Bytes, None), // absent for this row
/// ])
/// .unwrap();
/// let cells = unframe_value_cells_nullable(&blob, &tags).unwrap();
/// assert_eq!(cells, vec![Some(&[1, 2, 3, 4][..]), None]);
/// ```
pub fn frame_value_cells_nullable(cells: &[(TypeTag, Option<&[u8]>)]) -> Result<Vec<u8>> {
    let bitmap_len = cells.len().div_ceil(8);
    let mut out = alloc::vec![0u8; bitmap_len];
    for (i, (tag, cell)) in cells.iter().enumerate() {
        let Some(c) = cell else {
            continue; // null: leave the presence bit clear, append no bytes
        };
        if let Some(b) = out.get_mut(i / 8) {
            *b |= 1u8 << (i % 8);
        }
        match tag {
            TypeTag::Fixed(width) => {
                if c.len() != usize::from(*width) {
                    return Err(Error::InvalidHeader(
                        "columnar: fixed value sub-cell length does not match its type tag",
                    ));
                }
                out.extend_from_slice(c);
            }
            TypeTag::Bytes => {
                let len = u32::try_from(c.len()).map_err(|_| {
                    Error::InvalidHeader("columnar: framed value sub-cell exceeds u32")
                })?;
                out.extend_from_slice(&len.to_le_bytes());
                out.extend_from_slice(c);
            }
        }
    }
    Ok(out)
}

/// Splits a value blob produced by [`frame_value_cells_nullable`] back into its
/// sub-column cells, each `Some(bytes)` if present or `None` if absent.
///
/// Inverse of [`frame_value_cells_nullable`], given the value sub-columns'
/// [`TypeTag`]s in column order; the returned slices borrow from `blob`.
///
/// # Errors
///
/// Returns an error if the presence bitmap or any present cell is truncated, or
/// if bytes remain after the last cell.
pub fn unframe_value_cells_nullable<'a>(
    blob: &'a [u8],
    type_tags: &[TypeTag],
) -> Result<Vec<Option<&'a [u8]>>> {
    let bitmap_len = type_tags.len().div_ceil(8);
    let bitmap = blob.get(0..bitmap_len).ok_or(Error::InvalidHeader(
        "columnar: nullable framed value truncated (presence bitmap)",
    ))?;
    let mut pos = bitmap_len;
    let mut out = Vec::with_capacity(type_tags.len());
    for (i, tag) in type_tags.iter().enumerate() {
        let present = bitmap.get(i / 8).is_some_and(|b| (b >> (i % 8)) & 1 == 1);
        if !present {
            out.push(None);
            continue;
        }
        match tag {
            TypeTag::Fixed(width) => {
                let end = pos
                    .checked_add(*width as usize)
                    .ok_or(Error::InvalidHeader("columnar: framed value overflow"))?;
                let cell = blob.get(pos..end).ok_or(Error::InvalidHeader(
                    "columnar: nullable framed value truncated (fixed)",
                ))?;
                out.push(Some(cell));
                pos = end;
            }
            TypeTag::Bytes => {
                let len_end = pos
                    .checked_add(4)
                    .ok_or(Error::InvalidHeader("columnar: framed value overflow"))?;
                let len_bytes = blob.get(pos..len_end).ok_or(Error::InvalidHeader(
                    "columnar: nullable framed value truncated (length)",
                ))?;
                let len = u32::from_le_bytes(
                    <[u8; 4]>::try_from(len_bytes)
                        .map_err(|_| Error::InvalidHeader("columnar: framed value length"))?,
                ) as usize;
                let end = len_end
                    .checked_add(len)
                    .ok_or(Error::InvalidHeader("columnar: framed value overflow"))?;
                let cell = blob.get(len_end..end).ok_or(Error::InvalidHeader(
                    "columnar: nullable framed value truncated (bytes)",
                ))?;
                out.push(Some(cell));
                pos = end;
            }
        }
    }
    if pos != blob.len() {
        return Err(Error::InvalidHeader(
            "columnar: nullable framed value has trailing bytes",
        ));
    }
    Ok(out)
}

/// Splits a nullable value blob, substituting a per-column default for every
/// absent (null) cell.
///
/// `columns` gives each value sub-column's `(TypeTag, default)`; the engine is
/// value-agnostic, so the default bytes are caller-supplied. A present cell reads
/// back as its stored bytes, an absent one as the column's default.
///
/// # Errors
///
/// Returns an error if the blob is malformed (see [`unframe_value_cells_nullable`]).
///
/// # Examples
///
/// ```
/// use lsm_tree::table::columnar::{
///     frame_value_cells_nullable, unframe_value_cells_with_defaults, TypeTag,
/// };
///
/// let blob = frame_value_cells_nullable(&[
///     (TypeTag::Fixed(2), Some(&[7, 7][..])),
///     (TypeTag::Fixed(2), None),
/// ])
/// .unwrap();
/// let cells = unframe_value_cells_with_defaults(
///     &blob,
///     &[(TypeTag::Fixed(2), &[7, 7][..]), (TypeTag::Fixed(2), &[0, 0][..])],
/// )
/// .unwrap();
/// assert_eq!(cells, vec![&[7, 7][..], &[0, 0][..]]); // second is the default
/// ```
pub fn unframe_value_cells_with_defaults<'a>(
    blob: &'a [u8],
    columns: &[(TypeTag, &'a [u8])],
) -> Result<Vec<&'a [u8]>> {
    let tags: Vec<TypeTag> = columns.iter().map(|(t, _)| *t).collect();
    let cells = unframe_value_cells_nullable(blob, &tags)?;
    Ok(cells
        .into_iter()
        .zip(columns)
        .map(|(cell, (_, default))| cell.unwrap_or(default))
        .collect())
}

/// Returns row `row`'s cell from a value sub-column, or `None` if the column is
/// nullable and the row's presence bit is clear.
fn column_value_cell(col: &Column, row_count: u32, row: u32) -> Result<Option<&[u8]>> {
    if let Some(validity) = &col.validity {
        let byte = *validity
            .get((row / 8) as usize)
            .ok_or(Error::InvalidHeader(
                "columnar: validity bitmap shorter than row count",
            ))?;
        if (byte >> (row % 8)) & 1 == 0 {
            return Ok(None); // null row
        }
    }
    Ok(Some(column_cell(col, row_count, row)?))
}

#[expect(clippy::expect_used, clippy::indexing_slicing, reason = "test code")]
#[cfg(test)]
mod tests {
    use super::{
        COL_SEQNO, COL_USER_KEY, COL_VALUE, COL_VALUE_TYPE, CodecId, Column, ColumnBatch, TypeTag,
        column_batch_to_entries, entries_to_column_batch, frame_value_cells,
        frame_value_cells_nullable, unframe_value_cells, unframe_value_cells_nullable,
        unframe_value_cells_with_defaults,
    };
    use crate::{Slice, ValueType, key::InternalKey, value::InternalValue};

    #[test]
    fn fixed_only_value_framing_has_no_overhead_and_round_trips() {
        // Fixed-width cells are stored verbatim (width recoverable from the tag),
        // so the framed blob is the bare concatenation: zero per-cell overhead.
        let tags = [TypeTag::Fixed(4), TypeTag::Fixed(4)];
        let blob = frame_value_cells(&[
            (TypeTag::Fixed(4), &[1, 0, 0, 0][..]),
            (TypeTag::Fixed(4), &[2, 0, 0, 0][..]),
        ])
        .expect("frame");
        assert_eq!(blob, vec![1, 0, 0, 0, 2, 0, 0, 0], "no length prefixes");
        let cells = unframe_value_cells(&blob, &tags).expect("unframe");
        assert_eq!(cells, vec![&[1, 0, 0, 0][..], &[2, 0, 0, 0][..]]);
    }

    #[test]
    fn mixed_value_framing_round_trips() {
        let tags = [TypeTag::Bytes, TypeTag::Fixed(1), TypeTag::Bytes];
        let blob = frame_value_cells(&[
            (TypeTag::Bytes, b"abc"),
            (TypeTag::Fixed(1), &[7][..]),
            (TypeTag::Bytes, b""),
        ])
        .expect("frame");
        let cells = unframe_value_cells(&blob, &tags).expect("unframe");
        assert_eq!(cells, vec![&b"abc"[..], &[7][..], &b""[..]]);
    }

    #[test]
    fn empty_value_framing_round_trips() {
        let blob = frame_value_cells(&[]).expect("frame");
        assert!(blob.is_empty());
        assert!(unframe_value_cells(&blob, &[]).expect("unframe").is_empty());
    }

    #[test]
    fn unframe_rejects_truncated_blob() {
        // A fixed(4) tag over a 2-byte blob is truncated.
        assert!(unframe_value_cells(&[1, 2], &[TypeTag::Fixed(4)]).is_err());
        // A bytes tag whose declared length runs past the end.
        let mut blob = 8u32.to_le_bytes().to_vec();
        blob.extend_from_slice(b"abc"); // 3 bytes present, length claims 8
        assert!(unframe_value_cells(&blob, &[TypeTag::Bytes]).is_err());
    }

    #[test]
    fn unframe_rejects_trailing_bytes() {
        // One fixed(2) cell, but the blob carries an extra byte the tags do not
        // cover: the blob and the tag list disagree.
        assert!(unframe_value_cells(&[1, 2, 3], &[TypeTag::Fixed(2)]).is_err());
    }

    #[test]
    fn frame_rejects_fixed_cell_length_mismatch() {
        // A fixed(4) tag with a 3-byte cell would misframe (and shift later cells),
        // so framing rejects it.
        assert!(frame_value_cells(&[(TypeTag::Fixed(4), &[1, 2, 3][..])]).is_err());
    }

    #[test]
    fn nullable_framing_round_trips_mixed_present_and_null() {
        let tags = [TypeTag::Fixed(4), TypeTag::Bytes, TypeTag::Fixed(2)];
        let blob = frame_value_cells_nullable(&[
            (TypeTag::Fixed(4), Some(&[1, 0, 0, 0][..])),
            (TypeTag::Bytes, None),
            (TypeTag::Fixed(2), Some(&[9, 9][..])),
        ])
        .expect("frame");
        // Presence bitmap (1 byte): bits 0 and 2 set, bit 1 clear -> 0b101 = 5.
        assert_eq!(blob[0], 0b0000_0101, "presence bitmap marks the null cell");
        assert_eq!(
            unframe_value_cells_nullable(&blob, &tags).expect("unframe"),
            vec![Some(&[1, 0, 0, 0][..]), None, Some(&[9, 9][..])],
        );
    }

    #[test]
    fn nullable_framing_all_null_and_all_present() {
        let tags = [TypeTag::Bytes, TypeTag::Fixed(1)];
        let all_null =
            frame_value_cells_nullable(&[(TypeTag::Bytes, None), (TypeTag::Fixed(1), None)])
                .expect("frame");
        assert_eq!(all_null, vec![0u8], "bitmap only, no bodies");
        assert_eq!(
            unframe_value_cells_nullable(&all_null, &tags).expect("unframe"),
            vec![None, None],
        );

        let all_present = frame_value_cells_nullable(&[
            (TypeTag::Bytes, Some(&b"ab"[..])),
            (TypeTag::Fixed(1), Some(&[7][..])),
        ])
        .expect("frame");
        assert_eq!(
            unframe_value_cells_nullable(&all_present, &tags).expect("unframe"),
            vec![Some(&b"ab"[..]), Some(&[7][..])],
        );
    }

    #[test]
    fn nullable_framing_rejects_truncated_and_trailing() {
        // A present fixed(4) cell whose bytes are missing after the bitmap.
        let truncated = alloc::vec![0b0000_0001u8, 1, 2]; // bit 0 set, only 2 of 4 bytes
        assert!(unframe_value_cells_nullable(&truncated, &[TypeTag::Fixed(4)]).is_err());
        // A valid all-null encoding with an extra trailing byte.
        let mut trailing = frame_value_cells_nullable(&[(TypeTag::Bytes, None)]).expect("frame");
        trailing.push(0);
        assert!(unframe_value_cells_nullable(&trailing, &[TypeTag::Bytes]).is_err());
    }

    #[test]
    fn unframe_with_defaults_substitutes_for_null_cells() {
        let blob = frame_value_cells_nullable(&[
            (TypeTag::Fixed(2), Some(&[7, 7][..])),
            (TypeTag::Fixed(2), None),
        ])
        .expect("frame");
        let cells = unframe_value_cells_with_defaults(
            &blob,
            &[
                (TypeTag::Fixed(2), &[7, 7][..]),
                (TypeTag::Fixed(2), &[0, 0][..]),
            ],
        )
        .expect("unframe with defaults");
        assert_eq!(cells, vec![&[7, 7][..], &[0, 0][..]]);
    }

    #[test]
    fn first_user_key_rejects_a_non_key_first_column() {
        // A first column that is not the non-null bytes user-key column is
        // rejected before any low-level offset read.
        let fixed_first = ColumnBatch {
            row_count: 1,
            columns: alloc::vec![Column {
                column_id: COL_USER_KEY,
                type_tag: TypeTag::Fixed(4),
                validity: None,
                data: alloc::vec![0, 0, 0, 0],
            }],
        };
        assert!(fixed_first.first_user_key().is_err());

        let missing = ColumnBatch {
            row_count: 1,
            columns: alloc::vec![Column {
                column_id: 99,
                type_tag: TypeTag::Bytes,
                validity: None,
                data: alloc::vec![0, 0, 0, 0, 0, 0, 0, 0],
            }],
        };
        assert!(missing.first_user_key().is_err());
    }

    #[test]
    fn first_user_key_returns_the_first_row_key() {
        // A two-row key column: the first key is read without decoding the batch.
        let mut data = Vec::new();
        for off in [0u32, 2, 5] {
            data.extend_from_slice(&off.to_le_bytes());
        }
        data.extend_from_slice(b"k0k11");
        let batch = ColumnBatch {
            row_count: 2,
            columns: alloc::vec![Column {
                column_id: COL_USER_KEY,
                type_tag: TypeTag::Bytes,
                validity: None,
                data,
            }],
        };
        assert_eq!(batch.first_user_key().expect("first key"), Some(&b"k0"[..]));
    }

    fn entry(user_key: &[u8], seqno: u64, value_type: ValueType, value: &[u8]) -> InternalValue {
        InternalValue {
            key: InternalKey {
                user_key: Slice::from(user_key),
                seqno,
                value_type,
            },
            value: Slice::from(value),
        }
    }

    fn assert_entries_eq(a: &[InternalValue], b: &[InternalValue]) {
        assert_eq!(a.len(), b.len(), "entry count mismatch");
        for (x, y) in a.iter().zip(b) {
            assert_eq!(x.key.user_key.as_ref(), y.key.user_key.as_ref());
            assert_eq!(x.key.seqno, y.key.seqno);
            assert!(x.key.value_type == y.key.value_type, "value_type mismatch");
            assert_eq!(x.value.as_ref(), y.value.as_ref());
        }
    }

    #[test]
    fn delta_codec_round_trips_fixed8_column() {
        // A fixed-8 (u64) column auto-selects Delta and must round-trip exactly,
        // including a repeat and a decrease (wrapping delta).
        let seqnos: [u64; 5] = [100, 105, 105, 200, 199];
        let data: Vec<u8> = seqnos.iter().flat_map(|s| s.to_le_bytes()).collect();
        let batch = ColumnBatch {
            row_count: 5,
            columns: vec![Column {
                column_id: 1,
                type_tag: TypeTag::Fixed(8),
                validity: None,
                data: data.clone(),
            }],
        };
        let encoded = batch.encode(CodecId::Plain).expect("encode");
        // The codec byte (row_count 4 + col_count 4 + id 2 + type 1 + width 1 =
        // offset 12) must record Delta, auto-selected for the fixed-8 column.
        assert_eq!(encoded[12], u8::from(CodecId::Delta));
        let decoded = ColumnBatch::decode(&encoded).expect("decode");
        assert_eq!(
            decoded.columns[0].data, data,
            "delta column must round-trip"
        );
    }

    #[test]
    fn auto_codec_is_delta_only_for_the_seqno_column() {
        // A fixed-8 column that is not the seqno column keeps the default codec
        // (Plain): delta-encoding a non-monotonic column would only inflate it.
        let batch = ColumnBatch {
            row_count: 2,
            columns: vec![Column {
                column_id: 99, // not the intrinsic seqno column
                type_tag: TypeTag::Fixed(8),
                validity: None,
                data: vec![0u8; 16],
            }],
        };
        let encoded = batch.encode(CodecId::Plain).expect("encode");
        assert_eq!(
            encoded[12],
            u8::from(CodecId::Plain),
            "a non-seqno fixed-8 column must not auto-select Delta"
        );
    }

    #[test]
    fn encode_rejects_delta_on_a_non_fixed8_column() {
        // Forcing Delta on a Bytes column (via the fallback codec) is rejected
        // rather than silently truncating its bytes.
        let mut bytes_data = Vec::new();
        bytes_data.extend_from_slice(&0u32.to_le_bytes());
        bytes_data.extend_from_slice(&3u32.to_le_bytes());
        bytes_data.extend_from_slice(b"abc");
        let batch = ColumnBatch {
            row_count: 1,
            columns: vec![Column {
                column_id: 50,
                type_tag: TypeTag::Bytes,
                validity: None,
                data: bytes_data,
            }],
        };
        assert!(batch.encode(CodecId::Delta).is_err());
    }

    #[test]
    fn from_columnar_block_rejects_a_zero_row_block() {
        // A zero-row columnar block is corrupt (the writer never spills empty);
        // reconstructing it must error, not panic in the row encoder.
        let empty = entries_to_column_batch(&[])
            .expect("transpose")
            .encode(CodecId::Plain)
            .expect("encode");
        assert!(crate::table::data_block::DataBlock::from_columnar_block(&empty, 16).is_err());
    }

    #[test]
    fn decode_projected_decodes_only_the_wanted_columns() {
        // Project just the user-key column: the result carries ONLY that column,
        // proving the value / seqno / value-type columns were never decoded.
        let entries = vec![
            entry(b"alpha", 10, ValueType::Value, b"v1"),
            entry(b"bravo", 9, ValueType::Value, b"v2"),
        ];
        let bytes = entries_to_column_batch(&entries)
            .expect("transpose")
            .encode(CodecId::Plain)
            .expect("encode");

        let projected =
            ColumnBatch::decode_projected(&bytes, &[COL_USER_KEY]).expect("decode_projected");
        assert_eq!(projected.row_count, 2);
        assert_eq!(
            projected.columns.len(),
            1,
            "only the projected column is decoded"
        );
        assert_eq!(projected.columns[0].column_id, COL_USER_KEY);
        assert!(
            !projected.columns.iter().any(|c| c.column_id == COL_VALUE),
            "a key-only projection must not decode the value column"
        );

        // Projecting every column equals a full decode.
        let all = [COL_USER_KEY, COL_SEQNO, COL_VALUE_TYPE, COL_VALUE];
        assert_eq!(
            ColumnBatch::decode_projected(&bytes, &all).expect("decode_projected all"),
            ColumnBatch::decode(&bytes).expect("decode"),
        );
    }

    #[test]
    fn intrinsic_transpose_round_trips_entries() {
        // A mix of value kinds, including a tombstone (empty value) and a merge
        // operand, exercises the value column's variable width and the
        // value-type column.
        let entries = vec![
            entry(b"alpha", 10, ValueType::Value, b"v1"),
            entry(b"bravo", 9, ValueType::Tombstone, b""),
            entry(b"charlie", 8, ValueType::MergeOperand, b"+1"),
        ];
        let batch = entries_to_column_batch(&entries).expect("transpose");
        assert_eq!(batch.row_count, 3);
        assert_eq!(batch.columns.len(), 4, "four intrinsic columns");

        // Direct reconstruction.
        let back = column_batch_to_entries(&batch).expect("untranspose");
        assert_entries_eq(&entries, &back);

        // And through the block encode / decode, so the transpose composes with
        // the on-disk columnar format.
        let bytes = batch.encode(CodecId::Plain).expect("encode");
        let decoded = ColumnBatch::decode(&bytes).expect("decode");
        let back2 = column_batch_to_entries(&decoded).expect("untranspose decoded");
        assert_entries_eq(&entries, &back2);
    }

    #[test]
    fn intrinsic_untranspose_rejects_wrong_layout() {
        // A batch whose columns are not the four intrinsic columns is refused.
        let bad = ColumnBatch {
            row_count: 1,
            columns: vec![Column {
                column_id: 0,
                type_tag: TypeTag::Fixed(4),
                validity: None,
                data: vec![0; 4],
            }],
        };
        assert!(column_batch_to_entries(&bad).is_err());
    }

    #[test]
    fn intrinsic_untranspose_rejects_nullable_intrinsic() {
        // Intrinsic fields are never null; a validity bitmap on one marks a
        // malformed batch even if it is otherwise well-framed.
        let mut batch =
            entries_to_column_batch(&[entry(b"k", 1, ValueType::Value, b"v")]).expect("transpose");
        batch.columns[0].validity = Some(vec![0b1]); // one valid row, but nullable
        assert!(column_batch_to_entries(&batch).is_err());
    }

    #[test]
    fn intrinsic_untranspose_rejects_empty_key() {
        // An empty user key violates the engine's key invariant.
        let batch =
            entries_to_column_batch(&[entry(b"", 1, ValueType::Value, b"v")]).expect("transpose");
        assert!(column_batch_to_entries(&batch).is_err());
    }

    #[test]
    fn intrinsic_untranspose_rejects_huge_row_count() {
        // A hand-built batch claiming billions of rows but carrying tiny columns
        // is rejected by per-column validation before any allocation.
        let bad = ColumnBatch {
            row_count: u32::MAX,
            columns: vec![
                Column {
                    column_id: 0,
                    type_tag: TypeTag::Bytes,
                    validity: None,
                    data: 0u32.to_le_bytes().to_vec(),
                },
                Column {
                    column_id: 1,
                    type_tag: TypeTag::Fixed(8),
                    validity: None,
                    data: Vec::new(),
                },
                Column {
                    column_id: 2,
                    type_tag: TypeTag::Fixed(1),
                    validity: None,
                    data: Vec::new(),
                },
                Column {
                    column_id: 3,
                    type_tag: TypeTag::Bytes,
                    validity: None,
                    data: 0u32.to_le_bytes().to_vec(),
                },
            ],
        };
        assert!(column_batch_to_entries(&bad).is_err());
    }

    #[test]
    fn untranspose_frames_multiple_value_subcolumns() {
        // A consumer batch: the three intrinsic columns plus two value
        // sub-columns (a fixed-4 and a variable-width bytes). Each row's
        // reconstructed value is the framed concat of its two sub-cells, which
        // unframe_value_cells reverses.
        let mut batch = entries_to_column_batch(&[
            entry(b"k0", 1, ValueType::Value, b"ignored"),
            entry(b"k1", 2, ValueType::Value, b"ignored"),
        ])
        .expect("transpose");
        // Replace the single opaque value column with two value sub-columns.
        batch.columns.pop();
        batch.columns.push(Column {
            column_id: 3,
            type_tag: TypeTag::Fixed(4),
            validity: None,
            data: vec![1, 0, 0, 0, 2, 0, 0, 0], // row 0 = 1, row 1 = 2
        });
        let mut bytes_data = Vec::new();
        for off in [0u32, 2, 5] {
            bytes_data.extend_from_slice(&off.to_le_bytes());
        }
        bytes_data.extend_from_slice(b"aabbb"); // row 0 = "aa", row 1 = "bbb"
        batch.columns.push(Column {
            column_id: 4,
            type_tag: TypeTag::Bytes,
            validity: None,
            data: bytes_data,
        });

        let entries = column_batch_to_entries(&batch).expect("untranspose");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key.user_key.as_ref(), b"k0");
        assert_eq!(entries[1].key.seqno, 2);

        let tags = [TypeTag::Fixed(4), TypeTag::Bytes];
        assert_eq!(
            unframe_value_cells(entries[0].value.as_ref(), &tags).expect("unframe 0"),
            vec![&[1, 0, 0, 0][..], &b"aa"[..]],
        );
        assert_eq!(
            unframe_value_cells(entries[1].value.as_ref(), &tags).expect("unframe 1"),
            vec![&[2, 0, 0, 0][..], &b"bbb"[..]],
        );
    }

    #[test]
    fn untranspose_rejects_value_subcolumn_id_collisions() {
        // Projection selects value sub-columns by id, so a value sub-column that
        // reuses an intrinsic id (0/1/2) or duplicates another value id would make
        // projected results ambiguous. The untranspose must reject both.
        let make = |value_cols: Vec<Column>| -> ColumnBatch {
            let mut batch =
                entries_to_column_batch(&[entry(b"k0", 1, ValueType::Value, b"ignored")])
                    .expect("transpose");
            batch.columns.pop(); // drop the single opaque value column
            batch.columns.extend(value_cols);
            batch
        };

        // A value sub-column reusing COL_USER_KEY (0).
        let collide_intrinsic = make(vec![Column {
            column_id: COL_USER_KEY,
            type_tag: TypeTag::Fixed(4),
            validity: None,
            data: vec![0, 0, 0, 0],
        }]);
        assert!(
            column_batch_to_entries(&collide_intrinsic).is_err(),
            "a value sub-column reusing an intrinsic id must be rejected",
        );

        // Two value sub-columns sharing id 5.
        let duplicate = make(vec![
            Column {
                column_id: 5,
                type_tag: TypeTag::Fixed(4),
                validity: None,
                data: vec![0, 0, 0, 0],
            },
            Column {
                column_id: 5,
                type_tag: TypeTag::Fixed(4),
                validity: None,
                data: vec![1, 0, 0, 0],
            },
        ]);
        assert!(
            column_batch_to_entries(&duplicate).is_err(),
            "duplicate value sub-column ids must be rejected",
        );
    }

    #[test]
    fn untranspose_reconstructs_nullable_value_subcolumns() {
        // Two rows, two value sub-columns; the second (fixed-4) is nullable with
        // row 1 absent. Each row's reconstructed value is a presence-bitmap frame
        // that unframe_value_cells_nullable reverses to the original cells.
        let mut batch = entries_to_column_batch(&[
            InternalValue::from_components(b"k0", b"ignored", 0, ValueType::Value),
            InternalValue::from_components(b"k1", b"ignored", 0, ValueType::Value),
        ])
        .expect("transpose");
        batch.columns.pop();
        // Bytes sub-column (always present): "aa", "bbb".
        let mut bytes_data = Vec::new();
        for off in [0u32, 2, 5] {
            bytes_data.extend_from_slice(&off.to_le_bytes());
        }
        bytes_data.extend_from_slice(b"aabbb");
        batch.columns.push(Column {
            column_id: 3,
            type_tag: TypeTag::Bytes,
            validity: None,
            data: bytes_data,
        });
        // Fixed-4 sub-column, row 0 present (=1), row 1 null. validity bit 0 set.
        batch.columns.push(Column {
            column_id: 4,
            type_tag: TypeTag::Fixed(4),
            validity: Some(alloc::vec![0b0000_0001]),
            data: alloc::vec![1, 0, 0, 0, 0, 0, 0, 0],
        });

        let entries = column_batch_to_entries(&batch).expect("untranspose");
        let tags = [TypeTag::Bytes, TypeTag::Fixed(4)];
        assert_eq!(
            unframe_value_cells_nullable(entries[0].value.as_ref(), &tags).expect("row 0"),
            vec![Some(&b"aa"[..]), Some(&[1, 0, 0, 0][..])],
        );
        assert_eq!(
            unframe_value_cells_nullable(entries[1].value.as_ref(), &tags).expect("row 1"),
            vec![Some(&b"bbb"[..]), None],
            "row 1's fixed sub-cell is absent",
        );
    }

    fn sample_batch() -> ColumnBatch {
        // 3 rows: a fixed u32 column (row 1 null) and a variable-width Bytes
        // column (all valid). Bytes data = offset array [0,2,2,5] + payload.
        let bytes_offsets: Vec<u8> = [0u32, 2, 2, 5]
            .iter()
            .flat_map(|o| o.to_le_bytes())
            .collect();
        let mut bytes_data = bytes_offsets;
        bytes_data.extend_from_slice(b"hi"); // row 0
        // row 1 empty (offset 2..2)
        bytes_data.extend_from_slice(b"abc"); // row 2
        ColumnBatch {
            row_count: 3,
            columns: vec![
                Column {
                    column_id: 1,
                    type_tag: TypeTag::Fixed(4),
                    validity: Some(vec![0b0000_0101]), // rows 0 and 2 valid, row 1 null
                    data: vec![10, 0, 0, 0, 0, 0, 0, 0, 30, 0, 0, 0],
                },
                Column {
                    column_id: 2,
                    type_tag: TypeTag::Bytes,
                    validity: None,
                    data: bytes_data,
                },
            ],
        }
    }

    #[test]
    fn columnar_batch_round_trips_through_plain_codec() {
        let batch = sample_batch();
        let encoded = batch.encode(CodecId::Plain).expect("encode");
        let decoded = ColumnBatch::decode(&encoded).expect("decode");
        assert_eq!(decoded, batch, "columnar batch must survive a round-trip");
    }

    #[test]
    fn columnar_decode_rejects_truncated_payload() {
        let encoded = sample_batch().encode(CodecId::Plain).expect("encode");
        // Drop the last byte: the final column's data is now short.
        let truncated = &encoded[..encoded.len() - 1];
        assert!(ColumnBatch::decode(truncated).is_err());
    }

    #[test]
    fn columnar_encode_rejects_fixed_width_length_mismatch() {
        // A fixed(4) column claiming 3 rows but carrying 8 bytes (= 2 rows) is
        // rejected at encode, so no non-round-trippable payload is produced.
        let bad = ColumnBatch {
            row_count: 3,
            columns: vec![Column {
                column_id: 1,
                type_tag: TypeTag::Fixed(4),
                validity: None,
                data: vec![0; 8],
            }],
        };
        assert!(bad.encode(CodecId::Plain).is_err());
    }

    #[test]
    fn columnar_decode_rejects_unknown_codec_tag() {
        let mut encoded = sample_batch().encode(CodecId::Plain).expect("encode");
        // Codec byte of the first column sits after row_count(4) + col_count(4)
        // + column_id(2) + type_tag(1) + width(1) = offset 12.
        encoded[12] = 0xFF;
        assert!(ColumnBatch::decode(&encoded).is_err());
    }

    #[test]
    fn columnar_encode_rejects_zero_width_fixed_column() {
        // Fixed(0) is publicly constructible but cannot round-trip; encode must
        // reject it rather than emit bytes decode would refuse.
        let bad = ColumnBatch {
            row_count: 1,
            columns: vec![Column {
                column_id: 1,
                type_tag: TypeTag::Fixed(0),
                validity: None,
                data: Vec::new(),
            }],
        };
        assert!(bad.encode(CodecId::Plain).is_err());
    }

    #[test]
    fn columnar_encode_rejects_wrong_validity_length() {
        // 1 row needs a 1-byte bitmap; a 2-byte bitmap is rejected.
        let bad = ColumnBatch {
            row_count: 1,
            columns: vec![Column {
                column_id: 1,
                type_tag: TypeTag::Fixed(1),
                validity: Some(vec![0, 0]),
                data: vec![7],
            }],
        };
        assert!(bad.encode(CodecId::Plain).is_err());
    }

    #[test]
    fn columnar_encode_rejects_validity_padding_bits() {
        // 1 row: only bit 0 is meaningful; a set padding bit (0xFF) is rejected.
        let bad = ColumnBatch {
            row_count: 1,
            columns: vec![Column {
                column_id: 1,
                type_tag: TypeTag::Fixed(1),
                validity: Some(vec![0xFF]),
                data: vec![7],
            }],
        };
        assert!(bad.encode(CodecId::Plain).is_err());
    }

    #[test]
    fn columnar_decode_rejects_bytes_offset_out_of_bounds() {
        let mut encoded = sample_batch().encode(CodecId::Plain).expect("encode");
        // The Bytes column's first offset (must be 0) is at byte 41: col1 starts
        // at 31 (id 2 + type 1 + width 1 + codec 1 + has_validity 1 + len 4 = 10
        // header bytes), so its data / offset table begins at 41.
        encoded[41] = 9;
        assert!(ColumnBatch::decode(&encoded).is_err());
    }

    #[test]
    fn columnar_decode_rejects_trailing_bytes() {
        let mut encoded = sample_batch().encode(CodecId::Plain).expect("encode");
        encoded.push(0); // one byte past the last declared column
        assert!(ColumnBatch::decode(&encoded).is_err());
    }

    #[test]
    fn columnar_decode_rejects_huge_column_count() {
        // row_count = 0, column_count = u32::MAX, but no column bytes follow.
        let mut payload = 0u32.to_le_bytes().to_vec();
        payload.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(ColumnBatch::decode(&payload).is_err());
    }

    #[test]
    fn columnar_decode_rejects_non_boolean_validity_flag() {
        let mut encoded = sample_batch().encode(CodecId::Plain).expect("encode");
        // has_validity flag of the first column is at byte 13.
        encoded[13] = 2;
        assert!(ColumnBatch::decode(&encoded).is_err());
    }

    #[test]
    fn columnar_decode_rejects_non_zero_bytes_width() {
        let mut encoded = sample_batch().encode(CodecId::Plain).expect("encode");
        // The Bytes column's width byte (must be 0) is at byte 34.
        encoded[34] = 5;
        assert!(ColumnBatch::decode(&encoded).is_err());
    }
}

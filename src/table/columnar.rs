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
//!
//! # Schema evolution
//!
//! The format is schema-free: each column self-describes its `column_id`,
//! [`TypeTag`], and [`CodecId`], so segments written at different times may
//! carry different column sets and still read back through one projection. Three
//! consumer-facing conventions make that safe as a value sub-column schema
//! evolves:
//!
//! - **Column-id stability.** A `column_id` is a stable field identifier: it
//!   denotes the same logical field, with the same interpretation, in every
//!   segment. A consumer must not repurpose an id for a different field across
//!   schema versions, otherwise a projection for it would mean different things
//!   in old and new segments. Retire an id rather than reusing it.
//! - **Schema version.** The engine attaches no version to a batch. A consumer
//!   that needs to tell schema versions apart tags them itself, e.g. with a
//!   reserved `column_id` carrying a version number, which keeps the engine
//!   schema-free.
//! - **Projection over a missing column.** [`ColumnBatch::decode_projected`]
//!   (and the table-level columnar scan built on it) returns only the projected
//!   columns actually present in a block. Projecting a `column_id` absent from a
//!   segment is not an error: that segment's batches simply omit the column and
//!   the consumer applies its own default or treats it as null, while a newer
//!   segment that carries the column returns it. Mixed old/new segments thus
//!   coexist with no migration step.

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

    /// Returns the last row's user key, or `None` for an empty batch. Like
    /// [`Self::first_user_key`] but for the final row, so the ingest path can
    /// carry the ordering boundary forward after accumulating a batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the first column is not the non-null user-key column
    /// or its row framing is malformed.
    pub(crate) fn last_user_key(&self) -> Result<Option<&[u8]>> {
        let Some(last) = self.row_count.checked_sub(1) else {
            return Ok(None);
        };
        let key_col = self
            .columns
            .first()
            .filter(|c| c.column_id == COL_USER_KEY)
            .ok_or(Error::InvalidHeader(
                "columnar: first column is not the user-key column",
            ))?;
        if key_col.type_tag != TypeTag::Bytes || key_col.validity.is_some() {
            return Err(Error::InvalidHeader(
                "columnar: first column is not the non-null user-key column",
            ));
        }
        key_col.validate(self.row_count)?;
        bytes_column_row(&key_col.data, self.row_count, last).map(Some)
    }

    /// Whether `other` has the same column layout (each column's id and type, in
    /// order) as this batch, so the two can be appended into one rowgroup.
    #[must_use]
    pub(crate) fn same_layout(&self, other: &Self) -> bool {
        self.columns.len() == other.columns.len()
            && self
                .columns
                .iter()
                .zip(&other.columns)
                .all(|(a, b)| a.column_id == b.column_id && a.type_tag == b.type_tag)
    }

    /// Total size of the column bytes (data plus any validity bitmap), used to
    /// decide when an accumulated rowgroup has reached the target block size. The
    /// validity bitmap is `ceil(row_count / 8)` bytes per nullable column, so
    /// omitting it would let a nullable-heavy rowgroup overrun the target before
    /// the flush threshold trips.
    #[must_use]
    pub(crate) fn data_size(&self) -> usize {
        self.columns
            .iter()
            .map(|c| c.data.len() + c.validity.as_ref().map_or(0, Vec::len))
            .sum()
    }

    /// Appends `other`'s rows after this batch's, in place, so a sequence of
    /// small ingest batches can accumulate into one rowgroup before a block is
    /// written. The two batches must share the same columns (id + type) in the
    /// same order; a `Fixed` column concatenates verbatim, a `Bytes` column
    /// re-frames the merged cells, and a validity bitmap is combined across both
    /// (a `None` bitmap on either side counts that side's rows as present).
    ///
    /// # Errors
    ///
    /// Returns an error if the column counts or layouts differ, the combined row
    /// count exceeds `u32::MAX`, or either batch's columns are malformed for
    /// their own row count.
    pub(crate) fn append(&mut self, other: &Self) -> Result<()> {
        if self.columns.len() != other.columns.len() {
            return Err(Error::InvalidHeader(
                "columnar: append column-count mismatch",
            ));
        }
        let old_rows = self.row_count;
        let combined_rows = old_rows
            .checked_add(other.row_count)
            .ok_or(Error::InvalidHeader(
                "columnar: appended row count exceeds u32",
            ))?;
        // Validate the layout and framing of both batches before mutating, so an
        // invalid append leaves this batch unchanged.
        for (a, b) in self.columns.iter().zip(&other.columns) {
            if a.column_id != b.column_id || a.type_tag != b.type_tag {
                return Err(Error::InvalidHeader(
                    "columnar: append column layout mismatch",
                ));
            }
            a.validate(old_rows)?;
            b.validate(other.row_count)?;
        }
        // Encode every merged column into temporaries first; only after all
        // succeed do we mutate `self`. A fallible encode mid-loop would otherwise
        // leave the batch half-appended (some columns longer) while `row_count`
        // stays unchanged, corrupting the pending rowgroup.
        let mut merged: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::with_capacity(self.columns.len());
        for (a, b) in self.columns.iter().zip(&other.columns) {
            let new_validity = combine_validity(
                a.validity.as_deref(),
                old_rows,
                b.validity.as_deref(),
                other.row_count,
            )?;
            let new_data = match a.type_tag {
                TypeTag::Fixed(_) => {
                    let mut data = Vec::with_capacity(a.data.len() + b.data.len());
                    data.extend_from_slice(&a.data);
                    data.extend_from_slice(&b.data);
                    data
                }
                TypeTag::Bytes => {
                    let mut cells: Vec<&[u8]> = Vec::with_capacity(combined_rows as usize);
                    for i in 0..old_rows {
                        cells.push(bytes_column_row(&a.data, old_rows, i)?);
                    }
                    for j in 0..other.row_count {
                        cells.push(bytes_column_row(&b.data, other.row_count, j)?);
                    }
                    encode_bytes_column(&cells)?
                }
            };
            merged.push((new_data, new_validity));
        }
        for (col, (data, validity)) in self.columns.iter_mut().zip(merged) {
            col.data = data;
            col.validity = validity;
        }
        self.row_count = combined_rows;
        Ok(())
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

/// Encodes variable-width cells as a [`TypeTag::Bytes`] column body: a
/// `(len + 1)`-entry little-endian `u32` offset array followed by the
/// concatenated payload.
fn encode_bytes_column(cells: &[&[u8]]) -> Result<Vec<u8>> {
    let mut offsets = Vec::with_capacity((cells.len() + 1) * 4);
    let mut acc = 0u32;
    offsets.extend_from_slice(&acc.to_le_bytes());
    for c in cells {
        let len = u32::try_from(c.len())
            .map_err(|_| Error::InvalidHeader("columnar: bytes cell exceeds u32"))?;
        acc = acc
            .checked_add(len)
            .ok_or(Error::InvalidHeader("columnar: bytes payload exceeds u32"))?;
        offsets.extend_from_slice(&acc.to_le_bytes());
    }
    let mut out = offsets;
    out.reserve(acc as usize);
    for c in cells {
        out.extend_from_slice(c);
    }
    Ok(out)
}

/// Whether row `row`'s presence bit is set in a validity bitmap (set = valid).
fn validity_bit(bitmap: &[u8], row: u32) -> bool {
    bitmap
        .get((row / 8) as usize)
        .is_some_and(|b| (b >> (row % 8)) & 1 == 1)
}

/// Combines two columns' validity bitmaps for an append: the result spans
/// `a_rows + b_rows` rows, treating a `None` bitmap as all-present. Returns
/// `None` only when both inputs are `None` (the appended column stays non-null).
fn combine_validity(
    a: Option<&[u8]>,
    a_rows: u32,
    b: Option<&[u8]>,
    b_rows: u32,
) -> Result<Option<Vec<u8>>> {
    if a.is_none() && b.is_none() {
        return Ok(None);
    }
    let total = a_rows.checked_add(b_rows).ok_or(Error::InvalidHeader(
        "columnar: combined row count exceeds u32",
    ))?;
    let mut out = alloc::vec![0u8; validity_len(total)];
    for idx in 0..total {
        let present = if idx < a_rows {
            a.is_none_or(|v| validity_bit(v, idx))
        } else {
            // idx >= a_rows here, so the subtraction does not underflow and
            // idx - a_rows < b_rows.
            b.is_none_or(|v| validity_bit(v, idx - a_rows))
        };
        if present && let Some(byte) = out.get_mut((idx / 8) as usize) {
            *byte |= 1u8 << (idx % 8);
        }
    }
    Ok(Some(out))
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

/// Validates the intrinsic + value column layout and per-column framing of a
/// columnar batch, returning the destructured columns. Shared by
/// [`column_batch_to_entries`] (which then decodes every row) and
/// [`validate_columnar_ingest_batch`] (which checks the ingest contract without
/// decoding), so the structural checks cannot diverge between the two paths.
fn validate_columnar_columns(
    batch: &ColumnBatch,
) -> Result<(&Column, &Column, &Column, &[Column])> {
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
    // `row_count` against the fixed-width column lengths, so a malformed batch
    // claiming a huge row count is rejected instead of reserving for billions of
    // rows.
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
    Ok((key_col, seqno_col, vt_col, value_cols))
}

/// Validates a columnar batch against the ingest contract without decoding every
/// row into an [`InternalValue`].
///
/// The column layout / framing must be valid, every row's seqno `0` (the
/// ingestion assigns the sequence number), and keys strictly increasing within
/// the batch. The full decode runs once at flush on the accumulated rowgroup, so
/// this lets the ingestion reject a bad batch eagerly without deserialising every
/// submitted row twice. Cross-batch ordering is the caller's responsibility (it
/// tracks the last key written).
///
/// # Errors
///
/// Returns an error if the layout / framing is invalid, a row carries a non-zero
/// seqno, or the keys are empty / oversized / not strictly increasing within the
/// batch.
pub fn validate_columnar_ingest_batch(
    batch: &ColumnBatch,
    comparator: &crate::SharedComparator,
) -> Result<()> {
    let (key_col, seqno_col, vt_col, _value_cols) = validate_columnar_columns(batch)?;
    // Reject a malformed value-type tag on submit rather than letting it surface
    // only at flush-time decode (`column_batch_to_entries`).
    for &vt_byte in &vt_col.data {
        ValueType::try_from(vt_byte).map_err(|()| Error::InvalidTag(("ValueType", vt_byte)))?;
    }
    for i in 0..batch.row_count {
        if fixed_u64_row(&seqno_col.data, i)? != 0 {
            return Err(Error::FeatureUnsupported(
                "columnar batch ingest requires every row seqno to be 0 (the ingestion assigns the sequence number)",
            ));
        }
    }
    let mut prev: Option<&[u8]> = None;
    for i in 0..batch.row_count {
        let key = bytes_column_row(&key_col.data, batch.row_count, i)?;
        if key.is_empty() || key.len() > u16::MAX as usize {
            return Err(Error::InvalidHeader(
                "columnar: user key is empty or longer than u16::MAX",
            ));
        }
        if let Some(p) = prev
            && comparator.compare(p, key) != core::cmp::Ordering::Less
        {
            return Err(Error::InvalidHeader(
                "columnar batch ingest requires strictly increasing keys",
            ));
        }
        prev = Some(key);
    }
    Ok(())
}

pub fn column_batch_to_entries(batch: &ColumnBatch) -> Result<Vec<InternalValue>> {
    let (key_col, seqno_col, vt_col, value_cols) = validate_columnar_columns(batch)?;
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

/// Consuming, allocation-light counterpart to [`column_batch_to_entries`] for
/// the scan path.
///
/// The key column and a single non-nullable bytes value column are taken as
/// shared [`Slice`]s, so each row's key / value is a view into one buffer
/// (zero-copy for the Arc-backed large-value case) instead of a per-row copy.
/// Any other value layout (fixed-width, multiple sub-columns, or nullable) falls
/// back to the per-row framing reconstruction.
pub fn column_batch_into_entries(batch: ColumnBatch) -> Result<Vec<InternalValue>> {
    // Structural validation (intrinsic columns + framing) before we consume.
    validate_columnar_columns(&batch)?;
    let row_count = batch.row_count;
    let mut cols = batch.columns.into_iter();
    let key_col = cols
        .next()
        .ok_or(Error::InvalidHeader("columnar: missing key column"))?;
    let seqno_col = cols
        .next()
        .ok_or(Error::InvalidHeader("columnar: missing seqno column"))?;
    let vt_col = cols
        .next()
        .ok_or(Error::InvalidHeader("columnar: missing value-type column"))?;
    let value_cols: Vec<Column> = cols.collect();

    // Shared key buffer: every row's key is a view into it.
    let key_data = Slice::from(key_col.data);

    // A single non-nullable bytes value column lets every row's value be a view
    // into one shared buffer; otherwise reconstruct (frame / fixed-width) per row.
    let single_bytes_value = matches!(
        value_cols.as_slice(),
        [c] if c.type_tag == TypeTag::Bytes && c.validity.is_none()
    );
    let value_source = if single_bytes_value {
        let single = value_cols
            .into_iter()
            .next()
            .ok_or(Error::InvalidHeader("columnar: value column vanished"))?;
        ValueSource::SharedBytes(Slice::from(single.data))
    } else {
        ValueSource::Reconstruct(value_cols)
    };

    let mut out = Vec::with_capacity(row_count as usize);
    for i in 0..row_count {
        let user_key = bytes_row_slice(&key_data, row_count, i)?;
        // Same key invariants as column_batch_to_entries (non-empty, u16 length).
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
        let value = match &value_source {
            ValueSource::SharedBytes(data) => bytes_row_slice(data, row_count, i)?,
            ValueSource::Reconstruct(cols) => reconstruct_row_value(cols, row_count, i)?,
        };
        out.push(InternalValue {
            key: InternalKey {
                user_key,
                seqno,
                value_type,
            },
            value,
        });
    }
    Ok(out)
}

/// Per-row value source for [`column_batch_into_entries`]: a shared bytes buffer
/// (zero-copy views) or the per-row framing reconstruction.
enum ValueSource {
    SharedBytes(Slice),
    Reconstruct(Vec<Column>),
}

/// Returns row `i` of a [`TypeTag::Bytes`] column body as a zero-copy [`Slice`]
/// view into `data` (the column's shared buffer), bounds-checked.
fn bytes_row_slice(data: &Slice, row_count: u32, i: u32) -> Result<Slice> {
    let bytes: &[u8] = data.as_ref();
    let off_bytes = (row_count as usize + 1) * 4;
    let read_off = |idx: u32| -> Result<usize> {
        let base = idx as usize * 4;
        let b = bytes
            .get(base..base + 4)
            .ok_or(Error::InvalidHeader("columnar: bytes offset truncated"))?;
        let arr: [u8; 4] = b
            .try_into()
            .map_err(|_| Error::InvalidHeader("columnar: short bytes offset"))?;
        Ok(u32::from_le_bytes(arr) as usize)
    };
    let start = read_off(i)?;
    let end = read_off(i + 1)?;
    let payload_start = off_bytes.checked_add(start).ok_or(Error::InvalidHeader(
        "columnar: bytes payload offset overflow",
    ))?;
    let payload_end = off_bytes.checked_add(end).ok_or(Error::InvalidHeader(
        "columnar: bytes payload offset overflow",
    ))?;
    if start > end || payload_end > bytes.len() {
        return Err(Error::InvalidHeader("columnar: bytes row out of range"));
    }
    Ok(data.slice(payload_start..payload_end))
}

/// Reconstructs only the rows whose key equals `needle`, for the columnar
/// point-read path.
///
/// Binary-searches the key column to the first row `>= needle`, then collects the
/// contiguous `== needle` run as entries (newest-first, matching block order),
/// skipping rows masked by the positional delete-bitmap. Returns an empty vec
/// when the key is absent (or every matching row is deleted). The caller
/// re-encodes this handful of rows into a tiny block and runs the normal
/// seqno-aware point read, so a columnar point read decodes the block once and
/// touches one key's rows instead of untransposing and re-encoding the whole
/// block.
pub fn column_batch_match_entries(
    batch: &ColumnBatch,
    needle: &[u8],
    comparator: &crate::comparator::SharedComparator,
    deletes: Option<(&crate::table::delete_bitmap::DeleteBitmap, u32)>,
) -> Result<Vec<InternalValue>> {
    let (key_col, seqno_col, vt_col, value_cols) = validate_columnar_columns(batch)?;
    let row_count = batch.row_count;

    // Lower bound: first row whose key is `>= needle` (keys are block-index
    // sorted: user_key ASC, seqno DESC).
    let mut lo = 0u32;
    let mut hi = row_count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let k = bytes_column_row(&key_col.data, row_count, mid)?;
        if comparator.compare(k, needle) == core::cmp::Ordering::Less {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    // Collect the contiguous `== needle` run from `lo`, skipping masked rows.
    let mut out = Vec::new();
    let mut row = lo;
    while row < row_count {
        let k = bytes_column_row(&key_col.data, row_count, row)?;
        if comparator.compare(k, needle) != core::cmp::Ordering::Equal {
            break;
        }
        let masked = deletes.is_some_and(|(bitmap, start)| {
            start
                .checked_add(row)
                .is_some_and(|pos| bitmap.contains(pos))
        });
        if !masked {
            // Match the engine's key invariants (non-empty, u16 length).
            if k.is_empty() || k.len() > u16::MAX as usize {
                return Err(Error::InvalidHeader(
                    "columnar: user key is empty or longer than u16::MAX",
                ));
            }
            let seqno = fixed_u64_row(&seqno_col.data, row)?;
            let vt_byte = vt_col
                .data
                .get(row as usize)
                .copied()
                .ok_or(Error::InvalidHeader("columnar: value-type row truncated"))?;
            let value_type = ValueType::try_from(vt_byte)
                .map_err(|()| Error::InvalidTag(("ValueType", vt_byte)))?;
            let value = reconstruct_row_value(value_cols, row_count, row)?;
            out.push(InternalValue {
                key: InternalKey {
                    user_key: Slice::from(k),
                    seqno,
                    value_type,
                },
                value,
            });
        }
        row += 1;
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
        // `i < cells.len()` and `bitmap_len = cells.len().div_ceil(8)`, so
        // `i / 8 < bitmap_len` always. Fail loudly rather than silently skip the
        // bit if a future refactor ever breaks that invariant: a clear bit on a
        // present cell would desync the bitmap from the appended body below.
        let byte = out.get_mut(i / 8).ok_or(Error::InvalidHeader(
            "columnar: presence bitmap index out of range",
        ))?;
        *byte |= 1u8 << (i % 8);
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
mod tests;

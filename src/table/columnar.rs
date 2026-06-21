// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Columnar (PAX / rowgroup) block format and the [`ColumnBatch`] read unit.
//!
//! A columnar block holds a row-group laid out column-by-column: each column is
//! an opaque, typed, codec-tagged byte array plus an optional validity bitmap.
//! The engine attaches no relational or graph meaning to a column; it knows
//! only the physical [`TypeTag`], the [`CodecId`] used to encode the bytes, and
//! a caller-assigned `column_id`. This is the payload of a
//! [`BlockType::Columnar`](crate::table::block::BlockType::Columnar) block; the
//! transpose that produces it on flush / compaction is std-only and lives in
//! the writer, while this encode / decode path is `core` + `alloc`.
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

use crate::{Error, Result};
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
}

impl From<CodecId> for u8 {
    fn from(c: CodecId) -> Self {
        match c {
            CodecId::Plain => 0,
        }
    }
}

impl TryFrom<u8> for CodecId {
    type Error = Error;

    fn try_from(v: u8) -> Result<Self> {
        match v {
            0 => Ok(Self::Plain),
            _ => Err(Error::InvalidTag(("ColumnCodecId", v))),
        }
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
            let (type_tag, width) = col.type_tag.to_wire();
            out.extend_from_slice(&col.column_id.to_le_bytes());
            out.push(type_tag);
            out.push(width);
            out.push(codec.into());
            out.push(u8::from(col.validity.is_some()));
            // Plain codec: stored bytes equal the decoded column bytes.
            out.extend_from_slice(&(col.data.len() as u32).to_le_bytes());
            if let Some(v) = &col.validity {
                out.extend_from_slice(v);
            }
            out.extend_from_slice(&col.data);
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
            let validity = if has_validity {
                Some(cur.read_bytes(validity_len(row_count))?.to_vec())
            } else {
                None
            };
            let data = cur.read_bytes(data_len)?.to_vec();
            // Plain codec is the identity, so the stored bytes are already the
            // decoded column bytes; structure-aware codecs decode here.
            let CodecId::Plain = codec;
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

#[expect(clippy::expect_used, clippy::indexing_slicing, reason = "test code")]
#[cfg(test)]
mod tests {
    use super::{CodecId, Column, ColumnBatch, TypeTag};

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

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
//! let bytes = batch.encode(CodecId::Plain);
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
            1 => Ok(Self::Bytes),
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

impl ColumnBatch {
    /// Encodes the batch into a columnar block payload using `codec` for every
    /// column. The returned bytes are the block payload (without the surrounding
    /// block header / checksum, which the writer adds).
    #[must_use]
    #[expect(
        clippy::cast_possible_truncation,
        reason = "column count and per-column chunk length are bounded by the block size policy, far below u32::MAX"
    )]
    pub fn encode(&self, codec: CodecId) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&self.row_count.to_le_bytes());
        out.extend_from_slice(&(self.columns.len() as u32).to_le_bytes());
        for col in &self.columns {
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
        out
    }

    /// Decodes a columnar block payload produced by [`ColumnBatch::encode`].
    ///
    /// # Errors
    ///
    /// Returns an error if the payload is truncated, carries an unknown type /
    /// codec tag, or describes a fixed-width column whose byte length is not a
    /// whole number of rows.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut cur = Cursor::new(bytes);
        let row_count = cur.read_u32()?;
        let column_count = cur.read_u32()?;
        let mut columns = Vec::with_capacity(column_count as usize);
        for _ in 0..column_count {
            let column_id = cur.read_u16()?;
            let type_tag = cur.read_u8()?;
            let width = cur.read_u8()?;
            let codec = CodecId::try_from(cur.read_u8()?)?;
            let has_validity = cur.read_u8()? != 0;
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
            if let TypeTag::Fixed(w) = type_tag {
                let expected =
                    (row_count as usize)
                        .checked_mul(w as usize)
                        .ok_or(Error::InvalidHeader(
                            "columnar: fixed column length overflow",
                        ))?;
                if data.len() != expected {
                    return Err(Error::InvalidHeader(
                        "columnar: fixed column byte length is not row_count * width",
                    ));
                }
            }
            columns.push(Column {
                column_id,
                type_tag,
                validity,
                data,
            });
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
        let encoded = batch.encode(CodecId::Plain);
        let decoded = ColumnBatch::decode(&encoded).expect("decode");
        assert_eq!(decoded, batch, "columnar batch must survive a round-trip");
    }

    #[test]
    fn columnar_decode_rejects_truncated_payload() {
        let encoded = sample_batch().encode(CodecId::Plain);
        // Drop the last byte: the final column's data is now short.
        let truncated = &encoded[..encoded.len() - 1];
        assert!(ColumnBatch::decode(truncated).is_err());
    }

    #[test]
    fn columnar_decode_rejects_fixed_width_length_mismatch() {
        // A fixed(4) column claiming 3 rows but carrying 8 bytes (= 2 rows).
        let bad = ColumnBatch {
            row_count: 3,
            columns: vec![Column {
                column_id: 1,
                type_tag: TypeTag::Fixed(4),
                validity: None,
                data: vec![0; 8],
            }],
        };
        let encoded = bad.encode(CodecId::Plain);
        assert!(ColumnBatch::decode(&encoded).is_err());
    }

    #[test]
    fn columnar_decode_rejects_unknown_codec_tag() {
        let mut encoded = sample_batch().encode(CodecId::Plain);
        // Codec byte of the first column sits after row_count(4) + col_count(4)
        // + column_id(2) + type_tag(1) + width(1) = offset 12.
        encoded[12] = 0xFF;
        assert!(ColumnBatch::decode(&encoded).is_err());
    }
}

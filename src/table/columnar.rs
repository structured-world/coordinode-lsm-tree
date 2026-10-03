// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Columnar (PAX / rowgroup) column format and the [`ColumnBatch`] read unit.
//!
//! A row group is laid out column-by-column: each column is an opaque, typed
//! byte array plus an optional validity bitmap, its values stored as an
//! [`Expression`] of light encodings chosen per page. The engine attaches no
//! relational or graph meaning to a column; it knows only the physical
//! [`TypeTag`], the encoding, and a caller-assigned `column_id`. On disk each column is its own
//! [`BlockType::ColumnPage`](crate::table::block::BlockType::ColumnPage),
//! found through the group's page directory. The intrinsic-field transpose
//! ([`entries_to_column_batch`] and back), the encode, and the decode path are
//! all `core` + `alloc` and live here; wiring the transpose into the flush /
//! compaction writer is std-only and lands separately.
//!
//! # Examples
//!
//! ```
//! use lsm_tree::table::columnar::{Column, ColumnBatch, TypeTag};
//!
//! // Two rows, one fixed-width u32 column with the second row null.
//! let batch = ColumnBatch {
//!     row_count: 2,
//!     columns: vec![Column {
//!         column_id: 7,
//!         type_tag: TypeTag::Fixed(4),
//!         validity: Some(vec![0b0000_0001]), // row 0 valid, row 1 null
//!         data: vec![1, 0, 0, 0, 0, 0, 0, 0].into(),
//!     }],
//! };
//! let bytes = batch.encode().unwrap();
//! assert_eq!(ColumnBatch::decode(&bytes.into()).unwrap(), batch);
//! ```
//!
//! # Schema evolution
//!
//! The format is schema-free: each column self-describes its `column_id`,
//! [`TypeTag`], and [`Expression`], so segments written at different times may
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

use crate::table::zone_map::ColumnStats;
use crate::{Error, Result, Slice, ValueType, key::InternalKey, value::InternalValue};
use alloc::vec::Vec;

pub(crate) use super::column_type::comparable_bytes;
pub use super::column_type::{ByteOrder, Number, NumberKind, TypeTag};

mod cells;
mod expr;

pub(crate) use cells::{cell_refs, entries_to_cells_batch};
pub(crate) use expr::{Bounds, Cell, Choice, Values};
pub use expr::{Candidate, Expression, candidates};

/// Largest ratio of decoded-block bytes to served-view bytes at which a
/// projection is still handed out as zero-copy views of the block. Above it
/// the views are detached into exact-size copies, so a narrow projection over
/// wide rows cannot pin the whole block (and, batch by batch, the whole table)
/// for the sake of a few bytes. `2` keeps every full decode and every
/// projection covering at least half the block on the zero-copy path; the
/// copies it does force are, by construction, at most half a block per batch.
const MAX_VIEW_AMPLIFICATION: usize = 2;

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
    ///
    /// A zero-copy view of the page for a column stored in its own layout
    /// ([`Expression::Plain`]), so a projection never copies it; a buffer
    /// built from the encoding for any other [`Expression`].
    pub data: crate::Slice,
}

/// A decoded columnar row-group: the read unit obtained by decoding a row
/// group's [`BlockType::ColumnPage`](crate::table::block::BlockType::ColumnPage)s.
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
    let Some(off_bytes) = (row_count as usize)
        .checked_add(1)
        .and_then(|count| count.checked_mul(4))
    else {
        return Err(Error::InvalidHeader(
            "columnar: bytes offset table overflow",
        ));
    };
    let Some(table) = data.get(..off_bytes) else {
        return Err(Error::InvalidHeader(
            "columnar: bytes column shorter than its offset table",
        ));
    };
    let payload_len = data.len() - off_bytes;
    let mut prev = 0usize;
    for (i, chunk) in table.chunks_exact(4).enumerate() {
        let Some(chunk) = chunk.first_chunk::<4>() else {
            return Err(Error::InvalidHeader("columnar: short bytes offset"));
        };
        let off = u32::from_le_bytes(*chunk) as usize;
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

/// Validates a column's `data` as `row_count` rows of `type_tag`: a nonzero
/// fixed width with `row_count * width` bytes, or a correctly framed `Bytes`
/// offset table.
fn check_layout(type_tag: TypeTag, row_count: u32, data: &[u8]) -> Result<()> {
    match type_tag.fixed_width() {
        Some(0) => Err(Error::InvalidHeader("columnar: fixed column width is zero")),
        Some(w) => {
            let Some(expected) = (row_count as usize).checked_mul(w as usize) else {
                return Err(Error::InvalidHeader(
                    "columnar: fixed column length overflow",
                ));
            };
            if data.len() != expected {
                return Err(Error::InvalidHeader(
                    "columnar: fixed column byte length is not row_count * width",
                ));
            }
            Ok(())
        }
        None => check_bytes_framing(data, row_count),
    }
}

impl Column {
    /// Whether row `row` holds a cell rather than a null: set in the validity
    /// bitmap, or the column has none.
    pub(crate) fn is_valid(&self, row: u32) -> bool {
        self.validity.as_deref().is_none_or(|bits| {
            bits.get(row as usize / 8)
                .is_some_and(|byte| byte >> (row % 8) & 1 == 1)
        })
    }

    /// Validates that the column is well-formed for `row_count` rows: a nonzero
    /// fixed width with `row_count * width` data bytes, a correctly framed
    /// `Bytes` offset table, and a correctly sized / padded validity bitmap.
    /// `encode` and `decode` both run this so a payload is accepted by one iff
    /// it is accepted by the other.
    pub(crate) fn validate(&self, row_count: u32) -> Result<()> {
        check_layout(self.type_tag, row_count, &self.data)?;
        if let Some(v) = &self.validity {
            check_validity(v, row_count)?;
        }
        Ok(())
    }

    /// Appends this column's wire form to `out`: its header, its validity and
    /// its values, encoded as the expression that costs least to store and to
    /// read ([`expr`]), which it returns.
    ///
    /// ```text
    /// [column_id: u16 LE] [type: u8] [width: u8] [has_validity: u8]
    /// [values_len: var] [validity: ceil(rows / 8), when present] [values]
    /// ```
    ///
    /// This is the unit a column page carries, and the unit
    /// [`ColumnBatch::encode`] concatenates, so the two cannot drift apart.
    fn encode_into(
        &self,
        row_count: u32,
        encoding: crate::config::ColumnEncoding,
        out: &mut Vec<u8>,
    ) -> Result<Expression> {
        self.validate(row_count)?;
        let Choice {
            bytes, expression, ..
        } = expr::choose(self.type_tag, row_count, &self.data, encoding)?;
        let (type_tag, width) = self.type_tag.to_wire();
        out.extend_from_slice(&self.column_id.to_le_bytes());
        out.push(type_tag);
        out.push(width);
        out.push(u8::from(self.validity.is_some()));
        crate::table::column_page::put_varint(out, bytes.len() as u64);
        if let Some(v) = &self.validity {
            out.extend_from_slice(v);
        }
        out.extend_from_slice(&bytes);
        Ok(expression)
    }

    /// The null count of rows `start..end` of this `Bytes` column of
    /// `row_count` rows, and the byte-wise range of their non-null values
    /// (`None` when every one is null): what a row page's statistics zone
    /// records, before its bounds are cut.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidHeader`] when the column is not a `Bytes`
    /// column, `start..end` is not within `row_count`, or a row's framing is
    /// malformed.
    pub(crate) fn bytes_range(
        &self,
        row_count: u32,
        start: u32,
        end: u32,
    ) -> Result<NullsAndRange<'_>> {
        if self.type_tag != TypeTag::Bytes {
            return Err(Error::InvalidHeader(
                "columnar: a zone describes a bytes column",
            ));
        }
        if start > end || end > row_count {
            return Err(Error::InvalidHeader(
                "columnar: row range outside the column",
            ));
        }
        let mut nulls = 0u32;
        let mut range: Option<(&[u8], &[u8])> = None;
        for row in start..end {
            if !column_row_valid(self, row) {
                nulls += 1;
                continue;
            }
            let value = bytes_column_row(&self.data, row_count, row)?;
            range = Some(match range {
                None => (value, value),
                Some((min, max)) => (min.min(value), max.max(value)),
            });
        }
        Ok((nulls, range))
    }

    /// The null count of rows `start..end` of this number column, and the
    /// ordinal range ([`Number::ordinal`]) of their non-null values (`None`
    /// when every one is null): the number column's counterpart of
    /// [`Self::bytes_range`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidHeader`] when the column is not a
    /// [`TypeTag::Number`] or `start..end` is not within its rows.
    pub(crate) fn number_range(&self, start: u32, end: u32) -> Result<(u32, Option<(u128, u128)>)> {
        let TypeTag::Number(number) = self.type_tag else {
            return Err(Error::InvalidHeader(
                "columnar: a number range describes a number column",
            ));
        };
        let width = usize::from(number.width);
        let Some(cells) = self.data.get(start as usize * width..end as usize * width) else {
            return Err(Error::InvalidHeader(
                "columnar: row range outside the column",
            ));
        };
        let mut nulls = 0u32;
        let mut range: Option<(u128, u128)> = None;
        for (row, cell) in (start..end).zip(cells.chunks_exact(width)) {
            if !column_row_valid(self, row) {
                nulls += 1;
                continue;
            }
            let value = number.ordinal(cell);
            range = Some(match range {
                None => (value, value),
                Some((min, max)) => (min.min(value), max.max(value)),
            });
        }
        Ok((nulls, range))
    }

    /// Rows `start..end` of this column of `row_count` rows, as a column of
    /// their own: the unit one row page of the column holds.
    ///
    /// A fixed-width column's rows are a view of its data; a `Bytes` column's
    /// are re-framed under offsets that start at zero, and the validity bitmap
    /// is re-packed from bit `start`. The write path builds these, once per
    /// page it writes.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidHeader`] when `start..end` is not within
    /// `row_count`, or when the column is malformed for `row_count`.
    pub(crate) fn rows(&self, row_count: u32, start: u32, end: u32) -> Result<Self> {
        if start > end || end > row_count {
            return Err(Error::InvalidHeader(
                "columnar: row range outside the column",
            ));
        }
        let data = if let Some(w) = self.type_tag.fixed_width() {
            let w = usize::from(w);
            let (from, to) = (start as usize * w, end as usize * w);
            if to > self.data.len() {
                return Err(Error::InvalidHeader(
                    "columnar: fixed column shorter than its rows",
                ));
            }
            self.data.slice(from..to)
        } else {
            let cells = || (start..end).map(|i| bytes_column_row(&self.data, row_count, i));
            // Checked once, so the framing below can take the cells as they
            // are.
            for cell in cells() {
                cell?;
            }
            frame_bytes_column((end - start) as usize, || {
                cells().map(Result::unwrap_or_default)
            })?
        };
        let validity = self.validity.as_deref().map(|v| {
            let rows = end - start;
            let mut out = alloc::vec![0u8; validity_len(rows)];
            for i in 0..rows {
                if validity_bit(v, start + i)
                    && let Some(byte) = out.get_mut((i / 8) as usize)
                {
                    *byte |= 1u8 << (i % 8);
                }
            }
            out
        });
        Ok(Self {
            column_id: self.column_id,
            type_tag: self.type_tag,
            validity,
            data,
        })
    }

    /// The payload of this column's page: `stamp`, then the column's wire
    /// form, its values stored as `encoding` says.
    ///
    /// # Errors
    ///
    /// As [`ColumnBatch::encode`], for this one column.
    pub(crate) fn encode_page(
        &self,
        row_count: u32,
        stamp: crate::table::column_page::PageStamp,
        encoding: crate::config::ColumnEncoding,
    ) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        stamp.encode_into(&mut out);
        self.encode_into(row_count, encoding, &mut out)?;
        Ok(out)
    }

    /// A column page's payload for a group of `row_count` rows, refused unless
    /// it carries `expected` as its stamp: its header and its values as
    /// stored, borrowed from the page and not decoded. The one reader of a
    /// page's layout; decoding the page, describing its encoding and reading a
    /// few of its rows all start here.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidHeader`] for a stamp other than `expected`, for
    /// a malformed column, or for bytes left over after it: a page holds one
    /// column, and a tail means either a writer this build does not understand
    /// or a corruption.
    pub(crate) fn parse_page(
        bytes: &crate::Slice,
        row_count: u32,
        expected: crate::table::column_page::PageStamp,
    ) -> Result<PageColumn<'_>> {
        use crate::table::column_page::PageStamp;

        let mut cur = Cursor::new(bytes);
        if PageStamp::decode(cur.read_array::<{ PageStamp::LEN }>()?) != expected {
            return Err(Error::InvalidHeader(
                "columnar: page belongs to another row group, column part or row page",
            ));
        }
        let raw = RawColumn::read(&mut cur, row_count)?;
        if !cur.is_empty() {
            return Err(Error::InvalidHeader(
                "columnar: trailing bytes after the page's column",
            ));
        }
        raw.parse(row_count, bytes)
    }

    /// Decodes a column page's payload for a group of `row_count` rows,
    /// refusing it unless it carries `expected` as its stamp.
    ///
    /// A column stored in its own layout comes back as a zero-copy view of
    /// `bytes`, which here is the page's own payload rather than a whole row
    /// group's, so holding the column keeps exactly its own page alive and
    /// nothing else.
    ///
    /// # Errors
    ///
    /// As [`Self::parse_page`], and [`Error::InvalidHeader`] when the values
    /// do not decode to `row_count` rows.
    ///
    /// Adds to `copied` the validity bitmap it copies out of the page, and
    /// charges what it builds to `budget`, the page's group's.
    pub(crate) fn decode_page(
        bytes: &crate::Slice,
        row_count: u32,
        expected: crate::table::column_page::PageStamp,
        copied: &mut usize,
        budget: &mut DecodeBudget,
    ) -> Result<Self> {
        Self::parse_page(bytes, row_count, expected)?.decode(row_count, copied, budget)
    }

    /// Reads one column's wire form from `cur`, which walks `bytes`.
    ///
    /// Returns `None` when `want` is false: the column's validity and values
    /// are stepped over, still bounds-checked, but never copied or parsed,
    /// which is what a projection pays for a column it did not ask for. The
    /// second field is the length of a zero-copy view into `bytes`, zero when
    /// the column had to be decoded into its own buffer, so a caller can
    /// decide whether keeping `bytes` alive for the view is proportionate.
    /// Adds the validity bitmap it copies out to `copied`, and charges what
    /// it builds to `budget`, the group's.
    fn decode_from(
        cur: &mut Cursor<'_>,
        bytes: &crate::Slice,
        row_count: u32,
        want: impl Fn(u16) -> bool,
        copied: &mut usize,
        budget: &mut DecodeBudget,
    ) -> Result<Option<(Self, usize)>> {
        let raw = RawColumn::read(cur, row_count)?;
        if !want(raw.column_id) {
            return Ok(None);
        }
        let column = raw.parse(row_count, bytes)?;
        let viewed = match column.values {
            Values::Plain(data) => data.len(),
            _ => 0,
        };
        Ok(Some((column.decode(row_count, copied, budget)?, viewed)))
    }
}

/// One column's wire form, framed but not parsed: what stepping over a
/// column costs.
struct RawColumn<'a> {
    column_id: u16,
    type_tag: TypeTag,
    validity: Option<&'a [u8]>,
    values: &'a [u8],
}

impl<'a> RawColumn<'a> {
    /// Reads one column's header, validity and values off `cur` for a group
    /// of `row_count` rows.
    fn read(cur: &mut Cursor<'a>, row_count: u32) -> Result<Self> {
        let column_id = cur.read_u16()?;
        let type_tag = cur.read_u8()?;
        let width = cur.read_u8()?;
        let has_validity = match cur.read_u8()? {
            0 => false,
            1 => true,
            _ => {
                return Err(Error::InvalidHeader(
                    "columnar: validity flag must be 0 or 1",
                ));
            }
        };
        let values_len = cur.read_var_u32()? as usize;
        let type_tag = TypeTag::from_wire(type_tag, width)?;
        let validity = if has_validity {
            Some(cur.read_bytes(validity_len(row_count))?)
        } else {
            None
        };
        Ok(Self {
            column_id,
            type_tag,
            validity,
            values: cur.read_bytes(values_len)?,
        })
    }

    /// The column with its values parsed and its validity checked, its bytes
    /// read from `page`.
    fn parse(self, row_count: u32, page: &'a crate::Slice) -> Result<PageColumn<'a>> {
        if let Some(v) = self.validity {
            check_validity(v, row_count)?;
        }
        Ok(PageColumn {
            page,
            column_id: self.column_id,
            type_tag: self.type_tag,
            validity: self.validity,
            values: Values::parse(self.type_tag, row_count, self.values)?,
        })
    }
}

/// One column of one page as stored: its header and its values' encoding,
/// borrowed from the page.
pub(crate) struct PageColumn<'a> {
    /// The page the column was read from, which a column stored in its own
    /// layout is served as a view of.
    page: &'a crate::Slice,
    /// The column's id.
    pub(crate) column_id: u16,
    /// The column's type.
    pub(crate) type_tag: TypeTag,
    /// Its validity bitmap, checked for its length and padding.
    pub(crate) validity: Option<&'a [u8]>,
    /// Its values, parsed.
    pub(crate) values: Values<'a>,
}

impl PageColumn<'_> {
    /// The column decoded into its own layout for `row_count` rows, a view of
    /// its page where it is stored that way. Adds to `copied` the validity
    /// bitmap it copies and the bytes it builds from any other encoding:
    /// building a column is a copy of its values, where a view is none.
    /// What it builds is charged to `budget`, its group's.
    pub(crate) fn decode(
        self,
        row_count: u32,
        copied: &mut usize,
        budget: &mut DecodeBudget,
    ) -> Result<Column> {
        let built = !matches!(self.values, Values::Plain(_));
        let data = budget.build(self.page.len(), |limit| {
            self.values
                .materialize(self.type_tag, row_count, self.page, limit)
        })?;
        if built {
            *copied += data.len();
        }
        let validity = self.validity.map(|v| {
            *copied += v.len();
            v.to_vec()
        });
        let column = Column {
            column_id: self.column_id,
            type_tag: self.type_tag,
            validity,
            data,
        };
        // Same well-formedness gate the encoder runs, so a payload decodes
        // iff it could have been produced by `encode_into`.
        column.validate(row_count)?;
        Ok(column)
    }

    /// The rows of this column of `row_count` rows that `bounds` keeps,
    /// answered from its encoding without decoding it; a null row is never
    /// kept.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidHeader`] when the encoding does not describe
    /// `row_count` rows.
    pub(crate) fn select(
        &self,
        row_count: u32,
        bounds: &Bounds<'_>,
    ) -> Result<crate::table::columnar_predicate::Selection> {
        let mut kept = self.values.select(self.type_tag, row_count, bounds)?;
        if let Some(validity) = self.validity {
            kept.intersect(&crate::table::columnar_predicate::Selection::from_bitmap(
                validity, row_count,
            ));
        }
        Ok(kept)
    }

    /// The column's rows `keep` selects, of its `row_count`, as a column of
    /// their own, built straight from the encoding. Adds what it builds, and
    /// the validity bits it gathers, to `copied`, and charges what it builds
    /// to `budget`, its group's.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidHeader`] when the encoding does not describe
    /// `row_count` rows, [`Error::DecompressedSizeTooLarge`] when the rows
    /// would take more than `budget` allows.
    pub(crate) fn decode_rows(
        self,
        row_count: u32,
        keep: &crate::table::columnar_predicate::Selection,
        copied: &mut usize,
        budget: &mut DecodeBudget,
    ) -> Result<Column> {
        let rows = keep.count();
        let (type_tag, values) = (self.type_tag, self.values);
        let data = budget.build(self.page.len(), |limit| {
            values.materialize_rows(type_tag, row_count, keep, limit)
        })?;
        *copied += data.len();
        let validity = self.validity.map(|v| {
            let mut out = alloc::vec![0u8; validity_len(rows)];
            for (at, row) in keep.rows().enumerate() {
                if validity_bit(v, row)
                    && let Some(byte) = out.get_mut(at / 8)
                {
                    *byte |= 1u8 << (at % 8);
                }
            }
            *copied += out.len();
            out
        });
        let column = Column {
            column_id: self.column_id,
            type_tag: self.type_tag,
            validity,
            data,
        };
        column.validate(rows)?;
        Ok(column)
    }
}

/// What a read of one row group may make of its pages beyond their own bytes:
/// the columns it builds from their encodings, and the rows it holds decoded
/// to read them (a run's ends, a row's offset or integer).
///
/// A writer closes a group once its rows reach the group size, at most
/// [`crate::config::MAX_BLOCK_SIZE`], counting every column's bytes of each
/// row, so the rows before a group's last add up to less than that over all
/// its columns, and the last row is either stored on its pages or repeats
/// values of the rows before it. A group a writer cut therefore never builds
/// more than twice that past its pages, and since a column held decoded adds
/// at least a byte to each row, never holds more rows than that decoded
/// either. An encoding can describe far more bytes than it stores, so this is
/// what stops a forged group from decoding into gigabytes, whether from one
/// page or column or spread over thousands.
#[derive(Debug)]
pub(crate) struct DecodeBudget {
    /// What the group may build past its pages, and hold decoded, each.
    allowance: u64,
    /// Bytes built past the pages so far, at most `allowance`.
    built: u64,
    /// Rows held decoded so far, at most `allowance`.
    held: u64,
}

impl Default for DecodeBudget {
    /// The budget of one row group a writer cut.
    fn default() -> Self {
        Self {
            allowance: 2 * u64::from(crate::config::MAX_BLOCK_SIZE),
            built: 0,
            held: 0,
        }
    }
}

impl DecodeBudget {
    /// No budget beyond what the `u32` offsets of a column hold: for a
    /// caller's own batch payload, of any size its encoder accepts.
    fn unbounded() -> Self {
        Self {
            allowance: u64::MAX / 2,
            built: 0,
            held: 0,
        }
    }

    /// Builds a column from a page of `page_len` bytes with `build`, given the
    /// most bytes the column may take, and charges what it built past the
    /// page.
    ///
    /// # Errors
    ///
    /// What `build` returns, which refuses a column past the limit before
    /// building it.
    pub(crate) fn build(
        &mut self,
        page_len: usize,
        build: impl FnOnce(u64) -> Result<Slice>,
    ) -> Result<Slice> {
        debug_assert!(self.built <= self.allowance, "charged within the allowance");
        let page_len = page_len as u64;
        // A page is at most what a u32 block length holds, and the allowance
        // at most half a u64.
        let limit = page_len + (self.allowance - self.built);
        let data = build(limit)?;
        let len = data.len() as u64;
        debug_assert!(len <= limit, "a build refuses a column past its limit");
        // A column no larger than its page, a view of it among them, builds
        // nothing past it.
        if len > page_len {
            self.built += len - page_len;
        }
        Ok(data)
    }

    /// Charges `rows` rows held decoded, refused past the allowance.
    ///
    /// # Errors
    ///
    /// [`Error::DecompressedSizeTooLarge`] past the allowance.
    pub(crate) fn hold(&mut self, rows: u64) -> Result<()> {
        // At most the allowance, half a u64, plus a u32's worth of rows.
        let held = self.held + rows;
        if held > self.allowance {
            return Err(Error::DecompressedSizeTooLarge {
                declared: held,
                limit: self.allowance,
            });
        }
        self.held = held;
        Ok(())
    }

    /// `values`, of `n` rows of a column of `type_tag`, prepared for row
    /// reads, the rows the preparation may hold decoded charged first: none
    /// for a layout or a constant, which are read in place.
    ///
    /// # Errors
    ///
    /// As [`Self::hold`] and [`Values::rows`].
    pub(crate) fn rows<'a>(
        &mut self,
        values: Values<'a>,
        type_tag: TypeTag,
        n: u32,
    ) -> Result<expr::Rows<'a>> {
        if !matches!(values, Values::Plain(_) | Values::Constant(_)) {
            self.hold(u64::from(n))?;
        }
        values.rows(type_tag, n)
    }

    /// `column` as parsed, the run ends its parse holds decoded charged.
    ///
    /// # Errors
    ///
    /// As [`Self::hold`].
    pub(crate) fn parsed<'a>(&mut self, column: PageColumn<'a>) -> Result<PageColumn<'a>> {
        self.hold(column.values.held_rows())?;
        Ok(column)
    }
}

impl ColumnBatch {
    /// Cuts the batch's rows into row pages of at least `page_size` bytes
    /// each, the last one excepted, returning the rows in each.
    ///
    /// A row's bytes are what it adds across every column: a fixed column's
    /// width, a `Bytes` cell's length plus its offset. A page is closed once it
    /// reaches `page_size`, so a row wider than the target is a page of its
    /// own, and a `page_size` of zero puts every row on its own page.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidHeader`] when a `Bytes` column is malformed for
    /// the batch's rows.
    pub(crate) fn row_page_cuts(&self, page_size: u32) -> Result<Vec<u32>> {
        let mut cuts = Vec::new();
        let mut rows_in_page = 0u32;
        let mut bytes_in_page = 0u64;
        for row in 0..self.row_count {
            bytes_in_page += self.row_bytes(row)?;
            rows_in_page += 1;
            if bytes_in_page >= u64::from(page_size) {
                cuts.push(rows_in_page);
                rows_in_page = 0;
                bytes_in_page = 0;
            }
        }
        if rows_in_page > 0 {
            cuts.push(rows_in_page);
        }
        Ok(cuts)
    }

    /// The bytes row `row` adds across every column: a fixed column's width, a
    /// `Bytes` cell's length plus its offset. The one measure row pages and
    /// row groups are both cut by, so a page size and a group size of the same
    /// bytes hold the same rows.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidHeader`] when a `Bytes` column is malformed for
    /// the batch's rows.
    fn row_bytes(&self, row: u32) -> Result<u64> {
        let mut bytes = 0u64;
        for col in &self.columns {
            bytes += match col.type_tag.fixed_width() {
                Some(w) => u64::from(w),
                None => bytes_column_row(&col.data, self.row_count, row)?.len() as u64 + 4,
            };
        }
        Ok(bytes)
    }

    /// The bytes every row adds across every column ([`Self::row_bytes`]
    /// summed): what a row group accumulating this batch counts toward its
    /// size.
    ///
    /// # Errors
    ///
    /// As [`Self::row_bytes`].
    pub(crate) fn rows_bytes(&self) -> Result<u64> {
        (0..self.row_count).try_fold(0u64, |sum, row| Ok(sum + self.row_bytes(row)?))
    }

    /// The batch's rows `start..end` as a batch of their own, every column cut
    /// the same way ([`Column::rows`]).
    ///
    /// # Errors
    ///
    /// As [`Column::rows`].
    pub(crate) fn rows(&self, start: u32, end: u32) -> Result<Self> {
        Ok(Self {
            row_count: end.checked_sub(start).ok_or(Error::InvalidHeader(
                "columnar: row range outside the batch",
            ))?,
            columns: self
                .columns
                .iter()
                .map(|col| col.rows(self.row_count, start, end))
                .collect::<Result<_>>()?,
        })
    }

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
        let Some(key_col) = self.columns.first().filter(|c| c.column_id == COL_USER_KEY) else {
            return Err(Error::InvalidHeader(
                "columnar: first column is not the user-key column",
            ));
        };
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
        let Some(key_col) = self.columns.first().filter(|c| c.column_id == COL_USER_KEY) else {
            return Err(Error::InvalidHeader(
                "columnar: first column is not the user-key column",
            ));
        };
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

    /// Total size of the column bytes, data plus any validity bitmap: what a
    /// gather that built this batch copied, which the read counters record,
    /// and what a scan holding it counts against its payload budget.
    #[must_use]
    pub(crate) fn data_size(&self) -> usize {
        self.columns
            .iter()
            .map(|c| c.data.len() + c.validity.as_ref().map_or(0, Vec::len))
            .sum()
    }

    /// Per-column zone-map statistics for this block: one [`ColumnStats`] entry
    /// per [`TypeTag::Bytes`] and [`TypeTag::Number`] column, in column order.
    ///
    /// `min` / `max` are the byte-wise minimum / maximum of the column's
    /// comparable encoding over its non-null rows (an all-null column records
    /// an empty range and its null count): a `Bytes` column's raw value bytes,
    /// a `Number` column's [`Number::comparable`] bytes. An opaque
    /// [`TypeTag::Fixed`] column is omitted, since its bytes have no defined
    /// order and a raw-byte min / max could let a block-skip drop matching
    /// rows. Omitting it keeps
    /// [`can_skip_block`](super::columnar_predicate::ColumnRangePredicate::can_skip_block)
    /// conservative for it (no entry means the block is never skipped on it).
    ///
    /// The table writer records these for every columnar block, and
    /// `verify_zone_map` re-derives them from the decoded block to authenticate
    /// the recorded section, so the two computations must stay identical.
    #[must_use]
    pub(crate) fn zone_stats(&self) -> Vec<ColumnStats> {
        let rows = self.row_count;
        let mut stats = Vec::new();
        for col in &self.columns {
            if let TypeTag::Number(number) = col.type_tag {
                // A column whose length is not its rows' is refused by every
                // decode; recording nothing for it keeps the block unskipped.
                let Ok((null_count, range)) = col.number_range(0, rows) else {
                    continue;
                };
                let (min, max) = match range {
                    Some((min, max)) => (
                        comparable_bytes(number, min).to_vec(),
                        comparable_bytes(number, max).to_vec(),
                    ),
                    None => (Vec::new(), Vec::new()),
                };
                stats.push(ColumnStats {
                    column_id: u32::from(col.column_id),
                    type_tag: col.type_tag.to_wire().0,
                    codec_id: 0,
                    null_count,
                    row_count: rows,
                    min,
                    max,
                });
                continue;
            }
            let TypeTag::Bytes = col.type_tag else {
                continue;
            };
            let mut min: Option<&[u8]> = None;
            let mut max: Option<&[u8]> = None;
            // Bounded by `rows` (a u32), so the count never overflows.
            let mut null_count: u32 = 0;
            for i in 0..rows {
                if !column_row_valid(col, i) {
                    null_count += 1;
                    continue;
                }
                let Ok(value) = bytes_column_row(&col.data, rows, i) else {
                    continue;
                };
                if min.is_none_or(|m| value < m) {
                    min = Some(value);
                }
                if max.is_none_or(|m| value > m) {
                    max = Some(value);
                }
            }
            stats.push(ColumnStats {
                column_id: u32::from(col.column_id),
                type_tag: TypeTag::Bytes.to_wire().0,
                codec_id: 0,
                null_count,
                row_count: rows,
                min: min.unwrap_or(&[]).to_vec(),
                max: max.unwrap_or(&[]).to_vec(),
            });
        }
        stats
    }

    /// The statistics zones of this batch cut into row pages of `row_pages`
    /// rows: one per row page and ordered (`Bytes` or `Number`) column, in
    /// column order, over the same comparable encoding as [`Self::zone_stats`].
    ///
    /// The writer records these in a group's directory, and the verification
    /// gates re-derive them from the decoded group to authenticate it, so the
    /// two share this computation.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidHeader`] when the row pages overrun the batch
    /// or an ordered column is malformed.
    pub(crate) fn page_zones(
        &self,
        row_pages: &[u32],
    ) -> Result<crate::table::column_page::PageZones> {
        let ordered: Vec<&Column> = self
            .columns
            .iter()
            .filter(|c| matches!(c.type_tag, TypeTag::Bytes | TypeTag::Number(_)))
            .collect();
        let mut zones = crate::table::column_page::PageZones::new(
            ordered.iter().map(|c| c.column_id).collect(),
        );
        let mut start = 0u32;
        for &rows in row_pages {
            let Some(end) = start.checked_add(rows) else {
                return Err(Error::InvalidHeader(
                    "columnar: row pages overrun the batch",
                ));
            };
            for col in &ordered {
                if let TypeTag::Number(number) = col.type_tag {
                    let (nulls, range) = col.number_range(start, end)?;
                    match range {
                        Some((min, max)) => zones.push(
                            nulls,
                            Some((
                                &comparable_bytes(number, min),
                                &comparable_bytes(number, max),
                            )),
                        ),
                        None => zones.push(nulls, None),
                    }
                } else {
                    let (nulls, range) = col.bytes_range(self.row_count, start, end)?;
                    zones.push(nulls, range);
                }
            }
            start = end;
        }
        Ok(zones)
    }

    /// The statistics zones of this batch as a row group cut into row pages of
    /// `row_pages` rows lays them out: the key column's for its directory, and
    /// the other columns' for its zone block. A group of one row page has
    /// neither, since its zone is its zone-map entry.
    ///
    /// The writer lays a group out from these, and the verification gates
    /// re-derive them from the decoded group and require the group to carry
    /// exactly these, so the two share this one rule.
    ///
    /// # Errors
    ///
    /// As [`Self::page_zones`].
    pub(crate) fn group_zones(
        &self,
        row_pages: &[u32],
    ) -> Result<(
        crate::table::column_page::PageZones,
        crate::table::column_page::PageZones,
    )> {
        if row_pages.len() <= 1 {
            return Ok(Default::default());
        }
        let zones = self.page_zones(row_pages)?;
        Ok((
            zones.only(|c| c == COL_USER_KEY),
            zones.only(|c| c != COL_USER_KEY),
        ))
    }

    /// The rows of `pages`, in order, as one batch: a row group read page by
    /// page, put back together for a consumer that takes the group whole.
    ///
    /// One page is returned as it is. Several are joined column by column,
    /// each column framed once: a `Fixed` column's bodies back to back, a
    /// `Bytes` column's cells under one offset table, and a validity bitmap
    /// wherever a page has one (a page without counts its rows as present).
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidHeader`] for no pages, pages whose columns
    /// differ (id, type or count), a row count past `u32::MAX`, or a column
    /// malformed for its page's rows.
    pub(crate) fn concat(pages: Vec<Self>) -> Result<Self> {
        let mut pages = pages.into_iter();
        let Some(first) = pages.next() else {
            return Err(Error::InvalidHeader("columnar: row group has no row pages"));
        };
        let rest: Vec<Self> = pages.collect();
        if rest.is_empty() {
            return Ok(first);
        }
        let all = || core::iter::once(&first).chain(&rest);
        let mut row_count = 0u32;
        for page in all() {
            if page.columns.len() != first.columns.len() {
                return Err(Error::InvalidHeader(
                    "columnar: row pages disagree on their columns",
                ));
            }
            for (a, b) in first.columns.iter().zip(&page.columns) {
                if a.column_id != b.column_id || a.type_tag != b.type_tag {
                    return Err(Error::InvalidHeader(
                        "columnar: row pages disagree on their columns",
                    ));
                }
                b.validate(page.row_count)?;
            }
            let Some(sum) = row_count.checked_add(page.row_count) else {
                return Err(Error::InvalidHeader(
                    "columnar: row group row count exceeds u32",
                ));
            };
            row_count = sum;
        }
        let mut columns = Vec::with_capacity(first.columns.len());
        for (index, head) in first.columns.iter().enumerate() {
            let parts = || all().filter_map(|page| page.columns.get(index).map(|c| (page, c)));
            let data = match head.type_tag {
                TypeTag::Fixed(_) | TypeTag::Number(_) => {
                    let len = parts().map(|(_, c)| c.data.len()).sum();
                    let mut out = Vec::with_capacity(len);
                    for (_, c) in parts() {
                        out.extend_from_slice(&c.data);
                    }
                    Slice::from(out)
                }
                TypeTag::Bytes => frame_bytes_column(row_count as usize, || {
                    parts().flat_map(|(page, c)| {
                        // Every page's framing was validated above.
                        (0..page.row_count).map(move |i| {
                            bytes_column_row(&c.data, page.row_count, i).unwrap_or_default()
                        })
                    })
                })?,
            };
            let validity = if parts().any(|(_, c)| c.validity.is_some()) {
                let mut out = alloc::vec![0u8; validity_len(row_count)];
                let mut at = 0u32;
                for (page, c) in parts() {
                    for i in 0..page.row_count {
                        let present = c.validity.as_deref().is_none_or(|v| validity_bit(v, i));
                        if present && let Some(byte) = out.get_mut((at / 8) as usize) {
                            *byte |= 1u8 << (at % 8);
                        }
                        at += 1;
                    }
                }
                Some(out)
            } else {
                None
            };
            columns.push(Column {
                column_id: head.column_id,
                type_tag: head.type_tag,
                validity,
                data,
            });
        }
        Ok(Self { row_count, columns })
    }

    /// Encodes the batch into a columnar payload, each column's values as the
    /// expression that costs least to store and to read. The returned bytes
    /// are the payload alone, without a block header or checksum.
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
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        out.extend_from_slice(&self.row_count.to_le_bytes());
        out.extend_from_slice(&(self.columns.len() as u32).to_le_bytes());
        for col in &self.columns {
            col.encode_into(
                self.row_count,
                crate::config::ColumnEncoding::Auto,
                &mut out,
            )?;
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
    ///
    /// The payload is trusted for the size of its batch: it may describe up to
    /// `u32::MAX` rows of one repeated value in a few bytes, and every row is
    /// built. A table's reads bound what they build by the row group a writer
    /// cuts; this decoder has no such bound, since [`ColumnBatch::encode`]
    /// takes a batch of any size.
    pub fn decode(bytes: &crate::Slice) -> Result<Self> {
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
    pub fn decode_projected(bytes: &crate::Slice, wanted: &[u16]) -> Result<Self> {
        Self::decode_inner(bytes, Some(wanted))
    }

    /// Shared decode body. `wanted == None` decodes every column; `Some(ids)`
    /// decodes only the listed columns and skips the rest (their validity + data
    /// bytes are stepped over, never allocated or codec-decoded).
    ///
    /// Takes the refcounted block bytes so `Plain` columns come back as
    /// zero-copy views of the block instead of per-column copies — as long as
    /// the projection covers enough of the block for a view to be worth what
    /// it retains (see [`MAX_VIEW_AMPLIFICATION`]). No read path decodes a
    /// whole batch payload (a table's reads decode pages, and count their
    /// copies there), so nothing here is counted.
    fn decode_inner(bytes: &crate::Slice, wanted: Option<&[u16]>) -> Result<Self> {
        // Smallest possible column: id(2) + type(1) + width(1) +
        // has_validity(1) + a one-byte values length and a values expression
        // of at least its operator tag.
        const MIN_COLUMN_BYTES: usize = 7;
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
        // Bytes the served `Plain` views cover, and which columns they are:
        // the retention check below decides once per block whether keeping
        // the whole block alive for them is proportionate.
        let mut viewed_bytes = 0usize;
        let mut view_columns: Vec<usize> = Vec::new();
        let want = |id: u16| wanted.is_none_or(|w| w.contains(&id));
        let mut budget = DecodeBudget::unbounded();
        for _ in 0..column_count {
            let Some((column, viewed)) =
                Column::decode_from(&mut cur, bytes, row_count, want, &mut 0, &mut budget)?
            else {
                continue;
            };
            if viewed > 0 {
                viewed_bytes += viewed;
                view_columns.push(columns.len());
            }
            columns.push(column);
        }
        // Checked before the retention detach below, so a refused payload is
        // not copied for nothing.
        if !cur.is_empty() {
            return Err(Error::InvalidHeader(
                "columnar: trailing bytes after the last column",
            ));
        }
        // Retention check: a view keeps the WHOLE decoded block alive, skipped
        // columns included, for as long as any served column lives — and the
        // scan buffers one batch per block (a whole overlap group on the merge
        // path). A narrow projection over value-heavy rows (a key-only scan)
        // would then pin the full table payload while exposing a sliver of it.
        // When the views cover too small a share of the block, detach them
        // into exact-size copies instead: those columns are small by
        // definition, so the copy is cheap, and the block can be freed.
        if !view_columns.is_empty() && bytes.len() > MAX_VIEW_AMPLIFICATION * viewed_bytes {
            for idx in view_columns {
                if let Some(col) = columns.get_mut(idx) {
                    col.data = crate::Slice::from(&col.data[..]);
                }
            }
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
        let Some(end) = self.pos.checked_add(n) else {
            return Err(Error::InvalidHeader("columnar: read length overflow"));
        };
        let Some(slice) = self.buf.get(self.pos..end) else {
            return Err(Error::InvalidHeader("columnar: truncated block payload"));
        };
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

    /// Reads a LEB128 varint that must fit a `u32`.
    fn read_var_u32(&mut self) -> Result<u32> {
        let mut rest = self.buf.get(self.pos..).unwrap_or_default();
        let before = rest.len();
        let Some(value) = crate::table::column_page::take_varint(&mut rest, 5)
            .and_then(|v| u32::try_from(v).ok())
        else {
            return Err(Error::InvalidHeader("columnar: malformed length varint"));
        };
        self.pos += before - rest.len();
        Ok(value)
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
        let Some(end) = off.checked_add(len) else {
            return Err(Error::InvalidHeader("columnar: column payload exceeds u32"));
        };
        off = end;
        payload.extend_from_slice(r);
        offsets.extend_from_slice(&off.to_le_bytes());
    }
    offsets.extend_from_slice(&payload);
    Ok(offsets)
}

/// Whether row `i` of `col` is valid (non-null): a column with no validity
/// bitmap has every row valid; otherwise a set bit (LSB-first) means valid.
fn column_row_valid(col: &Column, i: u32) -> bool {
    match &col.validity {
        None => true,
        Some(bits) => {
            let idx = i as usize;
            bits.get(idx / 8)
                .is_some_and(|byte| byte & (1u8 << (idx % 8)) != 0)
        }
    }
}

/// A row range's null count and the `(min, max)` of its non-null values, or
/// `None` for the range when every row is null.
pub(crate) type NullsAndRange<'a> = (u32, Option<(&'a [u8], &'a [u8])>);

/// Reads row `i` of a [`TypeTag::Bytes`] column body (offset table + payload),
/// bounds-checked.
pub(crate) fn bytes_column_row(data: &[u8], row_count: u32, i: u32) -> Result<&[u8]> {
    let span = bytes_column_span(data, row_count, i)?;
    data.get(span)
        .ok_or(Error::InvalidHeader("columnar: bytes row out of range"))
}

/// Where row `i` of a [`TypeTag::Bytes`] column body sits in `data`,
/// bounds-checked: the span [`bytes_column_row`] reads, for a caller that
/// keeps it to read the row again later.
pub(crate) fn bytes_column_span(
    data: &[u8],
    row_count: u32,
    i: u32,
) -> Result<core::ops::Range<usize>> {
    // Called per row on the read paths, so the errors are built only on the
    // branch that returns them.
    let off_bytes = (row_count as usize + 1) * 4;
    let read_off = |idx: u32| {
        let base = idx as usize * 4;
        data.get(base..)
            .and_then(<[u8]>::first_chunk::<4>)
            .map(|b| u32::from_le_bytes(*b) as usize)
    };
    let (Some(start), Some(end)) = (read_off(i), read_off(i + 1)) else {
        return Err(Error::InvalidHeader("columnar: bytes offset truncated"));
    };
    if off_bytes > data.len() {
        return Err(Error::InvalidHeader("columnar: bytes payload truncated"));
    }
    match (off_bytes.checked_add(start), off_bytes.checked_add(end)) {
        (Some(start), Some(end)) if start <= end && end <= data.len() => Ok(start..end),
        _ => Err(Error::InvalidHeader("columnar: bytes row out of range")),
    }
}

/// Frames `count` variable-width cells as a [`TypeTag::Bytes`] column body: a
/// `(count + 1)`-entry little-endian `u32` offset array followed by the
/// concatenated payload, written once into a buffer of exactly that size.
///
/// `cells` yields the same cells on each call: once to size the buffer, once
/// to fill it, so no intermediate payload is built and copied again.
///
/// # Errors
///
/// Returns [`Error::DecompressedSizeTooLarge`] when the payload does not fit
/// the `u32` offsets, and [`Error::InvalidHeader`] when `cells` yields other
/// than `count` cells or yields differently on its second call.
pub(crate) fn frame_bytes_column<'a, I>(count: usize, cells: impl Fn() -> I) -> Result<Slice>
where
    I: Iterator<Item = &'a [u8]>,
{
    frame_bytes_column_within(count, u64::MAX, cells)
}

/// [`frame_bytes_column`], refused before anything is allocated when the
/// column would take more than `limit` bytes: what a read building a column
/// from an encoding holds it to, since an encoding can describe far more
/// bytes than it stores.
///
/// # Errors
///
/// As [`frame_bytes_column`], and [`Error::DecompressedSizeTooLarge`] for a
/// column past `limit`.
pub(crate) fn frame_bytes_column_within<'a, I>(
    count: usize,
    limit: u64,
    cells: impl Fn() -> I,
) -> Result<Slice>
where
    I: Iterator<Item = &'a [u8]>,
{
    let too_large = |declared: u64| Error::DecompressedSizeTooLarge {
        declared,
        limit: limit.min(u64::from(u32::MAX)),
    };
    let mut total = 0u32;
    let mut sized = 0usize;
    for cell in cells() {
        total = advance_bytes_offset(total, cell.len())?;
        sized += 1;
    }
    let mismatch = || Error::InvalidHeader("columnar: bytes cells changed between passes");
    if sized != count {
        return Err(mismatch());
    }
    let table = count
        .checked_add(1)
        .and_then(|n| n.checked_mul(4))
        .ok_or_else(|| too_large(count as u64))?;
    let len = table
        .checked_add(total as usize)
        .ok_or_else(|| too_large(u64::from(total)))?;
    if len as u64 > limit {
        return Err(too_large(len as u64));
    }
    // SAFETY: the buffer is frozen (and so read) only after the fill below
    // wrote all of it: slot 0, one offset slot per cell for exactly `count`
    // cells, and the payload through exactly `total` bytes. Any other outcome
    // returns early and drops the builder unread. A zeroing builder would add
    // a pass over bytes the fill overwrites anyway: measured 3-6% slower on
    // `columnar/filter_batch` at every keep ratio.
    #[expect(unsafe_code, reason = "see safety")]
    let mut out = unsafe { Slice::builder_unzeroed(len) };
    let (offsets, payload) = out.split_at_mut(table);
    let mut slots = offsets.chunks_exact_mut(4);
    let mut written = 0usize;
    let mut filled = 0usize;
    if let Some(first) = slots.next() {
        first.copy_from_slice(&0u32.to_le_bytes());
    }
    for cell in cells() {
        let end = written + cell.len();
        let (Some(dst), Some(slot)) = (payload.get_mut(written..end), slots.next()) else {
            return Err(mismatch());
        };
        dst.copy_from_slice(cell);
        written = end;
        let offset = u32::try_from(written).map_err(|_| mismatch())?;
        slot.copy_from_slice(&offset.to_le_bytes());
        filled += 1;
    }
    if filled != count || written != total as usize {
        return Err(mismatch());
    }
    Ok(Slice::from(out.freeze()))
}

/// Advances a `Bytes` column's running offset by one cell's length, returning
/// the new offset. A gather may repeat a cell, so a payload can outgrow the
/// column it came from; a saturating offset would then desync the table from
/// the payload (later rows mis-sliced on read), so this returns
/// [`Error::DecompressedSizeTooLarge`] when either the cell's length or the
/// running total would exceed the `u32` offset capacity.
fn advance_bytes_offset(acc: u32, value_len: usize) -> Result<u32> {
    let len = u32::try_from(value_len).map_err(|_| Error::DecompressedSizeTooLarge {
        declared: value_len as u64,
        limit: u64::from(u32::MAX),
    })?;
    acc.checked_add(len)
        .ok_or_else(|| Error::DecompressedSizeTooLarge {
            declared: u64::from(acc) + u64::from(len),
            limit: u64::from(u32::MAX),
        })
}

/// Gathers `count` fixed-width cells of `width` bytes into a column body of
/// exactly `count * width` bytes, written once. A cell `cells` does not supply,
/// or supplies at the wrong width, is zero-filled, so the framing always
/// matches `count` rows.
pub(crate) fn gather_fixed_column<'a>(
    width: usize,
    count: usize,
    mut cells: impl Iterator<Item = Option<&'a [u8]>>,
) -> Slice {
    // A cell count addresses rows of one block and `width` is a `u8`, so the
    // product fits.
    let len = count * width;
    // SAFETY: every `width`-byte chunk of the buffer, and so every byte, is
    // written (copied or zero-filled) before it is frozen and read.
    #[expect(unsafe_code, reason = "see safety")]
    let mut out = unsafe { Slice::builder_unzeroed(len) };
    for dst in out.chunks_exact_mut(width.max(1)) {
        match cells.next() {
            Some(Some(cell)) if cell.len() == dst.len() => dst.copy_from_slice(cell),
            _ => dst.fill(0),
        }
    }
    Slice::from(out.freeze())
}

/// Whether row `row`'s presence bit is set in a validity bitmap (set = valid).
fn validity_bit(bitmap: &[u8], row: u32) -> bool {
    bitmap
        .get((row / 8) as usize)
        .is_some_and(|b| (b >> (row % 8)) & 1 == 1)
}

/// Reads row `i` of a `Fixed(8)` column body as a little-endian `u64`.
pub(crate) fn fixed_u64_row(data: &[u8], i: u32) -> Result<u64> {
    let base = i as usize * 8;
    match data.get(base..).and_then(<[u8]>::first_chunk::<8>) {
        Some(b) => Ok(u64::from_le_bytes(*b)),
        None => Err(Error::InvalidHeader("columnar: fixed8 row truncated")),
    }
}

/// The bytes an entry with a `key_len`-byte user key and a `value_len`-byte
/// value adds across the columns [`entries_to_column_batch`] lays it out in,
/// counted as [`ColumnBatch::row_page_cuts`] counts a row: the key's and the
/// value's lengths plus an offset each, the 8-byte seqno and the 1-byte value
/// type. A writer cuts a row group of entries by it, so its groups and their
/// row pages agree on what a size means.
#[must_use]
pub(crate) const fn entry_row_bytes(key_len: usize, value_len: usize) -> usize {
    key_len + 4 + 8 + 1 + value_len + 4
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
            data: key_data.into(),
        },
        Column {
            column_id: COL_SEQNO,
            type_tag: TypeTag::Number(Number::U64_LE),
            validity: None,
            data: seqno_data.into(),
        },
        Column {
            column_id: COL_VALUE_TYPE,
            type_tag: TypeTag::Fixed(1),
            validity: None,
            data: vt_data.into(),
        },
        Column {
            column_id: COL_VALUE,
            type_tag: TypeTag::Bytes,
            validity: None,
            data: value_data.into(),
        },
    ];
    Ok(ColumnBatch { row_count, columns })
}

/// Reads row `i` of the value-type column: one byte a row, which must name a
/// value type.
fn value_type_row(data: &[u8], i: u32) -> Result<ValueType> {
    let Some(&byte) = data.get(i as usize) else {
        return Err(Error::InvalidHeader("columnar: value-type row truncated"));
    };
    ValueType::try_from(byte).map_err(|()| Error::InvalidTag(("ValueType", byte)))
}

/// Returns row `row`'s `width`-byte cell from a fixed-width column's data.
fn fixed_column_row(data: &[u8], width: u8, row: u32) -> Result<&[u8]> {
    let w = width as usize;
    let Some(end) = (row as usize)
        .checked_add(1)
        .and_then(|rows| rows.checked_mul(w))
    else {
        return Err(Error::InvalidHeader(
            "columnar: fixed column offset overflow",
        ));
    };
    match data.get(end - w..end) {
        Some(cell) => Ok(cell),
        None => Err(Error::InvalidHeader("columnar: fixed column row truncated")),
    }
}

/// Returns row `row`'s cell bytes from `col`, dispatching on the column's type.
fn column_cell(col: &Column, row_count: u32, row: u32) -> Result<&[u8]> {
    match col.type_tag.fixed_width() {
        Some(width) => fixed_column_row(&col.data, width, row),
        None => bytes_column_row(&col.data, row_count, row),
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
///
/// A group that holds rows written as cells gives back each row's whole value
/// or the cell row its fields encode to (see [`cells`]).
fn reconstruct_row_value(
    value_cols: &[Column],
    row_count: u32,
    row: u32,
    value_type: ValueType,
) -> Result<Slice> {
    // The whole-value column's id is the engine's in every table this format
    // reads: an ingested batch may not use it, and nothing else writes caller
    // sub-columns. A table of the previous format that used it as a caller id
    // is renumbered by the converter, never read here.
    if cells::holds_cells(value_cols.iter().map(|c| &c.column_id)) {
        let mut row_cells = Vec::with_capacity(value_cols.len());
        for col in value_cols {
            row_cells.push((
                col.column_id,
                col.type_tag,
                column_value_cell(col, row_count, row)?,
            ));
        }
        return cells::row_value(value_type, &row_cells);
    }
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
    // The seqno column is a typed `u64` and nothing else: the engine reads one
    // format, and an opaque 8-byte seqno from an older layout is rewritten by
    // the offline converter, not accepted here.
    if key_col.column_id != COL_USER_KEY
        || key_col.type_tag != TypeTag::Bytes
        || seqno_col.column_id != COL_SEQNO
        || seqno_col.type_tag != TypeTag::Number(Number::U64_LE)
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
    if cells::holds_cells(seen_value_column_ids.iter()) {
        let shapes: Vec<(u16, TypeTag)> = value_cols
            .iter()
            .map(|c| (c.column_id, c.type_tag))
            .collect();
        cells::check_value_columns(&shapes)?;
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
    let (key_col, seqno_col, vt_col, value_cols) = validate_columnar_columns(batch)?;
    check_ingested_field_ids(value_cols)?;
    // Reject a malformed value-type tag on submit rather than letting it surface
    // only at flush-time decode (`column_batch_to_entries`).
    for &vt_byte in vt_col.data.iter() {
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

/// `entries`, rows decoded from `batch`, laid out again as `batch` lays its
/// rows out: whole values, or a group of rows written as cells. `None` for a
/// batch of caller sub-columns, which rows cannot be split back into.
///
/// # Errors
///
/// As [`entries_to_column_batch`].
pub(crate) fn transpose_like(
    batch: &ColumnBatch,
    entries: &[InternalValue],
) -> Result<Option<ColumnBatch>> {
    if cells::holds_cells(batch.columns.iter().map(|c| &c.column_id)) {
        return entries_to_cells_batch(entries).map(Some);
    }
    if !rows_round_trip(batch) {
        return Ok(None);
    }
    entries_to_column_batch(entries).map(Some)
}

/// Whether the rows of `batch`, once decoded, can be laid out again as the
/// batch lays them out ([`transpose_like`]): every layout but a batch of
/// caller sub-columns.
pub(crate) fn rows_round_trip(batch: &ColumnBatch) -> bool {
    // A batch of the intrinsic columns and one caller sub-column has as many
    // columns as a whole-value batch: the value column itself tells them
    // apart, written whole as a non-null bytes column.
    cells::holds_cells(batch.columns.iter().map(|c| &c.column_id))
        || matches!(
            batch.columns.as_slice(),
            [_, _, _, value]
                if value.column_id == COL_VALUE
                    && value.type_tag == TypeTag::Bytes
                    && value.validity.is_none()
        )
}

/// Refuses a value sub-column of an ingested batch whose id the engine keeps
/// for itself (from [`RESERVED_COLUMNS`](crate::blob_tree::field_row::RESERVED_COLUMNS)
/// on): a group holding one would read as a group of rows written as cells.
///
/// # Errors
///
/// [`Error::InvalidHeader`] naming the fault.
pub(crate) fn check_ingested_field_ids(value_cols: &[Column]) -> Result<()> {
    if value_cols
        .iter()
        .any(|c| c.column_id >= crate::blob_tree::field_row::RESERVED_COLUMNS)
    {
        return Err(Error::InvalidHeader(
            "columnar: a value sub-column id is one the engine keeps for itself",
        ));
    }
    Ok(())
}

/// Reconstructs the entries from an intrinsic columnar batch produced by
/// [`entries_to_column_batch`].
///
/// # Errors
///
/// Returns an error if the batch does not carry exactly the four intrinsic
/// columns in order with the expected type tags, or if a row is truncated or
/// carries an unknown value type.
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
        let value_type = value_type_row(&vt_col.data, i)?;
        let value = reconstruct_row_value(value_cols, batch.row_count, i, value_type)?;
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

/// Whether [`column_batch_into_entries`] can hand each row its value as a view
/// into the column buffer: true for a single non-nullable bytes value column,
/// false when every value must be rebuilt (framed or fixed-width) per row.
fn values_are_views(value_cols: &[Column]) -> bool {
    matches!(
        value_cols,
        [c] if c.type_tag == TypeTag::Bytes && c.validity.is_none()
    )
}

/// Consuming, allocation-light counterpart to [`column_batch_to_entries`] for
/// the scan path.
///
/// The key column and a single non-nullable bytes value column are taken as
/// shared [`Slice`]s, so each row's key / value is a view into one buffer
/// (zero-copy for the Arc-backed large-value case) instead of a per-row copy.
/// Any other value layout (fixed-width, multiple sub-columns, or nullable) falls
/// back to the per-row framing reconstruction, whose rebuilt bytes are added to
/// `rebuilt` row by row, so a batch refused at a later row still counts them.
pub fn column_batch_into_entries(
    batch: ColumnBatch,
    rebuilt: &mut usize,
) -> Result<Vec<InternalValue>> {
    // Structural validation (intrinsic columns + framing) before we consume.
    validate_columnar_columns(&batch)?;
    let row_count = batch.row_count;
    let mut cols = batch.columns.into_iter();
    let (Some(key_col), Some(seqno_col), Some(vt_col)) = (cols.next(), cols.next(), cols.next())
    else {
        return Err(Error::InvalidHeader(
            "columnar: missing an intrinsic column",
        ));
    };
    let value_cols: Vec<Column> = cols.collect();

    // Shared key buffer: every row's key is a view into it.
    let key_data = key_col.data;

    let value_source = if values_are_views(&value_cols) {
        let Some(single) = value_cols.into_iter().next() else {
            return Err(Error::InvalidHeader("columnar: value column vanished"));
        };
        ValueSource::SharedBytes(single.data)
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
        let value_type = value_type_row(&vt_col.data, i)?;
        let value = match &value_source {
            ValueSource::SharedBytes(data) => bytes_row_slice(data, row_count, i)?,
            ValueSource::Reconstruct(cols) => {
                let value = reconstruct_row_value(cols, row_count, i, value_type)?;
                *rebuilt += value.len();
                value
            }
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
///
/// Not charged to `bytes_copied` even when the slice is short enough for
/// [`Slice`] to store it inline: a view is a view whatever its representation,
/// and the inline copy costs no more than building a shared handle.
fn bytes_row_slice(data: &Slice, row_count: u32, i: u32) -> Result<Slice> {
    let bytes: &[u8] = data.as_ref();
    let off_bytes = (row_count as usize + 1) * 4;
    let read_off = |idx: u32| {
        let base = idx as usize * 4;
        bytes
            .get(base..)
            .and_then(<[u8]>::first_chunk::<4>)
            .map(|b| u32::from_le_bytes(*b) as usize)
    };
    let (Some(start), Some(end)) = (read_off(i), read_off(i + 1)) else {
        return Err(Error::InvalidHeader("columnar: bytes offset truncated"));
    };
    let (Some(payload_start), Some(payload_end)) =
        (off_bytes.checked_add(start), off_bytes.checked_add(end))
    else {
        return Err(Error::InvalidHeader(
            "columnar: bytes payload offset overflow",
        ));
    };
    if start > end || payload_end > bytes.len() {
        return Err(Error::InvalidHeader("columnar: bytes row out of range"));
    }
    Ok(data.slice(payload_start..payload_end))
}

/// The rows of a key column of `row_count` rows whose key equals `needle`:
/// one contiguous run, since a group's rows are sorted by user key ascending
/// (seqno descending within a key). Found by binary search for the first row
/// `>= needle`, then extended while the key still equals it. `key_at` reads
/// one row's key, so a lookup reads the rows its search visits and nothing
/// else.
///
/// # Errors
///
/// Whatever `key_at` returns for a row it cannot read.
pub(crate) fn key_rows<'k>(
    row_count: u32,
    needle: &[u8],
    comparator: &crate::comparator::SharedComparator,
    key_at: impl Fn(u32) -> Result<Cell<'k>>,
) -> Result<core::ops::Range<u32>> {
    let mut lo = 0u32;
    let mut hi = row_count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if comparator.compare(&key_at(mid)?, needle) == core::cmp::Ordering::Less {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    let mut end = lo;
    while end < row_count {
        if comparator.compare(&key_at(end)?, needle) != core::cmp::Ordering::Equal {
            break;
        }
        end += 1;
    }
    Ok(lo..end)
}

/// One row page's columns as stored, for a lookup of a few of its rows.
pub(crate) struct RowPageColumns<'a> {
    /// The row page's ordinal in its group.
    pub(crate) ordinal: u16,
    /// The group row it starts at.
    pub(crate) start: u32,
    /// The row page's rows.
    pub(crate) rows: u32,
    /// Its columns, in write order.
    pub(crate) columns: Vec<PageColumn<'a>>,
}

/// The rows `run` of `page`, which the key search found holding `needle`,
/// appended to `out` as entries, reading those rows and decoding nothing
/// else: the columnar point read.
///
/// Their key is `needle` itself: the search matched them by comparator
/// equality, which is byte equality, so `page` carries every column but the
/// key's. A row `deletes` masks is skipped: its position is the second field
/// plus its row.
///
/// Adds to `copied` the key and value bytes of each row as it is copied out,
/// so rows copied before a later row fails are still counted, and charges
/// what preparing the columns for row reads holds decoded to `budget`, the
/// group's.
///
/// # Errors
///
/// [`Error::InvalidHeader`] when the page does not carry the seqno and value
/// type columns in order, carries no value column, holds no row, `run` falls
/// outside it, or a row it reads is malformed; [`Error::InvalidTag`] for an
/// unknown value type; [`Error::DecompressedSizeTooLarge`] past `budget`.
pub(crate) fn page_match_entries(
    page: RowPageColumns<'_>,
    run: core::ops::Range<u32>,
    needle: &[u8],
    deletes: Option<(&crate::table::delete_bitmap::DeleteBitmap, u32)>,
    copied: &mut usize,
    budget: &mut DecodeBudget,
    out: &mut Vec<InternalValue>,
) -> Result<()> {
    let rows = page.rows;
    if rows == 0 {
        // A zero-row page is malformed; fail closed like the scan path rather
        // than returning an empty match the caller reads as an absent key.
        return Err(Error::InvalidHeader(
            "columnar: empty reconstructed data block",
        ));
    }
    let mut columns = page.columns.into_iter();
    let (Some(seqno), Some(vt)) = (columns.next(), columns.next()) else {
        return Err(Error::InvalidHeader(
            "columnar: batch missing the intrinsic columns",
        ));
    };
    if seqno.column_id != COL_SEQNO
        || seqno.type_tag != TypeTag::Number(Number::U64_LE)
        || vt.column_id != COL_VALUE_TYPE
        || vt.type_tag != TypeTag::Fixed(1)
        || seqno.validity.is_some()
        || vt.validity.is_some()
    {
        return Err(Error::InvalidHeader(
            "columnar: unexpected intrinsic column layout",
        ));
    }
    if run.start >= run.end || run.end > rows {
        return Err(Error::InvalidHeader(
            "columnar: key run outside its row page",
        ));
    }
    if needle.is_empty() || needle.len() > u16::MAX as usize {
        return Err(Error::InvalidHeader(
            "columnar: user key is empty or longer than u16::MAX",
        ));
    }

    // The layout was checked above, so the intrinsic columns' types are known.
    let seqno_type = TypeTag::Number(Number::U64_LE);
    let vt_type = TypeTag::Fixed(1);
    let seqnos = budget.rows(seqno.values, seqno_type, rows)?;
    let types = budget.rows(vt.values, vt_type, rows)?;
    // The value sub-columns prepared for row reads in one pass, each id
    // checked against the ones before it.
    let mut values: Vec<(u16, TypeTag, Option<&[u8]>, expr::Rows<'_>)> =
        Vec::with_capacity(columns.len());
    let mut nullable = false;
    for c in columns {
        if c.column_id < COL_VALUE || values.iter().any(|&(id, ..)| id == c.column_id) {
            return Err(Error::InvalidHeader(
                "columnar: value sub-column ids must be unique and must not overlap intrinsic columns",
            ));
        }
        nullable |= c.validity.is_some();
        values.push((
            c.column_id,
            c.type_tag,
            c.validity,
            budget.rows(c.values, c.type_tag, rows)?,
        ));
    }
    if values.is_empty() {
        return Err(Error::InvalidHeader(
            "columnar: batch carries no value column",
        ));
    }
    let holds_cells = cells::holds_cells(values.iter().map(|(id, ..)| id));
    if holds_cells {
        let shapes: Vec<(u16, TypeTag)> = values.iter().map(|&(id, tag, ..)| (id, tag)).collect();
        cells::check_value_columns(&shapes)?;
    }
    // Copied once and shared by every version the run holds.
    let user_key = Slice::from(needle);
    *copied += user_key.len();

    for row in run {
        if let Some((bitmap, start)) = deletes {
            // Fail closed on a corrupt start row: an overflowing position must
            // error like the scan path, never silently expose the row.
            let Some(pos) = start.checked_add(row) else {
                return Err(Error::InvalidHeader(
                    "columnar: row position exceeds u32::MAX",
                ));
            };
            if bitmap.contains(pos) {
                continue;
            }
        }
        let Some(seqno) = seqnos
            .get(seqno_type, rows, row)?
            .first_chunk::<8>()
            .copied()
        else {
            return Err(Error::InvalidHeader("columnar: fixed8 row truncated"));
        };
        let Some(&vt_byte) = types.get(vt_type, rows, row)?.first() else {
            return Err(Error::InvalidHeader("columnar: value-type row truncated"));
        };
        let value_type =
            ValueType::try_from(vt_byte).map_err(|()| Error::InvalidTag(("ValueType", vt_byte)))?;
        let value = if holds_cells {
            let mut row_cells = Vec::with_capacity(values.len());
            for (id, type_tag, validity, access) in &values {
                let cell = if validity.is_none_or(|v| validity_bit(v, row)) {
                    Some(access.get(*type_tag, rows, row)?)
                } else {
                    None
                };
                row_cells.push((*id, *type_tag, cell));
            }
            let borrowed: Vec<(u16, TypeTag, Option<&[u8]>)> = row_cells
                .iter()
                .map(|(id, tag, cell)| (*id, *tag, cell.as_deref()))
                .collect();
            cells::row_value(value_type, &borrowed)?
        } else if let (false, [(_, type_tag, _, access)]) = (nullable, values.as_slice()) {
            // One value column without nulls, the common shape: its cell is
            // the value, with no framing to build.
            Slice::from(&*access.get(*type_tag, rows, row)?)
        } else {
            let cells = values
                .iter()
                .map(|(_, type_tag, validity, access)| {
                    Ok((
                        *type_tag,
                        if validity.is_none_or(|v| validity_bit(v, row)) {
                            Some(access.get(*type_tag, rows, row)?)
                        } else {
                            None
                        },
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            if nullable {
                let framed: Vec<(TypeTag, Option<&[u8]>)> =
                    cells.iter().map(|(t, c)| (*t, c.as_deref())).collect();
                Slice::from(frame_value_cells_nullable(&framed)?)
            } else {
                let framed: Vec<(TypeTag, &[u8])> = cells
                    .iter()
                    .map(|(t, c)| (*t, c.as_deref().unwrap_or_default()))
                    .collect();
                Slice::from(frame_value_cells(&framed)?)
            }
        };
        *copied += value.len();
        out.push(InternalValue {
            key: InternalKey {
                user_key: user_key.clone(),
                seqno: u64::from_le_bytes(seqno),
                value_type,
            },
            value,
        });
    }
    Ok(())
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
            TypeTag::Fixed(width) | TypeTag::Number(Number { width, .. }) => {
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
            TypeTag::Fixed(width) | TypeTag::Number(Number { width, .. }) => {
                let end = pos
                    .checked_add(usize::from(*width))
                    .ok_or_else(|| Error::InvalidHeader("columnar: framed value overflow"))?;
                let cell = blob.get(pos..end).ok_or_else(|| {
                    Error::InvalidHeader("columnar: framed value truncated (fixed)")
                })?;
                out.push(cell);
                pos = end;
            }
            TypeTag::Bytes => {
                let len_end = pos
                    .checked_add(4)
                    .ok_or_else(|| Error::InvalidHeader("columnar: framed value overflow"))?;
                let len_bytes = blob
                    .get(pos..len_end)
                    .and_then(<[u8]>::first_chunk::<4>)
                    .ok_or_else(|| {
                        Error::InvalidHeader("columnar: framed value truncated (length)")
                    })?;
                let len = u32::from_le_bytes(*len_bytes) as usize;
                let end = len_end
                    .checked_add(len)
                    .ok_or_else(|| Error::InvalidHeader("columnar: framed value overflow"))?;
                let cell = blob.get(len_end..end).ok_or_else(|| {
                    Error::InvalidHeader("columnar: framed value truncated (bytes)")
                })?;
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
        let byte = out
            .get_mut(i / 8)
            .ok_or_else(|| Error::InvalidHeader("columnar: presence bitmap index out of range"))?;
        *byte |= 1u8 << (i % 8);
        match tag {
            TypeTag::Fixed(width) | TypeTag::Number(Number { width, .. }) => {
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
    let bitmap = blob.get(0..bitmap_len).ok_or_else(|| {
        Error::InvalidHeader("columnar: nullable framed value truncated (presence bitmap)")
    })?;
    let mut pos = bitmap_len;
    let mut out = Vec::with_capacity(type_tags.len());
    for (i, tag) in type_tags.iter().enumerate() {
        let present = bitmap.get(i / 8).is_some_and(|b| (b >> (i % 8)) & 1 == 1);
        if !present {
            out.push(None);
            continue;
        }
        match tag {
            TypeTag::Fixed(width) | TypeTag::Number(Number { width, .. }) => {
                let end = pos
                    .checked_add(usize::from(*width))
                    .ok_or_else(|| Error::InvalidHeader("columnar: framed value overflow"))?;
                let cell = blob.get(pos..end).ok_or_else(|| {
                    Error::InvalidHeader("columnar: nullable framed value truncated (fixed)")
                })?;
                out.push(Some(cell));
                pos = end;
            }
            TypeTag::Bytes => {
                let len_end = pos
                    .checked_add(4)
                    .ok_or_else(|| Error::InvalidHeader("columnar: framed value overflow"))?;
                let len_bytes = blob
                    .get(pos..len_end)
                    .and_then(<[u8]>::first_chunk::<4>)
                    .ok_or_else(|| {
                        Error::InvalidHeader("columnar: nullable framed value truncated (length)")
                    })?;
                let len = u32::from_le_bytes(*len_bytes) as usize;
                let end = len_end
                    .checked_add(len)
                    .ok_or_else(|| Error::InvalidHeader("columnar: framed value overflow"))?;
                let cell = blob.get(len_end..end).ok_or_else(|| {
                    Error::InvalidHeader("columnar: nullable framed value truncated (bytes)")
                })?;
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
        let Some(&byte) = validity.get((row / 8) as usize) else {
            return Err(Error::InvalidHeader(
                "columnar: validity bitmap shorter than row count",
            ));
        };
        if (byte >> (row % 8)) & 1 == 0 {
            return Ok(None); // null row
        }
    }
    Ok(Some(column_cell(col, row_count, row)?))
}

#[expect(clippy::expect_used, clippy::indexing_slicing, reason = "test code")]
#[cfg(test)]
mod tests;

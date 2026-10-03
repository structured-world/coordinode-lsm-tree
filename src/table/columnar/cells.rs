// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The columns of a row group of a tree whose rows may be written as cells.
//!
//! A row written as cells ([`crate::BlobTree::insert_cells`]) is split into
//! its fields, each stored in the column its id names under its own type, so a
//! projection of a compact field reads that column alone and never the rest of
//! the row. A field whose bytes sit in a blob file is a reference, not a
//! value of its type, so its cell in the field's column is null and the
//! reference goes to the references column, beside the row's other
//! references. Every other row, and a cell row whose fields do not fit the
//! group's columns, keeps its value whole in the whole-value column, null for
//! a split row.
//!
//! The group gives back each row exactly as it was written: a split row's
//! fields and references, merged in column order, encode to the bytes of the
//! row, since a stored row keeps its fields in that order.
//!
//! # References column
//!
//! A [`TypeTag::Bytes`] column whose cell lists a split row's references in
//! column order, each as its little-endian `u16` column id, its type's wire
//! tag and width byte, a byte that is `1` when the row owns the object, and
//! the encoded blob indirection. A row without a reference is null.

use alloc::collections::BTreeMap;
use alloc::vec::Vec;

use super::{
    COL_SEQNO, COL_USER_KEY, COL_VALUE_TYPE, Column, ColumnBatch, Number, TypeTag,
    build_bytes_column, gather_fixed_column,
};
use crate::blob_tree::field_row::{
    CELL_REFS_COLUMN, RowCell, RowField, WHOLE_VALUE_COLUMN, decode_row, encode_row,
    is_field_column,
};
use crate::blob_tree::handle::BlobIndirection;
use crate::coding::{Decode, Encode};
use crate::{Error, Result, Slice, ValueType, value::InternalValue};

const CORRUPT: Error = Error::InvalidHeader("columnar: malformed cell-row columns");

/// How a row of the group is stored.
enum Stored<'a> {
    /// Its value whole, in the whole-value column.
    Whole(&'a [u8]),
    /// Its fields in their columns, in column order.
    Split(Vec<RowField<'a>>),
}

/// Whether the value columns of a group hold rows written as cells: the
/// whole-value column is there, which no other layout writes.
pub fn holds_cells<'a>(mut ids: impl Iterator<Item = &'a u16>) -> bool {
    ids.any(|&id| id == WHOLE_VALUE_COLUMN)
}

/// Lays `entries` out as a group of a tree whose rows may be written as
/// cells: the intrinsic columns, one column per field id the split rows use,
/// the references column when a split row holds one, and the whole-value
/// column.
///
/// A cell row is split unless it does not decode or a field of it holds a
/// value under another type than the group's column of that id already
/// does; it is then kept whole, as it was written.
///
/// # Errors
///
/// Returns an error if a column's bytes or the row count exceed the `u32`
/// wire limits.
pub fn entries_to_cells_batch(entries: &[InternalValue]) -> Result<ColumnBatch> {
    let row_count = u32::try_from(entries.len())
        .map_err(|_| Error::InvalidHeader("columnar: row count exceeds u32"))?;
    let count = entries.len();

    // Each field id's type, from the first split row that holds a value in it.
    let mut tags: BTreeMap<u16, TypeTag> = BTreeMap::new();
    let mut rows = Vec::with_capacity(count);
    for entry in entries {
        let split = (entry.key.value_type == ValueType::CellRow)
            .then(|| decode_row(&entry.value).ok())
            .flatten()
            .filter(|fields| fits(fields, &tags));
        rows.push(match split {
            Some(fields) => {
                for field in &fields {
                    if let RowCell::Value(_) = field.cell {
                        tags.entry(field.column).or_insert(field.tag);
                    }
                }
                Stored::Split(fields)
            }
            None => Stored::Whole(&entry.value),
        });
    }

    let mut seqnos = Vec::with_capacity(count * 8);
    let mut types = Vec::with_capacity(count);
    for entry in entries {
        seqnos.extend_from_slice(&entry.key.seqno.to_le_bytes());
        types.push(u8::from(entry.key.value_type));
    }
    let mut columns = alloc::vec![
        Column {
            column_id: COL_USER_KEY,
            type_tag: TypeTag::Bytes,
            validity: None,
            data: build_bytes_column(entries.iter().map(|e| e.key.user_key.as_ref()))?.into(),
        },
        Column {
            column_id: COL_SEQNO,
            type_tag: TypeTag::Number(Number::U64_LE),
            validity: None,
            data: seqnos.into(),
        },
        Column {
            column_id: COL_VALUE_TYPE,
            type_tag: TypeTag::Fixed(1),
            validity: None,
            data: types.into(),
        },
    ];

    for (&column_id, &type_tag) in &tags {
        let cells: Vec<Option<&[u8]>> = rows
            .iter()
            .map(|row| match row {
                Stored::Split(fields) => value_in(fields, column_id),
                Stored::Whole(_) => None,
            })
            .collect();
        columns.push(nullable_column(column_id, type_tag, &cells)?);
    }

    let mut refs: Vec<Option<Vec<u8>>> = Vec::with_capacity(count);
    for row in &rows {
        refs.push(match row {
            Stored::Split(fields) => encode_refs(fields)?,
            Stored::Whole(_) => None,
        });
    }
    if refs.iter().any(Option::is_some) {
        let cells: Vec<Option<&[u8]>> = refs.iter().map(Option::as_deref).collect();
        columns.push(nullable_column(CELL_REFS_COLUMN, TypeTag::Bytes, &cells)?);
    }

    let whole: Vec<Option<&[u8]>> = rows
        .iter()
        .map(|row| match row {
            Stored::Whole(value) => Some(*value),
            Stored::Split(_) => None,
        })
        .collect();
    columns.push(nullable_column(WHOLE_VALUE_COLUMN, TypeTag::Bytes, &whole)?);

    Ok(ColumnBatch { row_count, columns })
}

/// Whether every value of `fields` has the type the group's column of its id
/// holds, where the group has one, and that type's width: a fixed-width cell
/// of another width would not come back as it was.
fn fits(fields: &[RowField<'_>], tags: &BTreeMap<u16, TypeTag>) -> bool {
    fields.iter().all(|field| match field.cell {
        RowCell::Value(bytes) => {
            tags.get(&field.column).is_none_or(|&tag| tag == field.tag)
                && field
                    .tag
                    .fixed_width()
                    .is_none_or(|width| bytes.len() == usize::from(width))
        }
        RowCell::Ref { .. } => true,
    })
}

/// The value `fields`, in column order, holds in `column`, if it holds one
/// there rather than a reference or nothing.
fn value_in<'a>(fields: &[RowField<'a>], column: u16) -> Option<&'a [u8]> {
    let at = fields
        .binary_search_by_key(&column, |field| field.column)
        .ok()?;
    match fields.get(at)?.cell {
        RowCell::Value(bytes) => Some(bytes),
        RowCell::Ref { .. } => None,
    }
}

/// A column of `cells`, a missing cell null: zero bytes in a fixed-width
/// column, an empty cell in a bytes column.
fn nullable_column(column_id: u16, type_tag: TypeTag, cells: &[Option<&[u8]>]) -> Result<Column> {
    let count = cells.len();
    let validity = cells.iter().any(Option::is_none).then(|| {
        let mut bits = alloc::vec![0u8; count.div_ceil(8)];
        for (row, cell) in cells.iter().enumerate() {
            if cell.is_some()
                && let Some(byte) = bits.get_mut(row / 8)
            {
                *byte |= 1 << (row % 8);
            }
        }
        bits
    });
    let data = match type_tag.fixed_width() {
        Some(width) => gather_fixed_column(usize::from(width), count, cells.iter().copied()),
        None => build_bytes_column(cells.iter().map(|cell| cell.unwrap_or_default()))?.into(),
    };
    Ok(Column {
        column_id,
        type_tag,
        validity,
        data,
    })
}

/// The references cell of a split row, `None` when it holds no reference.
fn encode_refs(fields: &[RowField<'_>]) -> Result<Option<Vec<u8>>> {
    let mut out = Vec::new();
    for field in fields {
        if let RowCell::Ref { indirection, owner } = field.cell {
            let (tag, width) = field.tag.to_wire();
            out.extend_from_slice(&field.column.to_le_bytes());
            out.push(tag);
            out.push(width);
            out.push(u8::from(owner));
            indirection.encode_into(&mut out)?;
        }
    }
    Ok((!out.is_empty()).then_some(out))
}

/// The references a references cell lists, in column order.
///
/// # Errors
///
/// Returns an error for a cell that does not decode as a list of
/// references (see the module documentation).
pub fn cell_refs(cell: &[u8]) -> Result<Vec<RowField<'static>>> {
    let mut fields = Vec::new();
    decode_refs(cell, &mut fields)?;
    Ok(fields)
}

/// Appends the references a references cell lists to `fields`.
fn decode_refs(mut cell: &[u8], fields: &mut Vec<RowField<'_>>) -> Result<()> {
    while !cell.is_empty() {
        let Some((&[c0, c1, tag, width, owner], rest)) = cell.split_first_chunk::<5>() else {
            return Err(CORRUPT);
        };
        let owner = match owner {
            0 => false,
            1 => true,
            _ => return Err(CORRUPT),
        };
        let column = u16::from_le_bytes([c0, c1]);
        if !is_field_column(column) {
            return Err(CORRUPT);
        }
        let mut reader = rest;
        let indirection = BlobIndirection::decode_from(&mut reader)?;
        fields.push(RowField {
            column,
            tag: TypeTag::from_wire(tag, width).map_err(|_| CORRUPT)?,
            cell: RowCell::Ref { indirection, owner },
        });
        cell = reader;
    }
    Ok(())
}

/// The value of one row of a group that holds cell rows, from the row's cell
/// in each of the group's value columns (`None` for a null cell): its whole
/// value, or the cell row its fields and references encode to.
///
/// # Errors
///
/// Returns an error for a row that is not one the group could have stored:
/// a whole value beside fields, a split row that is not a cell row, a column
/// that is neither a field id nor a column the group keeps, a reference that
/// does not decode, or one column held twice.
pub fn row_value(value_type: ValueType, cells: &[(u16, TypeTag, Option<&[u8]>)]) -> Result<Slice> {
    let mut whole = None;
    let mut fields = Vec::new();
    for &(column_id, type_tag, cell) in cells {
        let Some(bytes) = cell else {
            continue;
        };
        match column_id {
            WHOLE_VALUE_COLUMN => whole = Some(bytes),
            CELL_REFS_COLUMN => decode_refs(bytes, &mut fields)?,
            column if is_field_column(column) => fields.push(RowField {
                column,
                tag: type_tag,
                cell: RowCell::Value(bytes),
            }),
            _ => return Err(CORRUPT),
        }
    }
    match whole {
        Some(value) if fields.is_empty() => Ok(Slice::from(value)),
        None if value_type == ValueType::CellRow => {
            fields.sort_by_key(|field| field.column);
            // A column held both as a value and as a reference, or listed
            // twice: no stored row has that.
            if fields
                .windows(2)
                .any(|pair| matches!(pair, [a, b] if a.column == b.column))
            {
                return Err(CORRUPT);
            }
            Ok(Slice::from(encode_row(&fields)?))
        }
        // A whole value beside fields, or a split row that is no cell row.
        _ => Err(CORRUPT),
    }
}

/// Checks the value columns of a group that holds cell rows: the whole-value
/// and references columns are byte columns, and every other one is a field
/// id's.
///
/// # Errors
///
/// [`Error::InvalidHeader`] for any other column.
pub fn check_value_columns(columns: &[(u16, TypeTag)]) -> Result<()> {
    for &(column_id, type_tag) in columns {
        let fine = match column_id {
            WHOLE_VALUE_COLUMN | CELL_REFS_COLUMN => type_tag == TypeTag::Bytes,
            column => is_field_column(column),
        };
        if !fine {
            return Err(CORRUPT);
        }
    }
    Ok(())
}

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test code"
)]
mod tests;

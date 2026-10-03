// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The projected fields of a blob tree's rows, read for the rows a scan
//! returns.
//!
//! A blob tree keeps a row whole (a plain value, or an indirection to one in
//! a blob file) or as a cell row, whose fields sit inline or, one by one, in
//! blob files; a columnar table splits a cell row into the columns of its
//! fields and lists its references beside them. The scan decides its rows on
//! their keys, seqnos and value types alone. Only then are the projected
//! fields of the rows it returns read here, and a blob object is read only
//! for a field a returned row holds by reference and the projection names,
//! or for a whole value kept in a blob file whose fields the projection
//! names. A field the projection does not name is never read.
//!
//! When the scan's predicate runs on a projected field, that field is read
//! first and the predicate drops the rows it rejects; the objects of the
//! other fields are read after, for the rows it kept. Each round of objects
//! is read in blob file and offset order, then put in place.

use alloc::vec::Vec;

use super::projection::{self, ProjectedField, ProjectedRow};
use super::{ColumnarScan, MISSING_BATCH_COLUMN, merge::COL_WHOLE_VALUE};
use crate::blob_tree::field_row::{CELL_REFS_COLUMN, RowCell, RowField, decode_row};
use crate::blob_tree::handle::BlobIndirection;
use crate::coding::Decode;
use crate::table::columnar::{
    COL_USER_KEY, COL_VALUE_TYPE, Column, ColumnBatch, bytes_column_row, cell_refs,
    frame_bytes_column,
};
use crate::table::columnar_predicate::{PredicateApply, take_column};
use crate::{Error, Slice, ValueType};

/// Where the objects of a blob tree's rows are read: the version the scan
/// reads, which keeps every object its rows hold readable.
pub(super) struct CellSource {
    pub(super) version: crate::version::SuperVersion,
    pub(super) source: crate::blob_tree::BlobSource,
}

/// An object one row needs: a field's, or its whole value's.
struct Fetch {
    row: usize,
    /// The declared field the object is the value of; `None` for a whole
    /// value, whose fields the projector reads.
    field: Option<usize>,
    indirection: BlobIndirection,
}

/// The rows of a decided batch as their fields are read.
struct Rows {
    keys: Vec<Slice>,
    /// Each row's cell of each declared field, by field then row.
    cells: Vec<Vec<Option<Slice>>>,
    /// Whole values read out through the projector: `(row, value)`.
    whole: Vec<(usize, Slice)>,
}

/// `batch`, the rows a scan of a blob tree returns, with each declared field
/// read: out of a cell row's own cells or the objects it references, out of
/// a whole value through the projector, a whole value kept in a blob file
/// read first. Rows the scan's predicate rejects on a declared field are
/// dropped before any object of another field is read. The row's value type
/// reads as a value, the form every read gives it, and the columns the scan
/// carried the values in leave the batch.
///
/// # Errors
///
/// Returns [`projection::MISTYPED`] for a field stored under another type
/// than declared, [`projection::UNREADABLE`] for a whole value with declared
/// fields and no projector, and an error for a row that does not decode or
/// an object that cannot be read or is not the size its reference records.
pub(super) fn materialize(
    scan: &ColumnarScan,
    cells: &CellSource,
    batch: ColumnBatch,
) -> crate::Result<ColumnBatch> {
    let declared: Vec<ProjectedField> = scan
        .fields
        .iter()
        .filter(|f| projection::is_declared(f))
        .cloned()
        .collect();
    // The predicate's field, when it is a declared one: read before the rest.
    let judged = scan
        .predicate
        .as_ref()
        .filter(|p| p.apply == PredicateApply::Filter)
        .and_then(|p| declared.iter().position(|f| f.column_id() == p.column_id));

    let row_count = batch.row_count as usize;
    let find = |id: u16| batch.columns.iter().find(|c| c.column_id == id);
    let (Some(keys), Some(types)) = (find(COL_USER_KEY), find(COL_VALUE_TYPE)) else {
        return Err(MISSING_BATCH_COLUMN);
    };
    let whole_col = find(COL_WHOLE_VALUE);
    let refs_col = find(CELL_REFS_COLUMN);

    let mut rows = Rows {
        keys: Vec::with_capacity(row_count),
        cells: Vec::with_capacity(declared.len()),
        whole: Vec::new(),
    };
    for field in &declared {
        let column = find(field.column_id());
        let mut cells = Vec::with_capacity(row_count);
        for row in 0..batch.row_count {
            cells.push(match column {
                Some(column) if column.is_valid(row) => {
                    Some(Slice::from(cell_of(column, batch.row_count, row)?))
                }
                _ => None,
            });
        }
        rows.cells.push(cells);
    }

    let mut now: Vec<Fetch> = Vec::new();
    let mut later: Vec<Fetch> = Vec::new();
    let mut out_types: Vec<u8> = Vec::with_capacity(row_count);
    for row in 0..batch.row_count {
        let at = row as usize;
        rows.keys.push(Slice::from(bytes_column_row(
            &keys.data,
            batch.row_count,
            row,
        )?));
        let byte = *types.data.get(at).ok_or(MISSING_BATCH_COLUMN)?;
        let value_type =
            ValueType::try_from(byte).map_err(|()| Error::InvalidTag(("ValueType", byte)))?;
        let whole = match whole_col {
            Some(column) if column.is_valid(row) => {
                Some(bytes_column_row(&column.data, batch.row_count, row)?)
            }
            _ => None,
        };
        // A field the row holds by reference: read now when the predicate
        // judges it, after the predicate otherwise.
        let mut reference = |field: usize, indirection: BlobIndirection| {
            let fetch = Fetch {
                row: at,
                field: Some(field),
                indirection,
            };
            if judged == Some(field) {
                now.push(fetch);
            } else {
                later.push(fetch);
            }
        };
        match (value_type, whole) {
            (ValueType::CellRow, Some(stored)) => {
                let fields = decode_row(stored)?;
                for (index, field) in declared.iter().enumerate() {
                    let held = fields.iter().find(|f| f.column == field.column_id());
                    let slot = rows
                        .cells
                        .get_mut(index)
                        .and_then(|cells| cells.get_mut(at))
                        .ok_or(MISSING_BATCH_COLUMN)?;
                    *slot = None;
                    let Some(held) = held else {
                        continue;
                    };
                    check_type(field, held)?;
                    match held.cell {
                        RowCell::Value(bytes) => *slot = Some(Slice::from(bytes)),
                        RowCell::Ref { indirection, .. } => reference(index, indirection),
                    }
                }
                out_types.push(u8::from(ValueType::Value));
            }
            (ValueType::CellRow, None) => {
                // A row a columnar table split: its values are in their
                // columns, its references listed beside them.
                if let Some(column) = refs_col.filter(|c| c.is_valid(row)) {
                    let listed = bytes_column_row(&column.data, batch.row_count, row)?;
                    for held in cell_refs(listed)? {
                        let Some(index) =
                            declared.iter().position(|f| f.column_id() == held.column)
                        else {
                            continue;
                        };
                        if let Some(field) = declared.get(index) {
                            check_type(field, &held)?;
                        }
                        if let RowCell::Ref { indirection, .. } = held.cell {
                            reference(index, indirection);
                        }
                    }
                }
                out_types.push(u8::from(ValueType::Value));
            }
            (ValueType::Indirection, Some(stored)) => {
                let mut reader = stored;
                let indirection = BlobIndirection::decode_from(&mut reader)?;
                // A predicate on a declared field reads it out of the value,
                // which has to be read for that.
                let fetch = Fetch {
                    row: at,
                    field: None,
                    indirection,
                };
                if judged.is_some() {
                    now.push(fetch);
                } else {
                    later.push(fetch);
                }
                out_types.push(u8::from(ValueType::Value));
            }
            (_, Some(value)) => {
                rows.whole.push((at, Slice::from(value)));
                out_types.push(byte);
            }
            (_, None) => out_types.push(byte),
        }
    }

    read_objects(cells, &mut rows, now)?;
    project(scan, &declared, &mut rows)?;

    // The predicate drops what it rejects before the rest is read.
    let mut kept: Option<Vec<u32>> = None;
    if let (Some(index), Some(predicate)) = (judged, scan.predicate.as_ref())
        && let Some(field) = declared.get(index)
    {
        let probe = ColumnBatch {
            row_count: batch.row_count,
            columns: alloc::vec![built_column(
                field,
                rows.cells.get(index).ok_or(MISSING_BATCH_COLUMN)?
            )?],
        };
        let (probe, _) = projection::conform_lenient(
            probe,
            core::slice::from_ref(field),
            Some(field.column_id()),
        )?;
        let matching = predicate.matching_rows(&probe);
        if matching.iter().any(|keep| !keep) {
            let indices: Vec<u32> = (0..batch.row_count)
                .zip(&matching)
                .filter(|&(_, &keep)| keep)
                .map(|(row, _)| row)
                .collect();
            later.retain(|fetch| matching.get(fetch.row).copied().unwrap_or(false));
            kept = Some(indices);
        }
    }

    read_objects(cells, &mut rows, later)?;
    project(scan, &declared, &mut rows)?;

    build(batch, &declared, &rows, &out_types, kept.as_deref())
}

/// Refuses a field stored under another type than its declaration.
fn check_type(field: &ProjectedField, held: &RowField<'_>) -> crate::Result<()> {
    if field.type_tag() == Some(held.tag) {
        Ok(())
    } else {
        Err(projection::MISTYPED)
    }
}

/// Reads `fetches`, in blob file and offset order, into the rows: a field's
/// object into its cell, a whole value into the values the projector reads.
fn read_objects(cells: &CellSource, rows: &mut Rows, mut fetches: Vec<Fetch>) -> crate::Result<()> {
    fetches.sort_by_key(|fetch| {
        (
            fetch.indirection.vhandle.blob_file_id,
            fetch.indirection.vhandle.offset,
        )
    });
    // Objects close together in one file are read in one request first;
    // each is then taken from the cache.
    if fetches.len() > 1 {
        let mut items = Vec::with_capacity(fetches.len());
        for fetch in &fetches {
            let key = rows.keys.get(fetch.row).ok_or(MISSING_BATCH_COLUMN)?;
            items.push((&**key, fetch.indirection.vhandle, 0));
        }
        cells.source.prefetch(&cells.version.version, &mut items);
    }
    for fetch in fetches {
        let key = rows.keys.get(fetch.row).ok_or(MISSING_BATCH_COLUMN)?;
        let object = cells
            .source
            .object(&cells.version.version, key, &fetch.indirection)?;
        if object.len() != fetch.indirection.size as usize {
            return Err(Error::InvalidHeader(
                "columnar_scan: a referenced object differs from its recorded size",
            ));
        }
        match fetch.field {
            Some(index) => {
                let slot = rows
                    .cells
                    .get_mut(index)
                    .and_then(|cells| cells.get_mut(fetch.row))
                    .ok_or(MISSING_BATCH_COLUMN)?;
                *slot = Some(object);
            }
            None => rows.whole.push((fetch.row, object)),
        }
    }
    Ok(())
}

/// Reads the declared fields out of the whole values waiting for the
/// projector, and clears them.
fn project(scan: &ColumnarScan, declared: &[ProjectedField], rows: &mut Rows) -> crate::Result<()> {
    if rows.whole.is_empty() {
        return Ok(());
    }
    if declared.is_empty() {
        rows.whole.clear();
        return Ok(());
    }
    let projector = scan.projector.as_deref().ok_or(projection::UNREADABLE)?;
    let mut out: Vec<Option<Vec<u8>>> = alloc::vec![None; declared.len()];
    for (row, value) in core::mem::take(&mut rows.whole) {
        out.fill(None);
        let key = rows.keys.get(row).ok_or(MISSING_BATCH_COLUMN)?;
        projector.project(key, &value, &mut ProjectedRow::new(declared, &mut out))?;
        for (index, cell) in out.iter_mut().enumerate() {
            let slot = rows
                .cells
                .get_mut(index)
                .and_then(|cells| cells.get_mut(row))
                .ok_or(MISSING_BATCH_COLUMN)?;
            *slot = cell.take().map(Slice::from);
        }
    }
    Ok(())
}

/// Row `row` of `column`, of `rows` rows, as stored.
fn cell_of(column: &Column, rows: u32, row: u32) -> crate::Result<&[u8]> {
    match column.type_tag.fixed_width() {
        Some(width) => {
            let width = usize::from(width);
            let start = row as usize * width;
            column
                .data
                .get(start..start + width)
                .ok_or(MISSING_BATCH_COLUMN)
        }
        None => bytes_column_row(&column.data, rows, row),
    }
}

/// The column of `field` holding `cells` under its declared type, a missing
/// cell null.
fn built_column(field: &ProjectedField, cells: &[Option<Slice>]) -> crate::Result<Column> {
    let type_tag = field.type_tag().ok_or(projection::MISTYPED)?;
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
        Some(width) => {
            let width = usize::from(width);
            let mut data = alloc::vec![0u8; count * width];
            for (slot, cell) in data.chunks_exact_mut(width).zip(cells) {
                if let Some(cell) = cell {
                    // A cell of another width is a field of another type.
                    if cell.len() != width {
                        return Err(projection::MISTYPED);
                    }
                    slot.copy_from_slice(cell);
                }
            }
            Slice::from(data)
        }
        None => frame_bytes_column(count, || cells.iter().map(|c| c.as_deref().unwrap_or(&[])))?,
    };
    Ok(Column {
        column_id: field.column_id(),
        type_tag,
        validity,
        data,
    })
}

/// The output batch: `batch`'s columns, the rows `kept` names when the
/// predicate dropped some, each declared field's column built from the rows'
/// cells, the value types as read, and without the columns the values were
/// carried in.
fn build(
    batch: ColumnBatch,
    declared: &[ProjectedField],
    rows: &Rows,
    types: &[u8],
    kept: Option<&[u32]>,
) -> crate::Result<ColumnBatch> {
    let row_count = batch.row_count as usize;
    let pick = |cells: &[Option<Slice>]| -> Vec<Option<Slice>> {
        match kept {
            Some(kept) => kept
                .iter()
                .map(|&row| cells.get(row as usize).cloned().flatten())
                .collect(),
            None => cells.to_vec(),
        }
    };
    let mut columns = Vec::with_capacity(batch.columns.len());
    for column in batch.columns {
        match column.column_id {
            COL_WHOLE_VALUE | CELL_REFS_COLUMN => {}
            COL_VALUE_TYPE => {
                let data: Vec<u8> = match kept {
                    Some(kept) => kept
                        .iter()
                        .map(|&row| types.get(row as usize).copied().ok_or(MISSING_BATCH_COLUMN))
                        .collect::<crate::Result<_>>()?,
                    None => types.to_vec(),
                };
                columns.push(Column {
                    data: Slice::from(data),
                    validity: None,
                    ..column
                });
            }
            id => match declared.iter().position(|f| f.column_id() == id) {
                Some(index) => {
                    let field = declared.get(index).ok_or(MISSING_BATCH_COLUMN)?;
                    let cells = rows.cells.get(index).ok_or(MISSING_BATCH_COLUMN)?;
                    columns.push(built_column(field, &pick(cells))?);
                }
                None => columns.push(match kept {
                    Some(kept) => take_column(&column, row_count, kept)?,
                    None => column,
                }),
            },
        }
    }
    // A declared field no column carried yet, as a row source's batch lacks.
    for (index, field) in declared.iter().enumerate() {
        if !columns.iter().any(|c| c.column_id == field.column_id()) {
            let cells = rows.cells.get(index).ok_or(MISSING_BATCH_COLUMN)?;
            columns.push(built_column(field, &pick(cells))?);
        }
    }
    let row_count = kept.map_or(row_count, <[u32]>::len);
    Ok(ColumnBatch {
        // A subset of the batch's rows, whose count is a u32.
        row_count: u32::try_from(row_count).unwrap_or(batch.row_count),
        columns,
    })
}

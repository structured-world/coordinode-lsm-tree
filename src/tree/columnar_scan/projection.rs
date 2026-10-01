// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! What a projected columnar scan returns, and what a field a row does not
//! have reads as.
//!
//! Sources disagree about which fields a row has: an older segment predates a
//! column, a row value a [`ValueProjector`] reads may not carry one. Each
//! projected field therefore declares what its absence means
//! ([`Absent`]), and the scan applies that declaration the same way to every
//! source, so one query over the same data returns the same rows wherever
//! they are stored. A null cell counts as absent: a row either has a value for
//! a field or it does not.
//!
//! Absence never means "take the field from an older version". Filling a
//! field from history is partial-update semantics, which a caller asks for
//! through a merge operator.

use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::table::columnar::{
    COL_SEQNO, COL_USER_KEY, COL_VALUE_TYPE, Column, ColumnBatch, Number, TypeTag,
    bytes_column_row, frame_bytes_column,
};
use crate::{Error, Slice};

/// What a projected field reads as in a row that does not have it: a segment
/// written without the column, a null cell, or a row value the
/// [`ValueProjector`] finds no such field in.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Absent {
    /// The cell is null: the column carries a validity bitmap and the row's
    /// bit is clear.
    Null,
    /// The cell reads as this value, which must fit the field's type.
    Default(Slice),
    /// The scan fails, naming the field.
    Error,
}

/// One field of a [`Projection`]: its column id, its physical type and what
/// its absence means.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectedField {
    column_id: u16,
    /// The declared type, or `None` for a field projected by id alone, whose
    /// type is whatever the segments store.
    type_tag: Option<TypeTag>,
    absent: Absent,
}

impl ProjectedField {
    /// A field with a declared type and absence.
    ///
    /// # Errors
    ///
    /// Returns an error if `absent` is a default that does not fit `type_tag`
    /// (a fixed-width type takes exactly its width), if `type_tag` is a fixed
    /// width of zero, which no column may carry, or if `column_id` names an
    /// intrinsic column, whose type and presence are fixed.
    pub fn new(column_id: u16, type_tag: TypeTag, absent: Absent) -> crate::Result<Self> {
        if intrinsic_type(column_id).is_some() {
            return Err(Error::Projection(
                "projection: an intrinsic column is projected by id, not declared",
            ));
        }
        if type_tag.fixed_width() == Some(0) {
            return Err(Error::Projection(
                "projection: a fixed-width field has a width of zero",
            ));
        }
        if let (Absent::Default(value), Some(width)) = (&absent, type_tag.fixed_width())
            && value.len() != usize::from(width)
        {
            return Err(Error::Projection(
                "projection: a default does not fit its field's width",
            ));
        }
        Ok(Self {
            column_id,
            type_tag: Some(type_tag),
            absent,
        })
    }

    /// A field projected by id alone: an intrinsic column, or a value column
    /// whose type is whatever the segments store. Its null cells stay null; a
    /// segment written without the column is an error, since nothing declares
    /// the type a stand-in column would need.
    #[must_use]
    pub fn by_id(column_id: u16) -> Self {
        Self {
            column_id,
            type_tag: intrinsic_type(column_id),
            absent: Absent::Null,
        }
    }

    /// The field's column id.
    #[must_use]
    pub fn column_id(&self) -> u16 {
        self.column_id
    }

    /// The field's declared type, or `None` when it was projected by id alone.
    #[must_use]
    pub fn type_tag(&self) -> Option<TypeTag> {
        self.type_tag
    }

    /// What the field reads as where a row does not have it.
    #[must_use]
    pub fn absent(&self) -> &Absent {
        &self.absent
    }
}

/// The type of an intrinsic column, or `None` for a value column.
fn intrinsic_type(column_id: u16) -> Option<TypeTag> {
    match column_id {
        COL_USER_KEY => Some(TypeTag::Bytes),
        COL_SEQNO => Some(TypeTag::Number(Number::U64_LE)),
        COL_VALUE_TYPE => Some(TypeTag::Fixed(1)),
        _ => None,
    }
}

/// Reads the projected fields out of a row value the engine cannot interpret:
/// a value in a memtable or a row-oriented table, or the result of a merge.
///
/// The engine does not guess an encoding; the caller that wrote the values
/// knows it. It is called only for the rows the scan returns: never for a
/// version a newer one shadows, a deleted one or one newer than the snapshot.
pub trait ValueProjector: Send + Sync {
    /// Writes into `row` the cell of each field the row value `value` of `key`
    /// has. A field left unset is absent from the row and reads as its
    /// declaration says.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is not a value this projector reads; the
    /// scan fails with it.
    fn project(&self, key: &[u8], value: &[u8], row: &mut ProjectedRow<'_>) -> crate::Result<()>;
}

/// The cells a [`ValueProjector`] writes for one row.
pub struct ProjectedRow<'a> {
    fields: &'a [ProjectedField],
    cells: &'a mut [Option<Vec<u8>>],
}

impl<'a> ProjectedRow<'a> {
    /// A row of `fields`, whose cells are written into `cells`, one per field.
    pub(crate) fn new(fields: &'a [ProjectedField], cells: &'a mut [Option<Vec<u8>>]) -> Self {
        Self { fields, cells }
    }

    /// The fields to write, in the order `set` indexes them.
    #[must_use]
    pub fn fields(&self) -> &[ProjectedField] {
        self.fields
    }

    /// Sets field `index` of the row to `cell`.
    ///
    /// # Errors
    ///
    /// Returns an error if `index` names no field, if the field was projected
    /// by id alone (a row value has no type the engine could check it
    /// against), or if `cell` does not fit the field's fixed width.
    pub fn set(&mut self, index: usize, cell: &[u8]) -> crate::Result<()> {
        let field = self.fields.get(index).ok_or(Error::Projection(
            "projection: the projector set a field the projection does not have",
        ))?;
        let type_tag = field.type_tag.ok_or(Error::Projection(
            "projection: a row value is projected into a field with no declared type",
        ))?;
        if let Some(width) = type_tag.fixed_width()
            && cell.len() != usize::from(width)
        {
            return Err(Error::Projection(
                "projection: the projector wrote a cell of the wrong width",
            ));
        }
        let slot = self.cells.get_mut(index).ok_or(Error::Projection(
            "projection: the projector set a field the projection does not have",
        ))?;
        let buf = slot.get_or_insert_with(Vec::new);
        buf.clear();
        buf.extend_from_slice(cell);
        Ok(())
    }
}

/// The columns a projected scan returns: intrinsic columns and value fields,
/// in the order the scan yields them, and the projector that reads the fields
/// out of row values.
#[derive(Clone, Default)]
pub struct Projection {
    fields: Vec<ProjectedField>,
    projector: Option<Arc<dyn ValueProjector>>,
}

impl core::fmt::Debug for Projection {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Projection")
            .field("fields", &self.fields)
            .field("projector", &self.projector.is_some())
            .finish()
    }
}

impl Projection {
    /// An empty projection.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a column projected by id alone (see [`ProjectedField::by_id`]).
    #[must_use]
    pub fn column(mut self, column_id: u16) -> Self {
        self.fields.push(ProjectedField::by_id(column_id));
        self
    }

    /// Adds a declared field.
    #[must_use]
    pub fn field(mut self, field: ProjectedField) -> Self {
        self.fields.push(field);
        self
    }

    /// Sets the projector that reads the fields out of row values.
    #[must_use]
    pub fn projector(mut self, projector: Arc<dyn ValueProjector>) -> Self {
        self.projector = Some(projector);
        self
    }

    /// The projected fields, in output order.
    #[must_use]
    pub fn fields(&self) -> &[ProjectedField] {
        &self.fields
    }

    /// The projector, when one is set.
    #[must_use]
    pub fn value_projector(&self) -> Option<&Arc<dyn ValueProjector>> {
        self.projector.as_ref()
    }

    /// The projected column ids, in output order.
    pub(crate) fn column_ids(&self) -> Vec<u16> {
        self.fields.iter().map(|f| f.column_id).collect()
    }
}

impl From<&[u16]> for Projection {
    fn from(ids: &[u16]) -> Self {
        Self {
            fields: ids.iter().copied().map(ProjectedField::by_id).collect(),
            projector: None,
        }
    }
}

impl<const N: usize> From<&[u16; N]> for Projection {
    fn from(ids: &[u16; N]) -> Self {
        Self::from(ids.as_slice())
    }
}

impl From<&Vec<u16>> for Projection {
    fn from(ids: &Vec<u16>) -> Self {
        Self::from(ids.as_slice())
    }
}

impl From<&Self> for Projection {
    fn from(projection: &Self) -> Self {
        projection.clone()
    }
}

/// Whether `field` was declared with a type, rather than projected by id: the
/// fields a projector writes and a whole value is read through.
pub fn is_declared(field: &ProjectedField) -> bool {
    field.type_tag.is_some() && intrinsic_type(field.column_id).is_none()
}

/// Reads, in a batch of rows a scan returns, the declared `fields` of each row
/// carrying a whole value in column `whole_id` out of that value through
/// `projector`, each value given with its key, in place of the cells the row
/// holds for them. A row without a whole value keeps its cells as its source
/// stored them, and a row that is not a value (a deletion) is not read. A
/// field the projector leaves unset is null here, and reads as declared once
/// the batch is conformed. A whole value to read with no projector set is an
/// error: its fields lie inside it and only the caller knows how.
pub fn project_decided(
    batch: ColumnBatch,
    fields: &[ProjectedField],
    projector: Option<&dyn ValueProjector>,
    whole_id: u16,
) -> crate::Result<ColumnBatch> {
    let row_count = batch.row_count;
    let rows = row_count as usize;
    let find = |id: u16| batch.columns.iter().find(|c| c.column_id == id);
    let (Some(values), Some(keys)) = (find(whole_id), find(COL_USER_KEY)) else {
        return Err(MISSING_SCAN_COLUMN);
    };
    let types = find(COL_VALUE_TYPE);
    let declared: Vec<ProjectedField> = fields.iter().filter(|f| is_declared(f)).cloned().collect();

    // Per declared field, the cells a projected row read; `None` in `read`
    // marks a row the projector did not read, which keeps its own cells.
    let mut read: Vec<Option<Vec<Option<Vec<u8>>>>> = Vec::with_capacity(rows);
    let mut cells: Vec<Option<Vec<u8>>> = alloc::vec![None; declared.len()];
    for row in 0..row_count {
        let is_value = match types {
            Some(types) => {
                let byte = *types.data.get(row as usize).ok_or(MISSING_SCAN_COLUMN)?;
                returns_a_value(
                    crate::ValueType::try_from(byte)
                        .map_err(|()| Error::InvalidTag(("ValueType", byte)))?,
                )
            }
            None => true,
        };
        if !(is_value && values.is_valid(row)) {
            read.push(None);
            continue;
        }
        let projector = projector.ok_or(UNREADABLE)?;
        cells.fill(None);
        let key = bytes_column_row(&keys.data, row_count, row)?;
        let value = bytes_column_row(&values.data, row_count, row)?;
        projector.project(key, value, &mut ProjectedRow::new(&declared, &mut cells))?;
        read.push(Some(core::mem::take(&mut cells)));
        cells = alloc::vec![None; declared.len()];
    }
    if read.iter().all(Option::is_none) {
        return Ok(batch);
    }

    let ColumnBatch {
        row_count,
        mut columns,
    } = batch;
    for (index, field) in declared.iter().enumerate() {
        let Some(at) = columns.iter().position(|c| c.column_id == field.column_id) else {
            return Err(MISSING_SCAN_COLUMN);
        };
        let old = columns.get(at).ok_or(MISSING_SCAN_COLUMN)?;
        let mut column: Vec<Option<Vec<u8>>> = Vec::with_capacity(rows);
        for (row, projected) in (0..row_count).zip(&read) {
            column.push(match projected {
                Some(cells) => cells.get(index).cloned().flatten(),
                None if old.is_valid(row) => Some(cell_of(old, row_count, row)?.to_vec()),
                None => None,
            });
        }
        let built = built_column(field, &column)?;
        if let Some(slot) = columns.get_mut(at) {
            *slot = built;
        }
    }
    Ok(ColumnBatch { row_count, columns })
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
                .ok_or(MISSING_SCAN_COLUMN)
        }
        None => bytes_column_row(&column.data, rows, row),
    }
}

/// A whole value holds declared fields and no projector is set to read them.
const UNREADABLE: Error = Error::Projection(
    "projection: a row returned stores its value whole, and declared fields are read out \
     of it through a projector, which is not set",
);

/// Whether a row of `value_type` is returned with a value: a value, or a merge
/// operand left unresolved, which is what a read of a tree without a merge
/// operator returns (a tree with one has its operands resolved by then).
fn returns_a_value(value_type: crate::ValueType) -> bool {
    matches!(
        value_type,
        crate::ValueType::Value | crate::ValueType::MergeOperand
    )
}

/// A column of `field` holding `cells`, a missing cell null.
fn built_column(field: &ProjectedField, cells: &[Option<Vec<u8>>]) -> crate::Result<Column> {
    let type_tag = field.type_tag.ok_or(ABSENT_FIELD)?;
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
                    slot.copy_from_slice(cell);
                }
            }
            Slice::from(data)
        }
        None => frame_bytes_column(count, || cells.iter().map(|c| c.as_deref().unwrap_or(&[])))?,
    };
    Ok(Column {
        column_id: field.column_id,
        type_tag,
        validity,
        data,
    })
}

/// A batch of a whole-value table lacks a column the scan decoded to read the
/// value through the projector.
const MISSING_SCAN_COLUMN: Error =
    Error::InvalidHeader("columnar_scan: a whole-value batch is missing its key or value column");

/// The column that stands for `field` in a batch of `rows` rows whose source
/// does not have it, or the error its declaration asks for when a row of it
/// is a value (`is_value`): a deletion carries no fields and is not refused.
pub fn absent_column(
    field: &ProjectedField,
    rows: u32,
    is_value: &dyn Fn(u32) -> bool,
) -> crate::Result<Column> {
    let type_tag = field.type_tag.ok_or(ABSENT_FIELD)?;
    let count = rows as usize;
    let (validity, data) = match &field.absent {
        Absent::Error if (0..rows).any(is_value) => return Err(ABSENT_FIELD),
        Absent::Null | Absent::Error => {
            let data = match type_tag.fixed_width() {
                Some(width) => Slice::from(alloc::vec![0u8; count * usize::from(width)]),
                None => frame_bytes_column(count, || core::iter::repeat_n(&[][..], count))?,
            };
            (Some(alloc::vec![0u8; count.div_ceil(8)]), data)
        }
        Absent::Default(value) => {
            let data = match type_tag.fixed_width() {
                Some(_) => Slice::from(value.repeat(count)),
                None => frame_bytes_column(count, || core::iter::repeat_n(&**value, count))?,
            };
            (None, data)
        }
    };
    Ok(Column {
        column_id: field.column_id,
        type_tag,
        validity,
        data,
    })
}

/// A projected field is absent from a row and declared an error, or was
/// projected by id alone.
const ABSENT_FIELD: Error = Error::Projection(
    "projection: a projected field is absent from a row and its declaration does not allow it",
);

/// Applies the absence rule of `fields` to a batch a columnar segment yielded:
/// a declared column the segment does not have is filled in as declared, a
/// null cell reads as its field's declaration, and a column stored under
/// another type than the one declared is an error. The columns come back in
/// the order of `fields`; a column the batch holds that no field names is
/// kept after them, in its place.
///
/// Only a row that is a value is held to its fields: a deletion or a merge
/// operand, which the batch's value-type column names when it carries one,
/// has none, and is decided by its type before it could be returned.
pub fn conform(batch: ColumnBatch, fields: &[ProjectedField]) -> crate::Result<ColumnBatch> {
    conform_with(batch, fields, Conform::Strict).map(|(batch, _)| batch)
}

/// [`conform`] for a batch whose rows are not yet decided: a field declared
/// an error where absent reads as null instead, and so does one with a
/// default unless it is the predicate's column `judged`, so a predicate over
/// the batch sees its own field as declared, and the rows a scan returns are
/// held to the declarations by [`conform`] once they are decided. A row that
/// is shadowed, deleted or filtered out never fails the scan, and no default
/// is laid out for it.
///
/// A column stored under another type than its field declares reads as null
/// here, and its id is returned: a row of the batch the scan returns fails it
/// with [`MISTYPED`], one shadowed or filtered out does not. A predicate over
/// such a column cannot judge the rows, so none of them is filtered out by it.
pub fn conform_lenient(
    batch: ColumnBatch,
    fields: &[ProjectedField],
    judged: Option<u16>,
) -> crate::Result<(ColumnBatch, Vec<u16>)> {
    conform_with(batch, fields, Conform::Lenient { judged })
}

/// A segment stores a projected field under another type than declared.
pub const MISTYPED: Error =
    Error::Projection("projection: a segment stores a projected field under another type");

/// How [`conform_with`] holds a batch to its fields.
#[derive(Clone, Copy)]
enum Conform {
    /// The rows are decided: each value row is held to every declaration.
    Strict,
    /// The rows are not decided yet; `judged` is the predicate's column.
    Lenient { judged: Option<u16> },
}

/// [`conform`], holding the rows that are values to [`Absent::Error`] and a
/// mistyped column to [`MISTYPED`] only when strict, and laying out a default
/// only when strict or for the judged column; also returns the ids of the
/// columns that were mistyped.
fn conform_with(
    batch: ColumnBatch,
    fields: &[ProjectedField],
    mode: Conform,
) -> crate::Result<(ColumnBatch, Vec<u16>)> {
    let strict = matches!(mode, Conform::Strict);
    // A default left out reads as null until the rows are decided, when the
    // strict pass lays it out for the rows returned.
    let defers = |field: &ProjectedField| match mode {
        Conform::Strict => false,
        Conform::Lenient { judged } => {
            judged != Some(field.column_id) && matches!(field.absent, Absent::Default(_))
        }
    };
    let ColumnBatch {
        row_count,
        mut columns,
    } = batch;
    let types = columns
        .iter()
        .find(|c| c.column_id == COL_VALUE_TYPE)
        .map(|c| c.data.clone());
    let is_value = |row: u32| {
        strict
            && types.as_ref().is_none_or(|types| {
                types
                    .get(row as usize)
                    .and_then(|&byte| crate::ValueType::try_from(byte).ok())
                    .is_some_and(returns_a_value)
            })
    };
    let mut mistyped = Vec::new();
    let mut out = Vec::with_capacity(fields.len().max(columns.len()));
    for field in fields {
        let at = columns.iter().position(|c| c.column_id == field.column_id);
        let column = match at {
            Some(at) => {
                let column = columns.remove(at);
                if field.type_tag.is_some_and(|t| t != column.type_tag) {
                    if strict {
                        return Err(MISTYPED);
                    }
                    // Null under the declared type, so the batch agrees with
                    // the other sources; its rows fail only if returned.
                    mistyped.push(field.column_id);
                    let null = ProjectedField {
                        absent: Absent::Null,
                        ..field.clone()
                    };
                    out.push(absent_column(&null, row_count, &|_| false)?);
                    continue;
                }
                if defers(field) {
                    column
                } else {
                    fill_nulls(column, field, row_count, &is_value)?
                }
            }
            // A column of no declared type has no absent form; its rows are
            // held to it once they are decided.
            None if !strict && field.type_tag.is_none() => continue,
            None if defers(field) => {
                let null = ProjectedField {
                    absent: Absent::Null,
                    ..field.clone()
                };
                absent_column(&null, row_count, &|_| false)?
            }
            None => absent_column(field, row_count, &is_value)?,
        };
        out.push(column);
    }
    out.extend(columns);
    Ok((
        ColumnBatch {
            row_count,
            columns: out,
        },
        mistyped,
    ))
}

/// `column` with its null cells read as `field` declares: kept for
/// [`Absent::Null`], set to the default for [`Absent::Default`], an error for
/// [`Absent::Error`] when a null row is a value (`is_value`).
fn fill_nulls(
    column: Column,
    field: &ProjectedField,
    rows: u32,
    is_value: &dyn Fn(u32) -> bool,
) -> crate::Result<Column> {
    let Some(validity) = &column.validity else {
        return Ok(column);
    };
    let is_null = |row: u32| {
        validity
            .get(row as usize / 8)
            .is_none_or(|byte| byte >> (row % 8) & 1 == 0)
    };
    if !(0..rows).any(is_null) {
        return Ok(column);
    }
    let value = match &field.absent {
        Absent::Error if (0..rows).any(|row| is_null(row) && is_value(row)) => {
            return Err(ABSENT_FIELD);
        }
        Absent::Null | Absent::Error => return Ok(column),
        Absent::Default(value) => value,
    };
    let count = rows as usize;
    let data = if let Some(width) = column.type_tag.fixed_width() {
        let width = usize::from(width);
        let mut data = column.data.to_vec();
        for row in (0..rows).filter(|&row| is_null(row)) {
            let at = row as usize * width;
            data.get_mut(at..at + width)
                .ok_or(Error::InvalidHeader("columnar: fixed column row truncated"))?
                .copy_from_slice(value);
        }
        Slice::from(data)
    } else {
        let cells = (0..rows)
            .map(|row| {
                if is_null(row) {
                    Ok(&**value)
                } else {
                    bytes_column_row(&column.data, rows, row)
                }
            })
            .collect::<crate::Result<Vec<&[u8]>>>()?;
        frame_bytes_column(count, || cells.iter().copied())?
    };
    Ok(Column {
        column_id: column.column_id,
        type_tag: column.type_tag,
        validity: None,
        data,
    })
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test code")]
mod tests;

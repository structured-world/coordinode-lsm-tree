// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Rows written as cells, some of which keep their bytes in a blob file.
//!
//! A caller that knows the fields of its values writes a row as a sequence of
//! [`Field`]s instead of one opaque value: each names its column and physical
//! type, as a row of the columnar format does, and holds a [`Cell`]. A
//! [`Cell::Value`] carries its bytes;
//! a [`Cell::Ref`] names a blob object the key already owns, which is how a
//! metadata-only update keeps a large field without rewriting it: the caller
//! reads the row's references through the reference-aware projection and
//! writes them back next to the cells it changed.
//!
//! # Reference lifecycle
//!
//! **Identity.** An object is one blob frame, named by its [`BlobRef`]: the
//! blob file, the offset in it and the frame's size. Two references are the
//! same object exactly when their handles are equal.
//!
//! **Sharing.** An object is shared only by versions of one key: a
//! [`BlobRef`] is bound to the key it was read from, and a write of it under
//! another key is refused.
//!
//! **Ownership.** Exactly one live reference to an object owns it, marked in
//! the row: the oldest holder. The cell a flush separates owns the object it
//! writes, and a reference written back by a caller, being newer, never owns.
//! A compaction meets a key's versions newest first, so it holds a key's kept
//! rows until the key ends; when it drops the owner of an object a kept row
//! also holds, ownership passes to the oldest such row, rewritten before it
//! is written. A relocation keeps each reference's owner bit on the copy.
//!
//! **Charging.** An object's bytes are charged to its blob file once: when its
//! owning reference leaves the tree and no kept row takes it over (a
//! compaction drops or transforms the version, a merge folds onto it, or its
//! table is dropped whole). A borrowed reference leaving charges nothing. The
//! owner being the oldest holder, no holder outside a compaction can still
//! hold an object it charges, except where a whole table is dropped without
//! the rows that borrow from it: the charged bytes then reach the file's size
//! early, and the removal rule below keeps the file.
//!
//! **Removal.** A blob file is removed, or its consumed prefix punched, only
//! when its charged bytes reach its size, or a relocation copied it, AND no
//! live table links it and no memtable references it. Every reference, owned
//! or borrowed, links its file from its table, and a reference written into a
//! memtable registers its file there, so charging early never frees an object
//! a version still holds. A relocation that cannot remove the file it copied
//! leaves it in place fully charged, to go when its last reference does.
//!
//! **Writes.** A written reference must name a frame of a file in the current
//! version at or past that file's live start, checked under the lock that
//! orders writes against version installs, together with registering the
//! file in the memtable the row lands in. A reference a background relocation
//! has already moved is refused as stale; the caller reads the key again and
//! gets the moved one.
//!
//! **Visibility.** A reference exists only inside a written version: in the
//! memtable until a flush, then in a table. An object stays live while any
//! version that holds it is readable by some snapshot, the rule a whole-value
//! indirection follows, so there is no window in which the old reference is
//! gone and the new one is not yet visible.
//!
//! **Restart.** Reachability is what the tables say, ownership and links
//! included; the memtable registrations of a replayed row are made again by
//! its write.
//!
//! # Encoding
//!
//! A row is self-describing, so the engine can list its references and lay
//! its fields out by column without the caller's schema: a little-endian
//! `u16` field count, a bitmap of one bit per field set for a reference, a
//! bitmap of the same size set for an owning reference (both least
//! significant bit first), one descriptor per field (its little-endian `u16`
//! column identifier, then its type's wire tag and width byte, as the columnar
//! format writes them), then each cell as a little-endian `u32` length and its
//! bytes. A reference cell holds an encoded blob indirection.
//!
//! The logical value a plain read returns is the fields in order, with every
//! reference replaced by the bytes of its object, framed as the columnar
//! format frames a row's value sub-columns: a variable-width field as a
//! little-endian `u32` length and its bytes, a fixed-width one as its bytes.

use alloc::vec::Vec;

use super::handle::BlobIndirection;
use crate::coding::{Decode, Encode};
pub use crate::table::column_type::TypeTag;
use crate::{Error, UserKey, vlog::BlobFileId};

/// A blob object a key holds, named by where its frame sits.
///
/// Borrowed from the [`RowCells`] of a reference-aware read and written back
/// in a [`Cell::Ref`] to keep the object in a newer version of the same key.
/// It stays bound to that key, to the tree it was read from and to the
/// version the read saw: a write under any other key, into any other tree,
/// or after that version's owner of the object was let go, refuses it. Blob
/// file numbers are local to a tree, so the tree is part of what the
/// reference names, and the borrow keeps the read that vouches for it alive.
#[derive(Clone, Copy, Debug)]
pub struct BlobRef<'a> {
    pub(crate) indirection: BlobIndirection,
    pub(crate) key: &'a [u8],
    pub(crate) tree: crate::TreeId,
    /// The version the read that handed the reference out saw.
    pub(crate) source: u64,
}

impl BlobRef<'_> {
    /// The blob file the object is stored in.
    #[must_use]
    pub fn blob_file_id(&self) -> BlobFileId {
        self.indirection.vhandle.blob_file_id
    }

    /// The object's logical size in bytes, before compression.
    #[must_use]
    pub fn size(&self) -> u32 {
        self.indirection.size
    }
}

impl PartialEq for BlobRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.indirection.vhandle == other.indirection.vhandle
    }
}

impl Eq for BlobRef<'_> {}

/// What a field of a row written as cells holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cell<'a> {
    /// The field's bytes. A cell at or above its column's separation
    /// threshold is stored in a blob file when the row is flushed.
    Value(&'a [u8]),
    /// An object the key already holds, kept without rewriting its bytes.
    Ref(BlobRef<'a>),
}

/// One field of a row written as cells: the caller's column identifier and
/// physical type, and the cell.
///
/// The identifier and type are stored with the row, so a stored row always
/// reads with the layout it was written with; the engine keeps no schema.
/// A column identifier names the same field in every row of the tree, as in
/// the columnar format. A reference is how a field is stored, not its type:
/// the type stays the field's own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Field<'a> {
    /// The caller-assigned column identifier.
    pub column: u16,
    /// The physical type of the field's value.
    pub tag: TypeTag,
    /// The value, or a reference to the object holding it.
    pub cell: Cell<'a>,
}

impl<'a> Field<'a> {
    /// A field of variable-width bytes.
    #[must_use]
    pub const fn bytes(column: u16, bytes: &'a [u8]) -> Self {
        Self {
            column,
            tag: TypeTag::Bytes,
            cell: Cell::Value(bytes),
        }
    }
}

/// A stored row's cells as written: value cells with their bytes, and every
/// reference as a [`BlobRef`], without reading any object.
///
/// It holds the version the read saw: while it lives, every object it
/// references stays readable through [`Self::resolve`], whatever relocation
/// or garbage collection does in the meantime, and the references it hands
/// out stay writable unless a later version let their object go.
///
/// Returned by [`BlobTree::get_cells`](crate::BlobTree::get_cells) and
/// [`BlobTree::range_cells`](crate::BlobTree::range_cells).
pub struct RowCells {
    pub(crate) key: UserKey,
    pub(crate) row: crate::Slice,
    /// The read the row came from, shared by every row of one range read.
    pub(crate) read: alloc::sync::Arc<CellRead>,
}

/// One reference-aware read: what keeps the rows it hands out readable and
/// their references writable.
pub(crate) struct CellRead {
    /// The tree the rows were read from.
    pub(crate) tree: crate::TreeId,
    /// The version the read saw, kept for resolving the rows' objects.
    pub(crate) version: crate::version::SuperVersion,
    /// Where the rows' objects are read from.
    pub(crate) source: super::BlobSource,
    /// The read's registration, which keeps the releases newer than its
    /// version on record for as long as the references are held.
    pub(crate) token: super::released::ReaderToken,
}

impl core::fmt::Debug for RowCells {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RowCells")
            .field("key", &self.key)
            .field("tree", &self.read.tree)
            .field("version", &self.read.token.version())
            .finish_non_exhaustive()
    }
}

impl RowCells {
    /// The key the row was read under.
    #[must_use]
    pub fn key(&self) -> &UserKey {
        &self.key
    }

    /// The row's fields in order, each holding a [`Cell::Value`] or a
    /// [`Cell::Ref`] bound to the row's key, ready to be written back in a
    /// newer version.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored row is malformed.
    pub fn fields(&self) -> crate::Result<Vec<Field<'_>>> {
        Ok(decode_row(&self.row)?
            .into_iter()
            .map(|field| Field {
                column: field.column,
                tag: field.tag,
                cell: match field.cell {
                    RowCell::Value(bytes) => Cell::Value(bytes),
                    RowCell::Ref { indirection, .. } => Cell::Ref(BlobRef {
                        indirection,
                        key: &self.key,
                        tree: self.read.tree,
                        source: self.read.token.version(),
                    }),
                },
            })
            .collect())
    }

    /// The bytes of the field in `column`: its own for a value cell, its
    /// object's for a reference, read through the version the read saw. Only
    /// that object is read. `None` when the row has no such field.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored row is malformed or the object cannot
    /// be read.
    pub fn resolve(&self, column: u16) -> crate::Result<Option<crate::Slice>> {
        let fields = decode_row(&self.row)?;
        let Some(field) = fields.iter().find(|field| field.column == column) else {
            return Ok(None);
        };
        match &field.cell {
            RowCell::Value(bytes) => Ok(Some(crate::Slice::from(*bytes))),
            RowCell::Ref { indirection, .. } => {
                let object =
                    self.read
                        .source
                        .object(&self.read.version.version, &self.key, indirection)?;
                if object.len() != indirection.size as usize {
                    return Err(Error::InvalidHeader(
                        "field row: referenced object differs from its recorded size",
                    ));
                }
                Ok(Some(object))
            }
        }
    }
}

/// A cell of an encoded row: its bytes, or the indirection of its object and
/// whether this row owns it.
#[derive(Clone, Copy, Debug)]
pub(crate) enum RowCell<'a> {
    Value(&'a [u8]),
    Ref {
        indirection: BlobIndirection,
        owner: bool,
    },
}

/// A field of an encoded row: its column identifier, its type and its cell.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RowField<'a> {
    pub(crate) column: u16,
    pub(crate) tag: TypeTag,
    pub(crate) cell: RowCell<'a>,
}

#[cfg(test)]
impl<'a> RowField<'a> {
    /// A field of variable-width bytes.
    pub(crate) const fn bytes(column: u16, cell: RowCell<'a>) -> Self {
        Self {
            column,
            tag: TypeTag::Bytes,
            cell,
        }
    }
}

const CORRUPT: Error = Error::InvalidHeader("field row: malformed cell row");

/// Bytes a field's descriptor takes: its column id, type tag and width.
const DESCRIPTOR_LEN: usize = 4;

/// The first column id a field may take: the ids below it name the key, the
/// seqno and the value type in a columnar table.
pub const FIRST_FIELD_COLUMN: u16 = 3;

/// The column id a columnar table keeps a whole value under, beside the
/// fields of rows written as cells: no field may take it.
#[cfg(feature = "columnar")]
pub(crate) const WHOLE_VALUE_COLUMN: u16 = u16::MAX;

/// The column id a columnar table keeps the references of rows written as
/// cells under: no field may take it.
pub(crate) const CELL_REFS_COLUMN: u16 = u16::MAX - 1;

/// The first column id past the field ids, which the engine keeps for itself.
pub const RESERVED_COLUMNS: u16 = CELL_REFS_COLUMN;

/// Whether `column` is an id a field may take.
pub(crate) const fn is_field_column(column: u16) -> bool {
    column >= FIRST_FIELD_COLUMN && column < RESERVED_COLUMNS
}

/// Puts `fields` in the order a row stores them, ascending by column, and
/// refuses a row a caller could not have meant: a column outside the field
/// ids (below [`FIRST_FIELD_COLUMN`], or from [`RESERVED_COLUMNS`] on), a type that has no wire
/// form (a zero-width [`TypeTag::Fixed`]), two fields in one column, or a
/// fixed-width field whose value or object is not its type's width.
///
/// One order for every row is what lets a columnar table store a row's fields
/// as columns and give back the row as it was written.
///
/// # Errors
///
/// Returns [`Error::CellRow`] naming the fault.
pub(crate) fn order_fields(fields: &mut [RowField<'_>]) -> crate::Result<()> {
    fields.sort_by_key(|field| field.column);
    let mut previous = None;
    for field in fields.iter() {
        if !is_field_column(field.column) {
            return Err(Error::CellRow("a field's column is not a field id"));
        }
        if previous == Some(field.column) {
            return Err(Error::CellRow("one column in two fields"));
        }
        previous = Some(field.column);
        let (tag, width) = field.tag.to_wire();
        if TypeTag::from_wire(tag, width).is_err() {
            return Err(Error::CellRow("a field's type has no wire form"));
        }
        if let Some(width) = field.tag.fixed_width() {
            let len = match field.cell {
                RowCell::Value(bytes) => bytes.len(),
                RowCell::Ref { indirection, .. } => indirection.size as usize,
            };
            if len != usize::from(width) {
                return Err(Error::CellRow("a field is not its type's width"));
            }
        }
    }
    Ok(())
}

/// Encodes `fields` as a row, references as their indirections.
///
/// # Errors
///
/// Returns an error if the row has more than `u16::MAX` fields or a cell is
/// longer than `u32::MAX` bytes.
pub(crate) fn encode_row(fields: &[RowField<'_>]) -> crate::Result<Vec<u8>> {
    let count = u16::try_from(fields.len())
        .map_err(|_| Error::InvalidHeader("field row: more than u16::MAX cells"))?;
    let bitmap_len = fields.len().div_ceil(8);
    let mut out = Vec::with_capacity(2 + 2 * bitmap_len + fields.len() * (DESCRIPTOR_LEN + 8));
    out.extend_from_slice(&count.to_le_bytes());
    let refs_at = out.len();
    let owners_at = refs_at + bitmap_len;
    out.resize(owners_at + bitmap_len, 0);
    for field in fields {
        let (tag, width) = field.tag.to_wire();
        out.extend_from_slice(&field.column.to_le_bytes());
        out.push(tag);
        out.push(width);
    }
    let mut encoded = Vec::new();
    for (i, field) in fields.iter().enumerate() {
        let bytes: &[u8] = match &field.cell {
            RowCell::Value(bytes) => bytes,
            RowCell::Ref { indirection, owner } => {
                set_bit(&mut out, refs_at, i);
                if *owner {
                    set_bit(&mut out, owners_at, i);
                }
                encoded.clear();
                indirection.encode_into(&mut encoded)?;
                &encoded
            }
        };
        let len = u32::try_from(bytes.len())
            .map_err(|_| Error::InvalidHeader("field row: cell exceeds u32"))?;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(bytes);
    }
    Ok(out)
}

fn set_bit(out: &mut [u8], bitmap_at: usize, i: usize) {
    if let Some(byte) = out.get_mut(bitmap_at + i / 8) {
        *byte |= 1 << (i % 8);
    }
}

fn bit(bitmap: &[u8], i: usize) -> bool {
    bitmap.get(i / 8).is_some_and(|b| b >> (i % 8) & 1 == 1)
}

/// The fields of an encoded row, in order.
///
/// # Errors
///
/// Returns an error if the row is truncated, carries trailing bytes, names a
/// type that does not exist, marks a value cell as owning, holds a reference
/// cell that is not one indirection, or holds a fixed-width field of another
/// width.
pub(crate) fn decode_row(row: &[u8]) -> crate::Result<Vec<RowField<'_>>> {
    let count = row
        .first_chunk::<2>()
        .map(|b| usize::from(u16::from_le_bytes(*b)))
        .ok_or(CORRUPT)?;
    let bitmap_len = count.div_ceil(8);
    let refs = row.get(2..2 + bitmap_len).ok_or(CORRUPT)?;
    let owners = row.get(2 + bitmap_len..2 + 2 * bitmap_len).ok_or(CORRUPT)?;
    let descriptors_at = 2 + 2 * bitmap_len;
    let descriptors = row
        .get(descriptors_at..descriptors_at + count * DESCRIPTOR_LEN)
        .ok_or(CORRUPT)?;
    let mut pos = descriptors_at + count * DESCRIPTOR_LEN;
    let mut fields = Vec::with_capacity(count);
    let mut previous = None;
    for (i, descriptor) in descriptors.chunks_exact(DESCRIPTOR_LEN).enumerate() {
        let [c0, c1, tag, width] =
            <[u8; DESCRIPTOR_LEN]>::try_from(descriptor).map_err(|_| CORRUPT)?;
        let column = u16::from_le_bytes([c0, c1]);
        // A written row holds field ids in ascending order; anything else is
        // damage, not a layout to read.
        if !is_field_column(column) || previous.is_some_and(|previous| column <= previous) {
            return Err(CORRUPT);
        }
        previous = Some(column);
        let tag = TypeTag::from_wire(tag, width).map_err(|_| CORRUPT)?;
        let len = row
            .get(pos..)
            .and_then(<[u8]>::first_chunk::<4>)
            .map(|b| u32::from_le_bytes(*b) as usize)
            .ok_or(CORRUPT)?;
        let start = pos + 4;
        let end = start.checked_add(len).ok_or(CORRUPT)?;
        let bytes = row.get(start..end).ok_or(CORRUPT)?;
        let owner = bit(owners, i);
        let cell = if bit(refs, i) {
            let mut reader = bytes;
            let indirection = BlobIndirection::decode_from(&mut reader)?;
            if !reader.is_empty() {
                return Err(CORRUPT);
            }
            RowCell::Ref { indirection, owner }
        } else if owner {
            return Err(CORRUPT);
        } else {
            RowCell::Value(bytes)
        };
        // A write refuses a fixed-width field of another width, so a stored
        // one is damage: read on, the row would be framed with the wrong
        // field boundaries.
        if let Some(width) = tag.fixed_width() {
            let len = match &cell {
                RowCell::Value(bytes) => bytes.len(),
                RowCell::Ref { indirection, .. } => indirection.size as usize,
            };
            if len != usize::from(width) {
                return Err(CORRUPT);
            }
        }
        fields.push(RowField { column, tag, cell });
        pos = end;
    }
    if pos != row.len() {
        return Err(CORRUPT);
    }
    Ok(fields)
}

/// The references of an encoded row, in field order, each with whether the
/// row owns its object.
///
/// # Errors
///
/// Returns an error if the row is malformed (see [`decode_row`]).
pub(crate) fn row_refs(row: &[u8]) -> crate::Result<Vec<(BlobIndirection, bool)>> {
    Ok(decode_row(row)?
        .into_iter()
        .filter_map(|field| match field.cell {
            RowCell::Ref { indirection, owner } => Some((indirection, owner)),
            RowCell::Value(_) => None,
        })
        .collect())
}

/// The bytes the framing of the logical value spends on a field of `tag`
/// besides its value: a length prefix for variable-width bytes, nothing for
/// a fixed width, whose width the tag already gives.
fn frame_overhead(tag: TypeTag) -> u32 {
    if tag.fixed_width().is_some() { 0 } else { 4 }
}

/// The length of the logical value `row` reads as, from the row alone: a
/// reference records its object's size.
///
/// # Errors
///
/// Returns an error if the row is malformed or its logical value would exceed
/// `u32::MAX` bytes, the limit of a value.
pub(crate) fn logical_len(row: &[u8]) -> crate::Result<u32> {
    let mut len = 0u32;
    for field in decode_row(row)? {
        let cell_len = match field.cell {
            RowCell::Value(bytes) => u32::try_from(bytes.len()).map_err(|_| CORRUPT)?,
            RowCell::Ref { indirection, .. } => indirection.size,
        };
        len = len
            .checked_add(frame_overhead(field.tag))
            .and_then(|len| len.checked_add(cell_len))
            .ok_or(Error::InvalidHeader(
                "field row: logical value exceeds u32::MAX bytes",
            ))?;
    }
    Ok(len)
}

/// Moves every value cell of `row` at or above its column's `threshold` into
/// a blob file through `write`, which stores the bytes and returns their
/// handle, and returns the row with those cells replaced by owning
/// references; `None` when no cell reaches its threshold and the row stays as
/// it is.
///
/// # Errors
///
/// Returns an error if the row is malformed or `write` fails.
pub(crate) fn separate_row(
    row: &[u8],
    threshold: impl Fn(u16) -> u32,
    mut write: impl FnMut(&[u8]) -> crate::Result<crate::vlog::ValueHandle>,
) -> crate::Result<Option<Vec<u8>>> {
    let mut fields = decode_row(row)?;
    let mut separated = false;
    for field in &mut fields {
        if let RowCell::Value(bytes) = field.cell {
            // A cell is at most `u32::MAX` bytes, the row encoding's limit.
            let size = u32::try_from(bytes.len()).map_err(|_| CORRUPT)?;
            if size >= threshold(field.column) {
                field.cell = RowCell::Ref {
                    indirection: BlobIndirection {
                        vhandle: write(bytes)?,
                        size,
                    },
                    owner: true,
                };
                separated = true;
            }
        }
    }
    if separated {
        encode_row(&fields).map(Some)
    } else {
        Ok(None)
    }
}

/// The logical value of an encoded row: its fields in order, framed as the
/// columnar format frames a row's value sub-columns (a variable-width field
/// as a little-endian `u32` length and its bytes, a fixed-width one as its
/// bytes), every reference replaced by the object `fetch` returns for it.
///
/// # Errors
///
/// Returns an error if the row is malformed, `fetch` fails, or an object's
/// length differs from the size its reference records (a dangling or
/// mis-typed reference is an error, never an empty value).
pub(crate) fn resolve_row(
    row: &[u8],
    mut fetch: impl FnMut(&BlobIndirection) -> crate::Result<crate::Slice>,
) -> crate::Result<Vec<u8>> {
    let fields = decode_row(row)?;
    let mut out = Vec::new();
    for field in fields {
        let prefixed = frame_overhead(field.tag) > 0;
        match field.cell {
            RowCell::Value(bytes) => {
                if prefixed {
                    let len = u32::try_from(bytes.len()).map_err(|_| CORRUPT)?;
                    out.extend_from_slice(&len.to_le_bytes());
                }
                out.extend_from_slice(bytes);
            }
            RowCell::Ref { indirection, .. } => {
                let object = fetch(&indirection)?;
                if object.len() != indirection.size as usize {
                    return Err(Error::InvalidHeader(
                        "field row: referenced object differs from its recorded size",
                    ));
                }
                if prefixed {
                    out.extend_from_slice(&indirection.size.to_le_bytes());
                }
                out.extend_from_slice(&object);
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::indexing_slicing, reason = "test code")]
mod tests;

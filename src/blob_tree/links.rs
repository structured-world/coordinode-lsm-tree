// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A table's links to the blob files its entries reference, derived from the
//! entries themselves, for a writer that receives entries it did not separate:
//! a salvage copy, an imported table.

/// How an entry holds a blob reference.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Holding {
    /// An indirection entry, which owns its object.
    Indirection,
    /// A cell row's reference with its owner bit set.
    OwnedCell,
    /// A cell row's borrowed reference.
    BorrowedCell,
}

impl Holding {
    /// Whether the entry owns the object.
    pub fn owns(self) -> bool {
        self != Self::BorrowedCell
    }
}

/// One blob reference recovered from an entry: the entry's key, the
/// reference, and how the entry holds it.
pub type RecoveredRef = (
    crate::UserKey,
    crate::blob_tree::handle::BlobIndirection,
    Holding,
);

/// Decodes every blob reference in `entries`, with the entry's key: the
/// [`crate::blob_tree::handle::BlobIndirection`] of an indirection entry, which
/// owns its object, and each reference of a cell row with its owner bit. An
/// entry TAGGED as either whose value fails to decode is corrupt content the
/// live read path could not follow either: the caller drops the block rather
/// than laundering it into the recovered copy.
pub fn collect_indirections(entries: &[crate::InternalValue]) -> crate::Result<Vec<RecoveredRef>> {
    use crate::coding::Decode;

    let mut out = Vec::new();
    for entry in entries {
        if entry.key.value_type == crate::ValueType::Indirection {
            let mut cursor = &entry.value[..];
            out.push((
                entry.key.user_key.clone(),
                crate::blob_tree::handle::BlobIndirection::decode_from(&mut cursor)?,
                Holding::Indirection,
            ));
        } else if entry.key.value_type == crate::ValueType::CellRow {
            for (ind, owned) in crate::blob_tree::field_row::row_refs(&entry.value)? {
                let holding = if owned {
                    Holding::OwnedCell
                } else {
                    Holding::BorrowedCell
                };
                out.push((entry.key.user_key.clone(), ind, holding));
            }
        }
    }
    Ok(out)
}

/// Folds one block's recovered references into the derived blob-link map,
/// mirroring the accumulation the live write path does per entry: an owned
/// reference adds to its file's counts, a borrowed one only links the file.
/// An object a cell row owns also goes to `owned_cells`, the table's
/// `owned_blob_objects`. Blocks are folded in key order, so a blob file's
/// first key is the first seen and its last key the latest.
pub fn fold_blob_links(
    derived: &mut crate::HashMap<crate::vlog::BlobFileId, crate::table::writer::LinkedFile>,
    owned_cells: &mut Vec<(crate::vlog::BlobFileId, u64)>,
    refs: &[RecoveredRef],
) {
    for (key, ind, holding) in refs {
        if *holding == Holding::OwnedCell {
            owned_cells.push((ind.vhandle.blob_file_id, ind.vhandle.offset));
        }
        let (len, bytes, on_disk_bytes) = if holding.owns() {
            (1, u64::from(ind.size), u64::from(ind.vhandle.on_disk_size))
        } else {
            (0, 0, 0)
        };
        derived
            .entry(ind.vhandle.blob_file_id)
            .and_modify(|link| {
                link.bytes += bytes;
                link.on_disk_bytes += on_disk_bytes;
                link.len += len;
                link.last_key.clone_from(key);
            })
            .or_insert_with(|| crate::table::writer::LinkedFile {
                blob_file_id: ind.vhandle.blob_file_id,
                bytes,
                on_disk_bytes,
                len,
                first_key: key.clone(),
                last_key: key.clone(),
            });
    }
}

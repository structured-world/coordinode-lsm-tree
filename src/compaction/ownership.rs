// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Moves a blob object's ownership to a version a compaction keeps when it
//! drops the version that owned it.
//!
//! The owner of an object is the oldest version of its key that holds it, and
//! a compaction meets a key's versions newest first: the versions it keeps
//! that also hold the object have already passed when the dropped owner is
//! seen. So the kept rows of a key are held from its first cell row until the
//! key ends, and the key's dropped owners are settled then: an object a held
//! row references passes to the oldest such row, which is rewritten before it
//! is written, and any other is charged to its blob file. Rows of a key with
//! no cell row pass straight through.

use alloc::vec::Vec;
use core::cell::RefCell;

use crate::blob_tree::FragmentationMap;
use crate::blob_tree::field_row::{RowCell, decode_row, encode_row, row_refs};
use crate::blob_tree::handle::BlobIndirection;
use crate::compaction::stream::DroppedKvCallback;
use crate::{InternalValue, UserKey, ValueType};

/// One compaction's blob accounting together with the key group it holds.
#[derive(Default)]
pub struct OwnershipLedger {
    frag: FragmentationMap,
    /// The key of the open group, if any.
    key: Option<UserKey>,
    /// The open group's kept rows, in the order the compaction emitted them.
    kept: Vec<InternalValue>,
    /// The objects the open group's dropped versions owned.
    orphans: Vec<BlobIndirection>,
    /// Rows of closed groups, settled and waiting to be written, in order.
    ready: Vec<InternalValue>,
}

impl OwnershipLedger {
    /// Whether the open group is `key`'s.
    fn is_open_for(&self, key: &[u8]) -> bool {
        self.key
            .as_ref()
            .is_some_and(|open| crate::comparator::same_user_key(open, key))
    }

    /// Hands `item`, a row the compaction keeps, on toward `write`, after the
    /// rows of earlier keys the ledger held, or holds it with the rest of its
    /// key's group.
    ///
    /// # Errors
    ///
    /// Returns the error of `write`, or of a held cell row that does not
    /// decode.
    pub fn admit(
        &mut self,
        item: InternalValue,
        write: &mut dyn FnMut(InternalValue) -> crate::Result<()>,
    ) -> crate::Result<()> {
        if !self.is_open_for(&item.key.user_key) {
            self.close()?;
        }
        for row in self.ready.drain(..) {
            write(row)?;
        }
        // A key's rows are held from its first cell row on, so they still
        // reach the writer in the order the compaction emitted them.
        if !self.kept.is_empty() || item.key.value_type.is_cell_row() {
            if self.key.is_none() {
                self.key = Some(item.key.user_key.clone());
            }
            self.kept.push(item);
            return Ok(());
        }
        write(item)
    }

    /// Records a dropped version: an indirection's object is charged now, a
    /// cell row's owned objects wait for the end of its key.
    fn dropped(&mut self, kv: &InternalValue) -> crate::Result<()> {
        if !kv.key.value_type.is_cell_row() {
            self.frag.on_dropped(kv);
            return Ok(());
        }
        if !self.is_open_for(&kv.key.user_key) {
            self.close()?;
            self.key = Some(kv.key.user_key.clone());
        }
        for (indirection, owned) in row_refs(&kv.value)? {
            if owned {
                self.orphans.push(indirection);
            }
        }
        Ok(())
    }

    /// Closes the open group: every orphan a held row references passes to
    /// the oldest such row, the others are charged, and the held rows wait in
    /// `ready` to be written.
    fn close(&mut self) -> crate::Result<()> {
        self.key = None;
        let mut kept = core::mem::take(&mut self.kept);
        for orphan in core::mem::take(&mut self.orphans) {
            if !adopt(&mut kept, &orphan)? {
                self.frag.charge(&orphan);
            }
        }
        self.ready.append(&mut kept);
        Ok(())
    }

    /// Ends the compaction: writes the rows still held and hands back the
    /// accounting.
    ///
    /// # Errors
    ///
    /// Returns the error of `write`, or of a held cell row that does not
    /// decode.
    pub fn finish(
        mut self,
        write: &mut dyn FnMut(InternalValue) -> crate::Result<()>,
    ) -> crate::Result<FragmentationMap> {
        self.close()?;
        for row in self.ready.drain(..) {
            write(row)?;
        }
        Ok(self.frag)
    }
}

/// Makes the oldest row of `rows` that references `orphan` its owner;
/// `false` when none does.
fn adopt(rows: &mut [InternalValue], orphan: &BlobIndirection) -> crate::Result<bool> {
    for row in rows.iter_mut().rev() {
        if row.key.value_type != ValueType::CellRow {
            continue;
        }
        let mut cells = decode_row(&row.value)?;
        let mut found = false;
        for cell in &mut cells {
            if let RowCell::Ref { indirection, owner } = cell
                && indirection.vhandle == orphan.vhandle
            {
                *owner = true;
                found = true;
            }
        }
        if found {
            row.value = encode_row(&cells)?.into();
            return Ok(true);
        }
    }
    Ok(false)
}

/// The dropped-version callback a compaction stream reports into, sharing
/// the ledger with the loop that admits the kept rows.
pub struct LedgerHook<'a> {
    pub ledger: &'a RefCell<OwnershipLedger>,
    /// The first error a dropped row raised: the callback cannot return one,
    /// so the loop checks it after every row.
    pub error: &'a RefCell<Option<crate::Error>>,
}

impl DroppedKvCallback for LedgerHook<'_> {
    fn on_dropped(&mut self, kv: &InternalValue) {
        if let Err(e) = self.ledger.borrow_mut().dropped(kv) {
            self.error.borrow_mut().get_or_insert(e);
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::indexing_slicing, reason = "test code")]
mod tests;

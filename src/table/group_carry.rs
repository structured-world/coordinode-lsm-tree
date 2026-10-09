// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Row groups a compaction may carry into its output whole.
//!
//! A scanner reading a compaction input hands out every row as usual and, for
//! each columnar row group it read, records a [`CarryCandidate`]: the group's
//! index entry and the rows it handed out for it. The compaction's write side
//! sees what the merge made of those rows; when the merge passed every row of
//! the group through untouched and nothing else landed among them, the group's
//! bytes on disk already are what the output would encode for them, and they
//! are copied instead of being encoded and compressed again.

use alloc::collections::VecDeque;
use alloc::sync::Arc;
use alloc::vec::Vec;

#[cfg(feature = "std")]
use parking_lot::Mutex;
// no-std: spin mirrors parking_lot's Mutex API without an allocator.
#[cfg(not(feature = "std"))]
use spin::Mutex;

use super::{BlockHandle, Table};
use crate::InternalValue;

/// A row group a scanner read, with the rows it handed out for it.
pub struct CarryCandidate {
    /// The table the group was read from.
    pub(crate) table: Table,
    /// The group's index entry: where it lies and the tag and lengths its
    /// directory carries.
    pub(crate) group: BlockHandle,
    /// The rows of the group, in the order the scanner handed them out.
    pub(crate) rows: Arc<[InternalValue]>,
}

/// The candidates the scanners of one compaction recorded, in the order each
/// read them; shared by every input's scanner and the write side.
pub type CarryQueue = Arc<Mutex<VecDeque<CarryCandidate>>>;

/// A scanner's carry state: the rows of the group it is handing out, and where
/// it records the groups it reads.
pub struct GroupCarry {
    table: Table,
    queue: CarryQueue,
    rows: Arc<[InternalValue]>,
    next: usize,
}

impl GroupCarry {
    /// Carry state for a scanner of `table` recording into `queue`.
    pub(crate) fn new(table: Table, queue: CarryQueue) -> Self {
        Self {
            table,
            queue,
            rows: Arc::from(Vec::new()),
            next: 0,
        }
    }

    /// Starts handing out `rows`, the rows of the group `group` names, and
    /// records the group as a candidate.
    pub(crate) fn start(&mut self, group: Option<BlockHandle>, rows: Vec<InternalValue>) {
        self.rows = Arc::from(rows);
        self.next = 0;
        if let Some(group) = group
            && group.row_group().is_some()
            && !self.rows.is_empty()
        {
            self.queue.lock().push_back(CarryCandidate {
                table: self.table.clone(),
                group,
                rows: Arc::clone(&self.rows),
            });
        }
    }

    /// The next row of the current group, or `None` once it is handed out.
    pub(crate) fn next_row(&mut self) -> Option<InternalValue> {
        let row = self.rows.get(self.next)?.clone();
        self.next += 1;
        Some(row)
    }
}

impl Table {
    /// Whether this table's row groups can be carried into another table as
    /// they lie on disk: a columnar table whose groups are self-contained once
    /// re-stamped for a new place. Encrypted blocks are bound to this table's
    /// id and parity to its scheme; a positional delete mask, a restricted
    /// view or an ingested table's seqno base change what its rows read as
    /// without changing their bytes; and the zone map supplies the copy's
    /// per-column statistics.
    pub(crate) fn carries_row_groups(&self) -> bool {
        self.metadata.columnar
            && self.encryption.is_none()
            && self.metadata.ecc_params.is_none()
            && !self.metadata.ecc_unrecognized
            && !self.zone_map.is_empty()
            && self.restrict_lower_bound().is_none()
            && self.global_seqno() == 0
            && !self.has_delete_bitmap_section()
            && self.delete_bitmap.is_empty()
    }

    /// The bytes of the row group `group` names, as they lie on disk.
    ///
    /// # Errors
    ///
    /// Any I/O error reading them.
    pub(crate) fn read_row_group_raw(
        &self,
        group: &BlockHandle,
        pace: Option<&dyn super::util::ReadPacer>,
    ) -> crate::Result<crate::Slice> {
        let fd = self
            .file_accessor
            .peek_or_open_table(&self.global_id(), &self.path)?;
        Ok(crate::file::read_exact_paced(
            fd.as_ref(),
            group.offset().0,
            group.size() as usize,
            pace,
        )?)
    }
}

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
use core::cmp::Ordering;
use core::ops::Bound;

#[cfg(feature = "std")]
use parking_lot::Mutex;
// no-std: spin mirrors parking_lot's Mutex API without an allocator.
#[cfg(not(feature = "std"))]
use spin::Mutex;

use super::{BlockHandle, Table};
use crate::comparator::SharedComparator;
use crate::{InternalValue, UserKey};

/// A row group a scanner read, with the rows it handed out for it.
pub struct CarryCandidate {
    /// The table the group was read from.
    pub(crate) table: Table,
    /// The group's index entry: where it lies.
    pub(crate) group: BlockHandle,
    /// The tag and lengths the group's directory carries, as its index entry
    /// names them.
    pub(crate) row_group: super::index_block::RowGroupRef,
    /// The rows of the group, in the order the scanner handed them out; never
    /// empty.
    pub(crate) rows: Arc<[InternalValue]>,
    /// The first and the last of `rows`' keys.
    keys: (UserKey, UserKey),
}

impl CarryCandidate {
    /// The key of the group's first row.
    pub(crate) fn first_key(&self) -> &UserKey {
        &self.keys.0
    }

    /// The key of the group's last row.
    pub(crate) fn last_key(&self) -> &UserKey {
        &self.keys.1
    }
}

/// The candidates the scanners of one compaction recorded, in the order each
/// read them; shared by every input's scanner and the write side.
pub type CarryQueue = Arc<Mutex<VecDeque<CarryCandidate>>>;

/// Where the scanners of one compaction record the groups they read, and
/// which tables' groups they record.
#[derive(Clone)]
pub struct CarryTarget {
    /// Where the groups are recorded.
    pub(crate) queue: CarryQueue,
    /// The data codec of the output: a copied group keeps its pages' codec,
    /// so only a table written with this one has groups worth recording.
    pub(crate) codec: crate::CompressionType,
}

impl CarryTarget {
    /// Whether `table`'s groups may be recorded: ones that can be copied
    /// (see [`Table::carries_row_groups`]) and compressed as the output
    /// compresses, so none is read twice only to be refused.
    pub(crate) fn takes(&self, table: &Table) -> bool {
        table.carries_row_groups() && table.metadata.data_block_compression == self.codec
    }

    /// A scanner's carry state for `table`, recording into this target.
    pub(crate) fn carry_for(&self, table: &Table) -> GroupCarry {
        GroupCarry::new(table.clone(), Arc::clone(&self.queue))
    }
}

/// The key range a scanner hands out rows of, under the tree's comparator.
pub struct KeyBounds {
    /// The lower bound.
    pub(crate) lo: Bound<UserKey>,
    /// The upper bound.
    pub(crate) hi: Bound<UserKey>,
    /// The order the bounds are taken in.
    pub(crate) comparator: SharedComparator,
}

impl KeyBounds {
    /// Whether `key` lies within the bounds.
    fn contains(&self, key: &[u8]) -> bool {
        let above_lo = match &self.lo {
            Bound::Included(lo) => self.comparator.compare(key, lo) != Ordering::Less,
            Bound::Excluded(lo) => self.comparator.compare(key, lo) == Ordering::Greater,
            Bound::Unbounded => true,
        };
        above_lo
            && match &self.hi {
                Bound::Included(hi) => self.comparator.compare(key, hi) != Ordering::Greater,
                Bound::Excluded(hi) => self.comparator.compare(key, hi) == Ordering::Less,
                Bound::Unbounded => true,
            }
    }
}

/// A scanner's carry state: the rows of the group it is handing out, and where
/// it records the groups it reads.
pub struct GroupCarry {
    table: Table,
    queue: CarryQueue,
    /// The range the scan hands out rows of, `None` for the whole table.
    bounds: Option<KeyBounds>,
    rows: Arc<[InternalValue]>,
    next: usize,
}

impl GroupCarry {
    /// Carry state for a scanner of `table` recording into `queue`.
    pub(crate) fn new(table: Table, queue: CarryQueue) -> Self {
        Self {
            table,
            queue,
            bounds: None,
            rows: Arc::from(Vec::new()),
            next: 0,
        }
    }

    /// Carry state for a scanner of `table` handing out the rows within
    /// `bounds` only: a group the bounds cut is handed out in part and not
    /// recorded, since the rows outside them are another range's to write.
    pub(crate) fn bounded(table: Table, queue: CarryQueue, bounds: KeyBounds) -> Self {
        Self {
            bounds: Some(bounds),
            ..Self::new(table, queue)
        }
    }

    /// Starts handing out `rows`, the rows of the group `group` names, and
    /// records the group as a candidate.
    ///
    /// The scan is asked for a new group once the merge has taken the last
    /// row of the one before, so every key below that row has gone through
    /// the merge: a candidate ending below it can no longer be emitted, and
    /// is dropped here, whether or not the merge emitted anything since. The
    /// queue then holds about one group per input, however long a run the
    /// merge drops. A one-key group the merge still holds at that moment is
    /// dropped too, and its rows are written instead of copied.
    pub(crate) fn start(&mut self, group: Option<BlockHandle>, mut rows: Vec<InternalValue>) {
        let read = rows.len();
        if let Some(bounds) = &self.bounds {
            rows.retain(|row| bounds.contains(&row.key.user_key));
        }
        let whole = rows.len() == read;
        let mut queue = self.queue.lock();
        if let Some(passed) = self.rows.last() {
            let comparator = &self.table.comparator;
            queue.retain(|candidate| {
                comparator.compare(candidate.last_key(), &passed.key.user_key) != Ordering::Less
            });
        }
        self.rows = Arc::from(rows);
        self.next = 0;
        if whole
            && let Some(group) = group
            && let Some(row_group) = group.row_group()
            && let (Some(first), Some(last)) = (self.rows.first(), self.rows.last())
        {
            let keys = (first.key.user_key.clone(), last.key.user_key.clone());
            queue.push_back(CarryCandidate {
                table: self.table.clone(),
                group,
                row_group,
                rows: Arc::clone(&self.rows),
                keys,
            });
        }
        #[cfg(test)]
        tests::note_queue_len(queue.len());
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
    /// without changing their bytes. A destination keeping a zone map takes
    /// the copy's per-column statistics from this table's, and refuses a group
    /// it has none for.
    pub(crate) fn carries_row_groups(&self) -> bool {
        self.metadata.columnar
            && self.encryption.is_none()
            && self.metadata.ecc_params.is_none()
            && !self.metadata.ecc_unrecognized
            && self.restrict_lower_bound().is_none()
            && self.global_seqno() == 0
            && !self.has_delete_bitmap_section()
            && self.delete_bitmap.is_empty()
    }

    /// A scan of the row groups that may hold a key within `bounds`, recording
    /// them in `queue` (see [`GroupCarry::bounded`]), or `None` when none can;
    /// only a table whose groups can be carried is read this way.
    ///
    /// # Errors
    ///
    /// Any error reading the index or opening the scan.
    pub(crate) fn scan_carrying(
        &self,
        bounds: KeyBounds,
        queue: CarryQueue,
        pace: Option<&super::util::Pacer>,
    ) -> crate::Result<Option<super::Scanner>> {
        let mut groups = Vec::new();
        let walk = self.maintenance_index_walk();
        let walk = match pace {
            Some(pace) => walk.with_pace(Arc::clone(pace)),
            None => walk,
        };
        for keyed in walk {
            let keyed = keyed?;
            let end = keyed.end_key();
            // Below the lower bound: no key of the group is in range.
            let below = match &bounds.lo {
                Bound::Included(lo) => bounds.comparator.compare(end, lo) == Ordering::Less,
                Bound::Excluded(lo) => bounds.comparator.compare(end, lo) != Ordering::Greater,
                Bound::Unbounded => false,
            };
            if below {
                continue;
            }
            groups.push(*keyed.as_ref());
            // The last group that can hold a key within the upper bound. A
            // group is cut by size, so one ending at an included bound may be
            // followed by older versions of that key, and the scan goes on.
            let reaches_hi = match &bounds.hi {
                Bound::Included(hi) => bounds.comparator.compare(end, hi) == Ordering::Greater,
                Bound::Excluded(hi) => bounds.comparator.compare(end, hi) != Ordering::Less,
                Bound::Unbounded => false,
            };
            if reaches_hi {
                break;
            }
        }
        let Some(first) = groups.first() else {
            return Ok(None);
        };
        let start = first.offset().0;
        let count = groups.len();
        Ok(Some(
            self.scan_groups(count, start, groups, pace)?
                .with_carry(GroupCarry::bounded(self.clone(), queue, bounds)),
        ))
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

#[cfg(test)]
mod tests;

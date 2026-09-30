// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Row sources of a projected scan: a memtable or a row-oriented table, read
//! at the scan's snapshot into batches of the same shape a columnar segment
//! of whole values yields, so the merge takes them the same way.

use core::ops::Bound;

use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::key::InternalKey;
use crate::memtable::Memtable;
use crate::table::columnar::{ColumnBatch, entries_to_column_batch};
use crate::table::columnar_cursor::ColumnarCursor;
use crate::table::columnar_predicate::PredicateSupport;
use crate::{InternalValue, SeqNo, UserKey};

/// Rows a row source reads into one batch.
const ROWS_PER_BATCH: usize = 1_024;

/// Where a row source reads from.
enum RowInput {
    /// A row-oriented table, read by its own range iterator.
    Table(alloc::boxed::Box<dyn Iterator<Item = crate::Result<InternalValue>> + Send>),
    /// A memtable, read from the entry after the last one read, so the source
    /// holds the memtable rather than a borrow of it.
    Memtable {
        memtable: Arc<Memtable>,
        lo: Bound<InternalKey>,
        hi: Bound<InternalKey>,
    },
}

/// Reads a row source at a snapshot into batches of `columns`: the intrinsic
/// ones it is asked for, and the whole value as the value column.
pub(super) struct RowCursor {
    input: RowInput,
    snapshot: SeqNo,
    columns: Vec<u16>,
    /// The payload bytes one batch may gather: the source's part of the
    /// scan's budget.
    share: u64,
    /// Rows larger than `share` read since [`Self::take_oversized`] was last
    /// called: each forms a batch of its own.
    oversized: u64,
}

impl RowCursor {
    /// A cursor over `table`'s rows in `lo..hi`, gathering at most `share`
    /// payload bytes a batch.
    pub(super) fn table(
        table: &crate::Table,
        lo: Bound<UserKey>,
        hi: Bound<UserKey>,
        snapshot: SeqNo,
        columns: Vec<u16>,
        share: u64,
    ) -> Self {
        Self {
            input: RowInput::Table(alloc::boxed::Box::new(table.range((lo, hi)))),
            snapshot,
            columns,
            share,
            oversized: 0,
        }
    }

    /// A cursor over `memtable`'s rows in `lo..hi`, gathering at most `share`
    /// payload bytes a batch.
    pub(super) fn memtable(
        memtable: Arc<Memtable>,
        lo: &Bound<UserKey>,
        hi: &Bound<UserKey>,
        snapshot: SeqNo,
        columns: Vec<u16>,
        share: u64,
    ) -> Self {
        let (lo, hi) = internal_bounds(lo, hi);
        Self {
            input: RowInput::Memtable { memtable, lo, hi },
            snapshot,
            columns,
            share,
            oversized: 0,
        }
    }

    /// The next batch of rows visible at the snapshot, or `None` once the
    /// source is read. A batch ends at [`ROWS_PER_BATCH`] rows or once its
    /// payload reaches the share; a row larger than the share alone is
    /// still read, and counted.
    fn next_batch(&mut self) -> Option<crate::Result<ColumnBatch>> {
        let snapshot = self.snapshot;
        let share = self.share;
        let mut entries = Vec::new();
        let mut bytes: u64 = 0;
        let mut take = |row: InternalValue| {
            // Lengths of in-memory slices: their sum stays far below u64.
            bytes += (row.key.user_key.len() + row.value.len()) as u64;
            entries.push(row);
            entries.len() < ROWS_PER_BATCH && bytes < share
        };
        match &mut self.input {
            RowInput::Table(rows) => loop {
                match rows.next() {
                    // A table read to its end yields what was gathered.
                    None => break,
                    Some(Err(e)) => return Some(Err(e)),
                    // Exclusive MVCC, as every read at a snapshot.
                    Some(Ok(row)) if row.key.seqno < snapshot => {
                        if !take(row) {
                            break;
                        }
                    }
                    Some(Ok(_)) => {}
                }
            },
            RowInput::Memtable { memtable, lo, hi } => {
                for row in memtable
                    .range_internal((lo.clone(), hi.clone()))
                    .filter(|row| row.key.seqno < snapshot)
                {
                    if !take(row) {
                        break;
                    }
                }
                if let Some(last) = entries.last() {
                    *lo = Bound::Excluded(last.key.clone());
                }
            }
        }
        if entries.is_empty() {
            return None;
        }
        if entries.len() == 1 && bytes > share {
            self.oversized += 1;
        }
        Some(self.build(&entries))
    }

    /// Rows read past the share since this was last called.
    fn take_oversized(&mut self) -> u64 {
        core::mem::take(&mut self.oversized)
    }

    /// `entries` as a batch of this cursor's columns, in their order.
    fn build(&self, entries: &[InternalValue]) -> crate::Result<ColumnBatch> {
        let ColumnBatch {
            row_count,
            mut columns,
        } = entries_to_column_batch(entries)?;
        let mut out = Vec::with_capacity(self.columns.len());
        for id in &self.columns {
            if let Some(at) = columns.iter().position(|c| c.column_id == *id) {
                out.push(columns.swap_remove(at));
            }
        }
        Ok(ColumnBatch {
            row_count,
            columns: out,
        })
    }
}

/// Internal-key bounds for a user-key range: every version of a bound key is
/// inside an inclusive bound and outside an exclusive one.
pub(super) fn internal_bounds(
    lo: &Bound<UserKey>,
    hi: &Bound<UserKey>,
) -> (Bound<InternalKey>, Bound<InternalKey>) {
    let lo = match lo {
        Bound::Included(key) => Bound::Included(InternalKey::new(
            key.clone(),
            SeqNo::MAX,
            crate::ValueType::Tombstone,
        )),
        Bound::Excluded(key) => Bound::Excluded(InternalKey::new(
            key.clone(),
            0,
            crate::ValueType::Tombstone,
        )),
        Bound::Unbounded => Bound::Unbounded,
    };
    let hi = match hi {
        Bound::Included(key) => {
            Bound::Included(InternalKey::new(key.clone(), 0, crate::ValueType::Value))
        }
        Bound::Excluded(key) => Bound::Excluded(InternalKey::new(
            key.clone(),
            SeqNo::MAX,
            crate::ValueType::Value,
        )),
        Bound::Unbounded => Bound::Unbounded,
    };
    (lo, hi)
}

/// The cursor a scan source is read through: a columnar segment's, or a row
/// source's.
pub(super) enum SourceCursor {
    /// Boxed: a columnar cursor is far larger than a row cursor, and one is
    /// allocated per source opened, not per row.
    Columnar(alloc::boxed::Box<ColumnarCursor>),
    Rows(RowCursor),
}

impl SourceCursor {
    /// The next batch, or `None` once the source is read.
    pub(super) fn next(&mut self) -> Option<crate::Result<ColumnBatch>> {
        match self {
            Self::Columnar(cursor) => cursor.next(),
            Self::Rows(rows) => rows.next_batch(),
        }
    }

    /// Page bytes the source holds besides the batch it handed out.
    pub(super) fn held_bytes(&self) -> u64 {
        match self {
            Self::Columnar(cursor) => cursor.held_bytes(),
            // A row source reads a batch at a time and holds nothing ahead.
            Self::Rows(_) => 0,
        }
    }

    /// Reads past a share since this was last called.
    pub(super) fn take_oversized(&mut self) -> u64 {
        match self {
            Self::Columnar(cursor) => cursor.take_oversized(),
            Self::Rows(rows) => rows.take_oversized(),
        }
    }

    /// How far a pushed-down predicate ran; a row source takes none.
    pub(super) fn predicate_support(&self) -> PredicateSupport {
        match self {
            Self::Columnar(cursor) => cursor.predicate_support(),
            Self::Rows(_) => PredicateSupport::Exact,
        }
    }
}

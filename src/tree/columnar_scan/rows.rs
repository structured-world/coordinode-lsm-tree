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

/// Encoding bytes of a batch before its first row: the leading offset of its
/// key and value columns.
const BATCH_BASE_BYTES: u64 = 8;

/// Encoding bytes a row adds besides its key and value: an offset in each of
/// the key and value columns, its seqno and its value type.
const ROW_OVERHEAD_BYTES: u64 = 4 + 4 + 8 + 1;

/// The encoded size a row adds to a batch.
fn row_bytes(row: &InternalValue) -> u64 {
    // Lengths of in-memory slices: their sum stays far below u64.
    (row.key.user_key.len() + row.value.len()) as u64 + ROW_OVERHEAD_BYTES
}

/// The rows of one batch as they are gathered, and the size their encoding
/// takes: every intrinsic column, so the batch as held never exceeds it.
struct Gather {
    entries: Vec<InternalValue>,
    bytes: u64,
    share: u64,
}

impl Gather {
    /// Whether `row` joins the batch: the first row always does, a later one
    /// only within the share.
    fn fits(&self, row: &InternalValue) -> bool {
        self.entries.is_empty() || self.bytes + row_bytes(row) <= self.share
    }

    fn push(&mut self, row: InternalValue) {
        self.bytes += row_bytes(&row);
        self.entries.push(row);
    }

    /// Whether the batch takes more rows.
    fn open(&self) -> bool {
        self.entries.len() < ROWS_PER_BATCH && self.bytes < self.share
    }
}

/// A table's rows in key order, each key's versions newest first.
type TableRows = alloc::boxed::Box<dyn Iterator<Item = crate::Result<InternalValue>> + Send>;

/// Where a row source reads from.
enum RowInput {
    /// A row-oriented table, read by its own range iterator.
    Table {
        table: crate::Table,
        hi: Bound<UserKey>,
        rows: TableRows,
        /// Where the next batch resumes when the last one stopped before a
        /// row that did not fit: that row's key and seqno. The row is read
        /// again rather than held, so a cursor between batches holds none.
        resume: Option<(UserKey, SeqNo)>,
    },
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
            input: RowInput::Table {
                rows: alloc::boxed::Box::new(table.range((lo, hi.clone()))),
                table: table.clone(),
                hi,
                resume: None,
            },
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
    /// source is read. A batch ends at [`ROWS_PER_BATCH`] rows or before the
    /// row that would take its encoding past the share; a row larger than the
    /// share alone is still read, and counted.
    fn next_batch(&mut self) -> Option<crate::Result<ColumnBatch>> {
        let snapshot = self.snapshot;
        let mut gather = Gather {
            entries: Vec::new(),
            bytes: BATCH_BASE_BYTES,
            share: self.share,
        };
        match &mut self.input {
            RowInput::Table {
                table,
                hi,
                rows,
                resume,
            } => {
                // The row that did not fit the last batch opens this one: the
                // table is read again from its key, past the versions of it
                // the last batch took (a key's versions come newest first).
                let mut skip = resume.take();
                if let Some((key, _)) = &skip {
                    *rows = alloc::boxed::Box::new(
                        table.range((Bound::Included(key.clone()), hi.clone())),
                    );
                }
                while gather.open() {
                    match rows.next() {
                        // A table read to its end yields what was gathered.
                        None => break,
                        Some(Err(e)) => return Some(Err(e)),
                        Some(Ok(row))
                            if skip.as_ref().is_some_and(|(key, seqno)| {
                                row.key.user_key == *key && row.key.seqno > *seqno
                            }) => {}
                        // Exclusive MVCC, as every read at a snapshot.
                        Some(Ok(row)) if row.key.seqno < snapshot => {
                            skip = None;
                            if !gather.fits(&row) {
                                *resume = Some((row.key.user_key.clone(), row.key.seqno));
                                break;
                            }
                            gather.push(row);
                        }
                        Some(Ok(_)) => {}
                    }
                }
            }
            RowInput::Memtable { memtable, lo, hi } => {
                // A row that does not fit stays past `lo`, read by the next
                // batch.
                for row in memtable
                    .range_internal((lo.clone(), hi.clone()))
                    .filter(|row| row.key.seqno < snapshot)
                {
                    if !gather.fits(&row) {
                        break;
                    }
                    gather.push(row);
                    if !gather.open() {
                        break;
                    }
                }
                if let Some(last) = gather.entries.last() {
                    *lo = Bound::Excluded(last.key.clone());
                }
            }
        }
        if gather.entries.is_empty() {
            return None;
        }
        if gather.bytes > gather.share {
            self.oversized += 1;
        }
        Some(self.build(&gather.entries))
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
    /// Boxed, as the row cursor is: either is allocated once per source
    /// opened, not per row, and the enum stays a pointer wide.
    Columnar(alloc::boxed::Box<ColumnarCursor>),
    Rows(alloc::boxed::Box<RowCursor>),
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

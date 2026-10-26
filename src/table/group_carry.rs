// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Row groups and data blocks a compaction may carry into its output whole.
//!
//! A scanner reading a compaction input hands out every row as usual and, for
//! each columnar row group or row-major data block it read, records a
//! [`CarryCandidate`]: where it lies and the rows it handed out for it. The
//! compaction's write side sees what the merge made of those rows; when the
//! merge passed every row through untouched and nothing else landed among
//! them, the bytes on disk already are what the output would encode for them,
//! and they are copied instead of being encoded and compressed again.

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

/// What a candidate copies.
pub enum CarryUnit {
    /// A columnar row group: the tag and lengths its directory carries, as its
    /// index entry names them. Its bytes are read when it is copied.
    #[cfg(feature = "columnar")]
    RowGroup(super::index_block::RowGroupRef),
    /// A row-major data block: its frame, header and payload, as the scan read
    /// it and verified its checksum, so the copy is of the bytes the rows were
    /// decoded from.
    DataBlock(crate::Slice),
}

/// A row group or data block a scanner read, with the rows it handed out for
/// it.
pub struct CarryCandidate {
    /// The table it was read from.
    pub(crate) table: Table,
    /// Where it lies: its index entry for a group, its extent for a block.
    pub(crate) group: BlockHandle,
    /// What it is.
    pub(crate) unit: CarryUnit,
    /// Its rows, in the order the scanner handed them out; never empty.
    pub(crate) rows: Arc<[InternalValue]>,
    /// The first and the last of `rows`' keys.
    keys: (UserKey, UserKey),
    /// The bytes of `rows`' keys and values.
    bytes: u64,
}

/// The bytes of `row`'s key and value, as a carry counts what it holds.
pub fn row_bytes(row: &InternalValue) -> u64 {
    (row.key.user_key.len() + row.value.len()) as u64
}

impl CarryCandidate {
    /// The bytes of the group's rows' keys and values.
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }

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

/// How the output lays out its data, which a copy keeps.
#[derive(Clone, Copy)]
pub(crate) enum CarryShape {
    /// Columnar row groups, cut and encoded as this says.
    #[cfg(feature = "columnar")]
    Groups(crate::table::meta::GroupShape),
    /// Row-major data blocks with restart heads every `restart_interval`
    /// entries: a table's locator names a key's restart head by that interval,
    /// so a block copied in must have been written with it.
    Blocks {
        /// The output's data block restart interval.
        restart_interval: u8,
    },
}

/// Where the scanners of one compaction record the groups and blocks they
/// read, and which tables' they record.
#[derive(Clone)]
pub struct CarryTarget {
    /// Where the candidates are recorded.
    pub(crate) queue: CarryQueue,
    /// The data codec of the output: a copy keeps its codec and dictionary,
    /// so only a table written with this one has data worth recording.
    pub(crate) codec: crate::CompressionType,
    /// How the output lays out its data: only a table laid out the same has
    /// data worth recording.
    pub(crate) shape: CarryShape,
}

impl CarryTarget {
    /// Whether `table`'s groups or blocks may be recorded: ones that can be
    /// copied (see [`Table::carries_row_groups`] and
    /// [`Table::carries_data_blocks`]), compressed as the output compresses
    /// and laid out as it writes them, so none is held only to be refused
    /// and none lands in a table that would read it otherwise.
    pub(crate) fn takes(&self, table: &Table) -> bool {
        if table.metadata.data_block_compression != self.codec {
            return false;
        }
        match self.shape {
            #[cfg(feature = "columnar")]
            CarryShape::Groups(shape) => {
                table.carries_row_groups() && table.metadata.group_shape == Some(shape)
            }
            CarryShape::Blocks { restart_interval } => {
                table.carries_data_blocks()
                    && table.metadata.data_block_restart_interval == restart_interval
            }
        }
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

    /// Starts handing out `rows`, the rows of the group or block `group` names,
    /// and records it as a candidate: a block when `frame` holds its bytes as
    /// read, else a group when `group` names one.
    ///
    /// A block split into several inner frames is not recorded: their
    /// boundaries come from the table's layout section, which no checksum of
    /// the block covers, and a copy would carry them unchecked.
    ///
    /// The scan is asked for a new group once the merge has taken the last
    /// row of the one before, so every key below that row has gone through
    /// the merge: a candidate ending below it can no longer be emitted, and
    /// is dropped here, whether or not the merge emitted anything since. One
    /// ending on that key is dropped too: a group whose earlier keys went
    /// through the merge either opened on them and left the queue or can no
    /// longer be copied whole, and a group of that key alone, whose versions
    /// the merge may still be emitting, is written as rows instead. The queue
    /// then holds about one group per input, however long a run the merge
    /// drops and however many groups one key's versions fill.
    pub(crate) fn start(
        &mut self,
        group: Option<BlockHandle>,
        frame: Option<crate::Slice>,
        mut rows: Vec<InternalValue>,
    ) {
        let read = rows.len();
        if let Some(bounds) = &self.bounds {
            rows.retain(|row| bounds.contains(&row.key.user_key));
        }
        let whole = rows.len() == read;
        let bytes = rows.iter().map(row_bytes).sum();
        let mut queue = self.queue.lock();
        if let Some(passed) = self.rows.last() {
            let comparator = &self.table.comparator;
            queue.retain(|candidate| {
                comparator.compare(candidate.last_key(), &passed.key.user_key) == Ordering::Greater
            });
        }
        self.rows = Arc::from(rows);
        self.next = 0;
        let unit = match (group, frame) {
            (Some(group), Some(frame)) => (!self.table.has_inner_block_layout(group.offset().0))
                .then_some(CarryUnit::DataBlock(frame)),
            #[cfg(feature = "columnar")]
            (Some(group), None) => group.row_group().map(CarryUnit::RowGroup),
            _ => None,
        };
        if whole
            && let (Some(group), Some(unit)) = (group, unit)
            && let (Some(first), Some(last)) = (self.rows.first(), self.rows.last())
        {
            let keys = (first.key.user_key.clone(), last.key.user_key.clone());
            queue.push_back(CarryCandidate {
                table: self.table.clone(),
                group,
                unit,
                rows: Arc::clone(&self.rows),
                keys,
                bytes,
            });
        }
        #[cfg(all(test, feature = "columnar"))]
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
    #[cfg(feature = "columnar")]
    pub(crate) fn carries_row_groups(&self) -> bool {
        self.metadata.columnar && self.carries_as_stored() && !self.has_delete_bitmap_section()
    }

    /// Whether this table's data blocks can be carried into another table as
    /// they lie on disk: a row-major table whose blocks are self-contained
    /// once re-stamped for a new place, for the reasons
    /// [`Self::carries_row_groups`] gives.
    pub(crate) fn carries_data_blocks(&self) -> bool {
        !self.metadata.columnar && self.carries_as_stored()
    }

    /// Whether this table's rows read as its bytes say: neither encrypted nor
    /// parity-protected, no restricted view, no ingested seqno base and no
    /// positional delete mask.
    fn carries_as_stored(&self) -> bool {
        self.encryption.is_none()
            && self.metadata.ecc_params.is_none()
            && !self.metadata.ecc_unrecognized
            && self.restrict_lower_bound().is_none()
            && self.global_seqno() == 0
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
        use super::block_index::BlockIndexIter;

        // A table wholly past the upper bound holds none of the range's keys.
        // The walk would still find its first group, which ends past the
        // bound, and open a scan that holds a file and its first chunk for as
        // long as the compaction reading the range runs.
        let min = self.metadata.key_range.min();
        let past = match &bounds.hi {
            Bound::Included(hi) => bounds.comparator.compare(min, hi) == Ordering::Greater,
            Bound::Excluded(hi) => bounds.comparator.compare(min, hi) != Ordering::Less,
            Bound::Unbounded => false,
        };
        if past {
            return Ok(None);
        }
        let mut groups = Vec::new();
        let walk = self.maintenance_index_walk();
        let mut walk = match pace {
            Some(pace) => walk.with_pace(Arc::clone(pace)),
            None => walk,
        };
        // Straight to the first group ending at or past the lower bound, as a
        // bounded read seeks, rather than through every entry before it: a
        // range of a table's tail reads the index from there on.
        if let Bound::Included(lo) | Bound::Excluded(lo) = &bounds.lo
            && !walk.seek_lower(lo, crate::SeqNo::MAX)
        {
            return Ok(None);
        }
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
    #[cfg(feature = "columnar")]
    pub(crate) fn read_row_group_raw(
        &self,
        group: &BlockHandle,
        pace: Option<&dyn super::util::ReadPacer>,
    ) -> crate::Result<crate::Slice> {
        #[cfg(test)]
        tests::note_raw_read(group.size());
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

#[cfg(all(test, feature = "columnar"))]
mod tests;

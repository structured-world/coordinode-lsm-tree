// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::{Choice, CompactionStrategy};
use crate::compaction::state::CompactionState;
use crate::compaction::worker::proven_older;
use crate::version::Version;
use crate::{HashSet, Table};
use crate::{KeyRange, config::Config, slice::Slice, version::run::Ranged};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::ops::{Bound, RangeBounds};

#[cfg(test)]
mod tests;

#[derive(Clone, Debug)]
pub struct OwnedBounds {
    pub start: Bound<Slice>,
    pub end: Bound<Slice>,
}

impl RangeBounds<Slice> for OwnedBounds {
    fn start_bound(&self) -> Bound<&Slice> {
        match &self.start {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(key) => Bound::Included(key),
            Bound::Excluded(key) => Bound::Excluded(key),
        }
    }

    fn end_bound(&self) -> Bound<&Slice> {
        match &self.end {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(key) => Bound::Included(key),
            Bound::Excluded(key) => Bound::Excluded(key),
        }
    }
}

impl OwnedBounds {
    /// Returns `true` if the key range is fully contained in these bounds,
    /// using the given comparator for key ordering.
    #[must_use]
    pub fn contains(&self, range: &KeyRange, cmp: &dyn crate::comparator::UserComparator) -> bool {
        use core::cmp::Ordering;

        let lower_ok = match &self.start {
            Bound::Unbounded => true,
            Bound::Included(key) => cmp.compare(key.as_ref(), range.min()) != Ordering::Greater,
            Bound::Excluded(key) => cmp.compare(key.as_ref(), range.min()) == Ordering::Less,
        };

        if !lower_ok {
            return false;
        }

        match &self.end {
            Bound::Unbounded => true,
            Bound::Included(key) => cmp.compare(key.as_ref(), range.max()) != Ordering::Less,
            Bound::Excluded(key) => cmp.compare(key.as_ref(), range.max()) == Ordering::Greater,
        }
    }
}

/// Drops the tables **contained** in a key range whose removal exposes no
/// older version of any key.
///
/// A table inside the bounds can shadow an older version of one of its keys
/// held by a table that reaches past them, which stays: its deletion or newer
/// value is what hides that version. Such a table is kept, and with it every
/// newer table over its keys, so the drop never brings a deleted or
/// overwritten value back.
pub struct Strategy {
    bounds: OwnedBounds,
}

impl Strategy {
    /// Configures a new `DropRange` compaction strategy.
    #[must_use]
    pub fn new(bounds: OwnedBounds) -> Self {
        Self { bounds }
    }
}

impl CompactionStrategy for Strategy {
    fn get_name(&self) -> &'static str {
        "DropRangeCompaction"
    }

    fn choose(&self, version: &Version, config: &Config, state: &CompactionState) -> Choice {
        let cmp = config.comparator.as_ref();

        let mut candidates: Vec<&Table> = version
            .iter_levels()
            .flat_map(|lvl| lvl.iter())
            .flat_map(|run| {
                run.range_overlap_indexes_cmp(&self.bounds, cmp)
                    .and_then(|(lo, hi)| run.get(lo..=hi))
                    .unwrap_or_default()
                    .iter()
                    .filter(|x| self.bounds.contains(x.key_range(), cmp))
            })
            .collect();
        let mut table_ids: HashSet<_> = candidates.iter().map(|t| t.id()).collect();

        // Keeping a table can make a newer one over its keys unsafe to drop,
        // so the kept set grows until nothing more joins it. Each pass keeps at
        // least one more table or ends, so at most one pass per candidate.
        loop {
            let (keep, drop): (Vec<&Table>, Vec<&Table>) = candidates
                .into_iter()
                .partition(|table| !drops_without_exposing(table, version, &table_ids, cmp));
            candidates = drop;
            if keep.is_empty() {
                break;
            }
            for table in keep {
                table_ids.remove(&table.id());
            }
        }

        // NOTE: This should generally not occur because of the
        // tree-level major compaction lock
        // But just as a fail-safe...
        let some_hidden = table_ids.iter().any(|&id| state.hidden_set().is_hidden(id));

        // No fully contained table is a no-op, not an empty drop (an empty drop
        // would still install a version edit); same shape as the FIFO strategy.
        if some_hidden || table_ids.is_empty() {
            Choice::DoNothing
        } else {
            Choice::Drop(table_ids)
        }
    }
}

/// Whether dropping `table` together with the tables of `dropped` leaves no
/// older version of its keys visible: every other table over the keys it
/// holds or its range tombstones cover is strictly newer than all of it.
///
/// Sequence numbers order the versions of a key, so a table whose lowest one
/// is above `table`'s highest holds only newer versions, which the drop does
/// not uncover. A table a bottommost compaction zeroed reads as oldest, so it
/// keeps whatever overlaps it, and so does a table whose seqno range does not
/// fit the seqno space: its age is unknown.
fn drops_without_exposing(
    table: &Table,
    version: &Version,
    dropped: &HashSet<crate::TableId>,
    cmp: &dyn crate::comparator::UserComparator,
) -> bool {
    let extent = covered_keys(table, cmp);
    version.iter_levels().flat_map(|lvl| lvl.iter()).all(|run| {
        run.range_overlap_indexes_cmp(&extent, cmp)
            .and_then(|(lo, hi)| run.get(lo..=hi))
            .unwrap_or_default()
            .iter()
            .all(|other| dropped.contains(&other.id()) || proven_older(table, other))
    })
}

/// The keys `table` holds or its range tombstones cover. A tombstone may reach
/// past the table's key range (an output cut at its last key keeps the
/// tombstone to that key's successor), and dropping the table drops its
/// coverage too. The exclusive tombstone end is taken as included, which only
/// widens the overlap test.
fn covered_keys(table: &Table, cmp: &dyn crate::comparator::UserComparator) -> OwnedBounds {
    use core::cmp::Ordering;

    let mut lower = table.key_range().min().clone();
    let mut upper = table.key_range().max().clone();
    for rt in table.range_tombstones() {
        if cmp.compare(&rt.start, &lower) == Ordering::Less {
            lower = rt.start.clone();
        }
        if cmp.compare(&rt.end, &upper) == Ordering::Greater {
            upper = rt.end.clone();
        }
    }
    OwnedBounds {
        start: Bound::Included(lower),
        end: Bound::Included(upper),
    }
}

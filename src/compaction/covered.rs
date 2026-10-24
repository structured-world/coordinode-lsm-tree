// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Whole tables a range tombstone deletes for every reader the tree serves.

use crate::compaction::state::hidden_set::HiddenSet;
use crate::comparator::UserComparator;
use crate::version::{RetentionEffect, Version, run::Ranged};
use crate::{HashMap, SeqNo, TableId};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::ops::Bound;

/// The tables a range tombstone of ANOTHER table of `version` deletes for every
/// snapshot at or above `gc_watermark`, with what dropping them owes older
/// snapshots: a refusal up to the newest tombstone a dropped table needs.
///
/// A table goes when a tombstone `R` held by another table
/// - is visible at the watermark (`R.seqno < gc_watermark`),
/// - is newer than every version and tombstone in it (`newest < R.seqno`), and
/// - spans its whole key range, which the writer widens over the table's own
///   range tombstones, so dropping it uncovers nothing `R` does not cover too.
///
/// Only tombstones in tables count: one still in a memtable is not durable
/// until its flush, and a drop that survived a crash the tombstone did not
/// would uncover the older versions it hid. A dropped table may itself hold
/// the tombstone that covers another: it is covered by a newer, wider one,
/// and the newest covering tombstone's table is never covered, so it stays.
/// Hidden tables belong to a running compaction and are left to it.
///
/// `None` when no table qualifies.
pub fn covered_tables(
    version: &Version,
    gc_watermark: SeqNo,
    hidden: &HiddenSet,
    cmp: &dyn UserComparator,
) -> Option<(Vec<TableId>, RetentionEffect)> {
    // Per table, the oldest tombstone covering it: a snapshot past that one
    // already reads the table as deleted, so it bounds the floor the drop owes.
    let mut covered: HashMap<TableId, SeqNo> = HashMap::default();

    for holder in version
        .iter_tables()
        .filter(|t| !t.range_tombstones().is_empty())
    {
        for rt in holder.visible_range_tombstones() {
            if !rt.visible_at(gc_watermark) {
                continue;
            }
            let span = (
                Bound::Included(rt.start.as_ref()),
                Bound::Excluded(rt.end.as_ref()),
            );
            for run in version.iter_levels().flat_map(|level| level.iter()) {
                let Some(overlapping) = run
                    .range_overlap_indexes_cmp::<&[u8], _, _>(&span, cmp)
                    .and_then(|(lo, hi)| run.get(lo..=hi))
                else {
                    continue;
                };
                for table in overlapping {
                    if table.id() == holder.id() || hidden.is_hidden(table.id()) {
                        continue;
                    }
                    // A table whose bounds do not fit the seqno space has no
                    // provable age, so no tombstone proves it older.
                    let Some((_, newest)) = table.seqno_range() else {
                        continue;
                    };
                    if newest >= rt.seqno {
                        continue;
                    }
                    let range = table.key_range();
                    if !rt.fully_covers_with(range.min(), range.max(), cmp) {
                        continue;
                    }
                    covered
                        .entry(table.id())
                        .and_modify(|oldest| *oldest = (*oldest).min(rt.seqno))
                        .or_insert(rt.seqno);
                }
            }
        }
    }

    // Every snapshot past this tombstone reads each dropped table as deleted.
    // It is below the watermark, so the successor is a seqno.
    let floor = covered.values().copied().max()?;
    let mut ids: Vec<TableId> = covered.into_keys().collect();
    ids.sort_unstable();
    Some((ids, RetentionEffect::GcBelow(floor + 1)))
}

#[cfg(test)]
mod tests;

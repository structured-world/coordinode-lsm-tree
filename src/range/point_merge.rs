// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Merge resolution of one key, read source by source.
//!
//! The memtables and the tables that may hold a version of the key are read
//! newest first, by the highest seqno each holds, as `RocksDB`'s
//! `Version::Get` collects operands in its `MergeContext`. Once a base is
//! found (a value, a point tombstone, or a range tombstone holding the key),
//! a source whose highest seqno is below it holds only versions the base
//! hides and tombstones too old to hide anything the merge reads, so it is
//! not opened. Ordering by seqno rather than by level keeps the stop right
//! where a deeper table holds newer data, as an ingestion with caller-chosen
//! seqnos can place it.
//!
//! [`MergeWalk`] holds that walk; a reader drives it table by table. A point
//! read does so on the calling thread ([`resolve_point_merge`]); a batched
//! read brings each table's blocks in with its other reads first, then reads
//! the versions the same way.

use super::{FilterAnswer, FilterQuery, filter_answer, seqno_filter, table_reader};
use crate::{
    InternalValue, SeqNo, UserKey, ValueType,
    blob_tree::BlobSource,
    key::InternalKey,
    memtable::Memtable,
    merge_operator::MergeOperator,
    mvcc_stream::{MvccStream, ValueLog},
    range_tombstone::RangeTombstone,
    table::Table,
    version::SuperVersion,
};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::ops::Bound;

/// A place a version of the key may be held.
#[derive(Clone, Copy)]
enum Source<'a> {
    Memtable(&'a Memtable),
    /// A table, with its level, its run in the level and its place in the run.
    Table(&'a Table, (usize, usize, usize)),
}

/// The merge of one key, read source by source: [`Self::next_table`] reads
/// the memtables up to the next table and hands that table out,
/// [`Self::take_table`] takes its versions of the key, and once no table is
/// left [`Self::finish`] resolves them.
pub struct MergeWalk<'v> {
    super_version: &'v SuperVersion,
    key: UserKey,
    seqno: SeqNo,
    /// Each source with its highest seqno and its recency rank: its place in
    /// the point read's order, memtables newest first, then the levels top
    /// down. Two sources holding the key at one seqno are ordered by the
    /// rank, which the highest seqno, taken over every key, cannot stand for.
    sources: Vec<(SeqNo, usize, Source<'v>)>,
    /// The next source to read.
    next: usize,
    /// Each version with the rank of the source it was read from.
    entries: Vec<(usize, InternalValue)>,
    /// Each with the read's seqno, the cutoff the merge checks it against.
    tombstones: Vec<(RangeTombstone, SeqNo)>,
    /// The seqno of the newest base found: what is below it is hidden.
    floor: Option<SeqNo>,
    /// The table handed out and not taken back: where its versions and
    /// tombstones start, and its rank.
    pending: Option<(usize, usize, usize)>,
}

/// A table [`MergeWalk::next_table`] hands out, and its place in the version:
/// level, run in the level, position in the run.
pub struct MergeTable<'v> {
    pub table: &'v Table,
    pub at: (usize, usize, usize),
}

impl<'v> MergeWalk<'v> {
    /// The merge of `key` at `seqno` in `super_version`, its sources found
    /// and ordered, none read yet.
    pub(crate) fn new(super_version: &'v SuperVersion, key: &[u8], seqno: SeqNo) -> Self {
        let comparator = super_version.active_memtable.comparator.as_ref();
        let mut sources: Vec<(SeqNo, usize, Source<'v>)> = Vec::new();
        for memtable in core::iter::once(&super_version.active_memtable)
            .chain(super_version.sealed_memtables.iter().rev())
        {
            if let Some(highest) = memtable.get_highest_seqno() {
                sources.push((highest, sources.len(), Source::Memtable(memtable)));
            }
        }
        let bounds = (Bound::Included(key), Bound::Included(key));
        for (level_idx, level) in super_version.version.iter_levels().enumerate() {
            for (run_idx, run) in level.iter().enumerate() {
                let Some((lo, hi)) =
                    run.range_overlap_indexes_cmp::<&[u8], _, _>(&bounds, comparator)
                else {
                    continue;
                };
                for (pos, table) in run
                    .get(lo..=hi)
                    .unwrap_or_default()
                    .iter()
                    .enumerate()
                    .map(|(at, table)| (lo + at, table))
                {
                    if table.check_key_range_overlap_cmp(&bounds, comparator) {
                        sources.push((
                            table.get_highest_seqno(),
                            sources.len(),
                            Source::Table(table, (level_idx, run_idx, pos)),
                        ));
                    }
                }
            }
        }
        sources.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        Self {
            super_version,
            key: UserKey::from(key),
            seqno,
            sources,
            next: 0,
            entries: Vec::new(),
            tombstones: Vec::new(),
            floor: None,
            pending: None,
        }
    }

    /// Reads the memtables up to the next table and hands that table out,
    /// the range tombstones of it that hold the key taken already; `None` once
    /// no source left can hold a version the base found so far does not hide.
    ///
    /// # Errors
    ///
    /// A memtable's versions fail to read.
    pub(crate) fn next_table(&mut self) -> crate::Result<Option<MergeTable<'v>>> {
        debug_assert!(
            self.pending.is_none(),
            "a table handed out was not taken back"
        );
        let super_version = self.super_version;
        let comparator = super_version.active_memtable.comparator.as_ref();
        // A handle on the key, so the walk's state can be updated while it is
        // read.
        let owned_key = self.key.clone();
        let key = &owned_key[..];
        while let Some(&(highest, rank, source)) = self.sources.get(self.next) {
            if self.floor.is_some_and(|floor| highest < floor) {
                // Every source left holds a highest seqno at most this one's.
                self.next = self.sources.len();
                break;
            }
            self.next += 1;
            let (entries_before, tombstones_before) = (self.entries.len(), self.tombstones.len());
            match source {
                Source::Memtable(memtable) => {
                    let seqno = self.seqno;
                    memtable.for_each_range_tombstone_containing(key, |rt| {
                        self.tombstones.push((rt.clone(), seqno));
                    });
                    let range = (
                        Bound::Included(InternalKey::bound(
                            self.key.clone(),
                            SeqNo::MAX,
                            ValueType::Tombstone,
                        )),
                        Bound::Included(InternalKey::bound(self.key.clone(), 0, ValueType::Value)),
                    );
                    take_versions(
                        memtable
                            .range_internal(range)
                            .filter(|item| seqno_filter(item.key.seqno, seqno))
                            .map(Ok),
                        rank,
                        &mut self.entries,
                    )?;
                    self.raise_floor(entries_before, tombstones_before);
                }
                Source::Table(table, at) => {
                    // A table's tombstones are sorted by start: those starting
                    // past the key cannot hold it.
                    let starting = table.range_tombstones().partition_point(|rt| {
                        comparator.compare(&rt.start, key) != core::cmp::Ordering::Greater
                    });
                    self.tombstones.extend(
                        table
                            .range_tombstones()
                            .iter()
                            .take(starting)
                            .filter(|rt| rt.contains_key_with(key, comparator))
                            .map(|rt| (rt.clone(), self.seqno)),
                    );
                    self.pending = Some((entries_before, tombstones_before, rank));
                    return Ok(Some(MergeTable { table, at }));
                }
            }
        }
        Ok(None)
    }

    /// Takes `versions`, the versions of the key the table handed out last
    /// holds at the merge's seqno, newest first.
    ///
    /// # Errors
    ///
    /// A version fails to read.
    pub(crate) fn take_table(
        &mut self,
        versions: impl Iterator<Item = crate::Result<InternalValue>>,
    ) -> crate::Result<()> {
        let Some((entries_before, tombstones_before, rank)) = self.pending.take() else {
            debug_assert!(false, "versions handed back for no table");
            return Ok(());
        };
        take_versions(versions, rank, &mut self.entries)?;
        self.raise_floor(entries_before, tombstones_before);
        Ok(())
    }

    /// Raises the floor to the newest base among the versions and tombstones
    /// read since `entries_before` and `tombstones_before`.
    fn raise_floor(&mut self, entries_before: usize, tombstones_before: usize) {
        let seqno = self.seqno;
        let bases = self
            .entries
            .get(entries_before..)
            .unwrap_or_default()
            .iter()
            .filter(|(_, entry)| !entry.key.value_type.is_merge_operand())
            .map(|(_, entry)| entry.key.seqno);
        let hiding = self
            .tombstones
            .get(tombstones_before..)
            .unwrap_or_default()
            .iter()
            .filter(|(rt, _)| rt.visible_at(seqno))
            .map(|(rt, _)| rt.seqno);
        self.floor = self.floor.max(bases.chain(hiding).max());
    }

    /// The value the operands read make over the newest base, or `None` when
    /// the key reads as deleted. `blob_source` reads a base kept in the value
    /// log, which only a blob tree's index holds.
    ///
    /// # Errors
    ///
    /// The merge operator fails, or a base in the value log fails to read.
    pub(crate) fn finish(
        self,
        merge_operator: &Arc<dyn MergeOperator>,
        blob_source: Option<&BlobSource>,
    ) -> crate::Result<Option<InternalValue>> {
        let Self {
            super_version,
            key,
            mut entries,
            tombstones,
            ..
        } = self;
        let comparator = super_version.active_memtable.comparator.as_ref();
        // Newest first; at one seqno the newer source first, as the point read
        // and a merging read take it. Stable: one batch's operands of the key
        // share a seqno and a source, and an operator may depend on their
        // order, which each source hands over as written.
        entries.sort_by(|(a_rank, a), (b_rank, b)| {
            b.key.seqno.cmp(&a.key.seqno).then(a_rank.cmp(b_rank))
        });
        let Some((_, head)) = entries.first() else {
            return Ok(None);
        };
        if tombstones
            .iter()
            .any(|(rt, cutoff)| rt.should_suppress_with(&key, head.key.seqno, *cutoff, comparator))
        {
            return Ok(None);
        }

        let resolved = MvccStream::new_with_comparator(
            entries.into_iter().map(|(_, entry)| Ok(entry)),
            Some(Arc::clone(merge_operator)),
            super_version.active_memtable.comparator.clone(),
        )
        .with_value_log(blob_source.map(|source| ValueLog {
            source,
            version: &super_version.version,
        }))
        .with_range_tombstones(tombstones)
        .next()
        .transpose()?;
        Ok(resolved.filter(|value| !value.key.is_tombstone()))
    }
}

/// What a table's filters said of a merge's key, the probe counted.
#[derive(Clone, Copy)]
pub struct TableFilter(FilterAnswer);

impl TableFilter {
    /// Asks `table`'s filters about `key`, counting the probe they answer.
    pub(crate) fn of(table: &Table, key: &[u8]) -> Self {
        // The partition-aware filter seeks by the key itself.
        Self(filter_answer(
            FilterQuery {
                prefix_hash: None,
                key_hash: Some(crate::hash::hash64(key)),
                bloom_key: Some(key),
                #[cfg(feature = "metrics")]
                metrics: None,
            },
            table,
        ))
    }

    /// Whether a filter rules the table out.
    pub(crate) fn absent(self) -> bool {
        self.0 == FilterAnswer::Absent
    }

    /// Counts the false positive of the probe a filter let through when
    /// `table`, read for the key, `found` no version of it at all, as the
    /// table's reader counts it in [`Self::versions`].
    pub(crate) fn settle(self, table: &Table, found: bool) {
        if !found && self.0.passed() && table.probe_stats().is_some() {
            table.count_probes(crate::table::probe_stats::ProbeCounts {
                probes: 0,
                negatives: 1,
            });
        }
    }

    /// `table`'s versions of `key` at `seqno`, newest first, read through the
    /// table's reader, which counts the filter's false positive when it holds
    /// none. Nothing for a table the filter ruled out.
    pub(crate) fn versions<'t>(
        self,
        table: &'t Table,
        key: &[u8],
        seqno: SeqNo,
    ) -> impl Iterator<Item = crate::Result<InternalValue>> + 't {
        let reader = (!self.absent()).then(|| {
            let key = UserKey::from(key);
            table_reader(
                self.0,
                table,
                (Bound::Included(key.clone()), Bound::Included(key)),
                seqno,
            )
        });
        reader.into_iter().flatten()
    }
}

/// Resolves the merge of `key` at `seqno` in `super_version`: the value the
/// operands visible there make over the newest base, or `None` when the key
/// reads as deleted. `blob_source` reads a base kept in the value log, which
/// only a blob tree's index holds.
pub fn resolve_point_merge(
    super_version: &SuperVersion,
    key: &[u8],
    seqno: SeqNo,
    merge_operator: &Arc<dyn MergeOperator>,
    blob_source: Option<&BlobSource>,
) -> crate::Result<Option<InternalValue>> {
    let mut walk = MergeWalk::new(super_version, key, seqno);
    while let Some(next) = walk.next_table()? {
        let filter = TableFilter::of(next.table, key);
        walk.take_table(filter.versions(next.table, key, seqno))?;
    }
    walk.finish(merge_operator, blob_source)
}

/// Takes one source's versions of the key, newest first, down to its first
/// base: the source's older versions are hidden by it. Each is kept with the
/// source's recency `rank`.
fn take_versions(
    versions: impl Iterator<Item = crate::Result<InternalValue>>,
    rank: usize,
    entries: &mut Vec<(usize, InternalValue)>,
) -> crate::Result<()> {
    for version in versions {
        let version = version?;
        let base = !version.key.value_type.is_merge_operand();
        entries.push((rank, version));
        if base {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;

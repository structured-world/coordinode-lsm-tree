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
enum Source<'a> {
    Memtable(&'a Memtable),
    Table(&'a Table),
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
    let comparator = super_version.active_memtable.comparator.as_ref();
    // The partition-aware filter seeks by the key itself.
    let filter_query = FilterQuery {
        prefix_hash: None,
        key_hash: Some(crate::hash::hash64(key)),
        bloom_key: Some(key),
        #[cfg(feature = "metrics")]
        metrics: None,
    };
    // The bounds of the memtable and table reads share this one copy.
    let user_key = UserKey::from(key);

    // Each source with its highest seqno and its recency rank: its place in
    // the point read's order, memtables newest first, then the levels top
    // down. Two sources holding the key at one seqno are ordered by the rank,
    // which the highest seqno, taken over every key, cannot stand for.
    let mut sources: Vec<(SeqNo, usize, Source<'_>)> = Vec::new();
    for memtable in core::iter::once(&super_version.active_memtable)
        .chain(super_version.sealed_memtables.iter().rev())
    {
        if let Some(highest) = memtable.get_highest_seqno() {
            sources.push((highest, sources.len(), Source::Memtable(memtable)));
        }
    }
    let bounds = (Bound::Included(key), Bound::Included(key));
    for run in super_version
        .version
        .iter_levels()
        .flat_map(|level| level.iter())
    {
        let Some((lo, hi)) = run.range_overlap_indexes_cmp::<&[u8], _, _>(&bounds, comparator)
        else {
            continue;
        };
        for table in run.get(lo..=hi).unwrap_or_default() {
            if table.check_key_range_overlap_cmp(&bounds, comparator) {
                sources.push((
                    table.get_highest_seqno(),
                    sources.len(),
                    Source::Table(table),
                ));
            }
        }
    }
    sources.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));

    // Each version with the rank of the source it was read from.
    let mut entries: Vec<(usize, InternalValue)> = Vec::new();
    // Each with the read's seqno, the cutoff the merge checks it against.
    let mut tombstones: Vec<(RangeTombstone, SeqNo)> = Vec::new();
    // The seqno of the newest base found: what is below it is hidden.
    let mut floor: Option<SeqNo> = None;

    for &(highest, rank, ref source) in &sources {
        if floor.is_some_and(|floor| highest < floor) {
            // Every source left holds a highest seqno at most this one's.
            break;
        }
        let (entries_before, tombstones_before) = (entries.len(), tombstones.len());
        match source {
            Source::Memtable(memtable) => {
                memtable.for_each_range_tombstone_containing(key, |rt| {
                    tombstones.push((rt.clone(), seqno));
                });
                let range = (
                    Bound::Included(InternalKey::new(
                        user_key.clone(),
                        SeqNo::MAX,
                        ValueType::Tombstone,
                    )),
                    Bound::Included(InternalKey::new(user_key.clone(), 0, ValueType::Value)),
                );
                take_versions(
                    memtable
                        .range_internal(range)
                        .filter(|item| seqno_filter(item.key.seqno, seqno))
                        .map(Ok),
                    rank,
                    &mut entries,
                )?;
            }
            Source::Table(table) => {
                // A table's tombstones are sorted by start: those starting
                // past the key cannot hold it.
                let starting = table.range_tombstones().partition_point(|rt| {
                    comparator.compare(&rt.start, key) != core::cmp::Ordering::Greater
                });
                tombstones.extend(
                    table
                        .range_tombstones()
                        .iter()
                        .take(starting)
                        .filter(|rt| rt.contains_key_with(key, comparator))
                        .map(|rt| (rt.clone(), seqno)),
                );
                let answer = filter_answer(filter_query, table);
                if answer != FilterAnswer::Absent {
                    take_versions(
                        table_reader(
                            answer,
                            *table,
                            (
                                Bound::Included(user_key.clone()),
                                Bound::Included(user_key.clone()),
                            ),
                            seqno,
                        ),
                        rank,
                        &mut entries,
                    )?;
                }
            }
        }

        let bases = entries
            .get(entries_before..)
            .unwrap_or_default()
            .iter()
            .filter(|(_, entry)| !entry.key.value_type.is_merge_operand())
            .map(|(_, entry)| entry.key.seqno);
        let hiding = tombstones
            .get(tombstones_before..)
            .unwrap_or_default()
            .iter()
            .filter(|(rt, _)| rt.visible_at(seqno))
            .map(|(rt, _)| rt.seqno);
        floor = floor.max(bases.chain(hiding).max());
    }

    // Newest first; at one seqno the newer source first, as the point read
    // and a merging read take it.
    entries.sort_unstable_by(|(a_rank, a), (b_rank, b)| {
        b.key.seqno.cmp(&a.key.seqno).then(a_rank.cmp(b_rank))
    });
    let Some((_, head)) = entries.first() else {
        return Ok(None);
    };
    if tombstones
        .iter()
        .any(|(rt, cutoff)| rt.should_suppress_with(key, head.key.seqno, *cutoff, comparator))
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

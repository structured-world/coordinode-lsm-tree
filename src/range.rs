// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::{
    BoxedIterator, InternalValue,
    comparator::SharedComparator,
    key::InternalKey,
    memtable::Memtable,
    merge_operator::MergeOperator,
    merge_source::CoherentIterSource,
    mvcc_stream::MvccStream,
    range_tombstone::RangeTombstone,
    range_tombstone_filter::RangeTombstoneFilter,
    run_reader::RunReader,
    seeking_merger::SeekingMerger,
    value::{SeqNo, UserKey},
    version::{Run, SuperVersion},
};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};
use core::ops::{Bound, RangeBounds};

use self_cell::self_cell;

#[must_use]
pub fn seqno_filter(item_seqno: SeqNo, seqno: SeqNo) -> bool {
    item_seqno < seqno
}

/// Wrap a vector of boxed double-ended source iterators into a
/// `SeekingMerger`, the loser-tree-based merging iterator that
/// replaces the legacy `Merger` (sorted-vector heap) on the read
/// path. The `BoxedIterator`s become `MergeSource`s via the
/// `CoherentIterSource` adapter — its coherent-cursor contract
/// is satisfied by every memtable / run / table iter we feed in
/// here (they all share a single front/back cursor over the
/// remaining range).
///
/// The source type stays concrete (`CoherentIterSource<BoxedIterator<'a>>`)
/// rather than being re-boxed behind a `dyn MergeSource` trait
/// object: `BoxedIterator` is itself already a heap-allocated
/// trait object, so wrapping it in another `Box<dyn ...>` would
/// cost an extra allocation per source AND an extra vtable
/// dispatch on every `next`/`next_back`. With the concrete
/// adapter type, only the inner `BoxedIterator`'s vtable
/// remains — exactly one indirect call per per-step source pull.
///
/// The compaction merge path still uses the legacy `Merger`
/// because compaction `Scanner`s are forward-only (`Iterator`,
/// not `DoubleEndedIterator`) and `MergeSource` requires
/// `next_back`. That swap is a separate refactor.
fn build_seeking<'a>(
    iters: Vec<BoxedIterator<'a>>,
    comparator: SharedComparator,
) -> SeekingMerger<CoherentIterSource<BoxedIterator<'a>>, SharedComparator> {
    let sources: Vec<CoherentIterSource<BoxedIterator<'a>>> =
        iters.into_iter().map(CoherentIterSource::new).collect();
    SeekingMerger::new(sources, comparator)
}

/// Calculates the prefix's upper range.
///
/// # Panics
///
/// Panics if the prefix is empty.
pub(crate) fn prefix_upper_range(prefix: &[u8]) -> Bound<UserKey> {
    use core::ops::Bound::{Excluded, Unbounded};

    assert!(!prefix.is_empty(), "prefix may not be empty");

    let mut end = prefix.to_vec();
    let len = end.len();

    for (idx, byte) in end.iter_mut().rev().enumerate() {
        let idx = len - 1 - idx;

        if *byte < 255 {
            *byte += 1;
            end.truncate(idx + 1);
            return Excluded(end.into());
        }
    }

    Unbounded
}

/// Converts a prefix to range bounds.
#[must_use]
#[expect(clippy::module_name_repetitions)]
pub fn prefix_to_range(prefix: &[u8]) -> (Bound<UserKey>, Bound<UserKey>) {
    use core::ops::Bound::{Included, Unbounded};

    if prefix.is_empty() {
        return (Unbounded, Unbounded);
    }

    (Included(prefix.into()), prefix_upper_range(prefix))
}

/// The iter state references the memtables used while the range is open
///
/// Because of Rust rules, the state is referenced using `self_cell`, see below.
///
/// `Clone` is cheap (every field is an `Arc` / small `Copy` value) and is used
/// by the seekable range iterator, which rebuilds its merge pipeline on each
/// reposition while keeping the same underlying version snapshot.
#[derive(Clone)]
pub struct IterState {
    pub(crate) version: SuperVersion,
    pub(crate) ephemeral: Option<(Arc<Memtable>, SeqNo)>,
    pub(crate) merge_operator: Option<Arc<dyn MergeOperator>>,

    /// User key comparator for merge ordering.
    pub(crate) comparator: crate::comparator::SharedComparator,

    /// Optional prefix hash for prefix bloom filter skipping.
    ///
    /// When set, segments whose bloom filter reports no match for this
    /// hash will be skipped entirely during the scan.
    pub(crate) prefix_hash: Option<u64>,

    /// Optional key hash for standard bloom filter pre-filtering.
    ///
    /// When set (typically for single-key point-read pipelines), segments
    /// whose bloom filter reports no match for this hash will be skipped.
    pub(crate) key_hash: Option<u64>,

    /// Optional user key for partition-aware bloom filter seeking.
    ///
    /// When set alongside `key_hash`, enables partitioned/TLI bloom filters
    /// to seek directly to the relevant partition instead of returning the
    /// conservative `Ok(true)` fallback. Only set for single-key pipelines
    /// (e.g. `resolve_merge_via_pipeline`).
    pub(crate) bloom_key: Option<UserKey>,

    /// Optional metrics handle for recording prefix-related statistics (e.g. bloom skips).
    ///
    /// `None` when the caller does not wish to record metrics; this is
    /// independent of whether the iterator uses a prefix.
    #[cfg(feature = "metrics")]
    pub(crate) metrics: Option<Arc<crate::Metrics>>,
}

type BoxedMerge<'a> = Box<dyn DoubleEndedIterator<Item = crate::Result<InternalValue>> + Send + 'a>;

self_cell!(
    pub struct TreeIter {
        owner: IterState,

        #[covariant]
        dependent: BoxedMerge,
    }
);

impl Iterator for TreeIter {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|_, iter| iter.next())
    }
}

impl DoubleEndedIterator for TreeIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|_, iter| iter.next_back())
    }
}

fn range_tombstone_overlaps_bounds(
    rt: &RangeTombstone,
    bounds: &(Bound<UserKey>, Bound<UserKey>),
    comparator: &dyn crate::comparator::UserComparator,
) -> bool {
    let overlaps_lo = match &bounds.0 {
        Bound::Included(key) | Bound::Excluded(key) => {
            comparator.compare(&rt.end, key) == core::cmp::Ordering::Greater
        }
        Bound::Unbounded => true,
    };

    let overlaps_hi = match &bounds.1 {
        Bound::Included(key) => comparator.compare(&rt.start, key) != core::cmp::Ordering::Greater,
        Bound::Excluded(key) => comparator.compare(&rt.start, key) == core::cmp::Ordering::Less,
        Bound::Unbounded => true,
    };

    overlaps_lo && overlaps_hi
}

/// Checks prefix and key bloom filters for a table.
///
/// Returns `true` if the table should be included (bloom says "maybe" or no
/// filter available), `false` if it can be safely skipped.
fn bloom_passes(state: &IterState, table: &crate::table::Table) -> bool {
    if let Some(prefix_hash) = state.prefix_hash {
        match table.maybe_contains_prefix(prefix_hash) {
            Ok(false) => {
                #[cfg(feature = "metrics")]
                if let Some(m) = &state.metrics {
                    m.prefix_bloom_skips
                        .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
                }
                return false;
            }
            Err(e) => {
                log::debug!("prefix bloom check failed for table {:?}: {e}", table.id(),);
            }
            _ => {}
        }
    }

    // bloom_key without key_hash is meaningless — catch misuse early
    debug_assert!(
        state.bloom_key.is_none() || state.key_hash.is_some(),
        "bloom_key requires key_hash to be set"
    );

    if let Some(key_hash) = state.key_hash {
        let result = if let Some(bloom_key) = &state.bloom_key {
            // UserKey (Slice) implements Deref<Target=[u8]>, coerces to &[u8]
            table.bloom_may_contain_key(bloom_key, key_hash)
        } else {
            table.bloom_may_contain_key_hash(key_hash)
        };
        match result {
            Ok(false) => return false,
            Err(e) => {
                log::debug!("key bloom check failed for table {:?}: {e}", table.id(),);
            }
            _ => {}
        }
    }

    true
}

impl TreeIter {
    /// Fast path for single-key point-read merge resolution.
    ///
    /// Unlike [`create_range`], this skips:
    /// - RT sort + dedup + table-skip computation
    /// - `RangeTombstoneFilter` wrapper (uses inline post-merge RT check instead)
    /// - Reverse-direction RT clone+sort (point reads are forward-only)
    ///
    /// Range tombstones are still collected from all tables (not just
    /// bloom-passing) because an RT in a bloom-negative table can suppress
    /// the target key. Only iterator construction is bloom-gated.
    ///
    /// `MvccStream::is_rt_suppressed` handles merge-internal suppression; the
    /// post-merge filter catches RT-suppressed resolved entries that would
    /// otherwise leak through.
    #[expect(
        clippy::too_many_lines,
        reason = "mirrors create_range structure for the point-read fast path; splitting would reduce clarity"
    )]
    #[must_use]
    pub fn create_range_point(guard: IterState, key: &[u8], seqno: SeqNo) -> Self {
        let key_slice = UserKey::from(key);

        Self::new(guard, |lock| {
            let user_range = (
                Bound::Included(key_slice.clone()),
                Bound::Included(key_slice.clone()),
            );

            let range = (
                Bound::Included(InternalKey::new(
                    key_slice.as_ref(),
                    SeqNo::MAX,
                    crate::ValueType::Tombstone,
                )),
                Bound::Included(InternalKey::new(
                    key_slice.as_ref(),
                    0,
                    crate::ValueType::Value,
                )),
            );

            let mut iters: Vec<BoxedIterator<'_>> = Vec::new();
            let mut range_tombstones: Vec<(RangeTombstone, SeqNo)> = Vec::new();

            // Constant for a point key — computed once and reused for
            // key-range overlap checks and bloom filtering across all runs.
            let bounds = (
                user_range.0.as_ref().map(core::convert::AsRef::as_ref),
                user_range.1.as_ref().map(core::convert::AsRef::as_ref),
            );

            for run in lock
                .version
                .version
                .iter_levels()
                .flat_map(|lvl| lvl.iter())
            {
                // Collect RTs from all key-range-overlapping tables regardless
                // of bloom — an RT in a bloom-negative table can still suppress
                // the target key. The key-range check avoids loading RTs from
                // tables that cannot possibly contain a covering tombstone.
                for table in run.iter() {
                    if !table.check_key_range_overlap_cmp(&bounds, lock.comparator.as_ref()) {
                        continue;
                    }
                    range_tombstones.extend(
                        table
                            .range_tombstones()
                            .iter()
                            .filter(|rt| {
                                range_tombstone_overlaps_bounds(
                                    rt,
                                    &user_range,
                                    lock.comparator.as_ref(),
                                )
                            })
                            .map(|rt| (rt.clone(), seqno)),
                    );
                }

                // Build iterators only from bloom-passing tables.
                match run.len() {
                    0 => {}
                    1 => {
                        #[expect(clippy::expect_used, reason = "we checked for length")]
                        let table = run.first().expect("should exist");

                        if table.check_key_range_overlap_cmp(&bounds, lock.comparator.as_ref())
                            && bloom_passes(lock, table)
                        {
                            let reader =
                                table
                                    .range(user_range.clone())
                                    .filter(move |item| match item {
                                        Ok(item) => seqno_filter(item.key.seqno, seqno),
                                        Err(_) => true,
                                    });
                            iters.push(Box::new(reader));
                        }
                    }
                    _ => {
                        let surviving: Vec<_> = run
                            .iter()
                            .filter(|table| {
                                table.check_key_range_overlap_cmp(&bounds, lock.comparator.as_ref())
                                    && bloom_passes(lock, table)
                            })
                            .cloned()
                            .collect();

                        match surviving.len() {
                            0 => {}
                            1 => {
                                if let Some(table) = surviving.into_iter().next() {
                                    let reader =
                                        table.range(user_range.clone()).filter(move |item| {
                                            match item {
                                                Ok(item) => seqno_filter(item.key.seqno, seqno),
                                                Err(_) => true,
                                            }
                                        });
                                    iters.push(Box::new(reader));
                                }
                            }
                            _ => {
                                #[expect(
                                    clippy::expect_used,
                                    reason = "Run::new returns None only for empty vecs"
                                )]
                                let new_run =
                                    Run::new(surviving).expect("non-empty surviving tables");
                                if let Some(reader) = RunReader::new_cmp(
                                    Arc::new(new_run),
                                    user_range.clone(),
                                    lock.comparator.as_ref(),
                                ) {
                                    iters.push(Box::new(reader.filter(move |item| match item {
                                        Ok(item) => seqno_filter(item.key.seqno, seqno),
                                        Err(_) => true,
                                    })));
                                }
                            }
                        }
                    }
                }
            }

            // Sealed memtables
            for memtable in lock.version.sealed_memtables.iter() {
                range_tombstones.extend(
                    memtable
                        .range_tombstones_sorted()
                        .into_iter()
                        .filter(|rt| {
                            range_tombstone_overlaps_bounds(
                                rt,
                                &user_range,
                                lock.comparator.as_ref(),
                            )
                        })
                        .map(|rt| (rt, seqno)),
                );

                let iter = memtable.range_internal(range.clone());
                iters.push(Box::new(
                    iter.filter(move |item| seqno_filter(item.key.seqno, seqno))
                        .map(Ok),
                ));
            }

            // Active memtable
            {
                range_tombstones.extend(
                    lock.version
                        .active_memtable
                        .range_tombstones_sorted()
                        .into_iter()
                        .filter(|rt| {
                            range_tombstone_overlaps_bounds(
                                rt,
                                &user_range,
                                lock.comparator.as_ref(),
                            )
                        })
                        .map(|rt| (rt, seqno)),
                );

                let iter = lock.version.active_memtable.range_internal(range);
                iters.push(Box::new(
                    iter.filter(move |item| seqno_filter(item.key.seqno, seqno))
                        .map(Ok),
                ));
            }

            let merged = build_seeking(iters, lock.comparator.clone());
            // Clone is cheap: point-read RT sets are typically 0-2 entries.
            // An Arc would add indirection overhead that exceeds the clone cost.
            let iter = MvccStream::new_with_comparator(
                merged,
                lock.merge_operator.clone(),
                lock.comparator.clone(),
            )
            .with_range_tombstones(range_tombstones.clone());

            // Post-merge RT suppression: unlike create_range which uses
            // RangeTombstoneFilter (requires sorted RTs + O(n log n) init),
            // point reads just do a linear scan over the (typically tiny) RT set.
            Box::new(iter.filter(move |x| match x {
                Ok(value) => {
                    if value.key.is_tombstone() {
                        return false;
                    }
                    !range_tombstones.iter().any(|(rt, cutoff)| {
                        rt.should_suppress_with(
                            &value.key.user_key,
                            value.key.seqno,
                            *cutoff,
                            lock.comparator.as_ref(),
                        )
                    })
                }
                Err(_) => true,
            }))
        })
    }

    #[expect(
        clippy::too_many_lines,
        reason = "create_range wires up multiple iterator sources, filters, and tombstone handling; splitting further would reduce clarity"
    )]
    pub fn create_range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        guard: IterState,
        range: R,
        seqno: SeqNo,
    ) -> Self {
        Self::new(guard, |lock| {
            let user_range = (
                match range.start_bound() {
                    Bound::Included(key) => Bound::Included(UserKey::from(key.as_ref())),
                    Bound::Excluded(key) => Bound::Excluded(UserKey::from(key.as_ref())),
                    Bound::Unbounded => Bound::Unbounded,
                },
                match range.end_bound() {
                    Bound::Included(key) => Bound::Included(UserKey::from(key.as_ref())),
                    Bound::Excluded(key) => Bound::Excluded(UserKey::from(key.as_ref())),
                    Bound::Unbounded => Bound::Unbounded,
                },
            );

            let range = (
                match &user_range.0 {
                    // NOTE: See memtable.rs for range explanation
                    Bound::Included(key) => Bound::Included(InternalKey::new(
                        key.as_ref(),
                        SeqNo::MAX,
                        crate::ValueType::Tombstone,
                    )),
                    Bound::Excluded(key) => Bound::Excluded(InternalKey::new(
                        key.as_ref(),
                        0,
                        crate::ValueType::Tombstone,
                    )),
                    Bound::Unbounded => Bound::Unbounded,
                },
                match &user_range.1 {
                    // NOTE: See memtable.rs for range explanation, this is the reverse case
                    // where we need to go all the way to the last seqno of an item
                    //
                    // Example: We search for (Unbounded..Excluded(abdef))
                    //
                    // key -> seqno
                    //
                    // a   -> 7 <<< This is the lowest key that matches the range
                    // abc -> 5
                    // abc -> 4
                    // abc -> 3 <<< This is the highest key that matches the range
                    // abcdef -> 6
                    // abcdef -> 5
                    //
                    Bound::Included(key) => {
                        Bound::Included(InternalKey::new(key.as_ref(), 0, crate::ValueType::Value))
                    }
                    Bound::Excluded(key) => Bound::Excluded(InternalKey::new(
                        key.as_ref(),
                        SeqNo::MAX,
                        crate::ValueType::Value,
                    )),
                    Bound::Unbounded => Bound::Unbounded,
                },
            );

            let mut iters: Vec<BoxedIterator<'_>> = Vec::with_capacity(5);
            // Each RT is paired with the per-source visibility cutoff so that
            // ephemeral memtable RTs use their own index_seqno instead of the
            // outer scan seqno (see issue #33).
            let mut all_range_tombstones: Vec<(RangeTombstone, SeqNo)> = Vec::new();
            let mut single_tables = Vec::new();
            let mut multi_runs = Vec::new();

            for run in lock
                .version
                .version
                .iter_levels()
                .flat_map(|lvl| lvl.iter())
            {
                match run.len() {
                    0 => {
                        // Do nothing
                    }
                    1 => {
                        #[expect(clippy::expect_used, reason = "we checked for length")]
                        let table = run.first().expect("should exist");

                        all_range_tombstones.extend(
                            table
                                .range_tombstones()
                                .iter()
                                .filter(|rt| {
                                    range_tombstone_overlaps_bounds(
                                        rt,
                                        &user_range,
                                        lock.comparator.as_ref(),
                                    )
                                })
                                .map(|rt| (rt.clone(), seqno)),
                        );

                        // Check key range overlap first (cheap metadata check) before
                        // running the O(rt_count) table-skip scan.
                        if table.check_key_range_overlap_cmp(
                            &(
                                user_range.0.as_ref().map(core::convert::AsRef::as_ref),
                                user_range.1.as_ref().map(core::convert::AsRef::as_ref),
                            ),
                            lock.comparator.as_ref(),
                        ) && bloom_passes(lock, table)
                        {
                            single_tables.push(table.clone());
                        }
                    }
                    _ => {
                        // Collect range tombstones from ALL tables in the run
                        // regardless of bloom filtering — they may affect keys
                        // in other tables/levels.
                        for table in run.iter() {
                            all_range_tombstones.extend(
                                table
                                    .range_tombstones()
                                    .iter()
                                    .filter(|rt| {
                                        range_tombstone_overlaps_bounds(
                                            rt,
                                            &user_range,
                                            lock.comparator.as_ref(),
                                        )
                                    })
                                    .map(|rt| (rt.clone(), seqno)),
                            );
                        }

                        // If a prefix or key hash is available, filter individual
                        // tables within the multi-table run using their bloom
                        // filters. This covers both prefix scans (prefix_hash)
                        // and point-read merge pipelines (key_hash).
                        if lock.prefix_hash.is_some() || lock.key_hash.is_some() {
                            let bounds = (
                                user_range.0.as_ref().map(core::convert::AsRef::as_ref),
                                user_range.1.as_ref().map(core::convert::AsRef::as_ref),
                            );

                            let surviving: Vec<_> = run
                                .iter()
                                .filter(|table| {
                                    // Cheap key-range metadata check first to avoid
                                    // bloom filter I/O for non-overlapping tables.
                                    if !table.check_key_range_overlap_cmp(
                                        &bounds,
                                        lock.comparator.as_ref(),
                                    ) {
                                        return false;
                                    }

                                    bloom_passes(lock, table)
                                })
                                .cloned()
                                .collect();

                            match surviving.len() {
                                0 => {
                                    // All tables in this run were filtered out.
                                }
                                1 => {
                                    // Demote to single-table path so it also
                                    // benefits from the range-tombstone table-skip
                                    // optimization below.
                                    if let Some(table) = surviving.into_iter().next() {
                                        single_tables.push(table);
                                    }
                                }
                                _ => {
                                    // surviving.len() >= 2, so Run::new cannot
                                    // return None (only empty vecs yield None).
                                    #[expect(
                                        clippy::expect_used,
                                        reason = "Run::new returns None only for empty vecs"
                                    )]
                                    let new_run =
                                        Run::new(surviving).expect("non-empty surviving tables");
                                    multi_runs.push(Arc::new(new_run));
                                }
                            }
                        } else {
                            multi_runs.push(run.clone());
                        }
                    }
                }
            }

            // Sort SST-sourced RTs by start key for binary search in
            // table-skip below. This is intentionally a separate sort from
            // the full sort+dedup later: table-skip runs here (before memtable
            // RTs are collected), so only SST RTs are present. The later sort
            // covers the complete list. Both sorts are O(n log n) on their
            // respective subsets; the SST-only subset is typically small.
            all_range_tombstones
                .sort_unstable_by(|(a, _), (b, _)| lock.comparator.compare(&a.start, &b.start));

            for table in single_tables {
                // Table-skip: if a range tombstone fully covers this table
                // with a higher seqno, skip it entirely (avoid I/O).
                //
                // Uses get_highest_kv_seqno() which excludes RT seqnos, so a
                // covering RT stored in the same table can now trigger skip.
                //
                // Binary search on sorted RT list: partition_point finds the
                // first RT with start > table_min; only the prefix [0..idx]
                // can have start <= table_min (required for fully_covers).
                // key_range.max() is inclusive; fully_covers checks max < rt.end
                // (half-open), so this is correct for inclusive upper bounds.
                let table_min: &[u8] = table.metadata.key_range.min().as_ref();
                let table_max: &[u8] = table.metadata.key_range.max().as_ref();
                let table_kv_seqno = table.get_highest_kv_seqno();

                let candidate_end = all_range_tombstones.partition_point(|(rt, _)| {
                    lock.comparator.compare(&rt.start, table_min) != core::cmp::Ordering::Greater
                });

                let is_covered =
                    all_range_tombstones
                        .iter()
                        .take(candidate_end)
                        .any(|(rt, cutoff)| {
                            rt.visible_at(*cutoff)
                                && rt.fully_covers_with(
                                    table_min,
                                    table_max,
                                    lock.comparator.as_ref(),
                                )
                                && rt.seqno > table_kv_seqno
                        });

                if !is_covered {
                    let reader = table
                        .range(user_range.clone())
                        .filter(move |item| match item {
                            Ok(item) => seqno_filter(item.key.seqno, seqno),
                            Err(_) => true,
                        });

                    iters.push(Box::new(reader));
                }
            }

            for run in multi_runs {
                if let Some(reader) =
                    RunReader::new_cmp(run, user_range.clone(), lock.comparator.as_ref())
                {
                    iters.push(Box::new(reader.filter(move |item| match item {
                        Ok(item) => seqno_filter(item.key.seqno, seqno),
                        Err(_) => true,
                    })));
                }
            }

            // Sealed memtables
            for memtable in lock.version.sealed_memtables.iter() {
                all_range_tombstones.extend(
                    memtable
                        .range_tombstones_sorted()
                        .into_iter()
                        .filter(|rt| {
                            range_tombstone_overlaps_bounds(
                                rt,
                                &user_range,
                                lock.comparator.as_ref(),
                            )
                        })
                        .map(|rt| (rt, seqno)),
                );

                let iter = memtable.range_internal(range.clone());

                iters.push(Box::new(
                    iter.filter(move |item| seqno_filter(item.key.seqno, seqno))
                        .map(Ok),
                ));
            }

            // Active memtable
            {
                all_range_tombstones.extend(
                    lock.version
                        .active_memtable
                        .range_tombstones_sorted()
                        .into_iter()
                        .filter(|rt| {
                            range_tombstone_overlaps_bounds(
                                rt,
                                &user_range,
                                lock.comparator.as_ref(),
                            )
                        })
                        .map(|rt| (rt, seqno)),
                );

                let iter = lock.version.active_memtable.range_internal(range.clone());

                iters.push(Box::new(
                    iter.filter(move |item| seqno_filter(item.key.seqno, seqno))
                        .map(Ok),
                ));
            }

            if let Some((mt, eph_seqno)) = &lock.ephemeral {
                all_range_tombstones.extend(
                    mt.range_tombstones_sorted()
                        .into_iter()
                        .filter(|rt| {
                            range_tombstone_overlaps_bounds(
                                rt,
                                &user_range,
                                lock.comparator.as_ref(),
                            )
                        })
                        .map(|rt| (rt, *eph_seqno)),
                );

                let iter = Box::new(
                    mt.range_internal(range)
                        .filter(move |item| seqno_filter(item.key.seqno, *eph_seqno))
                        .map(Ok),
                );
                iters.push(iter);
            }

            let merged = build_seeking(iters, lock.comparator.clone());
            // Clone needed: MvccStream uses the RT set for merge suppression,
            // while RangeTombstoneFilter below consumes it for post-merge
            // filtering. An Arc<[_]> could avoid the copy if RT sets grow large.
            let iter = MvccStream::new_with_comparator(
                merged,
                lock.merge_operator.clone(),
                lock.comparator.clone(),
            )
            .with_range_tombstones(all_range_tombstones.clone());

            let iter = iter.filter(|x| match x {
                Ok(value) => !value.key.is_tombstone(),
                Err(_) => true,
            });

            // Deduplicate: MultiWriter rotation copies the same RTs into each
            // output table, so collected tombstones can contain duplicates.
            // When the same RT appears from different sources with different
            // cutoffs (e.g., persisted SST + ephemeral), keep the max cutoff
            // so the RT stays visible if ANY source's snapshot includes it.
            all_range_tombstones
                .sort_by(|a, b| a.0.cmp_with_comparator(&b.0, lock.comparator.as_ref()));
            all_range_tombstones.dedup_by(|a, b| {
                if a.0 == b.0 {
                    // dedup_by passes (a=later, b=earlier); b survives, a is
                    // removed.  Merge a's cutoff into the surviving b.
                    b.1 = b.1.max(a.1);
                    true
                } else {
                    false
                }
            });

            // Fast path: skip filter wrapping when no tombstone is visible at
            // its per-source cutoff. Each RT carries the seqno of its originating
            // source, so the check is per-RT rather than global.
            if all_range_tombstones
                .iter()
                .all(|(rt, cutoff)| !rt.visible_at(*cutoff))
            {
                Box::new(iter)
            } else {
                Box::new(RangeTombstoneFilter::new_with_comparator(
                    iter,
                    all_range_tombstones,
                    lock.comparator.clone(),
                ))
            }
        })
    }
}

/// User-key bounds for a scan; the seekable iterator rebuilds its merge
/// pipeline for any sub-range of the union without recollecting sources.
type UserBounds = (Bound<UserKey>, Bound<UserKey>);

/// Translate user-key bounds into the internal-key bounds the per-source readers
/// expect. Mirrors the bound construction in [`TreeIter::create_range`] (see
/// there for the seqno-direction reasoning at each end).
fn user_to_internal_bounds(user: &UserBounds) -> (Bound<InternalKey>, Bound<InternalKey>) {
    let lo = match &user.0 {
        Bound::Included(key) => Bound::Included(InternalKey::new(
            key.as_ref(),
            SeqNo::MAX,
            crate::ValueType::Tombstone,
        )),
        Bound::Excluded(key) => Bound::Excluded(InternalKey::new(
            key.as_ref(),
            0,
            crate::ValueType::Tombstone,
        )),
        Bound::Unbounded => Bound::Unbounded,
    };
    let hi = match &user.1 {
        Bound::Included(key) => {
            Bound::Included(InternalKey::new(key.as_ref(), 0, crate::ValueType::Value))
        }
        Bound::Excluded(key) => Bound::Excluded(InternalKey::new(
            key.as_ref(),
            SeqNo::MAX,
            crate::ValueType::Value,
        )),
        Bound::Unbounded => Bound::Unbounded,
    };
    (lo, hi)
}

/// SSTs / runs / range-tombstones overlapping the union range, collected once.
///
/// Rebuilding the merge pipeline for a sub-range (a seek, or one batch interval)
/// reuses these — Phase 1 (level iteration + bloom + RT collection) is the
/// ~100µs setup that [`SeekableTreeIter`] amortizes across repositions.
#[derive(Clone)]
struct CollectedSources {
    single_tables: Vec<crate::table::Table>,
    multi_runs: Vec<Arc<Run<crate::table::Table>>>,
    range_tombstones: Vec<(RangeTombstone, SeqNo)>,
    union: UserBounds,
}

/// Phase 1: collect every source overlapping `union` once. The table-skip
/// optimization and the prefix/key-hash sub-filtering used by
/// [`TreeIter::create_range`] are intentionally omitted: this is a plain range
/// scan (no prefix/point pipeline), and skipping table-skip only ever reads a
/// table a tombstone fully covers — correct, just marginally less optimal.
fn collect_sources(state: &IterState, union: UserBounds, seqno: SeqNo) -> CollectedSources {
    let mut single_tables: Vec<crate::table::Table> = Vec::new();
    let mut multi_runs: Vec<Arc<Run<crate::table::Table>>> = Vec::new();
    let mut rts: Vec<(RangeTombstone, SeqNo)> = Vec::new();

    let bounds_ref = (
        union.0.as_ref().map(core::convert::AsRef::as_ref),
        union.1.as_ref().map(core::convert::AsRef::as_ref),
    );

    for run in state
        .version
        .version
        .iter_levels()
        .flat_map(|lvl| lvl.iter())
    {
        match run.len() {
            0 => {}
            1 => {
                #[expect(clippy::expect_used, reason = "len checked")]
                let table = run.first().expect("len == 1");
                rts.extend(
                    table
                        .range_tombstones()
                        .iter()
                        .filter(|rt| {
                            range_tombstone_overlaps_bounds(rt, &union, state.comparator.as_ref())
                        })
                        .map(|rt| (rt.clone(), seqno)),
                );
                if table.check_key_range_overlap_cmp(&bounds_ref, state.comparator.as_ref())
                    && bloom_passes(state, table)
                {
                    single_tables.push(table.clone());
                }
            }
            _ => {
                for table in run.iter() {
                    rts.extend(
                        table
                            .range_tombstones()
                            .iter()
                            .filter(|rt| {
                                range_tombstone_overlaps_bounds(
                                    rt,
                                    &union,
                                    state.comparator.as_ref(),
                                )
                            })
                            .map(|rt| (rt.clone(), seqno)),
                    );
                }
                multi_runs.push(run.clone());
            }
        }
    }

    let mut collect_mt_rts = |iter: alloc::vec::Vec<RangeTombstone>, cutoff: SeqNo| {
        rts.extend(
            iter.into_iter()
                .filter(|rt| range_tombstone_overlaps_bounds(rt, &union, state.comparator.as_ref()))
                .map(|rt| (rt, cutoff)),
        );
    };
    for memtable in state.version.sealed_memtables.iter() {
        collect_mt_rts(memtable.range_tombstones_sorted(), seqno);
    }
    collect_mt_rts(
        state.version.active_memtable.range_tombstones_sorted(),
        seqno,
    );
    if let Some((mt, eph_seqno)) = &state.ephemeral {
        collect_mt_rts(mt.range_tombstones_sorted(), *eph_seqno);
    }

    rts.sort_by(|a, b| a.0.cmp_with_comparator(&b.0, state.comparator.as_ref()));
    rts.dedup_by(|a, b| {
        if a.0 == b.0 {
            b.1 = b.1.max(a.1);
            true
        } else {
            false
        }
    });

    CollectedSources {
        single_tables,
        multi_runs,
        range_tombstones: rts,
        union,
    }
}

/// Phase 2: build the merge pipeline (`SeekingMerger` -> `MvccStream` ->
/// tombstone filter -> optional `RangeTombstoneFilter`) for the sub-range
/// `[lower, upper)` from already-collected sources. Reruns on every reposition;
/// the per-source readers reuse the tested seek-to-start path (no block I/O
/// until the first `next`).
fn build_stack<'a>(
    state: &'a IterState,
    collected: &CollectedSources,
    lower: Bound<UserKey>,
    upper: Bound<UserKey>,
    seqno: SeqNo,
) -> BoxedMerge<'a> {
    let user_range: UserBounds = (lower, upper);
    let range = user_to_internal_bounds(&user_range);

    let mut iters: Vec<BoxedIterator<'a>> = Vec::with_capacity(collected.single_tables.len() + 3);

    for table in &collected.single_tables {
        let reader = table
            .range(user_range.clone())
            .filter(move |item| match item {
                Ok(item) => seqno_filter(item.key.seqno, seqno),
                Err(_) => true,
            });
        iters.push(Box::new(reader));
    }
    for run in &collected.multi_runs {
        if let Some(reader) =
            RunReader::new_cmp(run.clone(), user_range.clone(), state.comparator.as_ref())
        {
            iters.push(Box::new(reader.filter(move |item| match item {
                Ok(item) => seqno_filter(item.key.seqno, seqno),
                Err(_) => true,
            })));
        }
    }
    for memtable in state.version.sealed_memtables.iter() {
        let iter = memtable.range_internal(range.clone());
        iters.push(Box::new(
            iter.filter(move |item| seqno_filter(item.key.seqno, seqno))
                .map(Ok),
        ));
    }
    {
        let iter = state.version.active_memtable.range_internal(range.clone());
        iters.push(Box::new(
            iter.filter(move |item| seqno_filter(item.key.seqno, seqno))
                .map(Ok),
        ));
    }
    if let Some((mt, eph_seqno)) = &state.ephemeral {
        let eph_seqno = *eph_seqno;
        let iter = mt.range_internal(range);
        iters.push(Box::new(
            iter.filter(move |item| seqno_filter(item.key.seqno, eph_seqno))
                .map(Ok),
        ));
    }

    let merged = build_seeking(iters, state.comparator.clone());
    let iter = MvccStream::new_with_comparator(
        merged,
        state.merge_operator.clone(),
        state.comparator.clone(),
    )
    .with_range_tombstones(collected.range_tombstones.clone());

    let iter = iter.filter(|x| match x {
        Ok(value) => !value.key.is_tombstone(),
        Err(_) => true,
    });

    if collected
        .range_tombstones
        .iter()
        .all(|(rt, cutoff)| !rt.visible_at(*cutoff))
    {
        Box::new(iter)
    } else {
        Box::new(RangeTombstoneFilter::new_with_comparator(
            iter,
            collected.range_tombstones.clone(),
            state.comparator.clone(),
        ))
    }
}

/// Owner half of [`SeekableTreeIter`]'s self-cell.
///
/// Holds the version snapshot, the collected sources (shared via `Arc` so a
/// reposition is a cheap refcount bump), and the current sub-range bounds the
/// dependent pipeline was built for.
pub struct SeekableOwner {
    state: IterState,
    collected: Arc<CollectedSources>,
    seqno: SeqNo,
    lower: Bound<UserKey>,
    upper: Bound<UserKey>,
}

self_cell!(
    /// A range iterator that can reposition (seek) in place without reopening
    /// per-SST readers. Built once over a union range; [`Self::seek_to`],
    /// [`Self::seek_to_for_prev`], and [`Self::reposition`] rebuild only the
    /// cheap Phase-2 merge pipeline, reusing the collected sources.
    pub struct SeekableTreeIter {
        owner: SeekableOwner,

        #[covariant]
        dependent: BoxedMerge,
    }
);

impl SeekableTreeIter {
    fn build(owner: SeekableOwner) -> Self {
        Self::new(owner, |o| {
            build_stack(
                &o.state,
                &o.collected,
                o.lower.clone(),
                o.upper.clone(),
                o.seqno,
            )
        })
    }

    /// Open a seekable iterator over `[union_lower, union_upper)`. The source
    /// collection (Phase 1) runs once here; subsequent repositions reuse it.
    #[must_use]
    pub fn create(
        state: IterState,
        union_lower: Bound<UserKey>,
        union_upper: Bound<UserKey>,
        seqno: SeqNo,
    ) -> Self {
        let collected = Arc::new(collect_sources(
            &state,
            (union_lower.clone(), union_upper.clone()),
            seqno,
        ));
        Self::build(SeekableOwner {
            state,
            collected,
            seqno,
            lower: union_lower,
            upper: union_upper,
        })
    }

    /// Rebuild the merge pipeline for the sub-range `[lower, upper)`, reusing the
    /// collected sources. Cheap: clones `Arc`s and reconstructs per-source
    /// readers (no block I/O until the next `next`/`next_back`).
    pub(crate) fn reposition(&mut self, lower: Bound<UserKey>, upper: Bound<UserKey>) {
        let state = self.borrow_owner().state.clone();
        let collected = Arc::clone(&self.borrow_owner().collected);
        let seqno = self.borrow_owner().seqno;
        *self = Self::build(SeekableOwner {
            state,
            collected,
            seqno,
            lower,
            upper,
        });
    }

    /// Reposition so the next [`Iterator::next`] yields the first entry with
    /// user key `>= key` (`RocksDB` `Seek`).
    ///
    /// The new lower bound is clamped to the iterator's collected window: Phase 1
    /// only gathered sources overlapping the original range, so seeking below the
    /// window lower would scan outside it (leaking rows below the window, or
    /// missing rows from sources that were skipped during collection). A `key`
    /// below the window lower positions at the window start; the upper bound
    /// stays the window upper.
    pub fn seek_to(&mut self, key: &[u8]) {
        let (lower, upper) = {
            let union = &self.borrow_owner().collected.union;
            let lower = match &union.0 {
                Bound::Included(floor) if floor.as_ref() > key => union.0.clone(),
                Bound::Excluded(floor) if floor.as_ref() >= key => union.0.clone(),
                _ => Bound::Included(UserKey::from(key)),
            };
            (lower, union.1.clone())
        };
        self.reposition(lower, upper);
    }

    /// Reposition so the next [`DoubleEndedIterator::next_back`] yields the last
    /// entry with user key `<= key` (`RocksDB` `SeekForPrev`).
    ///
    /// The new upper bound is clamped to the iterator's collected window (see
    /// [`seek_to`](Self::seek_to) for why): a `key` above the window upper
    /// positions at the window end; the lower bound stays the window lower.
    pub fn seek_to_for_prev(&mut self, key: &[u8]) {
        let (lower, upper) = {
            let union = &self.borrow_owner().collected.union;
            let upper = match &union.1 {
                Bound::Included(ceil) if ceil.as_ref() < key => union.1.clone(),
                Bound::Excluded(ceil) if ceil.as_ref() <= key => union.1.clone(),
                _ => Bound::Included(UserKey::from(key)),
            };
            (union.0.clone(), upper)
        };
        self.reposition(lower, upper);
    }

    /// The version snapshot this iterator reads from. Used by KV-separated trees
    /// to resolve blob handles against the same snapshot the keys came from.
    #[must_use]
    pub fn version(&self) -> crate::version::Version {
        self.borrow_owner().state.version.version.clone()
    }
}

impl Iterator for SeekableTreeIter {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|_, iter| iter.next())
    }
}

impl DoubleEndedIterator for SeekableTreeIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|_, iter| iter.next_back())
    }
}

/// Forward scan over a (possibly lazily-produced) sequence of disjoint,
/// ascending sub-ranges, reusing a single [`SeekableTreeIter`].
///
/// Each interval is served by repositioning the shared pipeline — the per-SST
/// setup is paid once (when the `SeekableTreeIter` is created over the union)
/// and amortized across every interval, rather than reopening readers per
/// interval.
///
/// The interval source is pulled on demand, so intervals may be generated
/// dynamically (the next interval can depend on rows already returned).
pub struct BatchRangeScan<I> {
    iter: SeekableTreeIter,
    intervals: I,
    /// Whether the shared iterator is currently positioned on an interval that
    /// may still yield rows.
    primed: bool,
}

impl<I: Iterator<Item = (Bound<UserKey>, Bound<UserKey>)>> BatchRangeScan<I> {
    /// Build a batch scan from a seekable iterator opened over the union range
    /// and a (lazy) sequence of `[lower, upper)` sub-ranges to visit in order.
    pub fn new(iter: SeekableTreeIter, intervals: I) -> Self {
        Self {
            iter,
            intervals,
            primed: false,
        }
    }
}

impl<I: Iterator<Item = (Bound<UserKey>, Bound<UserKey>)>> Iterator for BatchRangeScan<I> {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.primed {
                if let Some(item) = self.iter.next() {
                    return Some(item);
                }
                self.primed = false;
            }
            let (lower, upper) = self.intervals.next()?;
            self.iter.reposition(lower, upper);
            self.primed = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Slice;
    use core::ops::Bound::{Excluded, Included, Unbounded};
    use test_log::test;

    fn test_prefix(prefix: &[u8], upper_bound: Bound<&[u8]>) {
        let range = prefix_to_range(prefix);
        assert_eq!(
            range,
            (
                match prefix {
                    _ if prefix.is_empty() => Unbounded,
                    _ => Included(Slice::from(prefix)),
                },
                upper_bound.map(Slice::from),
            ),
        );
    }

    #[test]
    fn prefix_to_range_basic() {
        test_prefix(b"abc", Excluded(b"abd"));
    }

    #[test]
    fn prefix_to_range_empty() {
        test_prefix(b"", Unbounded);
    }

    #[test]
    fn prefix_to_range_single_char() {
        test_prefix(b"a", Excluded(b"b"));
    }

    #[test]
    fn prefix_to_range_1() {
        test_prefix(&[0, 250], Excluded(&[0, 251]));
    }

    #[test]
    fn prefix_to_range_2() {
        test_prefix(&[0, 250, 50], Excluded(&[0, 250, 51]));
    }

    #[test]
    fn prefix_to_range_3() {
        test_prefix(&[255, 255, 255], Unbounded);
    }

    #[test]
    fn prefix_to_range_char_max() {
        test_prefix(&[0, 255], Excluded(&[1]));
    }

    #[test]
    fn prefix_to_range_char_max_2() {
        test_prefix(&[0, 2, 255], Excluded(&[0, 3]));
    }
}

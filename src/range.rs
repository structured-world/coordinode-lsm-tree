// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::{
    BoxedIterator, InternalValue,
    comparator::SharedComparator,
    key::InternalKey,
    memtable::Memtable,
    merge_operator::MergeOperator,
    merge_source::{CoherentIterSource, CoherentMergeSource, IterItem, MergeSource},
    mvcc_stream::MvccStream,
    range_tombstone::RangeTombstone,
    range_tombstone_filter::RangeTombstoneFilter,
    reseek::{ReseekCtx, Reseekable},
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
/// by the seekable range iterator to snapshot the version once; repositions then
/// reseek the leaf cursors in place against that same snapshot.
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
    // Clone the `UserKey` (a reference-counted `Slice`: an `Arc` bump, no heap
    // copy) and hand it to `InternalKey::new` by value — `UserKey: Into<UserKey>`
    // is the identity, so the key bytes are reused. Passing `key.as_ref()`
    // (`&[u8]`) instead would force a fresh heap copy of the bytes on every
    // reposition, which the in-place seek loop must avoid.
    let lo = match &user.0 {
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
    let hi = match &user.1 {
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

/// Named tombstone-skip wrapper. The non-seekable read path uses an inline
/// `.filter(|x| !is_tombstone)` closure (unnameable), but the seekable pipeline
/// must stay a concrete, [`Reseekable`] type, so the same drop-resolved-
/// tombstones step is expressed as this struct. Errors pass through unchanged;
/// the reposition is stateless, so it just forwards to the inner layer.
struct TombstoneSkip<I> {
    inner: I,
}

impl<I: Iterator<Item = crate::Result<InternalValue>>> Iterator for TombstoneSkip<I> {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.inner.next()? {
                Ok(value) if value.key.is_tombstone() => {}
                other => return Some(other),
            }
        }
    }
}

impl<I: DoubleEndedIterator<Item = crate::Result<InternalValue>>> DoubleEndedIterator
    for TombstoneSkip<I>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            match self.inner.next_back()? {
                Ok(value) if value.key.is_tombstone() => {}
                other => return Some(other),
            }
        }
    }
}

impl<I: Reseekable> Reseekable for TombstoneSkip<I> {
    fn reseek(&mut self, ctx: &ReseekCtx) {
        self.inner.reseek(ctx);
    }
}

/// Leaf source over a single SST, re-positioned in place by re-seeking the
/// reader's owned index iterator (no new reader, no `Arc` re-clone).
struct TableLeaf {
    table: crate::table::Table,
    iter: crate::table::iter::Iter,
    seqno: SeqNo,
}

impl TableLeaf {
    fn new(table: crate::table::Table, user_range: UserBounds, seqno: SeqNo) -> Self {
        let iter = table.range_iter(user_range);
        Self { table, iter, seqno }
    }

    fn next_filtered(&mut self) -> Option<IterItem> {
        loop {
            match self.iter.next()? {
                Ok(value) if seqno_filter(value.key.seqno, self.seqno) => return Some(Ok(value)),
                Ok(_) => {}
                Err(e) => return Some(Err(e)),
            }
        }
    }

    fn next_back_filtered(&mut self) -> Option<IterItem> {
        loop {
            match self.iter.next_back()? {
                Ok(value) if seqno_filter(value.key.seqno, self.seqno) => return Some(Ok(value)),
                Ok(_) => {}
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

/// Leaf source over a multi-table run. Re-positioned by recomputing the run's
/// overlapping-table window; the boundary readers re-open lazily on the next
/// pull (run-internal residual allocation, unlike the alloc-free table / memtable
/// leaves — multi-table runs are L1+ and less common on the seekable path).
struct RunLeaf {
    reader: RunReader,
    comparator: SharedComparator,
    seqno: SeqNo,
}

impl RunLeaf {
    fn new(
        run: Arc<Run<crate::table::Table>>,
        user_range: UserBounds,
        seqno: SeqNo,
        comparator: SharedComparator,
    ) -> Option<Self> {
        let reader = RunReader::new_cmp(run, user_range, comparator.as_ref())?;
        Some(Self {
            reader,
            comparator,
            seqno,
        })
    }

    fn next_filtered(&mut self) -> Option<IterItem> {
        loop {
            match self.reader.next()? {
                Ok(value) if seqno_filter(value.key.seqno, self.seqno) => return Some(Ok(value)),
                Ok(_) => {}
                Err(e) => return Some(Err(e)),
            }
        }
    }

    fn next_back_filtered(&mut self) -> Option<IterItem> {
        loop {
            match self.reader.next_back()? {
                Ok(value) if seqno_filter(value.key.seqno, self.seqno) => return Some(Ok(value)),
                Ok(_) => {}
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

/// Leaf source over one memtable (active / sealed / ephemeral). Re-positioned by
/// recreating the skiplist range cursor (a `seek_ge` pointer-walk, no heap
/// allocation). Borrows the memtable from the owning [`IterState`] for `'a`.
struct MemtableLeaf<'a> {
    mt: &'a Memtable,
    range: crate::memtable::skiplist::Range<'a>,
    seqno: SeqNo,
}

impl<'a> MemtableLeaf<'a> {
    fn new(
        mt: &'a Memtable,
        internal: (Bound<InternalKey>, Bound<InternalKey>),
        seqno: SeqNo,
    ) -> Self {
        let range = mt.items.range(internal);
        Self { mt, range, seqno }
    }

    fn next_filtered(&mut self) -> Option<IterItem> {
        loop {
            let entry = self.range.next()?;
            let value = InternalValue {
                key: entry.key(),
                value: entry.value(),
            };
            if seqno_filter(value.key.seqno, self.seqno) {
                return Some(Ok(value));
            }
        }
    }

    fn next_back_filtered(&mut self) -> Option<IterItem> {
        loop {
            let entry = self.range.next_back()?;
            let value = InternalValue {
                key: entry.key(),
                value: entry.value(),
            };
            if seqno_filter(value.key.seqno, self.seqno) {
                return Some(Ok(value));
            }
        }
    }
}

/// One reseekable leaf of the seekable merge pipeline.
///
/// All three variants self-coordinate their forward/back cursors over a single
/// shrinking window (SST index span, run table window, skiplist node range), so
/// the enum is a [`CoherentMergeSource`]: mixed forward/backward consumption
/// never yields an item twice. The [`MergeSource::seek`] hook is a no-op — the
/// seekable path repositions via [`Reseekable`] (which carries both new bounds),
/// not the single-target merge-source seek.
enum SeekableLeaf<'a> {
    // The SST reader and run reader are large (owned index iterator / per-table
    // readers); box them so the enum (held one-per-source in a `Vec`) is sized to
    // the small memtable variant. This is a one-time per-leaf allocation at build,
    // not a per-seek cost.
    Table(Box<TableLeaf>),
    Run(Box<RunLeaf>),
    Memtable(MemtableLeaf<'a>),
}

impl MergeSource for SeekableLeaf<'_> {
    fn next(&mut self) -> Option<IterItem> {
        match self {
            SeekableLeaf::Table(l) => l.next_filtered(),
            SeekableLeaf::Run(l) => l.next_filtered(),
            SeekableLeaf::Memtable(l) => l.next_filtered(),
        }
    }

    fn next_back(&mut self) -> Option<IterItem> {
        match self {
            SeekableLeaf::Table(l) => l.next_back_filtered(),
            SeekableLeaf::Run(l) => l.next_back_filtered(),
            SeekableLeaf::Memtable(l) => l.next_back_filtered(),
        }
    }

    fn seek(&mut self, _target: &InternalKey) -> crate::Result<()> {
        // No-op: the seekable pipeline repositions via `Reseekable::reseek`
        // (which carries both the new lower AND upper bound). The single-target
        // `MergeSource::seek` is never invoked on this path; the leaf cursors
        // self-coordinate direction switches without it.
        Ok(())
    }
}

impl CoherentMergeSource for SeekableLeaf<'_> {}

impl Reseekable for SeekableLeaf<'_> {
    fn reseek(&mut self, ctx: &ReseekCtx) {
        match self {
            SeekableLeaf::Table(l) => l.table.reseek_range(&mut l.iter, ctx.user.clone()),
            SeekableLeaf::Run(l) => l.reader.reseek(ctx.user.clone(), l.comparator.as_ref()),
            SeekableLeaf::Memtable(l) => {
                l.range = l.mt.items.range(ctx.internal.clone());
            }
        }
    }
}

/// The concrete, [`Reseekable`] seekable merge pipeline: loser-tree merger ->
/// MVCC resolution -> drop-resolved-tombstones -> range-tombstone suppression.
/// Built once over the union range; every reposition reseeks it in place.
type SeekPipeline<'a> = RangeTombstoneFilter<
    TombstoneSkip<MvccStream<SeekingMerger<SeekableLeaf<'a>, SharedComparator>>>,
>;

/// Phase 2: build the [`SeekPipeline`] for the sub-range `[lower, upper)` from
/// already-collected sources. Runs ONCE per iterator (at construction); every
/// later reposition reseeks the returned pipeline in place rather than rebuilding
/// it. The per-source readers reuse the tested seek-to-start path (no block I/O
/// until the first `next`).
fn build_seek_pipeline<'a>(
    state: &'a IterState,
    collected: &CollectedSources,
    lower: Bound<UserKey>,
    upper: Bound<UserKey>,
    seqno: SeqNo,
) -> SeekPipeline<'a> {
    let user_range: UserBounds = (lower, upper);
    let internal = user_to_internal_bounds(&user_range);

    let mut sources: Vec<SeekableLeaf<'a>> =
        Vec::with_capacity(collected.single_tables.len() + collected.multi_runs.len() + 3);

    for table in &collected.single_tables {
        sources.push(SeekableLeaf::Table(Box::new(TableLeaf::new(
            table.clone(),
            user_range.clone(),
            seqno,
        ))));
    }
    for run in &collected.multi_runs {
        if let Some(leaf) = RunLeaf::new(
            run.clone(),
            user_range.clone(),
            seqno,
            state.comparator.clone(),
        ) {
            sources.push(SeekableLeaf::Run(Box::new(leaf)));
        }
    }
    for memtable in state.version.sealed_memtables.iter() {
        sources.push(SeekableLeaf::Memtable(MemtableLeaf::new(
            memtable,
            internal.clone(),
            seqno,
        )));
    }
    sources.push(SeekableLeaf::Memtable(MemtableLeaf::new(
        &state.version.active_memtable,
        internal.clone(),
        seqno,
    )));
    if let Some((mt, eph_seqno)) = &state.ephemeral {
        sources.push(SeekableLeaf::Memtable(MemtableLeaf::new(
            mt, internal, *eph_seqno,
        )));
    }

    let merged = SeekingMerger::new(sources, state.comparator.clone());
    let mvcc = MvccStream::new_with_comparator(
        merged,
        state.merge_operator.clone(),
        state.comparator.clone(),
    )
    .with_range_tombstones(collected.range_tombstones.clone());
    let skip = TombstoneSkip { inner: mvcc };
    // Always wrap in the range-tombstone filter: with an empty or all-invisible
    // tombstone set it suppresses nothing (a per-item visibility check), so the
    // result matches the non-seekable path's fast-path-or-filter branch while
    // staying a single concrete reseekable type.
    RangeTombstoneFilter::new_with_comparator(
        skip,
        collected.range_tombstones.clone(),
        state.comparator.clone(),
    )
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
    /// Internal self-referential merge cell: owns the collected sources and
    /// borrows them into the Phase-2 merge pipeline. Wrapped by
    /// [`SeekableTreeIter`], which adds the seek API and a lookahead buffer.
    ///
    /// Built ONCE; [`Self::reseek`] moves the leaf cursors in place, so the
    /// merge pipeline is never reconstructed across repositions.
    struct SeekableCell {
        owner: SeekableOwner,

        #[covariant]
        dependent: SeekPipeline,
    }
);

impl SeekableCell {
    fn build(owner: SeekableOwner) -> Self {
        Self::new(owner, |o| {
            build_seek_pipeline(
                &o.state,
                &o.collected,
                o.lower.clone(),
                o.upper.clone(),
                o.seqno,
            )
        })
    }

    /// Re-position the borrowed pipeline in place to the bounds in `ctx`,
    /// without rebuilding it. The owner (version snapshot + collected sources)
    /// is untouched.
    fn reseek(&mut self, ctx: &ReseekCtx) {
        self.with_dependent_mut(|_, pipeline| pipeline.reseek(ctx));
    }
}

impl Iterator for SeekableCell {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|_, iter| iter.next())
    }
}

impl DoubleEndedIterator for SeekableCell {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|_, iter| iter.next_back())
    }
}

/// A range iterator that can reposition (seek) in place without reopening
/// per-SST readers or rebuilding the merge stack.
///
/// Built once over a union range; [`Self::seek_to`], [`Self::seek_to_for_prev`],
/// and `Self::reposition` move the leaf cursors in place (SST index re-seek /
/// skiplist `seek_ge` / run-window recompute) while reusing the loser-tree
/// merger, MVCC stream, and tombstone filter, so a tight seek loop does not
/// reconstruct the pipeline.
///
/// A one-item lookahead buffer backs [`Self::peek_key`], which reads the next
/// key without consuming it (a leapfrog join takes the max of several
/// iterators' current keys before deciding where to seek next).
pub struct SeekableTreeIter {
    cell: SeekableCell,
    /// `None` = nothing peeked; `Some(None)` = peeked past the end;
    /// `Some(Some(item))` = buffered front item (the std `Peekable` shape).
    #[expect(
        clippy::option_option,
        reason = "std `Peekable` buffer shape: outer None = not yet peeked, \
                  inner None = peeked past the end, Some(Some) = buffered item"
    )]
    peeked: Option<Option<crate::Result<InternalValue>>>,
}

impl SeekableTreeIter {
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
        Self {
            cell: SeekableCell::build(SeekableOwner {
                state,
                collected,
                seqno,
                lower: union_lower,
                upper: union_upper,
            }),
            peeked: None,
        }
    }

    /// Re-position the merge pipeline to the sub-range `[lower, upper)` IN PLACE:
    /// the loser-tree merger, MVCC stream, and range-tombstone filter are reused;
    /// only the leaf cursors move (an SST index re-seek / skiplist `seek_ge` /
    /// run-window recompute). No merge-stack reconstruction, no source re-collection
    /// (no block I/O until the next `next`/`next_back`).
    pub(crate) fn reposition(&mut self, lower: Bound<UserKey>, upper: Bound<UserKey>) {
        let user_range: UserBounds = (lower, upper);
        let internal = user_to_internal_bounds(&user_range);
        let ctx = ReseekCtx {
            user: user_range,
            internal,
        };
        self.cell.reseek(&ctx);
        // The lookahead came from the old position; drop it.
        self.peeked = None;
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
            let union = &self.cell.borrow_owner().collected.union;
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
            let union = &self.cell.borrow_owner().collected.union;
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
        self.cell.borrow_owner().state.version.version.clone()
    }

    /// Return the user key the next [`Iterator::next`] would yield, without
    /// consuming it. Buffers one item; cleared by any reposition / seek.
    ///
    /// A successful key is cloned (cheap: `UserKey` is reference-counted) and
    /// the item stays buffered for `next`, so `peek_key()` followed by `next()`
    /// observes the SAME entry.
    ///
    /// # Error is a consuming peek
    ///
    /// `crate::Error` is not `Clone`, so a `peek_key()` that returns `Err(...)`
    /// CONSUMES the failing position: the error is moved out of the buffer and
    /// the iterator is logically advanced past it. A following `next()` yields
    /// the entry AFTER the error, NOT the error again. Treat a peeked `Err` as
    /// you would a consumed one (surface / propagate it; do not also expect
    /// `next()` to re-report it). The leapfrog / zig-zag join callers this
    /// method is built for read `peek_key()`, propagate any `Err`, and never
    /// fall through to a redundant `next()` on the failing iterator, so the
    /// distinction does not bite them; it is documented here for any caller that
    /// treats `peek_key` as a pure lookahead.
    pub fn peek_key(&mut self) -> Option<crate::Result<UserKey>> {
        if self.peeked.is_none() {
            self.peeked = Some(self.cell.next());
        }
        if matches!(self.peeked, Some(Some(Err(_)))) {
            return match self.peeked.take() {
                Some(Some(Err(e))) => Some(Err(e)),
                _ => None,
            };
        }
        match &self.peeked {
            Some(Some(Ok(item))) => Some(Ok(item.key.user_key.clone())),
            _ => None,
        }
    }
}

impl Iterator for SeekableTreeIter {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.peeked.take() {
            Some(buffered) => buffered,
            None => self.cell.next(),
        }
    }
}

impl DoubleEndedIterator for SeekableTreeIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        // The buffer holds the FRONT item, so pull from the back first and only
        // fall back to the buffered front once the back is exhausted.
        match self.cell.next_back() {
            Some(item) => Some(item),
            None => self.peeked.take().flatten(),
        }
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

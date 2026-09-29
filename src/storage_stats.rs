// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Read-only storage introspection: how much is stored, the average shape of a
//! stored entry, and an estimate of how many more entries fit in a byte budget.
//!
//! Computed from the live version's table + blob-file metadata plus one
//! size-stat per live file (the same accounting `Tree::create_checkpoint`
//! uses), so it never touches the data blocks. The blob reference figures also
//! read each table's blob-link section, once: the table keeps it after. See
//! [`crate::AbstractTree::storage_stats`].

use crate::version::Version;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// Coarse storage state of a tree.
///
/// With storage admission gating off (no configured quota and a backend that
/// cannot report free space) a tree reports [`Self::Healthy`] or, mid-run,
/// [`Self::CompactionInProgress`]. Once gating is active (bounded capacity), an
/// idle tree instead reports compaction availability:
/// [`Self::FullCompactionAvailable`] when a full compaction has working room,
/// [`Self::TightCompactionAvailable`] when only the opt-in tight-space mode
/// would fit, and [`Self::ReadOnlyOutOfSpace`] when the write gate is closed
/// (this takes precedence over a concurrent compaction).
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum StorageStatus {
    /// Normal operation: writes and a full compaction are available.
    Healthy,
    /// Enough free space for a normal (full) compaction.
    FullCompactionAvailable,
    /// Not enough space for a full compaction, but the opt-in tight-space
    /// (incremental-reclaim) compaction mode can still run.
    TightCompactionAvailable,
    /// Out of space: the tree is read-only until space is freed or the quota
    /// is raised.
    ReadOnlyOutOfSpace,
    /// A compaction is currently running.
    CompactionInProgress,
}

/// A point-in-time snapshot of a tree's on-disk storage footprint and the
/// average shape of a stored entry.
///
/// All byte figures are on-disk (post-compression, including any per-block
/// overhead and blob files). Averages are over every stored entry version, so
/// they pair with [`Self::item_count`].
#[must_use]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct StorageStats {
    /// Total on-disk bytes of all live SSTs plus blob files (including a
    /// restricted table's live `.restrict-bound` sidecar): how much is
    /// **occupied**. Pairs with [`Self::capacity_bytes`] / [`Self::available_bytes`]
    /// for an "X of Y used" view in a single call.
    pub used_bytes: u64,

    /// Total bytes the tree may occupy: the tighter of a configured byte quota
    /// (`storage_limit_bytes`) and the physical disk headroom (free space plus
    /// what is already used), across every volume the tree writes to. `None`
    /// when unbounded: no quota set AND the backend cannot report free space.
    pub capacity_bytes: Option<u64>,

    /// Free room left before the tree turns read-only: `capacity_bytes - used_bytes`
    /// (saturating). `None` exactly when [`Self::capacity_bytes`] is `None`
    /// (unbounded).
    pub available_bytes: Option<u64>,

    /// Whether a compaction can still run given the remaining free space (it
    /// needs working room to write merged output). `true` when unbounded or
    /// when at least [`Self::tight_compaction_bytes`] of free space remains;
    /// `false` when the disk is too full for a compaction to make progress. The
    /// finer full-vs-tight distinction is carried by [`Self::status`].
    pub compaction_possible: bool,

    /// Estimated free space (bytes) a FULL compaction needs for its transient
    /// output while the inputs still exist: the largest level's on-disk size
    /// (an upper bound on a single merge's input set). A full compaction has
    /// room when [`Self::available_bytes`] `>=` this. Pair with `used_bytes` /
    /// `capacity_bytes` to draw a capacity gauge: `used` → `used + tight_compaction_bytes`
    /// → `used + full_compaction_bytes` → `capacity`.
    pub full_compaction_bytes: u64,

    /// Estimated free space (bytes) a minimal (tight) space-reclaiming
    /// compaction needs to make forward progress: the reserved working floor.
    /// Tight compaction has room when [`Self::available_bytes`] `>=` this.
    pub tight_compaction_bytes: u64,

    /// Number of live entries (all versions) across all live SSTs.
    pub item_count: u64,

    /// Number of live SSTs.
    pub table_count: u64,

    /// Average on-disk bytes per entry (`used_bytes / item_count`), or `0` when
    /// the tree is empty. This is the figure
    /// [`Self::estimated_remaining_entries`] divides a budget by.
    pub avg_entry_on_disk_bytes: u64,

    /// Average user-key byte length per entry, or `None` if any live table was
    /// written before per-table key/value byte sums were recorded (the average
    /// key/value split is only exact when every table carries the figures).
    pub avg_key_bytes: Option<u64>,

    /// Average value byte length per entry, or `None` under the same condition
    /// as [`Self::avg_key_bytes`].
    pub avg_value_bytes: Option<u64>,

    /// Estimated bytes a full compaction could reclaim, from the
    /// weak-tombstone-reclaimable entry count times the average on-disk entry
    /// size. An estimate, not an exact figure.
    pub reclaimable_bytes_estimate: u64,

    /// Coarse storage state.
    pub status: StorageStatus,

    /// Blob files the tree's tables reference, and how many of them a scan
    /// interleaves where their key spans overlap most, across every level. All
    /// zero for a tree that does not separate values.
    pub blob_references: BlobReferenceStats,
}

/// How scattered the blob values behind a set of tables are: a table, a level,
/// or the whole tree.
///
/// The two figures answer different questions and must not be read as one.
/// [`Self::count`] is how many blob files are referenced at all; thirty-two
/// files each holding the values of one consecutive key range give a count of
/// 32 while a scan still reads one file at a time. [`Self::depth`] is how many
/// of those files a scan has to interleave where their key spans overlap most.
#[must_use]
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct BlobReferenceStats {
    /// Distinct blob files referenced (fan-out). A statistic, not a measure
    /// of locality.
    pub count: u64,
    /// The largest number of distinct blob files whose referenced key spans
    /// cover one key: an estimate of how many files a range scan through
    /// that key interleaves.
    ///
    /// An estimate from key spans alone. A span runs from the first to the
    /// last key that references its file, so keys inside it that point
    /// elsewhere still count as covered; a restricted table still reports the
    /// spans of the whole table it was cut from. The figure cannot see the
    /// block cache, how reads coalesce, or that two logical reads may land in
    /// one physical one: it is what a locality trigger can act on, and the
    /// blob-file reads of a scan are what show whether acting paid off.
    pub depth: u64,
}

/// Blob reference count and depth over the given tables' links. The tables
/// must share one comparator (one tree's).
///
/// # Errors
///
/// When a table's `linked_blob_files` section cannot be read or parsed.
pub(crate) fn blob_reference_stats<'a>(
    tables: impl IntoIterator<Item = &'a crate::table::Table>,
) -> crate::Result<BlobReferenceStats> {
    let mut comparator = None;
    let mut spans = Vec::new();
    collect_spans(tables, &mut comparator, &mut spans)?;
    Ok(comparator.map_or_else(BlobReferenceStats::default, |cmp| {
        span_stats(&mut spans, cmp.as_ref())
    }))
}

/// Per blob file the tables reference, the depth where its key spans overlap
/// most: the largest number of distinct files covering one key inside its
/// spans. A file whose figure stays within a depth limit does not take part in
/// any region that exceeds it. Ordered by file id; empty for tables without
/// blob links.
///
/// # Errors
///
/// When a table's `linked_blob_files` section cannot be read or parsed.
pub(crate) fn blob_file_depths<'a>(
    tables: impl IntoIterator<Item = &'a crate::table::Table>,
) -> crate::Result<Vec<(crate::vlog::BlobFileId, u64)>> {
    let mut comparator = None;
    let mut spans = Vec::new();
    collect_spans(tables, &mut comparator, &mut spans)?;
    Ok(comparator.map_or_else(Vec::new, |cmp| span_depths(&mut spans, cmp.as_ref())))
}

/// [`blob_file_depths`] over `(blob file, first key, last key)` spans ordered
/// by `cmp`. Sorts `spans` in place.
fn span_depths(
    spans: &mut [Span<'_>],
    cmp: &dyn crate::comparator::UserComparator,
) -> Vec<(crate::vlog::BlobFileId, u64)> {
    let (merged, _) = merge_spans(spans, cmp);

    // The open count after each event, and where each span starts and ends in
    // that sequence: a span's figure is the largest open count between its
    // start and its end. Answered by a range-maximum table, so a deeply
    // interleaved merge costs O(n log n) rather than a pass over every open
    // span at every start.
    let events = sweep_events(&merged, cmp);
    let mut open_after = Vec::with_capacity(events.len());
    let mut bounds = alloc::vec![(0usize, 0usize); merged.len()];
    let mut open = 0u64;
    for (position, &(_, is_end, index)) in events.iter().enumerate() {
        if let Some(bound) = bounds.get_mut(index) {
            if is_end {
                open -= 1;
                bound.1 = position;
            } else {
                open += 1;
                bound.0 = position;
            }
        }
        open_after.push(open);
    }
    let range_max = RangeMax::new(open_after);

    // `merged` is sorted by file, so one pass folds a file's spans together.
    // A span's end event sorts after its start, so its open counts are those
    // from its start up to just before its end.
    let mut per_file: Vec<(crate::vlog::BlobFileId, u64)> = Vec::new();
    for ((id, _, _), (start, end)) in merged.iter().zip(bounds) {
        debug_assert!(end > start, "a span ends after it starts");
        let depth = range_max.max(start, end - 1);
        match per_file.last_mut() {
            Some((last, value)) if last == id => *value = (*value).max(depth),
            _ => per_file.push((*id, depth)),
        }
    }
    per_file
}

/// Maximum over any range of a fixed sequence in O(1), after an
/// O(n log n) build: `levels[k][i]` is the maximum of `2^k` values from `i`.
struct RangeMax {
    levels: Vec<Vec<u64>>,
}

impl RangeMax {
    fn new(values: Vec<u64>) -> Self {
        let mut levels = alloc::vec![values];
        let mut width = 1;
        // Level `k` holds `n - 2^k + 1` maxima, so level `k + 1` exists while
        // `n >= 2^(k + 1)`: while the current level is longer than its width.
        while let Some(previous) = levels.last()
            && previous.len() > width
        {
            let next: Vec<u64> = previous
                .iter()
                .zip(previous.iter().skip(width))
                .map(|(a, b)| (*a).max(*b))
                .collect();
            levels.push(next);
            width *= 2;
        }
        Self { levels }
    }

    /// The maximum of the values at `from..=to`, `from <= to`.
    fn max(&self, from: usize, to: usize) -> u64 {
        debug_assert!(from <= to, "an empty range has no maximum");
        let len = to - from + 1;
        let level = (usize::BITS - 1 - len.leading_zeros()) as usize;
        let Some(values) = self.levels.get(level) else {
            return 0;
        };
        // `2^level <= len`, so the second window starts at or after `from`.
        let right = to + 1 - (1 << level);
        let left = values.get(from).copied().unwrap_or(0);
        left.max(values.get(right).copied().unwrap_or(0))
    }
}

/// Chooses which of `ranked` to relocate along with `fixed`, best first while
/// their cost fits `budget`. A file is chosen only when its spans in `tables`
/// overlap those of another relocated file of its group, so relocating it
/// merges it with something: either one already relocated, or the cheapest
/// file it overlaps, chosen with it when both fit.
///
/// `fixed` is `(file, group)`, sorted by file id, relocated whatever the
/// choice. `ranked` is `(file, group, cost)`, best first, disjoint from
/// `fixed`. Returns whether each of `ranked` is chosen, and the cost spent.
///
/// # Errors
///
/// When a table's `linked_blob_files` section cannot be read or parsed.
pub(crate) fn pick_overlapping_blob_files<'a>(
    tables: impl IntoIterator<Item = &'a crate::table::Table>,
    fixed: &[(crate::vlog::BlobFileId, usize)],
    ranked: &[(crate::vlog::BlobFileId, usize, u64)],
    budget: u64,
) -> crate::Result<(Vec<bool>, u64)> {
    let mut comparator = None;
    let mut spans = Vec::new();
    collect_spans(tables, &mut comparator, &mut spans)?;
    Ok(match comparator {
        Some(cmp) => pick_overlapping(&mut spans, fixed, ranked, budget, cmp.as_ref()),
        None => (alloc::vec![false; ranked.len()], 0),
    })
}

/// How a relocation plan takes a file: relocated anyway, in a group, or a
/// candidate at a rank.
#[derive(Clone, Copy)]
enum Role {
    Fixed(usize),
    Ranked(usize),
}

/// [`pick_overlapping_blob_files`] over `(blob file, first key, last key)`
/// spans ordered by `cmp`. Drops the spans of other files and sorts the rest.
fn pick_overlapping(
    spans: &mut Vec<Span<'_>>,
    fixed: &[(crate::vlog::BlobFileId, usize)],
    ranked: &[(crate::vlog::BlobFileId, usize, u64)],
    budget: u64,
    cmp: &dyn crate::comparator::UserComparator,
) -> (Vec<bool>, u64) {
    let mut roles: Vec<(crate::vlog::BlobFileId, Role)> = fixed
        .iter()
        .map(|&(id, group)| (id, Role::Fixed(group)))
        .chain(
            ranked
                .iter()
                .enumerate()
                .map(|(rank, &(id, _, _))| (id, Role::Ranked(rank))),
        )
        .collect();
    roles.sort_unstable_by_key(|&(id, _)| id);
    let role_of = |id: crate::vlog::BlobFileId| {
        roles
            .binary_search_by_key(&id, |&(file, _)| file)
            .ok()
            .and_then(|at| roles.get(at))
            .map(|&(_, role)| role)
    };
    spans.retain(|&(id, _, _)| role_of(id).is_some());
    let (merged, _) = merge_spans(spans, cmp);
    let events = sweep_events(&merged, cmp);
    let positions = events.len();

    // Where each span starts and ends in the sweep. Where spans meet at one
    // key, the start sorts first, so two spans overlap exactly when each
    // starts before the other ends. A file's merged spans never overlap each
    // other.
    let mut bounds = alloc::vec![(0usize, 0usize); merged.len()];
    for (position, &(_, is_end, index)) in events.iter().enumerate() {
        if let Some(bound) = bounds.get_mut(index) {
            if is_end {
                bound.1 = position;
            } else {
                bound.0 = position;
            }
        }
    }
    let group_and_rank = |index: usize| {
        let role = merged.get(index).and_then(|&(id, _, _)| role_of(id))?;
        Some(match role {
            Role::Fixed(group) => (group, None),
            Role::Ranked(rank) => (ranked.get(rank)?.1, Some(rank)),
        })
    };
    let groups = fixed
        .iter()
        .map(|&(_, group)| group)
        .chain(ranked.iter().map(|&(_, group, _)| group))
        .max()
        .map_or(0, |group| group + 1);
    let mut spans_of: Vec<Vec<(usize, usize)>> = alloc::vec![Vec::new(); ranked.len()];
    for (index, &bound) in bounds.iter().enumerate() {
        if let Some((_, Some(rank))) = group_and_rank(index)
            && let Some(own) = spans_of.get_mut(rank)
        {
            own.push(bound);
        }
    }

    // The cheapest candidate of another file each candidate overlaps, in one
    // sweep: when a span ends, the spans of its group that started before
    // that and end after its start are the ones it overlaps. Each span enters
    // its group's index as it starts, keyed by its end; positions count down
    // so the spans ending after a start form a prefix.
    let mut partner: Vec<Option<(u64, usize)>> = alloc::vec![None; ranked.len()];
    let mut started: Vec<PrefixCheapest> = (0..groups)
        .map(|_| PrefixCheapest::new(positions))
        .collect();
    for &(_, is_end, index) in &events {
        let Some((group, Some(rank))) = group_and_rank(index) else {
            continue;
        };
        let (Some(&(start, end)), Some(&(_, _, cost)), Some(index_of_group)) =
            (bounds.get(index), ranked.get(rank), started.get_mut(group))
        else {
            continue;
        };
        if is_end {
            let found = index_of_group
                .before(positions - 1 - start)
                .best_except(rank);
            if let (Some(found), Some(best)) = (found, partner.get_mut(rank))
                && best.is_none_or(|best| found < best)
            {
                *best = Some(found);
            }
        } else {
            index_of_group.add(positions - 1 - end, (cost, rank));
        }
    }

    // Best first: a candidate joins when a relocated span of its group
    // overlaps its own, which the relocated spans' largest end among those
    // starting before its end tells; otherwise it joins together with its
    // cheapest partner when both fit.
    let mut relocated: Vec<PrefixMax> = (0..groups).map(|_| PrefixMax::new(positions)).collect();
    for (index, &(start, end)) in bounds.iter().enumerate() {
        if let Some((group, None)) = group_and_rank(index)
            && let Some(tree) = relocated.get_mut(group)
        {
            tree.raise(start, end);
        }
    }
    let mut chosen = alloc::vec![false; ranked.len()];
    let mut spent = 0u64;
    for (rank, &(_, group, cost)) in ranked.iter().enumerate() {
        // Costs are sums of on-disk sizes, bounded by filesystem capacity.
        if chosen.get(rank) != Some(&false) || spent + cost > budget {
            continue;
        }
        let own = spans_of.get(rank).map_or(&[][..], Vec::as_slice);
        let joins = relocated
            .get(group)
            .is_some_and(|tree| own.iter().any(|&(start, end)| tree.max_before(end) > start));
        let with = if joins {
            None
        } else {
            match partner.get(rank).copied().flatten() {
                Some((partner_cost, partner_rank)) if spent + cost + partner_cost <= budget => {
                    Some(partner_rank)
                }
                _ => continue,
            }
        };
        for pick in core::iter::once(rank).chain(with) {
            let (Some(flag), Some(&(_, _, pick_cost)), Some(tree)) = (
                chosen.get_mut(pick),
                ranked.get(pick),
                relocated.get_mut(group),
            ) else {
                continue;
            };
            *flag = true;
            spent += pick_cost;
            for &(start, end) in spans_of.get(pick).map_or(&[][..], Vec::as_slice) {
                tree.raise(start, end);
            }
        }
    }
    (chosen, spent)
}

/// Largest value over any prefix of positions, values only ever raised.
struct PrefixMax(Vec<usize>);

impl PrefixMax {
    fn new(positions: usize) -> Self {
        Self(alloc::vec![0; positions])
    }

    /// Raises position `at` to at least `value`.
    fn raise(&mut self, at: usize, value: usize) {
        let mut node = at + 1;
        while let Some(slot) = self.0.get_mut(node - 1) {
            *slot = (*slot).max(value);
            node += node & node.wrapping_neg();
        }
    }

    /// Largest value at positions `..before`; zero when there is none.
    fn max_before(&self, before: usize) -> usize {
        let mut node = before;
        let mut max = 0;
        while node > 0 {
            if let Some(&value) = self.0.get(node - 1) {
                max = max.max(value);
            }
            node &= node - 1;
        }
        max
    }
}

/// The two cheapest `(cost, rank)` offers of distinct ranks.
#[derive(Clone, Copy, Default)]
struct TwoCheapest([Option<(u64, usize)>; 2]);

impl TwoCheapest {
    fn add(&mut self, offer: (u64, usize)) {
        let [first, second] = self.0;
        let mut all = [first, second, Some(offer)];
        all.sort_unstable_by_key(|offer| offer.unwrap_or((u64::MAX, usize::MAX)));
        let mut kept = [None; 2];
        let mut slots = kept.iter_mut();
        let mut first_rank = None;
        for offer in all.into_iter().flatten() {
            if first_rank == Some(offer.1) {
                continue;
            }
            let Some(slot) = slots.next() else {
                break;
            };
            first_rank.get_or_insert(offer.1);
            *slot = Some(offer);
        }
        self.0 = kept;
    }

    /// The cheapest offer of a rank other than `rank`.
    fn best_except(&self, rank: usize) -> Option<(u64, usize)> {
        self.0.into_iter().flatten().find(|offer| offer.1 != rank)
    }
}

/// [`TwoCheapest`] over any prefix of positions, offers only ever added.
struct PrefixCheapest(Vec<TwoCheapest>);

impl PrefixCheapest {
    fn new(positions: usize) -> Self {
        Self(alloc::vec![TwoCheapest::default(); positions])
    }

    fn add(&mut self, at: usize, offer: (u64, usize)) {
        let mut node = at + 1;
        while let Some(slot) = self.0.get_mut(node - 1) {
            slot.add(offer);
            node += node & node.wrapping_neg();
        }
    }

    /// The offers at positions `..before`.
    fn before(&self, before: usize) -> TwoCheapest {
        let mut node = before;
        let mut out = TwoCheapest::default();
        while node > 0 {
            if let Some(slot) = self.0.get(node - 1) {
                for offer in slot.0.into_iter().flatten() {
                    out.add(offer);
                }
            }
            node &= node - 1;
        }
        out
    }
}

/// A `(blob file, first key, last key)` span referenced from a table.
type Span<'k> = (
    crate::vlog::BlobFileId,
    &'k crate::UserKey,
    &'k crate::UserKey,
);

/// Appends every blob-link span of `tables` and remembers their comparator.
fn collect_spans<'a>(
    tables: impl IntoIterator<Item = &'a crate::table::Table>,
    comparator: &mut Option<crate::comparator::SharedComparator>,
    spans: &mut Vec<Span<'a>>,
) -> crate::Result<()> {
    for table in tables {
        comparator.get_or_insert_with(|| table.comparator.clone());
        spans.extend(
            table
                .blob_links()?
                .iter()
                .map(|link| (link.blob_file_id, &link.first_key, &link.last_key)),
        );
    }
    Ok(())
}

/// [`BlobReferenceStats`] of `(blob file, first key, last key)` spans ordered
/// by `cmp`. Spans of one file (from different tables) are merged where they
/// overlap, so the sweep counts distinct files, not references. Sorts `spans`
/// in place.
fn span_stats(
    spans: &mut [Span<'_>],
    cmp: &dyn crate::comparator::UserComparator,
) -> BlobReferenceStats {
    let (merged, count) = merge_spans(spans, cmp);
    let (mut open, mut depth) = (0u64, 0u64);
    for (_, is_end, _) in sweep_events(&merged, cmp) {
        if is_end {
            open -= 1;
        } else {
            open += 1;
            depth = depth.max(open);
        }
    }
    BlobReferenceStats { count, depth }
}

/// Merges the overlapping spans of each file, sorted by file then first key,
/// and counts the distinct files. Sorts `spans` in place.
fn merge_spans<'k>(
    spans: &mut [Span<'k>],
    cmp: &dyn crate::comparator::UserComparator,
) -> (Vec<Span<'k>>, u64) {
    use core::cmp::Ordering;

    // A span is closed at both ends. A recorded span is always ordered; one
    // that is not is taken by its bounds rather than as an empty span.
    for span in spans.iter_mut() {
        if cmp.compare(span.1, span.2) == Ordering::Greater {
            core::mem::swap(&mut span.1, &mut span.2);
        }
    }
    spans.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| cmp.compare(a.1, b.1)));

    // Sorted by file, so each change of file is one more distinct file.
    let mut merged: Vec<Span<'k>> = Vec::with_capacity(spans.len());
    let mut count = 0u64;
    for &span in spans.iter() {
        match merged.last_mut() {
            Some((id, _, last))
                if *id == span.0 && cmp.compare(span.1, last) != Ordering::Greater =>
            {
                if cmp.compare(span.2, last) == Ordering::Greater {
                    *last = span.2;
                }
            }
            last => {
                if last.is_none_or(|(id, _, _)| *id != span.0) {
                    count += 1;
                }
                merged.push(span);
            }
        }
    }
    (merged, count)
}

/// The start and end events of `merged`, as `(key, is end, span index)` in
/// key order. Starts come before ends at an equal key: spans that meet at one
/// key overlap.
fn sweep_events<'k>(
    merged: &[Span<'k>],
    cmp: &dyn crate::comparator::UserComparator,
) -> Vec<(&'k crate::UserKey, bool, usize)> {
    let mut events: Vec<(&'k crate::UserKey, bool, usize)> = merged
        .iter()
        .enumerate()
        .flat_map(|(index, &(_, first, last))| [(first, false, index), (last, true, index)])
        .collect();
    events.sort_by(|a, b| cmp.compare(a.0, b.0).then(a.1.cmp(&b.1)));
    events
}

/// Approximate size of a key range, estimated from SST block-index offsets and
/// the active memtable WITHOUT reading any data block. Returned by
/// [`crate::AbstractTree::approximate_range_stats`].
///
/// Both figures are estimates from the same in-range fraction per source: each
/// overlapping SST's data-block offsets are interpolated at the range
/// boundaries (block granularity) and that fraction is applied to the SST's
/// byte span and its entry count, while each memtable contributes its in-range
/// skiplist count and the matching share of its size. Accuracy is typically
/// within ~10-15% on roughly-uniform data; it is intended for query planning
/// (split-point selection, cost-based join ordering), not exact accounting.
#[must_use]
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct ApproximateRangeStats {
    /// Estimated on-disk bytes occupied by the range across all overlapping
    /// SSTs (key + pointer + apportioned blob bytes) plus the active and sealed
    /// memtables' in-range share. `0` for an empty range.
    pub bytes: u64,

    /// Estimated number of entry versions in the range: the sum, over each
    /// overlapping SST, of `item_count × in-range fraction`, plus each
    /// memtable's in-range skiplist count. `0` for an empty range.
    pub key_count: u64,
}

/// Size and entry count of one stored segment (SST), for per-segment tiering and
/// erasure-coding placement decisions.
#[must_use]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct SegmentStats {
    /// Identifier of the segment's SST within its tree.
    pub table_id: crate::TableId,
    /// LSM level the segment lives in (`0` is the newest / smallest level).
    pub level: usize,
    /// Physical on-disk bytes of the segment's SST file.
    pub used_bytes: u64,
    /// Number of entry versions stored in the segment.
    pub item_count: u64,
    /// Cumulative point reads that consulted this segment's data since it was
    /// created: only reads that pass the segment's seqno-range and bloom gates
    /// count (a bloom miss is not counted), so this tracks data hotness rather
    /// than raw probe frequency. A monotonic counter, not a rate: derive a
    /// read-rate / EMA from the delta between successive polls. `0` when never
    /// read.
    pub reads: u64,
    /// Unix seconds of the segment's most recent data-consulting read, or `0` if
    /// never read (or on a no-std build, which keeps no clock).
    pub last_access_secs: u64,
    /// Blob files the segment references, and how many of them a scan through
    /// it interleaves where their key spans overlap most.
    pub blob_references: BlobReferenceStats,
}

/// Per-LSM-level size + entry aggregates with the contributing segments, for
/// tiering and erasure-coding placement (which level / segment is large enough
/// to demote, EC-encode, or migrate).
///
/// Cheap to read: derived from version metadata plus one file-size stat per
/// segment, never a data-block scan (the blob reference figures read each
/// segment's blob-link section once). The per-level totals reconcile with the
/// tree-level [`StorageStats`]: summed across levels they equal the SST portion
/// of [`StorageStats::used_bytes`] and [`StorageStats::item_count`] (blob files
/// are tracked separately).
#[must_use]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LevelStats {
    /// LSM level index (`0` is the newest / smallest level).
    pub level: usize,
    /// Number of segments (SSTs) in the level.
    pub segment_count: usize,
    /// Physical on-disk bytes summed across the level's segments.
    pub used_bytes: u64,
    /// Entry versions summed across the level's segments.
    pub item_count: u64,
    /// Cumulative point-read probes summed across the level's segments.
    pub reads: u64,
    /// Most recent point-read probe across the level's segments, in unix
    /// seconds, or `0` if none was ever read.
    pub last_access_secs: u64,
    /// Blob files the level's segments reference, and how many of them a scan
    /// through the level interleaves where their key spans overlap most.
    pub blob_references: BlobReferenceStats,
    /// Per-segment breakdown, in level (run / table) order.
    pub segments: Vec<SegmentStats>,
}

/// Approximate cardinality and selectivity of a key range, for cost-based query
/// planning (join ordering, scan-vs-seek).
///
/// Both figures derive from the per-data-block zone map (per-block row counts +
/// key ranges) when present, falling back to the byte-fraction estimate of
/// [`ApproximateRangeStats`] otherwise. They are estimates at block granularity,
/// never exact.
#[must_use]
#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub struct RangeCardinality {
    /// Estimated number of rows (entry versions) the range covers: the sum of
    /// the per-block row counts of every data block whose key range overlaps the
    /// query range, plus each memtable's in-range count. `0` for an empty range.
    pub rows: u64,

    /// Estimated fraction of the tree's rows the range selects, in `0.0..=1.0`:
    /// `rows / total_rows`. Monotonic in predicate tightness (a narrower range
    /// never yields a larger selectivity). `0.0` when the tree is empty.
    pub selectivity: f64,
}

/// Grouped, object-safe read-only storage-statistics surface.
///
/// A coherent view over a tree's non-query statistics: on-disk footprint
/// ([`storage_stats`](Self::storage_stats)), per-level / per-segment sizing
/// ([`level_segment_stats`](Self::level_segment_stats)), compaction debt
/// ([`compaction_debt`](Self::compaction_debt)), and block-cache health
/// (`cache_stats`, behind the `metrics` feature). A planner / tiering / capacity consumer
/// bounds on `T: StorageStatistics` (or `&dyn StorageStatistics`) and a test can
/// supply a mock. Every [`AbstractTree`](crate::AbstractTree) implements it via a
/// blanket impl (`impl<T: AbstractTree + ?Sized> StorageStatistics for T`).
///
/// The per-query range estimators
/// ([`approximate_range_stats`](crate::AbstractTree::approximate_range_stats),
/// [`approximate_range_cardinality`](crate::AbstractTree::approximate_range_cardinality))
/// are generic over the range type and so not object-safe; they stay on
/// [`AbstractTree`](crate::AbstractTree) rather than joining this trait.
pub trait StorageStatistics {
    /// On-disk footprint and average entry shape: used / capacity / available
    /// bytes, item & table counts, average entry size, reclaimable-bytes
    /// estimate, and a coarse [`StorageStatus`]. See
    /// [`StorageStats::estimated_remaining_entries`] for a budget projection.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// use lsm_tree::{AbstractTree, Config, StorageStatistics};
    ///
    /// let folder = tempfile::tempdir()?;
    /// let tree = Config::new(&folder, Default::default(), Default::default()).open()?;
    /// for i in 0..100u32 {
    ///     tree.insert(format!("k{i:04}"), "v", 0);
    /// }
    /// tree.flush_active_memtable(0)?;
    ///
    /// // Both traits are in scope, so disambiguate the shared method name.
    /// let stats = StorageStatistics::storage_stats(&tree)?;
    /// assert_eq!(stats.item_count, 100);
    /// // Roughly how many more average-shaped entries fit in another 1 MiB.
    /// let _headroom = stats.estimated_remaining_entries(1024 * 1024);
    /// #
    /// # Ok::<(), TreeError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if a live file's size cannot be stat-ed, or a table's
    /// blob-link section cannot be read or parsed.
    fn storage_stats(&self) -> crate::Result<StorageStats>;

    /// Per-LSM-level and per-segment size + entry-count stats, for tiering and
    /// erasure-coding placement decisions (which level / segment is large enough
    /// to demote, EC-encode, or migrate).
    ///
    /// Cheap: derived from the live version's metadata plus one file-size stat
    /// per segment (no data-block scan); in a tree that separates values, each
    /// segment's blob-link section is also read once, then kept. The per-level
    /// totals reconcile with
    /// [`storage_stats`](Self::storage_stats): summed across levels they equal
    /// the SST portion of [`StorageStats::used_bytes`] and
    /// [`StorageStats::item_count`].
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// use lsm_tree::{AbstractTree, Config, StorageStatistics};
    ///
    /// let folder = tempfile::tempdir()?;
    /// let tree = Config::new(&folder, Default::default(), Default::default()).open()?;
    /// for i in 0..100u32 {
    ///     tree.insert(format!("k{i:04}"), "v", 0);
    /// }
    /// tree.flush_active_memtable(0)?;
    ///
    /// // Both traits are in scope, so disambiguate the shared method names.
    /// let levels = StorageStatistics::level_segment_stats(&tree)?;
    /// let total: u64 = levels.iter().map(|l| l.item_count).sum();
    /// assert_eq!(total, StorageStatistics::storage_stats(&tree)?.item_count);
    /// #
    /// # Ok::<(), TreeError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if a segment's file size cannot be stat-ed, or its
    /// blob-link section cannot be read or parsed.
    fn level_segment_stats(&self) -> crate::Result<Vec<LevelStats>>;

    /// Estimated bytes pending compaction under `strategy`: on-disk data above
    /// its level's target that must eventually be rewritten downward (a `RocksDB`
    /// `estimate-pending-compaction-bytes` analog), a compaction-debt signal for a
    /// scheduler / tiering consumer.
    ///
    /// The strategy is a caller argument because the engine does not own a
    /// configured compaction strategy (it is injected per compaction run); a
    /// `&dyn` keeps this object-safe. Returns `0` for strategies without a
    /// size-target notion of debt (FIFO, drop-range), or when the tree is at or
    /// below its target shape. See
    /// [`CompactionStrategy::pending_compaction_bytes`](crate::compaction::CompactionStrategy::pending_compaction_bytes).
    fn compaction_debt(&self, strategy: &dyn crate::compaction::CompactionStrategy) -> u64;

    /// A point-in-time [`CacheStats`](crate::CacheStats) snapshot of block-cache
    /// effectiveness (cumulative hit / miss counts and rate) and occupancy
    /// (current size against capacity).
    ///
    /// The stable, owned observability view over the block cache, so a consumer
    /// reads cache health without holding the mutable
    /// [`metrics`](crate::AbstractTree::metrics) handle. Counts are cumulative
    /// since process start; derive a rate over an interval from the delta between
    /// two polls.
    #[cfg(feature = "metrics")]
    fn cache_stats(&self) -> crate::CacheStats;
}

/// Every [`AbstractTree`](crate::AbstractTree) is a [`StorageStatistics`] by
/// delegating to its own inherent stats methods, so a `Tree` / `BlobTree` can be
/// used directly as `&dyn StorageStatistics`. The logic lives once on
/// `AbstractTree`; this is a thin object-safe re-exposure for the grouped /
/// mockable surface (a test mock implements `StorageStatistics` directly without
/// being an `AbstractTree`). When both traits are in scope, disambiguate a bare
/// `tree.storage_stats()` with `StorageStatistics::storage_stats(&tree)`.
impl<T: crate::AbstractTree + ?Sized> StorageStatistics for T {
    fn storage_stats(&self) -> crate::Result<StorageStats> {
        crate::AbstractTree::storage_stats(self)
    }

    fn level_segment_stats(&self) -> crate::Result<Vec<LevelStats>> {
        crate::AbstractTree::level_segment_stats(self)
    }

    fn compaction_debt(&self, strategy: &dyn crate::compaction::CompactionStrategy) -> u64 {
        crate::AbstractTree::compaction_debt(self, strategy)
    }

    #[cfg(feature = "metrics")]
    fn cache_stats(&self) -> crate::CacheStats {
        crate::AbstractTree::cache_stats(self)
    }
}

impl StorageStats {
    /// Approximately how many more average-shaped entries fit in `budget_bytes`,
    /// using [`Self::avg_entry_on_disk_bytes`].
    ///
    /// Returns `0` when the average entry size is unknown (an empty tree), since
    /// there is no basis for the estimate.
    #[must_use]
    pub fn estimated_remaining_entries(&self, budget_bytes: u64) -> u64 {
        if self.avg_entry_on_disk_bytes == 0 {
            0
        } else {
            budget_bytes / self.avg_entry_on_disk_bytes
        }
    }
}

/// Sums the true physical on-disk size of every live table and blob file in
/// `version` (one metadata stat per file).
///
/// This is the same physical basis [`compute_storage_stats`] reports as
/// `used_bytes` and that `Tree::create_checkpoint` totals, so the storage
/// admission gate agrees with both. It deliberately does NOT use
/// `Metadata::file_size` (undercounts by the meta block / footer) or
/// `disk_space()` (metadata `Level::size`, which also omits blob files).
///
/// # Errors
///
/// Returns an error if a live table or blob file's size cannot be stat-ed.
pub(crate) fn compute_used_bytes(version: &Version) -> crate::Result<u64> {
    // Sum of on-disk file sizes, bounded by the filesystem capacity → cannot
    // overflow u64; plain arithmetic.
    let mut used_bytes = 0u64;
    for table in version.iter_tables() {
        used_bytes += table_on_disk_bytes(table)?;
    }
    for blob in version.blob_files.iter() {
        used_bytes += blob_on_disk_bytes(blob)?;
    }
    Ok(used_bytes)
}

/// The physical bytes a live blob file occupies.
///
/// Blob files are punched in place by the same tight-space reclaim as SSTs, so
/// they are charged what they still occupy, not their logical length.
///
/// This is deliberately a DIFFERENT measure from `CheckpointInfo::total_bytes`,
/// which counts logical lengths because that is what restoring a snapshot
/// costs. The two agree for intact files and differ by exactly the punched
/// holes once a reclaim has run.
///
/// # Errors
///
/// Propagates the stat failures of the blob file.
pub(crate) fn blob_on_disk_bytes(blob: &crate::vlog::BlobFile) -> crate::Result<u64> {
    Ok(crate::file::on_disk_bytes(&*blob.0.fs, &blob.0.path)?)
}

/// The physical bytes a live table occupies: the SST file plus, for a
/// tight-space-RESTRICTED table, its `.restrict-bound` sidecar — a live
/// companion file a checkpoint links and totals too, so both surfaces cover the
/// same SET of files. A restricted view whose sidecar is missing on disk (a
/// geometry-derived restriction after a repair) counts the SST alone.
///
/// The two surfaces measure that set differently on purpose: this one is
/// physical (a punched prefix must leave the quota), while
/// `CheckpointInfo::total_bytes` is logical (that is what a restore costs).
pub(crate) fn table_on_disk_bytes(table: &crate::table::Table) -> crate::Result<u64> {
    // Physical bytes: charging the logical length would keep a tight-space
    // compaction's freed prefix on the quota forever, so under
    // `storage_limit_bytes` the headroom would never recover and the tree would
    // stay read-only despite the compaction having succeeded.
    #[cfg_attr(not(feature = "std"), expect(unused_mut, reason = "no sidecar arm"))]
    let mut bytes = crate::file::on_disk_bytes(&*table.fs, &table.path)?;
    // Restrictions are created only by the std-only tight-space / repair
    // paths, so the sidecar probe is std-gated with them.
    #[cfg(feature = "std")]
    if table.restrict_lower_bound().is_some() {
        match table
            .fs
            .metadata(&crate::restrict_bound::sidecar_path(&table.path))
        {
            Ok(m) => bytes += m.len,
            Err(e) if e.kind() == crate::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(bytes)
}

/// The transient-output bound a full compaction's space check uses: the largest
/// level's live size (the `full_compaction_bytes` gauge figure), an upper bound
/// on a single merge's input set. `0` for an empty tree.
///
/// Live, not `Level::size`: a tight-space-RESTRICTED table's `file_size` still
/// describes the punched original, and charging that superseded prefix to the
/// output would report the tree as tight — and the gate would stall an ordinary
/// merge — while the real output fits.
///
/// This is the DEMAND. The destination VOLUME is a separate concern: a full
/// compaction writes its output to the last configured level
/// (`level_count - 1`), not to whichever level is currently largest, so callers
/// pass the last level as the destination to the per-volume space check (the two
/// differ only under tiered routing, where they can be different filesystems).
///
/// # Errors
///
/// Propagates a restricted table's punch-offset lookup.
pub(crate) fn full_compaction_demand_bytes(version: &Version) -> crate::Result<u64> {
    let mut largest = 0u64;
    for level in version.iter_levels() {
        // A level's live size: a sum of on-disk byte counts, bounded by the
        // filesystem capacity, so it cannot overflow u64.
        let mut size = 0u64;
        for run in level.iter() {
            for table in run.iter() {
                size += table.live_file_size()?;
            }
        }
        largest = largest.max(size);
    }
    Ok(largest)
}

/// Computes [`StorageStats`] from a live version's table + blob-file metadata.
///
/// `is_compacting` selects [`StorageStatus::CompactionInProgress`] vs
/// [`StorageStatus::Healthy`]; the caller supplies it because compaction state
/// is engine-internal.
///
/// `value_bytes_are_user_values` must be `false` for a KV-separated
/// (`BlobTree`) tree: there the SST records a small indirection pointer per
/// large value, not the user value, so the per-table value-byte sum measures
/// pointers and the value average would misreport. When `false`,
/// [`StorageStats::avg_value_bytes`] is forced to `None`. Key bytes are never
/// separated, so [`StorageStats::avg_key_bytes`] stays exact either way.
///
/// `used_bytes` is the true on-disk footprint of every live table and blob file
/// (one stat per file), not the writer's `Metadata::file_size` or
/// `crate::version::Version::blob_files`' compressed-payload sum: those
/// undercount the physical file by the meta block / footer / blob trailer. It
/// covers the same files `Tree::create_checkpoint` totals, but measures them
/// physically rather than logically, so the two differ by the holes a
/// tight-space reclaim punched and agree everywhere else.
///
/// # Errors
///
/// Returns an error if a live table or blob file's size cannot be stat-ed, or a
/// table's blob-link section cannot be read or parsed.
pub(crate) fn compute_storage_stats(
    version: &Version,
    is_compacting: bool,
    value_bytes_are_user_values: bool,
) -> crate::Result<StorageStats> {
    let mut used_bytes = 0u64;
    let mut item_count = 0u64;
    let mut table_count = 0u64;
    let mut reclaimable_entries = 0u64;
    let mut sum_key = 0u64;
    let mut sum_value = 0u64;
    // The key/value split is only exact when EVERY live table records the byte
    // sums; a single legacy table without them makes the average unrepresentable.
    let mut all_have_shape = true;

    // Every running total below is a sum of on-disk byte sizes or live item
    // counts; both are bounded by the filesystem capacity / the live entry count
    // and cannot overflow u64, so plain arithmetic is correct (a debug-overflow
    // would itself signal a corrupt metadata read).
    for table in version.iter_tables() {
        let m = &table.metadata;
        // Physical file size, NOT m.file_size (which undercounts — see above);
        // a restricted table's live sidecar counts too (same basis as the
        // checkpoint total, see `table_on_disk_bytes`).
        let on_disk = table_on_disk_bytes(table)?;
        used_bytes += on_disk;
        // A restricted view's metadata still describes the whole original SST;
        // its consumed prefix belongs to the output that superseded it, and
        // both live in this version while a slice is in flight. Count what this
        // view serves, and scale the per-entry aggregates by the same share so
        // the averages stay consistent with the count. A report, not a read:
        // polling it must not move the read counters or the block cache.
        let live_items = table.live_item_count(crate::table::util::ReadCharge::Untraced)?;
        let share = |total: u64| -> u64 {
            if live_items == m.item_count || m.item_count == 0 {
                return total;
            }
            u64::try_from(u128::from(total) * u128::from(live_items) / u128::from(m.item_count))
                .unwrap_or(total)
        };
        item_count += live_items;
        table_count += 1;
        reclaimable_entries += share(m.weak_tombstone_reclaimable);
        match (
            m.sum_user_key_bytes.map(share),
            m.sum_value_bytes.map(share),
        ) {
            (Some(k), Some(v)) => {
                sum_key += k;
                sum_value += v;
            }
            _ => all_have_shape = false,
        }
    }

    // Physical blob-file size (metadata + trailer included), NOT
    // BlobFileList::on_disk_size() which sums only the compressed payload.
    for blob in version.blob_files.iter() {
        used_bytes += blob_on_disk_bytes(blob)?;
    }

    let avg_entry_on_disk_bytes = if item_count == 0 {
        0
    } else {
        used_bytes / item_count
    };

    let have_shape = all_have_shape && item_count > 0;
    let avg_key_bytes = have_shape.then(|| sum_key / item_count);
    // Value bytes are only meaningful when not KV-separated (see param doc).
    let avg_value_bytes =
        (have_shape && value_bytes_are_user_values).then(|| sum_value / item_count);

    // reclaimable_entries ≤ item_count and avg_entry_on_disk_bytes = used / item_count,
    // so the product is ≤ used_bytes (bounded by disk capacity): plain multiply.
    let reclaimable_bytes_estimate = reclaimable_entries * avg_entry_on_disk_bytes;

    // A full compaction's transient output is bounded by its input set; the
    // largest single merge is bounded by the largest level's on-disk size, so
    // that is the free space a full compaction needs.
    let full_compaction_bytes = full_compaction_demand_bytes(version)?;
    // A minimal (tight) space-reclaiming merge needs only the reserved working
    // floor to make forward progress.
    let tight_compaction_bytes = crate::tree::MIN_RESERVED_HEADROOM;

    let status = if is_compacting {
        StorageStatus::CompactionInProgress
    } else {
        StorageStatus::Healthy
    };

    Ok(StorageStats {
        used_bytes,
        // Capacity is disk-aware (quota + free-space probe) and lives at the
        // tree layer; this version-only computation leaves it unbounded. The
        // caller (`Tree::storage_stats`) fills the real figures.
        capacity_bytes: None,
        available_bytes: None,
        compaction_possible: true,
        full_compaction_bytes,
        tight_compaction_bytes,
        item_count,
        table_count,
        avg_entry_on_disk_bytes,
        avg_key_bytes,
        avg_value_bytes,
        reclaimable_bytes_estimate,
        status,
        blob_references: blob_reference_stats(version.iter_tables())?,
    })
}

/// Computes per-LSM-level and per-segment size + entry stats from a version.
///
/// Cost is O(levels x segments) plus one file-size stat per segment (the same
/// stat [`compute_storage_stats`] already performs), and the first time per
/// segment a read of its blob-link section; it never reads a data block.
///
/// # Errors
///
/// Returns an error if a segment's file size cannot be stat-ed or its blob-link
/// section cannot be read.
pub(crate) fn compute_level_segment_stats(version: &Version) -> crate::Result<Vec<LevelStats>> {
    use core::sync::atomic::Ordering::Relaxed;
    let mut levels = Vec::with_capacity(version.level_count());
    for (level, run_group) in version.iter_levels().enumerate() {
        let mut segments = Vec::new();
        let mut used_bytes = 0u64;
        let mut item_count = 0u64;
        let mut reads = 0u64;
        let mut last_access_secs = 0u64;
        // Each table's spans are collected once: they give its own figures,
        // then join the level's, which the level sweep needs all together.
        let mut comparator = None;
        let mut level_spans = Vec::new();
        let mut table_spans = Vec::new();
        for run in run_group.iter() {
            for table in run.iter() {
                collect_spans([table], &mut comparator, &mut table_spans)?;
                let blob_references = comparator
                    .as_ref()
                    .map_or_else(BlobReferenceStats::default, |cmp| {
                        span_stats(&mut table_spans, cmp.as_ref())
                    });
                level_spans.append(&mut table_spans);
                // Physical file size, NOT m.file_size (which undercounts), to
                // reconcile with the tree-level `used_bytes` — including a
                // restricted table's live restriction sidecar (the same basis
                // as `table_on_disk_bytes`), so summing the levels matches
                // the documented SST portion of the tree total.
                let on_disk = table_on_disk_bytes(table)?;
                // What this VIEW serves, on the same basis as the tree total: a
                // restricted table's metadata still counts the prefix the
                // superseding output owns.
                let items = table.live_item_count(crate::table::util::ReadCharge::Untraced)?;
                let seg_reads = table.read_count.load(Relaxed);
                let seg_access = table.last_access_secs.load(Relaxed);
                used_bytes += on_disk;
                item_count += items;
                reads = reads.saturating_add(seg_reads);
                last_access_secs = last_access_secs.max(seg_access);
                segments.push(SegmentStats {
                    table_id: table.metadata.id,
                    level,
                    used_bytes: on_disk,
                    item_count: items,
                    reads: seg_reads,
                    last_access_secs: seg_access,
                    blob_references,
                });
            }
        }
        levels.push(LevelStats {
            level,
            segment_count: segments.len(),
            used_bytes,
            item_count,
            reads,
            last_access_secs,
            blob_references: comparator.map_or_else(BlobReferenceStats::default, |cmp| {
                span_stats(&mut level_spans, cmp.as_ref())
            }),
            segments,
        });
    }
    Ok(levels)
}

#[cfg(test)]
mod tests;

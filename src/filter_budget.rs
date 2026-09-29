// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Filter widths chosen against a tree-wide memory budget, by where the
//! workload probes for keys a table does not hold.
//!
//! A flush or compaction plans its filters once ([`plan`]): a price per
//! filter byte is set over the live tables so their filters, each at the
//! width that minimises its false-positive cost plus the price of its bytes,
//! would fill the budget. Every filter the rewrite then builds ranks its
//! widths the same way ([`FilterSizing::candidates`]) and is built at the
//! first one whose encoded bytes the budget admits while keeping room for the
//! narrowest width over the keys still to come ([`FilterSizing::admit`]), so
//! a rewrite never takes the budget past its limit when the narrowest widths
//! fit it. A compaction sets the price again before each filter, over the
//! data and the budget still left, corrected by how far its builds ran from
//! their estimates, so the filters it writes first do not take the room of
//! the ones after them. Filters written before stay as they are.

use crate::config::{BloomConstructionPolicy, FilterAdvisor};
use crate::table::Table;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::ops::Bound;
use core::sync::atomic::{AtomicBool, Ordering::Relaxed};
// no-std: spin mirrors parking_lot's Mutex API without an allocator.
// parking_lot wins on the std path, so keep it for std.
#[cfg(feature = "std")]
use parking_lot::Mutex;
// 32-bit targets without native 64-bit atomics get them from the crate.
use portable_atomic::{AtomicI64, AtomicU64};
#[cfg(not(feature = "std"))]
use spin::Mutex;

/// The filter bytes a tree's budget holds, shared by every rewrite of the
/// tree.
///
/// The budget is a target: existing filters are rewritten only with their
/// tables, so a lowered budget, or a key count at which even the narrowest
/// width does not fit, leaves the filters over it until enough tables are
/// rewritten. The tree reports that state, and says so once in the log,
/// rather than holding a bound it cannot.
///
/// The held bytes are those of the published version's filters, plus those
/// each rewrite in progress has built, less the filters it replaces. A flush
/// and a compaction running together draw on this one figure, so the room
/// one of them builds into is not handed to the other.
#[derive(Debug, Default)]
pub struct FilterBudget {
    /// The over-budget state has been logged, so it is logged once.
    logged: AtomicBool,
    /// Filter bytes the budget holds, see above. Signed: a rewrite takes the
    /// credit for what it replaces when it plans, before its own filters
    /// are counted.
    held: AtomicI64,
    /// Filter bytes of the published version, whose changes `held` follows.
    published: AtomicU64,
}

impl FilterBudget {
    /// Follows a newly published version: its filters replace those of the
    /// version before in the held bytes. The history lock orders the calls.
    pub(crate) fn publish(&self, version: &crate::version::Version) {
        let live: u64 = version
            .iter_tables()
            .map(|table| u64::from(table.filter_size()))
            .sum();
        let before = self.published.swap(live, Relaxed);
        // Filter bytes of live tables, far below 2^63.
        #[expect(
            clippy::cast_possible_wrap,
            reason = "filter byte counts far below 2^63"
        )]
        self.held.fetch_add(live as i64 - before as i64, Relaxed);
    }

    /// Filter bytes of the published version.
    #[cfg(all(test, zstd_any))]
    pub(crate) fn published(&self) -> u64 {
        self.published.load(Relaxed)
    }

    /// The filter bytes the budget holds.
    pub(crate) fn held(&self) -> u64 {
        u64::try_from(self.held.load(Relaxed)).unwrap_or(0)
    }

    /// A rewrite found the live filters `used` bytes large.
    fn observe(&self, used: u64, budget: u64) {
        if used > budget {
            self.log_entering(used, budget);
        } else if self.logged.swap(false, Relaxed) {
            log::info!("Filters are back within their budget: {used} bytes against {budget}");
        }
    }

    fn log_entering(&self, used: u64, budget: u64) {
        if !self.logged.swap(true, Relaxed) {
            log::warn!(
                "Filters exceed their budget: {used} bytes against {budget}; \
                 narrow filters are written until rewrites bring them under it"
            );
        }
    }
}

/// The width choices of one flush or compaction.
#[derive(Debug)]
pub struct FilterSizing {
    /// Admissible widths in bits per key, ascending.
    widths: Vec<u8>,
    /// What the static policy would build, used before any probe is observed.
    fallback: BloomConstructionPolicy,
    /// Width of `fallback` in bits per key, which breaks ties.
    fallback_bits: u8,
    /// Price of a filter byte in false positives; infinite when even the
    /// narrowest filters exceed the budget.
    price: f64,
    budget: u64,
    /// Filter bytes this rewrite has built, held in the tree's budget until
    /// it ends: installed, they are the published version's, otherwise gone.
    spent: AtomicU64,
    /// Filter bytes of the tables this rewrite replaces, taken off the held
    /// bytes when it plans and given back before its install (see
    /// [`Self::release_replaced`]) or when it ends.
    replaced: u64,
    /// Whether `replaced` has been given back.
    replaced_released: AtomicBool,
    /// Estimated bytes of the filters this rewrite has built, as `f64` bits:
    /// against what they took, it corrects the estimate the price is set
    /// with for the rest of the rewrite.
    spent_estimate: AtomicU64,
    /// Keys a partition of the destination level holds, when partitioned.
    partition_keys: Option<usize>,
    /// Keys this rewrite has still to write filters for, bounded from above
    /// by its inputs' entries: each filter leaves room for the narrowest
    /// width over them, so a filter chosen early cannot crowd out the later
    /// ones. Zero for a flush.
    pending_keys: AtomicU64,
    /// The most keys a filter of this rewrite has held: the later filters are
    /// taken to be that large, not the size of the last partition of a table.
    typical_keys: AtomicU64,
    /// The most keys a table of this rewrite has held, which tells how many
    /// tables, each ending in a short filter, the keys still to come make.
    table_keys: AtomicU64,
    /// The tables this rewrite replaces, whose probe counts its filters
    /// inherit by range. Empty for a flush.
    inputs: Vec<Table>,
    /// Negative probes per key of each of `inputs`, shrunk towards the mean
    /// by the noise in its count (see [`shrunk_densities`]).
    input_densities: Vec<f64>,
    /// Negative probes a key drew across the live tables, the load a flushed
    /// table is expected to take.
    prior_density: f64,
    /// Whether any probe has been observed since the tree opened.
    observed: bool,
    /// How the rewrite is split into key ranges running side by side; `None`
    /// for one range, written in key order.
    split: Option<Split>,
    /// The keys the rewrite writes, when only part of its inputs'.
    span: Option<Span>,
    /// The order of the tree's keys, when the rewrite has inputs to share out.
    key_order: Option<KeyOrder>,
    /// Per key range, where its data still to be written starts.
    cursors: Mutex<Vec<RangeCursor>>,
    state: Arc<FilterBudget>,
}

/// What a flush or compaction rewrites, for its filter plan.
#[derive(Default)]
pub struct Rewrite {
    /// The tables it replaces; none for a flush.
    pub inputs: Vec<Table>,
    /// The key ranges a compaction runs side by side, when it is split.
    pub split: Option<Split>,
    /// The keys it writes, when it rewrites only part of its inputs.
    pub span: Option<Span>,
    /// Keys it writes filters for, bounded from above, when it has no
    /// inputs to count them from: a flush's memtable entries.
    pub keys: u64,
    /// The order of the tree's keys, which reads an input's share of a key
    /// range from its key range alone where that settles it.
    pub comparator: Option<crate::comparator::SharedComparator>,
}

/// The part of its inputs a rewrite writes: its keys from `lower` through
/// `upper`, ordered by `comparator`. The inputs hold no key below `lower`
/// still to be written, so an input ending before `upper` is rewritten
/// whole and the ones reaching past it stay live.
pub struct Span {
    pub lower: Bound<crate::UserKey>,
    pub upper: Bound<crate::UserKey>,
    pub comparator: crate::comparator::SharedComparator,
}

impl Span {
    /// Whether the rewrite leaves nothing of `input` behind.
    fn covers(&self, input: &Table) -> bool {
        let max = input.metadata.key_range.max();
        match &self.upper {
            Bound::Unbounded => true,
            Bound::Excluded(upper) => {
                self.comparator.compare(max, upper) == core::cmp::Ordering::Less
            }
            Bound::Included(upper) => {
                self.comparator.compare(max, upper) != core::cmp::Ordering::Greater
            }
        }
    }

    fn bounds(&self) -> (Bound<&[u8]>, Bound<&[u8]>) {
        (
            self.lower.as_ref().map(AsRef::as_ref),
            self.upper.as_ref().map(AsRef::as_ref),
        )
    }
}

impl core::fmt::Debug for Span {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Span")
            .field("lower", &self.lower)
            .field("upper", &self.upper)
            .finish_non_exhaustive()
    }
}

/// Where a key range of a rewrite stands, by the lower bounds of its filters.
#[derive(Clone, Debug, Default)]
struct RangeCursor {
    /// The latest filter priced: its data and everything after it is still
    /// to come. `None` before the range's first filter.
    last: Option<Bound<crate::UserKey>>,
    /// Filters priced and not yet taken into the budget, whose data is still
    /// to come too.
    in_flight: Vec<Bound<crate::UserKey>>,
}

/// The order of the tree's keys, for the share of an input a key range holds.
struct KeyOrder(crate::comparator::SharedComparator);

impl core::fmt::Debug for KeyOrder {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("KeyOrder")
    }
}

impl KeyOrder {
    /// The share of `input`'s data inside `bounds` when its key range alone
    /// settles it: none when the range lies wholly outside the bounds, all of
    /// it when wholly inside. `None` when the range crosses a bound, and only
    /// the block index can tell.
    fn settled_share(&self, input: &Table, bounds: (Bound<&[u8]>, Bound<&[u8]>)) -> Option<f64> {
        use core::cmp::Ordering::{Greater, Less};

        let order = &*self.0;
        let before_lower = |key: &[u8]| match bounds.0 {
            Bound::Unbounded => false,
            Bound::Included(lower) => order.compare(key, lower) == Less,
            Bound::Excluded(lower) => order.compare(key, lower) != Greater,
        };
        let past_upper = |key: &[u8]| match bounds.1 {
            Bound::Unbounded => false,
            Bound::Included(upper) => order.compare(key, upper) == Greater,
            Bound::Excluded(upper) => order.compare(key, upper) != Less,
        };
        let range = &input.metadata.key_range;
        let (min, max) = (range.min(), range.max());
        if before_lower(max) || past_upper(min) {
            Some(0.0)
        } else if !before_lower(min) && !past_upper(max) {
            Some(1.0)
        } else {
            None
        }
    }
}

/// Key ranges of one compaction that run side by side, each writing its own
/// tables: split at `boundaries`, ascending by `comparator`.
pub struct Split {
    pub boundaries: Vec<crate::UserKey>,
    pub comparator: crate::comparator::SharedComparator,
}

impl core::fmt::Debug for Split {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Split")
            .field("boundaries", &self.boundaries)
            .finish_non_exhaustive()
    }
}

/// A live table with the filter settings it was built under, which the price
/// models its filter by.
pub struct Live<'a> {
    pub table: &'a Table,
    /// Width in bits per key of its level's static policy, which breaks ties.
    pub fallback_bits: u8,
    /// Keys a partition of its filter holds, when the filter is partitioned.
    pub partition_keys: Option<usize>,
}

/// The tables of `version` that have a filter, each with the settings of the
/// level it lies in under `config`: a flush writing full filters prices the
/// partitioned ones below it as partitions. Whether a filter is partitioned
/// is read from the table itself, which a later policy change leaves as it
/// was built.
///
/// A table without a filter takes no filter bytes and draws no filter
/// probes; counting its keys would price the others for filters that are
/// never built.
pub fn live<'a>(version: &'a crate::version::Version, config: &crate::Config) -> Vec<Live<'a>> {
    use crate::config::FilterPolicyEntry;

    // Tables of a level share its settings; the partition search runs once
    // per distinct setting, not per table.
    let mut partitions: Vec<((u8, u32), usize)> = Vec::new();
    let mut live = Vec::new();
    for (level, tables) in version.iter_levels().enumerate() {
        let level_policy = match config.filter_policy.get(level) {
            FilterPolicyEntry::Bloom(policy) if policy.is_active() => Some(policy),
            FilterPolicyEntry::Bloom(_) | FilterPolicyEntry::None => None,
        };
        let partition_bytes = config.filter_block_partition_size_policy.get(level);
        for table in tables.iter().flat_map(|run| run.iter()) {
            if table.filter_size() == 0 {
                continue;
            }
            // A level whose policy no longer builds filters: the width the
            // table's own filter has.
            let fallback = level_policy.unwrap_or_else(|| {
                let bits = u64::from(table.filter_size()) * 8 / filter_keys(table).max(1);
                BloomConstructionPolicy::BitsPerKey(f32::from(
                    u8::try_from(bits).unwrap_or(u8::MAX),
                ))
            });
            let fallback_bits = bits_of(fallback);
            let partition_keys = table.regions.filter_tli.is_some().then(|| {
                let setting = (fallback_bits, partition_bytes);
                if let Some(&(_, keys)) = partitions.iter().find(|(key, _)| *key == setting) {
                    keys
                } else {
                    let keys = partition_keys(fallback, partition_bytes);
                    partitions.push((setting, keys));
                    keys
                }
            });
            live.push(Live {
                table,
                fallback_bits,
                partition_keys,
            });
        }
    }
    live
}

/// Plans the filters of one flush or compaction.
///
/// `live` are the tables of the current version that have a filter (see
/// [`live`]), `rewrite` what the rewrite replaces and how it runs, `fallback`
/// what the static policy builds at the destination level, and
/// `partition_bytes` the filter partition size there when its filters are
/// partitioned. `None` when that policy builds no filter there: the advisor
/// sizes filters, it does not add them.
pub fn plan(
    advisor: &FilterAdvisor,
    state: &Arc<FilterBudget>,
    live: &[Live<'_>],
    rewrite: Rewrite,
    fallback: BloomConstructionPolicy,
    partition_bytes: Option<u32>,
) -> Option<Arc<FilterSizing>> {
    if !fallback.is_active() {
        return None;
    }
    let Rewrite {
        inputs,
        split,
        span,
        keys,
        comparator,
    } = rewrite;
    let ranges = split.as_ref().map_or(1, |split| split.boundaries.len() + 1);

    // The window: halve every count once the live tables hold more probes
    // than it, before reading them for this plan.
    // A tree cannot see 2^64 probes.
    let observed: u64 = live
        .iter()
        .filter_map(|live| live.table.probe_stats())
        .map(crate::table::probe_stats::ProbeStats::probes)
        .sum();
    if observed > advisor.window_probes() {
        for stats in live.iter().filter_map(|live| live.table.probe_stats()) {
            stats.decay();
        }
    }

    let raw: Vec<(f64, usize)> = live
        .iter()
        .map(|live| {
            let negatives = live
                .table
                .probe_stats()
                .map_or(0, crate::table::probe_stats::ProbeStats::negatives);
            (as_f64(negatives), key_count(live.table))
        })
        .collect();
    let densities = shrunk_densities(&raw);
    let loads: Vec<Load> = raw
        .iter()
        .zip(&densities)
        .zip(live)
        .map(|((&(_, n), &density), live)| Load {
            negatives: density * as_f64(n as u64),
            keys: n,
            fallback_bits: live.fallback_bits,
            partition_keys: live.partition_keys,
        })
        .collect();
    let total_keys: usize = raw.iter().map(|(_, n)| n).sum();
    let total_negatives: f64 = raw.iter().map(|(q, _)| q).sum();
    let prior_density = if total_keys == 0 {
        0.0
    } else {
        total_negatives / as_f64(total_keys as u64)
    };
    // An input without a filter takes the prior density.
    let input_densities: Vec<f64> = inputs
        .iter()
        .map(|input| {
            live.iter()
                .position(|live| live.table.id() == input.id())
                .and_then(|index| densities.get(index).copied())
                .unwrap_or(prior_density)
        })
        .collect();

    let widths = advisor.bits_per_key().to_vec();
    let fallback_bits = bits_of(fallback);
    let budget = advisor.budget_bytes();
    let live_bytes: u64 = live
        .iter()
        .map(|live| u64::from(live.table.filter_size()))
        .sum();
    state.observe(live_bytes, budget);
    // Only an input the rewrite leaves nothing of drops out with the install;
    // one it rewrites part of stays live, filter and all.
    let replaced: u64 = live
        .iter()
        .filter(|live| {
            inputs.iter().any(|input| {
                input.id() == live.table.id() && span.as_ref().is_none_or(|span| span.covers(input))
            })
        })
        .map(|live| u64::from(live.table.filter_size()))
        .sum();
    // The inputs' keys within the span. A share that cannot be read counts
    // the input whole: the room kept for later keys errs on the large side.
    let pending_keys: u64 = if inputs.is_empty() {
        keys
    } else {
        inputs
            .iter()
            .map(|input| {
                let share = span.as_ref().map_or(1.0, |span| {
                    crate::table::probe_stats::fraction_of(input, span.bounds()).unwrap_or(1.0)
                });
                #[expect(
                    clippy::cast_possible_truncation,
                    clippy::cast_sign_loss,
                    reason = "a share of the input's own key count"
                )]
                let covered = libm::ceil(as_f64(filter_keys(input)) * share) as u64;
                covered
            })
            .sum()
    };

    // A partitioned level builds a table's filter as partitions of about
    // this many keys, each with its own fixed overhead; the price counts
    // them the way the writer will build them.
    let partition_keys = partition_bytes.map(|bytes| partition_keys(fallback, bytes));
    let price = price(&loads, &widths, budget);
    // The replaced filters leave with the install; the room they free is this
    // rewrite's to build into, and no other rewrite's.
    #[expect(
        clippy::cast_possible_wrap,
        reason = "filter byte counts far below 2^63"
    )]
    state.held.fetch_sub(replaced as i64, Relaxed);

    Some(Arc::new(FilterSizing {
        price,
        widths,
        fallback,
        fallback_bits,
        budget,
        spent: AtomicU64::new(0),
        replaced,
        replaced_released: AtomicBool::new(false),
        spent_estimate: AtomicU64::new(0.0f64.to_bits()),
        partition_keys,
        pending_keys: AtomicU64::new(pending_keys),
        typical_keys: AtomicU64::new(0),
        table_keys: AtomicU64::new(0),
        inputs,
        input_densities,
        prior_density,
        observed: observed > 0,
        split,
        span,
        key_order: comparator.map(KeyOrder),
        cursors: Mutex::new(alloc::vec![RangeCursor::default(); ranges]),
        state: Arc::clone(state),
    }))
}

impl FilterSizing {
    /// The widest filter any choice builds: what a writer bounds its output
    /// by before the choice is made.
    pub fn bound_policy(&self) -> BloomConstructionPolicy {
        let widest = self.widths.last().copied().unwrap_or(self.fallback_bits);
        if widest > self.fallback_bits {
            BloomConstructionPolicy::BitsPerKey(f32::from(widest))
        } else {
            self.fallback
        }
    }

    /// The widths to build a filter over `n` hashes of keys in `bounds` at,
    /// most preferred first and the narrowest last. The caller builds at each
    /// width in turn until [`Self::admit`] takes the result.
    ///
    /// Before any probe is observed the static policy comes first.
    ///
    /// The filter counts as in flight from here until [`Self::admit`] takes
    /// it: its data is still to come for every filter priced meanwhile.
    pub fn candidates(
        &self,
        bounds: (Bound<&[u8]>, Bound<&[u8]>),
        n: usize,
    ) -> crate::Result<Vec<BloomConstructionPolicy>> {
        let load = self.load(bounds, n)?;
        self.enter(bounds.0);
        let price = self.current_price()?;
        let mut candidates = self.preference(load, n, price);
        let narrowest = self.narrowest();
        candidates.retain(|&policy| policy != narrowest);
        candidates.push(narrowest);
        Ok(candidates)
    }

    /// The price of a filter byte for the rest of a compaction: the price at
    /// which the inputs' data still to come, at each input's load, fills what
    /// is left of the budget.
    ///
    /// The data still to come is every key range's from its earliest filter
    /// in flight on, or from its last priced filter when none is: so the
    /// ranges of a split compaction price alike in whatever order their
    /// writers reach them, and filters built side by side (the partitions of
    /// one table on the writer's workers) price alike in whatever order they
    /// are priced. A filter priced while an earlier one is in flight counts
    /// that one's data as still to come, as it would were they built in turn.
    ///
    /// Set again for every filter, it follows what the rewrite actually
    /// spends. What is left is scaled by how far the builds so far ran from
    /// their estimates, so an estimate that runs short, as it does for the
    /// short partitions that end tables, is charged to the price rather than
    /// to the last filters, and no range of keys written early takes the room
    /// of the ones after it.
    fn current_price(&self) -> crate::Result<f64> {
        if !self.observed || self.inputs.is_empty() {
            return Ok(self.price);
        }
        let used = self.state.held();
        let spent = as_f64(self.spent.load(Relaxed));
        let estimated = f64::from_bits(self.spent_estimate.load(Relaxed));
        let error = if estimated > 0.0 {
            spent / estimated
        } else {
            1.0
        };
        // Filters already over the budget leave no room at all.
        let left = as_f64(self.budget.saturating_sub(used)) / error;

        let cursors = self.cursors();
        let mut remaining = Vec::with_capacity(self.inputs.len());
        for (input, &density) in self.inputs.iter().zip(&self.input_densities) {
            let mut share = 0.0;
            for (range, cursor) in cursors.iter().enumerate() {
                let (low, high) = self.range(range);
                let from = cursor
                    .as_ref()
                    .map_or(low, |bound| bound.as_ref().map(AsRef::as_ref));
                share += self.share_of(input, (from, high))?;
            }
            let keys = as_f64(filter_keys(input)) * share;
            if keys >= 1.0 {
                #[expect(
                    clippy::cast_possible_truncation,
                    clippy::cast_sign_loss,
                    reason = "a positive key count below the table's own"
                )]
                // Written into the destination level, at its settings.
                remaining.push(Load {
                    negatives: keys * density,
                    keys: keys as usize,
                    fallback_bits: self.fallback_bits,
                    partition_keys: self.partition_keys,
                });
            }
        }
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "a non-negative byte count below the budget"
        )]
        let left = left as u64;
        Ok(price(&remaining, &self.widths, left))
    }

    /// The share of `input`'s data inside `bounds` (see
    /// [`crate::table::probe_stats::fraction_of`]). Asked for every input
    /// before every filter, it walks the block index only for an input whose
    /// key range crosses a bound: of a compaction's many inputs, most lie
    /// wholly on one side of a filter's range.
    fn share_of(&self, input: &Table, bounds: (Bound<&[u8]>, Bound<&[u8]>)) -> crate::Result<f64> {
        if let Some(share) = self
            .key_order
            .as_ref()
            .and_then(|order| order.settled_share(input, bounds))
        {
            return Ok(share);
        }
        crate::table::probe_stats::fraction_of(input, bounds)
    }

    /// The key range of the rewrite holding a filter over keys from `lower`.
    fn range_of(&self, lower: Bound<&[u8]>) -> usize {
        match (&self.split, lower) {
            (Some(split), Bound::Included(key) | Bound::Excluded(key)) => {
                split.boundaries.partition_point(|boundary| {
                    split.comparator.compare(boundary, key) != core::cmp::Ordering::Greater
                })
            }
            _ => 0,
        }
    }

    /// Orders two lower bounds by the keys they start from.
    fn lower_order(
        &self,
        a: &Bound<crate::UserKey>,
        b: &Bound<crate::UserKey>,
    ) -> core::cmp::Ordering {
        use core::cmp::Ordering::{Equal, Greater, Less};
        let order = self.key_order.as_ref().map(|order| &*order.0);
        match (a, b) {
            (Bound::Unbounded, Bound::Unbounded) => Equal,
            (Bound::Unbounded, _) => Less,
            (_, Bound::Unbounded) => Greater,
            (
                Bound::Included(a_key) | Bound::Excluded(a_key),
                Bound::Included(b_key) | Bound::Excluded(b_key),
            ) => {
                let keys =
                    order.map_or_else(|| a_key.cmp(b_key), |order| order.compare(a_key, b_key));
                // From a key on starts before past it.
                keys.then(match (a, b) {
                    (Bound::Included(_), Bound::Excluded(_)) => Less,
                    (Bound::Excluded(_), Bound::Included(_)) => Greater,
                    _ => Equal,
                })
            }
        }
    }

    /// Records a filter over keys from `lower` as priced and in flight.
    fn enter(&self, lower: Bound<&[u8]>) {
        let range = self.range_of(lower);
        let lower = lower.map(crate::UserKey::from);
        let mut cursors = self.cursors.lock();
        if let Some(cursor) = cursors.get_mut(range) {
            // The last priced filter only moves on: one priced after a later
            // one, out of order, does not take the range back to it.
            let later = cursor
                .last
                .as_ref()
                .is_none_or(|last| self.lower_order(&lower, last) == core::cmp::Ordering::Greater);
            if later {
                cursor.last = Some(lower.clone());
            }
            cursor.in_flight.push(lower);
        }
    }

    /// Records a filter over keys from `lower` as taken into the budget.
    fn leave(&self, lower: Bound<&[u8]>) {
        let range = self.range_of(lower);
        let lower = lower.map(crate::UserKey::from);
        let mut cursors = self.cursors.lock();
        if let Some(cursor) = cursors.get_mut(range)
            && let Some(index) = cursor.in_flight.iter().position(|bound| *bound == lower)
        {
            cursor.in_flight.swap_remove(index);
        }
    }

    /// Where each key range's data still to come starts: its earliest filter
    /// in flight, or its last priced one, or `None` before its first.
    fn cursors(&self) -> Vec<Option<Bound<crate::UserKey>>> {
        let cursors = self.cursors.lock();
        cursors
            .iter()
            .map(|cursor| {
                cursor
                    .in_flight
                    .iter()
                    .min_by(|a, b| self.lower_order(a, b))
                    .or(cursor.last.as_ref())
                    .cloned()
            })
            .collect()
    }

    /// The bounds of key range `index` of the rewrite.
    fn range(&self, index: usize) -> (Bound<&[u8]>, Bound<&[u8]>) {
        let Some(split) = &self.split else {
            return self
                .span
                .as_ref()
                .map_or((Bound::Unbounded, Bound::Unbounded), Span::bounds);
        };
        let low = index
            .checked_sub(1)
            .and_then(|below| split.boundaries.get(below))
            .map_or(Bound::Unbounded, |key| Bound::Included(key.as_ref()));
        let high = split
            .boundaries
            .get(index)
            .map_or(Bound::Unbounded, |key| Bound::Excluded(key.as_ref()));
        (low, high)
    }

    /// The narrowest width a filter can be built at: the narrowest configured
    /// one once probes are observed; before, while the static policy decides,
    /// the narrowest configured one below it, or the static policy itself.
    pub fn narrowest(&self) -> BloomConstructionPolicy {
        match self.widths.first() {
            Some(&bits) if self.observed || bits < self.fallback_bits => {
                BloomConstructionPolicy::BitsPerKey(f32::from(bits))
            }
            _ => self.fallback,
        }
    }

    /// Takes a built filter of `bytes` on disk over `n` keys into the budget
    /// when they leave room for the narrowest width, bounded from above, over
    /// the keys the rewrite has still to write. Refuses it otherwise, and the
    /// caller builds at its next candidate; the `last` one, the narrowest, is
    /// always taken, even past the budget. `frame` gives the on-disk bytes of
    /// a filter payload. The bytes are held in the tree's budget, shared with
    /// the rewrites running alongside this one, until this one ends.
    ///
    /// The later keys are taken to go into filters as large as the largest
    /// this rewrite has built, each with its own fixed overhead: sized by a
    /// table's short last partition instead, the room would be counted for
    /// many small filters that are never written.
    ///
    /// `estimated` is the estimate of the same filter's bytes, which the
    /// price corrects itself by (see [`Self::current_price`]).
    ///
    /// Charging the bytes a build wrote rather than a bound on them lets the
    /// filters fill the budget.
    ///
    /// `lower` is the lower bound [`Self::candidates`] priced the filter at:
    /// once taken, the filter is no longer in flight.
    pub fn admit(
        &self,
        lower: Bound<&[u8]>,
        n: usize,
        bytes: u64,
        estimated: u64,
        frame: &dyn Fn(u64) -> u64,
        last: bool,
    ) -> bool {
        let keys = u64::try_from(n).unwrap_or(u64::MAX);
        let per_filter = self.typical_keys.load(Relaxed).max(keys);
        let mean = |keys: u64| {
            #[expect(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "a non-negative byte count of one filter"
            )]
            let bytes = libm::round(
                self.narrowest()
                    .expected_filter_size(usize::try_from(keys).unwrap_or(usize::MAX)),
            ) as u64;
            frame(bytes)
        };
        // The later filters at the narrowest width, at their mean size, and
        // how far one can come out above it: a bound on each of them would
        // add up to far more than the filters take together, their later
        // layers at the floors few builds reach.
        let narrowest_bytes = mean(per_filter);
        let spread = frame(
            self.narrowest()
                .filter_size_bound(usize::try_from(per_filter).unwrap_or(usize::MAX))
                as u64,
        )
        .saturating_sub(narrowest_bytes);
        // On a partitioned level each table still to come ends in a short
        // partition, whose fixed overhead the per-key share of a full one
        // leaves out; elsewhere a table's one filter is the full one.
        let tail_overhead = if self.partition_keys.is_some() {
            mean(1)
        } else {
            0
        };
        let n = keys;
        let mut held = self.state.held.load(Relaxed);
        loop {
            let used = u64::try_from(held).unwrap_or(0);
            // The pending count bounds the rewrite's keys from above: a
            // filter over more keys than it holds leaves none pending.
            let later = self.pending_keys.load(Relaxed).saturating_sub(n);
            // Charged per key rather than per whole filter: the pending count
            // runs a little over the keys (versions of one key, overwritten
            // keys), and rounding that excess up to a filter would refuse the
            // last ones room they have.
            let floor = if later == 0 || per_filter == 0 {
                0
            } else {
                // `later` and the mean are both far below 2^32 in practice;
                // the product in u128 cannot overflow either way.
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "later * bytes / per_filter <= later * bytes, far below 2^64"
                )]
                let per_key = (u128::from(later) * u128::from(narrowest_bytes))
                    .div_ceil(u128::from(per_filter)) as u64;
                let table_keys = self.table_keys.load(Relaxed);
                let tables = if table_keys == 0 {
                    1
                } else {
                    later.div_ceil(table_keys)
                };
                // Each filter's later layers come out by chance, one filter's
                // independently of another's, so their sum strays from the
                // mean by about the square root of the filters' count times
                // one filter's spread.
                #[expect(
                    clippy::cast_possible_truncation,
                    clippy::cast_sign_loss,
                    reason = "the square root of a filter count, non-negative and small"
                )]
                let strays = libm::ceil(libm::sqrt(as_f64(later.div_ceil(per_filter)))) as u64;
                // The last keys may go into one short filter of their own,
                // bounded from above: it is the one no later filter makes up
                // for.
                let final_filter = frame(
                    self.narrowest()
                        .filter_size_bound(usize::try_from(later).unwrap_or(usize::MAX))
                        as u64,
                );
                (per_key + tables * tail_overhead + strays * spread).max(final_filter)
            };
            // Filters already over the budget leave no room at all.
            let fits = bytes + floor <= self.budget.saturating_sub(used);
            if !fits && !last {
                return false;
            }
            // A table cannot hold 2^63 filter bytes, so neither can the sum.
            #[expect(
                clippy::cast_possible_wrap,
                reason = "filter byte counts far below 2^63"
            )]
            let taken = held + bytes as i64;
            match self
                .state
                .held
                .compare_exchange_weak(held, taken, Relaxed, Relaxed)
            {
                Ok(_) => {
                    self.leave(lower);
                    self.spent.fetch_add(bytes, Relaxed);
                    // The room kept for later filters is a reserve, not a
                    // limit. Whether the tree is over its budget follows the
                    // filters its versions publish, not the ones a rewrite is
                    // building, which it may never install.
                    if used + bytes > self.budget {
                        self.state.log_entering(used + bytes, self.budget);
                    }
                    self.take_pending(n);
                    self.typical_keys.fetch_max(n, Relaxed);
                    let mut spent = self.spent_estimate.load(Relaxed);
                    while let Err(actual) = self.spent_estimate.compare_exchange_weak(
                        spent,
                        (f64::from_bits(spent) + as_f64(estimated)).to_bits(),
                        Relaxed,
                        Relaxed,
                    ) {
                        spent = actual;
                    }
                    return true;
                }
                Err(actual) => held = actual,
            }
        }
    }

    /// Gives back the credit for the filters this rewrite replaces, before
    /// its install drops them from the published version. Held bytes then
    /// count both the replaced filters and the new ones until the install
    /// and the end of the rewrite settle them, never fewer than there are.
    pub fn release_replaced(&self) {
        if !self.replaced_released.swap(true, Relaxed) {
            #[expect(
                clippy::cast_possible_wrap,
                reason = "filter byte counts far below 2^63"
            )]
            self.state.held.fetch_add(self.replaced as i64, Relaxed);
        }
    }

    /// Records that a table's filters, over `keys` hashes in all, are built.
    pub fn table_finished(&self, keys: usize) {
        self.table_keys
            .fetch_max(u64::try_from(keys).unwrap_or(u64::MAX), Relaxed);
    }

    /// Counts `n` keys as sized.
    fn take_pending(&self, n: u64) {
        let mut pending = self.pending_keys.load(Relaxed);
        // The pending count bounds the keys from above: a filter over more
        // keys than it holds leaves none pending, not a debt.
        while let Err(actual) = self.pending_keys.compare_exchange_weak(
            pending,
            pending.saturating_sub(n),
            Relaxed,
            Relaxed,
        ) {
            pending = actual;
        }
    }

    /// The negative probes a filter over `n` keys in `bounds` is expected to
    /// draw: `n` times the density of the inputs it covers, weighted by the
    /// keys each has in `bounds`, or for a flush the probes a key draws
    /// across the live tables.
    ///
    /// The share of an input in a range is estimated by whole blocks, so it
    /// weights the densities but does not count the keys: filters over the
    /// same density get the same load per key, and do not split between two
    /// widths on the error of that estimate.
    fn load(&self, bounds: (Bound<&[u8]>, Bound<&[u8]>), n: usize) -> crate::Result<f64> {
        let (mut keys, mut probes) = (0.0, 0.0);
        for (input, density) in self.inputs.iter().zip(&self.input_densities) {
            let covered = as_f64(filter_keys(input)) * self.share_of(input, bounds)?;
            keys += covered;
            probes += covered * density;
        }
        let density = if keys > 0.0 {
            probes / keys
        } else {
            self.prior_density
        };
        Ok(density * as_f64(n as u64))
    }

    /// The candidate policies for a filter drawing `load` negative probes
    /// over `n` keys at `price`, most preferred first.
    fn preference(&self, load: f64, n: usize, price: f64) -> Vec<BloomConstructionPolicy> {
        // Before any probe the static policy decides, stepping down through
        // the narrower widths only when it does not fit.
        if !self.observed {
            let mut preference = alloc::vec![self.fallback];
            preference.extend(
                self.widths
                    .iter()
                    .rev()
                    .filter(|&&bits| bits < self.fallback_bits)
                    .map(|&bits| BloomConstructionPolicy::BitsPerKey(f32::from(bits))),
            );
            return preference;
        }

        let bytes = sizes(n, &self.widths);
        let rates = rates(&self.widths);
        let mut order: Vec<usize> = (0..self.widths.len()).collect();
        order.sort_by(|&a, &b| {
            rank(
                load,
                &bytes,
                &rates,
                &self.widths,
                (a, b),
                price,
                self.fallback_bits,
            )
        });
        order
            .into_iter()
            .filter_map(|index| self.widths.get(index))
            .map(|&bits| BloomConstructionPolicy::BitsPerKey(f32::from(bits)))
            .collect()
    }
}

/// The rewrite has ended. Its own filters leave the held bytes: installed,
/// the published version counts them; not installed, they are gone. The
/// credit for what it replaces goes back the same way, if its install did not
/// already take it.
impl Drop for FilterSizing {
    fn drop(&mut self) {
        self.release_replaced();
        #[expect(
            clippy::cast_possible_wrap,
            reason = "filter byte counts far below 2^63"
        )]
        self.state
            .held
            .fetch_sub(self.spent.load(Relaxed) as i64, Relaxed);
    }
}

/// A table's filter as the price models it: the negative probes it draws
/// over `keys` keys, and the settings it is built under.
#[derive(Clone, Copy, Debug)]
struct Load {
    negatives: f64,
    keys: usize,
    /// Width of the static policy it is built under, which breaks ties.
    fallback_bits: u8,
    /// Keys a partition of it holds, when it is partitioned.
    partition_keys: Option<usize>,
}

/// The price of a filter byte at which the filters of `loads`, each at its
/// cheapest width, fill `budget`: zero when the widest choices fit, infinite
/// when even the narrowest do not.
fn price(loads: &[Load], widths: &[u8], budget: u64) -> f64 {
    // Each table's filter is decided the way its writer decides it: per
    // partition when partitioned, at the table's load density. Sizes and
    // rates do not depend on the price, so the search reads them from here.
    struct Entry {
        filter_load: f64,
        fallback_bits: u8,
        /// Index into the full partitions' sizes when the width is decided
        /// per partition; otherwise the table's one filter decides it.
        partition: Option<usize>,
        table_bytes: Vec<u64>,
    }
    /// Bytes at each width of the filter an entry's width is decided for.
    fn filter_bytes<'a>(entry: &'a Entry, partitions: &'a [(usize, Vec<u64>)]) -> &'a [u64] {
        entry
            .partition
            .and_then(|index| partitions.get(index))
            .map_or(&entry.table_bytes[..], |(_, full)| &full[..])
    }
    let rates = rates(widths);
    // A full partition's sizes depend on its key count alone, which the
    // tables of one level share: worked out once per count, not per table.
    let mut partitions: Vec<(usize, Vec<u64>)> = Vec::new();
    let entries: Vec<Entry> = loads
        .iter()
        .map(|load| {
            let n = load.keys;
            let partition = match load.partition_keys {
                Some(keys) if keys > 0 => keys,
                _ => usize::MAX,
            };
            let (partition_index, table_bytes) = if n > partition {
                let index = if let Some(index) =
                    partitions.iter().position(|(keys, _)| *keys == partition)
                {
                    index
                } else {
                    partitions.push((partition, sizes(partition, widths)));
                    partitions.len() - 1
                };
                let count = (n / partition) as u64;
                let full = partitions.get(index).map_or(&[][..], |(_, full)| full);
                let table = full
                    .iter()
                    .zip(sizes(n % partition, widths))
                    .map(|(&full, rest)| count * full + rest)
                    .collect();
                (Some(index), table)
            } else {
                (None, sizes(n, widths))
            };
            let filter = partition.min(n).max(1);
            Entry {
                filter_load: load.negatives * as_f64(filter as u64) / as_f64(n.max(1) as u64),
                fallback_bits: load.fallback_bits,
                partition: partition_index,
                table_bytes,
            }
        })
        .collect();
    // Filter sizes of real tables sum far below 2^64 bytes.
    let filled = |price: f64| -> u64 {
        entries
            .iter()
            .map(|entry| {
                let index = choose(
                    entry.filter_load,
                    filter_bytes(entry, &partitions),
                    &rates,
                    widths,
                    entry.fallback_bits,
                    price,
                );
                entry.table_bytes.get(index).copied().unwrap_or(0)
            })
            .sum()
    };
    if filled(0.0) <= budget {
        return 0.0;
    }

    // As the price falls, each table moves along the lower convex hull of
    // its widths' (bytes, false positives): from the fewest bytes, one step
    // wider at each price where the next hull width's saved false positives
    // pay for its extra bytes. Walking every table's steps from the highest
    // price down finds the lowest price whose widths fit, and the step above
    // it, with no search.
    let mut total: u64 = 0;
    let mut steps: Vec<(f64, i128)> = Vec::new();
    let mut hull: Vec<usize> = Vec::with_capacity(widths.len());
    for entry in &entries {
        let filter_bytes = filter_bytes(entry, &partitions);
        let point = |index: usize| {
            (
                filter_bytes
                    .get(index)
                    .copied()
                    .map_or(f64::INFINITY, as_f64),
                entry.filter_load * rates.get(index).copied().unwrap_or(0.0),
            )
        };
        let mut order: Vec<usize> = (0..widths.len()).collect();
        order.sort_by(|&a, &b| {
            let ((bytes_a, fp_a), (bytes_b, fp_b)) = (point(a), point(b));
            bytes_a.total_cmp(&bytes_b).then(fp_a.total_cmp(&fp_b))
        });
        hull.clear();
        for index in order {
            let (bytes, fp) = point(index);
            if let Some(&last) = hull.last() {
                // More bytes for no fewer false positives is never chosen.
                if fp >= point(last).1 || bytes <= point(last).0 {
                    continue;
                }
            }
            // Keep the hull convex: a point whose step price is not above
            // the next one's is passed over as the price falls.
            while let [.., before, last] = hull[..] {
                let ((bytes_a, fp_a), (bytes_b, fp_b)) = (point(before), point(last));
                if (fp_b - fp) * (bytes_b - bytes_a) >= (fp_a - fp_b) * (bytes - bytes_b) {
                    hull.pop();
                } else {
                    break;
                }
            }
            hull.push(index);
        }
        let table = |index: usize| entry.table_bytes.get(index).copied().unwrap_or(0);
        let Some(&first) = hull.first() else {
            continue;
        };
        total += table(first);
        let table = |index: usize| i128::from(table(index));
        for pair in hull.windows(2) {
            if let [a, b] = *pair {
                let ((bytes_a, fp_a), (bytes_b, fp_b)) = (point(a), point(b));
                steps.push(((fp_a - fp_b) / (bytes_b - bytes_a), table(b) - table(a)));
            }
        }
    }
    if total > budget {
        return f64::INFINITY;
    }

    // Highest price first; the steps one price takes are taken together.
    steps.sort_by(|a, b| b.0.total_cmp(&a.0));
    let mut filled = i128::from(total);
    let mut above = f64::INFINITY;
    let mut index = 0;
    while let Some(&(price, _)) = steps.get(index) {
        let group = steps
            .get(index..)
            .unwrap_or_default()
            .iter()
            .take_while(|step| step.0.total_cmp(&price).is_eq());
        let (count, grown) = group.fold((0, 0i128), |(count, sum), step| (count + 1, sum + step.1));
        if filled + grown > i128::from(budget) {
            // Every price between this step and the one above leaves the
            // same widths; take the middle of that range, on the exponent a
            // price spans orders of magnitude on.
            let high = if above.is_finite() {
                libm::log2(above)
            } else {
                libm::log2(price) + 1.0
            };
            return exp2(f64::midpoint(libm::log2(price), high));
        }
        filled += grown;
        above = price;
        index += count;
    }
    // Every table at its widest step fits, which a price below the lowest
    // step picks; without a step, every positive price picks the same.
    if above.is_finite() {
        exp2(libm::log2(above) - 1.0)
    } else {
        1.0
    }
}

/// The width minimising `cost` for a filter over `n` keys drawing `load`
/// negative probes, ties going to the width nearest the static policy's.
#[cfg(test)]
fn cheapest(load: f64, n: usize, widths: &[u8], fallback_bits: u8, price: f64) -> u8 {
    let bytes = sizes(n, widths);
    widths
        .get(choose(
            load,
            &bytes,
            &rates(widths),
            widths,
            fallback_bits,
            price,
        ))
        .copied()
        .unwrap_or(fallback_bits)
}

/// Serialised bytes of a filter over `n` keys at each of `widths`, from one
/// layer shape for all of them, as the builds come out on average, so the
/// price aims the filters at the budget itself. The room each filter keeps
/// for the last one stays bounded from above.
fn sizes(n: usize, widths: &[u8]) -> Vec<u64> {
    let shape = crate::table::filter::ExpectedSize::of(n);
    widths
        .iter()
        .map(|&bits| {
            #[expect(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "a non-negative byte count of one filter"
            )]
            let bytes = libm::round(shape.at(bits)) as u64;
            bytes
        })
        .collect()
}

/// The false-positive rate at each of `widths`: `2^-r` at `r` bits per key
/// for a `BuRR` filter.
fn rates(widths: &[u8]) -> Vec<f64> {
    widths.iter().map(|&bits| exp2(-f64::from(bits))).collect()
}

/// The index of the width minimising `cost` for a filter drawing `load`
/// negative probes, whose bytes and false-positive rates at each of `widths`
/// are `bytes` and `rates`.
fn choose(
    load: f64,
    bytes: &[u64],
    rates: &[f64],
    widths: &[u8],
    fallback_bits: u8,
    price: f64,
) -> usize {
    let count = widths.len().min(bytes.len()).min(rates.len());
    (0..count)
        .min_by(|&a, &b| rank(load, bytes, rates, widths, (a, b), price, fallback_bits))
        .unwrap_or(0)
}

/// Orders the widths at the indices `pair` by [`cost`], ties going to the
/// width nearest the static policy's. At an infinite price only the bytes
/// count.
#[expect(
    clippy::indexing_slicing,
    reason = "the indices address all three slices, which the callers size alike"
)]
fn rank(
    load: f64,
    bytes: &[u64],
    rates: &[f64],
    widths: &[u8],
    (a, b): (usize, usize),
    price: f64,
    fallback_bits: u8,
) -> core::cmp::Ordering {
    let by_cost = if price.is_infinite() {
        bytes[a].cmp(&bytes[b])
    } else {
        cost(load, rates[a], bytes[a], price).total_cmp(&cost(load, rates[b], bytes[b], price))
    };
    by_cost.then(
        widths[a]
            .abs_diff(fallback_bits)
            .cmp(&widths[b].abs_diff(fallback_bits)),
    )
}

/// The expected false positives of a filter of `bytes` at false-positive
/// rate `rate` drawing `load` negative probes, plus the price of its bytes.
fn cost(load: f64, rate: f64, bytes: u64, price: f64) -> f64 {
    #[expect(
        clippy::suboptimal_flops,
        reason = "the cost only orders widths, which a fused multiply-add's extra \
                  precision does not change, and a fused one is a software routine \
                  (`libm::fma` without std, `mul_add` on targets built without the FMA \
                  feature) on the price search's inner loop"
    )]
    let cost = load * rate + price * as_f64(bytes);
    cost
}

/// Negative probes per key of each table, shrunk towards their mean.
///
/// A table's count is a sample: over `n` keys at the mean density `d`, a
/// count varies by about `d / n` per key from chance alone. The spread the
/// counts actually show is part chance and part real difference, and each
/// density keeps only the real part: it moves from the mean by the fraction
/// `1 - noise / observed` of its distance, none when the counts differ no
/// more than chance would make them (the empirical-Bayes estimate). With an
/// even load every table gets the mean, so no filter is widened on a chance
/// excess and paid for by narrowing another, which costs more false
/// positives than the widening saves.
fn shrunk_densities(counts: &[(f64, usize)]) -> Vec<f64> {
    let keys: f64 = counts.iter().map(|&(_, n)| as_f64(n as u64)).sum();
    if keys <= 0.0 {
        return counts.iter().map(|_| 0.0).collect();
    }
    let mean = counts.iter().map(|&(q, _)| q).sum::<f64>() / keys;
    let density = |&(q, n): &(f64, usize)| if n == 0 { mean } else { q / as_f64(n as u64) };
    // Key-weighted spread of the densities, and what chance alone gives it:
    // each table contributes `n * (d / n)`, so the weighted mean is
    // `tables * d / keys`.
    let observed = counts
        .iter()
        .map(|entry| {
            let deviation = density(entry) - mean;
            as_f64(entry.1 as u64) * deviation * deviation
        })
        .sum::<f64>()
        / keys;
    let noise = as_f64(counts.len() as u64) * mean / keys;
    let keep = if observed > noise {
        1.0 - noise / observed
    } else {
        0.0
    };
    counts
        .iter()
        .map(|entry| mean + keep * (density(entry) - mean))
        .collect()
}

/// Keys a filter partition holds when the writer splits partitions at
/// `partition_bytes` estimated bytes under `policy`.
fn partition_keys(policy: BloomConstructionPolicy, partition_bytes: u32) -> usize {
    let target = partition_bytes as usize;
    // The estimate grows with the key count; find the count it reaches the
    // partition size at.
    let (mut low, mut high) = (1usize, 1usize);
    while policy.estimated_filter_size(high) < target && high < 1 << 30 {
        low = high;
        high *= 2;
    }
    while low + 1 < high {
        let mid = low + (high - low) / 2;
        if policy.estimated_filter_size(mid) < target {
            low = mid;
        } else {
            high = mid;
        }
    }
    high
}

/// [`sizes`] at one width.
#[cfg(test)]
fn estimate(n: usize, bits: u8) -> u64 {
    sizes(n, &[bits]).first().copied().unwrap_or(0)
}

/// The width in bits per key a policy builds at: its own for bits per key,
/// the narrowest reaching the rate for a false-positive rate.
fn bits_of(policy: BloomConstructionPolicy) -> u8 {
    match policy {
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "an active policy's bits per key lie in 1..=64"
        )]
        BloomConstructionPolicy::BitsPerKey(bits) => crate::f32_ceil(bits) as u8,
        BloomConstructionPolicy::FalsePositiveRate(rate) => (1..=64u8)
            .find(|&bits| exp2(-f64::from(bits)) <= f64::from(rate))
            .unwrap_or(64),
    }
}

/// Hashes a table's filter holds: one per distinct key, however many versions
/// of it the table keeps, and under a prefix extractor one per distinct
/// prefix, as its metadata records them. Without that record, the distinct
/// keys, or the entries, which bound them from above.
fn filter_keys(table: &Table) -> u64 {
    table
        .metadata
        .filter_hashes
        .or(table.metadata.key_count)
        .unwrap_or(table.metadata.item_count)
}

/// [`filter_keys`] as a count of hashes.
fn key_count(table: &Table) -> usize {
    usize::try_from(filter_keys(table)).unwrap_or(usize::MAX)
}

/// `2^x` for the whole and fractional exponents the price search uses.
fn exp2(x: f64) -> f64 {
    libm::exp2(x)
}

#[expect(
    clippy::cast_precision_loss,
    reason = "counts past 2^53 lose only digits the cost comparison does not need"
)]
fn as_f64(count: u64) -> f64 {
    count as f64
}

#[cfg(test)]
mod tests;

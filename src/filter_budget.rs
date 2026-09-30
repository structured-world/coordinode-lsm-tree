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
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};
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
    /// Held while a plan checks the probe window and halves the counts, so
    /// rewrites planning together halve one crossing of it once.
    window: Mutex<()>,
    /// The room each rewrite in progress keeps for its later filters at the
    /// narrowest width, summed: a filter one rewrite admits leaves room for
    /// the others' too.
    reserved: AtomicU64,
    /// Held while a filter is checked against the budget and taken into it,
    /// and while a rewrite's reservation changes, so two rewrites cannot both
    /// fit into one gap.
    admission: Mutex<()>,
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

    /// Halves every count of the `live` tables once they hold more probes than
    /// `window`, and returns the probes they held before. Without the lock two
    /// plans could both read the total past the window and quarter the counts
    /// where one after the other halves them once.
    fn slide_window(&self, live: &[Live<'_>], window: u64) -> u64 {
        let _window = self.window.lock();
        // A tree cannot see 2^64 probes.
        let observed: u64 = live
            .iter()
            .filter_map(|live| live.table.probe_stats())
            .map(crate::table::probe_stats::ProbeStats::probes)
            .sum();
        if observed > window {
            for stats in live.iter().filter_map(|live| live.table.probe_stats()) {
                stats.decay();
            }
        }
        observed
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
    /// Filter bytes of the tables this rewrite replaces: room its install
    /// frees, which it builds into and no other rewrite does, until it gives
    /// the credit up before its install (see [`Self::release_replaced`]) or
    /// ends. The held bytes keep counting them meanwhile, since the rewrite
    /// may still fail and leave them live.
    replaced: u64,
    /// Whether the credit for `replaced` has been given up.
    replaced_released: AtomicBool,
    /// Estimated bytes of the filters this rewrite has built, as `f64` bits:
    /// against what they took, it corrects the estimate the price is set
    /// with for the rest of the rewrite.
    spent_estimate: AtomicU64,
    /// Keys a partition of the destination level holds, when partitioned.
    partition_keys: Option<usize>,
    /// Keys this rewrite writes filters for, bounded from above: each filter
    /// leaves room for the narrowest width over the ones not yet covered, so
    /// a filter chosen early cannot crowd out the later ones.
    total_keys: AtomicU64,
    /// Keys the filters this rewrite has built cover.
    admitted_keys: AtomicU64,
    /// The keys `total_keys` estimates the built filters to cover: the
    /// inputs' keys inside their ranges, by the same measure. Against
    /// `admitted_keys` it tells how far the estimate runs from the keys the
    /// rewrite writes, where inputs overlap (a key in several counts once)
    /// or a share of data bytes is not one of keys.
    estimated_admitted: AtomicU64,
    /// Hashes a key still to come holds, as a fraction: under a prefix
    /// extractor a full filter holds a hash per prefix besides one per key.
    /// The plan's estimate until a filter is built, then the most any filter
    /// of this rewrite has held, since where the rewrite writes, not where
    /// its inputs lie, decides whether prefixes are hashed.
    hashes_per_key: (AtomicU64, AtomicU64),
    /// The room the filters this rewrite has still to write take at the
    /// narrowest width.
    floor: AtomicU64,
    /// This rewrite's part of [`FilterBudget::reserved`]: its floor, less
    /// the filters it replaces until it gives their credit up (see
    /// [`Self::published_floor`]).
    reservation: AtomicU64,
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
    /// Keys each of `inputs` serves (see [`served_keys`]), which its shares of
    /// a key range are shares of.
    input_keys: Vec<u64>,
    /// Distinct keys each of `inputs` serves (see [`served_distinct_keys`]).
    input_distinct: Vec<u64>,
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
    /// How the rewrite frames its filter blocks on disk.
    framing: Framing,
    /// Per key range, where its data still to be written starts.
    cursors: Mutex<Vec<RangeCursor>>,
    /// Live [`FilterPlan`] handles: the writers and installs of the rewrite.
    owners: AtomicUsize,
    /// Set, under the admission lock, when the last [`FilterPlan`] goes: the
    /// rewrite has settled with the budget and takes no filter into it.
    ended: AtomicBool,
    state: Arc<FilterBudget>,
}

/// The filter plan of one flush or compaction, held by what writes and
/// installs its output. The last handle to go ends the rewrite with the
/// budget: its reservation, its own filters and the credit for the ones it
/// replaces all settle then.
///
/// The rewrite ends with its owners, not with the last reference to its
/// [`FilterSizing`]: a pool thread that built a filter partition may let go
/// of its reference only after the rewrite has returned, and the budget
/// would hold the rewrite's filters twice until then.
#[derive(Debug)]
pub struct FilterPlan(Arc<FilterSizing>);

impl FilterPlan {
    fn new(sizing: Arc<FilterSizing>) -> Self {
        sizing.owners.fetch_add(1, Relaxed);
        Self(sizing)
    }

    /// The sizing alone, for work that outlives none of the plan's owners
    /// yet may let go of it after them: a partition built on a pool thread.
    pub fn sizing(&self) -> Arc<FilterSizing> {
        Arc::clone(&self.0)
    }
}

impl Clone for FilterPlan {
    fn clone(&self) -> Self {
        Self::new(Arc::clone(&self.0))
    }
}

impl core::ops::Deref for FilterPlan {
    type Target = FilterSizing;

    fn deref(&self) -> &FilterSizing {
        &self.0
    }
}

impl Drop for FilterPlan {
    fn drop(&mut self) {
        // The count only falls from here, so exactly one handle sees it reach
        // zero; `end` orders itself against the admissions by its lock.
        if self.0.owners.fetch_sub(1, Relaxed) == 1 {
            self.0.end();
        }
    }
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
    /// Keys it writes filters for and the hashes they hold, bounded from
    /// above, when it has no inputs to count them from: a flush's memtables.
    pub count: FilterCount,
    /// The order of the tree's keys, which reads an input's share of a key
    /// range from its key range alone where that settles it.
    pub comparator: Option<crate::comparator::SharedComparator>,
    /// How its filter blocks are framed on disk.
    pub framing: Framing,
}

/// Keys a filter covers, and the hashes it holds over them: more than the
/// keys under a prefix extractor, which a full filter hashes each distinct
/// prefix for.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FilterCount {
    pub keys: u64,
    pub hashes: u64,
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

/// How a rewrite frames each filter block on disk: the budget counts the
/// framed bytes, so the price does too.
#[derive(Clone, Default)]
pub struct Framing {
    pub encryption: Option<Arc<dyn crate::encryption::EncryptionProvider>>,
    pub ecc: Option<crate::table::block::EccParams>,
}

impl Framing {
    /// On-disk bytes of a filter block of `len` payload bytes; none for an
    /// empty filter, which is not written.
    fn frame(&self, len: u64) -> u64 {
        if len == 0 {
            return 0;
        }
        crate::table::block::framed_len_bound(
            len,
            crate::table::block::BlockType::Filter,
            crate::CompressionType::None,
            self.encryption.as_deref(),
            self.ecc,
        )
    }
}

impl core::fmt::Debug for Framing {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Framing")
            .field("encrypted", &self.encryption.is_some())
            .field("ecc", &self.ecc)
            .finish()
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
    /// Width in bits per key of the static policy it was built under, which
    /// breaks ties.
    pub fallback_bits: u8,
    /// Keys a partition of its filter holds, when the filter is partitioned.
    pub partition_keys: Option<usize>,
}

/// The tables of `version` that have a filter, each with the settings it was
/// built under, as the table records them: a flush writing full filters
/// prices the partitioned ones below it as partitions.
///
/// The settings of the level a table lies in do not tell them. A compaction
/// builds by the level it is written for, which differs from the level it
/// lands in while the levels above the last are empty, and a table keeps its
/// filter when it is moved down whole or the policy changes.
///
/// A table without a filter takes no filter bytes and draws no filter
/// probes; counting its keys would price the others for filters that are
/// never built.
pub fn live(version: &crate::version::Version) -> Vec<Live<'_>> {
    version
        .iter_tables()
        .filter(|table| table.filter_size() > 0)
        .map(|table| {
            // A table that does not record its static width: the width its
            // own filter has.
            let fallback_bits = table.metadata.filter_bits.unwrap_or_else(|| {
                let bits = u64::from(table.filter_size()) * 8 / filter_keys(table).max(1);
                u8::try_from(bits).unwrap_or(u8::MAX)
            });
            // A partitioned filter that does not record its partitions is
            // taken as one.
            let partition_keys = table.regions.filter_tli.is_some().then(|| {
                let keys = table
                    .metadata
                    .filter_partition_hashes
                    .unwrap_or_else(|| filter_keys(table));
                usize::try_from(keys).unwrap_or(usize::MAX)
            });
            Live {
                table,
                fallback_bits,
                partition_keys,
            }
        })
        .collect()
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
) -> Option<FilterPlan> {
    if !fallback.is_active() {
        return None;
    }
    let Rewrite {
        inputs,
        split,
        span,
        count,
        comparator,
        framing,
    } = rewrite;
    let ranges = split.as_ref().map_or(1, |split| split.boundaries.len() + 1);

    // The window: halve every count once the live tables hold more probes
    // than it, before reading them for this plan.
    let observed = state.slide_window(live, advisor.window_probes());

    // Densities are over the keys a table serves; its filter, and so its
    // bytes, over all it holds.
    let raw: Vec<(f64, usize)> = live
        .iter()
        .map(|live| {
            let negatives = live
                .table
                .probe_stats()
                .map_or(0, crate::table::probe_stats::ProbeStats::negatives);
            let served = usize::try_from(served_keys(live.table)).unwrap_or(usize::MAX);
            (as_f64(negatives), served)
        })
        .collect();
    let densities = shrunk_densities(&raw);
    let loads: Vec<Load> = raw
        .iter()
        .zip(&densities)
        .zip(live)
        .map(|((&(_, served), &density), live)| Load {
            negatives: density * as_f64(served as u64),
            keys: key_count(live.table),
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
    let input_keys: Vec<u64> = inputs.iter().map(served_keys).collect();

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
    // The inputs' keys within the span, and their hashes: an estimate, which
    // the filters the rewrite builds correct as it goes (see `pending_at`).
    // A share that cannot be read counts the input whole.
    let input_distinct: Vec<u64> = inputs.iter().map(served_distinct_keys).collect();
    let pending = if inputs.is_empty() {
        count
    } else {
        let mut pending = FilterCount::default();
        for ((input, &hashes), &distinct) in inputs.iter().zip(&input_keys).zip(&input_distinct) {
            let share = span.as_ref().map_or(1.0, |span| {
                crate::table::probe_stats::fraction_of(input, span.bounds()).unwrap_or(1.0)
            });
            #[expect(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "a share of the input's own key count"
            )]
            let covered = |count: u64| libm::ceil(as_f64(count) * share) as u64;
            // An input's counts are far below 2^63, and so are their sums.
            pending.keys += covered(distinct);
            pending.hashes += covered(hashes);
        }
        pending
    };

    // A partitioned level builds a table's filter as partitions of about
    // this many keys, each with its own fixed overhead; the price counts
    // them the way the writer will build them.
    let partition_keys = partition_bytes.map(|bytes| partition_keys(fallback, bytes));
    let price = price(&loads, &widths, budget, &|len| framing.frame(len));

    let sizing = FilterPlan::new(Arc::new(FilterSizing {
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
        total_keys: AtomicU64::new(pending.keys),
        admitted_keys: AtomicU64::new(0),
        estimated_admitted: AtomicU64::new(0),
        // Until a filter is built, the hashes a key holds in what it replaces;
        // one a key without a count.
        hashes_per_key: if pending.keys == 0 {
            (AtomicU64::new(1), AtomicU64::new(1))
        } else {
            (
                AtomicU64::new(pending.hashes.max(pending.keys)),
                AtomicU64::new(pending.keys),
            )
        },
        floor: AtomicU64::new(0),
        reservation: AtomicU64::new(0),
        typical_keys: AtomicU64::new(0),
        table_keys: AtomicU64::new(0),
        inputs,
        input_densities,
        input_keys,
        input_distinct,
        prior_density,
        observed: observed > 0,
        split,
        span,
        key_order: comparator.map(KeyOrder),
        framing,
        cursors: Mutex::new(alloc::vec![RangeCursor::default(); ranges]),
        owners: AtomicUsize::new(0),
        ended: AtomicBool::new(false),
        state: Arc::clone(state),
    }));
    // The room every filter this rewrite writes takes is reserved before any
    // of them, less what its install frees (see `published_floor`).
    let floor = sizing.floor_for(sizing.pending_hashes());
    {
        let _admission = state.admission.lock();
        sizing.set_reservation(0, floor);
    }
    Some(sizing)
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
        let price = self.current_price(Some((n, bounds)))?;
        let narrowest = self.narrowest();
        // Even the narrowest filters do not fit: a wider build is only ever
        // refused, so it is not built at all.
        if price.is_infinite() {
            return Ok(alloc::vec![narrowest]);
        }
        let mut candidates = self.preference(load, n, price);
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
    ///
    /// `priced` is the filter being priced, its hashes and its bounds: before
    /// any filter is built it is the one that tells how far the inputs'
    /// estimate runs from the keys written (see `remaining_loads`).
    ///
    /// Before any probe is observed the price only tells whether even the
    /// narrowest widths fit, and does so over what the rewrite leaves, not
    /// over the live tables it replaces.
    fn current_price(&self, priced: Option<Priced<'_>>) -> crate::Result<f64> {
        if self.inputs.is_empty() {
            return Ok(self.price);
        }
        let used = self.used();
        let spent = as_f64(self.spent.load(Relaxed));
        let estimated = f64::from_bits(self.spent_estimate.load(Relaxed));
        let error = if estimated > 0.0 {
            spent / estimated
        } else {
            1.0
        };
        // Filters already over the budget leave no room at all.
        let left = as_f64(self.budget.saturating_sub(used)) / error;

        let remaining = self.remaining_loads(priced)?;
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "a non-negative byte count below the budget"
        )]
        let left = left as u64;
        Ok(price(&remaining, &self.widths, left, &|len| {
            self.framing.frame(len)
        }))
    }

    /// The filters the inputs' data still to come makes, as the price models
    /// them: output tables of as many hashes as the largest this rewrite has
    /// written, filled in key order from the inputs' parts, each part at its
    /// input's load. The parts are the inputs' distinct keys past each key
    /// range's cursor, scaled by how far the estimate ran from the keys the
    /// built filters cover (see `pending_at`) and counted in the hashes the
    /// destination writes a key as.
    ///
    /// The destination groups the keys by its target size, not by input: a
    /// filter per input would charge a filter's fixed overhead for each of
    /// many small inputs merged into one output.
    ///
    /// Before any filter is built, `priced`, the filter being priced, tells
    /// how far the estimate runs: its hashes are known, and overlapping inputs
    /// counted each of its keys once per input holding it.
    fn remaining_loads(&self, priced: Option<Priced<'_>>) -> crate::Result<Vec<Load>> {
        let hashes_per_key = as_f64(self.hashes_per_key.0.load(Relaxed))
            / as_f64(self.hashes_per_key.1.load(Relaxed).max(1));
        let admitted = as_f64(self.admitted_keys.load(Relaxed));
        let estimated_admitted = as_f64(self.estimated_admitted.load(Relaxed));
        let written = if estimated_admitted > 0.0 {
            admitted / estimated_admitted
        } else if let Some((n, bounds)) = priced {
            // Its keys, at the rate the plan estimates a key's hashes.
            let estimated = as_f64(self.estimated_keys(bounds)?);
            if estimated > 0.0 {
                as_f64(u64::try_from(n).unwrap_or(u64::MAX)) / hashes_per_key / estimated
            } else {
                1.0
            }
        } else {
            1.0
        };

        // (where the part starts, its hashes, its negative probes)
        let cursors = self.cursors();
        let mut parts = Vec::with_capacity(self.inputs.len());
        for ((input, &density), (&hashes, &distinct)) in self
            .inputs
            .iter()
            .zip(&self.input_densities)
            .zip(self.input_keys.iter().zip(&self.input_distinct))
        {
            let mut share = 0.0;
            for (range, cursor) in cursors.iter().enumerate() {
                let (low, high) = self.range(range);
                let from = cursor
                    .as_ref()
                    .map_or(low, |bound| bound.as_ref().map(AsRef::as_ref));
                share += self.share_of(input, (from, high))?;
            }
            let out = as_f64(distinct) * share * written * hashes_per_key;
            if out >= 1.0 {
                // The input's probes fall on the keys it holds, its own hashes.
                let negatives = as_f64(hashes) * share * written * density;
                parts.push((input.metadata.key_range.min().as_ref(), out, negatives));
            }
        }
        match &self.key_order {
            Some(order) => parts.sort_by(|a, b| order.0.compare(a.0, b.0)),
            None => parts.sort_by(|a, b| a.0.cmp(b.0)),
        }

        // Before a table is written its size is unknown: the data still to
        // come is taken as one.
        let table = match self.table_keys.load(Relaxed) {
            0 => f64::INFINITY,
            hashes => as_f64(hashes),
        };
        let mut loads = Vec::new();
        let (mut hashes, mut negatives) = (0.0, 0.0);
        let mut close = |hashes: f64, negatives: f64| {
            if hashes >= 1.0 {
                #[expect(
                    clippy::cast_possible_truncation,
                    clippy::cast_sign_loss,
                    reason = "a positive hash count below the inputs' own"
                )]
                // Written into the destination level, at its settings.
                loads.push(Load {
                    negatives,
                    keys: hashes as usize,
                    fallback_bits: self.fallback_bits,
                    partition_keys: self.partition_keys,
                });
            }
        };
        for (_, mut out, mut probes) in parts {
            loop {
                let taken = table - hashes;
                if out <= taken {
                    break;
                }
                // The table fills with part of this input's; the rest opens
                // the next one, at the same load.
                let taken_probes = probes * taken / out;
                close(table, negatives + taken_probes);
                (hashes, negatives) = (0.0, 0.0);
                out -= taken;
                probes -= taken_probes;
            }
            hashes += out;
            negatives += probes;
        }
        close(hashes, negatives);
        Ok(loads)
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
    /// `bounds` are the ones [`Self::candidates`] priced the filter at: once
    /// taken, the filter is no longer in flight.
    ///
    /// `keys` are the keys the filter covers, fewer than its `n` hashes under
    /// a prefix extractor.
    #[expect(
        clippy::too_many_arguments,
        reason = "the filter's place, counts and bytes, as the build produced them"
    )]
    pub fn admit(
        &self,
        bounds: (Bound<&[u8]>, Bound<&[u8]>),
        n: usize,
        keys: usize,
        bytes: u64,
        estimated: u64,
        frame: &dyn Fn(u64) -> u64,
        last: bool,
    ) -> bool {
        let n = u64::try_from(n).unwrap_or(u64::MAX);
        let keys = u64::try_from(keys).unwrap_or(u64::MAX);
        // Read before the lock: it may walk the inputs' block indexes. One
        // that cannot be read estimates the filter's own keys.
        let estimated_keys = self.estimated_keys(bounds).unwrap_or(keys);
        // The keys still to come are claimed under the lock too: filters
        // admitted side by side (a table's partitions) each see the others'
        // keys gone once taken.
        let admission = self.state.admission.lock();
        // A rewrite that has ended installs nothing more: a filter a pool
        // thread finishes after that is dropped unwritten and holds nothing.
        if self.ended.load(Relaxed) {
            return true;
        }
        let per_filter = self.typical_keys.load(Relaxed).max(n);
        let ratio = self.ratio_with(n, keys);
        let admitted = self.admitted_keys.load(Relaxed) + keys;
        let estimated_admitted = self.estimated_admitted.load(Relaxed) + estimated_keys;
        let later = self.pending_at(admitted, estimated_admitted, ratio);
        let floor = self.floor(later, per_filter, frame);
        let used = self.used();
        let mine = self.reservation.load(Relaxed);
        // Every reservation changes under the admission lock, and this
        // rewrite's is one part of the sum.
        let others = self.state.reserved.load(Relaxed) - mine;
        // Filters already over the budget leave no room at all.
        let fits = bytes + floor + others <= self.budget.saturating_sub(used);
        if !fits && !last {
            return false;
        }
        // A table cannot hold 2^63 filter bytes, so neither can the sum.
        #[expect(
            clippy::cast_possible_wrap,
            reason = "filter byte counts far below 2^63"
        )]
        self.state.held.fetch_add(bytes as i64, Relaxed);
        // With the held bytes, so the end of the rewrite gives back all it took.
        self.spent.fetch_add(bytes, Relaxed);
        self.set_reservation(mine, floor);
        self.admitted_keys.store(admitted, Relaxed);
        self.estimated_admitted.store(estimated_admitted, Relaxed);
        self.hashes_per_key.0.store(ratio.0, Relaxed);
        self.hashes_per_key.1.store(ratio.1, Relaxed);
        self.typical_keys.fetch_max(n, Relaxed);
        drop(admission);

        self.leave(bounds.0);
        // The room kept for later filters is a reserve, not a limit. Whether
        // the tree is over its budget follows the filters its versions
        // publish, not the ones a rewrite is building, which it may never
        // install.
        if used + bytes > self.budget {
            self.state.log_entering(used + bytes, self.budget);
        }
        let mut spent = self.spent_estimate.load(Relaxed);
        while let Err(actual) = self.spent_estimate.compare_exchange_weak(
            spent,
            (f64::from_bits(spent) + as_f64(estimated)).to_bits(),
            Relaxed,
            Relaxed,
        ) {
            spent = actual;
        }
        true
    }

    /// The room `later` keys still to come take at the narrowest width, in
    /// filters of `per_filter` keys framed by `frame`.
    fn floor(&self, later: u64, per_filter: u64, frame: &dyn Fn(u64) -> u64) -> u64 {
        if later == 0 || per_filter == 0 {
            return 0;
        }
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
        // Charged per key rather than per whole filter: the pending count
        // runs a little over the keys (versions of one key, overwritten keys),
        // and rounding that excess up to a filter would refuse the last ones
        // room they have. `later` and the mean are both far below 2^32 in
        // practice; the product in u128 cannot overflow either way.
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
        // independently of another's, so their sum strays from the mean by
        // about the square root of the filters' count times one filter's
        // spread.
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "the square root of a filter count, non-negative and small"
        )]
        let strays = libm::ceil(libm::sqrt(as_f64(later.div_ceil(per_filter)))) as u64;
        // The last keys may go into one short filter of their own, bounded
        // from above: it is the one no later filter makes up for.
        let final_filter = frame(
            self.narrowest()
                .filter_size_bound(usize::try_from(later).unwrap_or(usize::MAX)) as u64,
        );
        (per_key + tables * tail_overhead + strays * spread).max(final_filter)
    }

    /// The room `pending` keys still to come take at the narrowest width,
    /// framed as this rewrite frames its blocks.
    fn floor_for(&self, pending: u64) -> u64 {
        let per_filter = match self.typical_keys.load(Relaxed) {
            // Before any filter, the keys as one filter's.
            0 => pending,
            keys => keys,
        };
        self.floor(pending, per_filter, &|len| self.framing.frame(len))
    }

    /// Sets this rewrite's floor to `floor` and replaces its reservation
    /// `mine` in the tree's sum with the part the others keep room for. The
    /// admission lock is held.
    fn set_reservation(&self, mine: u64, floor: u64) {
        let published = self.published_floor(floor);
        // `mine` is part of the sum, which changes only under the lock.
        let others = self.state.reserved.load(Relaxed) - mine;
        self.state.reserved.store(others + published, Relaxed);
        self.reservation.store(published, Relaxed);
        self.floor.store(floor, Relaxed);
    }

    /// The part of `floor` the other rewrites keep room for. The held bytes
    /// still count the filters this rewrite replaces, so while it holds
    /// their credit its floor asks only for what they do not cover: should it
    /// install, its later filters take the room they leave; should it fail,
    /// they stay and its own filters go.
    fn published_floor(&self, floor: u64) -> u64 {
        if self.replaced_released.load(Relaxed) {
            floor
        } else {
            // Room the replaced filters leave beyond the floor is a credit
            // only this rewrite spends: none of it goes to the others.
            floor.saturating_sub(self.replaced)
        }
    }

    /// The held bytes as this rewrite sees them: less the filters it
    /// replaces while it holds their credit, the room it builds into.
    fn used(&self) -> u64 {
        let held = self.state.held();
        if self.replaced_released.load(Relaxed) {
            held
        } else {
            // The held bytes count the replaced filters while they are live.
            held.saturating_sub(self.replaced)
        }
    }

    /// Gives up the credit for the filters this rewrite replaces, before its
    /// install drops them from the published version: from then on the room
    /// it builds into is what the held bytes leave, as for any rewrite.
    pub fn release_replaced(&self) {
        let _admission = self.state.admission.lock();
        self.release_locked();
    }

    /// [`Self::release_replaced`], with the admission lock held.
    fn release_locked(&self) {
        if !self.replaced_released.swap(true, Relaxed) {
            self.set_reservation(self.reservation.load(Relaxed), self.floor.load(Relaxed));
        }
    }

    /// Records that a table's filters, over `keys` hashes in all, are built.
    pub fn table_finished(&self, keys: usize) {
        self.table_keys
            .fetch_max(u64::try_from(keys).unwrap_or(u64::MAX), Relaxed);
    }

    /// Sets the keys this rewrite writes filters for in all, an upper bound,
    /// for a writer that learns the count only after the plan (an ingestion
    /// its caller tells): the ones no filter holds yet count as still to come.
    pub fn expect_keys(&self, keys: u64) {
        let _admission = self.state.admission.lock();
        self.total_keys.store(keys, Relaxed);
        let pending = self.pending_hashes();
        self.set_reservation(self.reservation.load(Relaxed), self.floor_for(pending));
    }

    /// The hashes the keys still to come hold, as the filters built so far
    /// have corrected the estimate.
    fn pending_hashes(&self) -> u64 {
        self.pending_at(
            self.admitted_keys.load(Relaxed),
            self.estimated_admitted.load(Relaxed),
            (
                self.hashes_per_key.0.load(Relaxed),
                self.hashes_per_key.1.load(Relaxed),
            ),
        )
    }

    /// The hashes the keys still to come hold once filters over `admitted`
    /// keys, estimated at `estimated_admitted`, are built, at `ratio` hashes
    /// a key.
    ///
    /// A rewrite with inputs estimates its keys by their shares of the
    /// inputs, which count a key in several overlapping inputs once in each
    /// and read a share of data bytes as one of keys. What the estimate has
    /// left is scaled by how far it ran from the keys the built filters
    /// cover, as the price is by its own error. A rewrite without inputs was
    /// told its keys, bounded from above.
    fn pending_at(&self, admitted: u64, estimated_admitted: u64, ratio: (u64, u64)) -> u64 {
        let total = self.total_keys.load(Relaxed);
        let keys = if self.inputs.is_empty() || estimated_admitted == 0 {
            // A bound from above: filters over more keys than it leave none
            // to come.
            total.saturating_sub(admitted)
        } else {
            // An estimate that ran past the inputs leaves none either.
            let left = total.saturating_sub(estimated_admitted);
            // `left` and `admitted` are key counts far below 2^32 each.
            u64::try_from(
                (u128::from(left) * u128::from(admitted)).div_ceil(u128::from(estimated_admitted)),
            )
            .unwrap_or(u64::MAX)
        };
        in_hashes(keys, ratio)
    }

    /// The inputs' distinct keys inside `bounds`, by the measure `total_keys`
    /// estimates them with; none for a rewrite without inputs.
    fn estimated_keys(&self, bounds: (Bound<&[u8]>, Bound<&[u8]>)) -> crate::Result<u64> {
        let mut keys = 0.0;
        for (input, &distinct) in self.inputs.iter().zip(&self.input_distinct) {
            keys += as_f64(distinct) * self.share_of(input, bounds)?;
        }
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "a non-negative sum of shares of the inputs' key counts"
        )]
        let keys = libm::ceil(keys) as u64;
        Ok(keys)
    }

    /// The hashes a key still to come holds once a filter of `n` hashes over
    /// `keys` keys is counted, as a fraction: the plan's estimate gives way
    /// to the first filter's, and after that the most any filter has held.
    /// The admission lock is held.
    fn ratio_with(&self, n: u64, keys: u64) -> (u64, u64) {
        let held = (
            self.hashes_per_key.0.load(Relaxed),
            self.hashes_per_key.1.load(Relaxed),
        );
        // A filter over no key says nothing of the rate.
        if keys == 0 {
            return held;
        }
        let first = self.admitted_keys.load(Relaxed) == 0;
        // Filter hash and key counts, far below 2^32 each, so the products
        // fit.
        if first || u128::from(n) * u128::from(held.1) > u128::from(held.0) * u128::from(keys) {
            (n, keys)
        } else {
            held
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
        for ((input, density), &input_keys) in self
            .inputs
            .iter()
            .zip(&self.input_densities)
            .zip(&self.input_keys)
        {
            let covered = as_f64(input_keys) * self.share_of(input, bounds)?;
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

impl FilterSizing {
    /// The rewrite has ended. Its own filters leave the held bytes: installed,
    /// the published version counts them; not installed, they are gone. The
    /// credit for what it replaces is given up, if its install did not
    /// already give it up.
    fn end(&self) {
        let _admission = self.state.admission.lock();
        if self.ended.swap(true, Relaxed) {
            return;
        }
        self.release_locked();
        // Its later filters are written or will never be.
        self.set_reservation(self.reservation.load(Relaxed), 0);
        #[expect(
            clippy::cast_possible_wrap,
            reason = "filter byte counts far below 2^63"
        )]
        self.state
            .held
            .fetch_sub(self.spent.load(Relaxed) as i64, Relaxed);
    }
}

/// A filter being priced: its hashes and the key range it covers.
type Priced<'a> = (usize, (Bound<&'a [u8]>, Bound<&'a [u8]>));

/// The hashes `keys` keys hold at `ratio` hashes a key, rounded up.
fn in_hashes(keys: u64, ratio: (u64, u64)) -> u64 {
    let (hashes, per) = ratio;
    // `per` is a key count of a filter, at least one; the quotient is at most
    // `keys` times the hashes a filter held per key.
    u64::try_from((u128::from(keys) * u128::from(hashes)).div_ceil(u128::from(per.max(1))))
        .unwrap_or(u64::MAX)
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
fn price(loads: &[Load], widths: &[u8], budget: u64, frame: &dyn Fn(u64) -> u64) -> f64 {
    // Each table's filter is decided the way its writer decides it: per
    // partition when partitioned, at the table's load density. Sizes and
    // rates do not depend on the price, so the search reads them from here.
    struct Entry {
        filter_load: f64,
        fallback_bits: u8,
        /// Index into the full partitions' sizes for a table's full
        /// partitions, whose width one of them decides; otherwise the entry
        /// is one filter, a table's or its short last partition.
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
    // On-disk bytes of each filter block at each width, as the budget counts
    // them: a block's framing weighs on many small filters.
    let sizes =
        |n: usize, widths: &[u8]| -> Vec<u64> { sizes(n, widths).into_iter().map(frame).collect() };
    // A full partition's sizes depend on its key count alone, which the
    // tables of one level share: worked out once per count, not per table.
    let mut partitions: Vec<(usize, Vec<u64>)> = Vec::new();
    let mut entries: Vec<Entry> = Vec::with_capacity(loads.len());
    for load in loads {
        let n = load.keys;
        // The load a filter over `keys` of the table's keys draws.
        let load_of = |keys: usize| load.negatives * as_f64(keys as u64) / as_f64(n.max(1) as u64);
        let partition = match load.partition_keys {
            Some(keys) if keys > 0 => keys,
            _ => usize::MAX,
        };
        if n <= partition {
            entries.push(Entry {
                filter_load: load_of(n.max(1)),
                fallback_bits: load.fallback_bits,
                partition: None,
                table_bytes: sizes(n, widths),
            });
            continue;
        }
        let index = if let Some(index) = partitions.iter().position(|(keys, _)| *keys == partition)
        {
            index
        } else {
            partitions.push((partition, sizes(partition, widths)));
            partitions.len() - 1
        };
        let count = (n / partition) as u64;
        let full = partitions.get(index).map_or(&[][..], |(_, full)| full);
        entries.push(Entry {
            filter_load: load_of(partition),
            fallback_bits: load.fallback_bits,
            partition: Some(index),
            table_bytes: full.iter().map(|&full| count * full).collect(),
        });
        // The short last partition: its writer prices it over its own keys,
        // where a filter's bytes per key run higher, so it can take a
        // narrower width than the full ones.
        let rest = n % partition;
        if rest > 0 {
            entries.push(Entry {
                filter_load: load_of(rest),
                fallback_bits: load.fallback_bits,
                partition: None,
                table_bytes: sizes(rest, widths),
            });
        }
    }
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
pub fn bits_of(policy: BloomConstructionPolicy) -> u8 {
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

/// The part of [`filter_keys`] a table's view serves: a view a tight-space
/// slice restricted serves only its suffix and keeps only the suffix's probe
/// counts, while its filter and metadata still hold the whole file's keys. The
/// suffix's share is its share of the data section; one that cannot be read
/// counts the table whole.
fn served_keys(table: &Table) -> u64 {
    served(table, filter_keys(table))
}

/// The distinct keys a table's view serves, the entries bounding them from
/// above where the table does not record them (see [`served_keys`]).
fn served_distinct_keys(table: &Table) -> u64 {
    served(
        table,
        table
            .metadata
            .key_count
            .unwrap_or(table.metadata.item_count),
    )
}

/// The part of `keys`, a count over the whole of `table`, its view serves.
fn served(table: &Table, keys: u64) -> u64 {
    if table.restrict_lower_bound().is_none() {
        return keys;
    }
    let Ok(Some(span)) = table.data_span(
        (Bound::Unbounded, Bound::Unbounded),
        crate::SeqNo::MAX,
        crate::table::SpanEdge::ByLastKey,
    ) else {
        return keys;
    };
    // `live_start <= data_end`, and `data_end > 0` for a span that exists.
    let live = span.data_end - span.live_start;
    #[expect(
        clippy::cast_possible_truncation,
        reason = "live <= data_end, so the quotient never exceeds the u64 count"
    )]
    let served = (u128::from(keys) * u128::from(live) / u128::from(span.data_end)) as u64;
    served
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

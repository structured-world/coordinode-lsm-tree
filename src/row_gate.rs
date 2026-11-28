// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The share of a tree's keys whose point reads go through the row cache,
//! chosen by a simulation in miniature.
//!
//! Rows pay where keys are read again while their rows still fit the cache,
//! and cost where they do not: a probe and a fill on every miss, and bytes
//! taken from the blocks. Which of the two a tree is in depends on the cache
//! against the data read, on how keys repeat, and on how often compaction
//! retires the tables rows are keyed by; no fixed setting is right for all.
//!
//! A tree therefore holds a fraction of its keys, by hash, inside the row
//! cache: none, an eighth, a quarter, a half or all. A read of a key outside
//! pays one load of that fraction and nothing else.
//!
//! The fraction is chosen by spatial sampling (SHARDS, Waldspurger et al.,
//! FAST 2015): one read in [`SAMPLE`] by key hash is also played against one
//! shadow cache per candidate fraction. A shadow holds weights and no values,
//! has the cache's capacity divided by the sampling rate, and runs the cache's
//! own policy and admission. Each window of sampled reads prices every
//! candidate from what its shadow did, into a running cost that halves with
//! every window, and the tree moves to the cheapest once it beats the
//! fraction in use by more than a margin. Every candidate is played all the
//! time, so a tree with rows off comes back to them when its reads change.

use crate::sharded_cache::{Priority, ShardedCache, Weighter};
use core::sync::atomic::{AtomicU8, Ordering};

/// The candidate fractions, in eighths of the keys.
const CANDIDATES: [u8; 5] = [0, 1, 2, 4, 8];

/// One read in this many is sampled, by key hash; in more where rows are
/// small (see [`ENTRY_FLOOR`]).
const SAMPLE: u64 = 256;

/// Bytes a shadow holds per entry: its slot in the table (48), its place in
/// a queue (8) and its share of the buckets (8).
const ENTRY_BYTES: u64 = 64;

/// Bytes a shadow holds per key it remembers having refused or evicted: the
/// fingerprint in its queue and in its set.
const GHOST_BYTES: u64 = 24;

/// A shadow remembers one such key per this many entries it can hold. The
/// cache remembers about one per row it can hold, of all its reads; a
/// shadow sees one read in [`SAMPLE`], so a quarter as many reach as far
/// back for it as the cache's do, and they are bytes of the same budget.
const GHOST_SHARE: u64 = 4;

/// The fewest entries a shadow can hold for a tree to be simulated at all.
/// A cache too small for that keeps every key in its row cache: a shadow of
/// a handful of entries tells nothing, and its bookkeeping alone would
/// outweigh the share of the cache it is given.
const SHADOW_ENTRIES_MIN: u64 = 16;

/// The least an entry weighs in a shadow that samples one read in
/// [`SAMPLE`]: what keeps the five shadows' entries, [`ENTRY_BYTES`] each,
/// and the keys they remember, [`GHOST_BYTES`] each, within a two-hundredth
/// of the cache's capacity. A tree of smaller rows samples one read in twice
/// as many, up to sixteen times, for each halving of the row below it, and
/// an entry lighter still is weighed at the floor.
const ENTRY_FLOOR: u64 =
    (CANDIDATES.len() as u64 * (ENTRY_BYTES + GHOST_BYTES / GHOST_SHARE) * 200).div_ceil(SAMPLE);

/// Sampled reads a window holds before its candidates are priced: 131,072 of
/// the tree's reads. A tree decides at its second window at the earliest.
const WINDOW: u32 = 512;

/// A candidate replaces the fraction in use only when its running cost is
/// lower by more than this many thousandths. Rows that are never read again
/// cost a tree a probe and a refusal or a fill each, a tenth of a read that
/// goes to its block; an eighth of the keys left inside costs an eighth of
/// that, which the margin has to stay under for such a tree to close fully.
const MARGIN_PER_MILLE: u64 = 5;

/// What each event of a point read costs, in nanoseconds, as the comparison
/// harness measures them on a block of 4 KiB held in the page cache. Only
/// their ratios matter to the choice. What a block miss adds to a block hit
/// is the one that differs by orders of magnitude between deployments (the
/// codec, the device, the block size), so where the cache has timed its
/// loads slower than [`cost::BLOCK_MISS`] allows for, that time prices a
/// miss (see [`block_miss_cost`]).
mod cost {
    /// A read answered by a cached row.
    pub(super) const ROW_HIT: u64 = 450;
    /// A row looked for and not found, before the read goes on to its block.
    pub(super) const ROW_PROBE: u64 = 60;
    /// A row refused on its first miss into a full cache: its key written
    /// to the ghost queue, an older one dropped from it. Measured as most of
    /// the 470 ns a read of 70,000 keys through a 16 MiB cache pays for a
    /// row cache that takes almost no row.
    pub(super) const ROW_REFUSAL: u64 = 350;
    /// A row admitted, copied out of its block and inserted.
    pub(super) const ROW_FILL: u64 = 400;
    /// A read answered from a cached block.
    pub(super) const BLOCK_HIT: u64 = 1_400;
    /// A read that loaded its block from the file.
    pub(super) const BLOCK_MISS: u64 = 4_000;
}

/// What a read that misses its block costs where the cache has timed its
/// loads at `load_nanos`, zero for none timed: a hit plus the load, and the
/// constant at the least. The timed load is the read and the decode; the
/// buffer, the insert and the eviction a miss also pays are in the constant
/// alone, so a load out of the page cache, a few hundred nanoseconds, does
/// not make a miss cost little more than a hit.
const fn block_miss_cost(load_nanos: u64) -> u64 {
    let timed = cost::BLOCK_HIT + load_nanos;
    if timed > cost::BLOCK_MISS {
        timed
    } else {
        cost::BLOCK_MISS
    }
}

/// A shadow's entry, a row or a block of one table, as one word mixed from
/// its kind, its table and its key hash or block offset: a third of the
/// bytes a tuple of the three would take in every slot. Two entries of a few
/// hundred sharing a word would be played as one, which a window's count of
/// events does not notice.
type ShadowKey = u64;
const ROW: u64 = 0x9E37_79B9_7F4A_7C15;
const BLOCK: u64 = 0xC2B2_AE3D_27D4_EB4F;

/// The word of the entry of `kind` in `table` at `place`.
const fn shadow_key(kind: u64, table: u64, place: u64) -> ShadowKey {
    (table.wrapping_mul(kind) ^ place)
        .wrapping_mul(kind)
        .rotate_left(29)
        ^ kind
}

/// Weighs a shadow's entry by the weight it holds in place of a value.
#[derive(Clone, Copy)]
struct Held;

impl Weighter<ShadowKey, u32> for Held {
    fn weight(&self, _: &ShadowKey, weight: &u32) -> u64 {
        u64::from(*weight)
    }
}

type Shadow = ShardedCache<ShadowKey, u32, Held, rustc_hash::FxBuildHasher>;

/// What one candidate's shadow did in a window.
#[derive(Clone, Copy, Default)]
struct Events {
    row_hits: u64,
    row_probes: u64,
    row_refusals: u64,
    row_fills: u64,
    block_hits: u64,
    block_misses: u64,
}

impl Events {
    /// The window's cost where a block miss costs `block_miss`.
    const fn cost(self, block_miss: u64) -> u64 {
        self.row_hits * cost::ROW_HIT
            + self.row_probes * cost::ROW_PROBE
            + self.row_refusals * cost::ROW_REFUSAL
            + self.row_fills * cost::ROW_FILL
            + self.block_hits * cost::BLOCK_HIT
            + self.block_misses * block_miss
    }
}

/// The simulation: one shadow and one window's events per candidate.
struct Sim {
    /// Built at the first sampled read, and again when the cache is resized.
    shadows: Option<[Shadow; CANDIDATES.len()]>,
    /// The capacity the shadows were built for.
    capacity: u64,
    events: [Events; CANDIDATES.len()],
    /// Sampled reads in the window so far.
    seen: u32,
    /// Each candidate's cost over the windows so far, a window counting half
    /// of what the next one does.
    running: [u64; CANDIDATES.len()],
    /// Windows closed so far.
    windows: u32,
    /// What the cache's loads were taking at the last sampled read, in
    /// nanoseconds; zero where it has timed none.
    load_nanos: u64,
    /// Sampled reads of the window whose row the cache held.
    held: u64,
    /// Windows the tree has had rows for, since it last had none.
    open_for: u32,
    /// Windows left in which the tree keeps no rows whatever its shadows
    /// say (see [`RowGate::undelivered`]).
    barred: u32,
    /// How long the next bar lasts, in windows.
    bar: u32,
}

/// Windows a tree has rows for before its cache's row hits are checked
/// against its shadows': the one in which the cache fills with them. A cache
/// that delivers does so from its first window, and one that does not is
/// then barred within a quarter of a million reads.
const BAR_SETTLE: u32 = 1;

/// Row hits the shadow of the fraction in use has to count in a window for
/// the cache's own to be checked against them: an eighth of the window's
/// reads. Fewer are a few keys the shadow remembers from reads that have
/// since changed, which the cache has rightly dropped: checked at seventeen
/// such hits, a tree whose reads turn from a hot set to all its keys and
/// back was barred from the rows of its hot set.
const BAR_HITS_MIN: u64 = WINDOW as u64 / 8;

/// The cache delivers too little when it gives fewer than one in this many
/// of the row hits the shadow counted. Far below what sampling a few hot
/// keys can explain, where a tree's cache gives two fifths of its shadow's
/// hits and its rows still pay.
const BAR_BELOW: u64 = 8;

/// The first bar, in windows: 33 million reads. Each bar that follows a
/// trial in which the cache delivered as little is twice as long.
const BAR_FIRST: u32 = 256;

/// The longest bar, in windows.
const BAR_LONGEST: u32 = 1 << 14;

/// A sampled read's place in its table: the block it came from, by offset,
/// and the block's weight in the cache.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BlockSeen {
    pub(crate) offset: u64,
    pub(crate) weight: u64,
}

/// See the module docs.
pub(crate) struct RowGate {
    /// Eighths of the keys inside the row cache.
    eighths: AtomicU8,
    /// How many times [`SAMPLE`] is doubled for this tree's rows: set when
    /// its shadows are built, from the first row sampled.
    sparser: AtomicU8,
    /// Taken by a sampled read that finds it free, and by none other.
    sim: spin::Mutex<Sim>,
}

impl core::fmt::Debug for RowGate {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RowGate")
            .field("eighths", &self.eighths())
            .finish_non_exhaustive()
    }
}

/// The bits of a key hash that place the key in or out of the fraction:
/// apart from the bits that pick a cache shard and from the sampling bits.
#[inline]
const fn place(key_hash: u64) -> u8 {
    ((key_hash >> 29) & 7) as u8
}

impl RowGate {
    /// A gate with every key inside: rows are on until the simulation has
    /// seen reads that say otherwise.
    pub(crate) fn new() -> Self {
        Self {
            eighths: AtomicU8::new(8),
            sparser: AtomicU8::new(0),
            sim: spin::Mutex::new(Sim {
                shadows: None,
                capacity: 0,
                events: [Events::default(); CANDIDATES.len()],
                seen: 0,
                running: [0; CANDIDATES.len()],
                windows: 0,
                load_nanos: 0,
                held: 0,
                open_for: 0,
                barred: 0,
                bar: BAR_FIRST,
            }),
        }
    }

    /// Eighths of the keys inside the row cache now.
    pub(crate) fn eighths(&self) -> u8 {
        self.eighths.load(Ordering::Relaxed)
    }

    /// Whether the row of the key hashed to `key_hash` goes through the row
    /// cache.
    #[inline]
    pub(crate) fn admits(&self, key_hash: u64) -> bool {
        place(key_hash) < self.eighths.load(Ordering::Relaxed)
    }

    /// Whether a read of the key hashed to `key_hash` is one the simulation
    /// plays. Such a read goes to its block whatever the row cache holds, so
    /// the block it came from is known.
    #[inline]
    pub(crate) fn samples(&self, key_hash: u64) -> bool {
        let rate = SAMPLE << self.sparser.load(Ordering::Relaxed);
        (key_hash >> 40) & (rate - 1) == 0
    }

    /// Bytes the shadows hold now.
    pub(crate) fn memory(&self) -> u64 {
        let sim = self.sim.lock();
        sim.shadows.as_ref().map_or(0, |shadows| {
            shadows
                .iter()
                .map(|shadow| {
                    shadow.len() as u64 * ENTRY_BYTES + shadow.ghosts() as u64 * GHOST_BYTES
                })
                .sum()
        })
    }

    /// Plays one sampled read against every candidate's shadow, and at the
    /// end of a window moves the fraction to the candidate that keeps
    /// winning. Skipped when another sampled read is being played. `capacity`
    /// is the cache's, which the shadows are scaled from, `load_nanos` what
    /// its cold loads have been taking, zero where none was timed, and
    /// `held` whether the cache held the key's row.
    #[expect(
        clippy::too_many_arguments,
        reason = "one sampled read, as the table saw it"
    )]
    pub(crate) fn observe(
        &self,
        capacity: u64,
        table: u64,
        key_hash: u64,
        row_weight: u64,
        block: BlockSeen,
        load_nanos: u64,
        held: bool,
    ) {
        let Some(mut sim) = self.sim.try_lock() else {
            return;
        };
        sim.load_nanos = load_nanos;
        sim.held += u64::from(held);
        if sim.shadows.is_none() {
            // Rows too small for the shadows' share of memory at one read in
            // `SAMPLE` are sampled more sparsely, by as much as they fall
            // short. The reads sampled before this one counted at the old
            // rate into shadows that did not exist yet.
            let short = ENTRY_FLOOR.div_ceil(row_weight.max(1)).next_power_of_two();
            let sparser = u8::try_from(short.trailing_zeros().min(4)).unwrap_or(4);
            self.sparser.store(sparser, Ordering::Relaxed);
        }
        let sparser = u32::from(self.sparser.load(Ordering::Relaxed));
        let rate = SAMPLE << sparser;
        let capacity = capacity / rate;
        let floor = (ENTRY_FLOOR >> sparser).max(1);
        let entries = capacity / floor;
        if entries < SHADOW_ENTRIES_MIN {
            sim.shadows = None;
            return;
        }
        if sim.shadows.is_none() || sim.capacity != capacity {
            sim.capacity = capacity;
            // The keys a shadow remembers are counted in its share of
            // memory like its entries, a fixed part of what it can hold. A
            // thousand of them whatever the cache took more than the whole
            // budget of a small cache's shadows.
            let ghosts = usize::try_from(entries / GHOST_SHARE).unwrap_or(usize::MAX);
            sim.shadows = Some(core::array::from_fn(|_| {
                let shadow = ShardedCache::with_weighter(
                    capacity,
                    1,
                    ghosts,
                    Held,
                    rustc_hash::FxBuildHasher,
                );
                shadow.set_ghosts(ghosts);
                shadow
            }));
        }

        // In the sampled universe a block holds only its sampled keys: about
        // one in `rate` of what it holds, or one key's share of it where it
        // holds fewer keys than that. Weighing it so keeps the sampled blocks
        // the same share of the shadow that all blocks are of the cache. No
        // entry weighs less than the floor that bounds the shadows' memory.
        let keys = (block.weight / row_weight.max(1)).clamp(1, rate);
        let block_weight = u32::try_from((block.weight / keys).max(floor)).unwrap_or(u32::MAX);
        let row_weight = u32::try_from(row_weight.max(floor)).unwrap_or(u32::MAX);
        let row = shadow_key(ROW, table, key_hash);
        let block = shadow_key(BLOCK, table, block.offset);

        let Sim {
            shadows, events, ..
        } = &mut *sim;
        let Some(shadows) = shadows else {
            return;
        };
        for ((shadow, events), eighths) in shadows.iter().zip(events.iter_mut()).zip(CANDIDATES) {
            let inside = place(key_hash) < eighths;
            if inside {
                if shadow.get(&row).is_some() {
                    events.row_hits += 1;
                    continue;
                }
                events.row_probes += 1;
            }
            if shadow.get(&block).is_some() {
                events.block_hits += 1;
            } else {
                events.block_misses += 1;
                shadow.insert(block, block_weight);
            }
            if !inside {
                continue;
            }
            // A row the shadow could never hold is refused with nothing
            // remembered of it, as the cache refuses one heavier than it
            // keeps.
            if u64::from(row_weight) <= capacity && shadow.admit(&row, u64::from(row_weight)) {
                shadow.insert_with_priority(row, row_weight, Priority::Normal);
                events.row_fills += 1;
            } else {
                events.row_refusals += 1;
            }
        }

        sim.seen += 1;
        if sim.seen >= WINDOW {
            self.close_window(&mut sim);
        }
    }

    /// Prices the window's candidates into their running costs and moves to
    /// the cheapest when it beats the fraction in use by the margin.
    fn close_window(&self, sim: &mut Sim) {
        let block_miss = block_miss_cost(sim.load_nanos);
        let in_use = self.eighths();
        let current = CANDIDATES
            .iter()
            .position(|&eighths| eighths == in_use)
            .unwrap_or(CANDIDATES.len() - 1);
        let undelivered = Self::undelivered(sim, current);
        // Each window counts half of what the one after it does. One window
        // alone does not tell the candidates apart: a shadow holds a few
        // hundred entries, and a handful of block misses more or less in it,
        // each worth dozens of row probes, falls to chance.
        for (running, events) in sim.running.iter_mut().zip(sim.events) {
            *running = *running / 2 + events.cost(block_miss);
        }
        sim.events = [Events::default(); CANDIDATES.len()];
        sim.seen = 0;
        sim.held = 0;
        sim.windows = sim.windows.saturating_add(1);
        if undelivered {
            sim.barred = sim.bar;
            sim.bar = sim.bar.saturating_mul(2).min(BAR_LONGEST);
            self.eighths.store(0, Ordering::Relaxed);
        }
        if sim.barred > 0 {
            sim.barred -= 1;
            return;
        }
        // The first window starts from empty shadows, where every candidate
        // misses alike: the second is the first to tell them apart. A tree
        // that waited for a third paid for rows that do not pay through
        // another 131,072 reads.
        if sim.windows < 2 {
            return;
        }

        let best = sim
            .running
            .iter()
            .enumerate()
            .min_by_key(|&(_, cost)| *cost)
            .map_or(current, |(candidate, _)| candidate);
        let (Some(&best_cost), Some(&current_cost)) =
            (sim.running.get(best), sim.running.get(current))
        else {
            return;
        };
        if best != current
            && best_cost.saturating_mul(1_000)
                < current_cost.saturating_mul(1_000 - MARGIN_PER_MILLE)
            && let Some(&eighths) = CANDIDATES.get(best)
        {
            self.eighths.store(eighths, Ordering::Relaxed);
        }
    }

    /// Whether, in the window `sim` holds, the cache gave almost none of the
    /// row hits the shadow of the fraction in use, the candidate at
    /// `current`, counted on the same sampled reads. Counts the window
    /// toward the time the tree has had rows for.
    ///
    /// A shadow is the cache in miniature and not the cache: a block stands
    /// in it as one sampled key's share, so a miss brings in a fraction of
    /// the bytes it brings into the cache, and rows outlive in a shadow the
    /// stream of blocks that evicts them from the cache before their keys
    /// are read again. Such a tree pays a probe and a refusal on every read
    /// for rows it never holds. It keeps no rows for a bar of windows, then
    /// lets its shadows choose again, and a trial that ends the same way
    /// doubles the bar; one in which the cache delivers ends the doubling.
    fn undelivered(sim: &mut Sim, current: usize) -> bool {
        if current == 0 {
            sim.open_for = 0;
            return false;
        }
        sim.open_for = sim.open_for.saturating_add(1);
        let counted = sim.events.get(current).map_or(0, |events| events.row_hits);
        if sim.open_for <= BAR_SETTLE || counted < BAR_HITS_MIN {
            return false;
        }
        if sim.held * BAR_BELOW < counted {
            return true;
        }
        sim.bar = BAR_FIRST;
        false
    }

    /// Sets the fraction, in eighths, as the simulation would: for tests,
    /// in this crate and of it.
    pub(crate) fn set_eighths(&self, eighths: u8) {
        self.eighths.store(eighths, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests;

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
//! candidate from what its shadow did, and the tree moves to the cheapest once
//! it has won two windows in a row by more than a margin. Every candidate is
//! played all the time, so a tree with rows off comes back to them when its
//! reads change.

use crate::sharded_cache::{Priority, ShardedCache, Weighter};
use core::sync::atomic::{AtomicU8, Ordering};

/// The candidate fractions, in eighths of the keys.
const CANDIDATES: [u8; 5] = [0, 1, 2, 4, 8];

/// One read in this many is sampled, by key hash.
const SAMPLE: u64 = 256;

/// Sampled reads a window holds before its candidates are priced.
const WINDOW: u32 = 1_024;

/// A candidate replaces the fraction in use only when it is cheaper by more
/// than this many thousandths, in two windows running. Below the cost of
/// the probe alone, about a hundredth of a read that goes to its block, so a
/// tree whose rows are never read again stops probing for them.
const MARGIN_PER_MILLE: u64 = 5;

/// What each event of a point read costs, in nanoseconds, as the comparison
/// harness measures them on a block of 4 KiB held in the page cache. Only
/// their ratios matter to the choice.
mod cost {
    /// A read answered by a cached row.
    pub(super) const ROW_HIT: u64 = 450;
    /// A row looked for and not found, before the read goes on to its block.
    pub(super) const ROW_PROBE: u64 = 40;
    /// A row copied out of its block and inserted.
    pub(super) const ROW_FILL: u64 = 200;
    /// A read answered from a cached block.
    pub(super) const BLOCK_HIT: u64 = 1_400;
    /// A read that loaded its block from the file.
    pub(super) const BLOCK_MISS: u64 = 4_000;
}

/// A shadow's entry: a row or a block of one table.
type ShadowKey = (u8, u64, u64);
const ROW: u8 = 0;
const BLOCK: u8 = 1;

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
    row_fills: u64,
    block_hits: u64,
    block_misses: u64,
}

impl Events {
    const fn cost(self) -> u64 {
        self.row_hits * cost::ROW_HIT
            + self.row_probes * cost::ROW_PROBE
            + self.row_fills * cost::ROW_FILL
            + self.block_hits * cost::BLOCK_HIT
            + self.block_misses * cost::BLOCK_MISS
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
    /// The candidate that won the last window by the margin, if one did.
    leader: Option<usize>,
}

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
            sim: spin::Mutex::new(Sim {
                shadows: None,
                capacity: 0,
                events: [Events::default(); CANDIDATES.len()],
                seen: 0,
                leader: None,
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
    pub(crate) const fn samples(key_hash: u64) -> bool {
        (key_hash >> 40).is_multiple_of(SAMPLE)
    }

    /// Plays one sampled read against every candidate's shadow, and at the
    /// end of a window moves the fraction to the candidate that keeps
    /// winning. Skipped when another sampled read is being played. `capacity`
    /// is the cache's, which the shadows are scaled from.
    pub(crate) fn observe(
        &self,
        capacity: u64,
        table: u64,
        key_hash: u64,
        row_weight: u64,
        block: BlockSeen,
    ) {
        let Some(mut sim) = self.sim.try_lock() else {
            return;
        };
        let capacity = capacity / SAMPLE;
        if sim.shadows.is_none() || sim.capacity != capacity {
            sim.capacity = capacity;
            sim.shadows = Some(core::array::from_fn(|_| {
                ShardedCache::with_weighter(capacity, 1, 1_024, Held, rustc_hash::FxBuildHasher)
            }));
        }

        // In the sampled universe a block holds only its sampled keys: about
        // one in `SAMPLE` of what it holds, or one key's share of it where it
        // holds fewer keys than that. Weighing it so keeps the sampled blocks
        // the same share of the shadow that all blocks are of the cache.
        let keys = (block.weight / row_weight.max(1)).clamp(1, SAMPLE);
        let block_weight = u32::try_from((block.weight / keys).max(1)).unwrap_or(u32::MAX);
        let row_weight = u32::try_from(row_weight.max(1)).unwrap_or(u32::MAX);
        let row: ShadowKey = (ROW, table, key_hash);
        let block: ShadowKey = (BLOCK, table, block.offset);

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
            if inside && shadow.admit(&row, u64::from(row_weight)) {
                shadow.insert_with_priority(row, row_weight, Priority::Probation);
                events.row_fills += 1;
            }
        }

        sim.seen += 1;
        if sim.seen >= WINDOW {
            self.close_window(&mut sim);
        }
    }

    /// Prices the window's candidates and moves to the cheapest when it has
    /// now won two windows running by the margin.
    fn close_window(&self, sim: &mut Sim) {
        let costs = sim.events.map(Events::cost);
        sim.events = [Events::default(); CANDIDATES.len()];
        sim.seen = 0;

        let in_use = self.eighths();
        let current = CANDIDATES
            .iter()
            .position(|&eighths| eighths == in_use)
            .unwrap_or(CANDIDATES.len() - 1);
        let best = costs
            .iter()
            .enumerate()
            .min_by_key(|&(_, cost)| *cost)
            .map_or(current, |(candidate, _)| candidate);
        let (Some(&best_cost), Some(&current_cost)) = (costs.get(best), costs.get(current)) else {
            return;
        };
        let wins = best != current && best_cost * 1_000 < current_cost * (1_000 - MARGIN_PER_MILLE);
        if !wins {
            sim.leader = None;
        } else if sim.leader == Some(best) {
            if let Some(&eighths) = CANDIDATES.get(best) {
                self.eighths.store(eighths, Ordering::Relaxed);
            }
            sim.leader = None;
        } else {
            sim.leader = Some(best);
        }
    }

    /// Sets the fraction, in eighths, as the simulation would.
    #[cfg(test)]
    pub(crate) fn set_eighths(&self, eighths: u8) {
        self.eighths.store(eighths, Ordering::Relaxed);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "test code")]
mod tests;

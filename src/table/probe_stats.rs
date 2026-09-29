// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! How often a table's filter is probed, and how often for a key the table
//! does not hold: what filter memory is allocated against.

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::sync::atomic::Ordering::Relaxed;
// 32-bit targets without native 64-bit atomics get them from the crate.
use portable_atomic::AtomicU64;

/// A table's filter probes over a sliding window, kept only while the tree
/// allocates filter memory by probe load.
///
/// A negative probe is a probe for a key the table holds no version of: the
/// filter said absent, or it said present and the data block read that
/// followed found no version. A key present only in versions the reader's
/// snapshot cannot see, or whose newest visible version is a tombstone, is
/// held by the table and is not one.
///
/// Not durable: a reopened tree starts cold. The window is halved by
/// [`Self::decay`], so a burst stops weighing on a table once it is over.
#[derive(Debug, Default)]
pub struct ProbeStats {
    probes: AtomicU64,
    negative: AtomicU64,
}

impl ProbeStats {
    /// Records a probe that reached the filter.
    pub fn probe(&self) {
        self.probes.fetch_add(1, Relaxed);
    }

    /// Records that a probe was for a key the table does not hold.
    pub fn negative(&self) {
        self.negative.fetch_add(1, Relaxed);
    }

    /// Probes that reached the filter, within the window.
    pub fn probes(&self) -> u64 {
        self.probes.load(Relaxed)
    }

    /// Probes for a key the table does not hold, within the window.
    pub fn negatives(&self) -> u64 {
        self.negative.load(Relaxed)
    }

    /// Adds counts: those a new table inherits from the tables it replaces,
    /// or those a read tallied while planning, once it answers from them.
    pub fn add(&self, counts: ProbeCounts) {
        self.probes.fetch_add(counts.probes, Relaxed);
        self.negative.fetch_add(counts.negatives, Relaxed);
    }

    /// Halves both counts: each window weighs half as much as the one after
    /// it. A probe racing the halving may be counted in either window.
    pub fn decay(&self) {
        for counter in [&self.probes, &self.negative] {
            let mut current = counter.load(Relaxed);
            while let Err(actual) =
                counter.compare_exchange_weak(current, current / 2, Relaxed, Relaxed)
            {
                current = actual;
            }
        }
    }
}

/// Probe counts carried from one table to another, or tallied by a read
/// before it counts them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProbeCounts {
    pub probes: u64,
    pub negatives: u64,
}

impl core::ops::AddAssign for ProbeCounts {
    fn add_assign(&mut self, other: Self) {
        // One read's probes, far below 2^64.
        self.probes += other.probes;
        self.negatives += other.negatives;
    }
}

/// The counts a table covering `first..=last` inherits from `inputs`, the
/// tables a compaction rewrote into it.
///
/// An input gives the share of its counts that its own data inside a part of
/// the range holds, measured by data block offsets in its index: an output
/// taking a hot narrow input whole and a slice of a cold wide one carries all
/// of the first and that slice of the second.
///
/// Where inputs overlap, the same lookups probed each of them, so each part
/// of the range takes one input's counts: the oldest one holding data there.
/// A point read goes from the newest table to the oldest and stops at the
/// key, so the lookups reaching the oldest input are those every newer one
/// let past, and its negatives are the keys none of them holds, the ones the
/// output will be probed for and not hold; it has also watched longest.
/// Inputs that do not overlap, the tables of one level, each give their own.
///
/// This is a heuristic, and its limit is the counter's: one scalar per table
/// carries no spatial distribution, so an input split across several outputs
/// hands each the same density, whichever part of it was actually hot.
pub fn inherited_counts(
    first: &[u8],
    last: &[u8],
    inputs: &[crate::table::Table],
) -> crate::Result<ProbeCounts> {
    use core::cmp::Ordering::{Equal, Greater, Less};
    use core::ops::Bound::{Excluded, Included};

    let Some(comparator) = inputs.first().map(|input| input.comparator.clone()) else {
        return Ok(ProbeCounts::default());
    };
    let cmp = |a: &[u8], b: &[u8]| comparator.compare(a, b);

    // Where the parts of the range start: where an input's data starts, and
    // just past where it ends (`true`).
    let mut starts: Vec<(&[u8], bool)> = Vec::new();
    for input in inputs {
        let range = &input.metadata.key_range;
        let (min, max) = (range.min().as_ref(), range.max().as_ref());
        if cmp(min, first) == Greater && cmp(min, last) != Greater {
            starts.push((min, false));
        }
        if cmp(max, first) != Less && cmp(max, last) == Less {
            starts.push((max, true));
        }
    }
    starts.sort_by(|a, b| cmp(a.0, b.0).then(a.1.cmp(&b.1)));
    starts.dedup_by(|a, b| cmp(a.0, b.0) == Equal && a.1 == b.1);

    let mut by_age: Vec<&crate::table::Table> = inputs.iter().collect();
    by_age.sort_by_key(|input| input.get_highest_seqno());

    let mut inherited = ProbeCounts::default();
    let mut lower = Included(first);
    for index in 0..=starts.len() {
        let next = starts.get(index).copied();
        let upper = match next {
            Some((key, true)) => Included(key),
            Some((key, false)) => Excluded(key),
            None => Included(last),
        };
        for input in &by_age {
            if !input.check_key_range_overlap_cmp(&(lower, upper), comparator.as_ref()) {
                continue;
            }
            if let Some(share) = region_share(input, (lower, upper))? {
                // Each share is at most a count a table held, and a table
                // cannot see 2^64 probes, so neither can the parts of one.
                inherited += share;
                break;
            }
        }
        if let Some((key, past)) = next {
            lower = if past { Excluded(key) } else { Included(key) };
        }
    }
    Ok(inherited)
}

/// Hands each of `outputs` the counts it inherits from `inputs`, the tables a
/// compaction rewrote into them (see [`inherited_counts`]).
pub fn inherit_into(
    outputs: &[crate::table::Table],
    inputs: &[crate::table::Table],
) -> crate::Result<()> {
    for output in outputs {
        if let Some(stats) = output.probe_stats() {
            let range = &output.metadata.key_range;
            stats.add(inherited_counts(range.min(), range.max(), inputs)?);
        }
    }
    Ok(())
}

/// The fraction of the data `table`'s view serves that lies inside `bounds`,
/// each data block going to the range holding its last key.
pub fn fraction_of(
    table: &crate::table::Table,
    bounds: (core::ops::Bound<&[u8]>, core::ops::Bound<&[u8]>),
) -> crate::Result<f64> {
    let Some(span) =
        table.data_span(bounds, crate::SeqNo::MAX, crate::table::SpanEdge::ByLastKey)?
    else {
        return Ok(0.0);
    };
    let live = span.data_end - span.live_start;
    if live == 0 {
        return Ok(0.0);
    }
    #[expect(
        clippy::cast_precision_loss,
        reason = "a fraction of two byte offsets; the digits lost past 2^53 do not matter"
    )]
    let fraction = span.covered as f64 / live as f64;
    Ok(fraction)
}

/// The share of `table`'s counts its data inside `bounds` holds, by data
/// block offsets over the part of the table its view serves.
// Called by the restricted view a tight-space slice opens, which is std-only.
#[cfg(feature = "std")]
pub fn share_of(
    table: &crate::table::Table,
    bounds: (core::ops::Bound<&[u8]>, core::ops::Bound<&[u8]>),
) -> crate::Result<ProbeCounts> {
    if table.probe_stats().is_none() {
        return Ok(ProbeCounts::default());
    }
    Ok(region_share(table, bounds)?.unwrap_or_default())
}

/// [`share_of`], or `None` when `table`'s view holds no data block inside
/// `bounds`: its counts say nothing of that part of the key space.
fn region_share(
    table: &crate::table::Table,
    bounds: (core::ops::Bound<&[u8]>, core::ops::Bound<&[u8]>),
) -> crate::Result<Option<ProbeCounts>> {
    // Each block goes to the range holding its last key, so the outputs of
    // one compaction share an input's counts without counting any block twice.
    let Some(span) =
        table.data_span(bounds, crate::SeqNo::MAX, crate::table::SpanEdge::ByLastKey)?
    else {
        return Ok(None);
    };
    let live = span.data_end - span.live_start;
    if live == 0 || span.covered == 0 {
        return Ok(None);
    }
    let Some(stats) = table.probe_stats() else {
        return Ok(Some(ProbeCounts::default()));
    };
    // `covered <= live`, so a share is at most the table's count.
    let share = |count: u64| {
        #[expect(
            clippy::cast_possible_truncation,
            reason = "covered <= live, so the quotient never exceeds the u64 count"
        )]
        let share = (u128::from(count) * u128::from(span.covered) / u128::from(live)) as u64;
        share
    };
    Ok(Some(ProbeCounts {
        probes: share(stats.probes()),
        negatives: share(stats.negatives()),
    }))
}

#[cfg(test)]
mod tests;

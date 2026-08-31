// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Merging iterator over `MergeSource`s, backed by two independent
//! `LoserTree` tournaments (forward min, backward max).
//!
//! `DoubleEndedIterator` is gated on the [`CoherentMergeSource`]
//! marker — only sources that promise "no item yielded twice"
//! under mixed forward/backward consumption can call `next_back`.
//! The merger does NOT call `MergeSource::seek` on direction
//! switches; the marker's promise is what keeps mixed direction
//! safe. Two source families satisfy the marker:
//!
//! - **Coherent sources** (`alloc::vec::IntoIter`,
//!   `alloc::collections::VecDeque`, range iterators from
//!   `alloc::collections::BTreeMap`, wrapped via
//!   `CoherentIterSource`) where the cursor state is literally
//!   shared between the two methods.
//! - **Self-coordinating index-window sources** that track a
//!   `(front_idx, back_idx)` window internally and refuse to
//!   yield once `front_idx >= back_idx`. SST scanners and
//!   `RunReader`-style impls qualify ONLY when they actually
//!   enforce that single shrinking window — an impl with two
//!   genuinely independent cursors must NOT mark itself coherent
//!   even if it happens to behave correctly on some workloads.
//!
//! Sources whose mixed direction would yield duplicates (truly
//! independent cursors with no shared state and no window guard)
//! must NOT implement [`CoherentMergeSource`]; the marker bound
//! keeps them out of `next_back` at compile time. Such sources
//! are still usable through `Iterator::next` only.
//!
//! `MergeSource::seek` is exposed for user-initiated
//! repositioning, not as a direction-switch reconciliation
//! primitive. Wiring it into the merger's switch path would
//! defeat the buffered-value migration in `initialize_forward` /
//! `initialize_backward` and pre-emptively shift cursors before
//! the destination tournament populates its edge leaves.
//!
//! The eager per-direction init below builds each tournament from
//! a pre-populated `Vec<Option<MergerEntry>>` on first use, lazy
//! per direction, independent of the other (mirrors the legacy
//! `MergeHeap` `init_lo` / `init_hi` pattern).

use crate::comparator::UserComparator;
use crate::loser_tree::LoserTree;
use crate::merge_source::{CoherentMergeSource, IterItem, MergeSource};
use alloc::vec::Vec;
use core::cmp::Ordering;

/// One leaf of the merge tournament. Carries the source index
/// alongside the value so the refill path knows which iterator to
/// pull from.
struct MergerEntry {
    source: usize,
    value: crate::InternalValue,
}

/// Forward-direction comparator passed to the min-tree.
///
/// Generic over the concrete `UserComparator` type `C` so the
/// `EntryComparator<MergerEntry>` impl monomorphises through
/// `LoserTree<MergerEntry, MinCmp<C>>` at every use site — no
/// `Arc<dyn UserComparator>` vtable lookup on the per-cmp path
/// inside `InternalKey::compare_with`. `C: Clone` is required
/// because each direction's tree owns its own copy of the
/// comparator (cheap for the common cases:
/// `DefaultUserComparator` is a unit struct, custom user
/// comparators are typically wrapper types over a few fields).
///
/// Splitting `MinCmp` / `MaxCmp` removes the per-cmp direction
/// branch a single combined struct would have carried
/// (direction is a compile-time fact, not a runtime field).
///
/// Smaller key wins; ties broken by lower source index.
#[derive(Clone)]
struct MinCmp<C: UserComparator + Clone> {
    comparator: C,
}

impl<C: UserComparator + Clone> crate::loser_tree::EntryComparator<MergerEntry> for MinCmp<C> {
    #[expect(
        clippy::inline_always,
        reason = "called O(log cap) per replay step on the merger's hot path; \
                  matching the loser-tree's own #[inline(always)] on cmp_indices \
                  is what makes the dispatch flatten — verified in disassembly"
    )]
    #[inline(always)]
    fn compare(&self, a: &MergerEntry, b: &MergerEntry) -> Ordering {
        a.value
            .key
            .compare_with(&b.value.key, &self.comparator)
            .then_with(|| a.source.cmp(&b.source))
    }
}

/// Backward-direction comparator passed to the max-tree.
///
/// Reversed key comparison + reversed source-order tiebreak.
/// The key reversal elects the LARGEST key as winner under the
/// loser tree's "smaller wins" semantics. The source-index
/// reversal keeps `next_back()`'s tie-break opposite of
/// `next()`'s, matching the legacy `MergeHeap` behaviour.
#[derive(Clone)]
struct MaxCmp<C: UserComparator + Clone> {
    comparator: C,
}

impl<C: UserComparator + Clone> crate::loser_tree::EntryComparator<MergerEntry> for MaxCmp<C> {
    #[expect(
        clippy::inline_always,
        reason = "called O(log cap) per replay step on the merger's hot path"
    )]
    #[inline(always)]
    fn compare(&self, a: &MergerEntry, b: &MergerEntry) -> Ordering {
        b.value
            .key
            .compare_with(&a.value.key, &self.comparator)
            .then_with(|| b.source.cmp(&a.source))
    }
}

fn build_min_cmp<C: UserComparator + Clone>(comparator: C) -> MinCmp<C> {
    MinCmp { comparator }
}

fn build_max_cmp<C: UserComparator + Clone>(comparator: C) -> MaxCmp<C> {
    MaxCmp { comparator }
}

/// The slot whose leaf is parked in `other` and now belongs to `active`, if it
/// beats what `active` holds itself.
///
/// A slot with no leaf in `active` but one in `other` is a source that has run
/// out in the active direction while the opposite tournament still buffers its
/// head. That head is the source's next item HERE, so it competes for the
/// winner: returning it only when it sorts ahead of the active tournament's own
/// winner is what keeps the stream sorted without stealing an item the opposite
/// end is still walking toward. With `active` empty, any parked head wins,
/// which is how the two directions hand over the last items of a range.
///
/// Memoized result of the parked-candidate scan in [`pick_parked`].
#[derive(Clone, Copy)]
enum ParkedCache {
    /// The parked set may have changed since the last scan; recompute.
    Stale,
    /// Scanned: no vacated slot currently holds a parked leaf.
    Empty,
    /// Scanned: this slot's parked leaf sorts first in the active direction.
    Best(usize),
}

/// `vacated` is the merger-maintained list of slots emptied in `active` since
/// its last (re)initialization — the only slots that can hold a parked leaf.
/// `cache` memoizes the scan's best candidate: the parked set changes only at
/// discrete events (a slot vacates, the opposite direction steps, a claim, a
/// full refill), all of which reset the cache to [`ParkedCache::Stale`], so a
/// long drain in
/// one direction pays the O(exhausted) scan once and an O(1) comparison per
/// item after — not the scan per item. An ORDERED structure over the parked
/// heads would not do better: it only pays off when the scan repeats, and the
/// scan repeats only under alternation, where the opposite direction's every
/// step can change a parked head and would invalidate that structure too.
///
/// `cmp` is the active direction's ordering, so "ahead" reads the same way in
/// both: `Less` under the min-tree, `Less` under the reversed max-tree.
fn pick_parked<A, O, C>(
    active: &LoserTree<MergerEntry, A>,
    other: Option<&LoserTree<MergerEntry, O>>,
    vacated: &[usize],
    cache: &mut ParkedCache,
    cmp: &C,
) -> Option<usize>
where
    A: crate::loser_tree::EntryComparator<MergerEntry>,
    O: crate::loser_tree::EntryComparator<MergerEntry>,
    C: crate::loser_tree::EntryComparator<MergerEntry>,
{
    let other = other?;
    if vacated.is_empty() || other.is_empty() {
        return None;
    }

    // A cached candidate must still be parked; every event that can move or
    // consume one resets the cache, so a vanished head means a missed
    // invalidation. Recompute rather than trust it.
    let best_slot = match *cache {
        ParkedCache::Best(slot) if other.peek_slot(slot).is_some() => Some(slot),
        ParkedCache::Empty => None,
        ParkedCache::Stale | ParkedCache::Best(_) => {
            let mut best: Option<(usize, &MergerEntry)> = None;
            for &slot in vacated {
                // A vacated slot stays empty in `active` until the next full
                // (re)initialization, which rebuilds this list.
                debug_assert!(
                    active.peek_slot(slot).is_none(),
                    "vacated slot holds a leaf in the active tournament"
                );
                // Nothing parked for it RIGHT NOW; the opposite direction may
                // still buffer one later (its next full refill), so the entry
                // stays listed.
                let Some(parked) = other.peek_slot(slot) else {
                    continue;
                };
                if best.is_none_or(|(_, current)| cmp.compare(parked, current) == Ordering::Less) {
                    best = Some((slot, parked));
                }
            }
            let computed = best.map(|(slot, _)| slot);
            *cache = computed.map_or(ParkedCache::Empty, ParkedCache::Best);
            computed
        }
    };

    let slot = best_slot?;
    let parked = other.peek_slot(slot)?;
    let winner = active.winner_slot().and_then(|slot| active.peek_slot(slot));
    match winner {
        // Ahead of what this tournament would emit next, so it goes first.
        Some(w) => (cmp.compare(parked, w) == Ordering::Less).then_some(slot),
        // Nothing of our own left: the parked heads are the remainder.
        None => Some(slot),
    }
}

/// Merging iterator over `MergeSource`s, backed by two independent
/// `LoserTree` tournaments.
///
/// Uses one min-tree for forward iteration and one max-tree (via a
/// reversed comparator) for backward iteration. Each tree holds `n`
/// leaves — one prefetched item per source per direction. Forward
/// `next()` and backward `next_back()` each stay O(log n) per step.
///
/// # Direction-switching
///
/// `DoubleEndedIterator` is gated on the [`CoherentMergeSource`]
/// marker, which promises "no duplicates under mixed direction".
/// Both literally-shared-cursor sources (`CoherentIterSource`)
/// and self-coordinating index-window sources qualify. The merger
/// does NOT invoke `MergeSource::seek` on direction switches —
/// the marker's promise is what keeps mixed direction safe (see
/// module-level docs for rationale).
pub struct SeekingMerger<S: MergeSource, C: UserComparator + Clone> {
    sources: Vec<S>,
    n_sources: usize,
    comparator: C,
    forward_tree: Option<LoserTree<MergerEntry, MinCmp<C>>>,
    backward_tree: Option<LoserTree<MergerEntry, MaxCmp<C>>>,
    /// Refill error queued during the previous forward step. When
    /// a source's `.next()` fails AFTER a value was already buffered
    /// in the tournament, we yield the buffered value first and stash
    /// the error here so the NEXT call to [`Iterator::next`] surfaces
    /// it. Avoids silent data loss of the prefetched value.
    pending_forward_error: Option<crate::Error>,
    /// Mirror of `pending_forward_error` for the backward direction.
    pending_backward_error: Option<crate::Error>,
    /// Whether `forward_tree` reflects the current position. `false` means the
    /// next forward pull must (re)populate it: build it the first time, or
    /// refill the retained storage in place after a reseek. Decoupled from
    /// `forward_tree.is_some()` so a reseek can keep the allocation while
    /// forcing a repopulate.
    forward_primed: bool,
    /// Mirror of `forward_primed` for the backward direction.
    backward_primed: bool,
    /// Slots emptied in `forward_tree` without a refill since its last full
    /// (re)initialization: source exhausted or errored forward, or the leaf
    /// taken by the backward direction. Only these can hold a parked leaf, so
    /// `pick_parked` scans this list instead of every slot. Rebuilt on each
    /// forward (re)initialization; entries are never removed in between — a
    /// vacated slot cannot refill outside a full init.
    forward_vacated: Vec<usize>,
    /// Mirror of `forward_vacated` for `backward_tree`.
    backward_vacated: Vec<usize>,
    /// Memoized best parked candidate for the forward direction (a slot in
    /// `forward_vacated` whose backward leaf sorts first under the min order).
    /// Reset to [`ParkedCache::Stale`] by every event that can change the
    /// parked set: a forward vacate, ANY backward step (it can move a parked
    /// head), a claim, a full (re)initialization, a reseek.
    forward_parked_best: ParkedCache,
    /// Mirror of `forward_parked_best` for the backward direction.
    backward_parked_best: ParkedCache,
}

impl<S: MergeSource, C: UserComparator + Clone> SeekingMerger<S, C> {
    /// Construct an empty merger ready to consume from `sources`.
    /// Tournaments are not built yet; both directions populate
    /// lazily on first use.
    #[must_use]
    pub fn new(sources: Vec<S>, comparator: C) -> Self {
        let n = sources.len();
        Self {
            sources,
            n_sources: n,
            comparator,
            forward_tree: None,
            backward_tree: None,
            pending_forward_error: None,
            pending_backward_error: None,
            forward_primed: false,
            backward_primed: false,
            forward_vacated: Vec::new(),
            backward_vacated: Vec::new(),
            forward_parked_best: ParkedCache::Stale,
            backward_parked_best: ParkedCache::Stale,
        }
    }

    fn initialize_forward(&mut self) {
        let Self {
            sources,
            n_sources,
            comparator,
            forward_tree,
            backward_tree,
            pending_forward_error,
            forward_vacated,
            backward_vacated,
            forward_parked_best,
            backward_parked_best,
            ..
        } = self;
        let n = *n_sources;
        // Pull the new forward head for slot `i`. On error, keep earlier
        // prefetches and queue the FIRST error (surfaces after buffered values).
        // On forward-exhaustion, MIGRATE any leaf the OPPOSITE tournament still
        // buffers for this source so it is not silently lost. After a reseek
        // both tournaments are cleared, so the migrate path finds nothing —
        // exactly the fresh-position behaviour.
        let mut pull = |i: usize| -> Option<MergerEntry> {
            #[expect(
                clippy::indexing_slicing,
                reason = "i < n_sources by construction; sources len == n_sources"
            )]
            match MergeSource::next(&mut sources[i]) {
                Some(Ok(value)) => Some(MergerEntry { source: i, value }),
                Some(Err(e)) => {
                    pending_forward_error.get_or_insert(e);
                    None
                }
                None => {
                    let migrated = backward_tree.as_mut().and_then(|bt| bt.take_slot(i));
                    if migrated.is_some() {
                        // The take emptied the backward slot without a refill.
                        backward_vacated.push(i);
                    }
                    migrated
                }
            }
        };
        let tree = if let Some(tree) = forward_tree {
            // Retained storage (post-reseek): refill in place, no allocation.
            tree.refill_with(&mut pull);
            tree
        } else {
            // First use: build the tournament (one-time allocation).
            let mut initial: Vec<Option<MergerEntry>> = Vec::with_capacity(n);
            for i in 0..n {
                initial.push(pull(i));
            }
            let cmp = build_min_cmp(comparator.clone());
            forward_tree.insert(LoserTree::build(initial, cmp))
        };
        // A full (re)initialization re-derives which slots stand empty; between
        // inits the list only grows at the vacate sites. Both parked caches are
        // stale: this rebuilt the forward candidates, and the migrate path may
        // have taken backward leaves.
        forward_vacated.clear();
        forward_vacated.extend((0..n).filter(|&i| tree.peek_slot(i).is_none()));
        *forward_parked_best = ParkedCache::Stale;
        *backward_parked_best = ParkedCache::Stale;
    }

    fn initialize_backward(&mut self) {
        let Self {
            sources,
            n_sources,
            comparator,
            forward_tree,
            backward_tree,
            pending_backward_error,
            forward_vacated,
            backward_vacated,
            forward_parked_best,
            backward_parked_best,
            ..
        } = self;
        let n = *n_sources;
        // Mirror of `initialize_forward`: pull the new backward head, queue the
        // first error, and migrate a buffered leaf from the forward tournament
        // for a source already exhausted backward (the single-item-window case).
        let mut pull = |i: usize| -> Option<MergerEntry> {
            #[expect(
                clippy::indexing_slicing,
                reason = "i < n_sources by construction; sources len == n_sources"
            )]
            match MergeSource::next_back(&mut sources[i]) {
                Some(Ok(value)) => Some(MergerEntry { source: i, value }),
                Some(Err(e)) => {
                    pending_backward_error.get_or_insert(e);
                    None
                }
                None => {
                    let migrated = forward_tree.as_mut().and_then(|ft| ft.take_slot(i));
                    if migrated.is_some() {
                        // The take emptied the forward slot without a refill.
                        forward_vacated.push(i);
                    }
                    migrated
                }
            }
        };
        let tree = if let Some(tree) = backward_tree {
            tree.refill_with(&mut pull);
            tree
        } else {
            let mut initial: Vec<Option<MergerEntry>> = Vec::with_capacity(n);
            for i in 0..n {
                initial.push(pull(i));
            }
            let cmp = build_max_cmp(comparator.clone());
            backward_tree.insert(LoserTree::build(initial, cmp))
        };
        backward_vacated.clear();
        backward_vacated.extend((0..n).filter(|&i| tree.peek_slot(i).is_none()));
        *forward_parked_best = ParkedCache::Stale;
        *backward_parked_best = ParkedCache::Stale;
    }
}

impl<S: MergeSource + crate::reseek::Reseekable, C: UserComparator + Clone>
    crate::reseek::Reseekable for SeekingMerger<S, C>
{
    /// Reposition every source to the new sub-range, then empty both tournaments
    /// IN PLACE (dropping stale buffered leaves but keeping their storage) and
    /// unprime them so the next pull refills the retained storage — no
    /// reallocation. Clearing both before any re-init also means the migrate
    /// path in `initialize_*` finds nothing stale to rescue. Queued refill
    /// errors are dropped; they belonged to the old position.
    fn reseek(&mut self, ctx: &crate::reseek::ReseekCtx) {
        for source in &mut self.sources {
            source.reseek(ctx);
        }
        if let Some(tree) = &mut self.forward_tree {
            tree.clear();
        }
        if let Some(tree) = &mut self.backward_tree {
            tree.clear();
        }
        self.forward_primed = false;
        self.backward_primed = false;
        self.forward_vacated.clear();
        self.backward_vacated.clear();
        self.forward_parked_best = ParkedCache::Stale;
        self.backward_parked_best = ParkedCache::Stale;
        self.pending_forward_error = None;
        self.pending_backward_error = None;
    }
}

impl<S: MergeSource, C: UserComparator + Clone> Iterator for SeekingMerger<S, C> {
    type Item = IterItem;

    fn next(&mut self) -> Option<Self::Item> {
        // Surface ANY queued error (this direction's OR the opposite
        // direction's) at the top of every call. Errors are signals
        // and should NOT be buffered behind unrelated yielded items
        // from other sources — that hides I/O / corruption failures
        // until the end of iteration. Cold path: two `Option::take`
        // calls are near-free when both are None.
        if let Some(e) = self.pending_forward_error.take() {
            return Some(Err(e));
        }
        if let Some(e) = self.pending_backward_error.take() {
            return Some(Err(e));
        }
        if !self.forward_primed {
            self.initialize_forward();
            self.forward_primed = true;
            // Init may have queued an error; surface it immediately
            // (same rationale as the top check — errors first).
            if let Some(e) = self.pending_forward_error.take() {
                return Some(Err(e));
            }
        }
        // Single mut borrow per tree field — split-borrow with
        // self.sources is OK because they're disjoint struct fields.
        let Self {
            sources,
            forward_tree,
            backward_tree,
            pending_forward_error,
            comparator,
            forward_vacated,
            backward_vacated,
            forward_parked_best,
            backward_parked_best,
            ..
        } = self;
        let tree = forward_tree.as_mut()?;
        // Every arm below mutates the forward tree, and any forward head can
        // be a backward-vacated slot's parked candidate.
        *backward_parked_best = ParkedCache::Stale;
        // winner_slot returns the source index directly (by
        // construction every MergerEntry's `source` field equals
        // its slot index). Avoids a separate peek_min call. None
        // means the tournament is exhausted.
        // A source that is drained forward may still have its head parked in
        // the BACKWARD tournament, and that head is the source's next item in
        // this direction. It has to compete for the winner right away: waiting
        // for this tournament to empty would emit it after values that sort
        // above it, and the pipeline above depends on a sorted stream.
        //
        // It cannot be claimed the moment its source runs dry, either. The
        // backward walk may still be stepping toward it, and taking it then
        // yields it from both ends. Ordering is what decides: the parked head
        // belongs to whichever end reaches it first, and comparing it against
        // this tournament's own winner is that question asked precisely.
        if let Some(slot) = pick_parked(
            tree,
            backward_tree.as_ref(),
            forward_vacated,
            forward_parked_best,
            &build_min_cmp(comparator.clone()),
        ) && let Some(entry) = backward_tree.as_mut().and_then(|bt| bt.take_slot(slot))
        {
            // The claim emptied the backward slot without a refill and
            // consumed the cached candidate.
            backward_vacated.push(slot);
            *forward_parked_best = ParkedCache::Stale;
            return Some(Ok(entry.value));
        }
        let source = tree.winner_slot()?;
        #[expect(
            clippy::indexing_slicing,
            reason = "source index < n_sources by construction"
        )]
        let next_pull = MergeSource::next(&mut sources[source]);
        match next_pull {
            Some(Ok(next_value)) => {
                let old = tree.replace_min(MergerEntry {
                    source,
                    value: next_value,
                });
                Some(Ok(old.value))
            }
            Some(Err(e)) => {
                // Refill failed AFTER the slot's prefetched value
                // was already in the tournament. Yield that value
                // and queue the error for the next call.
                // get_or_insert preserves the FIRST queued error
                // (matches the init-path semantic).
                let old = tree.pop_min()?;
                pending_forward_error.get_or_insert(e);
                forward_vacated.push(source);
                *forward_parked_best = ParkedCache::Stale;
                Some(Ok(old.value))
            }
            None => {
                // Source exhausted forward: drop its slot. Anything the
                // backward tournament buffers for it is picked up by
                // `pick_parked` above, on the step where it sorts ahead of
                // this tournament's own winner.
                let old = tree.pop_min()?;
                forward_vacated.push(source);
                *forward_parked_best = ParkedCache::Stale;
                Some(Ok(old.value))
            }
        }
    }
}

// `DoubleEndedIterator` is gated on `CoherentMergeSource`. The
// marker's no-duplicates-under-mixed-direction promise is what
// makes backward iteration safe given that the merger does NOT
// invoke `MergeSource::seek` at direction switches. The marker
// now spans both literally-shared-cursor sources
// (`CoherentIterSource`) and self-coordinating index-window
// sources (LSM SST scanners maintaining `(front_idx, back_idx)`).
// Sources that don't satisfy the promise can still be used via
// `Iterator::next()` only.
impl<S: CoherentMergeSource, C: UserComparator + Clone> DoubleEndedIterator
    for SeekingMerger<S, C>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        // Same error-first contract as next().
        if let Some(e) = self.pending_backward_error.take() {
            return Some(Err(e));
        }
        if let Some(e) = self.pending_forward_error.take() {
            return Some(Err(e));
        }
        if !self.backward_primed {
            self.initialize_backward();
            self.backward_primed = true;
            if let Some(e) = self.pending_backward_error.take() {
                return Some(Err(e));
            }
        }
        // Split-borrow, as in `next()`: the two tournaments and the sources are
        // disjoint fields, and the exhausted-source arm below needs all three.
        let Self {
            sources,
            forward_tree,
            backward_tree,
            pending_backward_error,
            comparator,
            forward_vacated,
            backward_vacated,
            forward_parked_best,
            backward_parked_best,
            ..
        } = self;
        let tree = backward_tree.as_mut()?;
        // Every arm below mutates the backward tree, and any backward head can
        // be a forward-vacated slot's parked candidate.
        *forward_parked_best = ParkedCache::Stale;
        // Mirror of `next()`: a head parked in the FORWARD tournament competes
        // for this end's winner as soon as its source is drained backward.
        if let Some(slot) = pick_parked(
            tree,
            forward_tree.as_ref(),
            backward_vacated,
            backward_parked_best,
            &build_max_cmp(comparator.clone()),
        ) && let Some(entry) = forward_tree.as_mut().and_then(|ft| ft.take_slot(slot))
        {
            // The claim emptied the forward slot without a refill and
            // consumed the cached candidate.
            forward_vacated.push(slot);
            *backward_parked_best = ParkedCache::Stale;
            return Some(Ok(entry.value));
        }
        let source = tree.winner_slot()?;
        #[expect(
            clippy::indexing_slicing,
            reason = "source index < n_sources by construction"
        )]
        let next_pull = MergeSource::next_back(&mut sources[source]);
        match next_pull {
            Some(Ok(next_value)) => {
                let old = tree.replace_min(MergerEntry {
                    source,
                    value: next_value,
                });
                Some(Ok(old.value))
            }
            Some(Err(e)) => {
                let old = tree.pop_min()?;
                pending_backward_error.get_or_insert(e);
                backward_vacated.push(source);
                *backward_parked_best = ParkedCache::Stale;
                Some(Ok(old.value))
            }
            None => {
                let old = tree.pop_min()?;
                backward_vacated.push(source);
                *backward_parked_best = ParkedCache::Stale;
                Some(Ok(old.value))
            }
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test assertions")]
mod tests;

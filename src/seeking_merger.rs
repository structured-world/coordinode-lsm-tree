// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Merging iterator over `MergeSource`s, backed by two independent
//! `LoserTree` tournaments (forward min, backward max).
//!
//! `DoubleEndedIterator` is bounded on `MergeSource`. The merger
//! does NOT call `MergeSource::seek` on direction switches — its
//! two-tree architecture relies on each source's `next` and
//! `next_back` self-coordinating so that they together shrink a
//! single remaining range from both ends. Two source families
//! satisfy this:
//!
//! - **Coherent sources** (std `vec::IntoIter`, `VecDeque`,
//!   `BTreeMap::range`, wrapped via `CoherentIterSource`) where
//!   the cursor state is literally shared between the two methods.
//! - **Self-coordinating independent-cursor sources** that track
//!   a `(front_idx, back_idx)` window internally and refuse to
//!   yield once `front_idx >= back_idx`. LSM SST scanners and
//!   future `RunReader` impls fit here.
//!
//! Sources with two truly independent cursors and no shared state
//! would emit duplicates under mixed direction — they are not
//! supported. `MergeSource::seek` exists in the trait for
//! user-initiated repositioning (e.g., starting a range scan at a
//! specific key); wiring it into the merger's direction-switch
//! path would defeat the buffered-value migration in
//! `initialize_forward` / `initialize_backward` and pre-emptively
//! shift cursors before the destination tournament populates its
//! edge leaves.
//!
//! The eager per-direction init below builds each tournament from
//! a pre-populated `Vec<Option<MergerEntry>>` on first use, lazy
//! per direction, independent of the other (mirrors the legacy
//! `MergeHeap` `init_lo` / `init_hi` pattern).

use crate::comparator::UserComparator;
use crate::loser_tree::LoserTree;
use crate::merge_source::{IterItem, MergeSource};
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
/// `DoubleEndedIterator` is bounded on `MergeSource`. Mixed
/// `next()` / `next_back()` is safe only when each source
/// self-coordinates between its forward and backward halves —
/// either through shared cursor state (coherent sources, wrapped
/// via `CoherentIterSource`) or through an internal
/// `(front_idx, back_idx)` window that refuses to yield once the
/// remaining range is empty. The merger does NOT invoke
/// `MergeSource::seek` on direction switches (see module-level
/// docs for rationale).
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
        }
    }

    fn initialize_forward(&mut self) {
        let mut initial: Vec<Option<MergerEntry>> = Vec::with_capacity(self.n_sources);
        for i in 0..self.n_sources {
            #[expect(
                clippy::indexing_slicing,
                reason = "i < n_sources by construction; sources len == n_sources"
            )]
            let pulled = MergeSource::next(&mut self.sources[i]);
            let slot = match pulled {
                Some(Ok(value)) => Some(MergerEntry { source: i, value }),
                Some(Err(e)) => {
                    // Don't drop earlier prefetched values: keep the
                    // erroring slot empty and queue the error so it
                    // surfaces AFTER any buffered prefetches are
                    // consumed. Same contract as the refill paths.
                    // get_or_insert preserves the FIRST queued error.
                    self.pending_forward_error.get_or_insert(e);
                    None
                }
                None => {
                    // Source exhausted forward. If backward_tree
                    // still holds a buffered leaf for this source
                    // (the OPPOSITE direction pulled it earlier and
                    // hasn't yielded it yet), MIGRATE it into the
                    // forward tournament — otherwise that value
                    // would be silently lost when the user iterates
                    // forward after some backward consumption.
                    self.backward_tree.as_mut().and_then(|bt| bt.take_slot(i))
                }
            };
            initial.push(slot);
        }
        let cmp = build_min_cmp(self.comparator.clone());
        self.forward_tree = Some(LoserTree::build(initial, cmp));
    }

    fn initialize_backward(&mut self) {
        let mut initial: Vec<Option<MergerEntry>> = Vec::with_capacity(self.n_sources);
        for i in 0..self.n_sources {
            #[expect(
                clippy::indexing_slicing,
                reason = "i < n_sources by construction; sources len == n_sources"
            )]
            let pulled = MergeSource::next_back(&mut self.sources[i]);
            let slot = match pulled {
                Some(Ok(value)) => Some(MergerEntry { source: i, value }),
                Some(Err(e)) => {
                    // Mirror of initialize_forward: keep prefetched
                    // backward values, queue the FIRST error for a
                    // later call (get_or_insert preserves it).
                    self.pending_backward_error.get_or_insert(e);
                    None
                }
                None => {
                    // Source exhausted backward. If forward_tree
                    // still holds a buffered leaf for this source
                    // (the OPPOSITE direction pulled it earlier and
                    // hasn't yielded it yet), MIGRATE it into the
                    // backward tournament. CodeRabbit's data-loss
                    // bug (single-source `[a, z]` returning None on
                    // post-forward next_back) is exactly this case.
                    self.forward_tree.as_mut().and_then(|ft| ft.take_slot(i))
                }
            };
            initial.push(slot);
        }
        let cmp = build_max_cmp(self.comparator.clone());
        self.backward_tree = Some(LoserTree::build(initial, cmp));
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
        if self.forward_tree.is_none() {
            self.initialize_forward();
            // Init may have queued an error; surface it immediately
            // (same rationale as the top check — errors first).
            if let Some(e) = self.pending_forward_error.take() {
                return Some(Err(e));
            }
        }
        // Single mut borrow on the tree field — split-borrow with
        // self.sources is OK because they're disjoint struct fields.
        let tree = self.forward_tree.as_mut()?;
        // winner_slot returns the source index directly (by
        // construction every MergerEntry's `source` field equals
        // its slot index). Avoids a separate peek_min call. None
        // means the tournament is exhausted.
        let source = tree.winner_slot()?;
        #[expect(
            clippy::indexing_slicing,
            reason = "source index < n_sources by construction"
        )]
        let next_pull = MergeSource::next(&mut self.sources[source]);
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
                self.pending_forward_error.get_or_insert(e);
                Some(Ok(old.value))
            }
            None => {
                // Source exhausted forward. Do NOT migrate from
                // backward_tree here — the buffered backward value
                // still legitimately belongs to backward direction.
                // Migration only happens at INIT.
                let old = tree.pop_min()?;
                Some(Ok(old.value))
            }
        }
    }
}

// `DoubleEndedIterator` is bounded on `MergeSource`. The previous
// `CoherentMergeSource` bound was relaxed because self-coordinating
// independent-cursor sources (e.g., LSM SST scanners that maintain
// a `(front_idx, back_idx)` window internally) satisfy the
// merger's "no duplicates under mixed direction" requirement
// without implementing the `CoherentMergeSource` marker. See
// module-level docs for the full source-shape contract.
impl<S: MergeSource, C: UserComparator + Clone> DoubleEndedIterator for SeekingMerger<S, C> {
    fn next_back(&mut self) -> Option<Self::Item> {
        // Same error-first contract as next().
        if let Some(e) = self.pending_backward_error.take() {
            return Some(Err(e));
        }
        if let Some(e) = self.pending_forward_error.take() {
            return Some(Err(e));
        }
        if self.backward_tree.is_none() {
            self.initialize_backward();
            if let Some(e) = self.pending_backward_error.take() {
                return Some(Err(e));
            }
        }
        let tree = self.backward_tree.as_mut()?;
        let source = tree.winner_slot()?;
        #[expect(
            clippy::indexing_slicing,
            reason = "source index < n_sources by construction"
        )]
        let next_pull = MergeSource::next_back(&mut self.sources[source]);
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
                self.pending_backward_error.get_or_insert(e);
                Some(Ok(old.value))
            }
            None => {
                let old = tree.pop_min()?;
                Some(Ok(old.value))
            }
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test assertions")]
mod tests {
    use super::*;
    use crate::InternalValue;
    use crate::ValueType::Value;
    use crate::comparator::{self, SharedComparator};
    use crate::key::InternalKey;
    use crate::merge_source::CoherentMergeSource;
    use alloc::collections::VecDeque;
    use test_log::test;

    /// Trivial `MergeSource` over a `VecDeque<InternalValue>` for
    /// unit tests. `next` pops from the front, `next_back` from the
    /// back, `seek` is a linear skip-while-front-is-less primitive.
    struct VecSource {
        items: VecDeque<InternalValue>,
        comparator: SharedComparator,
    }

    impl VecSource {
        fn new<I: IntoIterator<Item = InternalValue>>(
            items: I,
            comparator: SharedComparator,
        ) -> Self {
            Self {
                items: items.into_iter().collect(),
                comparator,
            }
        }
    }

    impl MergeSource for VecSource {
        fn next(&mut self) -> Option<IterItem> {
            self.items.pop_front().map(Ok)
        }

        fn next_back(&mut self) -> Option<IterItem> {
            self.items.pop_back().map(Ok)
        }

        fn seek(&mut self, target: &InternalKey) -> crate::Result<()> {
            // `VecSource` is a `CoherentMergeSource` (shared
            // front/back cursors via `VecDeque`), so seek is a hint
            // that the trait permits to be a no-op for this class
            // of source — the cursor discipline already enforces
            // "no item yielded twice" under mixed direction. We
            // still drop items strictly less than target from the
            // front as a courtesy (matches what a partially-seek-
            // aware implementation would do), since a single
            // coherent queue cannot simultaneously satisfy both
            // halves of the contract without re-ordering and
            // breaking the merger's monotonic-yield expectation.
            while let Some(front) = self.items.front() {
                if front.key.compare_with(target, self.comparator.as_ref()) == Ordering::Less {
                    self.items.pop_front();
                } else {
                    break;
                }
            }
            Ok(())
        }
    }
    impl CoherentMergeSource for VecSource {}

    fn make_iv(key: &[u8], seqno: u64) -> InternalValue {
        InternalValue::from_components(key, b"", seqno, Value)
    }

    fn k(v: &InternalValue) -> String {
        String::from_utf8_lossy(&v.key.user_key).to_string()
    }

    #[test]
    fn forward_only() {
        let cmp = comparator::default_comparator();
        let a = VecSource::new([make_iv(b"a", 0), make_iv(b"c", 0)], cmp.clone());
        let b = VecSource::new([make_iv(b"b", 0), make_iv(b"d", 0)], cmp.clone());
        let mut m = SeekingMerger::new(alloc::vec![a, b], cmp);
        let keys: Vec<String> = (&mut m).map(|r| k(&r.unwrap())).collect();
        assert_eq!(keys, ["a", "b", "c", "d"]);
    }

    #[test]
    fn backward_only() {
        let cmp = comparator::default_comparator();
        let a = VecSource::new([make_iv(b"a", 0), make_iv(b"c", 0)], cmp.clone());
        let b = VecSource::new([make_iv(b"b", 0), make_iv(b"d", 0)], cmp.clone());
        let mut iter = SeekingMerger::new(alloc::vec![a, b], cmp);
        let mut keys: Vec<String> = Vec::new();
        while let Some(item) = iter.next_back() {
            keys.push(k(&item.unwrap()));
        }
        assert_eq!(keys, ["d", "c", "b", "a"]);
    }

    #[test]
    fn mixed_direction() {
        // Sources have shared front/back cursors (VecDeque), so
        // forward + backward interleave correctly without seek.
        let cmp = comparator::default_comparator();
        let a = VecSource::new(
            [make_iv(b"a", 0), make_iv(b"c", 0), make_iv(b"e", 0)],
            cmp.clone(),
        );
        let b = VecSource::new(
            [make_iv(b"b", 0), make_iv(b"d", 0), make_iv(b"f", 0)],
            cmp.clone(),
        );
        let mut m = SeekingMerger::new(alloc::vec![a, b], cmp);
        assert_eq!(k(&m.next().unwrap().unwrap()), "a");
        assert_eq!(k(&m.next_back().unwrap().unwrap()), "f");
        assert_eq!(k(&m.next().unwrap().unwrap()), "b");
        assert_eq!(k(&m.next_back().unwrap().unwrap()), "e");
        assert_eq!(k(&m.next().unwrap().unwrap()), "c");
        assert_eq!(k(&m.next_back().unwrap().unwrap()), "d");
        assert!(m.next().is_none());
        assert!(m.next_back().is_none());
    }

    #[test]
    fn empty_sources() {
        let cmp = comparator::default_comparator();
        let mut m: SeekingMerger<VecSource, _> = SeekingMerger::new(alloc::vec![], cmp);
        assert!(Iterator::next(&mut m).is_none());
        assert!(m.next_back().is_none());
    }

    #[test]
    fn next_back_after_forward_exhausted_migrates_buffered_value() {
        // CodeRabbit thread #38 regression: with one coherent source
        // [a, z], next() yields `a` and prefetches `z` into
        // forward_tree. next_back() initializes from an already-
        // exhausted source — without migration the buffered `z`
        // would be silently lost (init pulls None from src, no
        // value in backward_tree, returns None even though `z` is
        // sitting in forward_tree).
        //
        // The fix is init-time migration: initialize_backward
        // detects the empty source AND a Some leaf in the
        // forward_tree, and takes that leaf into the backward
        // initial vec.
        let cmp = comparator::default_comparator();
        let src = VecSource::new([make_iv(b"a", 0), make_iv(b"z", 0)], cmp.clone());
        let mut m = SeekingMerger::new(alloc::vec![src], cmp);
        assert_eq!(k(&m.next().unwrap().unwrap()), "a");
        assert_eq!(
            k(&m.next_back().unwrap().unwrap()),
            "z",
            "migration must rescue `z` buffered in forward_tree",
        );
        assert!(m.next().is_none());
        assert!(m.next_back().is_none());
    }

    #[test]
    fn next_after_backward_exhausted_migrates_buffered_value() {
        // Mirror of next_back_after_forward_exhausted: backward
        // direction prefetched `a` (after yielding `z`), then
        // user switches to next() with the source already
        // exhausted. initialize_forward must migrate from
        // backward_tree.
        let cmp = comparator::default_comparator();
        let src = VecSource::new([make_iv(b"a", 0), make_iv(b"z", 0)], cmp.clone());
        let mut m = SeekingMerger::new(alloc::vec![src], cmp);
        assert_eq!(k(&m.next_back().unwrap().unwrap()), "z");
        assert_eq!(
            k(&m.next().unwrap().unwrap()),
            "a",
            "migration must rescue `a` buffered in backward_tree",
        );
        assert!(m.next().is_none());
        assert!(m.next_back().is_none());
    }

    #[test]
    fn single_source_drain_both_directions() {
        let cmp = comparator::default_comparator();
        let a = VecSource::new(
            [make_iv(b"a", 0), make_iv(b"b", 0), make_iv(b"c", 0)],
            cmp.clone(),
        );
        let mut m = SeekingMerger::new(alloc::vec![a], cmp);
        assert_eq!(k(&m.next().unwrap().unwrap()), "a");
        assert_eq!(k(&m.next_back().unwrap().unwrap()), "c");
        assert_eq!(k(&m.next().unwrap().unwrap()), "b");
        assert!(m.next().is_none());
        assert!(m.next_back().is_none());
    }

    /// `MergeSource` impl that yields a controlled `Err` on the
    /// first `next()` call. Used to assert error propagation through
    /// the merger.
    struct ErrSource {
        emit_forward_error: bool,
        emit_backward_error: bool,
    }

    impl MergeSource for ErrSource {
        fn next(&mut self) -> Option<IterItem> {
            if self.emit_forward_error {
                self.emit_forward_error = false;
                Some(Err(crate::Error::Unrecoverable))
            } else {
                None
            }
        }
        fn next_back(&mut self) -> Option<IterItem> {
            if self.emit_backward_error {
                self.emit_backward_error = false;
                Some(Err(crate::Error::Unrecoverable))
            } else {
                None
            }
        }
        fn seek(&mut self, _target: &InternalKey) -> crate::Result<()> {
            Ok(())
        }
    }
    impl CoherentMergeSource for ErrSource {}

    #[test]
    fn forward_init_propagates_error() {
        // initialize_forward sees the error during the per-source
        // pull and returns it as the first yielded item.
        let cmp = comparator::default_comparator();
        let a = ErrSource {
            emit_forward_error: true,
            emit_backward_error: false,
        };
        let mut m = SeekingMerger::new(alloc::vec![a], cmp);
        assert!(m.next().unwrap().is_err());
        // Subsequent next() returns None (init done, tournament empty).
        assert!(m.next().is_none());
    }

    #[test]
    fn backward_init_propagates_error() {
        let cmp = comparator::default_comparator();
        let a = ErrSource {
            emit_forward_error: false,
            emit_backward_error: true,
        };
        let mut m = SeekingMerger::new(alloc::vec![a], cmp);
        assert!(m.next_back().unwrap().is_err());
        assert!(m.next_back().is_none());
    }

    #[test]
    fn forward_init_keeps_earlier_prefetched_when_later_source_errs() {
        // Regression for the silent-data-loss case CodeRabbit
        // flagged: before the fix, when sources[i] returned Err
        // during init, sources[0..i]'s already-prefetched values
        // were dropped (early-return discarded the `initial` vec).
        // The fix queues the error AND keeps the prefetched
        // values; the error surfaces on the very next call (errors
        // are signals and surface ASAP, not buffered behind
        // unrelated yields).
        let cmp = comparator::default_comparator();
        let good: Box<dyn CoherentMergeSource> = Box::new(VecSource::new(
            [make_iv(b"good_a", 0), make_iv(b"good_b", 0)],
            cmp.clone(),
        ));
        let bad: Box<dyn CoherentMergeSource> = Box::new(ErrSource {
            emit_forward_error: true,
            emit_backward_error: false,
        });
        let mut m = SeekingMerger::new(alloc::vec![good, bad], cmp);
        // Call 1: init queued bad's error AND prefetched good_a.
        // Error-first contract: error surfaces immediately.
        assert!(m.next().unwrap().is_err());
        // Call 2+: good prefetches drain in sorted order — neither
        // was lost despite the init-time error.
        assert_eq!(k(&m.next().unwrap().unwrap()), "good_a");
        assert_eq!(k(&m.next().unwrap().unwrap()), "good_b");
        assert!(m.next().is_none());
    }

    #[test]
    fn refill_err_surfaces_before_unrelated_source_yields() {
        // Copilot thread #39 regression: with multiple sources, a
        // refill error on one source must surface on the very next
        // call after the buffered value is yielded — NOT after the
        // entire tree drains. Otherwise I/O / corruption failures
        // hide behind potentially many unrelated yields.
        let cmp = comparator::default_comparator();
        let bad: Box<dyn CoherentMergeSource> = Box::new(LateErrSource {
            first_value: Some(make_iv(b"x_bad", 0)),
            already_errored: false,
        });
        let good: Box<dyn CoherentMergeSource> = Box::new(VecSource::new(
            [
                make_iv(b"y_good_1", 0),
                make_iv(b"y_good_2", 0),
                make_iv(b"y_good_3", 0),
            ],
            cmp.clone(),
        ));
        let mut m = SeekingMerger::new(alloc::vec![bad, good], cmp);
        // Call 1: bad's x_bad wins (lex 'x' < 'y'). Yield, refill
        // bad → err queued.
        assert_eq!(k(&m.next().unwrap().unwrap()), "x_bad");
        // Call 2: error MUST surface here, not after draining
        // y_good_1, y_good_2, y_good_3.
        assert!(m.next().unwrap().is_err());
        // Call 3+: the unrelated good values now drain normally.
        assert_eq!(k(&m.next().unwrap().unwrap()), "y_good_1");
        assert_eq!(k(&m.next().unwrap().unwrap()), "y_good_2");
        assert_eq!(k(&m.next().unwrap().unwrap()), "y_good_3");
        assert!(m.next().is_none());
    }

    #[test]
    fn cross_direction_surface_forward_pending_in_next_back() {
        // CodeRabbit thread #33: a forward refill error must NOT
        // be sidestepped by switching to next_back(). The pending
        // forward error surfaces at the next call regardless of
        // direction.
        let cmp = comparator::default_comparator();
        let a = LateErrSource {
            first_value: Some(make_iv(b"only", 0)),
            already_errored: false,
        };
        let mut m = SeekingMerger::new(alloc::vec![a], cmp);
        // Forward: yields "only", queues forward refill err.
        assert_eq!(k(&m.next().unwrap().unwrap()), "only");
        // Switch to backward — the queued forward err must still
        // surface, NOT be lost.
        assert!(m.next_back().unwrap().is_err());
    }

    #[test]
    fn cross_direction_surface_backward_pending_in_next() {
        // Mirror of the above: backward refill error must surface
        // on a subsequent next() call.
        let cmp = comparator::default_comparator();
        let a = LateErrSource {
            first_value: Some(make_iv(b"only", 0)),
            already_errored: false,
        };
        let mut m = SeekingMerger::new(alloc::vec![a], cmp);
        assert_eq!(k(&m.next_back().unwrap().unwrap()), "only");
        assert!(m.next().unwrap().is_err());
    }

    #[test]
    fn backward_init_keeps_earlier_prefetched_when_later_source_errs() {
        let cmp = comparator::default_comparator();
        let good: Box<dyn CoherentMergeSource> = Box::new(VecSource::new(
            [make_iv(b"good_a", 0), make_iv(b"good_b", 0)],
            cmp.clone(),
        ));
        let bad: Box<dyn CoherentMergeSource> = Box::new(ErrSource {
            emit_forward_error: false,
            emit_backward_error: true,
        });
        let mut m = SeekingMerger::new(alloc::vec![good, bad], cmp);
        // Error-first contract: surfaces immediately, then the
        // prefetched good values drain in descending order.
        assert!(m.next_back().unwrap().is_err());
        assert_eq!(k(&m.next_back().unwrap().unwrap()), "good_b");
        assert_eq!(k(&m.next_back().unwrap().unwrap()), "good_a");
        assert!(m.next_back().is_none());
    }

    /// `MergeSource` impl that yields a successful value first, then
    /// an error on the second pull. Exercises the error path AFTER
    /// init (during the per-step refill).
    struct LateErrSource {
        first_value: Option<InternalValue>,
        already_errored: bool,
    }

    impl MergeSource for LateErrSource {
        fn next(&mut self) -> Option<IterItem> {
            if let Some(v) = self.first_value.take() {
                Some(Ok(v))
            } else if !self.already_errored {
                self.already_errored = true;
                Some(Err(crate::Error::Unrecoverable))
            } else {
                None
            }
        }
        fn next_back(&mut self) -> Option<IterItem> {
            self.next()
        }
        fn seek(&mut self, _target: &InternalKey) -> crate::Result<()> {
            Ok(())
        }
    }
    impl CoherentMergeSource for LateErrSource {}

    // The previous "drops prefetched" tests were removed —
    // SeekingMerger now yields the buffered value first and queues
    // the refill error for the following call (see tests below).
    // Removing the old tests is intentional: a test that pins
    // silent-data-loss behaviour shouldn't live in the suite once
    // the behaviour is corrected.

    /// Test double simulating a source iterator with INDEPENDENT
    /// front and back cursors (the LSM SST-scanner / `RunReader`
    /// shape). Backed by a sorted `Vec` plus `front_idx` / `back_idx`
    /// pointers that shrink independently from each end.
    ///
    /// **Self-coordinates** via the `front_idx >= back_idx` guard:
    /// once the two pointers meet, both `next()` and `next_back()`
    /// return `None`. That guarantee — not any seek invocation from
    /// the merger — is what makes mixed direction safe for
    /// `SeekingMerger` (see module-level docs).
    ///
    /// `seek(target)` is implemented for trait conformance but is
    /// **not** a usable repositioning primitive on this test double:
    /// the clamp keeps the self-coordination invariant by collapsing
    /// the window to empty after any call:
    ///   - `front_idx` becomes `max(front_idx, partition_point<target)`
    ///     — forced past or to the target;
    ///   - `back_idx` becomes `min(back_idx, partition_point<target)`
    ///     — forced at or before the target;
    ///   - combined: `front_idx >= back_idx`, so subsequent
    ///     `next()` / `next_back()` both return `None`.
    ///
    /// This is correct for the merger's no-duplicates contract on
    /// self-coordinating sources but is **not** the contract
    /// [`MergeSource::seek`] documents for a general independent-
    /// cursor implementation (real LSM scanners would hard-reset
    /// the cursor at `target`). Production impls should treat
    /// `MergeSource::seek` as a true reposition; this test double
    /// is a self-coordinating shape only.
    ///
    /// Unlike `VecSource` this does NOT implement
    /// [`CoherentMergeSource`] — cursors aren't literally shared,
    /// they're coordinated by index arithmetic on a single backing
    /// `Vec`.
    struct IndependentCursorSource {
        items: Vec<crate::InternalValue>,
        front_idx: usize,
        back_idx: usize,
        comparator: SharedComparator,
    }

    impl IndependentCursorSource {
        fn new<I: IntoIterator<Item = crate::InternalValue>>(
            items: I,
            comparator: SharedComparator,
        ) -> Self {
            let items: Vec<_> = items.into_iter().collect();
            // partition_point in seek() assumes ascending key order;
            // enforce it in debug builds to catch test misuse early.
            debug_assert!(
                items.is_sorted_by(|a, b| {
                    a.key.compare_with(&b.key, comparator.as_ref()) != Ordering::Greater
                }),
                "IndependentCursorSource items must be sorted ascending by key",
            );
            let n = items.len();
            Self {
                items,
                front_idx: 0,
                back_idx: n,
                comparator,
            }
        }
    }

    impl MergeSource for IndependentCursorSource {
        fn next(&mut self) -> Option<IterItem> {
            if self.front_idx >= self.back_idx {
                return None;
            }
            #[expect(
                clippy::indexing_slicing,
                reason = "front_idx < back_idx <= items.len() by invariant"
            )]
            let v = self.items[self.front_idx].clone();
            self.front_idx += 1;
            Some(Ok(v))
        }

        fn next_back(&mut self) -> Option<IterItem> {
            if self.front_idx >= self.back_idx {
                return None;
            }
            self.back_idx -= 1;
            #[expect(
                clippy::indexing_slicing,
                reason = "back_idx < items.len() after decrement, by invariant"
            )]
            let v = self.items[self.back_idx].clone();
            Some(Ok(v))
        }

        fn seek(&mut self, target: &InternalKey) -> crate::Result<()> {
            // Clamping seek: enforces the self-coordination invariant
            // by collapsing the window to empty (see struct docs).
            // A production source would hard-reset cursors to `target`;
            // this test double cannot, without risking already-emitted
            // items being re-yielded — which would break the no-
            // duplicates property under mixed direction.
            let idx = self.items.partition_point(|v| {
                v.key.compare_with(target, self.comparator.as_ref()) == Ordering::Less
            });
            self.front_idx = self.front_idx.max(idx);
            self.back_idx = self.back_idx.min(idx);
            Ok(())
        }
    }

    #[test]
    fn switch_to_backward_after_drain_emits_no_duplicates() {
        // Inverted regression for the deleted
        // `mvp_emits_duplicates_with_independent_cursor_source`.
        //
        // Old buggy version (separate forward/backward queues with
        // no shared state) would emit a,b,c,d forward AND d,c,b,a
        // backward — 8 emissions for 4 unique items.
        //
        // Current `IndependentCursorSource` self-coordinates via
        // the `front_idx >= back_idx` guard: after full forward
        // drain both pointers equal 4, so `next_back()` returns
        // `None` immediately. Total 4 emissions for 4 unique
        // items — each yielded exactly once, no merger-side seek
        // needed.
        let cmp = comparator::default_comparator();
        let src = IndependentCursorSource::new(
            [
                make_iv(b"a", 0),
                make_iv(b"b", 0),
                make_iv(b"c", 0),
                make_iv(b"d", 0),
            ],
            cmp.clone(),
        );
        let mut m = SeekingMerger::new(alloc::vec![src], cmp);

        // Drain forward.
        assert_eq!(k(&m.next().unwrap().unwrap()), "a");
        assert_eq!(k(&m.next().unwrap().unwrap()), "b");
        assert_eq!(k(&m.next().unwrap().unwrap()), "c");
        assert_eq!(k(&m.next().unwrap().unwrap()), "d");
        assert!(m.next().is_none(), "source exhausted forward");

        // Switch to backward. Source's `(front_idx, back_idx)`
        // window is now (4, 4); next_back's guard returns None
        // without yielding anything — no duplicates of the
        // forward emissions.
        assert!(
            m.next_back().is_none(),
            "backward must not re-emit forward-consumed items",
        );
        assert!(m.next_back().is_none(), "stays exhausted");
    }

    #[test]
    fn mid_stream_alternation_emits_no_duplicates_independent_cursor() {
        // Stronger property than the drain-then-switch test: switch
        // direction MID-stream and verify the (front_idx, back_idx)
        // window keeps the two halves disjoint. Source [a..f], six
        // items. Forward consumes 'a','b','c' from the front;
        // backward consumes 'f','e','d' from the back. They meet
        // at the middle with no overlap.
        let cmp = comparator::default_comparator();
        let src = IndependentCursorSource::new(
            [
                make_iv(b"a", 0),
                make_iv(b"b", 0),
                make_iv(b"c", 0),
                make_iv(b"d", 0),
                make_iv(b"e", 0),
                make_iv(b"f", 0),
            ],
            cmp.clone(),
        );
        let mut m = SeekingMerger::new(alloc::vec![src], cmp);

        assert_eq!(k(&m.next().unwrap().unwrap()), "a");
        assert_eq!(k(&m.next_back().unwrap().unwrap()), "f");
        assert_eq!(k(&m.next().unwrap().unwrap()), "b");
        assert_eq!(k(&m.next_back().unwrap().unwrap()), "e");
        assert_eq!(k(&m.next().unwrap().unwrap()), "c");
        assert_eq!(k(&m.next_back().unwrap().unwrap()), "d");
        // All six unique items yielded exactly once across the
        // alternation. Both ends now exhausted.
        assert!(m.next().is_none());
        assert!(m.next_back().is_none());
    }

    #[test]
    fn forward_refill_error_yields_buffered_then_err_on_next_call() {
        let cmp = comparator::default_comparator();
        let a = LateErrSource {
            first_value: Some(make_iv(b"first", 0)),
            already_errored: false,
        };
        let mut m = SeekingMerger::new(alloc::vec![a], cmp);
        // First call: returns the buffered "first", the refill Err
        // is queued.
        assert_eq!(k(&m.next().unwrap().unwrap()), "first");
        // Second call: surfaces the queued Err.
        assert!(m.next().unwrap().is_err());
        // Third call: source fully drained.
        assert!(m.next().is_none());
    }

    #[test]
    fn backward_refill_error_yields_buffered_then_err_on_next_call() {
        let cmp = comparator::default_comparator();
        let a = LateErrSource {
            first_value: Some(make_iv(b"first", 0)),
            already_errored: false,
        };
        let mut m = SeekingMerger::new(alloc::vec![a], cmp);
        assert_eq!(k(&m.next_back().unwrap().unwrap()), "first");
        assert!(m.next_back().unwrap().is_err());
        assert!(m.next_back().is_none());
    }
}

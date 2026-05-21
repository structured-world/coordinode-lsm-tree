// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Merging iterator over `MergeSource`s, backed by two independent
//! `LoserTree` tournaments (forward min, backward max).
//!
//! `DoubleEndedIterator` is gated on the [`CoherentMergeSource`]
//! marker: only sources whose `next()` and `next_back()` share
//! cursor state (the std `vec::IntoIter`, `VecDeque`,
//! `BTreeMap::range` shape) get the backward iterator. Mixed
//! direction on such sources is safe because their cursor
//! discipline already enforces "no item yielded twice" — no seek
//! reconciliation needed.
//!
//! For sources with INDEPENDENT cursors (LSM SST scanners, future
//! `RunReader`), `MergeSource::seek` will be the primitive that
//! reconciles the forward and backward prefetch streams. That
//! direction-switch path is the **follow-up** (issue #280) — this
//! MVP does NOT call `seek` from `SeekingMerger`, which is why the
//! `CoherentMergeSource` marker gate exists in the first place
//! (it keeps the dangerous combination of "non-coherent source +
//! mixed direction" off the public API at compile time).
//!
//! The eager per-direction init below builds each tournament from
//! a pre-populated `Vec<Option<MergerEntry>>` on first use, lazy
//! per direction, independent of the other (mirrors the legacy
//! `MergeHeap` `init_lo` / `init_hi` pattern).

use crate::comparator::SharedComparator;
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
/// Concrete struct (not a closure-in-a-box) so the
/// `EntryComparator<MergerEntry>` impl monomorphises through
/// `LoserTree<MergerEntry, MinCmp>` at every use site — no
/// vtable dispatch, no runtime direction branch. The previous
/// single-`MergerCmp` design carried a `Forward`/`Backward`
/// enum discriminant on every comparator call; splitting into
/// `MinCmp` / `MaxCmp` removes that per-cmp branch entirely
/// (direction is a compile-time fact, not a runtime field).
///
/// Smaller key wins; ties broken by lower source index.
#[derive(Clone)]
struct MinCmp {
    comparator: SharedComparator,
}

impl crate::loser_tree::EntryComparator<MergerEntry> for MinCmp {
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
            .compare_with(&b.value.key, self.comparator.as_ref())
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
struct MaxCmp {
    comparator: SharedComparator,
}

impl crate::loser_tree::EntryComparator<MergerEntry> for MaxCmp {
    #[expect(
        clippy::inline_always,
        reason = "called O(log cap) per replay step on the merger's hot path"
    )]
    #[inline(always)]
    fn compare(&self, a: &MergerEntry, b: &MergerEntry) -> Ordering {
        b.value
            .key
            .compare_with(&a.value.key, self.comparator.as_ref())
            .then_with(|| b.source.cmp(&a.source))
    }
}

fn build_min_cmp(comparator: SharedComparator) -> MinCmp {
    MinCmp { comparator }
}

fn build_max_cmp(comparator: SharedComparator) -> MaxCmp {
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
/// # Direction-switching scope (MVP — important)
///
/// The "RocksDB-style direction switching" framing in the issue and
/// PR description refers to the **target** design. The current
/// implementation is the **MVP**: it relies entirely on each source's
/// own `Iterator::next` / `DoubleEndedIterator::next_back` cursors
/// for "no item yielded twice" under mixed forward/backward use. It
/// **never calls** `MergeSource::seek`.
///
/// That means: **mixing `next()` and `next_back()` is only safe when
/// the source iterators have coherent front/back cursors** — i.e.,
/// calling `.next()` advances the same shared remaining range that
/// `.next_back()` walks from the other end. The std library's
/// `vec::IntoIter`, `VecDeque`, and `BTreeMap::range` all qualify.
/// The `CoherentIterSource` adapter wraps these.
///
/// For sources with **independent** front/back cursors (LSM SST
/// scanners and run readers, the intended follow-up impls), backward
/// iteration is currently UNAVAILABLE at the type level:
/// `impl DoubleEndedIterator for SeekingMerger<S>` is bounded on
/// `S: CoherentMergeSource`, and independent-cursor sources do NOT
/// implement that marker. So `merger.next_back()` won't even compile
/// for them — they're usable through `Iterator::next()` only. When
/// the seek-aware direction-switch path lands (issue #280) the bound
/// can relax back to `MergeSource` and backward iteration becomes
/// available for those sources too.
pub struct SeekingMerger<S: MergeSource> {
    sources: Vec<S>,
    n_sources: usize,
    comparator: SharedComparator,
    forward_tree: Option<LoserTree<MergerEntry, MinCmp>>,
    backward_tree: Option<LoserTree<MergerEntry, MaxCmp>>,
    /// Refill error queued during the previous forward step. When
    /// a source's `.next()` fails AFTER a value was already buffered
    /// in the tournament, we yield the buffered value first and stash
    /// the error here so the NEXT call to [`Iterator::next`] surfaces
    /// it. Avoids silent data loss of the prefetched value.
    pending_forward_error: Option<crate::Error>,
    /// Mirror of `pending_forward_error` for the backward direction.
    pending_backward_error: Option<crate::Error>,
}

impl<S: MergeSource> SeekingMerger<S> {
    /// Construct an empty merger ready to consume from `sources`.
    /// Tournaments are not built yet; both directions populate
    /// lazily on first use.
    #[must_use]
    pub fn new(sources: Vec<S>, comparator: SharedComparator) -> Self {
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

impl<S: MergeSource> Iterator for SeekingMerger<S> {
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

// `DoubleEndedIterator` is gated on `CoherentMergeSource` — without
// that marker, mixed `next()` / `next_back()` on an
// independent-cursor source would emit duplicates around the
// crossover key. The marker is the type-system encoding of "this
// source's cursors share state, so the merger doesn't have to call
// `seek` to reconcile". When the seek-aware direction switch lands
// (issue #280), the bound can be relaxed back to `MergeSource`.
impl<S: CoherentMergeSource> DoubleEndedIterator for SeekingMerger<S> {
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
        let mut m: SeekingMerger<VecSource> = SeekingMerger::new(alloc::vec![], cmp);
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

    // The previous `IndependentCursorSource` test double and its
    // `mvp_emits_duplicates_*` regression were DELETED, not ignored.
    // `DoubleEndedIterator` is now gated on `CoherentMergeSource`,
    // so a source whose cursors don't coordinate (the
    // `IndependentCursorSource` shape) cannot be wrapped in a
    // `SeekingMerger` that exposes `.next_back()` at all — the
    // dangerous mixed-direction usage is rejected at compile time
    // rather than masked by a yields-duplicates test. The follow-up
    // wiring of `seek` into a direction-switch path (issue #280)
    // will re-enable the bound to `MergeSource` and ship the
    // correct-by-construction non-coherent test then.

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

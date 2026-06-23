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
    fn new<I: IntoIterator<Item = InternalValue>>(items: I, comparator: SharedComparator) -> Self {
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
/// `seek(target)` is a deliberately conservative test-double
/// implementation: the clamp collapses the live window so the
/// source behaves as if exhausted after any seek. We pick this
/// over a hard cursor reset because the test suite never needs
/// real repositioning, and the conservative behaviour makes
/// reasoning about test outcomes simpler.
///   - `front_idx` becomes `max(front_idx, partition_point<target)`;
///   - `back_idx` becomes `min(back_idx, partition_point<target)`;
///   - combined: `front_idx >= back_idx`, so subsequent
///     `next()` / `next_back()` both return `None`.
///
/// Production LSM scanners that implement
/// [`CoherentMergeSource`] are free to hard-reset their cursors
/// on `seek` — the marker's no-duplicates promise covers mixed
/// direction *without* an intervening user seek (see the trait
/// docs).
///
/// Implements [`CoherentMergeSource`] (impl below) via the
/// self-coordinating `(front_idx, back_idx)` window — cursors
/// aren't literally shared (as in `VecSource` / std `VecDeque`),
/// but the index arithmetic on a single backing `Vec` provides
/// the same no-duplicates-under-mixed-direction guarantee the
/// marker promises.
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
        // Clamping seek: nudges the existing window toward
        // `target` without ever expanding it. A production
        // independent-cursor source would hard-reset its cursors
        // to `target` (the no-duplicates promise on
        // [`CoherentMergeSource`] is scoped to direction switches
        // *without* an intervening user `seek`, so re-yielding
        // previously-emitted items here would be allowed). This
        // test double sticks with clamping because the seeking
        // merger tests it backs do not exercise post-seek
        // direction switches — there's no behaviour to verify
        // that hard-reset would express, and clamping keeps the
        // assertion footprint smaller (the window's invariant
        // never changes across the seek call).
        let idx = self.items.partition_point(|v| {
            v.key.compare_with(target, self.comparator.as_ref()) == Ordering::Less
        });
        self.front_idx = self.front_idx.max(idx);
        self.back_idx = self.back_idx.min(idx);
        Ok(())
    }
}

// IndependentCursorSource satisfies CoherentMergeSource's
// no-duplicates-under-mixed-direction promise via the
// self-coordinating (front_idx, back_idx) window guard. The
// clamp on `seek` preserves the invariant — see struct docs.
impl CoherentMergeSource for IndependentCursorSource {}

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

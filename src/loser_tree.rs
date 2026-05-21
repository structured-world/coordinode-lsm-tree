// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Knuth tournament (loser) tree for `k`-way merge.
//!
//! A loser tree solves the same problem as a binary heap for merging
//! sorted inputs, but with one fewer comparison per `replace_min` and
//! a cache-friendlier memory layout. The winner sits at the root
//! (`tree[0]`); each internal node stores the **loser** of the game
//! played at that node. To replace the winner, only the path from its
//! leaf back up to the root needs to be replayed — `log₂(n)` comparisons
//! instead of the `2 log₂(n)` a binary heap would need (one per level,
//! comparing against the loser already stored there rather than against
//! both children).
//!
//! This module's `LoserTree<E, F>` is generic over the element type and
//! the comparator function (no `Arc<dyn Trait>` indirection — the
//! comparator is monomorphised per use site for zero-cost dispatch).
//!
//! # Layout
//!
//! `leaves[0..cap]` holds the current value for each input slot. `cap`
//! is `n.next_power_of_two().max(2)`, so the tournament always has a
//! complete binary tree above it. Slots beyond `n` are permanently
//! `None`, acting as `+∞` sentinels that never win.
//!
//! `tree[0]` stores the index (into `leaves`) of the overall winner.
//! `tree[1..cap]` stores the loser index at each internal node. The
//! conventional `2cap`-sized embedded-tree layout maps to our `cap`-sized
//! `tree` like this:
//!
//! - leaf `L`'s first internal-node parent lives at index `(cap + L) / 2`
//! - each parent's parent is `idx >> 1`
//! - the loop terminates at `idx == 1` (root); the final winner is
//!   stored at `tree[0]`
//!
//! # Sentinel handling
//!
//! Each leaf is logically `Option<E>` — present or sentinel. The
//! physical representation uses two parallel arrays:
//! `leaves: Vec<MaybeUninit<E>>` for the values and
//! `present: Vec<bool>` for the discriminants. This eliminates the
//! `Option` discriminant branch that LLVM emits inside `cmp_indices`
//! on every game (called O(log cap) times per merger step); the
//! present-bit fetch is a single byte load that branch-predicts
//! perfectly under steady-state merging (where leaves stay present
//! until exhaustion). `cmp_indices` treats absent leaves as
//! strictly greater than any present value, so exhausted sources
//! naturally lose every game.
//!
//! All `MaybeUninit::assume_init_*` calls are guarded by a check on
//! the corresponding `present[i]` bit and never widen the unsafe
//! contract beyond what the bitmap already guarantees. `Drop`
//! walks the bitmap and drops in place.

use alloc::vec::Vec;
use core::cmp::Ordering;
use core::mem::MaybeUninit;

/// A min-tournament tree over `n` input slots.
///
/// The "min" comes from how `cmp` is interpreted: the leaf whose value
/// is `Ordering::Less` than every other leaf wins. To get max-semantics,
/// pass a reversed comparator (`|a, b| cmp(a, b).reverse()`).
pub struct LoserTree<E, F> {
    /// Current value per source slot, untagged. Only meaningful when
    /// the matching `present[i]` bit is `true`. Length is `cap`
    /// (padded to the next power of two ≥ 2); the trailing
    /// `cap - n_sources` slots are permanent sentinels with
    /// `present[i] == false`.
    leaves: Vec<MaybeUninit<E>>,
    /// Discriminant for `leaves[i]`. `true` ⇒ initialised; `false` ⇒
    /// uninit (exhausted or padding sentinel). Walking this bitmap
    /// alongside `leaves` replaces the `Option` discriminant the
    /// previous layout carried inline.
    present: Vec<bool>,
    /// Tree of size `cap`. `tree[0]` = winner leaf index; `tree[1..cap]`
    /// = loser leaf index at each internal node.
    tree: Vec<usize>,
    /// Number of source slots originally supplied. Less than or equal
    /// to `leaves.len()` (= `cap`). Exposed via [`Self::slots`].
    n_sources: usize,
    /// Count of present leaves. When zero the tree is empty.
    active: usize,
    /// Comparator. Captures source-order tie-break and any other
    /// merge-precedence rules the caller wants.
    cmp: F,
}

impl<E: core::fmt::Debug, F> core::fmt::Debug for LoserTree<E, F> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // Reconstitute the conceptual Vec<Option<&E>> view for Debug
        // — never expose a MaybeUninit through Debug output.
        f.debug_struct("LoserTree")
            .field("n_sources", &self.n_sources)
            .field("active", &self.active)
            .field("cap", &self.leaves.len())
            .finish_non_exhaustive()
    }
}

impl<E, F> Drop for LoserTree<E, F> {
    fn drop(&mut self) {
        // Drop each present leaf in place. The MaybeUninit storage
        // doesn't drop on its own; without this loop, owned values
        // (Strings, Boxes, Arcs, ...) inside present leaves would
        // leak when the tree itself is dropped.
        #[expect(
            clippy::indexing_slicing,
            reason = "leaves.len() == present.len() by construction (set in build())"
        )]
        for i in 0..self.leaves.len() {
            if self.present[i] {
                // SAFETY: `present[i] == true` is the LoserTree
                // invariant for `leaves[i]` being initialised.
                unsafe { self.leaves[i].assume_init_drop() };
            }
        }
    }
}

impl<E, F> LoserTree<E, F>
where
    F: Fn(&E, &E) -> Ordering,
{
    /// Build a tournament over `initial` (one entry per source slot).
    ///
    /// `None` entries are treated as exhausted/sentinel. After
    /// construction, `peek_min()` returns the smallest `Some` value
    /// across all slots.
    ///
    /// O(n) work: each internal node is visited exactly once during
    /// the bottom-up build.
    pub fn build(initial: Vec<Option<E>>, cmp: F) -> Self {
        let n = initial.len();
        let cap = n.next_power_of_two().max(2);

        // Split the Option<E> input into parallel (MaybeUninit, bool)
        // arrays. Trailing padding past `n` is uninit + absent.
        let mut leaves: Vec<MaybeUninit<E>> = Vec::with_capacity(cap);
        let mut present: Vec<bool> = Vec::with_capacity(cap);
        let mut active = 0_usize;
        for item in initial {
            if let Some(v) = item {
                leaves.push(MaybeUninit::new(v));
                present.push(true);
                active += 1;
            } else {
                leaves.push(MaybeUninit::uninit());
                present.push(false);
            }
        }
        while leaves.len() < cap {
            leaves.push(MaybeUninit::uninit());
            present.push(false);
        }
        let tree = alloc::vec![0; cap];

        let mut t = Self {
            leaves,
            present,
            tree,
            n_sources: n,
            active,
            cmp,
        };
        t.build_subtree(1, 0, cap);
        t
    }

    /// Number of source slots `n` originally supplied (not `cap`).
    ///
    /// Reflects the *capacity* of the tournament; some slots may be
    /// exhausted (`None`) at any given moment.
    #[inline]
    #[expect(
        dead_code,
        reason = "part of the slot-set protocol, used by future callers"
    )]
    pub fn slots(&self) -> usize {
        self.n_sources
    }

    /// Whether every slot is exhausted (no present leaves).
    #[expect(
        clippy::inline_always,
        reason = "called from winner_slot() on every merger step; forcing cross-crate inlining \
                  for the bench compilation unit measurably tightens the hot loop"
    )]
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.active == 0
    }

    /// Number of slots still holding a value.
    #[inline]
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "diagnostic accessor used by unit tests and future callers"
        )
    )]
    pub fn active_count(&self) -> usize {
        self.active
    }

    /// Slot index of the current overall winner, or `None` if empty.
    ///
    /// Useful for callers (like the LSM merger) that need to know
    /// *which* source produced the current minimum, so the next item
    /// from that source can be pulled in.
    #[expect(
        clippy::inline_always,
        reason = "hot per-step routine; cross-crate inlining for benches"
    )]
    #[inline(always)]
    #[expect(
        clippy::indexing_slicing,
        reason = "tree[0] always exists: cap >= 2 by construction"
    )]
    pub fn winner_slot(&self) -> Option<usize> {
        if self.is_empty() {
            None
        } else {
            Some(self.tree[0])
        }
    }

    /// Borrow the current overall minimum, or `None` if empty.
    #[inline]
    #[expect(
        clippy::indexing_slicing,
        reason = "idx from winner_slot() is always < leaves.len()"
    )]
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "exposed for callers that want to inspect the winner without popping; \
                      seeking_merger reads the winner via winner_slot() since the slot index \
                      equals the source index by construction"
        )
    )]
    pub fn peek_min(&self) -> Option<&E> {
        let idx = self.winner_slot()?;
        if !self.present[idx] {
            return None;
        }
        // SAFETY: `present[idx] == true` is the LoserTree invariant
        // for `leaves[idx]` being initialised.
        Some(unsafe { self.leaves[idx].assume_init_ref() })
    }

    /// Replace the value at the winning slot with `new` and return the
    /// previous winner. Replays `log₂(cap)` games up the path from the
    /// winning leaf to the root.
    ///
    /// Caller-side contract: `new` must come from the **same source
    /// iterator** that produced the previous winner. Mixing slots
    /// breaks the merger's invariant that each slot reflects its own
    /// source's current item.
    ///
    /// # Panics
    ///
    /// Panics if the tree is empty (`peek_min().is_none()`). Callers
    /// must check `is_empty()` first or use [`Self::pop_min`] semantics.
    #[expect(
        clippy::expect_used,
        reason = "empty-tree panic is the documented contract"
    )]
    #[expect(
        clippy::indexing_slicing,
        reason = "slot from winner_slot() is always < leaves.len()"
    )]
    pub fn replace_min(&mut self, new: E) -> E {
        let slot = self
            .winner_slot()
            .expect("replace_min called on empty LoserTree");
        debug_assert!(
            self.present[slot],
            "LoserTree winner slot must be present when winner_slot() returns Some"
        );
        // SAFETY: `present[slot] == true` (asserted by winner_slot
        // returning Some plus the LoserTree invariant). The old
        // value is read out and the new is written in its place;
        // `present[slot]` stays `true` so no bitmap update needed.
        let old = unsafe {
            core::mem::replace(&mut self.leaves[slot], MaybeUninit::new(new)).assume_init()
        };
        self.replay(slot);
        old
    }

    /// Pop the winning value and mark its slot exhausted. The next
    /// `peek_min()` will reflect the new winner (or `None` if this
    /// drained the last slot). O(log cap).
    #[expect(
        clippy::indexing_slicing,
        reason = "slot from winner_slot() is always < leaves.len()"
    )]
    pub fn pop_min(&mut self) -> Option<E> {
        let slot = self.winner_slot()?;
        debug_assert!(
            self.present[slot],
            "LoserTree winner slot must be present when winner_slot() returns Some"
        );
        // SAFETY: present[slot] == true (LoserTree invariant for the
        // winner slot). Reading via `replace(_, uninit)` returns the
        // initialised value and leaves the slot as MaybeUninit::uninit;
        // we then flip the bitmap so subsequent code (cmp_indices,
        // peek_min, Drop) sees it as absent.
        let old = unsafe {
            core::mem::replace(&mut self.leaves[slot], MaybeUninit::uninit()).assume_init()
        };
        self.present[slot] = false;
        self.active -= 1;
        self.replay(slot);
        Some(old)
    }

    /// Take the value at a specific slot (regardless of winner status)
    /// and mark the slot exhausted. The next `peek_min` reflects the
    /// updated tournament. Returns the taken value, or `None` if the
    /// slot was already exhausted (or out of range).
    ///
    /// This exists for the seeking-merger's cross-direction migration:
    /// when one tournament's source has run out but the OPPOSITE
    /// tournament still holds a buffered leaf for that source, the
    /// active direction takes it via this API rather than dropping
    /// it on the floor. O(log cap).
    #[expect(
        clippy::indexing_slicing,
        reason = "slot is checked < self.leaves.len() before indexing"
    )]
    pub fn take_slot(&mut self, slot: usize) -> Option<E> {
        if slot >= self.leaves.len() || !self.present[slot] {
            return None;
        }
        // SAFETY: present[slot] == true (just checked above).
        let taken = unsafe {
            core::mem::replace(&mut self.leaves[slot], MaybeUninit::uninit()).assume_init()
        };
        self.present[slot] = false;
        self.active -= 1;
        self.replay(slot);
        Some(taken)
    }

    // ---------- internals ----------

    /// Recursive bottom-up build. Each internal node at array index
    /// `node` covers leaves `[leaf_lo, leaf_hi)`. The function returns
    /// the slot index of the subtree's winner; the loser of the final
    /// game at `node` is stored in `self.tree[node]`.
    #[expect(
        clippy::indexing_slicing,
        reason = "node is always in 1..cap by recursive construction; tree[0] always in bounds"
    )]
    fn build_subtree(&mut self, node: usize, leaf_lo: usize, leaf_hi: usize) -> usize {
        if leaf_hi - leaf_lo == 1 {
            // Base case: single leaf, no internal node game here.
            // (`node` corresponds to a leaf "position" in the conceptual
            // 2*cap-array; we don't write to tree[] for leaves.)
            return leaf_lo;
        }
        let mid = usize::midpoint(leaf_lo, leaf_hi);
        let left_winner = self.build_subtree(2 * node, leaf_lo, mid);
        let right_winner = self.build_subtree(2 * node + 1, mid, leaf_hi);

        let (winner, loser) = match self.cmp_indices(left_winner, right_winner) {
            Ordering::Less | Ordering::Equal => (left_winner, right_winner),
            Ordering::Greater => (right_winner, left_winner),
        };
        self.tree[node] = loser;
        if node == 1 {
            // Root subtree finished — store the overall winner.
            self.tree[0] = winner;
        }
        winner
    }

    /// Replay the tournament path from `leaf`'s position back up to the
    /// root, updating `tree[0]` with the new overall winner. O(log cap).
    ///
    /// `#[inline(always)]` — this is the single hot per-step routine
    /// called from `replace_min` / `pop_min` / `take_slot` on every
    /// merger step. Forcing inlining lets the caller hoist
    /// `self.tree.as_ptr()` / `self.cmp` out of the loop and avoids
    /// the call-site branch-predictor cost; default `#[inline]` is
    /// only a hint and stops at crate boundaries (benches live in a
    /// separate compilation unit).
    #[expect(
        clippy::inline_always,
        reason = "the single hot per-merger-step routine; cross-crate inlining for benches"
    )]
    #[inline(always)]
    #[expect(
        clippy::indexing_slicing,
        reason = "node traverses 1..cap by construction; tree[0] always exists"
    )]
    fn replay(&mut self, leaf: usize) {
        let mut winner = leaf;
        // Parent index in the conceptual 2*cap array: (cap + leaf) / 2.
        // Use midpoint to avoid potential overflow on 32-bit targets.
        let mut node = usize::midpoint(self.leaves.len(), leaf);
        while node >= 1 {
            let other = self.tree[node];
            // Smaller wins. If the stored loser is actually smaller
            // than our running winner, swap them: the running winner
            // becomes the new loser stored at this node.
            if self.cmp_indices(other, winner) == Ordering::Less {
                self.tree[node] = winner;
                winner = other;
            }
            node >>= 1;
        }
        self.tree[0] = winner;
    }

    /// Compare leaves by slot index, treating absent slots as `+∞`.
    /// Absent always loses to present; absent vs absent returns
    /// `Equal`.
    ///
    /// `#[inline(always)]` — called O(log cap) times per `replay()`
    /// step, which is itself the hot per-merger-step routine. The
    /// present-bit branch is a single byte load + predictable
    /// compare; inlining lets LLVM fuse consecutive checks across
    /// the replay loop.
    #[expect(
        clippy::inline_always,
        reason = "called O(log cap) per replay; cross-crate inlining for benches"
    )]
    #[inline(always)]
    #[expect(
        clippy::indexing_slicing,
        reason = "callers (build_subtree, replay) only pass slot indices < cap; \
                  present.len() == leaves.len() == cap by construction"
    )]
    fn cmp_indices(&self, a: usize, b: usize) -> Ordering {
        match (self.present[a], self.present[b]) {
            (true, true) => {
                // SAFETY: both bits set ⇒ both leaves initialised
                // (LoserTree invariant maintained by build/replace/
                // pop/take_slot).
                let va = unsafe { self.leaves[a].assume_init_ref() };
                let vb = unsafe { self.leaves[b].assume_init_ref() };
                (self.cmp)(va, vb)
            }
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => Ordering::Equal,
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test assertions")]
#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "cmp_u32 signature mirrors the Fn(&E, &E) -> Ordering trait"
)]
#[expect(
    clippy::cast_possible_truncation,
    reason = "test sizes are small (n <= 33)"
)]
mod tests {
    // Test names below are short forms (e.g. `empty_tree`,
    // `drain_in_order`) — the project convention requires
    // `<what>_<condition>_<expected>` for top-level tests, OR
    // short names inside a descriptively-named submodule. The
    // descriptively-named submodule path is what's exercised
    // here: `loser_tree::tests::<short_name>` reads as "loser
    // tree's <short_name> test", which gives the short forms the
    // missing `<what>` context.
    use super::*;
    use test_log::test;

    fn cmp_u32(a: &u32, b: &u32) -> Ordering {
        a.cmp(b)
    }

    fn collect<F: Fn(&u32, &u32) -> Ordering>(mut t: LoserTree<u32, F>) -> Vec<u32> {
        let mut out = Vec::new();
        while let Some(v) = t.pop_min() {
            out.push(v);
        }
        out
    }

    #[test]
    fn empty_tree() {
        let t: LoserTree<u32, fn(&u32, &u32) -> Ordering> =
            LoserTree::build(alloc::vec![None, None, None], cmp_u32);
        assert!(t.is_empty());
        assert_eq!(t.active_count(), 0);
        assert_eq!(t.peek_min(), None);
        assert_eq!(t.winner_slot(), None);
    }

    #[test]
    fn single_slot() {
        let mut t = LoserTree::build(alloc::vec![Some(42_u32)], cmp_u32);
        assert!(!t.is_empty());
        assert_eq!(t.peek_min(), Some(&42));
        assert_eq!(t.pop_min(), Some(42));
        assert!(t.is_empty());
        assert_eq!(t.pop_min(), None);
    }

    #[test]
    fn drain_in_order() {
        // 4 sources, each with a distinct value.
        let t = LoserTree::build(alloc::vec![Some(3_u32), Some(1), Some(4), Some(2)], cmp_u32);
        assert_eq!(collect(t), [1, 2, 3, 4]);
    }

    #[test]
    fn non_pow2_padding() {
        // 5 slots → cap = 8. Sentinels must not affect winner.
        let t = LoserTree::build(
            alloc::vec![Some(50_u32), Some(10), Some(40), Some(20), Some(30)],
            cmp_u32,
        );
        assert_eq!(collect(t), [10, 20, 30, 40, 50]);
    }

    #[test]
    fn replace_min_stays_winner_when_still_smallest() {
        // Slot 0 keeps yielding monotonically increasing values that
        // are still below everyone else.
        let mut t = LoserTree::build(
            alloc::vec![Some(1_u32), Some(100), Some(200), Some(300)],
            cmp_u32,
        );
        assert_eq!(t.replace_min(2), 1);
        assert_eq!(t.peek_min(), Some(&2));
        assert_eq!(t.winner_slot(), Some(0));
        assert_eq!(t.replace_min(3), 2);
        assert_eq!(t.peek_min(), Some(&3));
        assert_eq!(t.winner_slot(), Some(0));
    }

    #[test]
    fn replace_min_changes_winner() {
        let mut t = LoserTree::build(alloc::vec![Some(1_u32), Some(5), Some(3), Some(7)], cmp_u32);
        assert_eq!(t.winner_slot(), Some(0));
        // Replace slot 0's value with something larger than slot 2 (3)
        // but smaller than slot 1 (5).
        assert_eq!(t.replace_min(4), 1);
        assert_eq!(t.peek_min(), Some(&3)); // slot 2 wins now
        assert_eq!(t.winner_slot(), Some(2));
        assert_eq!(t.replace_min(6), 3);
        assert_eq!(t.peek_min(), Some(&4)); // slot 0 wins again with 4
        assert_eq!(t.winner_slot(), Some(0));
    }

    #[test]
    fn pop_min_then_drain() {
        let mut t = LoserTree::build(
            alloc::vec![Some(10_u32), Some(20), Some(5), Some(15)],
            cmp_u32,
        );
        assert_eq!(t.pop_min(), Some(5));
        assert_eq!(t.active_count(), 3);
        assert_eq!(t.peek_min(), Some(&10));
        assert_eq!(collect(t), [10, 15, 20]);
    }

    #[test]
    fn mixed_replace_and_pop() {
        // Drain interleaved: simulates a real merge where some sources
        // get exhausted partway through.
        let mut t = LoserTree::build(alloc::vec![Some(1_u32), Some(2), Some(3), Some(4)], cmp_u32);
        assert_eq!(t.replace_min(5), 1); // slot 0 now 5
        assert_eq!(t.replace_min(6), 2); // slot 1 now 6
        // Order should now be 3, 4, 5, 6.
        assert_eq!(t.pop_min(), Some(3));
        assert_eq!(t.pop_min(), Some(4));
        assert_eq!(t.pop_min(), Some(5));
        assert_eq!(t.pop_min(), Some(6));
        assert!(t.is_empty());
    }

    #[test]
    fn reverse_comparator_gives_max_tree() {
        // Same data, max-tree semantics via reversed cmp.
        let cmp = |a: &u32, b: &u32| b.cmp(a);
        let mut t = LoserTree::build(alloc::vec![Some(1_u32), Some(4), Some(2), Some(3)], cmp);
        assert_eq!(t.peek_min(), Some(&4)); // "min" under reversed cmp = max
        assert_eq!(t.pop_min(), Some(4));
        assert_eq!(t.pop_min(), Some(3));
        assert_eq!(t.pop_min(), Some(2));
        assert_eq!(t.pop_min(), Some(1));
    }

    #[test]
    fn deterministic_tiebreak_by_cmp() {
        // Ties go to whichever the comparator picks; we encode source
        // index via tuple so equal values resolve deterministically.
        let cmp = |a: &(u32, usize), b: &(u32, usize)| (a.0, a.1).cmp(&(b.0, b.1));
        let mut t = LoserTree::build(
            alloc::vec![Some((5_u32, 0)), Some((5, 1)), Some((5, 2)), Some((5, 3)),],
            cmp,
        );
        // All same key → slot 0 wins on tiebreak.
        assert_eq!(t.winner_slot(), Some(0));
        let mut order = Vec::new();
        while let Some((_, idx)) = t.pop_min() {
            order.push(idx);
        }
        assert_eq!(order, [0, 1, 2, 3]);
    }

    #[test]
    fn random_inputs_match_sorted_reference() {
        // Property-style: random multi-source data must drain in the
        // same order a sorted concatenation produces.
        use rand::SeedableRng;
        use rand::seq::SliceRandom;
        let mut rng = rand::rngs::StdRng::seed_from_u64(0xC0DE_F00D);
        for n in [1_usize, 2, 3, 7, 8, 9, 31, 32, 33] {
            for trial in 0..32 {
                let mut all: Vec<u32> = (0..(n as u32 * 4)).collect();
                all.shuffle(&mut rng);
                // Round-robin distribute into n buckets.
                let mut buckets: Vec<Vec<u32>> = (0..n).map(|_| Vec::new()).collect();
                for (i, v) in all.iter().enumerate() {
                    #[expect(clippy::indexing_slicing, reason = "i % n always < n")]
                    buckets[i % n].push(*v);
                }
                // Each bucket must be individually sorted (loser tree
                // assumes per-source sortedness, just like a merger).
                for b in &mut buckets {
                    b.sort_unstable();
                }
                // Snapshot the sorted reference before consuming buckets.
                let mut reference = all.clone();
                reference.sort_unstable();
                // Build tree from the first item of each bucket;
                // simulate refilling via replace_min until all drained.
                let mut iters: Vec<std::vec::IntoIter<u32>> =
                    buckets.into_iter().map(IntoIterator::into_iter).collect();
                let initial: Vec<Option<u32>> = iters.iter_mut().map(Iterator::next).collect();
                let mut t = LoserTree::build(initial, cmp_u32);
                let mut out = Vec::with_capacity(reference.len());
                while let Some(slot) = t.winner_slot() {
                    #[expect(clippy::indexing_slicing, reason = "slot < n by construction")]
                    if let Some(next_val) = iters[slot].next() {
                        out.push(t.replace_min(next_val));
                    } else {
                        out.push(t.pop_min().unwrap());
                    }
                }
                assert_eq!(out, reference, "n={n} trial={trial}");
            }
        }
    }
}

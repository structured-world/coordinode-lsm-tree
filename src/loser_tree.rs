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
//! `present: Vec<u8>` for the discriminants (one byte per slot,
//! `1` = present, `0` = absent). `Vec<u8>` is used over `Vec<bool>`
//! to make the byte-per-element layout self-evident from the type
//! and avoid any ambiguity about discriminant packing — the array
//! is a flat byte map by design.
//!
//! This eliminates the `Option` discriminant branch that LLVM
//! emits inside `cmp_indices` on every game (called O(log cap)
//! times per merger step); the present-byte fetch is a single
//! contiguous load + zero-compare that branch-predicts perfectly
//! under steady-state merging (where leaves stay present until
//! exhaustion). `cmp_indices` treats absent leaves (`present[i]
//! == 0`) as strictly greater than any present value, so
//! exhausted sources naturally lose every game.
//!
//! All `MaybeUninit::assume_init_*` calls are guarded by a check
//! on the corresponding `present[i]` byte and never widen the
//! unsafe contract beyond what the byte-map already guarantees.
//! `Drop` walks the byte-map and drops in place.

use alloc::vec::Vec;
use core::cmp::Ordering;
use core::mem::MaybeUninit;

/// Comparator strategy used by the tournament.
///
/// This trait exists instead of a plain `Fn(&E, &E) -> Ordering`
/// bound so callers can pass a concrete struct (e.g.
/// `SeekingMerger`'s `MinCmp<C>` / `MaxCmp<C>`) that
/// monomorphises through the type system. A blanket impl
/// forwards every `Fn` closure to this trait, so test code and
/// internal helpers that already use `|a, b| ...` closures
/// keep working without change.
///
/// **Why not just `Fn`:** wrapping the closure in `Box<dyn Fn>`
/// to satisfy a non-generic field type adds one indirect call per
/// `cmp_indices` invocation — and `cmp_indices` runs O(log cap)
/// times per `replace_min` / `pop_min` step, on the merger's
/// hottest path. With this trait, the comparator's concrete
/// type stays visible to LLVM, so the call inlines flat.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a comparator for `LoserTree<{E}, _>`",
    label = "missing `EntryComparator<{E}>` impl",
    note = "implement `EntryComparator<{E}>` directly, or pass a closure of \
            type `Fn(&{E}, &{E}) -> core::cmp::Ordering` — a blanket impl \
            forwards every such closure to this trait automatically"
)]
pub trait EntryComparator<E> {
    /// Compare two leaf values. Smaller wins in the tournament;
    /// to get max-semantics pass a reversed comparator.
    fn compare(&self, a: &E, b: &E) -> Ordering;
}

/// Blanket impl so existing `Fn(&E, &E) -> Ordering` closures
/// (tests, ad-hoc callers) satisfy the trait without rewriting.
/// `#[inline(always)]` propagates the closure's body into the
/// call site at the same cost as a direct call — the indirection
/// only existed when the closure was hidden behind `Box<dyn Fn>`,
/// which this trait specifically avoids by keeping the comparator
/// type concrete at every monomorphisation.
#[diagnostic::do_not_recommend]
impl<E, F> EntryComparator<E> for F
where
    F: Fn(&E, &E) -> Ordering,
{
    #[expect(
        clippy::inline_always,
        reason = "blanket forwarder must inline or the indirection this trait \
                  eliminates comes back; verified flat in disassembly"
    )]
    #[inline(always)]
    fn compare(&self, a: &E, b: &E) -> Ordering {
        (self)(a, b)
    }
}

/// A min-tournament tree over `n` input slots.
///
/// The "min" comes from how `cmp` is interpreted: the leaf whose value
/// is `Ordering::Less` than every other leaf wins. To get max-semantics,
/// pass a reversed comparator (`|a, b| cmp(a, b).reverse()`).
pub struct LoserTree<E, F> {
    /// Current value per source slot, untagged. Only meaningful
    /// when the matching `present[i]` byte is non-zero. Length is
    /// `cap` (padded to the next power of two ≥ 2); the trailing
    /// `cap - n_sources` slots are permanent sentinels with
    /// `present[i] == 0`.
    leaves: Vec<MaybeUninit<E>>,
    /// Discriminant byte for `leaves[i]`. `1` ⇒ initialised; `0` ⇒
    /// uninit (exhausted or padding sentinel). Walking this
    /// contiguous byte-map alongside `leaves` replaces the
    /// `Option` discriminant the previous layout carried inline.
    /// `u8` (not `bool`) is chosen so the byte-per-slot layout
    /// is unambiguous from the type itself.
    present: Vec<u8>,
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
        // Print only the structural shape — never expose leaf
        // bytes through Debug, since they sit behind MaybeUninit
        // and may be uninitialised at any given moment. The
        // `E: Debug` bound stays on the impl so this type can be
        // nested inside other `#[derive(Debug)]` structs without
        // breaking their derive expansion; we just don't use it.
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
            if self.present[i] != 0 {
                // SAFETY: `present[i] != 0` is the LoserTree
                // invariant for `leaves[i]` being initialised.
                unsafe { self.leaves[i].assume_init_drop() };
            }
        }
    }
}

impl<E, F> LoserTree<E, F>
where
    F: EntryComparator<E>,
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
        let mut present: Vec<u8> = Vec::with_capacity(cap);
        let mut active = 0_usize;
        for item in initial {
            if let Some(v) = item {
                leaves.push(MaybeUninit::new(v));
                present.push(1);
                active += 1;
            } else {
                leaves.push(MaybeUninit::uninit());
                present.push(0);
            }
        }
        while leaves.len() < cap {
            leaves.push(MaybeUninit::uninit());
            present.push(0);
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

    /// Empty the tournament in place: drop every present leaf and mark all slots
    /// absent, keeping the backing storage (`leaves` / `present` / `tree`)
    /// allocated for a later [`Self::refill_with`]. After this, `is_empty()` is
    /// true and `winner_slot()` is `None`.
    ///
    /// Used by the seekable range merger's in-place reposition: stale buffered
    /// leaves from the old position are dropped without freeing the storage.
    #[expect(
        clippy::indexing_slicing,
        reason = "leaves.len() == present.len() by construction (set in build())"
    )]
    pub fn clear(&mut self) {
        for i in 0..self.leaves.len() {
            if self.present[i] != 0 {
                // SAFETY: `present[i] != 0` is the LoserTree invariant for
                // `leaves[i]` being initialised.
                unsafe { self.leaves[i].assume_init_drop() };
                self.present[i] = 0;
            }
        }
        self.active = 0;
    }

    /// Re-prime the tournament in place from a per-slot `pull` closure, reusing
    /// the existing storage with NO reallocation. Drops any present leaves first
    /// (via [`Self::clear`]), then pulls a fresh head for each of the `n_sources`
    /// slots and rebuilds the tree.
    ///
    /// `pull(slot)` returns the new head value for `slot`, or `None` if that slot
    /// is exhausted at the new position. The slot count is unchanged from
    /// construction, so the power-of-two `cap` and all backing `Vec`s are reused
    /// as-is.
    #[expect(
        clippy::indexing_slicing,
        reason = "slot < n_sources <= leaves.len() == present.len() by construction"
    )]
    pub fn refill_with(&mut self, mut pull: impl FnMut(usize) -> Option<E>) {
        self.clear();
        for slot in 0..self.n_sources {
            if let Some(v) = pull(slot) {
                self.leaves[slot] = MaybeUninit::new(v);
                self.present[slot] = 1;
                self.active += 1;
            }
        }
        // Tree storage (size `cap`) is reused; build_subtree overwrites every
        // internal node and `tree[0]`.
        self.build_subtree(1, 0, self.leaves.len());
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
        if self.present[idx] == 0 {
            return None;
        }
        // SAFETY: `present[idx] != 0` is the LoserTree invariant
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
            self.present[slot] != 0,
            "LoserTree winner slot must be present when winner_slot() returns Some"
        );
        // SAFETY: `present[slot] != 0` (asserted by winner_slot
        // returning Some plus the LoserTree invariant). The old
        // value is read out and the new is written in its place;
        // `present[slot]` stays non-zero so no byte-map update
        // needed.
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
            self.present[slot] != 0,
            "LoserTree winner slot must be present when winner_slot() returns Some"
        );
        // SAFETY: present[slot] != 0 (LoserTree invariant for the
        // winner slot). Reading via `replace(_, uninit)` returns
        // the initialised value and leaves the slot as
        // MaybeUninit::uninit; we then flip the byte-map so
        // subsequent code (cmp_indices, peek_min, Drop) sees it
        // as absent.
        let old = unsafe {
            core::mem::replace(&mut self.leaves[slot], MaybeUninit::uninit()).assume_init()
        };
        self.present[slot] = 0;
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
        if slot >= self.leaves.len() || self.present[slot] == 0 {
            return None;
        }
        // SAFETY: present[slot] != 0 (just checked above).
        let taken = unsafe {
            core::mem::replace(&mut self.leaves[slot], MaybeUninit::uninit()).assume_init()
        };
        self.present[slot] = 0;
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
        match (self.present[a] != 0, self.present[b] != 0) {
            (true, true) => {
                // SAFETY: both bytes non-zero ⇒ both leaves
                // initialised (LoserTree invariant maintained by
                // build/replace/pop/take_slot).
                let va = unsafe { self.leaves[a].assume_init_ref() };
                let vb = unsafe { self.leaves[b].assume_init_ref() };
                self.cmp.compare(va, vb)
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
mod tests;

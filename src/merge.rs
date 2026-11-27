// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use crate::InternalValue;
use crate::comparator::SharedComparator;
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};
use core::cmp::Ordering;

type IterItem = crate::Result<InternalValue>;

pub type BoxedIterator<'a> = Box<dyn DoubleEndedIterator<Item = IterItem> + Send + 'a>;

/// The rank of a slot that holds no entry: above every key word, so the slot
/// loses to any that holds one.
const EMPTY: u128 = 1 << 64;

/// How long the winning input has been winning.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Run {
    /// It took over from another input, or nothing is known.
    None,
    /// It won again once, through its games.
    Repeated,
    /// It won again twice: its runner-up is known.
    Chased,
}

/// Merges multiple KV iterators into a single sorted stream, front to back.
///
/// The inputs' current entries play a tournament: `tree[0]` names the slot of
/// the smallest, and every other node the slot that lost the game played
/// there. Taking the smallest replaces it with its input's next entry and
/// plays again only the games between its leaf and the root, one per level,
/// whichever input the next entry comes from. An input that wins again twice
/// in a row has its runner-up found, and each further entry of its run costs
/// one game against that runner-up.
///
/// Most games are settled by a slot's rank alone: eight bytes of its key past
/// the prefix every key shares, as an integer. Whole keys are compared only
/// where two ranks tie.
///
/// When two entries have the same user key and sequence number, the
/// entry from the iterator with the **lower index** (earlier position
/// in the `iterators` vec) sorts first.  Callers that need "newer
/// wins" semantics (e.g. compaction) must pass sources in
/// newest-first order.
pub struct Merger<I> {
    iterators: Vec<I>,
    /// Each input's current entry; `None` once it ran out, and in the slots
    /// that pad the inputs to a power of two.
    entries: Vec<Option<InternalValue>>,
    /// Each slot's key word, or [`EMPTY`].
    ranks: Vec<u128>,
    /// `tree[0]` is the winning slot, `tree[node]` the slot that lost at
    /// `node`.
    tree: Vec<u32>,
    /// Slots that hold an entry.
    live: usize,
    /// While the run is [`Run::Chased`]: the best of the slots that lost to
    /// the winner on its path. A next entry of the winning input that comes
    /// before this slot's wins again with the tree as it stands, so no game
    /// is replayed for it.
    runner_up: u32,
    run: Run,
    /// Errors the inputs' first entries gave, yielded before any entry.
    failed: Vec<crate::Error>,
    /// The tournament is built and no error waits.
    ready: bool,
    comparator: SharedComparator,
    /// The comparator orders keys as their bytes do, so keys are compared
    /// directly rather than through it. See
    /// [`crate::comparator::UserComparator::is_lexicographic`].
    lexicographic: bool,
    /// Bytes every key of this merge starts with, which no comparison needs
    /// to look at.
    common_prefix: usize,
}

/// The eight bytes of `key` past `offset`, as a big-endian integer padded with
/// zeros, so two words that differ order their keys as their bytes do: a word
/// only ties, and a tie falls back to the keys. Zero unless keys are ordered by
/// their bytes, which ties every comparison.
#[inline]
fn key_word(key: &[u8], offset: usize, lexicographic: bool) -> u64 {
    if !lexicographic {
        return 0;
    }
    let tail = key.get(offset..).unwrap_or_default();
    if let Some(head) = tail.first_chunk::<8>() {
        return u64::from_be_bytes(*head);
    }
    let mut padded = [0; 8];
    if let Some(head) = padded.get_mut(..tail.len()) {
        head.copy_from_slice(tail);
    }
    u64::from_be_bytes(padded)
}

impl<I: Iterator<Item = IterItem>> Merger<I> {
    #[must_use]
    pub fn new(iterators: Vec<I>, comparator: SharedComparator) -> Self {
        let lexicographic = comparator.is_lexicographic();
        Self {
            iterators,
            entries: Vec::new(),
            ranks: Vec::new(),
            tree: Vec::new(),
            live: 0,
            runner_up: 0,
            run: Run::None,
            failed: Vec::new(),
            ready: false,
            comparator,
            lexicographic,
            common_prefix: 0,
        }
    }

    /// Declares that every key the sources hold starts with the same `len`
    /// bytes, under a comparator that orders keys by their bytes, or is
    /// ignored under any other.
    #[must_use]
    pub fn with_common_prefix(mut self, len: usize) -> Self {
        debug_assert!(self.tree.is_empty(), "set before the first entry is taken");
        self.common_prefix = len;
        self
    }

    fn rank_of(&self, entry: &InternalValue) -> u128 {
        debug_assert!(
            !self.lexicographic || entry.key.user_key.len() >= self.common_prefix,
            "a key shorter than the prefix every key is said to start with",
        );
        u128::from(key_word(
            &entry.key.user_key,
            self.common_prefix,
            self.lexicographic,
        ))
    }

    /// Whether the entry in slot `a` comes before the one in slot `b`. An
    /// empty slot comes after every entry.
    // Always inline: the ranks decide most games, and a game that stays a
    // call from the replay loop costs more than it compares. Several callers
    // would otherwise leave it one.
    #[expect(
        clippy::inline_always,
        reason = "two integer loads and a compare, on every game of every replay"
    )]
    #[inline(always)]
    #[expect(
        clippy::indexing_slicing,
        reason = "the tree only holds numbers of slots"
    )]
    fn beats(&self, a: usize, b: usize) -> bool {
        #[cfg(feature = "diagnostics")]
        crate::diagnostics::add(&crate::diagnostics::MERGE_WORD_COMPARES, 1);
        let (rank_a, rank_b) = (self.ranks[a], self.ranks[b]);
        if rank_a != rank_b {
            return rank_a < rank_b;
        }
        self.beats_by_key(a, b)
    }

    /// [`Self::beats`] for two slots of one rank: by their keys, then by
    /// their numbers.
    #[inline]
    #[expect(
        clippy::indexing_slicing,
        reason = "the tree only holds numbers of slots"
    )]
    fn beats_by_key(&self, a: usize, b: usize) -> bool {
        let (Some(x), Some(y)) = (&self.entries[a], &self.entries[b]) else {
            // Two empty slots: neither comes first.
            return false;
        };
        #[cfg(feature = "diagnostics")]
        crate::diagnostics::add(&crate::diagnostics::MERGE_KEY_COMPARES, 1);
        let keys = if self.lexicographic {
            x.key.cmp(&y.key)
        } else {
            x.key.compare_with(&y.key, self.comparator.as_ref())
        };
        keys.then_with(|| a.cmp(&b)) == Ordering::Less
    }

    /// Takes each input's first entry and plays the whole tournament.
    #[expect(
        clippy::indexing_slicing,
        reason = "slots and nodes are below the lengths set just above"
    )]
    #[expect(
        clippy::cast_possible_truncation,
        reason = "a merge has far fewer than 2^32 inputs"
    )]
    fn build(&mut self) {
        let leaves = self.iterators.len().next_power_of_two().max(2);
        self.entries = (0..leaves).map(|_| None).collect();
        self.ranks = alloc::vec![EMPTY; leaves];
        for slot in 0..self.iterators.len() {
            match self.iterators[slot].next() {
                Some(Ok(entry)) => {
                    self.ranks[slot] = self.rank_of(&entry);
                    self.entries[slot] = Some(entry);
                    self.live += 1;
                }
                Some(Err(e)) => self.failed.push(e),
                None => {}
            }
        }
        // The winner of each subtree by node number, the leaves from `leaves`
        // up.
        let mut winners: Vec<u32> = (0..2 * leaves)
            .map(|node| node.saturating_sub(leaves) as u32)
            .collect();
        self.tree = alloc::vec![0; leaves];
        for node in (1..leaves).rev() {
            let (left, right) = (winners[2 * node], winners[2 * node + 1]);
            let (winner, loser) = if self.beats(right as usize, left as usize) {
                (right, left)
            } else {
                (left, right)
            };
            self.tree[node] = loser;
            winners[node] = winner;
        }
        self.tree[0] = winners[1];
    }

    /// Plays again the games between `leaf` and the root.
    #[inline]
    #[expect(
        clippy::indexing_slicing,
        reason = "a node between a leaf and the root is below the tree's length"
    )]
    #[expect(
        clippy::cast_possible_truncation,
        reason = "a merge has far fewer than 2^32 inputs"
    )]
    fn replay(&mut self, leaf: usize) {
        let mut winner = leaf;
        let mut node = usize::midpoint(self.tree.len(), leaf);
        while node != 0 {
            let other = self.tree[node] as usize;
            if self.beats(other, winner) {
                #[cfg(feature = "diagnostics")]
                crate::diagnostics::add(&crate::diagnostics::MERGE_MOVES, 1);
                self.tree[node] = winner as u32;
                winner = other;
            }
            node >>= 1;
        }
        self.tree[0] = winner as u32;
    }

    /// Plays the tournament on without `slot`, whose input gave its last
    /// entry.
    #[cold]
    #[expect(
        clippy::indexing_slicing,
        reason = "the slot is an input's, below the ranks' length"
    )]
    fn retire(&mut self, slot: usize) {
        self.ranks[slot] = EMPTY;
        self.live -= 1;
        self.run = Run::None;
        self.replay(slot);
    }

    /// The best of the slots that lost on the path from `leaf` to the root:
    /// the slot that wins once `leaf`'s input stops winning.
    #[expect(
        clippy::indexing_slicing,
        reason = "a node between a leaf and the root is below the tree's length"
    )]
    fn path_best(&self, leaf: usize) -> u32 {
        let mut node = usize::midpoint(self.tree.len(), leaf);
        let mut best = self.tree[node];
        node >>= 1;
        while node != 0 {
            let loser = self.tree[node];
            if self.beats(loser as usize, best as usize) {
                best = loser;
            }
            node >>= 1;
        }
        best
    }

    /// Builds the tournament at the first call, and yields the errors the
    /// inputs' first entries gave, one a call.
    #[cold]
    fn prepare(&mut self) -> Option<crate::Error> {
        // Built once: the tree of a built tournament has its two nodes at
        // least.
        if self.tree.is_empty() {
            self.build();
        }
        let failed = self.failed.pop();
        self.ready = self.failed.is_empty();
        failed
    }
}

impl<I: Iterator<Item = IterItem>> Iterator for Merger<I> {
    type Item = IterItem;

    #[inline]
    #[expect(
        clippy::indexing_slicing,
        reason = "the winning slot is an input's, below every per-slot length"
    )]
    fn next(&mut self) -> Option<Self::Item> {
        // One source is already in order: a flush of one memtable would move
        // every entry through the tournament and back for nothing.
        if let [only] = self.iterators.as_mut_slice() {
            return only.next();
        }
        if !self.ready
            && let Some(e) = self.prepare()
        {
            return Some(Err(e));
        }
        if self.live == 0 {
            return None;
        }

        let slot = self.tree[0] as usize;
        let taken = match self.iterators[slot].next() {
            Some(Ok(next)) => {
                self.ranks[slot] = self.rank_of(&next);
                self.entries[slot].replace(next)
            }
            // The entry the input stood at is dropped with it, so the next
            // call makes progress on another input instead of asking this one
            // again.
            Some(Err(e)) => {
                self.entries[slot] = None;
                self.retire(slot);
                return Some(Err(e));
            }
            None => {
                let last = self.entries[slot].take();
                self.retire(slot);
                return last.map(Ok);
            }
        };
        // Ahead of the runner-up, the input's next entry is ahead of every
        // slot that lost on its path: the tree stands.
        if self.run == Run::Chased && self.beats(slot, self.runner_up as usize) {
            return taken.map(Ok);
        }
        let before = self.run;
        self.replay(slot);
        // The same input winning through its games twice in a row is a run:
        // its runner-up is then worth one walk of the path, after which each
        // entry of the run costs one game. Once is not: inputs that take
        // turns by chance repeat that often, and the walk would be wasted. A
        // run that just ended left the runner-up the winner.
        self.run = if self.tree[0] as usize != slot {
            Run::None
        } else if before == Run::None {
            Run::Repeated
        } else {
            self.runner_up = self.path_best(slot);
            Run::Chased
        };
        taken.map(Ok)
    }
}

#[cfg(test)]
#[allow(clippy::unnecessary_wraps)]
mod tests;

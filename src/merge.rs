// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use crate::InternalValue;
use crate::comparator::SharedComparator;
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};
use core::cell::Cell;
use core::cmp::Ordering;

type IterItem = crate::Result<InternalValue>;

pub type BoxedIterator<'a> = Box<dyn DoubleEndedIterator<Item = IterItem> + Send + 'a>;

/// The rank of a slot that holds no entry: the greatest key word, so the slot
/// loses to every entry of a lesser word by the ranks alone. An entry whose
/// own word is the greatest ties with it, and the tie is settled by which of
/// the two slots holds an entry.
const EMPTY: u64 = u64::MAX;

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

/// Where a merger stands between two calls.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// No input has been asked for an entry yet.
    Unbuilt,
    /// The tournament is built and errors the inputs' first entries gave wait
    /// to be yielded.
    Failing,
    /// The tournament is built and no error waits.
    Ready,
    /// A panic passed through the merger while its tournament was part way
    /// through a change: the tournament and the inputs no longer agree, and
    /// every further call is refused.
    Unwound,
}

/// Marks a merger [`Phase::Unwound`] unless the call it covers returns.
///
/// Held over a call of the user's comparator, which can panic while the
/// tournament is part way through a change, and over the pull of an input,
/// which can panic having moved past an entry: a caller that catches the
/// panic and asks the merger again would otherwise get entries out of order,
/// one missing, or a clean end with entries still waiting. A call that
/// returns forgets the guard, so nothing is stored for it.
struct Unwinding<'a>(&'a Cell<Phase>);

impl Unwinding<'_> {
    /// The call returned: the merger stays as it is.
    #[inline]
    fn disarm(self) {
        core::mem::forget(self);
    }
}

impl Drop for Unwinding<'_> {
    fn drop(&mut self) {
        self.0.set(Phase::Unwound);
    }
}

/// The error of a call to a merger a panic left part way through a change.
fn unwound() -> crate::Error {
    crate::Error::Io(crate::io::Error::new(
        crate::io::ErrorKind::Other,
        "an earlier step of this merge panicked",
    ))
}

/// What a merger did, counted under the `diagnostics` feature for benches.
#[cfg(feature = "diagnostics")]
#[derive(Clone, Copy, Debug, Default)]
pub struct MergeWork {
    /// Games played by two slots' key words.
    pub word_compares: u64,
    /// Games settled by two whole keys, where their words tied.
    pub key_compares: u64,
    /// Games a replayed entry lost, each rewriting one node of the
    /// tournament.
    pub moves: u64,
}

/// Adds one to a counter of the `diagnostics` build.
#[cfg(feature = "diagnostics")]
#[inline]
fn count(counter: &Cell<u64>) {
    counter.set(counter.get() + 1);
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
///
/// A panic of an input on its first entry, or of the comparator, leaves the
/// merger refusing: a caller that catches it and asks again gets an error,
/// never entries with one missing. A panic of an input on a later entry
/// changes nothing, since its entry is taken only once the input returned.
pub struct Merger<I> {
    iterators: Vec<I>,
    /// Each input's current entry; `None` once it ran out, and in the slots
    /// that pad the inputs to a power of two.
    entries: Vec<Option<InternalValue>>,
    /// Each slot's key word, or [`EMPTY`].
    ranks: Vec<u64>,
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
    /// Errors the inputs' first entries gave, the last input's first: yielded
    /// from the back, before any entry.
    failed: Vec<crate::Error>,
    /// In a cell so a game, which only reads the merger, can mark it unwound.
    phase: Cell<Phase>,
    comparator: SharedComparator,
    /// The comparator orders keys as their bytes do, so keys are compared
    /// directly rather than through it. See
    /// [`crate::comparator::UserComparator::is_lexicographic`].
    lexicographic: bool,
    /// Bytes every key of this merge starts with, which no comparison needs
    /// to look at.
    common_prefix: usize,
    #[cfg(feature = "diagnostics")]
    word_compares: Cell<u64>,
    #[cfg(feature = "diagnostics")]
    key_compares: Cell<u64>,
    #[cfg(feature = "diagnostics")]
    moves: Cell<u64>,
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
    for (slot, byte) in padded.iter_mut().zip(tail) {
        *slot = *byte;
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
            phase: Cell::new(Phase::Unbuilt),
            comparator,
            lexicographic,
            common_prefix: 0,
            #[cfg(feature = "diagnostics")]
            word_compares: Cell::new(0),
            #[cfg(feature = "diagnostics")]
            key_compares: Cell::new(0),
            #[cfg(feature = "diagnostics")]
            moves: Cell::new(0),
        }
    }

    /// Declares that every key the sources hold starts with the same `len`
    /// bytes, under a comparator that orders keys by their bytes, or is
    /// ignored under any other.
    #[must_use]
    pub fn with_common_prefix(mut self, len: usize) -> Self {
        debug_assert!(
            self.phase.get() == Phase::Unbuilt,
            "set before the first entry is taken"
        );
        self.common_prefix = len;
        self
    }

    /// What this merger did so far.
    #[cfg(feature = "diagnostics")]
    #[must_use]
    pub fn work(&self) -> MergeWork {
        MergeWork {
            word_compares: self.word_compares.get(),
            key_compares: self.key_compares.get(),
            moves: self.moves.get(),
        }
    }

    fn rank_of(&self, entry: &InternalValue) -> u64 {
        debug_assert!(
            !self.lexicographic || entry.key.user_key.len() >= self.common_prefix,
            "a key shorter than the prefix every key is said to start with",
        );
        key_word(&entry.key.user_key, self.common_prefix, self.lexicographic)
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
        count(&self.word_compares);
        let (rank_a, rank_b) = (self.ranks[a], self.ranks[b]);
        if rank_a != rank_b {
            return rank_a < rank_b;
        }
        self.beats_by_key(a, b)
    }

    /// [`Self::beats`] for two slots of one rank: by their keys, then by
    /// their numbers. A slot that holds an entry comes before an empty one,
    /// which an entry of the greatest key word ties with by rank.
    #[inline]
    #[expect(
        clippy::indexing_slicing,
        reason = "the tree only holds numbers of slots"
    )]
    fn beats_by_key(&self, a: usize, b: usize) -> bool {
        let (x, y) = match (&self.entries[a], &self.entries[b]) {
            (Some(x), Some(y)) => (x, y),
            (Some(_), None) => return true,
            // An empty slot comes before nothing.
            (None, _) => return false,
        };
        #[cfg(feature = "diagnostics")]
        count(&self.key_compares);
        let keys = if self.lexicographic {
            // Bytes compared in place: nothing here can panic.
            x.key.cmp(&y.key)
        } else {
            let unwinding = Unwinding(&self.phase);
            let keys = x.key.compare_with(&y.key, self.comparator.as_ref());
            unwinding.disarm();
            keys
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
        // Yielded from the back, the first input's first.
        self.failed.reverse();
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
    // Always inline: the step calls it once per entry, and with the cold
    // path calling it too the hint alone leaves it a call, 11% of the
    // merger's cycles on two inputs that take turns.
    #[expect(
        clippy::inline_always,
        reason = "one loop of a few games, called once per merged entry"
    )]
    #[inline(always)]
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
                count(&self.moves);
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
    /// inputs' first entries gave, one a call and in the order of the inputs.
    /// A merger a panic left part way through a change yields its refusal.
    #[cold]
    fn prepare(&mut self) -> Option<crate::Error> {
        match self.phase.get() {
            Phase::Unbuilt => {
                // Unwound until built: a panic of an input or of the
                // comparator in there leaves entries taken and no tournament
                // over them.
                self.phase.set(Phase::Unwound);
                self.build();
            }
            Phase::Unwound => return Some(unwound()),
            Phase::Failing | Phase::Ready => {}
        }
        let failed = self.failed.pop();
        self.phase.set(if self.failed.is_empty() {
            Phase::Ready
        } else {
            Phase::Failing
        });
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
            if self.phase.get() == Phase::Unwound {
                return Some(Err(unwound()));
            }
            let pulling = Unwinding(&self.phase);
            let item = only.next();
            pulling.disarm();
            return item;
        }
        if self.phase.get() != Phase::Ready
            && let Some(e) = self.prepare()
        {
            return Some(Err(e));
        }
        if self.live == 0 {
            return None;
        }

        let slot = self.tree[0] as usize;
        // An input that panics here leaves the merger's own state as it was,
        // its entry taken out of the slot only below, but not its own: it
        // may have moved past an entry it never returned, and asked again
        // would go on from the one after. The merger refuses from then on.
        let pulling = Unwinding(&self.phase);
        let pulled = self.iterators[slot].next();
        pulling.disarm();
        let taken = match pulled {
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
        //
        // Two inputs never chase: their replay is one game already, against
        // the same slot a chase would play, and the game a chase loses would
        // only be played again by the replay after it.
        self.run = if self.tree[0] as usize != slot {
            Run::None
        } else if before == Run::None || self.tree.len() == 2 {
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

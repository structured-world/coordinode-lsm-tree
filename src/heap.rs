// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Custom merge heap backed by a sorted vector of slot numbers.
//!
//! Supports both min and max extraction (for forward and reverse
//! iteration) on the same data structure, unlike two separate heaps.
//!
//! The key optimisation is `replace_min` / `replace_max`: replacing the
//! extremum in its slot and moving its number to the sorted position.  In
//! the common case — a sequential scan where the same source keeps
//! winning — the replacement is still the extremum and the operation
//! completes in **one comparison** (O(1)), which a tournament tree cannot
//! match.  Inputs that take turns cost O(log n) comparisons: the position
//! is found by binary search, and only the slot numbers move, never the
//! entries.

use crate::InternalValue;
use crate::comparator::SharedComparator;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::cmp::Ordering;

/// A single entry in the merge heap.
///
/// Comparator is stored once in the heap, not per entry — eliminating
/// the per-item `Arc` clone that the old `HeapItem` required.
pub struct HeapEntry {
    index: usize,
    value: InternalValue,
}

impl HeapEntry {
    pub fn new(index: usize, value: InternalValue) -> Self {
        Self { index, value }
    }

    #[inline]
    pub fn index(&self) -> usize {
        self.index
    }

    pub fn into_value(self) -> InternalValue {
        self.value
    }
}

// ---------------------------------------------------------------------------
// MergeHeap
// ---------------------------------------------------------------------------

/// Merge heap backed by a sorted vector of slot numbers.
///
/// Entries stay in `slots`; `order` lists the occupied slots in ascending
/// entry order, so `order[0]` is the minimum and `order[last]` the maximum.
/// This makes both `pop_min` / `pop_max` and `replace_min` / `replace_max`
/// straightforward, and an entry never moves while it is in the heap.
pub struct MergeHeap {
    slots: Vec<Option<HeapEntry>>,
    /// Slots a pop emptied, for the next push.
    free: Vec<u32>,
    order: Vec<u32>,
    comparator: SharedComparator,
    /// The comparator orders keys as their bytes do, so keys are compared
    /// directly rather than through it. See
    /// [`crate::comparator::UserComparator::is_lexicographic`].
    lexicographic: bool,
    /// Each slot's [`key_word`], so most comparisons stop at two integers
    /// beside `order` and never reach the entries, let alone their keys.
    words: Vec<u64>,
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

/// The order of two entries.
///
/// Ties (same user key + same seqno) are broken by source index,
/// with lower indices sorting first.  This ensures deterministic
/// merge order; callers that need "newer wins" semantics must pass
/// sources in newest-first precedence order.
#[inline]
fn compare(
    a: &HeapEntry,
    b: &HeapEntry,
    lexicographic: bool,
    comparator: &SharedComparator,
) -> Ordering {
    let keys = if lexicographic {
        a.value.key.cmp(&b.value.key)
    } else {
        a.value.key.compare_with(&b.value.key, comparator.as_ref())
    };
    keys.then_with(|| a.index.cmp(&b.index))
}

/// The entry in `slot`, which `order` lists as occupied.
#[inline]
#[expect(
    clippy::indexing_slicing,
    reason = "`order` only holds numbers of slots"
)]
#[expect(clippy::expect_used, reason = "`order` only holds occupied slots")]
fn occupied(slots: &[Option<HeapEntry>], slot: u32) -> &HeapEntry {
    slots[slot as usize]
        .as_ref()
        .expect("an ordered slot holds an entry")
}

impl MergeHeap {
    /// Creates an empty heap pre-allocated for `cap` entries.
    pub fn with_capacity(cap: usize, comparator: SharedComparator) -> Self {
        let lexicographic = comparator.is_lexicographic();
        Self {
            slots: Vec::with_capacity(cap),
            free: Vec::with_capacity(cap),
            order: Vec::with_capacity(cap),
            comparator,
            lexicographic,
            words: Vec::with_capacity(cap),
            common_prefix: 0,
        }
    }

    /// Declares that every key this merge will hold starts with the same
    /// `len` bytes, which the comparisons then skip.
    ///
    /// Must be set before the first push.
    pub fn use_common_prefix(&mut self, len: usize) {
        debug_assert!(self.order.is_empty(), "set before the first push");
        self.common_prefix = len;
    }

    fn word_of(&self, entry: &HeapEntry) -> u64 {
        debug_assert!(
            !self.lexicographic || entry.value.key.user_key.len() >= self.common_prefix,
            "a key shorter than the prefix every key is said to start with",
        );
        key_word(
            &entry.value.key.user_key,
            self.common_prefix,
            self.lexicographic,
        )
    }

    #[inline]
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    /// Returns a reference to the minimum (first) entry.
    #[inline]
    pub fn peek_min(&self) -> Option<&HeapEntry> {
        let slot = *self.order.first()?;
        Some(occupied(&self.slots, slot))
    }

    /// Returns a reference to the maximum (last) entry.
    #[inline]
    pub fn peek_max(&self) -> Option<&HeapEntry> {
        let slot = *self.order.last()?;
        Some(occupied(&self.slots, slot))
    }

    /// Inserts a new entry, maintaining sorted order.
    #[expect(
        clippy::indexing_slicing,
        reason = "a free slot is a slot; the new one is pushed just before"
    )]
    #[expect(
        clippy::cast_possible_truncation,
        reason = "the heap holds at most two entries per source"
    )]
    pub fn push(&mut self, entry: HeapEntry) {
        // A lone entry's word is not kept up: nothing compares it.
        if let [lone] = *self.order.as_slice() {
            self.words[lone as usize] = self.word_of(occupied(&self.slots, lone));
        }
        let word = self.word_of(&entry);
        let pos = self.order.partition_point(|&slot| {
            let held = occupied(&self.slots, slot);
            compare(held, &entry, self.lexicographic, &self.comparator) != Ordering::Greater
        });
        let slot = if let Some(slot) = self.free.pop() {
            self.slots[slot as usize] = Some(entry);
            self.words[slot as usize] = word;
            slot
        } else {
            self.slots.push(Some(entry));
            self.words.push(word);
            (self.slots.len() - 1) as u32
        };
        self.order.insert(pos, slot);
    }

    /// The order of the entry in `slot`, whose word is `word`, against the one
    /// in `held`: by their words, and by the entries when those tie.
    #[inline]
    #[expect(
        clippy::indexing_slicing,
        reason = "`order` only holds numbers of slots"
    )]
    fn order_against(&self, slot: u32, word: u64, held: u32) -> Ordering {
        match word.cmp(&self.words[held as usize]) {
            Ordering::Equal => compare(
                occupied(&self.slots, slot),
                occupied(&self.slots, held),
                self.lexicographic,
                &self.comparator,
            ),
            order => order,
        }
    }

    /// Empties `slot` for a later push.
    #[expect(
        clippy::indexing_slicing,
        reason = "`order` only holds numbers of slots"
    )]
    fn release(&mut self, slot: u32) -> Option<HeapEntry> {
        self.free.push(slot);
        self.slots[slot as usize].take()
    }

    /// Removes and returns the minimum entry.
    pub fn pop_min(&mut self) -> Option<HeapEntry> {
        if self.order.is_empty() {
            return None;
        }
        let slot = self.order.remove(0);
        self.release(slot)
    }

    /// Removes and returns the maximum entry.
    pub fn pop_max(&mut self) -> Option<HeapEntry> {
        let slot = self.order.pop()?;
        self.release(slot)
    }

    /// Replaces the minimum entry and moves the replacement to its
    /// sorted position.
    ///
    /// Returns the old minimum.  In the common case (replacement is
    /// still the minimum), this completes in **one comparison**.
    ///
    /// # Panics
    ///
    /// Panics if the heap is empty.
    #[expect(
        clippy::indexing_slicing,
        reason = "an empty heap panics by contract; the rest is behind the length guard"
    )]
    #[expect(clippy::expect_used, reason = "`order` only holds occupied slots")]
    pub fn replace_min(&mut self, entry: HeapEntry) -> HeapEntry {
        let slot = self.order[0];
        let held = self.slots[slot as usize]
            .as_mut()
            .expect("an ordered slot holds an entry");
        let old = core::mem::replace(held, entry);

        if self.order.len() == 1 {
            return old;
        }
        let word = self.word_of(occupied(&self.slots, slot));
        self.words[slot as usize] = word;
        if self.order_against(slot, word, self.order[1]) != Ordering::Greater {
            return old;
        }

        // Past `order[1]`: the number moves to behind every smaller entry.
        let rest = self.order[2..]
            .partition_point(|&held| self.order_against(slot, word, held) == Ordering::Greater);
        let end = 2 + rest;
        self.order.copy_within(1..end, 0);
        self.order[end - 1] = slot;

        old
    }

    /// Replaces the maximum entry and moves the replacement to its
    /// sorted position.
    ///
    /// Returns the old maximum.  In the common case (replacement is
    /// still the maximum), this completes in **one comparison**.
    ///
    /// # Panics
    ///
    /// Panics if the heap is empty.
    #[expect(
        clippy::indexing_slicing,
        reason = "an empty heap panics by contract; the rest is behind the length guard"
    )]
    #[expect(clippy::expect_used, reason = "`order` only holds occupied slots")]
    pub fn replace_max(&mut self, entry: HeapEntry) -> HeapEntry {
        let last = self.order.len() - 1;
        let slot = self.order[last];
        let held = self.slots[slot as usize]
            .as_mut()
            .expect("an ordered slot holds an entry");
        let old = core::mem::replace(held, entry);

        if last == 0 {
            return old;
        }
        let word = self.word_of(occupied(&self.slots, slot));
        self.words[slot as usize] = word;
        if self.order_against(slot, word, self.order[last - 1]) != Ordering::Less {
            return old;
        }

        // Before `order[last - 1]`: the number moves to ahead of every greater
        // entry.
        let pos = self.order[..last - 1]
            .partition_point(|&held| self.order_against(slot, word, held) != Ordering::Less);
        self.order.copy_within(pos..last, pos + 1);
        self.order[pos] = slot;

        old
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test assertions use unwrap for brevity")]
mod tests;

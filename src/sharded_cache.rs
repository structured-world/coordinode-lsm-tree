// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! In-tree sharded cache backing the block/blob cache ([`crate::Cache`]) and
//! the file-descriptor cache ([`crate::descriptor_table`]).
//!
//! One implementation serves `std` and `no_std + alloc` alike:
//!
//! - **Eviction:** per-shard byte-weighted **S3-FIFO** (small / main / ghost
//!   FIFO queues + a 2-bit frequency counter). A *read* only raises the
//!   entry's frequency and never reorders a queue (promotion small→main
//!   happens lazily at eviction time, keyed on that counter), unlike LRU
//!   which must move the entry to the MRU end on every hit.
//! - **Concurrency:** N shards (key hash → shard). A read takes no lock: it
//!   finds its entry in the shard's table and holds the entry's slot for as
//!   long as it clones the value (see [`table`]). Inserts, removals and
//!   eviction of one shard take turns behind that shard's lock, which holds
//!   the queues and the ghost set. A reader waits for it in one case: while
//!   a writer rebuilds the shard's table to a new size, a read that finds the
//!   rebuild under way waits behind the lock for it to end instead of
//!   spinning. Each shard's
//!   resident weight and entry count are atomics, so `weight()` and `len()`
//!   take no lock either.

mod table;

use alloc::collections::VecDeque;
use alloc::vec::Vec;
use core::hash::{BuildHasher, Hash};
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use portable_atomic::AtomicU64;
use table::{Node, Queue, Table, Write};

/// Maximum value of an entry's frequency counter (2-bit, S3-FIFO).
const MAX_FREQ: u8 = 3;

/// The lightest entry kept as a large one while a shard could still take it
/// (see `ShardedCache::is_large`): above the blocks tables are written in,
/// at the size of a blob frame.
const LARGE_FLOOR: u64 = 64 * 1024;

/// Assigns a byte weight to a cached `(key, value)` pair. The cache evicts to
/// keep the summed weight under its capacity.
pub(crate) trait Weighter<K, V> {
    /// Weight of this entry in the same unit as the cache's capacity (bytes for
    /// the block/blob cache, a unit count for the descriptor cache).
    fn weight(&self, key: &K, value: &V) -> u64;
}

/// A [`Weighter`] that assigns every entry a weight of 1, turning a byte-capacity
/// cache into a count-capacity (max-entries) cache. Used by the descriptor cache.
#[derive(Clone, Copy, Default)]
pub(crate) struct UnitWeighter;

impl<K, V> Weighter<K, V> for UnitWeighter {
    #[inline]
    fn weight(&self, _: &K, _: &V) -> u64 {
        1
    }
}

/// Admission priority for an inserted entry.
///
/// [`Priority::High`] entries skip the probationary *small* queue and enter the
/// *main* queue directly with a full frequency credit, so they survive several
/// eviction passes of churn through the small queue without first being
/// cold-evicted. Used to pin frequently-touched index / filter / range-tombstone
/// blocks against data-block churn when the working set exceeds the cache. Still
/// bounded: `evict_from_main` decrements the credit, so a high-priority entry can
/// eventually evict if such entries alone overflow capacity.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Priority {
    /// Normal admission: ghost-hit → main, otherwise the probationary small queue.
    Normal,
    /// Pinned admission: straight to main with a full frequency credit.
    High,
    /// Probationary admission: the small queue even on a ghost hit, so the
    /// entry reaches main only by being read again there. For entries one
    /// read each, like a separated value, whose stream must not push the
    /// main queue's blocks out.
    Probation,
}

/// 64-byte-aligned wrapper so adjacent shards land on separate cache lines
/// (no false sharing between shards under concurrent access).
#[repr(align(64))]
struct Padded<T>(T);

/// Per-shard S3-FIFO state, held behind the shard's table lock. Byte-weighted:
/// `small_bytes + main_bytes` is the resident weight, kept at or below
/// `capacity` by eviction. An entry taken out of the table stays in its FIFO
/// queue as a stale node, skipped when popped during eviction.
struct Policy {
    small: VecDeque<Node>,
    main: VecDeque<Node>,
    /// Fingerprints (the wrapper's precomputed key hashes) of keys evicted
    /// cold or missed into a full shard, oldest first. The hash is already
    /// mixed, so `ghost_set` is keyed by it as is: no key clone, no rehash.
    ghost: VecDeque<u64>,
    ghost_set: hashbrown::HashTable<u64>,
    small_bytes: u64,
    main_bytes: u64,
    /// Per-shard byte capacity.
    capacity: u64,
    /// This shard's share of what the cache's large entries weigh: bytes of
    /// `capacity` lent to them, as of the writer holding the lock.
    lent: u64,
    /// Weight of the large entries this lock's holder took out, owed back to
    /// the cache's large tally when the lock is released.
    large_released: u64,
    /// Target ceiling for the small queue (~10% of `capacity`); when the small
    /// queue exceeds it, eviction prefers the small queue.
    small_target: u64,
    /// Max number of fingerprints retained in the ghost queue.
    ghost_capacity: usize,
    /// Whether the shard has ever had to evict to stay within capacity: its
    /// table and queues have grown to what it holds, and an insert now takes
    /// the place of what it evicts rather than fresh memory.
    filled: bool,
    /// Turns of the main queue that spared an entry in the eviction under
    /// way, since one last took an entry out (see
    /// [`Write::evict_from_main`]).
    spared: usize,
}

impl Policy {
    fn new(capacity: u64, ghost_capacity: usize) -> Self {
        Self {
            small: VecDeque::new(),
            main: VecDeque::new(),
            ghost: VecDeque::new(),
            ghost_set: hashbrown::HashTable::new(),
            small_bytes: 0,
            main_bytes: 0,
            capacity,
            lent: 0,
            large_released: 0,
            small_target: small_target(capacity),
            ghost_capacity,
            filled: false,
            spared: 0,
        }
    }

    /// Resident weight of the shard's own queues.
    #[inline]
    fn resident_bytes(&self) -> u64 {
        self.small_bytes + self.main_bytes
    }

    /// What the shard's own queues may hold: its capacity less what it lends
    /// to the cache's large entries, and never less than its main queue
    /// holds. The share lent is taken from the small queue and from free
    /// room only: the main queue holds the index and filter blocks every
    /// read walks and what was read again, which a large entry is not let in
    /// at the cost of.
    #[inline]
    fn limit(&self) -> u64 {
        // Saturating: a resize can leave the share lent above a shrunk
        // capacity until the large entries are trimmed to the new one.
        self.capacity
            .saturating_sub(self.lent)
            .max(self.main_bytes.min(self.capacity))
    }

    /// Queues the node of an entry just written and counts its weight. A
    /// large entry is queued and counted by the cache, not here.
    ///
    /// The push follows the write of the entry with nothing reserved first,
    /// here and where an entry is promoted, evicted or its table rebuilt: no
    /// caller sees the state between the two. A push unwinds only past
    /// `isize::MAX` bytes, which no shard's queue reaches. An allocation
    /// that fails goes to the allocation error handler, not to a panic of
    /// this call: in a program that links `std`, whatever features this
    /// crate is built with, the handler aborts, and a program that does not
    /// has no `catch_unwind` to return from.
    fn enqueue(&mut self, node: Node, queue: Queue, weight: u64) {
        match queue {
            Queue::Main => {
                self.main_bytes += weight;
                self.main.push_back(node);
            }
            Queue::Small => {
                self.small_bytes += weight;
                self.small.push_back(node);
            }
            Queue::Large => {}
        }
    }

    /// Takes the weight of an entry out of its queue's tally.
    fn discount(&mut self, queue: Queue, weight: u64) {
        match queue {
            Queue::Main => self.main_bytes -= weight,
            Queue::Small => self.small_bytes -= weight,
            Queue::Large => self.large_released += weight,
        }
    }

    /// Whether the key hashed to `hash` is remembered in the ghost queue.
    #[inline]
    fn in_ghost(&self, hash: u64) -> bool {
        self.ghost_set.find(hash, |&h| h == hash).is_some()
    }

    /// Forgets `hash` from the ghost set, reporting whether it was there. Its
    /// queue node stays and ages out whatever `hash` is recorded as then: a
    /// fingerprint recorded again while its old node is queued is forgotten
    /// with that node, short of a full stay. Kept so by measurement: holding
    /// it for the full second stay admitted more blocks straight to the main
    /// queue, and a scan over a tree a fifth larger than the cache ran 7%
    /// slower, with no read path faster for it.
    fn take_ghost(&mut self, hash: u64) -> bool {
        match self.ghost_set.find_entry(hash, |&h| h == hash) {
            Ok(entry) => {
                entry.remove();
                true
            }
            Err(_) => false,
        }
    }

    fn push_ghost(&mut self, hash: u64) {
        if self.ghost_capacity == 0 {
            return;
        }
        if let hashbrown::hash_table::Entry::Vacant(slot) =
            self.ghost_set.entry(hash, |&h| h == hash, |&h| h)
        {
            slot.insert(hash);
            self.ghost.push_back(hash);
        }
        while self.ghost.len() > self.ghost_capacity {
            if let Some(old) = self.ghost.pop_front() {
                self.take_ghost(old);
            }
        }
    }
}

/// What a shard's writer does to it, with the shard's lock held.
impl<K: Eq, V> Write<'_, K, V, Policy> {
    /// Inserts or replaces `key`, evicting to stay within capacity.
    fn insert_entry(
        &mut self,
        hash: u64,
        key: K,
        value: V,
        weight: u64,
        priority: Priority,
        evicted: &mut Evicted<V>,
    ) {
        // An entry heavier than the shard can never stay resident: admitting
        // it would evict the shard's whole population and then be evicted
        // itself. Refused here, under the lock, so a resize between the
        // caller's weighing and this insert is honored; the value is dropped
        // with the evicted ones, after the lock.
        if weight > self.policy().capacity {
            evicted.push(value);
            return;
        }
        if let Some(old) = self.find(hash, &key) {
            let (old_weight, old_queue, old_freq) =
                (self.weight(old), self.queue(old), self.freq(old));
            // Honor a High-priority refresh of a probationary (small) entry:
            // promote it to main with full frequency credit so a re-inserted
            // index/filter/range-tombstone block gets the same churn protection
            // as a fresh High admission, instead of being left to cold-evict
            // from the small queue. An entry that was large and no longer is
            // starts in the shard's queues as its priority says.
            let (queue, freq) = match (old_queue, priority) {
                (Queue::Main, _) => (Queue::Main, old_freq),
                (_, Priority::High) => (Queue::Main, MAX_FREQ),
                (Queue::Small, _) => (Queue::Small, old_freq),
                (Queue::Large, _) => (Queue::Small, 0),
            };
            // The new value takes a slot of its own, so readers holding the
            // old one keep it; the old slot's queue node goes stale and the
            // entry queues again at the back.
            let node = self.replace(old, key, value, weight, freq, queue, evicted);
            let policy = self.policy_mut();
            policy.discount(old_queue, old_weight);
            policy.enqueue(node, queue, weight);
            self.sweep_queues();
        } else {
            // Fresh entry. High-priority entries (pinned metadata) go straight to
            // main with a full frequency credit so churn through the small queue
            // can't cold-evict them. Otherwise: a ghost-queue hit (recently
            // evicted from small) is re-admitted to main, else the entry enters
            // the probationary small queue.
            let (queue, freq) = match priority {
                Priority::High => (Queue::Main, MAX_FREQ),
                Priority::Normal if self.policy_mut().take_ghost(hash) => (Queue::Main, 0),
                Priority::Normal => (Queue::Small, 0),
                Priority::Probation => {
                    // The fingerprint was spent on admitting the entry.
                    self.policy_mut().take_ghost(hash);
                    (Queue::Small, 0)
                }
            };
            let node = self.insert(hash, key, value, weight, freq, queue);
            self.policy_mut().enqueue(node, queue, weight);
        }

        self.evict_to_capacity(evicted);
    }

    /// Inserts or replaces `key` as a large entry: in the shard's table, in
    /// neither of its queues, its weight left to the cache to count. Returns
    /// the entry's node for the cache's queue of large entries.
    fn insert_large(
        &mut self,
        hash: u64,
        key: K,
        value: V,
        weight: u64,
        priority: Priority,
        evicted: &mut Evicted<V>,
    ) -> Node {
        let freq = if priority == Priority::High {
            MAX_FREQ
        } else {
            0
        };
        self.policy_mut().take_ghost(hash);
        let Some(old) = self.find(hash, &key) else {
            return self.insert(hash, key, value, weight, freq, Queue::Large);
        };
        let (old_weight, old_queue) = (self.weight(old), self.queue(old));
        let freq = freq.max(self.freq(old));
        let node = self.replace(old, key, value, weight, freq, Queue::Large, evicted);
        self.policy_mut().discount(old_queue, old_weight);
        self.sweep_queues();
        node
    }

    /// One turn of the cache's clock over its large entries, at `node`: an
    /// entry read since the clock last aged keeps its place, for one
    /// frequency credit when this turn `ages` it; an unread one is taken out
    /// when `tenured` and left otherwise. A turn that does not `spare` takes
    /// the entry out read or not.
    fn turn_large(
        &mut self,
        node: Node,
        tenured: bool,
        ages: bool,
        spare: bool,
        evicted: &mut Evicted<V>,
    ) -> Turn {
        if !self.is_live(node) || self.queue(node.link) != Queue::Large {
            return Turn::Stale;
        }
        if spare && self.freq(node.link) > 0 {
            if ages {
                self.lower_freq(node.link);
            }
            return Turn::Kept;
        }
        if !tenured {
            return Turn::Young;
        }
        let hash = self.hash(node.link);
        self.remove_entry(node.link, evicted);
        self.policy_mut().push_ghost(hash);
        Turn::Evicted
    }

    /// Takes the entry in visible slot `link` out of the shard.
    fn remove_entry(&mut self, link: u32, evicted: &mut Evicted<V>) {
        let (queue, weight) = (self.queue(link), self.weight(link));
        self.remove(link, evicted);
        self.policy_mut().discount(queue, weight);
        self.sweep_queues();
    }

    /// Drops the stale nodes of both queues once they outnumber the entries
    /// two to one: a shard that never fills pops none of them by evicting,
    /// and each replace or removal leaves one behind.
    fn sweep_queues(&mut self) {
        let policy = self.policy();
        if policy.small.len() + policy.main.len() <= 2 * self.len() + 16 {
            return;
        }
        let mut small = core::mem::take(&mut self.policy_mut().small);
        let mut main = core::mem::take(&mut self.policy_mut().main);
        small.retain(|&node| self.is_live(node));
        main.retain(|&node| self.is_live(node));
        let policy = self.policy_mut();
        policy.small = small;
        policy.main = main;
    }

    /// Evicts until the shard is within capacity, handing what it evicts to
    /// `evicted` for the caller to drop once the lock is released.
    fn evict_to_capacity(&mut self, evicted: &mut Evicted<V>) {
        if self.policy().resident_bytes() > self.policy().limit() {
            self.policy_mut().filled = true;
        }
        // The turns that spare are counted within one eviction: entries
        // spared by evictions far apart were read in between, not during.
        self.policy_mut().spared = 0;
        while self.policy().resident_bytes() > self.policy().capacity {
            if !self.evict_one(evicted) {
                // Both queues are out of live entries: nothing left to free.
                break;
            }
        }
        // What the shard lends to the large entries comes out of its small
        // queue alone, an entry read since it went in moving to the main
        // queue on the way as at any eviction.
        while self.policy().resident_bytes() > self.policy().limit() {
            if !self.evict_from_small(evicted) {
                break;
            }
        }
    }

    /// Frees (or migrates) one live entry. Returns `false` only when both queues
    /// hold no live entry. A small→main migration returns `true` (progress) even
    /// though it frees no bytes: it shrinks the small queue, so a bounded number
    /// of further calls reach a real eviction.
    #[expect(
        clippy::if_same_then_else,
        reason = "the branches differ in which queue `||` tries first; the lint reads them as equal"
    )]
    fn evict_one(&mut self, evicted: &mut Evicted<V>) -> bool {
        let policy = self.policy();
        let prefer_small = policy.main_bytes == 0 || policy.small_bytes >= policy.small_target;
        if prefer_small {
            self.evict_from_small(evicted) || self.evict_from_main(evicted)
        } else {
            self.evict_from_main(evicted) || self.evict_from_small(evicted)
        }
    }

    /// Processes the small queue's oldest live entry: a frequently-used one
    /// (freq > 0) migrates to the main queue; an unused one is evicted and its
    /// key recorded in the ghost queue. Returns `false` if the small queue holds
    /// no live entry.
    fn evict_from_small(&mut self, evicted: &mut Evicted<V>) -> bool {
        while let Some(node) = self.policy_mut().small.pop_front() {
            if !self.is_live(node) {
                continue;
            }
            let weight = self.weight(node.link);
            if self.freq(node.link) > 0 {
                // Hot: promote to main, reset frequency.
                self.clear_freq(node.link);
                self.set_main(node.link);
                let policy = self.policy_mut();
                policy.small_bytes -= weight;
                policy.main_bytes += weight;
                policy.main.push_back(node);
            } else {
                // Cold: evict, remember the fingerprint in the ghost queue.
                let hash = self.hash(node.link);
                self.remove(node.link, evicted);
                let policy = self.policy_mut();
                policy.small_bytes -= weight;
                policy.push_ghost(hash);
            }
            return true;
        }
        false
    }

    /// Processes the main queue's oldest live entry: one with remaining
    /// frequency is reinserted at the back with its counter decremented; an
    /// unused one is evicted. Returns `false` if the main queue holds no live
    /// entry.
    ///
    /// Readers raise an entry's frequency without the lock, so a turn can
    /// find an entry it lowered raised again, and entries read between
    /// every two turns were spared for as long as the reads went on, with
    /// the shard's lock held. Turns that spared one after another, as many
    /// as wear down every credit the queue can hold, are followed by one
    /// that takes its entry read or not. Without such reads the bound is
    /// never reached: the credits run out first.
    fn evict_from_main(&mut self, evicted: &mut Evicted<V>) -> bool {
        while let Some(node) = self.policy_mut().main.pop_front() {
            if !self.is_live(node) {
                continue;
            }
            let policy = self.policy();
            let bound = (policy.main.len() + 1) * usize::from(MAX_FREQ);
            if policy.spared < bound && self.freq(node.link) > 0 {
                self.lower_freq(node.link);
                let policy = self.policy_mut();
                policy.spared += 1;
                policy.main.push_back(node);
            } else {
                let weight = self.weight(node.link);
                self.remove(node.link, evicted);
                let policy = self.policy_mut();
                policy.spared = 0;
                policy.main_bytes -= weight;
            }
            return true;
        }
        false
    }
}

/// What one insert replaced or evicted, dropped by its caller once the
/// shard's lock is released, so no writer of the shard waits behind the
/// frees. The first few are held in place, their places unwritten until used,
/// so collecting them allocates nothing under the lock and an insert that
/// evicts one or none pays for no more than that one.
pub(crate) struct Evicted<V> {
    /// The first [`HELD`] of them, the ones before `len` written.
    held: [core::mem::MaybeUninit<V>; HELD],
    len: usize,
    rest: Vec<V>,
}

/// The evicted entries held in place: a blob frame of 64 KiB over blocks of
/// 8 KiB evicts that many.
const HELD: usize = 8;

impl<V> Evicted<V> {
    pub(crate) const fn new() -> Self {
        Self {
            held: [const { core::mem::MaybeUninit::uninit() }; HELD],
            len: 0,
            rest: Vec::new(),
        }
    }

    fn push(&mut self, value: V) {
        if let Some(place) = self.held.get_mut(self.len) {
            place.write(value);
            self.len += 1;
        } else {
            self.rest.push(value);
        }
    }
}

impl<V> Drop for Evicted<V> {
    fn drop(&mut self) {
        for place in self.held.iter_mut().take(self.len) {
            // SAFETY: `push` wrote every place before `len`, and nothing takes
            // one out before this drop.
            #[expect(unsafe_code, reason = "see safety")]
            unsafe {
                place.assume_init_drop();
            }
        }
    }
}

/// One shard: its table, and what its writers publish for readers that only
/// want a number.
struct Shard<K, V> {
    table: Table<K, V, Policy>,
    /// The shard's resident weight, as of its last writer.
    resident: AtomicU64,
    /// The shard's capacity, as of its last writer.
    capacity: AtomicU64,
    /// The shard's entries, as of its last writer.
    len: AtomicUsize,
    /// Whether the shard has ever had to evict to stay within capacity.
    filled: AtomicBool,
    /// The weight of the shard's main queue, as of its last writer: what the
    /// shard keeps whatever it lends to the large entries.
    main: AtomicU64,
}

impl<K: Eq, V> Shard<K, V> {
    fn new(capacity: u64, ghost_capacity: usize) -> Self {
        Self {
            table: Table::new(Policy::new(capacity, ghost_capacity)),
            main: AtomicU64::new(0),
            resident: AtomicU64::new(0),
            capacity: AtomicU64::new(capacity),
            len: AtomicUsize::new(0),
            filled: AtomicBool::new(false),
        }
    }

    /// The shard's lock, its share of the cache's `large` bytes brought up to
    /// date first.
    fn write(&self, large: &AtomicU64, shards: usize) -> Write<'_, K, V, Policy> {
        let mut write = self.table.lock();
        write.policy_mut().lent = large.load(Ordering::Relaxed).div_ceil(shards as u64);
        write
    }

    /// [`Self::write`] when the lock is free now, for a writer that does not
    /// wait. Its share of the large bytes is brought up to date all the same:
    /// a shard no waiting writer has visited since a large entry went in
    /// still holds the share it lent before, and filled against that, the
    /// shards and the large entries together weighed more than the cache.
    fn try_write(&self, large: &AtomicU64, shards: usize) -> Option<Write<'_, K, V, Policy>> {
        let mut write = self.table.try_lock()?;
        write.policy_mut().lent = large.load(Ordering::Relaxed).div_ceil(shards as u64);
        Some(write)
    }

    /// Publishes what `write` left, gives back to the cache's `large` tally
    /// what the writer took out of it, releases the shard's lock, then drops
    /// what the writer evicted.
    ///
    /// Returns whether the shard holds more than its capacity less what it
    /// lends: its main queue has grown into the share the large entries
    /// were let in on, and they are the ones to give way.
    fn release(
        &self,
        mut write: Write<'_, K, V, Policy>,
        mut evicted: Evicted<V>,
        large: &AtomicU64,
        shards: usize,
    ) -> bool {
        let released = core::mem::take(&mut write.policy_mut().large_released);
        if released > 0 {
            large.fetch_sub(released, Ordering::Relaxed);
        }
        // A large entry that went in while this writer held the lock saw the
        // shard as it was published before, could not have it give up its
        // share, and left that to this writer's turn: the share is read
        // again, and what the shard holds past it evicted, before the shard
        // is published. Without it the shard kept what it grew by and the
        // large entry both until its next writer, if one came.
        //
        // Only a share that grew is taken up here. A smaller one is left to
        // the shard's next writer: the insert of a large entry has its
        // shards give up the share it is about to take, ahead of the tally
        // read here, and a release that took the tally's share back would
        // publish the room the entry was just given.
        let lent = large.load(Ordering::Relaxed).div_ceil(shards as u64);
        if lent > write.policy().lent {
            write.policy_mut().lent = lent;
            write.evict_to_capacity(&mut evicted);
        }
        let policy = write.policy();
        let over = policy.lent > 0
            && policy.resident_bytes() > policy.capacity.saturating_sub(policy.lent);
        self.main.store(policy.main_bytes, Ordering::Relaxed);
        self.resident
            .store(policy.resident_bytes(), Ordering::Relaxed);
        self.capacity.store(policy.limit(), Ordering::Relaxed);
        self.filled.store(policy.filled, Ordering::Relaxed);
        self.len.store(write.len(), Ordering::Relaxed);
        drop(write);
        drop(evicted);
        over
    }
}

/// What a turn of the clock over the large entries did at one node.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Turn {
    /// The node's entry is gone, or no longer large.
    Stale,
    /// The entry was read since its last turn and stays.
    Kept,
    /// The entry is unread and has not been in long enough to give way.
    Young,
    /// The entry was taken out.
    Evicted,
}

/// The cache's large entries, oldest first: each the number of its shard,
/// its node there, and the count of inserts that needed room when it went
/// in. Entries heavier than half a shard live in their shard's table, where a
/// lookup finds them like any other, and are evicted from here.
struct Large {
    queue: VecDeque<(usize, Node, u64, bool)>,
    /// Inserts of a large entry that found no room for it, so far.
    crowded: u64,
    /// `crowded` when the entries' frequencies were last aged.
    aged_at: u64,
    /// Inserts that needed room an unread entry stays for, per entry held.
    /// Doubled, up to [`TENURE_MAX`], each time the key that needs room is
    /// one the cache evicted not long ago: it is evicting what comes back,
    /// so entries have to stay longer to be read again. Eased by one, down
    /// to [`TENURE_MIN`], by each key that was not.
    tenure: u64,
}

/// The least and most of [`Large::tenure`].
const TENURE_MIN: u64 = 2;
const TENURE_MAX: u64 = 64;

impl Large {
    /// Whether the entries' frequencies are due to age: once per four
    /// inserts that needed room per entry held. Aged at every such insert,
    /// a set of entries each read once a round lost its credits to a round's
    /// misses whenever those outnumbered it, and was evicted while in use.
    fn ages(&mut self) -> bool {
        let due = self.crowded.wrapping_sub(self.aged_at) >= 4 * (self.queue.len() as u64 + 1);
        if due {
            self.aged_at = self.crowded;
        }
        due
    }

    /// Whether an unread entry that went in at `stamp` has been in long
    /// enough to give way to a new one: for two inserts that needed room per
    /// entry held. Without it, keys read in a round a little larger than the
    /// cache holds each evict the one read next, and none is ever found;
    /// with it the entries held stay for many rounds and are found in each.
    fn tenured(&self, stamp: u64) -> bool {
        self.crowded.wrapping_sub(stamp) >= self.tenure * (self.queue.len() as u64 + 1)
    }

    /// Counts an insert that needed room, for a key the cache evicted not
    /// long ago (`back`) or one it did not.
    fn crowd(&mut self, back: bool) {
        self.crowded += 1;
        self.tenure = if back {
            (self.tenure * 2).min(TENURE_MAX)
        } else {
            (self.tenure - 1).max(TENURE_MIN)
        };
    }
}

/// One shard's part of `capacity` spread over `shards`, rounded up so the
/// shards together never hold less than asked.
fn shard_capacity(capacity: u64, shards: usize) -> u64 {
    capacity.div_ceil(shards as u64)
}

/// The least a shard holds where the cache is split at all. Keys fall on
/// shards unevenly, by about the square root of the entries a shard holds,
/// and a shard past its share evicts while the others have room: at 64 KiB
/// a shard held fifteen blocks and a cache seven tenths full missed one read
/// in twenty-five.
const SHARD_BYTES_MIN: u64 = 1 << 20;

/// How many shards a cache of `capacity` bytes is split into: a power of two,
/// `most` at most, each of [`SHARD_BYTES_MIN`] or more. Shards only spread
/// the writers of misses; a read takes no lock.
pub(crate) fn shards_for(capacity: u64, most: usize) -> usize {
    let fit = usize::try_from(capacity / SHARD_BYTES_MIN).unwrap_or(usize::MAX);
    if fit <= 1 {
        return 1;
    }
    // The largest power of two within `fit`.
    let fit = 1 << fit.ilog2();
    fit.min(most)
}

/// The S3-FIFO small queue target of a shard of `capacity`: 10% of it, at
/// least 1 to stay meaningful for tiny shards.
const fn small_target(capacity: u64) -> u64 {
    let target = capacity / 10;
    if target == 0 { 1 } else { target }
}

/// A sharded, byte-weighted S3-FIFO cache. Generic over key, value, [`Weighter`]
/// and hasher; cloned values are returned from `get` / `peek`.
pub(crate) struct ShardedCache<K, V, W, S> {
    shards: Vec<Padded<Shard<K, V>>>,
    /// `shards.len() - 1`; `shards.len()` is always a power of two so this masks
    /// a hash to a shard index.
    shard_mask: u64,
    weighter: W,
    hasher: S,
    /// The total capacity, changed live by [`Self::set_capacity`].
    capacity: AtomicU64,
    /// One shard's capacity: the heaviest entry a shard's own queues take
    /// (see [`Self::shard_capacity`]). Each shard holds its own copy, set
    /// under its lock, which is what an insert is checked against.
    per_shard_capacity: AtomicU64,
    /// Weight of the large entries resident now: bytes of the capacity the
    /// shards lend, each an equal share.
    large_resident: AtomicU64,
    /// The large entries' queue, held by an insert or an eviction of one and
    /// by nothing else. Taken before any shard's lock.
    large: spin::Mutex<Large>,
    /// Held by a resize from its first store to its last shard, so the total,
    /// the per-shard capacity and every shard end at the same resize's values
    /// when two race.
    resizing: spin::Mutex<()>,
}

impl<K, V, W, S> ShardedCache<K, V, W, S>
where
    K: Eq + Hash,
    V: Clone,
    W: Weighter<K, V>,
    S: BuildHasher,
{
    /// Builds a cache with `capacity` total weight, split across
    /// `shard_count_hint` shards (rounded up to a power of two, clamped to
    /// `[1, 256]`). `est_items` seeds the ghost-queue capacity per shard.
    pub(crate) fn with_weighter(
        capacity: u64,
        shard_count_hint: usize,
        est_items: usize,
        weighter: W,
        hasher: S,
    ) -> Self {
        let shard_count = shard_count_hint.next_power_of_two().clamp(1, 256);
        let per_shard_cap = shard_capacity(capacity, shard_count);
        // Ghost queue holds roughly as many fingerprints as the shard is
        // expected to hold live entries (S3-FIFO sizes the ghost ~ main).
        let ghost_capacity = (est_items / shard_count).max(16);

        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(Padded(Shard::new(per_shard_cap, ghost_capacity)));
        }

        Self {
            shards,
            shard_mask: shard_count as u64 - 1,
            weighter,
            hasher,
            capacity: AtomicU64::new(capacity),
            per_shard_capacity: AtomicU64::new(per_shard_cap),
            large_resident: AtomicU64::new(0),
            large: spin::Mutex::new(Large {
                queue: VecDeque::new(),
                crowded: 0,
                aged_at: 0,
                tenure: TENURE_MIN,
            }),
            resizing: spin::Mutex::new(()),
        }
    }

    /// The heaviest entry one shard's own queues take: one shard's capacity.
    /// An entry within it costs the other shards nothing; writers cut their
    /// blocks to it where they can.
    pub(crate) fn shard_capacity(&self) -> u64 {
        self.per_shard_capacity.load(Ordering::Relaxed)
    }

    /// The heaviest entry this cache keeps: half of its capacity, or a
    /// shard's where the cache is one shard. Heavier inserts are refused
    /// (see [`Self::insert_with_priority`]); callers producing such entries
    /// can skip building them entirely. An entry heavier than a shard is
    /// kept as a large one, every shard lending an equal share of its bytes.
    pub(crate) fn max_entry_weight(&self) -> u64 {
        self.shard_capacity()
            .max(self.capacity.load(Ordering::Relaxed) / 2)
    }

    /// Whether an entry of `weight` is kept as a large one: heavier than a
    /// shard, which could not take it at all, or heavier than half a shard
    /// and no smaller than [`LARGE_FLOOR`]. A shard holds one entry of more
    /// than half of it at most, so two whose keys fall in one shard would
    /// evict each other however much room the cache has.
    ///
    /// The floor keeps ordinary blocks in the shards' own queues where a
    /// cache is so small that a block is half a shard: there a stream of
    /// data blocks through the large entries' one queue wore down and
    /// evicted the filter and index pieces of the same size, which a shard's
    /// queues keep.
    fn is_large(&self, weight: u64) -> bool {
        if self.shards.len() == 1 {
            return false;
        }
        let shard = self.shard_capacity();
        weight > shard || (weight >= LARGE_FLOOR && weight > shard / 2)
    }

    /// What the large entries may weigh together: seven eighths of the
    /// capacity at most, and no more than the shards' main queues leave (see
    /// `Policy::limit`).
    fn large_limit(&self) -> u64 {
        let capacity = self.capacity.load(Ordering::Relaxed);
        capacity - capacity / 8
    }

    fn lock<'a>(&self, shard: &'a Shard<K, V>) -> Write<'a, K, V, Policy> {
        shard.write(&self.large_resident, self.shards.len())
    }

    fn try_lock<'a>(&self, shard: &'a Shard<K, V>) -> Option<Write<'a, K, V, Policy>> {
        shard.try_write(&self.large_resident, self.shards.len())
    }

    fn release(&self, shard: &Shard<K, V>, write: Write<'_, K, V, Policy>, evicted: Evicted<V>) {
        if shard.release(write, evicted, &self.large_resident, self.shards.len()) {
            self.take_back();
        }
    }

    /// Has the large entries give way where the shards' main queues and they
    /// together weigh more than the cache: a large entry is let in on bytes
    /// the shards lend out of their small queues, and a main queue that
    /// grows afterwards keeps its entries, which outrank a large one.
    /// Without it a cache filled with large entries and then with blocks
    /// read again held both, up to nearly twice its capacity.
    ///
    /// Called with no shard's lock held; skipped where the large entries'
    /// lock is taken, by a writer that has them give way itself.
    #[cold]
    fn take_back(&self) {
        if self.weight() <= self.capacity() {
            return;
        }
        if let Some(mut large) = self.large.try_lock() {
            self.give_way(&mut large, false);
        }
    }

    /// Evicts large entries, oldest first, for what the cache weighs past
    /// its capacity. However lately they went in or were read: the capacity
    /// is a bound, and an entry spared for its tenure left the cache over it
    /// for as long as no write came. A writer that does not `wait` leaves
    /// the entries of shards whose locks are taken.
    fn give_way(&self, large: &mut Large, wait: bool) {
        let over = self.weight().saturating_sub(self.capacity());
        if over == 0 {
            return;
        }
        let keep = self
            .large_resident
            .load(Ordering::Relaxed)
            .saturating_sub(over);
        let room = self.large_limit().saturating_sub(keep);
        self.trim_large(large, room, true, false, wait);
    }

    /// Changes the total capacity of the live cache, split over its shards as
    /// at construction. A shard that shrank evicts down to its new capacity
    /// in its own S3-FIFO order before its lock is released; growing evicts
    /// nothing.
    pub(crate) fn set_capacity(&self, capacity: u64) {
        let per_shard = shard_capacity(capacity, self.shards.len());
        let _resizing = self.resizing.lock();
        self.capacity.store(capacity, Ordering::Relaxed);
        self.per_shard_capacity.store(per_shard, Ordering::Relaxed);
        // The large entries first, so each shard then evicts against the
        // share it still lends.
        let mut large = self.large.lock();
        self.trim_large(&mut large, 0, true, false, true);
        for shard in &self.shards {
            let mut evicted = Evicted::new();
            let mut write = self.lock(&shard.0);
            let policy = write.policy_mut();
            policy.capacity = per_shard;
            policy.small_target = small_target(per_shard);
            write.evict_to_capacity(&mut evicted);
            self.release(&shard.0, write, evicted);
        }
        // A shard keeps its main queue whatever it lends, so the main queues
        // and the large entries that fit their own limit can together weigh
        // more than the new capacity. The large entries give way for the
        // rest: the main queues hold what outranks them.
        self.give_way(&mut large, true);
    }

    /// Evicts large entries, oldest unread first, until `weight` more fits
    /// what they may weigh together, or none is left to give way. A resize
    /// is `forced`: it evicts down whatever the entries' time in the cache,
    /// the unread first and then the read.
    /// Room for a `pinned` entry (an index, a filter: what every read walks)
    /// is taken from unread entries that are not, however lately they went
    /// in, as in a shard's queues such entries outlast the data blocks.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "`release` takes the shard's lock and drops it, which the lint does not follow"
    )]
    fn trim_large(&self, large: &mut Large, weight: u64, forced: bool, pinned: bool, wait: bool) {
        let limit = self.large_limit();
        let ages = forced || large.ages();
        // Each entry is turned once, or once per frequency credit it holds
        // where the turns age it.
        let mut turns = large.queue.len() * if ages { usize::from(MAX_FREQ) + 1 } else { 1 };
        // A read gives an entry back the credit a turn took. Where readers
        // kept the entries in credit through all those turns, a forced trim
        // goes round once more and takes them read or not: the capacity it
        // evicts down to is a bound.
        let mut spare = true;
        while self.large_resident.load(Ordering::Relaxed) + weight > limit {
            if turns == 0 {
                if !forced || !spare {
                    return;
                }
                spare = false;
                turns = large.queue.len();
                continue;
            }
            turns -= 1;
            let Some((at, node, stamp, held_pinned)) = large.queue.pop_front() else {
                return;
            };
            let Some(shard) = self.shards.get(at) else {
                continue;
            };
            // Nor does an entry that is not pinned take a pinned one's room:
            // a stream of data blocks would otherwise wear down and evict
            // the index and filter blocks its own reads walk.
            if held_pinned && !pinned && !forced {
                large.queue.push_back((at, node, stamp, held_pinned));
                continue;
            }
            let tenured = forced || large.tenured(stamp) || (pinned && !held_pinned);
            let mut evicted = Evicted::new();
            let mut write = if wait {
                self.lock(&shard.0)
            } else if let Some(write) = self.try_lock(&shard.0) {
                write
            } else {
                // A writer that does not wait leaves the entry of a shard
                // whose lock is taken for the next turn.
                large.queue.push_back((at, node, stamp, held_pinned));
                continue;
            };
            let turn = write.turn_large(node, tenured, ages, spare, &mut evicted);
            self.release(&shard.0, write, evicted);
            match turn {
                Turn::Kept => large.queue.push_back((at, node, stamp, held_pinned)),
                // A pinned entry looks past the pinned ones still young for
                // one that is not.
                Turn::Young if pinned => large.queue.push_back((at, node, stamp, held_pinned)),
                Turn::Young => {
                    // The oldest unread entry stays: so do the ones after it.
                    large.queue.push_front((at, node, stamp, held_pinned));
                    return;
                }
                Turn::Stale | Turn::Evicted => {}
            }
        }
    }

    /// Inserts or replaces `key` as a large entry, evicting older large
    /// entries for its room, then the shards that hold more than the share
    /// they now lend leaves them. Refused when the large entries read since
    /// cannot make room, and when another large insert is under way: a miss
    /// never waits to be cached, and where nearly every block is a large one
    /// (a cache far too small for its blocks) the misses of many threads
    /// would otherwise all queue here.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "`release` takes the shard's lock and drops it, which the lint does not follow"
    )]
    fn insert_large(&self, key: K, value: V, weight: u64, priority: Priority, wait: bool) {
        let h = self.hasher.hash_one(&key);
        let at = self.index_of(h);
        let Some(shard) = self.shards.get(at) else {
            return;
        };
        {
            let Some(mut large) = self.large.try_lock() else {
                return;
            };
            // What the key holds now as a large entry gives its bytes back
            // when replaced, so only the difference needs room. Looked up
            // under the shard's lock and let go before any eviction, which
            // may need that same lock.
            let held_now = |write: &Write<'_, K, V, Policy>| {
                write
                    .find(h, &key)
                    .filter(|&link| write.queue(link) == Queue::Large)
                    .map_or(0, |link| write.weight(link))
            };
            let (held, back) = {
                let write = if wait {
                    shard.0.table.lock()
                } else if let Some(write) = shard.0.table.try_lock() {
                    write
                } else {
                    return;
                };
                (held_now(&write), write.policy().in_ghost(h))
            };
            let more = weight.saturating_sub(held);
            let limit = self.large_limit();
            if self.large_resident.load(Ordering::Relaxed) + more > limit {
                large.crowd(back);
                self.trim_large(&mut large, more, false, priority == Priority::High, wait);
            }
            if self.large_resident.load(Ordering::Relaxed) + more > limit {
                return;
            }
            // Main queues full of what outranks this entry leave it no room
            // whatever the small queues give up: seen before they are asked
            // to, or an entry that is then refused had them evict for it.
            let main: u64 = self
                .shards
                .iter()
                .map(|shard| shard.0.main.load(Ordering::Relaxed))
                .sum();
            if main + self.large_resident.load(Ordering::Relaxed) + more > self.capacity() {
                return;
            }
            // The shards give up their share of what the large entries will
            // weigh, out of their small queues; if the cache would still
            // hold more than its capacity, their main queues are full of
            // what outranks this entry, and it is not taken.
            self.lend(self.large_resident.load(Ordering::Relaxed) + more);
            if self.weight() + more > self.capacity() {
                return;
            }
            let mut evicted = Evicted::new();
            let mut write = if wait {
                self.lock(&shard.0)
            } else if let Some(write) = self.try_lock(&shard.0) {
                write
            } else {
                return;
            };
            // What the key held may have gone since it was looked up: the
            // trim above evicts it like any other, and a sweep of a deleted
            // file takes it out. The room found was for the difference, so
            // an entry that now needs more than that is not taken.
            let more_now = weight.saturating_sub(held_now(&write));
            if more_now > more && self.large_resident.load(Ordering::Relaxed) + more_now > limit {
                self.release(&shard.0, write, evicted);
                return;
            }
            let node = write.insert_large(h, key, value, weight, priority, &mut evicted);
            // Counted before the lock is released, so the shard publishes a
            // weight the cache's total already holds.
            self.large_resident.fetch_add(weight, Ordering::Relaxed);
            self.release(&shard.0, write, evicted);
            let stamp = large.crowded;
            large
                .queue
                .push_back((at, node, stamp, priority == Priority::High));
            self.sweep_large(&mut large, wait);
            // Writers of other shards may have filled their main queues
            // between the look at the cache's weight and the insert, and a
            // shard released meanwhile found this lock taken: what the cache
            // now weighs past its capacity is taken back here.
            self.give_way(&mut large, wait);
        }
    }

    /// Drops the queue nodes of large entries that are gone, once they
    /// outnumber what the cache can hold of such entries two to one. An
    /// entry replaced or taken out leaves its node behind, and nothing else
    /// pops it while the large entries stay within their limit: a key
    /// written over and over, or the files a compaction retires, grew the
    /// queue without bound.
    fn sweep_large(&self, large: &mut Large, wait: bool) {
        // A large entry weighs more than half a shard.
        let live_most = 2 * self.large_limit() / self.shard_capacity().max(1) + 1;
        let live_most = usize::try_from(live_most).unwrap_or(usize::MAX);
        if large.queue.len() <= live_most.saturating_mul(2).saturating_add(16) {
            return;
        }
        large.queue.retain(|&(at, node, _, _)| {
            let Some(shard) = self.shards.get(at) else {
                return false;
            };
            let write = if wait {
                shard.0.table.lock()
            } else if let Some(write) = shard.0.table.try_lock() {
                write
            } else {
                // Kept for the next sweep.
                return true;
            };
            write.is_live(node) && write.queue(node.link) == Queue::Large
        });
    }

    /// Has every shard give up its share of `large` bytes of large entries.
    /// Only a shard holding more than its capacity less that share has
    /// anything to give up, and one whose lock is taken gives it up at that
    /// writer's next turn: no shard is waited for.
    fn lend(&self, large: u64) {
        let lent = large.div_ceil(self.shards.len() as u64);
        let limit = self.shard_capacity().saturating_sub(lent);
        for shard in &self.shards {
            if shard.0.resident.load(Ordering::Relaxed) <= limit {
                continue;
            }
            if let Some(mut write) = shard.0.table.try_lock() {
                let mut evicted = Evicted::new();
                write.policy_mut().lent = lent;
                write.evict_to_capacity(&mut evicted);
                self.release(&shard.0, write, evicted);
            }
        }
    }

    /// The shard of a key hashed to `h`.
    #[inline]
    fn index_of(&self, h: u64) -> usize {
        // High bits are the best-mixed for most hashers; fold them down to the
        // shard index. `shards.len()` is a power of two, so `& mask` is exact.
        let masked = ((h >> 32) ^ h) & self.shard_mask;
        // `masked <= shard_mask <= 255` (shard count clamped to 256), exact cast.
        #[expect(
            clippy::cast_possible_truncation,
            reason = "masked index <= 255 fits usize on all targets"
        )]
        let idx = masked as usize;
        idx
    }

    /// Hashes `key` once and resolves both the owning shard and the hash that
    /// the shard's table reuses for its own lookup. Hashing here (instead of
    /// once for the shard and again inside the table) is the per-access win.
    #[inline]
    fn locate(&self, key: &K) -> (u64, &Shard<K, V>) {
        let h = self.hasher.hash_one(key);
        let idx = self.index_of(h);
        // `idx <= shard_mask < shards.len()` by construction → in bounds; `get`
        // would force an `expect`/`unwrap` for no safety gain.
        #[expect(
            clippy::indexing_slicing,
            reason = "idx = hash & (len - 1) with len a power of two, so idx < len"
        )]
        (h, &self.shards[idx].0)
    }

    /// Returns the value for `key`, counting the access as a hit (promotes).
    pub(crate) fn get(&self, key: &K) -> Option<V> {
        let (h, shard) = self.locate(key);
        shard
            .table
            .pin(h, key, true)
            .map(|held| held.value().clone())
    }

    /// Returns the value for `key` WITHOUT counting it as a hit (no promotion).
    pub(crate) fn peek(&self, key: &K) -> Option<V> {
        let (h, shard) = self.locate(key);
        shard
            .table
            .pin(h, key, false)
            .map(|held| held.value().clone())
    }

    /// Whether `key` is resident, without cloning it out or counting a hit.
    pub(crate) fn contains(&self, key: &K) -> bool {
        let (h, shard) = self.locate(key);
        shard.table.pin(h, key, false).is_some()
    }

    /// Whether a missed `key`, `weight` once built, should be inserted now:
    /// at once while its shard has room for it, since an insert that evicts
    /// nothing costs nothing later; into a full shard only when it was missed
    /// (or evicted cold) recently, which the ghost queue remembers. A first
    /// miss into a full shard is remembered there instead, so a read that
    /// never comes back costs a fingerprint, not an entry built, inserted and
    /// evicted unread.
    pub(crate) fn admit(&self, key: &K, weight: u64) -> bool {
        self.admit_where(key, weight, false)
    }

    /// [`Self::admit`], and never into a shard that has not yet had to evict
    /// to stay within capacity: an insert there grows the shard into fresh
    /// memory, where one into a shard that has filled takes the place of
    /// what it evicts. One look at the shard answers both.
    pub(crate) fn admit_once_filled(&self, key: &K, weight: u64) -> bool {
        self.admit_where(key, weight, true)
    }

    #[inline]
    fn admit_where(&self, key: &K, weight: u64, filled_only: bool) -> bool {
        let (h, shard) = self.locate(key);
        if filled_only && !shard.filled.load(Ordering::Relaxed) {
            return false;
        }
        let room = if self.is_large(weight) {
            // A large entry takes its bytes from every shard: at once only
            // while the whole cache has them free.
            self.weight() + weight <= self.capacity()
        } else {
            shard.resident.load(Ordering::Relaxed) + weight
                <= shard.capacity.load(Ordering::Relaxed)
        };
        if room {
            return true;
        }
        // The ghost set is a hint, asked on the read path: a read never waits
        // for the shard's writer for it, and a hint lost to a held lock costs
        // the key one more miss.
        let Some(mut write) = shard.table.try_lock() else {
            return false;
        };
        let policy = write.policy_mut();
        if policy.in_ghost(h) {
            return true;
        }
        policy.push_ghost(h);
        false
    }

    /// Whether `key`'s shard has ever had to evict to stay within capacity,
    /// so an insert there takes the place of what it evicts instead of
    /// growing the shard into fresh memory.
    #[cfg(test)]
    pub(crate) fn has_filled(&self, key: &K) -> bool {
        let (_, shard) = self.locate(key);
        shard.filled.load(Ordering::Relaxed)
    }

    /// Inserts or replaces `key` at [`Priority::Normal`], evicting as needed to
    /// stay within capacity.
    pub(crate) fn insert(&self, key: K, value: V) {
        self.insert_with_priority(key, value, Priority::Normal);
    }

    /// Inserts or replaces `key` at the given admission [`Priority`], evicting as
    /// needed to stay within capacity. [`Priority::High`] pins the entry against
    /// churn (see the enum docs).
    pub(crate) fn insert_with_priority(&self, key: K, value: V, priority: Priority) {
        let weight = self.weighter.weight(&key, &value);
        // Refused before the lock, so a cache too small for the entry (or one
        // of capacity zero) costs its writers no lock; the shard checks again
        // under it, for a resize that lands in between.
        if weight > self.max_entry_weight() {
            return;
        }
        if self.is_large(weight) {
            self.insert_large(key, value, weight, priority, true);
            return;
        }
        let (h, shard) = self.locate(&key);
        let mut evicted = Evicted::new();
        let mut write = self.lock(shard);
        write.insert_entry(h, key, value, weight, priority, &mut evicted);
        self.release(shard, write, evicted);
    }

    /// Inserts `key` as [`Self::insert_with_priority`] does, only when its
    /// shard's lock is free now: a caller that must not wait drops the entry.
    /// A large entry is dropped as well when a lock it needs is taken.
    pub(crate) fn try_insert_with_priority(&self, key: K, value: V, priority: Priority) {
        let weight = self.weighter.weight(&key, &value);
        if weight > self.max_entry_weight() {
            return;
        }
        if self.is_large(weight) {
            self.insert_large(key, value, weight, priority, false);
            return;
        }
        let (h, shard) = self.locate(&key);
        if let Some(mut write) = self.try_lock(shard) {
            let mut evicted = Evicted::new();
            write.insert_entry(h, key, value, weight, priority, &mut evicted);
            self.release(shard, write, evicted);
        }
    }

    /// Inserts the value `merge` derives from what `key` holds now, read and
    /// replaced under one hold of the shard's lock, so a concurrent writer of
    /// the key cannot slip between the two. `merge` returning `None` leaves
    /// the entry as it is; a derived value heavier than a shard is refused,
    /// as by [`Self::insert_with_priority`].
    #[cfg(feature = "zstd")]
    pub(crate) fn insert_merged(&self, key: K, merge: impl FnOnce(Option<&V>) -> Option<V>) {
        let (h, shard) = self.locate(&key);
        let mut write = self.lock(shard);
        let held = write.find(h, &key).map(|link| write.value(link));
        let Some(value) = merge(held) else {
            return;
        };
        let weight = self.weighter.weight(&key, &value);
        let mut evicted = Evicted::new();
        write.insert_entry(h, key, value, weight, Priority::Normal, &mut evicted);
        self.release(shard, write, evicted);
    }

    /// Removes `key` if present.
    pub(crate) fn remove(&self, key: &K) {
        let (h, shard) = self.locate(key);
        let mut evicted = Evicted::new();
        let mut write = self.lock(shard);
        if let Some(link) = write.find(h, key) {
            write.remove_entry(link, &mut evicted);
        }
        self.release(shard, write, evicted);
    }

    /// Total resident weight: every shard's own, as of its last writer, and
    /// the large entries'.
    pub(crate) fn weight(&self) -> u64 {
        self.shards
            .iter()
            .map(|shard| shard.0.resident.load(Ordering::Relaxed))
            .sum::<u64>()
            + self.large_resident.load(Ordering::Relaxed)
    }

    /// Total cache capacity: the value passed to [`Self::with_weighter`], or
    /// to the latest [`Self::set_capacity`].
    pub(crate) fn capacity(&self) -> u64 {
        self.capacity.load(Ordering::Relaxed)
    }

    /// Removes every entry whose key `doomed` picks and returns the weight
    /// they held. Walks every shard under its lock: for a sweep, not the hot
    /// path.
    #[cfg(feature = "std")]
    pub(crate) fn remove_where(&self, mut doomed: impl FnMut(&K) -> bool) -> u64 {
        let mut removed = 0;
        for shard in &self.shards {
            let mut evicted = Evicted::new();
            let mut write = self.lock(&shard.0);
            let links: Vec<u32> = write
                .links()
                .filter(|&link| doomed(write.key(link)))
                .collect();
            for link in links {
                removed += write.weight(link);
                write.remove_entry(link, &mut evicted);
            }
            self.release(&shard.0, write, evicted);
        }
        removed
    }

    /// Number of resident entries across all shards, as of each shard's last
    /// writer.
    pub(crate) fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.0.len.load(Ordering::Relaxed))
            .sum()
    }

    /// Sets how many keys the cache remembers having refused or evicted, all
    /// shards together, to exactly `ghosts`: below the least a cache built
    /// by [`Self::with_weighter`] keeps, for one whose memory is counted to
    /// the byte.
    pub(crate) fn set_ghosts(&self, ghosts: usize) {
        let each = ghosts / self.shards.len();
        for shard in &self.shards {
            shard.0.table.lock().policy_mut().ghost_capacity = each;
        }
    }

    /// Keys the cache remembers having refused or evicted now.
    pub(crate) fn ghosts(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.0.table.lock().policy().ghost.len())
            .sum()
    }

    /// One shard's capacity and resident weight, read under its lock.
    #[cfg(test)]
    fn shard_state(&self, shard: usize) -> Option<(u64, u64)> {
        let write = self.shards.get(shard)?.0.table.lock();
        let state = (write.policy().capacity, write.policy().resident_bytes());
        drop(write);
        Some(state)
    }

    /// Nodes in one shard's two queues, stale ones included.
    #[cfg(test)]
    fn queued_nodes(&self, shard: usize) -> Option<usize> {
        let write = self.shards.get(shard)?.0.table.lock();
        let queued = write.policy().small.len() + write.policy().main.len();
        drop(write);
        Some(queued)
    }

    /// One shard's running tallies of the small and main queues, then the
    /// same two summed over the entries it holds.
    #[cfg(test)]
    fn queue_tallies(&self, shard: usize) -> Option<[u64; 4]> {
        let write = self.shards.get(shard)?.0.table.lock();
        let (mut small, mut main) = (0, 0);
        for link in write.links() {
            match write.queue(link) {
                Queue::Main => main += write.weight(link),
                Queue::Small => small += write.weight(link),
                Queue::Large => {}
            }
        }
        let tallies = [
            write.policy().small_bytes,
            write.policy().main_bytes,
            small,
            main,
        ];
        drop(write);
        Some(tallies)
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::expect_used,
    reason = "test code"
)]
mod tests;

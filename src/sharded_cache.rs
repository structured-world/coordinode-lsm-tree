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
//!   the queues and the ghost set; readers never wait for it. Each shard's
//!   resident weight and entry count are atomics, so `weight()` and `len()`
//!   take no lock either.

mod table;

use alloc::collections::VecDeque;
use alloc::vec::Vec;
use core::hash::{BuildHasher, Hash};
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use portable_atomic::AtomicU64;
use table::{Node, Table, Write};

/// Maximum value of an entry's frequency counter (2-bit, S3-FIFO).
const MAX_FREQ: u8 = 3;

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
    /// Target ceiling for the small queue (~10% of `capacity`); when the small
    /// queue exceeds it, eviction prefers the small queue.
    small_target: u64,
    /// Max number of fingerprints retained in the ghost queue.
    ghost_capacity: usize,
    /// Whether the shard has ever had to evict to stay within capacity: its
    /// table and queues have grown to what it holds, and an insert now takes
    /// the place of what it evicts rather than fresh memory.
    filled: bool,
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
            small_target: small_target(capacity),
            ghost_capacity,
            filled: false,
        }
    }

    /// Resident weight of the shard.
    #[inline]
    fn resident_bytes(&self) -> u64 {
        self.small_bytes + self.main_bytes
    }

    /// Queues the node of an entry just written and counts its weight.
    fn enqueue(&mut self, node: Node, main: bool, weight: u64) {
        if main {
            self.main_bytes += weight;
            self.main.push_back(node);
        } else {
            self.small_bytes += weight;
            self.small.push_back(node);
        }
    }

    /// Takes the weight of an entry out of its queue's tally.
    fn discount(&mut self, main: bool, weight: u64) {
        if main {
            self.main_bytes -= weight;
        } else {
            self.small_bytes -= weight;
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
            let (old_weight, old_main, old_freq) =
                (self.weight(old), self.in_main(old), self.freq(old));
            // Honor a High-priority refresh of a probationary (small) entry:
            // promote it to main with full frequency credit so a re-inserted
            // index/filter/range-tombstone block gets the same churn protection
            // as a fresh High admission, instead of being left to cold-evict
            // from the small queue.
            let promote = priority == Priority::High && !old_main;
            let main = old_main || promote;
            let freq = if promote { MAX_FREQ } else { old_freq };
            // The new value takes a slot of its own, so readers holding the
            // old one keep it; the old slot's queue node goes stale and the
            // entry queues again at the back.
            let node = self.replace(old, key, value, weight, freq, main, evicted);
            let policy = self.policy_mut();
            policy.discount(old_main, old_weight);
            policy.enqueue(node, main, weight);
            self.sweep_queues();
        } else {
            // Fresh entry. High-priority entries (pinned metadata) go straight to
            // main with a full frequency credit so churn through the small queue
            // can't cold-evict them. Otherwise: a ghost-queue hit (recently
            // evicted from small) is re-admitted to main, else the entry enters
            // the probationary small queue.
            let (main, freq) = match priority {
                Priority::High => (true, MAX_FREQ),
                Priority::Normal => (self.policy_mut().take_ghost(hash), 0),
                Priority::Probation => {
                    // The fingerprint was spent on admitting the entry.
                    self.policy_mut().take_ghost(hash);
                    (false, 0)
                }
            };
            let node = self.insert(hash, key, value, weight, freq, main);
            self.policy_mut().enqueue(node, main, weight);
        }

        self.evict_to_capacity(evicted);
    }

    /// Takes the entry in visible slot `link` out of the shard.
    fn remove_entry(&mut self, link: u32, evicted: &mut Evicted<V>) {
        let (main, weight) = (self.in_main(link), self.weight(link));
        self.remove(link, evicted);
        self.policy_mut().discount(main, weight);
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
        if self.policy().resident_bytes() > self.policy().capacity {
            self.policy_mut().filled = true;
        }
        while self.policy().resident_bytes() > self.policy().capacity {
            if !self.evict_one(evicted) {
                // Both queues are out of live entries: nothing left to free.
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
    fn evict_from_main(&mut self, evicted: &mut Evicted<V>) -> bool {
        while let Some(node) = self.policy_mut().main.pop_front() {
            if !self.is_live(node) {
                continue;
            }
            if self.freq(node.link) > 0 {
                self.lower_freq(node.link);
                self.policy_mut().main.push_back(node);
            } else {
                let weight = self.weight(node.link);
                self.remove(node.link, evicted);
                self.policy_mut().main_bytes -= weight;
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
}

impl<K, V> Shard<K, V> {
    fn new(capacity: u64, ghost_capacity: usize) -> Self {
        Self {
            table: Table::new(Policy::new(capacity, ghost_capacity)),
            resident: AtomicU64::new(0),
            capacity: AtomicU64::new(capacity),
            len: AtomicUsize::new(0),
            filled: AtomicBool::new(false),
        }
    }

    /// Publishes what `write` left, releases the shard's lock, then drops
    /// what the writer evicted.
    fn release(&self, write: Write<'_, K, V, Policy>, evicted: Evicted<V>) {
        let policy = write.policy();
        self.resident
            .store(policy.resident_bytes(), Ordering::Relaxed);
        self.capacity.store(policy.capacity, Ordering::Relaxed);
        self.filled.store(policy.filled, Ordering::Relaxed);
        self.len.store(write.len(), Ordering::Relaxed);
        drop(write);
        drop(evicted);
    }
}

/// One shard's part of `capacity` spread over `shards`, rounded up so the
/// shards together never hold less than asked.
fn shard_capacity(capacity: u64, shards: usize) -> u64 {
    capacity.div_ceil(shards as u64)
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
    /// One shard's capacity: the heaviest entry that can stay resident, read
    /// by callers deciding whether to build one (see
    /// [`Self::max_entry_weight`]). Each shard holds its own copy, set under
    /// its lock, which is what an insert is checked against.
    per_shard_capacity: AtomicU64,
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
            resizing: spin::Mutex::new(()),
        }
    }

    /// The heaviest entry this cache can actually keep: one shard's capacity.
    /// Heavier inserts are refused (see [`Self::insert_with_priority`]);
    /// callers producing large entries can skip building them entirely.
    pub(crate) fn max_entry_weight(&self) -> u64 {
        self.per_shard_capacity.load(Ordering::Relaxed)
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
        for shard in &self.shards {
            let mut evicted = Evicted::new();
            let mut write = shard.0.table.lock();
            let policy = write.policy_mut();
            policy.capacity = per_shard;
            policy.small_target = small_target(per_shard);
            write.evict_to_capacity(&mut evicted);
            shard.0.release(write, evicted);
        }
    }

    /// Hashes `key` once and resolves both the owning shard and the hash that
    /// the shard's table reuses for its own lookup. Hashing here (instead of
    /// once for the shard and again inside the table) is the per-access win.
    #[inline]
    fn locate(&self, key: &K) -> (u64, &Shard<K, V>) {
        let h = self.hasher.hash_one(key);
        // High bits are the best-mixed for most hashers; fold them down to the
        // shard index. `shards.len()` is a power of two, so `& mask` is exact.
        let masked = ((h >> 32) ^ h) & self.shard_mask;
        // `masked <= shard_mask <= 255` (shard count clamped to 256), exact cast.
        #[expect(
            clippy::cast_possible_truncation,
            reason = "masked index <= 255 fits usize on all targets"
        )]
        let idx = masked as usize;
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
        if shard.resident.load(Ordering::Relaxed) + weight <= shard.capacity.load(Ordering::Relaxed)
        {
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
        let (h, shard) = self.locate(&key);
        let mut evicted = Evicted::new();
        let mut write = shard.table.lock();
        write.insert_entry(h, key, value, weight, priority, &mut evicted);
        shard.release(write, evicted);
    }

    /// Inserts `key` as [`Self::insert_with_priority`] does, only when its
    /// shard's lock is free now: a caller that must not wait drops the entry.
    pub(crate) fn try_insert_with_priority(&self, key: K, value: V, priority: Priority) {
        let weight = self.weighter.weight(&key, &value);
        if weight > self.max_entry_weight() {
            return;
        }
        let (h, shard) = self.locate(&key);
        if let Some(mut write) = shard.table.try_lock() {
            let mut evicted = Evicted::new();
            write.insert_entry(h, key, value, weight, priority, &mut evicted);
            shard.release(write, evicted);
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
        let mut write = shard.table.lock();
        let held = write.find(h, &key).map(|link| write.value(link));
        let Some(value) = merge(held) else {
            return;
        };
        let weight = self.weighter.weight(&key, &value);
        let mut evicted = Evicted::new();
        write.insert_entry(h, key, value, weight, Priority::Normal, &mut evicted);
        shard.release(write, evicted);
    }

    /// Removes `key` if present.
    pub(crate) fn remove(&self, key: &K) {
        let (h, shard) = self.locate(key);
        let mut evicted = Evicted::new();
        let mut write = shard.table.lock();
        if let Some(link) = write.find(h, key) {
            write.remove_entry(link, &mut evicted);
        }
        shard.release(write, evicted);
    }

    /// Total resident weight across all shards, as of each shard's last
    /// writer.
    pub(crate) fn weight(&self) -> u64 {
        self.shards
            .iter()
            .map(|shard| shard.0.resident.load(Ordering::Relaxed))
            .sum()
    }

    /// Total cache capacity: the value passed to [`Self::with_weighter`], or
    /// to the latest [`Self::set_capacity`].
    pub(crate) fn capacity(&self) -> u64 {
        self.capacity.load(Ordering::Relaxed)
    }

    /// Removes every entry whose key `doomed` picks. Walks every shard under
    /// its lock: for a sweep, not the hot path.
    #[cfg(feature = "std")]
    pub(crate) fn remove_where(&self, mut doomed: impl FnMut(&K) -> bool) {
        for shard in &self.shards {
            let mut evicted = Evicted::new();
            let mut write = shard.0.table.lock();
            let links: Vec<u32> = write
                .links()
                .filter(|&link| doomed(write.key(link)))
                .collect();
            for link in links {
                write.remove_entry(link, &mut evicted);
            }
            shard.0.release(write, evicted);
        }
    }

    /// Number of resident entries across all shards, as of each shard's last
    /// writer.
    pub(crate) fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.0.len.load(Ordering::Relaxed))
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

    /// One shard's running tallies of the small and main queues, then the
    /// same two summed over the entries it holds.
    #[cfg(test)]
    fn queue_tallies(&self, shard: usize) -> Option<[u64; 4]> {
        let write = self.shards.get(shard)?.0.table.lock();
        let (mut small, mut main) = (0, 0);
        for link in write.links() {
            if write.in_main(link) {
                main += write.weight(link);
            } else {
                small += write.weight(link);
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

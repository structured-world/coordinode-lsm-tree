// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! In-tree sharded cache backing the block/blob cache ([`crate::Cache`]) and
//! the file-descriptor cache ([`crate::descriptor_table`]).
//!
//! Replaces `quick_cache`, the last `std`-only dependency standing between the
//! crate and a `no_std + alloc` build. A single implementation serves both
//! targets:
//!
//! - **Eviction:** per-shard byte-weighted **S3-FIFO** (small / main / ghost
//!   FIFO queues + a 2-bit frequency counter). S3-FIFO is uniquely suited to a
//!   lock-free read path: a *read* only bumps the entry's frequency counter and
//!   never reorders a queue (promotion small→main happens lazily at eviction
//!   time, keyed on that counter), unlike LRU which must move the entry to the
//!   MRU end on every hit. The frequency counter is an [`AtomicU8`], so reads
//!   run under a *shared* lock and proceed concurrently.
//! - **Concurrency:** N shards (key hash high-bits → shard), each behind an
//!   `RwLock` that is `parking_lot::RwLock` under `std` (small, userspace
//!   fast-path) and `spin::RwLock` under `no_std`. `get` / `peek` take the read
//!   lock; `insert` / `remove` / eviction take the write lock. The total
//!   resident weight is an [`AtomicU64`] so `size()` is lock-free.
//!
//! Full lock-free eviction (concurrent S3-FIFO queues with epoch reclamation)
//! was rejected: it is `std`-only (no mature `no_std` epoch GC) and a large UB
//! surface for marginal gain on a read-heavy block cache. The shared-read +
//! per-shard-write design captures the read-concurrency win on both targets
//! without that risk.

use alloc::collections::VecDeque;
use alloc::vec::Vec;
use core::hash::{BuildHasher, Hash};
use core::sync::atomic::{AtomicU8, Ordering};

#[cfg(feature = "std")]
type RwLock<T> = parking_lot::RwLock<T>;
#[cfg(not(feature = "std"))]
type RwLock<T> = spin::RwLock<T>;

/// Maximum value of an entry's frequency counter (2-bit, S3-FIFO).
const MAX_FREQ: u8 = 3;

/// Assigns a byte weight to a cached `(key, value)` pair. The cache evicts to
/// keep the summed weight under its capacity.
pub trait Weighter<K, V> {
    /// Weight of this entry in the same unit as the cache's capacity (bytes for
    /// the block/blob cache, a unit count for the descriptor cache).
    fn weight(&self, key: &K, value: &V) -> u64;
}

/// A [`Weighter`] that assigns every entry a weight of 1, turning a byte-capacity
/// cache into a count-capacity (max-entries) cache. Used by the descriptor cache.
#[derive(Clone, Copy, Default)]
pub struct UnitWeighter;

impl<K, V> Weighter<K, V> for UnitWeighter {
    #[inline]
    fn weight(&self, _: &K, _: &V) -> u64 {
        1
    }
}

/// Which FIFO queue an entry currently lives in. Mutated only under the shard
/// write lock (eviction moves small→main); never read on the lock-free path.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Location {
    Small,
    Main,
}

/// One cached entry: the value, its weight, the S3-FIFO frequency counter, and
/// the queue it belongs to. `freq` is atomic so a `get` can bump it under a
/// shared (read) lock; all other fields change only under the write lock.
struct Slot<V> {
    value: V,
    weight: u64,
    freq: AtomicU8,
    loc: Location,
}

/// 64-byte-aligned wrapper so adjacent shard locks land on separate cache lines
/// (no false sharing between shards under concurrent access).
#[repr(align(64))]
struct Padded<T>(T);

/// Per-shard S3-FIFO core. Byte-weighted: `small_bytes + main_bytes` is the
/// resident weight, kept at or below `capacity` by eviction. Removal is O(1)
/// via lazy tombstoning — a removed key stays in its FIFO queue and is skipped
/// (as "stale") when popped during eviction.
struct ShardCore<K, V, S> {
    map: hashbrown::HashMap<K, Slot<V>, S>,
    small: VecDeque<K>,
    main: VecDeque<K>,
    ghost: VecDeque<K>,
    ghost_set: hashbrown::HashSet<K, S>,
    small_bytes: u64,
    main_bytes: u64,
    /// Per-shard byte capacity.
    capacity: u64,
    /// Target ceiling for the small queue (~10% of `capacity`); when the small
    /// queue exceeds it, eviction prefers the small queue.
    small_target: u64,
    /// Max number of fingerprints retained in the ghost queue.
    ghost_capacity: usize,
}

impl<K, V, S> ShardCore<K, V, S>
where
    K: Eq + Hash + Clone,
    V: Clone,
    S: BuildHasher + Clone,
{
    fn new(capacity: u64, ghost_capacity: usize, hasher: S) -> Self {
        Self {
            map: hashbrown::HashMap::with_hasher(hasher.clone()),
            small: VecDeque::new(),
            main: VecDeque::new(),
            ghost: VecDeque::new(),
            ghost_set: hashbrown::HashSet::with_hasher(hasher),
            small_bytes: 0,
            main_bytes: 0,
            capacity,
            // S3-FIFO small queue target: 10% of capacity (at least 1 to stay
            // meaningful for tiny shards).
            small_target: (capacity / 10).max(1),
            ghost_capacity,
        }
    }

    /// Lock-free-path read: returns the value (cloned) and bumps the frequency
    /// counter. Runs under a shared lock — the only mutation is the atomic
    /// `freq` bump, which is safe to race (the counter is a hint).
    fn get(&self, key: &K) -> Option<V> {
        let slot = self.map.get(key)?;
        // Saturating bump. A racing pair of `get`s may drop one increment; that
        // only mildly under-counts frequency, never corrupts state.
        let f = slot.freq.load(Ordering::Relaxed);
        if f < MAX_FREQ {
            slot.freq.store(f + 1, Ordering::Relaxed);
        }
        Some(slot.value.clone())
    }

    /// Read without promotion: returns the value (cloned) without touching the
    /// frequency counter. Used by the partial-decode tier, which inspects an
    /// entry without counting it as a hit.
    #[cfg(any(feature = "zstd", test))]
    fn peek(&self, key: &K) -> Option<V> {
        self.map.get(key).map(|slot| slot.value.clone())
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    /// Inserts or replaces `key`, evicting to stay within capacity. The shard's
    /// running `small_bytes` / `main_bytes` tallies are kept current throughout
    /// (the sharded wrapper reads them under a shared lock for `weight()`).
    fn insert(&mut self, key: K, value: V, weight: u64) {
        if let Some(slot) = self.map.get_mut(&key) {
            // Replace in place: adjust the owning queue's byte tally by the
            // weight delta, keep the queue position and frequency.
            let old = slot.weight;
            slot.value = value;
            slot.weight = weight;
            match slot.loc {
                Location::Small => self.small_bytes = adjust(self.small_bytes, old, weight),
                Location::Main => self.main_bytes = adjust(self.main_bytes, old, weight),
            }
        } else {
            // Fresh entry. A key found in the ghost queue (recently evicted from
            // the small queue) is admitted straight to the main queue; otherwise
            // it enters the small queue.
            let loc = if self.ghost_set.remove(&key) {
                Location::Main
            } else {
                Location::Small
            };
            self.map.insert(
                key.clone(),
                Slot {
                    value,
                    weight,
                    freq: AtomicU8::new(0),
                    loc,
                },
            );
            match loc {
                Location::Small => {
                    self.small_bytes += weight;
                    self.small.push_back(key);
                }
                Location::Main => {
                    self.main_bytes += weight;
                    self.main.push_back(key);
                }
            }
        }

        self.evict_to_capacity();
    }

    /// Removes `key` if present (lazy tombstone — the stale queue entry is
    /// skipped on its next pop).
    fn remove(&mut self, key: &K) {
        let Some(slot) = self.map.remove(key) else {
            return;
        };
        match slot.loc {
            Location::Small => self.small_bytes -= slot.weight,
            Location::Main => self.main_bytes -= slot.weight,
        }
    }

    /// Resident weight of this shard (`small_bytes + main_bytes`). Read under a
    /// shared lock by the wrapper's `weight()`.
    #[inline]
    fn resident_bytes(&self) -> u64 {
        self.small_bytes + self.main_bytes
    }

    fn evict_to_capacity(&mut self) {
        while self.resident_bytes() > self.capacity {
            if !self.evict_one() {
                // Both queues are out of live entries — nothing left to free
                // (e.g. a single entry larger than the whole shard capacity).
                break;
            }
        }
    }

    /// Frees (or migrates) one live entry. Returns `false` only when both queues
    /// hold no live entry. A small→main migration returns `true` (progress) even
    /// though it frees no bytes: it shrinks the small queue, so a bounded number
    /// of further calls reach a real eviction.
    fn evict_one(&mut self) -> bool {
        let prefer_small = self.main_bytes == 0 || self.small_bytes >= self.small_target;
        if prefer_small {
            self.evict_from_small() || self.evict_from_main()
        } else {
            self.evict_from_main() || self.evict_from_small()
        }
    }

    /// Processes the small queue's oldest live entry: a frequently-used one
    /// (freq > 0) migrates to the main queue; an unused one is evicted and its
    /// key recorded in the ghost queue. Returns `false` if the small queue holds
    /// no live entry.
    fn evict_from_small(&mut self) -> bool {
        while let Some(key) = self.small.pop_front() {
            let Some(slot) = self.map.get_mut(&key) else {
                continue; // stale tombstone — already removed
            };
            let w = slot.weight;
            if slot.freq.load(Ordering::Relaxed) > 0 {
                // Hot: promote to main, reset frequency.
                slot.freq.store(0, Ordering::Relaxed);
                slot.loc = Location::Main;
                self.small_bytes -= w;
                self.main_bytes += w;
                self.main.push_back(key);
            } else {
                // Cold: evict, remember the fingerprint in the ghost queue.
                self.map.remove(&key);
                self.small_bytes -= w;
                self.push_ghost(key);
            }
            return true;
        }
        false
    }

    /// Processes the main queue's oldest live entry: one with remaining
    /// frequency is reinserted at the back with its counter decremented; an
    /// unused one is evicted. Returns `false` if the main queue holds no live
    /// entry.
    fn evict_from_main(&mut self) -> bool {
        while let Some(key) = self.main.pop_front() {
            let Some(slot) = self.map.get_mut(&key) else {
                continue; // stale tombstone
            };
            let f = slot.freq.load(Ordering::Relaxed);
            if f > 0 {
                slot.freq.store(f - 1, Ordering::Relaxed);
                self.main.push_back(key);
            } else {
                let w = slot.weight;
                self.map.remove(&key);
                self.main_bytes -= w;
            }
            return true;
        }
        false
    }

    fn push_ghost(&mut self, key: K) {
        if self.ghost_capacity == 0 {
            return;
        }
        if self.ghost_set.insert(key.clone()) {
            self.ghost.push_back(key);
        }
        while self.ghost.len() > self.ghost_capacity {
            if let Some(old) = self.ghost.pop_front() {
                self.ghost_set.remove(&old);
            }
        }
    }
}

/// Applies a weight replacement (`old` → `new`) to a running byte tally without
/// risking underflow when `new > old`.
#[inline]
fn adjust(total: u64, old: u64, new: u64) -> u64 {
    total - old + new
}

/// A sharded, byte-weighted S3-FIFO cache. Generic over key, value, [`Weighter`]
/// and hasher; cloned values are returned from `get` / `peek`.
pub struct ShardedCache<K, V, W, S> {
    shards: Vec<Padded<RwLock<ShardCore<K, V, S>>>>,
    /// `shards.len() - 1`; `shards.len()` is always a power of two so this masks
    /// a hash to a shard index.
    shard_mask: u64,
    weighter: W,
    hasher: S,
    capacity: u64,
}

impl<K, V, W, S> ShardedCache<K, V, W, S>
where
    K: Eq + Hash + Clone,
    V: Clone,
    W: Weighter<K, V>,
    S: BuildHasher + Clone,
{
    /// Builds a cache with `capacity` total weight, split across
    /// `shard_count_hint` shards (rounded up to a power of two, clamped to
    /// `[1, 256]`). `est_items` seeds the ghost-queue capacity per shard.
    pub fn with_weighter(
        capacity: u64,
        shard_count_hint: usize,
        est_items: usize,
        weighter: W,
        hasher: S,
    ) -> Self {
        let shard_count = shard_count_hint.next_power_of_two().clamp(1, 256);
        // Distribute capacity evenly; round up so the summed shard capacity is
        // >= the requested total (never silently smaller).
        let per_shard_cap = capacity.div_ceil(shard_count as u64);
        // Ghost queue holds roughly as many fingerprints as the shard is
        // expected to hold live entries (S3-FIFO sizes the ghost ~ main).
        let ghost_capacity = (est_items / shard_count).max(16);

        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(Padded(RwLock::new(ShardCore::new(
                per_shard_cap,
                ghost_capacity,
                hasher.clone(),
            ))));
        }

        Self {
            shards,
            shard_mask: shard_count as u64 - 1,
            weighter,
            hasher,
            capacity,
        }
    }

    #[inline]
    fn shard(&self, key: &K) -> &RwLock<ShardCore<K, V, S>> {
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
        &self.shards[idx].0
    }

    /// Returns the value for `key`, counting the access as a hit (promotes).
    pub fn get(&self, key: &K) -> Option<V> {
        self.shard(key).read().get(key)
    }

    /// Returns the value for `key` WITHOUT counting it as a hit (no promotion).
    #[cfg(any(feature = "zstd", test))]
    pub fn peek(&self, key: &K) -> Option<V> {
        self.shard(key).read().peek(key)
    }

    /// Inserts or replaces `key`, evicting as needed to stay within capacity.
    pub fn insert(&self, key: K, value: V) {
        let weight = self.weighter.weight(&key, &value);
        self.shard(&key).write().insert(key, value, weight);
    }

    /// Removes `key` if present.
    pub fn remove(&self, key: &K) {
        self.shard(key).write().remove(key);
    }

    /// Total resident weight across all shards. Sums each shard's tally under a
    /// shared (read) lock — not the hot path (metrics / tests), and avoids a
    /// 64-bit atomic (unavailable on 32-bit no-std targets like
    /// `thumbv7em-none-eabihf`).
    pub fn weight(&self) -> u64 {
        self.shards
            .iter()
            .map(|s| s.0.read().resident_bytes())
            .sum()
    }

    /// Total cache capacity (the value passed to [`Self::with_weighter`]).
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Number of resident entries across all shards (takes each shard's read
    /// lock; intended for diagnostics, not the hot path).
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.0.read().len()).sum()
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::expect_used,
    reason = "test code"
)]
mod tests {
    use super::*;
    use rustc_hash::FxBuildHasher;

    /// A byte weighter over `(u64 key, Vec<u8> value)`: weight is the value len.
    #[derive(Clone, Copy)]
    struct LenWeighter;
    impl Weighter<u64, alloc::vec::Vec<u8>> for LenWeighter {
        fn weight(&self, _: &u64, v: &alloc::vec::Vec<u8>) -> u64 {
            v.len() as u64
        }
    }

    fn byte_cache(
        capacity: u64,
    ) -> ShardedCache<u64, alloc::vec::Vec<u8>, LenWeighter, FxBuildHasher> {
        ShardedCache::with_weighter(capacity, 8, 1024, LenWeighter, FxBuildHasher)
    }

    #[test]
    fn insert_get_roundtrip() {
        let c = byte_cache(10_000);
        c.insert(1, vec![0u8; 100]);
        assert_eq!(c.get(&1), Some(vec![0u8; 100]));
        assert_eq!(c.get(&2), None);
    }

    #[test]
    fn peek_does_not_promote_but_get_does() {
        let c = byte_cache(10_000);
        c.insert(7, vec![1u8; 50]);
        // peek returns the value without counting a hit.
        assert_eq!(c.peek(&7), Some(vec![1u8; 50]));
        assert_eq!(c.peek(&999), None);
    }

    #[test]
    fn weight_tracks_resident_bytes() {
        let c = byte_cache(10_000);
        assert_eq!(c.weight(), 0);
        c.insert(1, vec![0u8; 100]);
        c.insert(2, vec![0u8; 200]);
        assert_eq!(c.weight(), 300);
        c.remove(&1);
        assert_eq!(c.weight(), 200);
        c.remove(&999); // absent — no-op
        assert_eq!(c.weight(), 200);
    }

    #[test]
    fn replace_adjusts_weight_in_place() {
        let c = byte_cache(10_000);
        c.insert(1, vec![0u8; 100]);
        c.insert(1, vec![0u8; 250]); // replace, larger
        assert_eq!(c.weight(), 250);
        assert_eq!(c.get(&1), Some(vec![0u8; 250]));
        c.insert(1, vec![0u8; 30]); // replace, smaller
        assert_eq!(c.weight(), 30);
    }

    #[test]
    fn eviction_keeps_resident_under_capacity() {
        let c = byte_cache(1_000);
        // Insert far more than capacity; the cache must evict to stay bounded.
        for i in 0..1_000u64 {
            c.insert(i, vec![0u8; 100]);
        }
        // Allow a single in-flight item of slack over the nominal capacity.
        assert!(
            c.weight() <= 1_000 + 100,
            "resident weight {} exceeded capacity",
            c.weight(),
        );
    }

    #[test]
    fn frequently_read_entries_survive_eviction_pressure() {
        let c = byte_cache(2_000);
        // A hot key, read repeatedly, should be retained under churn that
        // evicts many cold keys.
        c.insert(0, vec![0u8; 100]);
        for _ in 0..8 {
            assert_eq!(c.get(&0), Some(vec![0u8; 100]));
        }
        for i in 1..200u64 {
            c.insert(i, vec![0u8; 100]);
            let _ = c.get(&0); // keep touching the hot key
        }
        assert_eq!(c.get(&0), Some(vec![0u8; 100]), "hot key was evicted");
    }

    #[test]
    fn unit_weighter_is_count_capacity() {
        let c: ShardedCache<u64, u64, UnitWeighter, FxBuildHasher> =
            ShardedCache::with_weighter(4, 2, 64, UnitWeighter, FxBuildHasher);
        for i in 0..100u64 {
            c.insert(i, i);
        }
        // Unit weight → capacity is a max entry count (+ per-shard slack).
        assert!(c.weight() <= 4 + 2, "entry count {} exceeded", c.weight());
    }

    #[test]
    fn oversized_entry_does_not_wedge_the_cache() {
        let c = byte_cache(1_000);
        c.insert(1, vec![0u8; 5_000]); // larger than the whole cache
        // It must not be retained, and the cache must stay usable.
        c.insert(2, vec![0u8; 100]);
        assert_eq!(c.get(&2), Some(vec![0u8; 100]));
        assert!(c.weight() <= 1_100);
    }

    // Concurrent stress: many threads hammer get/insert/remove on a shared
    // cache. The win of the design is that `get` takes only a shared lock, so
    // readers run in parallel; this test exercises that path under contention
    // and asserts the invariants (no panic, no deadlock, weight stays bounded
    // and consistent with what is resident) hold afterwards.
    #[cfg(feature = "std")]
    #[test]
    fn concurrent_stress_keeps_invariants() {
        use std::sync::Arc;
        use std::thread;

        let cache = Arc::new(byte_cache(50_000));
        let threads: Vec<_> = (0..8)
            .map(|t| {
                let cache = Arc::clone(&cache);
                thread::spawn(move || {
                    for i in 0..5_000u64 {
                        let key = (t * 5_000 + i) % 2_000; // overlapping key space
                        match i % 4 {
                            // Payload byte value is irrelevant (all entries are
                            // 64 B); a constant avoids a u64→u8 cast lint.
                            0 => cache.insert(key, vec![0u8; 64]),
                            1 => {
                                let _ = cache.get(&key);
                            }
                            2 => {
                                let _ = cache.peek(&key);
                            }
                            _ => cache.remove(&key),
                        }
                    }
                })
            })
            .collect();
        for h in threads {
            h.join().expect("worker thread panicked");
        }

        // Weight must stay within capacity (+ one in-flight item of slack) and
        // equal the actual resident bytes recomputed from the entry count.
        assert!(
            cache.weight() <= 50_000 + 64,
            "weight {} exceeded capacity after concurrent churn",
            cache.weight(),
        );
        // The atomic total must agree with len()*item_weight (all items are 64B):
        // proves the signed-delta bookkeeping never drifted under concurrency.
        assert_eq!(
            cache.weight(),
            cache.len() as u64 * 64,
            "atomic weight diverged from resident entries",
        );
    }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Coordination of concurrent cold loads of the same cached object.
//!
//! Readers that miss the cache for one block (or blob) at the same time would
//! each read, verify and decode the same bytes. A miss here claims the load:
//! the first claimant leads and does the work, the others wait for it and
//! take its result, so the physical work happens once.
//!
//! Only a miss reaches this registry: a cache hit never touches it. A load is
//! joined only by a request with the same identity (role, extent, codec), and
//! the registry is bounded: when it is full, or when an incompatible load of
//! the same key is in flight, the request loads on its own as if the registry
//! did not exist. A leader that fails or unwinds abandons its ticket, and its
//! waiters then load on their own and meet the real outcome themselves, so an
//! error is never cached and never handed to a reader that did not cause it.
//!
//! Waiting blocks the thread. A load served from the page cache settles in
//! microseconds, less than parking and waking a thread costs, so a waiter
//! first spins and yields for about that long; only a slower load parks it,
//! on a condition variable under `std` (`no_std` keeps spinning). No lock of
//! this registry or of the cache is held while a load runs or while a waiter
//! waits.
//!
//! A ticket nobody else holds once its load closes is kept for the next load
//! of its shard, so an uncontended miss allocates nothing here.

use alloc::sync::Arc;
use alloc::vec::Vec;
use core::cell::UnsafeCell;
use core::hash::{BuildHasher, Hash};
use core::sync::atomic::{AtomicBool, AtomicU32, Ordering};

#[cfg(feature = "std")]
type Mutex<T> = parking_lot::Mutex<T>;
// no-std: a spin mutex; the waiter below spins on it instead of parking.
#[cfg(not(feature = "std"))]
type Mutex<T> = spin::Mutex<T>;

/// Rounds of exponentially longer spinning a waiter does before yielding:
/// 1023 spin hints in all, a few microseconds.
const SPIN_ROUNDS: u32 = 10;
/// Times a waiter yields its core, once spinning is over, before parking.
#[cfg(feature = "std")]
const YIELD_ROUNDS: u32 = 8;
/// Tickets one shard keeps for reuse.
const SPARE_TICKETS: usize = 4;

/// One load in flight: what it loads (its identity) and how it ended.
///
/// Its lead settles it once: it writes `value` (`None` for a load that failed
/// or unwound, whose waiters then load on their own) and only then sets
/// `is_settled`. A reader looks at `value` only after seeing `is_settled`
/// set, and `value` is written again only through `&mut`, once nothing else
/// holds the ticket, so the waiters of a load read its value at once, without
/// taking turns on a lock.
struct Ticket<V, I> {
    identity: I,
    value: UnsafeCell<Option<V>>,
    is_settled: AtomicBool,
    /// Waiters that spun out and park; the lead wakes them only if any did.
    #[cfg(feature = "std")]
    parked: core::sync::atomic::AtomicU32,
    #[cfg(feature = "std")]
    park_lock: parking_lot::Mutex<()>,
    #[cfg(feature = "std")]
    settled: parking_lot::Condvar,
}

// SAFETY: `value` is written once, before `is_settled` is set with release
// ordering, and read only after `is_settled` is seen set with acquire
// ordering; every other write takes `&mut self`. Sharing a ticket shares `&V`
// between threads (`V: Sync`) and hands them clones of it (`V: Send`).
unsafe impl<V: Send + Sync, I: Sync> Sync for Ticket<V, I> {}

impl<V, I> Ticket<V, I> {
    fn new(identity: I) -> Self {
        Self {
            identity,
            value: UnsafeCell::new(None),
            is_settled: AtomicBool::new(false),
            #[cfg(feature = "std")]
            parked: core::sync::atomic::AtomicU32::new(0),
            #[cfg(feature = "std")]
            park_lock: parking_lot::Mutex::new(()),
            #[cfg(feature = "std")]
            settled: parking_lot::Condvar::new(),
        }
    }

    /// Settles the load with `value`, `None` when it failed.
    ///
    /// # Safety
    ///
    /// Called at most once while the ticket is shared: by its lead, which
    /// settles it exactly once.
    unsafe fn settle(&self, value: Option<V>) {
        // SAFETY: the lead is the one writer, and no reader looks at the
        // value before `is_settled` is set below.
        unsafe {
            *self.value.get() = value;
        }
        self.is_settled.store(true, Ordering::SeqCst);
        // A waiter counts itself parked before it checks `is_settled` and
        // sleeps, all under `park_lock`; both sides are sequentially
        // consistent, so either it sees the ticket settled and never sleeps,
        // or this sees it counted and wakes it under the same lock.
        #[cfg(feature = "std")]
        if self.parked.load(Ordering::SeqCst) > 0 {
            drop(self.park_lock.lock());
            self.settled.notify_all();
        }
    }

    /// The settled outcome, or `None` while the load runs.
    fn outcome(&self) -> Option<&Option<V>> {
        if self.is_settled.load(Ordering::Acquire) {
            // SAFETY: `is_settled` is set after the value was written, and the
            // value is not written again while the ticket is shared.
            Some(unsafe { &*self.value.get() })
        } else {
            None
        }
    }

    /// Readies a ticket nobody else holds for another load, returning the
    /// value of its last one for the caller to drop.
    fn reset(&mut self, identity: I) -> Option<V> {
        self.identity = identity;
        *self.is_settled.get_mut() = false;
        #[cfg(feature = "std")]
        {
            *self.parked.get_mut() = 0;
        }
        self.value.get_mut().take()
    }
}

impl<V: Clone, I> Ticket<V, I> {
    /// Waits for the leader, returning its value, or `None` when it gave up.
    fn wait(&self) -> Option<V> {
        let mut round = 0;
        loop {
            if let Some(outcome) = self.outcome() {
                return outcome.clone();
            }
            // `round` stops counting at its last stage, so it stays at most
            // `SPIN_ROUNDS + YIELD_ROUNDS`.
            if round < SPIN_ROUNDS {
                for _ in 0..1_u32 << round {
                    core::hint::spin_loop();
                }
                round += 1;
            } else {
                #[cfg(feature = "std")]
                {
                    if round >= SPIN_ROUNDS + YIELD_ROUNDS {
                        return self.park();
                    }
                    std::thread::yield_now();
                    round += 1;
                }
                // no-std: nothing to park on; keep spinning.
                #[cfg(not(feature = "std"))]
                core::hint::spin_loop();
            }
        }
    }

    /// Sleeps until the lead settles the load.
    #[cfg(feature = "std")]
    fn park(&self) -> Option<V> {
        let mut guard = self.park_lock.lock();
        self.parked.fetch_add(1, Ordering::SeqCst);
        while !self.is_settled.load(Ordering::SeqCst) {
            self.settled.wait(&mut guard);
        }
        drop(guard);
        self.outcome().and_then(Option::clone)
    }

    /// The leader's value if it has published one, without waiting.
    fn try_take(&self) -> Option<V> {
        self.outcome().and_then(Option::clone)
    }
}

#[cfg(feature = "std")]
std::thread_local! {
    /// Loads the calling thread leads for a batched read. Such a read
    /// publishes them only once its driver handles their completions, and the
    /// driver runs other loads on the same thread meanwhile: were one of those
    /// to wait on a load another driver leads while that driver waits on one
    /// of ours, neither would ever publish. So a thread holding a batched lead
    /// never waits.
    static BATCH_LEADS: core::cell::Cell<usize> = const { core::cell::Cell::new(0) };
}

/// Whether the calling thread may wait on another request's load: it holds
/// no lead of a batched read.
#[must_use]
pub fn may_wait() -> bool {
    #[cfg(feature = "std")]
    {
        BATCH_LEADS.with(core::cell::Cell::get) == 0
    }
    // no-std: no thread-local state; batched reads take no leads there.
    #[cfg(not(feature = "std"))]
    {
        true
    }
}

/// Whether a batched read may lead loads on the calling thread: only where
/// the thread's held leads are counted, so its other loads stop waiting.
#[must_use]
pub const fn batches_may_lead() -> bool {
    cfg!(feature = "std")
}

/// Counts one batched lead on the calling thread for as long as it lives.
///
/// Created only by a read the blocking driver runs, which takes the lead,
/// publishes or drops it and runs its other loads all on the one thread it
/// drives on; a read a caller resumes on other threads takes no lead.
pub struct BatchToken(());

impl BatchToken {
    /// Counts a batched lead on the calling thread.
    #[must_use]
    pub fn new() -> Self {
        #[cfg(feature = "std")]
        BATCH_LEADS.with(|count| count.set(count.get() + 1));
        Self(())
    }
}

impl Default for BatchToken {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for BatchToken {
    fn drop(&mut self) {
        // Every token counted one on this thread (it cannot leave it), so the
        // count is at least one here.
        #[cfg(feature = "std")]
        BATCH_LEADS.with(|count| count.set(count.get() - 1));
    }
}

/// 64-byte-aligned so neighbouring shard locks do not share a cache line.
#[repr(align(64))]
struct Padded<T>(T);

/// What one shard holds: the tickets of its keys' loads in flight, and
/// settled tickets nobody holds any more, kept for reuse.
struct ShardState<K, V, I, S> {
    loads: hashbrown::HashMap<K, Arc<Ticket<V, I>>, S>,
    spare: Vec<Arc<Ticket<V, I>>>,
}

/// One shard of the registry: its loads, and how long the physical loads of
/// its keys take.
struct Shard<K, V, I, S> {
    state: Mutex<ShardState<K, V, I, S>>,
    /// Smoothed duration of the shard's sampled physical loads, in
    /// nanoseconds; zero until one is sampled.
    load_nanos: AtomicU32,
}

/// One in this many physical loads of a thread is timed.
#[cfg(feature = "std")]
const LOAD_SAMPLE_EVERY: u32 = 16;

#[cfg(feature = "std")]
std::thread_local! {
    /// Physical loads the calling thread has started, counting modulo
    /// [`LOAD_SAMPLE_EVERY`] to pick the ones it times.
    static LOADS_STARTED: core::cell::Cell<u32> = const { core::cell::Cell::new(0) };
}

/// The timing of one physical load, when it is one of the sampled ones.
#[derive(Clone, Copy)]
pub struct LoadTimer {
    // no-std: no clock to read; loads are not timed and the estimates stay
    // at zero.
    #[cfg(feature = "std")]
    started: Option<std::time::Instant>,
}

impl LoadTimer {
    /// Starts timing a physical load about to run on the calling thread, if
    /// it is one of the sampled ones.
    #[must_use]
    pub fn start() -> Self {
        #[cfg(feature = "std")]
        {
            let sampled = LOADS_STARTED.with(|count| {
                // A counter modulo the sampling period: it is meant to wrap.
                let next = count.get().wrapping_add(1) % LOAD_SAMPLE_EVERY;
                count.set(next);
                next == 0
            });
            Self {
                started: sampled.then(std::time::Instant::now),
            }
        }
        #[cfg(not(feature = "std"))]
        Self {}
    }
}

/// The loads in flight, sharded by key hash and bounded per shard.
///
/// A miss coordinates through the registry only while the physical loads of
/// its shard take at least the coalescing threshold: a load quicker than
/// that costs less than waiting for another reader's, and the miss then
/// loads alone without taking any lock here.
pub struct InflightLoads<K, V, I, S> {
    shards: Vec<Padded<Shard<K, V, I, S>>>,
    shard_mask: u64,
    hasher: S,
    /// Most loads one shard coordinates at once; past it a request loads
    /// alone, so a burst of distinct cold keys cannot grow the registry
    /// without bound.
    per_shard_limit: usize,
    /// The load duration, in nanoseconds, from which misses coordinate;
    /// zero coordinates every miss, and one past any estimate (an estimate
    /// is at most `u32::MAX`) none.
    coalesce_after_nanos: u64,
}

/// What a cache miss does about its load.
pub enum Claim<'a, K, V, I, S>
where
    K: Eq + Hash + Clone,
    S: BuildHasher,
{
    /// No load of this object is in flight: this request does it and must
    /// [`Lead::publish`] the result (dropping the lead abandons it).
    Lead(Lead<'a, K, V, I, S>),
    /// A compatible load is in flight: [`Join::wait`] for it.
    Join(Join<V, I>),
    /// Load alone: an incompatible load of the key is in flight, or the
    /// registry is full.
    Alone,
}

/// The leader's hold on a load. Settles the ticket exactly once: by
/// [`Self::publish`], or as abandoned when dropped without it, which covers
/// every error return and an unwind.
///
/// Borrows the registry rather than sharing ownership of it: every reader on
/// every core leads loads, and a reference count on the registry would be one
/// cache line they all write.
pub struct Lead<'a, K, V, I, S>
where
    K: Eq + Hash + Clone,
    S: BuildHasher,
{
    loads: &'a InflightLoads<K, V, I, S>,
    key: K,
    /// Present until the lead finishes, publishing or dropped.
    ticket: Option<Arc<Ticket<V, I>>>,
}

/// A waiter's hold on a load another request leads.
pub struct Join<V, I>(Arc<Ticket<V, I>>);

impl<V: Clone, I> Join<V, I> {
    /// Blocks until the leader settles, returning its value, or `None` when
    /// it failed and this request has to load on its own.
    pub fn wait(self) -> Option<V> {
        self.0.wait()
    }

    /// The leader's value if it has published one, without waiting: `None`
    /// while it is still loading or after it failed. For a reader that must
    /// not block (one holding leads of its own others may wait on), which
    /// then loads on its own instead.
    pub fn try_take(&self) -> Option<V> {
        self.0.try_take()
    }
}

impl<K, V, I, S> Lead<'_, K, V, I, S>
where
    K: Eq + Hash + Clone,
    V: Clone,
    S: BuildHasher,
{
    /// Hands `value` to every waiter and closes the load. Publish only a
    /// value already made visible where a later request looks first (the
    /// cache), or a value no later request needs: once closed, a new miss of
    /// the key leads a load of its own.
    pub fn publish(self, value: V) {
        self.publish_with(|| value);
    }

    /// As [`Self::publish`], building the value only when a request waits
    /// for it.
    pub fn publish_with(mut self, value: impl FnOnce() -> V) {
        self.finish(Some(value));
    }

    /// Whether any request is waiting for this load now. Only the registry
    /// and this lead hold the ticket otherwise. A request that joins after the
    /// answer and before the load closes finds no value and loads on its own,
    /// so `false` lets a leader skip building a value nobody would take.
    pub fn has_waiters(&self) -> bool {
        self.ticket
            .as_ref()
            .is_some_and(|ticket| Arc::strong_count(ticket) > 2)
    }
}

impl<K, V, I, S> Drop for Lead<'_, K, V, I, S>
where
    K: Eq + Hash + Clone,
    S: BuildHasher,
{
    fn drop(&mut self) {
        // Not published: the load failed or unwound. Waiters load on their
        // own, so none is left waiting and no error is remembered.
        self.finish(None::<fn() -> V>);
    }
}

impl<K, V, I, S> Lead<'_, K, V, I, S>
where
    K: Eq + Hash + Clone,
    S: BuildHasher,
{
    /// Closes the load and settles it with the value `value` builds (`None`
    /// when it failed) for the requests waiting on it; a no-op once done.
    fn finish(&mut self, value: Option<impl FnOnce() -> V>) {
        let Some(ticket) = self.ticket.take() else {
            return;
        };
        if let Some(ticket) = self.loads.close(&self.key, ticket) {
            // SAFETY: the ticket was taken out of the lead above, so this is
            // its one settling.
            unsafe { ticket.settle(value.map(|build| build())) };
        }
    }
}

impl<K, V, I, S> InflightLoads<K, V, I, S>
where
    K: Eq + Hash + Clone,
    V: Clone,
    I: PartialEq,
    S: BuildHasher + Clone,
{
    /// A registry of `shards` shards (rounded up to a power of two, at most
    /// 256) coordinating up to `per_shard_limit` loads each, once their
    /// physical loads take `coalesce_after` (see [`Self::set_coalesce_after`]).
    pub fn new(
        shards: usize,
        per_shard_limit: usize,
        coalesce_after: core::time::Duration,
        hasher: S,
    ) -> Self {
        let count = shards.next_power_of_two().clamp(1, 256);
        let mut list = Vec::with_capacity(count);
        for _ in 0..count {
            list.push(Padded(Shard {
                state: Mutex::new(ShardState {
                    loads: hashbrown::HashMap::with_hasher(hasher.clone()),
                    spare: Vec::with_capacity(SPARE_TICKETS),
                }),
                load_nanos: AtomicU32::new(0),
            }));
        }
        let mut loads = Self {
            shards: list,
            shard_mask: count as u64 - 1,
            hasher,
            per_shard_limit,
            coalesce_after_nanos: 0,
        };
        loads.set_coalesce_after(coalesce_after);
        loads
    }

    /// Claims the load of `key` for a request of `identity`. The caller has
    /// just missed the cache; a leader checks it once more before loading,
    /// since a load that closed between that miss and this claim has already
    /// put its result there. While the key's shard loads quicker than the
    /// coalescing threshold, the request loads alone without a lock taken.
    pub fn claim(&self, key: K, identity: I) -> Claim<'_, K, V, I, S> {
        let shard = self.shard(&key);
        if !self.coalesces(shard) {
            return Claim::Alone;
        }
        let mut shard = shard.state.lock();
        if let Some(ticket) = shard.loads.get(&key) {
            return if ticket.identity == identity {
                Claim::Join(Join(Arc::clone(ticket)))
            } else {
                Claim::Alone
            };
        }
        if shard.loads.len() >= self.per_shard_limit {
            return Claim::Alone;
        }
        // A spare ticket is held by nothing but the shard (and holds no value:
        // the close that kept it took it), so it is reset in place rather
        // than allocated anew.
        let ticket = match shard.spare.pop() {
            Some(mut spare) => match Arc::get_mut(&mut spare) {
                Some(unshared) => {
                    let previous = unshared.reset(identity);
                    debug_assert!(previous.is_none(), "a spare ticket holds no value");
                    spare
                }
                None => Arc::new(Ticket::new(identity)),
            },
            None => Arc::new(Ticket::new(identity)),
        };
        shard.loads.insert(key.clone(), Arc::clone(&ticket));
        drop(shard);
        Claim::Lead(Lead {
            loads: self,
            key,
            ticket: Some(ticket),
        })
    }

    /// The value of a load of `key` for `identity` that has already been
    /// published and not yet closed, without claiming anything: for a reader
    /// that may neither wait nor lead.
    pub fn take_ready(&self, key: &K, identity: &I) -> Option<V> {
        let shard = self.shard(key);
        if !self.coalesces(shard) {
            return None;
        }
        let ticket = Arc::clone(shard.state.lock().loads.get(key)?);
        if ticket.identity != *identity {
            return None;
        }
        ticket.try_take()
    }

    /// How many loads are in flight, for tests.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.0.state.lock().loads.len())
            .sum()
    }

    /// How many requests wait on loads in flight, for tests.
    #[cfg(test)]
    pub fn waiters(&self) -> usize {
        // A ticket in the registry is held by the registry and by its lead,
        // which closes it before letting go; every other holder waits.
        self.shards
            .iter()
            .flat_map(|shard| {
                shard
                    .0
                    .state
                    .lock()
                    .loads
                    .values()
                    .map(|ticket| Arc::strong_count(ticket) - 2)
                    .collect::<Vec<_>>()
            })
            .sum()
    }
}

impl<K, V, I, S> InflightLoads<K, V, I, S>
where
    K: Eq + Hash + Clone,
    S: BuildHasher,
{
    /// Sets the duration a physical load must take for misses to coordinate:
    /// zero coordinates every miss; one longer than any load is ever measured
    /// to take, none. Without a clock (`no_std`) loads are never measured, so
    /// only zero coordinates there.
    pub fn set_coalesce_after(&mut self, coalesce_after: core::time::Duration) {
        // Past `u64::MAX` nanoseconds (five centuries) is as good as never.
        self.coalesce_after_nanos = u64::try_from(coalesce_after.as_nanos()).unwrap_or(u64::MAX);
    }

    /// The duration a physical load must take for misses to coordinate.
    pub fn coalesce_after(&self) -> core::time::Duration {
        core::time::Duration::from_nanos(self.coalesce_after_nanos)
    }

    /// Whether misses of `shard` coordinate: its loads take at least the
    /// threshold.
    fn coalesces(&self, shard: &Shard<K, V, I, S>) -> bool {
        u64::from(shard.load_nanos.load(Ordering::Relaxed)) >= self.coalesce_after_nanos
    }

    /// Folds the physical load of `key` that `timer` timed, if it is a
    /// sampled one, into its shard's estimate.
    pub fn record_load(&self, key: &K, timer: LoadTimer) {
        #[cfg(feature = "std")]
        if let Some(started) = timer.started {
            // A load past `u32::MAX` nanoseconds (four seconds) is simply slow.
            let sample = u32::try_from(started.elapsed().as_nanos()).unwrap_or(u32::MAX);
            self.fold_sample(key, sample);
        }
        #[cfg(not(feature = "std"))]
        let _ = (key, timer);
    }

    /// Moves the estimate of `key`'s shard an eighth of the way to `sample`.
    /// Read and written apart: it is a statistic, and two samples folded at
    /// once lose one of them rather than anything that needs to be exact.
    fn fold_sample(&self, key: &K, sample: u32) {
        let estimate = &self.shard(key).load_nanos;
        let old = estimate.load(Ordering::Relaxed);
        // Seven eighths of one `u32` and an eighth of another stay within one.
        estimate.store(old - old / 8 + sample / 8, Ordering::Relaxed);
    }

    /// The load-time estimate of `key`'s shard, in nanoseconds, for tests.
    #[cfg(test)]
    fn estimate(&self, key: &K) -> u32 {
        self.shard(key).load_nanos.load(Ordering::Relaxed)
    }

    fn shard(&self, key: &K) -> &Shard<K, V, I, S> {
        let hash = self.hasher.hash_one(key);
        #[expect(
            clippy::cast_possible_truncation,
            reason = "the masked index is at most 255"
        )]
        let index = (((hash >> 32) ^ hash) & self.shard_mask) as usize;
        #[expect(
            clippy::indexing_slicing,
            reason = "the mask keeps the index below the power-of-two shard count"
        )]
        &self.shards[index].0
    }

    /// Removes `ticket` from the registry, unless another load of the key has
    /// taken its place. Returns it when requests wait on it, for the lead to
    /// settle; otherwise nobody can ever see its outcome, so it is not
    /// settled at all, and is kept for reuse.
    fn close(&self, key: &K, mut ticket: Arc<Ticket<V, I>>) -> Option<Arc<Ticket<V, I>>> {
        let mut shard = self.shard(key).state.lock();
        if shard
            .loads
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(current, &ticket))
        {
            shard.loads.remove(key);
        }
        // Out of the registry, the ticket gains no new holder, so a ticket
        // only the lead holds now has no waiter and never will.
        if Arc::get_mut(&mut ticket).is_none() {
            return Some(ticket);
        }
        if shard.spare.len() < SPARE_TICKETS {
            shard.spare.push(ticket);
        }
        None
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::expect_used, reason = "test code")]
mod tests;

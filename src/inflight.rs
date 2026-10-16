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
//! Waiting blocks the thread: on a condition variable under `std`, by spinning
//! under `no_std`. No lock of this registry or of the cache is held while a
//! load runs or while a waiter waits.

use alloc::sync::Arc;
use alloc::vec::Vec;
use core::hash::{BuildHasher, Hash};

#[cfg(feature = "std")]
type Mutex<T> = parking_lot::Mutex<T>;
// no-std: a spin mutex; the waiter below spins on it instead of parking.
#[cfg(not(feature = "std"))]
type Mutex<T> = spin::Mutex<T>;

/// Where a ticket's load stands.
enum State<V> {
    /// The leader is loading.
    Pending,
    /// The leader loaded `V`; every waiter takes a clone.
    Ready(V),
    /// The leader failed or unwound: waiters load on their own.
    Abandoned,
}

/// One load in flight: what it loads (its identity) and how it ended.
struct Ticket<V, I> {
    identity: I,
    state: Mutex<State<V>>,
    #[cfg(feature = "std")]
    settled: parking_lot::Condvar,
}

impl<V, I> Ticket<V, I> {
    fn new(identity: I) -> Self {
        Self {
            identity,
            state: Mutex::new(State::Pending),
            #[cfg(feature = "std")]
            settled: parking_lot::Condvar::new(),
        }
    }

    fn settle(&self, state: State<V>) {
        *self.state.lock() = state;
        #[cfg(feature = "std")]
        self.settled.notify_all();
    }
}

impl<V: Clone, I> Ticket<V, I> {
    /// Waits for the leader, returning its value, or `None` when it gave up.
    fn wait(&self) -> Option<V> {
        #[cfg(feature = "std")]
        {
            let mut state = self.state.lock();
            loop {
                match &*state {
                    State::Pending => self.settled.wait(&mut state),
                    State::Ready(value) => return Some(value.clone()),
                    State::Abandoned => return None,
                }
            }
        }
        #[cfg(not(feature = "std"))]
        loop {
            match &*self.state.lock() {
                State::Pending => {}
                State::Ready(value) => return Some(value.clone()),
                State::Abandoned => return None,
            }
            core::hint::spin_loop();
        }
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

/// One shard: the tickets of its keys' loads in flight.
type Shard<K, V, I, S> = Mutex<hashbrown::HashMap<K, Arc<Ticket<V, I>>, S>>;

/// The loads in flight, sharded by key hash and bounded per shard.
pub struct InflightLoads<K, V, I, S> {
    shards: Vec<Padded<Shard<K, V, I, S>>>,
    shard_mask: u64,
    hasher: S,
    /// Most loads one shard coordinates at once; past it a request loads
    /// alone, so a burst of distinct cold keys cannot grow the registry
    /// without bound.
    per_shard_limit: usize,
}

/// What a cache miss does about its load.
pub enum Claim<K, V, I, S>
where
    K: Eq + Hash + Clone,
    S: BuildHasher,
{
    /// No load of this object is in flight: this request does it and must
    /// [`Lead::publish`] the result (dropping the lead abandons it).
    Lead(Lead<K, V, I, S>),
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
/// Holds the registry rather than borrowing it, so a load handed to another
/// stage (a batched read completing later) carries its lead along.
pub struct Lead<K, V, I, S>
where
    K: Eq + Hash + Clone,
    S: BuildHasher,
{
    loads: Arc<InflightLoads<K, V, I, S>>,
    key: K,
    ticket: Arc<Ticket<V, I>>,
    published: bool,
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
        match &*self.0.state.lock() {
            State::Ready(value) => Some(value.clone()),
            State::Pending | State::Abandoned => None,
        }
    }
}

impl<K, V, I, S> Lead<K, V, I, S>
where
    K: Eq + Hash + Clone,
    V: Clone,
    S: BuildHasher,
{
    /// Hands `value` to every waiter and closes the load. Publish only a
    /// value already made visible where a later request looks first (the
    /// cache), or a value no later request needs: once closed, a new miss of
    /// the key leads a load of its own.
    pub fn publish(mut self, value: V) {
        self.ticket.settle(State::Ready(value));
        self.published = true;
    }

    /// Whether any request is waiting for this load now. Only the registry
    /// and this lead hold the ticket otherwise. A request that joins after the
    /// answer and before the load closes finds no value and loads on its own,
    /// so `false` lets a leader skip building a value nobody would take.
    pub fn has_waiters(&self) -> bool {
        Arc::strong_count(&self.ticket) > 2
    }
}

impl<K, V, I, S> Drop for Lead<K, V, I, S>
where
    K: Eq + Hash + Clone,
    S: BuildHasher,
{
    fn drop(&mut self) {
        // Not published: the load failed or unwound. Waiters load on their
        // own, so none is left waiting and no error is remembered.
        if !self.published {
            self.ticket.settle(State::Abandoned);
        }
        self.loads.close(&self.key, &self.ticket);
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
    /// 256) coordinating up to `per_shard_limit` loads each.
    pub fn new(shards: usize, per_shard_limit: usize, hasher: S) -> Self {
        let count = shards.next_power_of_two().clamp(1, 256);
        let mut list = Vec::with_capacity(count);
        for _ in 0..count {
            list.push(Padded(Mutex::new(hashbrown::HashMap::with_hasher(
                hasher.clone(),
            ))));
        }
        Self {
            shards: list,
            shard_mask: count as u64 - 1,
            hasher,
            per_shard_limit,
        }
    }

    /// Claims the load of `key` for a request of `identity`. The caller has
    /// just missed the cache; a leader checks it once more before loading,
    /// since a load that closed between that miss and this claim has already
    /// put its result there.
    pub fn claim(self: &Arc<Self>, key: K, identity: I) -> Claim<K, V, I, S> {
        let mut shard = self.shard(&key).lock();
        if let Some(ticket) = shard.get(&key) {
            return if ticket.identity == identity {
                Claim::Join(Join(Arc::clone(ticket)))
            } else {
                Claim::Alone
            };
        }
        if shard.len() >= self.per_shard_limit {
            return Claim::Alone;
        }
        let ticket = Arc::new(Ticket::new(identity));
        shard.insert(key.clone(), Arc::clone(&ticket));
        drop(shard);
        Claim::Lead(Lead {
            loads: Arc::clone(self),
            key,
            ticket,
            published: false,
        })
    }

    /// The value of a load of `key` for `identity` that has already been
    /// published and not yet closed, without claiming anything: for a reader
    /// that may neither wait nor lead.
    pub fn take_ready(&self, key: &K, identity: &I) -> Option<V> {
        let ticket = Arc::clone(self.shard(key).lock().get(key)?);
        if ticket.identity != *identity {
            return None;
        }
        match &*ticket.state.lock() {
            State::Ready(value) => Some(value.clone()),
            State::Pending | State::Abandoned => None,
        }
    }

    /// How many loads are in flight, for tests.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.shards.iter().map(|shard| shard.0.lock().len()).sum()
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
                    .lock()
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
    /// taken its place.
    fn close(&self, key: &K, ticket: &Arc<Ticket<V, I>>) {
        let mut shard = self.shard(key).lock();
        if shard
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(current, ticket))
        {
            shard.remove(key);
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::expect_used, reason = "test code")]
mod tests;

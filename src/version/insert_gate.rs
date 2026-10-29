// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! What an insert holds instead of the version-history lock.

use crate::memtable::Memtable;
use crate::memtable::own_line::OwnLine;
use alloc::sync::Arc;
use core::cell::{Cell, UnsafeCell};
use core::sync::atomic::{AtomicUsize, Ordering};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Shards of the gate: more than the writers that usually insert at once, so
/// two of them seldom share one.
const SHARDS: usize = 64;

/// Lets inserts through without a lock word every writer shares, hands them
/// the active memtable, and lets a version change that replaces it wait for
/// the inserts already into it.
///
/// An insert takes its thread's shard for reading; [`close`](Self::close)
/// takes every shard for writing. A thread keeps one shard for its life, so
/// writers on different shards touch no common cache line.
///
/// The active memtable lives here rather than only in the version mirror: an
/// insert reads it under its shard with one plain load, where the mirror would
/// cost it an `arc-swap` guard, and it can only change while every shard is
/// held for writing, so no insert sees it change under it.
pub(crate) struct InsertGate {
    shards: [OwnLine<RwLock<()>>; SHARDS],
    /// Written only through a [`Closed`] gate, read only under a shard.
    active: UnsafeCell<Arc<Memtable>>,
}

// SAFETY: `active` is the one field without synchronization of its own, and
// the shards are its lock: it is written only through `Closed`, which holds
// every shard for writing, and read only through `Entered`, which holds one
// for reading, so a write excludes every read and every other write, exactly
// as one `RwLock<Arc<Memtable>>` would. `Arc<Memtable>` is `Send + Sync`.
unsafe impl Sync for InsertGate {}

/// An insert admitted through the gate: the active memtable, which cannot be
/// replaced until the guard drops.
pub(crate) struct Entered<'a> {
    _shard: RwLockReadGuard<'a, ()>,
    memtable: &'a Memtable,
}

impl Entered<'_> {
    /// The active memtable this insert lands in.
    pub(crate) fn memtable(&self) -> &Memtable {
        self.memtable
    }
}

/// The gate closed: no insert is admitted, none is still in, and the active
/// memtable may be replaced.
pub(crate) struct Closed<'a> {
    gate: &'a InsertGate,
    _shards: [RwLockWriteGuard<'a, ()>; SHARDS],
}

impl Closed<'_> {
    /// Makes `memtable` the one the next inserts land in.
    pub(crate) fn set_active(&self, memtable: Arc<Memtable>) {
        // SAFETY: `self` holds every shard for writing, so no `Entered` and no
        // other `Closed` exists: this is the only access to `active`.
        unsafe { *self.gate.active.get() = memtable };
    }
}

/// Gives each thread its shard, in the order threads first insert.
static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);

std::thread_local! {
    /// The shard of this thread; `usize::MAX` until its first insert.
    static SHARD: Cell<usize> = const { Cell::new(usize::MAX) };
}

fn this_threads_shard() -> usize {
    SHARD.with(|shard| {
        let mut index = shard.get();
        if index == usize::MAX {
            index = NEXT_SHARD.fetch_add(1, Ordering::Relaxed) % SHARDS;
            shard.set(index);
        }
        index
    })
}

impl InsertGate {
    /// A gate open on `active`, the memtable inserts land in first.
    pub(crate) fn new(active: Arc<Memtable>) -> Self {
        Self {
            shards: core::array::from_fn(|_| OwnLine(RwLock::new(()))),
            active: UnsafeCell::new(active),
        }
    }

    /// Admits one insert into the active memtable until the guard drops.
    #[inline]
    #[expect(
        clippy::indexing_slicing,
        reason = "the shard index is reduced modulo SHARDS"
    )]
    pub(crate) fn enter(&self) -> Entered<'_> {
        let shard = self.shards[this_threads_shard()].read();
        // SAFETY: the shard is held for reading, so no `Closed` exists and
        // `active` is not written until it drops; the read lock acquires what
        // the last `Closed` released, so the write is visible. The borrow
        // lives no longer than the guard it is returned with.
        let memtable = unsafe { &**self.active.get() };
        Entered {
            _shard: shard,
            memtable,
        }
    }

    /// Waits for every insert admitted so far and admits none until the guard
    /// drops.
    pub(crate) fn close(&self) -> Closed<'_> {
        Closed {
            gate: self,
            _shards: core::array::from_fn(|i| {
                #[expect(clippy::indexing_slicing, reason = "i < SHARDS")]
                self.shards[i].write()
            }),
        }
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
mod tests;

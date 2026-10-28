// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! What an insert holds instead of the version-history lock.

use crate::memtable::own_line::OwnLine;
use core::cell::Cell;
use core::sync::atomic::{AtomicUsize, Ordering};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Shards of the gate: more than the writers that usually insert at once, so
/// two of them seldom share one.
const SHARDS: usize = 64;

/// Lets inserts through without a lock word every writer shares, and lets a
/// version change that replaces the active memtable wait for the inserts
/// already into it.
///
/// An insert takes its thread's shard for reading; [`close`](Self::close)
/// takes every shard for writing. A thread keeps one shard for its life, so
/// writers on different shards touch no common cache line.
pub(crate) struct InsertGate {
    shards: [OwnLine<RwLock<()>>; SHARDS],
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
    pub(crate) fn new() -> Self {
        Self {
            shards: core::array::from_fn(|_| OwnLine(RwLock::new(()))),
        }
    }

    /// Admits one insert until the guard drops.
    #[inline]
    #[expect(
        clippy::indexing_slicing,
        reason = "the shard index is reduced modulo SHARDS"
    )]
    pub(crate) fn enter(&self) -> RwLockReadGuard<'_, ()> {
        self.shards[this_threads_shard()].read()
    }

    /// Waits for every insert admitted so far and admits none until the guard
    /// drops.
    pub(crate) fn close(&self) -> [RwLockWriteGuard<'_, ()>; SHARDS] {
        core::array::from_fn(|i| {
            #[expect(clippy::indexing_slicing, reason = "i < SHARDS")]
            self.shards[i].write()
        })
    }
}

#[cfg(test)]
mod tests;

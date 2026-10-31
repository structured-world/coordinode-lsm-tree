// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Process-wide counts of what the hot paths do, for benches to price a change.
//!
//! Unlike `metrics`, which a deployment reads per tree, these sit on every key
//! clone and drop, so a build pays for them only when it asks for this feature.
//! They count the default slice backend: `bytes_1` keys and values pass them by.
//! They carry no stability promise.

use core::sync::atomic::Ordering;
use portable_atomic::AtomicU64;

pub(crate) static FUSED: AtomicU64 = AtomicU64::new(0);
pub(crate) static CHUNK_KEYS: AtomicU64 = AtomicU64::new(0);
pub(crate) static KEY_BYTES: AtomicU64 = AtomicU64::new(0);
pub(crate) static NEW_BYTES: AtomicU64 = AtomicU64::new(0);
pub(crate) static SLICES: AtomicU64 = AtomicU64::new(0);
pub(crate) static REFS_UP: AtomicU64 = AtomicU64::new(0);
pub(crate) static REFS_DOWN: AtomicU64 = AtomicU64::new(0);

/// Adds `by` to `counter`.
#[inline]
pub(crate) fn add(counter: &AtomicU64, by: usize) {
    counter.fetch_add(by as u64, Ordering::Relaxed);
}

/// A reading of every counter.
#[derive(Clone, Copy, Debug, Default)]
pub struct Counts {
    /// Keys and values fused into a buffer of their own.
    pub fused: u64,
    /// Keys written into a shared chunk.
    pub chunk_keys: u64,
    /// Bytes both of the above copied.
    pub key_bytes: u64,
    /// Bytes copied into a new view from a slice.
    pub new_bytes: u64,
    /// Views taken of a view, clones included.
    pub slices: u64,
    /// Atomic reference count increments.
    pub refs_up: u64,
    /// Atomic reference count decrements.
    pub refs_down: u64,
}

impl Counts {
    /// The counters as they stand.
    #[must_use]
    pub fn now() -> Self {
        let read = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        Self {
            fused: read(&FUSED),
            chunk_keys: read(&CHUNK_KEYS),
            key_bytes: read(&KEY_BYTES),
            new_bytes: read(&NEW_BYTES),
            slices: read(&SLICES),
            refs_up: read(&REFS_UP),
            refs_down: read(&REFS_DOWN),
        }
    }

    /// What happened between `before` and this reading.
    #[must_use]
    pub fn since(self, before: Self) -> Self {
        Self {
            fused: self.fused - before.fused,
            chunk_keys: self.chunk_keys - before.chunk_keys,
            key_bytes: self.key_bytes - before.key_bytes,
            new_bytes: self.new_bytes - before.new_bytes,
            slices: self.slices - before.slices,
            refs_up: self.refs_up - before.refs_up,
            refs_down: self.refs_down - before.refs_down,
        }
    }
}

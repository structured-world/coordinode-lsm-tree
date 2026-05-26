// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! `RuntimeConfigHandle` — atomic snapshot wrapper around
//! [`RuntimeConfig`].
//!
//! Std-bound because `arc_swap::ArcSwap` depends on std-side
//! primitives (the `experimental-thread-local` `no_std` mode is
//! out of scope for now).
//!
//! Hot-path semantics:
//! - Read: `load()` returns a `Guard<Arc<RuntimeConfig>>` via a
//!   single atomic load. No locks, no allocation, no syscall.
//!   Cheap enough to call on every block write / manifest commit.
//! - Update: `update(|cfg| ...)` clones the current snapshot,
//!   applies the caller's mutator, and atomically swaps the new
//!   snapshot in. Single-writer; concurrent readers may observe
//!   either the old or the new snapshot, never a torn one.

use super::types::RuntimeConfig;
use arc_swap::ArcSwap;
use std::sync::Arc;

/// Lockless atomic snapshot of [`RuntimeConfig`].
///
/// Constructed once at `Tree::open()`, lives for the lifetime of
/// the Tree. Cloned snapshots from `load()` outlive the handle if
/// caller holds the returned `Arc` past handle drop.
pub struct RuntimeConfigHandle {
    inner: ArcSwap<RuntimeConfig>,
}

impl RuntimeConfigHandle {
    /// Build a handle wrapping the given initial config.
    #[must_use]
    pub fn new(initial: RuntimeConfig) -> Self {
        Self {
            inner: ArcSwap::from_pointee(initial),
        }
    }

    /// Load the current snapshot — lockless single atomic load.
    /// The returned guard is cheap to drop; for longer-lived
    /// references use [`Self::load_full`] which returns an
    /// owned `Arc<RuntimeConfig>`.
    pub fn load(&self) -> arc_swap::Guard<Arc<RuntimeConfig>> {
        self.inner.load()
    }

    /// Load the current snapshot as an owned `Arc`.
    /// Slightly more expensive than [`Self::load`] (one atomic
    /// increment) but the result outlives the handle.
    #[must_use]
    pub fn load_full(&self) -> Arc<RuntimeConfig> {
        self.inner.load_full()
    }

    /// Apply `mutator` to a clone of the current snapshot, then
    /// atomically swap the new snapshot in. Concurrent readers
    /// observe either the old or the new snapshot — never a torn
    /// intermediate state.
    ///
    /// Concurrent writers race **last-writer-wins**: each `update`
    /// loads the current snapshot, mutates a clone, then `store`s
    /// the new pointer. If two writers `load` the same starting
    /// snapshot, the second `store` overwrites the first
    /// outright — the first writer's mutation is lost. There is
    /// no CAS / RCU merge here. Callers that need lost-update
    /// avoidance (e.g. two threads concurrently toggling
    /// different fields) MUST serialize their updates at the
    /// caller layer, typically via a `Mutex` around the
    /// `update` call site.
    pub fn update<F>(&self, mutator: F)
    where
        F: FnOnce(&mut RuntimeConfig),
    {
        let current = self.inner.load_full();
        let mut next = (*current).clone();
        mutator(&mut next);
        self.inner.store(Arc::new(next));
    }
}

impl core::fmt::Debug for RuntimeConfigHandle {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RuntimeConfigHandle")
            .field("inner", &*self.load())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::ChecksumAlgorithm;
    use super::*;
    use std::sync::Barrier;
    use std::thread;

    // Stress-test parameters for the torn-read test.
    const STRESS_READERS: usize = 8;
    const STRESS_READS_PER_THREAD: usize = 5_000;

    #[test]
    fn load_returns_initial_config() {
        let handle = RuntimeConfigHandle::new(RuntimeConfig::default());
        let snap = handle.load();
        assert_eq!(snap.block_checksum_algo, ChecksumAlgorithm::Xxh3_64);
    }

    #[test]
    fn update_applies_mutation_visible_on_next_load() {
        let handle = RuntimeConfigHandle::new(RuntimeConfig::default());
        handle.update(|cfg| {
            cfg.block_checksum_algo = ChecksumAlgorithm::Crc32c;
        });
        let snap = handle.load();
        assert_eq!(snap.block_checksum_algo, ChecksumAlgorithm::Crc32c);
    }

    #[test]
    fn snapshot_held_during_update_is_unchanged() {
        // A snapshot captured before update() must remain at the
        // old value — Arc semantics: snapshot owns its clone, the
        // swap only affects subsequent loads. This is the
        // load-old-then-act-old guarantee that compaction-as-
        // migration relies on (in-flight compaction finishes with
        // its starting snapshot; next compaction picks up new
        // config).
        let handle = RuntimeConfigHandle::new(RuntimeConfig::default());
        let snap_before = handle.load_full();

        handle.update(|cfg| {
            cfg.block_checksum_algo = ChecksumAlgorithm::Crc32c;
        });

        // The old snapshot still observes the original algo.
        assert_eq!(snap_before.block_checksum_algo, ChecksumAlgorithm::Xxh3_64);
        // A fresh load sees the new algo.
        assert_eq!(handle.load().block_checksum_algo, ChecksumAlgorithm::Crc32c,);
    }

    #[test]
    fn concurrent_reads_during_single_update_observe_consistent_snapshot() {
        // Stress test torn-read guarantee: many reader threads
        // continuously load() while one writer thread swaps the
        // snapshot. Every observed snapshot must be one of the
        // two valid states (initial or post-update) — never a
        // half-updated combination.
        //
        // RuntimeConfig only has two fields right now, so torn
        // reads would manifest as a snapshot where block_checksum
        // is post-update but kv_checksum is pre-update. ArcSwap
        // makes this impossible (swap is single-pointer atomic
        // and clone preserves field correlation), but the test
        // locks the invariant so future field additions don't
        // accidentally break it.
        let handle = Arc::new(RuntimeConfigHandle::new(RuntimeConfig {
            block_checksum_algo: ChecksumAlgorithm::Xxh3_64,
            kv_checksum_algo: ChecksumAlgorithm::Xxh3_64,
        }));

        let barrier = Arc::new(Barrier::new(STRESS_READERS + 1));

        let reader_handles: Vec<_> = (0..STRESS_READERS)
            .map(|_| {
                let handle = Arc::clone(&handle);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    for _ in 0..STRESS_READS_PER_THREAD {
                        let snap = handle.load();
                        // Valid states: both Xxh3_64 (initial) or
                        // both Crc32c (post-update). Any other
                        // pairing means a torn read.
                        let (a, b) = (snap.block_checksum_algo, snap.kv_checksum_algo);
                        let initial =
                            a == ChecksumAlgorithm::Xxh3_64 && b == ChecksumAlgorithm::Xxh3_64;
                        let updated =
                            a == ChecksumAlgorithm::Crc32c && b == ChecksumAlgorithm::Crc32c;
                        assert!(initial || updated, "torn read: block={a:?}, kv={b:?}",);
                    }
                })
            })
            .collect();

        // Writer waits at the barrier with the readers, then
        // performs the swap once.
        barrier.wait();
        handle.update(|cfg| {
            cfg.block_checksum_algo = ChecksumAlgorithm::Crc32c;
            cfg.kv_checksum_algo = ChecksumAlgorithm::Crc32c;
        });

        for h in reader_handles {
            // Propagate panics from reader threads as a join failure.
            // Using assert! avoids clippy::expect_used in test code.
            assert!(h.join().is_ok(), "reader thread panicked");
        }
    }

    #[test]
    fn multiple_back_to_back_updates_final_state_observable() {
        // Three back-to-back updates from a single writer — the
        // final load must observe the last-written value, no
        // updates lost in the middle.
        let handle = RuntimeConfigHandle::new(RuntimeConfig::default());

        handle.update(|cfg| cfg.block_checksum_algo = ChecksumAlgorithm::Crc32c);
        handle.update(|cfg| cfg.kv_checksum_algo = ChecksumAlgorithm::Xxh3Low32);
        handle.update(|cfg| cfg.block_checksum_algo = ChecksumAlgorithm::Xxh3Low32);

        let snap = handle.load();
        assert_eq!(snap.block_checksum_algo, ChecksumAlgorithm::Xxh3Low32);
        assert_eq!(snap.kv_checksum_algo, ChecksumAlgorithm::Xxh3Low32);
    }
}

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
//!   snapshot in. Concurrent readers always observe either the old
//!   or the new snapshot, never a torn one. Concurrent *writers*
//!   are allowed but race **last-writer-wins** — two `update` calls
//!   that load the same starting snapshot will see the second
//!   `store` overwrite the first, losing the first writer's
//!   mutation. Callers needing lost-update avoidance must serialize
//!   at the call site (see [`RuntimeConfigHandle::try_update`]).

use super::types::RuntimeConfig;
use alloc::sync::Arc;
#[cfg(feature = "std")]
use arc_swap::ArcSwap;
// no-std: arc-swap 1.x is not no_std; spin::RwLock<Arc<_>> gives the same
// snapshot-swap semantics (serialized writers, consistent readers) without an
// allocator. parking_lot/arc-swap win the std hot path, so keep them there.
#[cfg(not(feature = "std"))]
use spin::RwLock;

/// Lockless atomic snapshot of [`RuntimeConfig`].
///
/// Constructed once at `Tree::open()`, lives for the lifetime of
/// the Tree. For snapshots that must outlive the handle borrow,
/// use [`Self::load_full`] — it returns an owned
/// `Arc<RuntimeConfig>` that the caller can hold across the handle
/// being dropped. [`Self::load`] returns a borrow-bound
/// `arc_swap::Guard` and cannot outlive the handle; callers who
/// need an owned reference from a `Guard` can clone the inner
/// `Arc` out of it via `Arc::clone(&*guard)`.
pub struct RuntimeConfigHandle {
    /// `std`: lock-free `ArcSwap` (single atomic load on the read hot path).
    #[cfg(feature = "std")]
    inner: ArcSwap<RuntimeConfig>,
    /// `no_std`: `spin::RwLock<Arc<_>>` — reads take a brief read lock, writes
    /// serialize. Slower than `ArcSwap` but allocator-only.
    #[cfg(not(feature = "std"))]
    inner: RwLock<Arc<RuntimeConfig>>,
}

impl RuntimeConfigHandle {
    /// Build a handle wrapping the given initial config.
    #[must_use]
    pub fn new(initial: RuntimeConfig) -> Self {
        Self {
            #[cfg(feature = "std")]
            inner: ArcSwap::from_pointee(initial),
            #[cfg(not(feature = "std"))]
            inner: RwLock::new(Arc::new(initial)),
        }
    }

    /// Load the current snapshot — lockless single atomic load under `std`
    /// (`no_std` takes a brief read lock). The returned guard is cheap to
    /// drop; for longer-lived references use [`Self::load_full`] which returns
    /// an owned `Arc<RuntimeConfig>`.
    #[cfg(feature = "std")]
    pub fn load(&self) -> arc_swap::Guard<Arc<RuntimeConfig>> {
        self.inner.load()
    }

    /// See the `std` variant. Under `no_std` the guard borrows the handle via
    /// the read lock; deref reaches `RuntimeConfig` through the inner `Arc`.
    #[cfg(not(feature = "std"))]
    pub fn load(&self) -> spin::RwLockReadGuard<'_, Arc<RuntimeConfig>> {
        self.inner.read()
    }

    /// Load the current snapshot as an owned `Arc`.
    /// Slightly more expensive than [`Self::load`] (one atomic
    /// increment) but the result outlives the handle.
    #[must_use]
    pub fn load_full(&self) -> Arc<RuntimeConfig> {
        #[cfg(feature = "std")]
        {
            self.inner.load_full()
        }
        #[cfg(not(feature = "std"))]
        {
            self.inner.read().clone()
        }
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
    /// `try_update` call site.
    ///
    /// Validates the mutated snapshot against compile-time
    /// feature gates before swapping it in. Returns the
    /// validation error without applying the update, so the live
    /// snapshot stays at the pre-mutation value.
    ///
    /// Checks operate on the EFFECTIVE ECC state (the global `page_ecc`
    /// flag OR any per-scope override), not the raw flag:
    /// (1) the `page_ecc` cargo feature — enabling ECC on a binary that
    /// doesn't link the Reed-Solomon codec would silently no-op at the
    /// writer (the `*Ecc` `BlockTransform` arms are feature-gated out), so
    /// the update is rejected; (2) when ECC is on, the scheme + granularity
    /// must be ones the writer can emit — `Secded` (the on-default) is wired at
    /// `Block` granularity, `Page` granularity is rejected (only `Block` is
    /// wired), a zero shard count is rejected (no implicit RS(4,2) fallback),
    /// and `ReedSolomon` needs >= 2 parity shards (single parity is expressed
    /// as `Xor`); (3) `kv_checksum_compute_point = AtInsert` with an algorithm
    /// that is not a compiled-in 4-byte one. `AtInsert` stores the digest in
    /// the skiplist node's 4-byte reserved slot, so it requires a 4-byte
    /// algorithm (`Xxh3Low32` / `Crc32c`): the 8-byte `Xxh3_64` would grow
    /// every node, and a 4-byte algorithm not compiled into the build (e.g.
    /// `Crc32c` without the `crc32c` feature) cannot produce a digest. Both
    /// are rejected.
    ///
    /// # Errors
    ///
    /// - [`crate::Error::PageEccUnsupported`] when the mutator enables ECC
    ///   (via the flag or an override) on a build without the cargo feature.
    /// - [`crate::Error::FeatureUnsupported`] when the mutator enables ECC
    ///   with an unwired / invalid scheme or granularity (`Page`, a zero shard
    ///   count, single-parity `ReedSolomon`), or sets
    ///   `kv_checksum_compute_point = AtInsert` with an 8-byte or
    ///   not-compiled-in algorithm.
    pub fn try_update<F>(&self, mutator: F) -> crate::Result<()>
    where
        F: FnOnce(&mut RuntimeConfig),
    {
        let current = self.load_full();
        let mut next = (*current).clone();
        mutator(&mut next);
        // Validate the EFFECTIVE ECC state, not just the global `page_ecc`
        // flag: a per-scope override (`data_block_ecc_override` /
        // `kv_checksums_ecc_override`) can enable ECC even with
        // `page_ecc == false`, and the manifest tracks the global flag.
        let ecc_enabled = next.data_block_ecc() || next.kv_checksums_ecc() || next.manifest_ecc();
        if ecc_enabled && !cfg!(feature = "page_ecc") {
            return Err(crate::Error::PageEccUnsupported);
        }
        // When ECC is on, the scheme + granularity must be one the writer
        // can actually emit. `Secded` (the per-word Hamming tier, the
        // on-default) is wired at Block granularity. Page granularity is still
        // unimplemented for every scheme. There is no implicit RS(4,2)
        // fallback: a shard scheme needs non-zero counts (and `ReedSolomon`
        // needs >= 2 parity shards — single parity is `Xor`).
        if ecc_enabled {
            use crate::runtime_config::{EccGranularity, EccScheme};
            if next.ecc_granularity != EccGranularity::Block {
                return Err(crate::Error::FeatureUnsupported(
                    "ecc_granularity=Page (not yet wired; use Block)",
                ));
            }
            match next.ecc_scheme {
                EccScheme::Xor { data_shards: 0 } => {
                    return Err(crate::Error::FeatureUnsupported(
                        "ecc_scheme=Xor data_shards=0",
                    ));
                }
                EccScheme::ReedSolomon { data_shards: 0, .. } => {
                    return Err(crate::Error::FeatureUnsupported(
                        "ecc_scheme=ReedSolomon data_shards=0",
                    ));
                }
                EccScheme::ReedSolomon { parity_shards, .. } if parity_shards < 2 => {
                    return Err(crate::Error::FeatureUnsupported(
                        "ecc_scheme=ReedSolomon needs >= 2 parity shards (use Xor for single parity)",
                    ));
                }
                _ => {}
            }
        }
        // AtInsert (compute the per-KV digest at memtable insert, then verify
        // it against a recompute at flush to catch a RAM bit-flip during
        // memtable residence) stores the digest in the skiplist node's 4-byte
        // reserved slot. That slot only fits a 4-byte digest, so AtInsert
        // requires a 4-byte algorithm (`Xxh3Low32` / `Crc32c`); the 8-byte
        // `Xxh3_64` would grow every memtable node and is rejected. This keeps
        // AtInsert zero-size-overhead in the memtable.
        if matches!(
            next.kv_checksum_compute_point,
            crate::runtime_config::KvChecksumComputePoint::AtInsert
        ) {
            // Short, machine-matchable markers per the FeatureUnsupported
            // contract; the 4-byte-slot rationale lives in the comment above.
            if next.kv_checksum_algo.digest_size() != 4 {
                return Err(crate::Error::FeatureUnsupported(
                    "kv_checksum_compute_point=AtInsert requires a 4-byte algorithm",
                ));
            }
            // The algorithm must also be compiled in: a 4-byte algorithm whose
            // digest cannot be computed (e.g. Crc32c without the `crc32c`
            // feature) would silently skip residence digests at insert and fail
            // at flush. Reject it here instead.
            if !next.kv_checksum_algo.is_available() {
                return Err(crate::Error::FeatureUnsupported(
                    "kv_checksum_compute_point=AtInsert requires a compiled-in algorithm",
                ));
            }
        }
        #[cfg(feature = "std")]
        {
            self.inner.store(Arc::new(next));
        }
        #[cfg(not(feature = "std"))]
        {
            *self.inner.write() = Arc::new(next);
        }
        Ok(())
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
#[expect(
    clippy::unwrap_used,
    reason = "tests panic on the unhappy paths to surface failures loudly"
)]
mod tests;

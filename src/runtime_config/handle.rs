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
//!   at the call site (see [`RuntimeConfigHandle::update`]).

use super::types::RuntimeConfig;
use arc_swap::ArcSwap;
use std::sync::Arc;

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
    /// must be ones the writer can emit — `Secded` (the on-default) is
    /// rejected until #255 wires it, `Page` granularity is rejected (only
    /// `Block` is wired), a zero shard count is rejected (no implicit
    /// RS(4,2) fallback), and `ReedSolomon` needs >= 2 parity shards
    /// (single parity is expressed as `Xor`); (3) `kv_checksum_compute_point
    /// = AtInsert`, which is not yet wired into the memtable / writer path —
    /// accepting it would silently behave as `AtBlockCompile`.
    ///
    /// # Errors
    ///
    /// - [`crate::Error::PageEccUnsupported`] when the mutator enables ECC
    ///   (via the flag or an override) on a build without the cargo feature.
    /// - [`crate::Error::FeatureUnsupported`] when the mutator enables ECC
    ///   with an unwired / invalid scheme or granularity (`Secded`, `Page`,
    ///   a zero shard count, single-parity `ReedSolomon`), or sets
    ///   `kv_checksum_compute_point = AtInsert`.
    pub fn try_update<F>(&self, mutator: F) -> crate::Result<()>
    where
        F: FnOnce(&mut RuntimeConfig),
    {
        let current = self.inner.load_full();
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
        // can actually emit. `Secded` (the on-default) is the per-word
        // Hamming tier, not yet wired (#255) — accepting it would silently
        // write no parity. Page granularity is likewise unimplemented. There
        // is no implicit RS(4,2) fallback: enabling ECC today requires an
        // explicit shard scheme with non-zero counts (and `ReedSolomon`
        // needs >= 2 parity shards — single parity is `Xor`).
        if ecc_enabled {
            use crate::runtime_config::{EccGranularity, EccScheme};
            if next.ecc_granularity != EccGranularity::Block {
                return Err(crate::Error::FeatureUnsupported(
                    "ecc_granularity=Page (not yet wired; use Block)",
                ));
            }
            match next.ecc_scheme {
                EccScheme::Secded => {
                    return Err(crate::Error::FeatureUnsupported(
                        "ecc_scheme=Secded (not yet wired; use Xor/ReedSolomon)",
                    ));
                }
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
        // AtInsert (compute the per-KV digest at memtable insert and carry
        // it through flush) is not yet wired into the memtable / writer
        // path: accepting it would silently behave as AtBlockCompile,
        // downgrading the caller's intent without notice. Reject it with a
        // typed error until the carry path lands. (When wired, AtInsert
        // will additionally require a 4-byte algorithm so the digest fits
        // the memtable node's reserved slot.)
        if matches!(
            next.kv_checksum_compute_point,
            crate::runtime_config::KvChecksumComputePoint::AtInsert
        ) {
            // Short, machine-matchable marker per the FeatureUnsupported
            // contract; the "not yet wired, use AtBlockCompile" rationale lives
            // in the comment above rather than in the error payload.
            return Err(crate::Error::FeatureUnsupported(
                "kv_checksum_compute_point=AtInsert",
            ));
        }
        self.inner.store(Arc::new(next));
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
    #[cfg(feature = "page_ecc")]
    fn try_update_rejects_ecc_on_with_secded_until_wired() {
        use super::super::types::EccScheme;

        // Enabling ECC with the on-default Secded scheme must be rejected
        // (SECDED not yet wired, #255) — there is NO implicit RS(4,2)
        // fallback. The live snapshot stays off.
        let handle = RuntimeConfigHandle::new(RuntimeConfig::default());
        let secded = handle.try_update(|c| c.page_ecc = true);
        assert!(
            matches!(secded, Err(crate::Error::FeatureUnsupported(_))),
            "ECC on + Secded must be rejected until #255, got {secded:?}"
        );
        assert!(
            !handle.load().page_ecc,
            "rejected update must not enable ECC"
        );

        // An explicit shard scheme is accepted.
        let xor = handle.try_update(|c| {
            c.page_ecc = true;
            c.ecc_scheme = EccScheme::Xor { data_shards: 10 };
        });
        assert!(
            xor.is_ok(),
            "ECC on + explicit Xor must be accepted, got {xor:?}"
        );
        assert!(handle.load().page_ecc);

        // Zero shard count is rejected (non-recoverable layout) for every
        // shard-based scheme: Xor with no data shards, and Reed-Solomon with
        // either a zero data-shard or a zero parity-shard count.
        for bad_scheme in [
            EccScheme::Xor { data_shards: 0 },
            EccScheme::ReedSolomon {
                data_shards: 0,
                parity_shards: 2,
            },
            EccScheme::ReedSolomon {
                data_shards: 8,
                parity_shards: 0,
            },
        ] {
            let h = RuntimeConfigHandle::new(RuntimeConfig::default());
            let bad = h.try_update(|c| {
                c.page_ecc = true;
                c.ecc_scheme = bad_scheme;
            });
            assert!(
                matches!(bad, Err(crate::Error::FeatureUnsupported(_))),
                "zero-shard scheme {bad_scheme:?} must be rejected, got {bad:?}"
            );
            assert!(!h.load().page_ecc, "rejected update must not enable ECC");
        }
    }

    #[test]
    #[cfg(feature = "page_ecc")]
    fn try_update_rejects_unwritable_ecc_layouts_when_enabled() {
        use super::super::types::{EccGranularity, EccScheme};

        // Page granularity is not yet wired; single-parity ReedSolomon is a
        // non-canonical layout (single parity is `Xor`). Both must be rejected
        // when ECC is enabled, leaving the live snapshot off.
        let page = RuntimeConfigHandle::new(RuntimeConfig::default());
        let r = page.try_update(|c| {
            c.page_ecc = true;
            c.ecc_scheme = EccScheme::ReedSolomon {
                data_shards: 8,
                parity_shards: 2,
            };
            c.ecc_granularity = EccGranularity::Page;
        });
        assert!(
            matches!(r, Err(crate::Error::FeatureUnsupported(_))),
            "{r:?}"
        );
        assert!(!page.load().page_ecc);

        let rs1 = RuntimeConfigHandle::new(RuntimeConfig::default());
        let r = rs1.try_update(|c| {
            c.page_ecc = true;
            c.ecc_scheme = EccScheme::ReedSolomon {
                data_shards: 8,
                parity_shards: 1,
            };
        });
        assert!(
            matches!(r, Err(crate::Error::FeatureUnsupported(_))),
            "{r:?}"
        );
        assert!(!rs1.load().page_ecc);

        // An override enables ECC even with page_ecc=false: a valid scheme is
        // accepted, an invalid one (Secded) is still rejected.
        let ovr = RuntimeConfigHandle::new(RuntimeConfig::default());
        let r = ovr.try_update(|c| {
            c.data_block_ecc_override = Some(true);
            c.ecc_scheme = EccScheme::Xor { data_shards: 8 };
        });
        assert!(
            r.is_ok(),
            "override-enabled valid scheme must be accepted: {r:?}"
        );

        let ovr_bad = RuntimeConfigHandle::new(RuntimeConfig::default());
        let r = ovr_bad.try_update(|c| {
            c.data_block_ecc_override = Some(true);
            // ecc_scheme stays the Secded default → rejected even though
            // page_ecc is false, because the override enables ECC.
        });
        assert!(
            matches!(r, Err(crate::Error::FeatureUnsupported(_))),
            "{r:?}"
        );
    }

    #[test]
    fn try_update_rejects_at_insert_as_not_implemented() {
        use super::super::types::KvChecksumComputePoint;

        // AtInsert is not yet wired into the memtable / writer path.
        // Accepting it would silently behave as AtBlockCompile, so it must
        // be rejected with a typed error and leave the live snapshot
        // unchanged — regardless of the algorithm width.
        let handle = RuntimeConfigHandle::new(RuntimeConfig::default());
        let result = handle.try_update(|c| {
            c.kv_checksum_compute_point = KvChecksumComputePoint::AtInsert;
        });
        assert!(
            matches!(result, Err(crate::Error::FeatureUnsupported(_))),
            "AtInsert must be rejected as not-yet-implemented, got {result:?}"
        );
        assert_eq!(
            handle.load().kv_checksum_compute_point,
            KvChecksumComputePoint::AtBlockCompile,
            "rejected update must not mutate the live snapshot"
        );
    }

    #[test]
    fn try_update_rejects_at_insert_even_with_4_byte_algorithm() {
        use super::super::types::KvChecksumComputePoint;

        // Even with a 4-byte algorithm (the eventual requirement), AtInsert
        // is rejected until the carry path is implemented — the not-yet-
        // wired gate fires before the algorithm-width check.
        let handle = RuntimeConfigHandle::new(RuntimeConfig::default());
        let result = handle.try_update(|c| {
            c.kv_checksum_algo = ChecksumAlgorithm::Xxh3Low32;
            c.kv_checksum_compute_point = KvChecksumComputePoint::AtInsert;
        });
        assert!(
            matches!(result, Err(crate::Error::FeatureUnsupported(_))),
            "AtInsert must be rejected even with a 4-byte algorithm, got {result:?}"
        );
    }

    #[test]
    fn update_applies_mutation_visible_on_next_load() {
        let handle = RuntimeConfigHandle::new(RuntimeConfig::default());
        handle
            .try_update(|cfg| {
                cfg.block_checksum_algo = ChecksumAlgorithm::Crc32c;
            })
            .unwrap();
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

        handle
            .try_update(|cfg| {
                cfg.block_checksum_algo = ChecksumAlgorithm::Crc32c;
            })
            .unwrap();

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
            ..RuntimeConfig::default()
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
        handle
            .try_update(|cfg| {
                cfg.block_checksum_algo = ChecksumAlgorithm::Crc32c;
                cfg.kv_checksum_algo = ChecksumAlgorithm::Crc32c;
            })
            .unwrap();

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

        handle
            .try_update(|cfg| cfg.block_checksum_algo = ChecksumAlgorithm::Crc32c)
            .unwrap();
        handle
            .try_update(|cfg| cfg.kv_checksum_algo = ChecksumAlgorithm::Xxh3Low32)
            .unwrap();
        handle
            .try_update(|cfg| cfg.block_checksum_algo = ChecksumAlgorithm::Xxh3Low32)
            .unwrap();

        let snap = handle.load();
        assert_eq!(snap.block_checksum_algo, ChecksumAlgorithm::Xxh3Low32);
        assert_eq!(snap.kv_checksum_algo, ChecksumAlgorithm::Xxh3Low32);
    }
}

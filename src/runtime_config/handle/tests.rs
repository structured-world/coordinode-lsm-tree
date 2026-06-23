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
fn try_update_accepts_ecc_on_with_secded_and_shard_schemes() {
    use super::super::types::EccScheme;

    // Enabling ECC with the on-default Secded scheme (Block granularity) is
    // accepted: the per-word SEC-DED write/read path is wired.
    let handle = RuntimeConfigHandle::new(RuntimeConfig::default());
    let secded = handle.try_update(|c| c.page_ecc = true);
    assert!(
        secded.is_ok(),
        "ECC on + Secded (Block) must be accepted, got {secded:?}"
    );
    assert!(handle.load().page_ecc, "accepted update must enable ECC");
    assert_eq!(handle.load().ecc_scheme, EccScheme::Secded);

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

    // An override enables ECC even with page_ecc=false: an explicit shard
    // scheme is accepted.
    let ovr = RuntimeConfigHandle::new(RuntimeConfig::default());
    let r = ovr.try_update(|c| {
        c.data_block_ecc_override = Some(true);
        c.ecc_scheme = EccScheme::Xor { data_shards: 8 };
    });
    assert!(
        r.is_ok(),
        "override-enabled valid scheme must be accepted: {r:?}"
    );

    // The same override with the Secded default (Block granularity) is also
    // accepted now that the per-word SEC-DED path is wired.
    let ovr_secded = RuntimeConfigHandle::new(RuntimeConfig::default());
    let r = ovr_secded.try_update(|c| {
        c.data_block_ecc_override = Some(true);
        // ecc_scheme stays the Secded default.
    });
    assert!(
        r.is_ok(),
        "override-enabled Secded (Block) must be accepted: {r:?}"
    );
}

#[test]
fn try_update_rejects_at_insert_with_8_byte_algorithm() {
    use super::super::types::KvChecksumComputePoint;

    // AtInsert stores the digest in the node's 4-byte reserved slot, so
    // the 8-byte Xxh3_64 (the default algo) does not fit and must be
    // rejected with a typed error, leaving the live snapshot unchanged.
    let handle = RuntimeConfigHandle::new(RuntimeConfig::default());
    assert_eq!(
        handle.load().kv_checksum_algo,
        ChecksumAlgorithm::Xxh3_64,
        "precondition: default algo is the 8-byte Xxh3_64"
    );
    let result = handle.try_update(|c| {
        c.kv_checksum_compute_point = KvChecksumComputePoint::AtInsert;
    });
    assert!(
        matches!(result, Err(crate::Error::FeatureUnsupported(_))),
        "AtInsert + 8-byte algo must be rejected, got {result:?}"
    );
    assert_eq!(
        handle.load().kv_checksum_compute_point,
        KvChecksumComputePoint::AtBlockCompile,
        "rejected update must not mutate the live snapshot"
    );
}

#[test]
fn try_update_accepts_at_insert_with_4_byte_algorithm() {
    use super::super::types::KvChecksumComputePoint;

    // Asserts AtInsert + a compiled 4-byte algorithm is accepted and
    // visible on the next load. Xxh3Low32 is always compiled; Crc32c only
    // when the `crc32c` feature is on (its rejection without the feature is
    // covered by `try_update_rejects_at_insert_with_uncompiled_algorithm`).
    let assert_accepted = |algo: ChecksumAlgorithm| {
        let handle = RuntimeConfigHandle::new(RuntimeConfig::default());
        let result = handle.try_update(|c| {
            c.kv_checksum_algo = algo;
            c.kv_checksum_compute_point = KvChecksumComputePoint::AtInsert;
        });
        assert!(
            result.is_ok(),
            "AtInsert + compiled 4-byte {algo:?} must be accepted, got {result:?}"
        );
        assert_eq!(
            handle.load().kv_checksum_compute_point,
            KvChecksumComputePoint::AtInsert,
            "accepted AtInsert must be visible on next load for {algo:?}"
        );
    };

    assert_accepted(ChecksumAlgorithm::Xxh3Low32);
    #[cfg(feature = "crc32c")]
    assert_accepted(ChecksumAlgorithm::Crc32c);
}

#[cfg(not(feature = "crc32c"))]
#[test]
fn try_update_rejects_at_insert_with_uncompiled_algorithm() {
    use super::super::types::KvChecksumComputePoint;

    // Crc32c is a 4-byte algorithm, but without the crc32c feature its
    // digest cannot be computed (compute returns None). Accepting AtInsert
    // with it would silently skip residence digests at insert and/or fail
    // later at flush, so the config update must reject it up front.
    let handle = RuntimeConfigHandle::new(RuntimeConfig::default());
    let result = handle.try_update(|c| {
        c.kv_checksum_algo = ChecksumAlgorithm::Crc32c;
        c.kv_checksum_compute_point = KvChecksumComputePoint::AtInsert;
    });
    assert!(
        matches!(result, Err(crate::Error::FeatureUnsupported(_))),
        "AtInsert + uncompiled Crc32c must be rejected, got {result:?}"
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
                    let updated = a == ChecksumAlgorithm::Crc32c && b == ChecksumAlgorithm::Crc32c;
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

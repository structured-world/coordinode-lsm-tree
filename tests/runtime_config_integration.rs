// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! End-to-end Tree-level integration of the runtime-config foundation
//! (#352): `Tree::update_runtime_config` swaps the live snapshot
//! atomically, `Tree::runtime_config` returns the current snapshot,
//! and a snapshot captured before an update reflects the pre-update
//! state (load-old-then-act-old guarantee that compaction-as-migration
//! relies on).

// `AbstractTree` is used for its trait methods (`insert`,
// `flush_active_memtable`, `get_internal_entry`) called on the tree below;
// it must stay in scope even though no path names the trait directly.
use lsm_tree::{
    AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter, get_tmp_folder,
    runtime_config::{ChecksumAlgorithm, KvChecksumPolicy, RuntimeConfig},
};
use std::sync::Arc;
use test_log::test;

fn open_tree(path: &std::path::Path) -> lsm_tree::Tree {
    let any = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("Tree open should succeed");

    match any {
        AnyTree::Standard(t) => t,
        AnyTree::Blob(_) => panic!("expected Standard tree, got Blob"),
    }
}

#[test]
fn tree_runtime_config_starts_at_default() {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    let cfg = tree.runtime_config();
    assert_eq!(cfg.block_checksum_algo, ChecksumAlgorithm::Xxh3_64);
    assert_eq!(cfg.kv_checksum_algo, ChecksumAlgorithm::Xxh3_64);
}

#[test]
fn tree_update_runtime_config_visible_on_next_load() {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    tree.update_runtime_config(|cfg| {
        cfg.block_checksum_algo = ChecksumAlgorithm::Crc32c;
    })
    .unwrap();

    let after = tree.runtime_config();
    assert_eq!(after.block_checksum_algo, ChecksumAlgorithm::Crc32c);
    // kv_checksum_algo stays at default — partial updates don't bleed
    // across fields.
    assert_eq!(after.kv_checksum_algo, ChecksumAlgorithm::Xxh3_64);
}

#[test]
fn tree_snapshot_outlives_update_with_pre_update_state() {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // Capture the snapshot BEFORE the update — simulates an in-flight
    // compaction that loaded its config at start.
    let snap_before: Arc<RuntimeConfig> = tree.runtime_config();

    tree.update_runtime_config(|cfg| {
        cfg.block_checksum_algo = ChecksumAlgorithm::Crc32c;
        cfg.kv_checksum_algo = ChecksumAlgorithm::Xxh3Low32;
    })
    .unwrap();

    // Pre-update snapshot still observes original values — the swap
    // doesn't reach into Arcs already in caller hands.
    assert_eq!(snap_before.block_checksum_algo, ChecksumAlgorithm::Xxh3_64);
    assert_eq!(snap_before.kv_checksum_algo, ChecksumAlgorithm::Xxh3_64);

    // Fresh load sees the updated state.
    let snap_after = tree.runtime_config();
    assert_eq!(snap_after.block_checksum_algo, ChecksumAlgorithm::Crc32c);
    assert_eq!(snap_after.kv_checksum_algo, ChecksumAlgorithm::Xxh3Low32);
}

#[test]
fn tree_runtime_config_resets_to_default_on_reopen() {
    // The runtime config is process-state, not persisted. After Tree
    // close + reopen, the snapshot resets to defaults — caller is
    // responsible for re-applying their desired runtime overrides on
    // open. Pin this contract so a future "persist runtime config to
    // manifest" change doesn't break the documented expectation
    // silently.
    let folder = get_tmp_folder();

    {
        let tree = open_tree(folder.path());
        tree.update_runtime_config(|cfg| {
            cfg.block_checksum_algo = ChecksumAlgorithm::Crc32c;
        })
        .unwrap();
        assert_eq!(
            tree.runtime_config().block_checksum_algo,
            ChecksumAlgorithm::Crc32c,
        );
    }

    let tree = open_tree(folder.path());
    assert_eq!(
        tree.runtime_config().block_checksum_algo,
        ChecksumAlgorithm::Xxh3_64,
        "runtime config must reset to default on reopen — \
         not persisted to manifest by design"
    );
}

#[test]
fn tree_kv_checksums_all_levels_round_trips_through_disk() {
    // End-to-end: with `kv_checksums = AllLevels`, the flush path emits
    // data blocks with the KV_CHECKSUM_FOOTER flag + per-KV footer, and the
    // read path transparently strips them. The values written must read
    // back identically after flushing to disk — proving the writer-emit and
    // reader-accept wiring is correct through the live runtime config.
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    tree.update_runtime_config(|cfg| {
        cfg.kv_checksums = KvChecksumPolicy::AllLevels;
        cfg.kv_checksum_algo = ChecksumAlgorithm::Xxh3_64;
    })
    .expect("enabling per-KV checksums must succeed");

    let pairs = [
        (b"alpha".as_slice(), b"one".as_slice()),
        (b"bravo".as_slice(), b"two".as_slice()),
        (b"delta".as_slice(), b"three".as_slice()),
    ];
    for (i, (k, v)) in pairs.iter().enumerate() {
        tree.insert(*k, *v, i as u64);
    }

    // Flush to disk: the on-disk data block carries the KV_CHECKSUM_FOOTER flag.
    tree.flush_active_memtable(0)
        .expect("flush with per-KV checksums must succeed");

    // Read back from disk through the standard read path, which routes
    // through DataBlock::from_loaded and strips the footer.
    for (i, (k, v)) in pairs.iter().enumerate() {
        let item = tree
            .get_internal_entry(k, SeqNo::MAX)
            .expect("read must succeed")
            .expect("key must be present after flush");
        assert_eq!(&*item.key.user_key, *k);
        assert_eq!(item.key.seqno, i as u64);
        assert_eq!(
            &*item.value, *v,
            "value must round-trip through the per-KV checksum footer"
        );
    }

    // Drop the first tree to release the cross-process directory lock before
    // reopening the same directory (a second concurrent open would be rejected
    // with `Error::Locked`).
    drop(tree);

    // Reopen (fresh block cache) and re-read to exercise the cold disk
    // read path, not just the post-flush cache.
    let reopened = open_tree(folder.path());
    for (k, v) in &pairs {
        let item = reopened
            .get_internal_entry(k, SeqNo::MAX)
            .expect("read after reopen must succeed")
            .expect("key must persist across reopen");
        assert_eq!(&*item.value, *v);
    }
}

#[test]
fn seqno_in_index_round_trips_through_disk_and_reopen() -> lsm_tree::Result<()> {
    // With `seqno_in_index` enabled, the writer emits the parallel
    // `seqno_bounds` SST section (one `(seqno_min, seqno_max)` per data block);
    // the index entries themselves stay legacy. The whole point is that the
    // data must read back identically: flush, point-read, reopen, point-read
    // again. Reopen is the load-bearing step — it re-parses the on-disk index
    // and the seqno-bounds section from scratch, so a broken decode surfaces as
    // a read failure.
    let folder = get_tmp_folder();

    {
        let tree = open_tree(folder.path());
        tree.update_runtime_config(|cfg| {
            cfg.seqno_in_index = true;
        })?;

        // Enough keys (with rising seqnos) to spill multiple data blocks, so
        // the index holds several entries with differing seqno bounds.
        for i in 0..500u64 {
            let key = format!("key{i:05}");
            let value = format!("value{i:05}");
            tree.insert(key.as_bytes(), value.as_bytes(), i);
        }
        tree.flush_active_memtable(0)?;

        // Make the multi-block precondition explicit: the per-block
        // seqno-bounds path is only exercised when the SST holds more than one
        // data block. If a future default (block size / dataset) drops this to
        // a single block, fail loudly here instead of silently no longer
        // covering the bug class this test locks down.
        let data_blocks: u64 = tree
            .current_version()
            .iter_tables()
            .map(|t| t.metadata.data_block_count)
            .sum();
        assert!(
            data_blocks > 1,
            "test must produce >1 data block to exercise per-block seqno \
             bounds, got {data_blocks}",
        );

        for i in 0..500u64 {
            let key = format!("key{i:05}");
            let got = tree.get(key.as_bytes(), SeqNo::MAX)?;
            assert_eq!(
                got.as_deref(),
                Some(format!("value{i:05}").as_bytes()),
                "post-flush read of {key} must return its value"
            );
        }
    }

    // Reopen: the index blocks and the seqno-bounds section are decoded fresh
    // from disk. Runtime config resets to default on reopen (not persisted),
    // but the on-disk data must still read back correctly.
    let tree = open_tree(folder.path());
    for i in 0..500u64 {
        let key = format!("key{i:05}");
        let got = tree.get(key.as_bytes(), SeqNo::MAX)?;
        assert_eq!(
            got.as_deref(),
            Some(format!("value{i:05}").as_bytes()),
            "post-reopen read of {key} must return its value"
        );
    }

    // A key never inserted must still be absent (index seek lands correctly).
    assert!(tree.get(b"key99999", SeqNo::MAX)?.is_none());

    Ok(())
}

#[test]
fn at_insert_kv_checksum_round_trips_through_flush_and_reopen() -> lsm_tree::Result<()> {
    // KvChecksumComputePoint::AtInsert with a 4-byte algorithm: each entry's
    // digest is computed at insert and verified at flush against a recompute.
    // With no corruption the residence check passes and the data reads back
    // intact through flush and reopen.
    let folder = get_tmp_folder();
    {
        let tree = open_tree(folder.path());
        tree.update_runtime_config(|cfg| {
            cfg.kv_checksums = KvChecksumPolicy::AllLevels;
            cfg.kv_checksum_algo = ChecksumAlgorithm::Xxh3Low32;
            cfg.kv_checksum_compute_point =
                lsm_tree::runtime_config::KvChecksumComputePoint::AtInsert;
        })?;

        for i in 0..500u64 {
            let key = format!("key{i:05}");
            let value = format!("value{i:05}");
            tree.insert(key.as_bytes(), value.as_bytes(), i);
        }
        // Flush runs the AtInsert residence verify over the sealed memtable;
        // it must succeed (no corruption) and produce the SST.
        tree.flush_active_memtable(0)?;

        for i in 0..500u64 {
            let key = format!("key{i:05}");
            let got = tree.get(key.as_bytes(), SeqNo::MAX)?;
            assert_eq!(
                got.as_deref(),
                Some(format!("value{i:05}").as_bytes()),
                "post-flush read of {key} under AtInsert must return its value"
            );
        }
    }

    // Reopen: data must read back identically (AtInsert is a residence check,
    // not an on-disk format change beyond the per-KV footer).
    let tree = open_tree(folder.path());
    for i in 0..500u64 {
        let key = format!("key{i:05}");
        let got = tree.get(key.as_bytes(), SeqNo::MAX)?;
        assert_eq!(
            got.as_deref(),
            Some(format!("value{i:05}").as_bytes()),
            "post-reopen read of {key} must return its value"
        );
    }
    Ok(())
}

#[test]
fn at_insert_handles_off_to_at_insert_toggle_mid_memtable() -> lsm_tree::Result<()> {
    // Live toggle Off -> AtInsert within one memtable's lifetime: entries
    // inserted before the toggle carry no digest, entries after do. The
    // flush-time residence check must verify only the digest-bearing entries
    // and flush the mixed-variant memtable successfully, with all data intact.
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // Pre-toggle: default (no AtInsert digest).
    for i in 0..200u64 {
        let key = format!("key{i:05}");
        let value = format!("value{i:05}");
        tree.insert(key.as_bytes(), value.as_bytes(), i);
    }

    // Toggle AtInsert on with a 4-byte algorithm.
    tree.update_runtime_config(|cfg| {
        cfg.kv_checksums = KvChecksumPolicy::AllLevels;
        cfg.kv_checksum_algo = ChecksumAlgorithm::Xxh3Low32;
        cfg.kv_checksum_compute_point = lsm_tree::runtime_config::KvChecksumComputePoint::AtInsert;
    })?;

    // Post-toggle: same active memtable now also holds digest-bearing nodes.
    for i in 200..400u64 {
        let key = format!("key{i:05}");
        let value = format!("value{i:05}");
        tree.insert(key.as_bytes(), value.as_bytes(), i);
    }

    // Flush the mixed-variant memtable: verify checks only post-toggle nodes.
    tree.flush_active_memtable(0)?;

    for i in 0..400u64 {
        let key = format!("key{i:05}");
        let got = tree.get(key.as_bytes(), SeqNo::MAX)?;
        assert_eq!(
            got.as_deref(),
            Some(format!("value{i:05}").as_bytes()),
            "mixed-variant flush must preserve {key}"
        );
    }
    Ok(())
}

/// Locks the public contract of `Tree::update_runtime_config` when
/// the build does NOT enable the `page_ecc` cargo feature:
/// flipping `page_ecc = true` must return the typed error AND
/// leave the live snapshot unchanged. Without this guard the
/// silent-downgrade path comes back the moment the validator is
/// removed from `try_update`.
#[cfg(not(feature = "page_ecc"))]
#[test]
fn tree_update_runtime_config_rejects_page_ecc_without_feature() {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    let before = tree.runtime_config();
    let result = tree.update_runtime_config(|cfg| {
        cfg.page_ecc = true;
    });

    match result {
        Err(lsm_tree::Error::PageEccUnsupported) => {}
        Err(other) => panic!("expected PageEccUnsupported, got {other:?}"),
        Ok(()) => panic!(
            "update_runtime_config(page_ecc=true) on a non-`page_ecc` \
             build must return PageEccUnsupported, not silently apply"
        ),
    }

    // Invariant: a rejected update leaves the live snapshot at its
    // pre-call value. Same Arc identity is not guaranteed (the
    // handle may have re-loaded), so compare by structural equality.
    let after = tree.runtime_config();
    assert_eq!(
        before.page_ecc, after.page_ecc,
        "rejected update must not mutate the live runtime config"
    );
}

/// A runtime `ecc_scheme` change must be picked up by the flush writer,
/// not pinned to the startup snapshot.
///
/// A plain round-trip read can't catch this: the writer and the on-disk
/// descriptor are always self-consistent, so reads succeed whichever
/// scheme was used. The bug (writer reading the startup config instead of
/// the live snapshot) only shows up by asserting the scheme actually
/// recorded in the SST. Open with RS(4,2), switch to RS(8,2) at runtime,
/// flush, and assert the new SST records (8,2).
#[cfg(feature = "page_ecc")]
#[test]
fn ecc_scheme_runtime_change_is_written_to_new_ssts() -> lsm_tree::Result<()> {
    use lsm_tree::runtime_config::EccScheme;

    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .page_ecc(true)
    .ecc_scheme(EccScheme::ReedSolomon {
        data_shards: 4,
        parity_shards: 2,
    })
    .open()?;
    let tree = match any {
        AnyTree::Standard(t) => t,
        AnyTree::Blob(_) => panic!("expected Standard tree, got Blob"),
    };

    tree.update_runtime_config(|cfg| {
        cfg.ecc_scheme = EccScheme::ReedSolomon {
            data_shards: 8,
            parity_shards: 2,
        };
    })?;

    for i in 0..200u64 {
        tree.insert(format!("k{i:05}"), format!("v{i:05}"), i);
    }
    tree.flush_active_memtable(0)?;

    let schemes: Vec<(u8, u8)> = tree
        .current_version()
        .iter_tables()
        .filter_map(|t| t.metadata.ecc_params)
        .map(|p| (p.data_shards(), p.parity_shards()))
        .collect();

    assert!(
        !schemes.is_empty(),
        "flush must produce at least one ECC-bearing SST"
    );
    for s in &schemes {
        assert_eq!(
            *s,
            (8, 2),
            "the flushed SST must record the live runtime ecc_scheme (8,2), \
             not the startup scheme (4,2)"
        );
    }

    Ok(())
}

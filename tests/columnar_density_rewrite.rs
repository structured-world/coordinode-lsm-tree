// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Density-based rewrite of a columnar segment whose delete-bitmap has grown
//! past the adaptive purge threshold. A segment first relocated with a
//! sub-threshold bitmap (merge-on-read) becomes over-budget when the operator
//! lowers the threshold at runtime; an otherwise-idle compaction must then pick
//! it for a materializing rewrite that physically drops the masked rows and
//! clears the bitmap, instead of paying the mask cost on every scan forever.

#![cfg(feature = "columnar")]

use lsm_tree::compaction::Leveled;
use lsm_tree::config::{DeleteStrategy, DeleteStrategyPolicy};
use lsm_tree::{
    AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter, UserKey, get_tmp_folder,
};
use std::sync::Arc;
use test_log::test;

fn key(i: u32) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

fn value(i: u32) -> Vec<u8> {
    format!("value-{i}-payload").into_bytes()
}

#[test]
fn density_rewrite_materializes_an_over_threshold_bitmap_segment() {
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("open");
    let AnyTree::Standard(tree) = any else {
        panic!("expected standard tree");
    };

    // Columnar + zone map + Adaptive with a HIGH purge threshold, so the initial
    // delete relocates into a bitmap (merge-on-read) rather than purging.
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
        cfg.delete_strategy = DeleteStrategyPolicy::all(DeleteStrategy::Adaptive {
            purge_threshold_percent: 90,
        });
    })
    .expect("enable columnar adaptive");

    let n = 200u32;
    for i in 0..n {
        tree.insert(key(i), value(i), u64::from(i));
    }
    // Delete the first 50 (25%) at a seqno above every row, in the same memtable.
    tree.remove_range(
        UserKey::from(&key(0)[..]),
        UserKey::from(&key(50)[..]),
        1000,
    );
    tree.flush_active_memtable(0).expect("flush");

    // Materialize the range tombstone into a delete-bitmap via relocation: 25% is
    // below the 90% threshold, so the segment is relocated carrying the bitmap.
    tree.major_compact(64 * 1024 * 1024, 5000)
        .expect("relocate");

    {
        let version = tree.current_version();
        let tables: Vec<_> = version.iter_tables().collect();
        assert_eq!(tables.len(), 1, "one relocated columnar segment");
        let density = tables[0].delete_density();
        assert!(
            matches!(density, Some(p) if (20..=30).contains(&p)),
            "segment carries a ~25% delete-bitmap, got {density:?}",
        );
    }

    // The operator lowers the threshold below the segment's density. The segment
    // is now over budget, but no structural compaction is due.
    tree.update_runtime_config(|cfg| {
        cfg.delete_strategy = DeleteStrategyPolicy::all(DeleteStrategy::Adaptive {
            purge_threshold_percent: 10,
        });
    })
    .expect("lower threshold");

    // An otherwise-idle leveled compaction must pick the dense segment for a
    // materializing rewrite (the density-rewrite fallback at the orchestrator).
    tree.compact(Arc::new(Leveled::default()), 5000)
        .expect("density rewrite");

    // The bitmap is gone (rows physically dropped) and the data still round-trips.
    let version = tree.current_version();
    let tables: Vec<_> = version.iter_tables().collect();
    assert!(
        tables.iter().all(|t| t.delete_density().is_none()),
        "density rewrite must clear the delete-bitmap (materialized, not masked)",
    );
    assert!(
        tables.iter().all(|t| t.metadata.columnar),
        "the rewritten segment stays columnar",
    );
    for i in 0..n {
        let got = tree.get(key(i), SeqNo::MAX).expect("get");
        if i < 50 {
            assert!(
                got.is_none(),
                "deleted key {i} stays absent after materialization",
            );
        } else {
            assert_eq!(
                &*got.expect("live key must be present"),
                value(i).as_slice(),
                "live key {i} value survives the rewrite",
            );
        }
    }
}

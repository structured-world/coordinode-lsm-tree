// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! End-to-end tests for the opt-in computed write-backpressure verdict
//! ([`AbstractTree::write_backpressure`]): the L0-count signal driving the
//! Slowdown then Stop tiers, the off-by-default no-op, live re-configuration,
//! and BlobTree delegation. The pure tier/ramp arithmetic is unit-tested in
//! `src/backpressure/tests.rs`; here we prove the wiring reads the live version
//! and the runtime config.

use core::time::Duration;
use lsm_tree::compaction::Leveled;
use lsm_tree::{
    AbstractTree, AnyTree, Backpressure, BackpressureThresholds, Config, KvSeparationOptions,
    SequenceNumberCounter, get_tmp_folder,
};
use std::sync::Arc;

fn open_tree(path: &std::path::Path) -> lsm_tree::Tree {
    match Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("open")
    {
        AnyTree::Standard(t) => t,
        AnyTree::Blob(_) => panic!("expected Standard tree"),
    }
}

fn open_blob_tree(path: &std::path::Path) -> lsm_tree::BlobTree {
    match Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    .open()
    .expect("open")
    {
        AnyTree::Blob(t) => t,
        AnyTree::Standard(_) => panic!("expected Blob tree"),
    }
}

/// Flush one non-empty memtable, adding exactly one L0 table (compaction is
/// caller-driven, so nothing merges L0 underneath us).
fn add_l0_table(tree: &lsm_tree::Tree, round: u64) {
    tree.insert(
        format!("k{round:05}").as_bytes(),
        b"payload".as_slice(),
        round,
    );
    tree.flush_active_memtable(0).expect("flush");
}

#[test]
fn backpressure_off_by_default_is_none() {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());
    // Build a tall L0 with no thresholds configured: the verdict stays None.
    for round in 0..6 {
        add_l0_table(&tree, round);
    }
    assert_eq!(
        tree.write_backpressure(&Leveled::default()),
        Backpressure::None,
        "off-by-default must never throttle, regardless of L0 height"
    );
}

#[test]
fn l0_count_drives_slowdown_then_stop() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());
    let strat = Leveled::default();

    // Isolate the L0-count axis (bytes axis unset) for a deterministic test:
    // each flush adds exactly one L0 table.
    tree.update_runtime_config(|c| {
        c.backpressure = BackpressureThresholds {
            l0_slowdown: Some(2),
            l0_stop: Some(4),
            bytes_slowdown: None,
            bytes_stop: None,
            max_slowdown: Some(Duration::from_millis(5)),
        };
    })?;

    // 0 L0 tables: below the slowdown trigger.
    assert_eq!(tree.write_backpressure(&strat), Backpressure::None);

    // 2 L0 tables: at the slowdown trigger.
    add_l0_table(&tree, 0);
    add_l0_table(&tree, 1);
    assert!(
        matches!(
            tree.write_backpressure(&strat),
            Backpressure::Slowdown { .. }
        ),
        "two L0 tables must reach the slowdown tier"
    );

    // 4 L0 tables: at the stop trigger.
    add_l0_table(&tree, 2);
    add_l0_table(&tree, 3);
    assert_eq!(
        tree.write_backpressure(&strat),
        Backpressure::Stop,
        "four L0 tables must reach the stop tier"
    );
    Ok(())
}

#[test]
fn thresholds_are_live_toggleable() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());
    let strat = Leveled::default();

    for round in 0..4 {
        add_l0_table(&tree, round);
    }

    // Turn the policy on with a low stop trigger: the existing L0 height now
    // reports Stop (the verdict is computed, not latched).
    tree.update_runtime_config(|c| {
        c.backpressure = BackpressureThresholds {
            l0_slowdown: Some(1),
            l0_stop: Some(2),
            bytes_slowdown: None,
            bytes_stop: None,
            max_slowdown: Some(Duration::from_millis(1)),
        };
    })?;
    assert_eq!(tree.write_backpressure(&strat), Backpressure::Stop);

    // Turn it back off: same L0 height, verdict clears immediately.
    tree.update_runtime_config(|c| {
        c.backpressure = BackpressureThresholds::OFF;
    })?;
    assert_eq!(tree.write_backpressure(&strat), Backpressure::None);
    Ok(())
}

#[test]
fn draining_compaction_clears_the_verdict() -> lsm_tree::Result<()> {
    // Closed loop: honouring Stop means the caller drains via compaction, which
    // must clear the verdict (it is computed on the live version, not latched).
    // Paired with `backpressure_off_by_default_is_none` (which lets L0 grow with
    // no signal), this proves the verdict is a real, actionable signal.
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());
    let strat = Arc::new(Leveled::default());

    tree.update_runtime_config(|c| {
        c.backpressure = BackpressureThresholds {
            l0_slowdown: Some(2),
            l0_stop: Some(4),
            bytes_slowdown: None,
            bytes_stop: None,
            max_slowdown: Some(Duration::from_millis(5)),
        };
    })?;

    for round in 0..5 {
        add_l0_table(&tree, round);
    }
    assert_eq!(
        tree.write_backpressure(&*strat),
        Backpressure::Stop,
        "five L0 tables must be at the stop tier before draining"
    );

    // Honour the signal by draining: a full compaction folds L0 into deeper
    // levels, dropping the L0 count below the stop trigger.
    tree.compact(strat.clone(), u64::MAX)?;
    assert_ne!(
        tree.write_backpressure(&*strat),
        Backpressure::Stop,
        "a draining compaction must clear the Stop verdict"
    );
    Ok(())
}

#[test]
fn blob_tree_delegates_backpressure_to_index() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_blob_tree(folder.path());
    let strat = Leveled::default();

    // Runtime config lives on the index tree (the blob tree forwards reads /
    // version through it); the backpressure override delegates there too.
    tree.index.update_runtime_config(|c| {
        c.backpressure = BackpressureThresholds {
            l0_slowdown: Some(1),
            l0_stop: Some(2),
            bytes_slowdown: None,
            bytes_stop: None,
            max_slowdown: Some(Duration::from_millis(1)),
        };
    })?;

    // Off before any L0 tables.
    assert_eq!(tree.write_backpressure(&strat), Backpressure::None);

    // Two flushes of the index tree -> stop tier, observed through the blob
    // tree's delegating override.
    for round in 0..2u64 {
        tree.insert(
            format!("k{round:05}").as_bytes(),
            b"payload".as_slice(),
            round,
        );
        tree.flush_active_memtable(0).expect("flush");
    }
    assert_eq!(tree.write_backpressure(&strat), Backpressure::Stop);
    Ok(())
}

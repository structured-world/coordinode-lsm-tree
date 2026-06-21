// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Demonstrates the metadata-block priority pin: when the data working set
//! exceeds the block cache, heavy data-block churn evicts metadata blocks from
//! the shared queue, forcing a disk re-read on the next seek. Pinning index /
//! filter / range-tombstone blocks at high priority lets them survive the
//! churn. This test reproduces the eviction regime (working set >> cache,
//! scattered reads over many SSTs) and asserts the pin materially cuts filter
//! block disk re-reads (uncached loads) versus the pin disabled: with these
//! small SSTs the per-SST filter block is the metadata that loses the eviction
//! race, while the tiny single index block per SST stays resident either way.
//!
//! Gated on `metrics` because the per-block load counters live behind it.

#![cfg(feature = "metrics")]

use lsm_tree::{
    AbstractTree, AnyTree, Cache, Config, SeqNo, SequenceNumberCounter,
    config::{BlockSizePolicy, PinningPolicy},
    get_tmp_folder,
};
use std::sync::Arc;
use test_log::test;

fn key(i: u32) -> Vec<u8> {
    format!("k{i:08}").into_bytes()
}

/// Builds a many-SST tree on a cache sized to hold the metadata working set
/// plus only part of the data, warms the index / filter blocks, then churns
/// scattered point reads over a data working set that exceeds the cache.
/// Returns the index and filter block disk re-reads (uncached loads) accrued
/// during the churn phase only.
fn churn_index_filter_reloads(metadata_priority: bool) -> (usize, usize) {
    let folder = get_tmp_folder();
    // Cache holds the metadata set plus a fraction of the data, far less than
    // the whole data working set, so data churn drives eviction. With the pin
    // ON the metadata is held in the protected tier and data is evicted instead.
    let cache =
        Arc::new(Cache::with_capacity_bytes(256 * 1024).with_metadata_priority(metadata_priority));
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .use_cache(cache)
    .data_block_size_policy(BlockSizePolicy::all(4 * 1024))
    // Unpin index / filter blocks so they live in the shared block cache (the
    // regime the priority pin targets); pinned-in-reader blocks bypass the cache
    // and never feel eviction.
    .index_block_pinning_policy(PinningPolicy::all(false))
    .filter_block_pinning_policy(PinningPolicy::all(false))
    .open()
    .expect("open");
    let AnyTree::Standard(tree) = any else {
        panic!("expected standard tree");
    };

    // Many SSTs so each one's index / filter block is touched only on the small
    // fraction of reads that land in its key range: cold enough that, without
    // the priority pin, data-block churn evicts it from the probationary queue
    // before its next use (with the pin it goes straight to the protected tier).
    let per_sst = 130u32;
    let ssts = 30u32;
    let n = per_sst * ssts;
    for sst in 0..ssts {
        for i in 0..per_sst {
            tree.insert(key(sst * per_sst + i), vec![b'v'; 200], 0);
        }
        tree.flush_active_memtable(0).expect("flush");
    }

    let m = tree.metrics();
    let uncached = |load: usize, cached: usize| load.saturating_sub(cached);

    // Warm: one strided pass touches every SST's index + filter.
    for i in (0..n).step_by(97) {
        let _ = tree.get(key(i), SeqNo::MAX).expect("get");
    }
    let idx_before = uncached(
        m.index_block_load_count(),
        m.index_block_load_cached_count(),
    );
    let filt_before = uncached(
        m.filter_block_load_count(),
        m.filter_block_load_cached_count(),
    );

    // Churn: many passes of scattered reads over the whole working set, evicting
    // data blocks repeatedly. A simple multiplicative hash spreads the keys.
    for round in 0..8u32 {
        for i in (0..n).step_by(3) {
            let k = i
                .wrapping_mul(2_654_435_761)
                .wrapping_add(round.wrapping_mul(7))
                % n;
            let _ = tree.get(key(k), SeqNo::MAX).expect("get");
        }
    }
    let idx_after = uncached(
        m.index_block_load_count(),
        m.index_block_load_cached_count(),
    );
    let filt_after = uncached(
        m.filter_block_load_count(),
        m.filter_block_load_cached_count(),
    );

    (idx_after - idx_before, filt_after - filt_before)
}

#[test]
fn metadata_priority_cuts_filter_reloads_under_data_churn() {
    let (idx_on, filt_on) = churn_index_filter_reloads(true);
    let (idx_off, filt_off) = churn_index_filter_reloads(false);

    // The OFF baseline must actually show churn-driven filter reloads, else the
    // test is not exercising the eviction regime it claims to.
    assert!(
        filt_off > 0,
        "baseline must show filter reloads under churn (else not the eviction regime)"
    );
    // With the pin ON, filter blocks survive the data-block churn, so they are
    // re-read from disk materially less often than with the pin OFF (where the
    // shared queue lets data churn evict them).
    assert!(
        filt_on < filt_off,
        "metadata priority must cut filter reloads under churn: on={filt_on} off={filt_off}"
    );
    // Materially: the pin cuts filter disk re-reads by more than a third.
    assert!(
        filt_on * 3 < filt_off * 2,
        "metadata priority must materially cut filter reloads: on={filt_on} off={filt_off}"
    );
    // Index blocks for these small SSTs stay resident in either mode (a single
    // tiny index block per SST never loses the eviction race), so the pin must
    // at least never make index reloads worse.
    assert!(
        idx_on <= idx_off,
        "metadata priority must not raise index reloads: on={idx_on} off={idx_off}"
    );
}

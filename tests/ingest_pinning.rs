// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Ingested tables land in L0, so they follow the L0 block pinning policy the
//! way a flushed table does.

use lsm_tree::config::PinningPolicy;
use lsm_tree::{
    AbstractTree, AnyTree, Config, KvSeparationOptions, SequenceNumberCounter, get_tmp_folder,
};

const KEYS: u32 = 500;

fn key(i: u32) -> String {
    format!("key_{i:06}")
}

fn open(
    folder: &std::path::Path,
    configure: impl FnOnce(Config) -> Config,
) -> lsm_tree::Result<AnyTree> {
    configure(Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    ))
    .open()
}

fn ingest(tree: &AnyTree) -> lsm_tree::Result<()> {
    let mut ingest = tree.ingestion()?;
    for i in 0..KEYS {
        ingest.write(key(i), "v")?;
    }
    ingest.finish()
}

/// Under the default policy (L0 filter and index pinned) an ingested table
/// pins its filter and index like a flushed one, instead of loading them
/// through the cache on every point read until the tree reopens.
#[test]
fn ingested_tables_follow_the_l0_pinning_policy() -> lsm_tree::Result<()> {
    let flushed_dir = get_tmp_folder();
    let flushed = open(flushed_dir.path(), |c| c)?;
    for i in 0..KEYS {
        flushed.insert(key(i), "v", u64::from(i));
    }
    flushed.flush_active_memtable(u64::from(KEYS))?;
    assert!(
        flushed.pinned_filter_size() > 0,
        "a flushed L0 table pins its filter"
    );
    assert!(
        flushed.pinned_block_index_size() > 0,
        "a flushed L0 table pins its index"
    );

    let ingested_dir = get_tmp_folder();
    let ingested = open(ingested_dir.path(), |c| c)?;
    ingest(&ingested)?;
    assert_eq!(
        ingested.current_version().l0().table_count(),
        ingested.table_count(),
        "ingested tables are installed in L0"
    );
    assert!(
        ingested.pinned_filter_size() > 0,
        "an ingested L0 table must pin its filter"
    );
    assert!(
        ingested.pinned_block_index_size() > 0,
        "an ingested L0 table must pin its index"
    );
    Ok(())
}

/// The blob tree's ingestion builds its index tables through its own path;
/// they follow the L0 policy the same way.
#[test]
fn blob_tree_ingested_tables_follow_the_l0_pinning_policy() -> lsm_tree::Result<()> {
    let dir = get_tmp_folder();
    let tree = open(dir.path(), |c| {
        c.with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    })?;
    ingest(&tree)?;
    assert!(
        tree.pinned_filter_size() > 0,
        "an ingested blob-tree L0 table must pin its filter"
    );
    assert!(
        tree.pinned_block_index_size() > 0,
        "an ingested blob-tree L0 table must pin its index"
    );
    Ok(())
}

/// A policy that pins nothing in L0 leaves ingested tables unpinned too.
#[test]
fn ingested_tables_stay_unpinned_when_l0_pins_nothing() -> lsm_tree::Result<()> {
    let dir = get_tmp_folder();
    let tree = open(dir.path(), |c| {
        c.filter_block_pinning_policy(PinningPolicy::all(false))
            .index_block_pinning_policy(PinningPolicy::all(false))
    })?;
    ingest(&tree)?;
    assert_eq!(0, tree.pinned_filter_size());
    assert_eq!(0, tree.pinned_block_index_size());
    Ok(())
}

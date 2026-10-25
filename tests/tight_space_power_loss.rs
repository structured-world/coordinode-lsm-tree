// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A tight-space pass cut short by a power loss at any of its syncs.
//!
//! The pass installs one version edit per slice, then publishes each surviving
//! input's restriction sidecar and punches the prefix the slice consumed. A
//! crash anywhere in that sequence must reopen into a tree whose every key
//! reads its latest value: a reader never lands in a punched prefix, and no
//! slice's rows are both served from its output and dropped from its input.

use lsm_tree::config::BlockSizePolicy;
use lsm_tree::fs::{CrashFs, FaultFs, Fs, MemFs};
use lsm_tree::{AbstractTree, AnyTree, Config, KvSeparationOptions, SeqNo, SequenceNumberCounter};
use std::sync::Arc;

const KEYS: u32 = 2_000;

fn key(i: u32) -> Vec<u8> {
    format!("key{i:08}").into_bytes()
}

/// What generation `generation` writes under key `i`.
fn value(i: u32, generation: u8, blobs: bool) -> Vec<u8> {
    let len = if blobs { 200 } else { 64 };
    let mut v = vec![generation; len];
    v[..4].copy_from_slice(&i.to_le_bytes());
    v
}

fn open_on(fs: &Arc<dyn Fs>, blobs: bool) -> lsm_tree::Result<AnyTree> {
    let config = Config::new(
        "/db",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(1_024))
    .with_shared_fs(Arc::clone(fs));
    if blobs {
        config
            .with_kv_separation(Some(
                KvSeparationOptions::default()
                    .separation_threshold(64)
                    .age_cutoff(1.0)
                    .staleness_threshold(0.1)
                    .file_target_size(48 * 1_024),
            ))
            .open()
    } else {
        config.open()
    }
}

/// The generation key `i` reads after the second one: every key of a
/// standard tree, the even keys of a KV-separated one, which the second
/// generation overwrites to leave the first one's blob files half dead.
fn latest(i: u32, blobs: bool) -> u8 {
    if blobs && i % 2 == 1 { 1 } else { 2 }
}

/// The gc watermark of the passes: above every write, so a merge may drop
/// what the second generation shadows and relocate the live blobs.
const WATERMARK: SeqNo = 4 * KEYS as SeqNo;

fn update(
    tree: &AnyTree,
    f: impl Fn(&mut lsm_tree::runtime_config::RuntimeConfig),
) -> lsm_tree::Result<()> {
    match tree {
        AnyTree::Standard(t) => t.update_runtime_config(f),
        AnyTree::Blob(t) => t.update_runtime_config(f),
    }
}

/// A tree on `fs` holding two flushed, overlapping generations, with a quota
/// that leaves room for a slice of their merge but not for all of it, and
/// tight-space compaction enabled. A KV-separated tree's merge relocates the
/// live blobs of the half-dead first generation: a merge with room to spare
/// learns how much of each blob file is dead first.
fn before_pass(fs: &Arc<dyn Fs>, blobs: bool) -> lsm_tree::Result<AnyTree> {
    let tree = open_on(fs, blobs)?;
    let mut seqno = 0;
    for generation in [1u8, 2] {
        for i in (0..KEYS).filter(|i| generation == 1 || latest(*i, blobs) == 2) {
            tree.insert(key(i), value(i, generation, blobs), seqno);
            seqno += 1;
        }
        tree.flush_active_memtable(0)?;
    }
    if blobs {
        tree.major_compact(64 * 1024 * 1024, WATERMARK)?;
    }
    let used = tree.storage_stats()?.used_bytes;
    update(&tree, |c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = Some(used + used / 4);
        c.tight_space_compaction = true;
    })?;
    Ok(tree)
}

fn pass(tree: &AnyTree) -> lsm_tree::Result<lsm_tree::compaction::CompactionResult> {
    tree.major_compact(64 * 1024 * 1024, WATERMARK)
}

/// A crash simulator and a fault layer over it the tree runs on.
fn crash_stack() -> (CrashFs, Arc<FaultFs<CrashFs>>) {
    let crash = CrashFs::new(MemFs::new());
    let fault = Arc::new(FaultFs::new(crash.clone()));
    (crash, fault)
}

/// A power loss after any sync of a tight-space pass reopens into a tree
/// whose every key reads its latest value: the slices installed before
/// the loss serve their keys, the inputs serve the rest, and no read reaches
/// a prefix a slice punched.
#[test]
fn tight_space_pass_survives_a_power_loss_at_every_sync() -> lsm_tree::Result<()> {
    for blobs in [false, true] {
        let (_, clean) = crash_stack();
        let fs: Arc<dyn Fs> = clean.clone();
        let tree = before_pass(&fs, blobs)?;
        let tables_before = tree.table_count();
        drop(clean.injector().take_syncs());
        pass(&tree)?;
        let tables_after = tree.table_count();
        let syncs = clean.injector().take_syncs().len();
        assert!(
            !clean.injector().punches_for("").is_empty(),
            "self-check: the pass is a tight-space one, it punches (blobs: {blobs})",
        );
        assert!(
            tree.table_count() > 1 && tree.table_count() != tables_before,
            "self-check: the pass rewrote the inputs in slices (blobs: {blobs})",
        );
        drop(tree);

        for kept in 0..=syncs {
            let (crash, fault) = crash_stack();
            let fs: Arc<dyn Fs> = fault.clone();
            let tree = before_pass(&fs, blobs)?;
            if kept < syncs {
                fault.injector().power_off_after_syncs(kept);
            }
            // The power going out fails the pass; the reopen after the crash
            // is what this checks either way.
            let returned = pass(&tree).is_ok();
            drop(tree);
            crash.crash();

            let fs: Arc<dyn Fs> = Arc::new(crash.clone());
            let tree = open_on(&fs, blobs)?;
            if returned {
                assert_eq!(
                    tables_after,
                    tree.table_count(),
                    "a pass that returned was lost (sync {kept} of {syncs}, blobs: {blobs})",
                );
            }
            for i in 0..KEYS {
                assert_eq!(
                    tree.get(key(i), SeqNo::MAX)?.as_deref(),
                    Some(value(i, latest(i, blobs), blobs).as_slice()),
                    "key {i} after a power loss at sync {kept} of {syncs} (blobs: {blobs})",
                );
            }
        }
    }
    Ok(())
}

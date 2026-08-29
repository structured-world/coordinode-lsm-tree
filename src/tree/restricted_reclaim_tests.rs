// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Recovery re-derives the tight-space punch intent of a committed
//! restriction. A reclaim the running tree had to defer (a checkpoint still
//! hard-linked the file) lived only in that session's queue, and the
//! unrestricted view able to re-arm it is gone after a restart.

use crate::fs::{Fs, MemFs};
use crate::{AbstractTree, AnyTree, Config, InternalValue, SequenceNumberCounter, ValueType};
use std::sync::Arc;

/// Writes a multi-block table `0` under `dir/tables`, records a committed
/// restriction for it WITHOUT punching the prefix away (the shape a deferred
/// reclaim leaves behind), and returns the config that opens it.
fn tree_with_an_unreclaimed_restriction(memfs: &Arc<MemFs>) -> crate::Result<Config> {
    let fs_dyn: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db_unreclaimed_restriction")?;
    let table_path = root.join("tables").join("0");
    memfs.create_dir_all(table_path.parent().expect("tables folder"))?;

    let mut writer = crate::table::Writer::new(table_path.clone(), 0, 0, Arc::clone(&fs_dyn))?
        .use_data_block_size(128);
    for i in 0..400u32 {
        writer.write(InternalValue::from_components(
            format!("key{i:06}").into_bytes(),
            alloc::vec![0xABu8; 64],
            u64::from(i) + 1,
            ValueType::Value,
        ))?;
    }
    assert!(writer.finish()?.is_some(), "the table is non-empty");

    let config = || {
        Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(memfs.clone())
    };
    // The bound the tight-space loop consumed up to. The sidecar is the
    // authority a repair reads, so writing it and repairing commits the
    // restriction to the manifest exactly as the loop would have.
    let bound: crate::UserKey = b"key000200".to_vec().into();
    crate::restrict_bound::write(
        &*fs_dyn,
        &table_path,
        None,
        0,
        &bound,
        crate::fs::SyncMode::Normal,
    )?;
    config().repair()?;
    Ok(config())
}

#[test]
fn a_reopen_reclaims_the_prefix_of_a_committed_restriction() -> crate::Result<()> {
    let memfs = Arc::new(MemFs::new());
    let config = tree_with_an_unreclaimed_restriction(&memfs)?;
    let before = memfs.punched_bytes();

    let tree = match config.open()? {
        AnyTree::Standard(t) => t,
        AnyTree::Blob(_) => panic!("expected Standard tree"),
    };

    assert!(
        memfs.punched_bytes() > before,
        "the open must re-derive the punch the committed restriction implies \
         (punched {}, unchanged)",
        memfs.punched_bytes(),
    );
    assert_eq!(
        tree.get(b"key000300", crate::MAX_SEQNO)?.as_deref(),
        Some(&[0xABu8; 64][..]),
        "the reclaim must not touch the live suffix",
    );
    assert_eq!(
        tree.get(b"key000100", crate::MAX_SEQNO)?,
        None,
        "the consumed prefix stays gone",
    );
    Ok(())
}

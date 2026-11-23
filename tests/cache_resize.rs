// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A block cache shared by two trees resized while both are open: reads stay
//! correct across a shrink and a grow, the shrink leaves the cache within its
//! new capacity at once, and reads after the grow fill it past the old one.

use lsm_tree::{AbstractTree, Cache, Config, SeqNo, SequenceNumberCounter, get_tmp_folder};
use std::sync::Arc;
use test_log::test;

const KEYS: u32 = 20_000;

fn value(tree: u8, i: u32) -> Vec<u8> {
    let mut value = vec![tree; 200];
    value[..4].copy_from_slice(&i.to_be_bytes());
    value
}

#[test]
fn a_shared_cache_resized_under_open_trees_serves_every_read_and_refills() -> lsm_tree::Result<()> {
    let cache = Arc::new(Cache::with_capacity_bytes(256 << 10));
    let folders = [get_tmp_folder(), get_tmp_folder()];
    let mut trees = Vec::new();
    for (n, folder) in (0u8..).zip(&folders) {
        let tree = Config::new(
            folder.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .use_cache(Arc::clone(&cache))
        .open()?;
        for i in 0..KEYS {
            tree.insert(i.to_be_bytes(), value(n, i), u64::from(i));
        }
        tree.flush_active_memtable(0)?;
        trees.push((n, tree));
    }
    let read_all = || -> lsm_tree::Result<()> {
        for (n, tree) in &trees {
            for i in (0..KEYS).step_by(7) {
                assert_eq!(
                    tree.get(i.to_be_bytes(), SeqNo::MAX)?.as_deref(),
                    Some(&value(*n, i)[..]),
                    "tree {n} key {i}"
                );
            }
        }
        Ok(())
    };

    read_all()?;
    let first = cache.size();
    read_all()?;
    assert!(
        cache.size() > 64 << 10,
        "{first} then {} bytes cached",
        cache.size()
    );

    cache.set_capacity(64 << 10);
    assert!(
        cache.size() <= 64 << 10,
        "{} bytes after the shrink",
        cache.size()
    );
    read_all()?;
    assert!(
        cache.size() <= 64 << 10,
        "{} bytes read into the shrunk cache",
        cache.size()
    );

    cache.set_capacity(4 << 20);
    read_all()?;
    assert!(
        cache.size() > 256 << 10,
        "{} bytes after reading into the grown cache",
        cache.size()
    );
    assert!(cache.size() <= 4 << 20);
    Ok(())
}

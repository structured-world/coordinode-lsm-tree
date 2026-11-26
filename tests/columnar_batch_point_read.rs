// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A batch read of a columnar table answers as the point reads of its keys
//! do, whether a row group is read through its pages for the few keys in it
//! or rebuilt for many.

#![cfg(feature = "columnar")]

use lsm_tree::config::BlockSizePolicy;
use lsm_tree::{AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter};

const KEYS: usize = 600;

/// The key of row `i`; only even rows are written.
fn key(i: usize) -> Vec<u8> {
    format!("key-{i:06}").into_bytes()
}

/// A columnar tree whose row groups hold a handful of keys each, every key
/// written in three versions (seqnos 0, 1 and 2), every fifth one deleted at
/// seqno 2: a key's versions straddle row pages and row groups.
fn tree() -> lsm_tree::Result<(AnyTree, tempfile::TempDir)> {
    let folder = tempfile::tempdir()?;
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .columnar_row_group_size_policy(BlockSizePolicy::all(4 * 1_024))
    .columnar_page_size_policy(BlockSizePolicy::all(1_024))
    .open()?;
    let AnyTree::Standard(standard) = &tree else {
        panic!("a standard tree");
    };
    standard.update_runtime_config(|cfg| cfg.columnar = true)?;
    for version in 0..3u64 {
        for i in 0..KEYS {
            if version == 2 && i % 5 == 0 {
                tree.remove(key(i * 2), version);
            } else {
                let mut value = vec![b'v'; 200];
                value[..8].copy_from_slice(&((i as u64) << 8 | version).to_le_bytes());
                tree.insert(key(i * 2), value, version);
            }
        }
    }
    tree.flush_active_memtable(0)?;
    assert!(tree.table_count() > 0, "self-check: the rows are in tables");
    Ok((tree, folder))
}

/// Orders keys by length first, then by their bytes: an order the byte-wise
/// zones of a row page do not follow.
struct ShorterFirst;

impl lsm_tree::UserComparator for ShorterFirst {
    fn name(&self) -> &'static str {
        "shorter-first"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> core::cmp::Ordering {
        a.len().cmp(&b.len()).then_with(|| a.cmp(b))
    }
}

/// Under an order that is not the byte order, a batch read answers as point
/// reads do. A row page's zone is byte-wise, so the row pages it selects for
/// one key need not lie side by side: a key of the batch that falls between
/// two of them is not absent, it is on a page the first key's zone did not
/// select. Keys of one, two and three letters, so that a short key's zone
/// also holds longer keys that sort far after it.
#[test]
fn a_batch_read_under_another_key_order_answers_as_point_reads_do() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .comparator(std::sync::Arc::new(ShorterFirst))
    .columnar_row_group_size_policy(BlockSizePolicy::all(64 * 1_024))
    .columnar_page_size_policy(BlockSizePolicy::all(1_024))
    .open()?;
    let AnyTree::Standard(standard) = &tree else {
        panic!("a standard tree");
    };
    standard.update_runtime_config(|cfg| cfg.columnar = true)?;

    let letters = b'a'..=b'z';
    let mut keys: Vec<Vec<u8>> = letters.clone().map(|a| vec![a]).collect();
    keys.extend(
        letters
            .clone()
            .flat_map(|a| letters.clone().map(move |b| vec![a, b])),
    );
    keys.extend(letters.clone().step_by(5).flat_map(|a| {
        let letters = letters.clone();
        letters.step_by(3).map(move |b| vec![a, b, b'x'])
    }));
    // Every other key is written, so the rest read absent.
    for (i, key) in keys.iter().enumerate().filter(|(i, _)| i % 2 == 0) {
        let mut value = vec![b'v'; 200];
        value[..8].copy_from_slice(&(i as u64).to_le_bytes());
        tree.insert(key, value, 0);
    }
    tree.flush_active_memtable(0)?;
    assert!(tree.table_count() > 0, "self-check: the rows are in tables");

    keys.sort_by(|a, b| lsm_tree::UserComparator::compare(&ShorterFirst, a, b));
    let expected = keys
        .iter()
        .map(|key| tree.get(key, SeqNo::MAX))
        .collect::<lsm_tree::Result<Vec<_>>>()?;
    assert!(
        expected.iter().any(Option::is_some) && expected.iter().any(Option::is_none),
        "self-check: the keys read both present and absent"
    );
    for batch in [2, 3, 16, keys.len()] {
        for stride in [1, 5, 11] {
            let picked: Vec<&Vec<u8>> = keys.iter().step_by(stride).collect();
            for (at, chunk) in picked.chunks(batch).enumerate() {
                let got = tree.multi_get(chunk, SeqNo::MAX)?;
                assert_eq!(got.len(), chunk.len(), "an answer for every key");
                for (offset, (key, value)) in chunk.iter().zip(&got).enumerate() {
                    let index = (at * batch + offset) * stride;
                    assert_eq!(
                        value.as_deref(),
                        expected[index].as_deref(),
                        "key {:?}, batches of {batch}, every {stride}",
                        String::from_utf8_lossy(key),
                    );
                }
            }
        }
    }
    Ok(())
}

/// Reads every key, present and absent, in batches of each size at each
/// snapshot, and holds every answer to the point read's. Covers a batch that
/// lands one key in a group, one that lands a few, and one that lands more
/// than a group is read through its pages for; a snapshot below a key's
/// newest version, whose visible version lies further down its run; and a
/// deleted key.
#[test]
fn a_batch_read_of_a_columnar_table_answers_as_point_reads_do() -> lsm_tree::Result<()> {
    let (tree, _folder) = tree()?;
    let keys: Vec<Vec<u8>> = (0..KEYS * 2).map(key).collect();
    for seqno in [1, 2, 3, SeqNo::MAX] {
        let expected = keys
            .iter()
            .map(|key| tree.get(key, seqno))
            .collect::<lsm_tree::Result<Vec<_>>>()?;
        assert!(
            expected.iter().any(Option::is_some) && expected.iter().any(Option::is_none),
            "self-check: the keys read both present and absent at {seqno}"
        );
        for batch in [1, 3, 16, 17, 64, keys.len()] {
            // Spread batches too: every `batch`-th key, so a batch lands a
            // key or two in each group it touches.
            for stride in [1, 7] {
                let picked: Vec<&Vec<u8>> = keys.iter().step_by(stride).collect();
                for (at, chunk) in picked.chunks(batch).enumerate() {
                    let got = tree.multi_get(chunk, seqno)?;
                    assert_eq!(got.len(), chunk.len(), "an answer for every key");
                    for (offset, (key, value)) in chunk.iter().zip(&got).enumerate() {
                        let index = (at * batch + offset) * stride;
                        assert_eq!(
                            value.as_deref(),
                            expected[index].as_deref(),
                            "key {} at seqno {seqno}, batches of {batch}, every {stride}",
                            String::from_utf8_lossy(key),
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

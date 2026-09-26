// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A block read at another block's offset is corruption.
//!
//! A misdirected write, a remapped sector or a copy error can leave a
//! checksum-valid block of a table where another one belongs. Every block
//! verifies on its own bytes, so without a binding to its place, a point read
//! served from the wrong block answers with a false miss and a scan or a
//! compaction streams its rows out of order. These tests swap two data blocks
//! of the same length in place and require every read path to refuse them.

use lsm_tree::config::CompressionPolicy;
use lsm_tree::{AbstractTree, CompressionType, Config, Guard, SeqNo, SequenceNumberCounter};
use std::path::{Path, PathBuf};
use test_log::test;

/// Rows written: enough for several data blocks of the default size.
const ROWS: u32 = 400;

fn key(i: u32) -> Vec<u8> {
    format!("k{i:07}").into_bytes()
}

fn value(i: u32) -> Vec<u8> {
    let mut v = format!("v{i:07}").into_bytes();
    v.resize(100, b'.');
    v
}

/// The single table file of the tree at `dir`.
fn table_file(dir: &Path) -> PathBuf {
    let tables = dir.join(lsm_tree::file::TABLES_FOLDER);
    let mut files: Vec<PathBuf> = std::fs::read_dir(&tables)
        .expect("tables folder")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.is_file())
        .collect();
    assert_eq!(files.len(), 1, "one table file: {files:?}");
    files.remove(0)
}

/// The on-disk lengths of the data blocks at the front of an SST whose blocks
/// carry no parity: a header is 4 magic bytes, the block type, a 16-byte
/// checksum, the payload and uncompressed lengths and a header checksum, with
/// the payload length at byte 21.
fn data_blocks(bytes: &[u8]) -> Vec<(usize, usize)> {
    const HEADER: usize = 33;
    const DATA: u8 = 0;
    let mut blocks = Vec::new();
    let mut at = 0;
    while let Some(header) = bytes.get(at..at + HEADER) {
        if header[..4] != lsm_tree::file::MAGIC_BYTES || header[4] != DATA {
            break;
        }
        let payload = u32::from_le_bytes(header[21..25].try_into().expect("four bytes")) as usize;
        blocks.push((at, HEADER + payload));
        at += HEADER + payload;
    }
    blocks
}

/// Swaps two adjacent data blocks of the same length in the table file.
fn swap_two_blocks(file: &Path) {
    let mut bytes = std::fs::read(file).expect("read table");
    let blocks = data_blocks(&bytes);
    let (a, b) = blocks
        .windows(2)
        .find_map(|w| (w[0].1 == w[1].1).then_some((w[0], w[1])))
        .expect("two adjacent data blocks of one length");
    let first = bytes[a.0..a.0 + a.1].to_vec();
    let second = bytes[b.0..b.0 + b.1].to_vec();
    bytes[a.0..a.0 + a.1].copy_from_slice(&second);
    bytes[b.0..b.0 + b.1].copy_from_slice(&first);
    std::fs::write(file, bytes).expect("write table");
}

fn open(dir: &Path, config: impl FnOnce(Config) -> Config) -> lsm_tree::AnyTree {
    config(
        Config::new(
            dir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .data_block_compression_policy(CompressionPolicy::all(CompressionType::None)),
    )
    .open()
    .expect("open")
}

/// Writes the rows, flushes one table, swaps two of its blocks and reopens.
fn misplaced_tree(dir: &Path, config: impl Fn(Config) -> Config) -> lsm_tree::AnyTree {
    {
        let tree = open(dir, &config);
        for i in 0..ROWS {
            tree.insert(key(i), value(i), u64::from(i));
        }
        tree.flush_active_memtable(0).expect("flush");
    }
    swap_two_blocks(&table_file(dir));
    open(dir, config)
}

/// Every read path refuses the misplaced blocks.
fn assert_refused(tree: &lsm_tree::AnyTree) {
    // Point reads: a key whose block was moved is an error, never a miss and
    // never another key's value.
    let mut refused = 0;
    for i in 0..ROWS {
        match tree.get(key(i), SeqNo::MAX) {
            Ok(Some(v)) => assert_eq!(&*v, value(i).as_slice(), "key {i}"),
            Ok(None) => panic!("key {i} reads as absent from a misplaced block"),
            Err(_) => refused += 1,
        }
    }
    assert!(
        refused > 0,
        "a point read reached a misplaced block and refused it"
    );

    // A scan through the index errors instead of yielding the rows out of
    // order.
    let scanned: Result<Vec<_>, _> = tree
        .iter(SeqNo::MAX, None)
        .map(|g| g.into_inner().map(|(k, _)| k))
        .collect();
    assert!(
        scanned.is_err(),
        "the scan must refuse the misplaced blocks"
    );
    let reversed: Result<Vec<_>, _> = tree
        .iter(SeqNo::MAX, None)
        .rev()
        .map(|g| g.into_inner().map(|(k, _)| k))
        .collect();
    assert!(
        reversed.is_err(),
        "a reverse scan must refuse the misplaced blocks"
    );

    // A range read positioned inside a slot never skips to later keys: its
    // first entry is the key it starts at, or an error.
    for i in 0..ROWS {
        match tree.range(key(i).., SeqNo::MAX, None).next() {
            Some(g) => {
                if let Ok((k, _)) = g.into_inner() {
                    assert_eq!(&*k, key(i).as_slice(), "range from key {i}");
                }
            }
            None => panic!("a range from key {i} is empty"),
        }
    }

    // A batched read answers every key, or errors.
    let keys: Vec<Vec<u8>> = (0..ROWS).map(key).collect();
    if let Ok(values) = tree.multi_get(&keys, SeqNo::MAX) {
        assert_eq!(values.len(), keys.len(), "multi_get result count");
        for (i, (v, expected)) in values.iter().zip((0..ROWS).map(value)).enumerate() {
            assert_eq!(v.as_deref(), Some(expected.as_slice()), "multi_get key {i}");
        }
    }

    // The offline verifier reports the damage.
    let report = lsm_tree::verify::verify_block_checksums(tree);
    assert!(
        !report.errors.is_empty(),
        "verify must report the misplaced blocks: {report:?}"
    );

    // Compaction streams the table without its index and must refuse too,
    // rather than write the rows out of order.
    assert!(
        tree.major_compact(u64::MAX, SeqNo::MAX).is_err(),
        "compaction must refuse the misplaced blocks"
    );
}

#[test]
fn a_data_block_moved_to_another_blocks_place_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let tree = misplaced_tree(dir.path(), |c| c);
    assert_refused(&tree);
}

/// A batch too large for a tiny cache goes through the chunked resolver,
/// which reads blocks into a scratch and point-reads them without the cache.
#[test]
fn a_chunked_multi_get_with_a_misplaced_block_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let tree = misplaced_tree(dir.path(), |c| {
        c.use_cache(std::sync::Arc::new(lsm_tree::Cache::with_capacity_bytes(
            16 * 1024,
        )))
    });
    let keys: Vec<Vec<u8>> = (0..ROWS).chain(0..ROWS).map(key).collect();
    assert!(keys.len() > 512, "the batch must take the chunked path");
    if let Ok(values) = tree.multi_get(&keys, SeqNo::MAX) {
        assert_eq!(values.len(), keys.len(), "multi_get result count");
        let expected = (0..ROWS).chain(0..ROWS).map(value);
        for (i, (v, expected)) in values.iter().zip(expected).enumerate() {
            assert_eq!(
                v.as_deref(),
                Some(expected.as_slice()),
                "multi_get position {i}"
            );
        }
    }
}

#[cfg(feature = "encryption")]
#[test]
fn an_encrypted_data_block_moved_to_another_blocks_place_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let key = [0x42; 32];
    let tree = misplaced_tree(dir.path(), |c| {
        c.with_encryption(Some(std::sync::Arc::new(lsm_tree::Aes256GcmProvider::new(
            &key,
        ))))
    });
    assert_refused(&tree);
}

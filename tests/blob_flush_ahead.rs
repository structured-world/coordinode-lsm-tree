// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A flush compresses its separated values on the compaction pool, under the
//! codec the blob file holds when each is handed over, while the file's codec
//! moves under them: its own dictionary is built from its first values, so
//! the values in flight then were compressed without it. The flush must write
//! every value as the file's codec has it when the value is written, so a
//! flush on the pool reads back as one without it and takes the same bytes.

#![cfg(feature = "zstd")]
#![expect(clippy::expect_used, reason = "test assertions")]

use lsm_tree::table::writer::CompactionSpawner;
use lsm_tree::{
    AbstractTree, AnyTree, CompressionType, Config, KvSeparationOptions, SeqNo,
    SequenceNumberCounter,
};
use std::sync::Arc;

/// Runs every task on a thread of its own, so the values in flight compress
/// beside the flush.
struct ThreadPerTask;

impl CompactionSpawner for ThreadPerTask {
    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        std::thread::spawn(task);
    }
}

/// One instant for every tree, so two blob files of the same values differ
/// only where their writes did.
struct Stopped;

impl lsm_tree::Clock for Stopped {
    fn unix_time(&self) -> core::time::Duration {
        core::time::Duration::from_secs(1_800_000_000)
    }
}

fn key(i: u32) -> String {
    format!("key_{i:08}")
}

/// The value of `key(i)`: mostly small values that pack under the file's
/// dictionary, every 97th one large enough for a frame of its own, each
/// different so no two compress alike.
fn value(i: u32) -> Vec<u8> {
    let len = if i.is_multiple_of(97) {
        20_000
    } else {
        200 + (i % 120) as usize
    };
    let word = format!("value {i} of the flush, ");
    word.bytes().cycle().take(len).collect()
}

/// A tree flushed once with the values above, tombstones and values kept in
/// the table among them, its separated values compressed on a pool of its own
/// when `pool`, and on the flush's thread otherwise: a tree of one compaction
/// thread is given no pool.
fn flushed(pool: bool) -> lsm_tree::Result<(tempfile::TempDir, AnyTree)> {
    let folder = tempfile::tempdir().expect("tempdir");
    let mut config = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_clock(Arc::new(Stopped))
    .blob_compression(CompressionType::Zstd(3))
    .with_kv_separation(Some(
        KvSeparationOptions::default()
            .separation_threshold(64)
            .file_dictionary(16 * 1_024),
    ));
    config = if pool {
        config
            .compaction_threads(4)
            .compaction_pool(Some(Arc::new(ThreadPerTask) as Arc<dyn CompactionSpawner>))
    } else {
        config.compaction_threads(1)
    };
    let tree = config.open()?;
    for i in 0..5_000 {
        match i % 50 {
            // A tombstone and a value too small to separate between the
            // separated ones: each has to follow the values before it.
            7 => {
                tree.remove(key(i), SeqNo::from(i));
            }
            13 => {
                tree.insert(key(i), b"short", SeqNo::from(i));
            }
            _ => {
                tree.insert(key(i), value(i), SeqNo::from(i));
            }
        }
    }
    tree.flush_active_memtable(0)?;
    Ok((folder, tree))
}

/// Every blob file of the tree at `folder`, by name, whole.
fn blob_files(folder: &std::path::Path) -> Vec<(std::ffi::OsString, Vec<u8>)> {
    let mut files: Vec<_> = std::fs::read_dir(folder.join("blobs"))
        .expect("blob folder")
        .map(|entry| {
            let entry = entry.expect("entry");
            (
                entry.file_name(),
                std::fs::read(entry.path()).expect("read"),
            )
        })
        .collect();
    files.sort();
    files
}

#[test]
fn values_compressed_on_the_pool_read_back_as_written() -> lsm_tree::Result<()> {
    let (_folder, tree) = flushed(true)?;
    for i in 0..5_000 {
        let got = tree.get(key(i), SeqNo::MAX)?;
        let want = match i % 50 {
            7 => None,
            13 => Some(b"short".to_vec()),
            _ => Some(value(i)),
        };
        assert_eq!(got.as_deref(), want.as_deref(), "key {i}");
    }
    Ok(())
}

#[test]
fn a_flush_on_the_pool_takes_the_bytes_of_one_without() -> lsm_tree::Result<()> {
    let (on_pool, _tree) = flushed(true)?;
    let (inline, _other) = flushed(false)?;
    assert!(
        blob_files(on_pool.path()) == blob_files(inline.path()),
        "a value compressed ahead under a codec its file no longer had was \
         written as compressed, not compressed again",
    );
    Ok(())
}

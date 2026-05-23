// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Integration tests for the meta-block mirror (TAIL + MID copies,
//! see writer/mod.rs `write_meta_section`). A single bit-flip or
//! torn-write that takes out one copy must not prevent table open —
//! the surviving copy is used.

use lsm_tree::{AbstractTree, Config, SequenceNumberCounter, get_tmp_folder};
use std::{fs::OpenOptions, io::Write, path::Path};
use test_log::test;

fn find_table_file(dir: &Path) -> std::path::PathBuf {
    let tables_dir = dir.join("tables");
    let search_dir = if tables_dir.exists() {
        tables_dir
    } else {
        dir.to_path_buf()
    };
    for entry in std::fs::read_dir(&search_dir).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() {
            return entry.path();
        }
    }
    panic!("no table file found in {}", search_dir.display());
}

/// Reads the SFA TOC of `path` and returns `(offset, length)` of the
/// named section, or `None` if the section is absent.
fn locate_section(path: &Path, name: &[u8]) -> Option<(u64, u64)> {
    let mut file = std::fs::File::open(path).unwrap();
    let reader = sfa::Reader::from_reader(&mut file).unwrap();
    reader
        .toc()
        .section(name)
        .map(|entry| (entry.pos(), entry.len()))
}

/// Overwrites `len` bytes at `offset` in `path` with the given byte.
/// Used to simulate localised corruption (bad sector, bit flip storm).
fn corrupt_region(path: &Path, offset: u64, len: u64, byte: u8) {
    use std::io::Seek;
    let mut file = OpenOptions::new().write(true).open(path).unwrap();
    file.seek(std::io::SeekFrom::Start(offset)).unwrap();
    let buf = vec![byte; len as usize];
    file.write_all(&buf).unwrap();
    file.sync_all().unwrap();
}

fn build_tree_with_items(items: usize) -> (tempfile::TempDir, std::path::PathBuf) {
    let folder = get_tmp_folder();
    let dir = tempfile::TempDir::new_in(folder).unwrap();

    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .unwrap();

    for i in 0..items {
        tree.insert(format!("key-{i:06}"), b"value", 0);
    }
    tree.flush_active_memtable(0).unwrap();

    let sst = find_table_file(dir.path());
    (dir, sst)
}

#[test]
fn meta_mirror_writer_emits_both_copies() {
    let (_dir, sst) = build_tree_with_items(64);

    let tail = locate_section(&sst, b"meta");
    let mid = locate_section(&sst, b"meta_mid");

    assert!(tail.is_some(), "TAIL meta section must be present");
    assert!(
        mid.is_some(),
        "MID meta_mid section must be present (mirror)"
    );

    let (tail_off, _) = tail.unwrap();
    let (mid_off, _) = mid.unwrap();
    // MID is written before TAIL — its offset must be smaller, and
    // there must be at least one 4 KiB filesystem sector between
    // them (meta_separator section enforces this).
    assert!(
        mid_off < tail_off,
        "MID at offset {mid_off} should come before TAIL at {tail_off}"
    );
    assert!(
        tail_off - mid_off >= 4096,
        "MID..TAIL gap of {} bytes is below 4 KiB sector separation",
        tail_off - mid_off,
    );
}

#[test]
fn table_reopens_via_mid_when_tail_meta_is_zeroed() {
    let (dir, sst) = build_tree_with_items(64);

    // Wipe the entire TAIL meta region with zeros — XXH3 over zeros
    // will not match the header checksum, so Block::from_file rejects
    // it and the recovery path must fall back to MID.
    let (tail_off, tail_len) = locate_section(&sst, b"meta").expect("TAIL meta missing");
    corrupt_region(&sst, tail_off, tail_len, 0x00);

    // Reopen — should succeed via MID.
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("table open should succeed using MID meta fallback");

    // Sanity: the recovered table is usable.
    let value = tree
        .get(b"key-000000", lsm_tree::MAX_SEQNO)
        .expect("get should succeed on table recovered via MID")
        .expect("key should exist");
    assert_eq!(&*value, b"value");
}

#[test]
fn table_reopens_via_tail_when_mid_meta_is_zeroed() {
    let (dir, sst) = build_tree_with_items(64);

    // Wipe MID — TAIL is still authoritative, so this should be a
    // no-op as far as the open path is concerned (TAIL is tried first
    // and succeeds).
    let (mid_off, mid_len) = locate_section(&sst, b"meta_mid").expect("MID meta missing");
    corrupt_region(&sst, mid_off, mid_len, 0x00);

    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("table open should still succeed when only MID is corrupt");

    let value = tree
        .get(b"key-000000", lsm_tree::MAX_SEQNO)
        .expect("get should succeed")
        .expect("key should exist");
    assert_eq!(&*value, b"value");
}

#[test]
fn table_open_fails_when_both_meta_copies_are_zeroed() {
    let (dir, sst) = build_tree_with_items(64);

    let (tail_off, tail_len) = locate_section(&sst, b"meta").expect("TAIL meta missing");
    let (mid_off, mid_len) = locate_section(&sst, b"meta_mid").expect("MID meta missing");
    corrupt_region(&sst, tail_off, tail_len, 0x00);
    corrupt_region(&sst, mid_off, mid_len, 0x00);

    let result = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open();

    assert!(
        result.is_err(),
        "open must fail when both meta copies are unreadable",
    );
}

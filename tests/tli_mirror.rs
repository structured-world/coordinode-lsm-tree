// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Integration tests for the TLI-block mirror (head + tail copies,
//! see writer/mod.rs `Block::write_into` of `tli_tail`). A single
//! bit-flip or torn-write that takes out one copy must not prevent
//! table open — the surviving copy is used.

use lsm_tree::{
    AbstractTree, Config, SequenceNumberCounter, config::PinningPolicy, get_tmp_folder,
};
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

fn locate_section(path: &Path, name: &[u8]) -> Option<(u64, u64)> {
    let mut file = std::fs::File::open(path).unwrap();
    let reader = lsm_tree::sfa::Reader::from_reader(&mut file).unwrap();
    reader
        .toc()
        .section(name)
        .map(|entry| (entry.pos(), entry.len()))
}

fn corrupt_region(path: &Path, offset: u64, len: u64, byte: u8) {
    use std::io::Seek;
    let mut file = OpenOptions::new().write(true).open(path).unwrap();
    file.seek(std::io::SeekFrom::Start(offset)).unwrap();
    let buf = vec![byte; len as usize];
    file.write_all(&buf).unwrap();
    file.sync_all().unwrap();
}

/// Build a tree forcing partitioned-index for all levels so the TLI
/// section is small (a few entries) AND so the recovery path takes
/// the `read_tli` branch (rather than the volatile-full branch which
/// bypasses `read_tli` entirely).
fn build_partitioned_tree(items: usize) -> (tempfile::TempDir, std::path::PathBuf) {
    let folder = get_tmp_folder();
    let dir = tempfile::TempDir::new_in(folder).unwrap();

    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .index_block_partitioning_policy(PinningPolicy::all(true))
    .open()
    .unwrap();

    for i in 0..items {
        tree.insert(format!("key-{i:06}"), b"value", 0);
    }
    tree.flush_active_memtable(0).unwrap();

    let sst = find_table_file(dir.path());
    (dir, sst)
}

fn reopen_partitioned(dir: &Path) -> lsm_tree::AnyTree {
    Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .index_block_partitioning_policy(PinningPolicy::all(true))
    .open()
    .expect("table open should succeed")
}

#[test]
fn tli_mirror_writer_emits_both_copies() {
    let (_dir, sst) = build_partitioned_tree(2048);

    let head = locate_section(&sst, b"tli");
    let tail = locate_section(&sst, b"tli_tail");

    assert!(head.is_some(), "head tli section must be present");
    assert!(
        tail.is_some(),
        "tail tli_tail section must be present (mirror)"
    );

    let (head_off, _head_len) = head.unwrap();
    let (tail_off, _tail_len) = tail.unwrap();

    // Head is written first (near file start, after the data section);
    // tail copy sits near the file end after meta_separator.
    assert!(
        head_off < tail_off,
        "head tli at {head_off} should come before tail tli_tail at {tail_off}"
    );
}

#[test]
fn tli_tail_sits_after_meta_separator_and_before_meta() {
    let (_dir, sst) = build_partitioned_tree(2048);

    let (sep_off, sep_len) =
        locate_section(&sst, b"meta_separator").expect("meta_separator missing");
    let (tail_off, tail_len) = locate_section(&sst, b"tli_tail").expect("tli_tail missing");
    let (meta_off, _meta_len) = locate_section(&sst, b"meta").expect("meta missing");

    let sep_end = sep_off + sep_len;
    assert!(
        tail_off >= sep_end,
        "tli_tail at {tail_off} must come after meta_separator end {sep_end}"
    );
    let tail_end = tail_off + tail_len;
    assert!(
        tail_end <= meta_off,
        "tli_tail must end (at {tail_end}) before TAIL meta starts (at {meta_off})"
    );
}

#[test]
fn table_reopens_via_head_when_tail_tli_is_zeroed() {
    let (dir, sst) = build_partitioned_tree(2048);

    // Wipe the tail TLI copy. XXH3 over zeros won't match the header
    // checksum, so Block::from_file rejects it and the recovery path
    // must fall back to the head copy.
    let (tail_off, tail_len) = locate_section(&sst, b"tli_tail").expect("tail tli missing");
    corrupt_region(&sst, tail_off, tail_len, 0x00);

    let tree = reopen_partitioned(dir.path());

    let value = tree
        .get(b"key-000000", lsm_tree::MAX_SEQNO)
        .expect("get should succeed on table recovered via head TLI")
        .expect("key should exist");
    assert_eq!(&*value, b"value");
}

#[test]
fn table_reopens_via_tail_when_head_tli_is_zeroed() {
    let (dir, sst) = build_partitioned_tree(2048);

    // Wipe the head TLI copy. Reader tries tail first, so this is the
    // path that exercises the tail-first / head-fallback inversion:
    // head is broken, tail must carry the table.
    let (head_off, head_len) = locate_section(&sst, b"tli").expect("head tli missing");
    corrupt_region(&sst, head_off, head_len, 0x00);

    let tree = reopen_partitioned(dir.path());

    let value = tree
        .get(b"key-000000", lsm_tree::MAX_SEQNO)
        .expect("get should succeed on table recovered via tail TLI")
        .expect("key should exist");
    assert_eq!(&*value, b"value");
}

#[test]
fn table_open_fails_when_both_tli_copies_are_zeroed() {
    let (dir, sst) = build_partitioned_tree(2048);

    let (head_off, head_len) = locate_section(&sst, b"tli").expect("head tli missing");
    let (tail_off, tail_len) = locate_section(&sst, b"tli_tail").expect("tail tli missing");
    corrupt_region(&sst, head_off, head_len, 0x00);
    corrupt_region(&sst, tail_off, tail_len, 0x00);

    let result = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .index_block_partitioning_policy(PinningPolicy::all(true))
    .open();

    assert!(
        result.is_err(),
        "open must fail when both TLI copies are unreadable"
    );
}

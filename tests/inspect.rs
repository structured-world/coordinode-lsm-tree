// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Regression coverage for `lsm_tree::inspect::read_table_properties`'s
//! TAIL-first / MID-fallback recovery path. Companion to
//! `tests/meta_mirror.rs`, which covers the same recovery shape on the
//! live `Tree::recover` open path. Both code paths share the same
//! contract: a corrupted tail `meta` section must transparently fall
//! back to the mid mirror, and a corrupted mid section must be a no-op
//! when the tail is intact. Both copies failing must surface a hard
//! error.

use lsm_tree::{
    AbstractTree, Config, SequenceNumberCounter, get_tmp_folder, inspect::read_table_properties,
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

fn build_tree_with_items(items: u64) -> (tempfile::TempDir, std::path::PathBuf) {
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
fn read_table_properties_uses_tail_when_mid_corrupted() {
    let (_dir, sst) = build_tree_with_items(64);

    // Wipe MID. Reader tries TAIL first and succeeds, so this is
    // effectively a no-op for `read_table_properties` — same property
    // as `Table::recover` (see `tests/meta_mirror.rs`).
    let (mid_off, mid_len) = locate_section(&sst, b"meta_mid").expect("MID meta missing");
    corrupt_region(&sst, mid_off, mid_len, 0x00);

    let props = read_table_properties(&sst)
        .expect("reader must succeed when only MID is corrupt and TAIL is intact");
    assert_eq!(props.item_count, 64);
    assert!(
        !props.min_key.is_empty(),
        "min_key must be populated from the surviving meta block",
    );
}

#[test]
fn read_table_properties_falls_back_to_mid_when_tail_corrupted() {
    let (_dir, sst) = build_tree_with_items(64);

    // Wipe the entire TAIL meta region. The reader's first attempt
    // (decode `meta`) must fail on XXH3 mismatch, then transparently
    // fall back to the MID mirror.
    let (tail_off, tail_len) = locate_section(&sst, b"meta").expect("TAIL meta missing");
    corrupt_region(&sst, tail_off, tail_len, 0x00);

    let props = read_table_properties(&sst)
        .expect("reader must fall back to MID meta when TAIL is corrupted");
    assert_eq!(
        props.item_count, 64,
        "MID copy carries identical metadata to TAIL by writer contract — \
         both are populated from the same in-memory snapshot",
    );
    assert!(
        !props.min_key.is_empty(),
        "min_key must be populated from the MID mirror",
    );
}

#[test]
fn read_table_properties_errors_when_both_meta_copies_zeroed() {
    let (_dir, sst) = build_tree_with_items(64);

    // Wipe both copies. The reader must surface a hard error,
    // returning the original TAIL failure (per the inspect module
    // docs); the MID failure is dropped silently.
    let (tail_off, tail_len) = locate_section(&sst, b"meta").expect("TAIL meta missing");
    let (mid_off, mid_len) = locate_section(&sst, b"meta_mid").expect("MID meta missing");
    corrupt_region(&sst, tail_off, tail_len, 0x00);
    corrupt_region(&sst, mid_off, mid_len, 0x00);

    let err = read_table_properties(&sst)
        .expect_err("reader must fail when both meta copies are unreadable");
    // The exact variant depends on which decode step trips first
    // (XXH3 of the header, structural mismatch, or downstream
    // ParsedMeta parse). The behaviour under test is "the call
    // returns Err", not the specific variant; an exhaustive variant
    // match would couple this regression test to internal error
    // routing and break the next time the error enum changes.
    let _ = err;
}

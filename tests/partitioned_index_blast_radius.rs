// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Integration test for partitioned-index blast-radius isolation.
//!
//! When the index is partitioned, each sub-index block carries its own
//! header / checksum. A bit-flip inside one sub-index partition must
//! make ONLY that partition's data unreachable; reads against keys
//! covered by OTHER sub-index partitions must still succeed.
//!
//! This is the test #296's acceptance matrix deferred and that
//! justifies flipping the partitioned-index default ON for every
//! level: blast radius shrinks from "whole SST unreadable" to "one
//! partition unreadable".

use lsm_tree::{
    AbstractTree, Config, SequenceNumberCounter,
    config::{BlockSizePolicy, PinningPolicy},
    get_tmp_folder,
    inspect::{IndexEntry, read_top_level_index_entries},
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

fn corrupt_region(path: &Path, offset: u64, len: u64, byte: u8) {
    use std::io::Seek;
    let mut file = OpenOptions::new().write(true).open(path).unwrap();
    file.seek(std::io::SeekFrom::Start(offset)).unwrap();
    let buf = vec![byte; len as usize];
    file.write_all(&buf).unwrap();
    file.sync_all().unwrap();
}

fn key_for(i: usize) -> String {
    format!("key-{i:08}")
}

/// Builds a single-SST tree with a partitioned index that has many
/// sub-index partitions, returns (dir, sst_path, total_items).
fn build_partitioned_tree() -> (tempfile::TempDir, std::path::PathBuf, usize) {
    let folder = get_tmp_folder();
    let dir = tempfile::TempDir::new_in(folder).unwrap();

    // Small data blocks → many data blocks → many index handles →
    // many sub-index partitions (the partition-size budget is the
    // hardcoded 4 KiB in the writer; we shrink the data block side
    // to fit dozens of partitions in a reasonable corpus).
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .index_block_partitioning_policy(PinningPolicy::all(true))
    .data_block_size_policy(BlockSizePolicy::all(256))
    .open()
    .unwrap();

    let n = 30_000;
    for i in 0..n {
        tree.insert(key_for(i), b"value", 0);
    }
    tree.flush_active_memtable(0).unwrap();

    let sst = find_table_file(dir.path());
    (dir, sst, n)
}

fn reopen_partitioned(dir: &Path) -> lsm_tree::AnyTree {
    Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .index_block_partitioning_policy(PinningPolicy::all(true))
    .data_block_size_policy(BlockSizePolicy::all(256))
    .open()
    .expect("table reopen should succeed")
}

#[test]
fn partitioned_index_corrupting_one_sub_block_only_affects_its_keys() {
    let (dir, sst, n) = build_partitioned_tree();

    let tli: Vec<IndexEntry> = read_top_level_index_entries(&sst).expect("read tli");
    assert!(
        tli.len() >= 3,
        "test expects multiple sub-index partitions (got {})",
        tli.len()
    );

    // Pick a middle sub-index partition — neither first nor last — so
    // we exercise the case where a partition with intact neighbours on
    // both sides is corrupted.
    let victim_idx = tli.len() / 2;
    let victim = &tli[victim_idx];
    let safe_low = &tli[victim_idx - 1];
    let safe_high = &tli[victim_idx + 1];

    // Establish coverage anchors before corruption: the TLI entry's
    // `end_key` is the LAST user-key covered by the pointed-at block,
    // so it is the most-likely-to-route-here key for that partition.
    let victim_last_key = victim.end_key.clone();
    let safe_low_last_key = safe_low.end_key.clone();
    let safe_high_last_key = safe_high.end_key.clone();

    // Sanity: the three keys are distinct.
    assert_ne!(victim_last_key, safe_low_last_key);
    assert_ne!(victim_last_key, safe_high_last_key);

    // Zero the victim sub-index block's bytes. `size` already
    // includes the BlockHeader prefix, so this nukes both header and
    // payload — header checksum will not match XXH3(zeros).
    corrupt_region(&sst, victim.offset, u64::from(victim.size), 0x00);

    let tree = reopen_partitioned(dir.path());

    // Keys whose data block is referenced by an UN-CORRUPTED sub-index
    // partition must still be readable. Use the partition's own
    // `end_key` so the read path routes to that intact partition.
    let v_low = tree
        .get(&safe_low_last_key, lsm_tree::MAX_SEQNO)
        .expect("read against intact lower partition must succeed");
    assert_eq!(
        v_low.as_deref(),
        Some(&b"value"[..]),
        "intact lower partition: expected value, got {v_low:?}"
    );

    let v_high = tree
        .get(&safe_high_last_key, lsm_tree::MAX_SEQNO)
        .expect("read against intact upper partition must succeed");
    assert_eq!(
        v_high.as_deref(),
        Some(&b"value"[..]),
        "intact upper partition: expected value, got {v_high:?}"
    );

    // The read that routes through the corrupted sub-index partition
    // must surface the corruption as an error, NOT silently return
    // None / wrong data. With the corruption method we use (the whole
    // sub-index block zeroed, header + payload), `Header::decode_from`
    // hits the `MAGIC_BYTES` check first and returns
    // `Error::InvalidHeader("Block")` deterministically — the
    // following XXH3 header-checksum comparison never runs because
    // magic mismatch short-circuits the decode. So the test pins the
    // single expected variant; a generic `is_err()` would also accept
    // unrelated failures (I/O error mid-test, an unrelated
    // `Unrecoverable`, etc.) and weaken the blast-radius assertion.
    let v_victim = tree.get(&victim_last_key, lsm_tree::MAX_SEQNO);
    assert!(
        matches!(v_victim, Err(lsm_tree::Error::InvalidHeader("Block"))),
        "read against corrupted sub-index partition must surface as block-header InvalidHeader(\"Block\") (zeroed magic short-circuits before XXH3 check); got {v_victim:?}"
    );

    // Sanity: very-early and very-late keys (covered by partitions
    // far from the victim) read OK — corruption is not collateral.
    let first = tree
        .get(key_for(0).as_bytes(), lsm_tree::MAX_SEQNO)
        .expect("first key read must succeed");
    assert_eq!(first.as_deref(), Some(&b"value"[..]));

    let last = tree
        .get(key_for(n - 1).as_bytes(), lsm_tree::MAX_SEQNO)
        .expect("last key read must succeed");
    assert_eq!(last.as_deref(), Some(&b"value"[..]));
}

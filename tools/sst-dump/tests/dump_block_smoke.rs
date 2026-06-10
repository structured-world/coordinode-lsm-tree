// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! End-to-end smoke test for the `dump-block` forensic subcommand:
//! build a real ENCRYPTED SST via `lsm-tree`, then drive `sst-dump
//! dump-block` against the first block and confirm it prints the
//! AAD-bound metadata WITHOUT the key. Also confirms a non-encrypted
//! block is reported as such (no MetadataFrame envelope).

use lsm_tree::{
    AbstractTree, Aes256GcmProvider, Config, SequenceNumberCounter, compression::CompressionType,
    config::CompressionPolicy,
};
use std::process::Command;
use std::sync::Arc;

const SST_DUMP_BIN: &str = env!("CARGO_BIN_EXE_sst-dump");

fn first_sst(dir: &std::path::Path) -> std::path::PathBuf {
    let tables = dir.join("tables");
    std::fs::read_dir(&tables)
        .expect("tables dir")
        .filter_map(Result::ok)
        .find(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
        .expect("at least one SST")
        .path()
}

fn populate(tree: &impl AbstractTree) {
    for i in 0u64..200 {
        tree.insert(format!("key-{i:06}"), format!("value-{i}"), 1 + i);
    }
    tree.flush_active_memtable(0).expect("flush");
}

/// Encrypted SST: uncompressed blocks sealed via the AAD-bound block path
/// (`with_encryption`), so each block's payload is a `MetadataFrame ‖ BodyFrame`
/// envelope the forensic parser can read key-free.
fn build_encrypted_sst() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let key = [0x42u8; 32];
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
    .with_encryption(Some(Arc::new(Aes256GcmProvider::new(&key))))
    .open()
    .expect("open encrypted tree");
    populate(&tree);
    drop(tree);
    let sst = first_sst(dir.path());
    (dir, sst)
}

fn build_plain_sst() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
    .open()
    .expect("open tree");
    populate(&tree);
    drop(tree);
    let sst = first_sst(dir.path());
    (dir, sst)
}

#[test]
fn dump_block_on_encrypted_block_prints_metadata_key_free() {
    let (_dir, sst) = build_encrypted_sst();

    // Offset 0 is the first block's `Header`; for an encrypted SST its
    // payload is the AAD-bound envelope. No key is passed to the tool.
    let out = Command::new(SST_DUMP_BIN)
        .arg(&sst)
        .arg("dump-block")
        .arg("0")
        .output()
        .expect("spawn sst-dump");

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "expected exit 0; stdout:\n{stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(stdout.contains("encrypted: true"), "got:\n{stdout}");
    assert!(stdout.contains("format_version: 1"), "got:\n{stdout}");
    assert!(stdout.contains("suite: Aes256Gcm"), "got:\n{stdout}");
    // Nonce + tag are dumped as quoted hex.
    assert!(stdout.contains("nonce: \""), "got:\n{stdout}");
    assert!(stdout.contains("aead_tag: \""), "got:\n{stdout}");
    // tree_id / table_id are not on disk and were not supplied.
    assert!(stdout.contains("tree_id: null"), "got:\n{stdout}");
}

#[test]
fn dump_block_echoes_caller_supplied_ids() {
    let (_dir, sst) = build_encrypted_sst();
    let out = Command::new(SST_DUMP_BIN)
        .arg(&sst)
        .arg("dump-block")
        .arg("0")
        .arg("--tree-id")
        .arg("7")
        .arg("--table-id")
        .arg("42")
        .output()
        .expect("spawn sst-dump");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "expected exit 0; got:\n{stdout}");
    assert!(stdout.contains("tree_id: 7"), "got:\n{stdout}");
    assert!(stdout.contains("table_id: 42"), "got:\n{stdout}");
}

#[test]
fn dump_block_on_plain_block_reports_not_encrypted() {
    let (_dir, sst) = build_plain_sst();
    let out = Command::new(SST_DUMP_BIN)
        .arg(&sst)
        .arg("dump-block")
        .arg("0")
        .output()
        .expect("spawn sst-dump");

    assert!(
        !out.status.success(),
        "expected non-zero exit on a non-encrypted block",
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("not an AAD-bound encrypted block"),
        "expected the not-encrypted notice; got:\n{stderr}",
    );
}

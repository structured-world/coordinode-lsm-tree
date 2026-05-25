// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! End-to-end smoke test for the `index-dump` subcommand: build a
//! real SST, then drive `sst-dump index-dump` against it and check
//! the structure of the printed table.

use lsm_tree::{
    AbstractTree, Config, SequenceNumberCounter, compression::CompressionType,
    config::CompressionPolicy,
};
use std::process::Command;

const SST_DUMP_BIN: &str = env!("CARGO_BIN_EXE_sst-dump");

fn build_one_sst(item_count: u64) -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
    .open()
    .expect("open tree");

    for i in 0..item_count {
        tree.insert(format!("key-{i:06}"), format!("value-{i}"), 1 + i);
    }
    tree.flush_active_memtable(0).expect("flush");
    drop(tree);

    let tables = dir.path().join("tables");
    let sst = std::fs::read_dir(&tables)
        .expect("tables dir")
        .filter_map(Result::ok)
        .find(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
        .expect("at least one SST")
        .path();
    (dir, sst)
}

fn line_value(text: &str, label: &str) -> Option<String> {
    let prefix = format!("{label}:");
    text.lines().find_map(|line| {
        line.strip_prefix(&prefix)
            .map(|rest| rest.trim().to_owned())
    })
}

#[test]
fn index_dump_prints_header_row_and_entries() {
    let (_dir, sst) = build_one_sst(200);

    let out = Command::new(SST_DUMP_BIN)
        .arg(&sst)
        .arg("index-dump")
        .output()
        .expect("spawn sst-dump");

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "expected exit 0, got {:?}; stdout:\n{stdout}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr),
    );

    // Entry count line is present and matches a non-zero parse.
    let entry_count: u64 = line_value(&stdout, "tli_entry_count")
        .and_then(|s| s.parse().ok())
        .expect("tli_entry_count parses");
    assert!(
        entry_count >= 1,
        "expected >= 1 TLI entries (single-block SSTs still emit one row); got {entry_count}",
    );

    // Header row of the table is present.
    assert!(
        stdout.contains("#"),
        "expected header row in output; got:\n{stdout}",
    );
    assert!(
        stdout.contains("end_key"),
        "expected `end_key` column header; got:\n{stdout}",
    );

    // The last entry's end_key must be the largest key we inserted
    // ("key-000199"), because TLI is sorted and the final row's
    // end_key covers the highest key in the SST. Use the quoted
    // form to anchor against the format_key wrapping.
    assert!(
        stdout.contains("\"key-000199\""),
        "expected the largest inserted key 'key-000199' in output; got:\n{stdout}",
    );
}

#[test]
fn index_dump_fails_on_missing_file() {
    let bogus = tempfile::tempdir().expect("tempdir");
    let nonexistent = bogus.path().join("does-not-exist");

    let out = Command::new(SST_DUMP_BIN)
        .arg(&nonexistent)
        .arg("index-dump")
        .output()
        .expect("spawn sst-dump");

    assert!(
        !out.status.success(),
        "expected non-zero exit on missing file",
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("error:"),
        "expected `error:` line in stderr; got:\n{stderr}",
    );
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! End-to-end smoke test for the `dump` subcommand: build a real
//! SST, then drive `sst-dump dump` against it under various
//! `--from` / `--to` / `--max` / `--keys-only` combinations and
//! check the line shape + counts.

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

#[test]
fn dump_emits_one_line_per_entry_with_key_equals_value() {
    const ITEM_COUNT: u64 = 50;
    let (_dir, sst) = build_one_sst(ITEM_COUNT);

    let out = Command::new(SST_DUMP_BIN)
        .arg(&sst)
        .arg("dump")
        .output()
        .expect("spawn sst-dump");

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "expected exit 0, got {:?}; stdout:\n{stdout}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr),
    );

    // 50 lines, one per entry. Use `count` (not `len`) so a trailing
    // empty line doesn't inflate the assertion.
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(
        lines.len() as u64,
        ITEM_COUNT,
        "expected {ITEM_COUNT} output lines; got {}: \n{stdout}",
        lines.len(),
    );

    // Each line has the shape `"key"="value"`. Check the first and
    // last to confirm both the format and the sort order (smallest
    // key first, largest last).
    assert_eq!(lines[0], r#""key-000000"="value-0""#);
    assert_eq!(lines[49], r#""key-000049"="value-49""#);
}

#[test]
fn dump_honours_from_to_bounds() {
    let (_dir, sst) = build_one_sst(100);

    let out = Command::new(SST_DUMP_BIN)
        .arg(&sst)
        .arg("dump")
        .arg("--from")
        .arg("key-000010")
        .arg("--to")
        .arg("key-000020")
        .output()
        .expect("spawn sst-dump");

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "expected exit 0; got:\n{stdout}");

    // Range is `[from, to)` — inclusive lower, exclusive upper. So
    // keys 10..20 = 10 entries. Crucial: this is the "standard Rust
    // range" semantic the CLI doc promises, NOT the "fully-closed
    // bounds" semantic some other tools use.
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(
        lines.len(),
        10,
        "expected 10 entries in [key-000010, key-000020); got {}",
        lines.len(),
    );
    assert_eq!(lines[0], r#""key-000010"="value-10""#);
    assert_eq!(lines[9], r#""key-000019"="value-19""#);
}

#[test]
fn dump_honours_max_cap() {
    let (_dir, sst) = build_one_sst(100);

    let out = Command::new(SST_DUMP_BIN)
        .arg(&sst)
        .arg("dump")
        .arg("--max")
        .arg("7")
        .output()
        .expect("spawn sst-dump");

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success());
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 7, "expected exactly 7 lines under --max=7");
    assert_eq!(lines[0], r#""key-000000"="value-0""#);
    assert_eq!(lines[6], r#""key-000006"="value-6""#);
}

#[test]
fn dump_keys_only_omits_value_column() {
    let (_dir, sst) = build_one_sst(5);

    let out = Command::new(SST_DUMP_BIN)
        .arg(&sst)
        .arg("dump")
        .arg("--keys-only")
        .output()
        .expect("spawn sst-dump");

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success());
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 5);
    // No `=` separator anywhere on the line under --keys-only.
    for line in &lines {
        assert!(
            !line.contains('='),
            "--keys-only line should not contain `=`; got: {line}",
        );
    }
    // Sort order check.
    assert_eq!(lines[0], r#""key-000000""#);
    assert_eq!(lines[4], r#""key-000004""#);
}

#[test]
fn dump_fails_on_missing_file() {
    let bogus = tempfile::tempdir().expect("tempdir");
    let nonexistent = bogus.path().join("does-not-exist");

    let out = Command::new(SST_DUMP_BIN)
        .arg(&nonexistent)
        .arg("dump")
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

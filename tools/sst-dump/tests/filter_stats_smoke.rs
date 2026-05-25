// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! End-to-end smoke test for the `filter-stats` subcommand: build a
//! real SST with a full BuRR filter, then drive `sst-dump
//! filter-stats` against it and check the printed metrics.

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
fn filter_stats_prints_expected_fields() {
    const ITEM_COUNT: u64 = 200;
    let (_dir, sst) = build_one_sst(ITEM_COUNT);

    let out = Command::new(SST_DUMP_BIN)
        .arg(&sst)
        .arg("filter-stats")
        .output()
        .expect("spawn sst-dump");

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "expected exit 0, got {:?}; stdout:\n{stdout}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr),
    );

    // Default Config builds a full BuRR filter for this fixture
    // (filter policy is on by default; we did not call any
    // filter-disabling builder). So all four fields must be
    // populated with non-zero values.
    assert_eq!(
        line_value(&stdout, "item_count"),
        Some(ITEM_COUNT.to_string()),
        "expected item_count={ITEM_COUNT}; got:\n{stdout}",
    );

    let filter_bytes: u64 = line_value(&stdout, "filter_section_size")
        .and_then(|s| s.strip_suffix(" bytes").map(str::to_owned))
        .and_then(|s| s.parse().ok())
        .expect("filter_section_size parses as `<n> bytes`");
    assert!(
        filter_bytes > 0,
        "expected filter_section_size > 0 (default filter is on); got 0",
    );

    let layers: u64 = line_value(&stdout, "layer_count")
        .and_then(|s| s.parse().ok())
        .expect("layer_count parses");
    assert!(layers >= 1, "expected at least 1 BuRR layer; got {layers}",);

    // bits_per_key prints to three decimals; just check it parses
    // and is positive.
    let bpk: f64 = line_value(&stdout, "bits_per_key")
        .and_then(|s| s.parse().ok())
        .expect("bits_per_key parses as float");
    assert!(bpk > 0.0, "expected bits_per_key > 0; got {bpk}");
}

#[test]
fn filter_stats_fails_on_missing_file() {
    let bogus = tempfile::tempdir().expect("tempdir");
    let nonexistent = bogus.path().join("does-not-exist");

    let out = Command::new(SST_DUMP_BIN)
        .arg(&nonexistent)
        .arg("filter-stats")
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

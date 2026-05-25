// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! End-to-end smoke test for the `filter-stats` subcommand: build a
//! real SST with a full BuRR filter, then drive `sst-dump
//! filter-stats` against it and check the printed metrics.

use lsm_tree::{
    AbstractTree, Config, SequenceNumberCounter,
    compression::CompressionType,
    config::{
        BloomConstructionPolicy, CompressionPolicy, FilterPolicy, FilterPolicyEntry, PinningPolicy,
    },
};
use std::process::Command;

const SST_DUMP_BIN: &str = env!("CARGO_BIN_EXE_sst-dump");

fn build_one_sst(item_count: u64) -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    // Pin the filter policy explicitly instead of relying on the
    // crate's default. The default ships with BuRR-on-every-level
    // today, but a future refactor (default tweak, level-0 carve-out,
    // dynamic policy) could turn filters off for the level our
    // single-flush SST lands in, and this test would then fail for
    // reasons unrelated to filter-stats itself. Explicit pin keeps
    // the smoke test stable.
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
    .filter_policy(FilterPolicy::all(FilterPolicyEntry::Bloom(
        BloomConstructionPolicy::BitsPerKey(10.0),
    )))
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
fn filter_stats_rejects_partitioned_filter_with_not_supported_message() {
    // Build an SST with the filter partitioning policy turned on for
    // every level. The writer emits both `filter` and `filter_tli`
    // SFA sections, which is exactly the layout this subcommand
    // refuses to handle today (the partitioned `filter` section is a
    // concatenation of per-partition BuRR payloads, not a single
    // parseable wire buffer).
    let dir = tempfile::tempdir().expect("tempdir");
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
    .filter_policy(FilterPolicy::all(FilterPolicyEntry::Bloom(
        BloomConstructionPolicy::BitsPerKey(10.0),
    )))
    .filter_block_partitioning_policy(PinningPolicy::all(true))
    .open()
    .expect("open tree");

    // Enough keys to actually produce multiple filter partitions:
    // a tiny SST with one partition collapses back to the full case
    // on some configurations.
    for i in 0..2048u64 {
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

    // Sanity-check the on-disk shape before driving the CLI: if the
    // writer didn't actually emit `filter_tli` for this fixture
    // (e.g. because the keyset shrank below the partitioning
    // threshold on some platform), the assertions below would test
    // the wrong code path.
    let has_filter_tli = {
        let mut file = std::fs::File::open(&sst).expect("open sst for toc inspection");
        let reader = sfa::Reader::from_reader(&mut file).expect("sfa trailer parse");
        reader.toc().section(b"filter_tli").is_some()
    };
    assert!(
        has_filter_tli,
        "test fixture must produce a `filter_tli` section; \
         filter partitioning policy may have failed to take effect",
    );

    let out = Command::new(SST_DUMP_BIN)
        .arg(&sst)
        .arg("filter-stats")
        .output()
        .expect("spawn sst-dump");

    assert!(
        !out.status.success(),
        "expected non-zero exit on partitioned-filter SST; got success",
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("not supported"),
        "expected `not supported` in stderr; got:\n{stderr}",
    );
    assert!(
        stderr.contains("filter_tli"),
        "expected error message to name `filter_tli` so operators can confirm; got:\n{stderr}",
    );
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

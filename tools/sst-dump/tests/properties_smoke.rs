// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! End-to-end smoke test for the `properties` subcommand: build a
//! real SST via `lsm-tree`, then drive `sst-dump properties` against
//! it and assert the printed metadata matches what we inserted.

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
fn properties_prints_expected_counts_and_key_range() {
    const ITEM_COUNT: u64 = 200;
    let (_dir, sst) = build_one_sst(ITEM_COUNT);

    let out = Command::new(SST_DUMP_BIN)
        .arg(&sst)
        .arg("properties")
        .output()
        .expect("spawn sst-dump");

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "expected exit 0, got {:?}; stdout:\n{stdout}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr),
    );

    assert_eq!(
        line_value(&stdout, "item_count"),
        Some(ITEM_COUNT.to_string()),
        "expected item_count={ITEM_COUNT}; got:\n{stdout}",
    );
    assert_eq!(
        line_value(&stdout, "tombstone_count"),
        Some("0".to_owned()),
        "expected no tombstones in this fixture; got:\n{stdout}",
    );
    // Compression was forced to None by the test fixture.
    assert_eq!(
        line_value(&stdout, "data_compression"),
        Some("None".to_owned()),
        "expected None compression; got:\n{stdout}",
    );

    // Key range: keys are `key-000000`..`key-000199`. The CLI's
    // `format_key` helper wraps them in double quotes.
    let key_range =
        line_value(&stdout, "key_range").expect("expected key_range line; got:\n{stdout}");
    assert!(
        key_range.contains("min=\"key-000000\""),
        "expected min key 'key-000000' in key_range, got: {key_range}",
    );
    assert!(
        key_range.contains("max=\"key-000199\""),
        "expected max key 'key-000199' in key_range, got: {key_range}",
    );

    // Sanity: data_block_count > 0 and index_block_count >= 1
    // (every non-empty SST emits at least one of each).
    let data_blocks: u64 = line_value(&stdout, "data_block_count")
        .and_then(|s| s.parse().ok())
        .expect("data_block_count parses");
    assert!(data_blocks > 0, "expected data_block_count > 0");
    let index_blocks: u64 = line_value(&stdout, "index_block_count")
        .and_then(|s| s.parse().ok())
        .expect("index_block_count parses");
    assert!(index_blocks >= 1, "expected index_block_count >= 1");

    // `file_size` here is the writer's recorded data-blocks-end
    // offset, NOT the on-disk total (see the docstring on
    // `TableProperties::file_size`). It must be > 0 and strictly less
    // than the actual on-disk file size (which adds index / filter /
    // meta / SFA-trailer sections after the data-blocks region).
    let reported_data_size: u64 = line_value(&stdout, "file_size")
        .and_then(|s| s.strip_suffix(" bytes").map(str::to_owned))
        .and_then(|s| s.parse().ok())
        .expect("file_size parses as `<n> bytes`");
    let on_disk = std::fs::metadata(&sst).unwrap().len();
    assert!(reported_data_size > 0, "expected reported file_size > 0");
    assert!(
        reported_data_size < on_disk,
        "reported data-region size ({reported_data_size}) must be less than \
         on-disk size ({on_disk}); the meta+trailer sections always add bytes",
    );
}

#[test]
fn properties_fails_on_missing_file() {
    let bogus = tempfile::tempdir().expect("tempdir");
    let nonexistent = bogus.path().join("does-not-exist");

    let out = Command::new(SST_DUMP_BIN)
        .arg(&nonexistent)
        .arg("properties")
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

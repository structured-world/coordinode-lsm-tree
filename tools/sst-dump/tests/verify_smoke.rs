// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! End-to-end smoke test: build a real SST via `lsm-tree`, then drive
//! the `sst-dump verify` binary against the file and assert the
//! exit code + output. The clean case must exit 0 with `OK`; a
//! tampered file must exit 1 with `CORRUPT` plus at least one
//! per-block error line.
//!
//! Uses the cargo-built binary at `target/{debug,release}/sst-dump`
//! resolved via `CARGO_BIN_EXE_sst-dump` — the env var cargo
//! provides to integration tests for the crate's own binaries.

use lsm_tree::{
    AbstractTree, Config, SequenceNumberCounter, compression::CompressionType,
    config::CompressionPolicy,
};
use std::process::Command;

const SST_DUMP_BIN: &str = env!("CARGO_BIN_EXE_sst-dump");

/// Builds a small populated SST in a tempdir, flushes it to disk,
/// drops the tree, returns the path to the first SST file.
fn build_one_sst() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
    .open()
    .expect("open tree");

    for i in 0u64..200 {
        tree.insert(format!("key-{i:06}"), format!("value-{i}"), 1 + i);
    }
    tree.flush_active_memtable(0).expect("flush");
    drop(tree);

    // Tables/ subdir, take the first entry — single SST after one flush.
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
fn verify_clean_sst_exits_zero_with_ok() {
    let (_dir, sst) = build_one_sst();

    let out = Command::new(SST_DUMP_BIN)
        .arg(&sst)
        .arg("verify")
        .output()
        .expect("spawn sst-dump");

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "expected exit 0, got {:?}; stdout:\n{stdout}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr),
    );
    // Parse `<label>: <value>` lines by trimming around the colon
    // rather than hard-coding column alignment. Robust against
    // future formatting tweaks in sst-dump's output (column-width
    // changes, switching to tab-separated, etc.).
    assert_eq!(
        line_value(&stdout, "status"),
        Some("OK".to_owned()),
        "expected status=OK in output; got:\n{stdout}",
    );
    assert_eq!(
        line_value(&stdout, "errors"),
        Some("0".to_owned()),
        "expected errors=0 in output; got:\n{stdout}",
    );
}

/// Returns the trimmed value after the first `<label>:` line in
/// `text`, or `None` if no such line exists. Whitespace around the
/// value is normalised so callers don't have to encode the exact
/// column alignment sst-dump uses (which is implementation detail).
fn line_value(text: &str, label: &str) -> Option<String> {
    let prefix = format!("{label}:");
    text.lines().find_map(|line| {
        line.strip_prefix(&prefix)
            .map(|rest| rest.trim().to_owned())
    })
}

#[test]
fn verify_tampered_sst_exits_nonzero_with_corrupt() {
    let (_dir, sst) = build_one_sst();

    // Flip a byte inside the first data block's payload. Offset 64
    // lands inside the plain payload (past the 33-byte Header), so
    // the header's own XXH3 still verifies but the payload XXH3
    // mismatches → DataCorrupted. Bounds-check via `get_mut` so a
    // future writer-layout change that produces a sub-65-byte SST
    // fails the test with a clear message instead of an index panic.
    const TAMPER_OFFSET: usize = 64;
    let mut bytes = std::fs::read(&sst).expect("read sst");
    let len = bytes.len();
    let byte = bytes.get_mut(TAMPER_OFFSET).unwrap_or_else(|| {
        panic!(
            "SST file is {len} bytes long, smaller than tamper offset {TAMPER_OFFSET} — \
             writer layout changed or fixture is too small",
        )
    });
    *byte ^= 0xFF;
    std::fs::write(&sst, &bytes).expect("write tampered sst");

    let out = Command::new(SST_DUMP_BIN)
        .arg(&sst)
        .arg("verify")
        .output()
        .expect("spawn sst-dump");

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        !out.status.success(),
        "expected non-zero exit on corruption; stdout:\n{stdout}",
    );
    assert_eq!(
        line_value(&stdout, "status"),
        Some("CORRUPT".to_owned()),
        "expected status=CORRUPT in output; got:\n{stdout}",
    );
    // The behaviour under test is "corruption is detected and
    // reported", not the specific variant. A future writer-layout
    // change could move the byte-flipped region from a data block
    // into the TOC region or end up surfacing as SstFileUnreadable
    // / TocCorrupted / DataReadError instead — all of those are
    // valid "corruption detected" outcomes. Accept any of the five
    // variant tags sst-dump prints.
    const VARIANT_TAGS: &[&str] = &[
        "SstFileUnreadable",
        "HeaderCorrupted",
        "DataCorrupted",
        "DataReadError",
        "TocCorrupted",
    ];
    let saw_variant = VARIANT_TAGS.iter().any(|tag| stdout.contains(tag));
    assert!(
        saw_variant,
        "expected one of {VARIANT_TAGS:?} in output; got:\n{stdout}",
    );
}

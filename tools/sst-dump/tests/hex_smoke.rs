// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! End-to-end smoke test for the `hex` subcommand: build a real SST
//! via `lsm-tree`, then drive the `sst-dump hex` binary against
//! known offsets and check both the success path (block header
//! decodes, dump body produced) and the documented error paths
//! (offset past EOF, oversized `--len`).

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
fn hex_at_first_block_decodes_header_and_prints_dump() {
    let (_dir, sst) = build_one_sst();

    // Offset 0 is the start of the very first `data` block. The SST
    // writer emits a `Header`-prefixed block here, so `hex 0` should
    // both decode the header successfully and produce a dump body.
    let out = Command::new(SST_DUMP_BIN)
        .arg(&sst)
        .arg("hex")
        .arg("0")
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
        line_value(&stdout, "offset"),
        Some("0 (0x00000000)".to_owned()),
        "expected offset=0 with hex annotation; got:\n{stdout}",
    );

    // Header section: the writer wrote a real block at offset 0, so
    // the decode must succeed (= the section prints `header:` with
    // sub-lines, NOT `header: decode failed`).
    assert!(
        stdout.contains("header:\n"),
        "expected a populated `header:` section; got:\n{stdout}",
    );
    assert!(
        stdout.contains("block_type:"),
        "expected decoded `block_type` line; got:\n{stdout}",
    );
    assert!(
        !stdout.contains("decode failed"),
        "header decode unexpectedly failed at offset 0; got:\n{stdout}",
    );

    // Hex dump body: at least one xxd-style line starting with the
    // 8-digit hex offset.
    assert!(
        stdout.contains("00000000  "),
        "expected an xxd-style line for offset 0; got:\n{stdout}",
    );
}

#[test]
fn hex_with_no_header_flag_skips_decode_section() {
    let (_dir, sst) = build_one_sst();

    let out = Command::new(SST_DUMP_BIN)
        .arg(&sst)
        .arg("hex")
        .arg("0")
        .arg("--no-header")
        .output()
        .expect("spawn sst-dump");

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "expected exit 0; got:\n{stdout}");
    assert!(
        stdout.contains("header:         skipped (--no-header)"),
        "expected the skip-header notice; got:\n{stdout}",
    );
    assert!(
        !stdout.contains("block_type:"),
        "header decode should NOT have run under --no-header; got:\n{stdout}",
    );
}

#[test]
fn hex_past_eof_exits_nonzero() {
    let (_dir, sst) = build_one_sst();
    let file_size = std::fs::metadata(&sst).unwrap().len();

    let past_eof = file_size + 1;
    let out = Command::new(SST_DUMP_BIN)
        .arg(&sst)
        .arg("hex")
        .arg(past_eof.to_string())
        .output()
        .expect("spawn sst-dump");

    assert!(
        !out.status.success(),
        "expected non-zero exit when offset is past EOF; got success",
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("past end of file"),
        "expected past-EOF error in stderr; got:\n{stderr}",
    );
}

#[test]
fn hex_oversized_len_is_rejected() {
    let (_dir, sst) = build_one_sst();

    // Caller-supplied `--len` above the 1 MiB ceiling is a typo
    // guard, not a clamp — the tool should refuse rather than
    // silently shrinking the request.
    let oversized = (1024 * 1024) + 1;
    let out = Command::new(SST_DUMP_BIN)
        .arg(&sst)
        .arg("hex")
        .arg("0")
        .arg("--len")
        .arg(oversized.to_string())
        .output()
        .expect("spawn sst-dump");

    assert!(!out.status.success(), "expected non-zero exit");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("exceeds maximum"),
        "expected len-exceeds-max error in stderr; got:\n{stderr}",
    );
}

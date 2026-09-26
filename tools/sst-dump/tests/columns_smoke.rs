// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! End-to-end smoke test for the `columns` subcommand: build a columnar SST
//! whose columns each suit a different encoding, then drive
//! `sst-dump columns` against it and assert what it reports.

use lsm_tree::{
    AbstractTree, Config, SequenceNumberCounter, compression::CompressionType,
    config::CompressionPolicy,
};
use std::process::Command;

const SST_DUMP_BIN: &str = env!("CARGO_BIN_EXE_sst-dump");

/// One SST of `items` rows under `columnar`: sequential keys, seqnos in a
/// narrow range, and one repeated value.
fn build_one_sst(items: u64, columnar: bool) -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
    .open()
    .expect("open tree");
    let lsm_tree::AnyTree::Standard(standard) = &tree else {
        panic!("a standard tree");
    };
    standard
        .update_runtime_config(|rc| rc.columnar = columnar)
        .expect("runtime config");
    for i in 0..items {
        tree.insert(format!("key-{i:06}"), "the same value", 1 + i);
    }
    tree.flush_active_memtable(0).expect("flush");
    drop(tree);

    let sst = std::fs::read_dir(dir.path().join("tables"))
        .expect("tables dir")
        .filter_map(Result::ok)
        .find(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
        .expect("at least one SST")
        .path();
    (dir, sst)
}

fn run(sst: &std::path::Path, args: &[&str]) -> String {
    let out = Command::new(SST_DUMP_BIN)
        .arg(sst)
        .arg("columns")
        .args(args)
        .output()
        .expect("spawn sst-dump");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        out.status.success(),
        "expected exit 0, got {:?}; stdout:\n{stdout}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr),
    );
    stdout
}

/// The summary reports every engine column, and the encodings the writer
/// chose for these values: a repeated value as a constant, and seqnos in a
/// narrow range by their ordinals, not stored whole.
#[test]
fn columns_reports_each_page_and_what_it_was_encoded_as() {
    let (_dir, sst) = build_one_sst(400, true);

    let pages = run(&sst, &[]);
    assert!(
        pages.starts_with("group row_page column rows bytes expression"),
        "a page listing first; got:\n{pages}",
    );

    let summary = run(&sst, &["--summary"]);
    let lines: Vec<&str> = summary.lines().collect();
    assert_eq!(
        lines.first().copied(),
        Some("column pages rows bytes expression")
    );
    let for_column = |column: u16| -> Vec<String> {
        lines
            .iter()
            .skip(1)
            .filter(|l| l.split(' ').next() == Some(&column.to_string()))
            .map(|l| l.split(' ').skip(4).collect::<Vec<_>>().join(" "))
            .collect()
    };
    for column in 0..4u16 {
        assert!(
            !for_column(column).is_empty(),
            "column {column} is reported; got:\n{summary}",
        );
    }
    assert!(
        for_column(3).iter().all(|e| e == "constant"),
        "one repeated value is a constant on every page; got:\n{summary}",
    );
    assert!(
        for_column(1).iter().all(|e| e.starts_with("ordinals(")),
        "seqnos in a narrow range are encoded by their ordinals; got:\n{summary}",
    );
    let rows: u64 = lines
        .iter()
        .skip(1)
        .filter(|l| l.starts_with("0 "))
        .filter_map(|l| l.split(' ').nth(2)?.parse::<u64>().ok())
        .sum();
    assert_eq!(rows, 400, "the key column's pages hold every row");
}

#[test]
fn columns_on_a_row_major_table_reports_no_pages() {
    let (_dir, sst) = build_one_sst(50, false);
    assert_eq!(
        run(&sst, &[]).trim(),
        "no column pages: the table is row-major"
    );
}

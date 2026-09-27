// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! End-to-end smoke test for the `columns` subcommand: build a columnar SST
//! whose columns each suit a different encoding, then drive
//! `sst-dump columns` against it and assert what it reports.

use lsm_tree::{
    AbstractTree, Config, SequenceNumberCounter,
    compression::CompressionType,
    config::{ColumnEncoding, ColumnEncodingPolicy, CompressionPolicy},
};
use std::process::Command;

const SST_DUMP_BIN: &str = env!("CARGO_BIN_EXE_sst-dump");

/// One SST of `items` rows under `columnar`, its pages stored as `encoding`
/// says or as the tree's default when `None`: sequential keys, seqnos in a
/// narrow range, and one repeated value.
fn build_one_sst(
    items: u64,
    columnar: bool,
    encoding: Option<ColumnEncoding>,
) -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut config = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::None));
    if let Some(encoding) = encoding {
        config = config.column_encoding_policy(ColumnEncodingPolicy::all(encoding));
    }
    let tree = config.open().expect("open tree");
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

/// The key column's pages, rows and offset bytes, summed over a summary's
/// lines for column 0.
fn key_column_offsets(summary: &str) -> (u64, u64, u64) {
    summary
        .lines()
        .skip(1)
        .filter(|l| l.starts_with("0 "))
        .map(|l| {
            let field = |i: usize| -> u64 {
                l.split(' ')
                    .nth(i)
                    .and_then(|f| f.parse().ok())
                    .unwrap_or_else(|| panic!("field {i} of {l:?} is a count"))
            };
            (field(1), field(2), field(4))
        })
        .fold((0, 0, 0), |(p, r, o), (pages, rows, offsets)| {
            (p + pages, r + rows, o + offsets)
        })
}

/// The summary reports every engine column, and the encodings the writer
/// chose for these values under the automatic encoding: a repeated value as
/// a constant, seqnos in a narrow range by their ordinals, not stored whole,
/// and keys by their lengths, whose bytes are reported apart from the keys'
/// and are fewer than the offset table they replace.
#[test]
fn columns_reports_each_page_and_what_it_was_encoded_as() {
    let (_dir, sst) = build_one_sst(400, true, Some(ColumnEncoding::Auto));

    let pages = run(&sst, &[]);
    assert!(
        pages.starts_with("group row_page column rows bytes offsets expression"),
        "a page listing first; got:\n{pages}",
    );
    // The listing's rows, up to the blank line before the summary: group, row
    // page, column, rows, bytes, offsets, then the expression.
    let listed: Vec<Vec<&str>> = pages
        .lines()
        .skip(1)
        .take_while(|l| !l.is_empty())
        .map(|l| l.splitn(7, ' ').collect())
        .collect();
    let count = |field: &str| -> u64 {
        field
            .parse()
            .unwrap_or_else(|_| panic!("{field:?} is a count"))
    };
    let listed_of = |column: u16| {
        listed
            .iter()
            .filter(move |page| page.get(2) == Some(&column.to_string().as_str()))
    };
    assert_eq!(
        listed_of(0).map(|page| count(page[3])).sum::<u64>(),
        400,
        "the listed key pages hold every row; got:\n{pages}",
    );
    assert!(
        listed_of(3).all(|page| page[6] == "constant"),
        "every listed page of the repeated value is a constant; got:\n{pages}",
    );

    let summary = run(&sst, &["--summary"]);
    let lines: Vec<&str> = summary.lines().collect();
    for column in 0..4u16 {
        let summarized: u64 = lines
            .iter()
            .skip(1)
            .filter(|l| l.split(' ').next() == Some(&column.to_string()))
            .map(|l| count(l.split(' ').nth(1).unwrap_or_default()))
            .sum();
        assert_eq!(
            listed_of(column).count() as u64,
            summarized,
            "column {column} lists as many pages as its summary counts",
        );
    }
    assert_eq!(
        lines.first().copied(),
        Some("column pages rows bytes offsets expression")
    );
    let for_column = |column: u16| -> Vec<String> {
        lines
            .iter()
            .skip(1)
            .filter(|l| l.split(' ').next() == Some(&column.to_string()))
            .map(|l| l.split(' ').skip(5).collect::<Vec<_>>().join(" "))
            .collect()
    };
    let (key_pages, key_rows, key_offsets) = key_column_offsets(&summary);
    assert!(
        key_offsets < 4 * (key_rows + key_pages),
        "the keys' lengths take fewer bytes than their offset tables; got:\n{summary}",
    );
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

/// A tree left at its default encoding stores every page plain, the same
/// values that encode as a constant and by ordinals above included, and a
/// plain key page spends four bytes per row and one more on its offsets.
#[test]
fn columns_under_the_default_encoding_are_all_plain() {
    let (_dir, sst) = build_one_sst(400, true, None);
    let summary = run(&sst, &["--summary"]);
    let expressions: Vec<&str> = summary
        .lines()
        .skip(1)
        .filter_map(|l| l.split(' ').nth(5))
        .collect();
    let (key_pages, key_rows, key_offsets) = key_column_offsets(&summary);
    assert_eq!(key_rows, 400);
    assert_eq!(
        key_offsets,
        4 * (key_rows + key_pages),
        "a plain key page reports its offset table; got:\n{summary}",
    );
    assert!(
        !expressions.is_empty(),
        "pages are reported; got:\n{summary}"
    );
    assert!(
        expressions.iter().all(|e| *e == "plain"),
        "every page is plain; got:\n{summary}",
    );
}

/// A row-major table stores its rows in data blocks, not column pages, so it
/// has no page encodings to report: the command says so instead of printing
/// an empty listing that would read as a columnar table with no pages.
#[test]
fn columns_on_a_row_major_table_reports_no_pages() {
    let (_dir, sst) = build_one_sst(50, false, None);
    assert_eq!(
        run(&sst, &[]).trim(),
        "no column pages: the table is row-major"
    );
}

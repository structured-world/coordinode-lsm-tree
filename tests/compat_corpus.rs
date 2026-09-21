//! Cross-version (golden) corpus.
//!
//! A small tree written by a released on-disk format, checked in as an opaque
//! binary artifact frozen at that format, plus a test that opens and reads it
//! under the current code. This guards forward-compatibility of the V5 format
//! across releases: when the writer changes in a later release, this test still
//! reads the old bytes and catches a backward-read regression — the matrix in
//! `compat_matrix.rs` only proves the *current* writer round-trips with itself.
//!
//! The fixture under [`FIXTURE`] was written by the v5.6.0 release format with
//! no compression / ECC / encryption (plain V5), so it reads back under any
//! feature build. It is decoupled from the code that generated it: the reader
//! never re-derives it, only opens the frozen bytes.
//!
//! Re-baseline the fixture at a new released format (only when intentionally
//! advancing the guarded version) with:
//!
//! ```text
//! cargo test --test compat_corpus -- --ignored --exact regenerate_golden_corpus
//! ```

use lsm_tree::{
    AbstractTree, CompressionType, Config, SequenceNumberCounter, config::CompressionPolicy,
    get_tmp_folder,
};
use std::path::Path;

/// Checked-in golden tree, written by the v5.6.0 release format.
const FIXTURE: &str = "tests/fixtures/compat_v5_6_0";

/// Number of known keys in the corpus.
const N: u32 = 20;

fn key(i: u32) -> Vec<u8> {
    format!("k{i:04}").into_bytes()
}

fn val(i: u32) -> Vec<u8> {
    format!("v{i:04}-golden").into_bytes()
}

/// Writes the deterministic corpus into `dir` (plain V5: no compression, ECC,
/// or encryption, so the fixture reads back under any feature build).
fn write_corpus(dir: &Path) {
    let tree = Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
    .open()
    .expect("open corpus tree");
    for i in 0..N {
        tree.insert(key(i), val(i), u64::from(i));
    }
    tree.flush_active_memtable(0).expect("flush corpus");
}

/// Recursively copies `src` into `dst` (no `std::fs::copy_dir`).
fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create dst dir");
    for entry in std::fs::read_dir(src).expect("read_dir") {
        let entry = entry.expect("dir entry");
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if entry.file_type().expect("file_type").is_dir() {
            copy_dir(&from, &to);
        } else {
            std::fs::copy(&from, &to).expect("copy file");
        }
    }
}

#[test]
#[ignore = "regenerates the checked-in golden fixture; run manually when re-baselining"]
fn regenerate_golden_corpus() {
    let dir = Path::new(FIXTURE);
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).expect("create fixture dir");
    write_corpus(dir);
    // Drop the lock file so the checked-in fixture carries no stale lock; the
    // reader copies the fixture and acquires its own lock in the copy.
    let _ = std::fs::remove_file(dir.join("LOCK"));
}

#[test]
fn golden_v5_corpus_is_refused_at_open_naming_the_converter() {
    // The 6.0 filter format replaces its predecessor outright and no reader
    // for the old layout ships, so this fixture — written by 5.6.0 — must no
    // longer open. What this test guards is the SHAPE of that refusal, which
    // is the part that can regress silently:
    //
    //   * it happens at OPEN, not on the first point read. A tree that opens
    //     and then fails somewhere under a `get` tells an operator nothing
    //     about what to do, and may have served reads from the levels that
    //     happened to parse before reaching one that did not.
    //   * it is a TYPED error that names the remedy, not a parse failure.
    //
    // When the offline converter lands, the companion test to write is the
    // positive one: convert this same fixture and assert it opens and reads.
    let fixture = Path::new(FIXTURE);
    assert!(
        fixture.join("current").exists(),
        "golden fixture missing — regenerate with `cargo test --test compat_corpus \
         -- --ignored --exact regenerate_golden_corpus`"
    );

    // Copy the frozen fixture into a writable tempdir: opening acquires a
    // directory lock and may write recovery state, which must not touch the
    // checked-in bytes.
    let tmp = get_tmp_folder();
    copy_dir(fixture, tmp.path());
    let _ = std::fs::remove_file(tmp.path().join("LOCK"));

    let err = Config::new(
        tmp.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .err()
    .expect("a v5 store must not open under the 6.0 filter format");

    assert!(
        matches!(
            err,
            lsm_tree::Error::UnsupportedFilterFormat { found: None, .. }
        ),
        "expected UnsupportedFilterFormat naming the converter, got: {err:?}",
    );
    // The message is the operator-facing half of the contract: a Debug dump
    // would not tell anyone that a tool exists.
    let rendered = err.to_string();
    assert!(
        rendered.contains("offline converter"),
        "the error must name the converter, got: {rendered}",
    );
}

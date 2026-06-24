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
    AbstractTree, CompressionType, Config, SeqNo, SequenceNumberCounter, config::CompressionPolicy,
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
fn golden_v5_corpus_opens_and_reads_under_current_code() {
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

    let tree = Config::new(
        tmp.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("current code must open the v5.6.0 golden corpus");

    for i in 0..N {
        let got = tree
            .get(key(i), SeqNo::MAX)
            .expect("get")
            .unwrap_or_else(|| panic!("golden corpus: key {i} missing"));
        assert_eq!(
            &*got,
            val(i).as_slice(),
            "golden corpus: value mismatch for key {i}"
        );
    }
    let scanned = tree.range(key(0)..key(1_000_000), SeqNo::MAX, None).count();
    assert_eq!(
        scanned, N as usize,
        "golden corpus: range scan must see every row"
    );
}

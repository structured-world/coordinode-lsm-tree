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
    // The 6.0 format replaces its predecessor outright and no reader for the
    // old layout ships, so this fixture — written by 5.6.0 — must no longer
    // open. It is refused at the manifest's format version, before any table
    // is opened. What this test guards is the SHAPE of that refusal, which is
    // the part that can regress silently:
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
    .expect("a v5 store must not open under the 6.0 format");

    assert!(
        matches!(err, lsm_tree::Error::InvalidVersion(5)),
        "expected the format-version refusal for a V5 store, got: {err:?}",
    );
    // The message is the operator-facing half of the contract: a Debug dump
    // would not tell anyone that a tool exists.
    let rendered = err.to_string();
    assert!(
        rendered.contains("offline converter"),
        "the error must name the converter, got: {rendered}",
    );
}

#[test]
fn repairing_a_v5_store_refuses_instead_of_discarding_its_tables() {
    // Refusing the OPEN is only half the contract. An operator whose store
    // will not open reaches for `repair()` next, and repair grades every SST
    // it cannot recover as damaged: a table that fails recovery is left out
    // of the rebuilt manifest and scheduled for removal once that manifest is
    // durable. For a legacy store that would be every filtered table in it,
    // deleted — over an error whose entire meaning is "the data is intact and
    // awaiting conversion".
    //
    // So the refusal has to travel as one of the errors repair PROPAGATES
    // rather than grades, the way a missing zstd dictionary already does: the
    // bytes are healthy, the caller's environment is wrong, and a rerun after
    // fixing it finds everything still on disk. Repair reads the committed
    // manifest's format label before it looks at a single table, so the
    // refusal is the manifest's version, whatever the tables would have said.
    let fixture = Path::new(FIXTURE);
    assert!(fixture.join("current").exists(), "golden fixture missing");

    let tmp = get_tmp_folder();
    copy_dir(fixture, tmp.path());
    let _ = std::fs::remove_file(tmp.path().join("LOCK"));

    // Count the SSTs before, so "nothing was discarded" is asserted against
    // the directory rather than against the report's own accounting. The
    // count must be non-zero: a walk of a directory that does not exist
    // returns 0 before and after, and would pass while proving nothing.
    let sst_count = |dir: &Path| -> usize { walk_count(&dir.join("tables")) };
    let before = sst_count(tmp.path());
    assert!(
        before > 0,
        "the fixture must hold SSTs for the comparison to mean anything",
    );

    let err = Config::new(
        tmp.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair()
    .expect_err("repair must refuse a store it cannot read rather than rebuild it");

    assert!(
        matches!(err, lsm_tree::Error::InvalidVersion(5)),
        "repair must propagate the format refusal, not grade the tables as \
         damaged; got: {err:?}",
    );
    assert_eq!(
        sst_count(tmp.path()),
        before,
        "repair removed files from a store whose data is intact",
    );
}

/// Counts files under `dir` recursively, or 0 if it does not exist.
fn walk_count(dir: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|e| {
            if e.file_type().is_ok_and(|t| t.is_dir()) {
                walk_count(&e.path())
            } else {
                1
            }
        })
        .sum()
}

// A columnar table's pages are stored as its level's encoding says: plain
// where the policy asks for views, encoded where it asks for the cheapest
// expression, and a compaction writes under the policy of the level it
// writes into.
#![cfg(feature = "columnar")]

use lsm_tree::{
    AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter,
    config::{ColumnEncoding, ColumnEncodingPolicy},
    get_tmp_folder,
    inspect::read_column_encodings,
    table::columnar::Expression,
};
use test_log::test;

/// The expressions of every page of every table under `folder`. A table a
/// compaction replaced may be deleted in the background while the folder is
/// listed; one gone by the time it is read is skipped.
fn page_expressions(folder: &std::path::Path) -> lsm_tree::Result<Vec<Expression>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(folder.join("tables"))? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        match read_column_encodings(&entry.path()) {
            Ok(pages) => out.extend(pages.into_iter().map(|page| page.expression)),
            Err(lsm_tree::Error::Io(e)) if e.kind() == lsm_tree::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    Ok(out)
}

/// A columnar tree under `policy` holding 400 rows of one repeated value,
/// flushed into one table at level 0.
fn flushed_tree(
    folder: &std::path::Path,
    policy: ColumnEncodingPolicy,
) -> lsm_tree::Result<AnyTree> {
    let tree = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .column_encoding_policy(policy)
    .open()?;
    let AnyTree::Standard(standard) = &tree else {
        panic!("a standard tree");
    };
    standard.update_runtime_config(|rc| rc.columnar = true)?;
    for i in 0..400u64 {
        tree.insert(format!("key-{i:06}"), "the same value", 1 + i);
    }
    tree.flush_active_memtable(0)?;
    Ok(tree)
}

#[test]
fn a_level_plain_by_policy_stores_plain_pages_and_one_below_encodes_them() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = flushed_tree(
        folder.path(),
        ColumnEncodingPolicy::new([ColumnEncoding::Plain, ColumnEncoding::Auto]),
    )?;

    // Level 0 asked for plain pages: the repeated value is stored whole.
    let flushed = page_expressions(folder.path())?;
    assert!(!flushed.is_empty(), "the flush wrote column pages");
    assert!(
        flushed.iter().all(|e| *e == Expression::Plain),
        "level 0 is plain; got {flushed:?}",
    );

    // Compacted into level 1, which asks for the cheapest expression: the
    // repeated value becomes a constant.
    tree.major_compact(64 * 1_024 * 1_024, SeqNo::MAX)?;
    let compacted = page_expressions(folder.path())?;
    assert!(
        compacted.contains(&Expression::Constant),
        "level 1 encodes the repeated value; got {compacted:?}",
    );
    assert_eq!(tree.len(SeqNo::MAX, None)?, 400, "every row still reads");
    Ok(())
}

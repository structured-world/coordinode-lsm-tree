use lsm_tree::{AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter, get_tmp_folder};
use std::ops::Bound::{Excluded, Included, Unbounded};

fn populate_tables(tree: &AnyTree) -> lsm_tree::Result<()> {
    for key in 'a'..='e' {
        tree.insert([key as u8], "", 0);
        tree.flush_active_memtable(0)?;
    }
    Ok(())
}

#[test]
fn tree_drop_range_basic() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    populate_tables(&tree)?;

    assert_eq!(1, tree.l0_run_count());
    assert_eq!(5, tree.table_count());

    tree.drop_range("a"..="c")?;

    assert!(!tree.contains_key("a", SeqNo::MAX)?);
    assert!(!tree.contains_key("b", SeqNo::MAX)?);
    assert!(!tree.contains_key("c", SeqNo::MAX)?);
    assert!(tree.contains_key("d", SeqNo::MAX)?);
    assert!(tree.contains_key("e", SeqNo::MAX)?);

    assert_eq!(1, tree.l0_run_count());
    assert_eq!(2, tree.table_count());

    Ok(())
}

#[test]
fn tree_drop_range_partial_table_overlap_kept() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    for key in ['a', 'b', 'c', 'd', 'e'] {
        tree.insert([key as u8], "", 0);
    }
    tree.flush_active_memtable(0)?;

    assert_eq!(1, tree.l0_run_count());
    assert_eq!(1, tree.table_count());

    tree.drop_range("b".."d")?;

    for key in ['a', 'b', 'c', 'd', 'e'] {
        assert!(tree.contains_key([key as u8], SeqNo::MAX)?);
    }

    assert_eq!(1, tree.l0_run_count());
    assert_eq!(1, tree.table_count());

    Ok(())
}

#[test]
fn tree_drop_range_upper_exclusive() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    populate_tables(&tree)?;

    tree.drop_range("a".."d")?;

    assert!(!tree.contains_key("a", SeqNo::MAX)?);
    assert!(!tree.contains_key("b", SeqNo::MAX)?);
    assert!(!tree.contains_key("c", SeqNo::MAX)?);
    assert!(tree.contains_key("d", SeqNo::MAX)?);
    assert!(tree.contains_key("e", SeqNo::MAX)?);

    Ok(())
}

#[test]
fn tree_drop_range_lower_exclusive() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    populate_tables(&tree)?;

    tree.drop_range::<&str, _>((Excluded("a"), Included("c")))?;

    assert!(tree.contains_key("a", SeqNo::MAX)?);
    assert!(!tree.contains_key("b", SeqNo::MAX)?);
    assert!(!tree.contains_key("c", SeqNo::MAX)?);
    assert!(tree.contains_key("d", SeqNo::MAX)?);

    Ok(())
}

#[test]
fn tree_drop_range_unbounded_lower_inclusive_upper() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    populate_tables(&tree)?;

    tree.drop_range::<&str, _>((Unbounded, Included("c")))?;

    assert!(!tree.contains_key("a", SeqNo::MAX)?);
    assert!(!tree.contains_key("b", SeqNo::MAX)?);
    assert!(!tree.contains_key("c", SeqNo::MAX)?);
    assert!(tree.contains_key("d", SeqNo::MAX)?);
    assert!(tree.contains_key("e", SeqNo::MAX)?);

    Ok(())
}

#[test]
fn tree_drop_range_unbounded_lower_exclusive_upper() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    populate_tables(&tree)?;

    tree.drop_range::<&str, _>((Unbounded, Excluded("d")))?;

    assert!(!tree.contains_key("a", SeqNo::MAX)?);
    assert!(!tree.contains_key("b", SeqNo::MAX)?);
    assert!(!tree.contains_key("c", SeqNo::MAX)?);
    assert!(tree.contains_key("d", SeqNo::MAX)?);

    Ok(())
}

#[test]
fn tree_drop_range_exclusive_empty_interval() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    populate_tables(&tree)?;

    tree.drop_range::<&str, _>((Excluded("b"), Excluded("b")))?;

    assert!(tree.contains_key("a", SeqNo::MAX)?);
    assert!(tree.contains_key("b", SeqNo::MAX)?);
    assert!(tree.contains_key("c", SeqNo::MAX)?);

    Ok(())
}

#[test]
fn tree_drop_range_empty_tree() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.drop_range("a"..="c")?;

    assert_eq!(0, tree.l0_run_count());
    assert_eq!(0, tree.table_count());

    Ok(())
}

#[test]
fn tree_drop_range_unbounded_upper() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    populate_tables(&tree)?;

    tree.drop_range("c"..)?;

    assert!(tree.contains_key("a", SeqNo::MAX)?);
    assert!(tree.contains_key("b", SeqNo::MAX)?);
    assert!(!tree.contains_key("c", SeqNo::MAX)?);
    assert!(!tree.contains_key("d", SeqNo::MAX)?);
    assert!(!tree.contains_key("e", SeqNo::MAX)?);

    Ok(())
}

#[test]
fn tree_drop_range_clear_all() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    populate_tables(&tree)?;

    tree.drop_range::<&str, _>(..)?;

    assert_eq!(0, tree.l0_run_count());
    assert_eq!(0, tree.table_count());
    assert!(!tree.contains_key("a", SeqNo::MAX)?);
    assert!(!tree.contains_key("e", SeqNo::MAX)?);

    Ok(())
}

/// A tree with an older table holding `k = old` and `z`, which reaches past the
/// bounds `a..m`, and a newer table inside them holding what `newer` writes
/// for `k` at seqno 3. The folder comes back with the tree, which lives in it.
fn older_table_past_the_bounds(
    newer: impl Fn(&AnyTree),
) -> lsm_tree::Result<(tempfile::TempDir, AnyTree)> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    tree.insert("k", "old", 1);
    tree.insert("z", "outside", 2);
    tree.flush_active_memtable(0)?;
    newer(&tree);
    tree.flush_active_memtable(0)?;
    assert_eq!(2, tree.table_count(), "the case under test: two tables");
    Ok((folder, tree))
}

/// Dropping a table that deletes a key must not bring back the older value an
/// older table keeps: that table reaches past the bounds, so it stays, and the
/// deletion must stay with it.
#[test]
fn tree_drop_range_keeps_a_deletion_whose_older_value_stays() -> lsm_tree::Result<()> {
    let (_folder, tree) = older_table_past_the_bounds(|tree| {
        tree.remove("k", 3);
    })?;
    assert_eq!(None, tree.get("k", SeqNo::MAX)?);

    tree.drop_range("a".."m")?;

    assert_eq!(
        None,
        tree.get("k", SeqNo::MAX)?,
        "the deleted value came back"
    );
    assert_eq!(
        Some("outside".as_bytes().into()),
        tree.get("z", SeqNo::MAX)?
    );
    Ok(())
}

/// Dropping a table that overwrites a key must not hand out the older value an
/// older table keeps: a version the tree had replaced would come back.
#[test]
fn tree_drop_range_keeps_an_overwrite_whose_older_value_stays() -> lsm_tree::Result<()> {
    let (_folder, tree) = older_table_past_the_bounds(|tree| {
        tree.insert("k", "new", 3);
    })?;

    tree.drop_range("a".."m")?;

    assert_ne!(
        Some("old".as_bytes().into()),
        tree.get("k", SeqNo::MAX)?,
        "the overwritten value came back"
    );
    Ok(())
}

/// A table kept because an older one past the bounds overlaps it keeps every
/// newer table over its keys too: dropping the newest would expose the kept
/// middle one's older version.
#[test]
fn tree_drop_range_keeps_the_tables_above_a_kept_one() -> lsm_tree::Result<()> {
    let (_folder, tree) = older_table_past_the_bounds(|tree| {
        tree.insert("k", "middle", 3);
    })?;
    tree.insert("k", "newest", 4);
    tree.flush_active_memtable(0)?;
    assert_eq!(3, tree.table_count(), "the case under test: three tables");

    tree.drop_range("a".."m")?;

    assert_eq!(Some("newest".as_bytes().into()), tree.get("k", SeqNo::MAX)?);
    assert_eq!(3, tree.table_count());
    Ok(())
}

/// Tables wholly inside the bounds go together however they overlap: nothing
/// older is left to show through.
#[test]
fn tree_drop_range_drops_overlapping_tables_wholly_inside() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    tree.insert("k", "old", 1);
    tree.flush_active_memtable(0)?;
    tree.remove("k", 2);
    tree.flush_active_memtable(0)?;
    tree.insert("z", "outside", 3);
    tree.flush_active_memtable(0)?;

    tree.drop_range("a".."m")?;

    assert_eq!(
        1,
        tree.table_count(),
        "only the table past the bounds stays"
    );
    assert_eq!(None, tree.get("k", SeqNo::MAX)?);
    assert_eq!(
        Some("outside".as_bytes().into()),
        tree.get("z", SeqNo::MAX)?
    );
    Ok(())
}

#[test]
fn tree_drop_range_inverted_bounds_is_noop() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    populate_tables(&tree)?;

    tree.drop_range("c".."a")?;
    tree.drop_range("c"..="a")?;

    // All keys remain because the range is treated as empty.
    for key in 'a'..='e' {
        assert!(tree.contains_key([key as u8], SeqNo::MAX)?);
    }

    Ok(())
}

use super::AnyTree;
use crate::{
    AbstractTree, Config, InternalValue, KvSeparationOptions, SequenceNumberCounter, ValueType,
    abstract_tree::ChecksumRefreshOutcome, checksum::Checksum, compaction::Leveled,
};
use alloc::sync::Arc;
use test_log::test;

fn open(folder: &std::path::Path, blob: bool) -> crate::Result<AnyTree> {
    let config = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    );
    let config = if blob {
        config.with_kv_separation(Some(KvSeparationOptions::default()))
    } else {
        config
    };
    config.open()
}

/// Calls made through `AnyTree` read the same state as calls made on the tree
/// it holds.
fn assert_reads_match<T: AbstractTree>(any: &AnyTree, inner: &T) {
    assert_eq!(any.table_file_cache_size(), inner.table_file_cache_size());
    assert_eq!(any.disk_space(), inner.disk_space());
    assert_eq!(any.next_table_id(), inner.next_table_id());
    assert_eq!(
        any.prefix_extractor().is_some(),
        inner.prefix_extractor().is_some()
    );
    assert_eq!(
        any.write_backpressure(&Leveled::default()),
        inner.write_backpressure(&Leveled::default())
    );
    assert!(Arc::ptr_eq(
        &any.write_rate_limiter(),
        &inner.write_rate_limiter()
    ));
    assert!(Arc::ptr_eq(
        &any.active_memtable(),
        &inner.active_memtable()
    ));
}

fn forwards_to_the_tree_it_holds(blob: bool) -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = open(folder.path(), blob)?;
    assert_eq!(matches!(tree, AnyTree::Blob(_)), blob);

    // Admission control is off by default: every gated write goes through.
    tree.write_admission()?;
    assert!(!tree.is_read_only());
    tree.try_insert("a", "1", 0)?;
    tree.try_insert("b", "2", 1)?;
    tree.try_insert("c", "3", 2)?;
    tree.try_remove("a", 3)?;
    tree.try_remove_weak("b", 4)?;
    assert!(tree.try_remove_range("c", "d", 5)? > 0);
    for key in ["a", "b", "c"] {
        assert_eq!(tree.get(key, 6)?, None, "{key} should be deleted");
    }

    tree.try_merge("m", "x", 6)?;
    assert!(!tree.active_memtable().is_empty());
    tree.clear_active_memtable();
    assert!(tree.active_memtable().is_empty());

    tree.insert("k", "v", 7);
    tree.flush_active_memtable(0)?;
    assert_eq!(tree.table_count(), 1);
    assert!(tree.disk_space() > 0);

    let id = tree.get_next_table_id();
    assert_eq!(tree.next_table_id(), id + 1);

    // No table has that id, so there is nothing to refresh.
    assert_eq!(
        tree.refresh_table_checksum(id, Checksum::from_raw(0), None)?,
        ChecksumRefreshOutcome::Stale
    );

    let written = tree.flush_to_tables_with_rt(
        core::iter::once(Ok(InternalValue::from_components(
            "w",
            "v",
            8,
            ValueType::Value,
        ))),
        Vec::new(),
        1,
        1,
        None,
    )?;
    let (tables, _, _) = written.expect("one entry writes a table");
    assert_eq!(tables.len(), 1);

    match &tree {
        AnyTree::Standard(inner) => assert_reads_match(&tree, inner),
        AnyTree::Blob(inner) => assert_reads_match(&tree, inner),
    }
    Ok(())
}

#[test]
fn any_tree_standard_forwards_to_tree() -> crate::Result<()> {
    forwards_to_the_tree_it_holds(false)
}

#[test]
fn any_tree_blob_forwards_to_blob_tree() -> crate::Result<()> {
    forwards_to_the_tree_it_holds(true)
}

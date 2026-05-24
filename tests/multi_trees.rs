use lsm_tree::{AbstractTree, Config, SequenceNumberCounter, get_tmp_folder};
use test_log::test;

/// Two trees opened in the same process must keep INDEPENDENT
/// `table_id` counters: each tree's table ids start at 0 and grow
/// independently of any other tree the process has opened.
///
/// Tree ids themselves are drawn from a process-global counter
/// (`TREE_ID_COUNTER` in `src/tree/inner.rs`), so the absolute tree
/// id values depend on test execution order. This test only asserts
/// the property that matters here — `tree0.id() != tree1.id()` — and
/// makes no claim about which specific values they hold.
#[test]
fn tree_multi_table_ids() -> lsm_tree::Result<()> {
    let folder0 = get_tmp_folder();
    let folder1 = get_tmp_folder();

    let tree0 = Config::new(
        &folder0,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    assert_eq!(0, tree0.next_table_id());

    tree0.insert("a", "a", 0);
    tree0.flush_active_memtable(0)?;

    assert_eq!(1, tree0.next_table_id());

    assert_eq!(
        0,
        tree0
            .current_version()
            .level(0)
            .expect("level should exist")
            .first()
            .expect("run should exist")
            .first()
            .expect("table should exist")
            .metadata
            .id
    );

    let tree1 = Config::new(
        &folder1,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    assert_ne!(
        tree0.id(),
        tree1.id(),
        "tree ids must be unique across the process",
    );

    assert_eq!(0, tree1.next_table_id());

    tree1.insert("a", "a", 0);
    tree1.flush_active_memtable(0)?;

    assert_eq!(1, tree1.next_table_id());

    assert_eq!(
        0,
        tree1
            .current_version()
            .level(0)
            .expect("level should exist")
            .first()
            .expect("run should exist")
            .first()
            .expect("table should exist")
            .metadata
            .id
    );

    Ok(())
}

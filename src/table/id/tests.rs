use super::*;
use test_log::test;

#[test]
fn global_table_id_accessors() {
    let tree_id = 42;
    let table_id: TableId = 7;
    let global_table_id = GlobalTableId::from((tree_id, table_id));

    assert_eq!(global_table_id.tree_id(), 42);
    assert_eq!(global_table_id.table_id(), 7);
}

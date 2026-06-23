use super::*;

fn id(tree: u64, table: u64) -> GlobalTableId {
    GlobalTableId::from((tree, table))
}

#[test]
fn record_dedups_same_id_returns_false_on_repeat() {
    let hints = HealHints::default();
    assert!(hints.record(id(1, 7)));
    assert!(!hints.record(id(1, 7)));
    assert_eq!(hints.snapshot(), vec![id(1, 7)]);
}

#[test]
fn record_collects_distinct_ids() {
    let hints = HealHints::default();
    hints.record(id(1, 1));
    hints.record(id(1, 2));
    hints.record(id(1, 1));
    let snapshot = hints.snapshot();
    assert_eq!(snapshot, vec![id(1, 1), id(1, 2)]);
}

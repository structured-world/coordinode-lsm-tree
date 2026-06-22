// Regression for #527: a range tombstone must keep deleting a key across a major
// compaction, even when a later merge operand exists. Before the fix, compaction
// dropped the range tombstone while the operand still depended on it, so the
// operand folded onto the pre-delete value instead of an empty base.

use lsm_tree::{
    AbstractTree, AnyTree, Config, MergeOperator, SequenceNumberCounter, UserValue, get_tmp_folder,
};
use std::sync::Arc;

/// Lexicographic-max merge: the result is the largest of the base and operands.
struct MaxMerge;
impl MergeOperator for MaxMerge {
    fn merge(&self, _k: &[u8], base: Option<&[u8]>, ops: &[&[u8]]) -> lsm_tree::Result<UserValue> {
        let mut best: &[u8] = base.unwrap_or(&[]);
        for op in ops {
            if *op > best {
                best = op;
            }
        }
        Ok(UserValue::from(best))
    }
}

/// `Insert([54]) -> remove_range(covers key) -> merge([0])`, then a major
/// compaction, then read. The range delete clears the base, so the merge operand
/// folds onto an empty base, giving the operand value.
fn merge_after_range_delete_compacted() -> Option<Vec<u8>> {
    let tmpdir = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let visible = SequenceNumberCounter::default();
    let AnyTree::Standard(tree) = Config::new(tmpdir.path(), seqno.clone(), visible.clone())
        .with_merge_operator(Some(Arc::new(MaxMerge)))
        .open()
        .expect("open")
    else {
        panic!("expected a standard tree");
    };

    let key = vec![6u8];
    tree.insert(key.clone(), vec![54u8], seqno.next()); // @0 base value
    let _ = tree.remove_range(vec![0u8], vec![7u8], seqno.next()); // @1 covers key 6
    tree.merge(key.clone(), vec![0u8], seqno.next()); // @2 merge operand
    visible.fetch_max(3);

    tree.flush_active_memtable(0).expect("flush");
    tree.major_compact(64 * 1024 * 1024, seqno.get())
        .expect("compact");

    tree.get(&key, 5).expect("get").map(|v| v.to_vec())
}

#[test]
fn merge_after_range_delete_survives_compaction() {
    assert_eq!(
        merge_after_range_delete_compacted(),
        Some(vec![0u8]),
        "a range delete must keep deleting the key across compaction, so the \
         later merge operand folds onto an empty base"
    );
}

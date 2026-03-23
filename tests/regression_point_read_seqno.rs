mod common;
use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};

#[test]
fn exact_replay() -> lsm_tree::Result<()> {
    let tmpdir = lsm_tree::get_tmp_folder();
    let tree = Config::new(
        &tmpdir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let key = vec![0u8];
    let v0 = vec![0u8; 8];
    let v1 = vec![1u8; 8];

    // Exact proptest sequence:
    tree.major_compact(common::COMPACTION_TARGET, 1)?; // Compact
    tree.flush_active_memtable(0)?; // Flush
    tree.insert(&key, &v0, 1); // Insert(0,0)
    tree.flush_active_memtable(0)?; // Flush
    tree.insert(&key, &v0, 2); // Insert(0,0)
    tree.major_compact(common::COMPACTION_TARGET, 3)?; // Compact
    tree.major_compact(common::COMPACTION_TARGET, 3)?; // Compact
    tree.major_compact(common::COMPACTION_TARGET, 3)?; // Compact
    tree.flush_active_memtable(0)?; // Flush
    tree.insert(&key, &v0, 3); // Insert(0,0)
    tree.flush_active_memtable(0)?; // Flush
    tree.insert(&key, &v0, 4); // Insert(0,0)
    tree.flush_active_memtable(0)?; // Flush
    tree.insert(&key, &v0, 5); // Insert(0,0)
    tree.flush_active_memtable(0)?; // Flush
    tree.major_compact(common::COMPACTION_TARGET, 6)?; // Compact
    tree.insert(&key, &v0, 6); // Insert(0,0)
    tree.flush_active_memtable(0)?; // Flush
    tree.insert(&key, &v0, 7); // Insert(0,0)
    tree.insert(&key, &v1, 8); // Insert(0,1)

    let actual = tree.get(&key, 9)?;
    assert_eq!(actual.as_ref().map(|v| v.to_vec()), Some(v1));
    Ok(())
}

use lsm_tree::{AbstractTree, Config, SeqNo, SequenceNumberCounter, get_tmp_folder};
use test_log::test;

#[test]
fn tree_recover_large_value() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    {
        let tree = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;
        tree.insert("a", "a".repeat(100_000), 0);
        tree.flush_active_memtable(0)?;
    }

    {
        let tree = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;
        assert_eq!(
            &*tree.get("a", SeqNo::MAX)?.expect("should exist"),
            "a".repeat(100_000).as_bytes()
        );
    }

    Ok(())
}

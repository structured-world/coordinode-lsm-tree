use lsm_tree::AbstractTree;

/// Regression for upstream #286: `BlobTree::clear()` must reset the
/// version history (active memtable, sealed memtables, version id) like
/// `Tree::clear()`, otherwise a subsequent reopen of the tree fails to
/// recover a consistent state and the on-disk blob tree is corrupted.
#[test_log::test]
fn blob_tree_clear_then_reopen_succeeds() -> lsm_tree::Result<()> {
    let folder = lsm_tree::get_tmp_folder();
    let seqno = lsm_tree::SequenceNumberCounter::default();

    {
        let tree = lsm_tree::Config::new(
            &folder,
            seqno.clone(),
            lsm_tree::SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(lsm_tree::KvSeparationOptions::default()))
        .open()?;

        tree.insert("foo", b"1", 0);
        tree.clear()?;
    }

    {
        let _tree = lsm_tree::Config::new(
            &folder,
            seqno.clone(),
            lsm_tree::SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(lsm_tree::KvSeparationOptions::default()))
        .open()?;
    }

    Ok(())
}

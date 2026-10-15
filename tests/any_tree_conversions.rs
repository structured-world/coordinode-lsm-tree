// `AnyTree` converts from either tree type and back into the one it holds;
// asking it for the other one names both variants.

use lsm_tree::{
    AbstractTree, AnyTree, BlobTree, Config, KvSeparationOptions, SequenceNumberCounter, Tree,
    get_tmp_folder,
};
use test_log::test;

fn config(folder: &std::path::Path) -> Config {
    Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
}

#[test]
fn standard_tree_converts_into_tree_and_back() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let any = config(folder.path()).open()?;
    any.insert("a", "1", 0);

    let blob: Result<BlobTree, _> = any.clone().try_into();
    assert_eq!(
        blob.err(),
        Some("Tried to convert variant Standard to Blob")
    );

    let tree: Tree = any.try_into().expect("a standard tree");
    let any = AnyTree::from(tree);
    assert!(matches!(any, AnyTree::Standard(_)));
    assert_eq!(any.get("a", 1)?.as_deref(), Some(b"1".as_slice()));
    Ok(())
}

#[test]
fn blob_tree_converts_into_blob_tree_and_back() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let any = config(folder.path())
        .with_kv_separation(Some(KvSeparationOptions::default()))
        .open()?;
    any.insert("a", "1", 0);

    let tree: Result<Tree, _> = any.clone().try_into();
    assert_eq!(
        tree.err(),
        Some("Tried to convert variant Blob to Standard")
    );

    let blob: BlobTree = any.try_into().expect("a blob tree");
    let any = AnyTree::from(blob);
    assert!(matches!(any, AnyTree::Blob(_)));
    assert_eq!(any.get("a", 1)?.as_deref(), Some(b"1".as_slice()));
    Ok(())
}

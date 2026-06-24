//! Regression: a blob tree's index honors the columnar runtime flag.
//!
//! The blob tree's index flush path previously wired only a subset of the
//! per-flush SST options, silently dropping `columnar` (and the other
//! runtime-driven options) — so enabling columnar on a blob tree's index left
//! it row-major. The round-trip still passed (the reader is format-transparent),
//! hiding the gap.

#![cfg(feature = "columnar")]

use lsm_tree::{
    AbstractTree, AnyTree, Config, KvSeparationOptions, SeqNo, SequenceNumberCounter,
    get_tmp_folder,
};

#[test]
fn blob_index_honors_the_columnar_flag() {
    let folder = get_tmp_folder();
    let AnyTree::Blob(tree) = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    .open()
    .expect("open blob tree") else {
        panic!("expected a blob tree");
    };

    tree.index
        .update_runtime_config(|rc| rc.columnar = true)
        .expect("enable columnar on the index");

    for i in 0..50u32 {
        tree.insert(format!("k{i:04}"), format!("v{i:04}-payload"), u64::from(i));
    }
    tree.flush_active_memtable(0).expect("flush");

    // The blob index flush must wire the columnar flag, so the index SSTs are
    // column-organized rather than silently row-major.
    let version = tree.index.current_version();
    assert!(
        version.iter_tables().next().is_some(),
        "expected at least one flushed index SST"
    );
    assert!(
        version.iter_tables().all(|t| t.metadata.columnar),
        "blob tree index SSTs must be columnar when the flag is set"
    );

    // The round-trip still holds through the columnar -> row reader.
    for i in 0..50u32 {
        let got = tree
            .get(format!("k{i:04}"), SeqNo::MAX)
            .expect("get")
            .expect("key present");
        assert_eq!(&*got, format!("v{i:04}-payload").as_bytes());
    }
}

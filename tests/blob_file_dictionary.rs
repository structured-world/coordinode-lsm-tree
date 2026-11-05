//! A zstd blob file that builds a dictionary from its own values: every
//! value a frame of its own, compressed with it, read back whole, after a
//! reopen and after its frames are copied into another file.
#![cfg(feature = "zstd")]

use lsm_tree::{
    AbstractTree, CompressionType, Config, KvSeparationOptions, SeqNo, SequenceNumberCounter,
    get_tmp_folder,
};

/// Documents alike in shape, as a product's records are: what a dictionary
/// of their first few gives each of the rest.
fn document(i: u32) -> Vec<u8> {
    format!(
        "{{\"id\":{i},\"tenant\":\"acme\",\"kind\":\"order\",\"status\":\"shipped\",\
         \"region\":\"eu-west-{}\",\"items\":[{{\"sku\":\"SKU-{}\",\"qty\":{}}}],\
         \"note\":\"customer asked for delivery before noon on a weekday\"}}",
        i % 3,
        i % 97,
        i % 5 + 1
    )
    .into_bytes()
}

fn open(path: &std::path::Path, file_dictionary: u32) -> lsm_tree::Result<lsm_tree::AnyTree> {
    open_with(path, file_dictionary, 8 * 1_024)
}

fn open_with(
    path: &std::path::Path,
    file_dictionary: u32,
    block_size: u32,
) -> lsm_tree::Result<lsm_tree::AnyTree> {
    Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .blob_compression(CompressionType::Zstd(3))
    .with_kv_separation(Some(
        KvSeparationOptions::default()
            .separation_threshold(1)
            .block_size(block_size)
            .file_dictionary(file_dictionary),
    ))
    .open()
}

const KEYS: u32 = 2_000;

fn key(i: u32) -> String {
    format!("{i:06}")
}

fn fill(tree: &lsm_tree::AnyTree) -> lsm_tree::Result<()> {
    for i in 0..KEYS {
        tree.insert(key(i), document(i), u64::from(i));
    }
    tree.flush_active_memtable(0)?;
    Ok(())
}

fn assert_all(tree: &lsm_tree::AnyTree) -> lsm_tree::Result<()> {
    for i in 0..KEYS {
        assert_eq!(
            tree.get(key(i), SeqNo::MAX)?.as_deref(),
            Some(document(i).as_slice()),
            "key {i}"
        );
    }
    Ok(())
}

#[test]
fn values_under_a_file_dictionary_read_back_and_take_less() -> lsm_tree::Result<()> {
    let plain = get_tmp_folder();
    let plain_tree = open_with(plain.path(), 0, 0)?;
    fill(&plain_tree)?;
    let blocks = get_tmp_folder();
    let blocks_tree = open(blocks.path(), 0)?;
    fill(&blocks_tree)?;

    let folder = get_tmp_folder();
    {
        let tree = open(folder.path(), 16 * 1_024)?;
        fill(&tree)?;
        assert_all(&tree)?;
        // Each value alone, but primed with the file's dictionary: less than
        // each compressed with nothing.
        let (with, without) = (tree.disk_space(), plain_tree.disk_space());
        eprintln!(
            "per value: {without} B, per value with the file's dictionary: {with} B, blocks: {} B",
            blocks_tree.disk_space()
        );
        assert!(
            with * 10 < without * 9,
            "{with} bytes under the file's dictionary against {without} without, {} in blocks",
            blocks_tree.disk_space()
        );
    }
    // The dictionary comes back with the file.
    let tree = open(folder.path(), 16 * 1_024)?;
    assert_all(&tree)?;
    Ok(())
}

#[test]
fn a_file_dictionary_goes_with_the_frames_a_collection_copies() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path(), 16 * 1_024)?;
    fill(&tree)?;
    // Half the values overwritten: the file is worth collecting.
    for i in (0..KEYS).step_by(2) {
        tree.insert(key(i), document(i + 1_000_000), u64::from(KEYS + i));
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, SeqNo::MAX)?;
    for i in 0..KEYS {
        let want = if i % 2 == 0 {
            document(i + 1_000_000)
        } else {
            document(i)
        };
        assert_eq!(
            tree.get(key(i), SeqNo::MAX)?.as_deref(),
            Some(want.as_slice()),
            "key {i}"
        );
    }
    drop(tree);
    let tree = open(folder.path(), 16 * 1_024)?;
    assert_eq!(
        tree.get(key(1), SeqNo::MAX)?.as_deref(),
        Some(document(1).as_slice())
    );
    Ok(())
}

#![cfg(all(feature = "zstd", feature = "parallel"))]

use lsm_tree::{
    AbstractTree, CompressionType, Config, KvSeparationOptions, SequenceNumberCounter,
    get_tmp_folder,
};

fn value(i: u32) -> Vec<u8> {
    i.to_be_bytes().repeat(100)
}

/// Compressed in the background, separated values interleave with small values
/// and tombstones the flush writes itself. Every key has to come back with its
/// own value, or the handle of one value landed on another's key: more keys than
/// the encoder holds in flight, so the flush also meets it full.
#[test]
fn zstd_blob_flush_keeps_every_key_on_its_value() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(128),
    ))
    .blob_compression(CompressionType::Zstd(3))
    // A pool of its own whatever the host's core count: without one a flush
    // compresses inline.
    .compaction_threads(2)
    .open()?;

    let count = 1_000_u32;
    for i in 0..count {
        let key = i.to_be_bytes();
        let seqno = u64::from(i);
        match i % 10 {
            3 => {
                tree.insert(key, b"small", seqno);
            }
            7 => {
                tree.remove(key, seqno);
            }
            _ => {
                tree.insert(key, value(i), seqno);
            }
        }
    }
    tree.flush_active_memtable(0)?;
    assert!(tree.blob_file_count() > 0, "values were separated");

    for i in 0..count {
        let got = tree.get(i.to_be_bytes(), u64::MAX - 1)?;
        match i % 10 {
            3 => assert_eq!(got.as_deref(), Some(b"small".as_slice()), "key {i}"),
            7 => assert!(got.is_none(), "key {i} was removed"),
            _ => assert_eq!(got.as_deref(), Some(value(i).as_slice()), "key {i}"),
        }
    }
    Ok(())
}

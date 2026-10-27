use lsm_tree::{AbstractTree, KvSeparationOptions, SeqNo, SequenceNumberCounter, get_tmp_folder};
use test_log::test;

#[test]
fn blob_ingest_gc_stats() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);
    let new_big_value = b"winter!".repeat(128_000);

    {
        let tree = lsm_tree::Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(KvSeparationOptions::default()))
        .blob_compression(lsm_tree::CompressionType::None)
        .open()?;

        let mut ingestion = tree.ingestion()?;
        ingestion.write("big", &big_value)?;
        ingestion.write("smol", "small value")?;
        ingestion.finish()?;

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        let mut ingestion = tree.ingestion()?;
        ingestion.write("big", &new_big_value)?;
        ingestion.finish()?;

        // Blob file has no fragmentation before compaction (in stats), so it
        // is not rewritten: the compaction expires "big":0, the only value in
        // it, which charges the file in full, and its install drops the file.
        tree.major_compact(64_000_000, 1_000)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());
        assert_eq!(
            &lsm_tree::HashMap::default(),
            &*tree.current_version().gc_stats().clone()
        );
        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, new_big_value);
    }

    Ok(())
}

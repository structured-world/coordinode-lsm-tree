use lsm_tree::{
    AbstractTree, Config, Guard, KvSeparationOptions, SeqNo, SequenceNumberCounter, get_tmp_folder,
};
use test_log::test;

const ITEM_COUNT: usize = 100_000;

#[test]
fn tree_bulk_ingest() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let visible_seqno = SequenceNumberCounter::default();

    let tree = Config::new(&folder, seqno.clone(), visible_seqno.clone()).open()?;

    let mut ingestion = tree.ingestion()?;
    for x in 0..ITEM_COUNT as u64 {
        let k = x.to_be_bytes();
        let v = nanoid::nanoid!();
        ingestion.write(k, v)?;
    }
    ingestion.finish()?;

    assert_eq!(visible_seqno.get(), seqno.get());

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT);
    assert_eq!(
        tree.iter(SeqNo::MAX, None).flat_map(|x| x.key()).count(),
        ITEM_COUNT,
    );
    assert_eq!(
        tree.iter(SeqNo::MAX, None)
            .rev()
            .flat_map(|x| x.key())
            .count(),
        ITEM_COUNT,
    );

    Ok(())
}

#[test]
fn tree_copy() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let visible_seqno = SequenceNumberCounter::default();

    let src = Config::new(&folder, seqno.clone(), visible_seqno.clone()).open()?;

    let mut ingestion = src.ingestion()?;
    for x in 0..ITEM_COUNT as u64 {
        let k = x.to_be_bytes();
        let v = nanoid::nanoid!();
        ingestion.write(k, v)?;
    }
    ingestion.finish()?;

    assert_eq!(visible_seqno.get(), seqno.get());

    assert_eq!(src.len(SeqNo::MAX, None)?, ITEM_COUNT);
    assert_eq!(
        src.iter(SeqNo::MAX, None).flat_map(|x| x.key()).count(),
        ITEM_COUNT,
    );
    assert_eq!(
        src.iter(SeqNo::MAX, None)
            .rev()
            .flat_map(|x| x.key())
            .count(),
        ITEM_COUNT,
    );

    let folder = get_tmp_folder();
    let dest = Config::new(&folder, seqno.clone(), visible_seqno.clone()).open()?;

    let mut ingestion = dest.ingestion()?;
    for item in src.iter(SeqNo::MAX, None) {
        let (k, v) = item.into_inner().unwrap();
        ingestion.write(k, v)?;
    }
    ingestion.finish()?;

    assert_eq!(visible_seqno.get(), seqno.get());

    assert_eq!(dest.len(SeqNo::MAX, None)?, ITEM_COUNT);
    assert_eq!(
        dest.iter(SeqNo::MAX, None).flat_map(|x| x.key()).count(),
        ITEM_COUNT,
    );
    assert_eq!(
        dest.iter(SeqNo::MAX, None)
            .rev()
            .flat_map(|x| x.key())
            .count(),
        ITEM_COUNT,
    );

    Ok(())
}

#[test]
fn blob_tree_bulk_ingest() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let visible_seqno = SequenceNumberCounter::default();

    let tree = Config::new(&folder, seqno.clone(), visible_seqno.clone())
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        .open()?;

    let mut ingestion = tree.ingestion()?;
    for x in 0..ITEM_COUNT as u64 {
        let k = x.to_be_bytes();
        let v = nanoid::nanoid!();
        ingestion.write(k, v)?;
    }
    ingestion.finish()?;

    assert_eq!(visible_seqno.get(), seqno.get());

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT);
    assert_eq!(
        tree.iter(SeqNo::MAX, None).flat_map(|x| x.key()).count(),
        ITEM_COUNT,
    );
    assert_eq!(
        tree.iter(SeqNo::MAX, None)
            .rev()
            .flat_map(|x| x.key())
            .count(),
        ITEM_COUNT,
    );
    assert_eq!(1, tree.blob_file_count());

    Ok(())
}

#[test]
fn blob_tree_copy() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let visible_seqno = SequenceNumberCounter::default();

    let src = Config::new(&folder, seqno.clone(), visible_seqno.clone())
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        .open()?;

    let mut ingestion = src.ingestion()?;
    for x in 0..ITEM_COUNT as u64 {
        let k = x.to_be_bytes();
        let v = nanoid::nanoid!();
        ingestion.write(k, v)?;
    }
    ingestion.finish()?;

    assert_eq!(visible_seqno.get(), seqno.get());

    assert_eq!(src.len(SeqNo::MAX, None)?, ITEM_COUNT);
    assert_eq!(
        src.iter(SeqNo::MAX, None).flat_map(|x| x.key()).count(),
        ITEM_COUNT,
    );
    assert_eq!(
        src.iter(SeqNo::MAX, None)
            .rev()
            .flat_map(|x| x.key())
            .count(),
        ITEM_COUNT,
    );
    assert_eq!(1, src.blob_file_count());

    let folder = get_tmp_folder();
    let dest = Config::new(&folder, seqno.clone(), visible_seqno.clone())
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        .open()?;

    let mut ingestion = dest.ingestion()?;
    for item in src.iter(SeqNo::MAX, None) {
        let (k, v) = item.into_inner().unwrap();
        ingestion.write(k, v)?;
    }
    ingestion.finish()?;

    assert_eq!(visible_seqno.get(), seqno.get());

    assert_eq!(dest.len(SeqNo::MAX, None)?, ITEM_COUNT);
    assert_eq!(
        dest.iter(SeqNo::MAX, None).flat_map(|x| x.key()).count(),
        ITEM_COUNT,
    );
    assert_eq!(
        dest.iter(SeqNo::MAX, None)
            .rev()
            .flat_map(|x| x.key())
            .count(),
        ITEM_COUNT,
    );
    assert_eq!(1, dest.blob_file_count());

    Ok(())
}

/// End-to-end: a bulk-ingested table relies on a manifest-only `global_seqno`
/// offset for its effective MVCC ordering, so manifest repair (which cannot
/// recover that offset from the SST) must FAIL CLOSED and quarantine it rather
/// than register it with offset 0 and silently age its entries. This exercises
/// the real ingest path (which flags the SST `descriptor#bulk_ingested`) plus
/// repair, guarding the ingest → flag → quarantine chain end to end.
#[test]
fn repair_quarantines_a_bulk_ingested_table() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let visible_seqno = SequenceNumberCounter::default();

    {
        let tree = Config::new(&folder, seqno.clone(), visible_seqno.clone()).open()?;
        let mut ingestion = tree.ingestion()?;
        for x in 0..64u64 {
            ingestion.write(x.to_be_bytes(), b"v")?;
        }
        ingestion.finish()?;
    }

    // The ingested SST on disk before repair. Quarantine must MOVE it out of
    // tables/ (not merely omit it from the rebuilt manifest), or the next open's
    // orphan cleanup would delete the only copy of the preserved original.
    let ingested_sst = std::fs::read_dir(folder.path().join("tables"))?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
        })
        .expect("the ingest produced a numerically-named SST file");

    // The ingest allocated a nonzero global_seqno (a manifest-only offset the
    // SST does not carry), so repair cannot reconstruct it.
    assert!(
        seqno.get() > 0,
        "the ingest allocated a global_seqno offset"
    );

    let report = Config::new(&folder, seqno, visible_seqno).repair()?;
    assert_eq!(
        report.recovered, 0,
        "the bulk-ingested table must not be recovered with a lost offset: {report:?}",
    );
    assert_eq!(
        report.unreadable, 1,
        "the bulk-ingested table is quarantined: {:?}",
        report.unreadable_files,
    );
    assert!(
        report.unreadable_files[0].1.contains("sequence offset"),
        "the reason names the unrecoverable ingest offset: {:?}",
        report.unreadable_files,
    );
    assert!(
        !ingested_sst.exists(),
        "the quarantined SST must be moved out of tables/ ({}), not left in place",
        ingested_sst.display(),
    );

    Ok(())
}

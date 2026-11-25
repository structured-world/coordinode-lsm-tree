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
/// offset for its effective MVCC ordering, so a manifest-LOSS repair (which
/// cannot recover that offset from the SST alone) must FAIL CLOSED and drop
/// it rather than register it with offset 0 and silently age its entries.
/// With a CLEAN manifest the repair reuses the recorded offset instead, so
/// the fixture loses the manifest first. This exercises the real ingest path
/// (which flags the SST `descriptor#bulk_ingested`) plus repair, guarding
/// the ingest → flag → drop chain end to end.
#[test]
fn repair_drops_a_bulk_ingested_table() -> lsm_tree::Result<()> {
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

    // Lose the manifest: with a CLEAN manifest the repair would reuse the
    // recorded offset and keep the table, so only a manifest loss exercises
    // the fail-closed drop.
    for entry in std::fs::read_dir(folder.path())? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let is_manifest = name
            .strip_prefix('v')
            .or_else(|| name.strip_prefix("edits-"))
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_manifest {
            std::fs::remove_file(entry.path())?;
        }
    }

    let report = Config::new(&folder, seqno, visible_seqno).repair()?;
    assert_eq!(
        report.recovered, 0,
        "the bulk-ingested table must not be recovered with a lost offset: {report:?}",
    );
    assert_eq!(
        report.unreadable, 1,
        "the bulk-ingested table is dropped: {:?}",
        report.unreadable_files,
    );
    assert!(
        report.unreadable_files[0].1.contains("sequence offset"),
        "the reason names the unrecoverable ingest offset: {:?}",
        report.unreadable_files,
    );
    assert!(
        !ingested_sst.exists(),
        "the dropped SST must be removed from tables/ ({}), not left in place",
        ingested_sst.display(),
    );

    Ok(())
}

/// Removes every manifest snapshot and edit log of the tree in `folder`.
fn lose_the_manifest(folder: &std::path::Path) -> lsm_tree::Result<()> {
    for entry in std::fs::read_dir(folder)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let is_manifest = name
            .strip_prefix('v')
            .or_else(|| name.strip_prefix("edits-"))
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_manifest {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

/// A replacing ingestion installs its tables at seqno 0, the offset a repair
/// rebuilds a lost manifest with, so the repair keeps them, index tables and
/// blob files alike, and the tree reads the replaced content alone. Guards
/// against the replacing tables being flagged as an adding ingestion's, whose
/// offset a lost manifest takes with it and which the repair drops.
#[test]
fn repair_keeps_a_replacing_ingestion() -> lsm_tree::Result<()> {
    for blob in [false, true] {
        let folder = get_tmp_folder();
        let seqno = SequenceNumberCounter::default();
        let visible_seqno = SequenceNumberCounter::default();
        let config = || {
            let config = Config::new(&folder, seqno.clone(), visible_seqno.clone());
            if blob {
                config.with_kv_separation(Some(
                    KvSeparationOptions::default().separation_threshold(1),
                ))
            } else {
                config
            }
        };

        {
            let tree = config().open()?;
            for x in 0..64u64 {
                tree.insert(x.to_be_bytes(), b"old", seqno.next());
            }
            tree.flush_active_memtable(0)?;
            let mut ingestion = tree.replacing_ingestion()?;
            for x in 32..96u64 {
                ingestion.write(x.to_be_bytes(), b"new")?;
            }
            ingestion.finish()?;
            // The earlier tables are deleted before the manifest goes.
            tree.close().wait();
        }

        lose_the_manifest(folder.path())?;
        let report = config().repair()?;
        assert_eq!(report.unreadable, 0, "blob: {blob}, {report:?}");
        assert!(report.recovered > 0, "blob: {blob}, {report:?}");

        let tree = config().open()?;
        assert_eq!(tree.iter(SeqNo::MAX, None).count(), 64, "blob: {blob}");
        for x in 32..96u64 {
            assert_eq!(
                tree.get(x.to_be_bytes(), SeqNo::MAX)?.as_deref(),
                Some(b"new".as_slice()),
                "blob: {blob}"
            );
        }
        assert!(tree.get(0u64.to_be_bytes(), SeqNo::MAX)?.is_none());
    }
    Ok(())
}

/// The files with bytes under `folder/sub`, each read whole: the tables or
/// blob files as they lie, the empty files a folder keeps for tables to come
/// left out.
fn files_with_bytes(
    folder: &std::path::Path,
    sub: &str,
) -> lsm_tree::Result<Vec<(std::path::PathBuf, Vec<u8>)>> {
    let mut files = Vec::new();
    let Ok(entries) = std::fs::read_dir(folder.join(sub)) else {
        return Ok(files);
    };
    for entry in entries {
        let entry = entry?;
        if entry.metadata()?.len() > 0 {
            files.push((entry.path(), std::fs::read(entry.path())?));
        }
    }
    Ok(files)
}

/// Copies the directory `from` into `to`, as a power loss leaves it.
fn copy_dir(from: &std::path::Path, to: &std::path::Path) -> lsm_tree::Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}

fn config_at(folder: &std::path::Path, seqno: &SequenceNumberCounter, blob: bool) -> Config {
    let config = Config::new(folder, seqno.clone(), SequenceNumberCounter::default());
    if blob {
        config.with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    } else {
        config
    }
}

/// An earlier table whose removal the replace's install left undone, and a
/// manifest lost after that: the repair rebuilds the replaced content and what
/// was written after it, and none of the earlier table's rows, which sit at
/// higher seqnos than the replaced ones and would read over them. Guards
/// against a repair that cannot tell what a replace superseded.
#[test]
fn repair_after_a_replace_drops_an_earlier_table_left_on_the_disk() -> lsm_tree::Result<()> {
    for blob in [false, true] {
        let folder = get_tmp_folder();
        let seqno = SequenceNumberCounter::default();
        // Seqnos from 1: an earlier row then reads over a replaced one.
        seqno.fetch_max(1);
        // Small tables: the ingestion's run is several of them.
        let config = || config_at(folder.path(), &seqno, blob).table_target_size(4_096);

        let mut left = Vec::new();
        {
            let tree = config().open()?;
            for x in 0..64u64 {
                tree.insert(x.to_be_bytes(), b"old", seqno.next());
            }
            tree.flush_active_memtable(0)?;
            left.extend(files_with_bytes(folder.path(), "tables")?);
            left.extend(files_with_bytes(folder.path(), "blobs")?);
            assert!(
                !left.is_empty(),
                "self-check: the earlier content is on disk"
            );

            let earlier = files_with_bytes(folder.path(), "tables")?.len();
            let mut ingestion = tree.replacing_ingestion()?;
            for x in 32..20_000u64 {
                ingestion.write(x.to_be_bytes(), b"new")?;
            }
            ingestion.finish()?;
            assert!(
                tree.table_count() > 1,
                "self-check: the run is several tables, blob: {blob}"
            );
            assert!(earlier > 0, "self-check: an earlier table, blob: {blob}");
            tree.insert(200u64.to_be_bytes(), b"later", seqno.next());
            tree.flush_active_memtable(0)?;
            tree.close().wait();
        }

        // The earlier files back where they lay, as when their removal failed.
        for (path, bytes) in &left {
            assert!(
                !path.exists(),
                "self-check: {} was removed, blob: {blob}",
                path.display()
            );
            std::fs::write(path, bytes)?;
        }
        lose_the_manifest(folder.path())?;
        let report = config().repair()?;
        assert_eq!(report.unreadable, 0, "blob: {blob}, {report:?}");

        let tree = config().open()?;
        assert_eq!(tree.iter(SeqNo::MAX, None).count(), 19_968, "blob: {blob}");
        for x in 32..96u64 {
            assert_eq!(
                tree.get(x.to_be_bytes(), SeqNo::MAX)?.as_deref(),
                Some(b"new".as_slice()),
                "key {x}, blob: {blob}"
            );
        }
        assert!(tree.get(0u64.to_be_bytes(), SeqNo::MAX)?.is_none());
        assert_eq!(
            tree.get(200u64.to_be_bytes(), SeqNo::MAX)?.as_deref(),
            Some(b"later".as_slice()),
            "blob: {blob}"
        );
        tree.close().wait();

        // A second loss of the manifest rebuilds the same tree: what the
        // repair kept still says where it belongs.
        lose_the_manifest(folder.path())?;
        config().repair()?;
        let tree = config().open()?;
        assert_eq!(tree.iter(SeqNo::MAX, None).count(), 19_968, "blob: {blob}");
        assert!(tree.get(0u64.to_be_bytes(), SeqNo::MAX)?.is_none());
    }
    Ok(())
}

/// A replacing ingestion the power cut short before its install, and a
/// manifest lost before the next open: the repair rebuilds the earlier content
/// alone. The tables the ingestion had finished are on the disk, named by no
/// install. Guards against a repair adopting them beside the content they
/// never replaced.
#[test]
fn repair_drops_a_replacing_ingestion_that_never_installed() -> lsm_tree::Result<()> {
    for blob in [false, true] {
        let folder = get_tmp_folder();
        let crashed = get_tmp_folder();
        let seqno = SequenceNumberCounter::default();
        seqno.fetch_max(1);
        let config =
            |path: &std::path::Path| config_at(path, &seqno, blob).table_target_size(4_096);

        {
            let tree = config(folder.path()).open()?;
            for x in 0..64u64 {
                tree.insert(x.to_be_bytes(), b"old", seqno.next());
            }
            tree.flush_active_memtable(0)?;
            tree.persist_flushes()?;
            let earlier = files_with_bytes(folder.path(), "tables")?.len();

            let mut ingestion = tree.replacing_ingestion()?;
            for x in 32..20_000u64 {
                ingestion.write(x.to_be_bytes(), b"new")?;
            }
            assert!(
                files_with_bytes(folder.path(), "tables")?.len() > earlier + 1,
                "self-check: the ingestion finished tables, blob: {blob}"
            );
            // The power goes here: the folder as it lies is what is left.
            copy_dir(folder.path(), crashed.path())?;
            drop(ingestion);
            tree.close().wait();
        }

        lose_the_manifest(crashed.path())?;
        config(crashed.path()).repair()?;

        let tree = config(crashed.path()).open()?;
        assert_eq!(tree.iter(SeqNo::MAX, None).count(), 64, "blob: {blob}");
        for x in 0..64u64 {
            assert_eq!(
                tree.get(x.to_be_bytes(), SeqNo::MAX)?.as_deref(),
                Some(b"old".as_slice()),
                "key {x}, blob: {blob}"
            );
        }
        assert!(tree.get(1_000u64.to_be_bytes(), SeqNo::MAX)?.is_none());
    }
    Ok(())
}

/// A compaction that merged a replacing ingestion's tables with later writes
/// leaves tables that say where they belong themselves: a repair keeps them.
#[test]
fn repair_keeps_a_compaction_of_a_replacing_ingestion() -> lsm_tree::Result<()> {
    for blob in [false, true] {
        let folder = get_tmp_folder();
        let seqno = SequenceNumberCounter::default();
        seqno.fetch_max(1);

        {
            let tree = config_at(folder.path(), &seqno, blob).open()?;
            for x in 0..64u64 {
                tree.insert(x.to_be_bytes(), b"old", seqno.next());
            }
            tree.flush_active_memtable(0)?;
            let mut ingestion = tree.replacing_ingestion()?;
            for x in 32..96u64 {
                ingestion.write(x.to_be_bytes(), b"new")?;
            }
            ingestion.finish()?;
            tree.insert(200u64.to_be_bytes(), b"later", seqno.next());
            tree.flush_active_memtable(0)?;
            tree.major_compact(u64::MAX, 0)?;
            tree.close().wait();
        }

        lose_the_manifest(folder.path())?;
        let report = config_at(folder.path(), &seqno, blob).repair()?;
        assert_eq!(report.unreadable, 0, "blob: {blob}, {report:?}");

        let tree = config_at(folder.path(), &seqno, blob).open()?;
        assert_eq!(tree.iter(SeqNo::MAX, None).count(), 65, "blob: {blob}");
        assert_eq!(
            tree.get(40u64.to_be_bytes(), SeqNo::MAX)?.as_deref(),
            Some(b"new".as_slice()),
            "blob: {blob}"
        );
    }
    Ok(())
}

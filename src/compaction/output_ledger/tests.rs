use super::*;
use crate::fs::{FaultFs, FsOpenOptions, MemFs};

fn create(fs: &dyn Fs, path: &crate::path::Path) -> crate::Result<()> {
    drop(fs.open(path, &FsOpenOptions::new().write(true).create_new(true))?);
    Ok(())
}

/// A run holding no room has nothing to charge a finished file to: it does
/// not even look the file up, and is never out of room.
#[test]
fn a_run_without_room_charges_nothing() -> crate::Result<()> {
    let fs = MemFs::new();
    let ledger = OutputLedger::default();
    ledger.landed(&fs, &PathBuf::from("/never/written"))?;
    assert!(!ledger.out_of_room());
    Ok(())
}

/// Every recorded file goes, a table and a blob file alike, and the ledger is
/// empty afterwards: a second removal has nothing left to touch.
#[test]
fn remove_uninstalled_deletes_every_recorded_file() -> crate::Result<()> {
    let fs: Arc<dyn Fs> = Arc::new(MemFs::new());
    let table = PathBuf::from("/tree/tables/7");
    let blob = PathBuf::from("/tree/blobs/3");
    fs.create_dir_all(&PathBuf::from("/tree/tables"))?;
    fs.create_dir_all(&PathBuf::from("/tree/blobs"))?;
    create(&*fs, &table)?;
    create(&*fs, &blob)?;

    let ledger = OutputLedger::default();
    ledger.record_table(7, table.clone(), fs.clone());
    ledger.record_blob_file(3, blob.clone(), fs.clone());
    ledger.remove_uninstalled(0, None);

    assert!(!fs.exists(&table)?, "the uninstalled table must be removed");
    assert!(
        !fs.exists(&blob)?,
        "the uninstalled blob file must be removed"
    );

    // A file written at the same path after the run must survive a stray
    // second call: the first one emptied the ledger.
    create(&*fs, &table)?;
    ledger.remove_uninstalled(0, None);
    assert!(fs.exists(&table)?, "a drained ledger removes nothing");
    Ok(())
}

/// An install hands every file recorded so far to the version: none of them
/// may be removed afterwards, while a file recorded after the install still is.
#[test]
fn installed_files_are_never_removed() -> crate::Result<()> {
    let fs: Arc<dyn Fs> = Arc::new(MemFs::new());
    fs.create_dir_all(&PathBuf::from("/tree/tables"))?;
    let installed = PathBuf::from("/tree/tables/1");
    let later = PathBuf::from("/tree/tables/2");
    create(&*fs, &installed)?;
    create(&*fs, &later)?;

    let ledger = OutputLedger::default();
    ledger.record_table(1, installed.clone(), fs.clone());
    ledger.installed();
    ledger.record_table(2, later.clone(), fs.clone());
    ledger.remove_uninstalled(0, None);

    assert!(fs.exists(&installed)?, "an installed output must stay");
    assert!(
        !fs.exists(&later)?,
        "an output recorded after the install goes"
    );
    Ok(())
}

/// A file already gone (a writer removed its own empty output) does not stop
/// the removal of the files recorded after it.
#[test]
fn a_missing_file_does_not_stop_the_rest() -> crate::Result<()> {
    let fs: Arc<dyn Fs> = Arc::new(MemFs::new());
    fs.create_dir_all(&PathBuf::from("/tree/tables"))?;
    let gone = PathBuf::from("/tree/tables/1");
    let present = PathBuf::from("/tree/tables/2");
    create(&*fs, &present)?;

    let ledger = OutputLedger::default();
    ledger.record_table(1, gone, fs.clone());
    ledger.record_table(2, present.clone(), fs.clone());
    ledger.remove_uninstalled(0, None);

    assert!(!fs.exists(&present)?, "the file after the missing one goes");
    Ok(())
}

/// An output the run cannot remove stays on disk, which a quota counted from
/// the installed version does not see: its bytes stay held in the tree's
/// quota, while the room of one removed is given back.
#[test]
fn an_output_that_cannot_be_removed_keeps_its_quota_held() -> crate::Result<()> {
    use crate::compaction::capacity::{CapacityPool, Demand};
    use crate::fs::{Fault, FaultOp, FaultRule};

    let fault = FaultFs::new(MemFs::new());
    let injector = fault.injector();
    let fs: Arc<dyn Fs> = Arc::new(fault);
    let folder = PathBuf::from("/tree/tables");
    fs.create_dir_all(&folder)?;
    let stuck = PathBuf::from("/tree/tables/1");
    let removed = PathBuf::from("/tree/tables/2");
    for (path, len) in [(&stuck, 300), (&removed, 200)] {
        let mut file = fs.open(path, &FsOpenOptions::new().write(true).create_new(true))?;
        file.write_all(&vec![7u8; len])?;
    }

    let tree_id = 4;
    let pool = Arc::new(CapacityPool::default());
    let Some(room) = pool.reserve(
        tree_id,
        &[Demand {
            fs: &*fs,
            path: &folder,
            bytes: 500,
        }],
        None,
    ) else {
        panic!("an unchecked reservation is never refused");
    };
    let ledger = OutputLedger::default();
    ledger.reserve(room);
    for (id, path) in [(1, &stuck), (2, &removed)] {
        ledger.record_table(id, path.clone(), fs.clone());
        ledger.landed(&*fs, path)?;
    }
    injector.arm(
        FaultRule::new(
            FaultOp::RemoveFile,
            Fault::Error(crate::io::ErrorKind::PermissionDenied),
        )
        .on_path("tables/1"),
    );
    ledger.remove_uninstalled(tree_id, None);
    injector.clear();

    assert!(fs.exists(&stuck)?, "the fixture must refuse the removal");
    assert!(!fs.exists(&removed)?);
    assert_eq!(pool.quota_held(tree_id), 300);
    Ok(())
}

/// An output that outgrew its run's room and cannot be removed keeps all of
/// its bytes held, past the room the run reserved too.
#[test]
fn an_output_past_its_room_that_cannot_be_removed_keeps_all_of_it_held() -> crate::Result<()> {
    use crate::compaction::capacity::{CapacityPool, Demand};
    use crate::fs::{Fault, FaultOp, FaultRule};

    let fault = FaultFs::new(MemFs::new());
    let injector = fault.injector();
    let fs: Arc<dyn Fs> = Arc::new(fault);
    let folder = PathBuf::from("/tree/tables");
    fs.create_dir_all(&folder)?;
    let stuck = PathBuf::from("/tree/tables/1");
    let mut file = fs.open(&stuck, &FsOpenOptions::new().write(true).create_new(true))?;
    file.write_all(&[7u8; 150])?;
    drop(file);

    let tree_id = 5;
    let pool = Arc::new(CapacityPool::default());
    let Some(room) = pool.reserve(
        tree_id,
        &[Demand {
            fs: &*fs,
            path: &folder,
            bytes: 100,
        }],
        None,
    ) else {
        panic!("an unchecked reservation is never refused");
    };
    let ledger = OutputLedger::default();
    ledger.reserve(room);
    ledger.record_table(1, stuck.clone(), fs.clone());
    ledger.landed(&*fs, &stuck)?;
    injector.arm(
        FaultRule::new(
            FaultOp::RemoveFile,
            Fault::Error(crate::io::ErrorKind::PermissionDenied),
        )
        .on_path("tables/1"),
    );
    ledger.remove_uninstalled(tree_id, None);
    injector.clear();

    assert!(fs.exists(&stuck)?, "the fixture must refuse the removal");
    assert_eq!(pool.quota_held(tree_id), 150);
    Ok(())
}

/// A cached descriptor keeps the file open, and a backend that refuses to
/// unlink an open file (as Windows does) would leave it behind: the
/// descriptor is evicted first, for a table and a blob file alike.
#[test]
fn cached_descriptors_are_evicted_before_the_unlink() -> crate::Result<()> {
    let fault = FaultFs::new(MemFs::new());
    fault.injector().refuse_removing_open_files();
    let fs: Arc<dyn Fs> = Arc::new(fault);
    fs.create_dir_all(&PathBuf::from("/tree/tables"))?;
    fs.create_dir_all(&PathBuf::from("/tree/blobs"))?;
    let table = PathBuf::from("/tree/tables/5");
    let blob = PathBuf::from("/tree/blobs/5");
    create(&*fs, &table)?;
    create(&*fs, &blob)?;

    let tree_id = 9;
    // Room for both entries in every shard, so neither is evicted on insert.
    let descriptors = DescriptorTable::new(1_000);
    let open = |path: &PathBuf| -> crate::Result<Arc<dyn crate::fs::FsFile>> {
        Ok(Arc::from(fs.open(path, &FsOpenOptions::new().read(true))?))
    };
    descriptors.insert_for_table((tree_id, 5).into(), open(&table)?);
    descriptors.insert_for_blob_file((tree_id, 5).into(), open(&blob)?);
    assert!(
        fs.remove_file(&table).is_err(),
        "the fixture must refuse to unlink a file a descriptor holds open",
    );

    let ledger = OutputLedger::default();
    ledger.record_table(5, table.clone(), fs.clone());
    ledger.record_blob_file(5, blob.clone(), fs.clone());
    ledger.remove_uninstalled(tree_id, Some(&descriptors));

    assert!(
        !fs.exists(&table)?,
        "the table's descriptor must not pin it"
    );
    assert!(
        !fs.exists(&blob)?,
        "the blob file's descriptor must not pin it"
    );
    Ok(())
}

/// The install of a merge fails after every output was finished and opened,
/// on a backend that refuses to unlink a file still held open, as Windows
/// does. The run removes them all the same: by the time it does, no handle of
/// its own and no cached descriptor keeps one open.
#[test]
fn a_failed_install_leaves_no_output_where_open_files_cannot_be_unlinked() -> crate::Result<()> {
    use crate::fs::{Fault, FaultOp, FaultRule, StdFs};
    use crate::{AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter};

    /// The table and blob files under `root`, as `folder/name`.
    fn listing(root: &std::path::Path) -> crate::Result<std::collections::BTreeSet<String>> {
        let mut all = std::collections::BTreeSet::new();
        for folder in [crate::file::TABLES_FOLDER, crate::file::BLOBS_FOLDER] {
            for entry in std::fs::read_dir(root.join(folder))? {
                let name = entry?.file_name().to_string_lossy().into_owned();
                all.insert(alloc::format!("{folder}/{name}"));
            }
        }
        Ok(all)
    }
    let value = |i: u64, generation: u64| {
        let mut v = alloc::format!("gen{generation}-{i}-").into_bytes();
        v.resize(220, b'v');
        v
    };

    let dir = tempfile::tempdir()?;
    let fault = FaultFs::new(StdFs);
    let injector = fault.injector();
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(crate::config::BlockSizePolicy::all(512))
    .with_kv_separation(Some(
        KvSeparationOptions::default()
            .separation_threshold(64)
            .age_cutoff(1.0)
            .staleness_threshold(0.1)
            .file_target_size(16 * 1024),
    ))
    .with_fs(fault)
    .open()?;
    for i in 0..1_000 {
        tree.insert(alloc::format!("key_{i:06}"), value(i, 0), i);
    }
    tree.flush_active_memtable(0)?;
    // Half of the first generation goes stale. The first merge drops it and
    // records the dead half, so the second relocates the rest and writes blob
    // files as well as tables.
    for i in (0..1_000).step_by(2) {
        tree.insert(alloc::format!("key_{i:06}"), value(i, 1), 1_000 + i);
    }
    tree.flush_active_memtable(0)?;
    let past_every_version = 10_000;
    tree.major_compact(u64::MAX, past_every_version)?;

    let before = listing(dir.path())?;
    injector.refuse_removing_open_files();
    // The edit log append is the install's commit point.
    injector.arm(
        FaultRule::new(FaultOp::Write, Fault::Error(crate::io::ErrorKind::Other)).on_path("edits-"),
    );

    let result = tree.major_compact(16 * 1024, past_every_version);
    injector.clear();
    assert!(
        result.is_err(),
        "the refused version install must fail the merge: {result:?}"
    );
    // Only new files count: the inputs the first merge retired may still be
    // on their way out, unlinked in the background.
    let left_behind: Vec<String> = listing(dir.path())?.difference(&before).cloned().collect();
    assert!(
        left_behind.is_empty(),
        "every output of the failed install must be removed: {left_behind:?}",
    );
    for i in 0..1_000 {
        let generation = u64::from(i % 2 == 0);
        assert_eq!(
            tree.get(alloc::format!("key_{i:06}"), crate::MAX_SEQNO)?
                .as_deref(),
            Some(value(i, generation).as_slice()),
            "key {i} after the failed install",
        );
    }
    Ok(())
}

/// A run removes only the files it created itself. Options can be shared by
/// runs that overlap, and an output another run recorded through them is that
/// run's to install or remove: here a run with nothing to do must leave it.
#[test]
fn a_run_leaves_the_outputs_another_run_recorded() -> crate::Result<()> {
    use crate::compaction::worker::{Options, do_compaction};
    use crate::{Config, SequenceNumberCounter};

    let dir = tempfile::tempdir()?;
    let crate::AnyTree::Standard(tree) = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?
    else {
        panic!("a standard tree");
    };
    let opts = Options::from_tree(&tree, Arc::new(crate::compaction::Leveled::default()));

    let pending = dir.path().join("another-runs-output");
    std::fs::write(&pending, b"in flight")?;
    opts.outputs
        .record_table(1_000, pending.clone(), Arc::clone(&tree.config.fs));

    do_compaction(&opts)?;
    assert!(
        pending.exists(),
        "a run must not remove an output another run recorded and may still install",
    );
    Ok(())
}

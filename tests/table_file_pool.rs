// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Tables written into files a tables folder made ahead of them: what a
//! flush, a compaction, an ingestion, an open and a repair do with the empty
//! files a folder keeps, and what a power loss leaves of them.

use lsm_tree::config::LevelRoute;
use lsm_tree::fs::{CrashFs, Fault, FaultFs, FaultOp, FaultRule, MemFs, StdFs, SyncKind};
use lsm_tree::{AbstractTree, AnyTree, Config, SequenceNumberCounter, get_tmp_folder};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use test_log::test;

fn config(folder: &Path) -> Config {
    Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
}

/// The ids of the files in `folder` named like tables, split into the empty
/// ones (pooled) and the ones holding a table.
fn table_files(folder: &Path) -> (Vec<u64>, Vec<u64>) {
    let mut pooled = Vec::new();
    let mut tables = Vec::new();
    for entry in std::fs::read_dir(folder).expect("tables folder") {
        let entry = entry.expect("entry");
        let Ok(id) = entry.file_name().to_string_lossy().parse::<u64>() else {
            continue;
        };
        if entry.metadata().expect("metadata").len() == 0 {
            pooled.push(id);
        } else {
            tables.push(id);
        }
    }
    pooled.sort_unstable();
    tables.sort_unstable();
    (pooled, tables)
}

/// A flush after an ingestion holds newer data than it, whatever ids their
/// files were made under: an ingestion takes its place in L0 when it is
/// installed, and a flush when it starts, while a pooled file's id was taken
/// when its batch was made, before the ingestion.
#[test]
fn a_flush_after_an_ingestion_reads_as_newer_than_it() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let tree = Config::new(&folder, seqno.clone(), SequenceNumberCounter::default()).open()?;

    tree.insert("key", "flushed", seqno.next());
    tree.flush_active_memtable(0)?;

    let mut ingestion = tree.ingestion()?;
    ingestion.write("key", "ingested")?;
    ingestion.finish()?;
    // The flush below writes at the ingestion's own sequence number: only the
    // order of the tables tells the newer one.
    let ingested_at = seqno.get() - 1;

    tree.insert("key", "newer", ingested_at);
    tree.flush_active_memtable(0)?;
    assert_eq!(
        tree.get("key", u64::MAX)?.as_deref(),
        Some(b"newer".as_slice())
    );

    drop(tree);
    let tree = Config::new(&folder, seqno, SequenceNumberCounter::default()).open()?;
    assert_eq!(
        tree.get("key", u64::MAX)?.as_deref(),
        Some(b"newer".as_slice()),
        "after a reopen"
    );
    Ok(())
}

/// A compaction output's id is above every input's, even when its folder's
/// pool holds files made before an input: the id is what tells an output from
/// the input whose age it takes.
#[test]
fn a_compaction_output_is_numbered_above_its_inputs() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let hot = folder.path().join("hot");
    let cold = folder.path().join("cold");
    let fs: Arc<dyn lsm_tree::fs::Fs> = Arc::new(StdFs);
    let tree = config(folder.path())
        .table_file_pool(2)
        .level_routes(vec![
            LevelRoute {
                levels: 0..1,
                path: hot.clone(),
                fs: fs.clone(),
            },
            LevelRoute {
                levels: 1..7,
                path: cold.clone(),
                fs,
            },
        ])
        .open()?;

    // The cold folder makes its batch at the first compaction; the hot one
    // makes a later batch after it, so the cold pool holds ids below the
    // next flush's.
    tree.insert("a", "1", 0);
    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, 0)?;
    for (i, key) in ["b", "c"].into_iter().enumerate() {
        tree.insert(key, "1", 1 + i as u64);
        tree.flush_active_memtable(0)?;
    }
    let (_, hot_tables) = table_files(&hot.join("tables"));
    let (cold_pooled, cold_tables) = table_files(&cold.join("tables"));
    let inputs_max = hot_tables
        .iter()
        .chain(&cold_tables)
        .copied()
        .max()
        .expect("inputs");
    assert!(
        cold_pooled.iter().any(|id| *id < inputs_max),
        "self-check: the cold pool holds an id below an input's: {cold_pooled:?} < {inputs_max}"
    );

    tree.major_compact(u64::MAX, 0)?;
    let (_, outputs) = table_files(&cold.join("tables"));
    assert_eq!(outputs.len(), 1, "one output: {outputs:?}");
    assert!(
        outputs.iter().all(|id| *id > inputs_max),
        "{outputs:?} above every input, the highest {inputs_max}"
    );
    for key in ["a", "b", "c"] {
        assert!(tree.get(key, u64::MAX)?.is_some(), "{key}");
    }
    Ok(())
}

/// An open uses the pooled files it finds again, lowest id first, rather than
/// leaving them and making more; the ids it hands out after them are above
/// every one of them.
#[test]
fn an_open_uses_the_pooled_files_it_finds() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tables = folder.path().join("tables");
    {
        let tree = config(folder.path()).table_file_pool(4).open()?;
        tree.insert("a", "1", 0);
        tree.flush_active_memtable(0)?;
    }
    let (pooled, _) = table_files(&tables);
    assert_eq!(pooled.len(), 3, "self-check: the rest of the batch");

    let tree = config(folder.path()).table_file_pool(4).open()?;
    for (i, key) in ["b", "c", "d", "e"].into_iter().enumerate() {
        tree.insert(key, "1", 1 + i as u64);
        tree.flush_active_memtable(0)?;
    }
    let (now_pooled, written) = table_files(&tables);
    for id in &pooled {
        assert!(
            written.contains(id),
            "pooled file {id} written: {written:?}"
        );
    }
    let above = written
        .iter()
        .copied()
        .filter(|id| !pooled.contains(id))
        .min()
        .expect("a table past the found ones");
    assert!(
        pooled.iter().all(|id| *id < above),
        "a new batch's ids are above the found ones"
    );
    assert!(now_pooled.iter().all(|id| *id > above - 1));
    for key in ["a", "b", "c", "d", "e"] {
        assert!(tree.get(key, u64::MAX)?.is_some(), "{key}");
    }
    Ok(())
}

/// A pooled file another name links to shares its inode with whatever made
/// that link (a capture of the folder): it is unlinked from the pool, never
/// written, so the other name keeps the empty file it linked.
#[test]
fn a_pooled_file_linked_elsewhere_is_never_written() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tables = folder.path().join("tables");
    let tree = config(folder.path()).table_file_pool(4).open()?;
    tree.insert("a", "1", 0);
    tree.flush_active_memtable(0)?;

    let (pooled, _) = table_files(&tables);
    let next = *pooled.first().expect("a pooled file");
    let capture = folder.path().join("captured");
    std::fs::hard_link(tables.join(next.to_string()), &capture)?;

    tree.insert("b", "1", 1);
    tree.flush_active_memtable(0)?;
    assert_eq!(
        std::fs::metadata(&capture)?.len(),
        0,
        "the captured link is never written"
    );
    assert!(
        !tables.join(next.to_string()).exists(),
        "the linked file left the pool"
    );
    assert!(tree.get("b", u64::MAX)?.is_some());
    Ok(())
}

/// A repair of a tree holding pooled files finds the same tables, and reports
/// the same losses (none), as one without them: an empty file under a table
/// name holds no rows. It removes them as it removes every file the rebuilt
/// manifest does not name, and the tree makes new ones.
#[test]
fn a_repair_takes_no_pooled_file_for_a_lost_table() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tables = folder.path().join("tables");
    {
        let tree = config(folder.path()).table_file_pool(4).open()?;
        for (i, key) in ["a", "b"].into_iter().enumerate() {
            tree.insert(key, "1", i as u64);
            tree.flush_active_memtable(0)?;
        }
    }
    let (pooled, _) = table_files(&tables);
    assert!(!pooled.is_empty(), "self-check: pooled files are there");
    for entry in std::fs::read_dir(folder.path())? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('v') || name.starts_with("edits-") {
            std::fs::remove_file(entry.path())?;
        }
    }

    let report = config(folder.path()).repair()?;
    assert_eq!(report.recovered, 2);
    assert_eq!(report.unreadable, 0, "{:?}", report.unreadable_files);
    assert!(report.unknowable_losses.is_empty());
    assert!(report.lost_coverage.is_empty());
    assert_eq!(
        report.wal_replay_scope(),
        lsm_tree::WalReplayScope::TailOnly
    );
    let (left, _) = table_files(&tables);
    assert!(left.is_empty(), "pooled files removed: {left:?}");

    let (tree, _) = config(folder.path()).open_or_repair(lsm_tree::RepairPolicy::default())?;
    tree.insert("c", "1", 2);
    tree.flush_active_memtable(0)?;
    for key in ["a", "b", "c"] {
        assert!(tree.get(key, u64::MAX)?.is_some(), "{key}");
    }
    Ok(())
}

const DB: &str = "/db/tree";

fn crash_config(fs: &Arc<FaultFs<CrashFs>>) -> Config {
    Config::new(
        DB,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(fs.clone())
    .table_file_pool(4)
}

/// The rule that fails the sync `at` of `syncs`, the syncs an operation
/// issued in a run that failed none: the same kind of sync on a path the
/// rule matches, skipping the ones before it that match as well.
fn fail_sync(syncs: &[(PathBuf, SyncKind)], at: usize) -> FaultRule {
    let (path, kind) = syncs.get(at).expect("a sync");
    let op = match kind {
        SyncKind::Directory => FaultOp::SyncDirectory,
        SyncKind::FileData => FaultOp::SyncData,
        SyncKind::File => FaultOp::SyncAll,
        other => panic!("no {other:?} sync expected"),
    };
    let needle = path.to_string_lossy().into_owned();
    let skip = syncs
        .iter()
        .take(at)
        .filter(|(earlier, earlier_kind)| {
            earlier_kind == kind && earlier.to_string_lossy().contains(&needle)
        })
        .count();
    FaultRule::new(op, Fault::Error(lsm_tree::io::ErrorKind::Other))
        .on_path(needle)
        .skip(skip as u64)
        .once()
}

/// Runs `operation` on a tree `setup` built, over a store that loses power
/// right after each prefix of the syncs `operation` issues, and hands the
/// reopened tree to `check` with whether the whole operation was synced.
fn at_every_sync_prefix(
    setup: impl Fn(&AnyTree),
    operation: impl Fn(&AnyTree) -> lsm_tree::Result<()>,
    check: impl Fn(&AnyTree, bool),
) {
    let syncs = {
        let fs = Arc::new(FaultFs::new(CrashFs::new(MemFs::new())));
        let tree = crash_config(&fs).open().expect("create");
        setup(&tree);
        let _ = fs.injector().take_syncs();
        operation(&tree).expect("the operation");
        fs.injector().take_syncs()
    };
    assert!(!syncs.is_empty(), "self-check: the operation syncs");

    for at in 0..=syncs.len() {
        let crash = CrashFs::new(MemFs::new());
        let fs = Arc::new(FaultFs::new(crash.clone()));
        {
            let tree = crash_config(&fs).open().expect("create");
            setup(&tree);
            if let Some(rule) = (at < syncs.len()).then(|| fail_sync(&syncs, at)) {
                fs.injector().arm(rule);
            }
            let finished = operation(&tree);
            assert_eq!(
                finished.is_ok(),
                at == syncs.len(),
                "self-check: the operation fails at sync {at} of {syncs:?}"
            );
        }
        crash.crash();
        let fs = Arc::new(FaultFs::new(crash.clone()));
        let tree = crash_config(&fs)
            .open()
            .unwrap_or_else(|e| panic!("reopens after a power loss past {at} syncs: {e}"));
        check(&tree, at == syncs.len());

        // What the power loss left is a tree the pool keeps working in.
        tree.insert("after", "1", 1_000);
        tree.flush_active_memtable(0)
            .expect("a flush after the power loss");
        assert!(tree.get("after", u64::MAX).expect("get").is_some());
    }
}

/// A flush into a pooled file, the first one (which makes the batch) or a
/// later one, leaves after a power loss at any point of it either the tree
/// before it or the tree with its keys, and no file the open takes for data
/// it is not.
#[test]
fn a_flush_survives_a_power_loss_at_every_sync() {
    for warm in [false, true] {
        at_every_sync_prefix(
            |tree| {
                if warm {
                    tree.insert("warm", "1", 0);
                    tree.flush_active_memtable(0).expect("warm the pool");
                }
            },
            |tree| {
                tree.insert("key", "value", 10);
                tree.flush_active_memtable(0)
            },
            |tree, synced| {
                let key = tree.get("key", u64::MAX).expect("get");
                if synced {
                    assert_eq!(key.as_deref(), Some(b"value".as_slice()), "warm: {warm}");
                } else {
                    assert!(key.is_none(), "warm: {warm}");
                }
                if warm {
                    assert!(tree.get("warm", u64::MAX).expect("get").is_some());
                }
            },
        );
    }
}

/// A compaction into a pooled file keeps every key through a power loss at
/// any point of it: the tree reopens either before it or after it.
#[test]
fn a_compaction_survives_a_power_loss_at_every_sync() {
    at_every_sync_prefix(
        |tree| {
            for i in 0..3u64 {
                tree.insert(format!("key{i}"), "value", i);
                tree.flush_active_memtable(0).expect("flush");
            }
        },
        |tree| tree.major_compact(u64::MAX, 0).map(drop),
        |tree, _| {
            for i in 0..3u64 {
                assert!(
                    tree.get(format!("key{i}"), u64::MAX)
                        .expect("get")
                        .is_some(),
                    "key{i}"
                );
            }
        },
    );
}

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
/// ones (pooled) and the ones holding a table. A file the tree unlinks in the
/// background between the listing and its stat is gone, and not counted.
fn table_files(folder: &Path) -> (Vec<u64>, Vec<u64>) {
    let mut pooled = Vec::new();
    let mut tables = Vec::new();
    for entry in std::fs::read_dir(folder).expect("tables folder") {
        let entry = entry.expect("entry");
        let Ok(id) = entry.file_name().to_string_lossy().parse::<u64>() else {
            continue;
        };
        match entry.metadata() {
            Ok(meta) if meta.len() == 0 => pooled.push(id),
            Ok(_) => tables.push(id),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => panic!("metadata of {}: {e}", entry.path().display()),
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
    for (seqno, key) in [(1, "b"), (2, "c")] {
        tree.insert(key, "1", seqno);
        tree.flush_active_memtable(0)?;
    }
    // The tables come from the tree, not from a folder listing: a replaced
    // table's file can outlive its removal from the tree while a reader still
    // holds it open, and Windows deletes such a file only once it is closed.
    let live = || -> Vec<u64> {
        let mut ids: Vec<u64> = tree
            .current_version()
            .iter_tables()
            .map(|t| t.id())
            .collect();
        ids.sort_unstable();
        ids
    };
    let inputs_max = live().into_iter().max().expect("inputs");
    let (cold_pooled, _) = table_files(&cold.join("tables"));
    assert!(
        cold_pooled.iter().any(|id| *id < inputs_max),
        "self-check: the cold pool holds an id below an input's: {cold_pooled:?} < {inputs_max}"
    );

    tree.major_compact(u64::MAX, 0)?;
    let outputs = live();
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
    let (pooled, before) = table_files(&tables);
    assert_eq!(pooled.len(), 3, "self-check: the rest of the batch");

    let tree = config(folder.path()).table_file_pool(4).open()?;
    for (seqno, key) in [(1, "b"), (2, "c"), (3, "d"), (4, "e")] {
        tree.insert(key, "1", seqno);
        tree.flush_active_memtable(0)?;
    }
    let (now_pooled, written) = table_files(&tables);
    for id in &pooled {
        assert!(
            written.contains(id),
            "pooled file {id} written: {written:?}"
        );
    }
    // The fourth flush found the pool empty and made a batch.
    let fresh = written
        .iter()
        .copied()
        .filter(|id| !pooled.contains(id) && !before.contains(id))
        .min()
        .expect("a table past the found ones");
    assert!(
        pooled.iter().all(|id| *id < fresh),
        "a new batch's ids are above the found ones"
    );
    assert!(
        now_pooled.iter().all(|id| *id > fresh),
        "{now_pooled:?} the rest of the new batch"
    );
    for key in ["a", "b", "c", "d", "e"] {
        assert!(tree.get(key, u64::MAX)?.is_some(), "{key}");
    }
    Ok(())
}

/// An empty symlink under a table name is not a file the tree made: an open
/// does not pool it, so no table is ever written through it into the file it
/// points to.
#[cfg(unix)]
#[test]
fn an_open_pools_no_symlink() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tables = folder.path().join("tables");
    {
        let tree = config(folder.path()).table_file_pool(4).open()?;
        tree.insert("a", "1", 0);
        tree.flush_active_memtable(0)?;
    }
    let (pooled, _) = table_files(&tables);
    for id in pooled {
        std::fs::remove_file(tables.join(id.to_string()))?;
    }
    let outside = folder.path().join("outside");
    std::fs::write(&outside, b"")?;
    std::os::unix::fs::symlink(&outside, tables.join("1000"))?;

    let tree = config(folder.path()).table_file_pool(4).open()?;
    tree.insert("b", "1", 1);
    tree.flush_active_memtable(0)?;
    assert_eq!(
        std::fs::metadata(&outside)?.len(),
        0,
        "no table is written through the symlink"
    );
    assert!(tree.get("b", u64::MAX)?.is_some());
    Ok(())
}

/// An empty file whose name no table id can reach (the counter's top, past
/// it) is not a pooled file: an open does not start its ids above it, which
/// would leave none to hand out.
#[test]
fn an_open_pools_no_name_past_the_ids() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tables = folder.path().join("tables");
    {
        let tree = config(folder.path()).open()?;
        tree.insert("a", "1", 0);
        tree.flush_active_memtable(0)?;
    }
    for name in [i64::MAX.to_string(), u64::MAX.to_string()] {
        std::fs::write(tables.join(name), b"")?;
    }

    let tree = config(folder.path()).open()?;
    tree.insert("b", "1", 1);
    tree.flush_active_memtable(0)?;
    assert!(tree.get("a", u64::MAX)?.is_some());
    assert!(tree.get("b", u64::MAX)?.is_some());
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

/// Removes a tree's manifest: its snapshots and edit logs.
fn remove_manifest(folder: &Path) -> lsm_tree::Result<()> {
    for entry in std::fs::read_dir(folder)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('v') || name.starts_with("edits-") {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

/// A tree of two flushed tables over a pool of four, closed.
fn two_tables(folder: &Path) -> lsm_tree::Result<()> {
    let tree = config(folder).table_file_pool(4).open()?;
    for (seqno, key) in [(0, "a"), (1, "b")] {
        tree.insert(key, "1", seqno);
        tree.flush_active_memtable(0)?;
    }
    Ok(())
}

/// A repair of a tree holding pooled files finds the same tables, and reports
/// the same losses (none), as one without them: an empty file under a table
/// name holds no rows. It removes them as it removes every file the rebuilt
/// manifest does not name, lists them among the files it left out, and the
/// tree makes new ones.
#[test]
fn a_repair_takes_no_pooled_file_for_a_lost_table() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tables = folder.path().join("tables");
    two_tables(folder.path())?;
    let (pooled, _) = table_files(&tables);
    assert!(!pooled.is_empty(), "self-check: pooled files are there");
    remove_manifest(folder.path())?;

    let report = config(folder.path()).repair()?;
    assert_eq!(report.recovered, 2);
    assert_eq!(report.unreadable, 0, "{:?}", report.unreadable_files);
    assert!(report.unknowable_losses.is_empty());
    assert!(report.lost_coverage.is_empty());
    assert_eq!(
        report.wal_replay_scope(),
        lsm_tree::WalReplayScope::TailOnly
    );
    let mut excluded: Vec<u64> = report
        .excluded_files
        .iter()
        .filter_map(|(path, _)| path.file_name()?.to_str()?.parse().ok())
        .collect();
    excluded.sort_unstable();
    assert_eq!(excluded, pooled, "the pooled files are reported left out");
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

/// A pooled file's id is above every table written into its folder, so an
/// empty file below the folder's newest table is no pooled file: a table
/// whose bytes were lost. A repair without a manifest reports it as a loss
/// it cannot scope, rather than remove it as pooled.
#[test]
fn a_repair_reports_an_emptied_table_below_the_newest_as_lost() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tables = folder.path().join("tables");
    two_tables(folder.path())?;
    let (_, written) = table_files(&tables);
    let older = *written.first().expect("two tables");
    std::fs::File::create(tables.join(older.to_string()))?.set_len(0)?;
    remove_manifest(folder.path())?;

    let report = config(folder.path()).repair()?;
    assert_eq!(report.recovered, 1);
    let lost: Vec<String> = report
        .unknowable_losses
        .iter()
        .filter_map(|path| Some(path.file_name()?.to_string_lossy().into_owned()))
        .collect();
    assert_eq!(lost, vec![older.to_string()], "{report:?}");
    assert_ne!(
        report.wal_replay_scope(),
        lsm_tree::WalReplayScope::TailOnly
    );
    Ok(())
}

/// The newest table of a folder counts whether the repair keeps it or sets
/// it aside: an empty file below an ingested table, which a repair without a
/// manifest cannot place and drops, is a table that lost its bytes all the
/// same, never a pooled file.
#[test]
fn a_repair_counts_a_table_it_sets_aside_as_the_newest() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tables = folder.path().join("tables");
    {
        let tree = config(folder.path()).table_file_pool(4).open()?;
        for (seqno, key) in [(0, "a"), (1, "b")] {
            tree.insert(key, "1", seqno);
            tree.flush_active_memtable(0)?;
        }
        let mut ingestion = tree.ingestion()?;
        ingestion.write("c", "1")?;
        ingestion.finish()?;
    }
    let (_, written) = table_files(&tables);
    let [_, emptied, ingested] = written[..] else {
        panic!("self-check: two flushed tables and an ingested one: {written:?}");
    };
    std::fs::File::create(tables.join(emptied.to_string()))?.set_len(0)?;
    remove_manifest(folder.path())?;

    let report = config(folder.path()).repair()?;
    assert_eq!(report.recovered, 1, "{report:?}");
    let named = |paths: Vec<&PathBuf>| -> Vec<String> {
        paths
            .into_iter()
            .filter_map(|path| Some(path.file_name()?.to_string_lossy().into_owned()))
            .collect()
    };
    assert!(
        named(
            report
                .unreadable_files
                .iter()
                .map(|(path, _)| path)
                .collect()
        )
        .contains(&ingested.to_string()),
        "self-check: the ingested table is set aside: {report:?}"
    );
    assert!(
        named(report.unknowable_losses.iter().collect()).contains(&emptied.to_string()),
        "the emptied table is a loss: {report:?}"
    );
    assert_ne!(
        report.wal_replay_scope(),
        lsm_tree::WalReplayScope::TailOnly
    );
    Ok(())
}

/// A copy of a table in a second folder is a table written into that folder,
/// even when the repair keeps the first copy and leaves this one out: an empty
/// file below it in that folder lost its bytes, never a pooled file.
#[test]
fn a_repair_counts_a_duplicate_it_leaves_out_as_its_folders_newest() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let hot = folder.path().join("hot");
    let cold = folder.path().join("cold");
    let fs: Arc<dyn lsm_tree::fs::Fs> = Arc::new(StdFs);
    let routed = || {
        config(folder.path()).table_file_pool(4).level_routes(vec![
            LevelRoute {
                levels: 0..1,
                path: hot.clone(),
                fs: fs.clone(),
            },
            LevelRoute {
                levels: 1..7,
                path: cold.clone(),
                fs: fs.clone(),
            },
        ])
    };
    {
        let tree = routed().open()?;
        for (seqno, key) in [(0, "a"), (1, "b"), (2, "c")] {
            tree.insert(key, "1", seqno);
            tree.flush_active_memtable(0)?;
        }
    }
    let hot_tables = hot.join("tables");
    let cold_tables = cold.join("tables");
    let (_, written) = table_files(&hot_tables);
    let [_, gone, newest] = written[..] else {
        panic!("self-check: three flushed tables: {written:?}");
    };
    // The cold folder holds a copy of the newest table and an empty file
    // under an id below it that no other folder holds.
    std::fs::remove_file(hot_tables.join(gone.to_string()))?;
    std::fs::create_dir_all(&cold_tables)?;
    std::fs::copy(
        hot_tables.join(newest.to_string()),
        cold_tables.join(newest.to_string()),
    )?;
    std::fs::write(cold_tables.join(gone.to_string()), b"")?;
    remove_manifest(folder.path())?;

    let report = routed().repair()?;
    let emptied = cold_tables.join(gone.to_string());
    assert!(
        report
            .excluded_files
            .iter()
            .any(|(path, _)| *path == cold_tables.join(newest.to_string())),
        "self-check: the cold copy is the one left out: {report:?}"
    );
    assert!(
        report.unknowable_losses.contains(&emptied),
        "the emptied table is a loss: {report:?}"
    );
    assert_ne!(
        report.wal_replay_scope(),
        lsm_tree::WalReplayScope::TailOnly
    );
    Ok(())
}

/// A checkpoint takes the tables the version names and no pooled file, and
/// the two trees go on apart: what either writes into its pooled files lands
/// in no file of the other.
#[test]
fn a_checkpoint_carries_no_pooled_file_and_shares_none() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tables = folder.path().join("tables");
    let copy = folder.path().join("copy");
    let tree = config(folder.path()).table_file_pool(4).open()?;
    for (seqno, key) in [(0, "a"), (1, "b")] {
        tree.insert(key, "1", seqno);
        tree.flush_active_memtable(0)?;
    }
    let (pooled, written) = table_files(&tables);
    assert!(!pooled.is_empty(), "self-check: pooled files are there");

    tree.create_checkpoint(&copy)?;
    let copy_tables = copy.join("tables");
    let (copy_pooled, copy_written) = table_files(&copy_tables);
    assert!(copy_pooled.is_empty(), "no pooled file: {copy_pooled:?}");
    assert_eq!(copy_written, written, "the tables the version names");
    let lengths = |dir: &Path, ids: &[u64]| -> Vec<u64> {
        ids.iter()
            .map(|id| {
                std::fs::metadata(dir.join(id.to_string()))
                    .map(|meta| meta.len())
                    .unwrap_or_else(|e| panic!("{id}: {e}"))
            })
            .collect()
    };
    let copy_lengths = lengths(&copy_tables, &copy_written);

    // Each tree writes into its own pooled files.
    for (seqno, key) in [(2, "c"), (3, "d")] {
        tree.insert(key, "1", seqno);
        tree.flush_active_memtable(0)?;
    }
    let restored = config(&copy).table_file_pool(4).open()?;
    restored.insert("e", "1", 2);
    restored.flush_active_memtable(0)?;
    assert_eq!(
        lengths(&copy_tables, &copy_written),
        copy_lengths,
        "the copy's tables are as taken"
    );
    let (_, now_written) = table_files(&tables);
    for id in &pooled {
        assert!(
            now_written.contains(id),
            "the source wrote its pooled file {id}: {now_written:?}"
        );
    }
    for key in ["a", "b", "c", "d"] {
        assert!(tree.get(key, u64::MAX)?.is_some(), "source {key}");
    }
    for key in ["a", "b", "e"] {
        assert!(restored.get(key, u64::MAX)?.is_some(), "copy {key}");
    }
    assert!(restored.get("c", u64::MAX)?.is_none(), "the copy is apart");
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
        .skip(u64::try_from(skip).expect("a sync count fits u64"))
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

/// Flushes the tree it is handed while the compaction it filters is writing,
/// then fails that compaction: a write numbered above the compaction's
/// output installs first, and the output is left empty.
struct FlushMidway {
    tree: std::panic::AssertUnwindSafe<Arc<std::sync::Mutex<Option<AnyTree>>>>,
}

impl lsm_tree::compaction::filter::Factory for FlushMidway {
    fn name(&self) -> &'static str {
        "flush midway"
    }

    fn make_filter(
        &self,
        _ctx: &lsm_tree::compaction::filter::Context,
    ) -> Box<dyn lsm_tree::compaction::filter::CompactionFilter> {
        Box::new(FlushMidwayFilter {
            tree: Arc::clone(&self.tree),
        })
    }
}

struct FlushMidwayFilter {
    tree: Arc<std::sync::Mutex<Option<AnyTree>>>,
}

impl lsm_tree::compaction::filter::CompactionFilter for FlushMidwayFilter {
    fn filter_item(
        &mut self,
        _item: lsm_tree::compaction::filter::ItemAccessor<'_>,
        _ctx: &lsm_tree::compaction::filter::Context,
    ) -> lsm_tree::Result<lsm_tree::compaction::filter::Verdict> {
        let tree = self
            .tree
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(tree) = tree {
            tree.insert("late", "1", 100);
            tree.flush_active_memtable(0)?;
        }
        Err(std::io::Error::other("the power goes").into())
    }
}

/// Removes a tree's manifest on `fs`: its snapshots and edit logs.
fn remove_manifest_on(fs: &dyn lsm_tree::fs::Fs, folder: &Path) -> lsm_tree::Result<()> {
    for entry in fs.read_dir(folder)? {
        if entry.file_name.starts_with('v') || entry.file_name.starts_with("edits-") {
            fs.remove_file(&entry.path)?;
        }
    }
    Ok(())
}

/// A compaction writing into a pooled file while a flush numbered above it
/// installs first, then a power loss: the compaction's file is left empty
/// below an installed table. The tree reopens with every key, and a repair
/// without a manifest finds every table, takes the empty file for an output
/// cut short and reports no loss.
#[test]
fn an_output_cut_short_below_an_installed_table_is_no_loss() -> lsm_tree::Result<()> {
    use lsm_tree::fs::Fs;

    let crash = CrashFs::new(MemFs::new());
    let fs = Arc::new(FaultFs::new(crash.clone()));
    let handle = Arc::new(std::sync::Mutex::new(None));
    let tree = crash_config(&fs)
        .with_compaction_filter_factory(Some(Arc::new(FlushMidway {
            tree: std::panic::AssertUnwindSafe(Arc::clone(&handle)),
        })))
        .open()?;
    for (seqno, key) in [(0, "a"), (1, "b"), (2, "c")] {
        tree.insert(key, "1", seqno);
        tree.flush_active_memtable(0)?;
    }
    let inputs: Vec<u64> = tree
        .current_version()
        .iter_tables()
        .map(|t| t.id())
        .collect();
    *handle
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(tree.clone());
    assert!(
        tree.major_compact(u64::MAX, 0).is_err(),
        "self-check: the compaction is cut short"
    );
    let version = tree.current_version();
    let Some(late) = version.iter_tables().max_by_key(|t| t.id()) else {
        panic!("the late table");
    };
    let [cut_short] = late.metadata.writing_below[..] else {
        panic!(
            "self-check: the late table lists the output being written: {:?}",
            late.metadata.writing_below
        );
    };
    assert!(
        inputs.iter().all(|id| *id < cut_short),
        "self-check: {cut_short} is the output, above {inputs:?}"
    );
    drop(version);
    drop(tree);
    crash.crash();

    let tables = std::path::absolute(DB)?.join("tables");
    let cut_short_path = tables.join(cut_short.to_string());
    assert_eq!(
        crash.metadata(&cut_short_path)?.len,
        0,
        "self-check: the power loss leaves the output empty under its name"
    );

    let fs = Arc::new(FaultFs::new(crash.clone()));
    {
        let tree = crash_config(&fs).open()?;
        for key in ["a", "b", "c", "late"] {
            assert!(
                tree.get(key, u64::MAX)?.is_some(),
                "{key} after the power loss"
            );
        }
    }
    remove_manifest_on(&crash, &std::path::absolute(DB)?)?;

    let report = crash_config(&fs).repair()?;
    assert_eq!(report.recovered, inputs.len() + 1, "{report:?}");
    assert!(report.unknowable_losses.is_empty(), "{report:?}");
    assert!(
        report
            .excluded_files
            .iter()
            .any(|(path, _)| *path == cut_short_path),
        "the output cut short is left out: {report:?}"
    );
    assert_eq!(
        report.wal_replay_scope(),
        lsm_tree::WalReplayScope::TailOnly
    );
    let tree = crash_config(&fs).open()?;
    for key in ["a", "b", "c", "late"] {
        assert!(tree.get(key, u64::MAX)?.is_some(), "{key} after the repair");
    }
    Ok(())
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

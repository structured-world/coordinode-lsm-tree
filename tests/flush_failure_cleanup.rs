// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A flush that fails before it installs its result leaves none of the files
//! it created on disk, finished or not, without waiting for the orphan sweep
//! of the next open; its memtables stay sealed, and a retry writes them.

#![cfg(feature = "std")]

use lsm_tree::config::{BlockSizePolicy, KvSeparationOptions};
use lsm_tree::fs::{Fault, FaultFs, FaultInjector, FaultOp, FaultRule, StdFs};
use lsm_tree::io::ErrorKind;
use lsm_tree::{AbstractTree, AnyTree, Config, MAX_SEQNO, SequenceNumberCounter};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use test_log::test;

const KEYS: u64 = 2_000;

/// The flushes write tables of this size, so one flush spans several.
const TABLE_TARGET: u64 = 16 * 1024;

fn key(i: u64) -> String {
    format!("key_{i:08}")
}

/// Above the separation threshold of the KV-separated fixtures.
fn value(i: u64) -> Vec<u8> {
    let mut v = format!("value-{i}-").into_bytes();
    v.resize(220, b'v');
    v
}

/// Every file name in `folder`, none when the folder does not exist. In the
/// tables folder an empty file is one its pool keeps for tables to come, made
/// by the pool whenever it runs low, a flush's included: no output of a
/// flush, so it is not counted. The blobs folder has no pool, so an empty
/// file there is an output left behind, and counts.
fn files(folder: &Path) -> std::io::Result<BTreeSet<String>> {
    let pooled = folder.ends_with("tables");
    let entries = match std::fs::read_dir(folder) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeSet::new()),
        Err(e) => return Err(e),
    };
    let mut names = BTreeSet::new();
    for entry in entries {
        let entry = entry?;
        match entry.metadata() {
            Ok(meta) if meta.len() > 0 || !pooled => {}
            Ok(_) => continue,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        }
        names.insert(entry.file_name().to_string_lossy().into_owned());
    }
    Ok(names)
}

/// The table and blob files of the tree at `root`, as `folder/name`.
fn on_disk(root: &Path) -> std::io::Result<BTreeSet<String>> {
    let mut all = BTreeSet::new();
    for folder in ["tables", "blobs"] {
        all.extend(
            files(&root.join(folder))?
                .into_iter()
                .map(|name| format!("{folder}/{name}")),
        );
    }
    Ok(all)
}

struct Fixture {
    tree: AnyTree,
    injector: Arc<FaultInjector>,
    dir: tempfile::TempDir,
}

fn open(configure: impl FnOnce(Config) -> Config) -> lsm_tree::Result<Fixture> {
    let dir = tempfile::tempdir()?;
    let fs = FaultFs::new(StdFs);
    let injector = fs.injector();
    let tree = configure(
        Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .data_block_size_policy(BlockSizePolicy::all(512))
        .table_target_size(TABLE_TARGET)
        .with_fs(fs),
    )
    .open()?;
    Ok(Fixture {
        tree,
        injector,
        dir,
    })
}

fn blob_tree(c: Config) -> Config {
    c.with_kv_separation(Some(
        KvSeparationOptions::default()
            .separation_threshold(64)
            .file_target_size(16 * 1024),
    ))
}

/// Fails every write under `folder` with a full volume once `skip` of them
/// have passed.
fn full_after(folder: &str, skip: u64) -> FaultRule {
    FaultRule::new(FaultOp::Write, Fault::Error(ErrorKind::StorageFull))
        .on_path(folder)
        .skip(skip)
}

/// Writes every key, arms `rules`, runs a flush that must fail, and checks
/// that no table or blob file appeared and every key still reads, all
/// without reopening the tree. Then a retry with the faults cleared flushes
/// the same memtables and leaves on disk exactly the files the tree names.
fn assert_flush_fails_cleanly(
    f: &Fixture,
    rules: impl IntoIterator<Item = FaultRule>,
    failed: impl Fn(&lsm_tree::Error) -> bool,
) -> lsm_tree::Result<()> {
    for i in 0..KEYS {
        f.tree.insert(key(i), value(i), i);
    }
    let before = on_disk(f.dir.path())?;
    for rule in rules {
        f.injector.arm(rule);
    }
    let result = f.tree.flush_active_memtable(0);
    // Checked before the faults are cleared, with what the injector observed.
    match &result {
        Err(e) => assert!(failed(e), "the flush failed otherwise: {e:?}"),
        Ok(()) => panic!("the injected fault must fail the flush"),
    }
    f.injector.clear();

    let left_behind: BTreeSet<String> = on_disk(f.dir.path())?
        .difference(&before)
        .cloned()
        .collect();
    assert!(
        left_behind.is_empty(),
        "the failed flush must leave none of its files behind: {left_behind:?}",
    );
    assert_eq!(f.tree.table_count(), 0, "nothing was installed");
    assert_eq!(
        f.tree.sealed_memtable_count(),
        1,
        "the memtable stays sealed for a retry"
    );
    for i in (0..KEYS).step_by(97) {
        assert_eq!(
            f.tree.get(key(i), MAX_SEQNO)?.as_deref(),
            Some(value(i).as_slice()),
            "key {i} after the failed flush",
        );
    }

    let lock = f.tree.get_flush_lock();
    f.tree.flush(&lock, 0)?;
    drop(lock);
    assert_eq!(f.tree.sealed_memtable_count(), 0, "the retry flushed it");
    let tables = files(&f.dir.path().join("tables"))?;
    assert_eq!(
        tables.len(),
        f.tree.table_count(),
        "only the tables the retry installed are on disk: {tables:?}",
    );
    let blobs = files(&f.dir.path().join("blobs"))?;
    assert_eq!(
        blobs.len(),
        f.tree.blob_file_count(),
        "only the blob files the retry installed are on disk: {blobs:?}",
    );
    for i in 0..KEYS {
        assert_eq!(
            f.tree.get(key(i), MAX_SEQNO)?.as_deref(),
            Some(value(i).as_slice()),
            "key {i} after the retry",
        );
    }
    Ok(())
}

fn storage_full(e: &lsm_tree::Error) -> bool {
    matches!(e, lsm_tree::Error::Io(io) if io.kind() == ErrorKind::StorageFull)
}

/// The tables finished before the volume filled and the one it filled in:
/// neither stays.
#[test]
fn a_flush_the_volume_fills_under_leaves_none_of_its_tables() -> lsm_tree::Result<()> {
    let f = open(|c| c)?;
    assert_flush_fails_cleanly(&f, [full_after("tables", 8)], storage_full)
}

/// A blob tree's flush that fills the volume part-way through its blob files
/// leaves neither its blob files nor its tables.
#[test]
fn a_blob_flush_the_volume_fills_under_leaves_none_of_its_files() -> lsm_tree::Result<()> {
    let f = open(blob_tree)?;
    assert_flush_fails_cleanly(&f, [full_after("blobs", 4)], storage_full)
}

/// A blob tree's flush whose tables fill the volume after its blob files are
/// written leaves neither.
#[test]
fn a_blob_flush_failing_on_its_tables_leaves_its_blob_files_neither() -> lsm_tree::Result<()> {
    let f = open(blob_tree)?;
    assert_flush_fails_cleanly(&f, [full_after("tables", 2)], storage_full)
}

/// Every table is written and finished, and syncing them fails before the
/// flush installs them: the finished tables go too.
#[test]
fn a_flush_whose_table_sync_fails_leaves_none_of_its_tables() -> lsm_tree::Result<()> {
    let f = open(|c| c)?;
    assert_flush_fails_cleanly(
        &f,
        [FaultRule::new(FaultOp::SyncData, Fault::Error(ErrorKind::Other)).on_path("tables")],
        |e| !matches!(e, lsm_tree::Error::InstallOutcomeUnknown(_)),
    )
}

/// A caller that registers the same tables again after an install of unknown
/// outcome installs them: the kept files are the installed tables now, and
/// stay, so the tree reopens with every key.
#[test]
fn a_retried_registration_keeps_the_tables_it_installs() -> lsm_tree::Result<()> {
    use lsm_tree::{InternalValue, ValueType};

    let f = open(|c| c)?;
    let stream = (0..KEYS).map(|i| {
        Ok(InternalValue::from_components(
            key(i),
            value(i),
            i,
            ValueType::Value,
        ))
    });
    let Some((tables, blob_files, pin)) = f.tree.flush_to_tables(stream)? else {
        panic!("the stream holds keys");
    };
    f.injector.arm(
        FaultRule::new(FaultOp::SyncData, Fault::Error(ErrorKind::Other))
            .on_path("edits-")
            .once(),
    );
    assert!(matches!(
        f.tree
            .register_tables(&tables, blob_files.as_deref(), None, &[], 0, false),
        Err(lsm_tree::Error::InstallOutcomeUnknown(_))
    ));
    f.injector.clear();

    // The retry rotates, superseding the failed edit, and names the same
    // files.
    f.tree
        .register_tables(&tables, blob_files.as_deref(), None, &[], 0, false)?;
    drop(pin);
    drop(tables);
    let installed = files(&f.dir.path().join("tables"))?;
    assert_eq!(
        installed.len(),
        f.tree.table_count(),
        "every installed table is on disk: {installed:?}",
    );

    let Fixture { tree, dir, .. } = f;
    drop(tree);
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    for i in 0..KEYS {
        assert_eq!(
            tree.get(key(i), MAX_SEQNO)?.as_deref(),
            Some(value(i).as_slice()),
            "key {i} after the reopen",
        );
    }
    Ok(())
}

/// A registration retried after an install of unknown outcome fails again,
/// before it could supersede that install: the files stay, since a reopen
/// after a power loss may still find the first install, and it reads every
/// key.
#[test]
fn a_failed_retry_of_a_registration_keeps_the_tables_it_may_name() -> lsm_tree::Result<()> {
    use lsm_tree::{InternalValue, ValueType};

    let f = open(|c| c)?;
    let stream = (0..KEYS).map(|i| {
        Ok(InternalValue::from_components(
            key(i),
            value(i),
            i,
            ValueType::Value,
        ))
    });
    let Some((tables, blob_files, pin)) = f.tree.flush_to_tables(stream)? else {
        panic!("the stream holds keys");
    };
    f.injector.arm(
        FaultRule::new(FaultOp::SyncData, Fault::Error(ErrorKind::Other))
            .on_path("edits-")
            .once(),
    );
    assert!(matches!(
        f.tree
            .register_tables(&tables, blob_files.as_deref(), None, &[], 0, false),
        Err(lsm_tree::Error::InstallOutcomeUnknown(_))
    ));
    f.injector.clear();
    let written = files(&f.dir.path().join("tables"))?;

    // The retry rotates: its snapshot fails to write, an ordinary error.
    f.injector
        .arm(FaultRule::new(FaultOp::Write, Fault::Error(ErrorKind::Other)).on_path(".tmp"));
    let retry = f
        .tree
        .register_tables(&tables, blob_files.as_deref(), None, &[], 0, false);
    f.injector.clear();
    assert!(
        matches!(&retry, Err(e) if !matches!(e, lsm_tree::Error::InstallOutcomeUnknown(_))),
        "the retry fails before it installs: {retry:?}"
    );
    drop(pin);
    drop(tables);
    let now = files(&f.dir.path().join("tables"))?;
    assert_eq!(now, written, "the tables the first install may name stay");
    Ok(())
}

/// A compaction whose install may have happened keeps its outputs, since a
/// reopen after a power loss may find them named; they go once a later
/// install supersedes the unknown one. A synced flush writes no edit, so its
/// install has no unknown outcome: the compaction after it does.
#[test]
fn a_compaction_of_unknown_install_outcome_keeps_its_tables_until_superseded()
-> lsm_tree::Result<()> {
    assert_unknown_outcome_keeps_until_superseded(&open(|c| c)?)
}

/// The same for a blob tree, its blob files with its tables.
#[test]
fn a_blob_compaction_of_unknown_install_outcome_keeps_its_files_until_superseded()
-> lsm_tree::Result<()> {
    assert_unknown_outcome_keeps_until_superseded(&open(blob_tree)?)
}

/// The tables are written and finished, and loading one of them fails: none
/// of the flush's files stays.
#[test]
fn a_flush_failing_to_load_its_tables_leaves_none_of_them() -> lsm_tree::Result<()> {
    let f = open(|c| c)?;
    assert_flush_fails_cleanly(&f, [read_error("tables")], |_| true)
}

/// The same for a blob tree's flush, its blob files with its tables.
#[test]
fn a_blob_flush_failing_to_load_its_tables_leaves_none_of_its_files() -> lsm_tree::Result<()> {
    let f = open(blob_tree)?;
    assert_flush_fails_cleanly(&f, [read_error("tables")], |_| true)
}

/// A blob tree with a single-level index and no filter whose flush fills the
/// volume part-way through its blob files, after some of its tables are
/// finished, leaves none of them.
#[test]
fn a_blob_flush_without_filters_leaves_none_of_its_files() -> lsm_tree::Result<()> {
    use lsm_tree::config::{FilterPolicy, PinningPolicy};

    fn configure(c: Config) -> Config {
        blob_tree(c)
            .filter_policy(FilterPolicy::disabled())
            .index_block_partitioning_policy(PinningPolicy::disabled())
    }

    // How many writes the flush makes to its blob files depends on how the
    // build buffers them, so a flush that succeeds counts them first.
    let sample = open(configure)?;
    for i in 0..KEYS {
        sample.tree.insert(key(i), value(i), i);
    }
    sample.tree.flush_active_memtable(0)?;
    let blob_writes = sample.injector.writes_for("blobs").len();
    assert!(
        blob_writes >= 4,
        "the flush writes {blob_writes} times to blob files"
    );
    drop(sample);

    // The volume fills three quarters of the way through those writes.
    let f = open(configure)?;
    let full = full_after(
        "blobs",
        u64::try_from(blob_writes * 3 / 4).unwrap_or(u64::MAX),
    );
    let probe = Arc::clone(&f.injector);
    assert_flush_fails_cleanly(&f, [full], |e| {
        // A table reaches the volume only once it is finished.
        assert!(
            !probe.writes_for("tables").is_empty(),
            "a table is finished before the volume fills"
        );
        storage_full(e)
    })
}

/// A blob tree's flush that fills the volume while it moves the heavy cells
/// of rows into its blob files leaves none of its files, and the rows read
/// back once a retry has flushed them.
#[test]
fn a_flush_of_cell_rows_the_volume_fills_under_leaves_none_of_its_files() -> lsm_tree::Result<()> {
    use lsm_tree::blob_tree::field_row::{FIRST_FIELD_COLUMN, Field};

    let f = open(blob_tree)?;
    let AnyTree::Blob(tree) = &f.tree else {
        panic!("a tree with kv separation opens as a blob tree");
    };
    let body = vec![b'b'; 1_000];
    for i in 0..KEYS {
        tree.insert_cells(
            key(i),
            &[
                Field::bytes(FIRST_FIELD_COLUMN, b"draft"),
                Field::bytes(FIRST_FIELD_COLUMN + 1, &body),
            ],
            i,
        )?;
    }
    let before = on_disk(f.dir.path())?;
    f.injector.arm(full_after("blobs", 4));
    let result = f.tree.flush_active_memtable(0);
    f.injector.clear();
    assert!(
        result.as_ref().is_err_and(storage_full),
        "the full volume fails the flush: {result:?}"
    );
    let left_behind: BTreeSet<String> = on_disk(f.dir.path())?
        .difference(&before)
        .cloned()
        .collect();
    assert!(
        left_behind.is_empty(),
        "the failed flush must leave none of its files behind: {left_behind:?}",
    );

    let lock = f.tree.get_flush_lock();
    f.tree.flush(&lock, 0)?;
    drop(lock);
    for i in (0..KEYS).step_by(97) {
        let row = tree
            .get_cells(key(i), MAX_SEQNO)?
            .expect("the row is flushed");
        assert_eq!(row.fields()?.len(), 2, "row {i} after the retry");
    }
    Ok(())
}

/// Any install that rotates supersedes an install of unknown outcome, not a
/// flush or a compaction alone: the files kept for it go with a `clear` that
/// follows, with nothing flushed or compacted after it.
#[test]
fn a_clear_after_an_install_of_unknown_outcome_removes_the_kept_files() -> lsm_tree::Result<()> {
    let f = open(blob_tree)?;
    let kept = compaction_of_unknown_outcome(&f)?;
    assert!(!kept.is_empty(), "the compaction keeps its outputs");

    // The live files the clear drops go once their last reader does; the
    // kept ones have no reader and go with the install.
    f.tree.clear()?;
    let left: BTreeSet<String> = on_disk(f.dir.path())?
        .intersection(&kept)
        .cloned()
        .collect();
    assert!(
        left.is_empty(),
        "the clear supersedes the unknown install, and its files go: {left:?}"
    );
    Ok(())
}

/// Fails every positioned read under `folder`.
fn read_error(folder: &str) -> FaultRule {
    FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::Other)).on_path(folder)
}

/// Flushes every key, then compacts the tree failing the sync of the edit
/// that installs the compaction: an install of unknown outcome. Returns the
/// files it added, which the tree keeps.
fn compaction_of_unknown_outcome(f: &Fixture) -> lsm_tree::Result<BTreeSet<String>> {
    for i in 0..KEYS {
        f.tree.insert(key(i), value(i), i);
    }
    f.tree.flush_active_memtable(0)?;
    let before = on_disk(f.dir.path())?;
    f.injector.arm(
        FaultRule::new(FaultOp::SyncData, Fault::Error(ErrorKind::Other))
            .on_path("edits-")
            .once(),
    );
    assert!(matches!(
        f.tree.major_compact(u64::MAX, 0),
        Err(lsm_tree::Error::InstallOutcomeUnknown(_))
    ));
    f.injector.clear();
    Ok(on_disk(f.dir.path())?
        .difference(&before)
        .cloned()
        .collect())
}

fn assert_unknown_outcome_keeps_until_superseded(f: &Fixture) -> lsm_tree::Result<()> {
    let kept = compaction_of_unknown_outcome(f)?;
    assert!(!kept.is_empty(), "the compaction keeps its outputs");

    // The retry compacts again; its install rotates, superseding the failed
    // edit.
    f.tree.major_compact(u64::MAX, 0)?;
    let now = on_disk(f.dir.path())?;
    for name in &kept {
        assert!(
            !now.contains(name),
            "the kept table {name} goes once superseded"
        );
    }
    // The retired inputs leave through the background deleter: wait for it,
    // bounded.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let tables = loop {
        let tables = files(&f.dir.path().join("tables"))?;
        if tables.len() == f.tree.table_count() || std::time::Instant::now() >= deadline {
            break tables;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    };
    assert_eq!(
        tables.len(),
        f.tree.table_count(),
        "only the tables the retry installed are on disk: {tables:?}",
    );
    for i in 0..KEYS {
        assert_eq!(
            f.tree.get(key(i), MAX_SEQNO)?.as_deref(),
            Some(value(i).as_slice()),
            "key {i} after the retry",
        );
    }
    Ok(())
}

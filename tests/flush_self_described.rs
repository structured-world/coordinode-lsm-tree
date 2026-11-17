// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A synced flush writes no manifest edit: its tables carry the flush's number,
//! their place in its run and the run's length, and it returns once they and
//! their folders are synced. The open installs every whole flush above the
//! manifest's watermark from the tables themselves, and leaves out a flush a
//! power loss cut short, which never returned.

use lsm_tree::fs::{CrashFs, Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
use lsm_tree::{AbstractTree, AnyTree, Config, KvSeparationOptions, SequenceNumberCounter};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use test_log::test;

const DB: &str = "/db";

/// `path` as a tree opened at it names it.
fn at(path: &str) -> PathBuf {
    std::path::absolute(path).expect("an absolute path")
}

/// A disk a power loss can take, and the backend a tree runs on it through.
struct Disk {
    crash: CrashFs,
    fs: Arc<FaultFs<CrashFs>>,
}

impl Disk {
    fn new() -> Self {
        Self::over(CrashFs::new(MemFs::new()))
    }

    /// The disk `crash` as it came back from a power loss.
    fn over(crash: CrashFs) -> Self {
        Self {
            fs: Arc::new(FaultFs::new(crash.clone())),
            crash,
        }
    }

    fn fs(&self) -> Arc<dyn Fs> {
        self.fs.clone()
    }

    /// Fails the next `op` on a path containing `path`.
    fn fail_once(&self, op: FaultOp, path: &str) {
        self.fs.injector().arm(
            FaultRule::new(op, Fault::Error(lsm_tree::io::ErrorKind::Other))
                .on_path(path)
                .once(),
        );
    }

    /// Cuts the power under `tree` and returns the disk as it comes back:
    /// what was never synced is gone, and the tree, dropped after it, changes
    /// nothing on its way out.
    fn power_loss(self, tree: AnyTree) -> CrashFs {
        for op in [
            FaultOp::Open,
            FaultOp::RemoveFile,
            FaultOp::Rename,
            FaultOp::CreateDir,
            FaultOp::CreateDirAll,
            FaultOp::SyncDirectory,
            FaultOp::HardLink,
            FaultOp::RemoveDir,
            FaultOp::RemoveDirAll,
        ] {
            self.fs.injector().arm(FaultRule::new(
                op,
                Fault::Error(lsm_tree::io::ErrorKind::Other),
            ));
        }
        self.crash.crash();
        drop(tree);
        self.crash
    }
}

/// A tree at [`DB`] on `fs`, without a table file pool: a flush makes its
/// table's directory entry, so losing the folder's sync loses the table.
fn config(fs: Arc<dyn Fs>) -> Config {
    Config::new(
        DB,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(fs)
    .table_file_pool(0)
}

/// [`config`] with every value kept apart in blob files.
fn blob_config(fs: Arc<dyn Fs>) -> Config {
    config(fs).with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
}

/// The value of `key` the latest snapshot reads, as text.
fn value(tree: &AnyTree, key: &str) -> lsm_tree::Result<Option<String>> {
    Ok(tree
        .get(key, u64::MAX)?
        .map(|v| String::from_utf8_lossy(&v).into_owned()))
}

/// The files in `folder` on `fs` named like tables or blob files and holding
/// data.
fn data_files(fs: &dyn Fs, folder: &Path) -> Vec<u64> {
    let mut ids: Vec<u64> = fs
        .read_dir(folder)
        .map(|entries| {
            entries
                .into_iter()
                .filter_map(|entry| {
                    let id = entry.file_name.parse::<u64>().ok()?;
                    let len = fs.metadata(&folder.join(&entry.file_name)).ok()?.len;
                    (len > 0).then_some(id)
                })
                .collect()
        })
        .unwrap_or_default();
    ids.sort_unstable();
    ids
}

/// The bytes the manifest's edit logs at [`DB`] hold, all generations.
fn edit_log_bytes(fs: &dyn Fs) -> lsm_tree::Result<u64> {
    let mut bytes = 0;
    for entry in fs.read_dir(&at(DB))? {
        if entry.file_name.starts_with("edits-") {
            bytes += fs.metadata(&at(DB).join(&entry.file_name))?.len;
        }
    }
    Ok(bytes)
}

/// Flushes that returned survive a power loss right after them, tables and
/// blob files, though no manifest edit names them. Guards against a flush
/// returning before its files are synced, and against an open that installs
/// only what the manifest names.
#[test]
fn flushes_that_returned_survive_a_power_loss_without_an_edit() -> lsm_tree::Result<()> {
    let big = "x".repeat(4_096);
    let disk = Disk::new();
    let tree = blob_config(disk.fs()).open()?;
    let logged = edit_log_bytes(&disk.crash)?;
    for (seqno, key) in [(0, "a"), (1, "b"), (2, "c")] {
        tree.insert(key, &big, seqno);
        tree.flush_active_memtable(0)?;
    }
    assert_eq!(
        edit_log_bytes(&disk.crash)?,
        logged,
        "a synced flush writes no manifest edit"
    );
    let crash = disk.power_loss(tree);

    let disk = Disk::over(crash);
    let tree = blob_config(disk.fs()).open()?;
    for key in ["a", "b", "c"] {
        assert_eq!(value(&tree, key)?.as_deref(), Some(big.as_str()), "{key}");
    }
    assert_eq!(tree.table_count(), 3);
    assert_eq!(tree.blob_file_count(), 3);

    // The open recorded them: a further flush and power loss keep all four.
    tree.insert("d", &big, 3);
    tree.flush_active_memtable(0)?;
    let crash = disk.power_loss(tree);
    let tree = blob_config(Arc::new(crash)).open()?;
    for key in ["a", "b", "c", "d"] {
        assert_eq!(value(&tree, key)?.as_deref(), Some(big.as_str()), "{key}");
    }
    assert_eq!(tree.table_count(), 4);
    Ok(())
}

/// A flush whose table sync fails never returned, and is gone after a power
/// loss; the flush before it stays, and a later one survives the next power
/// loss. Guards against an open that serves a table that never reached the
/// disk, and against a later flush taking the lost one's number and mixing
/// with what it left.
#[test]
fn a_flush_whose_table_did_not_reach_the_disk_is_gone() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let tree = config(disk.fs()).open()?;
    tree.insert("kept", "1", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("lost", "2", 1);
    disk.fail_once(FaultOp::SyncData, "tables");
    assert!(
        tree.flush_active_memtable(0).is_err(),
        "the flush fails on its table's sync"
    );
    let crash = disk.power_loss(tree);

    let disk = Disk::over(crash);
    let tree = config(disk.fs()).open()?;
    assert_eq!(value(&tree, "kept")?.as_deref(), Some("1"));
    assert_eq!(value(&tree, "lost")?, None, "the flush never returned");
    assert_eq!(tree.table_count(), 1);

    tree.insert("after", "3", 2);
    tree.flush_active_memtable(0)?;
    let crash = disk.power_loss(tree);

    let tree = config(Arc::new(crash.clone())).open()?;
    assert_eq!(value(&tree, "kept")?.as_deref(), Some("1"));
    assert_eq!(value(&tree, "after")?.as_deref(), Some("3"));
    assert_eq!(value(&tree, "lost")?, None);
    assert_eq!(tree.table_count(), 2);
    assert_eq!(
        data_files(&crash, &at(DB).join("tables")).len(),
        2,
        "no table of the lost flush is left on disk"
    );
    Ok(())
}

/// As above, with the table's bytes durable and its directory entry not.
/// Guards against an open that reads a missing table as an error.
#[test]
fn a_flush_whose_table_entry_did_not_reach_the_disk_is_gone() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let tree = config(disk.fs()).open()?;
    tree.insert("kept", "1", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("lost", "2", 1);
    disk.fail_once(FaultOp::SyncDirectory, "tables");
    assert!(
        tree.flush_active_memtable(0).is_err(),
        "the flush fails on its folder's sync"
    );
    let crash = disk.power_loss(tree);

    let tree = config(Arc::new(crash)).open()?;
    assert_eq!(value(&tree, "kept")?.as_deref(), Some("1"));
    assert_eq!(value(&tree, "lost")?, None);
    assert_eq!(tree.table_count(), 1);
    Ok(())
}

/// A blob file that did not reach the disk leaves its flush out too: its
/// table may be whole, but it points at values that are gone. Guards against
/// an open that checks a flush's tables and not its blob files.
#[test]
fn a_flush_whose_blob_file_did_not_reach_the_disk_is_gone() -> lsm_tree::Result<()> {
    let big = "x".repeat(4_096);
    let disk = Disk::new();
    let tree = blob_config(disk.fs()).open()?;
    tree.insert("kept", &big, 0);
    tree.flush_active_memtable(0)?;

    tree.insert("lost", &big, 1);
    disk.fail_once(FaultOp::SyncData, "blobs");
    assert!(
        tree.flush_active_memtable(0).is_err(),
        "the flush fails on its blob file's sync"
    );
    let crash = disk.power_loss(tree);

    let tree = blob_config(Arc::new(crash)).open()?;
    assert_eq!(value(&tree, "kept")?.as_deref(), Some(big.as_str()));
    assert_eq!(value(&tree, "lost")?, None);
    assert_eq!(tree.blob_file_count(), 1);
    Ok(())
}

/// A flush is installed whole or not at all: with one table of its run gone,
/// none of the others is served. Guards against an open that installs the
/// tables of a flush it finds, rather than the flush.
#[test]
fn a_flush_missing_a_table_of_its_run_is_left_out_whole() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let tree = config(disk.fs()).table_target_size(1_024).open()?;
    tree.insert("kept", "1", 0);
    tree.flush_active_memtable(0)?;
    let before = data_files(&disk.crash, &at(DB).join("tables"));

    let filler = "y".repeat(256);
    for i in 0..64u64 {
        tree.insert(format!("run-{i:03}"), &filler, 1 + i);
    }
    tree.flush_active_memtable(0)?;
    let run: Vec<u64> = data_files(&disk.crash, &at(DB).join("tables"))
        .into_iter()
        .filter(|id| !before.contains(id))
        .collect();
    assert!(
        run.len() >= 2,
        "self-check: the flush wrote a run of tables"
    );
    let crash = disk.power_loss(tree);

    crash.remove_file(&at(DB).join("tables").join(run[run.len() / 2].to_string()))?;
    let tree = config(Arc::new(crash)).table_target_size(1_024).open()?;
    assert_eq!(value(&tree, "kept")?.as_deref(), Some("1"));
    for i in 0..64u64 {
        assert_eq!(value(&tree, &format!("run-{i:03}"))?, None, "run-{i:03}");
    }
    assert_eq!(tree.table_count(), 1);
    Ok(())
}

/// A clear decides every flush before it: their tables, still on disk after a
/// power loss that took the clear's deletions, are not installed again.
/// Guards against cleared data coming back from a flush's own description.
#[test]
fn a_cleared_flush_does_not_come_back_after_a_power_loss() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let tree = config(disk.fs()).open()?;
    for (seqno, key) in [(0, "a"), (1, "b")] {
        tree.insert(key, "1", seqno);
        tree.flush_active_memtable(0)?;
    }
    tree.clear()?;
    let crash = disk.power_loss(tree);

    let tree = config(Arc::new(crash)).open()?;
    assert_eq!(value(&tree, "a")?, None);
    assert_eq!(value(&tree, "b")?, None);
    assert_eq!(tree.table_count(), 0);
    Ok(())
}

/// A flush that collects history under a watermark above every sequence number
/// it holds raises the recovered retention floor no further than the live
/// install did. Guards against an open that restores the watermark itself as
/// the floor and refuses every snapshot the counter can still hand out.
#[test]
fn a_flush_collecting_all_history_keeps_later_snapshots_after_a_power_loss() -> lsm_tree::Result<()>
{
    let disk = Disk::new();
    let tree = config(disk.fs()).open()?;
    tree.insert("a", "1", 0);
    tree.insert("a", "2", 1);
    tree.flush_active_memtable(u64::MAX)?;
    let live = tree.oldest_retained_seqno();
    assert!(
        live < 5,
        "self-check: the live floor is bounded, got {live:?}"
    );
    assert_eq!(
        tree.get("a", 5)?.as_deref(),
        Some(&b"2"[..]),
        "a later snapshot reads before the power loss"
    );
    let crash = disk.power_loss(tree);

    let tree = config(Arc::new(crash)).open()?;
    assert_eq!(tree.oldest_retained_seqno(), live);
    assert_eq!(
        tree.get("a", 5)?.as_deref(),
        Some(&b"2"[..]),
        "the same snapshot reads after it"
    );
    Ok(())
}

/// A flush survives a reopen that routes level 0 elsewhere: its tables are
/// found in the folder they were written to. Guards against an open that
/// looks for self-described flushes in the current level-0 folder only and
/// sweeps the earlier one's tables as orphans.
#[test]
fn a_flush_survives_a_reopen_that_routes_level_zero_elsewhere() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let tree = config(disk.fs()).open()?;
    tree.insert("a", "1", 0);
    tree.flush_active_memtable(0)?;
    let crash = disk.power_loss(tree);

    let fs: Arc<dyn Fs> = Arc::new(crash);
    let tree = config(fs.clone())
        .level_routes(vec![lsm_tree::config::LevelRoute {
            levels: 0..1,
            path: at("/hot"),
            fs,
        }])
        .open()?;
    assert_eq!(value(&tree, "a")?.as_deref(), Some("1"));
    assert_eq!(tree.table_count(), 1);
    Ok(())
}

/// A flush whose table holds a damaged data block is left out: a power loss
/// can keep a table's entry and trailer and lose blocks before them, when the
/// flush never returned. Guards against an open that checks a found table's
/// trailer only and serves a flush whose blocks do not read.
#[test]
fn a_flush_whose_table_has_a_damaged_block_is_left_out() -> lsm_tree::Result<()> {
    use std::io::{Seek, SeekFrom, Write};

    let disk = Disk::new();
    let tree = config(disk.fs()).open()?;
    tree.insert("kept", "1", 0);
    tree.flush_active_memtable(0)?;
    let before = data_files(&disk.crash, &at(DB).join("tables"));
    tree.insert("torn", "x".repeat(512), 1);
    tree.flush_active_memtable(0)?;
    let torn: Vec<u64> = data_files(&disk.crash, &at(DB).join("tables"))
        .into_iter()
        .filter(|id| !before.contains(id))
        .collect();
    assert_eq!(torn.len(), 1, "self-check: the flush wrote one table");
    let crash = disk.power_loss(tree);

    // The first data block starts the file: overwrite bytes inside it.
    let path = at(DB).join("tables").join(torn[0].to_string());
    let mut file = crash.open(&path, &lsm_tree::fs::FsOpenOptions::new().write(true))?;
    file.seek(SeekFrom::Start(40))?;
    file.write_all(&[0xA5; 16])?;
    file.sync_all()?;
    drop(file);

    let tree = config(Arc::new(crash.clone())).open()?;
    assert_eq!(value(&tree, "kept")?.as_deref(), Some("1"));
    assert_eq!(value(&tree, "torn")?, None, "the damaged flush is left out");
    assert_eq!(tree.table_count(), 1);
    assert!(
        crash.exists(&path.with_file_name(format!("{}.torn-flush", torn[0])))?,
        "its table is set aside, not removed"
    );
    Ok(())
}

/// A flush that returned and was damaged since fails the open: a later
/// flush's tables record that it returned, so the damage is the medium's and
/// is reported, not read as a flush a power loss cut short. Guards against an
/// open that drops acknowledged data and serves older versions in its place.
#[test]
fn a_returned_flush_with_a_damaged_block_fails_the_open() -> lsm_tree::Result<()> {
    use std::io::{Seek, SeekFrom, Write};

    let disk = Disk::new();
    let tree = config(disk.fs()).open()?;
    tree.insert("a", "old", 0);
    tree.flush_active_memtable(0)?;
    tree.persist_flushes()?;
    let before = data_files(&disk.crash, &at(DB).join("tables"));
    tree.insert("a", "x".repeat(512), 1);
    tree.flush_active_memtable(0)?;
    let returned: Vec<u64> = data_files(&disk.crash, &at(DB).join("tables"))
        .into_iter()
        .filter(|id| !before.contains(id))
        .collect();
    assert_eq!(returned.len(), 1, "self-check: the flush wrote one table");
    // The flush after it records that it returned.
    tree.insert("b", "2", 2);
    tree.flush_active_memtable(0)?;
    let crash = disk.power_loss(tree);

    let path = at(DB).join("tables").join(returned[0].to_string());
    let mut file = crash.open(&path, &lsm_tree::fs::FsOpenOptions::new().write(true))?;
    file.seek(SeekFrom::Start(40))?;
    file.write_all(&[0xA5; 16])?;
    file.sync_all()?;
    drop(file);

    assert!(
        matches!(
            config(Arc::new(crash.clone())).open(),
            Err(lsm_tree::Error::Unrecoverable)
        ),
        "the damage to a returned flush is reported"
    );
    assert!(crash.exists(&path)?, "its table is kept for a repair");
    Ok(())
}

/// Bytes no compression shrinks: a blob value's frame then holds them as they
/// are, so a byte at a known place of the file is a byte of its payload.
fn noise(len: usize, seed: u64) -> Vec<u8> {
    let mut state = 0x9E37_79B9_7F4A_7C15_u64 ^ seed;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

/// The data files `step` adds to `folder`.
fn added_by(
    fs: &dyn Fs,
    folder: &Path,
    step: impl FnOnce() -> lsm_tree::Result<()>,
) -> lsm_tree::Result<Vec<u64>> {
    let before = data_files(fs, folder);
    step()?;
    Ok(data_files(fs, folder)
        .into_iter()
        .filter(|id| !before.contains(id))
        .collect())
}

/// Overwrites 16 bytes of `path` on `fs` at `offset`, durably.
fn damage(fs: &CrashFs, path: &Path, offset: u64) -> lsm_tree::Result<()> {
    use std::io::{Seek, SeekFrom, Write};
    let mut file = fs.open(path, &lsm_tree::fs::FsOpenOptions::new().write(true))?;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(&[0xA5; 16])?;
    file.sync_all()?;
    Ok(())
}

/// A flush that returned and of which no table is found fails the open: the
/// flush after it names it as returned, so its loss is the medium's. Guards
/// against an open that passes without acknowledged writes.
#[test]
fn a_returned_flush_with_no_table_left_fails_the_open() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let tree = config(disk.fs()).open()?;
    tree.insert("a", "old", 0);
    tree.flush_active_memtable(0)?;
    tree.persist_flushes()?;
    let tables = at(DB).join("tables");
    let returned = added_by(&disk.crash, &tables, || {
        tree.insert("a", "new", 1);
        tree.flush_active_memtable(0).map(drop)
    })?;
    assert_eq!(returned.len(), 1, "self-check: the flush wrote one table");
    tree.insert("b", "2", 2);
    tree.flush_active_memtable(0)?;
    let crash = disk.power_loss(tree);

    crash.remove_file(&tables.join(returned[0].to_string()))?;
    assert!(
        matches!(
            config(Arc::new(crash)).open(),
            Err(lsm_tree::Error::Unrecoverable)
        ),
        "the loss of a returned flush is reported"
    );
    Ok(())
}

/// A blob flush whose files are whole is taken in after a power loss with its
/// values read back, one of them too large for a frame and stored as a head
/// and chunks. Guards against the frame check an open runs refusing a whole
/// flush.
#[test]
fn a_whole_blob_flush_with_chunked_values_is_taken_in() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let tree = blob_config(disk.fs()).open()?;
    let big = noise(4 << 20, 7);
    tree.insert("big", &big, 0);
    for i in 0..200u64 {
        tree.insert(format!("k{i:04}"), noise(64, i), 1 + i);
    }
    tree.flush_active_memtable(0)?;
    let crash = disk.power_loss(tree);

    let tree = blob_config(Arc::new(crash)).open()?;
    assert_eq!(tree.get("big", u64::MAX)?.as_deref(), Some(big.as_slice()));
    assert_eq!(
        tree.get("k0199", u64::MAX)?.as_deref(),
        Some(noise(64, 199).as_slice())
    );
    Ok(())
}

/// A repair of a store that lost a returned flush whole reports the loss as
/// unscopable, so a caller replays its whole journal. Guards against a report
/// that tells the caller only the tail needs replaying while the lost flush's
/// writes lie below it.
#[test]
fn a_repair_reports_a_returned_flush_with_no_table_left() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let tree = config(disk.fs()).open()?;
    tree.insert("a", "old", 0);
    tree.flush_active_memtable(0)?;
    tree.persist_flushes()?;
    let tables = at(DB).join("tables");
    let returned = added_by(&disk.crash, &tables, || {
        tree.insert("a", "new", 1);
        tree.flush_active_memtable(0).map(drop)
    })?;
    tree.insert("b", "2", 2);
    tree.flush_active_memtable(0)?;
    let crash = disk.power_loss(tree);
    crash.remove_file(&tables.join(returned[0].to_string()))?;

    let report = config(Arc::new(crash)).repair()?;
    assert!(
        !report.unknowable_losses.is_empty(),
        "the lost flush is reported: {report:?}"
    );
    assert_eq!(
        report.wal_replay_scope(),
        lsm_tree::WalReplayScope::FullHistory
    );
    Ok(())
}

/// A repair of a store that lost one table of a returned flush reports the
/// loss as unscopable: the surviving tables do not bound what the missing one
/// held. Guards against a report that tells the caller only the tail needs
/// replaying.
#[test]
fn a_repair_reports_a_returned_flush_missing_a_table() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let open = |fs| config(fs).table_target_size(1_024);
    let tree = open(disk.fs()).open()?;
    tree.insert("a", "old", 0);
    tree.flush_active_memtable(0)?;
    tree.persist_flushes()?;
    let tables = at(DB).join("tables");
    let filler = "y".repeat(256);
    let returned = added_by(&disk.crash, &tables, || {
        for i in 0..64u64 {
            tree.insert(format!("k{i:03}"), &filler, 1 + i);
        }
        tree.flush_active_memtable(0).map(drop)
    })?;
    assert!(
        returned.len() > 1,
        "self-check: the flush wrote several tables"
    );
    tree.insert("z", "2", 100);
    tree.flush_active_memtable(0)?;
    let crash = disk.power_loss(tree);
    crash.remove_file(&tables.join(returned[0].to_string()))?;

    let report = open(Arc::new(crash)).repair()?;
    assert_eq!(
        report.wal_replay_scope(),
        lsm_tree::WalReplayScope::FullHistory,
        "the missing table is reported: {report:?}"
    );
    Ok(())
}

/// Files in a tables folder that do not open, under a table's name or a
/// set-aside one, carry no flush mark: the open passes over them and reads
/// the flushes that are whole. Guards against an open refused by a file it
/// cannot tie to any flush.
#[test]
fn files_that_do_not_open_are_passed_over() -> lsm_tree::Result<()> {
    use std::io::Write;
    let disk = Disk::new();
    let tree = config(disk.fs()).open()?;
    tree.insert("a", "1", 0);
    tree.flush_active_memtable(0)?;
    let crash = disk.power_loss(tree);
    let tables = at(DB).join("tables");
    for name in ["900", "901.torn-flush"] {
        let mut file = crash.open(
            &tables.join(name),
            &lsm_tree::fs::FsOpenOptions::new().write(true).create(true),
        )?;
        file.write_all(&[0x5A; 4_096])?;
        file.sync_all()?;
    }

    let tree = config(Arc::new(crash)).open()?;
    assert_eq!(value(&tree, "a")?.as_deref(), Some("1"));
    Ok(())
}

/// A table set aside from a cut-short flush still names the flush before it
/// as returned: an open retried after one that set it aside and failed reads
/// the same evidence. Guards against a retried open or repair that no longer
/// sees the loss of a returned flush only the set-aside table named.
#[test]
fn a_set_aside_table_still_names_the_flush_before_it() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let tree = config(disk.fs()).open()?;
    tree.insert("a", "old", 0);
    tree.flush_active_memtable(0)?;
    tree.persist_flushes()?;
    let tables = at(DB).join("tables");
    let returned = added_by(&disk.crash, &tables, || {
        tree.insert("a", "new", 1);
        tree.flush_active_memtable(0).map(drop)
    })?;
    let witness = added_by(&disk.crash, &tables, || {
        tree.insert("b", "2", 2);
        tree.flush_active_memtable(0).map(drop)
    })?;
    let crash = disk.power_loss(tree);
    // The returned flush is lost, and the flush that names it was set aside
    // by an attempt that failed before it raised the watermark.
    crash.remove_file(&tables.join(returned[0].to_string()))?;
    crash.rename(
        &tables.join(witness[0].to_string()),
        &tables.join(format!("{}.torn-flush", witness[0])),
    )?;

    assert!(
        matches!(
            config(Arc::new(crash)).open(),
            Err(lsm_tree::Error::Unrecoverable)
        ),
        "the loss of the returned flush is still found"
    );
    Ok(())
}

/// An open that fails on a damaged returned flush leaves the store as it
/// found it: a retry fails the same way. Guards against the first open
/// setting aside the cut-short flush whose tables named the damaged one, so a
/// retry no longer knows it returned and opens without it.
#[test]
fn a_failed_open_on_a_damaged_returned_flush_fails_again() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let open = |fs| config(fs).table_target_size(1_024).open();
    let tree = open(disk.fs())?;
    tree.insert("a", "old", 0);
    tree.flush_active_memtable(0)?;
    tree.persist_flushes()?;
    let tables = at(DB).join("tables");
    let damaged = added_by(&disk.crash, &tables, || {
        tree.insert("a", "x".repeat(512), 1);
        tree.flush_active_memtable(0).map(drop)
    })?;
    assert_eq!(damaged.len(), 1, "self-check: the flush wrote one table");
    // The latest flush, of several tables naming the damaged one, cut short.
    let filler = "y".repeat(256);
    let torn = added_by(&disk.crash, &tables, || {
        for i in 0..64u64 {
            tree.insert(format!("k{i:03}"), &filler, 10 + i);
        }
        tree.flush_active_memtable(0).map(drop)
    })?;
    assert!(
        torn.len() > 1,
        "self-check: the latest flush wrote several tables"
    );
    let crash = disk.power_loss(tree);
    damage(&crash, &tables.join(damaged[0].to_string()), 40)?;
    crash.remove_file(&tables.join(torn[0].to_string()))?;

    for attempt in ["first", "retried"] {
        assert!(
            matches!(
                open(Arc::new(crash.clone())),
                Err(lsm_tree::Error::Unrecoverable)
            ),
            "the {attempt} open reports the damaged returned flush"
        );
    }
    Ok(())
}

/// A flush that failed, below one that returned, is a flush that never
/// returned: the table of it a power loss kept is set aside, and the open
/// reads the retry that did return. Guards against an open that takes every
/// number below a returned one as returned and refuses the store.
#[test]
fn a_failed_flush_below_a_returned_one_is_set_aside() -> lsm_tree::Result<()> {
    use lsm_tree::io::ErrorKind;
    let disk = Disk::new();
    // Pooled table files: their names are durable before a flush writes
    // them, so a table whose sync succeeded survives the power loss.
    let open = |fs| {
        Config::new(
            DB,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(fs)
        .table_target_size(1_024)
        .table_file_refill_inline(true)
        .open()
    };
    let tree = open(disk.fs())?;
    let filler = "y".repeat(256);
    for i in 0..64u64 {
        tree.insert(format!("k{i:03}"), &filler, i);
    }
    // One table's sync fails, so the flush fails with the others synced; its
    // removal of them fails too.
    let injector = disk.fs.injector();
    injector.arm(
        FaultRule::new(FaultOp::SyncData, Fault::Error(ErrorKind::Other))
            .on_path("tables")
            .skip(1)
            .once(),
    );
    injector
        .arm(FaultRule::new(FaultOp::RemoveFile, Fault::Error(ErrorKind::Other)).on_path("tables"));
    assert!(
        tree.flush_active_memtable(0).is_err(),
        "self-check: the flush fails"
    );
    injector.clear();
    // The retry returns, and the flush after it names it.
    let lock = tree.get_flush_lock();
    tree.flush(&lock, 0)?;
    drop(lock);
    tree.insert("z", "last", 100);
    tree.flush_active_memtable(0)?;
    let crash = disk.power_loss(tree);

    let tree = open(Arc::new(crash.clone()))?;
    for i in (0..64u64).step_by(7) {
        assert_eq!(
            value(&tree, &format!("k{i:03}"))?.as_deref(),
            Some(filler.as_str()),
            "k{i:03}"
        );
    }
    assert_eq!(value(&tree, "z")?.as_deref(), Some("last"));
    Ok(())
}

/// A flush whose blob file kept its trailer and lost frames is not taken in:
/// the latest flush, which never returned, is set aside, and the open reads
/// without it. Guards against an open that adopts the blob file on its index
/// and fails every read of its values after.
#[test]
fn a_flush_whose_blob_frames_did_not_reach_the_disk_is_left_out() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let tree = blob_config(disk.fs()).open()?;
    tree.insert("a", noise(8_192, 1), 0);
    tree.flush_active_memtable(0)?;
    tree.persist_flushes()?;
    let blobs = at(DB).join("blobs");
    let torn = added_by(&disk.crash, &blobs, || {
        tree.insert("b", noise(8_192, 2), 1);
        tree.flush_active_memtable(0).map(drop)
    })?;
    assert_eq!(torn.len(), 1, "self-check: the flush wrote one blob file");
    let crash = disk.power_loss(tree);
    // A payload byte of the value's frame, well inside it.
    damage(&crash, &blobs.join(torn[0].to_string()), 2_048)?;

    let tree = blob_config(Arc::new(crash)).open()?;
    assert_eq!(value(&tree, "b")?, None, "the flush is left out");
    assert!(value(&tree, "a")?.is_some());
    Ok(())
}

/// A repair that keeps a returned flush's damaged table for its scan keeps the
/// blob files it refers to: the rows the scan salvages read their values.
/// Guards against a sweep that removes them before the scan runs.
#[test]
fn a_repair_keeps_the_blob_files_of_a_damaged_returned_flush() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let tree = blob_config(disk.fs()).open()?;
    tree.insert("0", "x", 0);
    tree.flush_active_memtable(0)?;
    tree.persist_flushes()?;
    let tables = at(DB).join("tables");
    let damaged = added_by(&disk.crash, &tables, || {
        for i in 0..2_000u64 {
            tree.insert(format!("k{i:04}"), noise(64, i), 1 + i);
        }
        tree.flush_active_memtable(0).map(drop)
    })?;
    assert_eq!(damaged.len(), 1, "self-check: the flush wrote one table");
    tree.insert("z", "x", 10_000);
    tree.flush_active_memtable(0)?;
    let crash = disk.power_loss(tree);
    // The first data block: the later rows still read.
    damage(&crash, &tables.join(damaged[0].to_string()), 40)?;

    let disk = Disk::over(crash);
    let report = blob_config(disk.fs()).repair_with_salvage(true)?;
    assert_eq!(
        report.salvaged, 1,
        "self-check: the scan salvages the table"
    );
    let tree = blob_config(disk.fs()).open()?;
    assert_eq!(
        tree.get("k1999", u64::MAX)?.as_deref(),
        Some(noise(64, 1_999).as_slice()),
        "a salvaged row reads its value"
    );
    Ok(())
}

/// A flush of several tables, with range tombstones across them, reads the
/// same after a power loss as before: the open places its tables as the
/// flush's install did, each a run of its own that the placement merges when
/// disjoint.
#[test]
fn a_flush_of_overlapping_tables_reads_the_same_after_a_power_loss() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let tree = config(disk.fs()).table_target_size(1_024).open()?;
    let filler = "y".repeat(256);
    for i in 0..64u64 {
        tree.insert(format!("run-{i:03}"), &filler, i);
    }
    tree.flush_active_memtable(0)?;
    tree.remove_range("run-010", "run-050", 100);
    for i in 0..64u64 {
        tree.insert(format!("run-{i:03}"), "new", 200 + i);
    }
    tree.remove_range("run-020", "run-030", 300);
    tree.flush_active_memtable(0)?;
    let expected: Vec<Option<String>> = (0..64u64)
        .map(|i| value(&tree, &format!("run-{i:03}")))
        .collect::<lsm_tree::Result<_>>()?;
    let crash = disk.power_loss(tree);

    let tree = config(Arc::new(crash)).table_target_size(1_024).open()?;
    for (i, want) in (0..64u64).zip(&expected) {
        assert_eq!(&value(&tree, &format!("run-{i:03}"))?, want, "run-{i:03}");
    }
    Ok(())
}

/// An open that installs a flush whose files never reached the disk (its
/// process died before their sync) makes them durable before a manifest names
/// them: a power loss after that open keeps the tree openable. Guards against
/// an open that persists a manifest naming tables only the page cache holds.
#[test]
fn an_open_syncs_the_flushes_it_installs() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let tree = config(disk.fs()).open()?;
    tree.insert("a", "1", 0);
    // The process dies inside the flush's sync: its table is written and
    // stays, unsynced.
    disk.fs.injector().arm(FaultRule::new(
        FaultOp::RemoveFile,
        Fault::Error(lsm_tree::io::ErrorKind::Other),
    ));
    disk.fail_once(FaultOp::SyncData, "tables");
    assert!(
        tree.flush_active_memtable(0).is_err(),
        "the flush fails on its table's sync"
    );
    // Nothing more is written on the way out.
    for op in [FaultOp::Open, FaultOp::Rename] {
        disk.fs.injector().arm(FaultRule::new(
            op,
            Fault::Error(lsm_tree::io::ErrorKind::Other),
        ));
    }
    drop(tree);
    disk.fs.injector().clear();

    let tree = config(disk.fs()).open()?;
    assert_eq!(value(&tree, "a")?.as_deref(), Some("1"));
    tree.persist_flushes()?;
    let crash = disk.power_loss(tree);

    let tree = config(Arc::new(crash)).open()?;
    assert_eq!(value(&tree, "a")?.as_deref(), Some("1"));
    Ok(())
}

/// A repair over a manifest that loads keeps the flushes that returned since
/// it last persisted. Guards against the repair's sweep of tables the
/// manifest does not name removing ones an open installs from their own
/// description.
#[test]
fn a_repair_keeps_the_flushes_no_manifest_names_yet() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let tree = config(disk.fs()).open()?;
    tree.insert("a", "1", 0);
    tree.flush_active_memtable(0)?;
    tree.insert("b", "2", 1);
    tree.flush_active_memtable(0)?;
    let crash = disk.power_loss(tree);

    let fs: Arc<dyn Fs> = Arc::new(crash);
    config(fs.clone()).repair()?;
    let tree = config(fs).open()?;
    assert_eq!(value(&tree, "a")?.as_deref(), Some("1"));
    assert_eq!(value(&tree, "b")?.as_deref(), Some("2"));
    Ok(())
}

/// A repair decides every flush whose table it found, the ones it leaves out
/// included: a flush table the rebuilt manifest leaves out, still on disk when
/// its removal after the commit fails, is not installed by the next open.
/// Guards against a rebuilt manifest that decides only the flushes of the
/// tables it keeps.
#[test]
fn a_flush_a_repair_left_out_stays_out() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    // The later flush's table stays on disk after the compaction that took it
    // in, as when the power goes before its removal; the earlier one is gone.
    let later = at(DB).join("tables").join("1");
    disk.fs.injector().arm(
        FaultRule::new(
            FaultOp::RemoveFile,
            Fault::Error(lsm_tree::io::ErrorKind::Other),
        )
        .on_path(later.to_str().unwrap_or_default()),
    );
    let tree = config(disk.fs()).open()?;
    tree.insert("a", "1", 0);
    tree.flush_active_memtable(0)?;
    tree.insert("a", "2", 1);
    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, 0)?;
    assert_eq!(tree.table_count(), 1);
    let crash = disk.power_loss(tree);
    // The earlier input is gone, whether its removal ran before the power
    // went or not.
    match crash.remove_file(&at(DB).join("tables").join("0")) {
        Ok(()) => {}
        Err(e) if e.kind() == lsm_tree::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    assert_eq!(
        data_files(&crash, &at(DB).join("tables")),
        [1, 2],
        "self-check: the later input stayed beside the output"
    );

    // The manifest is lost: the repair rebuilds one from the tables.
    for entry in crash.read_dir(&at(DB))? {
        if entry.file_name.starts_with('v') || entry.file_name.starts_with("edits-") {
            crash.remove_file(&at(DB).join(&entry.file_name))?;
        }
    }
    let disk = Disk::over(crash);
    disk.fs.injector().arm(
        FaultRule::new(
            FaultOp::RemoveFile,
            Fault::Error(lsm_tree::io::ErrorKind::Other),
        )
        .on_path(later.to_str().unwrap_or_default()),
    );
    // The repair commits its manifest, leaving the input out, and its removal
    // of that input fails after the commit.
    match config(disk.fs()).repair() {
        Err(lsm_tree::Error::RepairedButUnopened { report, cause }) => {
            assert!(
                matches!(*cause, lsm_tree::Error::Io(_)),
                "the failure is the removal after the commit: {cause:?}"
            );
            assert_eq!(report.excluded_files.len(), 1, "the input is left out");
        }
        other => panic!("self-check: the repair commits, then fails its cleanup: {other:?}"),
    }
    disk.fs.injector().clear();

    let tree = config(disk.fs()).open()?;
    assert_eq!(value(&tree, "a")?.as_deref(), Some("2"));
    assert_eq!(tree.table_count(), 1, "a left-out flush came back");
    Ok(())
}

/// A compaction of flushed tables decides their flushes: the inputs it
/// replaced, still on disk after a power loss that took their deletion, are
/// not installed beside its output. Guards against a flush's tables coming
/// back next to the table that replaced them.
#[test]
fn a_compacted_flush_does_not_come_back_beside_its_output() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let tree = config(disk.fs()).open()?;
    tree.insert("a", "1", 0);
    tree.flush_active_memtable(0)?;
    tree.insert("a", "2", 1);
    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, 0)?;
    assert_eq!(tree.table_count(), 1);
    let crash = disk.power_loss(tree);

    let tree = config(Arc::new(crash)).open()?;
    assert_eq!(value(&tree, "a")?.as_deref(), Some("2"));
    assert_eq!(tree.table_count(), 1);
    Ok(())
}

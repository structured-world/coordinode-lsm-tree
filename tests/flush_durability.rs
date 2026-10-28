// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Flushes whose durability waits for a barrier: what a deferred flush syncs,
//! what a power loss leaves of it before and after the barrier, and the
//! installs that persist it on their own.

use lsm_tree::config::FlushDurability;
use lsm_tree::fs::{
    CountingFs, CrashFs, Fault, FaultFs, FaultOp, FaultRule, Fs, FsDirEntry, FsFile, FsMetadata,
    FsOpenOptions, MemFs, SyncMode,
};
use lsm_tree::{AbstractTree, AnyTree, Config, KvSeparationOptions, SequenceNumberCounter, io};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use test_log::test;

const DB: &str = "/db";

/// `path` as a tree opened at it names it: a config makes its path absolute,
/// which on Windows puts a drive in front of `/db`.
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
        let crash = CrashFs::new(MemFs::new());
        Self {
            fs: Arc::new(FaultFs::new(crash.clone())),
            crash,
        }
    }

    /// The backend a tree runs on.
    fn fs(&self) -> Arc<dyn Fs> {
        self.fs.clone()
    }

    /// Cuts the power under `tree` and returns the disk as it comes back. The
    /// process goes down with the power: the tree, dropped after it, changes
    /// nothing on the disk on its way out, and what was never synced is gone.
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

/// A tree at [`DB`] on `fs` whose flushes are deferred.
fn deferred(fs: Arc<dyn Fs>) -> Config {
    Config::new(
        DB,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(fs)
    .flush_durability(FlushDurability::Deferred)
}

/// The tree under `config`, failing the test when it does not open.
fn open(config: Config) -> AnyTree {
    config.open().expect("the tree opens")
}

/// The value of `key`, as a string.
fn value(tree: &AnyTree, key: &str) -> Option<String> {
    tree.get(key, u64::MAX)
        .expect("get")
        .map(|v| String::from_utf8(v.to_vec()).expect("utf-8 value"))
}

/// The files in `folder` on `fs` named like tables and holding data: an empty
/// one is a pooled file, kept for a table to come.
fn table_files(fs: &dyn Fs, folder: &Path) -> Vec<u64> {
    let mut ids: Vec<u64> = fs
        .read_dir(folder)
        .expect("tables folder")
        .into_iter()
        .filter_map(|entry| {
            let id = entry.file_name.parse::<u64>().ok()?;
            let len = fs.metadata(&folder.join(&entry.file_name)).ok()?.len;
            (len > 0).then_some(id)
        })
        .collect();
    ids.sort_unstable();
    ids
}

/// What [`ProbeFs`] runs on a probe for a heal marker: whether it did its
/// work, or is to run again on the next probe.
type ProbeHook = Box<dyn FnMut() -> bool + Send>;

/// A backend that runs a hook when a table's heal marker is probed for. A
/// checkpoint probes for them after it has flushed and taken its link
/// window, just before it reads the version it links.
struct ProbeFs {
    inner: Arc<dyn Fs>,
    hook: Mutex<Option<ProbeHook>>,
}

impl ProbeFs {
    fn new(inner: Arc<dyn Fs>) -> Self {
        Self {
            inner,
            hook: Mutex::new(None),
        }
    }

    fn set_hook(&self, hook: ProbeHook) {
        *self.hook.lock().unwrap_or_else(PoisonError::into_inner) = Some(hook);
    }

    /// Whether the hook did its work: it is dropped then, with whatever it holds.
    fn fired(&self) -> bool {
        self.hook
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_none()
    }
}

impl Fs for ProbeFs {
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
        self.inner.open(path, opts)
    }
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.create_dir_all(path)
    }
    fn create_dir(&self, path: &Path) -> io::Result<()> {
        self.inner.create_dir(path)
    }
    fn read_dir(&self, path: &Path) -> io::Result<Vec<FsDirEntry>> {
        self.inner.read_dir(path)
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_file(path)
    }
    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_dir_all(path)
    }
    fn same_file(&self, a: &Path, b: &Path) -> io::Result<bool> {
        self.inner.same_file(a, b)
    }
    fn read_link(&self, path: &Path) -> io::Result<Option<PathBuf>> {
        self.inner.read_link(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.inner.rename(from, to)
    }
    fn metadata(&self, path: &Path) -> io::Result<FsMetadata> {
        self.inner.metadata(path)
    }
    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        self.inner.sync_directory(path)
    }
    fn sync_directory_with(&self, path: &Path, mode: SyncMode) -> io::Result<()> {
        self.inner.sync_directory_with(path, mode)
    }
    fn exists(&self, path: &Path) -> io::Result<bool> {
        if path.to_string_lossy().ends_with(".heal-attest") {
            // Taken out to run: the hook's own work may probe again.
            let hook = self
                .hook
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            if let Some(mut hook) = hook
                && !hook()
            {
                self.set_hook(hook);
            }
        }
        self.inner.exists(path)
    }
    fn hard_link(&self, src: &Path, dst: &Path) -> io::Result<()> {
        self.inner.hard_link(src, dst)
    }
    // Without it a checkpoint sees two namespaces and copies with a sync
    // instead of linking, which hides an unsynced table.
    fn backend_id(&self) -> Option<u64> {
        self.inner.backend_id()
    }
}

/// A deferred flush issues no sync at all, of a file, a folder or the
/// manifest; the barrier issues them. Guards against a writer, a folder sync
/// or a manifest edit left on the flush path, which would keep the cost the
/// mode exists to take away.
#[test]
fn a_deferred_flush_syncs_nothing_until_the_barrier() -> lsm_tree::Result<()> {
    let fs = Arc::new(CountingFs::new(Arc::new(MemFs::new())));
    // No pool: a refill would sync the folder for files of its own.
    let tree = open(deferred(fs.clone()).table_file_pool(0));
    tree.insert("a", "1", 0);

    let before = fs.stats().snapshot();
    tree.flush_active_memtable(0)?;
    let flushed = fs
        .stats()
        .snapshot()
        .since(&before)
        .expect("one counter set");
    assert_eq!(flushed.syncs(), 0, "the flush synced: {flushed:?}");
    assert_eq!(
        value(&tree, "a").as_deref(),
        Some("1"),
        "readable before the barrier"
    );

    let before = fs.stats().snapshot();
    tree.persist_flushes()?;
    let persisted = fs
        .stats()
        .snapshot()
        .since(&before)
        .expect("one counter set");
    assert!(
        persisted
            .origin(lsm_tree::fs::WriteOrigin::FlushTable)
            .syncs
            >= 1,
        "the barrier syncs the table: {persisted:?}"
    );
    assert!(
        persisted.origin(lsm_tree::fs::WriteOrigin::Manifest).syncs >= 1,
        "the barrier syncs the manifest: {persisted:?}"
    );

    // Nothing is pending once the barrier returns: a second one writes nothing.
    let before = fs.stats().snapshot();
    tree.persist_flushes()?;
    let again = fs
        .stats()
        .snapshot()
        .since(&before)
        .expect("one counter set");
    assert_eq!(again.syncs(), 0, "an idle barrier synced: {again:?}");
    Ok(())
}

/// A power loss before the barrier takes the flushes after the last barrier
/// and nothing before it, and the tree opens at that barrier with the lost
/// flushes' tables removed. Guards against the manifest naming a table a
/// deferred flush never synced, which would fail the open after the power
/// loss, and against a lost table left on disk.
#[test]
fn a_power_loss_before_the_barrier_reopens_at_the_last_barrier() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let tree = open(deferred(disk.fs()));
    tree.insert("kept", "1", 0);
    tree.flush_active_memtable(0)?;
    tree.persist_flushes()?;
    tree.insert("lost", "2", 1);
    tree.flush_active_memtable(0)?;
    assert_eq!(value(&tree, "lost").as_deref(), Some("2"));

    let crash = disk.power_loss(tree);
    let tree = open(deferred(Arc::new(crash.clone())));
    assert_eq!(
        value(&tree, "kept").as_deref(),
        Some("1"),
        "the barrier's flush"
    );
    assert_eq!(value(&tree, "lost"), None, "the flush after the barrier");
    assert_eq!(tree.table_count(), 1);
    assert_eq!(
        table_files(&crash, &at(DB).join("tables")).len(),
        1,
        "no table of the lost flush is left on disk"
    );
    assert_eq!(tree.get_highest_persisted_seqno(), Some(0));
    Ok(())
}

/// What the barrier covered survives a power loss. Guards against a barrier
/// that writes the manifest without syncing the tables it names.
#[test]
fn a_power_loss_after_the_barrier_keeps_every_flush_before_it() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let tree = open(deferred(disk.fs()).table_file_pool(0));
    for (seqno, key) in [(0, "a"), (1, "b"), (2, "c")] {
        tree.insert(key, "v", seqno);
        tree.flush_active_memtable(0)?;
    }
    tree.persist_flushes()?;
    let crash = disk.power_loss(tree);

    let tree = open(deferred(Arc::new(crash.clone())));
    for key in ["a", "b", "c"] {
        assert_eq!(value(&tree, key).as_deref(), Some("v"), "{key}");
    }
    assert_eq!(tree.table_count(), 3);
    Ok(())
}

/// The highest persisted seqno, by which a caller trims its log, counts only
/// what a barrier made durable, while the highest seqno counts every write the
/// tree holds. Guards against a caller dropping log records a power loss would
/// take from the tree, and against a caller seeding its counter below a write
/// a deferred flush moved out of the memtable.
#[test]
fn the_persisted_seqno_waits_for_the_barrier() -> lsm_tree::Result<()> {
    let tree = open(deferred(Arc::new(MemFs::new())));
    tree.insert("a", "1", 5);
    tree.flush_active_memtable(0)?;
    assert_eq!(tree.get_highest_persisted_seqno(), None);
    assert_eq!(
        tree.get_highest_seqno(),
        Some(5),
        "the flushed write counts"
    );
    tree.persist_flushes()?;
    assert_eq!(tree.get_highest_persisted_seqno(), Some(5));
    Ok(())
}

/// A barrier that fails leaves its flushes installed and pending, whether it
/// fails syncing a table or writing the manifest after the tables are synced,
/// and the next barrier persists them. Guards against a failed barrier
/// forgetting a file it was to make durable, which a later barrier would then
/// name in the manifest without syncing it.
#[test]
fn a_failed_barrier_leaves_its_flushes_for_the_next() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let tree = open(deferred(disk.fs()));
    tree.insert("a", "1", 0);
    tree.flush_active_memtable(0)?;

    for (op, path) in [(FaultOp::SyncData, "tables"), (FaultOp::Write, "edits-")] {
        disk.fs.injector().arm(
            FaultRule::new(op, Fault::Error(lsm_tree::io::ErrorKind::Other))
                .on_path(path)
                .once(),
        );
        assert!(
            tree.persist_flushes().is_err(),
            "the barrier fails on {op:?}"
        );
        assert_eq!(value(&tree, "a").as_deref(), Some("1"), "still readable");
        assert_eq!(tree.get_highest_persisted_seqno(), None, "still pending");
    }
    tree.persist_flushes()?;
    assert_eq!(tree.get_highest_persisted_seqno(), Some(0));

    let crash = disk.power_loss(tree);
    let tree = open(deferred(Arc::new(crash)));
    assert_eq!(value(&tree, "a").as_deref(), Some("1"));
    Ok(())
}

/// A compaction is durable on its own and persists the deferred flushes with
/// it: one that consumed them leaves after a power loss its output and no
/// trace of its inputs. Guards against a compaction edit written from the
/// current version but over a manifest that never named its inputs, and
/// against the inputs being needed after it.
#[test]
fn a_compaction_over_deferred_flushes_survives_a_power_loss() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let tree = open(deferred(disk.fs()));
    for (seqno, key) in [(0, "a"), (1, "b"), (2, "c")] {
        tree.insert(key, "v", seqno);
        tree.flush_active_memtable(0)?;
    }
    tree.major_compact(u64::MAX, 0)?;
    assert_eq!(tree.table_count(), 1);
    let crash = disk.power_loss(tree);

    let tree = open(deferred(Arc::new(crash.clone())));
    for key in ["a", "b", "c"] {
        assert_eq!(value(&tree, key).as_deref(), Some("v"), "{key}");
    }
    assert_eq!(tree.table_count(), 1, "the compaction's output alone");
    assert_eq!(
        table_files(&crash, &at(DB).join("tables")).len(),
        1,
        "no input is left on disk"
    );
    Ok(())
}

/// A flush deferred before a compaction and one deferred after it: the
/// compaction persists the first, the second stays pending, and a power loss
/// takes only the second. Guards against the persisted version losing track
/// of the deferred files a compaction did not consume.
#[test]
fn a_compaction_persists_the_flushes_it_did_not_consume() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let tree = open(deferred(disk.fs()));
    for (seqno, key) in [(0, "a"), (1, "b")] {
        tree.insert(key, "v", seqno);
        tree.flush_active_memtable(0)?;
    }
    tree.major_compact(u64::MAX, 0)?;
    tree.insert("after", "v", 2);
    tree.flush_active_memtable(0)?;
    let crash = disk.power_loss(tree);

    let tree = open(deferred(Arc::new(crash.clone())));
    assert_eq!(value(&tree, "a").as_deref(), Some("v"));
    assert_eq!(value(&tree, "b").as_deref(), Some("v"));
    assert_eq!(value(&tree, "after"), None, "flushed after the compaction");
    Ok(())
}

/// A tree that is dropped persists its deferred flushes. Guards against a
/// clean close leaving a caller to replay what it flushed.
#[test]
fn a_dropped_tree_persists_its_deferred_flushes() -> lsm_tree::Result<()> {
    let crash = CrashFs::new(MemFs::new());
    {
        let tree = open(deferred(Arc::new(crash.clone())));
        tree.insert("a", "1", 0);
        tree.flush_active_memtable(0)?;
    }
    crash.crash();

    let tree = open(deferred(Arc::new(crash.clone())));
    assert_eq!(value(&tree, "a").as_deref(), Some("1"));
    Ok(())
}

/// A checkpoint taken with deferred flushes pending holds them through a
/// power loss: it persists before it links. Guards against a checkpoint
/// linking table files whose bytes nothing synced.
#[test]
fn a_checkpoint_persists_the_deferred_flushes_first() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let tree = open(deferred(disk.fs()));
    tree.insert("a", "1", 0);
    tree.flush_active_memtable(0)?;
    tree.create_checkpoint(&at("/checkpoint"))?;
    let crash = disk.power_loss(tree);

    let restored = open(checkpoint(Arc::new(crash)));
    assert_eq!(value(&restored, "a").as_deref(), Some("1"));
    Ok(())
}

/// The tree a checkpoint at `/checkpoint` on `fs` opens as.
fn checkpoint(fs: Arc<dyn Fs>) -> Config {
    Config::new(
        "/checkpoint",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(fs)
}

/// A flush another thread installs after the checkpoint's own persist, but
/// before it reads the version it links, is linked durable all the same.
/// Guards against a checkpoint linking a deferred table whose bytes nothing
/// synced, which a power loss leaves empty in the checkpoint.
#[test]
fn a_checkpoint_syncs_a_flush_installed_after_its_persist() -> lsm_tree::Result<()> {
    let disk = Disk::new();
    let probe = Arc::new(ProbeFs::new(disk.fs()));
    let tree = open(deferred(probe.clone()));
    tree.insert("a", "1", 0);

    let racer = tree.clone();
    probe.set_hook(Box::new(move || {
        // Not before the checkpoint's own flush: until then the memtable
        // still holds "a".
        if racer.get_highest_memtable_seqno().is_some() {
            return false;
        }
        racer.insert("b", "2", 1);
        std::thread::scope(|scope| {
            scope
                .spawn(|| racer.flush_active_memtable(0))
                .join()
                .expect("the racing flush returns")
        })
        .expect("the racing flush");
        true
    }));
    tree.create_checkpoint(&at("/checkpoint"))?;
    assert!(probe.fired(), "the racing flush ran inside the checkpoint");
    let crash = disk.power_loss(tree);

    let restored = open(checkpoint(Arc::new(crash)));
    assert_eq!(value(&restored, "a").as_deref(), Some("1"));
    assert_eq!(value(&restored, "b").as_deref(), Some("2"));
    Ok(())
}

/// A restore under deferred durability is whole before it clears its marker:
/// what it flushed from the pinned memtable survives a power loss. Guards
/// against a restore that clears its marker over a flush still pending, after
/// which an ordinary open serves the tree without those writes.
#[test]
fn a_restore_persists_its_flush_before_it_clears_its_marker() -> lsm_tree::Result<()> {
    let src = tempfile::tempdir()?;
    let visible = SequenceNumberCounter::default();
    let source = Config::new(
        src.path(),
        SequenceNumberCounter::default(),
        visible.clone(),
    )
    .open()?;
    source.insert("flushed", "1", 0);
    source.flush_active_memtable(0)?;
    source.insert("streamed", "2", 1);
    // A pin sees what the visible watermark has published.
    visible.set(2);
    let pin = source.pin();
    assert!(
        pin.memtable_entries().count() > 0,
        "the pin holds a memtable"
    );

    let disk = Disk::new();
    let root = at("/restored");
    let fs = disk.fs();
    for (folder, files) in [("tables", pin.tables()), ("blobs", pin.blob_files())] {
        for file in files {
            fs.create_dir_all(&root.join(folder))?;
            let mut copy = fs.open(
                &root.join(folder).join(file.id().to_string()),
                &FsOpenOptions::new().write(true).create_new(true),
            )?;
            copy.write_all(&std::fs::read(file.path())?)?;
            copy.flush()?;
        }
    }
    let restored = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(disk.fs())
    .flush_durability(FlushDurability::Deferred)
    .restore_pinned(
        &pin.layout()?,
        pin.memtable_entries(),
        pin.memtable_range_tombstones(),
    )?;
    let crash = disk.power_loss(restored);

    let tree = open(
        Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(Arc::new(crash)),
    );
    assert_eq!(value(&tree, "flushed").as_deref(), Some("1"));
    assert_eq!(value(&tree, "streamed").as_deref(), Some("2"));
    Ok(())
}

/// With values kept apart, the blob files of a deferred flush follow its
/// tables: lost with them before the barrier, kept with them after it.
/// Guards against a blob file the barrier forgot, which would leave a table
/// pointing at values that are gone.
#[test]
fn a_blob_trees_deferred_flush_persists_its_blob_files() -> lsm_tree::Result<()> {
    let blob_config = |fs: Arc<dyn Fs>| {
        deferred(fs)
            .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    };
    let big = "x".repeat(4_096);

    let disk = Disk::new();
    let tree = open(blob_config(disk.fs()));
    tree.insert("kept", &big, 0);
    tree.flush_active_memtable(0)?;
    tree.persist_flushes()?;
    tree.insert("lost", &big, 1);
    tree.flush_active_memtable(0)?;
    let crash = disk.power_loss(tree);

    let tree = open(blob_config(Arc::new(crash.clone())));
    assert_eq!(value(&tree, "kept").as_deref(), Some(big.as_str()));
    assert_eq!(value(&tree, "lost"), None);
    assert_eq!(tree.blob_file_count(), 1, "the barrier's blob file alone");
    Ok(())
}

/// A compaction over deferred flushes of a blob tree keeps their blob files,
/// and its persist syncs them: the compacted table still points into them.
/// Guards against a persist that syncs only the deferred tables a version
/// names, which after a power loss leaves the compacted table pointing at
/// values that are gone.
#[test]
fn a_blob_trees_compaction_syncs_the_blob_files_it_keeps() -> lsm_tree::Result<()> {
    let blob_config = |fs: Arc<dyn Fs>| {
        deferred(fs)
            .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    };
    let big = "x".repeat(4_096);

    let disk = Disk::new();
    let tree = open(blob_config(disk.fs()));
    for (seqno, key) in [(0, "a"), (1, "b")] {
        tree.insert(key, &big, seqno);
        tree.flush_active_memtable(0)?;
    }
    tree.major_compact(u64::MAX, 0)?;
    assert_eq!(tree.table_count(), 1);
    assert_eq!(tree.blob_file_count(), 2, "the compaction keeps both");
    let crash = disk.power_loss(tree);

    let tree = open(blob_config(Arc::new(crash)));
    for key in ["a", "b"] {
        assert_eq!(value(&tree, key).as_deref(), Some(big.as_str()), "{key}");
    }
    Ok(())
}

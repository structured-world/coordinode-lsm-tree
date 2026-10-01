//! Under `SyncMode::Barrier` a flush keeps its writes in order and makes them
//! durable once the devices are flushed: a power loss before that keeps an
//! ordered prefix of the flushes on each device, never a manifest naming a
//! table that did not survive, and loses nothing synced before a device flush.

use lsm_tree::config::LevelRoute;
use lsm_tree::fs::{CrashFs, Fs, MemFs, SyncMode};
use lsm_tree::{AbstractTree, AnyTree, Config, SequenceNumberCounter};
use std::path::PathBuf;
use std::sync::Arc;

const KEYS: usize = 4;

fn key(i: usize) -> String {
    format!("k{i:02}")
}

/// The tree folder, absolute as the engine makes every configured path.
fn base() -> PathBuf {
    std::path::absolute("/db_barrier").expect("absolute base")
}

/// A tree under `SyncMode::Barrier` on `main`, its levels on `hot` when given.
fn open(main: Arc<dyn Fs>, hot: Option<Arc<dyn Fs>>) -> lsm_tree::Result<AnyTree> {
    let base = base();
    let config = Config::new(
        &base,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(main)
    .sync_mode(SyncMode::Barrier);
    match hot {
        Some(fs) => config.level_routes(vec![LevelRoute {
            levels: 0..7,
            path: base.join("hot"),
            fs,
        }]),
        None => config,
    }
    .open()
}

/// Creates the tree and makes it durable, then flushes `KEYS` keys one by
/// one, each flush ordered but not flushed to the devices.
fn flushes(main: &CrashFs, hot: Option<&CrashFs>) -> lsm_tree::Result<AnyTree> {
    let hot = hot.map(|hot| Arc::new(hot.clone()) as Arc<dyn Fs>);
    let tree = open(Arc::new(main.clone()), hot)?;
    tree.sync_devices()?;
    for i in 0..KEYS {
        tree.insert(key(i), "v", i as u64);
        tree.flush_active_memtable(0)?;
    }
    Ok(tree)
}

/// Reopens the tree on what survived and returns how many of the keys, from
/// the first, it holds, asserting it holds no other.
fn surviving_prefix(main: &CrashFs, hot: Option<&CrashFs>, what: &str) -> lsm_tree::Result<usize> {
    let tree = open(main.inner(), hot.map(CrashFs::inner))
        .unwrap_or_else(|e| panic!("{what}: the tree does not reopen: {e}"));
    let mut prefix = 0;
    while prefix < KEYS && tree.contains_key(key(prefix), u64::MAX)? {
        prefix += 1;
    }
    for i in prefix..KEYS {
        assert!(
            !tree.contains_key(key(i), u64::MAX)?,
            "{what}: key {i} survived without key {prefix}"
        );
    }
    Ok(prefix)
}

/// A backend reporting the folder named `nested` as a volume of its own, as a
/// mount nested under the tree folder is, and recording the device flushes
/// asked of it.
struct NestedFs {
    inner: Arc<dyn Fs>,
    nested: &'static str,
    flushed: std::sync::Mutex<Vec<PathBuf>>,
    dir_syncs: std::sync::Mutex<Vec<(PathBuf, SyncMode)>>,
}

impl NestedFs {
    fn new(inner: Arc<dyn Fs>, nested: &'static str) -> Arc<Self> {
        Arc::new(Self {
            inner,
            nested,
            flushed: std::sync::Mutex::default(),
            dir_syncs: std::sync::Mutex::default(),
        })
    }

    fn flushed(&self) -> Vec<PathBuf> {
        self.flushed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// The modes the nested folder was synced with.
    fn nested_dir_syncs(&self) -> Vec<SyncMode> {
        self.dir_syncs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|(path, _)| path.ends_with(self.nested))
            .map(|&(_, mode)| mode)
            .collect()
    }
}

impl Fs for NestedFs {
    fn open(
        &self,
        path: &std::path::Path,
        opts: &lsm_tree::fs::FsOpenOptions,
    ) -> lsm_tree::io::Result<Box<dyn lsm_tree::fs::FsFile>> {
        self.inner.open(path, opts)
    }
    fn create_dir_all(&self, path: &std::path::Path) -> lsm_tree::io::Result<()> {
        self.inner.create_dir_all(path)
    }
    fn read_dir(
        &self,
        path: &std::path::Path,
    ) -> lsm_tree::io::Result<Vec<lsm_tree::fs::FsDirEntry>> {
        self.inner.read_dir(path)
    }
    fn remove_file(&self, path: &std::path::Path) -> lsm_tree::io::Result<()> {
        self.inner.remove_file(path)
    }
    fn remove_dir_all(&self, path: &std::path::Path) -> lsm_tree::io::Result<()> {
        self.inner.remove_dir_all(path)
    }
    fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> lsm_tree::io::Result<()> {
        self.inner.rename(from, to)
    }
    fn metadata(&self, path: &std::path::Path) -> lsm_tree::io::Result<lsm_tree::fs::FsMetadata> {
        self.inner.metadata(path)
    }
    fn sync_directory(&self, path: &std::path::Path) -> lsm_tree::io::Result<()> {
        self.inner.sync_directory(path)
    }
    fn sync_directory_with(
        &self,
        path: &std::path::Path,
        mode: SyncMode,
    ) -> lsm_tree::io::Result<()> {
        self.dir_syncs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((path.to_path_buf(), mode));
        self.inner.sync_directory_with(path, mode)
    }
    fn exists(&self, path: &std::path::Path) -> lsm_tree::io::Result<bool> {
        self.inner.exists(path)
    }
    fn volume_id(&self, path: &std::path::Path) -> Option<u64> {
        Some(u64::from(path.ends_with(self.nested)))
    }
    fn sync_device(&self, path: &std::path::Path) -> lsm_tree::io::Result<()> {
        self.flushed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(path.to_path_buf());
        self.inner.sync_device(path)
    }
    // The rest delegates, so a tight-space compaction runs on it as on its inner.
    fn create_dir(&self, path: &std::path::Path) -> lsm_tree::io::Result<()> {
        self.inner.create_dir(path)
    }
    fn same_file(&self, a: &std::path::Path, b: &std::path::Path) -> lsm_tree::io::Result<bool> {
        self.inner.same_file(a, b)
    }
    fn read_link(&self, path: &std::path::Path) -> lsm_tree::io::Result<Option<PathBuf>> {
        self.inner.read_link(path)
    }
    fn hard_link(&self, src: &std::path::Path, dst: &std::path::Path) -> lsm_tree::io::Result<()> {
        self.inner.hard_link(src, dst)
    }
    fn backend_id(&self) -> Option<u64> {
        self.inner.backend_id()
    }
    fn capabilities(&self, path: &std::path::Path) -> lsm_tree::fs::FsCapabilities {
        self.inner.capabilities(path)
    }
    fn try_disable_cow(&self, path: &std::path::Path) -> lsm_tree::io::Result<()> {
        self.inner.try_disable_cow(path)
    }
    fn punch_hole(
        &self,
        path: &std::path::Path,
        offset: u64,
        len: u64,
    ) -> lsm_tree::io::Result<()> {
        self.inner.punch_hole(path, offset, len)
    }
    fn reflink_file(
        &self,
        src: &std::path::Path,
        dst: &std::path::Path,
    ) -> lsm_tree::io::Result<()> {
        self.inner.reflink_file(src, dst)
    }
    fn truncate_file(&self, path: &std::path::Path) -> lsm_tree::io::Result<()> {
        self.inner.truncate_file(path)
    }
    fn hard_link_count(&self, path: &std::path::Path) -> lsm_tree::io::Result<u64> {
        self.inner.hard_link_count(path)
    }
    fn available_space(&self, path: &std::path::Path) -> lsm_tree::io::Result<u64> {
        self.inner.available_space(path)
    }
    fn allocated_size(&self, path: &std::path::Path) -> lsm_tree::io::Result<Option<u64>> {
        self.inner.allocated_size(path)
    }
    fn extent_is_hole(
        &self,
        path: &std::path::Path,
        offset: u64,
        len: u64,
    ) -> lsm_tree::io::Result<Option<bool>> {
        self.inner.extent_is_hole(path, offset, len)
    }
    fn extent_contains_hole(
        &self,
        path: &std::path::Path,
        offset: u64,
        len: u64,
    ) -> lsm_tree::io::Result<Option<bool>> {
        self.inner.extent_contains_hole(path, offset, len)
    }
}

/// A blob tree whose `blobs` folder is on another volume than the tree folder
/// has that volume flushed too: the blob files a flush wrote are durable once
/// `sync_devices` returns.
#[test]
fn sync_devices_flushes_the_volume_of_the_blobs_folder() -> lsm_tree::Result<()> {
    let fs = NestedFs::new(Arc::new(CrashFs::new(MemFs::new())), "blobs");
    let base = base();
    let tree = Config::new(
        &base,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::clone(&fs) as Arc<dyn Fs>)
    .sync_mode(SyncMode::Barrier)
    .with_kv_separation(Some(
        lsm_tree::KvSeparationOptions::default().separation_threshold(1),
    ))
    .open()?;
    // The syncs of a flush, not those that made the empty folder at open.
    fs.dir_syncs
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear();
    tree.insert("key", "a blob value", 0);
    tree.flush_active_memtable(0)?;
    tree.sync_devices()?;

    let flushed = fs.flushed();
    assert!(
        flushed.iter().any(|path| path.ends_with("blobs")),
        "the blobs volume was not flushed: {flushed:?}"
    );
    // The manifest above is on another volume, so the blob files' folder is
    // synced in full rather than ordered by a barrier it would not respect.
    let modes = fs.nested_dir_syncs();
    assert!(
        !modes.is_empty() && modes.iter().all(|&mode| mode == SyncMode::Full),
        "the blobs folder on its own volume is synced in full: {modes:?}"
    );
    Ok(())
}

/// A tree whose `dicts` folder is on another volume than the tree folder
/// syncs that folder in full when it registers a dictionary, since the
/// version edit naming it is on the volume above, and flushes that volume in
/// `sync_devices`: the tables written against the dictionary are readable once
/// it returns.
#[cfg(feature = "zstd")]
#[test]
fn a_dictionary_on_another_volume_is_durable_once_the_devices_are_flushed() -> lsm_tree::Result<()>
{
    let fs = NestedFs::new(Arc::new(CrashFs::new(MemFs::new())), "dicts");
    let lsm_tree::AnyTree::Standard(tree) = Config::new(
        base(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::clone(&fs) as Arc<dyn Fs>)
    .sync_mode(SyncMode::Barrier)
    .open()?
    else {
        panic!("a standard tree");
    };
    let dict = lsm_tree::ZstdDictionary::new(&b"a dictionary for the nested volume".repeat(40));
    tree.register_zstd_dictionary(Arc::new(dict))?;
    tree.sync_devices()?;

    let modes = fs.nested_dir_syncs();
    assert!(
        !modes.is_empty() && modes.iter().all(|&mode| mode == SyncMode::Full),
        "the dicts folder on its own volume is synced in full: {modes:?}"
    );
    let flushed = fs.flushed();
    assert!(
        flushed.iter().any(|path| path.ends_with("dicts")),
        "the dicts volume was not flushed: {flushed:?}"
    );
    Ok(())
}

/// An install that drops a blob file on another volume than the manifest
/// syncs its manifest edit in full: the blob file is removed once the edit is
/// installed, and a barrier on the manifest's volume would not keep that
/// removal from reaching the blobs volume first.
#[test]
fn an_edit_dropping_a_blob_file_on_another_volume_is_synced_in_full() -> lsm_tree::Result<()> {
    let recorder = lsm_tree::fs::FaultFs::new(MemFs::new());
    let injector = recorder.injector();
    let fs = NestedFs::new(Arc::new(recorder), "blobs");
    let tree = Config::new(
        base(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::clone(&fs) as Arc<dyn Fs>)
    .sync_mode(SyncMode::Barrier)
    .with_kv_separation(Some(
        lsm_tree::KvSeparationOptions::default().separation_threshold(1),
    ))
    .open()?;
    tree.insert("key", "a blob value", 0);
    tree.flush_active_memtable(0)?;
    assert_eq!(tree.blob_file_count(), 1);

    // The tables dropped with it lie on the manifest's volume; only the blob
    // file is elsewhere.
    tree.drop_range::<&[u8], _>(..)?;
    assert_eq!(tree.blob_file_count(), 0, "the drop removed the blob file");
    let edit_syncs = injector.sync_modes_for("edits-");
    assert_eq!(
        edit_syncs.last(),
        Some(&SyncMode::Full),
        "the edit dropping the blob file: {edit_syncs:?}"
    );
    Ok(())
}

/// A tight-space compaction restricts its input and punches the consumed
/// prefix once the restricting edit is installed. With the tables on another
/// volume than the manifest, a barrier on the manifest's volume would not keep
/// the punch from reaching the tables' volume first, and a power loss could
/// reopen the unrestricted version over punched blocks: every edit of the
/// compaction is synced in full.
#[test]
fn a_tight_space_compaction_on_another_volume_syncs_its_edits_in_full() -> lsm_tree::Result<()> {
    let recorder = lsm_tree::fs::FaultFs::new(MemFs::new());
    let injector = recorder.injector();
    let hot = MemFs::with_capacity(u64::MAX);
    let base = base();
    let any = Config::new(
        &base,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(recorder) as Arc<dyn Fs>)
    .sync_mode(SyncMode::Barrier)
    .level_routes(vec![LevelRoute {
        levels: 0..7,
        path: base.join("hot"),
        fs: Arc::new(hot.clone()),
    }])
    .open()?;
    let AnyTree::Standard(tree) = any else {
        panic!("a standard tree");
    };
    for i in 0..2_000u64 {
        tree.insert(format!("key{i:08}"), vec![0xCDu8; 64], i);
    }
    tree.flush_active_memtable(0)?;
    let used = tree.storage_stats()?.used_bytes;

    // Too little room on the tables' volume for a full rewrite of the table:
    // the merge is sliced, restricting the input and punching what it consumed.
    hot.set_capacity(used + used / 4);
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
        c.tight_space_compaction = true;
    })?;
    let before = injector.sync_modes_for("edits-").len();
    tree.major_compact(64 * 1024 * 1024, 0)?;
    assert!(hot.punched_bytes() > 0, "the compaction punched its input");

    let edits = injector.sync_modes_for("edits-");
    let compaction_edits = edits.get(before..).unwrap_or_default();
    assert!(
        !compaction_edits.is_empty() && compaction_edits.iter().all(|&mode| mode == SyncMode::Full),
        "the edits of a compaction punching another volume are synced in full: {compaction_edits:?}"
    );
    Ok(())
}

/// The blob-file twin: a tight-space defragmentation moves a stale blob
/// file's live data start and punches the relocated prefix once the edit is
/// installed. With the blobs folder on its own volume, every edit of that
/// compaction is synced in full.
#[test]
fn a_tight_space_blob_defrag_on_another_volume_syncs_its_edits_in_full() -> lsm_tree::Result<()> {
    // High-entropy values, so the relocation transient is real and the gate
    // slices the merge.
    let value = |i: u64, generation: u8| -> Vec<u8> {
        let mut s = (i + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ (u64::from(generation) << 1);
        (0..200u32)
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                (s >> 24) as u8
            })
            .collect()
    };
    let mem = MemFs::with_capacity(u64::MAX);
    let recorder = lsm_tree::fs::FaultFs::new(mem.clone());
    let injector = recorder.injector();
    let fs = NestedFs::new(Arc::new(recorder), "blobs");
    let any = Config::new(
        base(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::clone(&fs) as Arc<dyn Fs>)
    .sync_mode(SyncMode::Barrier)
    .with_kv_separation(Some(
        lsm_tree::KvSeparationOptions::default()
            .separation_threshold(64)
            .age_cutoff(1.0)
            .staleness_threshold(0.1)
            .file_target_size(48 * 1024),
    ))
    .open()?;
    let AnyTree::Blob(tree) = any else {
        panic!("a blob tree");
    };
    let n = 4_000u64;
    for i in 0..n {
        tree.insert(format!("key{i:08}"), value(i, 1), i);
    }
    tree.flush_active_memtable(0)?;
    // Overwriting the even keys leaves every first-generation blob file about
    // half dead, which a merge learns.
    for i in (0..n).step_by(2) {
        tree.insert(format!("key{i:08}"), value(i, 2), n + i);
    }
    tree.flush_active_memtable(0)?;
    let gc_watermark = 4 * n;
    tree.index.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
    })?;
    tree.major_compact(64 * 1024 * 1024, gc_watermark)?;

    let used = tree.storage_stats()?.used_bytes;
    mem.set_capacity(used + used / 4);
    tree.index.update_runtime_config(|c| {
        c.tight_space_compaction = true;
    })?;
    let punched_before = mem.punched_bytes();
    let before = injector.sync_modes_for("edits-").len();
    tree.major_compact(64 * 1024 * 1024, gc_watermark)?;
    assert!(
        mem.punched_bytes() > punched_before,
        "the defragmentation punched relocated blob prefixes"
    );

    let edits = injector.sync_modes_for("edits-");
    let compaction_edits = edits.get(before..).unwrap_or_default();
    assert!(
        !compaction_edits.is_empty() && compaction_edits.iter().all(|&mode| mode == SyncMode::Full),
        "the edits of a defragmentation punching the blobs volume are synced in full: \
         {compaction_edits:?}"
    );
    Ok(())
}

/// Every prefix of the ordered syncs on one device is a tree that reopens
/// with a prefix of the flushes, and a device flush keeps them all.
#[test]
fn a_power_loss_keeps_an_ordered_prefix_of_barrier_flushes() -> lsm_tree::Result<()> {
    let probe = CrashFs::new(MemFs::new());
    drop(flushes(&probe, None)?);
    let ordered = probe.ordered_syncs();
    assert!(ordered > 0, "the flushes are ordered, not yet durable");

    for kept in 0..=ordered {
        let main = CrashFs::new(MemFs::new());
        drop(flushes(&main, None)?);
        main.crash_keeping(kept);
        surviving_prefix(&main, None, &format!("{kept} of {ordered} kept"))?;
    }

    let main = CrashFs::new(MemFs::new());
    let tree = flushes(&main, None)?;
    tree.sync_devices()?;
    drop(tree);
    main.crash();
    assert_eq!(
        surviving_prefix(&main, None, "after a device flush")?,
        KEYS,
        "nothing synced before a device flush is lost"
    );
    Ok(())
}

/// With the tables on one device and the manifest on another, every pair of
/// prefixes the two devices can keep is a tree that reopens: the manifest
/// never names a table its device lost.
#[test]
fn a_flush_across_two_devices_never_names_a_lost_table() -> lsm_tree::Result<()> {
    let (probe_main, probe_hot) = (CrashFs::new(MemFs::new()), CrashFs::new(MemFs::new()));
    drop(flushes(&probe_main, Some(&probe_hot))?);
    let (ordered_main, ordered_hot) = (probe_main.ordered_syncs(), probe_hot.ordered_syncs());

    for kept_main in 0..=ordered_main {
        for kept_hot in 0..=ordered_hot {
            let (main, hot) = (CrashFs::new(MemFs::new()), CrashFs::new(MemFs::new()));
            drop(flushes(&main, Some(&hot))?);
            main.crash_keeping(kept_main);
            hot.crash_keeping(kept_hot);
            surviving_prefix(
                &main,
                Some(&hot),
                &format!(
                    "manifest device {kept_main} of {ordered_main} kept, \
                     table device {kept_hot} of {ordered_hot} kept"
                ),
            )?;
        }
    }

    let (main, hot) = (CrashFs::new(MemFs::new()), CrashFs::new(MemFs::new()));
    let tree = flushes(&main, Some(&hot))?;
    tree.sync_devices()?;
    drop(tree);
    main.crash();
    hot.crash();
    assert_eq!(
        surviving_prefix(&main, Some(&hot), "after a device flush")?,
        KEYS,
        "nothing synced before a device flush is lost on either device"
    );
    Ok(())
}

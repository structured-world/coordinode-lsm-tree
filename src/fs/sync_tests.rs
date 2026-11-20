// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::{
    Fault, FaultFs, FaultOp, FaultRule, Fs, FsOpenOptions, MemFs, SyncMode, SyncTarget,
    WriteOrigin, Writeback,
};
use std::io::Write as _;
use std::path::Path;

/// A synced flush syncs each file it wrote through the handle it was written
/// through, which reports every writeback error of the file, and opens no
/// new one for the sync: a table and a blob file are each opened for writing
/// once, and synced once, so the sync went through that one handle. Guards
/// against the sync going through a handle opened after the write, which
/// reports a writeback error only while the kernel keeps the file's inode in
/// memory, and against a flush that skips the sync.
#[test]
fn a_synced_flush_syncs_through_the_handles_it_wrote_with() -> crate::Result<()> {
    use crate::{AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter};
    use std::sync::Arc;

    for separated in [false, true] {
        let backend = Arc::new(super::CountingFs::new(Arc::new(MemFs::new())));
        let fs: Arc<dyn Fs> = backend.clone();
        let config = Config::new(
            std::path::absolute("/db")?,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(fs)
        .table_file_pool(0);
        let config = if separated {
            config.with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        } else {
            config
        };
        let tree = config.open()?;
        let before = backend.stats().snapshot();
        tree.insert("a", "a value that goes to a blob file", 0);
        tree.flush_active_memtable(0)?;

        let Some(flush) = backend.stats().snapshot().since(&before) else {
            panic!("the counters only grow");
        };
        let mut origins = vec![WriteOrigin::FlushTable];
        if separated {
            origins.push(WriteOrigin::FlushBlob);
        }
        for origin in origins {
            let files = flush.origin(origin);
            assert_eq!(
                (files.opened, files.syncs),
                (1, 1),
                "{origin:?}: one write open, and one sync, of the flush's file"
            );
        }
    }
    Ok(())
}

/// Each kind of sync target names its path, and its debug form names its
/// kind, path, origin and mode, leaving out the handle of a kept file, which
/// has no debug form of its own. Guards a failure report that names the wrong
/// file, or none.
#[test]
fn a_sync_target_names_its_path_and_kind() -> crate::Result<()> {
    let fs = MemFs::new();
    let file_path = Path::new("/t/1");
    fs.create_dir_all(Path::new("/t"))?;
    let kept = fs.open(file_path, &FsOpenOptions::new().write(true).create(true))?;
    let targets = [
        SyncTarget::FileData {
            path: file_path,
            origin: WriteOrigin::FlushTable,
            mode: SyncMode::Full,
        },
        SyncTarget::OpenFile {
            file: &*kept,
            path: file_path,
            origin: WriteOrigin::FlushBlob,
            mode: SyncMode::Normal,
        },
        SyncTarget::Folder {
            path: Path::new("/t"),
            mode: SyncMode::Barrier,
        },
    ];
    let paths: Vec<&Path> = targets.iter().map(SyncTarget::path).collect();
    assert_eq!(paths, [file_path, file_path, Path::new("/t")]);
    let shown: Vec<String> = targets.iter().map(|target| format!("{target:?}")).collect();
    for (shown, words) in shown.iter().zip([
        ["FileData", "/t/1", "FlushTable", "Full"],
        ["OpenFile", "/t/1", "FlushBlob", "Normal"],
        ["Folder", "/t", "Barrier", "mode"],
    ]) {
        for word in words {
            assert!(shown.contains(word), "{shown} names {word}");
        }
    }
    let open = shown.get(1).map_or("", String::as_str);
    assert!(
        open.ends_with(".. }"),
        "the kept handle is left out: {open}"
    );
    Ok(())
}

/// A backend that keeps the default batch syncs every target of it, files
/// side by side and then the folders, and reports the first failure.
#[test]
fn the_default_batch_syncs_every_target() -> crate::Result<()> {
    let fs = FaultFs::new(MemFs::new());
    fs.create_dir_all(Path::new("/db"))?;
    for name in ["/db/1", "/db/2"] {
        fs.open(
            Path::new(name),
            &FsOpenOptions::new().write(true).create_new(true),
        )?
        .write_all(b"x")?;
    }
    let mode = SyncMode::default();
    let targets = [
        SyncTarget::FileData {
            path: Path::new("/db/1"),
            origin: WriteOrigin::FlushTable,
            mode,
        },
        SyncTarget::FileData {
            path: Path::new("/db/2"),
            origin: WriteOrigin::FlushTable,
            mode,
        },
        SyncTarget::Folder {
            path: Path::new("/db"),
            mode,
        },
    ];
    drop(fs.injector().take_syncs());
    fs.sync_many(&targets)?;
    let synced: Vec<std::path::PathBuf> = fs
        .injector()
        .take_syncs()
        .into_iter()
        .map(|(path, _)| path)
        .collect();
    let (files, folder) = synced.split_at(synced.len().saturating_sub(1));
    let mut files = files.to_vec();
    files.sort();
    assert_eq!(
        files,
        [Path::new("/db/1"), Path::new("/db/2")],
        "both files are synced, in either order: {synced:?}"
    );
    assert_eq!(folder, [Path::new("/db")], "then the folder: {synced:?}");

    fs.injector().arm(
        FaultRule::new(
            FaultOp::SyncDirectory,
            Fault::Error(crate::io::ErrorKind::Other),
        )
        .once(),
    );
    assert!(fs.sync_many(&targets).is_err(), "the folder's failure");
    Ok(())
}

/// Waits until `ready` holds, bounded so a condition that never comes fails
/// the test rather than hangs it.
fn wait_until(ready: impl Fn() -> bool, what: &str) -> crate::io::Result<()> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !ready() {
        if std::time::Instant::now() > deadline {
            return Err(crate::io::Error::other(format!("{what} never happened")));
        }
        std::thread::yield_now();
    }
    Ok(())
}

/// What a failing [`Gated`] sync waits for before it fails.
type Before = Box<dyn Fn() -> bool + Send + Sync>;

/// A handle whose sync fails once `before_failing` holds, or records that it
/// synced and then waits until the batch has recorded the failure
/// (`recorded`).
struct Gated {
    inner: Box<dyn super::FsFile>,
    at: usize,
    fails: bool,
    before_failing: Option<Before>,
    recorded: std::sync::Arc<std::sync::atomic::AtomicBool>,
    synced: std::sync::Arc<parking_lot::Mutex<Vec<usize>>>,
}

impl std::io::Read for Gated {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

impl std::io::Write for Gated {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl std::io::Seek for Gated {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}

impl super::FsFile for Gated {
    fn sync_all(&self) -> crate::io::Result<()> {
        self.sync_data()
    }
    fn sync_data(&self) -> crate::io::Result<()> {
        use std::sync::atomic::Ordering;
        if self.fails {
            if let Some(before) = &self.before_failing {
                wait_until(before, "what the failing sync waits for")?;
            }
            return Err(crate::io::Error::other("the first sync fails"));
        }
        self.synced.lock().push(self.at);
        // Ends only once the failure is recorded and the batch's stop flag
        // set.
        wait_until(
            || self.recorded.load(Ordering::SeqCst),
            "the failure's record",
        )
    }
    fn metadata(&self) -> crate::io::Result<super::FsMetadata> {
        self.inner.metadata()
    }
    fn set_len(&self, size: u64) -> crate::io::Result<()> {
        self.inner.set_len(size)
    }
    fn read_at(&self, buf: &mut [u8], offset: u64) -> crate::io::Result<usize> {
        self.inner.read_at(buf, offset)
    }
    fn lock_exclusive(&self) -> crate::io::Result<()> {
        self.inner.lock_exclusive()
    }
}

/// Once a sync of the batch failed, no file claimed after it is synced: the
/// syncs under way end, and the files no worker had claimed yet are left
/// unsynced. Guards
/// against a worker that finishes its sync after the failure claiming the
/// next file and syncing it, against the batch's contract.
#[test]
fn no_file_sync_starts_after_a_failure() -> crate::Result<()> {
    use std::sync::Arc;

    const FILES: usize = 18;
    let memory = MemFs::new();
    memory.create_dir_all(Path::new("/db"))?;
    let recorded = Arc::new(std::sync::atomic::AtomicBool::new(false));
    super::FAILURE_RECORDED.set(Some(Arc::clone(&recorded)));
    let synced = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let paths: Vec<std::path::PathBuf> = (0..FILES)
        .map(|at| std::path::PathBuf::from(format!("/db/{at}")))
        .collect();
    let mut files = Vec::new();
    for (at, path) in paths.iter().enumerate() {
        // The first file fails only once a peer is inside its sync, so the
        // test sees what a worker does after its sync under way ends.
        let peers = Arc::clone(&synced);
        files.push(Gated {
            inner: memory.open(path, &FsOpenOptions::new().write(true).create_new(true))?,
            at,
            fails: at == 0,
            before_failing: Some(Box::new(move || !peers.lock().is_empty())),
            recorded: Arc::clone(&recorded),
            synced: Arc::clone(&synced),
        });
    }
    let targets: Vec<SyncTarget<'_>> = files
        .iter()
        .zip(&paths)
        .map(|(file, path)| SyncTarget::OpenFile {
            file,
            path,
            origin: WriteOrigin::FlushTable,
            mode: SyncMode::default(),
        })
        .collect();
    let result = super::sync_each_target(&memory, &targets);
    super::FAILURE_RECORDED.set(None);
    assert!(result.is_err());
    let synced = core::mem::take(&mut *synced.lock());
    assert!(
        !synced.is_empty(),
        "a peer's sync was under way at the failure"
    );
    // The workers each claimed one of the first files; the last two were
    // claimed only after the failure, by a worker done with its sync. Checked
    // by claim, not by when a sync began: a file claimed before the failure
    // whose stop check passed may enter its sync after the record, which the
    // batch allows.
    assert!(
        synced.iter().all(|&at| at < FILES - 2),
        "no sync started after the failure: {synced:?}"
    );
    Ok(())
}

/// A backend over [`MemFs`] whose open of `held` waits until the batch has
/// recorded a failure, noting first that it is under way and then that it
/// returned a handle (`returned`), which records its syncs; the open of any
/// other file hands back one whose sync fails once that open is under way.
struct HeldOpen {
    inner: MemFs,
    held: std::path::PathBuf,
    opening: std::sync::Arc<std::sync::atomic::AtomicBool>,
    returned: std::sync::Arc<std::sync::atomic::AtomicBool>,
    recorded: std::sync::Arc<std::sync::atomic::AtomicBool>,
    synced: std::sync::Arc<parking_lot::Mutex<Vec<usize>>>,
}

impl Fs for HeldOpen {
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> crate::io::Result<Box<dyn super::FsFile>> {
        use std::sync::Arc;
        use std::sync::atomic::Ordering;
        let inner = self.inner.open(path, opts)?;
        let held = path == self.held;
        if held {
            self.opening.store(true, Ordering::SeqCst);
            wait_until(
                || self.recorded.load(Ordering::SeqCst),
                "the failure's record",
            )?;
            self.returned.store(true, Ordering::SeqCst);
        }
        let opening = Arc::clone(&self.opening);
        Ok(Box::new(Gated {
            inner,
            at: usize::from(held),
            fails: !held,
            before_failing: Some(Box::new(move || opening.load(Ordering::SeqCst))),
            recorded: Arc::clone(&self.recorded),
            synced: Arc::clone(&self.synced),
        }))
    }
    fn create_dir_all(&self, path: &Path) -> crate::io::Result<()> {
        self.inner.create_dir_all(path)
    }
    fn read_dir(&self, path: &Path) -> crate::io::Result<Vec<super::FsDirEntry>> {
        self.inner.read_dir(path)
    }
    fn remove_file(&self, path: &Path) -> crate::io::Result<()> {
        self.inner.remove_file(path)
    }
    fn remove_dir_all(&self, path: &Path) -> crate::io::Result<()> {
        self.inner.remove_dir_all(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> crate::io::Result<()> {
        self.inner.rename(from, to)
    }
    fn metadata(&self, path: &Path) -> crate::io::Result<super::FsMetadata> {
        self.inner.metadata(path)
    }
    fn sync_directory(&self, path: &Path) -> crate::io::Result<()> {
        self.inner.sync_directory(path)
    }
    fn exists(&self, path: &Path) -> crate::io::Result<bool> {
        self.inner.exists(path)
    }
}

/// A file whose open was under way when another file's sync failed is left
/// unsynced and unspent once the open returns. Guards against a sync started
/// after the batch stopped, whose failure would mark one more file as never
/// to be acknowledged.
#[test]
fn a_file_opened_after_a_failure_is_not_synced() -> crate::Result<()> {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    let backend = HeldOpen {
        inner: MemFs::new(),
        held: std::path::PathBuf::from("/db/held"),
        opening: Arc::new(AtomicBool::new(false)),
        returned: Arc::new(AtomicBool::new(false)),
        recorded: Arc::new(AtomicBool::new(false)),
        synced: Arc::new(parking_lot::Mutex::new(Vec::new())),
    };
    backend.create_dir_all(Path::new("/db"))?;
    let paths = [Path::new("/db/fails"), Path::new("/db/held")];
    for path in paths {
        backend
            .inner
            .open(path, &FsOpenOptions::new().write(true).create_new(true))?;
    }
    super::FAILURE_RECORDED.set(Some(Arc::clone(&backend.recorded)));
    let targets: Vec<SyncTarget<'_>> = paths
        .iter()
        .map(|path| SyncTarget::FileData {
            path,
            origin: WriteOrigin::FlushTable,
            mode: SyncMode::default(),
        })
        .collect();
    let result = super::sync_each_target(&backend, &targets);
    super::FAILURE_RECORDED.set(None);
    let Err(failure) = result else {
        panic!("the first file's sync fails");
    };
    assert_eq!(failure.spent, [0], "only the failed sync is spent");
    assert!(
        backend.returned.load(std::sync::atomic::Ordering::SeqCst),
        "self-check: the held open returned a handle after the failure"
    );
    assert!(
        backend.synced.lock().is_empty(),
        "the held file was not synced once its open returned"
    );
    Ok(())
}

/// A batch of one sync on the std backend syncs that target alone, file or
/// folder, and reports its failure. Guards against a lone sync paying for a
/// ring or a thread.
#[test]
fn a_batch_of_one_on_the_std_backend_syncs_it_alone() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = super::StdFs;
    let file = dir.path().join("1");
    std::fs::write(&file, b"x")?;
    let mode = SyncMode::default();
    fs.sync_many(&[SyncTarget::FileData {
        path: &file,
        origin: WriteOrigin::FlushTable,
        mode,
    }])?;
    fs.sync_many(&[SyncTarget::Folder {
        path: dir.path(),
        mode,
    }])?;
    assert!(
        fs.sync_many(&[SyncTarget::FileData {
            path: &dir.path().join("missing"),
            origin: WriteOrigin::FlushTable,
            mode,
        }])
        .is_err(),
        "a file that cannot be opened"
    );
    Ok(())
}

/// The `io_uring` backend batches its syncs as the std backend does: every
/// target is synced, through the thread's ring where a probe sync goes
/// through one. Guards against its batch falling back to the trait's default,
/// which syncs one target at a time.
#[cfg(io_uring_fs)]
#[test]
fn the_io_uring_backend_syncs_a_batch() -> crate::Result<()> {
    // A kernel without io_uring has no such backend to test.
    let Ok(fs) = super::IoUringFs::new() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let files = [dir.path().join("1"), dir.path().join("2")];
    for file in &files {
        std::fs::write(file, b"x")?;
    }
    let mode = SyncMode::default();
    let mut targets: Vec<SyncTarget<'_>> = files
        .iter()
        .map(|path| SyncTarget::FileData {
            path,
            origin: WriteOrigin::FlushTable,
            mode,
        })
        .collect();
    targets.push(SyncTarget::Folder {
        path: dir.path(),
        mode,
    });
    // A kernel or sandbox that sets rings up and refuses the sync opcode
    // syncs the batch another way, as it should: the ring is asked of the
    // batch only where a probe of its own, before the batch, went through.
    let rings = super::std_fs::ring_runs_syncs(&files[0]);
    assert!(
        !super::std_fs::thread_ring(false),
        "no ring before the batch"
    );
    fs.sync_many(&targets)?;
    if rings {
        assert!(
            super::std_fs::thread_ring(false),
            "the batch went through the thread's ring"
        );
    }
    Ok(())
}

/// A backend over [`MemFs`] that counts the batches handed to it.
struct Batching {
    inner: MemFs,
    batches: std::sync::atomic::AtomicUsize,
}

impl Fs for Batching {
    fn sync_many(&self, targets: &[SyncTarget<'_>]) -> Result<(), super::SyncFailure> {
        self.batches
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        super::sync_each_target(&self.inner, targets)
    }
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> crate::io::Result<Box<dyn super::FsFile>> {
        self.inner.open(path, opts)
    }
    fn create_dir_all(&self, path: &Path) -> crate::io::Result<()> {
        self.inner.create_dir_all(path)
    }
    fn read_dir(&self, path: &Path) -> crate::io::Result<Vec<super::FsDirEntry>> {
        self.inner.read_dir(path)
    }
    fn remove_file(&self, path: &Path) -> crate::io::Result<()> {
        self.inner.remove_file(path)
    }
    fn remove_dir_all(&self, path: &Path) -> crate::io::Result<()> {
        self.inner.remove_dir_all(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> crate::io::Result<()> {
        self.inner.rename(from, to)
    }
    fn metadata(&self, path: &Path) -> crate::io::Result<super::FsMetadata> {
        self.inner.metadata(path)
    }
    fn sync_directory(&self, path: &Path) -> crate::io::Result<()> {
        self.inner.sync_directory(path)
    }
    fn exists(&self, path: &Path) -> crate::io::Result<bool> {
        self.inner.exists(path)
    }
}

/// A batch of a flush's syncs goes to the backend's own batch only when the
/// schedule says so (under a scheduler that idles between queues), and on
/// threads otherwise. Guards against the ring, slower than threads elsewhere,
/// being taken on every device.
#[test]
fn the_backends_batch_is_taken_only_when_scheduled() -> crate::Result<()> {
    use crate::io_policy::SyncSchedule;
    let backend = std::sync::Arc::new(Batching {
        inner: MemFs::new(),
        batches: std::sync::atomic::AtomicUsize::new(0),
    });
    backend.create_dir_all(Path::new("/db"))?;
    let fs: std::sync::Arc<dyn Fs> = backend.clone();
    let folders = [(&fs, Path::new("/db"))];
    for (schedule, batches) in [
        (SyncSchedule::SideBySide, 0),
        (SyncSchedule::OneAtATime, 0),
        (SyncSchedule::Batched, 1),
    ] {
        crate::version::SuperVersions::sync_files(
            core::iter::empty(),
            &folders,
            SyncMode::default(),
            schedule,
            (&*fs, Path::new("/db")),
        )?;
        assert_eq!(
            backend
                .batches
                .swap(0, std::sync::atomic::Ordering::Relaxed),
            batches,
            "{schedule:?}"
        );
    }
    Ok(())
}

/// A backend over `F` on two devices, `fast` and everything else, that
/// records the devices it is asked to flush, and in one log with them the
/// renames that publish files.
struct TwoDevices<F> {
    inner: F,
    /// The root of the second device, as the paths it is asked about name it
    /// (drive-qualified on Windows once a config made it absolute).
    fast: std::path::PathBuf,
    flushed: parking_lot::Mutex<Vec<std::path::PathBuf>>,
    /// Each device flush (`true`) and rename (`false`), by its path, in order.
    events: parking_lot::Mutex<Vec<(bool, std::path::PathBuf)>>,
    /// When set, the next device flush fails before it begins: its folder
    /// does not open.
    unopened: std::sync::atomic::AtomicBool,
    /// Each file opened as a sync by its path opens it (for writing, without
    /// creating or cutting it), with the device flushes made before it.
    sync_opens: parking_lot::Mutex<Vec<(std::path::PathBuf, usize)>>,
}

impl<F> TwoDevices<F> {
    fn new(inner: F, fast: std::path::PathBuf) -> Self {
        Self {
            inner,
            fast,
            flushed: parking_lot::Mutex::new(Vec::new()),
            events: parking_lot::Mutex::new(Vec::new()),
            unopened: std::sync::atomic::AtomicBool::new(false),
            sync_opens: parking_lot::Mutex::new(Vec::new()),
        }
    }
}

impl<F: Fs> Fs for TwoDevices<F> {
    fn volume_id(&self, path: &Path) -> Option<u64> {
        Some(if path.starts_with(&self.fast) { 2 } else { 1 })
    }
    fn sync_device(&self, path: &Path) -> crate::io::Result<()> {
        self.flushed.lock().push(path.to_path_buf());
        self.events.lock().push((true, path.to_path_buf()));
        Ok(())
    }
    fn sync_device_noting(&self, path: &Path) -> Result<(), (crate::io::Error, bool)> {
        if self
            .unopened
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err((
                crate::io::Error::other("the device's folder did not open"),
                false,
            ));
        }
        self.sync_device(path).map_err(|error| (error, true))
    }
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> crate::io::Result<Box<dyn super::FsFile>> {
        if opts.write && !opts.create && !opts.create_new && !opts.truncate {
            let flushes = self.flushed.lock().len();
            self.sync_opens.lock().push((path.to_path_buf(), flushes));
        }
        self.inner.open(path, opts)
    }
    fn create_dir_all(&self, path: &Path) -> crate::io::Result<()> {
        self.inner.create_dir_all(path)
    }
    fn read_dir(&self, path: &Path) -> crate::io::Result<Vec<super::FsDirEntry>> {
        self.inner.read_dir(path)
    }
    fn remove_file(&self, path: &Path) -> crate::io::Result<()> {
        self.inner.remove_file(path)
    }
    fn remove_dir_all(&self, path: &Path) -> crate::io::Result<()> {
        self.inner.remove_dir_all(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> crate::io::Result<()> {
        self.inner.rename(from, to)?;
        self.events.lock().push((false, to.to_path_buf()));
        Ok(())
    }
    fn metadata(&self, path: &Path) -> crate::io::Result<super::FsMetadata> {
        self.inner.metadata(path)
    }
    fn sync_directory(&self, path: &Path) -> crate::io::Result<()> {
        self.inner.sync_directory(path)
    }
    fn exists(&self, path: &Path) -> crate::io::Result<bool> {
        self.inner.exists(path)
    }
}

/// Barrier syncs on a device other than the one the next write goes to are
/// flushed once per device before the batch returns, and syncs on that one
/// device, or under another mode, flush nothing. Guards against a flush's
/// blob files on one device being lost to a power loss that keeps a later
/// flush's tables on another, which name the first flush as returned.
#[test]
fn barrier_syncs_on_another_device_are_flushed_once() -> crate::Result<()> {
    use crate::io_policy::SyncSchedule;
    let backend = std::sync::Arc::new(TwoDevices::new(
        MemFs::new(),
        std::path::PathBuf::from("/fast"),
    ));
    for folder in ["/db", "/db/blobs", "/fast/tables", "/fast/more"] {
        backend.create_dir_all(Path::new(folder))?;
    }
    let fs: std::sync::Arc<dyn Fs> = backend.clone();
    let folders = [
        (&fs, Path::new("/db/blobs")),
        (&fs, Path::new("/fast/tables")),
        (&fs, Path::new("/fast/more")),
    ];
    let flushed = |mode: SyncMode, step: &str| -> crate::Result<Vec<std::path::PathBuf>> {
        crate::version::SuperVersions::sync_files(
            core::iter::empty(),
            &folders,
            mode,
            SyncSchedule::SideBySide,
            (&*fs, Path::new(step)),
        )?;
        Ok(core::mem::take(&mut *backend.flushed.lock()))
    };
    let paths = |list: &[&str]| -> Vec<std::path::PathBuf> {
        list.iter().map(std::path::PathBuf::from).collect()
    };

    assert_eq!(
        flushed(SyncMode::Barrier, "/fast/tables")?,
        paths(&["/db/blobs"]),
        "the other device, once"
    );
    assert_eq!(
        flushed(SyncMode::Barrier, "/db")?,
        paths(&["/fast/tables"]),
        "one flush covers both folders of a device"
    );
    assert_eq!(flushed(SyncMode::Normal, "/fast/tables")?, paths(&[]));
    assert_eq!(flushed(SyncMode::Full, "/fast/tables")?, paths(&[]));
    assert_eq!(
        flushed(SyncMode::Barrier, "/db/blobs")?,
        paths(&["/fast/tables"]),
        "the step's own device is not flushed"
    );
    Ok(())
}

/// An open that takes in a flush whose tables lie on another device than the
/// manifest flushes that device under barrier syncs before it writes the
/// manifest naming them. Guards against that manifest outlasting, through a
/// power loss, the tables it names.
#[test]
fn an_open_flushes_the_device_of_the_flushes_it_takes_in() -> crate::Result<()> {
    use crate::config::LevelRoute;
    use crate::{AbstractTree, Config, SequenceNumberCounter};
    use std::sync::Arc;

    let fault = FaultFs::new(MemFs::new());
    let injector = fault.injector();
    let fast = std::path::absolute("/fast")?;
    let backend = Arc::new(TwoDevices::new(fault, fast.clone()));
    let fs: Arc<dyn Fs> = backend.clone();
    let open = || {
        Config::new(
            std::path::absolute("/db")?,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(Arc::clone(&fs))
        .table_file_pool(0)
        .sync_mode(SyncMode::Barrier)
        .level_routes(vec![LevelRoute {
            levels: 0..1,
            path: fast.clone(),
            fs: Arc::clone(&fs),
        }])
        .open()
    };

    let tree = open()?;
    tree.insert("a", "1", 0);
    tree.flush_active_memtable(0)?;
    // The process dies: nothing the closing tree does reaches the disk.
    for op in [FaultOp::Open, FaultOp::RemoveFile, FaultOp::Rename] {
        injector.arm(FaultRule::new(
            op,
            Fault::Error(crate::io::ErrorKind::Other),
        ));
    }
    drop(tree);
    injector.clear();
    backend.events.lock().clear();

    let tree = open()?;
    assert_eq!(tree.get("a", u64::MAX)?.as_deref(), Some(&b"1"[..]));
    let events = core::mem::take(&mut *backend.events.lock());
    let flushed = events
        .iter()
        .position(|(flush, path)| *flush && path.starts_with(&fast));
    let published = events
        .iter()
        .position(|(flush, path)| !*flush && !path.starts_with(&fast));
    let (Some(flushed), Some(published)) = (flushed, published) else {
        panic!("the tables' device is flushed and a manifest published: {events:?}");
    };
    assert!(
        flushed < published,
        "the tables' device is flushed before the manifest naming them: {events:?}"
    );
    Ok(())
}

/// A persist of a deferred flush whose tables lie on another device, under
/// barrier syncs, whose flush of that device fails before it begins (its
/// folder does not open), leaves the tables to be synced again: the next
/// persist syncs them, flushes the device and succeeds. Guards against the
/// tables marked as failed syncs, which fails every persist naming them until
/// the tree reopens, though no sync of theirs failed.
#[test]
fn a_device_flush_that_never_began_leaves_the_files_to_sync_again() -> crate::Result<()> {
    use crate::config::{FlushDurability, LevelRoute};
    use crate::{AbstractTree, Config, SequenceNumberCounter};
    use std::sync::Arc;

    let fast = std::path::absolute("/fast")?;
    let backend = Arc::new(TwoDevices::new(MemFs::new(), fast.clone()));
    let fs: Arc<dyn Fs> = backend.clone();
    let tree = Config::new(
        std::path::absolute("/db")?,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::clone(&fs))
    .table_file_pool(0)
    .sync_mode(SyncMode::Barrier)
    .flush_durability(FlushDurability::Deferred)
    .level_routes(vec![LevelRoute {
        levels: 0..1,
        path: fast.clone(),
        fs: Arc::clone(&fs),
    }])
    .open()?;
    tree.insert("a", "1", 0);
    tree.flush_active_memtable(0)?;

    backend
        .unopened
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(
        tree.persist_flushes().is_err(),
        "the device's flush did not begin"
    );
    backend.flushed.lock().clear();
    backend.sync_opens.lock().clear();
    tree.persist_flushes()?;
    let flushed = backend
        .flushed
        .lock()
        .iter()
        .position(|path| path.starts_with(&fast));
    let Some(flushed) = flushed else {
        panic!("the retry flushed the tables' device");
    };
    let synced = backend.sync_opens.lock().clone();
    assert!(
        synced
            .iter()
            .any(|(path, flushes_before)| path.starts_with(&fast) && *flushes_before <= flushed),
        "the retry synced the table again before flushing its device: {synced:?}"
    );
    Ok(())
}

/// A writeback request the file refuses turns the requests off: the next
/// one is not made, and neither fails the writer, whose final sync writes
/// the file back. Guards against a hint failing a write.
#[test]
fn a_refused_writeback_request_turns_the_requests_off() -> crate::Result<()> {
    let fs = FaultFs::new(MemFs::new());
    let injector = fs.injector();
    fs.create_dir_all(Path::new("/db"))?;
    let file = fs.open(
        Path::new("/db/1"),
        &FsOpenOptions::new().write(true).create_new(true),
    )?;
    let mut writer = crate::io::FileWriter::for_file(4, file, true);
    let mut writeback = Writeback::every(4);

    injector.arm(FaultRule::new(
        FaultOp::StartWriteback,
        Fault::Error(crate::io::ErrorKind::Unsupported),
    ));
    writer.write_all(b"abcd")?;
    writeback.hint(&mut writer, 0, 4)?;
    writer.write_all(b"efgh")?;
    writeback.hint(&mut writer, 4, 4)?;
    assert_eq!(writeback.bytes, 0, "the requests are off");
    writer.flush()?;
    Ok(())
}

/// A failed batch converts to the error of the sync that failed, whichever
/// error type its caller returns.
#[test]
fn a_sync_failure_converts_to_its_error() {
    let failure = || super::SyncFailure {
        error: crate::io::Error::new(crate::io::ErrorKind::StorageFull, "full"),
        spent: vec![0],
    };
    let io: crate::io::Error = failure().into();
    assert_eq!(io.kind(), crate::io::ErrorKind::StorageFull);
    let crate::Error::Io(error) = crate::Error::from(failure()) else {
        panic!("a sync failure is an I/O error");
    };
    assert_eq!(error.kind(), crate::io::ErrorKind::StorageFull);
}

/// A writeback request a worker makes, refused, turns the requests off too:
/// one queued behind it before the worker ran is not made, and the request
/// after that is not queued. Guards against a hint failing a write of a file
/// written on workers, or hints going on after one was refused.
#[test]
fn a_writeback_request_refused_on_a_worker_turns_the_requests_off() -> crate::Result<()> {
    use crate::fs::{CountingFs, WriteOrigin};

    /// Holds each task until the test runs it.
    #[derive(Default)]
    struct Held(parking_lot::Mutex<Vec<Box<dyn FnOnce() + Send + 'static>>>);
    impl crate::table::writer::CompactionSpawner for Held {
        fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
            self.0.lock().push(task);
        }
    }
    impl Held {
        fn run(&self) {
            loop {
                let tasks = core::mem::take(&mut *self.0.lock());
                if tasks.is_empty() {
                    return;
                }
                for task in tasks {
                    task();
                }
            }
        }
    }

    let faulty = FaultFs::new(MemFs::new());
    let injector = faulty.injector();
    let fs = CountingFs::new(std::sync::Arc::new(faulty));
    fs.create_dir_all(Path::new("/db"))?;
    let file = fs.open(
        Path::new("/db/1"),
        &FsOpenOptions::new()
            .write(true)
            .create_new(true)
            .write_origin(WriteOrigin::FlushTable),
    )?;
    let workers = std::sync::Arc::new(Held::default());
    let mut writer = crate::io::FileWriter::for_file(4, file, true);
    writer.use_spawner_now(workers.clone());
    let mut writeback = Writeback::every(4);

    // The first request is refused; a second one made would go through.
    injector.arm(
        FaultRule::new(
            FaultOp::StartWriteback,
            Fault::Error(crate::io::ErrorKind::Unsupported),
        )
        .once(),
    );
    writer.write_all(b"abcd")?;
    writeback.hint(&mut writer, 0, 4)?;
    writer.write_all(b"efgh")?;
    writeback.hint(&mut writer, 4, 4)?;
    // Both are queued; the worker runs them now.
    workers.run();
    writer.write_all(b"ijkl")?;
    writeback.hint(&mut writer, 8, 4)?;
    assert_eq!(writeback.bytes, 0, "the requests are off");
    workers.run();
    writer.flush()?;
    // The backend's own log, refused requests included: only the first was
    // made, so the refusal fell on it and no later one followed.
    assert_eq!(
        injector.writebacks_for("/db/1"),
        [(0, 4)],
        "the first request reached the backend, and no other"
    );
    let flush = fs.stats().snapshot().origin(WriteOrigin::FlushTable);
    assert_eq!(
        flush.writebacks, 0,
        "no request after the refused one was made"
    );
    Ok(())
}

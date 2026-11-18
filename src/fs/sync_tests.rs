// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::{
    Fault, FaultFs, FaultOp, FaultRule, Fs, FsOpenOptions, MemFs, SyncMode, SyncTarget,
    WriteOrigin, Writeback,
};
use std::io::Write as _;
use std::path::Path;

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
    fs.sync_many(&targets)?;

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
/// target is synced. Guards against its batch falling back to the trait's
/// default, which syncs one target at a time.
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
    fs.sync_many(&targets)?;
    Ok(())
}

/// A backend over [`MemFs`] that counts the batches handed to it.
struct Batching {
    inner: MemFs,
    batches: std::sync::atomic::AtomicUsize,
}

impl Fs for Batching {
    fn sync_many(&self, targets: &[SyncTarget<'_>]) -> crate::io::Result<()> {
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

/// A backend over `F` on two devices, `/fast` and everything else, that
/// records the devices it is asked to flush.
struct TwoDevices<F> {
    inner: F,
    flushed: parking_lot::Mutex<Vec<String>>,
}

impl<F: Fs> Fs for TwoDevices<F> {
    fn volume_id(&self, path: &Path) -> Option<u64> {
        Some(if path.starts_with("/fast") { 2 } else { 1 })
    }
    fn sync_device(&self, path: &Path) -> crate::io::Result<()> {
        self.flushed.lock().push(path.display().to_string());
        Ok(())
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

/// Barrier syncs on a device other than the one the next write goes to are
/// flushed once per device before the batch returns, and syncs on that one
/// device, or under another mode, flush nothing. Guards against a flush's
/// blob files on one device being lost to a power loss that keeps a later
/// flush's tables on another, which name the first flush as returned.
#[test]
fn barrier_syncs_on_another_device_are_flushed_once() -> crate::Result<()> {
    use crate::io_policy::SyncSchedule;
    let backend = std::sync::Arc::new(TwoDevices {
        inner: MemFs::new(),
        flushed: parking_lot::Mutex::new(Vec::new()),
    });
    for folder in ["/db", "/db/blobs", "/fast/tables", "/fast/more"] {
        backend.create_dir_all(Path::new(folder))?;
    }
    let fs: std::sync::Arc<dyn Fs> = backend.clone();
    let folders = [
        (&fs, Path::new("/db/blobs")),
        (&fs, Path::new("/fast/tables")),
        (&fs, Path::new("/fast/more")),
    ];
    let flushed = |mode: SyncMode, step: &str| -> crate::Result<Vec<String>> {
        crate::version::SuperVersions::sync_files(
            core::iter::empty(),
            &folders,
            mode,
            SyncSchedule::SideBySide,
            (&*fs, Path::new(step)),
        )?;
        Ok(core::mem::take(&mut *backend.flushed.lock()))
    };
    let none: [&str; 0] = [];

    assert_eq!(
        flushed(SyncMode::Barrier, "/fast/tables")?,
        ["/db/blobs"],
        "the other device, once"
    );
    assert_eq!(
        flushed(SyncMode::Barrier, "/db")?,
        ["/fast/tables"],
        "one flush covers both folders of a device"
    );
    assert_eq!(flushed(SyncMode::Normal, "/fast/tables")?, none);
    assert_eq!(flushed(SyncMode::Full, "/fast/tables")?, none);
    assert_eq!(
        flushed(SyncMode::Barrier, "/db/blobs")?,
        ["/fast/tables"],
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
    let backend = Arc::new(TwoDevices {
        inner: fault,
        flushed: parking_lot::Mutex::new(Vec::new()),
    });
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
            path: std::path::absolute("/fast")?,
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
    backend.flushed.lock().clear();

    let tree = open()?;
    assert_eq!(tree.get("a", u64::MAX)?.as_deref(), Some(&b"1"[..]));
    assert!(
        backend
            .flushed
            .lock()
            .iter()
            .any(|path| path.starts_with("/fast")),
        "the tables' device is flushed: {:?}",
        backend.flushed.lock()
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

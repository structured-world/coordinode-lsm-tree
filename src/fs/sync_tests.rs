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

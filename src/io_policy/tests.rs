use super::{IoStrategy, attach, refresh_io_policy};
use crate::fs::{Fs, FsDirEntry, FsFile, FsMetadata, FsOpenOptions, MemFs};
use crate::path::Path;
use crate::runtime_config::RuntimeConfig;
use crate::runtime_config::handle::RuntimeConfigHandle;
use alloc::boxed::Box;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// A backend on one volume whose scheduler idles between queues or not, as
/// the test sets it, counting how often it is asked.
struct Scheduled {
    inner: MemFs,
    volume: u64,
    idles: AtomicBool,
    asked: AtomicUsize,
}

impl Scheduled {
    fn on_volume(volume: u64, idles: bool) -> Self {
        Self {
            inner: MemFs::new(),
            volume,
            idles: AtomicBool::new(idles),
            asked: AtomicUsize::new(0),
        }
    }
}

impl Fs for Scheduled {
    fn idles_between_io_queues(&self, _path: &Path) -> bool {
        self.asked.fetch_add(1, Ordering::Relaxed);
        self.idles.load(Ordering::Relaxed)
    }
    fn volume_id(&self, _path: &Path) -> Option<u64> {
        Some(self.volume)
    }
    fn backend_id(&self) -> Option<u64> {
        Some(0x5c_4ed)
    }
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> crate::io::Result<Box<dyn FsFile>> {
        self.inner.open(path, opts)
    }
    fn create_dir_all(&self, path: &Path) -> crate::io::Result<()> {
        self.inner.create_dir_all(path)
    }
    fn read_dir(&self, path: &Path) -> crate::io::Result<Vec<FsDirEntry>> {
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
    fn metadata(&self, path: &Path) -> crate::io::Result<FsMetadata> {
        self.inner.metadata(path)
    }
    fn sync_directory(&self, path: &Path) -> crate::io::Result<()> {
        self.inner.sync_directory(path)
    }
    fn exists(&self, path: &Path) -> crate::io::Result<bool> {
        self.inner.exists(path)
    }
}

fn handle() -> Arc<RuntimeConfigHandle> {
    Arc::new(RuntimeConfigHandle::new(RuntimeConfig::default()))
}

/// A tree opening on a device whose scheduler idles between queues takes the
/// strategy for it, and a second tree on the same location takes it without
/// the device being asked again: the scheduler is read once per location.
#[test]
fn a_location_is_asked_once_for_every_tree_on_it() {
    let fs = Scheduled::on_volume(0xa1, true);
    let (first, second) = (handle(), handle());
    attach(&fs, Path::new("/a"), &first);
    attach(&fs, Path::new("/b"), &second);
    assert_eq!(
        fs.asked.load(Ordering::Relaxed),
        1,
        "asked for the first tree"
    );
    for tree in [&first, &second] {
        assert_eq!(tree.load().io, IoStrategy::for_scheduler(true));
    }
}

/// After the scheduler of a device changes, a refresh asks it again and
/// switches every tree still open on the location; the runtime configuration
/// a caller set is kept.
#[test]
fn a_refresh_switches_every_tree_on_the_location() -> crate::Result<()> {
    let fs = Scheduled::on_volume(0xa2, true);
    let (first, second) = (handle(), handle());
    attach(&fs, Path::new("/a"), &first);
    attach(&fs, Path::new("/b"), &second);
    first.try_update(|config| config.auto_heal = true)?;

    fs.idles.store(false, Ordering::Relaxed);
    assert!(!refresh_io_policy(&fs, Path::new("/a")));
    for tree in [&first, &second] {
        assert_eq!(
            tree.load().io,
            IoStrategy::NONE,
            "switched to the new scheduler"
        );
    }
    assert!(first.load().auto_heal, "the caller's setting is kept");

    // A tree opened after the refresh takes what the location runs now,
    // without asking it.
    let asked = fs.asked.load(Ordering::Relaxed);
    let third = handle();
    attach(&fs, Path::new("/c"), &third);
    assert_eq!(third.load().io, IoStrategy::NONE);
    assert_eq!(fs.asked.load(Ordering::Relaxed), asked);
    Ok(())
}

/// A tree that has closed is not kept by its location: a refresh reaches
/// the trees still open and nothing of the closed one.
#[test]
fn a_closed_tree_is_let_go() {
    let fs = Scheduled::on_volume(0xa3, false);
    let open = handle();
    let closed = handle();
    attach(&fs, Path::new("/a"), &open);
    attach(&fs, Path::new("/b"), &closed);
    let weak = Arc::downgrade(&closed);
    drop(closed);

    fs.idles.store(true, Ordering::Relaxed);
    assert!(refresh_io_policy(&fs, Path::new("/a")));
    assert_eq!(open.load().io, IoStrategy::for_scheduler(true));
    assert!(
        weak.upgrade().is_none(),
        "the location held no closed tree alive"
    );
}

/// A blob writer under the strategy for a device that idles between queues
/// starts no writeback; anywhere else it gathers what is configured.
#[test]
fn the_strategy_sets_the_blob_writeback() {
    assert_eq!(
        IoStrategy::for_scheduler(true).blob_writeback_bytes(1 << 20),
        0
    );
    assert_eq!(IoStrategy::NONE.blob_writeback_bytes(1 << 20), 1 << 20);
}

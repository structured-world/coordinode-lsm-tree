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
    /// When set, the answer read by the ask with this number (counted from
    /// 1) is held until the gate opens.
    hold: Option<(usize, std::sync::Arc<Gate>)>,
}

/// A gate one ask waits at: `entered` once it holds its answer, `open` to
/// let it return it.
#[derive(Default)]
struct Gate {
    entered: AtomicBool,
    open: AtomicBool,
}

impl Scheduled {
    fn on_volume(volume: u64, idles: bool) -> Self {
        Self {
            inner: MemFs::new(),
            volume,
            idles: AtomicBool::new(idles),
            asked: AtomicUsize::new(0),
            hold: None,
        }
    }
}

impl Fs for Scheduled {
    fn idles_between_io_queues(&self, _path: &Path) -> bool {
        let ask = self.asked.fetch_add(1, Ordering::SeqCst) + 1;
        let idles = self.idles.load(Ordering::SeqCst);
        if let Some((held, gate)) = &self.hold
            && *held == ask
        {
            gate.entered.store(true, Ordering::SeqCst);
            while !gate.open.load(Ordering::SeqCst) {
                std::thread::yield_now();
            }
        }
        idles
    }
    /// Volume `0` stands for a backend that names no volume.
    fn volume_id(&self, _path: &Path) -> Option<u64> {
        (self.volume != 0).then_some(self.volume)
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

/// A location whose trees have all closed is not kept: the next tree opened
/// on it asks its device again. Guards against a process that opens trees on
/// ever new volumes keeping a record of each for good.
#[test]
fn a_location_without_open_trees_is_let_go() {
    let fs = Scheduled::on_volume(0xa4, false);
    let closed = handle();
    attach(&fs, Path::new("/a"), &closed);
    drop(closed);
    let reopened = handle();
    attach(&fs, Path::new("/a"), &reopened);
    assert_eq!(
        fs.asked.load(Ordering::Relaxed),
        2,
        "the location was asked anew, its record gone with its last tree"
    );
}

/// A backend that names no volume is asked each time and kept nowhere: a
/// tree opening on it takes what it answers, and a refresh answers what it
/// answers now.
#[test]
fn a_backend_without_volumes_is_asked_each_time() {
    let fs = Scheduled::on_volume(0, true);
    let tree = handle();
    attach(&fs, Path::new("/a"), &tree);
    assert_eq!(tree.load().io, IoStrategy::for_scheduler(true));
    fs.idles.store(false, Ordering::SeqCst);
    assert!(!refresh_io_policy(&fs, Path::new("/a")));
    assert_eq!(fs.asked.load(Ordering::SeqCst), 2);
}

/// Of two refreshes, the trees run what the one that asked last found: a
/// refresh that read the scheduler before another does not publish its older
/// answer over the newer one. Guards against trees left on a stale strategy
/// until the next refresh.
#[test]
fn a_refresh_that_asked_first_does_not_overwrite_a_later_one() {
    let gate = std::sync::Arc::new(Gate::default());
    let mut fs = Scheduled::on_volume(0xa5, true);
    // The open asks once; the first refresh's ask is the second.
    fs.hold = Some((2, std::sync::Arc::clone(&gate)));
    let fs = std::sync::Arc::new(fs);
    let tree = handle();
    attach(&*fs, Path::new("/a"), &tree);

    let first = {
        let fs = std::sync::Arc::clone(&fs);
        std::thread::spawn(move || refresh_io_policy(&*fs, Path::new("/a")))
    };
    while !gate.entered.load(Ordering::SeqCst) {
        std::thread::yield_now();
    }
    // The scheduler changes, and a second refresh asks after the first did.
    fs.idles.store(false, Ordering::SeqCst);
    let second = {
        let fs = std::sync::Arc::clone(&fs);
        std::thread::spawn(move || refresh_io_policy(&*fs, Path::new("/a")))
    };
    // Room for the second to run to its end, where it does not wait.
    std::thread::sleep(std::time::Duration::from_millis(100));
    gate.open.store(true, Ordering::SeqCst);
    assert!(first.join().is_ok_and(|idles| idles));
    assert!(second.join().is_ok_and(|idles| !idles));
    assert_eq!(tree.load().io, IoStrategy::NONE, "the later answer stands");
}

/// Under a scheduler that idles between queues only a flush that started
/// writeback syncs its files one after another; anywhere else every flush
/// syncs them side by side.
#[test]
fn only_a_flush_that_started_writeback_syncs_serially_under_bfq() {
    let bfq = IoStrategy::for_scheduler(true);
    assert!(bfq.serial_syncs(true));
    assert!(!bfq.serial_syncs(false));
    assert!(!IoStrategy::NONE.serial_syncs(true));
    assert!(!IoStrategy::NONE.serial_syncs(false));
}

use super::{IoPolicy, IoStrategy};
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
    /// How often an attach or a refresh asked which backend it is, the last
    /// call each makes before it takes its location's lock.
    located: AtomicUsize,
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
            located: AtomicUsize::new(0),
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
        self.located.fetch_add(1, Ordering::SeqCst);
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

/// The policy prints as itself, without the trees and locations it holds:
/// a config that carries it prints without walking them.
#[test]
fn a_policy_prints_without_what_it_holds() {
    assert_eq!(format!("{:?}", IoPolicy::new()), "IoPolicy { .. }");
}

/// A tree opening on a device whose scheduler idles between queues takes the
/// strategy for it, and a second tree on the same location takes it without
/// the device being asked again: the scheduler is read once per location.
#[test]
fn a_location_is_asked_once_for_every_tree_on_it() {
    let fs = Scheduled::on_volume(0xa1, true);
    let policy = IoPolicy::new();
    let (first, second) = (handle(), handle());
    policy.attach(&fs, Path::new("/a"), &first);
    policy.attach(&fs, Path::new("/b"), &second);
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
    let policy = IoPolicy::new();
    let (first, second) = (handle(), handle());
    policy.attach(&fs, Path::new("/a"), &first);
    policy.attach(&fs, Path::new("/b"), &second);
    first.try_update(|config| config.auto_heal = true)?;

    fs.idles.store(false, Ordering::Relaxed);
    assert!(!policy.refresh(&fs, Path::new("/a")));
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
    policy.attach(&fs, Path::new("/c"), &third);
    assert_eq!(third.load().io, IoStrategy::NONE);
    assert_eq!(fs.asked.load(Ordering::Relaxed), asked);
    Ok(())
}

/// Two policies share nothing: a tree opened with the second asks the device
/// itself, and a refresh through one does not touch the trees of the other.
/// Guards against a registry that every tree of the process shares whether
/// its caller gave it one or not.
#[test]
fn policies_share_no_location() {
    let fs = Scheduled::on_volume(0xa8, true);
    let (one, other) = (IoPolicy::new(), IoPolicy::new());
    let (first, second) = (handle(), handle());
    one.attach(&fs, Path::new("/a"), &first);
    other.attach(&fs, Path::new("/b"), &second);
    assert_eq!(fs.asked.load(Ordering::SeqCst), 2, "each policy asked");

    fs.idles.store(false, Ordering::SeqCst);
    assert!(!one.refresh(&fs, Path::new("/a")));
    assert_eq!(first.load().io, IoStrategy::NONE);
    assert_eq!(
        second.load().io,
        IoStrategy::for_scheduler(true),
        "the other policy's tree keeps its strategy"
    );
}

/// A tree that has closed is not kept by its location: a refresh reaches
/// the trees still open and nothing of the closed one.
#[test]
fn a_closed_tree_is_let_go() {
    let fs = Scheduled::on_volume(0xa3, false);
    let policy = IoPolicy::new();
    let open = handle();
    let closed = handle();
    policy.attach(&fs, Path::new("/a"), &open);
    policy.attach(&fs, Path::new("/b"), &closed);
    let weak = Arc::downgrade(&closed);
    drop(closed);

    fs.idles.store(true, Ordering::Relaxed);
    assert!(policy.refresh(&fs, Path::new("/a")));
    assert_eq!(open.load().io, IoStrategy::for_scheduler(true));
    assert!(
        weak.upgrade().is_none(),
        "the location held no closed tree alive"
    );
}

/// A location whose trees have all closed keeps its answer: a tree opened on
/// it again takes the strategy without the device being asked, and the
/// closed tree is not held. Guards against every open of a tree asking the
/// device anew (half a millisecond on a host with many mounts) once the
/// previous tree on it has closed.
#[test]
fn a_location_keeps_its_answer_after_its_trees_close() {
    let fs = Scheduled::on_volume(0xa4, true);
    let policy = IoPolicy::new();
    let closed = handle();
    policy.attach(&fs, Path::new("/a"), &closed);
    let weak = Arc::downgrade(&closed);
    drop(closed);
    let reopened = handle();
    policy.attach(&fs, Path::new("/a"), &reopened);
    assert_eq!(
        fs.asked.load(Ordering::Relaxed),
        1,
        "the device was asked once"
    );
    assert_eq!(reopened.load().io, IoStrategy::for_scheduler(true));
    assert!(weak.upgrade().is_none(), "the closed tree was let go");
}

/// A backend that names no volume is asked each time and kept nowhere: a
/// tree opening on it takes what it answers, and a refresh answers what it
/// answers now.
#[test]
fn a_backend_without_volumes_is_asked_each_time() {
    let fs = Scheduled::on_volume(0, true);
    let policy = IoPolicy::new();
    let tree = handle();
    policy.attach(&fs, Path::new("/a"), &tree);
    assert_eq!(tree.load().io, IoStrategy::for_scheduler(true));
    fs.idles.store(false, Ordering::SeqCst);
    assert!(!policy.refresh(&fs, Path::new("/a")));
    assert_eq!(fs.asked.load(Ordering::SeqCst), 2);
}

/// Waits until a held ask entered `gate`. A refresh that never reaches the
/// ask fails the test instead of spinning, with the gate opened for a late
/// one to finish.
fn wait_entered(gate: &Gate) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !gate.entered.load(Ordering::SeqCst) {
        if std::time::Instant::now() > deadline {
            gate.open.store(true, Ordering::SeqCst);
            panic!("the refresh never asked the scheduler");
        }
        std::thread::yield_now();
    }
}

/// Waits until attaches and refreshes of `fs` reached their location's lock
/// `times` times in all, failing the test after a bound, with `gate` opened
/// for a held ask to finish.
fn wait_located(fs: &Scheduled, times: usize, gate: &Gate) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while fs.located.load(Ordering::SeqCst) < times {
        if std::time::Instant::now() > deadline {
            gate.open.store(true, Ordering::SeqCst);
            panic!("the call never reached its location");
        }
        std::thread::yield_now();
    }
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
    let policy = Arc::new(IoPolicy::new());
    let tree = handle();
    policy.attach(&*fs, Path::new("/a"), &tree);

    let first = {
        let (fs, policy) = (std::sync::Arc::clone(&fs), Arc::clone(&policy));
        std::thread::spawn(move || policy.refresh(&*fs, Path::new("/a")))
    };
    wait_entered(&gate);
    // The scheduler changes, and a second refresh asks after the first did.
    fs.idles.store(false, Ordering::SeqCst);
    let second = {
        let (fs, policy) = (std::sync::Arc::clone(&fs), Arc::clone(&policy));
        std::thread::spawn(move || policy.refresh(&*fs, Path::new("/a")))
    };
    // The second has reached the location's lock (the open, the first and
    // the second refresh each reach it once), and is given room to run to its
    // end where it does not wait there.
    wait_located(&fs, 3, &gate);
    std::thread::sleep(std::time::Duration::from_millis(100));
    gate.open.store(true, Ordering::SeqCst);
    assert!(first.join().is_ok_and(|idles| idles));
    assert!(second.join().is_ok_and(|idles| !idles));
    assert_eq!(tree.load().io, IoStrategy::NONE, "the later answer stands");
}

/// A refresh of a location no tree opened on yet and a tree opening there
/// meanwhile meet at one entry: the tree runs what the location records,
/// which is what the refresh returns. Guards against the refresh returning
/// one answer while the tree it raced keeps the strategy of an older ask.
#[test]
fn a_refresh_racing_the_first_attach_reaches_its_tree() {
    let gate = std::sync::Arc::new(Gate::default());
    let mut fs = Scheduled::on_volume(0xa8, true);
    // The refresh asks first, and is held with its answer.
    fs.hold = Some((1, std::sync::Arc::clone(&gate)));
    let fs = std::sync::Arc::new(fs);
    let policy = Arc::new(IoPolicy::new());
    let refresh = {
        let (fs, policy) = (std::sync::Arc::clone(&fs), Arc::clone(&policy));
        std::thread::spawn(move || policy.refresh(&*fs, Path::new("/a")))
    };
    wait_entered(&gate);
    // A tree opens while the refresh holds its answer; an ask of its own
    // would read another one.
    fs.idles.store(false, Ordering::SeqCst);
    let tree = handle();
    let attach = {
        let (fs, policy, tree) = (
            std::sync::Arc::clone(&fs),
            Arc::clone(&policy),
            Arc::clone(&tree),
        );
        std::thread::spawn(move || policy.attach(&*fs, Path::new("/a"), &tree))
    };
    // The open has reached the location's lock (after the refresh), and is
    // given room to run to its end where it does not wait there.
    wait_located(&fs, 2, &gate);
    std::thread::sleep(std::time::Duration::from_millis(100));
    gate.open.store(true, Ordering::SeqCst);
    let returned = refresh.join().is_ok_and(|idles| idles);
    assert!(attach.join().is_ok());
    assert!(returned, "the refresh found the scheduler idling");
    assert_eq!(
        tree.load().io,
        IoStrategy::for_scheduler(returned),
        "the tree runs what the refresh returned"
    );
}

/// A location whose scheduler is slow to answer holds up only the trees on
/// it: a tree opening on another location takes its strategy meanwhile.
/// Guards against one device's query stalling every open in the process.
#[test]
fn a_slow_location_does_not_hold_up_another() {
    let gate = std::sync::Arc::new(Gate::default());
    let mut slow = Scheduled::on_volume(0xa6, true);
    // The open asks once; the refresh's ask is the second.
    slow.hold = Some((2, std::sync::Arc::clone(&gate)));
    let slow = std::sync::Arc::new(slow);
    let policy = Arc::new(IoPolicy::new());
    let tree = handle();
    policy.attach(&*slow, Path::new("/a"), &tree);
    let refresh = {
        let (slow, policy) = (std::sync::Arc::clone(&slow), Arc::clone(&policy));
        std::thread::spawn(move || policy.refresh(&*slow, Path::new("/a")))
    };
    wait_entered(&gate);

    let opened = std::sync::Arc::new(AtomicBool::new(false));
    let other = {
        let (opened, policy) = (std::sync::Arc::clone(&opened), Arc::clone(&policy));
        std::thread::spawn(move || {
            let fs = Scheduled::on_volume(0xa7, false);
            let tree = handle();
            policy.attach(&fs, Path::new("/b"), &tree);
            opened.store(true, Ordering::SeqCst);
            tree.load().io
        })
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !opened.load(Ordering::SeqCst) && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    let opened_meanwhile = opened.load(Ordering::SeqCst);
    gate.open.store(true, Ordering::SeqCst);
    assert!(refresh.join().is_ok_and(|idles| idles));
    assert!(other.join().is_ok_and(|io| io == IoStrategy::NONE));
    assert!(
        opened_meanwhile,
        "the other location's tree opened while the slow one was asked"
    );
}

/// Syncs side by side go to the backend as one batch only under a scheduler
/// that idles between queues; elsewhere they run on threads, and a flush that
/// started writeback under it, or a backend that syncs one file at a time,
/// syncs them one after another. Guards against the ring batch, slower than
/// threads without such a scheduler, running everywhere.
#[test]
fn syncs_are_batched_only_under_a_scheduler_that_idles() {
    use super::SyncSchedule;
    let bfq = IoStrategy::for_scheduler(true);
    let other = IoStrategy::for_scheduler(false);
    assert_eq!(bfq.sync_schedule(false, false), SyncSchedule::Batched);
    assert_eq!(bfq.sync_schedule(true, false), SyncSchedule::OneAtATime);
    assert_eq!(other.sync_schedule(false, false), SyncSchedule::SideBySide);
    assert_eq!(other.sync_schedule(true, false), SyncSchedule::SideBySide);
    assert_eq!(
        IoStrategy::NONE.sync_schedule(false, false),
        SyncSchedule::SideBySide
    );
    for strategy in [bfq, other] {
        assert_eq!(
            strategy.sync_schedule(false, true),
            SyncSchedule::OneAtATime
        );
    }
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

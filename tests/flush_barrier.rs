//! `flush_active_memtable` is the durability barrier a caller with its own
//! journal releases that journal on: once it returns `Ok`, every memtable
//! sealed before the call, and the one active at it, survives a power loss.

use lsm_tree::fs::{CrashFs, Fs, FsDirEntry, FsFile, FsMetadata, FsOpenOptions, MemFs, SyncMode};
use lsm_tree::{AbstractTree, SequenceNumberCounter, io};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex, PoisonError};
use test_log::test;

/// A backend that, once armed, stops the first new table file it is asked to
/// create: it reports that the flush is inside, then waits to be told whether
/// the create fails or goes on.
struct GateFs {
    inner: Arc<dyn Fs>,
    armed: Mutex<Option<(Sender<()>, Receiver<bool>)>>,
}

impl GateFs {
    fn gate(&self, path: &Path) -> io::Result<()> {
        let is_new_table = path.to_string_lossy().contains("tables") && !self.inner.exists(path)?;
        if !is_new_table {
            return Ok(());
        }
        let armed = self
            .armed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some((entered, fail)) = armed {
            entered
                .send(())
                .expect("the test waits for the flush to enter");
            if fail.recv().expect("the test says how the create ends") {
                return Err(io::Error::other("the gated create fails"));
            }
        }
        Ok(())
    }
}

impl Fs for GateFs {
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
        self.gate(path)?;
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
        self.inner.exists(path)
    }
    fn hard_link(&self, src: &Path, dst: &Path) -> io::Result<()> {
        self.inner.hard_link(src, dst)
    }
}

/// A flush of a memtable holding nothing but a range tombstone persists the
/// deletion: once the barrier returns `Ok`, the deleted key stays deleted
/// across a power loss.
#[test]
fn a_flush_of_a_lone_range_tombstone_persists_the_deletion() -> lsm_tree::Result<()> {
    let crash = CrashFs::new(MemFs::new());
    let db = "/db";
    let open = |fs: Arc<dyn Fs>| {
        lsm_tree::Config::new(
            db,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(fs)
        .open()
    };

    {
        let tree = open(Arc::new(crash.clone()))?;
        tree.insert("a", "deleted later", 0);
        tree.flush_active_memtable(0)?;
        tree.remove_range("a", "b", 1);
        tree.flush_active_memtable(0)?;
    }

    crash.crash();

    let tree = open(crash.inner())?;
    assert_eq!(
        tree.get("a", u64::MAX)?,
        None,
        "the deletion acknowledged as flushed survives the crash"
    );
    Ok(())
}

/// A memtable sealed by a flush that another thread started, and that fails,
/// is persisted by a later `flush_active_memtable` that returns `Ok`, together
/// with the memtable active at that call.
#[test]
fn a_flush_barrier_persists_a_memtable_another_flush_sealed() -> lsm_tree::Result<()> {
    let crash = CrashFs::new(MemFs::new());
    let (entered_tx, entered) = channel();
    let (release, release_rx) = channel();
    let gate = Arc::new(GateFs {
        inner: Arc::new(crash.clone()),
        armed: Mutex::new(None),
    });
    let db = "/db";
    let open = |fs: Arc<dyn Fs>| {
        lsm_tree::Config::new(
            db,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(fs)
        .open()
    };

    {
        let tree = open(Arc::clone(&gate) as Arc<dyn Fs>)?;
        tree.insert("sealed", "by the first flush", 0);
        *gate.armed.lock().unwrap_or_else(PoisonError::into_inner) = Some((entered_tx, release_rx));

        std::thread::scope(|scope| -> lsm_tree::Result<()> {
            let first = scope.spawn(|| tree.flush_active_memtable(0));
            entered.recv().expect("the first flush reaches its table");
            // The first flush holds the memtable sealed while its table is
            // being made; the barrier is called meanwhile.
            tree.insert("active", "at the barrier", 1);
            let barrier = scope.spawn(|| tree.flush_active_memtable(0));
            release.send(true).expect("the first flush waits");
            assert!(
                first.join().expect("the first flush returns").is_err(),
                "the gated create fails the first flush"
            );
            barrier.join().expect("the barrier returns")
        })?;
    }

    crash.crash();

    let tree = open(crash.inner())?;
    assert_eq!(
        tree.get("sealed", u64::MAX)?.as_deref(),
        Some(&b"by the first flush"[..]),
        "the memtable the failed flush sealed is persisted by the barrier"
    );
    assert_eq!(
        tree.get("active", u64::MAX)?.as_deref(),
        Some(&b"at the barrier"[..]),
        "the memtable active at the barrier is persisted"
    );
    Ok(())
}

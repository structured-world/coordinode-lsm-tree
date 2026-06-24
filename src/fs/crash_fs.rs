// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Power-loss crash simulator [`Fs`] backend for recovery testing.
//!
//! [`CrashFs`] wraps an inner [`Fs`] and models the durability contract of a
//! real disk: bytes become durable only when a file handle is `fsync`ed
//! ([`FsFile::sync_all`] / [`FsFile::sync_data`]). [`CrashFs::crash`] simulates
//! a power loss by rolling every file back to its last-synced content and
//! removing files that were never synced at all — exactly the bytes a real
//! crash would lose. Reopening the storage engine on the post-crash backend
//! (via [`CrashFs::inner`]) then exercises its recovery path against the
//! worst-case durable image.
//!
//! Unlike [`FaultFs`](crate::fs::FaultFs), which makes a *chosen* operation
//! return an error, `CrashFs` is about *durability*: every operation succeeds
//! during the run, but a `crash()` reveals which writes were actually durable.
//! The two compose — wrap `CrashFs` in a `FaultFs` to fail a specific `fsync`
//! and then crash to discard the tail that the failed sync never made durable,
//! reproducing a torn write mid-flush.
//!
//! # Model and limitations
//!
//! Durability is tracked at file-content granularity: a file's durable image is
//! its full content as of its most recent successful `sync_all` / `sync_data`.
//! A file written but never synced vanishes on `crash()`; a synced file keeps
//! exactly its last-synced bytes (a later un-synced append or truncate is
//! rolled back). Directories are not rolled back — the engine fsyncs its data
//! directory on open, and modelling directory-entry durability separately would
//! add no coverage the file-content model lacks for LSM recovery. This is the
//! same power-loss model `RocksDB`'s crash test uses.
//!
//! This is a test/dev surface: it is gated behind the `std` feature and is not
//! part of the production storage path.
//!
//! # Examples
//!
//! ```
//! use lsm_tree::fs::{CrashFs, MemFs};
//!
//! let fs = CrashFs::new(MemFs::new());
//! // ... run a workload through `fs`, fsyncing durable checkpoints ...
//! fs.crash(); // discard everything written since the last fsync of each file
//! // ... reopen the engine on `fs.inner()` and verify recovery ...
//! ```

use super::{
    FileHint, Fs, FsCapabilities, FsDirEntry, FsFile, FsMetadata, FsOpenOptions, SyncMode,
};
use crate::io;
use crate::path::{Path, PathBuf};
use alloc::boxed::Box;
use alloc::sync::Arc;
use alloc::vec::Vec;
use hashbrown::{HashMap, HashSet};

/// Shared crash state: the durable image of each file plus the set of files
/// written during the current run (so `crash()` can vanish never-synced ones).
#[derive(Default)]
struct CrashState {
    /// Last-synced full content per file path. Presence means "this path has
    /// been made durable at least once"; the bytes are its durable image.
    durable: HashMap<PathBuf, Vec<u8>>,
    /// Every path opened for writing this run, whether or not yet synced.
    /// `crash()` visits these (plus `durable` keys) to roll back or remove.
    touched: HashSet<PathBuf>,
}

/// A power-loss crash simulator wrapping an inner [`Fs`].
///
/// See the [module docs](self) for the durability model. The inner backend is
/// held as an [`Arc<dyn Fs>`]; obtain a clone with [`inner`](Self::inner) to
/// reopen the engine directly on the post-crash store (e.g. via
/// [`Config::with_shared_fs`](crate::Config::with_shared_fs)).
#[derive(Clone)]
pub struct CrashFs {
    inner: Arc<dyn Fs>,
    state: Arc<spin::Mutex<CrashState>>,
}

impl CrashFs {
    /// Wraps `inner`, treating its current contents as the initial durable
    /// image (nothing is rolled back until something is written and then a
    /// `crash()` occurs).
    #[must_use]
    pub fn new<F: Fs>(inner: F) -> Self {
        Self::from_shared(Arc::new(inner))
    }

    /// Wraps an existing shared backend handle.
    #[must_use]
    pub fn from_shared(inner: Arc<dyn Fs>) -> Self {
        Self {
            inner,
            state: Arc::new(spin::Mutex::new(CrashState::default())),
        }
    }

    /// Returns a clone of the wrapped backend handle, for reopening the engine
    /// on the same store after a [`crash`](Self::crash).
    #[must_use]
    pub fn inner(&self) -> Arc<dyn Fs> {
        Arc::clone(&self.inner)
    }

    /// Simulates a power loss: every file is rolled back to its last-synced
    /// content, and files written but never synced are removed. After this the
    /// backend holds exactly the bytes a real crash would have left durable.
    ///
    /// # Panics
    ///
    /// Panics if rolling a file back to its durable image fails (open / write /
    /// remove on the inner backend). A crash simulator that could not restore
    /// the durable image would silently under-test recovery, so the failure is
    /// surfaced loudly rather than swallowed. In-memory backends never hit this.
    pub fn crash(&self) {
        let mut state = self.state.lock();
        // Visit every path we wrote, plus any durable path (defensive: a file
        // synced in a prior life but only read this run still gets its durable
        // image reasserted).
        let paths: Vec<PathBuf> = state
            .touched
            .iter()
            .chain(state.durable.keys())
            .cloned()
            .collect();

        for path in paths {
            match state.durable.get(&path) {
                Some(bytes) => self.restore_durable(&path, bytes),
                None => {
                    // Never synced -> the file never existed durably.
                    match self.inner.remove_file(&path) {
                        Ok(()) => {}
                        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                        Err(e) => {
                            panic!("crash(): removing un-synced {} failed: {e}", path.display())
                        }
                    }
                }
            }
        }

        // Post-crash, only durable files exist and they are "clean".
        state.touched = state.durable.keys().cloned().collect();
    }

    /// Overwrites the inner file at `path` with its durable image `bytes`.
    fn restore_durable(&self, path: &Path, bytes: &[u8]) {
        let mut file = self
            .inner
            .open(
                path,
                &FsOpenOptions::new().write(true).create(true).truncate(true),
            )
            .unwrap_or_else(|e| {
                panic!(
                    "crash(): reopening {} for rollback failed: {e}",
                    path.display()
                )
            });
        std::io::Write::write_all(&mut file, bytes).unwrap_or_else(|e| {
            panic!(
                "crash(): rewriting durable image of {} failed: {e}",
                path.display()
            )
        });
    }
}

impl Fs for CrashFs {
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
        let writable = opts.write || opts.create || opts.create_new || opts.append || opts.truncate;
        let inner = self.inner.open(path, opts)?;
        if writable {
            self.state.lock().touched.insert(path.to_path_buf());
        }
        Ok(Box::new(CrashFile {
            inner,
            path: path.to_path_buf(),
            fs: Arc::clone(&self.inner),
            state: Arc::clone(&self.state),
        }))
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
        self.inner.remove_file(path)?;
        let mut state = self.state.lock();
        state.durable.remove(path);
        state.touched.remove(path);
        Ok(())
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_dir_all(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.inner.rename(from, to)?;
        // The destination now reads as the source's content; carry its durable
        // image and write-tracking across the rename.
        let mut state = self.state.lock();
        if let Some(bytes) = state.durable.remove(from) {
            state.durable.insert(to.to_path_buf(), bytes);
        }
        if state.touched.remove(from) {
            state.touched.insert(to.to_path_buf());
        }
        Ok(())
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

    fn backend_id(&self) -> Option<u64> {
        self.inner.backend_id()
    }

    fn volume_id(&self, path: &Path) -> Option<u64> {
        self.inner.volume_id(path)
    }

    fn capabilities(&self, path: &Path) -> FsCapabilities {
        self.inner.capabilities(path)
    }

    fn try_disable_cow(&self, path: &Path) -> io::Result<()> {
        self.inner.try_disable_cow(path)
    }

    fn punch_hole(&self, path: &Path, offset: u64, len: u64) -> io::Result<()> {
        self.inner.punch_hole(path, offset, len)
    }

    fn reflink_file(&self, src: &Path, dst: &Path) -> io::Result<()> {
        self.inner.reflink_file(src, dst)
    }

    fn truncate_file(&self, path: &Path) -> io::Result<()> {
        self.inner.truncate_file(path)
    }

    fn hard_link_count(&self, path: &Path) -> io::Result<u64> {
        self.inner.hard_link_count(path)
    }

    fn available_space(&self, path: &Path) -> io::Result<u64> {
        self.inner.available_space(path)
    }
}

/// A file handle that records its durable image on `fsync`.
struct CrashFile {
    inner: Box<dyn FsFile>,
    path: PathBuf,
    /// Backend handle, used to reopen `path` read-only when snapshotting its
    /// content on `fsync` (the write handle may lack read access).
    fs: Arc<dyn Fs>,
    state: Arc<spin::Mutex<CrashState>>,
}

impl CrashFile {
    /// Captures this path's full current content as its durable image. Called
    /// after a successful `fsync`. Reopens the path read-only because the write
    /// handle being synced is not necessarily readable.
    fn snapshot(&self) -> io::Result<()> {
        let mut rf = self.fs.open(&self.path, &FsOpenOptions::new().read(true))?;
        let mut buf = Vec::new();
        std::io::Read::read_to_end(&mut rf, &mut buf)?;
        self.state.lock().durable.insert(self.path.clone(), buf);
        Ok(())
    }
}

impl std::io::Read for CrashFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

impl std::io::Write for CrashFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // flush pushes to the backend but is NOT a durability barrier; the
        // durable image is captured on sync, not flush.
        self.inner.flush()
    }
}

impl std::io::Seek for CrashFile {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}

impl FsFile for CrashFile {
    fn sync_all(&self) -> io::Result<()> {
        self.inner.sync_all()?;
        self.snapshot()
    }

    fn sync_data(&self) -> io::Result<()> {
        self.inner.sync_data()?;
        self.snapshot()
    }

    fn sync_all_with(&self, mode: SyncMode) -> io::Result<()> {
        self.inner.sync_all_with(mode)?;
        self.snapshot()
    }

    fn sync_data_with(&self, mode: SyncMode) -> io::Result<()> {
        self.inner.sync_data_with(mode)?;
        self.snapshot()
    }

    fn metadata(&self) -> io::Result<FsMetadata> {
        self.inner.metadata()
    }

    fn set_len(&self, size: u64) -> io::Result<()> {
        self.inner.set_len(size)
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.inner.read_at(buf, offset)
    }

    fn lock_exclusive(&self) -> io::Result<()> {
        self.inner.lock_exclusive()
    }

    fn try_lock_exclusive(&self) -> io::Result<bool> {
        self.inner.try_lock_exclusive()
    }

    fn hint(&self, hint: FileHint) -> io::Result<()> {
        self.inner.hint(hint)
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test code"
)]
mod tests;

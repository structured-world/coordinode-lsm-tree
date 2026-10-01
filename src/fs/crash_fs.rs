// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

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
//! rolled back). Hard links the backend reports as one file
//! ([`Fs::same_file`]) share one durable image, since they share their bytes;
//! on a backend where a hard link is a copy each name keeps its own. A write
//! through a symbolic link is tracked under the entry the link chain ends at,
//! the one the write creates or changes.
//!
//! A new directory entry (a created file, the destination of a rename, a hard
//! link or a reflink) is durable only once its parent directory is synced, as
//! POSIX promises: `crash()` removes a file whose entry was never made durable,
//! even when its content was synced. Removed entries are not brought back, and
//! directories themselves are not rolled back. A directory is matched by the
//! path it is named with: an entry made through one spelling of a directory
//! and synced through another (a symlink, `..`) stays pending. Operations
//! that make or remove entries, and directory syncs, are linearized: each
//! happens wholly before or wholly after any other, so an entry is made
//! either before a sync that then covers it or after one that does not.
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
    /// Paths whose directory entry was made this run and whose parent
    /// directory has not been synced since: `crash()` removes them.
    pending_entries: HashSet<PathBuf>,
    /// Names the backend reports as hard links of one file, by group: they
    /// share their bytes, so they share one durable image.
    link_group: HashMap<PathBuf, u64>,
    /// The id the next link group takes.
    next_link_group: u64,
}

impl CrashState {
    /// `path` and every name linked to the same file.
    fn names_of(&self, path: &Path) -> Vec<PathBuf> {
        match self.link_group.get(path) {
            Some(&group) => self
                .link_group
                .iter()
                .filter(|&(_, &other)| other == group)
                .map(|(name, _)| name.clone())
                .collect(),
            None => alloc::vec![path.to_path_buf()],
        }
    }

    /// Records `bytes` as the durable image of `path` and of every name
    /// linked to it.
    fn set_durable(&mut self, path: &Path, bytes: Vec<u8>) {
        let mut names = self.names_of(path);
        let last = names.pop().unwrap_or_else(|| path.to_path_buf());
        for name in names {
            self.durable.insert(name, bytes.clone());
        }
        self.durable.insert(last, bytes);
    }

    /// Records `dst` as a hard link of the file `src` names.
    fn link(&mut self, src: &Path, dst: &Path) {
        let group = if let Some(&group) = self.link_group.get(src) {
            group
        } else {
            let group = self.next_link_group;
            // One group per hard link a test makes, far below 2^64.
            self.next_link_group += 1;
            self.link_group.insert(src.to_path_buf(), group);
            group
        };
        self.link_group.insert(dst.to_path_buf(), group);
    }
}

/// How many symbolic links a path may pass through before the chain is
/// refused: Linux's `MAXSYMLINKS`.
const MAX_SYMLINKS: usize = 40;

/// A power-loss crash simulator wrapping an inner [`Fs`].
///
/// See the module-level documentation for the durability model. The inner backend is
/// held as an [`Arc<dyn Fs>`]; obtain a clone with [`inner`](Self::inner) to
/// reopen the engine directly on the post-crash store (e.g. via
/// [`Config::with_shared_fs`](crate::Config::with_shared_fs)).
#[derive(Clone)]
pub struct CrashFs {
    inner: Arc<dyn Fs>,
    state: Arc<spin::Mutex<CrashState>>,
    /// Linearizes the namespace: every operation that makes or removes a
    /// directory entry holds it from its checks through the backend call to
    /// the state it records, and a directory sync holds it from the backend
    /// sync to clearing what that sync made durable. An entry is then made
    /// either before a sync, and covered by it, or after, and pending; and
    /// whether an open or a rename made an entry is decided by probes no
    /// other entry change can interleave with. Blocking, not spinning: a
    /// directory sync holds it across a system call.
    namespace: Arc<parking_lot::Mutex<()>>,
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
            namespace: Arc::new(parking_lot::Mutex::new(())),
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
        // An entry its directory never made durable is lost with whatever
        // content it had.
        let lost: Vec<PathBuf> = state.pending_entries.drain().collect();
        for path in &lost {
            state.durable.remove(path);
            state.link_group.remove(path);
            state.touched.insert(path.clone());
        }
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
        let CrashState {
            durable,
            link_group,
            ..
        } = &mut *state;
        link_group.retain(|name, _| durable.contains_key(name));
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

    /// Reads the current content of `path` if it already exists, for use as the
    /// initial durable baseline. Returns `Ok(None)` only when the file does not
    /// exist (a brand-new file has no durable image until its first sync); any
    /// other open/read error is propagated rather than swallowed, so a transient
    /// read failure cannot silently drop a pre-existing file's durable image.
    fn read_baseline(&self, path: &Path) -> io::Result<Option<Vec<u8>>> {
        let mut f = match self.inner.open(path, &FsOpenOptions::new().read(true)) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let mut buf = Vec::new();
        std::io::Read::read_to_end(&mut f, &mut buf)?;
        Ok(Some(buf))
    }

    /// Records `path`'s pre-existing content as its durable baseline on the FIRST
    /// touch of this run, then marks it touched. Re-touching is a no-op for the
    /// baseline (a file already touched is tracked by sync; re-reading it would
    /// wrongly promote un-synced bytes to durable). Used by every entry point
    /// that creates or mutates a file's content: `open` (writable), `punch_hole`,
    /// `truncate_file`.
    fn capture_first_touch(&self, path: &Path) -> io::Result<()> {
        let pb = path.to_path_buf();
        // A file is first touched when none of its names was: a write through
        // a linked name already put un-synced bytes in it, and its durable
        // image is the one its names share.
        let first_touch = {
            let state = self.state.lock();
            !state
                .names_of(path)
                .iter()
                .any(|name| state.touched.contains(name))
        };
        if first_touch {
            let baseline = self.read_baseline(path)?;
            if let Some(bytes) = baseline {
                self.state.lock().set_durable(path, bytes);
            }
        }
        self.state.lock().touched.insert(pb);
        Ok(())
    }

    /// The entry a write through `path` lands on: `path` itself, or the end of
    /// the symlink chain it starts, which is the entry an open that creates
    /// makes and the one whose bytes a sync makes durable.
    fn entry_of(&self, path: &Path) -> io::Result<PathBuf> {
        let mut entry = path.to_path_buf();
        for _ in 0..MAX_SYMLINKS {
            let Some(target) = self.inner.read_link(&entry)? else {
                return Ok(entry);
            };
            entry = match entry.parent() {
                Some(directory) if target.is_relative() => directory.join(target),
                _ => target,
            };
        }
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "too many levels of symbolic links",
        ))
    }

    /// Records that `path` got a new directory entry, durable once its parent
    /// directory is synced.
    fn new_entry(&self, path: &Path) {
        self.state.lock().pending_entries.insert(path.to_path_buf());
    }

    /// Runs the backend sync of `directory` and then makes its pending
    /// entries durable, with the namespace held throughout: no entry can be
    /// made or removed between the backend sync and what it is credited with.
    fn sync_entries_of(
        &self,
        directory: &Path,
        sync: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<()> {
        let _namespace = self.namespace.lock();
        sync()?;
        // Directories are matched by spelling: an entry made through one
        // spelling of a directory and synced through another (a symlink,
        // `..`) stays pending. The engine names both from its tree folder.
        self.state
            .lock()
            .pending_entries
            .retain(|entry| crate::file::entry_directory(entry) != directory);
        Ok(())
    }

    /// Records the destination of a copy-style op (`hard_link` / `reflink`): it
    /// mirrors the source's durability so a crash either restores the
    /// linked/cloned bytes (durable source) or removes an un-synced copy.
    ///
    /// The source's durable image comes from one of two places: an entry already
    /// in `durable` (synced or captured this run), or, for a source that is
    /// pre-existing and never touched this run, its on-disk baseline (the same
    /// "current contents are initially durable" rule [`Self::capture_first_touch`]
    /// applies). A source that is touched but not durable holds un-synced bytes,
    /// so the copy correctly inherits no durable image and a crash removes it.
    ///
    /// Reads the baseline outside the state lock (mirroring
    /// [`Self::capture_first_touch`]) so backend I/O never runs under the mutex.
    fn track_copy(&self, src: &Path, dst: &Path) -> io::Result<()> {
        // The backend made `dst` already: record it as a written, pending
        // entry before the fallible baseline read, so a read that fails
        // leaves an entry a crash removes rather than one it never saw.
        let (src_durable, src_touched) = {
            let mut state = self.state.lock();
            state.touched.insert(dst.to_path_buf());
            state.pending_entries.insert(dst.to_path_buf());
            (state.durable.get(src).cloned(), state.touched.contains(src))
        };
        let dst_image = match src_durable {
            Some(bytes) => Some(bytes),
            None if !src_touched => self.read_baseline(src)?,
            None => None,
        };
        if let Some(bytes) = dst_image {
            self.state.lock().durable.insert(dst.to_path_buf(), bytes);
        }
        Ok(())
    }
}

impl Fs for CrashFs {
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
        let writable = opts.write || opts.create || opts.create_new || opts.append || opts.truncate;
        // An open that may create holds the namespace from the probe to the
        // registration, so whether it made the entry is what the open did.
        let may_create = opts.create || opts.create_new;
        let namespace = may_create.then(|| self.namespace.lock());
        // A write follows symlinks: through a dangling one it creates the
        // target, so the target is the entry made and the file tracked.
        let entry = if writable {
            self.entry_of(path)?
        } else {
            path.to_path_buf()
        };
        let creates = may_create && !self.inner.exists(&entry)?;
        if writable {
            // Capture the pre-existing durable image BEFORE the open (which may
            // truncate); a brand-new file captures nothing, so a crash before its
            // first sync removes it.
            self.capture_first_touch(&entry)?;
        }
        let inner = self.inner.open(path, opts)?;
        if creates {
            self.new_entry(&entry);
        }
        drop(namespace);
        Ok(Box::new(CrashFile {
            inner,
            path: entry,
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
        let _namespace = self.namespace.lock();
        self.inner.remove_file(path)?;
        let mut state = self.state.lock();
        state.durable.remove(path);
        state.touched.remove(path);
        state.pending_entries.remove(path);
        state.link_group.remove(path);
        Ok(())
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        let _namespace = self.namespace.lock();
        self.inner.remove_dir_all(path)?;
        // Purge crash state for every tracked path under the removed directory,
        // so crash() neither resurrects nor panics recreating a file whose
        // parent is gone.
        let mut state = self.state.lock();
        state.durable.retain(|k, _| !k.starts_with(path));
        state.touched.retain(|k| !k.starts_with(path));
        state.pending_entries.retain(|k| !k.starts_with(path));
        state.link_group.retain(|k, _| !k.starts_with(path));
        Ok(())
    }

    fn same_file(&self, a: &Path, b: &Path) -> io::Result<bool> {
        self.inner.same_file(a, b)
    }

    fn read_link(&self, path: &Path) -> io::Result<Option<PathBuf>> {
        self.inner.read_link(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let _namespace = self.namespace.lock();
        self.inner.rename(from, to)?;
        // POSIX rename(2): when both entries are links to one file (one path,
        // or two hard links of one inode) the call succeeds and changes
        // nothing, so `from` is still there and the crash state stays as it
        // was. Whether it is still there is what tells, on every backend: a
        // file-identity probe follows symlinks, which rename does not, and
        // not every backend can answer one. The namespace is held, so no other
        // operation can have made `from` again since the rename, and the
        // inner backend is the disk this simulates, so its answer is the
        // truth: a fault layer composes above the simulator, never below it.
        // A probe that fails is read as an ordinary rename.
        if from == to || matches!(self.inner.exists(from), Ok(true)) {
            return Ok(());
        }
        let mut state = self.state.lock();
        // The destination is replaced on disk: drop its prior durable image and
        // write-tracking first, then carry the source's across (the rename is
        // as durable as the source was). Clearing `to` unconditionally is what
        // stops a replaced, previously-synced destination from being resurrected
        // to its stale content on crash.
        let from_durable = state.durable.remove(from);
        state.durable.remove(to);
        if let Some(bytes) = from_durable {
            state.durable.insert(to.to_path_buf(), bytes);
        }
        let from_touched = state.touched.remove(from);
        state.touched.remove(to);
        if from_touched {
            state.touched.insert(to.to_path_buf());
        }
        // The destination's entry is new until its directory is synced.
        state.pending_entries.remove(from);
        state.pending_entries.insert(to.to_path_buf());
        // The name moves with its file: `to` stops naming what it named, and
        // names whatever `from` was linked to.
        state.link_group.remove(to);
        if let Some(group) = state.link_group.remove(from) {
            state.link_group.insert(to.to_path_buf(), group);
        }
        Ok(())
    }

    fn metadata(&self, path: &Path) -> io::Result<FsMetadata> {
        self.inner.metadata(path)
    }

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        self.sync_entries_of(path, || self.inner.sync_directory(path))
    }

    fn sync_directory_with(&self, path: &Path, mode: SyncMode) -> io::Result<()> {
        self.sync_entries_of(path, || self.inner.sync_directory_with(path, mode))
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        self.inner.exists(path)
    }

    fn hard_link(&self, src: &Path, dst: &Path) -> io::Result<()> {
        let _namespace = self.namespace.lock();
        self.inner.hard_link(src, dst)?;
        self.track_copy(src, dst)?;
        // On a backend where a hard link is a second name of one file, a sync
        // through either name makes the other's bytes durable too; on one
        // where it is a copy (`MemFs`), each name keeps its own image.
        if self.inner.same_file(src, dst)? {
            self.state.lock().link(src, dst);
        }
        Ok(())
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
        // Content-mutating: capture the pre-mutation durable image so an
        // un-synced punch rolls back on crash.
        self.capture_first_touch(&self.entry_of(path)?)?;
        self.inner.punch_hole(path, offset, len)
    }

    fn reflink_file(&self, src: &Path, dst: &Path) -> io::Result<()> {
        let _namespace = self.namespace.lock();
        self.inner.reflink_file(src, dst)?;
        self.track_copy(src, dst)?;
        Ok(())
    }

    fn truncate_file(&self, path: &Path) -> io::Result<()> {
        // Content-mutating: capture the pre-truncate image so an un-synced
        // reclaim rolls back on crash.
        self.capture_first_touch(&self.entry_of(path)?)?;
        self.inner.truncate_file(path)
    }

    fn hard_link_count(&self, path: &Path) -> io::Result<u64> {
        self.inner.hard_link_count(path)
    }

    fn available_space(&self, path: &Path) -> io::Result<u64> {
        self.inner.available_space(path)
    }

    fn allocated_size(&self, path: &Path) -> io::Result<Option<u64>> {
        self.inner.allocated_size(path)
    }

    fn extent_is_hole(&self, path: &Path, offset: u64, len: u64) -> io::Result<Option<bool>> {
        self.inner.extent_is_hole(path, offset, len)
    }

    fn extent_contains_hole(&self, path: &Path, offset: u64, len: u64) -> io::Result<Option<bool>> {
        self.inner.extent_contains_hole(path, offset, len)
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
        self.state.lock().set_durable(&self.path, buf);
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

    fn hard_link_count(&self) -> io::Result<u64> {
        self.inner.hard_link_count()
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
#[expect(clippy::unwrap_used, clippy::expect_used, reason = "test code")]
mod tests;

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
//! on a backend where a hard link is a copy each name keeps its own. Every
//! name is tracked resolved, as the backend resolves it: a symbolic link,
//! final or in a directory along the way, and `..` lead to the entry they
//! reach, so a file written or synced through any spelling of that kind is
//! the one file. Two names the filesystem alone makes one (a case it
//! ignores, a bind mount) are tracked as two files.
//!
//! A new directory entry (a created file, the destination of a rename, a hard
//! link or a reflink) is durable only once its parent directory is synced, as
//! POSIX promises: `crash()` removes a file whose entry was never made durable,
//! even when its content was synced. The directory synced is matched by what
//! the backend reports as the same directory, so any spelling of it covers
//! its entries. Removed entries are not brought back, and directories
//! themselves are not rolled back. Opens, operations that make or remove
//! entries, and directory syncs are linearized: each
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
use std::path::Component;

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

    /// Forgets the name `path`, removed from the namespace. The file it named
    /// lives on under its other hard links, and what the file went through
    /// stays with them: a write through the removed name that was never
    /// synced is still unsynced under each of them.
    fn forget_name(&mut self, path: &Path) {
        self.durable.remove(path);
        self.pending_entries.remove(path);
        let touched = self.touched.remove(path);
        if let Some(group) = self.link_group.remove(path)
            && touched
        {
            let survivors: Vec<PathBuf> = self
                .link_group
                .iter()
                .filter(|&(_, &other)| other == group)
                .map(|(name, _)| name.clone())
                .collect();
            self.touched.extend(survivors);
        }
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

/// The components of `path`, each as a path of its own, last first.
fn components_reversed(path: &Path) -> Vec<PathBuf> {
    path.components()
        .rev()
        .map(|component| PathBuf::from(component.as_os_str()))
        .collect()
}

/// `path` with `.` and `..` folded away by spelling alone; `..` of a root is
/// the root, a `..` with nothing to climb out of stays, and nothing left is
/// `.`.
fn folded(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => match out.components().next_back() {
                Some(Component::Normal(_)) => {
                    out.pop();
                }
                Some(Component::RootDir | Component::Prefix(_)) => {}
                _ => out.push(component.as_os_str()),
            },
            other => out.push(other.as_os_str()),
        }
    }
    if out.as_os_str().is_empty() {
        out.push(".");
    }
    out
}

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
    /// directory entry, every open and every write that resolves the entry it
    /// lands on,
    /// holds it from its checks through the backend call to the state it
    /// records, and a directory sync holds it from the backend
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

    /// The entry a write through `path` lands on: `path` with every symlink
    /// it passes through resolved, the final one included, which is the
    /// entry an open that creates makes and the one whose bytes a sync makes
    /// durable.
    fn entry_of(&self, path: &Path) -> io::Result<PathBuf> {
        self.resolve(path, true)
    }

    /// The entry `path` names itself, as `rename`, `unlink` and `link` see
    /// it: its directory resolved, its final component kept even when it is
    /// a symlink.
    fn name_of(&self, path: &Path) -> io::Result<PathBuf> {
        self.resolve(path, false)
    }

    /// [`Self::name_of`] for an operation the backend has already done: the
    /// operation stands, so a probe that cannot answer leaves the name as it
    /// was given rather than failing it.
    fn name_after(&self, path: &Path) -> PathBuf {
        self.name_of(path).unwrap_or_else(|_| path.to_path_buf())
    }

    /// `path` spelled the one way every name of its entry is tracked under,
    /// so a file reached through a symlinked directory or through `..` is the
    /// file reached through its real directory.
    ///
    /// Symlinks are resolved component by component, as the kernel walks a
    /// path, through at most `MAX_SYMLINKS` links. Then `.` and `..` are
    /// folded away, which is what the walk would do over a prefix that holds
    /// no symlink, but only when the backend agrees that the folded directory
    /// is the one it reaches: a backend that keeps `..` as part of a name
    /// (`MemFs`) gets the name it was given.
    fn resolve(&self, path: &Path, follow_final: bool) -> io::Result<PathBuf> {
        let mut resolved = PathBuf::new();
        // Components still to walk, the next one last.
        let mut rest: Vec<PathBuf> = components_reversed(path);
        let mut followed = 0;
        while let Some(part) = rest.pop() {
            if !matches!(part.components().next(), Some(Component::Normal(_))) {
                // A root replaces what was walked, `.` adds nothing, and `..`
                // is folded below, once the prefix it climbs out of is known
                // to hold no symlink.
                if !matches!(part.components().next(), Some(Component::CurDir)) {
                    resolved.push(part);
                }
                continue;
            }
            let candidate = resolved.join(&part);
            if rest.is_empty() && !follow_final {
                resolved = candidate;
                break;
            }
            match self.inner.read_link(&candidate)? {
                Some(target) => {
                    // MAX_SYMLINKS links are followed; only a link past them
                    // is refused.
                    if followed == MAX_SYMLINKS {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "too many levels of symbolic links",
                        ));
                    }
                    followed += 1;
                    // A relative target continues from the directory holding
                    // the link; an absolute one starts again at its root.
                    rest.extend(components_reversed(&target));
                }
                None => resolved = candidate,
            }
        }
        if resolved.as_os_str().is_empty() {
            // `.` alone: the current directory, as the engine names it.
            resolved.push(".");
        }
        let folded = folded(&resolved);
        if folded == resolved {
            return Ok(resolved);
        }
        // The entry itself may not exist yet (an open that creates), its
        // directory does: that is what the backend is asked about.
        let (spelled, plain) = match (resolved.components().next_back(), folded.file_name()) {
            (Some(Component::Normal(_)), Some(_)) => (
                crate::file::entry_directory(&resolved).to_path_buf(),
                crate::file::entry_directory(&folded).to_path_buf(),
            ),
            _ => (resolved.clone(), folded.clone()),
        };
        Ok(
            if matches!(self.inner.same_file(&spelled, &plain), Ok(true)) {
                folded
            } else {
                resolved
            },
        )
    }

    /// Whether `path`'s directory lists an entry spelled exactly as its final
    /// component. A listing that cannot be read answers no.
    fn listed_as_spelled(&self, path: &Path) -> bool {
        let Some(name) = path.file_name() else {
            return false;
        };
        self.inner
            .read_dir(crate::file::entry_directory(path))
            .is_ok_and(|entries| {
                entries
                    .iter()
                    .any(|entry| std::ffi::OsStr::new(&entry.file_name) == name)
            })
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
        // Entries are tracked under their resolved names, so the directory
        // is matched resolved too: a sync through a symlink or `..` covers
        // the entries of the directory it reaches. The sync is done, so a
        // probe that cannot answer matches the directory as it was named.
        let directory = self
            .entry_of(directory)
            .unwrap_or_else(|_| directory.to_path_buf());
        // A spelling that resolving does not fold, a case the filesystem
        // ignores or a bind mount, still reaches the same directory: what
        // the backend says about identity decides, outside the state lock.
        // The namespace is held, so the pending set cannot change meanwhile.
        let pending = self.state.lock().pending_entries.clone();
        let covered: Vec<PathBuf> = pending
            .into_iter()
            .filter(|entry| {
                let parent = crate::file::entry_directory(entry);
                parent == directory || matches!(self.inner.same_file(parent, &directory), Ok(true))
            })
            .collect();
        let mut state = self.state.lock();
        for entry in &covered {
            state.pending_entries.remove(entry);
        }
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
        // An open holds the namespace from resolving its entry to the
        // registration: the backend follows the same symlinks to the same
        // file, and whether an open that may create made the entry is what
        // the open did. A handle opened only for reading resolves too, since
        // a sync through it makes the file it reached durable.
        let may_create = opts.create || opts.create_new;
        let namespace = self.namespace.lock();
        // An open that may only create (`O_EXCL`) does not follow a final
        // symlink and refuses any existing name: the backend answers whether
        // it can, and if it does the file is new, at the path itself, with no
        // prior image to capture.
        if opts.create_new {
            let inner = self.inner.open(path, opts)?;
            let entry = self.name_after(path);
            self.state.lock().touched.insert(entry.clone());
            self.new_entry(&entry);
            drop(namespace);
            return Ok(Box::new(CrashFile {
                inner,
                path: entry,
                fs: Arc::clone(&self.inner),
                state: Arc::clone(&self.state),
            }));
        }
        // A write follows symlinks: through a dangling one it creates the
        // target, so the target is the entry made and the file tracked.
        if !writable {
            // The open stands, so a probe that cannot answer leaves the
            // handle tracked under the name it was opened with.
            let inner = self.inner.open(path, opts)?;
            let entry = self.entry_of(path).unwrap_or_else(|_| path.to_path_buf());
            drop(namespace);
            return Ok(Box::new(CrashFile {
                inner,
                path: entry,
                fs: Arc::clone(&self.inner),
                state: Arc::clone(&self.state),
            }));
        }
        let entry = self.entry_of(path)?;
        let creates = may_create && !self.inner.exists(&entry)?;
        // Capture the pre-existing durable image BEFORE the open (which may
        // truncate); a brand-new file captures nothing, so a crash before its
        // first sync removes it.
        self.capture_first_touch(&entry)?;
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
        let name = self.name_after(path);
        self.state.lock().forget_name(&name);
        Ok(())
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        let _namespace = self.namespace.lock();
        self.inner.remove_dir_all(path)?;
        let name = self.name_after(path);
        let path = name.as_path();
        // Purge crash state for every tracked path under the removed directory,
        // so crash() neither resurrects nor panics recreating a file whose
        // parent is gone.
        let mut state = self.state.lock();
        // A linked name under the directory hands what its file went through
        // to the names that survive outside it.
        let linked: Vec<PathBuf> = state
            .link_group
            .keys()
            .filter(|k| k.starts_with(path))
            .cloned()
            .collect();
        for name in &linked {
            state.forget_name(name);
        }
        state.durable.retain(|k, _| !k.starts_with(path));
        state.touched.retain(|k| !k.starts_with(path));
        state.pending_entries.retain(|k| !k.starts_with(path));
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
        // POSIX rename(2): when both entries are links to one file (one path,
        // or two hard links of one inode) the call succeeds and changes
        // nothing, and the crash state stays as it was. Two names the backend
        // reports as one file may still be two entries (two symlinks to one
        // file) or one entry under two spellings (a filesystem that ignores
        // case), so whether the rename changed anything is told by the exact
        // spellings its directories list, before and after it. The namespace
        // is held, so nothing else changes them meanwhile, and the inner
        // backend is the disk this simulates: a fault layer composes above the
        // simulator, never below it.
        let spelled = || (self.listed_as_spelled(from), self.listed_as_spelled(to));
        let before = matches!(self.inner.same_file(from, to), Ok(true)).then(spelled);
        self.inner.rename(from, to)?;
        // Without a file identity to go on (a backend that has none, a
        // dangling symlink), the source still being there, the link itself
        // rather than what it points to, under its own spelling, is what
        // tells. A probe that fails is read as an ordinary rename.
        let unchanged = match before {
            Some(before) => before == spelled(),
            None => {
                (matches!(self.inner.read_link(from), Ok(Some(_)))
                    || matches!(self.inner.exists(from), Ok(true)))
                    && self.listed_as_spelled(from)
            }
        };
        if from == to || unchanged {
            return Ok(());
        }
        let (from, to) = (self.name_after(from), self.name_after(to));
        let (from, to) = (from.as_path(), to.as_path());
        let mut state = self.state.lock();
        // The source's state moves with its file, under the new name.
        let from_durable = state.durable.remove(from);
        let from_touched = state.touched.remove(from);
        let from_group = state.link_group.remove(from);
        state.pending_entries.remove(from);
        // The destination is replaced on disk, as a removal of that name: its
        // durable image and write-tracking go (so a replaced, previously
        // synced destination is not resurrected to its stale content on
        // crash), and what its file went through stays with the file's other
        // hard links.
        state.forget_name(to);
        // The rename is as durable as the source was.
        if let Some(bytes) = from_durable {
            state.durable.insert(to.to_path_buf(), bytes);
        }
        if from_touched {
            state.touched.insert(to.to_path_buf());
        }
        // The destination's entry is new until its directory is synced.
        state.pending_entries.insert(to.to_path_buf());
        if let Some(group) = from_group {
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
        // A symlink linked as itself (Linux `linkat(2)` without
        // `AT_SYMLINK_FOLLOW`) is a new entry with no bytes of its own: no
        // image is kept under it, and it is grouped with nothing. The link is
        // made, so a probe that cannot answer does not fail it either.
        let link = self.name_after(dst);
        if matches!(self.inner.read_link(dst), Ok(Some(_))) {
            self.new_entry(&link);
            return Ok(());
        }
        // A backend that followed a symlink source made `dst` a name of the
        // file the link chain ends at, so that file is the one copied and
        // the name `dst` is grouped with.
        let file = self.entry_of(src).unwrap_or_else(|_| self.name_after(src));
        self.track_copy(&file, &link)?;
        // On a backend where a hard link is a second name of one file, a sync
        // through either name makes the other's bytes durable too; on one
        // where it is a copy (`MemFs`), each name keeps its own image. A
        // probe that cannot answer leaves the names separate.
        if matches!(self.inner.same_file(&file, &link), Ok(true)) {
            let mut state = self.state.lock();
            state.link(&file, &link);
            // The image `dst` took is the one file's, `src`'s baseline
            // included: every name holds it, so a later first touch through
            // any of them, which finds the group already touched, still has
            // durable bytes to roll back to.
            if let Some(bytes) = state.durable.get(&link).cloned() {
                state.set_durable(&link, bytes);
            }
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
        // un-synced punch rolls back on crash. The namespace is held so the
        // backend reaches the file that was resolved.
        let _namespace = self.namespace.lock();
        self.capture_first_touch(&self.entry_of(path)?)?;
        self.inner.punch_hole(path, offset, len)
    }

    fn reflink_file(&self, src: &Path, dst: &Path) -> io::Result<()> {
        let _namespace = self.namespace.lock();
        self.inner.reflink_file(src, dst)?;
        let file = self.entry_of(src).unwrap_or_else(|_| self.name_after(src));
        self.track_copy(&file, &self.name_after(dst))?;
        Ok(())
    }

    fn truncate_file(&self, path: &Path) -> io::Result<()> {
        // Content-mutating: capture the pre-truncate image so an un-synced
        // reclaim rolls back on crash. The namespace is held so the backend
        // reaches the file that was resolved.
        let _namespace = self.namespace.lock();
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

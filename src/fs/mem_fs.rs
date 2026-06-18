// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

//! In-memory [`Fs`] implementation for testing and ephemeral trees.
//!
//! All file data lives in memory - there are no durability guarantees.
//! `sync_all`, `sync_data`, and `sync_directory` are deliberate no-ops.
//!
//! # Known limitations
//!
//! - **Compaction**: Some code paths in the compaction finalization still
//!   bypass the `Fs` trait. Write + flush + point-read works; compaction
//!   may fail with `ENOENT` on virtual paths.

use super::{Fs, FsCapabilities, FsDirEntry, FsFile, FsMetadata, FsOpenOptions};
use crate::io::{self, SeekFrom};
// Trait names referenced only by the no_std trait impls below (the std impls
// target `std::io::*` directly, so these would be unused under `std`).
#[cfg(not(feature = "std"))]
use crate::io::{Read, Seek, Write};
use crate::path::{Path, PathBuf};
#[cfg(not(feature = "std"))]
use alloc::borrow::ToOwned;
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};
// no_std-capable primitives so this reference backend compiles on
// `--no-default-features --features alloc` (it's the template a no_std
// consumer copies for a real backend, e.g. WASM/IndexedDB): `spin` locks
// (no poisoning, userspace), `hashbrown` maps. The locks see no real
// contention here — a single ephemeral in-memory tree — so spin is fine.
use hashbrown::{HashMap, HashSet};
use spin::{Mutex, RwLock};

// ---------------------------------------------------------------------------
// MemFs
// ---------------------------------------------------------------------------

/// In-memory [`Fs`] backend for testing and ephemeral in-memory trees.
///
/// Backed by a `HashMap<PathBuf, Arc<Mutex<Vec<u8>>>>` - no disk I/O is
/// performed. Clones share the same backing store, and individual file
/// contents are synchronized through a per-file [`Mutex`].
///
/// # Example
///
/// ```
/// use lsm_tree::fs::MemFs;
/// use std::sync::Arc;
///
/// let fs = MemFs::new();
/// let dyn_fs: Arc<dyn lsm_tree::fs::Fs> = Arc::new(fs);
/// ```
#[derive(Clone, Debug)]
pub struct MemFs {
    state: Arc<RwLock<State>>,
    /// Per-instance namespace ID used by [`Fs::backend_id`]. Cloned
    /// `MemFs` values share the same `state` Arc AND the same ID - they
    /// are the same backend by all observable behaviour. Independently
    /// constructed `MemFs::new()` values get DIFFERENT IDs because they
    /// have disjoint file trees.
    namespace_id: u64,
    /// Total simulated disk capacity in bytes. `u64::MAX` (default) means
    /// "unbounded": [`Fs::available_space`] then reports `u64::MAX` (no disk
    /// pressure). When set to a finite value via [`MemFs::with_capacity`] /
    /// [`MemFs::set_capacity`], `available_space` reports `capacity − bytes
    /// stored`, so the simulated disk fills as data is written and reaches zero
    /// when full — a real capped disk. Shared across clones (same backend).
    ///
    /// `portable_atomic::AtomicU64` (not `core`'s): native 64-bit atomics are
    /// absent on some `no_std` targets (e.g. thumbv7em).
    capacity: Arc<portable_atomic::AtomicU64>,
    /// Total bytes deallocated by [`Fs::punch_hole`] across all files, subtracted
    /// from [`Self::stored_bytes`] so the simulated disk reflects in-place extent
    /// reclaim (the real `fallocate(PUNCH_HOLE)` frees physical blocks while the
    /// file's logical length is unchanged). Shared across clones. The contract is
    /// non-overlapping punches (the tight-compaction reclaim loop punches
    /// strictly advancing input prefixes), so summing punched lengths cannot
    /// double-count. Reset to zero when a file is removed or truncated is not
    /// modelled — `MemFs` is a test backend, and the reclaim accounting only
    /// needs to be monotonic within one compaction's punch sequence.
    punched_bytes: Arc<portable_atomic::AtomicU64>,
}

#[derive(Debug, Default)]
struct State {
    files: HashMap<PathBuf, Arc<Mutex<Vec<u8>>>>,
    dirs: HashSet<PathBuf>,
}

impl MemFs {
    /// Creates a new, empty in-memory filesystem.
    #[must_use]
    pub fn new() -> Self {
        let mut state = State::default();
        // Seed the root directory so exists("/") and read_dir("/") work.
        state.dirs.insert(PathBuf::from("/"));
        Self {
            state: Arc::new(RwLock::new(state)),
            namespace_id: next_mem_fs_namespace_id(),
            capacity: Arc::new(portable_atomic::AtomicU64::new(u64::MAX)),
            punched_bytes: Arc::new(portable_atomic::AtomicU64::new(0)),
        }
    }

    /// Creates an empty in-memory filesystem with a fixed total capacity in
    /// bytes — a simulated capped disk. [`Fs::available_space`] reports
    /// `capacity − bytes stored`, so the disk fills as data is written and the
    /// storage-admission gate drives the tree read-only when it is full,
    /// without any manual free-space poking. `u64::MAX` means unbounded (same
    /// as [`MemFs::new`]).
    #[must_use]
    pub fn with_capacity(capacity_bytes: u64) -> Self {
        let fs = Self::new();
        fs.set_capacity(capacity_bytes);
        fs
    }

    /// Sets the simulated total disk capacity (shared across clones). See
    /// [`MemFs::with_capacity`]. `u64::MAX` restores unbounded behaviour.
    pub fn set_capacity(&self, capacity_bytes: u64) {
        self.capacity
            .store(capacity_bytes, portable_atomic::Ordering::Relaxed);
    }

    /// Total bytes currently stored across all files (the simulated disk
    /// usage). Sums every file's length under the state read lock.
    fn stored_bytes(&self) -> u64 {
        let state = self.state.read();
        let logical = state
            .files
            .values()
            .map(|data| data.lock().len() as u64)
            .fold(0u64, u64::saturating_add);
        // Subtract bytes reclaimed by punch_hole: their physical blocks are
        // freed even though the logical file length is unchanged.
        logical.saturating_sub(self.punched_bytes.load(portable_atomic::Ordering::Relaxed))
    }

    /// Total bytes reclaimed by [`Fs::punch_hole`] across all files on this
    /// simulated disk. Lets a test assert that an in-place extent reclaim (e.g.
    /// the tight-space compaction prefix punch) actually fired, distinct from a
    /// whole-file deletion.
    #[must_use]
    pub fn punched_bytes(&self) -> u64 {
        self.punched_bytes.load(portable_atomic::Ordering::Relaxed)
    }
}

/// Allocates the next per-instance `MemFs` namespace ID. Values are
/// process-unique (monotonic atomic counter) so two `MemFs::new()`
/// values never collide; cloned `MemFs` instances reuse the same ID
/// because `MemFs` derives `Clone`.
fn next_mem_fs_namespace_id() -> u64 {
    use core::sync::atomic::{AtomicU32, Ordering};
    // `AtomicU32`, not `AtomicU64`: 64-bit atomics are unavailable on some
    // no_std targets (e.g. thumbv7em). u32 IDs are ample for distinct
    // in-memory backends in one process; widened to u64 at the call site.
    // Start at 1 so a future `0` sentinel stays available if needed.
    static COUNTER: AtomicU32 = AtomicU32::new(1);
    u64::from(COUNTER.fetch_add(1, Ordering::Relaxed))
}

impl Default for MemFs {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// MemFile
// ---------------------------------------------------------------------------

/// An open file handle backed by an in-memory buffer.
struct MemFile {
    data: Arc<Mutex<Vec<u8>>>,
    cursor: u64,
    readable: bool,
    writable: bool,
    is_append: bool,
}

/// Copies bytes from `data[pos..]` into `buf`, returning byte count.
fn copy_from_data(buf: &mut [u8], data: &[u8], pos: usize) -> usize {
    let available = data.get(pos..).unwrap_or_default();
    let n = buf.len().min(available.len());
    if let (Some(dst), Some(src)) = (buf.get_mut(..n), available.get(..n)) {
        dst.copy_from_slice(src);
    }
    n
}

// Bodies live on inherent `*_impl` methods returning `crate::io::Result`; the
// trait impls are dual-gated thin wrappers. Under `std`, `crate::io::{Read,
// Write,Seek}` are method-less supertrait aliases (blanket-impl'd for
// `std::io::*`), so the real impl must target `std::io::*` there and bridge the
// error back via `Into`; under `no_std` it targets the native `crate::io::*`.
impl MemFile {
    fn read_impl(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if !self.readable {
            return Err(io::Error::other("file not opened for reading"));
        }
        let data = lock(&self.data)?;
        let pos = usize::try_from(self.cursor).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "cursor exceeds addressable memory",
            )
        })?;
        let n = copy_from_data(buf, &data, pos);
        drop(data);
        self.cursor += n as u64;
        Ok(n)
    }

    fn write_impl(&mut self, buf: &[u8]) -> io::Result<usize> {
        if !self.writable {
            return Err(io::Error::other("file not opened for writing"));
        }
        if buf.is_empty() {
            return Ok(0);
        }
        let mut data = lock(&self.data)?;

        let pos = if self.is_append {
            data.len()
        } else {
            usize::try_from(self.cursor).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "write position exceeds addressable memory",
                )
            })?
        };

        let end = pos.checked_add(buf.len()).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "write position overflow")
        })?;
        if end > data.len() {
            data.resize(end, 0);
        }
        if let Some(dst) = data.get_mut(pos..end) {
            dst.copy_from_slice(buf);
        }
        drop(data);
        self.cursor = end as u64;
        Ok(buf.len())
    }

    fn seek_impl(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos: u64 = match pos {
            SeekFrom::Start(n) => n,
            SeekFrom::End(n) => {
                let len = {
                    let data = lock(&self.data)?;
                    u64::try_from(data.len()).map_err(|_| {
                        io::Error::other("in-memory file length does not fit in u64")
                    })?
                };
                let result = i128::from(len) + i128::from(n);
                if result < 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "seek to negative position",
                    ));
                }
                u64::try_from(result).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "seek position overflow")
                })?
            }
            SeekFrom::Current(n) => {
                let result = i128::from(self.cursor) + i128::from(n);
                if result < 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "seek to negative position",
                    ));
                }
                u64::try_from(result).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "seek position overflow")
                })?
            }
        };

        self.cursor = new_pos;
        Ok(self.cursor)
    }
}

#[cfg(feature = "std")]
impl std::io::Read for MemFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.read_impl(buf).map_err(Into::into)
    }
}
#[cfg(not(feature = "std"))]
impl Read for MemFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.read_impl(buf)
    }
}

#[cfg(feature = "std")]
impl std::io::Write for MemFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.write_impl(buf).map_err(Into::into)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
#[cfg(not(feature = "std"))]
impl Write for MemFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_impl(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(feature = "std")]
impl std::io::Seek for MemFile {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.seek_impl(pos.into()).map_err(Into::into)
    }
}
#[cfg(not(feature = "std"))]
impl Seek for MemFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.seek_impl(pos)
    }
}

impl FsFile for MemFile {
    fn sync_all(&self) -> io::Result<()> {
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        Ok(())
    }

    fn metadata(&self) -> io::Result<FsMetadata> {
        let data = lock(&self.data)?;
        Ok(FsMetadata {
            len: data.len() as u64,
            is_dir: false,
            is_file: true,
        })
    }

    fn set_len(&self, size: u64) -> io::Result<()> {
        if !self.writable {
            return Err(io::Error::other("set_len requires write access"));
        }
        let new_len = usize::try_from(size).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "set_len size exceeds usize::MAX",
            )
        })?;
        lock(&self.data)?.resize(new_len, 0);
        Ok(())
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        if !self.readable {
            return Err(io::Error::other("read_at requires read access"));
        }
        let offset = usize::try_from(offset).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "read_at offset exceeds usize::MAX",
            )
        })?;
        let data = lock(&self.data)?;
        Ok(copy_from_data(buf, &data, offset))
    }

    /// No-op: in-memory files are not shared across processes. `MemFs` is a
    /// test/ephemeral backend - cross-process exclusivity is not meaningful.
    fn lock_exclusive(&self) -> io::Result<()> {
        Ok(())
    }

    fn try_lock_exclusive(&self) -> io::Result<bool> {
        // `MemFs` is a single-process in-memory backend: there is no other
        // process to contend with, so the directory lock is vacuously held.
        // Opt in explicitly (the trait default fails closed for backends that
        // have not implemented non-blocking locking).
        Ok(true)
    }
}

/// Rejects empty paths before they can create entries in the `/`-rooted namespace.
fn ensure_non_empty_path(path: &Path) -> io::Result<()> {
    if path.as_os_str().is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "empty path"));
    }
    Ok(())
}

/// Validates that the parent directory of `path` exists and is a directory.
///
/// Returns `Ok(())` when the parent is root, empty, or an existing directory.
/// Returns `Err(Other)` when the parent is a file, or `Err(NotFound)` when
/// it does not exist at all.
fn ensure_parent_dir(path: &Path, state: &State) -> io::Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
        && parent != Path::new("/")
        && !state.dirs.contains(parent)
    {
        if state.files.contains_key(parent) {
            return Err(io::Error::other(format!(
                "parent is not a directory: {}",
                parent.display()
            )));
        }
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("parent directory does not exist: {}", parent.display()),
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Fs for MemFs
// ---------------------------------------------------------------------------

impl Fs for MemFs {
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
        ensure_non_empty_path(path)?;
        let mut state = write_state(&self.state)?;
        let path = path.to_path_buf();
        let wants_write = opts.write || opts.append;

        // Validate flag combinations first (path-independent), before any
        // filesystem lookups. This ensures consistent InvalidInput errors
        // regardless of whether the parent directory exists.
        if !opts.read && !wants_write {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "open requires at least read, write, or append access",
            ));
        }
        if opts.truncate && opts.append {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "truncate and append cannot be used together",
            ));
        }
        if opts.truncate && !opts.write {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "truncate requires write access",
            ));
        }
        if (opts.create || opts.create_new) && !wants_write {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "create/create_new requires write or append access",
            ));
        }

        ensure_parent_dir(&path, &state)?;

        let exists = state.files.contains_key(&path);
        let is_dir = state.dirs.contains(&path);

        // Opening a directory path without create flags is an error (mirrors EISDIR).
        if is_dir && !opts.create && !opts.create_new {
            return Err(io::Error::other(format!(
                "path is a directory: {}",
                path.display()
            )));
        }

        // Reject creating a file at a path that is already a directory.
        if is_dir && (opts.create || opts.create_new) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("path is a directory: {}", path.display()),
            ));
        }

        if opts.create_new {
            if exists {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("file already exists: {}", path.display()),
                ));
            }
            let data = Arc::new(Mutex::new(Vec::new()));
            state.files.insert(path, Arc::clone(&data));
            return Ok(Box::new(MemFile {
                data,
                cursor: 0,
                readable: opts.read,
                writable: opts.write || opts.append,
                is_append: opts.append,
            }));
        }

        if exists {
            let data = state
                .files
                .get(&path)
                .map(Arc::clone)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "concurrent removal"))?;

            if opts.truncate {
                lock(&data)?.clear();
            }

            // Cursor starts at 0 even in append mode - append only affects
            // where writes land (Write::write checks is_append), not the
            // read cursor. This matches std::fs::File behaviour.
            let cursor = 0;

            Ok(Box::new(MemFile {
                data,
                cursor,
                readable: opts.read,
                writable: opts.write || opts.append,
                is_append: opts.append,
            }))
        } else if opts.create {
            let data = Arc::new(Mutex::new(Vec::new()));
            state.files.insert(path, Arc::clone(&data));
            Ok(Box::new(MemFile {
                data,
                cursor: 0,
                readable: opts.read,
                writable: opts.write || opts.append,
                is_append: opts.append,
            }))
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("file not found: {}", path.display()),
            ))
        }
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        ensure_non_empty_path(path)?;
        let mut state = write_state(&self.state)?;

        // Collect all components first, then validate, then insert.
        // This avoids partial insertion if an ancestor is a regular file.
        let mut to_create = Vec::new();
        let mut current = path.to_path_buf();
        loop {
            if state.files.contains_key(&current) {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("path conflicts with existing file: {}", current.display()),
                ));
            }
            to_create.push(current.clone());
            if !current.pop() || current.as_os_str().is_empty() {
                break;
            }
        }

        for dir in to_create {
            state.dirs.insert(dir);
        }
        Ok(())
    }

    fn create_dir(&self, path: &Path) -> io::Result<()> {
        ensure_non_empty_path(path)?;
        let mut state = write_state(&self.state)?;

        // Atomic single-leaf create: reject if anything (file OR dir)
        // already occupies the path. Mirrors POSIX `mkdir(2)` semantics.
        if state.dirs.contains(path) || state.files.contains_key(path) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("path already exists: {}", path.display()),
            ));
        }

        // Parent must exist AND be a directory. Delegating to
        // `ensure_parent_dir` gives the caller a `NotFound` vs
        // `parent-is-a-file` diagnostic (matching POSIX `ENOTDIR`),
        // instead of a single ambiguous `NotFound` for both cases.
        ensure_parent_dir(path, &state)?;

        state.dirs.insert(path.to_path_buf());
        Ok(())
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<FsDirEntry>> {
        let state = read_state(&self.state)?;

        if !state.dirs.contains(path) {
            // Distinguish "path is a file" from "path does not exist".
            if state.files.contains_key(path) {
                return Err(io::Error::other(format!(
                    "not a directory: {}",
                    path.display()
                )));
            }
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("directory not found: {}", path.display()),
            ));
        }

        let mut entries = Vec::new();

        for file_path in state.files.keys() {
            if file_path.parent() == Some(path)
                && let Some(name) = file_path.file_name()
            {
                // Match StdFs contract: reject non-UTF-8 names with InvalidData.
                #[cfg(feature = "std")]
                let file_name = name.to_str().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "non-UTF-8 filename in directory {}: {}",
                            path.display(),
                            name.display()
                        ),
                    )
                })?;
                // no_std: keys are UTF-8 `&str` by construction.
                #[cfg(not(feature = "std"))]
                let file_name = name;
                entries.push(FsDirEntry {
                    path: file_path.clone(),
                    file_name: file_name.to_owned(),
                    is_dir: false,
                });
            }
        }

        for dir_path in &state.dirs {
            if dir_path.parent() == Some(path)
                && dir_path != path
                && let Some(name) = dir_path.file_name()
            {
                #[cfg(feature = "std")]
                let file_name = name.to_str().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "non-UTF-8 filename in directory {}: {}",
                            path.display(),
                            name.display()
                        ),
                    )
                })?;
                // no_std: keys are UTF-8 `&str` by construction.
                #[cfg(not(feature = "std"))]
                let file_name = name;
                entries.push(FsDirEntry {
                    path: dir_path.clone(),
                    file_name: file_name.to_owned(),
                    is_dir: true,
                });
            }
        }

        Ok(entries)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        let mut state = write_state(&self.state)?;
        if state.dirs.contains(path) {
            return Err(io::Error::other(format!(
                "cannot remove_file on directory: {}",
                path.display()
            )));
        }
        if state.files.remove(path).is_none() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("file not found: {}", path.display()),
            ));
        }
        Ok(())
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        let mut state = write_state(&self.state)?;

        // Reject files - std::fs::remove_dir_all errors on non-directories.
        if state.files.contains_key(path) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("path is not a directory: {}", path.display()),
            ));
        }

        if !state.dirs.contains(path) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("path not found: {}", path.display()),
            ));
        }

        state.files.retain(|p, _| !p.starts_with(path));
        state.dirs.retain(|p| !p.starts_with(path));

        // Re-seed root so exists("/") and read_dir("/") remain valid.
        state.dirs.insert(PathBuf::from("/"));
        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        ensure_non_empty_path(from)?;
        ensure_non_empty_path(to)?;
        let mut state = write_state(&self.state)?;

        ensure_parent_dir(to, &state)?;

        // Reject renaming onto an existing directory. Otherwise `to` would end
        // up present in both `files` and `dirs`, corrupting MemFs state.
        if state.dirs.contains(to) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("destination is a directory: {}", to.display()),
            ));
        }

        // Directory renames are not implemented in MemFs because they require
        // updating descendant paths in both `dirs` and `files`.
        if state.dirs.contains(from) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("path is a directory: {}", from.display()),
            ));
        }

        if let Some(data) = state.files.remove(from) {
            state.files.insert(to.to_path_buf(), data);
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("file not found: {}", from.display()),
            ))
        }
    }

    fn metadata(&self, path: &Path) -> io::Result<FsMetadata> {
        let state = read_state(&self.state)?;

        if let Some(data) = state.files.get(path) {
            let d = lock(data)?;
            Ok(FsMetadata {
                len: d.len() as u64,
                is_dir: false,
                is_file: true,
            })
        } else if state.dirs.contains(path) {
            Ok(FsMetadata {
                len: 0,
                is_dir: true,
                is_file: false,
            })
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("path not found: {}", path.display()),
            ))
        }
    }

    fn available_space(&self, _path: &Path) -> io::Result<u64> {
        let capacity = self.capacity.load(portable_atomic::Ordering::Relaxed);
        // Unbounded → no disk pressure. Otherwise the simulated free space is
        // capacity minus what is currently stored (saturating: an over-capacity
        // state reports zero free, never wraps).
        if capacity == u64::MAX {
            Ok(u64::MAX)
        } else {
            Ok(capacity.saturating_sub(self.stored_bytes()))
        }
    }

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        // Durability is a no-op, but validate the path is an existing directory.
        let state = read_state(&self.state)?;
        if !state.dirs.contains(path) {
            if state.files.contains_key(path) {
                return Err(io::Error::other(format!(
                    "sync_directory: not a directory: {}",
                    path.display()
                )));
            }
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("sync_directory: path not found: {}", path.display()),
            ));
        }
        Ok(())
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        let state = read_state(&self.state)?;
        Ok(state.files.contains_key(path) || state.dirs.contains(path))
    }

    fn hard_link(&self, src: &Path, dst: &Path) -> io::Result<()> {
        ensure_non_empty_path(src)?;
        ensure_non_empty_path(dst)?;
        let mut state = write_state(&self.state)?;

        ensure_parent_dir(dst, &state)?;

        if state.dirs.contains(dst) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("destination is a directory: {}", dst.display()),
            ));
        }
        if state.files.contains_key(dst) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("destination already exists: {}", dst.display()),
            ));
        }

        // MemFs has no inode concept - produce an independent copy so the
        // destination has the same byte contents but its own backing buffer.
        // This matches the documented [`Fs::hard_link`] semantics for
        // in-memory backends.
        let bytes = {
            let src_data = state.files.get(src).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("source file not found: {}", src.display()),
                )
            })?;
            let guard = lock(src_data)?;
            guard.clone()
        };

        state
            .files
            .insert(dst.to_path_buf(), Arc::new(Mutex::new(bytes)));
        Ok(())
    }

    fn backend_id(&self) -> Option<u64> {
        Some(self.namespace_id)
    }

    fn volume_id(&self, _path: &Path) -> Option<u64> {
        // One `MemFs` instance is one simulated disk with a single capacity /
        // free-space pool (shared across clones via the same `state` Arc), so the
        // per-instance namespace ID also identifies the volume. Independently
        // constructed instances are independent volumes.
        Some(self.namespace_id)
    }

    /// In-memory backend: no filesystem-level guarantees on any path.
    /// Explicitly returns the all-`false` default so the "no integrity / no
    /// `CoW` / no reflink" stance is intentional rather than inherited by
    /// accident. Only `punch_hole` is set: [`Self::punch_hole`] simulates
    /// in-place extent reclaim, so tight-space compaction (and its tests) can
    /// run against this backend.
    fn capabilities(&self, _path: &Path) -> FsCapabilities {
        FsCapabilities {
            punch_hole: true,
            ..FsCapabilities::default()
        }
    }

    /// Simulates `fallocate(PUNCH_HOLE)`: zeroes `[offset, offset+len)` in the
    /// file (so the hole reads back as zeros) and records the reclaimed bytes so
    /// [`Fs::available_space`] reflects the freed space, while the file's logical
    /// length stays unchanged. The range is clamped to the current file length;
    /// a punch wholly past EOF is a no-op.
    fn punch_hole(&self, path: &Path, offset: u64, len: u64) -> io::Result<()> {
        let state = self.state.read();
        let data = state
            .files
            .get(path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "punch_hole: file not found"))?;
        let mut buf = data.lock();
        let file_len = buf.len() as u64;
        // Clamp to the file: a hole cannot extend the logical length.
        let start = offset.min(file_len);
        let end = offset.saturating_add(len).min(file_len);
        if start >= end {
            return Ok(());
        }
        #[expect(
            clippy::cast_possible_truncation,
            reason = "start/end are clamped to buf.len() (a usize), so they fit usize"
        )]
        let (s, e) = (start as usize, end as usize);
        if let Some(slice) = buf.get_mut(s..e) {
            slice.fill(0);
        }
        self.punched_bytes
            .fetch_add(end - start, portable_atomic::Ordering::Relaxed);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Lock helpers - convert PoisonError to io::Error
// ---------------------------------------------------------------------------

// `spin` locks cannot be poisoned (no unwind-during-hold concept), so these
// always succeed; the `io::Result` return is kept so the `?`-using call sites
// stay unchanged.
// Kept returning `io::Result` (always `Ok`) so the `?`-using call sites are
// untouched — spin locks never poison, but a future fallible lock layer would
// slot in here without churning every caller.
#[expect(
    clippy::unnecessary_wraps,
    reason = "Result kept for ?-compatible call sites and future fallible-lock parity"
)]
fn lock<T>(m: &Mutex<T>) -> io::Result<impl core::ops::DerefMut<Target = T> + '_> {
    Ok(m.lock())
}

#[expect(
    clippy::unnecessary_wraps,
    reason = "Result kept for ?-compatible call sites and future fallible-lock parity"
)]
fn read_state(rw: &RwLock<State>) -> io::Result<impl core::ops::Deref<Target = State> + '_> {
    Ok(rw.read())
}

#[expect(
    clippy::unnecessary_wraps,
    reason = "Result kept for ?-compatible call sites and future fallible-lock parity"
)]
fn write_state(rw: &RwLock<State>) -> io::Result<impl core::ops::DerefMut<Target = State> + '_> {
    Ok(rw.write())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::unnecessary_wraps,
    reason = "test code"
)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::sync::Arc;
    use test_log::test;

    #[test]
    fn create_read_write() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/data"))?;

        let path = Path::new("/data/test.txt");
        let opts = FsOpenOptions::new().write(true).create(true);
        let mut file = fs.open(path, &opts)?;
        file.write_all(b"hello world")?;
        drop(file);

        let opts = FsOpenOptions::new().read(true);
        let mut file = fs.open(path, &opts)?;
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        assert_eq!(buf, "hello world");

        Ok(())
    }

    #[test]
    fn punch_hole_zeroes_range_keeps_length_and_reclaims_space() -> io::Result<()> {
        let fs = MemFs::with_capacity(1000);
        let path = Path::new("/f");
        let mut file = fs.open(path, &FsOpenOptions::new().write(true).create(true))?;
        file.write_all(&[0xAB; 600])?;
        drop(file);

        assert!(
            fs.capabilities(path).punch_hole,
            "MemFs advertises punch-hole"
        );
        assert_eq!(fs.available_space(path)?, 400, "1000 capacity − 600 stored");

        // Punch [100, 300): 200 bytes freed.
        fs.punch_hole(path, 100, 200)?;

        let mut buf = Vec::new();
        fs.open(path, &FsOpenOptions::new().read(true))?
            .read_to_end(&mut buf)?;
        assert_eq!(buf.len(), 600, "logical length unchanged by the hole");
        assert!(
            buf.iter().take(100).all(|&b| b == 0xAB),
            "data before the hole is intact"
        );
        assert!(
            buf.iter().skip(100).take(200).all(|&b| b == 0),
            "the hole reads back as zeros"
        );
        assert!(
            buf.iter().skip(300).all(|&b| b == 0xAB),
            "data after the hole is intact"
        );
        assert_eq!(
            fs.available_space(path)?,
            600,
            "1000 capacity − (600 − 200 punched) stored"
        );
        Ok(())
    }

    #[test]
    fn punch_hole_clamps_past_eof_to_a_noop() -> io::Result<()> {
        let fs = MemFs::with_capacity(1000);
        let path = Path::new("/f");
        let mut file = fs.open(path, &FsOpenOptions::new().write(true).create(true))?;
        file.write_all(&[0xCD; 100])?;
        drop(file);

        // Wholly past EOF → nothing freed.
        fs.punch_hole(path, 200, 50)?;
        assert_eq!(fs.available_space(path)?, 900, "no reclaim past EOF");
        // Straddling EOF → only the in-file portion is freed.
        fs.punch_hole(path, 80, 100)?;
        assert_eq!(
            fs.available_space(path)?,
            920,
            "only [80,100) (20 bytes) freed"
        );
        Ok(())
    }

    #[test]
    fn punch_hole_on_missing_file_is_not_found() {
        let fs = MemFs::new();
        let err = fs.punch_hole(Path::new("/nope"), 0, 10).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn directory_operations() -> io::Result<()> {
        let fs = MemFs::new();
        let nested = PathBuf::from("/a/b/c");
        fs.create_dir_all(&nested)?;
        assert!(fs.exists(&nested)?);
        assert!(fs.exists(Path::new("/a/b"))?);

        let file_path = nested.join("data.bin");
        let opts = FsOpenOptions::new().write(true).create_new(true);
        let mut file = fs.open(&file_path, &opts)?;
        file.write_all(b"data")?;
        drop(file);

        let entries = fs.read_dir(&nested)?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].file_name, "data.bin");
        assert!(!entries[0].is_dir);

        let meta = fs.metadata(&file_path)?;
        assert!(meta.is_file);
        assert!(!meta.is_dir);
        assert_eq!(meta.len, 4);

        fs.remove_file(&file_path)?;
        assert!(!fs.exists(&file_path)?);

        fs.remove_dir_all(Path::new("/a"))?;
        assert!(!fs.exists(Path::new("/a"))?);
        assert!(!fs.exists(&nested)?);

        Ok(())
    }

    #[test]
    fn rename_file() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir"))?;

        let src = Path::new("/dir/src.txt");
        let dst = Path::new("/dir/dst.txt");

        let opts = FsOpenOptions::new().write(true).create(true);
        let mut file = fs.open(src, &opts)?;
        file.write_all(b"content")?;
        drop(file);

        fs.rename(src, dst)?;
        assert!(!fs.exists(src)?);
        assert!(fs.exists(dst)?);

        Ok(())
    }

    #[test]
    fn rename_atomically_replaces_existing_destination() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir"))?;

        let src = Path::new("/dir/new.txt");
        let dst = Path::new("/dir/existing.txt");

        // Create destination with old content
        let opts = FsOpenOptions::new().write(true).create(true);
        let mut file = fs.open(dst, &opts)?;
        file.write_all(b"old")?;
        drop(file);

        // Create source with new content
        let mut file = fs.open(src, &opts)?;
        file.write_all(b"new")?;
        drop(file);

        // Rename should atomically replace destination
        fs.rename(src, dst)?;
        assert!(!fs.exists(src)?);

        let mut file = fs.open(dst, &FsOpenOptions::new().read(true))?;
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        assert_eq!(buf, "new");

        Ok(())
    }

    #[test]
    fn sync_directory_is_noop() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir"))?;
        fs.sync_directory(Path::new("/dir"))?;
        Ok(())
    }

    #[test]
    fn file_metadata_and_set_len() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir"))?;

        let path = Path::new("/dir/meta.bin");
        let opts = FsOpenOptions::new().write(true).create(true).read(true);
        let mut file = fs.open(path, &opts)?;
        file.write_all(b"12345")?;

        let meta = file.metadata()?;
        assert!(meta.is_file);
        assert_eq!(meta.len, 5);

        file.set_len(3)?;
        let meta = file.metadata()?;
        assert_eq!(meta.len, 3);

        Ok(())
    }

    #[test]
    fn read_at_positional() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir"))?;

        let path = Path::new("/dir/pread.bin");
        let opts = FsOpenOptions::new().write(true).create(true).read(true);
        let mut file = fs.open(path, &opts)?;
        file.write_all(b"hello world")?;

        let mut buf = [0u8; 5];
        let n = file.read_at(&mut buf, 6)?;
        assert_eq!(n, 5);
        assert_eq!(&buf, b"world");

        let n = file.read_at(&mut buf, 0)?;
        assert_eq!(n, 5);
        assert_eq!(&buf, b"hello");

        // Past EOF
        let n = file.read_at(&mut buf, 100)?;
        assert_eq!(n, 0);

        Ok(())
    }

    #[test]
    fn lock_exclusive_is_noop() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir"))?;

        let path = Path::new("/dir/lock");
        let opts = FsOpenOptions::new().write(true).create(true);
        let file = fs.open(path, &opts)?;
        file.lock_exclusive()?;
        Ok(())
    }

    #[test]
    fn open_create_new_fails_on_existing() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir"))?;

        let path = Path::new("/dir/file");
        let opts = FsOpenOptions::new().write(true).create_new(true);
        fs.open(path, &opts)?;

        let err = fs.open(path, &opts).err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        Ok(())
    }

    #[test]
    fn open_nonexistent_without_create_fails() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir"))?;

        let path = Path::new("/dir/missing");
        let opts = FsOpenOptions::new().read(true);
        let err = fs.open(path, &opts).err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        Ok(())
    }

    #[test]
    fn open_fails_when_parent_missing() -> io::Result<()> {
        let fs = MemFs::new();
        let path = Path::new("/no/such/dir/file");
        let opts = FsOpenOptions::new().write(true).create(true);
        let err = fs.open(path, &opts).err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        Ok(())
    }

    #[test]
    fn truncate_on_open() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir"))?;

        let path = Path::new("/dir/trunc.txt");
        let opts = FsOpenOptions::new().write(true).create(true);
        let mut file = fs.open(path, &opts)?;
        file.write_all(b"hello world")?;
        drop(file);

        let opts = FsOpenOptions::new().write(true).truncate(true);
        let mut file = fs.open(path, &opts)?;
        file.write_all(b"hi")?;
        drop(file);

        let meta = fs.metadata(path)?;
        assert_eq!(meta.len, 2);
        Ok(())
    }

    #[test]
    fn append_mode() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir"))?;

        let path = Path::new("/dir/append.txt");
        let opts = FsOpenOptions::new().write(true).create(true);
        let mut file = fs.open(path, &opts)?;
        file.write_all(b"hello")?;
        drop(file);

        let opts = FsOpenOptions::new().write(true).append(true);
        let mut file = fs.open(path, &opts)?;
        file.write_all(b" world")?;
        drop(file);

        let opts = FsOpenOptions::new().read(true);
        let mut file = fs.open(path, &opts)?;
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        assert_eq!(buf, "hello world");
        Ok(())
    }

    #[test]
    fn read_append_cursor_starts_at_zero() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir"))?;

        let path = Path::new("/dir/rw_append.txt");
        let opts = FsOpenOptions::new().write(true).create(true);
        let mut file = fs.open(path, &opts)?;
        file.write_all(b"existing")?;
        drop(file);

        // Open with read + append - cursor should start at 0 for reads,
        // but writes go to EOF.
        let opts = FsOpenOptions::new().read(true).append(true);
        let mut file = fs.open(path, &opts)?;

        // Read should return existing content from offset 0.
        let mut buf = [0u8; 8];
        let n = file.read(&mut buf)?;
        assert_eq!(n, 8);
        assert_eq!(&buf, b"existing");

        // Write appends to EOF.
        file.write_all(b"+new")?;
        drop(file);

        // Verify full content.
        let opts = FsOpenOptions::new().read(true);
        let mut file = fs.open(path, &opts)?;
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        assert_eq!(buf, "existing+new");

        Ok(())
    }

    #[test]
    fn seek_and_overwrite() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir"))?;

        let path = Path::new("/dir/seek.bin");
        let opts = FsOpenOptions::new().write(true).create(true).read(true);
        let mut file = fs.open(path, &opts)?;
        file.write_all(b"hello world")?;

        file.seek(std::io::SeekFrom::Start(6))?;
        file.write_all(b"rust!")?;

        file.seek(std::io::SeekFrom::Start(0))?;
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        assert_eq!(buf, "hello rust!");

        Ok(())
    }

    #[test]
    fn object_safety() -> io::Result<()> {
        let fs: Arc<dyn Fs> = Arc::new(MemFs::new());
        let bogus = Path::new("/nonexistent");
        assert!(!fs.exists(bogus)?);
        Ok(())
    }

    #[test]
    fn metadata_directory() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/mydir"))?;
        let meta = fs.metadata(Path::new("/mydir"))?;
        assert!(meta.is_dir);
        assert!(!meta.is_file);
        Ok(())
    }

    #[test]
    fn read_dir_with_subdirectory() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/root/subdir"))?;

        let file_path = Path::new("/root/file.txt");
        let opts = FsOpenOptions::new().write(true).create(true);
        fs.open(file_path, &opts)?;

        let mut entries = fs.read_dir(Path::new("/root"))?;
        entries.sort_by(|a, b| a.file_name.cmp(&b.file_name));
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].file_name, "file.txt");
        assert!(!entries[0].is_dir);
        assert_eq!(entries[1].file_name, "subdir");
        assert!(entries[1].is_dir);
        Ok(())
    }

    #[test]
    fn remove_file_nonexistent_fails() -> io::Result<()> {
        let fs = MemFs::new();
        let err = fs.remove_file(Path::new("/missing")).err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        Ok(())
    }

    #[test]
    fn rename_nonexistent_fails() -> io::Result<()> {
        let fs = MemFs::new();
        let err = fs
            .rename(Path::new("/missing"), Path::new("/dst"))
            .err()
            .unwrap();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        Ok(())
    }

    #[test]
    fn read_dir_nonexistent_fails() -> io::Result<()> {
        let fs = MemFs::new();
        let err = fs.read_dir(Path::new("/missing")).err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        Ok(())
    }

    #[test]
    fn metadata_nonexistent_fails() -> io::Result<()> {
        let fs = MemFs::new();
        let err = fs.metadata(Path::new("/missing")).err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        Ok(())
    }

    #[test]
    fn sync_data_is_noop() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir"))?;
        let path = Path::new("/dir/file");
        let opts = FsOpenOptions::new().write(true).create(true);
        let mut file = fs.open(path, &opts)?;
        file.write_all(b"data")?;
        file.sync_data()?;
        Ok(())
    }

    #[test]
    fn clones_share_state() -> io::Result<()> {
        let fs1 = MemFs::new();
        let fs2 = fs1.clone();

        fs1.create_dir_all(Path::new("/shared"))?;
        let path = Path::new("/shared/file.txt");
        let opts = FsOpenOptions::new().write(true).create(true);
        let mut file = fs1.open(path, &opts)?;
        file.write_all(b"shared data")?;
        drop(file);

        assert!(fs2.exists(path)?);
        let meta = fs2.metadata(path)?;
        assert_eq!(meta.len, 11);
        Ok(())
    }

    // ── Wrong-type error-path tests ─────────────────────────────────────

    #[test]
    fn read_dir_on_file_returns_not_a_directory() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir"))?;
        let opts = FsOpenOptions::new().write(true).create(true);
        fs.open(Path::new("/dir/file"), &opts)?;

        let err = fs.read_dir(Path::new("/dir/file")).unwrap_err();
        // Must NOT be NotFound - the path exists but is a file.
        assert_ne!(err.kind(), io::ErrorKind::NotFound);
        Ok(())
    }

    #[test]
    fn remove_file_on_dir_returns_error() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/somedir"))?;

        let err = fs.remove_file(Path::new("/somedir")).unwrap_err();
        assert_ne!(err.kind(), io::ErrorKind::NotFound);
        Ok(())
    }

    #[test]
    fn sync_directory_on_file_returns_not_a_directory() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir"))?;
        let opts = FsOpenOptions::new().write(true).create(true);
        fs.open(Path::new("/dir/file"), &opts)?;

        let err = fs.sync_directory(Path::new("/dir/file")).unwrap_err();
        assert_ne!(err.kind(), io::ErrorKind::NotFound);
        Ok(())
    }

    #[test]
    fn open_with_parent_as_file_returns_error() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir"))?;
        let opts = FsOpenOptions::new().write(true).create(true);
        fs.open(Path::new("/dir/file"), &opts)?;

        // Try to create a file whose "parent" is actually a file.
        let err = fs
            .open(Path::new("/dir/file/child"), &opts)
            .map(|_| ())
            .unwrap_err();
        assert_ne!(err.kind(), io::ErrorKind::NotFound);
        Ok(())
    }

    #[test]
    fn rename_directory_returns_invalid_input() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/src_dir"))?;
        fs.create_dir_all(Path::new("/dst_parent"))?;

        let err = fs
            .rename(Path::new("/src_dir"), Path::new("/dst_parent/moved"))
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        Ok(())
    }

    #[test]
    fn rename_onto_directory_returns_invalid_input() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir"))?;
        let opts = FsOpenOptions::new().write(true).create(true);
        fs.open(Path::new("/dir/file"), &opts)?;
        fs.create_dir_all(Path::new("/dir/dst_dir"))?;

        let err = fs
            .rename(Path::new("/dir/file"), Path::new("/dir/dst_dir"))
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        Ok(())
    }

    #[test]
    fn rename_with_file_as_dest_parent_returns_error() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir"))?;
        let opts = FsOpenOptions::new().write(true).create(true);
        fs.open(Path::new("/dir/src"), &opts)?;
        fs.open(Path::new("/dir/blocker"), &opts)?;

        // /dir/blocker is a file, not a directory - cannot be parent of dst.
        let err = fs
            .rename(Path::new("/dir/src"), Path::new("/dir/blocker/child"))
            .unwrap_err();
        assert_ne!(err.kind(), io::ErrorKind::NotFound);
        Ok(())
    }

    #[test]
    fn remove_dir_all_on_file_returns_invalid_input() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir"))?;
        let opts = FsOpenOptions::new().write(true).create(true);
        fs.open(Path::new("/dir/file"), &opts)?;

        let err = fs.remove_dir_all(Path::new("/dir/file")).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        Ok(())
    }

    #[test]
    fn set_len_without_write_access_returns_error() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir"))?;

        let path = Path::new("/dir/file.bin");
        let mut file = fs.open(path, &FsOpenOptions::new().write(true).create(true))?;
        file.write_all(b"data")?;
        drop(file);

        let file = fs.open(path, &FsOpenOptions::new().read(true))?;
        assert!(file.set_len(1).is_err());
        Ok(())
    }

    #[test]
    fn read_at_without_read_access_returns_error() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir"))?;

        let path = Path::new("/dir/file.bin");
        let mut file = fs.open(path, &FsOpenOptions::new().write(true).create(true))?;
        file.write_all(b"data")?;

        let mut buf = [0u8; 1];
        assert!(file.read_at(&mut buf, 0).is_err());
        Ok(())
    }

    #[test]
    fn open_empty_path_returns_invalid_input() -> io::Result<()> {
        let fs = MemFs::new();
        let err = fs
            .open(Path::new(""), &FsOpenOptions::new().read(true))
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        Ok(())
    }

    #[test]
    fn create_dir_all_empty_path_returns_invalid_input() -> io::Result<()> {
        let fs = MemFs::new();
        let err = fs.create_dir_all(Path::new("")).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        Ok(())
    }

    #[test]
    fn rename_empty_path_returns_invalid_input() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir"))?;
        let opts = FsOpenOptions::new().write(true).create(true);
        fs.open(Path::new("/dir/file"), &opts)?;

        let err = fs.rename(Path::new(""), Path::new("/dir/dst")).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        let err = fs
            .rename(Path::new("/dir/file"), Path::new(""))
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        Ok(())
    }

    #[test]
    fn hard_link_creates_independent_copy() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir"))?;

        let src = Path::new("/dir/src.bin");
        let dst = Path::new("/dir/dst.bin");
        let opts = FsOpenOptions::new().write(true).create(true);
        let mut file = fs.open(src, &opts)?;
        file.write_all(b"checkpoint")?;
        drop(file);

        fs.hard_link(src, dst)?;

        // Both exist and contain the same bytes.
        let opts = FsOpenOptions::new().read(true);
        let mut buf = String::new();
        fs.open(src, &opts)?.read_to_string(&mut buf)?;
        assert_eq!(buf, "checkpoint");
        let mut buf = String::new();
        fs.open(dst, &opts)?.read_to_string(&mut buf)?;
        assert_eq!(buf, "checkpoint");

        // Critical invariant: `MemFs::hard_link` returns an *independent*
        // copy (no `Arc<Mutex<Vec<u8>>>` aliasing). Mutate the source and
        // verify the destination is unaffected - if the test only relied
        // on `remove_file` it would pass even with an aliased buffer.
        let mut writer = fs.open(src, &FsOpenOptions::new().write(true).truncate(true))?;
        writer.write_all(b"mutated")?;
        drop(writer);

        let mut after = String::new();
        fs.open(dst, &FsOpenOptions::new().read(true))?
            .read_to_string(&mut after)?;
        assert_eq!(
            after, "checkpoint",
            "dst must not see writes to src - buffers must be independent",
        );

        // Removing the source leaves the destination intact.
        fs.remove_file(src)?;
        assert!(!fs.exists(src)?);
        assert!(fs.exists(dst)?);
        Ok(())
    }

    #[test]
    fn fs_capabilities_default_reports_no_guarantees() {
        // The conservative default is load-bearing: any backend that does not
        // override capabilities() must be treated as offering nothing, so an
        // unknown FS never skips a checksum or disables `CoW` by accident.
        let caps = FsCapabilities::default();
        assert!(!caps.per_block_integrity_on_read);
        assert!(!caps.background_scrub);
        assert!(!caps.copy_on_write);
        assert!(!caps.reflink);
        assert!(!caps.native_snapshot);
    }

    #[test]
    fn memfs_capabilities_advertise_only_punch_hole() {
        // RAM has no FS-level integrity / `CoW` / reflink, so those stay false;
        // only `punch_hole` is set, since `MemFs::punch_hole` simulates in-place
        // extent reclaim for tight-space compaction tests.
        assert_eq!(
            MemFs::new().capabilities(Path::new("/dir/sst.bin")),
            FsCapabilities {
                punch_hole: true,
                ..FsCapabilities::default()
            }
        );
    }

    #[test]
    fn try_disable_cow_without_cow_support_is_noop() {
        // MemFs reports copy_on_write=false, so the default no-op path applies:
        // the call succeeds and changes nothing.
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir")).unwrap();
        let path = Path::new("/dir/sst.bin");
        fs.open(path, &FsOpenOptions::new().write(true).create(true))
            .unwrap();
        assert!(
            fs.try_disable_cow(path).is_ok(),
            "no-op must succeed on a non-CoW backend"
        );
    }

    #[test]
    fn reflink_file_without_backend_support_copies_independently() -> io::Result<()> {
        // No backend reflink support → default streamed-copy fallback. The
        // clone must be byte-identical AND an independent file (writing the
        // source afterwards must not change the clone).
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir"))?;
        let src = Path::new("/dir/src.bin");
        let dst = Path::new("/dir/clone.bin");

        let mut f = fs.open(src, &FsOpenOptions::new().write(true).create(true))?;
        f.write_all(b"original-contents")?;
        drop(f);

        fs.reflink_file(src, dst)?;

        let mut buf = String::new();
        fs.open(dst, &FsOpenOptions::new().read(true))?
            .read_to_string(&mut buf)?;
        assert_eq!(buf, "original-contents");

        // Independence: mutate src, clone must be unaffected.
        let mut w = fs.open(src, &FsOpenOptions::new().write(true).truncate(true))?;
        w.write_all(b"changed")?;
        drop(w);

        let mut after = String::new();
        fs.open(dst, &FsOpenOptions::new().read(true))?
            .read_to_string(&mut after)?;
        assert_eq!(
            after, "original-contents",
            "reflink clone must be independent"
        );

        Ok(())
    }

    #[test]
    fn reflink_file_rejects_existing_destination() {
        // Default fallback opens dst with create_new, so an existing target is
        // an error (no silent overwrite of a checkpoint file).
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir")).unwrap();
        let src = Path::new("/dir/src.bin");
        let dst = Path::new("/dir/dst.bin");
        for p in [src, dst] {
            fs.open(p, &FsOpenOptions::new().write(true).create(true))
                .unwrap();
        }
        let err = fs.reflink_file(src, dst).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    }

    #[test]
    fn hard_link_rejects_existing_destination() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir"))?;

        let opts = FsOpenOptions::new().write(true).create(true);
        fs.open(Path::new("/dir/a"), &opts)?;
        fs.open(Path::new("/dir/b"), &opts)?;

        let err = fs
            .hard_link(Path::new("/dir/a"), Path::new("/dir/b"))
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        Ok(())
    }

    #[test]
    fn hard_link_rejects_missing_source() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir"))?;

        let err = fs
            .hard_link(Path::new("/dir/missing"), Path::new("/dir/dst"))
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        Ok(())
    }
}

// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! In-memory [`Fs`] implementation for testing and ephemeral trees.
//!
//! All file data lives in memory — there are no durability guarantees.
//! `sync_all`, `sync_data`, and `sync_directory` are deliberate no-ops.
//!
//! # Known limitations
//!
//! - **Tree reopen**: `Tree::open` checks for the `CURRENT` version file
//!   via `Path::try_exists()` (bypasses `Fs`). New trees work correctly
//!   (the check returns `false` → `create_new`), but reopening an
//!   in-memory tree after drop is not supported.
//! - **Version GC**: Old version file cleanup in `SuperVersions::gc` uses
//!   `std::fs` directly — stale version entries accumulate in memory
//!   until the `MemFs` is dropped. Acceptable for testing / ephemeral use.
//! - **Compaction**: Some code paths in the compaction finalization still
//!   bypass the `Fs` trait. Write + flush + point-read works; compaction
//!   may fail with `ENOENT` on virtual paths.

use super::{Fs, FsDirEntry, FsFile, FsMetadata, FsOpenOptions};
use std::collections::{HashMap, HashSet};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

// ---------------------------------------------------------------------------
// MemFs
// ---------------------------------------------------------------------------

/// In-memory [`Fs`] backend for testing and ephemeral in-memory trees.
///
/// Backed by a `HashMap<PathBuf, Arc<Mutex<Vec<u8>>>>` — no disk I/O is
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
        }
    }
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

#[expect(
    clippy::cast_possible_truncation,
    reason = "MemFs is a test/ephemeral backend — files never exceed usize::MAX"
)]
impl Read for MemFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if !self.readable {
            return Err(io::Error::other("file not opened for reading"));
        }
        let data = lock(&self.data)?;
        let pos = self.cursor as usize;
        let n = copy_from_data(buf, &data, pos);
        drop(data);
        self.cursor += n as u64;
        Ok(n)
    }
}

#[expect(
    clippy::cast_possible_truncation,
    reason = "MemFs is a test/ephemeral backend — files never exceed usize::MAX"
)]
impl Write for MemFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if !self.writable {
            return Err(io::Error::other("file not opened for writing"));
        }
        let mut data = lock(&self.data)?;

        let pos = if self.is_append {
            data.len()
        } else {
            self.cursor as usize
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

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Seek for MemFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
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

    #[expect(
        clippy::cast_possible_truncation,
        reason = "MemFs is a test/ephemeral backend — files never exceed usize::MAX"
    )]
    fn set_len(&self, size: u64) -> io::Result<()> {
        if !self.writable {
            return Err(io::Error::other("set_len requires write access"));
        }
        lock(&self.data)?.resize(size as usize, 0);
        Ok(())
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "MemFs is a test/ephemeral backend — files never exceed usize::MAX"
    )]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        if !self.readable {
            return Err(io::Error::other("read_at requires read access"));
        }
        let data = lock(&self.data)?;
        Ok(copy_from_data(buf, &data, offset as usize))
    }

    /// No-op: in-memory files are not shared across processes. `MemFs` is a
    /// test/ephemeral backend — cross-process exclusivity is not meaningful.
    fn lock_exclusive(&self) -> io::Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Fs for MemFs
// ---------------------------------------------------------------------------

#[expect(
    clippy::significant_drop_tightening,
    reason = "RwLock guards are intentionally held for the duration of each method"
)]
impl Fs for MemFs {
    #[expect(
        clippy::too_many_lines,
        reason = "open() validates many flag combinations"
    )]
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
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
        if opts.truncate && !wants_write {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "truncate requires write or append access",
            ));
        }
        if (opts.create || opts.create_new) && !wants_write {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "create/create_new requires write or append access",
            ));
        }

        // Verify parent directory exists (mirrors std::fs behaviour).
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

            // Cursor starts at 0 even in append mode — append only affects
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

        // Reject files — std::fs::remove_dir_all errors on non-directories.
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
        let mut state = write_state(&self.state)?;

        // Validate destination parent exists (mirrors std::fs behaviour).
        if let Some(parent) = to.parent()
            && !parent.as_os_str().is_empty()
            && parent != Path::new("/")
            && !state.dirs.contains(parent)
        {
            if state.files.contains_key(parent) {
                return Err(io::Error::other(format!(
                    "destination parent is not a directory: {}",
                    parent.display()
                )));
            }
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("destination parent not found: {}", parent.display()),
            ));
        }

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

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        // Durability is a no-op, but validate the path is an existing directory.
        let state = read_state(&self.state)?;
        if !state.dirs.contains(path) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("sync_directory: not a directory: {}", path.display()),
            ));
        }
        Ok(())
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        let state = read_state(&self.state)?;
        Ok(state.files.contains_key(path) || state.dirs.contains(path))
    }
}

// ---------------------------------------------------------------------------
// Lock helpers — convert PoisonError to io::Error
// ---------------------------------------------------------------------------

fn lock<T>(m: &Mutex<T>) -> io::Result<std::sync::MutexGuard<'_, T>> {
    m.lock().map_err(|_| io::Error::other("mutex poisoned"))
}

fn read_state(rw: &RwLock<State>) -> io::Result<std::sync::RwLockReadGuard<'_, State>> {
    rw.read().map_err(|_| io::Error::other("rwlock poisoned"))
}

fn write_state(rw: &RwLock<State>) -> io::Result<std::sync::RwLockWriteGuard<'_, State>> {
    rw.write().map_err(|_| io::Error::other("rwlock poisoned"))
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
    fn seek_and_overwrite() -> io::Result<()> {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/dir"))?;

        let path = Path::new("/dir/seek.bin");
        let opts = FsOpenOptions::new().write(true).create(true).read(true);
        let mut file = fs.open(path, &opts)?;
        file.write_all(b"hello world")?;

        file.seek(SeekFrom::Start(6))?;
        file.write_all(b"rust!")?;

        file.seek(SeekFrom::Start(0))?;
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
}

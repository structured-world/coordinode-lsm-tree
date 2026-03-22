// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Fs, FsDirEntry, FsFile, FsMetadata, FsOpenOptions};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

/// Default [`Fs`] implementation backed by [`std::fs`].
///
/// This is a zero-sized type — when used as a monomorphized generic
/// parameter it adds no runtime overhead.
#[derive(Clone, Copy, Debug, Default)]
pub struct StdFs;

/// Iterator over directory entries returned by [`StdFs::read_dir`].
pub struct StdReadDir(std::fs::ReadDir);

impl Iterator for StdReadDir {
    type Item = io::Result<FsDirEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|res| {
            let entry = res?;
            let file_type = entry.file_type()?;
            let file_name = entry.file_name().to_string_lossy().into_owned();
            Ok(FsDirEntry {
                path: entry.path(),
                file_name,
                is_dir: file_type.is_dir(),
            })
        })
    }
}

// ---------------------------------------------------------------------------
// FsFile for std::fs::File
// ---------------------------------------------------------------------------

impl FsFile for File {
    fn sync_all(&self) -> io::Result<()> {
        Self::sync_all(self)
    }

    fn sync_data(&self) -> io::Result<()> {
        Self::sync_data(self)
    }

    fn metadata(&self) -> io::Result<FsMetadata> {
        let m = Self::metadata(self)?;
        Ok(FsMetadata {
            len: m.len(),
            is_dir: m.is_dir(),
            is_file: m.is_file(),
        })
    }

    fn set_len(&self, size: u64) -> io::Result<()> {
        Self::set_len(self, size)
    }

    fn lock_exclusive(&self) -> io::Result<()> {
        sys::lock_exclusive(self)
    }
}

// ---------------------------------------------------------------------------
// Fs for StdFs
// ---------------------------------------------------------------------------

impl Fs for StdFs {
    type File = File;
    type ReadDir = StdReadDir;

    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<File> {
        OpenOptions::new()
            .read(opts.read)
            .write(opts.write)
            .create(opts.create)
            .create_new(opts.create_new)
            .truncate(opts.truncate)
            .append(opts.append)
            .open(path)
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        std::fs::create_dir_all(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<StdReadDir> {
        std::fs::read_dir(path).map(StdReadDir)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_dir_all(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    fn metadata(&self, path: &Path) -> io::Result<FsMetadata> {
        let m = std::fs::metadata(path)?;
        Ok(FsMetadata {
            len: m.len(),
            is_dir: m.is_dir(),
            is_file: m.is_file(),
        })
    }

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        #[cfg(not(target_os = "windows"))]
        {
            let dir = File::open(path)?;
            dir.sync_all()
        }

        #[cfg(target_os = "windows")]
        {
            let _ = path;
            Ok(())
        }
    }

    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }
}

// ---------------------------------------------------------------------------
// Platform-specific file locking
// ---------------------------------------------------------------------------

#[cfg(unix)]
mod sys {
    use std::fs::File;
    use std::io;
    use std::os::unix::io::AsRawFd;

    // Declare flock directly to avoid requiring libc as a direct dependency.
    const LOCK_EX: i32 = 2;

    unsafe extern "C" {
        fn flock(fd: i32, operation: i32) -> i32;
    }

    pub(super) fn lock_exclusive(file: &File) -> io::Result<()> {
        let fd = file.as_raw_fd();

        // SAFETY: fd is a valid file descriptor owned by `file`.
        #[expect(unsafe_code, reason = "flock FFI call with valid fd")]
        let ret = unsafe { flock(fd, LOCK_EX) };

        if ret != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

#[cfg(windows)]
mod sys {
    use std::fs::File;
    use std::io;
    use std::os::windows::io::AsRawHandle;

    pub(super) fn lock_exclusive(file: &File) -> io::Result<()> {
        use std::ptr;

        // https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-lockfileex
        const LOCKFILE_EXCLUSIVE_LOCK: u32 = 0x0000_0002;

        unsafe extern "system" {
            fn LockFileEx(
                h_file: *mut std::ffi::c_void,
                dw_flags: u32,
                dw_reserved: u32,
                n_number_of_bytes_to_lock_low: u32,
                n_number_of_bytes_to_lock_high: u32,
                lp_overlapped: *mut Overlapped,
            ) -> i32;
        }

        #[repr(C)]
        struct Overlapped {
            internal: usize,
            internal_high: usize,
            offset: u32,
            offset_high: u32,
            h_event: *mut std::ffi::c_void,
        }

        let handle = file.as_raw_handle();
        let mut overlapped: Overlapped = Overlapped {
            internal: 0,
            internal_high: 0,
            offset: 0,
            offset_high: 0,
            h_event: ptr::null_mut(),
        };

        // SAFETY: handle is a valid file handle owned by `file`.
        #[expect(unsafe_code, reason = "LockFileEx FFI call with valid handle")]
        let ret = unsafe {
            LockFileEx(
                handle as *mut std::ffi::c_void,
                LOCKFILE_EXCLUSIVE_LOCK,
                0,
                u32::MAX,
                u32::MAX,
                &mut overlapped,
            )
        };

        if ret == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

#[cfg(not(any(unix, windows)))]
mod sys {
    use std::fs::File;
    use std::io;

    pub(super) fn lock_exclusive(_file: &File) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "file locking is not supported on this platform",
        ))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::sync::Arc;
    use test_log::test;

    #[test]
    fn std_fs_create_read_write() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;

        // Create and write
        let path = dir.path().join("test.txt");
        let opts = FsOpenOptions::new().write(true).create(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"hello world")?;
        FsFile::sync_all(&file)?;
        drop(file);

        // Read back
        let opts = FsOpenOptions::new().read(true);
        let mut file = fs.open(&path, &opts)?;
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        assert_eq!(buf, "hello world");

        Ok(())
    }

    #[test]
    fn std_fs_directory_operations() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;

        let nested = dir.path().join("a").join("b").join("c");
        fs.create_dir_all(&nested)?;
        assert!(fs.exists(&nested));

        // Create a file inside
        let file_path = nested.join("data.bin");
        let opts = FsOpenOptions::new().write(true).create_new(true);
        let mut file = fs.open(&file_path, &opts)?;
        file.write_all(b"data")?;
        drop(file);

        // read_dir
        let entries: Vec<_> = fs.read_dir(&nested)?.collect::<io::Result<Vec<_>>>()?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].file_name, "data.bin");
        assert!(!entries[0].is_dir);

        // metadata
        let meta = fs.metadata(&file_path)?;
        assert!(meta.is_file);
        assert!(!meta.is_dir);
        assert_eq!(meta.len, 4);

        // remove_file
        fs.remove_file(&file_path)?;
        assert!(!fs.exists(&file_path));

        // remove_dir_all
        let top = dir.path().join("a");
        fs.remove_dir_all(&top)?;
        assert!(!fs.exists(&top));

        Ok(())
    }

    #[test]
    fn std_fs_rename() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;

        let src = dir.path().join("src.txt");
        let dst = dir.path().join("dst.txt");

        let opts = FsOpenOptions::new().write(true).create(true);
        let mut file = fs.open(&src, &opts)?;
        file.write_all(b"content")?;
        drop(file);

        fs.rename(&src, &dst)?;
        assert!(!fs.exists(&src));
        assert!(fs.exists(&dst));

        Ok(())
    }

    #[test]
    fn std_fs_sync_directory() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;

        // Should not error on valid directories
        fs.sync_directory(dir.path())?;
        Ok(())
    }

    #[test]
    fn fs_file_metadata() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;

        let path = dir.path().join("meta.bin");
        let opts = FsOpenOptions::new().write(true).create(true).read(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"12345")?;

        let meta = FsFile::metadata(&file)?;
        assert!(meta.is_file);
        assert_eq!(meta.len, 5);

        Ok(())
    }

    #[test]
    fn fs_file_set_len() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;

        let path = dir.path().join("truncate.bin");
        let opts = FsOpenOptions::new().write(true).create(true).read(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"hello world")?;
        FsFile::set_len(&file, 5)?;

        let meta = FsFile::metadata(&file)?;
        assert_eq!(meta.len, 5);

        Ok(())
    }

    #[test]
    fn fs_file_lock_exclusive() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;

        let path = dir.path().join("lockfile");
        let opts = FsOpenOptions::new().write(true).create(true);
        let file = fs.open(&path, &opts)?;
        FsFile::lock_exclusive(&file)?;

        // Lock acquired — if we got here without blocking, the test passes
        Ok(())
    }

    /// Compile-time assertion: `Fs` is object-safe when associated types
    /// are specified.
    #[test]
    fn object_safety() -> io::Result<()> {
        let fs: Arc<dyn Fs<File = File, ReadDir = StdReadDir>> = Arc::new(StdFs);
        let dir = tempfile::tempdir()?;
        let bogus = dir.path().join("nonexistent");
        assert!(!fs.exists(&bogus));
        Ok(())
    }
}

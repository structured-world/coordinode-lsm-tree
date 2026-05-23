// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

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

// ---------------------------------------------------------------------------
// FsFile for std::fs::File
// ---------------------------------------------------------------------------
// Self:: calls delegate to File's inherent methods (clippy::use_self preference).

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

    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        // Fill-or-EOF loop: retry on short reads and EINTR so that callers
        // see either a full buffer or a short read that signals EOF.
        let mut filled = 0usize;

        while filled < buf.len() {
            // SAFETY: loop guard `filled < buf.len()` ensures this is in-bounds.
            #[expect(clippy::expect_used, reason = "filled < buf.len() by loop guard")]
            let remaining = buf.get_mut(filled..).expect("filled < buf.len()");
            let off = offset.saturating_add(filled as u64);

            let n = {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::FileExt;
                    match FileExt::read_at(self, remaining, off) {
                        Ok(n) => n,
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                        Err(e) => return Err(e),
                    }
                }

                #[cfg(windows)]
                {
                    use std::os::windows::fs::FileExt;
                    match FileExt::seek_read(self, remaining, off) {
                        Ok(n) => n,
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                        Err(e) => return Err(e),
                    }
                }

                #[cfg(not(any(unix, windows)))]
                {
                    let _ = (remaining, off);
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "read_at is not supported on this platform",
                    ));
                }
            };

            if n == 0 {
                break; // EOF
            }
            filled += n;
        }

        Ok(filled)
    }

    fn lock_exclusive(&self) -> io::Result<()> {
        sys::lock_exclusive(self)
    }
}

// ---------------------------------------------------------------------------
// Fs for StdFs
// ---------------------------------------------------------------------------

impl Fs for StdFs {
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
        let mut builder = OpenOptions::new();
        builder
            .read(opts.read)
            .write(opts.write)
            .create(opts.create)
            .create_new(opts.create_new)
            .truncate(opts.truncate)
            .append(opts.append);

        // O_DIRECT on Linux/Android (architectures with `asm-generic/fcntl.h`
        // value 0o40000: x86, x86_64, aarch64, riscv32/64, loongarch64,
        // s390x — i.e. every Linux arch we plausibly run on). Architectures
        // with a divergent O_DIRECT (arm 0o200000, mips 0o100000, parisc,
        // sparc) are not gated here on purpose: misencoding the flag would
        // silently pass the wrong bit to open(2). The FsOpenOptions doc
        // contract permits `direct_io` to be ignored, so divergent archs
        // simply fall through to a cached open — correctness preserved.
        //
        // macOS / Windows / other Unixes: same "may be silently ignored"
        // contract. macOS has no O_DIRECT (F_NOCACHE via fcntl post-open
        // is the closest equivalent and is out of scope here).
        #[cfg(all(
            any(target_os = "linux", target_os = "android"),
            any(
                target_arch = "x86",
                target_arch = "x86_64",
                target_arch = "aarch64",
                target_arch = "riscv32",
                target_arch = "riscv64",
                target_arch = "loongarch64",
                target_arch = "s390x",
            ),
        ))]
        if opts.direct_io {
            use std::os::unix::fs::OpenOptionsExt;
            // asm-generic/fcntl.h: #define O_DIRECT 00040000
            const O_DIRECT: i32 = 0o0_040_000;
            builder.custom_flags(O_DIRECT);
        }

        let file = builder.open(path)?;
        Ok(Box::new(file))
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        std::fs::create_dir_all(path)
    }

    fn create_dir(&self, path: &Path) -> io::Result<()> {
        std::fs::create_dir(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<FsDirEntry>> {
        // Fail-fast on bad entries is intentional: non-UTF-8 filenames in an
        // lsm-tree data directory indicate filesystem corruption (see FsDirEntry docs).
        std::fs::read_dir(path)?
            .map(|res| {
                let entry = res?;
                let file_type = entry.file_type()?;
                let file_name_os = entry.file_name();
                let file_name = file_name_os.into_string().map_err(|os| {
                    #[expect(
                        clippy::unnecessary_debug_formatting,
                        reason = "OsString has no Display impl — Debug is required"
                    )]
                    let msg = format!("non-UTF-8 filename in directory {}: {os:?}", path.display());
                    io::Error::new(io::ErrorKind::InvalidData, msg)
                })?;
                Ok(FsDirEntry {
                    path: entry.path(),
                    file_name,
                    is_dir: file_type.is_dir(),
                })
            })
            .collect()
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
            if !dir.metadata()?.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "sync_directory: path is not a directory",
                ));
            }
            dir.sync_all()
        }

        // Windows cannot fsync directories — no-op, same as crate::file::fsync_directory.
        #[cfg(target_os = "windows")]
        {
            let _ = path;
            Ok(())
        }
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        path.try_exists()
    }

    fn hard_link(&self, src: &Path, dst: &Path) -> io::Result<()> {
        match std::fs::hard_link(src, dst) {
            Ok(()) => Ok(()),
            Err(e) if is_cross_device(&e) => {
                // `debug!`, not `warn!`: a tier-misconfigured checkpoint
                // can hit this path once per SST + once per blob (potentially
                // thousands of times). The checkpoint driver is responsible
                // for emitting a single summary warning if the fallback rate
                // is excessive; per-file noise here would drown real logs.
                log::debug!(
                    "hard_link({}, {}) crossed filesystems — falling back to copy",
                    src.display(),
                    dst.display(),
                );
                copy_fallback(src, dst)
            }
            Err(e) => Err(e),
        }
    }

    fn backend_id(&self) -> Option<u64> {
        Some(KERNEL_BACKEND_ID)
    }
}

/// Shared by every backend that resolves paths against the host kernel's
/// filesystem table — currently `StdFs` and `IoUringFs`. Two such backends
/// can hard-link across instances because `std::fs::hard_link(src, dst)`
/// resolves both paths through the same kernel namespace.
pub const KERNEL_BACKEND_ID: u64 = 0x4b45_524e_454c_5f46; // "KERNEL_F"

/// Detects `EXDEV` (cross-device link) errors across platforms.
///
/// Unix exposes the raw `EXDEV` (errno 18 on Linux/macOS/BSDs); we also
/// accept [`io::ErrorKind::CrossesDevices`] (stable since Rust 1.85) and
/// [`io::ErrorKind::Unsupported`] which Windows surfaces when a volume
/// configuration disallows hard links altogether.
///
/// We deliberately do NOT treat [`io::ErrorKind::PermissionDenied`] as a
/// cross-device signal even though Windows can sometimes surface
/// cross-volume link failures that way. Misclassifying genuine ACL /
/// permission errors as "different filesystem" would silently turn a
/// "you can't read this" into a full byte copy, hiding real security
/// misconfigurations. Operators hitting that edge case on Windows can
/// fix it explicitly (move the target volume, adjust ACLs); the
/// fall-back is opt-out by design.
fn is_cross_device(err: &io::Error) -> bool {
    #[cfg(unix)]
    {
        // POSIX `EXDEV` ("invalid cross-device link"). The raw value is
        // 18 on every supported Unix target (Linux, macOS, FreeBSD,
        // OpenBSD, NetBSD, Android — see `errno.h` on each platform).
        // We declare it as a named constant rather than depending on
        // `libc`: the crate deliberately avoids a `libc` dependency
        // (see the `flock` extern in this file for the same pattern),
        // and EXDEV's value is locked by ABI compatibility.
        const EXDEV: i32 = 18;
        if err.raw_os_error() == Some(EXDEV) {
            return true;
        }
    }
    matches!(
        err.kind(),
        io::ErrorKind::CrossesDevices | io::ErrorKind::Unsupported
    )
}

/// Byte-copy fallback used when [`hard_link`](Fs::hard_link) cannot create
/// a true link (cross-device or in-memory FS without inode semantics).
///
/// Uses `create_new` semantics so an accidental clobber surfaces as
/// [`io::ErrorKind::AlreadyExists`] — matching real `hard_link` behaviour.
fn copy_fallback(src: &Path, dst: &Path) -> io::Result<()> {
    use std::io::{Read, Write};

    // Wrap the copy in an inner closure so any post-`create_new` error
    // (ENOSPC, EIO, write_all failure, sync_all failure) triggers a
    // best-effort unlink of the partially-written destination. Without
    // this, a failed `copy_fallback` leaves a truncated file behind and
    // the next retry surfaces as `AlreadyExists` — callers would then
    // see a corrupt file from an operation that already reported error.
    let res: io::Result<()> = (|| {
        let mut src_file = File::open(src)?;
        let mut dst_file = OpenOptions::new().write(true).create_new(true).open(dst)?;

        // Heap-allocated buffer — checkpoint is cold-path I/O, so a
        // 64 KiB Vec is cheaper than blowing past clippy's stack-array budget.
        let mut buf = vec![0u8; 64 * 1024].into_boxed_slice();
        loop {
            let n = match src_file.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            };
            #[expect(
                clippy::indexing_slicing,
                reason = "n was just produced by read() and bounded by buf.len()"
            )]
            dst_file.write_all(&buf[..n])?;
        }
        dst_file.sync_all()
    })();

    if res.is_err() {
        // Best-effort: if cleanup itself fails (permission denied,
        // already removed by another process), there's nothing more we
        // can do — the original error is what the caller needs to see.
        let _ = std::fs::remove_file(dst);
    }
    res
}

// ---------------------------------------------------------------------------
// Platform-specific file locking
// ---------------------------------------------------------------------------

#[cfg(unix)]
mod sys {
    use std::ffi::c_int;
    use std::fs::File;
    use std::io;
    use std::os::unix::io::AsRawFd;

    // Declare flock directly to avoid requiring libc as a direct dependency.
    const LOCK_EX: c_int = 2;

    // SAFETY: declaration matches the POSIX `flock` ABI on Unix targets.
    unsafe extern "C" {
        fn flock(fd: c_int, operation: c_int) -> c_int;
    }

    pub(super) fn lock_exclusive(file: &File) -> io::Result<()> {
        let fd = file.as_raw_fd();

        loop {
            // SAFETY: fd is a valid file descriptor owned by `file`.
            #[expect(unsafe_code, reason = "flock FFI call with valid fd")]
            let ret = unsafe { flock(fd, LOCK_EX) };

            if ret == 0 {
                return Ok(());
            }

            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
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

        // SAFETY: declaration matches the Windows `LockFileEx` ABI and `Overlapped` layout.
        #[expect(non_snake_case, reason = "FFI name matches Windows API")]
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
        let mut overlapped = Overlapped {
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
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::useless_vec,
    reason = "test code"
)]
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
        file.sync_all()?;
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
        assert!(fs.exists(&nested)?);

        // Create a file inside
        let file_path = nested.join("data.bin");
        let opts = FsOpenOptions::new().write(true).create_new(true);
        let mut file = fs.open(&file_path, &opts)?;
        file.write_all(b"data")?;
        drop(file);

        // read_dir
        let entries = fs.read_dir(&nested)?;
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
        assert!(!fs.exists(&file_path)?);

        // remove_dir_all
        let top = dir.path().join("a");
        fs.remove_dir_all(&top)?;
        assert!(!fs.exists(&top)?);

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
        assert!(!fs.exists(&src)?);
        assert!(fs.exists(&dst)?);

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

        let meta = file.metadata()?;
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
        file.set_len(5)?;

        let meta = file.metadata()?;
        assert_eq!(meta.len, 5);

        Ok(())
    }

    #[test]
    #[cfg(any(unix, windows))]
    fn fs_file_lock_exclusive() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;

        let path = dir.path().join("lockfile");
        let opts = FsOpenOptions::new().write(true).create(true);
        let file = fs.open(&path, &opts)?;
        file.lock_exclusive()?;

        // Verifies flock() syscall succeeds without error. Testing actual
        // lock contention (try_lock from second thread) is out of scope for
        // the Fs trait definition — belongs in integration tests.
        Ok(())
    }

    #[test]
    #[cfg(any(unix, windows))]
    fn fs_file_read_at() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;

        let path = dir.path().join("pread.bin");
        let opts = FsOpenOptions::new().write(true).create(true).read(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"hello world")?;

        // read_at at offset 6 should return "world"
        let mut buf = [0u8; 5];
        let n = file.read_at(&mut buf, 6)?;
        assert_eq!(n, 5);
        assert_eq!(&buf, b"world");

        // read_at at offset 0 should return "hello"
        let n = file.read_at(&mut buf, 0)?;
        assert_eq!(n, 5);
        assert_eq!(&buf, b"hello");

        Ok(())
    }

    #[test]
    fn fs_open_options_default() {
        let opts = FsOpenOptions::default();
        assert!(!opts.read);
        assert!(!opts.write);
        assert!(!opts.create);
        assert!(!opts.create_new);
        assert!(!opts.truncate);
        assert!(!opts.append);
        assert!(!opts.direct_io);
    }

    #[test]
    fn fs_open_options_builders() {
        let opts = FsOpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .create_new(false)
            .truncate(true)
            .append(false)
            .direct_io(true);
        assert!(opts.read);
        assert!(opts.write);
        assert!(opts.create);
        assert!(!opts.create_new);
        assert!(opts.truncate);
        assert!(!opts.append);
        assert!(opts.direct_io);
    }

    #[test]
    fn fs_file_sync_data() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;

        let path = dir.path().join("sync_data.bin");
        let opts = FsOpenOptions::new().write(true).create(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"data")?;
        file.sync_data()?;

        Ok(())
    }

    #[test]
    fn fs_open_truncate_and_append() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;
        let path = dir.path().join("trunc.txt");

        // Create with content
        let opts = FsOpenOptions::new().write(true).create(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"hello world")?;
        drop(file);

        // Truncate on reopen
        let opts = FsOpenOptions::new().write(true).truncate(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"hi")?;
        drop(file);

        let meta = fs.metadata(&path)?;
        assert_eq!(meta.len, 2);

        // Append mode
        let opts = FsOpenOptions::new().write(true).append(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"!")?;
        drop(file);

        let meta = fs.metadata(&path)?;
        assert_eq!(meta.len, 3);

        Ok(())
    }

    #[test]
    fn fs_dir_entry_fields() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;

        // Create a subdirectory and a file
        let sub = dir.path().join("subdir");
        fs.create_dir_all(&sub)?;
        let file_path = dir.path().join("file.txt");
        let opts = FsOpenOptions::new().write(true).create(true);
        fs.open(&file_path, &opts)?;

        let mut entries = fs.read_dir(dir.path())?;
        entries.sort_by(|a, b| a.file_name.cmp(&b.file_name));

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].file_name, "file.txt");
        assert!(!entries[0].is_dir);
        assert_eq!(entries[1].file_name, "subdir");
        assert!(entries[1].is_dir);

        Ok(())
    }

    #[test]
    fn fs_metadata_directory() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;
        let meta = fs.metadata(dir.path())?;
        assert!(meta.is_dir);
        assert!(!meta.is_file);

        Ok(())
    }

    // Linux only: macOS (HFS+/APFS) rejects non-UTF-8 filenames at the FS layer.
    #[test]
    #[cfg(target_os = "linux")]
    fn read_dir_rejects_non_utf8_filename() -> io::Result<()> {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;

        let dir = tempfile::tempdir()?;
        // Create a file with invalid UTF-8 bytes in its name.
        let bad_name = OsStr::from_bytes(&[0xff, 0xfe]);
        let bad_path = dir.path().join(bad_name);
        if std::fs::write(&bad_path, b"data").is_err() {
            // Filesystem rejected the non-UTF-8 filename (e.g. overlay,
            // container mounts, restrictive mount options) — test
            // precondition cannot be met, skip gracefully.
            return Ok(());
        }

        let fs = StdFs;
        match fs.read_dir(dir.path()) {
            Err(err) => {
                assert_eq!(err.kind(), io::ErrorKind::InvalidData);
                let msg = err.to_string();
                assert!(
                    msg.contains("non-UTF-8 filename"),
                    "unexpected error: {msg}"
                );
                assert!(
                    msg.contains(&dir.path().display().to_string()),
                    "error should include directory path: {msg}",
                );
            }
            Ok(_) => panic!("read_dir should fail on non-UTF-8 filename"),
        }
        Ok(())
    }

    /// Compile-time assertion: `Fs` is object-safe without specifying
    /// associated types — enables simple `Arc<dyn Fs>` for per-level routing.
    #[test]
    fn object_safety() -> io::Result<()> {
        let fs: Arc<dyn Fs> = Arc::new(StdFs);
        let dir = tempfile::tempdir()?;
        let bogus = dir.path().join("nonexistent");
        assert!(!fs.exists(&bogus)?);
        Ok(())
    }

    #[test]
    fn hard_link_creates_second_path_to_same_inode() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;

        let src = dir.path().join("src.bin");
        let dst = dir.path().join("dst.bin");
        std::fs::write(&src, b"checkpoint payload")?;

        fs.hard_link(&src, &dst)?;

        // Both paths exist and have the same content.
        assert_eq!(std::fs::read(&dst)?, b"checkpoint payload");

        // Mutating the link changes both views (same inode), proving this
        // was a true hard link, not a copy. We only check this on Unix —
        // Windows hard links share content but inode equality is not
        // exposed through std.
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let src_meta = std::fs::metadata(&src)?;
            let dst_meta = std::fs::metadata(&dst)?;
            assert_eq!(src_meta.ino(), dst_meta.ino());
            assert_eq!(src_meta.dev(), dst_meta.dev());
            assert_eq!(dst_meta.nlink(), 2);
        }

        // Removing the source leaves the link intact.
        fs.remove_file(&src)?;
        assert_eq!(std::fs::read(&dst)?, b"checkpoint payload");
        Ok(())
    }

    #[test]
    fn hard_link_rejects_existing_destination() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;

        let src = dir.path().join("src");
        let dst = dir.path().join("dst");
        std::fs::write(&src, b"src")?;
        std::fs::write(&dst, b"dst")?;

        let err = fs.hard_link(&src, &dst).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        Ok(())
    }

    #[test]
    fn hard_link_rejects_missing_source() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;

        let err = fs
            .hard_link(&dir.path().join("missing"), &dir.path().join("dst"))
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        Ok(())
    }

    /// Forces the EXDEV fallback by calling [`copy_fallback`] directly.
    /// A real cross-device scenario needs two mounted filesystems which is
    /// impractical in unit tests, but the fallback path itself is exercised.
    #[test]
    fn copy_fallback_copies_bytes_independently() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let src = dir.path().join("src");
        let dst = dir.path().join("dst");
        std::fs::write(&src, b"payload-for-fallback")?;

        copy_fallback(&src, &dst)?;
        assert_eq!(std::fs::read(&dst)?, b"payload-for-fallback");

        // Writing to dst must NOT affect src (independent file).
        std::fs::write(&dst, b"modified")?;
        assert_eq!(std::fs::read(&src)?, b"payload-for-fallback");
        Ok(())
    }

    #[test]
    fn is_cross_device_detects_exdev_and_kind_variants() {
        // Synthesise a raw EXDEV error — what Linux/macOS/BSDs return when
        // hard_link spans devices. `is_cross_device` must accept it so the
        // fallback copy path kicks in. Gated to Unix because errno 18 has
        // a different meaning on Windows (ERROR_NO_MORE_FILES) and the
        // `#[cfg(unix)]` branch in `is_cross_device` is never compiled on
        // non-Unix targets.
        #[cfg(unix)]
        {
            // Matches the `EXDEV` constant inside `is_cross_device`.
            const EXDEV: i32 = 18;
            let exdev = io::Error::from_raw_os_error(EXDEV);
            assert!(is_cross_device(&exdev), "raw EXDEV must be recognised");
        }

        // ErrorKind::CrossesDevices is the modern stable variant (Rust 1.85+).
        let crosses = io::Error::from(io::ErrorKind::CrossesDevices);
        assert!(is_cross_device(&crosses));

        // ErrorKind::Unsupported covers Windows / exotic filesystems where
        // hard links are simply not implemented — also a cue to fall back.
        let unsupported = io::Error::from(io::ErrorKind::Unsupported);
        assert!(is_cross_device(&unsupported));

        // A garden-variety NotFound must NOT be misclassified — the caller
        // needs to surface that error verbatim, not silently copy.
        let notfound = io::Error::from(io::ErrorKind::NotFound);
        assert!(!is_cross_device(&notfound));
    }

    #[test]
    fn copy_fallback_refuses_to_overwrite() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let src = dir.path().join("src");
        let dst = dir.path().join("dst");
        std::fs::write(&src, b"src")?;
        std::fs::write(&dst, b"dst")?;

        let err = copy_fallback(&src, &dst).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        Ok(())
    }
}

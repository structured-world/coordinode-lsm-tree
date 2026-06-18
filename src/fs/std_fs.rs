// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use super::{FileHint, Fs, FsDirEntry, FsFile, FsMetadata, FsOpenOptions, SyncMode};
use crate::io;
use crate::path::Path;
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, string::String, vec::Vec};
use std::fs::{File, OpenOptions};

/// Plain `fsync` (no `F_FULLFSYNC`) for [`SyncMode::Normal`].
///
/// On macOS, `File::sync_all` issues `fcntl(F_FULLFSYNC)` (a full hardware
/// barrier, ~50× slower than `fsync`). `Normal` mode wants the cheaper plain
/// `fsync` - which std does not expose on macOS - so call it directly via
/// `libc`. On every other platform `File::sync_all` IS plain `fsync`, so just
/// delegate.
#[cfg(target_os = "macos")]
fn normal_fsync(file: &File) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    // SAFETY: `fd` is a valid open descriptor for the lifetime of `file`.
    let rc = unsafe { libc::fsync(file.as_raw_fd()) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().into())
    }
}

#[cfg(not(target_os = "macos"))]
fn normal_fsync(file: &File) -> io::Result<()> {
    // No `F_FULLFSYNC` distinction off macOS - `sync_all` is plain `fsync`.
    File::sync_all(file).map_err(io::Error::from)
}

/// Default [`Fs`] implementation backed by [`std::fs`].
///
/// This is a zero-sized type - when used as a monomorphized generic
/// parameter it adds no runtime overhead.
#[derive(Clone, Copy, Debug, Default)]
pub struct StdFs;

// ---------------------------------------------------------------------------
// FsFile for std::fs::File
// ---------------------------------------------------------------------------
// Self:: calls delegate to File's inherent methods (clippy::use_self preference).

impl FsFile for File {
    fn sync_all(&self) -> io::Result<()> {
        Self::sync_all(self).map_err(io::Error::from)
    }

    fn sync_data(&self) -> io::Result<()> {
        Self::sync_data(self).map_err(io::Error::from)
    }

    fn sync_all_with(&self, mode: SyncMode) -> io::Result<()> {
        match mode {
            // `File::sync_all` is `fcntl(F_FULLFSYNC)` on macOS.
            SyncMode::Full => Self::sync_all(self).map_err(io::Error::from),
            SyncMode::Normal => normal_fsync(self),
        }
    }

    fn sync_data_with(&self, mode: SyncMode) -> io::Result<()> {
        match mode {
            SyncMode::Full => Self::sync_data(self).map_err(io::Error::from),
            // Normal data-sync collapses to plain `fsync`: on macOS
            // `sync_data` is also `F_FULLFSYNC`, and a plain `fsync` already
            // covers the data, so there is no cheaper data-only barrier to
            // issue.
            SyncMode::Normal => normal_fsync(self),
        }
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
        Self::set_len(self, size).map_err(io::Error::from)
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
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(e) => return Err(e.into()),
                    }
                }

                #[cfg(windows)]
                {
                    use std::os::windows::fs::FileExt;
                    match FileExt::seek_read(self, remaining, off) {
                        Ok(n) => n,
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(e) => return Err(e.into()),
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
        sys::lock_exclusive(self).map_err(io::Error::from)
    }

    fn try_lock_exclusive(&self) -> io::Result<bool> {
        sys::try_lock_exclusive(self).map_err(io::Error::from)
    }

    fn hint(&self, hint: FileHint) -> io::Result<()> {
        sys::fadvise(self, hint).map_err(io::Error::from)
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

        // Gate matches the `mod direct_io;` declaration in `fs/mod.rs`
        // - the submodule only exists when `feature = "std"` is on.
        // Without the gate this site would fail to compile under
        // `--no-default-features --features alloc` even before the
        // wider std-bound surface of `StdFs` itself hits the trait
        // signatures; keeping the cfg in sync prevents adding a
        // resolution-time error on top of the type-checking ones
        // already tracked under the no-std migration epic.
        #[cfg(feature = "std")]
        super::direct_io::apply_direct_io_flag(&mut builder, opts.direct_io);

        let file = builder.open(path)?;
        Ok(Box::new(file))
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        std::fs::create_dir_all(path).map_err(io::Error::from)
    }

    fn create_dir(&self, path: &Path) -> io::Result<()> {
        std::fs::create_dir(path).map_err(io::Error::from)
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
                        reason = "OsString has no Display impl - Debug is required"
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
        std::fs::remove_file(path).map_err(io::Error::from)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_dir_all(path).map_err(io::Error::from)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to).map_err(io::Error::from)
    }

    fn metadata(&self, path: &Path) -> io::Result<FsMetadata> {
        let m = std::fs::metadata(path)?;
        Ok(FsMetadata {
            len: m.len(),
            is_dir: m.is_dir(),
            is_file: m.is_file(),
        })
    }

    #[cfg(unix)]
    fn available_space(&self, path: &Path) -> io::Result<u64> {
        super::statvfs_available_space(path).map_err(io::Error::from)
    }

    #[cfg(windows)]
    fn available_space(&self, path: &Path) -> io::Result<u64> {
        available_space_sys::disk_free_available(path).map_err(io::Error::from)
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
            dir.sync_all().map_err(io::Error::from)
        }

        // Windows cannot fsync directories - no-op, same as crate::file::fsync_directory.
        #[cfg(target_os = "windows")]
        {
            let _ = path;
            Ok(())
        }
    }

    fn sync_directory_with(&self, path: &Path, mode: SyncMode) -> io::Result<()> {
        #[cfg(not(target_os = "windows"))]
        {
            let dir = File::open(path)?;
            if !dir.metadata()?.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "sync_directory: path is not a directory",
                ));
            }
            match mode {
                SyncMode::Full => dir.sync_all().map_err(io::Error::from),
                SyncMode::Normal => normal_fsync(&dir),
            }
        }

        // Windows cannot fsync directories - no-op.
        #[cfg(target_os = "windows")]
        {
            let _ = (path, mode);
            Ok(())
        }
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        path.try_exists().map_err(io::Error::from)
    }

    fn hard_link(&self, src: &Path, dst: &Path) -> io::Result<()> {
        // Pure hard link. On a cross-device target this returns the EXDEV
        // error instead of silently byte-copying: the copy belongs to the
        // caller that actually wants a cross-filesystem copy
        // (checkpoint's `link_or_copy_cross_fs`), which detects the
        // cross-device error via `is_cross_device` and runs its own
        // SyncMode-aware streamed copy. Keeping the copy in one place means
        // the copied file's durability honors `Config::sync_mode` instead
        // of always paying `F_FULLFSYNC` here.
        //
        // Normalise a cross-device failure to `ErrorKind::CrossesDevices`
        // while the errno is still readable on the std error: once it folds
        // into `crate::io::Error` at this trait boundary the errno is gone,
        // so the caller's kind-based `is_cross_device` would otherwise miss
        // the platforms where std reports EXDEV without the matching kind.
        std::fs::hard_link(src, dst).map_err(|e| {
            if is_std_cross_device(&e) {
                io::Error::from_kind(io::ErrorKind::CrossesDevices)
            } else {
                e.into()
            }
        })
    }

    fn backend_id(&self) -> Option<u64> {
        Some(KERNEL_BACKEND_ID)
    }

    #[cfg(unix)]
    fn volume_id(&self, path: &Path) -> Option<u64> {
        super::unix_volume_id(path)
    }

    fn hard_link_count(&self, path: &Path) -> io::Result<u64> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Ok(std::fs::metadata(path)?.nlink())
        }
        // Windows: no portable stable-Rust nlink. NTFS hard links are real, so
        // truncating without a confirmed link count could zero a checkpoint's
        // hard-linked copy — the same hazard the Unix guard prevents. Report
        // "unsupported" so the reclaim path conservatively SKIPS the
        // synchronous truncate and relies on the async unlink alone (correct,
        // just without the immediate-footprint fast path). Narrowing the
        // contract this way is deliberate; raising it to truncate-on-Windows
        // would need a winapi `GetFileInformationByHandle` link-count query.
        #[cfg(not(unix))]
        {
            let _ = path;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "hard_link_count requires a Unix target",
            ))
        }
    }

    /// macOS: detects APFS (the common macOS filesystem with copy-on-write,
    /// reflink, and native snapshots) via `statfs(2)`'s `f_fstypename`. Other
    /// macOS filesystems (HFS+, exFAT, SMB/NFS mounts) and any detection failure
    /// report the conservative default. (Linux has its own `statfs` `f_type`
    /// detection in the `target_os = "linux"` variant of this method below;
    /// other targets fall back to the trait's conservative default.)
    #[cfg(target_os = "macos")]
    fn capabilities(&self, path: &Path) -> super::FsCapabilities {
        macos_caps::capabilities(path)
    }

    /// macOS: O(1) clone via `clonefile(2)` on APFS. When the filesystem
    /// declines the clone (non-APFS mount, cross-device) it falls back to the
    /// shared streamed byte copy, so the clone still succeeds (just without
    /// block sharing). A genuine I/O error propagates.
    #[cfg(target_os = "macos")]
    fn reflink_file(&self, src: &Path, dst: &Path) -> io::Result<()> {
        match macos_caps::clonefile(src, dst) {
            Ok(()) => Ok(()),
            Err(e) if macos_caps::clone_should_fall_back(&e) => {
                super::copy_file_streamed(self, src, dst)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Linux: detect the filesystem via `statfs(2)`'s `f_type` magic. Btrfs and
    /// ZFS report the full profile (integrity, scrub, copy-on-write, reflink,
    /// snapshot); XFS reports copy-on-write plus reflink (reflink only when the
    /// volume was formatted `reflink=1`, which `reflink_file` handles by falling
    /// back). Other filesystems and any detection failure report the
    /// conservative default.
    #[cfg(target_os = "linux")]
    fn capabilities(&self, path: &Path) -> super::FsCapabilities {
        linux_caps::capabilities(path)
    }

    /// Linux: O(1) clone via `ioctl(FICLONE)` (Btrfs, XFS-reflink). Falls back
    /// to the shared streamed copy when the filesystem declines the clone
    /// (non-reflink FS, cross-device); a genuine I/O error propagates.
    #[cfg(target_os = "linux")]
    fn reflink_file(&self, src: &Path, dst: &Path) -> io::Result<()> {
        match linux_caps::ficlone(src, dst) {
            Ok(()) => Ok(()),
            Err(e) if linux_caps::clone_should_fall_back(&e) => {
                super::copy_file_streamed(self, src, dst)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Linux: clear per-file copy-on-write (`FS_NOCOW_FL` via
    /// `ioctl(FS_IOC_SETFLAGS)`) so write-once SST files on Btrfs avoid the
    /// copy-on-write fragmentation penalty. The flag only takes effect on a
    /// still-empty file,
    /// so callers invoke this right after creating the SST. A no-op on
    /// filesystems that do not support the inode-flags ioctl.
    #[cfg(target_os = "linux")]
    fn try_disable_cow(&self, path: &Path) -> io::Result<()> {
        linux_caps::try_disable_cow(path).map_err(io::Error::from)
    }

    /// Linux: reclaim a mid-file byte range via `fallocate(PUNCH_HOLE)`. Gated
    /// by callers on [`super::FsCapabilities::punch_hole`], which `capabilities`
    /// sets for ext4 / xfs / btrfs / zfs / tmpfs. macOS and other targets keep
    /// the trait default ([`io::ErrorKind::Unsupported`]).
    #[cfg(target_os = "linux")]
    fn punch_hole(&self, path: &Path, offset: u64, len: u64) -> io::Result<()> {
        linux_caps::punch_hole(path, offset, len).map_err(io::Error::from)
    }
}

/// Shared by every backend that resolves paths against the host kernel's
/// filesystem table - currently `StdFs` and `IoUringFs`. Two such backends
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
// `pub(crate)`, surfaced crate-wide via the `pub(crate) use` re-export in
// `fs/mod.rs` so `checkpoint::link_or_copy_cross_fs` can detect cross-device
// errors. The crate-scoped visibility (not `pub`) keeps this off any public
// surface even if `std_fs` is ever exported. clippy's `redundant_pub_crate`
// fires only because the enclosing module is currently private; the re-export
// genuinely needs crate visibility, so the lint is a false positive here.
/// Cross-device detection on a raw `std::io::Error`, where the errno is still
/// available. Used by [`StdFs::hard_link`] to NORMALISE a cross-device link
/// failure into [`crate::io::ErrorKind::CrossesDevices`] before the error
/// crosses the `Fs` trait boundary into [`crate::io::Error`] (which carries no
/// errno). After normalisation the kind-based [`is_cross_device`] is enough.
fn is_std_cross_device(err: &std::io::Error) -> bool {
    #[cfg(unix)]
    {
        // POSIX `EXDEV` ("invalid cross-device link"). The raw value is
        // 18 on every supported Unix target (Linux, macOS, FreeBSD,
        // OpenBSD, NetBSD, Android - see `errno.h` on each platform).
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
        std::io::ErrorKind::CrossesDevices | std::io::ErrorKind::Unsupported
    )
}

/// Cross-device detection on a [`crate::io::Error`] (post-`Fs`-boundary, no
/// errno). [`StdFs::hard_link`] normalises the raw errno case to the
/// `CrossesDevices` kind, so a kind check is authoritative here.
#[expect(
    clippy::redundant_pub_crate,
    reason = "re-exported crate-wide via fs::mod; pub(crate) communicates the intended scope"
)]
pub(crate) fn is_cross_device(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::CrossesDevices | io::ErrorKind::Unsupported
    )
}

// ---------------------------------------------------------------------------
// Platform-specific file locking
// ---------------------------------------------------------------------------

#[cfg(unix)]
mod sys {
    use super::FileHint;
    use std::ffi::c_int;
    use std::fs::File;
    use std::io;
    use std::os::unix::io::AsRawFd;

    // Declare flock directly to avoid requiring libc as a direct dependency.
    const LOCK_EX: c_int = 2;
    // Non-blocking modifier: flock returns EWOULDBLOCK instead of waiting when
    // the lock is already held.
    const LOCK_NB: c_int = 4;

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

            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
    }

    pub(super) fn try_lock_exclusive(file: &File) -> io::Result<bool> {
        let fd = file.as_raw_fd();

        loop {
            // SAFETY: fd is a valid file descriptor owned by `file`.
            #[expect(unsafe_code, reason = "flock FFI call with valid fd")]
            let ret = unsafe { flock(fd, LOCK_EX | LOCK_NB) };

            if ret == 0 {
                return Ok(true);
            }

            // EINTR can interrupt even the non-blocking call; retry. EWOULDBLOCK
            // (the lock is held elsewhere) is the expected contention signal and
            // maps to `Ok(false)`, not an error.
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            if err.kind() == std::io::ErrorKind::WouldBlock {
                return Ok(false);
            }
            return Err(err);
        }
    }

    // POSIX_FADV_* values are stable across glibc / musl / *BSD libc.
    // macOS has no posix_fadvise - routed to a no-op `fadvise` below.
    #[cfg(not(target_os = "macos"))]
    const POSIX_FADV_NORMAL: c_int = 0;
    #[cfg(not(target_os = "macos"))]
    const POSIX_FADV_RANDOM: c_int = 1;
    #[cfg(not(target_os = "macos"))]
    const POSIX_FADV_SEQUENTIAL: c_int = 2;
    #[cfg(not(target_os = "macos"))]
    const POSIX_FADV_DONTNEED: c_int = 4;

    // EINVAL == 22 on every Linux / *BSD target we ship to. Used to
    // map "kernel rejected this hint" to Ok(()) per the FsFile::hint
    // contract - hints are advisory, EINVAL means "the kernel didn't
    // like this advice code", not "the file is broken".
    #[cfg(not(target_os = "macos"))]
    const EINVAL: c_int = 22;

    // libc symbol selection (the 32-bit glibc trap):
    //
    // Plain `posix_fadvise` on 32-bit glibc takes a 32-bit off_t
    // unless the caller opts into `_FILE_OFFSET_BITS=64` - and Rust
    // FFI declarations don't get that define. Passing i64 args into
    // a 32-bit-off_t syscall wrapper corrupts the stack.
    //
    // `posix_fadvise64` always takes int64_t but is a glibc-only
    // symbol; musl doesn't export it (the kernel sees the same
    // syscall, but musl has no userspace wrapper). bionic (Android)
    // does export it.
    //
    // Resolution by target:
    // - 32-bit glibc Linux + 32-bit Android: posix_fadvise64
    //   (otherwise stack corruption)
    // - musl on any pointer width: posix_fadvise (musl is sane on
    //   32-bit too - its plain wrapper takes 64-bit off_t)
    // - 64-bit Linux/Android: posix_fadvise (off_t is always 64-bit)
    // - BSDs (freebsd/netbsd/dragonfly): posix_fadvise (off_t is
    //   always 64-bit, no *64 variant exists)
    #[cfg(all(
        any(target_os = "linux", target_os = "android"),
        target_pointer_width = "32",
        any(target_env = "gnu", target_env = "")
    ))]
    // SAFETY: matches glibc / bionic posix_fadvise64 ABI on 32-bit.
    unsafe extern "C" {
        fn posix_fadvise64(fd: c_int, offset: i64, len: i64, advice: c_int) -> c_int;
    }

    #[cfg(all(
        not(target_os = "macos"),
        not(all(
            any(target_os = "linux", target_os = "android"),
            target_pointer_width = "32",
            any(target_env = "gnu", target_env = "")
        ))
    ))]
    // SAFETY: matches POSIX posix_fadvise ABI (off_t is 64-bit on
    // 64-bit Linux/Android, BSD, and on musl regardless of pointer
    // width).
    unsafe extern "C" {
        fn posix_fadvise(fd: c_int, offset: i64, len: i64, advice: c_int) -> c_int;
    }

    #[cfg(not(target_os = "macos"))]
    pub(super) fn fadvise(file: &File, hint: FileHint) -> io::Result<()> {
        let advice = match hint {
            FileHint::Default => POSIX_FADV_NORMAL,
            FileHint::Sequential => POSIX_FADV_SEQUENTIAL,
            FileHint::Random => POSIX_FADV_RANDOM,
            FileHint::WriteOnce => POSIX_FADV_DONTNEED,
        };
        // offset=0 + len=0 = "apply to the whole file" per
        // posix_fadvise(2). posix_fadvise{,64} returns the errno
        // DIRECTLY (not via errno; 0 on success, positive errno on
        // failure).
        let fd = file.as_raw_fd();
        // SAFETY: fd is a valid file descriptor owned by `file`;
        // offset / len / advice are all valid inputs.
        #[expect(unsafe_code, reason = "posix_fadvise FFI call with valid fd")]
        let ret = unsafe {
            #[cfg(all(
                any(target_os = "linux", target_os = "android"),
                target_pointer_width = "32",
                any(target_env = "gnu", target_env = "")
            ))]
            {
                posix_fadvise64(fd, 0, 0, advice)
            }
            #[cfg(not(all(
                any(target_os = "linux", target_os = "android"),
                target_pointer_width = "32",
                any(target_env = "gnu", target_env = "")
            )))]
            {
                posix_fadvise(fd, 0, 0, advice)
            }
        };
        // EINVAL = kernel doesn't recognise this advice code (or the
        // fd isn't a regular file - e.g. pipe / socket / character
        // device). Hints are advisory; treat as a no-op per the
        // FsFile::hint contract.
        if ret == 0 || ret == EINVAL {
            Ok(())
        } else {
            Err(std::io::Error::from_raw_os_error(ret))
        }
    }

    // macOS has no posix_fadvise. The closest primitives are
    // `fcntl(F_RDADVISE)` (sequential prefetch hint, requires a byte
    // range - not useful for the whole-file hints we want) and
    // `fcntl(F_NOCACHE)` (toggle uncached I/O - too blunt for our use
    // case). Treat as a no-op for now; the performance benefit on
    // macOS is small enough that wiring a half-equivalent isn't worth
    // the complexity.
    #[cfg(target_os = "macos")]
    #[expect(
        clippy::unnecessary_wraps,
        reason = "FsFile::hint signature requires io::Result<()>; macOS branch is a no-op until we wire fcntl(F_RDADVISE)"
    )]
    pub(super) fn fadvise(_file: &File, _hint: FileHint) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(windows)]
mod sys {
    use super::FileHint;
    use std::fs::File;
    use std::io;
    use std::os::windows::io::AsRawHandle;

    #[expect(
        clippy::unnecessary_wraps,
        reason = "FsFile::hint signature requires io::Result<()>; Windows branch is a no-op until we thread the hint through FsOpenOptions for CreateFile flags"
    )]
    pub(super) fn fadvise(_file: &File, _hint: FileHint) -> io::Result<()> {
        // Windows has no direct posix_fadvise equivalent. The closest
        // primitive (`FILE_FLAG_SEQUENTIAL_SCAN` / `_RANDOM_ACCESS`)
        // must be set at CreateFile time and can't be changed for an
        // already-open handle. Treat as a no-op - if Windows
        // performance becomes a concern we'd thread the hint through
        // FsOpenOptions instead and set the flag at open time.
        Ok(())
    }

    pub(super) fn lock_exclusive(file: &File) -> io::Result<()> {
        use core::ptr;

        // https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-lockfileex
        const LOCKFILE_EXCLUSIVE_LOCK: u32 = 0x0000_0002;

        // SAFETY: declaration matches the Windows `LockFileEx` ABI and `Overlapped` layout.
        // `allow` not `expect`: the foreign-fn name `LockFileEx` is not flagged by
        // `non_snake_case` on the Windows toolchain, so `expect` would be unfulfilled
        // and warn on every build; `allow` documents the intentional API-matching name
        // without depending on whether the lint fires.
        #[allow(non_snake_case, reason = "FFI name matches Windows API")]
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
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(())
    }

    pub(super) fn try_lock_exclusive(file: &File) -> io::Result<bool> {
        use core::ptr;

        const LOCKFILE_EXCLUSIVE_LOCK: u32 = 0x0000_0002;
        // Non-blocking: fail immediately with ERROR_LOCK_VIOLATION instead of
        // waiting when the region is already locked.
        const LOCKFILE_FAIL_IMMEDIATELY: u32 = 0x0000_0001;
        // Windows system error code returned when the lock is already held.
        const ERROR_LOCK_VIOLATION: i32 = 33;

        // SAFETY: declaration matches the Windows `LockFileEx` ABI and `Overlapped` layout.
        #[allow(non_snake_case, reason = "FFI name matches Windows API")]
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
                LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
                0,
                u32::MAX,
                u32::MAX,
                &mut overlapped,
            )
        };

        if ret == 0 {
            let err = std::io::Error::last_os_error();
            // The "already locked" case is the expected contention signal and
            // maps to `Ok(false)`, mirroring the unix EWOULDBLOCK path.
            if err.raw_os_error() == Some(ERROR_LOCK_VIOLATION) {
                return Ok(false);
            }
            return Err(err);
        }
        Ok(true)
    }
}

#[cfg(not(any(unix, windows)))]
mod sys {
    use super::FileHint;
    use std::fs::File;
    use std::io;

    pub(super) fn lock_exclusive(_file: &File) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "file locking is not supported on this platform",
        ))
    }

    pub(super) fn try_lock_exclusive(_file: &File) -> io::Result<bool> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "file locking is not supported on this platform",
        ))
    }

    #[expect(
        clippy::unnecessary_wraps,
        reason = "FsFile::hint signature requires io::Result<()>; unsupported platforms have no fallible path"
    )]
    pub(super) fn fadvise(_file: &File, _hint: FileHint) -> io::Result<()> {
        // Unsupported platform - silently ignore. Hints are advisory.
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// macOS filesystem capability detection (statfs) + clonefile reflink
// ---------------------------------------------------------------------------

#[cfg(target_os = "macos")]
mod macos_caps {
    use crate::fs::FsCapabilities;
    use core::mem::MaybeUninit;
    use std::ffi::{CStr, CString};
    use std::io;
    use std::os::unix::ffi::OsStrExt;
    use std::path::Path;

    fn to_cstring(path: &Path) -> io::Result<CString> {
        CString::new(path.as_os_str().as_bytes()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "path contains an interior NUL byte",
            )
        })
    }

    /// Filesystem type name for the mount backing `path` (e.g. `"apfs"`),
    /// read from `statfs(2)`'s `f_fstypename`.
    fn fs_type_name(path: &Path) -> io::Result<String> {
        let c = to_cstring(path)?;
        let mut buf = MaybeUninit::<libc::statfs>::uninit();
        // SAFETY: `c` is a valid NUL-terminated C string; on success (rc == 0)
        // the kernel fully initializes the `statfs` buffer.
        #[expect(unsafe_code, reason = "statfs FFI to read the filesystem type name")]
        let rc = unsafe { libc::statfs(c.as_ptr(), buf.as_mut_ptr()) };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: statfs returned 0, so `buf` is initialized; `f_fstypename`
        // is a NUL-terminated `c_char` array.
        #[expect(unsafe_code, reason = "read initialized statfs.f_fstypename")]
        let name = unsafe {
            let st = buf.assume_init();
            CStr::from_ptr(st.f_fstypename.as_ptr())
                .to_string_lossy()
                .into_owned()
        };
        Ok(name)
    }

    /// APFS is the one common macOS filesystem with copy-on-write, reflink, and
    /// native snapshots. Detection failure falls back to the conservative
    /// default (capabilities are an optimization, never a correctness
    /// requirement).
    pub(super) fn capabilities(path: &Path) -> FsCapabilities {
        if matches!(fs_type_name(path).as_deref(), Ok("apfs")) {
            FsCapabilities {
                copy_on_write: true,
                reflink: true,
                native_snapshot: true,
                ..FsCapabilities::default()
            }
        } else {
            FsCapabilities::default()
        }
    }

    /// O(1) data clone via `clonefile(2)` (APFS block sharing).
    pub(super) fn clonefile(src: &Path, dst: &Path) -> io::Result<()> {
        let s = to_cstring(src)?;
        let d = to_cstring(dst)?;
        // SAFETY: both args are valid NUL-terminated C strings; flags = 0.
        #[expect(unsafe_code, reason = "clonefile FFI for an O(1) APFS clone")]
        let rc = unsafe { libc::clonefile(s.as_ptr(), d.as_ptr(), 0) };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    /// Whether a `clonefile` failure means "this filesystem / placement cannot
    /// clone" (so fall back to a byte copy) rather than a real I/O failure.
    pub(super) fn clone_should_fall_back(err: &io::Error) -> bool {
        // ENOTSUP/EOPNOTSUPP (45): non-APFS mount or clone unsupported.
        // EXDEV (18): cross-device - cannot clone across filesystems.
        const ENOTSUP: i32 = 45;
        const EXDEV: i32 = 18;
        matches!(err.raw_os_error(), Some(ENOTSUP | EXDEV))
            || matches!(
                err.kind(),
                io::ErrorKind::Unsupported | io::ErrorKind::CrossesDevices
            )
    }
}

// ---------------------------------------------------------------------------
// Linux filesystem capability detection (statfs) + FICLONE reflink + NoCoW
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
mod linux_caps {
    use crate::fs::FsCapabilities;
    use std::fs::{File, OpenOptions};
    use std::io;
    use std::os::unix::io::AsRawFd;
    use std::path::Path;

    // statfs-based capability detection only compiles on 64-bit (the magic
    // numbers exceed i32::MAX); these imports + helper feed only that path, so
    // gate them to avoid unused-import / dead_code warnings on 32-bit targets.
    #[cfg(target_pointer_width = "64")]
    use core::mem::MaybeUninit;
    #[cfg(target_pointer_width = "64")]
    use std::ffi::CString;
    #[cfg(target_pointer_width = "64")]
    use std::os::unix::ffi::OsStrExt;

    #[cfg(target_pointer_width = "64")]
    fn to_cstring(path: &Path) -> io::Result<CString> {
        CString::new(path.as_os_str().as_bytes()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "path contains an interior NUL byte",
            )
        })
    }

    /// Capabilities derived from `statfs(2)`'s `f_type` magic for the mount
    /// backing `path`. Btrfs / ZFS get the full profile; XFS gets copy-on-write
    /// plus reflink (reflink is only present on `reflink=1` volumes, which
    /// `ficlone` handles by falling back). Everything else (ext4, tmpfs,
    /// network) and any detection failure report the conservative default.
    ///
    /// Gated to 64-bit targets: `statfs.f_type` is `__fsword_t` (`c_long`),
    /// and the magic numbers exceed `i32::MAX`, so a clean comparison needs the
    /// 64-bit field. 32-bit Linux is not a storage-engine deployment target;
    /// it reports the conservative default.
    #[cfg(target_pointer_width = "64")]
    pub(super) fn capabilities(path: &Path) -> FsCapabilities {
        // statfs f_type magic numbers (stable kernel ABI, arch-independent).
        const BTRFS: i64 = 0x9123_683E;
        const ZFS: i64 = 0x2FC1_2FC1;
        const XFS: i64 = 0x5846_5342;
        const EXT_FAMILY: i64 = 0x0000_EF53; // ext2/3/4 share this magic
        const TMPFS: i64 = 0x0102_1994;

        let Ok(c) = to_cstring(path) else {
            return FsCapabilities::default();
        };
        let mut buf = MaybeUninit::<libc::statfs>::uninit();
        // SAFETY: `c` is a valid NUL-terminated C string; on success (rc == 0)
        // the kernel fully initializes the `statfs` buffer.
        #[expect(unsafe_code, reason = "statfs FFI to read f_type")]
        let rc = unsafe { libc::statfs(c.as_ptr(), buf.as_mut_ptr()) };
        if rc != 0 {
            return FsCapabilities::default();
        }
        // SAFETY: statfs returned 0, so `buf` is initialized. `f_type` is
        // `c_long` (i64) on glibc but `c_ulong` (u64) on musl, so normalize to
        // i64 before matching the magic constants (their bit patterns are
        // identical across the cast).
        #[expect(unsafe_code, reason = "read initialized statfs.f_type")]
        #[allow(
            clippy::unnecessary_cast,
            clippy::cast_possible_wrap,
            reason = "f_type is i64 on glibc (no-op cast) and u64 on musl (cast required)"
        )]
        let f_type = unsafe { buf.assume_init() }.f_type as i64;

        // fallocate(PUNCH_HOLE) is supported by every common local Linux
        // filesystem (ext4 / xfs / btrfs / zfs / tmpfs); leave it false for
        // unknown / network mounts so a runtime EOPNOTSUPP never surprises the
        // tight-space compaction gate.
        match f_type {
            BTRFS | ZFS => FsCapabilities {
                per_block_integrity_on_read: true,
                background_scrub: true,
                copy_on_write: true,
                reflink: true,
                native_snapshot: true,
                punch_hole: true,
            },
            XFS => FsCapabilities {
                copy_on_write: true,
                reflink: true,
                punch_hole: true,
                ..FsCapabilities::default()
            },
            EXT_FAMILY | TMPFS => FsCapabilities {
                punch_hole: true,
                ..FsCapabilities::default()
            },
            _ => FsCapabilities::default(),
        }
    }

    /// Deallocates `[offset, offset+len)` inside the file at `path` via
    /// `fallocate(FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE)`: frees the
    /// physical blocks, keeps the logical length, the hole reads back as zeros.
    ///
    /// 64-bit only: `off_t` is `i64` here, matching the `i64::try_from` range
    /// guard. 32-bit Linux reports `punch_hole = false` (see `capabilities`), so
    /// the mode never engages there; the stub below keeps the call site building.
    #[cfg(target_pointer_width = "64")]
    pub(super) fn punch_hole(path: &Path, offset: u64, len: u64) -> io::Result<()> {
        // off_t is i64 on 64-bit Linux; a file offset / length that exceeds
        // i64::MAX is not a real SST extent, so reject rather than wrap.
        let off = i64::try_from(offset)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "punch offset exceeds i64"))?;
        let length = i64::try_from(len)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "punch len exceeds i64"))?;
        let f = OpenOptions::new().write(true).open(path)?;
        // SAFETY: `f` owns a valid writable fd for the duration of the call;
        // the flags + range are plain integers.
        #[expect(
            unsafe_code,
            reason = "fallocate(PUNCH_HOLE) FFI for in-place extent reclaim"
        )]
        let rc = unsafe {
            libc::fallocate(
                f.as_raw_fd(),
                libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
                off,
                length,
            )
        };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// 32-bit Linux: `punch_hole` capability is reported false, so this is
    /// unreachable in practice; surface `Unsupported` to keep the contract.
    #[cfg(not(target_pointer_width = "64"))]
    pub(super) fn punch_hole(_path: &Path, _offset: u64, _len: u64) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "punch_hole unsupported on 32-bit",
        ))
    }

    /// 32-bit Linux: not a storage-engine deployment target; the `statfs`
    /// `f_type` magic comparison needs the 64-bit field, so report the
    /// conservative default.
    #[cfg(not(target_pointer_width = "64"))]
    pub(super) fn capabilities(_path: &Path) -> FsCapabilities {
        FsCapabilities::default()
    }

    /// O(1) data clone via `ioctl(FICLONE)`. Creates `dst` (failing if it
    /// exists), then clones `src` into it; on clone failure the empty `dst` is
    /// removed so a caller's copy fallback can re-create it.
    pub(super) fn ficlone(src: &Path, dst: &Path) -> io::Result<()> {
        let src_f = File::open(src)?;
        let dst_f = OpenOptions::new().write(true).create_new(true).open(dst)?;
        // SAFETY: FICLONE takes the source fd as its argument; both fds are
        // valid and owned by `src_f` / `dst_f` for the duration of the call.
        #[expect(unsafe_code, reason = "ioctl(FICLONE) for an O(1) reflink clone")]
        let rc = unsafe {
            libc::ioctl(
                dst_f.as_raw_fd(),
                // ioctl's request param is c_ulong on glibc but c_int on musl;
                // infer the target type so FICLONE casts to whichever applies.
                libc::FICLONE as _,
                src_f.as_raw_fd(),
            )
        };
        if rc != 0 {
            let err = std::io::Error::last_os_error();
            drop(dst_f);
            // Best-effort cleanup of the empty target so the copy fallback's
            // `create_new` does not trip over it.
            let _ = std::fs::remove_file(dst);
            return Err(err);
        }
        Ok(())
    }

    /// Whether a `ficlone` failure means "this filesystem / placement cannot
    /// reflink" (fall back to a byte copy) rather than a real I/O failure.
    pub(super) fn clone_should_fall_back(err: &io::Error) -> bool {
        // EOPNOTSUPP/ENOTSUP (95): FS has no reflink. EXDEV (18): cross-device.
        // EINVAL (22): kernel/FS rejected the clone (e.g. non-reflink XFS).
        const EOPNOTSUPP: i32 = 95;
        const EXDEV: i32 = 18;
        const EINVAL: i32 = 22;
        matches!(err.raw_os_error(), Some(EOPNOTSUPP | EXDEV | EINVAL))
            || matches!(
                err.kind(),
                io::ErrorKind::Unsupported | io::ErrorKind::CrossesDevices
            )
    }

    /// Clears per-file `CoW` (`FS_NOCOW_FL`) on the asm-generic ioctl
    /// architectures (the only ones where these constant values are correct).
    /// A no-op on other architectures and on filesystems without the
    /// inode-flags ioctl.
    #[cfg(any(
        target_arch = "x86",
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv32",
        target_arch = "riscv64",
        target_arch = "loongarch64",
        target_arch = "s390x"
    ))]
    pub(super) fn try_disable_cow(path: &Path) -> io::Result<()> {
        // asm-generic `_IO*` encodings of FS_IOC_{GET,SET}FLAGS (size = 8 =
        // sizeof(long)); FS_NOCOW_FL is the inode flag bit.
        const FS_IOC_GETFLAGS: libc::c_ulong = 0x8008_6601;
        const FS_IOC_SETFLAGS: libc::c_ulong = 0x4008_6602;
        const FS_NOCOW_FL: libc::c_int = 0x0080_0000;

        let f = OpenOptions::new().read(true).write(true).open(path)?;
        let mut flags: libc::c_int = 0;
        // SAFETY: valid fd; GETFLAGS writes one `int` through the pointer arg.
        #[expect(unsafe_code, reason = "ioctl(FS_IOC_GETFLAGS) to read inode flags")]
        #[allow(
            clippy::unnecessary_cast,
            clippy::cast_possible_truncation,
            reason = "ioctl request is c_ulong on glibc (no-op) but c_int on musl; \
                      the 32-bit request code's low bits are preserved by truncation"
        )]
        let rc = unsafe { libc::ioctl(f.as_raw_fd(), FS_IOC_GETFLAGS as _, &raw mut flags) };
        if rc != 0 {
            return ignore_if_unsupported(std::io::Error::last_os_error());
        }
        if flags & FS_NOCOW_FL != 0 {
            return Ok(()); // already NoCoW
        }
        flags |= FS_NOCOW_FL;
        // SAFETY: valid fd; SETFLAGS reads one `int` through the pointer arg.
        #[expect(unsafe_code, reason = "ioctl(FS_IOC_SETFLAGS) to set FS_NOCOW_FL")]
        #[allow(
            clippy::unnecessary_cast,
            clippy::cast_possible_truncation,
            reason = "ioctl request is c_ulong on glibc (no-op) but c_int on musl; \
                      the 32-bit request code's low bits are preserved by truncation"
        )]
        let rc = unsafe { libc::ioctl(f.as_raw_fd(), FS_IOC_SETFLAGS as _, &raw const flags) };
        if rc != 0 {
            return ignore_if_unsupported(std::io::Error::last_os_error());
        }
        Ok(())
    }

    #[cfg(not(any(
        target_arch = "x86",
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv32",
        target_arch = "riscv64",
        target_arch = "loongarch64",
        target_arch = "s390x"
    )))]
    pub(super) fn try_disable_cow(_path: &Path) -> io::Result<()> {
        // The FS_IOC_* constants above are only valid for the asm-generic ioctl
        // encoding; on other arches leave `CoW` alone (optimization, not
        // correctness).
        Ok(())
    }

    /// Maps "filesystem does not support the inode-flags ioctl" errors to a
    /// no-op success - disabling `CoW` is an optimization, never required.
    #[cfg(any(
        target_arch = "x86",
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv32",
        target_arch = "riscv64",
        target_arch = "loongarch64",
        target_arch = "s390x"
    ))]
    fn ignore_if_unsupported(err: io::Error) -> io::Result<()> {
        // ENOTTY (25): ioctl not supported by this FS. EOPNOTSUPP/ENOTSUP (95):
        // flag not supported. EPERM/EINVAL: FS rejected the flag.
        const ENOTTY: i32 = 25;
        const EOPNOTSUPP: i32 = 95;
        const EINVAL: i32 = 22;
        if matches!(err.raw_os_error(), Some(ENOTTY | EOPNOTSUPP | EINVAL))
            || matches!(err.kind(), io::ErrorKind::Unsupported)
        {
            Ok(())
        } else {
            Err(err)
        }
    }
}

// ---------------------------------------------------------------------------
// Filesystem free-space probe
// ---------------------------------------------------------------------------

#[cfg(windows)]
mod available_space_sys {
    use std::io;
    use std::os::windows::ffi::OsStrExt;
    use std::path::Path;

    // GetDiskFreeSpaceExW: free bytes available to the calling user on the
    // volume containing `path`.
    //
    // SAFETY (ABI): the signature matches the Win32 `GetDiskFreeSpaceExW`
    // contract — a wide directory-name pointer plus three optional `u64` out
    // pointers, returning a non-zero `BOOL` on success. Edition 2024 requires
    // foreign declarations in an `unsafe extern` block.
    #[allow(non_snake_case, reason = "Win32 API signature")]
    unsafe extern "system" {
        fn GetDiskFreeSpaceExW(
            lpDirectoryName: *const u16,
            lpFreeBytesAvailableToCaller: *mut u64,
            lpTotalNumberOfBytes: *mut u64,
            lpTotalNumberOfFreeBytes: *mut u64,
        ) -> i32;
    }

    pub(super) fn disk_free_available(path: &Path) -> io::Result<u64> {
        // NUL-terminated wide string of the path. Reject an interior NUL first:
        // Win32 treats the first NUL as the string terminator, so a path with an
        // embedded NUL would silently probe a truncated path (a different volume)
        // and feed admission / storage_stats free space for the wrong filesystem.
        // Mirrors the unix statvfs helper's `CString::new` rejection.
        let mut wide: Vec<u16> = path.as_os_str().encode_wide().collect();
        if wide.contains(&0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "path contains an interior NUL byte",
            ));
        }
        wide.push(0);
        let mut avail: u64 = 0;
        // SAFETY: `wide` is a valid NUL-terminated UTF-16 string; the three out
        // pointers are valid for the call; we read `avail` only on success.
        #[expect(unsafe_code, reason = "GetDiskFreeSpaceExW FFI for free space")]
        let rc = unsafe {
            GetDiskFreeSpaceExW(
                wide.as_ptr(),
                &mut avail,
                core::ptr::null_mut(),
                core::ptr::null_mut(),
            )
        };
        if rc == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(avail)
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

    /// Linux: `fallocate(PUNCH_HOLE)` frees a mid-file range and reads it back
    /// as zeros, leaving the logical length unchanged. Skips cleanly on a mount
    /// that does not advertise the capability (e.g. overlayfs in CI), so the
    /// test never fails on an unsupported filesystem.
    #[cfg(target_os = "linux")]
    #[test]
    fn std_fs_punch_hole_zeroes_range_on_linux() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("f");
        std::fs::write(&path, vec![0xABu8; 16 * 1024])?;

        if !StdFs.capabilities(&path).punch_hole {
            return Ok(()); // mount without punch-hole support → nothing to assert
        }

        // Block-aligned range so the extent is actually deallocated.
        StdFs.punch_hole(&path, 4096, 4096)?;

        let data = std::fs::read(&path)?;
        assert_eq!(data.len(), 16 * 1024, "logical length unchanged");
        assert!(
            data.iter().skip(4096).take(4096).all(|&b| b == 0),
            "the punched range reads back as zeros"
        );
        assert!(
            data.iter().take(4096).all(|&b| b == 0xAB),
            "data before the hole is intact"
        );
        Ok(())
    }

    /// Off Linux, `StdFs` keeps the trait default: hole punching is unsupported
    /// and the capability is not advertised, so tight-space compaction never
    /// engages on those targets.
    #[cfg(not(target_os = "linux"))]
    #[test]
    fn std_fs_punch_hole_is_unsupported_off_linux() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("f");
        std::fs::write(&path, vec![0u8; 64])?;

        assert!(
            !StdFs.capabilities(&path).punch_hole,
            "punch-hole capability is not advertised off Linux"
        );
        let err = StdFs.punch_hole(&path, 0, 16).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
        Ok(())
    }

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

    #[cfg(unix)]
    #[test]
    fn std_fs_volume_id_is_shared_within_a_mount_and_none_for_missing_path() -> io::Result<()> {
        // `volume_id` is the `st_dev` of the mount: two directories under one
        // tempdir share it (one free-space pool), so the space gate combines
        // their budgets. A path that cannot be stat-ed yields `None`
        // (independence unproven → callers combine conservatively).
        let dir = tempfile::tempdir()?;
        let fs = StdFs;
        let a = dir.path().join("a");
        let b = dir.path().join("b");
        fs.create_dir_all(&a)?;
        fs.create_dir_all(&b)?;
        let id_a = fs.volume_id(&a);
        assert!(id_a.is_some(), "a real path reports its device id");
        assert_eq!(id_a, fs.volume_id(&b), "same mount → same volume id");
        assert_eq!(
            fs.volume_id(&dir.path().join("does-not-exist")),
            None,
            "an un-stat-able path is unproven, not falsely independent"
        );
        Ok(())
    }

    #[test]
    fn std_fs_sync_with_both_modes_persists() -> io::Result<()> {
        // Both durability modes must succeed and leave the bytes readable.
        // On macOS this exercises the plain-`fsync` (Normal) and
        // `F_FULLFSYNC` (Full) branches; elsewhere both are plain `fsync`.
        let dir = tempfile::tempdir()?;
        let fs = StdFs;
        for (name, mode) in [("normal", SyncMode::Normal), ("full", SyncMode::Full)] {
            let path = dir.path().join(name);
            let mut file = fs.open(&path, &FsOpenOptions::new().write(true).create(true))?;
            file.write_all(b"durable")?;
            file.sync_data_with(mode)?;
            file.sync_all_with(mode)?;
            drop(file);
            // Directory sync at both modes must also succeed.
            fs.sync_directory_with(dir.path(), mode)?;

            let mut file = fs.open(&path, &FsOpenOptions::new().read(true))?;
            let mut buf = String::new();
            file.read_to_string(&mut buf)?;
            assert_eq!(buf, "durable", "{name} mode lost data");
        }
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
        // the Fs trait definition - belongs in integration tests.
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
            // container mounts, restrictive mount options) - test
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
    /// associated types - enables simple `Arc<dyn Fs>` for per-level routing.
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
        // was a true hard link, not a copy. We only check this on Unix -
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

    #[test]
    fn is_cross_device_detects_exdev_and_kind_variants() {
        // Synthesise a raw EXDEV error - what Linux/macOS/BSDs return when
        // hard_link spans devices. `is_cross_device` must accept it so the
        // fallback copy path kicks in. Gated to Unix because errno 18 has
        // a different meaning on Windows (ERROR_NO_MORE_FILES) and the
        // `#[cfg(unix)]` branch in `is_cross_device` is never compiled on
        // non-Unix targets.
        #[cfg(unix)]
        {
            // Matches the `EXDEV` constant inside `is_cross_device`.
            const EXDEV: i32 = 18;
            // Raw EXDEV (no matching kind) is detected by the std-side helper.
            let exdev = std::io::Error::from_raw_os_error(EXDEV);
            assert!(is_std_cross_device(&exdev), "raw EXDEV must be recognised");
        }

        // ErrorKind::CrossesDevices is the modern stable variant (Rust 1.85+).
        let crosses = io::Error::from(io::ErrorKind::CrossesDevices);
        assert!(is_cross_device(&crosses));

        // ErrorKind::Unsupported covers Windows / exotic filesystems where
        // hard links are simply not implemented - also a cue to fall back.
        let unsupported = io::Error::from(io::ErrorKind::Unsupported);
        assert!(is_cross_device(&unsupported));

        // A garden-variety NotFound must NOT be misclassified - the caller
        // needs to surface that error verbatim, not silently copy.
        let notfound = io::Error::from(io::ErrorKind::NotFound);
        assert!(!is_cross_device(&notfound));
    }

    #[test]
    fn fadvise_accepts_every_hint_variant() -> io::Result<()> {
        // Smoke test: every FileHint variant produces Ok on every
        // platform our CI builds for. Linux exercises the real
        // posix_fadvise syscall; macOS / Windows fall through the
        // no-op branches. Failure of any variant is a regression in
        // the platform-specific glue, not in the hint contract
        // itself (which is advisory).
        let dir = tempfile::tempdir()?;
        let fs = StdFs;
        let path = dir.path().join("hint_smoke.bin");

        // Write some content so posix_fadvise has a non-empty file
        // to act on (POSIX_FADV_DONTNEED is a no-op on 0-byte files
        // on every kernel I've tested, but checking with content
        // catches more potential regressions).
        let opts = FsOpenOptions::new().write(true).create_new(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(&vec![0u8; 64 * 1024])?;
        file.sync_all()?;
        drop(file);

        let opts = FsOpenOptions::new().read(true);
        let file = fs.open(&path, &opts)?;
        for hint in [
            FileHint::Default,
            FileHint::Sequential,
            FileHint::Random,
            FileHint::WriteOnce,
        ] {
            file.hint(hint)?;
        }
        Ok(())
    }

    /// On macOS the capability profile for any real directory must be either
    /// the full APFS profile (copy-on-write + reflink + native snapshot - the
    /// common case, including CI runners and `/var/folders` temp dirs) or the
    /// conservative all-false default (non-APFS mount). Never a partial mix.
    #[cfg(target_os = "macos")]
    #[test]
    fn capabilities_macos_is_apfs_profile_or_conservative() {
        use crate::fs::FsCapabilities;
        let dir = tempfile::tempdir().unwrap();
        let caps = StdFs.capabilities(dir.path());
        let apfs = FsCapabilities {
            copy_on_write: true,
            reflink: true,
            native_snapshot: true,
            ..FsCapabilities::default()
        };
        assert!(
            caps == apfs || caps == FsCapabilities::default(),
            "unexpected macOS capability profile: {caps:?}"
        );
    }

    /// macOS `reflink_file` must produce an independent, byte-identical clone
    /// whether it went through `clonefile(2)` (APFS) or the byte-copy fallback:
    /// the clone matches the source, and a later overwrite of the source does
    /// not change the clone.
    #[cfg(target_os = "macos")]
    #[test]
    fn reflink_file_macos_produces_independent_identical_copy() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let src = dir.path().join("src.bin");
        let dst = dir.path().join("clone.bin");
        let fs = StdFs;

        let mut f = fs.open(&src, &FsOpenOptions::new().write(true).create_new(true))?;
        f.write_all(b"reflink-source-data")?;
        f.sync_all()?;
        drop(f);

        fs.reflink_file(&src, &dst)?;

        let mut buf = Vec::new();
        fs.open(&dst, &FsOpenOptions::new().read(true))?
            .read_to_end(&mut buf)?;
        assert_eq!(buf, b"reflink-source-data");

        // Overwrite the source; the clone must be unaffected (clonefile shares
        // blocks copy-on-write, the fallback is a separate file - either way
        // independent).
        let mut w = fs.open(&src, &FsOpenOptions::new().write(true).truncate(true))?;
        w.write_all(b"changed")?;
        w.sync_all()?;
        drop(w);

        let mut after = Vec::new();
        fs.open(&dst, &FsOpenOptions::new().read(true))?
            .read_to_end(&mut after)?;
        assert_eq!(
            after, b"reflink-source-data",
            "reflink clone must be independent"
        );
        Ok(())
    }

    /// Linux `reflink_file` must produce an independent, byte-identical clone
    /// on any filesystem: `ioctl(FICLONE)` on Btrfs / XFS-reflink, the byte-copy
    /// fallback on ext4 / tmpfs. Robust regardless of the CI runner's FS.
    #[cfg(target_os = "linux")]
    #[test]
    fn reflink_file_linux_produces_independent_identical_copy() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let src = dir.path().join("src.bin");
        let dst = dir.path().join("clone.bin");
        let fs = StdFs;

        let mut f = fs.open(&src, &FsOpenOptions::new().write(true).create_new(true))?;
        f.write_all(b"reflink-source-data")?;
        f.sync_all()?;
        drop(f);

        fs.reflink_file(&src, &dst)?;

        let mut buf = Vec::new();
        fs.open(&dst, &FsOpenOptions::new().read(true))?
            .read_to_end(&mut buf)?;
        assert_eq!(buf, b"reflink-source-data");

        let mut w = fs.open(&src, &FsOpenOptions::new().write(true).truncate(true))?;
        w.write_all(b"changed")?;
        w.sync_all()?;
        drop(w);

        let mut after = Vec::new();
        fs.open(&dst, &FsOpenOptions::new().read(true))?
            .read_to_end(&mut after)?;
        assert_eq!(
            after, b"reflink-source-data",
            "reflink clone must be independent"
        );
        Ok(())
    }

    /// Linux `try_disable_cow` must succeed on any filesystem: it sets
    /// `FS_NOCOW_FL` on Btrfs and is a graceful no-op elsewhere (the
    /// inode-flags ioctl is unsupported on tmpfs / ext4-without-the-flag), never
    /// surfacing an error for a missing optimization.
    #[cfg(target_os = "linux")]
    #[test]
    fn try_disable_cow_linux_succeeds_or_noops() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("sst.bin");
        let fs = StdFs;
        // Empty file, as at SST creation time (the flag only takes on an empty
        // file).
        fs.open(&path, &FsOpenOptions::new().write(true).create_new(true))?;
        fs.try_disable_cow(&path)?;
        Ok(())
    }
}

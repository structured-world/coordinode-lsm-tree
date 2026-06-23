// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

#[cfg(not(feature = "std"))]
use crate::io::Write;
use crate::path::Path;
use crate::{
    Slice,
    fs::{Fs, FsFile, SyncMode},
};
#[cfg(feature = "std")]
use std::io::Write;

// The trailing byte is bumped on every wire-format break of the block
// header. Pre-V5 readers see `4` and reject the header immediately
// (InvalidHeader) without trying to parse fields that have moved or
// changed size. V5 used `3`. The manifest format-version gate is the
// primary protection against version skew; this is the secondary
// defense at the block layer.
pub const MAGIC_BYTES: [u8; 4] = [b'L', b'S', b'M', 4];

pub const TABLES_FOLDER: &str = "tables";
pub const BLOBS_FOLDER: &str = "blobs";
pub const CURRENT_VERSION_FILE: &str = "current";

/// Reads bytes from a file at the given offset without changing the cursor.
///
/// Uses [`FsFile::read_at`] (equivalent to `pread(2)`) so multiple threads
/// can call this concurrently on the same file handle.
pub fn read_exact(file: &dyn FsFile, offset: u64, size: usize) -> crate::io::Result<Slice> {
    // SAFETY: This slice builder starts uninitialized, but we know its length
    //
    // We use FsFile::read_at which gives us the number of bytes read.
    // If that number does not match the slice length, the function errors,
    // so the (partially) uninitialized buffer is discarded.
    //
    // Additionally, generally, block loads furthermore do a checksum check which
    // would likely catch the buffer being wrong somehow.
    #[expect(unsafe_code, reason = "see safety")]
    let mut builder = unsafe { Slice::builder_unzeroed(size) };

    // Single call is correct: FsFile::read_at has fill-or-EOF semantics —
    // implementations handle EINTR/short-read retry internally.
    let bytes_read = file.read_at(&mut builder, offset)?;

    if bytes_read != size {
        return Err(crate::io::Error::new(
            crate::io::ErrorKind::UnexpectedEof,
            format!(
                "read_exact({bytes_read}) at {offset} did not read enough bytes {size}; file has length {}",
                file.metadata()?.len
            ),
        ));
    }

    Ok(builder.freeze().into())
}

/// Atomically rewrites a file via the [`Fs`] trait.
///
/// Writes `content` to a temporary file in the same directory, fsyncs it,
/// then renames over `path`. This ensures readers never see a partial write.
pub fn rewrite_atomic(
    path: &Path,
    content: &[u8],
    fs: &dyn Fs,
    mode: SyncMode,
) -> crate::io::Result<()> {
    use crate::fs::FsOpenOptions;
    use core::sync::atomic::Ordering;
    use portable_atomic::AtomicU64;

    static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

    #[expect(
        clippy::expect_used,
        reason = "every file should have a parent directory"
    )]
    let folder = path.parent().expect("should have a parent");

    // no-std: no process model — a fixed id is fine, the seq counter
    // disambiguates temp names within a process.
    #[cfg(feature = "std")]
    let pid = std::process::id();
    #[cfg(not(feature = "std"))]
    let pid = 0u32;

    // Retry with incrementing seq on AlreadyExists — handles leftover temp
    // files from a previous crash (PID can be reused, especially in containers).
    let tmp_path = loop {
        let seq = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let candidate = folder.join(format!(".tmp_{pid}_{seq}"));
        match fs.open(
            &candidate,
            &FsOpenOptions::new().write(true).create_new(true),
        ) {
            Ok(mut file) => {
                let write_result = file
                    .write_all(content)
                    .map_err(crate::io::Error::from)
                    .and_then(|()| file.flush().map_err(crate::io::Error::from))
                    .and_then(|()| FsFile::sync_all_with(&*file, mode));
                if let Err(e) = write_result {
                    drop(file);
                    let _ = fs.remove_file(&candidate);
                    return Err(e);
                }
                break candidate;
            }
            // Leftover temp file from a previous crash — retry with next seq.
            Err(e) if e.kind() == crate::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e),
        }
    };

    // std::fs::rename overwrites existing destinations on all platforms
    // (Rust uses MoveFileExW with MOVEFILE_REPLACE_EXISTING on Windows).
    if let Err(e) = fs.rename(&tmp_path, path) {
        let _ = fs.remove_file(&tmp_path);
        return Err(e);
    }
    fsync_directory(folder, fs, mode)?;

    Ok(())
}

/// Delegates directory sync to the backend.
///
/// On Windows, `StdFs::sync_directory` already returns `Ok(())` (directory
/// fsync is unsupported), but non-`StdFs` backends (e.g., `MemFs`) may use
/// this call for path validation. Always delegate rather than short-circuiting.
pub fn fsync_directory(path: &Path, fs: &dyn Fs, mode: SyncMode) -> crate::io::Result<()> {
    fs.sync_directory_with(path, mode)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::useless_vec,
    reason = "test code"
)]
mod tests;

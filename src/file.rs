// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    Slice,
    fs::{Fs, FsFile},
};
use std::{io::Write, path::Path};

pub const MAGIC_BYTES: [u8; 4] = [b'L', b'S', b'M', 3];

pub const TABLES_FOLDER: &str = "tables";
pub const BLOBS_FOLDER: &str = "blobs";
pub const CURRENT_VERSION_FILE: &str = "current";

/// Reads bytes from a file at the given offset without changing the cursor.
///
/// Uses [`FsFile::read_at`] (equivalent to `pread(2)`) so multiple threads
/// can call this concurrently on the same file handle.
pub fn read_exact(file: &dyn FsFile, offset: u64, size: usize) -> std::io::Result<Slice> {
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
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
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
pub fn rewrite_atomic(path: &Path, content: &[u8], fs: &dyn Fs) -> std::io::Result<()> {
    use crate::fs::FsOpenOptions;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

    #[expect(
        clippy::expect_used,
        reason = "every file should have a parent directory"
    )]
    let folder = path.parent().expect("should have a parent");

    // PID + monotonic seq gives uniqueness within a process and across
    // concurrent processes. A crash-then-PID-reuse collision is theoretically
    // possible but vanishingly unlikely (requires exact PID reuse AND seq
    // counter restart to same value). lsm-tree uses exclusive file locking
    // so the same data directory is never written by two processes.
    let seq = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let tmp_path = folder.join(format!(".tmp_{pid}_{seq}"));

    let result = (|| -> std::io::Result<()> {
        let mut file = fs.open(
            &tmp_path,
            &FsOpenOptions::new().write(true).create_new(true),
        )?;
        file.write_all(content)?;
        file.flush()?;
        FsFile::sync_all(&*file)?;
        drop(file);
        // std::fs::rename overwrites existing destinations on all platforms
        // (Rust uses MoveFileExW with MOVEFILE_REPLACE_EXISTING on Windows).
        fs.rename(&tmp_path, path)?;
        Ok(())
    })();

    if result.is_err() {
        // Best-effort cleanup of the temp file on any failure path.
        // Safe to call even if fs.open() failed (file never created) —
        // remove_file will return NotFound which we ignore.
        let _ = fs.remove_file(&tmp_path);
    }
    result?;
    fsync_directory(folder, fs)?;

    Ok(())
}

#[cfg(not(target_os = "windows"))]
pub fn fsync_directory(path: &Path, fs: &dyn Fs) -> std::io::Result<()> {
    fs.sync_directory(path)
}

#[cfg(target_os = "windows")]
pub fn fsync_directory(_path: &Path, _fs: &dyn Fs) -> std::io::Result<()> {
    // Cannot fsync directory on Windows
    Ok(())
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::useless_vec,
    reason = "test code"
)]
mod tests {
    use super::*;
    use crate::fs::StdFs;
    use std::fs::File;
    use std::io::Write;
    use test_log::test;

    #[test]
    fn read_exact_short_read_returns_error() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("short.bin");
        {
            let mut f = File::create(&path)?;
            f.write_all(b"hello")?; // 5 bytes
        }

        let file = File::open(&path)?;
        // Request 10 bytes from a 5-byte file → short read → UnexpectedEof
        let err = read_exact(&file, 0, 10).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);

        Ok(())
    }

    #[test]
    fn atomic_rewrite() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let path = dir.path().join("test.txt");
        {
            let mut file = File::create(&path)?;
            write!(file, "asdasdasdasdasd")?;
        }

        rewrite_atomic(&path, b"newcontent", &StdFs)?;

        let content = std::fs::read_to_string(&path)?;
        assert_eq!("newcontent", content);

        Ok(())
    }
}

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

/// Suffix of a table replacement a manifest repair has not published yet.
///
/// A repair builds the replacement under this name until the manifest that
/// adopts it is durable. Recognized here rather than in `repair` so an open
/// without that (std-only) module still classifies the name instead of failing
/// on it.
pub const REPAIR_TMP_SUFFIX: &str = ".repair-tmp";

/// The table id a `{id}.repair-tmp` name claims, or `None` for any other name.
///
/// Only the EXACT shape is owned recovery state. A foreign name that merely
/// contains the suffix (an operator's `5.repair-tmp.backup`) is not a temp and
/// must never be swept or swapped as one.
#[must_use]
pub fn table_id_from_repair_tmp_name(file_name: &str) -> Option<crate::TableId> {
    file_name
        .strip_suffix(REPAIR_TMP_SUFFIX)
        .and_then(|id| id.parse::<crate::TableId>().ok())
}

/// What a directory entry in a `tables/` folder IS — the one grammar every
/// scanner classifies names against.
///
/// Both `Tree::open`'s recovery sweep and manifest repair's table scan walk the
/// same directory and must agree on which names the engine OWNS: a kind added to
/// one scanner but not the other is a file one path deletes while the other
/// depends on it. The grammar therefore lives here, once; what each scanner DOES
/// with a kind (sweep, preserve, adopt, reject) stays its own policy.
///
/// Ownership is exact-shape: every id (and the healtmp sequence) must parse as a
/// number, so a foreign name that merely contains a suffix (an operator's
/// `5.heal-attest.backup`) classifies as [`Self::Foreign`] and is never treated
/// as engine state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableDirEntry {
    /// `{id}` — a table file.
    Table(crate::TableId),
    /// `{id}.heal-attest` — an in-place heal corrected `{id}` but its manifest
    /// digest refresh may not have landed; the next scrub reconciles through it.
    HealAttest(crate::TableId),
    /// `{id}.heal-attest.tmp` — a crashed attestation publish (written + synced
    /// for an atomic rename that never ran). Disposable.
    HealAttestTmp(crate::TableId),
    /// `{id}.healtmp-{n}` — an in-place heal's detach copy, renamed over the
    /// live path on success. A survivor is never referenced. Disposable.
    HealTmp(crate::TableId),
    /// `{id}.restrict-bound` — the exact tight-space restriction bound of a
    /// hole-punched `{id}`, read by manifest repair.
    RestrictBound(crate::TableId),
    /// `{id}.restrict-bound.tmp` — a crashed bound publish. Disposable.
    RestrictBoundTmp(crate::TableId),
    /// `{id}.repair-tmp` — a repair's unpublished replacement for `{id}`; see
    /// [`REPAIR_TMP_SUFFIX`].
    RepairTmp(crate::TableId),
    /// `{id}.repair-tmp.restrict-bound` (or its `.tmp`) — the restriction
    /// sidecar a restricted salvage wrote beside its replacement. Its fate
    /// follows the temp's: a committed swap renames it into place, an
    /// abandoned build's companion is disposable.
    RepairTmpCompanion(crate::TableId),
    /// None of the shapes the engine owns.
    Foreign,
}

impl TableDirEntry {
    /// Classifies a file name in a `tables/` folder.
    ///
    /// Longer suffixes are matched before their prefixes (`.heal-attest.tmp`
    /// before `.heal-attest`, `.restrict-bound.tmp` before `.restrict-bound`),
    /// so a temp can never classify as its live sidecar.
    #[must_use]
    pub fn classify(file_name: &str) -> Self {
        let owned_id = |rest: &str, make: fn(crate::TableId) -> Self| {
            rest.parse::<crate::TableId>().map_or(Self::Foreign, make)
        };
        if let Some(rest) = file_name.strip_suffix(".heal-attest.tmp") {
            return owned_id(rest, Self::HealAttestTmp);
        }
        if let Some(rest) = file_name.strip_suffix(".heal-attest") {
            return owned_id(rest, Self::HealAttest);
        }
        if let Some((id, seq)) = file_name.split_once(".healtmp-") {
            // BOTH halves must parse: `5.healtmp-backup` is not owned.
            if seq.parse::<u64>().is_ok() {
                return owned_id(id, Self::HealTmp);
            }
            return Self::Foreign;
        }
        if let Some(rest) = file_name.strip_suffix(".restrict-bound.tmp") {
            if let Some(temp_owner) = rest.strip_suffix(REPAIR_TMP_SUFFIX) {
                return owned_id(temp_owner, Self::RepairTmpCompanion);
            }
            return owned_id(rest, Self::RestrictBoundTmp);
        }
        if let Some(rest) = file_name.strip_suffix(".restrict-bound") {
            if let Some(temp_owner) = rest.strip_suffix(REPAIR_TMP_SUFFIX) {
                return owned_id(temp_owner, Self::RepairTmpCompanion);
            }
            return owned_id(rest, Self::RestrictBound);
        }
        if let Some(rest) = file_name.strip_suffix(REPAIR_TMP_SUFFIX) {
            return owned_id(rest, Self::RepairTmp);
        }
        file_name
            .parse::<crate::TableId>()
            .map_or(Self::Foreign, Self::Table)
    }
}

/// The bytes `path` occupies for accounting: its length MINUS the holes
/// punched out of it.
///
/// A tight-space compaction punches the consumed prefix out of its input, which
/// leaves `len` reporting the original size while those blocks are gone from
/// the device; charging the length would keep the freed bytes on the quota
/// forever. The allocation is the lower of the two only when a hole exists:
/// a normal file's allocation is ROUNDED UP to a block, and adopting that would
/// inflate every file in the tree by up to a block for no gain. So take the
/// smaller, which is the length for an intact file and the live extents for a
/// punched one. A backend that cannot report allocation answers `None` (it also
/// never punches), where the length is exact.
///
/// Every accounting surface measures through this one rule: `storage_stats` and
/// the checkpoint totals are asserted equal, so a second spelling of "how big is
/// this file" is a divergence waiting to happen.
///
/// # Errors
///
/// Propagates the stat failures of `path`.
pub(crate) fn on_disk_bytes(fs: &dyn Fs, path: &Path) -> crate::io::Result<u64> {
    let len = fs.metadata(path)?.len;
    Ok(match fs.allocated_size(path)? {
        Some(allocated) => allocated.min(len),
        None => len,
    })
}

/// Streams `path` from byte `start` to end through XXH3-128, splicing
/// `overrides` over the bytes on disk as it goes.
///
/// `start == 0` with no overrides reproduces the whole-file digest a normal
/// write accumulates; a non-zero `start` digests only the LIVE SUFFIX of a
/// hole-punched file, which is what the manifest records for a restricted SST
/// or a punched blob. Each override replaces the bytes at its offset, so a
/// caller can predict the digest a pending in-place repair would produce
/// without writing it first.
///
/// Lives here rather than beside the repair that grew it: the blob open path
/// needs the same digest to tell two directory entries of one id apart, and
/// that path compiles without `std`.
///
/// # Errors
///
/// Propagates the open / read failures of `path`.
pub(crate) fn checksum_from_with_overrides(
    fs: &dyn Fs,
    path: &Path,
    start: u64,
    overrides: &[(u64, alloc::vec::Vec<u8>)],
) -> crate::Result<u128> {
    // `FsFile` inherits whichever `Read`/`Seek` the build has: std's under the
    // `std` feature, the crate's own under `no_std`.
    #[cfg(not(feature = "std"))]
    use crate::io::{Read, Seek, SeekFrom};
    #[cfg(feature = "std")]
    use std::io::{Read, Seek, SeekFrom};

    let mut file = fs.open(path, &crate::fs::FsOpenOptions::new().read(true))?;
    // Seek + sequential read: keeps the `start == 0` read pattern identical to
    // the plain whole-file digest, so a restricted file is not read through a
    // different access pattern than an unrestricted one.
    if start != 0 {
        file.seek(SeekFrom::Start(start))?;
    }
    let mut hasher = xxhash_rust::xxh3::Xxh3Default::new();
    let mut buf = alloc::vec![0u8; 256 * 1024];
    let mut chunk_start = start;
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break; // EOF
        }
        let chunk_end = chunk_start + n as u64;
        let Some(chunk) = buf.get_mut(..n) else { break };
        // Splice every override overlapping this chunk. Overrides are few (one
        // per corrupt block), so scanning them per chunk is negligible.
        for (off, bytes) in overrides {
            let ov_end = *off + bytes.len() as u64;
            let lo = (*off).max(chunk_start);
            let hi = ov_end.min(chunk_end);
            // Skip a non-overlapping override BEFORE computing relative offsets:
            // the bound subtractions below are unsigned, and an override ending
            // before this chunk (or starting after it) would otherwise underflow
            // (a debug panic). Once `lo < hi` holds, `chunk_start <= lo < hi <=
            // chunk_end` and `off <= lo < hi <= ov_end`, so all four differences
            // are non-negative.
            if lo >= hi {
                continue;
            }
            // The overlap lies inside a `<= 256 KiB` chunk, so every difference
            // fits `usize`; `try_from` handles the 32-bit target without a cast.
            let (Ok(dst_lo), Ok(dst_hi), Ok(src_lo), Ok(src_hi)) = (
                usize::try_from(lo - chunk_start),
                usize::try_from(hi - chunk_start),
                usize::try_from(lo - *off),
                usize::try_from(hi - *off),
            ) else {
                continue;
            };
            if let (Some(dst), Some(src)) =
                (chunk.get_mut(dst_lo..dst_hi), bytes.get(src_lo..src_hi))
            {
                dst.copy_from_slice(src);
            }
        }
        hasher.update(&*chunk);
        chunk_start = chunk_end;
    }
    Ok(hasher.digest128())
}

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

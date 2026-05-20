// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Implementation of [`Tree::create_checkpoint`](crate::Tree::create_checkpoint)
//! and [`BlobTree::create_checkpoint`](crate::BlobTree::create_checkpoint).
//!
//! A checkpoint is a hard-linked, fully-functional snapshot of the tree's
//! on-disk state. It can be opened independently via
//! [`Config::open`](crate::Config::open) without affecting the source tree.
//!
//! The algorithm mirrors `RocksDB`'s `Checkpoint::CreateCheckpoint`:
//!
//! 1. Acquire a [`Pause`](crate::deletion_pause::Pause) on the source tree's
//!    deletion gate. Compaction continues, but obsolete files queued for
//!    removal are held back until the checkpoint is complete.
//! 2. Flush the active memtable so all live data is in SSTs.
//! 3. Snapshot the current `Version`; iterate its tables (and blob files,
//!    for [`BlobTree`]) and hard-link each one into `target/tables/` (or
//!    `target/blobs/`).
//! 4. Copy the manifest, version file (`v<id>`), and `current` pointer.
//! 5. Drop the pause guard — queued deletions run.

// `Path`, `io::{Read, Write}` and `io::copy` come from `std::*` because
// no `core` / `alloc` equivalents exist; they are also the same types
// the underlying `Fs` trait operates on, so this module inherits its
// host `fs` module's std dependency rather than introducing a new one.
use crate::{
    AbstractTree, CheckpointInfo, SeqNo,
    file::{BLOBS_FOLDER, CURRENT_VERSION_FILE, TABLES_FOLDER, fsync_directory},
    fs::{Fs, FsFile, FsOpenOptions},
    version::Version,
    vlog::BlobFile,
};
use alloc::{sync::Arc, vec};
use std::{
    io::{Read, Write},
    path::Path,
};

/// Internal helper: returns the byte-name used inside the checkpoint
/// directory for a given table ID.
fn table_link_name(id: crate::TableId) -> String {
    id.to_string()
}

/// Internal helper: returns the byte-name used inside the checkpoint
/// directory for a given blob-file ID.
fn blob_link_name(id: crate::vlog::BlobFileId) -> String {
    id.to_string()
}

/// Creates the directory structure for a fresh checkpoint.
///
/// Uses the atomic [`Fs::create_dir`] primitive (POSIX `mkdir(2)`) to
/// claim the target directory: two concurrent callers race the kernel
/// and the losing one observes [`std::io::ErrorKind::AlreadyExists`].
/// This replaces an earlier `exists()` + `create_dir_all()` sequence
/// that had a TOCTOU window between the two calls.
///
/// Once the leaf directory is ours, the `tables/` and (optionally)
/// `blobs/` subdirectories are created. If any of those secondary
/// creates fails, the freshly-claimed root directory is removed before
/// the error is returned so the caller can retry against the same path
/// — leaving `target` behind would lock out the next attempt with
/// `AlreadyExists` and contradict the "partial cleanup" contract.
///
/// The caller's parent path must exist; this function does not recurse.
pub fn prepare_target(target: &Path, include_blobs: bool, target_fs: &dyn Fs) -> crate::Result<()> {
    // Atomic claim — fails with AlreadyExists if any other process /
    // thread / prior checkpoint already created the directory.
    target_fs.create_dir(target).map_err(|e| {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!(
                    "checkpoint target {} already exists; refusing to overwrite",
                    target.display(),
                ),
            )
        } else {
            e
        }
    })?;

    // From this point on, the root directory is ours — any failure must
    // undo it so retries against the same path work. Local RAII guard
    // (defined at module scope to avoid `items_after_statements`).
    let mut cleanup = RootCleanup {
        target,
        fs: target_fs,
        armed: true,
    };

    target_fs.create_dir(&target.join(TABLES_FOLDER))?;
    if include_blobs {
        target_fs.create_dir(&target.join(BLOBS_FOLDER))?;
    }

    cleanup.armed = false;
    Ok(())
}

/// Internal RAII guard used by [`prepare_target`] to undo a successful
/// `create_dir(target)` when a subsequent subdirectory create fails.
struct RootCleanup<'a> {
    target: &'a Path,
    fs: &'a dyn Fs,
    armed: bool,
}

impl Drop for RootCleanup<'_> {
    fn drop(&mut self) {
        if self.armed
            && let Err(e) = self.fs.remove_dir_all(self.target)
        {
            log::warn!(
                "Failed to clean up partial checkpoint target {}: {e:?}",
                self.target.display(),
            );
        }
    }
}

/// Links (or copies) one file across [`Fs`] backends.
///
/// Strategy:
///
/// 1. **Try `dst_fs.hard_link(src, dst)` first.** A real filesystem
///    backend that can see `src` (same kernel filesystem, just a
///    different `Arc<dyn Fs>` handle — common when `level_routes`
///    builds `Arc::new(StdFs)` independently from `config.fs`) will
///    succeed in O(1) without doubling disk usage. `StdFs::hard_link`
///    handles its own `EXDEV` → byte-copy fallback transparently.
/// 2. **On `NotFound`** (the dst backend doesn't see `src` at all —
///    e.g. `MemFs` target with `StdFs` source) **or `Unsupported`**
///    (in-memory backends that don't implement linking), stream bytes
///    through both trait objects. This is the only path that doubles
///    storage. The fallback itself is silent here (the [`StdFs`] EXDEV
///    fallback emits one [`log::debug`] per file); operator-visible
///    notification of unexpected copies is the checkpoint driver's
///    responsibility — a per-file warning would drown real signal on
///    a misconfigured tier with thousands of SSTs.
///
/// Using `Arc::ptr_eq` as the discriminator would be too strict: two
/// `Arc::new(StdFs)` values produced independently (e.g. one in the
/// tree's primary `config.fs`, one in a `LevelRoute`) are not pointer-
/// equal but back the same kernel filesystem, so they CAN hard-link.
/// The "try first, fall back on demand" pattern catches both cases
/// without exposing a backend-identity API on the `Fs` trait.
pub fn link_or_copy_cross_fs(
    src_fs: &Arc<dyn Fs>,
    src: &Path,
    dst_fs: &Arc<dyn Fs>,
    dst: &Path,
) -> std::io::Result<u64> {
    match dst_fs.hard_link(src, dst) {
        Ok(()) => return Ok(dst_fs.metadata(dst)?.len),
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::Unsupported
            ) =>
        {
            // dst_fs cannot see src (cross-backend) or does not support
            // hard links at all → fall through to streamed copy.
        }
        Err(e) => return Err(e),
    }

    // Cross-backend / no-hardlink path — stream bytes through the trait.
    // The buffer is heap-allocated to avoid bloating the stack frame;
    // checkpoint is a cold-path operation so the extra allocation is
    // negligible.
    let mut src_file = src_fs.open(src, &FsOpenOptions::new().read(true))?;
    let mut dst_file = dst_fs.open(dst, &FsOpenOptions::new().write(true).create_new(true))?;

    let mut buf = vec![0u8; 64 * 1024].into_boxed_slice();
    let mut total: u64 = 0;
    loop {
        // Retry on EINTR — matches `StdFs::copy_fallback` and avoids
        // spurious checkpoint failures when a signal arrives during the
        // copy (common under shell-managed Ctrl-C handlers).
        let n = match src_file.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        #[expect(
            clippy::indexing_slicing,
            reason = "n was just produced by read() and is bounded by buf.len()"
        )]
        dst_file.write_all(&buf[..n])?;
        total = total.saturating_add(n as u64);
    }
    dst_file.flush()?;
    FsFile::sync_all(&*dst_file)?;
    Ok(total)
}

/// Hard-links every live SST in `version` into `target/tables/`.
///
/// Returns `(count, total_bytes)`. Tables on routed tiers
/// (`level_routes`) keep their original storage backend on the source
/// side; the destination is always the checkpoint's primary [`Fs`].
pub fn link_tables(
    version: &Version,
    target_root: &Path,
    target_fs: &Arc<dyn Fs>,
) -> crate::Result<(usize, u64)> {
    let tables_dir = target_root.join(TABLES_FOLDER);
    let mut count = 0usize;
    let mut bytes: u64 = 0;

    for table in version.iter_tables() {
        let dst = tables_dir.join(table_link_name(table.id()));

        // Source Fs may differ from `target_fs` when `level_routes` points
        // a hot tier at one backend (e.g. tmpfs) and the rest of the tree
        // at another. `link_or_copy_cross_fs` picks the right strategy.
        let written = link_or_copy_cross_fs(&table.fs, &table.path, target_fs, &dst)
            .map_err(crate::Error::from)?;
        bytes = bytes.saturating_add(written);
        count = count.saturating_add(1);
    }
    Ok((count, bytes))
}

/// Hard-links every live blob file in `version` into `target/blobs/`.
///
/// Returns `(count, total_bytes)`. Blob files always live under the
/// tree's primary path (no per-level routing today), so the source `Fs`
/// is `target_fs`'s counterpart on the source tree.
pub fn link_blob_files(
    blob_files: impl IntoIterator<Item = BlobFile>,
    target_root: &Path,
    target_fs: &Arc<dyn Fs>,
) -> crate::Result<(usize, u64)> {
    let blobs_dir = target_root.join(BLOBS_FOLDER);
    let mut count = 0usize;
    let mut bytes: u64 = 0;

    for blob in blob_files {
        let dst = blobs_dir.join(blob_link_name(blob.id()));
        let written = link_or_copy_cross_fs(&blob.0.fs, &blob.0.path, target_fs, &dst)
            .map_err(crate::Error::from)?;
        bytes = bytes.saturating_add(written);
        count = count.saturating_add(1);
    }
    Ok((count, bytes))
}

/// Whether a metadata file is required for the checkpoint to be openable.
#[derive(Clone, Copy)]
enum MetaRequirement {
    /// File must exist on the source; absence is an error.
    ///
    /// Used for the `v<id>` snapshot whose disappearance between
    /// `current_version()` and metadata copy would otherwise produce a
    /// checkpoint that points (via `current`) at a version file that
    /// does not exist in the checkpoint directory.
    Required,
    /// File may legitimately be absent (treated as a freshly-initialised
    /// tree by recovery). Used for `manifest` on never-written trees.
    Optional,
}

/// Copies one of the small metadata files (manifest, `v<id>`, or
/// `current`) from `src_root` to `target_root`.
///
/// Opens the source directly instead of `exists()` + `open()` to avoid
/// the TOCTOU window where the file disappears between the two calls.
/// `NotFound` is the only ignorable error and only when
/// `requirement == Optional`; for `Required` files a missing source is
/// surfaced as the original `NotFound` so the checkpoint fails-fast
/// instead of producing an unopenable snapshot.
fn copy_metadata_file(
    src_fs: &dyn Fs,
    src_root: &Path,
    target_fs: &dyn Fs,
    target_root: &Path,
    file_name: &str,
    requirement: MetaRequirement,
) -> crate::Result<()> {
    let src = src_root.join(file_name);
    let mut src_file = match src_fs.open(&src, &FsOpenOptions::new().read(true)) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return match requirement {
                MetaRequirement::Optional => Ok(()),
                MetaRequirement::Required => Err(crate::Error::from(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!(
                        "checkpoint required metadata file {} missing from source",
                        src.display(),
                    ),
                ))),
            };
        }
        Err(e) => return Err(e.into()),
    };
    let dst = target_root.join(file_name);
    let mut dst_file = target_fs.open(&dst, &FsOpenOptions::new().write(true).create_new(true))?;

    std::io::copy(&mut src_file, &mut dst_file)?;
    dst_file.flush()?;
    FsFile::sync_all(&*dst_file)?;
    Ok(())
}

/// Writes the checkpoint's `current` pointer for the captured
/// `version_id`.
///
/// The original source-tree `CURRENT` may have advanced concurrently
/// between when we captured `version` and when we get here — copying it
/// verbatim would risk pointing the checkpoint at a `v<N+1>` that we
/// never linked. Instead we write a fresh `current` file in the same
/// wire format as [`crate::version::persist_version`]: `u64 version_id`
/// + `u128 checksum` + `u8 checksum_type`.
///
/// The checksum field is intentionally written as zero: recovery's
/// [`crate::version::recovery`] reads it for forward-compatibility but
/// does not validate it against the `v<id>` file's contents, so any
/// value works. The zero is a deliberate "no checksum carried" sentinel,
/// not an attempt to forge a real digest.
fn write_current_for_version(
    target_fs: &dyn Fs,
    target_root: &Path,
    version_id: u64,
) -> crate::Result<()> {
    use crate::file::rewrite_atomic;
    use byteorder::{LittleEndian, WriteBytesExt};

    let mut content = vec![];
    content.write_u64::<LittleEndian>(version_id)?;
    content.write_u128::<LittleEndian>(0)?; // checksum (not validated on read)
    content.write_u8(0)?; // checksum_type = 0 (xxh3)

    rewrite_atomic(&target_root.join(CURRENT_VERSION_FILE), &content, target_fs)?;
    Ok(())
}

/// Replicates manifest + `v<id>` + writes a fresh `current` pointer.
///
/// Pulls the manifest and the active version snapshot (`v<id>`) from
/// the source tree into the checkpoint, then writes a fresh `current`
/// pointer for the captured version (see [`write_current_for_version`]
/// for why we don't copy the live file).
///
/// `version_id` is the ID of the version captured at checkpoint time; the
/// corresponding `v<id>` file MUST exist on the source because checkpoint
/// is called after a flush that publishes a new version through
/// [`crate::version::persist_version`].
pub fn copy_metadata(
    src_fs: &dyn Fs,
    src_root: &Path,
    target_fs: &dyn Fs,
    target_root: &Path,
    version_id: u64,
) -> crate::Result<()> {
    // Manifest stores level count + comparator name. On a never-written
    // tree the manifest may legitimately be absent (recovery treats
    // missing manifest as a freshly-initialised tree), so this is Optional.
    copy_metadata_file(
        src_fs,
        src_root,
        target_fs,
        target_root,
        "manifest",
        MetaRequirement::Optional,
    )?;
    // Active version snapshot — `v<id>` is immutable once published, so
    // copying it verbatim is race-free against concurrent version
    // transitions. But version-GC can DELETE older `v<id>` files; if the
    // file we captured disappears between `current_version()` and this
    // copy, the resulting checkpoint would have `current` pointing at a
    // missing file and fail to open. Treat as Required so checkpoint
    // fails-fast in that race instead of producing a corrupt snapshot.
    copy_metadata_file(
        src_fs,
        src_root,
        target_fs,
        target_root,
        &format!("v{version_id}"),
        MetaRequirement::Required,
    )?;
    // CURRENT pointer is generated fresh for the captured `version_id`
    // (NOT copied from source) so a concurrent publish to `v<N+1>` on
    // the source can never leave the checkpoint pointing at a version
    // we did not link. Written LAST so a crash before this point leaves
    // the checkpoint in a "version present, no pointer" state, which
    // recovery treats as a freshly-initialised tree.
    write_current_for_version(target_fs, target_root, version_id)?;
    Ok(())
}

/// Inputs to [`run_checkpoint`] bundled together to keep the function
/// signature within clippy's `too_many_arguments` budget.
pub struct CheckpointParams<'a> {
    /// Destination root directory for the checkpoint.
    pub target_root: &'a Path,
    /// `Fs` backend that owns `target_root`.
    pub target_fs: &'a Arc<dyn Fs>,
    /// Source tree's root directory (contains manifest / `v<id>` / current).
    pub src_root: &'a Path,
    /// `Fs` backend that owns `src_root`.
    pub src_fs: &'a Arc<dyn Fs>,
    /// Pause gate that defers compaction-driven deletions for the duration
    /// of the checkpoint.
    pub deletion_pause: &'a Arc<crate::deletion_pause::DeletionPause>,
    /// Visible-seqno counter, recorded into [`CheckpointInfo::seqno`].
    pub visible_seqno: &'a crate::seqno::SharedSequenceNumberGenerator,
    /// Whether to capture the value log under `target/blobs/`.
    pub include_blobs: bool,
}

/// RAII guard that removes a partially-built checkpoint directory on
/// early return. Call [`PartialCheckpointGuard::commit`] just before the
/// final success path to disarm it; otherwise its `Drop` walks the tree
/// and best-effort removes it.
struct PartialCheckpointGuard<'a> {
    target_root: &'a Path,
    target_fs: &'a Arc<dyn Fs>,
    armed: bool,
}

impl<'a> PartialCheckpointGuard<'a> {
    fn new(target_root: &'a Path, target_fs: &'a Arc<dyn Fs>) -> Self {
        Self {
            target_root,
            target_fs,
            armed: true,
        }
    }

    fn commit(mut self) {
        self.armed = false;
    }
}

impl Drop for PartialCheckpointGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Best-effort: a failure to clean up the partial checkpoint is
        // logged but does not turn into a panic — the original error
        // from `run_checkpoint` is what the caller wants to see.
        if let Err(e) = self.target_fs.remove_dir_all(self.target_root) {
            log::warn!(
                "Failed to clean up partial checkpoint at {}: {e:?}",
                self.target_root.display(),
            );
        }
    }
}

/// Common driver shared by [`Tree`](crate::Tree) and
/// [`BlobTree`](crate::BlobTree). Performs the flush + link + metadata
/// copy under a held [`Pause`](crate::deletion_pause::Pause) guard.
pub fn run_checkpoint<T: AbstractTree>(
    tree: &T,
    params: &CheckpointParams<'_>,
) -> crate::Result<CheckpointInfo> {
    let target_root = params.target_root;
    let target_fs = params.target_fs;
    let src_root = params.src_root;
    let src_fs = params.src_fs;
    let deletion_pause = params.deletion_pause;
    let visible_seqno = params.visible_seqno;
    let include_blobs = params.include_blobs;

    prepare_target(target_root, include_blobs, &**target_fs)?;

    // From this point on, any early return must clean up the partial
    // checkpoint so retries against the same path don't hit
    // `AlreadyExists`. The guard is disarmed via `commit()` once the
    // final `fsync_directory` succeeds.
    let cleanup = PartialCheckpointGuard::new(target_root, target_fs);

    // Hold the pause guard for the duration of the checkpoint so any
    // tables / blob files that compaction marks as deleted are held back.
    let _pause = deletion_pause.acquire();

    // Capture the seqno BEFORE the flush. Sampling later (between flush
    // and `current_version()`) is unsafe: a concurrent writer can land
    // in the freshly-rotated active memtable, advance `visible_seqno`,
    // and bump the captured value above what the snapshot actually
    // contains — those writes are in the new memtable, NOT in the SSTs
    // we're about to link. With the "before flush" ordering the
    // captured seqno is a strict lower bound on the snapshot's
    // contents: every record visible at sample time has reached the
    // memtable, the flush forces it into SSTs, and the version snapshot
    // sees the resulting on-disk state. Later writes can advance the
    // live counter but cannot pull our `captured_seqno` upward.
    let captured_seqno = visible_seqno.get();

    // Force a flush so the captured version reflects all data that has
    // reached the active memtable. Using a sentinel eviction seqno
    // (`SeqNo::MAX`) lets `flush_active_memtable` flush regardless of
    // current visible seqno — the data is durable in either case.
    tree.flush_active_memtable(SeqNo::MAX)?;

    let version = tree.current_version();

    let (sst_files, sst_bytes) = link_tables(&version, target_root, target_fs)?;

    let (blob_files, blob_bytes) = if include_blobs {
        link_blob_files(version.blob_files.iter().cloned(), target_root, target_fs)?
    } else {
        (0, 0)
    };

    copy_metadata(&**src_fs, src_root, &**target_fs, target_root, version.id())?;

    // fsync each populated child directory BEFORE the root so the
    // directory entries we just created (`tables/<id>`, `blobs/<id>`,
    // `current`, `manifest`, `v<id>`) survive a power loss. The root
    // fsync alone only persists the existence of `tables/` and
    // `blobs/`, not their contents.
    fsync_directory(&target_root.join(TABLES_FOLDER), &**target_fs)?;
    if include_blobs {
        fsync_directory(&target_root.join(BLOBS_FOLDER), &**target_fs)?;
    }

    fsync_directory(target_root, &**target_fs)?;

    // Finally, fsync the directory that CONTAINS `target_root`. Without
    // this the checkpoint's own directory entry can disappear after a
    // power loss even though the children we just synced would still be
    // intact on the underlying inodes. Required by the same fsync-
    // ordering rule that drove the child-directory syncs above.
    if let Some(parent) = target_root.parent().filter(|p| !p.as_os_str().is_empty()) {
        fsync_directory(parent, &**target_fs)?;
    }

    cleanup.commit();

    Ok(CheckpointInfo {
        sst_files,
        blob_files,
        total_bytes: sst_bytes.saturating_add(blob_bytes),
        version_id: version.id(),
        seqno: captured_seqno,
    })
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;
    use crate::fs::{FsOpenOptions, MemFs, StdFs};
    use std::io::{Read, Write};

    /// `link_or_copy_cross_fs` must transparently stream bytes through
    /// both trait objects when source and destination back ends differ
    /// (here: `StdFs` source vs. `MemFs` target — the `MemFs` backend
    /// has no way to see the on-disk source file, so the hard-link
    /// attempt returns `NotFound` and we fall through to a streamed
    /// copy). Verifies BOTH the copy lands AND the two filesystems
    /// stay independent under subsequent mutation.
    #[test]
    fn cross_fs_link_or_copy_streams_through_trait() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("payload.bin");
        std::fs::write(&src, b"cross-fs-payload").unwrap();

        let std_fs: Arc<dyn Fs> = Arc::new(StdFs);
        let mem_fs: Arc<dyn Fs> = Arc::new(MemFs::new());
        mem_fs.create_dir_all(Path::new("/dst")).unwrap();

        let dst = Path::new("/dst/payload.bin");
        let bytes = link_or_copy_cross_fs(&std_fs, &src, &mem_fs, dst).unwrap();
        assert_eq!(bytes, b"cross-fs-payload".len() as u64);

        // Bytes landed in MemFs.
        let mut buf = String::new();
        mem_fs
            .open(dst, &FsOpenOptions::new().read(true))
            .unwrap()
            .read_to_string(&mut buf)
            .unwrap();
        assert_eq!(buf, "cross-fs-payload");

        // Mutating `dst` via MemFs must NOT affect the StdFs source —
        // proves the streamed copy produced an independent file rather
        // than aliasing.
        let mut writer = mem_fs
            .open(dst, &FsOpenOptions::new().write(true).truncate(true))
            .unwrap();
        writer.write_all(b"mutated-via-mem-fs").unwrap();
        drop(writer);

        assert_eq!(std::fs::read(&src).unwrap(), b"cross-fs-payload");

        let mut after = String::new();
        mem_fs
            .open(dst, &FsOpenOptions::new().read(true))
            .unwrap()
            .read_to_string(&mut after)
            .unwrap();
        assert_eq!(after, "mutated-via-mem-fs");
    }
}

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
    AbstractTree, CheckpointInfo,
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
/// The `hard_link` path is gated on a positive [`Fs::backend_id`]
/// match. `Arc::ptr_eq` would have been too strict (two independent
/// `Arc::new(StdFs)` values back the same kernel filesystem but are
/// not pointer-equal); a "try first, fall back on `NotFound`" pattern
/// would have been too loose (a `MemFs` source paired with a `StdFs`
/// destination could let the kernel resolve `src` against the host
/// filesystem and silently link an unrelated file). `Fs::backend_id`
/// is the explicit capability check that catches both cases safely.
pub fn link_or_copy_cross_fs(
    src_fs: &Arc<dyn Fs>,
    src: &Path,
    dst_fs: &Arc<dyn Fs>,
    dst: &Path,
) -> std::io::Result<u64> {
    // Refuse to attempt `hard_link` unless both backends positively
    // assert (via `Fs::backend_id`) that they resolve paths against
    // the same namespace. Without this gate a MemFs source paired with
    // a StdFs destination would let the kernel resolve `src` against
    // the HOST filesystem; if a real file happens to live at the same
    // spelling the checkpoint would silently capture THAT file instead
    // of the in-memory source. See `Fs::backend_id` for the contract.
    let shared_namespace = match (src_fs.backend_id(), dst_fs.backend_id()) {
        (Some(a), Some(b)) => a == b,
        _ => false,
    };

    if shared_namespace {
        match dst_fs.hard_link(src, dst) {
            // The dst stat syscall here is intentional — do NOT replace
            // it with a caller-supplied "known size" from
            // `Table::file_size()` or
            // `BlobFileMetadata::total_compressed_bytes`. Those values
            // record the writer's `file_pos` BEFORE the metadata block
            // and footer were appended, so they undercount the on-disk
            // file by hundreds to thousands of bytes per table. The
            // streamed-copy fallback below counts the actual bytes it
            // writes, so the two branches must agree on physical bytes
            // for `CheckpointInfo::total_bytes` to match on-disk reality
            // (asserted by `checkpoint_info_total_bytes_matches_disk`).
            // One extra stat per linked file is cheap relative to the
            // link itself.
            Ok(()) => return Ok(dst_fs.metadata(dst)?.len),
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::Unsupported
                ) =>
            {
                // Same kernel namespace but the link didn't take —
                // either dst_fs's backend doesn't support hard_link at
                // all, or the file moved out from under us before the
                // syscall. Either way, fall through to the streamed
                // copy below. Log at `debug` for symmetry with
                // `StdFs::hard_link`'s EXDEV fallback — operators
                // wanting visibility of unexpected full copies grep the
                // `fs` / `checkpoint` modules at debug level. `warn`
                // would drown real signal on a misconfigured tier with
                // thousands of SSTs.
                log::debug!(
                    "link_or_copy_cross_fs({}, {}) falling back to streamed copy ({})",
                    src.display(),
                    dst.display(),
                    e.kind(),
                );
            }
            Err(e) => return Err(e),
        }
    } else {
        // Backends do not share a namespace (e.g. MemFs source vs
        // StdFs destination). A hard_link attempt here would resolve
        // `src` against the WRONG namespace and could silently link an
        // unrelated file; skip straight to the streamed copy.
        log::debug!(
            "link_or_copy_cross_fs({}, {}) crossing namespaces — streaming copy",
            src.display(),
            dst.display(),
        );
    }

    // Cross-backend / no-hardlink path — stream bytes through the trait.
    // The buffer is heap-allocated to avoid bloating the stack frame;
    // checkpoint is a cold-path operation so the extra allocation is
    // negligible.
    let mut src_file = src_fs.open(src, &FsOpenOptions::new().read(true))?;
    let mut dst_file = dst_fs.open(dst, &FsOpenOptions::new().write(true).create_new(true))?;

    // Run the copy in an inner closure so any failure (read, write,
    // flush, fsync) leaves us with the original error AND lets us
    // best-effort `remove_file(dst)` before propagating. Without this,
    // a mid-copy ENOSPC/EIO leaves a partial `dst` file on the
    // destination FS; a subsequent retry hits `create_new`'s
    // AlreadyExists check and fails for a wholly different reason,
    // hiding the real cause. `PartialCheckpointGuard` cleans up the
    // whole target dir on the normal failure path, but this helper is
    // also called from cross-Fs tests and from any future caller that
    // doesn't sit inside that guard — so the local best-effort
    // cleanup is the safer invariant.
    let result: std::io::Result<u64> = (|| {
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
    })();

    match result {
        Ok(total) => Ok(total),
        Err(e) => {
            // Release the dst handle before unlink so backends that
            // block unlink while a handle is open (Windows) can succeed.
            drop(dst_file);
            let _ = dst_fs.remove_file(dst);
            Err(e)
        }
    }
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

/// Copies an OPTIONAL metadata file from `src_root` to `target_root`.
///
/// "Optional" = the source file may legitimately be absent (recovery
/// treats a missing manifest as a freshly-initialised tree). Required
/// metadata is no longer copied through this path — `v<id>` is now
/// serialised directly from the captured in-memory Version (see
/// [`copy_metadata`]) so its source-file lifetime no longer matters.
///
/// Opens the source directly instead of `exists()` + `open()` to avoid
/// the TOCTOU window where the file disappears between the two calls.
fn copy_metadata_file_optional(
    src_fs: &dyn Fs,
    src_root: &Path,
    target_fs: &dyn Fs,
    target_root: &Path,
    file_name: &str,
) -> crate::Result<()> {
    let src = src_root.join(file_name);
    let mut src_file = match src_fs.open(&src, &FsOpenOptions::new().read(true)) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
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
    use crate::checksum::ChecksumType;
    use crate::file::rewrite_atomic;
    use byteorder::{LittleEndian, WriteBytesExt};

    // The `current` wire format is `version_id: u64 | checksum: u128 |
    // checksum_type: u8`. The checksum field is reserved for future
    // verification — recovery reads it but does not validate it against
    // the `v<id>` contents — so a zero literal is the documented "no
    // digest carried" sentinel, not a forged digest. The checksum type
    // MUST come from `ChecksumType` so any future format evolution
    // (e.g. a real checksum) shifts this writer and the recovery
    // checker in lockstep through one shared enum.
    const RESERVED_CHECKSUM: u128 = 0;

    let mut content = vec![];
    content.write_u64::<LittleEndian>(version_id)?;
    content.write_u128::<LittleEndian>(RESERVED_CHECKSUM)?;
    content.write_u8(u8::from(ChecksumType::Xxh3))?;

    rewrite_atomic(&target_root.join(CURRENT_VERSION_FILE), &content, target_fs)?;
    Ok(())
}

/// Replicates manifest + `v<id>` + writes a fresh `current` pointer.
///
/// Best-effort-copies the manifest from the source tree, then writes
/// `v<id>` and `current` into the checkpoint directory from the
/// captured in-memory `Version` rather than copying the source file.
///
/// `version` is the captured snapshot held by the checkpoint driver
/// from `tree.current_version()`. Writing it through
/// [`crate::version::persist_version`] removes the dependency on the
/// source `v<id>` file's lifetime: a concurrent
/// [`crate::version::SuperVersions::maintenance`] call may delete the
/// source file between capture and this function, but the snapshot is
/// fully reconstructible from memory, so checkpoint creation does not
/// fail under that race. `comparator_name` is required to encode the
/// version through the same wire-format path the live tree uses (see
/// [`crate::version::persist_version`]'s signature). `current` is
/// then written via [`write_current_for_version`] referencing the
/// freshly-persisted `version.id()`.
pub fn copy_metadata(
    src_fs: &dyn Fs,
    src_root: &Path,
    target_fs: &dyn Fs,
    target_root: &Path,
    version: &crate::version::Version,
    comparator_name: &str,
) -> crate::Result<()> {
    // Manifest stores level count + comparator name. On a never-written
    // tree the manifest may legitimately be absent (recovery treats
    // missing manifest as a freshly-initialised tree), so this is optional.
    copy_metadata_file_optional(src_fs, src_root, target_fs, target_root, "manifest")?;
    // Re-serialise the captured Version into target/v<id> rather than
    // copying the source file. Reason: SuperVersions::maintenance can
    // physically remove the source v<id> between current_version() and
    // this point (manifest GC fires when seqno < mvcc_gc_watermark for
    // a version older than the active one). The captured `version` is
    // an in-memory snapshot held by the checkpoint driver and is the
    // authoritative source for the snapshot we just hard-linked SSTs
    // for, so writing it from memory eliminates the race entirely —
    // the source file's lifetime no longer matters.
    crate::version::persist_version(target_root, version, comparator_name, target_fs)?;
    // CURRENT pointer is generated fresh for the captured `version_id`
    // (NOT copied from source) so a concurrent publish to `v<N+1>` on
    // the source can never leave the checkpoint pointing at a version
    // we did not link. Written LAST so a crash before this point leaves
    // the checkpoint with a version file but no CURRENT pointer:
    // `Tree::open` on such a directory will fail to recover (no valid
    // pointer to load) — the partial dir must be removed and the
    // checkpoint retried. `PartialCheckpointGuard` performs that
    // removal on the normal error path; an unclean crash (no Drop) is
    // the only case the operator must clean up manually.
    write_current_for_version(target_fs, target_root, version.id())?;
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
    let target_fs = params.target_fs;
    let src_root = params.src_root;
    let src_fs = params.src_fs;
    let deletion_pause = params.deletion_pause;
    let visible_seqno = params.visible_seqno;
    let include_blobs = params.include_blobs;

    // Normalise the target by dropping all CurDir (`.`) components so
    // every downstream call sees the same canonical form regardless of
    // how the caller spelled the path. Without this, `"./checkpoint"`
    // and `"checkpoint"` behave differently on backends that don't
    // resolve `.` against a host-wide CWD (e.g. MemFs's `create_dir`
    // and `sync_directory` reject `.` as not-found because the in-memory
    // directory set never inserts it). ParentDir (`..`) is preserved —
    // it's a semantic component that affects the resulting path.
    let normalized_target: std::path::PathBuf = params
        .target_root
        .components()
        .filter(|c| !matches!(c, std::path::Component::CurDir))
        .collect();
    if normalized_target.as_os_str().is_empty() {
        return Err(crate::Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "checkpoint target_root must name at least one path component",
        )));
    }
    let target_root = normalized_target.as_path();

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
    // reached the active memtable. The eviction seqno parameter doubles
    // as `CompactionStream::gc_seqno_threshold` — any older version of
    // a key with `seqno < threshold` is dropped during the flush-time
    // merge, and snapshot readers on the SOURCE tree lose that history.
    //
    // We pass `0` so the checkpoint-triggered flush never expands GC
    // beyond what would have happened anyway: a checkpoint must NOT
    // change the source's MVCC visibility semantics. Using a tighter
    // threshold (e.g. `captured_seqno`) would still wrongly drop
    // history readers below that watermark might need; using
    // `SeqNo::MAX` (a previous oversight) wiped every older version
    // of every key.
    tree.flush_active_memtable(0)?;

    let version = tree.current_version();

    let (sst_files, sst_bytes) = link_tables(&version, target_root, target_fs)?;

    let (blob_files, blob_bytes) = if include_blobs {
        link_blob_files(version.blob_files.iter().cloned(), target_root, target_fs)?
    } else {
        (0, 0)
    };

    copy_metadata(
        &**src_fs,
        src_root,
        &**target_fs,
        target_root,
        &version,
        tree.tree_config().comparator.name(),
    )?;

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

    // Finally, fsync the directory that CONTAINS `target_root` so the
    // checkpoint's own directory entry survives a power loss even
    // though the children we just synced would otherwise stay intact
    // on the underlying inodes. Required by the same fsync-ordering
    // rule that drove the child-directory syncs above.
    //
    // Only fsync a NAMED parent. After the CurDir-stripping
    // normalisation at the top of run_checkpoint, a single-component
    // target like `"checkpoint"` has an empty parent — there is no
    // backend-portable directory to fsync (in particular, MemFs has
    // no CWD, so `sync_directory(".")` returns NotFound). Skip the
    // fsync in that case; callers needing the parent-dir-entry-
    // survives-power-loss guarantee pass an absolute target path.
    if let Some(parent) = target_root.parent()
        && !parent.as_os_str().is_empty()
    {
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

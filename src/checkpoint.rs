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
//!    for [`BlobTree`](crate::blob_tree::BlobTree)) and hard-link each one into `target/tables/` (or
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
    fs::{Fs, FsFile, FsOpenOptions, SyncMode},
    version::Version,
    vlog::BlobFile,
};
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, string::String, vec::Vec};
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
        if e.kind() == crate::io::ErrorKind::AlreadyExists {
            crate::io::Error::new(
                crate::io::ErrorKind::AlreadyExists,
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
///    is a pure link — it surfaces the underlying error rather than
///    copying.
/// 2. **On cross-device (`EXDEV` / `CrossesDevices`), `Unsupported`, or
///    `NotFound`**, stream bytes through both trait objects. This is the
///    only path that doubles storage, and it owns the cross-filesystem
///    copy so the copied file's durability honors `sync_mode` (via
///    `sync_all_with` below). Logged at [`log::debug`]; operator-visible
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
    sync_mode: SyncMode,
    use_reflink: bool,
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

    // Reflink fast path: when enabled and the destination filesystem reports
    // O(1) reflink support (Btrfs / XFS-reflink / APFS), clone into an
    // INDEPENDENT inode (modifying the checkpoint never touches the original,
    // no max-links-per-inode limit) at copy-on-write cost. Gated on a shared
    // namespace for the same reason as `hard_link` below: `reflink_file`
    // resolves `src` through `dst_fs`, so a cross-namespace clone could capture
    // the wrong file. `reflink_file` clones when it can and byte-copies on a
    // rare decline, so it always yields an independent file or a genuine I/O
    // error (the latter cleaned up by the caller's `PartialCheckpointGuard`).
    if use_reflink
        && shared_namespace
        && dst.parent().is_some_and(|p| dst_fs.capabilities(p).reflink)
    {
        dst_fs.reflink_file(src, dst)?;
        return Ok(dst_fs.metadata(dst)?.len);
    }

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
                if crate::fs::is_cross_device(&e)
                    || e.kind() == crate::io::ErrorKind::Unsupported
                    || e.kind() == crate::io::ErrorKind::NotFound =>
            {
                // The link didn't take, for one of:
                //   - cross-device (EXDEV / CrossesDevices) — src and dst
                //     sit on different filesystems, so a true link is
                //     impossible. `StdFs::hard_link` now surfaces this
                //     instead of byte-copying, so the SyncMode-aware
                //     streamed copy below owns the cross-fs copy and the
                //     copied file honors `Config::sync_mode`.
                //   - Unsupported — dst_fs's backend has no hard_link.
                //   - NotFound — the file moved out before the syscall.
                // All fall through to the streamed copy below. Log at
                // `debug`: operators wanting visibility of full copies grep
                // the `fs` / `checkpoint` modules at debug level; `warn`
                // would drown real signal on a misconfigured tier with
                // thousands of SSTs.
                log::debug!(
                    "link_or_copy_cross_fs({}, {}) falling back to streamed copy ({})",
                    src.display(),
                    dst.display(),
                    e.kind(),
                );
            }
            Err(e) => return Err(e.into()),
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
            // Running copied-byte total, bounded by the source file size, so a
            // plain add cannot overflow u64.
            total += n as u64;
        }
        dst_file.flush()?;
        FsFile::sync_all_with(&*dst_file, sync_mode)?;
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
    sync_mode: SyncMode,
    use_reflink: bool,
) -> crate::Result<(usize, u64)> {
    let tables_dir = target_root.join(TABLES_FOLDER);
    let mut count = 0usize;
    let mut bytes: u64 = 0;

    for table in version.iter_tables() {
        let dst = tables_dir.join(table_link_name(table.id()));

        // Source Fs may differ from `target_fs` when `level_routes` points
        // a hot tier at one backend (e.g. tmpfs) and the rest of the tree
        // at another. `link_or_copy_cross_fs` picks the right strategy.
        let written = link_or_copy_cross_fs(
            &table.fs,
            &table.path,
            target_fs,
            &dst,
            sync_mode,
            use_reflink,
        )
        .map_err(crate::Error::from)?;
        // Checkpoint totals: `bytes` is bounded by the disk size and `count` by
        // the number of files, so plain adds cannot overflow.
        bytes += written;
        count += 1;
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
    sync_mode: SyncMode,
    use_reflink: bool,
) -> crate::Result<(usize, u64)> {
    let blobs_dir = target_root.join(BLOBS_FOLDER);
    let mut count = 0usize;
    let mut bytes: u64 = 0;

    for blob in blob_files {
        let dst = blobs_dir.join(blob_link_name(blob.id()));
        let written = link_or_copy_cross_fs(
            &blob.0.fs,
            &blob.0.path,
            target_fs,
            &dst,
            sync_mode,
            use_reflink,
        )
        .map_err(crate::Error::from)?;
        // Checkpoint totals: `bytes` is bounded by the disk size and `count` by
        // the number of files, so plain adds cannot overflow.
        bytes += written;
        count += 1;
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
    sync_mode: SyncMode,
) -> crate::Result<()> {
    let src = src_root.join(file_name);
    let mut src_file = match src_fs.open(&src, &FsOpenOptions::new().read(true)) {
        Ok(f) => f,
        Err(e) if e.kind() == crate::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    let dst = target_root.join(file_name);
    let mut dst_file = target_fs.open(&dst, &FsOpenOptions::new().write(true).create_new(true))?;

    std::io::copy(&mut src_file, &mut dst_file)?;
    dst_file.flush()?;
    FsFile::sync_all_with(&*dst_file, sync_mode)?;
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
/// The checksum field is the canonical CURRENT digest produced by
/// [`crate::manifest_blocks::current_digest::compute`]: an XXH3-128
/// over (`version_id`, `layout_version`, flags, sorted TOC tuples
/// with each section's own Block-level XXH3-128). Reusing the exact
/// path `get_current_version` re-derives on `Tree::open` guarantees
/// bit-identical digest computation between writer and reader. A
/// section-byte swap is caught (changes the per-section checksum in
/// the TOC), a tail-only torn write is recovered via the head mirror
/// before the digest is computed, and a per-Block ECC repair on
/// section read does not invalidate the digest (the TOC-bound
/// section checksum was stamped at writer-time, not derived from
/// on-disk bytes).
fn write_current_for_version(
    target_fs: &dyn Fs,
    target_root: &Path,
    version_id: u64,
    runtime: Arc<crate::runtime_config::RuntimeConfig>,
    encryption: Option<Arc<dyn crate::encryption::EncryptionProvider>>,
    sync_mode: SyncMode,
) -> crate::Result<()> {
    use crate::checksum::ChecksumType;
    use crate::file::rewrite_atomic;
    use crate::io::{LittleEndian, WriteBytesExt};
    use crate::manifest_blocks::{current_digest, reader::ManifestArchiveReader};

    let manifest_path = target_root.join(format!("v{version_id}"));
    // Open the freshly-written manifest through the same reader
    // `get_current_version` will use, derive the canonical CURRENT
    // digest from its parsed footer, and stamp the pointer. Using
    // the reader here (vs. re-reading raw bytes ourselves) guarantees
    // bit-identical digest computation between writer and reader
    // paths: any mismatch is a real bug, not a derivation drift.
    //
    // Runtime + encryption come from the checkpoint driver and
    // mirror the snapshot used by the preceding `persist_version`
    // call — otherwise the reader would try to decode an
    // encryption-wrapped manifest without the provider and fail to
    // produce the footer, leaving the checkpoint with a dangling
    // (manifest written, no CURRENT) state.
    let archive = ManifestArchiveReader::open(&manifest_path, target_fs, runtime, encryption)?;
    let checksum = current_digest::compute(version_id, archive.footer())?;

    let mut content = vec![];
    content.write_u64::<LittleEndian>(version_id)?;
    content.write_u128::<LittleEndian>(checksum)?;
    content.write_u8(u8::from(ChecksumType::Xxh3))?;

    rewrite_atomic(
        &target_root.join(CURRENT_VERSION_FILE),
        &content,
        target_fs,
        sync_mode,
    )?;
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
#[expect(
    clippy::too_many_arguments,
    reason = "checkpoint metadata copy threads (src fs+root, target fs+root, version, \
              comparator, runtime, encryption) — every parameter is load-bearing and \
              wrapping into a struct would just move the count to the struct literal"
)]
pub fn copy_metadata(
    src_fs: &dyn Fs,
    src_root: &Path,
    target_fs: &dyn Fs,
    target_root: &Path,
    version: &crate::version::Version,
    comparator_name: &str,
    runtime: std::sync::Arc<crate::runtime_config::RuntimeConfig>,
    encryption: Option<std::sync::Arc<dyn crate::encryption::EncryptionProvider>>,
    sync_mode: SyncMode,
) -> crate::Result<()> {
    // Manifest stores level count + comparator name. On a never-written
    // tree the manifest may legitimately be absent (recovery treats
    // missing manifest as a freshly-initialised tree), so this is optional.
    copy_metadata_file_optional(
        src_fs,
        src_root,
        target_fs,
        target_root,
        "manifest",
        sync_mode,
    )?;
    // Re-serialise the captured Version into target/v<id> rather than
    // copying the source file. Reason: SuperVersions::maintenance can
    // physically remove the source v<id> between current_version() and
    // this point (manifest GC fires when seqno < mvcc_gc_watermark for
    // a version older than the active one). The captured `version` is
    // an in-memory snapshot held by the checkpoint driver and is the
    // authoritative source for the snapshot we just hard-linked SSTs
    // for, so writing it from memory eliminates the race entirely —
    // the source file's lifetime no longer matters.
    // Checkpoints carry their own snapshot of the runtime config so
    // the captured manifest is encoded with the same toggles the
    // live tree used at capture time. Receives the snapshot from the
    // driver — Tree::create_checkpoint loads it via load_full() on
    // the source tree's RuntimeConfigHandle so the checkpoint sees
    // exactly the config in effect at capture.
    crate::version::persist_version(
        target_root,
        version,
        comparator_name,
        target_fs,
        Arc::clone(&runtime),
        encryption.clone(),
        sync_mode,
    )?;
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
    //
    // The runtime + encryption snapshot here MUST match what
    // `persist_version` above used — the helper reopens the manifest
    // via `ManifestArchiveReader`, and an encrypted manifest reopened
    // without its provider would fail to decode the footer Block.
    write_current_for_version(
        target_fs,
        target_root,
        version.id(),
        runtime,
        encryption,
        sync_mode,
    )?;
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
    /// Snapshot of the source tree's runtime config at capture time.
    /// Forwarded to [`copy_metadata`] / [`crate::version::persist_version`]
    /// so the checkpoint manifest is encoded with the same toggles
    /// the live tree used when the checkpoint started — eliminates
    /// drift between source-tree manifest and captured-tree manifest.
    /// Callers obtain the snapshot via
    /// `tree.0.runtime_config.load_full()` on the source `Tree`.
    pub runtime_config: Arc<crate::runtime_config::RuntimeConfig>,
    /// Encryption provider cloned from the source tree's
    /// `Config::encryption`. Threaded through to the manifest writer
    /// so the captured manifest is encrypted with the same key
    /// chain the source tree uses — a checkpoint of an encrypted
    /// tree is itself encrypted end-to-end.
    pub encryption: Option<Arc<dyn crate::encryption::EncryptionProvider>>,
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
        return Err(crate::Error::from(std::io::Error::new(
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

    // Checkpoint fsyncs follow the source tree's configured durability.
    let sync_mode = tree.tree_config().sync_mode;

    // Reflink the snapshot files when the live config opts in (default) and
    // the destination filesystem supports it; otherwise the hard-link path is
    // used. Read from the captured live RuntimeConfig so a runtime toggle is
    // honoured.
    let use_reflink = params.runtime_config.use_reflink_for_checkpoint;

    let (sst_files, sst_bytes) =
        link_tables(&version, target_root, target_fs, sync_mode, use_reflink)?;

    let (blob_files, blob_bytes) = if include_blobs {
        link_blob_files(
            version.blob_files.iter().cloned(),
            target_root,
            target_fs,
            sync_mode,
            use_reflink,
        )?
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
        Arc::clone(&params.runtime_config),
        params.encryption.clone(),
        sync_mode,
    )?;

    // fsync each populated child directory BEFORE the root so the
    // directory entries we just created (`tables/<id>`, `blobs/<id>`,
    // `current`, `manifest`, `v<id>`) survive a power loss. The root
    // fsync alone only persists the existence of `tables/` and
    // `blobs/`, not their contents.
    fsync_directory(&target_root.join(TABLES_FOLDER), &**target_fs, sync_mode)?;
    if include_blobs {
        fsync_directory(&target_root.join(BLOBS_FOLDER), &**target_fs, sync_mode)?;
    }

    fsync_directory(target_root, &**target_fs, sync_mode)?;

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
        fsync_directory(parent, &**target_fs, sync_mode)?;
    }

    cleanup.commit();

    Ok(CheckpointInfo {
        sst_files,
        blob_files,
        // Sum of two on-disk byte totals, bounded by the disk size; cannot
        // overflow u64.
        total_bytes: sst_bytes + blob_bytes,
        version_id: version.id(),
        seqno: captured_seqno,
    })
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test code")]
mod tests;

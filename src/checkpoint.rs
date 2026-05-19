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
/// Builds `<target>`, `<target>/tables`, and optionally `<target>/blobs`
/// under `target_fs`. Rejects an already existing target so a
/// partially-overlapping checkpoint cannot be accidentally produced.
pub fn prepare_target(target: &Path, include_blobs: bool, target_fs: &dyn Fs) -> crate::Result<()> {
    if target_fs.exists(target)? {
        return Err(crate::Error::from(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!(
                "checkpoint target {} already exists; refusing to overwrite",
                target.display(),
            ),
        )));
    }

    target_fs.create_dir_all(target)?;
    target_fs.create_dir_all(&target.join(TABLES_FOLDER))?;
    if include_blobs {
        target_fs.create_dir_all(&target.join(BLOBS_FOLDER))?;
    }
    Ok(())
}

/// Links (or copies) one file across [`Fs`] backends.
///
/// When `src_fs` and `dst_fs` refer to the same backend instance
/// (`Arc::ptr_eq`), this delegates to [`Fs::hard_link`], which transparently
/// falls back to a byte copy if the link would cross physical filesystems
/// (Unix `EXDEV`). When the two backends differ — e.g. an SST that lives
/// on a tiered-storage route backed by [`MemFs`](crate::fs::MemFs) while
/// the checkpoint target is on [`StdFs`](crate::fs::StdFs) — the function
/// streams the bytes through both trait objects so the link semantics
/// degrade to a normal file copy.
pub fn link_or_copy_cross_fs(
    src_fs: &Arc<dyn Fs>,
    src: &Path,
    dst_fs: &Arc<dyn Fs>,
    dst: &Path,
) -> std::io::Result<u64> {
    if Arc::ptr_eq(src_fs, dst_fs) {
        dst_fs.hard_link(src, dst)?;
        return Ok(dst_fs.metadata(dst)?.len);
    }

    // Different Fs backends — stream bytes through the trait. The buffer
    // is heap-allocated to avoid bloating the stack frame; checkpoint is a
    // cold-path operation so the extra allocation is negligible.
    let mut src_file = src_fs.open(src, &FsOpenOptions::new().read(true))?;
    let mut dst_file = dst_fs.open(dst, &FsOpenOptions::new().write(true).create_new(true))?;

    let mut buf = vec![0u8; 64 * 1024].into_boxed_slice();
    let mut total: u64 = 0;
    loop {
        let n = src_file.read(&mut buf)?;
        if n == 0 {
            break;
        }
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

/// Copies one of the small metadata files (manifest, `v<id>`, or
/// `current`) from `src_root` to `target_root` if it exists. Missing files
/// are silently ignored — recovery treats the absence of these files as a
/// freshly-initialised tree.
fn copy_metadata_file(
    src_fs: &dyn Fs,
    src_root: &Path,
    target_fs: &dyn Fs,
    target_root: &Path,
    file_name: &str,
) -> crate::Result<()> {
    let src = src_root.join(file_name);
    if !src_fs.exists(&src)? {
        return Ok(());
    }
    let dst = target_root.join(file_name);

    let mut src_file = src_fs.open(&src, &FsOpenOptions::new().read(true))?;
    let mut dst_file = target_fs.open(&dst, &FsOpenOptions::new().write(true).create_new(true))?;

    std::io::copy(&mut src_file, &mut dst_file)?;
    dst_file.flush()?;
    FsFile::sync_all(&*dst_file)?;
    Ok(())
}

/// Replicates the manifest, the active version snapshot (`v<id>`), and the
/// `current` pointer file from the source tree into the checkpoint.
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
    // Manifest stores level count + comparator name; required on open.
    copy_metadata_file(src_fs, src_root, target_fs, target_root, "manifest")?;
    // Active version snapshot.
    copy_metadata_file(
        src_fs,
        src_root,
        target_fs,
        target_root,
        &format!("v{version_id}"),
    )?;
    // CURRENT pointer must be copied LAST so a crash between version copy
    // and CURRENT copy leaves the checkpoint in a "version present, no
    // pointer" state — equivalent to a tree that was just created.
    copy_metadata_file(
        src_fs,
        src_root,
        target_fs,
        target_root,
        CURRENT_VERSION_FILE,
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

    // Hold the pause guard for the duration of the checkpoint so any
    // tables / blob files that compaction marks as deleted are held back.
    let _pause = deletion_pause.acquire();

    // Force a flush so the captured version reflects all data that has
    // reached the active memtable. Using a sentinel eviction seqno
    // (`SeqNo::MAX`) lets `flush_active_memtable` flush regardless of
    // current visible seqno — the data is durable in either case.
    tree.flush_active_memtable(SeqNo::MAX)?;

    let version = tree.current_version();
    let captured_seqno = visible_seqno.get();

    let (sst_files, sst_bytes) = link_tables(&version, target_root, target_fs)?;

    let (blob_files, blob_bytes) = if include_blobs {
        link_blob_files(version.blob_files.iter().cloned(), target_root, target_fs)?
    } else {
        (0, 0)
    };

    copy_metadata(&**src_fs, src_root, &**target_fs, target_root, version.id())?;
    fsync_directory(target_root, &**target_fs)?;

    Ok(CheckpointInfo {
        sst_files,
        blob_files,
        total_bytes: sst_bytes.saturating_add(blob_bytes),
        version_id: version.id(),
        seqno: captured_seqno,
    })
}

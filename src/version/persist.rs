use crate::{
    encryption::EncryptionProvider,
    file::fsync_directory,
    fs::{Fs, SyncMode},
    manifest_blocks::writer::ManifestArchiveWriter,
    runtime_config::RuntimeConfig,
    version::Version,
};
use alloc::sync::Arc;

use crate::path::Path;

/// The suffix of the name a snapshot is written under until it is installed.
pub const SNAPSHOT_TEMPORARY_SUFFIX: &str = ".tmp";

/// Crate-internal (version module is not exported).
///
/// Writes `version` as the snapshot `v{N}` (Blocks-based layout,
/// `manifest_layout_version` = 1) and installs it: the snapshot is written
/// under the temporary name `v{N}.tmp` and its bytes synced, its empty edit
/// log `edits-{N}` is created, the snapshot is renamed to `v{N}`, and one
/// directory sync makes both names durable. The name `v{N}` is what installs
/// the snapshot (see [`installed_snapshot`](super::recovery::installed_snapshot)):
/// a rename is atomic, so the name never holds a snapshot whose bytes were
/// not synced first, and it costs no sync of its own.
///
/// The log needs no sync either: until the first edit appends to it, a log a
/// power loss took reads as the empty log it was, and that append syncs it.
///
/// A temporary or a log of the same id left by an earlier attempt is
/// replaced, and so is a snapshot of that id, by the rename. Ids only grow,
/// so one can only be the remains of an install that failed, which the
/// caller reported as failed.
///
/// # Errors
///
/// Propagates an I/O or encoding error. When the directory sync after the
/// rename fails, the snapshot's name is taken back first, so the previous
/// generation stays the installed one; a power loss can still have made the
/// rename durable and not the withdrawal, so the caller treats the outcome as
/// unknown (see `SuperVersions::persist_change`).
pub fn persist_version(
    folder: &Path,
    version: &Version,
    comparator_name: &str,
    fs: &dyn Fs,
    runtime: Arc<RuntimeConfig>,
    encryption: Option<Arc<dyn EncryptionProvider>>,
    sync_mode: SyncMode,
) -> crate::Result<()> {
    if comparator_name.len() > crate::comparator::MAX_COMPARATOR_NAME_BYTES {
        return Err(crate::Error::from(crate::io::Error::new(
            crate::io::ErrorKind::InvalidInput,
            format!(
                "comparator name is {} bytes (max {})",
                comparator_name.len(),
                crate::comparator::MAX_COMPARATOR_NAME_BYTES,
            ),
        )));
    }

    log::trace!(
        "Persisting version {} in {}",
        version.id(),
        folder.display(),
    );

    let path = folder.join(format!("v{}", version.id()));
    let temporary = folder.join(format!("v{}{SNAPSHOT_TEMPORARY_SUFFIX}", version.id()));
    let log = super::edit_log::log_path(folder, version.id());
    // A log holding edits is a live generation's: nothing appends to the
    // log of a failed install, so it is never a leftover.
    if super::edit_log::log_size(fs, &log)? > 0 {
        return Err(crate::Error::from(crate::io::Error::new(
            crate::io::ErrorKind::AlreadyExists,
            format!(
                "manifest snapshot {} is a live generation; reopen the tree",
                path.display()
            ),
        )));
    }
    remove_leftover(fs, &temporary)?;

    // Compose the Blocks-based manifest. The writer reserves the
    // 4 KiB head region on create(), accepts per-section writes
    // (each flushed as a BlockType::Manifest Block on the next
    // start() / finish()), and on finish() writes the tail footer
    // Block + size-hint trailer + optional head mirror per the
    // runtime config, and syncs the file.
    let mut writer = ManifestArchiveWriter::create(&temporary, fs, runtime, encryption, sync_mode)?;
    version.encode_into(&mut writer, comparator_name)?;
    writer.finish()?;
    super::edit_log::create_log(fs, folder, version.id(), sync_mode)?;
    fs.rename(&temporary, &path)?;

    // The snapshot's name and its log's, and every entry made in the folder
    // before (a new tree's folders), durable together.
    if let Err(e) = fsync_directory(folder, fs, sync_mode) {
        withdraw(folder, &path, fs, sync_mode);
        return Err(e.into());
    }
    Ok(())
}

/// Takes back the name of a snapshot whose install failed at its directory
/// sync, so the previous generation is the installed one again: the caller
/// reports the failure and removes what it wrote for the install (a
/// compaction's outputs), which the withdrawn snapshot would name. The removal
/// is synced where it can be; whatever this cannot do leaves the outcome
/// unknown, which the caller already assumes.
fn withdraw(folder: &Path, snapshot: &Path, fs: &dyn Fs, sync_mode: SyncMode) {
    if let Err(e) = fs.remove_file(snapshot) {
        log::warn!(
            "persist: could not withdraw {} after a failed install: {e}",
            snapshot.display()
        );
        return;
    }
    if let Err(e) = fsync_directory(folder, fs, sync_mode) {
        log::warn!(
            "persist: the withdrawal of {} is not durable: {e}",
            snapshot.display()
        );
    }
}

/// Removes `path`, left by a failed install of the same id, if it exists.
fn remove_leftover(fs: &dyn Fs, path: &Path) -> crate::Result<()> {
    match fs.remove_file(path) {
        Ok(()) => {
            log::warn!(
                "removed {}, left by an earlier failed manifest install",
                path.display()
            );
            Ok(())
        }
        Err(e) if e.kind() == crate::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests;

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

/// Crate-internal (version module is not exported).
///
/// Writes `version` as the snapshot `v{N}` (Blocks-based layout,
/// `manifest_layout_version` = 1) and installs it: the snapshot's bytes are
/// synced, then its empty edit log `edits-{N}` is created, and one directory
/// sync makes both names durable. The log is what installs the snapshot (see
/// [`installed_snapshot`](super::recovery::installed_snapshot)): made only
/// after the snapshot's bytes were synced, it cannot outlive them, and it
/// costs no sync of its own.
///
/// A snapshot or log of the same id left by an earlier attempt is removed
/// first. Ids only grow, so one can only be the remains of an install that
/// failed, which the caller reported as failed.
///
/// # Errors
///
/// Propagates an I/O or encoding error. On an error after the log was
/// created, the log is removed again where it can be, so the snapshot is not
/// left installed under an install reported as failed; the caller treats the
/// outcome as unknown regardless (see `SuperVersions::persist_change`).
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
    // The log first: without it the leftover snapshot is not installed.
    for leftover in [&log, &path] {
        remove_leftover(fs, leftover)?;
    }

    // Compose the Blocks-based manifest. The writer reserves the
    // 4 KiB head region on create(), accepts per-section writes
    // (each flushed as a BlockType::Manifest Block on the next
    // start() / finish()), and on finish() writes the tail footer
    // Block + size-hint trailer + optional head mirror per the
    // runtime config, and syncs the file.
    let mut writer = ManifestArchiveWriter::create(&path, fs, runtime, encryption, sync_mode)?;
    version.encode_into(&mut writer, comparator_name)?;
    writer.finish()?;
    super::edit_log::create_log(fs, folder, version.id(), sync_mode)?;

    // The snapshot's name and its log's, and every entry made in the folder
    // before (a new tree's folders), durable together.
    if let Err(e) = fsync_directory(folder, fs, sync_mode) {
        if let Err(removal) = fs.remove_file(&log) {
            log::warn!(
                "persist: could not withdraw the log {} of a failed install: {removal}",
                log.display()
            );
        }
        return Err(e.into());
    }

    Ok(())
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

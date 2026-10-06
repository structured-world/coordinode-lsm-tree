use crate::io::{LittleEndian, WriteBytesExt};
use crate::{
    encryption::EncryptionProvider,
    file::{CURRENT_VERSION_FILE, fsync_directory, rewrite_atomic},
    fs::{Fs, SyncMode},
    manifest_blocks::{current_digest, writer::ManifestArchiveWriter},
    runtime_config::RuntimeConfig,
    version::Version,
};
use alloc::sync::Arc;

use crate::path::Path;

/// Crate-internal (version module is not exported).
///
/// Writes a new `v{N}` manifest file using the Blocks-based layout
/// (`manifest_layout_version` = 1) and atomically updates the
/// `CURRENT_VERSION_FILE` pointer to reference it.
///
/// The pointer file is rewritten via [`rewrite_atomic`] only after
/// the manifest itself is fully fsynced — recovery never follows
/// `CURRENT` to a truncated/missing manifest.
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
    clear_unnamed_snapshot(folder, &path, version.id(), fs)?;

    // Compose the Blocks-based manifest. The writer reserves the
    // 4 KiB head region on create(), accepts per-section writes
    // (each flushed as a BlockType::Manifest Block on the next
    // start() / finish()), and on finish() writes the tail footer
    // Block + size-hint trailer + optional head mirror per the
    // runtime config.
    let mut writer = ManifestArchiveWriter::create(&path, fs, runtime, encryption, sync_mode)?;
    version.encode_into(&mut writer, comparator_name)?;
    let footer = writer.finish()?;

    // IMPORTANT: fsync folder on Unix
    fsync_directory(folder, fs, sync_mode)?;

    // CURRENT pointer carries a content-binding XXH3-128 over the
    // canonical footer payload (version_id + layout_version + flags +
    // sorted TOC entries that include each section's own XXH3-128
    // from its Block header). Compared to the earlier raw-byte hash
    // over `[HEAD_FOOTER_RESERVED_SIZE, section_end)`, this preserves
    // per-Block Page ECC repair on read: a section bit-flip that
    // ECC heals at decode time no longer trips this checksum,
    // because the digest is computed from writer-time section
    // checksums (which the section Block's own header carries on
    // read regardless of disk corruption).
    //
    // Threat coverage:
    //   T1 (mislinking) — version_id + TOC bind logical identity
    //   T2 (half-recovery) — caught earlier by ManifestArchiveReader
    //                        before the digest is computed
    //   T3 (bit-rot)    — caught per-Block by XXH3; ECC repairs
    //                     when enabled; CURRENT no longer interferes
    //   T4 (adversarial) — out of scope; enable Config::with_encryption
    //                      for per-Block AEAD authentication
    let checksum = current_digest::compute(version.id(), &footer)?;

    let mut current_file_content = vec![];
    current_file_content.write_u64::<LittleEndian>(version.id())?;
    current_file_content.write_u128::<LittleEndian>(checksum)?;
    current_file_content.write_u8(0)?; // 0 = xxh3

    rewrite_atomic(
        &folder.join(CURRENT_VERSION_FILE),
        &current_file_content,
        fs,
        sync_mode,
    )?;

    Ok(())
}

/// Removes a `v{id}` snapshot that `CURRENT` does not name, so the rotation
/// about to write `v{id}` can create it.
///
/// Such a file is left by an attempt that failed between writing the snapshot
/// and repointing `CURRENT` (or by a crash there). Recovery never reads it,
/// but the writer creates the snapshot with `create_new`, and a failed install
/// leaves the in-memory version where it was, so the retry derives the same id
/// and would be refused on every rotation until the process restarts.
///
/// A `v{id}` that `CURRENT` already names is never touched: that is the state
/// after a failure past the repoint (the directory sync), and rewriting the
/// snapshot under the pointer that names it would break the next open.
fn clear_unnamed_snapshot(
    folder: &Path,
    path: &Path,
    id: crate::version::VersionId,
    fs: &dyn Fs,
) -> crate::Result<()> {
    match fs.metadata(path) {
        Ok(_) => {}
        Err(e) if e.kind() == crate::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    }
    if named_by_current(folder, fs)? == Some(id) {
        return Err(crate::Error::from(crate::io::Error::new(
            crate::io::ErrorKind::AlreadyExists,
            format!(
                "manifest snapshot {} is the one CURRENT names; reopen the tree",
                path.display()
            ),
        )));
    }
    log::warn!(
        "removing manifest snapshot {} that CURRENT does not name, left by an earlier failed rotation",
        path.display()
    );
    fs.remove_file(path)?;
    Ok(())
}

/// The version id `CURRENT` names, or `None` when there is no `CURRENT` yet.
fn named_by_current(folder: &Path, fs: &dyn Fs) -> crate::Result<Option<u64>> {
    use crate::fs::FsOpenOptions;
    use crate::io::{LittleEndian, ReadBytesExt};

    match fs.open(
        &folder.join(CURRENT_VERSION_FILE),
        &FsOpenOptions::new().read(true),
    ) {
        Ok(mut file) => Ok(Some(file.read_u64::<LittleEndian>()?)),
        Err(e) if e.kind() == crate::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests;

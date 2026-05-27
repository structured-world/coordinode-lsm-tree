use crate::{
    encryption::EncryptionProvider,
    file::{CURRENT_VERSION_FILE, fsync_directory, hash_file_xxh3, rewrite_atomic},
    fs::Fs,
    manifest_blocks::writer::ManifestArchiveWriter,
    runtime_config::RuntimeConfig,
    version::Version,
};
use byteorder::{LittleEndian, WriteBytesExt};
use std::{path::Path, sync::Arc};

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
) -> crate::Result<()> {
    if comparator_name.len() > crate::comparator::MAX_COMPARATOR_NAME_BYTES {
        return Err(crate::Error::from(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
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

    // Compose the Blocks-based manifest. The writer reserves the
    // 4 KiB head region on create(), accepts per-section writes
    // (each flushed as a BlockType::Manifest Block on the next
    // start() / finish()), and on finish() writes the tail footer
    // Block + size-hint trailer + optional head mirror per the
    // runtime config.
    let mut writer = ManifestArchiveWriter::create(&path, fs, runtime, encryption)?;
    version.encode_into(&mut writer, comparator_name)?;
    writer.finish()?;

    // IMPORTANT: fsync folder on Unix
    fsync_directory(folder, fs)?;

    // Compute the file-level XXH3-128 of the just-written manifest
    // for the CURRENT pointer's integrity check. Per-Block XXH3
    // already verifies content on read; this file-level hash
    // additionally defends against substitution of the whole file
    // between persist and the next open (e.g., an attacker
    // renaming v0 → v1 to roll back state). Manifest is small
    // (KB-MB scale at most) so a second pass to hash it is cheap
    // relative to the durability-critical fsync just performed.
    // Shares `hash_file_xxh3` with the recovery path so both sides
    // of the persist / verify contract stay in lockstep.
    let checksum = hash_file_xxh3(fs, &path)?;

    let mut current_file_content = vec![];
    current_file_content.write_u64::<LittleEndian>(version.id())?;
    current_file_content.write_u128::<LittleEndian>(checksum)?;
    current_file_content.write_u8(0)?; // 0 = xxh3

    rewrite_atomic(
        &folder.join(CURRENT_VERSION_FILE),
        &current_file_content,
        fs,
    )?;

    Ok(())
}

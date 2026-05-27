use crate::{
    encryption::EncryptionProvider,
    file::{CURRENT_VERSION_FILE, fsync_directory, hash_file_range_xxh3, rewrite_atomic},
    fs::Fs,
    manifest_blocks::{HEAD_FOOTER_RESERVED_SIZE, writer::ManifestArchiveWriter},
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
    let section_end = writer.finish()?;

    // IMPORTANT: fsync folder on Unix
    fsync_directory(folder, fs)?;

    // Stamp the CURRENT pointer with an XXH3-128 over the section
    // bytes ONLY — the [HEAD_FOOTER_RESERVED_SIZE, section_end)
    // range. Excluding the head mirror + tail footer + size-hint
    // trailer means a torn or bit-rotted tail that
    // ManifestArchiveReader recovers through the head-mirror
    // fallback does NOT trip the CURRENT pointer's integrity check
    // first. The section bytes are the load-bearing content
    // (per-Block XXH3 still catches in-section bit-rot at read
    // time); the digest's job is detecting accidental substitution
    // or mislinking — e.g., a copy/restore picking the wrong
    // manifest, a half-applied snapshot recovery, or a sysadmin
    // renaming v0 over v1. XXH3-128 is NOT a cryptographic MAC: an
    // adversary with write access can craft matching content and
    // bypass this check. For adversarial tamper resistance enable
    // `Config::encryption` (AEAD authenticates every Block).
    let section_length = section_end.saturating_sub(HEAD_FOOTER_RESERVED_SIZE);
    let checksum = hash_file_range_xxh3(fs, &path, HEAD_FOOTER_RESERVED_SIZE, section_length)?;

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

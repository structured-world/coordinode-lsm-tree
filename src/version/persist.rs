use crate::{
    Checksum,
    file::{CURRENT_VERSION_FILE, fsync_directory, rewrite_atomic},
    fs::{Fs, FsOpenOptions},
    manifest_blocks::writer::ManifestArchiveWriter,
    runtime_config::RuntimeConfig,
    version::Version,
};
use byteorder::{LittleEndian, WriteBytesExt};
use std::{io::Read, path::Path, sync::Arc};

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
    let mut writer = ManifestArchiveWriter::create(&path, fs, runtime)?;
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
    let checksum = hash_manifest_file(fs, &path)?;

    let mut current_file_content = vec![];
    current_file_content.write_u64::<LittleEndian>(version.id())?;
    current_file_content.write_u128::<LittleEndian>(checksum.into_u128())?;
    current_file_content.write_u8(0)?; // 0 = xxh3

    rewrite_atomic(
        &folder.join(CURRENT_VERSION_FILE),
        &current_file_content,
        fs,
    )?;

    Ok(())
}

/// Stream the file at `path` through an XXH3-128 hasher in 64 KiB
/// chunks. Used by [`persist_version`] to stamp the CURRENT pointer
/// with the manifest's content hash.
fn hash_manifest_file(fs: &dyn Fs, path: &Path) -> crate::Result<Checksum> {
    let mut file = fs.open(path, &FsOpenOptions::new().read(true))?;
    let mut hasher = xxhash_rust::xxh3::Xxh3Default::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        // `n` is bounded by `buf.len()` (the read contract), so the
        // slice never out-of-bounds. `.get(..n)` would be defensive
        // but adds an Option unwrap that is itself a slice op.
        #[expect(
            clippy::indexing_slicing,
            reason = "n is bounded by buf.len() per std::io::Read::read contract"
        )]
        hasher.update(&buf[..n]);
    }
    Ok(Checksum::from_raw(hasher.digest128()))
}

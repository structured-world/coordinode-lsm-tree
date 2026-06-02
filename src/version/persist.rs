use crate::{
    encryption::EncryptionProvider,
    file::{CURRENT_VERSION_FILE, fsync_directory, rewrite_atomic},
    fs::{Fs, SyncMode},
    manifest_blocks::{current_digest, writer::ManifestArchiveWriter},
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
    sync_mode: SyncMode,
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

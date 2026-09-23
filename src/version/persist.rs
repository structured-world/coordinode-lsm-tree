use crate::io::{LittleEndian, WriteBytesExt};
use crate::{
    encryption::EncryptionProvider,
    file::{CURRENT_VERSION_FILE, fsync_directory, rewrite_atomic},
    fs::{Fs, SyncMode},
    manifest_blocks::{current_digest, writer::ManifestArchiveWriter},
    runtime_config::RuntimeConfig,
    version::{Version, edit::BootstrapEdit, edit_log},
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
///
/// A snapshot counts a level's runs in one byte, so a version with a wider
/// level is written as the part a snapshot holds plus an `edits-{id}` log whose
/// one record restores the rest; recovery replays it on top as it does any
/// edit. The snapshot's `bootstrap_edit` section records that record's length
/// and digest, so recovery requires it instead of opening the tree without the
/// wide levels. Both are synced before `CURRENT` names them. Returns the size of the
/// log this leaves for the new generation, `0` when the snapshot holds the
/// whole version.
pub fn persist_version(
    folder: &Path,
    version: &Version,
    comparator_name: &str,
    fs: &dyn Fs,
    runtime: Arc<RuntimeConfig>,
    encryption: Option<Arc<dyn EncryptionProvider>>,
    sync_mode: SyncMode,
) -> crate::Result<u64> {
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

    // Compose the Blocks-based manifest. The writer reserves the
    // 4 KiB head region on create(), accepts per-section writes
    // (each flushed as a BlockType::Manifest Block on the next
    // start() / finish()), and on finish() writes the tail footer
    // Block + size-hint trailer + optional head mirror per the
    // runtime config.
    let base = (!version.fits_snapshot()).then(|| version.snapshot_base());
    // The edit completing a wide version, encoded before the snapshot so the
    // snapshot can name it: recovery then refuses a log that lacks it rather
    // than opening a tree whose wide levels are empty.
    let bootstrap = match &base {
        Some(base) => {
            let mut payload = Vec::new();
            version.diff(base)?.encode(&mut payload)?;
            let record = BootstrapEdit::of(&payload)?;
            Some((payload, record))
        }
        None => None,
    };
    let mut writer = ManifestArchiveWriter::create(&path, fs, runtime, encryption, sync_mode)?;
    base.as_ref()
        .unwrap_or(version)
        .encode_into(&mut writer, comparator_name)?;
    if let Some((_, record)) = &bootstrap {
        writer.start("bootstrap_edit")?;
        writer.write_u32::<LittleEndian>(record.len)?;
        writer.write_u64::<LittleEndian>(record.digest)?;
    }
    let footer = writer.finish()?;

    // The log recovery replays on top of this snapshot. A file left there
    // under the same id belongs to no generation this one continues, so it
    // is replaced (or removed) before the pointer can make it live.
    let log_path = folder.join(format!("edits-{}", version.id()));
    let log_bytes = if let Some((payload, _)) = &bootstrap {
        edit_log::write_log(fs, &log_path, payload, sync_mode)?
    } else {
        match fs.remove_file(&log_path) {
            Ok(()) => {}
            Err(e) if e.kind() == crate::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        0
    };

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

    Ok(log_bytes)
}

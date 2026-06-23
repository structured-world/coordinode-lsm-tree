use super::*;
use crate::io::WriteBytesExt;
use crate::{
    fs::{Fs, MemFs, StdFs},
    manifest_blocks::writer::ManifestArchiveWriter,
};
use std::{io::Write, path::Path};

/// Write a minimal valid Blocks-based manifest with all four
/// mandatory sections (and optionally a `comparator_name`).
fn write_test_manifest(
    path: &Path,
    comparator_name: Option<&str>,
    fs: &dyn Fs,
) -> crate::Result<()> {
    let mut writer = ManifestArchiveWriter::create(
        path,
        fs,
        std::sync::Arc::new(crate::runtime_config::RuntimeConfig::default()),
        None,
        crate::fs::SyncMode::Normal,
    )?;

    writer.start("format_version")?;
    writer.write_u8(FormatVersion::V5.into())?;

    writer.start("tree_type")?;
    writer.write_u8(TreeType::Standard.into())?;

    writer.start("level_count")?;
    writer.write_u8(7)?;

    writer.start("filter_hash_type")?;
    writer.write_u8(u8::from(ChecksumType::Xxh3))?;

    if let Some(name) = comparator_name {
        writer.start("comparator_name")?;
        writer.write_all(name.as_bytes())?;
    }

    writer.finish()?;
    Ok(())
}

fn decode_manifest(path: &Path, fs: &dyn Fs) -> crate::Result<Manifest> {
    let mut reader = ManifestArchiveReader::open(
        path,
        fs,
        std::sync::Arc::new(crate::runtime_config::RuntimeConfig::default()),
        None,
    )?;
    Manifest::decode_from(&mut reader)
}

// ------------------------------------------------------------------
// StdFs tests
// ------------------------------------------------------------------

#[test]
fn manifest_without_comparator_name_defaults_to_default() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("manifest");

    write_test_manifest(&path, None, &StdFs)?;

    let manifest = decode_manifest(&path, &StdFs)?;
    assert_eq!(manifest.comparator_name, "default");
    Ok(())
}

#[test]
fn manifest_with_comparator_name_round_trips() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("manifest");

    write_test_manifest(&path, Some("u64-big-endian"), &StdFs)?;

    let manifest = decode_manifest(&path, &StdFs)?;
    assert_eq!(manifest.comparator_name, "u64-big-endian");
    Ok(())
}

#[test]
fn manifest_rejects_oversized_comparator_name() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("manifest");

    let long_name = "x".repeat(300);
    write_test_manifest(&path, Some(&long_name), &StdFs)?;

    let result = decode_manifest(&path, &StdFs);
    assert!(
        matches!(result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
        "expected DecompressedSizeTooLarge"
    );
    Ok(())
}

#[test]
fn manifest_rejects_invalid_utf8_comparator_name() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("manifest");

    // Compose a manifest where the comparator_name section
    // carries invalid UTF-8 bytes. Writer accepts arbitrary
    // bytes via write_all; the Manifest decoder enforces UTF-8.
    let mut writer = ManifestArchiveWriter::create(
        &path,
        &StdFs,
        std::sync::Arc::new(crate::runtime_config::RuntimeConfig::default()),
        None,
        crate::fs::SyncMode::Normal,
    )?;
    writer.start("format_version")?;
    writer.write_u8(FormatVersion::V5.into())?;
    writer.start("tree_type")?;
    writer.write_u8(TreeType::Standard.into())?;
    writer.start("level_count")?;
    writer.write_u8(7)?;
    writer.start("filter_hash_type")?;
    writer.write_u8(u8::from(ChecksumType::Xxh3))?;
    writer.start("comparator_name")?;
    writer.write_all(&[0xFF, 0xFE])?;
    writer.finish()?;

    let result = decode_manifest(&path, &StdFs);
    assert!(
        matches!(result, Err(crate::Error::Utf8(_))),
        "expected Utf8 error"
    );
    Ok(())
}

// ------------------------------------------------------------------
// MemFs tests — verify decode_from works with non-StdFs backends
// ------------------------------------------------------------------

#[test]
fn manifest_memfs_default_comparator() -> crate::Result<()> {
    let fs = MemFs::new();
    let dir = Path::new("/memfs");
    fs.create_dir_all(dir)?;
    let path = dir.join("manifest_default");

    write_test_manifest(&path, None, &fs)?;

    let manifest = decode_manifest(&path, &fs)?;
    assert_eq!(manifest.comparator_name, "default");
    assert_eq!(manifest.level_count, 7);
    assert!(matches!(manifest.version, FormatVersion::V5));
    assert!(matches!(manifest.tree_type, TreeType::Standard));
    Ok(())
}

#[test]
fn manifest_memfs_custom_comparator_round_trips() -> crate::Result<()> {
    let fs = MemFs::new();
    let dir = Path::new("/memfs");
    fs.create_dir_all(dir)?;
    let path = dir.join("manifest_custom");

    write_test_manifest(&path, Some("u64-big-endian"), &fs)?;

    let manifest = decode_manifest(&path, &fs)?;
    assert_eq!(manifest.comparator_name, "u64-big-endian");
    Ok(())
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::{
    FormatVersion, TreeType, checksum::ChecksumType, manifest_blocks::reader::ManifestArchiveReader,
};
#[cfg(not(feature = "std"))]
use alloc::{borrow::ToOwned, string::String};

pub struct Manifest {
    pub version: FormatVersion,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "deserialized from on-disk manifest, retained for validation; read in tests"
        )
    )]
    pub tree_type: TreeType,
    pub level_count: u8,
    pub comparator_name: String,
}

impl Manifest {
    /// Decode the `Manifest` metadata from a freshly-opened
    /// [`ManifestArchiveReader`]. Reads the mandatory sections
    /// (`format_version`, `tree_type`, `level_count`,
    /// `filter_hash_type`) and the optional `comparator_name`.
    ///
    /// The reader's per-section Block reads already cover XXH3 /
    /// optional ECC / optional AEAD; this function only parses the
    /// payload bytes.
    ///
    /// # Errors
    ///
    /// - [`crate::Error::ManifestSectionInvalid`] when a mandatory
    ///   section name is not present in the TOC (the per-section
    ///   error variant surfaced by
    ///   [`ManifestArchiveReader::read_section`])
    /// - [`crate::Error::InvalidVersion`] when `format_version`
    ///   carries an unknown discriminant
    /// - [`crate::Error::InvalidTag`] for unknown `TreeType` /
    ///   `ChecksumType` discriminants
    /// - [`crate::Error::DecompressedSizeTooLarge`] when
    ///   `comparator_name` exceeds the configured length cap
    /// - [`crate::Error::Utf8`] when `comparator_name` bytes are
    ///   not valid UTF-8
    /// - [`crate::Error::InvalidHeader`] when a single-byte mandatory
    ///   section is empty / truncated (`format_version`,
    ///   `tree_type`, `level_count`, `filter_hash_type`)
    /// - propagates Block I/O / verification errors from the
    ///   reader (including [`crate::Error::ManifestFooterInvalid`]
    ///   for footer-level corruption)
    pub fn decode_from(reader: &mut ManifestArchiveReader) -> Result<Self, crate::Error> {
        let format_version_bytes = reader.read_section("format_version")?;
        let version = {
            let v = format_version_bytes
                .first()
                .copied()
                .ok_or(crate::Error::InvalidHeader("format_version"))?;
            FormatVersion::try_from(v).map_err(|()| crate::Error::InvalidVersion(v))?
        };

        let tree_type_bytes = reader.read_section("tree_type")?;
        let tree_type = {
            let raw = tree_type_bytes
                .first()
                .copied()
                .ok_or(crate::Error::InvalidHeader("tree_type"))?;
            raw.try_into()
                .map_err(|()| crate::Error::InvalidTag(("TreeType", raw)))?
        };

        let level_count_bytes = reader.read_section("level_count")?;
        // Mirror format_version / tree_type above: a truncated /
        // empty section is structural corruption, not generic I/O.
        // `Cursor::read_u8` would surface Io(UnexpectedEof) which
        // is harder to route at the caller; the InvalidHeader
        // variant carries the section name for diagnostics and
        // matches the sibling sections' classification.
        let level_count = *level_count_bytes
            .first()
            .ok_or(crate::Error::InvalidHeader("level_count"))?;

        // Currently level count is hard coded to 7. The byte comes
        // from disk, so a corrupted / forged manifest could carry
        // any value here — return InvalidHeader instead of panicking
        // so the caller (Tree::open) gets a routable error rather
        // than a process abort.
        if level_count != 7 {
            return Err(crate::Error::InvalidHeader("level_count"));
        }

        {
            let filter_hash_type_bytes = reader.read_section("filter_hash_type")?;
            // Only one supported right now (and probably forever).
            // Same disk-sourced rationale as `level_count` above —
            // surface mismatch as InvalidHeader, not assert.
            if filter_hash_type_bytes.as_slice() != [u8::from(ChecksumType::Xxh3)] {
                return Err(crate::Error::InvalidHeader("filter_hash_type"));
            }
        }

        // Optional section — absent in manifests written before
        // comparator identity persistence was added. The
        // `UserComparator` trait was introduced in the same release
        // cycle, so all pre-existing trees used
        // `DefaultUserComparator` whose `name()` returns "default".
        // Custom comparators cannot exist in old manifests.
        let comparator_name = match reader.section("comparator_name") {
            Some(_entry) => {
                let bytes = reader.read_section("comparator_name")?;
                let limit = crate::comparator::MAX_COMPARATOR_NAME_BYTES as u64;
                if bytes.len() as u64 > limit {
                    return Err(crate::Error::DecompressedSizeTooLarge {
                        declared: bytes.len() as u64,
                        limit,
                    });
                }
                String::from_utf8(bytes).map_err(|e| crate::Error::Utf8(e.utf8_error()))?
            }
            None => "default".to_owned(),
        };

        Ok(Self {
            version,
            tree_type,
            level_count,
            comparator_name,
        })
    }
}

#[cfg(test)]
mod tests {
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
}

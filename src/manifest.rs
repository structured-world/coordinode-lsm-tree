// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    FormatVersion, TreeType,
    checksum::ChecksumType,
    fs::{Fs, open_section_reader},
};
use byteorder::ReadBytesExt;
use std::{io::Read, path::Path};

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
    pub fn decode_from(
        path: &Path,
        reader: &sfa::Reader,
        fs: &dyn Fs,
    ) -> Result<Self, crate::Error> {
        let toc = reader.toc();

        let version = {
            #[expect(
                clippy::expect_used,
                reason = "format_version section must exist in manifest"
            )]
            let section = toc
                .section(b"format_version")
                .expect("format_version section should exist in manifest");

            let mut reader = open_section_reader(fs, path, section)?;
            let version = reader.read_u8()?;
            FormatVersion::try_from(version).map_err(|()| crate::Error::InvalidVersion(version))?
        };

        let tree_type = {
            #[expect(
                clippy::expect_used,
                reason = "tree_type section must exist in manifest"
            )]
            let section = toc
                .section(b"tree_type")
                .expect("tree_type section should exist in manifest");

            let mut reader = open_section_reader(fs, path, section)?;
            let tree_type = reader.read_u8()?;
            tree_type
                .try_into()
                .map_err(|()| crate::Error::InvalidTag(("TreeType", tree_type)))?
        };

        let level_count = {
            #[expect(
                clippy::expect_used,
                reason = "level_count section must exist in manifest"
            )]
            let section = toc
                .section(b"level_count")
                .expect("level_count section should exist in manifest");

            let mut reader = open_section_reader(fs, path, section)?;
            reader.read_u8()?
        };

        // Currently level count is hard coded to 7
        assert_eq!(7, level_count, "level count should be 7");

        {
            let filter_hash_type = {
                #[expect(
                    clippy::expect_used,
                    reason = "filter_hash_type section must exist in manifest"
                )]
                let section = toc
                    .section(b"filter_hash_type")
                    .expect("filter_hash_type section should exist in manifest");

                open_section_reader(fs, path, section)?
                    .bytes()
                    .collect::<Result<Vec<_>, _>>()?
            };

            // Only one supported right now (and probably forever)
            assert_eq!(
                &[u8::from(ChecksumType::Xxh3)],
                &*filter_hash_type,
                "filter_hash_type should be XXH3"
            );
        }

        // Optional section — absent in manifests written before comparator
        // identity persistence was added. The `UserComparator` trait was
        // introduced in the same release cycle, so all pre-existing trees
        // used `DefaultUserComparator` whose `name()` returns "default".
        // Custom comparators cannot exist in old manifests.
        let comparator_name = match toc.section(b"comparator_name") {
            Some(section) => {
                let limit = crate::comparator::MAX_COMPARATOR_NAME_BYTES as u64;

                if section.len() > limit {
                    return Err(crate::Error::DecompressedSizeTooLarge {
                        declared: section.len(),
                        limit,
                    });
                }

                let mut bytes = Vec::new();
                open_section_reader(fs, path, section)?.read_to_end(&mut bytes)?;

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
    use crate::fs::{Fs, FsOpenOptions, StdFs};
    use byteorder::WriteBytesExt;
    use std::io::Write;

    /// Write the mandatory manifest sections (`format_version`, `tree_type`,
    /// `level_count`, `filter_hash_type`) into an sfa archive via the [`Fs`]
    /// trait. If `comparator_name` is `Some`, also writes that section.
    fn write_test_manifest(
        path: &std::path::Path,
        comparator_name: Option<&str>,
        fs: &dyn Fs,
    ) -> crate::Result<()> {
        let file = fs.open(
            path,
            &FsOpenOptions::new().write(true).create(true).truncate(true),
        )?;
        let mut writer = sfa::Writer::from_writer(std::io::BufWriter::new(file));

        writer.start("format_version")?;
        writer.write_u8(FormatVersion::V4.into())?;

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

    /// Decode a manifest from `path` using the given [`Fs`] backend.
    fn decode_manifest(path: &std::path::Path, fs: &dyn Fs) -> crate::Result<Manifest> {
        let mut file = fs.open(path, &FsOpenOptions::new().read(true))?;
        let reader = sfa::Reader::from_reader(&mut file)?;
        Manifest::decode_from(path, &reader, fs)
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

        // Write manifest with invalid UTF-8 bytes in comparator_name.
        // This needs raw Write access — write_test_manifest only handles
        // valid strings, so we inline the sfa construction here.
        let file = StdFs.open(
            &path,
            &FsOpenOptions::new().write(true).create(true).truncate(true),
        )?;
        let mut writer = sfa::Writer::from_writer(std::io::BufWriter::new(file));

        writer.start("format_version")?;
        writer.write_u8(FormatVersion::V4.into())?;
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
        use crate::fs::MemFs;

        let fs = MemFs::new();
        let dir = std::path::Path::new("/memfs");
        fs.create_dir_all(dir)?;
        let path = dir.join("manifest_default");

        write_test_manifest(&path, None, &fs)?;

        let manifest = decode_manifest(&path, &fs)?;
        assert_eq!(manifest.comparator_name, "default");
        assert_eq!(manifest.level_count, 7);
        assert!(matches!(manifest.version, FormatVersion::V4));
        assert!(matches!(manifest.tree_type, TreeType::Standard));
        Ok(())
    }

    #[test]
    fn manifest_memfs_custom_comparator_round_trips() -> crate::Result<()> {
        use crate::fs::MemFs;

        let fs = MemFs::new();
        let dir = std::path::Path::new("/memfs");
        fs.create_dir_all(dir)?;
        let path = dir.join("manifest_custom");

        write_test_manifest(&path, Some("u64-big-endian"), &fs)?;

        let manifest = decode_manifest(&path, &fs)?;
        assert_eq!(manifest.comparator_name, "u64-big-endian");
        Ok(())
    }
}

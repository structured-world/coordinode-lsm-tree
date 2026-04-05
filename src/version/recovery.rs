// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    Checksum, SeqNo, TableId, TreeType,
    coding::Decode,
    file::CURRENT_VERSION_FILE,
    fs::{Fs, FsOpenOptions, open_section_reader},
    version::VersionId,
    vlog::BlobFileId,
};
use byteorder::{LittleEndian, ReadBytesExt};
use std::path::Path;

/// Reads and validates the CURRENT version pointer file.
///
/// The file format is: `version_id: u64 | checksum: u128 | checksum_type: u8`
/// (25 bytes total, written atomically by `rewrite_atomic`).
///
/// Returns the version ID after verifying the checksum type tag is valid.
/// The checksum itself is not validated here — it is the hash of the version
/// *file* content, which is validated after the file is fully read in
/// [`recover()`].
pub fn get_current_version(folder: &Path, fs: &dyn Fs) -> crate::Result<VersionId> {
    use byteorder::{LittleEndian, ReadBytesExt};

    let path = folder.join(CURRENT_VERSION_FILE);
    let mut file = fs.open(&path, &FsOpenOptions::new().read(true))?;

    let version_id = file.read_u64::<LittleEndian>()?;
    let _checksum = file.read_u128::<LittleEndian>()?;
    let checksum_type = file.read_u8()?;

    // Validate checksum type tag — a non-zero value indicates corruption
    // or a file from an incompatible version (only xxh3 = 0 is supported).
    if checksum_type != 0 {
        return Err(crate::Error::InvalidTag(("ChecksumType", checksum_type)));
    }

    Ok(version_id)
}

pub struct RecoveredTable {
    pub id: TableId,
    pub checksum: Checksum,
    pub global_seqno: SeqNo,
}

pub struct Recovery {
    pub tree_type: TreeType,
    pub curr_version_id: VersionId,
    pub table_ids: Vec<Vec<Vec<RecoveredTable>>>,
    pub blob_file_ids: Vec<(BlobFileId, Checksum)>,
    pub gc_stats: crate::blob_tree::FragmentationMap,
}

pub fn recover(folder: &Path, fs: &dyn Fs) -> crate::Result<Recovery> {
    let curr_version_id = get_current_version(folder, fs)?;
    let version_file_path = folder.join(format!("v{curr_version_id}"));

    log::info!(
        "Recovering current manifest at {}",
        version_file_path.display(),
    );

    let mut file = fs.open(&version_file_path, &FsOpenOptions::new().read(true))?;
    let reader = sfa::Reader::from_reader(&mut file)?;
    let toc = reader.toc();

    // // TODO: vvv move into Version::decode vvv
    let mut levels = vec![];

    {
        let section = toc
            .section(b"tables")
            .ok_or(crate::Error::Unrecoverable)
            .inspect_err(|_| {
                log::error!("tables section not found in version #{curr_version_id} - maybe the file is corrupted?");
            })?;

        let mut reader = open_section_reader(fs, &version_file_path, section)?;

        let level_count = reader.read_u8()?;

        for _ in 0..level_count {
            let mut level = vec![];
            let run_count = reader.read_u8()?;

            for _ in 0..run_count {
                let mut run = vec![];
                let table_count = reader.read_u32::<LittleEndian>()?;

                // Bound by total section length (33 bytes per entry). Uses
                // section.len() because BufReader buffering makes
                // Take::limit() unreliable for remaining-byte checks.
                if u64::from(table_count) > section.len() / 33 {
                    return Err(crate::Error::Unrecoverable);
                }

                for _ in 0..table_count {
                    let id = reader.read_u64::<LittleEndian>()?;
                    let checksum_type = reader.read_u8()?;

                    if checksum_type != 0 {
                        return Err(crate::Error::InvalidTag(("ChecksumType", checksum_type)));
                    }

                    let checksum = reader.read_u128::<LittleEndian>()?;
                    let checksum = Checksum::from_raw(checksum);

                    let global_seqno = reader.read_u64::<LittleEndian>()?;

                    run.push(RecoveredTable {
                        id,
                        checksum,
                        global_seqno,
                    });
                }

                level.push(run);
            }

            levels.push(level);
        }
    }

    let blob_file_ids = {
        let section = toc
            .section(b"blob_files")
            .ok_or(crate::Error::Unrecoverable)
            .inspect_err(|_| {
                log::error!("blob_files section not found in version #{curr_version_id} - maybe the file is corrupted?");
            })?;

        let mut reader = open_section_reader(fs, &version_file_path, section)?;

        let blob_file_count = reader.read_u32::<LittleEndian>()?;

        // Bound by section payload (25 bytes per entry, minus 4-byte count header).
        if u64::from(blob_file_count) > section.len().saturating_sub(4) / 25 {
            return Err(crate::Error::Unrecoverable);
        }

        let mut blob_file_ids = Vec::with_capacity(blob_file_count as usize);

        for _ in 0..blob_file_count {
            let id = reader.read_u64::<LittleEndian>()?;

            let checksum_type = reader.read_u8()?;

            if checksum_type != 0 {
                return Err(crate::Error::InvalidTag(("ChecksumType", checksum_type)));
            }

            let checksum = reader.read_u128::<LittleEndian>()?;
            let checksum = Checksum::from_raw(checksum);

            blob_file_ids.push((id, checksum));
        }

        blob_file_ids.sort_by_key(|(id, _)| *id);
        blob_file_ids
    };

    debug_assert!(blob_file_ids.is_sorted_by_key(|(id, _)| id));

    let gc_stats = {
        let section = toc
            .section(b"blob_gc_stats")
            .ok_or(crate::Error::Unrecoverable)
            .inspect_err(|_| {
                log::error!("blob_gc_stats section not found in version #{curr_version_id} - maybe the file is corrupted?");
            })?;

        let mut reader = open_section_reader(fs, &version_file_path, section)?;

        crate::blob_tree::FragmentationMap::decode_from(&mut reader)?
    };

    Ok(Recovery {
        tree_type: {
            let section = toc.section(b"tree_type").ok_or(crate::Error::Unrecoverable)
            .inspect_err(|_|{
                log::error!("tree_type section not found in version #{curr_version_id} - maybe the file is corrupted?");
            })?;

            let mut reader = open_section_reader(fs, &version_file_path, section)?;
            let byte = reader.read_u8()?;

            TreeType::try_from(byte).map_err(|()| crate::Error::InvalidHeader("TreeType"))?
        },
        curr_version_id,
        table_ids: levels,
        blob_file_ids,
        gc_stats,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::{FsOpenOptions, MemFs};
    use byteorder::{LittleEndian, WriteBytesExt};

    /// Write a CURRENT pointer so `recover()` can find the version file.
    fn write_current(folder: &Path, version_id: u64, fs: &dyn Fs) -> crate::Result<()> {
        let path = folder.join(CURRENT_VERSION_FILE);
        let mut f = fs.open(
            &path,
            &FsOpenOptions::new().write(true).create(true).truncate(true),
        )?;
        f.write_u64::<LittleEndian>(version_id)?;
        f.write_u128::<LittleEndian>(0)?; // checksum placeholder
        f.write_u8(0)?; // checksum type
        Ok(())
    }

    /// Write a version sfa archive with a corrupt `table_count` (`u32::MAX`).
    ///
    /// All four sfa sections are written because `recover()` requires them
    /// all — only the tables section carries the corrupt payload.
    fn write_corrupt_table_count(folder: &Path, id: u64, fs: &dyn Fs) -> crate::Result<()> {
        let path = folder.join(format!("v{id}"));
        let file = fs.open(
            &path,
            &FsOpenOptions::new().write(true).create(true).truncate(true),
        )?;
        let mut w = sfa::Writer::from_writer(std::io::BufWriter::new(file));

        w.start("tree_type")?;
        w.write_u8(0)?;

        w.start("tables")?;
        w.write_u8(1)?; // 1 level
        w.write_u8(1)?; // 1 run
        w.write_u32::<LittleEndian>(u32::MAX)?; // corrupt: exceeds section length

        w.start("blob_files")?;
        w.write_u32::<LittleEndian>(0)?;

        w.start("blob_gc_stats")?;
        w.write_u32::<LittleEndian>(0)?;

        w.finish().map_err(|e| match e {
            sfa::Error::Io(e) => crate::Error::from(e),
            _ => crate::Error::Unrecoverable,
        })?;
        Ok(())
    }

    /// Write a version sfa archive with a corrupt `blob_file_count` (`u32::MAX`).
    ///
    /// All four sfa sections required by `recover()` are present — only the
    /// `blob_files` section carries the corrupt payload.
    fn write_corrupt_blob_count(folder: &Path, id: u64, fs: &dyn Fs) -> crate::Result<()> {
        let path = folder.join(format!("v{id}"));
        let file = fs.open(
            &path,
            &FsOpenOptions::new().write(true).create(true).truncate(true),
        )?;
        let mut w = sfa::Writer::from_writer(std::io::BufWriter::new(file));

        w.start("tree_type")?;
        w.write_u8(0)?;

        w.start("tables")?;
        w.write_u8(0)?; // 0 levels

        w.start("blob_files")?;
        w.write_u32::<LittleEndian>(u32::MAX)?; // corrupt

        w.start("blob_gc_stats")?;
        w.write_u32::<LittleEndian>(0)?;

        w.finish().map_err(|e| match e {
            sfa::Error::Io(e) => crate::Error::from(e),
            _ => crate::Error::Unrecoverable,
        })?;
        Ok(())
    }

    #[test]
    fn recover_rejects_corrupt_table_count() -> crate::Result<()> {
        let fs = MemFs::new();
        let folder = Path::new("/corrupt/tables");
        fs.create_dir_all(folder)?;

        write_current(folder, 1, &fs)?;
        write_corrupt_table_count(folder, 1, &fs)?;

        let Err(err) = recover(folder, &fs) else {
            panic!("corrupt table_count should fail");
        };
        assert!(
            matches!(err, crate::Error::Unrecoverable),
            "expected Unrecoverable, got: {err:?}"
        );

        Ok(())
    }

    #[test]
    fn recover_rejects_corrupt_blob_file_count() -> crate::Result<()> {
        let fs = MemFs::new();
        let folder = Path::new("/corrupt/blobs");
        fs.create_dir_all(folder)?;

        write_current(folder, 1, &fs)?;
        write_corrupt_blob_count(folder, 1, &fs)?;

        let Err(err) = recover(folder, &fs) else {
            panic!("corrupt blob_file_count should fail");
        };
        assert!(
            matches!(err, crate::Error::Unrecoverable),
            "expected Unrecoverable, got: {err:?}"
        );

        Ok(())
    }
}

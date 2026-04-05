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

pub fn get_current_version(folder: &Path, fs: &dyn Fs) -> crate::Result<VersionId> {
    use byteorder::{LittleEndian, ReadBytesExt};

    let path = folder.join(CURRENT_VERSION_FILE);
    let mut file = fs.open(&path, &FsOpenOptions::new().read(true))?;
    let version_id = file.read_u64::<LittleEndian>()?;
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

    // TODO: maybe validate current version using the checksum in "current"

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

        // Sanity-check: we support at most 255 levels (u8), but the section
        // must contain at least 1 byte per level (run_count). Guard against
        // corrupt files that would spin in a long loop on garbage data.
        // The actual hard limit is MAX_LEVELS from config, but we don't have
        // that here — u8::MAX is the wire-format ceiling.

        for _ in 0..level_count {
            let mut level = vec![];
            let run_count = reader.read_u8()?;

            for _ in 0..run_count {
                let mut run = vec![];
                let table_count = reader.read_u32::<LittleEndian>()?;

                // Each table entry is: id(8) + checksum_type(1) + checksum(16) + seqno(8) = 33 bytes.
                // Reject obviously corrupt counts that would cause huge allocations.
                if table_count > 10_000_000 {
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

        // Each blob entry is: id(8) + checksum_type(1) + checksum(16) = 25 bytes.
        // Reject corrupt counts that would cause huge allocations.
        if blob_file_count > 10_000_000 {
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

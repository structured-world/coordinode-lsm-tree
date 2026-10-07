// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! One blob file's meta items and value frames, read with the decoders the
//! read path uses.

use super::{BlobFileRecord, Section};
use crate::fs::{Fs, FsOpenOptions};
use crate::path::{Path, PathBuf};
use crate::table::DataBlock;
use crate::table::block::{BlockIdentity, BlockTransform, BlockType, ParsedItem};
use crate::vlog::blob_file::meta::{METADATA_HEADER_MAGIC, Metadata};
use crate::vlog::blob_file::scanner::Scanner;
use crate::{Checksum, SeqNo, UserKey, UserValue, vlog::BlobFileId};
use alloc::sync::Arc;

/// One value frame of a blob file, its checksum verified.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobFrame {
    /// Where the frame starts in the file: the offset value handles name.
    pub offset: u64,
    /// Where the next frame starts.
    pub frame_end: u64,
    /// The key the value was written under.
    pub key: UserKey,
    /// The value's local seqno.
    pub seqno: SeqNo,
    /// The value as stored, compressed under the file's codec.
    pub stored: UserValue,
    /// The value's length once decompressed.
    pub uncompressed_len: u32,
}

/// A blob file opened for export: its digest has been checked against the
/// manifest's and its meta block decoded; nothing is written.
pub struct BlobFileExport {
    path: PathBuf,
    id: BlobFileId,
    live_from: u64,
    fs: Arc<dyn Fs>,
    meta: Vec<(UserKey, UserValue)>,
    sections: Vec<Section>,
    created_at: u128,
    compression: crate::CompressionType,
}

impl BlobFileExport {
    /// Opens the blob file at `path` that the manifest records as `record`.
    /// `live_from` is the offset of its first live frame: `0`, or the frontier
    /// the manifest holds for a file whose consumed prefix was reclaimed.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::ChecksumMismatch`] when the file from
    /// `live_from` on does not hash to the manifest's checksum, and any error
    /// reading or decoding its trailer and meta block, including a meta block
    /// that names another id.
    pub fn open(
        path: &Path,
        record: &BlobFileRecord,
        live_from: u64,
        fs: Arc<dyn Fs>,
    ) -> crate::Result<Self> {
        let digest = crate::repair::compute_table_checksum_from(&*fs, path, live_from)?;
        if digest != record.checksum {
            return Err(crate::Error::ChecksumMismatch {
                got: Checksum::from_raw(digest),
                expected: Checksum::from_raw(record.checksum),
            });
        }

        let mut file = fs.open(path, &FsOpenOptions::new().read(true))?;
        let trailer = crate::sfa::Reader::from_reader(&mut file)?;
        let sections: Vec<Section> = trailer
            .toc()
            .iter()
            .map(|entry| Section {
                name: entry.name().to_vec(),
                offset: entry.pos(),
                len: entry.len(),
            })
            .collect();
        let meta_section = trailer
            .toc()
            .section(b"meta")
            .ok_or(crate::Error::InvalidHeader("BlobFileMeta"))?;
        let meta_len = usize::try_from(meta_section.len())
            .map_err(|_| crate::Error::InvalidHeader("BlobFileMeta"))?;
        let meta_bytes = crate::file::read_exact(&*file, meta_section.pos(), meta_len)?;

        // The decode the open runs: version, magic, required keys.
        let parsed = Metadata::from_slice(&meta_bytes)?;
        if parsed.id != record.id {
            return Err(crate::Error::InvalidHeader("BlobFileMeta"));
        }

        let mut reader = meta_bytes
            .get(METADATA_HEADER_MAGIC.len()..)
            .ok_or(crate::Error::InvalidHeader("BlobFileMeta"))?;
        let block = DataBlock::new(crate::table::Block::from_reader(
            &mut reader,
            BlockIdentity {
                table_id: 0,
                block_type: BlockType::Meta,
                dict_id: 0,
                window_log: 0,
            },
            &BlockTransform::PLAIN,
        )?);
        let meta = block
            .try_iter(crate::comparator::default_comparator())?
            .map(|item| {
                let item = item.materialize(block.as_slice());
                (item.key.user_key, item.value)
            })
            .collect();

        Ok(Self {
            path: path.to_path_buf(),
            id: record.id,
            live_from,
            fs,
            meta,
            sections,
            created_at: parsed.created_at,
            compression: parsed.compression,
        })
    }

    /// When the file was written: nanoseconds since the Unix epoch.
    #[must_use]
    pub fn created_at(&self) -> u128 {
        self.created_at
    }

    /// The codec every stored value of the file is compressed with.
    #[must_use]
    pub fn compression(&self) -> crate::CompressionType {
        self.compression
    }

    /// The blob file's id.
    #[must_use]
    pub fn id(&self) -> BlobFileId {
        self.id
    }

    /// The table of contents, in file order.
    #[must_use]
    pub fn sections(&self) -> &[Section] {
        &self.sections
    }

    /// Every key and value of the meta block, in key order.
    #[must_use]
    pub fn meta(&self) -> &[(UserKey, UserValue)] {
        &self.meta
    }

    /// Every live frame, in file order, each checksum verified by the read
    /// path's own scanner.
    ///
    /// # Errors
    ///
    /// Fails on the first frame that does not verify, and on a frame the
    /// scanner reached only by searching past damage: its start is not proven.
    pub fn frames(&self) -> crate::Result<Vec<BlobFrame>> {
        let scanner = if self.live_from == 0 {
            Scanner::new(&self.path, &*self.fs, self.id)?
        } else {
            Scanner::resume(&self.path, &*self.fs, self.id, self.live_from)?
        };
        scanner
            .map(|entry| {
                let entry = entry?;
                if entry.resynced {
                    return Err(crate::Error::InvalidHeader("Blob"));
                }
                Ok(BlobFrame {
                    offset: entry.offset,
                    frame_end: entry.frame_end,
                    key: entry.key,
                    seqno: entry.seqno,
                    stored: entry.value,
                    uncompressed_len: entry.uncompressed_len,
                })
            })
            .collect()
    }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The table half of [`crate::export`]: each part read from the file with the
//! decoder the read path uses, bypassing the block cache.

use super::block::{BlockIdentity, BlockTransform, BlockType, ParsedItem};
use super::block_index::BlockIndex;
use super::meta::ParsedMeta;
use super::{Block, BlockHandle, BlockOffset, DataBlock, IndexBlock, Table};
use crate::export::{BlockRef, BurrKind, Filter, FilterPartition, Locator, Section, decode_burr};
use crate::fs::{FsFile, FsOpenOptions};
use crate::{InternalValue, UserKey, UserValue};
use alloc::boxed::Box;

impl Table {
    fn export_file(&self) -> crate::Result<Box<dyn FsFile>> {
        Ok(self.fs.open(&self.path, &FsOpenOptions::new().read(true))?)
    }

    /// Reads one block of `block_type` at `handle`, rejecting a block that
    /// carries another role.
    fn export_block(
        &self,
        file: &dyn FsFile,
        handle: BlockHandle,
        block_type: BlockType,
        transform: &BlockTransform<'_>,
    ) -> crate::Result<Block> {
        let block = Block::from_file(
            file,
            handle,
            BlockIdentity {
                table_id: self.metadata.id,
                block_type,
                dict_id: 0,
                window_log: 0,
            },
            transform,
        )?;
        if block.header.block_type != block_type {
            return Err(crate::Error::InvalidTag((
                "BlockType",
                block.header.block_type.into(),
            )));
        }
        Ok(block)
    }

    /// The transform a block of `block_type` was written under: data blocks
    /// carry the data codec and dictionary, index blocks the index codec, a
    /// meta block its own parity flag, every other section only encryption
    /// and the table's parity scheme.
    fn export_transform(&self, block_type: BlockType) -> crate::Result<BlockTransform<'_>> {
        let coded = match block_type {
            BlockType::Data | BlockType::Columnar => BlockTransform::from_parts(
                self.metadata.data_block_compression,
                self.encryption.as_deref(),
                #[cfg(zstd_any)]
                self.zstd_dictionary.as_deref(),
            )?,
            BlockType::Index => BlockTransform::from_parts(
                self.metadata.index_block_compression,
                self.encryption.as_deref(),
                #[cfg(zstd_any)]
                None,
            )?,
            BlockType::Meta => {
                return Ok(match self.encryption.as_deref() {
                    Some(enc) => BlockTransform::Encrypted(enc),
                    None => BlockTransform::PLAIN,
                });
            }
            _ => return Ok(self.section_transform()),
        };
        Ok(match self.metadata.ecc_params {
            Some(ecc) => coded.with_ecc(ecc),
            None => coded,
        })
    }

    pub(crate) fn export_frame(
        &self,
        offset: u64,
        size: u32,
        block_type: BlockType,
    ) -> crate::Result<crate::export::VerifiedFrame> {
        let file = self.export_file()?;
        let transform = self.export_transform(block_type)?;
        let dict_id = match block_type {
            BlockType::Data | BlockType::Columnar => self.metadata.data_block_compression.dict_id(),
            _ => 0,
        };
        let (header, payload, status, recovery) = Block::read_verified_payload(
            &*file,
            BlockHandle::new(BlockOffset(offset), size),
            BlockIdentity {
                table_id: self.metadata.id,
                block_type,
                dict_id,
                window_log: 0,
            },
            &transform,
        )?;
        if header.block_type != block_type {
            return Err(crate::Error::InvalidTag((
                "BlockType",
                header.block_type.into(),
            )));
        }
        Ok(crate::export::VerifiedFrame {
            block_type,
            uncompressed_length: header.uncompressed_length,
            payload: payload.to_vec(),
            ecc_status: status,
            ecc_recovery: recovery,
        })
    }

    /// The digest the live region would have once every data block the parity
    /// trailers can repair is repaired, computed the way the in-place heal
    /// predicts it and without writing; `None` for a table without parity.
    #[cfg(feature = "page_ecc")]
    pub(crate) fn export_repaired_digest(&self) -> crate::Result<Option<u128>> {
        if self.metadata.ecc_params.is_none() {
            return Ok(None);
        }
        let file = self.export_file()?;
        let transform = self.export_transform(self.data_block_role())?;
        let (digest, _) =
            self.predict_heal_digest_and_offsets(&*file, &transform, self.punch_offset()?)?;
        Ok(Some(digest))
    }

    pub(crate) fn export_sections(&self) -> crate::Result<Vec<Section>> {
        let mut file = self.export_file()?;
        let trailer = crate::sfa::Reader::from_reader(&mut file)?;
        Ok(trailer
            .toc()
            .iter()
            .map(|entry| Section {
                name: entry.name().to_vec(),
                offset: entry.pos(),
                len: entry.len(),
            })
            .collect())
    }

    pub(crate) fn export_meta(&self) -> crate::Result<Vec<(UserKey, UserValue)>> {
        let file = self.export_file()?;
        let mut failure = None;
        // The tail copy first, then the mirror, as the open tries them: the
        // items come from the copy that decodes to what the open holds.
        for handle in [Some(self.regions.metadata), self.regions.metadata_mid]
            .into_iter()
            .flatten()
        {
            match self.export_meta_at(&*file, handle) {
                Ok(items) => return Ok(items),
                Err(e) => failure = Some(e),
            }
        }
        Err(failure.unwrap_or(crate::Error::InvalidHeader("TableMeta")))
    }

    fn export_meta_at(
        &self,
        file: &dyn FsFile,
        handle: BlockHandle,
    ) -> crate::Result<Vec<(UserKey, UserValue)>> {
        let parsed = ParsedMeta::load_with_handle(
            file,
            &handle,
            Some(self.metadata.id),
            self.encryption.as_deref(),
        )?;
        if parsed != self.metadata {
            return Err(crate::Error::InvalidHeader("TableMeta"));
        }
        // Meta blocks are written uncompressed and carry their own ECC flag.
        let transform = match self.encryption.as_deref() {
            Some(enc) => BlockTransform::Encrypted(enc),
            None => BlockTransform::PLAIN,
        };
        let block = DataBlock::new(self.export_block(file, handle, BlockType::Meta, &transform)?);
        Ok(block
            .try_iter(crate::comparator::default_comparator())?
            .map(|item| {
                let item = item.materialize(block.as_slice());
                (item.key.user_key, item.value)
            })
            .collect())
    }

    pub(crate) fn export_data_blocks(&self) -> crate::Result<Vec<BlockRef>> {
        self.block_index
            .iter()
            .map(|keyed| {
                keyed.map(|keyed| BlockRef {
                    end_key: keyed.end_key().clone(),
                    seqno: keyed.seqno(),
                    offset: keyed.offset().0,
                    size: keyed.size(),
                })
            })
            .collect()
    }

    pub(crate) fn export_rows(&self, block: &BlockRef) -> crate::Result<Vec<InternalValue>> {
        let handle = BlockHandle::new(BlockOffset(block.offset), block.size);
        #[cfg(feature = "columnar")]
        if self.metadata.columnar {
            let (loaded, _) = self.load_block_from_disk(&handle, BlockType::Columnar)?;
            let batch = crate::table::columnar::ColumnBatch::decode(&loaded.data)?;
            return crate::table::columnar::column_batch_to_entries(&batch);
        }
        let (loaded, _) = self.load_block_from_disk(&handle, BlockType::Data)?;
        let loaded = DataBlock::from_loaded(loaded, self.metadata.kv_checksum_algo.is_some())?;
        Ok(loaded
            .try_iter(self.comparator.clone())?
            .map(|item| item.materialize(loaded.as_slice()))
            .collect())
    }

    #[cfg(feature = "columnar")]
    pub(crate) fn export_columnar_batch(
        &self,
        block: &BlockRef,
    ) -> crate::Result<crate::table::columnar::ColumnBatch> {
        if !self.metadata.columnar {
            return Err(crate::Error::InvalidHeader(
                "columnar batch requested from a row table",
            ));
        }
        let handle = BlockHandle::new(BlockOffset(block.offset), block.size);
        let (loaded, _) = self.load_block_from_disk(&handle, BlockType::Columnar)?;
        crate::table::columnar::ColumnBatch::decode(&loaded.data)
    }

    #[cfg(feature = "columnar")]
    pub(crate) fn export_deleted_rows_in(&self, block: &BlockRef) -> crate::Result<Vec<u32>> {
        // A normal open fails on a damaged bitmap rather than degrading it, so
        // the decoded bitmap is the stored one.
        if self.delete_bitmap.is_empty() {
            return Ok(Vec::new());
        }
        // The bitmap numbers rows across every block of the table, punched
        // prefix included; the open maps each block to its first row from the
        // zone map, which is what makes a live block's rows addressable
        // without reading the blocks before it.
        let start = self
            .delete_block_starts
            .as_ref()
            .and_then(|starts| starts.get(&block.offset))
            .copied()
            .ok_or(crate::Error::InvalidHeader(
                "delete bitmap: no first row recorded for the block",
            ))?;
        let rows = self.export_columnar_batch(block)?.row_count;
        let mut deleted = Vec::new();
        for local in 0..rows {
            let position = start.checked_add(local).ok_or(crate::Error::InvalidHeader(
                "columnar: row position exceeds u32::MAX",
            ))?;
            if self.delete_bitmap.contains(position) {
                deleted.push(local);
            }
        }
        Ok(deleted)
    }

    pub(crate) fn export_range_tombstones(&self) -> Vec<crate::export::RangeDelete> {
        self.range_tombstones()
            .iter()
            .map(|rt| crate::export::RangeDelete {
                start: rt.start.clone(),
                end: rt.end.clone(),
                seqno: rt.seqno,
            })
            .collect()
    }

    pub(crate) fn export_filter(&self) -> crate::Result<Option<Filter>> {
        let file = self.export_file()?;
        let transform = self.section_transform();

        if let Some(index_handle) = self.regions.filter_tli {
            let index_transform = {
                let t = BlockTransform::from_parts(
                    self.metadata.index_block_compression,
                    self.encryption.as_deref(),
                    #[cfg(zstd_any)]
                    None,
                )?;
                match self.metadata.ecc_params {
                    Some(ecc) => t.with_ecc(ecc),
                    None => t,
                }
            };
            let index = IndexBlock::new(self.export_block(
                &*file,
                index_handle,
                BlockType::Index,
                &index_transform,
            )?);
            let mut partitions = Vec::new();
            for item in index.try_iter(self.comparator.clone())? {
                let keyed = item.materialize(index.as_slice());
                let entry = BlockRef {
                    end_key: keyed.end_key().clone(),
                    seqno: keyed.seqno(),
                    offset: keyed.offset().0,
                    size: keyed.size(),
                };
                let block =
                    self.export_block(&*file, keyed.into_inner(), BlockType::Filter, &transform)?;
                partitions.push(FilterPartition {
                    entry,
                    solution: decode_burr(&block.data, BurrKind::Membership)?,
                });
            }
            return Ok(Some(Filter::Partitioned(partitions)));
        }

        let Some(handle) = self.regions.filter else {
            return Ok(None);
        };
        let block = self.export_block(&*file, handle, BlockType::Filter, &transform)?;
        Ok(Some(Filter::Full(decode_burr(
            &block.data,
            BurrKind::Membership,
        )?)))
    }

    pub(crate) fn export_locator(&self) -> crate::Result<Option<Locator>> {
        use super::locator::{SECTION_HEADER_LEN, SECTION_VERSION};

        let Some(handle) = self.regions.locator else {
            return Ok(None);
        };
        let file = self.export_file()?;
        let block = self.export_block(
            &*file,
            handle,
            BlockType::Locator,
            &self.section_transform(),
        )?;
        let (header, payload) = block
            .data
            .split_at_checked(SECTION_HEADER_LEN)
            .ok_or(crate::Error::InvalidHeader("LocatorSection"))?;
        let [version, precision, block_id_bits, slot_bits] = *header else {
            return Err(crate::Error::InvalidHeader("LocatorSection"));
        };
        if version != SECTION_VERSION {
            return Err(crate::Error::InvalidHeader("LocatorSection version"));
        }
        Ok(Some(Locator {
            precision,
            block_id_bits,
            slot_bits,
            solution: decode_burr(payload, BurrKind::Retrieval)?,
        }))
    }
}

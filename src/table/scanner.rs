// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use super::{Block, DataBlock};
use crate::{
    CompressionType, InternalValue, SeqNo,
    comparator::SharedComparator,
    encryption::EncryptionProvider,
    fs::{FileHint, FsFile},
    table::{block::BlockType, iter::OwnedDataBlockIter},
};
use std::{fs::File, io::BufReader, path::Path, sync::Arc};

/// Table reader that is optimized for consuming an entire table
pub struct Scanner {
    reader: BufReader<File>,
    iter: OwnedDataBlockIter,

    compression: CompressionType,
    block_count: usize,
    read_count: usize,

    global_seqno: SeqNo,

    encryption: Option<Arc<dyn EncryptionProvider>>,
    comparator: SharedComparator,

    /// Per-SST Page-ECC scheme from table metadata: data blocks omit the
    /// `block_flags` byte, so the scanner's block-read transform is told
    /// here the parity scheme (sizing + recovery). `None` = no parity.
    ecc: Option<crate::table::block::EccParams>,
    /// Per-SST per-KV-footer flag (`kv_checksum_algo.is_some()`): supplied to
    /// `from_loaded` so it strips the footer (data blocks omit the byte).
    has_kv_footer: bool,

    #[cfg(zstd_any)]
    zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,

    /// Table id of the SST being scanned; threaded through to
    /// per-block reads via `BlockIdentity`.
    table_id: crate::TableId,

    /// Owning tree id, threaded into per-block `BlockIdentity` so encrypted
    /// blocks decrypt under the same AAD the writer sealed.
    tree_id: crate::tree::inner::TreeId,
}

impl Scanner {
    #[expect(
        clippy::too_many_arguments,
        reason = "scanner ctor takes one local per piece of state it needs to thread \
                  through fetch_next_block; collapsing them into a config struct would \
                  add an indirection without removing any per-call decision the caller \
                  makes about the values"
    )]
    pub fn new(
        path: &Path,
        block_count: usize,
        compression: CompressionType,
        global_seqno: SeqNo,
        encryption: Option<Arc<dyn EncryptionProvider>>,
        ecc: Option<crate::table::block::EccParams>,
        has_kv_footer: bool,
        #[cfg(zstd_any)] zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,
        comparator: SharedComparator,
        tree_id: crate::tree::inner::TreeId,
        table_id: crate::TableId,
    ) -> crate::Result<Self> {
        // 2 MiB buffer matches RocksDB's `compaction_readahead_size`
        // default and is large enough that the kernel can fold the
        // sequential-scan readahead heuristic into a single big
        // userspace fill per ~500 typical (4 KiB) data blocks instead
        // of one syscall every 8 blocks. Picked to dominate any
        // pre-#133-Phase1c micro-cost: the loss is at most a few MB
        // of allocator overhead per concurrent compaction, and
        // compaction concurrency is bounded by the scheduler. HDD-
        // tuning beyond this would benefit from a configurable knob,
        // tracked as the configurable-readahead follow-up under #133.
        const SCANNER_READAHEAD_BYTES: usize = 2 * 1024 * 1024;
        let file = File::open(path)?;
        // The scanner walks every block in order — tell the kernel so
        // it can ramp readahead aggressively and evict already-read
        // pages instead of pinning them. Best-effort: hint() is
        // advisory and any failure here would just leave the kernel
        // on the default heuristic, so drop the error rather than
        // failing the open.
        let _ = file.hint(FileHint::Sequential);
        let mut reader = BufReader::with_capacity(SCANNER_READAHEAD_BYTES, file);

        let block = Self::fetch_next_block(
            &mut reader,
            tree_id,
            table_id,
            compression,
            encryption.as_deref(),
            ecc,
            has_kv_footer,
            #[cfg(zstd_any)]
            zstd_dictionary.as_deref(),
        )?;
        let cmp = comparator.clone();
        let iter = OwnedDataBlockIter::try_new(block, |b| b.try_iter(cmp))?;

        Ok(Self {
            reader,
            iter,

            compression,
            block_count,
            read_count: 1,

            global_seqno,
            encryption,
            comparator,

            ecc,
            has_kv_footer,

            #[cfg(zstd_any)]
            zstd_dictionary,

            table_id,
            tree_id,
        })
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "per-block read threads each piece of decode state (tree/table id, codec, \
                  encryption, ecc, kv-footer, dict) needed to build the BlockIdentity + \
                  transform; a config struct would add indirection without removing a decision"
    )]
    fn fetch_next_block(
        reader: &mut BufReader<File>,
        tree_id: crate::tree::inner::TreeId,
        table_id: crate::TableId,
        compression: CompressionType,
        encryption: Option<&dyn EncryptionProvider>,
        ecc: Option<crate::table::block::EccParams>,
        has_kv_footer: bool,
        #[cfg(zstd_any)] zstd_dict: Option<&crate::compression::ZstdDictionary>,
    ) -> crate::Result<DataBlock> {
        let block = Block::from_reader(
            reader,
            crate::table::block::BlockIdentity {
                tree_id,
                table_id,
                block_type: BlockType::Data,
                dict_id: compression.dict_id(),
                window_log: 0,
            },
            &{
                // SST blocks omit the block_flags byte, so ECC presence is a
                // per-SST property: upgrade the transform to its `*Ecc`
                // variant when this table was written with Page ECC, so the
                // reader expects the parity trailer. Identity without the
                // `page_ecc` feature.
                let t = crate::table::block::BlockTransform::from_parts(
                    compression,
                    encryption,
                    #[cfg(zstd_any)]
                    zstd_dict,
                )?;
                if let Some(ecc) = ecc {
                    t.with_ecc(ecc)
                } else {
                    t
                }
            },
        );

        match block {
            Ok(block) => {
                // A data block is always BlockType::Data; `from_loaded`
                // strips the per-KV checksum footer when this SST carries one
                // (per-SST `has_kv_footer`, since data blocks omit the byte),
                // so the scan path is unchanged.
                if block.header.block_type != BlockType::Data {
                    return Err(crate::Error::InvalidTag((
                        "BlockType",
                        block.header.block_type.into(),
                    )));
                }

                DataBlock::from_loaded(block, has_kv_footer)
            }
            Err(e) => Err(e),
        }
    }
}

impl Iterator for Scanner {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(mut item) = self.iter.next() {
                item.key.seqno += self.global_seqno;
                return Some(Ok(item));
            }

            if self.read_count >= self.block_count {
                return None;
            }

            // Init new block
            let block = match Self::fetch_next_block(
                &mut self.reader,
                self.tree_id,
                self.table_id,
                self.compression,
                self.encryption.as_deref(),
                self.ecc,
                self.has_kv_footer,
                #[cfg(zstd_any)]
                self.zstd_dictionary.as_deref(),
            ) {
                Ok(block) => block,
                Err(e) => {
                    self.read_count = self.block_count;
                    return Some(Err(e));
                }
            };
            let cmp = self.comparator.clone();
            match OwnedDataBlockIter::try_new(block, |b| b.try_iter(cmp)) {
                Ok(iter) => {
                    self.iter = iter;
                    self.read_count += 1;
                }
                Err(e) => {
                    // Poison the scanner so callers cannot silently skip
                    // the corrupt block and resume on later blocks.
                    self.read_count = self.block_count;
                    return Some(Err(e));
                }
            }
        }
    }
}

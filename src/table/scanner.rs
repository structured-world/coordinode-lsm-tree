// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use super::{Block, DataBlock};
use crate::{
    CompressionType, InternalValue, SeqNo,
    comparator::SharedComparator,
    encryption::EncryptionProvider,
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

    #[cfg(zstd_any)]
    zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,

    /// Table id of the SST being scanned; threaded through to
    /// per-block reads via `BlockIdentity`.
    table_id: crate::TableId,
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
        #[cfg(zstd_any)] zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,
        comparator: SharedComparator,
        table_id: crate::TableId,
    ) -> crate::Result<Self> {
        // TODO: a larger buffer size may be better for HDD, maybe make this configurable
        let mut reader = BufReader::with_capacity(8 * 4_096, File::open(path)?);

        let block = Self::fetch_next_block(
            &mut reader,
            table_id,
            compression,
            encryption.as_deref(),
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

            #[cfg(zstd_any)]
            zstd_dictionary,

            table_id,
        })
    }

    fn fetch_next_block(
        reader: &mut BufReader<File>,
        table_id: crate::TableId,
        compression: CompressionType,
        encryption: Option<&dyn EncryptionProvider>,
        #[cfg(zstd_any)] zstd_dict: Option<&crate::compression::ZstdDictionary>,
    ) -> crate::Result<DataBlock> {
        let block = Block::from_reader(
            reader,
            crate::table::block::BlockIdentity {
                tree_id: 0,
                table_id,
                // Sequential scan via from_reader: BufReader<File>
                // does implement Seek (and could report position
                // via stream_position()), but querying it
                // invalidates the prefetched buffer and turns the
                // sequential scan into a series of seek+refill
                // cycles. The scan walks blocks in order so the
                // per-block offset isn't load-bearing for AAD
                // matching with the writer's per-block_offset
                // values — write/read agreement happens through
                // the file's structural layout, not per-block
                // identity. AAD wiring (#251) will either thread
                // a running offset accumulator here or accept
                // offset=0 for the scan path explicitly.
                block_offset: 0,
                block_type: BlockType::Data,
                dict_id: compression.dict_id(),
                window_log: 0,
            },
            compression,
            encryption,
            #[cfg(zstd_any)]
            zstd_dict,
        );

        match block {
            Ok(block) => {
                if block.header.block_type != BlockType::Data {
                    return Err(crate::Error::InvalidTag((
                        "BlockType",
                        block.header.block_type.into(),
                    )));
                }

                Ok(DataBlock::new(block))
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
                self.table_id,
                self.compression,
                self.encryption.as_deref(),
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

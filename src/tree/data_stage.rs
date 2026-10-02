// Copyright (c) 2026-present, Dmitry Prudnikov
// This source code is licensed under the Apache 2.0 License
// (found in the LICENSE-APACHE file in the repository)

//! One chunk of a level's data blocks as a machine: it point-reads the blocks
//! at hand, names the work the others need, and takes each block back as it
//! arrives. It never opens, reads or waits itself; whoever drives it carries
//! the work out.

use super::{BlockTask, TaskBlock, Tree};
use crate::{InternalValue, Table, TableId, fs::FsFile, table::BlockHandle};
use alloc::sync::Arc;
use alloc::vec::Vec;

/// A table file a chunk's reads go through, by table.
pub(super) type ChunkFile = (TableId, Arc<dyn FsFile>);

/// A chunk's file of a table, `None` while it is being opened.
pub(super) type ChunkSlot = (TableId, Option<Arc<dyn FsFile>>);

/// Work a chunk asks for before its blocks can be read.
pub(super) enum ChunkWork<'a> {
    /// A Page-ECC or columnar table's block, loaded through the table's own
    /// path, which heals a corrected ECC block and reconstructs a columnar
    /// one; handed back through [`ChunkRead::loaded`].
    Load {
        index: usize,
        table: &'a Table,
        handle: BlockHandle,
    },
    /// The file of `table`, opened before the chunk's reads from it; handed
    /// back through [`ChunkRead::opened`].
    Open { slot: usize, table: &'a Table },
}

/// A block to read whole at the handle's offset into `buf`, from the file in
/// `slot` of the chunk's files; handed back through [`ChunkRead::read`].
pub(super) struct ChunkFill<'a> {
    pub(super) index: usize,
    pub(super) table: &'a Table,
    pub(super) handle: BlockHandle,
    pub(super) slot: usize,
    pub(super) buf: Vec<u8>,
}

/// The read of one chunk of block tasks for `keys`.
pub(super) struct ChunkRead<'c, 'a, 'k, K> {
    chunk: &'c [BlockTask<'a>],
    keys: &'k [K],
    /// The reads of the blocks the chunk lacks, in task order.
    fills: Vec<ChunkFill<'a>>,
    /// The files those reads go through, once per table: a table's tasks are
    /// consecutive.
    files: Vec<ChunkSlot>,
    /// `(task index, key index, entry)` for every key found so far.
    hits: Vec<(usize, usize, InternalValue)>,
    /// The lowest task whose block failed to decode, and why: held rather than
    /// returned, so reads still in flight are not abandoned, and the lowest
    /// wins as a decode in task order would.
    decode_failure: Option<(usize, crate::Error)>,
}

impl<'c, 'a, 'k, K: AsRef<[u8]>> ChunkRead<'c, 'a, 'k, K> {
    /// Point-reads every task whose block is held, and returns the work the
    /// others need: a Page-ECC or columnar table's load, and the files to open
    /// for the reads [`Self::take_reads`] then hands out.
    ///
    /// `carried` is the previous chunk's last file: taken over when this
    /// chunk's first read is from its table, and let go otherwise, before any
    /// file of this chunk is asked for.
    ///
    /// # Errors
    ///
    /// A held block's point read fails, or a block to read has a size no block
    /// of its table can have (refused before its buffer is allocated).
    pub(super) fn start(
        chunk: &'c [BlockTask<'a>],
        cached: &[TaskBlock],
        keys: &'k [K],
        carried: &mut Option<ChunkFile>,
    ) -> crate::Result<(Self, Vec<ChunkWork<'a>>)> {
        let mut read = Self {
            chunk,
            keys,
            fills: Vec::new(),
            files: Vec::new(),
            hits: Vec::new(),
            decode_failure: None,
        };
        let mut work = Vec::new();
        for (index, (task, block)) in chunk.iter().zip(cached).enumerate() {
            match block {
                TaskBlock::Held(block) => {
                    Tree::read_task_keys(task, index, block, keys, &mut read.hits)?;
                }
                TaskBlock::Load => work.push(ChunkWork::Load {
                    index,
                    table: task.table,
                    handle: task.handle,
                }),
                TaskBlock::Read => {}
            }
        }
        let mut carry = carried.take();
        for (index, (task, block)) in chunk.iter().zip(cached).enumerate() {
            if !matches!(block, TaskBlock::Read) {
                continue;
            }
            let buf = task.table.block_buffer(&task.handle)?;
            let id = task.table.id();
            if read.files.last().is_none_or(|&(last, _)| last != id) {
                let file = match carry.take() {
                    Some((carried_id, file)) if carried_id == id => Some(file),
                    // Another table's file is let go before this one is asked
                    // for: a scrutinee left unbound lives to the end of the
                    // match.
                    other => {
                        drop(other);
                        work.push(ChunkWork::Open {
                            slot: read.files.len(),
                            table: task.table,
                        });
                        None
                    }
                };
                read.files.push((id, file));
            }
            read.fills.push(ChunkFill {
                index,
                table: task.table,
                handle: task.handle,
                slot: read.files.len() - 1,
                buf,
            });
        }
        Ok((read, work))
    }

    /// Takes back the file opened for `slot`.
    pub(super) fn opened(&mut self, slot: usize, file: Arc<dyn FsFile>) {
        if let Some((_, held)) = self.files.get_mut(slot) {
            *held = Some(file);
        }
    }

    /// Hands out the chunk's reads in task order and the files they go
    /// through, once every file asked for is back.
    pub(super) fn take_reads(&mut self) -> (Vec<ChunkFill<'a>>, Vec<ChunkSlot>) {
        (
            core::mem::take(&mut self.fills),
            core::mem::take(&mut self.files),
        )
    }

    /// Takes back the load of task `index`: `None` when the table holds no
    /// key there (a columnar block its delete mask removes whole).
    ///
    /// # Errors
    ///
    /// The point read of the block fails.
    pub(super) fn loaded(
        &mut self,
        index: usize,
        block: Option<crate::table::DataBlock>,
    ) -> crate::Result<()> {
        let (Some(block), Some(task)) = (block, self.chunk.get(index)) else {
            return Ok(());
        };
        Tree::read_task_keys(task, index, &block, self.keys, &mut self.hits)
    }

    /// Takes back the read of task `index` as its on-disk `bytes`: decoded,
    /// kept in the cache while it fits in `keep_room`, and point-read.
    pub(super) fn read(&mut self, index: usize, bytes: &[u8], keep_room: &mut u64) {
        let Some(task) = self.chunk.get(index) else {
            return;
        };
        if let Err(e) =
            Tree::resolve_block_task(task, index, bytes, keep_room, self.keys, &mut self.hits)
            && self
                .decode_failure
                .as_ref()
                .is_none_or(|(held, _)| index < *held)
        {
            self.decode_failure = Some((index, e));
        }
    }

    /// Applies what the chunk found once every block is back, keeping the
    /// highest-seqno hit per key in `results`, and adds the `(table, key)`
    /// pairs a read found to `found` when it is kept.
    ///
    /// Hits are applied in task order: tasks follow the level's runs newest
    /// first, and at an equal seqno the first task's entry is the one a
    /// single-key read returns, whatever order the blocks arrived in.
    ///
    /// # Errors
    ///
    /// The lowest task whose block failed to decode.
    pub(super) fn finish(
        self,
        results: &mut [Option<InternalValue>],
        found: Option<&mut Vec<(crate::TableId, usize)>>,
    ) -> crate::Result<()> {
        let Self {
            chunk,
            mut hits,
            decode_failure,
            ..
        } = self;
        if let Some((_, e)) = decode_failure {
            return Err(e);
        }
        // A task holds each key once, so only the order across tasks matters.
        hits.sort_unstable_by_key(|&(task, _, _)| task);
        if let Some(found) = found {
            found.extend(
                hits.iter()
                    .filter_map(|&(task, kidx, _)| Some((chunk.get(task)?.table.id(), kidx))),
            );
        }
        for (_, kidx, item) in hits {
            Tree::keep_highest(results, kidx, item);
        }
        Ok(())
    }
}

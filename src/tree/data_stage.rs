// Copyright (c) 2026-present, Dmitry Prudnikov
// This source code is licensed under the Apache 2.0 License
// (found in the LICENSE-APACHE file in the repository)

//! One chunk of a level's data blocks as a machine: it point-reads the blocks
//! at hand, names the work the others need, and takes each block back as it
//! arrives. It never opens, reads or waits itself; whoever drives it carries
//! the work out.

use super::{BlockTask, TaskBlock, Tree};
use crate::{InternalValue, Table, table::BlockHandle};
use alloc::vec::Vec;

/// A block a chunk lacks.
pub(super) enum ChunkWork<'a> {
    /// A Page-ECC or columnar table's block, loaded through the table's own
    /// path, which heals a corrected ECC block and reconstructs a columnar
    /// one; handed back through [`ChunkRead::loaded`].
    Load {
        index: usize,
        table: &'a Table,
        handle: BlockHandle,
    },
    /// A block to read from its table's file into `buf`, the whole of it at
    /// the handle's offset; handed back through [`ChunkRead::read`].
    Read {
        index: usize,
        table: &'a Table,
        handle: BlockHandle,
        buf: Vec<u8>,
    },
}

/// The read of one chunk of block tasks for `keys`.
pub(super) struct ChunkRead<'c, 'a, 'k, K> {
    chunk: &'c [BlockTask<'a>],
    keys: &'k [K],
    /// `(task index, key index, entry)` for every key found so far.
    hits: Vec<(usize, usize, InternalValue)>,
    /// The lowest task whose block failed to decode, and why: held rather than
    /// returned, so reads still in flight are not abandoned, and the lowest
    /// wins as a decode in task order would.
    decode_failure: Option<(usize, crate::Error)>,
}

impl<'c, 'a, 'k, K: AsRef<[u8]>> ChunkRead<'c, 'a, 'k, K> {
    /// Point-reads every task whose block is held, and returns the work the
    /// others need, a Page-ECC or columnar table's load and every read in task
    /// order.
    ///
    /// # Errors
    ///
    /// A held block's point read fails, or a block to read has a size no block
    /// of its table can have (refused before its buffer is allocated).
    pub(super) fn start(
        chunk: &'c [BlockTask<'a>],
        cached: &[TaskBlock],
        keys: &'k [K],
    ) -> crate::Result<(Self, Vec<ChunkWork<'a>>)> {
        let mut read = Self {
            chunk,
            keys,
            hits: Vec::new(),
            decode_failure: None,
        };
        let mut loads = Vec::new();
        for (index, (task, block)) in chunk.iter().zip(cached).enumerate() {
            match block {
                TaskBlock::Held(block) => {
                    Tree::read_task_keys(task, index, block, keys, &mut read.hits)?;
                }
                TaskBlock::Load => loads.push(ChunkWork::Load {
                    index,
                    table: task.table,
                    handle: task.handle,
                }),
                TaskBlock::Read => {}
            }
        }
        let mut work = loads;
        for (index, (task, block)) in chunk.iter().zip(cached).enumerate() {
            if matches!(block, TaskBlock::Read) {
                work.push(ChunkWork::Read {
                    index,
                    table: task.table,
                    handle: task.handle,
                    buf: task.table.block_buffer(&task.handle)?,
                });
            }
        }
        Ok((read, work))
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

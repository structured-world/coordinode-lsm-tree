// Copyright (c) 2026-present, Dmitry Prudnikov
// This source code is licensed under the Apache 2.0 License
// (found in the LICENSE-APACHE file in the repository)

//! One chunk of a level's data blocks as a machine: it point-reads the blocks
//! at hand, names the work the others need, and takes each block back as it
//! arrives. It never opens, reads or waits itself; whoever drives it carries
//! the work out.

use super::read_job::{BlockRequest, Job};
use super::{BlockTask, TaskBlock, Tree};
use crate::{InternalValue, TableId, fs::FsFile};
use alloc::sync::Arc;
use alloc::vec::Vec;

/// A table file a chunk's reads go through, by table.
pub(super) type ChunkFile = (TableId, Arc<dyn FsFile>);

/// A block to read whole into `buf`, the block of task `index`, from the file
/// in `slot` of the chunk's files.
struct Fill {
    index: usize,
    slot: usize,
    buf: Vec<u8>,
}

/// The read of one chunk of block tasks for `keys`. The chunk's tasks are
/// not held: every call takes them again, and the indices it takes and hands
/// out are positions in them.
pub(super) struct ChunkRead<'k, K> {
    keys: &'k [K],
    /// The reads of the blocks the chunk lacks, in task order.
    fills: Vec<Fill>,
    /// The files those reads go through, once per table (a table's tasks are
    /// consecutive), `None` while a file is being opened.
    files: Vec<(TableId, Option<Arc<dyn FsFile>>)>,
    /// `(task index, key index, entry)` for every key found so far.
    hits: Vec<(usize, usize, InternalValue)>,
    /// The lowest task whose block failed to decode, and why: held rather than
    /// returned, so reads still in flight are not abandoned, and the lowest
    /// wins as a decode in task order would.
    decode_failure: Option<(usize, crate::Error)>,
}

impl<'k, K: AsRef<[u8]>> ChunkRead<'k, K> {
    /// Point-reads every task of `chunk` whose block is held, and returns the
    /// jobs the others need first: a Page-ECC or columnar table's load,
    /// handed back through [`Self::loaded`], and the files to open for the
    /// reads [`Self::take_reads`] then hands out, handed back through
    /// [`Self::opened`].
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
        chunk: &[BlockTask<'_>],
        cached: &[TaskBlock],
        keys: &'k [K],
        carried: &mut Option<ChunkFile>,
    ) -> crate::Result<(Self, Vec<Job>)> {
        let mut read = Self {
            keys,
            fills: Vec::new(),
            files: Vec::new(),
            hits: Vec::new(),
            decode_failure: None,
        };
        let mut jobs = Vec::new();
        for (index, (task, block)) in chunk.iter().zip(cached).enumerate() {
            match block {
                TaskBlock::Held(block) => {
                    Tree::read_task_keys(task, index, block, keys, &mut read.hits)?;
                }
                TaskBlock::Load => jobs.push(Job::Load {
                    index,
                    table: task.at,
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
                        jobs.push(Job::Open {
                            tag: read.files.len(),
                            table: task.at,
                        });
                        None
                    }
                };
                read.files.push((id, file));
            }
            read.fills.push(Fill {
                index,
                slot: read.files.len() - 1,
                buf,
            });
        }
        Ok((read, jobs))
    }

    /// Takes back the file opened for the job tagged `slot`.
    pub(super) fn opened(&mut self, slot: usize, file: Arc<dyn FsFile>) {
        if let Some((_, held)) = self.files.get_mut(slot) {
            *held = Some(file);
        }
    }

    /// Hands out the reads of `chunk`'s blocks in task order, each tagged
    /// with its task's index, once every file asked for is back, and charges
    /// them as issued. The last file is left in `carried` for the next chunk,
    /// which takes it when it goes on with the same table.
    ///
    /// # Errors
    ///
    /// A file a read goes through was never handed back.
    pub(super) fn take_reads<'a>(
        &mut self,
        chunk: &[BlockTask<'a>],
        carried: &mut Option<ChunkFile>,
    ) -> crate::Result<Vec<BlockRequest<'a>>> {
        let mut reads = Vec::with_capacity(self.fills.len());
        for Fill { index, slot, buf } in core::mem::take(&mut self.fills) {
            let (Some(task), Some((_, Some(file)))) = (chunk.get(index), self.files.get(slot))
            else {
                return Err(crate::Error::Io(crate::io::Error::new(
                    crate::io::ErrorKind::Other,
                    "a chunk read was handed out before its file was opened",
                )));
            };
            // These reads bypass the per-block load path that charges every
            // other read.
            task.table.record_batched_read(
                crate::table::block::BlockType::Data,
                core::slice::from_ref(&task.handle),
            );
            reads.push(BlockRequest {
                tag: index,
                table: task.table,
                file: Arc::clone(file),
                offset: *task.handle.offset(),
                buf,
            });
        }
        *carried = core::mem::take(&mut self.files)
            .into_iter()
            .last()
            .and_then(|(id, file)| Some((id, file?)));
        Ok(reads)
    }

    /// Takes back the load of task `index` of `chunk`: `None` when the table
    /// holds no key there (a columnar block its delete mask removes whole).
    ///
    /// # Errors
    ///
    /// The point read of the block fails.
    pub(super) fn loaded(
        &mut self,
        chunk: &[BlockTask<'_>],
        index: usize,
        block: Option<crate::table::DataBlock>,
    ) -> crate::Result<()> {
        let (Some(block), Some(task)) = (block, chunk.get(index)) else {
            return Ok(());
        };
        Tree::read_task_keys(task, index, &block, self.keys, &mut self.hits)
    }

    /// Takes back the read of task `index` of `chunk` as its on-disk `bytes`:
    /// decoded, kept in the cache while it fits in `keep_room`, and
    /// point-read.
    pub(super) fn read(
        &mut self,
        chunk: &[BlockTask<'_>],
        index: usize,
        bytes: &[u8],
        keep_room: &mut u64,
    ) {
        let Some(task) = chunk.get(index) else {
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

    /// Applies what `chunk` found once every block is back, keeping the
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
        chunk: &[BlockTask<'_>],
        results: &mut [Option<InternalValue>],
        found: Option<&mut Vec<(crate::TableId, usize)>>,
    ) -> crate::Result<()> {
        let Self {
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

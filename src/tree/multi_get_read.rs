// Copyright (c) 2026-present, Dmitry Prudnikov
// This source code is licensed under the Apache 2.0 License
// (found in the LICENSE-APACHE file in the repository)

//! A multi-get as a machine: the memtables answer what they hold when it
//! starts, the version's levels are read for the rest, and each key's newest
//! version is then turned into its value: a separated value is read through
//! the read's queue, and a merge walks its sources table by table, the data
//! blocks of each table it reads coming through the queue. It reads the one
//! version its context holds for the whole read and never opens, reads or
//! waits itself.

use super::Tree;
use super::read_job::{
    BlockRequest, Job, JobDone, MergeLocated, ReadCtx, ReadMachine, ReadSink, TableAt, Values,
};
use super::tables_read::TablesRead;
use crate::blob_tree::{ValuePlan, ValueRead};
use crate::fs::FsFile;
use crate::range::{MergeTable, MergeWalk, TableFilter};
use crate::table::BlockHandle;
use crate::{InternalValue, Table, UserValue};
use alloc::sync::Arc;
use alloc::vec::Vec;

/// A separated value read through the read's queue: the key it is for, its
/// indirection, the record to read and the blob file it is in.
struct BlobRead<'a> {
    idx: usize,
    item: InternalValue,
    read: ValueRead<'a>,
    /// The slot of its blob file in [`MultiGetRead::blob_files`].
    slot: usize,
}

/// A read out after the levels: a separated value's record, or a data block
/// of the table a merge is reading.
enum ValueReadOut<'a> {
    Blob(BlobRead<'a>),
    MergeBlock {
        table: &'a Table,
        handle: BlockHandle,
        /// The merges waiting on it, each by its slot in
        /// [`MultiGetRead::merges`] and the block's place among the table's
        /// blocks of its key.
        waiters: Vec<(usize, usize)>,
    },
}

/// The merge of one key, walked table by table.
struct MergeKey<'a> {
    idx: usize,
    walk: MergeWalk<'a>,
    /// The table being read, once handed out.
    table: Option<MergeTable<'a>>,
    /// What that table's filters said, once located.
    filter: Option<TableFilter>,
    /// The table's blocks that can hold versions of the key, in index order:
    /// `None` while one is being read.
    blocks: Vec<Option<crate::table::DataBlock>>,
    /// Its blocks still being read.
    waiting: usize,
    /// A block of the table failed to read or decode.
    failed: bool,
}

/// The multi-get of the keys of its context.
pub(super) struct MultiGetRead<'a, K> {
    ctx: &'a ReadCtx<K>,
    /// Each key's newest version, from the memtables or the levels.
    entries: Vec<Option<InternalValue>>,
    /// `(duplicate index, representative index)` of keys asked more than
    /// once: the levels are read for the representative only.
    duplicates: Vec<(usize, usize)>,
    /// The read of the levels, while it goes on.
    tables: Option<TablesRead<'a, K>>,
    /// Whether every entry was turned into a value or handed out as a job.
    resolved: bool,
    results: Vec<Option<UserValue>>,
    /// Value jobs not yet back.
    jobs: usize,
    /// The failure of the lowest key a value job failed for: the one a
    /// resolve in key order would report.
    failure: Option<(usize, crate::Error)>,
    /// The reads out after the levels, by tag; `None` once back.
    reads: Vec<Option<ValueReadOut<'a>>>,
    /// The blob files the value reads go through, each opened once: `None`
    /// while its open is out.
    blob_files: Vec<(crate::vlog::BlobFileId, Option<Arc<dyn FsFile>>)>,
    /// Values whose read failed or did not check out, to read on their own
    /// path, which reports what is wrong as a single read would.
    alone: Vec<(usize, InternalValue)>,
    /// The merges walked, by slot; `None` once over.
    merges: Vec<Option<MergeKey<'a>>>,
    /// Merges whose blocks are all back, to take the versions of next.
    merges_ready: Vec<usize>,
    /// The tag of each merge block being read, by table and offset: a merge
    /// needing a block another merge is reading waits on that read.
    merge_blocks_out: crate::HashMap<(crate::GlobalTableId, u64), usize>,
    /// What the merges' blocks may still take of the cache, as a level read
    /// keeps its blocks within half of it; set by the first block back.
    merge_room: Option<u64>,
}

impl<'a, K: AsRef<[u8]>> MultiGetRead<'a, K> {
    /// The multi-get of `ctx`'s keys: the memtables are read here, and the
    /// keys they do not hold are sorted under the read's comparator for the
    /// levels.
    #[expect(
        clippy::indexing_slicing,
        reason = "indices are generated from 0..n, always in bounds"
    )]
    pub(super) fn new(ctx: &'a ReadCtx<K>) -> Self {
        let keys = &ctx.keys;
        let n = keys.len();
        let super_version = &ctx.super_version;
        let comparator = ctx.comparator.as_ref();
        // The memtables first, unsorted: a lookup there is O(log n) per key
        // regardless of order, so a batch they answer whole skips the sort and
        // the hashing.
        let mut entries: Vec<Option<InternalValue>> = alloc::vec![None; n];
        let mut remaining: Vec<usize> = Vec::with_capacity(n);
        for idx in 0..n {
            let key = keys[idx].as_ref();
            if let Some(entry) = super_version.active_memtable.get(key, ctx.seqno) {
                entries[idx] = Some(entry);
                continue;
            }
            if let Some(entry) =
                Tree::get_internal_entry_from_sealed_memtables(super_version, key, ctx.seqno)
            {
                entries[idx] = Some(entry);
                continue;
            }
            remaining.push(idx);
        }

        let mut duplicates = Vec::new();
        let tables = (!remaining.is_empty()).then(|| {
            remaining.sort_by(|&a, &b| comparator.compare(keys[a].as_ref(), keys[b].as_ref()));
            // The levels take strictly sorted, unique keys: an equal key asked
            // again is answered from its representative.
            let (miss_keys, dups) = Tree::dedup_sorted_miss_keys(&remaining, keys, comparator);
            duplicates = dups;
            TablesRead::new(ctx, miss_keys)
        });

        Self {
            ctx,
            entries,
            duplicates,
            tables,
            resolved: false,
            results: alloc::vec![None; n],
            jobs: 0,
            failure: None,
            reads: Vec::new(),
            blob_files: Vec::new(),
            alone: Vec::new(),
            merges: Vec::new(),
            merges_ready: Vec::new(),
            merge_blocks_out: crate::HashMap::default(),
            merge_room: None,
        }
    }

    /// Turns every key's newest version into its value: a tombstone, or a
    /// version a range tombstone covers, has none; a merge operand, with a
    /// merge operator, and a blob indirection are handed out as jobs.
    #[expect(
        clippy::indexing_slicing,
        reason = "indices are generated from 0..n, always in bounds"
    )]
    fn resolve(&mut self, out: &mut impl ReadSink<'a>) {
        let ctx = self.ctx;
        for idx in 0..self.entries.len() {
            let Some(entry) = self.entries[idx].take() else {
                continue;
            };
            if entry.is_tombstone() {
                continue;
            }
            if Tree::is_suppressed_by_range_tombstones(
                &ctx.super_version,
                ctx.key(idx),
                entry.key.seqno,
                ctx.seqno,
                ctx.comparator.as_ref(),
            ) {
                continue;
            }
            if entry.key.value_type.is_merge_operand() {
                // Without a merge operator the operand is the value, as a
                // single-key read returns it. Operands are stored inline in a
                // blob tree too, so the merge result is a plain value.
                if ctx.merge_operator.is_some() {
                    self.jobs += 1;
                    self.merges.push(Some(MergeKey {
                        idx,
                        walk: MergeWalk::new(&ctx.super_version, ctx.key(idx), ctx.seqno),
                        table: None,
                        filter: None,
                        blocks: Vec::new(),
                        waiting: 0,
                        failed: false,
                    }));
                    self.step_merge(self.merges.len() - 1, out);
                } else {
                    self.results[idx] = Some(entry.value);
                }
                continue;
            }
            // A cell row resolves its references from the value log as an
            // indirection does.
            let Values::Blob(source) = &ctx.values else {
                self.results[idx] = Some(entry.value);
                continue;
            };
            if !entry.key.value_type.is_indirection() && !entry.key.value_type.is_cell_row() {
                self.results[idx] = Some(entry.value);
                continue;
            }
            match source.plan_value(ctx.version(), &entry) {
                ValuePlan::Ready(value) => self.results[idx] = Some(value),
                ValuePlan::Alone => {
                    out.job(Job::Blob { idx, item: entry });
                    self.jobs += 1;
                }
                ValuePlan::Read(read) => {
                    let id = read.vhandle.blob_file_id;
                    let held = self.blob_files.iter().position(|&(held, _)| held == id);
                    let slot = held.unwrap_or_else(|| {
                        out.job(Job::OpenBlob {
                            tag: self.blob_files.len(),
                            blob_file: id,
                        });
                        self.blob_files.push((id, None));
                        self.blob_files.len() - 1
                    });
                    self.reads.push(Some(ValueReadOut::Blob(BlobRead {
                        idx,
                        item: entry,
                        read,
                        slot,
                    })));
                    self.jobs += 1;
                }
            }
        }
    }

    /// Takes back the blob file opened for `slot` and hands `out` the value
    /// reads waiting on it; reads of a file that would not open go on their
    /// own path, which reports why.
    fn blob_opened<S: ReadSink<'a>>(
        &mut self,
        slot: usize,
        file: crate::Result<Arc<dyn FsFile>>,
        out: &mut S,
    ) {
        let Values::Blob(source) = &self.ctx.values else {
            debug_assert!(false, "a blob file opened for a read of inline values");
            return;
        };
        let version = self.ctx.version();
        match file {
            Ok(file) => {
                for (tag, pending) in self.reads.iter().enumerate() {
                    let Some(ValueReadOut::Blob(pending)) = pending else {
                        continue;
                    };
                    if pending.slot != slot {
                        continue;
                    }
                    source.count_issued(version, &pending.read);
                    out.read(BlockRequest {
                        tag,
                        fs: &pending.read.blob_file.0.fs,
                        cache: &source.cache,
                        file: Arc::clone(&file),
                        offset: pending.read.vhandle.offset,
                        buf: alloc::vec![0u8; pending.read.len],
                    });
                }
                if let Some((_, held)) = self.blob_files.get_mut(slot) {
                    *held = Some(file);
                }
            }
            Err(_) => {
                for pending in &mut self.reads {
                    if let Some(ValueReadOut::Blob(read)) = pending.take_if(
                        |pending| matches!(pending, ValueReadOut::Blob(read) if read.slot == slot),
                    ) {
                        self.alone.push((read.idx, read.item));
                    }
                }
            }
        }
    }

    /// Takes back a read out after the levels.
    fn value_read_done(&mut self, done: crate::fs::ReadDone) {
        match self.reads.get_mut(done.tag).and_then(Option::take) {
            Some(ValueReadOut::Blob(pending)) => self.blob_read_done(pending, done),
            Some(ValueReadOut::MergeBlock {
                table,
                handle,
                waiters,
            }) => self.merge_block_done(table, &handle, &waiters, &done),
            None => debug_assert!(false, "a read handed back twice or never asked for"),
        }
    }

    /// Takes back a value read: checked and taken as the key's value, or, when
    /// it failed or does not check out, left to the value's own path.
    fn blob_read_done(&mut self, pending: BlobRead<'a>, done: crate::fs::ReadDone) {
        let file = self
            .blob_files
            .get(pending.slot)
            .and_then(|(_, file)| file.as_ref());
        let value = match (&self.ctx.values, file, done.result) {
            (Values::Blob(source), Some(file), Ok(())) => source
                .finish_value(
                    self.ctx.version(),
                    &pending.item.key.user_key,
                    &pending.read,
                    file.as_ref(),
                    &crate::Slice::from(done.buf),
                )
                .ok(),
            _ => None,
        };
        match value {
            Some(value) => self.value_done(pending.idx, Ok(Some(value))),
            None => self.alone.push((pending.idx, pending.item)),
        }
    }

    /// Takes the final value of the key at `idx`, or the failure of reading
    /// it: the lowest key's failure is the one a resolve in key order reports.
    fn value_done(&mut self, idx: usize, value: crate::Result<Option<UserValue>>) {
        self.jobs -= 1;
        match value {
            Ok(value) => {
                if let Some(slot) = self.results.get_mut(idx) {
                    *slot = value;
                }
            }
            Err(error) => {
                if self.failure.as_ref().is_none_or(|(held, _)| idx < *held) {
                    self.failure = Some((idx, error));
                }
            }
        }
    }

    /// Moves the merge in `slot` on to its next table, asking `out` to locate
    /// it, or, with no table left, resolves it.
    fn step_merge(&mut self, slot: usize, out: &mut impl ReadSink<'a>) {
        let Some(merge) = self.merges.get_mut(slot).and_then(Option::as_mut) else {
            return;
        };
        match merge.walk.next_table() {
            Ok(Some(next)) => {
                let (level, run, pos) = next.at;
                out.job(Job::MergeLocate {
                    tag: slot,
                    idx: merge.idx,
                    table: TableAt { level, run, pos },
                });
                merge.table = Some(next);
                merge.filter = None;
            }
            Ok(None) => {
                let Some(merge) = self.merges.get_mut(slot).and_then(Option::take) else {
                    return;
                };
                // A blob tree's merge may read its base from the value log as
                // it resolves, which blocks: that goes out as a job. Any other
                // merge resolves from what it holds.
                if self.ctx.merge_base.is_some() {
                    out.job(Job::MergeFinish {
                        idx: merge.idx,
                        read: merge.walk.into_read(),
                    });
                    return;
                }
                let value = match &self.ctx.merge_operator {
                    Some(operator) => merge
                        .walk
                        .finish(operator, self.ctx.merge_base.as_ref())
                        .map(|entry| entry.map(|entry| entry.value)),
                    None => Ok(None),
                };
                self.value_done(merge.idx, value);
            }
            Err(_) => self.merge_alone(slot, out),
        }
    }

    /// Gives up walking the merge in `slot` and resolves it on its own path,
    /// which reads every source again and reports what failed as a single
    /// read would.
    fn merge_alone(&mut self, slot: usize, out: &mut impl ReadSink<'a>) {
        if let Some(merge) = self.merges.get_mut(slot).and_then(Option::take) {
            out.job(Job::Merge { idx: merge.idx });
        }
    }

    /// Takes back the table the merge in `slot` located: versions read in
    /// the job are taken at once; the blocks that can hold them are taken
    /// from the cache or read through the queue, and their versions taken
    /// once every one is in hand.
    fn merge_located(
        &mut self,
        slot: usize,
        located: crate::Result<MergeLocated>,
        out: &mut impl ReadSink<'a>,
    ) {
        let (filter, file, handles) = match located {
            Ok(MergeLocated::Versions(versions)) => {
                self.take_merge_table(slot, versions, out);
                return;
            }
            Ok(MergeLocated::Blocks {
                filter,
                file,
                blocks,
            }) => (filter, file, blocks),
            Err(_) => {
                self.merge_alone(slot, out);
                return;
            }
        };
        let Some(merge) = self.merges.get_mut(slot).and_then(Option::as_mut) else {
            return;
        };
        let Some(table) = merge.table.as_ref().map(|next| next.table) else {
            return;
        };
        merge.filter = Some(filter);
        merge.blocks = handles
            .iter()
            .map(|handle| table.cached_data_block(handle))
            .collect();
        for (at, handle) in handles.iter().enumerate() {
            if merge.blocks.get(at).is_some_and(Option::is_some) {
                continue;
            }
            merge.waiting += 1;
            // A block another merge is reading is waited on, not read again.
            let place = (table.global_id(), *handle.offset());
            if let Some(&tag) = self.merge_blocks_out.get(&place)
                && let Some(Some(ValueReadOut::MergeBlock { waiters, .. })) =
                    self.reads.get_mut(tag)
            {
                waiters.push((slot, at));
                continue;
            }
            // This read bypasses the per-block load path that charges every
            // other read.
            table.record_batched_read(
                crate::table::block::BlockType::Data,
                core::slice::from_ref(handle),
            );
            let tag = self.reads.len();
            out.read(BlockRequest {
                tag,
                fs: &table.fs,
                cache: &table.cache,
                file: Arc::clone(&file),
                offset: *handle.offset(),
                buf: alloc::vec![0u8; handle.size() as usize],
            });
            self.merge_blocks_out.insert(place, tag);
            self.reads.push(Some(ValueReadOut::MergeBlock {
                table,
                handle: *handle,
                waiters: alloc::vec![(slot, at)],
            }));
        }
        if merge.waiting == 0 {
            self.merge_blocks_in_hand(slot, out);
        }
    }

    /// Takes back a block of a table merges are reading: decoded once, kept
    /// in the cache while it fits half of it, and held by every merge waiting
    /// on it for the versions to be taken out of. One that fails sends those
    /// merges on their own path once their table's other blocks are back.
    fn merge_block_done(
        &mut self,
        table: &'a Table,
        handle: &BlockHandle,
        waiters: &[(usize, usize)],
        done: &crate::fs::ReadDone,
    ) {
        self.merge_blocks_out
            .remove(&(table.global_id(), *handle.offset()));
        let block = match &done.result {
            Ok(()) => {
                let room = self
                    .merge_room
                    .get_or_insert_with(|| table.cache_capacity() / 2);
                table
                    .decode_data_block_keeping(&done.buf, handle, room, None)
                    .ok()
            }
            Err(_) => None,
        };
        for &(slot, at) in waiters {
            let Some(merge) = self.merges.get_mut(slot).and_then(Option::as_mut) else {
                continue;
            };
            match (&block, merge.blocks.get_mut(at)) {
                (Some(block), Some(held)) => *held = Some(block.clone()),
                _ => merge.failed = true,
            }
            merge.waiting -= 1;
            if merge.waiting == 0 {
                self.merges_ready.push(slot);
            }
        }
    }

    /// Takes the versions of the key out of the blocks the merge in `slot`
    /// holds of its table, settles the filter's false positive as the table's
    /// reader does, and moves the merge on.
    fn merge_blocks_in_hand(&mut self, slot: usize, out: &mut impl ReadSink<'a>) {
        let Some(merge) = self.merges.get_mut(slot).and_then(Option::as_mut) else {
            return;
        };
        let blocks: Option<Vec<_>> = core::mem::take(&mut merge.blocks).into_iter().collect();
        let (Some(next), Some(filter), Some(blocks), false) =
            (&merge.table, merge.filter, blocks, merge.failed)
        else {
            self.merge_alone(slot, out);
            return;
        };
        let (versions, found) =
            next.table
                .key_versions(&blocks, self.ctx.key(merge.idx), self.ctx.seqno);
        filter.settle(next.table, found);
        self.take_merge_table(slot, versions, out);
    }

    /// Takes `versions`, the versions of the key the table the merge in
    /// `slot` is at holds, newest first, and moves the merge on.
    fn take_merge_table(
        &mut self,
        slot: usize,
        versions: Vec<InternalValue>,
        out: &mut impl ReadSink<'a>,
    ) {
        let Some(merge) = self.merges.get_mut(slot).and_then(Option::as_mut) else {
            return;
        };
        merge.table = None;
        merge.filter = None;
        if merge.walk.take_table(versions.into_iter().map(Ok)).is_err() {
            self.merge_alone(slot, out);
            return;
        }
        self.step_merge(slot, out);
    }
}

impl<'a, K: AsRef<[u8]>> ReadMachine<'a> for MultiGetRead<'a, K> {
    /// Each key's value, in the order the keys were asked.
    type Output = crate::Result<Vec<Option<UserValue>>>;

    fn pump<S: ReadSink<'a>>(&mut self, out: &mut S) -> Option<Self::Output> {
        if let Some(tables) = &mut self.tables {
            let read = tables.pump(&mut self.entries, out)?;
            self.tables = None;
            if let Err(error) = read {
                return Some(Err(error));
            }
            Tree::fan_out_duplicates(&self.duplicates, &mut self.entries);
        }
        if !self.resolved {
            self.resolved = true;
            self.resolve(out);
        }
        // Counted among the jobs already, as the reads they replace were.
        for (idx, item) in self.alone.drain(..) {
            out.job(Job::Blob { idx, item });
        }
        for slot in core::mem::take(&mut self.merges_ready) {
            self.merge_blocks_in_hand(slot, out);
        }
        if self.jobs > 0 {
            return None;
        }
        if let Some((_, error)) = self.failure.take() {
            return Some(Err(error));
        }
        Some(Ok(core::mem::take(&mut self.results)))
    }

    fn job_done<S: ReadSink<'a>>(&mut self, done: JobDone, out: &mut S) {
        match done {
            JobDone::Value { idx, value } => self.value_done(idx, value),
            JobDone::MergeLocated { tag, located } => self.merge_located(tag, located, out),
            done => {
                if let Some(tables) = &mut self.tables {
                    tables.job_done(done, out);
                } else if let JobDone::Opened { tag, file } = done {
                    self.blob_opened(tag, file, out);
                } else {
                    debug_assert!(false, "a job handed back to a read that did not ask for it");
                }
            }
        }
    }

    // The levels are read before any value is, so a read back while they
    // are being read is theirs, and one back after is a value's.
    fn read_done(&mut self, done: crate::fs::ReadDone) {
        if let Some(tables) = &mut self.tables {
            tables.read_done(done);
        } else {
            self.value_read_done(done);
        }
    }
}

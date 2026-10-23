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
use alloc::collections::VecDeque;
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

/// A blob file the value reads go through: opened once, when the read has
/// room for another file, and given back once its last read is back.
struct BlobSlot {
    id: crate::vlog::BlobFileId,
    /// The file, while it is open.
    file: Option<Arc<dyn FsFile>>,
    /// Its reads not yet back.
    reads_out: usize,
}

/// A user of a file, waiting for room under the read's file cap.
#[derive(Clone, Copy)]
enum FileWait {
    /// The blob file of a slot in [`MultiGetRead::blob_files`].
    Blob(usize),
    /// The table a merge, by its slot in [`MultiGetRead::merges`], is at.
    Merge(usize),
}

/// A read out after the levels: a separated value's record, or a data block
/// of the table a merge is reading.
enum ValueReadOut<'a> {
    Blob(BlobRead<'a>),
    MergeBlock {
        table: &'a Table,
        handle: BlockHandle,
        /// The merges waiting on it, by their slots in
        /// [`MultiGetRead::merges`]: it is the next block of each.
        waiters: Vec<usize>,
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
    /// The table's file, and its blocks that can hold versions of the key,
    /// in index order, so newest first.
    file: Option<Arc<dyn FsFile>>,
    handles: Vec<BlockHandle>,
    /// The next of those blocks to take versions from. They are taken one at
    /// a time down to the first base, as the table's reader takes them: the
    /// history beneath a base is hidden by it and is not read.
    next_block: usize,
    /// The table's versions of the key taken so far, newest first.
    versions: Vec<InternalValue>,
    /// Whether a block taken holds a version of the key at all.
    found: bool,
    /// The block being read failed to read or decode.
    failed: bool,
    /// Whether it holds one of the read's files: from the locate of its table
    /// until the table is over.
    holds_file: bool,
}

/// The multi-get of the keys of its context.
pub(super) struct MultiGetRead<'a, K> {
    ctx: &'a ReadCtx<K>,
    /// Each key's newest version, from the memtables or the levels.
    entries: Vec<Option<InternalValue>>,
    /// `(duplicate index, representative index)` of keys asked more than
    /// once: the representative alone is read, and its value handed to each
    /// duplicate once the read is over.
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
    /// The blob files the value reads go through, each opened once.
    blob_files: Vec<BlobSlot>,
    /// The most files the value reads and the merges hold at once: as many
    /// as the descriptor cache keeps, as a level's chunk spans, so a cache
    /// evicting a file can close it; set by the first file asked for.
    file_cap: Option<usize>,
    /// The files held: blob files open or opening, tables a merge is at.
    files_held: usize,
    /// The users of a file waiting for room under the cap, first come first.
    file_waits: VecDeque<FileWait>,
    /// Values whose read failed or did not check out, to read on their own
    /// path, which reports what is wrong as a single read would.
    alone: Vec<(usize, InternalValue)>,
    /// The merges walked, by slot; `None` once over.
    merges: Vec<Option<MergeKey<'a>>>,
    /// Merges whose block is back, to move on next.
    merges_ready: Vec<usize>,
    /// Merges with a block to read and no room for it under the read budget,
    /// in the order they asked.
    merges_parked: VecDeque<usize>,
    /// The tag of each merge block being read, by table and offset: a merge
    /// needing a block another merge is reading waits on that read.
    merge_blocks_out: crate::HashMap<(crate::GlobalTableId, u64), usize>,
    /// The bytes of the merge blocks being read. They stay within half the
    /// cache, as a level's chunk does, so the buffers out never dwarf the
    /// cache they are read for; one block is always let out.
    merge_bytes_out: u64,
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
            file_cap: None,
            files_held: 0,
            file_waits: VecDeque::new(),
            alone: Vec::new(),
            merges: Vec::new(),
            merges_ready: Vec::new(),
            merges_parked: VecDeque::new(),
            merge_blocks_out: crate::HashMap::default(),
            merge_bytes_out: 0,
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
                        file: None,
                        handles: Vec::new(),
                        next_block: 0,
                        versions: Vec::new(),
                        found: false,
                        failed: false,
                        holds_file: false,
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
                    let held = self.blob_files.iter().position(|slot| slot.id == id);
                    let slot = held.unwrap_or_else(|| {
                        Self::note_file_cap(
                            &mut self.file_cap,
                            read.blob_file
                                .file_accessor()
                                .as_descriptor_table()
                                .map(crate::descriptor_table::DescriptorTable::capacity),
                        );
                        self.blob_files.push(BlobSlot {
                            id,
                            file: None,
                            reads_out: 0,
                        });
                        let slot = self.blob_files.len() - 1;
                        self.file_waits.push_back(FileWait::Blob(slot));
                        slot
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
        let Ok(file) = file else {
            for pending in &mut self.reads {
                if let Some(ValueReadOut::Blob(read)) = pending.take_if(
                    |pending| matches!(pending, ValueReadOut::Blob(read) if read.slot == slot),
                ) {
                    self.alone.push((read.idx, read.item));
                }
            }
            self.files_held -= 1;
            return;
        };
        let mut issued = 0usize;
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
            issued += 1;
        }
        // Held until its last read is back.
        if let Some(held) = self.blob_files.get_mut(slot) {
            held.reads_out = issued;
            if issued > 0 {
                held.file = Some(file);
                return;
            }
        }
        self.files_held -= 1;
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
            .and_then(|slot| slot.file.as_ref());
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
        // Its last read back, the file is given back: the descriptor cache can
        // close it, and another file can be opened in its place.
        if let Some(slot) = self.blob_files.get_mut(pending.slot) {
            slot.reads_out -= 1;
            if slot.reads_out == 0 {
                slot.file = None;
                self.files_held -= 1;
            }
        }
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
                // Located once the read has room for its file.
                Self::note_file_cap(&mut self.file_cap, next.table.descriptor_capacity());
                merge.table = Some(next);
                merge.filter = None;
                self.file_waits.push_back(FileWait::Merge(slot));
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
            if merge.holds_file {
                self.files_held -= 1;
            }
            out.job(Job::Merge { idx: merge.idx });
        }
    }

    /// Sets the read's file cap from the first file asked for: as many files
    /// as `capacity`, the descriptor cache's, keeps, at least one; no bound
    /// without a cache, as a level read has none then.
    fn note_file_cap(cap: &mut Option<usize>, capacity: Option<u64>) {
        cap.get_or_insert_with(|| {
            capacity.map_or(usize::MAX, |capacity| {
                usize::try_from(capacity).unwrap_or(usize::MAX).max(1)
            })
        });
    }

    /// Hands out the opens of blob files and the locates of tables waiting
    /// for room under the file cap, first come first, while there is room.
    fn open_files(&mut self, out: &mut impl ReadSink<'a>) {
        let cap = self.file_cap.unwrap_or(usize::MAX);
        while self.files_held < cap {
            let Some(wait) = self.file_waits.pop_front() else {
                return;
            };
            match wait {
                FileWait::Blob(slot) => {
                    let Some(blob) = self.blob_files.get(slot) else {
                        continue;
                    };
                    out.job(Job::OpenBlob {
                        tag: slot,
                        blob_file: blob.id,
                    });
                }
                FileWait::Merge(slot) => {
                    let Some(merge) = self.merges.get_mut(slot).and_then(Option::as_mut) else {
                        continue;
                    };
                    let Some((level, run, pos)) = merge.table.as_ref().map(|next| next.at) else {
                        continue;
                    };
                    out.job(Job::MergeLocate {
                        tag: slot,
                        idx: merge.idx,
                        table: TableAt { level, run, pos },
                    });
                    merge.holds_file = true;
                }
            }
            self.files_held += 1;
        }
    }

    /// Takes back the table the merge in `slot` located: versions read in
    /// the job are taken at once; the blocks that can hold them are taken
    /// next, one at a time, down to the first base.
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
        merge.filter = Some(filter);
        merge.file = Some(file);
        merge.handles = handles;
        merge.next_block = 0;
        merge.found = false;
        merge.failed = false;
        if self.advance_merge(slot, out) {
            self.merges_parked.push_back(slot);
        }
    }

    /// Moves the merge in `slot` through its table's blocks: those the cache
    /// holds are taken at once; the first it lacks is read through the queue,
    /// or waited on when another merge is reading it. Past its last block,
    /// or its first base, the table is over. `true` when the block to read
    /// has no room under the read budget yet: the merge is to be moved on
    /// again once reads are back.
    fn advance_merge(&mut self, slot: usize, out: &mut impl ReadSink<'a>) -> bool {
        let ctx = self.ctx;
        loop {
            let Some(merge) = self.merges.get_mut(slot).and_then(Option::as_mut) else {
                return false;
            };
            let Some(table) = merge.table.as_ref().map(|next| next.table) else {
                return false;
            };
            if merge.failed {
                self.merge_alone(slot, out);
                return false;
            }
            let Some(&handle) = merge.handles.get(merge.next_block) else {
                self.finish_merge_table(slot, out);
                return false;
            };
            if let Some(block) = table.cached_data_block(&handle) {
                Self::take_block(merge, table, &block, ctx);
                continue;
            }
            // A size no block of the table can have is refused before a
            // buffer is allocated for it, as a level read refuses it; the
            // merge's own path reports it.
            if table.check_block_size(&handle).is_err() {
                self.merge_alone(slot, out);
                return false;
            }
            // A block another merge is reading is waited on, not read again.
            let place = (table.global_id(), *handle.offset());
            if let Some(&tag) = self.merge_blocks_out.get(&place)
                && let Some(Some(ValueReadOut::MergeBlock { waiters, .. })) =
                    self.reads.get_mut(tag)
            {
                waiters.push(slot);
                return false;
            }
            let size = u64::from(handle.size());
            // Bounded: what is out stays within the budget plus one block.
            if self.merge_bytes_out > 0
                && self.merge_bytes_out + size > (table.cache_capacity() / 2).max(1)
            {
                return true;
            }
            let Some(file) = merge.file.as_ref().map(Arc::clone) else {
                self.merge_alone(slot, out);
                return false;
            };
            // This read bypasses the per-block load path that charges every
            // other read.
            table.record_batched_read(
                crate::table::block::BlockType::Data,
                core::slice::from_ref(&handle),
            );
            let tag = self.reads.len();
            out.read(BlockRequest {
                tag,
                fs: &table.fs,
                cache: &table.cache,
                file,
                offset: *handle.offset(),
                // Its size is checked above.
                buf: alloc::vec![0u8; handle.size() as usize],
            });
            self.merge_bytes_out += size;
            self.merge_blocks_out.insert(place, tag);
            self.reads.push(Some(ValueReadOut::MergeBlock {
                table,
                handle,
                waiters: alloc::vec![slot],
            }));
            return false;
        }
    }

    /// Takes the versions of the key out of `block`, the next block of the
    /// merge's table, ending the table at the first base: the versions
    /// beneath it are hidden by it.
    fn take_block(
        merge: &mut MergeKey<'a>,
        table: &Table,
        block: &crate::table::DataBlock,
        ctx: &ReadCtx<K>,
    ) {
        let (versions, found) = table.key_versions(block, ctx.key(merge.idx), ctx.seqno);
        merge.found |= found;
        let base = versions
            .iter()
            .any(|version| !version.key.value_type.is_merge_operand());
        merge.versions.extend(versions);
        merge.next_block = if base {
            merge.handles.len()
        } else {
            merge.next_block + 1
        };
    }

    /// Takes back a block of a table merges are reading: decoded once, kept
    /// in the cache while it fits half of it, and its versions taken by every
    /// merge waiting on it. One that fails sends those merges on their own
    /// path.
    fn merge_block_done(
        &mut self,
        table: &'a Table,
        handle: &BlockHandle,
        waiters: &[usize],
        done: &crate::fs::ReadDone,
    ) {
        self.merge_blocks_out
            .remove(&(table.global_id(), *handle.offset()));
        self.merge_bytes_out -= u64::from(handle.size());
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
        let ctx = self.ctx;
        for &slot in waiters {
            let Some(merge) = self.merges.get_mut(slot).and_then(Option::as_mut) else {
                continue;
            };
            match &block {
                Some(block) => Self::take_block(merge, table, block, ctx),
                None => merge.failed = true,
            }
            self.merges_ready.push(slot);
        }
    }

    /// Ends the table the merge in `slot` is reading: settles the filter's
    /// false positive as the table's reader does, and moves the merge on with
    /// the versions its blocks held.
    fn finish_merge_table(&mut self, slot: usize, out: &mut impl ReadSink<'a>) {
        let Some(merge) = self.merges.get_mut(slot).and_then(Option::as_mut) else {
            return;
        };
        let (Some(table), Some(filter)) =
            (merge.table.as_ref().map(|next| next.table), merge.filter)
        else {
            self.merge_alone(slot, out);
            return;
        };
        filter.settle(table, merge.found);
        let versions = core::mem::take(&mut merge.versions);
        merge.handles = Vec::new();
        merge.file = None;
        self.take_merge_table(slot, versions, out);
    }

    /// Moves on the merges whose blocks are back, then those waiting for room
    /// under the read budget, first come first, while there is room.
    fn move_merges(&mut self, out: &mut impl ReadSink<'a>) {
        while let Some(&slot) = self.merges_parked.front() {
            if self.advance_merge(slot, out) {
                break;
            }
            self.merges_parked.pop_front();
        }
        for slot in core::mem::take(&mut self.merges_ready) {
            if self.advance_merge(slot, out) {
                self.merges_parked.push_back(slot);
            }
        }
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
        // The table is over: its file is given back.
        if core::mem::take(&mut merge.holds_file) {
            self.files_held -= 1;
        }
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
        }
        if !self.resolved {
            self.resolved = true;
            self.resolve(out);
        }
        // Counted among the jobs already, as the reads they replace were.
        for (idx, item) in self.alone.drain(..) {
            out.job(Job::Blob { idx, item });
        }
        self.move_merges(out);
        self.open_files(out);
        if self.jobs > 0 {
            return None;
        }
        if let Some((_, error)) = self.failure.take() {
            return Some(Err(error));
        }
        // A key asked again answers its representative's value, read once.
        Tree::fan_out_duplicates(&self.duplicates, &mut self.results);
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

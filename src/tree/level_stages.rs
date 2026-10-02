// Copyright (c) 2026-present, Dmitry Prudnikov
// This source code is licensed under the Apache 2.0 License
// (found in the LICENSE-APACHE file in the repository)

//! The staged read of a level's tables as a machine: it decides which filter
//! and index blocks each table needs next and takes them back as they arrive,
//! and never submits, reads or waits itself. Whoever drives it carries the
//! reads out and hands each result back.

use super::level_resolve::{BlockRequest, LevelJob, LevelWork};
use super::{BlockTask, LevelRead, LevelTable, LevelTasks, MetaHeld, Tree};
use crate::table::probe_stats::PlanCounts;
use crate::table::staged::{StagedRead, StagedStart};
use crate::{SeqNo, Table, table::BlockHandle};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::ops::Range;

/// A table left to the serial planner, which is planned over its span of the
/// batch and handed back through [`LevelStages::planned`].
pub(super) struct StagePlan<'a> {
    pub(super) at: usize,
    pub(super) table: &'a Table,
    pub(super) keys: Range<usize>,
}

/// The staged read of a level's `tables` for their spans of `batch`.
pub(super) struct LevelStages<'a, 'k> {
    tables: Vec<LevelTable<'a>>,
    batch: Vec<(&'k [u8], u64)>,
    seqno: SeqNo,
    metadata_budget: u64,
    /// What each read asked for is for: its tag is its position here.
    asked: Vec<(usize, BlockHandle)>,
    /// Each table's reads still in flight.
    waiting: Vec<usize>,
    /// At most as many tables hold a file open for their stages as the
    /// descriptor cache keeps open: a level wider than that, read stage by
    /// stage all at once, would otherwise open a file per table and run the
    /// process out of descriptors. A table whose stages are over lets its
    /// file go and its place with it, so the tables waiting for a place
    /// always get one.
    open_cap: usize,
    staged: Vec<bool>,
    in_stage: usize,
    meta: MetaHeld,
    /// The bytes a table whose file is being opened holds for the stage it
    /// opens it for: counted against the budget as in flight, so the tables
    /// after it see the level as they would with that stage's reads out.
    opening: Vec<u64>,
    /// Whether the last pass left a table waiting for a place under the cap,
    /// or for room under the budget.
    deferred: bool,
}

impl<'a, 'k> LevelStages<'a, 'k> {
    /// The staged read of `level` for the keys of `remaining` (indices into
    /// `keys` with their filter hashes, sorted under `comparator`), at the
    /// snapshot `seqno`. Each table of each run covers a span of the batch;
    /// its read begins once it has a place to read in.
    #[expect(
        clippy::indexing_slicing,
        reason = "i < batch.len() is loop-checked; remaining's indices are valid key indices"
    )]
    pub(super) fn new<K: AsRef<[u8]>>(
        level: &'a crate::version::Level,
        remaining: &[(usize, u64)],
        keys: &'k [K],
        comparator: &dyn crate::comparator::UserComparator,
        seqno: SeqNo,
        metadata_budget: u64,
    ) -> Self {
        // The batch the level is read for, once: each table covers a span of
        // it, in every run alike.
        let batch: Vec<(&'k [u8], u64)> = remaining
            .iter()
            .map(|&(idx, hash)| (keys[idx].as_ref(), hash))
            .collect();

        // The level's tables, each with the span of keys it covers.
        let mut tables: Vec<LevelTable<'a>> = Vec::new();
        for run in level.iter() {
            let mut i = 0;
            while i < batch.len() {
                let Some(table) = run.get_for_key_cmp(batch[i].0, comparator) else {
                    i += 1;
                    continue;
                };
                let table_id = table.id();
                let start = i;
                while i < batch.len() {
                    match run.get_for_key_cmp(batch[i].0, comparator) {
                        Some(t) if t.id() == table_id => i += 1,
                        _ => break,
                    }
                }
                tables.push(LevelTable {
                    table,
                    keys: start..i,
                    read: LevelRead::Pending,
                    file: None,
                });
            }
        }

        let open_cap = Tree::open_cap(tables.iter().map(|entry| entry.table));
        let count = tables.len();
        Self {
            tables,
            batch,
            seqno,
            metadata_budget,
            asked: Vec::new(),
            waiting: alloc::vec![0; count],
            open_cap,
            staged: alloc::vec![false; count],
            in_stage: 0,
            meta: MetaHeld::new(count),
            opening: alloc::vec![0; count],
            deferred: false,
        }
    }

    /// The keys of the level's batch in `span`.
    pub(super) fn span(&self, span: Range<usize>) -> &[(&'k [u8], u64)] {
        self.batch.get(span).unwrap_or_default()
    }

    /// Whether none of the level's tables waits on a read or an open.
    pub(super) fn idle(&self) -> bool {
        self.waiting.iter().all(|&waiting| waiting == 0)
    }

    /// Whether the last pass left a table waiting: one that will move once a
    /// table in flight is back, or once nothing is.
    pub(super) fn deferred(&self) -> bool {
        self.deferred
    }

    /// One pass over the level: every table with none of its blocks in flight
    /// moves on as far as the blocks it holds take it, and asks for the next
    /// stage's blocks once its read lacks some, as long as it has a place
    /// under the cap and room under the budget. The files to open and the
    /// reads asked for are appended to `out`.
    #[expect(
        clippy::indexing_slicing,
        reason = "`at` indexes the level's tables, which every per-table vector is sized to; a table's span lies in `batch`"
    )]
    pub(super) fn pump(&mut self, out: &mut LevelWork<'a, '_>) {
        self.deferred = false;
        // Whether a table before the one at hand holds metadata blocks: the
        // first that does is never held back, so the level advances.
        let mut held_before = false;
        for at in 0..self.tables.len() {
            let first_holder = !held_before;
            if self.waiting[at] > 0 {
                held_before = true;
                continue;
            }
            let entry = &mut self.tables[at];
            let keys = &self.batch[entry.keys.clone()];
            if matches!(entry.read, LevelRead::Pending) {
                if self.in_stage >= self.open_cap {
                    self.deferred = true;
                    continue;
                }
                entry.read = match StagedRead::start(entry.table, keys, self.seqno) {
                    StagedStart::Nothing => LevelRead::Nothing,
                    StagedStart::Serial => LevelRead::Serial,
                    StagedStart::Staged(read) => LevelRead::Staged(read),
                };
            }
            while let LevelRead::Staged(read) = &mut entry.read
                && !read.is_done()
            {
                let (block_type, need) = read.need();
                if need.is_empty() {
                    // A stage passed lets go of the blocks it alone read.
                    if read.advance(keys).is_ok() {
                        self.meta.settle(at, read.held_bytes());
                    } else {
                        entry.read = LevelRead::Serial;
                        self.meta.settle(at, 0);
                    }
                    continue;
                }
                let table: &'a Table = entry.table;
                // A table with no place under the cap waits before any of its
                // stage's buffers is allocated: it is passed over again on
                // every pass until a place frees.
                if entry.file.is_none() && self.in_stage >= self.open_cap {
                    self.deferred = true;
                    break;
                }
                // Nor is any allocated for a stage past the budget, unless no
                // table before this one holds blocks: then it goes on even
                // alone above the budget, or the level would stop.
                let asked_bytes: u64 = need.iter().map(|handle| u64::from(handle.size())).sum();
                if !first_holder && self.meta.total + asked_bytes > self.metadata_budget {
                    self.deferred = true;
                    break;
                }
                // A size no block can have is refused before any buffer is
                // allocated for it, or its file opened; the serial planner then
                // reports the corruption as the load path does.
                if need
                    .iter()
                    .any(|handle| table.check_block_size(handle).is_err())
                {
                    entry.read = LevelRead::Serial;
                    self.meta.settle(at, 0);
                    break;
                }
                // A table's file is opened before its first stage is read, and
                // held for every later one. The stage's bytes count as in
                // flight while it is opened, so the tables after it see the
                // level as they would with its reads out.
                let Some(file) = &entry.file else {
                    out.jobs.push(LevelJob::Open { tag: at, table });
                    self.staged[at] = true;
                    self.in_stage += 1;
                    self.opening[at] = asked_bytes;
                    self.meta.in_flight[at] += asked_bytes;
                    self.meta.settle(at, read.held_bytes());
                    self.waiting[at] += 1;
                    break;
                };
                let file = Arc::clone(file);
                let buffers: Vec<Vec<u8>> = need
                    .iter()
                    .map(|handle| alloc::vec![0u8; handle.size() as usize])
                    .collect();
                table.record_batched_read(block_type, need);
                self.meta.in_flight[at] += asked_bytes;
                self.meta.settle(at, read.held_bytes());
                for (handle, buf) in need.iter().zip(buffers) {
                    out.reads.push(BlockRequest {
                        tag: self.asked.len(),
                        table,
                        file: Arc::clone(&file),
                        offset: *handle.offset(),
                        buf,
                    });
                    self.asked.push((at, *handle));
                    self.waiting[at] += 1;
                }
                break;
            }
            // A table whose stages are over, or that is left to the serial
            // planner, needs its file no more: its data blocks are read
            // through the file their chunk opens, and holding it would keep a
            // descriptor per table of the level open until the whole level is
            // read.
            if self.waiting[at] == 0 && entry.read.is_over() {
                entry.file = None;
                if self.staged[at] {
                    self.staged[at] = false;
                    self.in_stage -= 1;
                }
                // Planned, or left to the serial planner: the blocks its
                // stages held are its read's no more.
                self.meta.settle(at, 0);
            }
            held_before |= self.meta.held[at] > 0;
        }
    }

    /// Takes back the file of the table at `at`, opened for its next stage,
    /// whose reads the next pass asks for. A table whose file does not open
    /// is planned serially.
    #[expect(
        clippy::indexing_slicing,
        reason = "`at` tagged an open job of this level, which indexes its tables"
    )]
    pub(super) fn opened(&mut self, at: usize, file: crate::Result<Arc<dyn crate::fs::FsFile>>) {
        self.waiting[at] -= 1;
        self.meta.in_flight[at] -= self.opening[at];
        self.opening[at] = 0;
        let entry = &mut self.tables[at];
        match file {
            Ok(file) => {
                entry.file = Some(file);
                let held = match &entry.read {
                    LevelRead::Staged(read) => read.held_bytes(),
                    _ => 0,
                };
                self.meta.settle(at, held);
            }
            Err(error) => {
                log::debug!(
                    "a staged level read could not open a table, it is planned serially: {error}"
                );
                entry.read = LevelRead::Serial;
                self.meta.settle(at, 0);
            }
        }
    }

    /// Takes back the read tagged `tag`: `result` is how it went, and `buf`
    /// holds the block when it went well. A read that loses a block is
    /// dropped, and its table planned serially.
    #[expect(
        clippy::indexing_slicing,
        reason = "a tag indexes `asked`, whose entries index the level's tables and `waiting`, all by construction"
    )]
    pub(super) fn complete(&mut self, tag: usize, result: crate::io::Result<()>, buf: &[u8]) {
        let (at, handle) = self.asked[tag];
        self.waiting[at] -= 1;
        // Back from flight: held from here on decoded, if at all.
        self.meta.in_flight[at] -= u64::from(handle.size());
        let LevelRead::Staged(read) = &mut self.tables[at].read else {
            self.meta.settle(at, 0);
            return;
        };
        let supplied = match result {
            Ok(()) => read.supply(handle, buf).is_ok(),
            Err(error) => {
                log::debug!(
                    "a staged level read lost a block, its table is planned serially: {error}"
                );
                false
            }
        };
        if supplied {
            self.meta.settle(at, read.held_bytes());
        } else {
            self.tables[at].read = LevelRead::Serial;
            self.meta.settle(at, 0);
        }
    }

    /// The tables the stages left to the serial planner, in level order, once
    /// no stage is in flight: those a stage gave up on, and any whose stages
    /// did not finish.
    pub(super) fn serial_plans(&self) -> Vec<StagePlan<'a>> {
        self.tables
            .iter()
            .enumerate()
            .filter(|(_, entry)| match &entry.read {
                LevelRead::Staged(read) => !read.is_done(),
                LevelRead::Pending | LevelRead::Serial => true,
                LevelRead::Planned(..) | LevelRead::Nothing => false,
            })
            .map(|(at, entry)| StagePlan {
                at,
                table: entry.table,
                keys: entry.keys.clone(),
            })
            .collect()
    }

    /// Takes back the serial plan of the table at `at`: `None` when it covers
    /// none of its keys, with the filter probes planning took in `tally`.
    pub(super) fn planned(
        &mut self,
        at: usize,
        plan: Option<crate::table::BlockTaskPlan>,
        tally: PlanCounts,
    ) {
        if let Some(entry) = self.tables.get_mut(at) {
            let (table_seqno, blocks) =
                plan.map_or((0, Vec::new()), |(_, seqno, _, blocks)| (seqno, blocks));
            entry.read = LevelRead::Planned(table_seqno, blocks, tally);
        }
    }

    /// The level's data block tasks in level order, each with the indices
    /// into the caller's keys of the keys it reads, from the positions of
    /// `remaining` the batch was built from; and per table the filter probes
    /// planning took, not yet counted.
    #[expect(
        clippy::indexing_slicing,
        reason = "a plan's positions lie in its table's span, which lies in the batch built from `remaining`"
    )]
    pub(super) fn into_tasks(self, remaining: &[(usize, u64)]) -> LevelTasks<'a> {
        let mut tasks: Vec<BlockTask<'a>> = Vec::new();
        let mut probes: Vec<(&'a Table, PlanCounts)> = Vec::new();
        for LevelTable {
            table, keys, read, ..
        } in self.tables
        {
            let (table_seqno, blocks, tally) = match read {
                LevelRead::Staged(read) => read.into_plan(),
                LevelRead::Planned(table_seqno, blocks, tally) => (table_seqno, blocks, tally),
                LevelRead::Pending | LevelRead::Serial | LevelRead::Nothing => continue,
            };
            if tally != PlanCounts::default() {
                probes.push((table, tally));
            }
            if blocks.is_empty() {
                continue;
            }
            let special = table.is_chunk_special();
            for (handle, positions) in blocks {
                let task_keys: Vec<usize> = positions
                    .iter()
                    .map(|&pos| remaining[keys.start + pos].0)
                    .collect();
                tasks.push(BlockTask {
                    table,
                    handle,
                    table_seqno,
                    special,
                    keys: task_keys,
                });
            }
        }
        (tasks, probes)
    }
}

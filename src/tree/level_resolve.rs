// Copyright (c) 2026-present, Dmitry Prudnikov
// This source code is licensed under the Apache 2.0 License
// (found in the LICENSE-APACHE file in the repository)

//! The resolve of one level for a key batch as a machine: the staged read of
//! its tables' filter and index blocks, the serial plan of the tables the
//! stages cannot serve, then its data blocks chunk by chunk. It never opens,
//! reads or waits itself: it hands its driver block reads, and the work a
//! block read cannot express as jobs the driver may run anywhere, and takes
//! each back as it finishes.

use super::data_stage::{ChunkFile, ChunkRead};
use super::level_stages::LevelStages;
use super::read_job::{Job, JobDone, ReadSink};
use super::{BlockTask, TaskBlock, Tree};
use crate::table::probe_stats::PlanCounts;
use crate::{InternalValue, SeqNo, Table, TableId};
use alloc::vec::Vec;

/// Where a level's resolve stands after a [`LevelResolve::pump`].
#[derive(Debug, PartialEq, Eq)]
pub(super) enum LevelStep {
    /// Work is asked for, or out: the level goes on once some of it is back.
    Pending,
    /// The level is answered from its plan.
    Resolved,
    /// A table could not be planned, or a block of the plan read: the level
    /// is the serial resolve's, from no answer, and nothing is out.
    Serial,
}

/// The data block phase: the level's tasks read chunk by chunk.
struct Chunks<'a, K> {
    tasks: Vec<BlockTask<'a>>,
    probes: Vec<(&'a Table, PlanCounts)>,
    cached: Vec<TaskBlock>,
    /// What the blocks read may still add to the shared cache: half of it,
    /// weighed decoded, as the cache weighs them.
    keep_room: u64,
    /// The most a chunk reads or loads, in bytes on disk: half the shared
    /// cache, so a chunk's scratch never dwarfs the cache it is meant to
    /// spare; at least one, so a chunk always takes a task.
    budget: u64,
    /// The `(table, key)` pairs a read found, when some table counts its
    /// filter probes: a key whose version may continue past a block's end is
    /// read in that block and the next, so a miss is a pair no read of it
    /// found, not one block that came back empty.
    found: Option<Vec<(TableId, usize)>>,
    /// A chunk opens the files of the tables it reads blocks of, so it spans
    /// at most as many of them as the descriptor cache keeps.
    open_cap: usize,
    /// The last table a chunk read, with its file: a table whose blocks span
    /// two chunks is opened once for both.
    carried: Option<ChunkFile>,
    start: usize,
    current: Option<Current<'a, K>>,
}

impl<'a, K: AsRef<[u8]>> Current<'a, K> {
    /// Asks `out` for the chunk's reads once every job it asked for is back,
    /// and only once; a chunk that failed asks for none.
    fn ask_reads(
        &mut self,
        chunk: &[BlockTask<'a>],
        carried: &mut Option<ChunkFile>,
        out: &mut impl ReadSink<'a>,
    ) {
        if self.jobs > 0 || self.failed || self.reads_asked {
            return;
        }
        self.reads_asked = true;
        match self.read.take_reads(chunk, carried, out) {
            Ok(reads) => self.reads = reads,
            Err(error) => {
                log::debug!("a batched level read failed, the level is read serially: {error}");
                self.failed = true;
            }
        }
    }
}

/// The chunk in flight.
struct Current<'a, K> {
    end: usize,
    read: ChunkRead<'a, K>,
    /// Its jobs not yet back.
    jobs: usize,
    /// Whether its reads were handed out, and how many are not yet back.
    reads_asked: bool,
    reads: usize,
    /// A job or a read failed: the level goes to the serial resolve once
    /// nothing of the chunk is out.
    failed: bool,
}

enum Phase<'a, K> {
    Stages(LevelStages<'a>),
    Plans {
        stages: LevelStages<'a>,
        asked: bool,
        left: usize,
        failed: bool,
    },
    Chunks(Chunks<'a, K>),
    Over,
}

/// The resolve of one level for the keys of `remaining`.
pub(super) struct LevelResolve<'a, K> {
    keys: &'a [K],
    /// `(key index, filter hash)` of the keys still to resolve, sorted.
    remaining: Vec<(usize, u64)>,
    phase: Phase<'a, K>,
}

impl<'a, K: AsRef<[u8]>> LevelResolve<'a, K> {
    /// The resolve of `level`, the version's level `level_idx`, at the
    /// snapshot `seqno` for the keys of `remaining` (indices into `keys` with
    /// their filter hashes, sorted under `comparator`), whose filter and
    /// index blocks the tables hold within `metadata_budget`.
    pub(super) fn new(
        level: &'a crate::version::Level,
        level_idx: usize,
        remaining: Vec<(usize, u64)>,
        keys: &'a [K],
        comparator: &dyn crate::comparator::UserComparator,
        seqno: SeqNo,
        metadata_budget: u64,
    ) -> Self {
        let stages = LevelStages::new(
            level,
            level_idx,
            &remaining,
            keys,
            comparator,
            seqno,
            metadata_budget,
        );
        Self {
            keys,
            remaining,
            phase: Phase::Stages(stages),
        }
    }

    /// The keys still to resolve: the ones the level did not answer once it
    /// is resolved, all of them once it is the serial resolve's.
    pub(super) fn into_remaining(self) -> Vec<(usize, u64)> {
        self.remaining
    }

    /// Moves the level on as far as what is back takes it, asking `out` for
    /// the work it needs next. An answer is kept in `results` (the
    /// highest-seqno version per key); a level handed to the serial resolve
    /// clears the answers of its keys first.
    #[expect(
        clippy::indexing_slicing,
        reason = "a serial plan's span lies in the batch built from `remaining`"
    )]
    pub(super) fn pump(
        &mut self,
        results: &mut [Option<InternalValue>],
        out: &mut impl ReadSink<'a>,
    ) -> LevelStep {
        loop {
            match &mut self.phase {
                Phase::Stages(stages) => {
                    stages.pump(out);
                    // Every file open and read the stages ask for keeps its
                    // table waiting until it is back.
                    if !stages.idle() {
                        return LevelStep::Pending;
                    }
                    // Nothing out: a table that waited for a place gets one
                    // now; once none waits, the stages are over.
                    if stages.deferred() {
                        continue;
                    }
                    let Phase::Stages(stages) = core::mem::replace(&mut self.phase, Phase::Over)
                    else {
                        unreachable!("matched above");
                    };
                    self.phase = Phase::Plans {
                        stages,
                        asked: false,
                        left: 0,
                        failed: false,
                    };
                }
                Phase::Plans {
                    stages,
                    asked,
                    left,
                    failed,
                } => {
                    if !*asked {
                        *asked = true;
                        for plan in stages.serial_plans() {
                            out.job(Job::Plan {
                                at: plan.at,
                                table: plan.table,
                                keys: self.remaining[plan.keys].to_vec(),
                            });
                            *left += 1;
                        }
                    }
                    if *left > 0 {
                        return LevelStep::Pending;
                    }
                    if *failed {
                        return self.hand_back(results);
                    }
                    let Phase::Plans { stages, .. } =
                        core::mem::replace(&mut self.phase, Phase::Over)
                    else {
                        unreachable!("matched above");
                    };
                    let (tasks, probes) = stages.into_tasks(&self.remaining);
                    let Some(chunks) = Chunks::new(tasks, probes) else {
                        // No key of the batch reaches a block of this level:
                        // the filter probes that found so are its answer.
                        return LevelStep::Resolved;
                    };
                    self.phase = Phase::Chunks(chunks);
                }
                Phase::Chunks(chunks) => match chunks.pump(self.keys, results, out) {
                    ChunkStep::Pending => return LevelStep::Pending,
                    ChunkStep::Failed => return self.hand_back(results),
                    ChunkStep::Over => {
                        let Phase::Chunks(chunks) =
                            core::mem::replace(&mut self.phase, Phase::Over)
                        else {
                            unreachable!("matched above");
                        };
                        chunks.count();
                        self.remaining
                            .retain(|&(idx, _)| results.get(idx).is_some_and(Option::is_none));
                        return LevelStep::Resolved;
                    }
                },
                Phase::Over => return LevelStep::Resolved,
            }
        }
    }

    /// The level goes to the serial resolve, from no answer: on level 0 a key
    /// it finds at the read's ceiling only skips the older runs when it sets
    /// that answer itself.
    fn hand_back(&mut self, results: &mut [Option<InternalValue>]) -> LevelStep {
        for &(idx, _) in &self.remaining {
            if let Some(slot) = results.get_mut(idx) {
                *slot = None;
            }
        }
        self.phase = Phase::Over;
        LevelStep::Serial
    }

    /// Takes back a finished job, asking `out` for the work it unblocks.
    pub(super) fn job_done(&mut self, done: JobDone, out: &mut impl ReadSink<'a>) {
        match (&mut self.phase, done) {
            (Phase::Stages(stages), JobDone::Opened { tag, file }) => {
                stages.opened(tag, file, out);
            }
            (
                Phase::Plans {
                    stages,
                    left,
                    failed,
                    ..
                },
                JobDone::Planned { at, plan, tally },
            ) => {
                *left -= 1;
                match plan {
                    Ok(plan) => stages.planned(at, plan, tally),
                    Err(error) => {
                        log::debug!(
                            "a staged level plan failed, the level is read serially: {error}"
                        );
                        *failed = true;
                    }
                }
            }
            (Phase::Chunks(chunks), done) => chunks.job_done(done, out),
            _ => debug_assert!(
                false,
                "a job handed back to a phase that did not ask for it"
            ),
        }
    }

    /// Takes back a finished block read.
    pub(super) fn read_done(&mut self, done: crate::fs::ReadDone) {
        match &mut self.phase {
            Phase::Stages(stages) => stages.complete(done.tag, done.result, &done.buf),
            Phase::Chunks(chunks) => chunks.read_done(done),
            _ => debug_assert!(
                false,
                "a read handed back to a phase that did not ask for it"
            ),
        }
    }
}

/// Where the data block phase stands.
enum ChunkStep {
    Pending,
    Failed,
    Over,
}

impl<'a, K: AsRef<[u8]>> Chunks<'a, K> {
    /// The data block phase of `tasks`, or `None` when there are none.
    fn new(tasks: Vec<BlockTask<'a>>, probes: Vec<(&'a Table, PlanCounts)>) -> Option<Self> {
        let Some(first) = tasks.first() else {
            for (table, counts) in probes {
                table.count_plan(counts);
            }
            return None;
        };
        // Each task's block, when the cache holds it; the others are read. A
        // Page-ECC or columnar table's block is loaded through its own path,
        // the same load a point read makes: the scratch decode is row-format
        // only. It is loaded when its chunk is read, so the blocks loaded stay
        // within the chunk budget too.
        let cached: Vec<TaskBlock> = tasks
            .iter()
            .map(|task| {
                if task.special {
                    TaskBlock::Load
                } else {
                    task.table
                        .cached_data_block(&task.handle)
                        .map_or(TaskBlock::Read, TaskBlock::Held)
                }
            })
            .collect();
        let capacity = first.table.cache_capacity();
        let found = tasks
            .iter()
            .any(|task| task.table.counts_filter_misses())
            .then(Vec::new);
        let open_cap = Tree::open_cap(tasks.iter().map(|task| task.table));
        Some(Self {
            tasks,
            probes,
            cached,
            keep_room: capacity / 2,
            budget: (capacity / 2).max(1),
            found,
            open_cap,
            carried: None,
            start: 0,
            current: None,
        })
    }

    /// The end of the chunk that begins at `start`: within the budget and
    /// the descriptor cap, and at least one task. A table's tasks are
    /// consecutive, so a new table is a change of table.
    #[expect(
        clippy::indexing_slicing,
        reason = "end < tasks.len() is loop-checked, and `cached` is as long as `tasks`"
    )]
    fn chunk_end(&self) -> usize {
        let start = self.start;
        let mut bytes = 0u64;
        let mut end = start;
        let mut opened = 0usize;
        let mut last_read: Option<TableId> = None;
        while end < self.tasks.len() {
            let reads = matches!(self.cached[end], TaskBlock::Read);
            let sz = if reads || matches!(self.cached[end], TaskBlock::Load) {
                u64::from(self.tasks[end].handle.size())
            } else {
                0
            };
            let opens = reads && last_read != Some(self.tasks[end].table.id());
            if end > start && (bytes + sz > self.budget || (opens && opened == self.open_cap)) {
                break;
            }
            if opens {
                opened += 1;
                last_read = Some(self.tasks[end].table.id());
            }
            bytes += sz;
            end += 1;
        }
        end
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "a chunk's start..end lies within `tasks` and `cached`"
    )]
    fn pump(
        &mut self,
        keys: &'a [K],
        results: &mut [Option<InternalValue>],
        out: &mut impl ReadSink<'a>,
    ) -> ChunkStep {
        loop {
            let Some(current) = &mut self.current else {
                if self.start >= self.tasks.len() {
                    return ChunkStep::Over;
                }
                let end = self.chunk_end();
                let chunk = &self.tasks[self.start..end];
                let (read, count) = match ChunkRead::start(
                    chunk,
                    &self.cached[self.start..end],
                    keys,
                    &mut self.carried,
                    out,
                ) {
                    Ok(started) => started,
                    Err(error) => {
                        log::debug!(
                            "a batched level read failed, the level is read serially: {error}"
                        );
                        return ChunkStep::Failed;
                    }
                };
                self.current = Some(Current {
                    end,
                    read,
                    jobs: count,
                    reads_asked: false,
                    reads: 0,
                    failed: false,
                });
                continue;
            };
            if current.jobs > 0 {
                return ChunkStep::Pending;
            }
            current.ask_reads(&self.tasks[self.start..current.end], &mut self.carried, out);
            if current.reads > 0 {
                return ChunkStep::Pending;
            }
            let Some(Current {
                end, read, failed, ..
            }) = self.current.take()
            else {
                unreachable!("matched above");
            };
            if failed {
                return ChunkStep::Failed;
            }
            if let Err(error) =
                read.finish(&self.tasks[self.start..end], results, self.found.as_mut())
            {
                log::debug!("a batched level read failed, the level is read serially: {error}");
                return ChunkStep::Failed;
            }
            self.start = end;
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "a chunk's start..end lies within `tasks`"
    )]
    /// Takes back a finished job of the chunk out, and asks `out` for the
    /// chunk's reads once it was the last.
    fn job_done(&mut self, done: JobDone, out: &mut impl ReadSink<'a>) {
        let Some(current) = &mut self.current else {
            debug_assert!(false, "a job handed back with no chunk out");
            return;
        };
        current.jobs -= 1;
        let chunk = &self.tasks[self.start..current.end];
        let failure = match done {
            JobDone::Opened { tag, file } => file.map(|file| current.read.opened(tag, file)),
            JobDone::Loaded { index, block } => {
                block.and_then(|block| current.read.loaded(chunk, index, block))
            }
            JobDone::Planned { .. } | JobDone::Serial { .. } | JobDone::Value { .. } => {
                debug_assert!(false, "a job handed back to the data block phase");
                Ok(())
            }
        };
        if let Err(error) = failure {
            log::debug!("a batched level read failed, the level is read serially: {error}");
            current.failed = true;
        }
        current.ask_reads(chunk, &mut self.carried, out);
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "a chunk's start..end lies within `tasks`"
    )]
    fn read_done(&mut self, done: crate::fs::ReadDone) {
        let Some(current) = &mut self.current else {
            debug_assert!(false, "a read handed back with no chunk out");
            return;
        };
        current.reads -= 1;
        match done.result {
            Ok(()) => current.read.read(
                &self.tasks[self.start..current.end],
                done.tag,
                &done.buf,
                &mut self.keep_room,
            ),
            Err(error) => {
                log::debug!("a batched level read failed, the level is read serially: {error}");
                current.failed = true;
            }
        }
    }

    /// Counts what the level answered from its plan: its filter probes,
    /// queries and skips, and the keys a filter let through that no read
    /// found. A level handed back is probed and counted by the serial
    /// resolve.
    fn count(self) {
        for (table, counts) in self.probes {
            table.count_plan(counts);
        }
        if let Some(found) = self.found {
            Tree::count_chunked_false_positives(&self.tasks, found);
        }
    }
}

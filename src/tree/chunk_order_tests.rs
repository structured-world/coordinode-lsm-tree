// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::data_stage::ChunkRead;
use super::read_job::{Job, JobDone, ReadCtx, TableAt, Values};
use super::{BlockTask, TaskBlock};
use crate::{AbstractTree, Config, SeqNo, SequenceNumberCounter, Tree, value::InternalValue};

/// The context of a read of `keys` in `tree`'s current version.
pub(super) fn ctx_for<K>(tree: &Tree, keys: Vec<K>) -> crate::Result<ReadCtx<K>> {
    Ok(ReadCtx {
        super_version: tree.snapshot_for_read(SeqNo::MAX)?,
        keys,
        seqno: SeqNo::MAX,
        comparator: crate::comparator::default_comparator(),
        merge_operator: None,
        values: Values::Inline,
        metadata_budget: crate::config::DEFAULT_MULTI_GET_METADATA_BUDGET,
    })
}

/// The data block tasks of every table of level 0 that holds `key`, in level
/// order, each reading `key` as key index 0.
pub(super) fn tasks_for<'a>(
    level: &'a crate::version::Level,
    key: &[u8],
) -> crate::Result<Vec<BlockTask<'a>>> {
    let batch = [(key, crate::hash::hash64(key))];
    let mut tasks = Vec::new();
    for (run_idx, run) in level.iter().enumerate() {
        for (pos, table) in run.iter().enumerate() {
            let mut tally = crate::table::probe_stats::PlanCounts::default();
            let Some((_, table_seqno, _, blocks)) =
                table.plan_block_tasks(&batch, SeqNo::MAX, &mut tally)?
            else {
                continue;
            };
            for (handle, positions) in blocks {
                tasks.push(BlockTask {
                    table,
                    at: TableAt {
                        level: 0,
                        run: run_idx,
                        pos,
                    },
                    handle,
                    table_seqno,
                    special: table.is_chunk_special(),
                    keys: positions.iter().map(|_| 0).collect(),
                });
            }
        }
    }
    Ok(tasks)
}

/// Two level-0 tables holding one key at one seqno: the chunked resolve keeps
/// the value a single-key read returns, whatever order its driver hands the
/// two blocks back in. At an equal seqno the task earlier in the plan wins, as
/// it does when the blocks are decoded in plan order.
#[test]
fn a_chunked_resolve_breaks_an_equal_seqno_tie_by_plan_order() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let any = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    let crate::AnyTree::Standard(tree) = &any else {
        panic!("a standard tree");
    };
    for value in ["older", "newer"] {
        tree.insert("key", value, 7);
        tree.flush_active_memtable(0)?;
    }

    let version = tree.current_version();
    let Some(level) = version.level(0) else {
        panic!("level 0 exists");
    };
    assert_eq!(level.len(), 2, "one run per flush");
    let keys = ["key"];
    let tasks = tasks_for(level, b"key")?;
    assert_eq!(tasks.len(), 2, "one block per table");

    // Both blocks read, so the driver decides the order they are back in.
    let uncached = [TaskBlock::Read, TaskBlock::Read];
    let ctx = ctx_for(tree, keys.to_vec())?;
    let (mut chunk, jobs) = ChunkRead::start(&tasks, &uncached, &keys, &mut None)?;
    for job in jobs {
        let Job::Open { .. } = &job else {
            panic!("a row table's block needs only its file");
        };
        let JobDone::Opened { tag, file } = job.run(&ctx) else {
            panic!("an open job opens a file");
        };
        chunk.opened(tag, file?);
    }
    let mut reads = chunk.take_reads(&tasks, &mut None)?;
    // Handed back last to first, as a ring may when the later reads of a
    // batch complete first.
    reads.reverse();
    let mut keep_room = 0;
    for mut read in reads {
        let filled = read.file.read_at(&mut read.buf, read.offset)?;
        assert_eq!(filled, read.buf.len(), "the whole block is read");
        chunk.read(&tasks, read.tag, &read.buf, &mut keep_room);
    }
    let mut results: Vec<Option<InternalValue>> = alloc::vec![None];
    chunk.finish(&tasks, &mut results, None)?;

    let Some(single) = tree.get("key", SeqNo::MAX)? else {
        panic!("the key was written");
    };
    let Some(Some(chunked)) = results.first() else {
        panic!("the chunk resolves the key");
    };
    assert_eq!(chunked.value, single);
    Ok(())
}

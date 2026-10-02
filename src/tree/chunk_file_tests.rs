// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::TaskBlock;
use super::chunk_order_tests::{ctx_for, tasks_for};
use super::data_stage::ChunkRead;
use super::read_job::{Job, JobDone};
use crate::fs::{Fs, FsFile, FsOpenOptions, StdFs};
use crate::{AbstractTree, Config, SequenceNumberCounter, value::InternalValue};
use alloc::sync::Arc;

/// A chunk that starts on a table other than the one the previous chunk
/// carried over lets that file go before it asks for its own: the carried
/// file, held by nothing else, is closed by the time the chunk hands out the
/// job that opens the next one, so a chunk never holds one descriptor past
/// the cap it was sized to.
#[test]
fn a_chunk_on_another_table_closes_the_carried_file_before_asking_for_its_own() -> crate::Result<()>
{
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
    for key in ["a", "b"] {
        tree.insert(key, "value", 0);
        tree.flush_active_memtable(0)?;
    }

    let version = tree.current_version();
    let Some(level) = version.level(0) else {
        panic!("level 0 exists");
    };
    let keys = ["b"];
    let tasks = tasks_for(level, b"b")?;
    let [task] = tasks.as_slice() else {
        panic!("one block, in the table holding the key");
    };

    // The previous chunk's last file, of another table, held by nothing else.
    let carried_file: Arc<dyn FsFile> = Arc::from(StdFs.open(
        &folder.path().join("carried"),
        &FsOpenOptions::new().write(true).create(true),
    )?);
    let watched = Arc::downgrade(&carried_file);
    // Table ids are small here, so one past the task's names another table.
    let mut carried = Some((task.table.id() + 1, carried_file));

    let (mut chunk, jobs) = ChunkRead::start(&tasks, &[TaskBlock::Read], &keys, &mut carried)?;
    assert!(
        watched.upgrade().is_none(),
        "the carried file is closed before the chunk asks for its own"
    );
    assert!(carried.is_none(), "nothing is carried into the chunk");
    let [job] = <[Job; 1]>::try_from(jobs)
        .unwrap_or_else(|_| panic!("one job: the file of the table read"));
    let ctx = ctx_for(tree, keys.to_vec())?;
    let JobDone::Opened { tag, file } = job.run(&ctx) else {
        panic!("an open job opens a file");
    };
    chunk.opened(tag, file?);

    let mut keep_room = 0;
    for mut read in chunk.take_reads(&tasks, &mut carried)? {
        let filled = read.file.read_at(&mut read.buf, read.offset)?;
        assert_eq!(filled, read.buf.len(), "the whole block is read");
        chunk.read(&tasks, read.tag, &read.buf, &mut keep_room);
    }
    let mut results: Vec<Option<InternalValue>> = alloc::vec![None];
    chunk.finish(&tasks, &mut results, None)?;
    assert!(
        carried
            .as_ref()
            .is_some_and(|(id, _)| *id == task.table.id()),
        "the chunk carries its table's file over to the next"
    );
    assert!(
        results.first().is_some_and(Option::is_some),
        "the chunk reads the key"
    );
    Ok(())
}

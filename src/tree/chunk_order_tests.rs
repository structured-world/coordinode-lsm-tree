// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use crate::fs::{BlockRead, Fs, FsDirEntry, FsFile, FsMetadata, FsOpenOptions, StdFs};
use crate::io;
use crate::path::Path;
use crate::{AbstractTree, Config, SeqNo, SequenceNumberCounter, Tree, value::InternalValue};
use alloc::sync::Arc;

/// A backend that hands each batch's reads over last to first, as a ring may
/// when the later reads of a batch complete first.
struct ReverseCompletionFs;

impl Fs for ReverseCompletionFs {
    fn read_blocks_batched_each(
        &self,
        reqs: &mut [BlockRead<'_>],
        on_read: &mut dyn FnMut(usize, &BlockRead<'_>),
    ) -> io::Result<()> {
        self.read_blocks_batched(reqs)?;
        for (index, req) in reqs.iter().enumerate().rev() {
            on_read(index, req);
        }
        Ok(())
    }

    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
        StdFs.open(path, opts)
    }
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        StdFs.create_dir_all(path)
    }
    fn read_dir(&self, path: &Path) -> io::Result<Vec<FsDirEntry>> {
        StdFs.read_dir(path)
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        StdFs.remove_file(path)
    }
    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        StdFs.remove_dir_all(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        StdFs.rename(from, to)
    }
    fn metadata(&self, path: &Path) -> io::Result<FsMetadata> {
        StdFs.metadata(path)
    }
    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        StdFs.sync_directory(path)
    }
    fn exists(&self, path: &Path) -> io::Result<bool> {
        StdFs.exists(path)
    }
}

/// Two level-0 tables holding one key at one seqno: the chunked resolve keeps
/// the value a single-key read returns, whatever order the backend hands the
/// two blocks over in. At an equal seqno the task earlier in the plan wins, as
/// it does when the blocks are decoded in plan order.
#[test]
fn a_chunked_resolve_breaks_an_equal_seqno_tie_by_plan_order() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let any = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(ReverseCompletionFs))
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
    let remaining = [(0, crate::hash::hash64(b"key"))];
    let comparator = crate::comparator::default_comparator();
    let (tasks, _) =
        Tree::plan_level_block_tasks(level, &remaining, &keys, SeqNo::MAX, comparator.as_ref())?;
    assert_eq!(tasks.len(), 2, "one block per table");

    let mut results: Vec<Option<InternalValue>> = alloc::vec![None];
    Tree::resolve_block_task_chunk(&tasks, &keys, &mut results, None)?;
    let Some(single) = tree.get("key", SeqNo::MAX)? else {
        panic!("the key was written");
    };
    let Some(Some(chunked)) = results.first() else {
        panic!("the chunk resolves the key");
    };
    assert_eq!(chunked.value, single);
    Ok(())
}

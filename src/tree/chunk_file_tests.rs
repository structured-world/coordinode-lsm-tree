// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use crate::fs::{Fs, FsDirEntry, FsFile, FsMetadata, FsOpenOptions, StdFs};
use crate::io;
use crate::path::Path;
use crate::table::GlobalTableId;
use crate::{
    AbstractTree, Cache, Config, DescriptorTable, SeqNo, SequenceNumberCounter, Tree,
    value::InternalValue,
};
use alloc::sync::{Arc, Weak};
use std::sync::{Mutex, PoisonError};

/// The file a probe watches, and whether it was still open at each file the
/// backend opened.
#[derive(Default)]
struct Probe {
    watched: Option<Weak<dyn FsFile>>,
    alive_at_open: Vec<bool>,
}

/// A backend recording, at each file it opens, whether the watched file is
/// still open.
struct OpenProbeFs {
    probe: Arc<Mutex<Probe>>,
}

impl Fs for OpenProbeFs {
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
        let mut probe = self.probe.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(watched) = &probe.watched {
            let alive = watched.upgrade().is_some();
            probe.alive_at_open.push(alive);
        }
        drop(probe);
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

/// A chunk that starts on a table other than the one the previous chunk
/// carried over lets that file go before it opens its own: the carried file,
/// held by nothing else, is closed by the time the next file opens, so a chunk
/// never holds one descriptor past the cap it was sized to.
#[test]
fn a_chunk_on_another_table_closes_the_carried_file_before_opening() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let probe = Arc::new(Mutex::new(Probe::default()));
    let descriptors = Arc::new(DescriptorTable::new(4));
    let any = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(OpenProbeFs {
        probe: Arc::clone(&probe),
    }))
    .use_cache(Arc::new(Cache::with_capacity_bytes(0)))
    .use_descriptor_table(Some(Arc::clone(&descriptors)))
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
    let remaining = [(0, crate::hash::hash64(b"b"))];
    let comparator = crate::comparator::default_comparator();
    let (tasks, _) = Tree::plan_level_block_tasks(
        level,
        &remaining,
        &keys,
        SeqNo::MAX,
        comparator.as_ref(),
        crate::config::DEFAULT_MULTI_GET_METADATA_BUDGET,
    )?;
    let [task] = tasks.as_slice() else {
        panic!("one block, in the table holding the key");
    };
    // The table's file is not cached, so the chunk opens it.
    descriptors.remove_for_table(&GlobalTableId::from((tree.id(), task.table.id())));

    // The previous chunk's last file, of another table, held by nothing else.
    let carried_file: Arc<dyn FsFile> = Arc::from(StdFs.open(
        &folder.path().join("carried"),
        &FsOpenOptions::new().write(true).create(true),
    )?);
    probe.lock().unwrap_or_else(PoisonError::into_inner).watched =
        Some(Arc::downgrade(&carried_file));
    // Table ids are small here, so one past the task's names another table.
    let mut carried = Some((task.table.id() + 1, carried_file));

    let mut results: Vec<Option<InternalValue>> = alloc::vec![None];
    Tree::resolve_block_task_chunk(
        core::slice::from_ref(task),
        &[super::TaskBlock::Read],
        &mut 0,
        &keys,
        &mut results,
        None,
        &mut carried,
    )?;

    let alive_at_open = core::mem::take(
        &mut probe
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .alive_at_open,
    );
    assert_eq!(
        alive_at_open,
        [false],
        "the chunk opened its table's file once, after the carried one was closed"
    );
    assert!(
        results.first().is_some_and(Option::is_some),
        "the chunk reads the key"
    );
    Ok(())
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A compaction that fails before it installs its result leaves none of the
//! files it created on disk, finished or not, without waiting for the orphan
//! sweep of the next open, and its inputs stay as they were.

#![cfg(feature = "std")]

use lsm_tree::compaction::filter::{CompactionFilter, Context, Factory, ItemAccessor, Verdict};
use lsm_tree::config::{BlockSizePolicy, KvSeparationOptions};
use lsm_tree::fs::{Fault, FaultFs, FaultInjector, FaultOp, FaultRule, StdFs};
use lsm_tree::io::ErrorKind;
use lsm_tree::{
    AbstractTree, AnyTree, Config, MAX_SEQNO, MergeOperator, SequenceNumberCounter, UserValue,
};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use test_log::test;

const KEYS: u64 = 2_000;

/// Above every seqno the fixtures write, so a merge may collect the versions
/// it shadows and fold operands onto their base.
const PAST_EVERY_VERSION: u64 = 10 * KEYS;

fn key(i: u64) -> String {
    format!("key_{i:08}")
}

/// Above the separation threshold of the KV-separated fixtures.
fn value(i: u64, generation: u64) -> Vec<u8> {
    let mut v = format!("gen{generation}-{i}-").into_bytes();
    v.resize(220, b'v');
    v
}

/// Every file name in `folder`; none when the folder does not exist.
fn files(folder: &Path) -> std::io::Result<BTreeSet<String>> {
    match std::fs::read_dir(folder) {
        Ok(entries) => entries
            .map(|e| e.map(|e| e.file_name().to_string_lossy().into_owned()))
            .collect(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(BTreeSet::new()),
        Err(e) => Err(e),
    }
}

/// The table and the blob files of the tree at `root`, as `folder/name`.
fn on_disk(root: &Path) -> std::io::Result<BTreeSet<String>> {
    let mut all = BTreeSet::new();
    for folder in ["tables", "blobs"] {
        all.extend(
            files(&root.join(folder))?
                .into_iter()
                .map(|name| format!("{folder}/{name}")),
        );
    }
    Ok(all)
}

struct Fixture {
    tree: AnyTree,
    injector: Arc<FaultInjector>,
    dir: tempfile::TempDir,
}

fn open(configure: impl FnOnce(Config) -> Config) -> lsm_tree::Result<Fixture> {
    let dir = tempfile::tempdir()?;
    let fs = FaultFs::new(StdFs);
    let injector = fs.injector();
    let tree = configure(
        Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .data_block_size_policy(BlockSizePolicy::all(512))
        .with_fs(fs),
    )
    .open()?;
    Ok(Fixture {
        tree,
        injector,
        dir,
    })
}

/// Fails every `op` under `folder` once `skip` of them have passed.
fn io_error(op: FaultOp, folder: &str, skip: u64) -> FaultRule {
    FaultRule::new(op, Fault::Error(ErrorKind::Other))
        .on_path(folder)
        .skip(skip)
}

/// Arms `rules`, runs a major compaction that must fail, and checks that no
/// table or blob file appeared that was not there before it and that every key
/// still reads `expected`, all without reopening the tree. A file can only go
/// meanwhile: an input an earlier merge retired is unlinked in the background.
fn assert_fails_cleanly(
    f: &Fixture,
    rules: impl IntoIterator<Item = FaultRule>,
    target_size: u64,
    expected: impl Fn(u64) -> Vec<u8>,
) -> lsm_tree::Result<()> {
    let before = on_disk(f.dir.path())?;
    for rule in rules {
        f.injector.arm(rule);
    }
    let result = f.tree.major_compact(target_size, PAST_EVERY_VERSION);
    f.injector.clear();

    assert!(
        result.is_err(),
        "the injected fault must fail the compaction: {result:?}"
    );
    let left_behind: BTreeSet<String> = on_disk(f.dir.path())?
        .difference(&before)
        .cloned()
        .collect();
    assert!(
        left_behind.is_empty(),
        "the failed compaction must leave none of its files behind: {left_behind:?}",
    );
    for i in 0..KEYS {
        assert_eq!(
            f.tree.get(key(i), MAX_SEQNO)?.as_deref(),
            Some(expected(i).as_slice()),
            "key {i} after the failed compaction",
        );
    }
    Ok(())
}

/// The outputs written before the failing one are finished, the failing one
/// is partial: neither stays.
#[test]
fn a_merge_failing_to_write_an_output_leaves_none_of_its_tables() -> lsm_tree::Result<()> {
    let f = open(|c| c)?;
    for i in 0..KEYS {
        f.tree.insert(key(i), value(i, 0), i);
    }
    f.tree.flush_active_memtable(0)?;

    assert_fails_cleanly(
        &f,
        [io_error(FaultOp::Write, "tables", 8)],
        16 * 1024,
        |i| value(i, 0),
    )
}

/// A read error part-way through the inputs stops a merge that has already
/// finished outputs of its own.
#[test]
fn a_merge_failing_to_read_an_input_leaves_none_of_its_tables() -> lsm_tree::Result<()> {
    let f = open(|c| c)?;
    for i in 0..KEYS {
        f.tree.insert(key(i), value(i, 0), i);
    }
    f.tree.flush_active_memtable(0)?;

    assert_fails_cleanly(
        &f,
        [
            io_error(FaultOp::ReadAt, "tables", 300),
            io_error(FaultOp::Read, "tables", 300),
        ],
        16 * 1024,
        |i| value(i, 0),
    )
}

/// One failed sub-compaction aborts the install: the outputs its siblings
/// finished go as well as its own.
#[test]
fn a_failed_sub_compaction_leaves_none_of_the_compactions_tables() -> lsm_tree::Result<()> {
    let f = open(|c| c.compaction_threads(4).subcompaction_min_bytes(0))?;
    for i in 0..KEYS {
        f.tree.insert(key(i), value(i, 0), i);
    }
    f.tree.flush_active_memtable(0)?;
    // Several bottom-level tables give the next merge boundaries to split on.
    f.tree.major_compact(4_096, 0)?;
    for i in 0..KEYS {
        f.tree.insert(key(i), value(i, 1), KEYS + i);
    }
    f.tree.flush_active_memtable(0)?;

    assert_fails_cleanly(&f, [io_error(FaultOp::Write, "tables", 4)], u64::MAX, |i| {
        value(i, 1)
    })
}

/// A merge that relocates the live frames of a stale blob file stops part-way
/// through: neither its relocated blob files nor its tables stay.
#[test]
fn a_failed_relocation_leaves_none_of_its_blob_files() -> lsm_tree::Result<()> {
    let f = open(|c| {
        c.with_kv_separation(Some(
            KvSeparationOptions::default()
                .separation_threshold(64)
                .age_cutoff(1.0)
                .staleness_threshold(0.1)
                .file_target_size(16 * 1024),
        ))
    })?;
    for i in 0..KEYS {
        f.tree.insert(key(i), value(i, 0), i);
    }
    f.tree.flush_active_memtable(0)?;
    // Half of the first generation goes stale. The first merge drops it and
    // records the dead half, so the next one relocates the rest.
    for i in (0..KEYS).step_by(2) {
        f.tree.insert(key(i), value(i, 1), KEYS + i);
    }
    f.tree.flush_active_memtable(0)?;
    f.tree.major_compact(u64::MAX, PAST_EVERY_VERSION)?;

    assert_fails_cleanly(&f, [io_error(FaultOp::Write, "blobs", 4)], u64::MAX, |i| {
        value(i, u64::from(i % 2 == 0))
    })
}

/// A compaction filter that rewrites values into the value log fails part-way
/// through: the blob files it wrote go.
#[test]
fn a_failed_filter_rewrite_leaves_none_of_its_blob_files() -> lsm_tree::Result<()> {
    struct Grow;
    impl CompactionFilter for Grow {
        fn filter_item(&mut self, _: ItemAccessor<'_>, _: &Context) -> lsm_tree::Result<Verdict> {
            Ok(Verdict::ReplaceValue(vec![b'r'; 300].into()))
        }
    }
    struct GrowFactory;
    impl Factory for GrowFactory {
        fn name(&self) -> &str {
            "grow"
        }
        fn make_filter(&self, _: &Context) -> Box<dyn CompactionFilter> {
            Box::new(Grow)
        }
    }

    let f = open(|c| {
        c.with_kv_separation(Some(
            KvSeparationOptions::default()
                .separation_threshold(100)
                .file_target_size(16 * 1024),
        ))
        .with_compaction_filter_factory(Some(Arc::new(GrowFactory)))
    })?;
    for i in 0..KEYS {
        f.tree.insert(key(i), format!("small-{i}"), i);
    }
    f.tree.flush_active_memtable(0)?;

    assert_fails_cleanly(&f, [io_error(FaultOp::Write, "blobs", 4)], u64::MAX, |i| {
        format!("small-{i}").into_bytes()
    })
}

struct Concat;

impl MergeOperator for Concat {
    fn merge(
        &self,
        _key: &[u8],
        base: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> lsm_tree::Result<UserValue> {
        let mut result = base.unwrap_or_default().to_vec();
        for op in operands {
            result.extend_from_slice(op);
        }
        Ok(result.into())
    }
}

/// Folding operands onto a base in the value log writes the merged value back
/// to the value log; a failure part-way through leaves none of those files.
#[test]
fn a_failed_fold_onto_a_separated_base_leaves_none_of_its_blob_files() -> lsm_tree::Result<()> {
    let f = open(|c| {
        c.with_kv_separation(Some(
            KvSeparationOptions::default()
                .separation_threshold(100)
                .file_target_size(16 * 1024),
        ))
        .with_merge_operator(Some(Arc::new(Concat)))
    })?;
    for i in 0..KEYS {
        f.tree.insert(key(i), value(i, 0), i);
    }
    f.tree.flush_active_memtable(0)?;
    for i in 0..KEYS {
        f.tree.merge(key(i), "_A", KEYS + i);
    }
    f.tree.flush_active_memtable(0)?;

    assert_fails_cleanly(&f, [io_error(FaultOp::Write, "blobs", 4)], u64::MAX, |i| {
        let mut v = value(i, 0);
        v.extend_from_slice(b"_A");
        v
    })
}

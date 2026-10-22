// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Opening a batch of trees in one folder: the new trees' names are made
//! durable together, by one sync of that folder after every tree is made, so
//! a power loss before it leaves none of the batch's new trees and one after
//! it leaves all of them.

use lsm_tree::fs::{CrashFs, Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
use lsm_tree::{AbstractTree, AnyTree, Config, KvSeparationOptions, SeqNo, SequenceNumberCounter};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const TREES: usize = 4;

/// The folder the batch makes its trees in, spelled as `Config` spells it.
fn data() -> PathBuf {
    std::path::absolute("/data").expect("absolute path")
}

fn tree_path(i: usize) -> PathBuf {
    data().join(format!("tree{i}"))
}

/// The batch's configs over `fs`: the last one KV-separated.
fn configs(fs: &Arc<dyn Fs>) -> Vec<Config> {
    (0..TREES)
        .map(|i| {
            let config = Config::new(
                tree_path(i),
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            )
            .with_shared_fs(Arc::clone(fs))
            .table_file_refill_inline(true);
            if i == TREES - 1 {
                config.with_kv_separation(Some(KvSeparationOptions::default()))
            } else {
                config
            }
        })
        .collect()
}

/// A crash simulator holding the data folder durably, and a fault layer over
/// it the batch runs on.
fn stack() -> (CrashFs, Arc<FaultFs<CrashFs>>) {
    stack_with(true)
}

/// [`stack`], with the data folder made beforehand when `warm`, and left for
/// the batch to make otherwise.
fn stack_with(warm: bool) -> (CrashFs, Arc<FaultFs<CrashFs>>) {
    let crash = CrashFs::new(MemFs::new()).tracking_folders();
    if warm {
        crash.create_dir_all(&data()).expect("the data folder");
        crash
            .sync_directory(data().parent().expect("a parent"))
            .expect("its name");
    }
    let fault = Arc::new(FaultFs::new(crash.clone()));
    (crash, fault)
}

/// The batch's trees whose folders the crashed disk still holds.
fn named(fs: &CrashFs) -> Vec<usize> {
    (0..TREES)
        .filter(|&i| fs.exists(&tree_path(i)).expect("probe"))
        .collect()
}

/// A power loss after each sync of a batch's opening leaves either none of
/// its trees, before the sync of their folder, or all of them; and the trees
/// it leaves open again, empty. Every sync before the last one of the batch
/// is a power loss with no tree named, since that last one is the folder's.
/// A batch that makes the data folder too syncs that folder's name as well,
/// and a power loss before both leaves no tree either.
#[test]
fn a_power_loss_leaves_all_of_a_batch_or_none() {
    for warm in [true, false] {
        let (_, clean) = stack_with(warm);
        let fs: Arc<dyn Fs> = clean.clone();
        let _ = clean.injector().take_syncs();
        drop(Config::open_many(configs(&fs)).expect("the batch"));
        let syncs = clean.injector().take_syncs();
        assert!(!syncs.is_empty(), "self-check: the batch syncs");

        for kept in 0..=syncs.len() {
            let (crash, fault) = stack_with(warm);
            let fs: Arc<dyn Fs> = fault.clone();
            if kept < syncs.len() {
                fault.injector().power_off_after_syncs(kept);
            }
            let opened = Config::open_many(configs(&fs));
            assert_eq!(
                opened.is_ok(),
                kept == syncs.len(),
                "the batch returns only with every sync made, {kept} of {} (warm: {warm})",
                syncs.len()
            );
            drop(opened);
            crash.crash();

            let expected: Vec<usize> = if kept == syncs.len() {
                (0..TREES).collect()
            } else {
                Vec::new()
            };
            assert_eq!(
                named(&crash),
                expected,
                "power lost after {kept} syncs (warm: {warm})"
            );

            let fs: Arc<dyn Fs> = Arc::new(crash.clone());
            let trees = Config::open_many(configs(&fs)).expect("the batch opens again");
            for tree in &trees {
                assert!(tree.is_empty(SeqNo::MAX, None).expect("read"));
            }
        }
    }
}

/// A batch one of whose trees cannot be made takes back every tree it made,
/// before that one and after a power loss alike, and the data folder too when
/// it made that, and closes them: a retry opens the whole batch.
#[test]
fn a_failed_batch_takes_back_the_trees_it_made() {
    for warm in [true, false] {
        let (crash, fault) = stack_with(warm);
        let fs: Arc<dyn Fs> = fault.clone();
        // The last tree's snapshot is never installed, after every other
        // tree is made.
        fault.injector().arm(
            FaultRule::new(
                FaultOp::Rename,
                Fault::Error(lsm_tree::io::ErrorKind::Other),
            )
            .on_path(tree_path(TREES - 1).to_string_lossy()),
        );
        assert!(Config::open_many(configs(&fs)).is_err(), "the batch fails");
        for i in 0..TREES {
            assert!(
                !fault.exists(&tree_path(i)).expect("probe"),
                "tree {i} is taken back (warm: {warm})"
            );
        }
        assert_eq!(
            fault.exists(&data()).expect("probe"),
            warm,
            "the data folder stays only when it was there (warm: {warm})"
        );
        crash.crash();
        assert_eq!(
            named(&crash),
            Vec::<usize>::new(),
            "after a power loss (warm: {warm})"
        );

        fault.injector().clear();
        let trees = Config::open_many(configs(&fs)).expect("the retry");
        assert_eq!(trees.len(), TREES);
    }
}

/// A tree whose open fails after it was made, registering its dictionary,
/// is taken back, alone as in a batch: no tree is left that the caller was
/// told was not opened.
#[cfg(feature = "zstd")]
#[test]
fn a_tree_whose_dictionary_cannot_be_registered_is_taken_back() {
    let dictionary = Arc::new(lsm_tree::ZstdDictionary::new(
        &b"a dictionary the open registers".repeat(40),
    ));
    for batched in [true, false] {
        let (_, fault) = stack();
        let fs: Arc<dyn Fs> = fault.clone();
        // The dictionary file is never published.
        fault.injector().arm(
            FaultRule::new(
                FaultOp::Rename,
                Fault::Error(lsm_tree::io::ErrorKind::Other),
            )
            .on_path("dicts"),
        );
        let mut configs = configs(&fs);
        let first = configs
            .remove(0)
            .zstd_dictionary(Some(Arc::clone(&dictionary)));
        configs.insert(0, first);
        let failed = if batched {
            Config::open_many(configs).is_err()
        } else {
            configs.swap_remove(0).open().is_err()
        };
        assert!(failed, "the open fails (batched: {batched})");
        assert!(
            !fault.exists(&tree_path(0)).expect("probe"),
            "the tree is taken back (batched: {batched})"
        );
    }
}

/// A tree of the batch that exists already is opened as it is, keeps its
/// data, and is left in place by a batch that fails after it.
#[test]
fn a_batch_opens_a_tree_that_exists_and_never_takes_it_back() {
    let (_, fault) = stack();
    let fs: Arc<dyn Fs> = fault.clone();
    {
        let first = configs(&fs).into_iter().next().expect("a config");
        let tree = first.open().expect("the existing tree");
        tree.insert("kept", "value", 0);
        tree.flush_active_memtable(0).expect("flush");
    }
    let syncs_of_data = |fault: &FaultFs<CrashFs>| {
        fault
            .injector()
            .take_syncs()
            .into_iter()
            .filter(|(path, _)| *path == data())
            .count()
    };
    let _ = syncs_of_data(&fault);

    fault.injector().arm(
        FaultRule::new(
            FaultOp::Rename,
            Fault::Error(lsm_tree::io::ErrorKind::Other),
        )
        .on_path(tree_path(1).to_string_lossy()),
    );
    assert!(Config::open_many(configs(&fs)).is_err(), "the batch fails");
    assert!(
        fault.exists(&tree_path(0)).expect("probe"),
        "the existing tree stays"
    );
    fault.injector().clear();

    let trees: Vec<AnyTree> = Config::open_many(configs(&fs)).expect("the batch");
    assert_eq!(
        trees[0].get("kept", SeqNo::MAX).expect("read").as_deref(),
        Some(&b"value"[..])
    );
    assert_eq!(syncs_of_data(&fault), 1, "one sync for the new trees");
}

/// Configs that each hold their own handle of one backend, as configs made
/// with `Config::new` each hold their own `StdFs`, share their folder: it is
/// synced once for all of them.
#[test]
fn a_batch_syncs_its_folder_once_through_separate_handles_of_one_backend() {
    let memory = MemFs::new();
    memory.create_dir_all(&data()).expect("the data folder");
    let injector = Arc::new(lsm_tree::fs::FaultInjector::new());
    let configs: Vec<Config> = (0..TREES)
        .map(|i| {
            let handle = FaultFs::with_injector(memory.clone(), Arc::clone(&injector));
            Config::new(
                tree_path(i),
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            )
            .with_fs(handle)
        })
        .collect();
    let trees = Config::open_many(configs).expect("the batch");
    assert_eq!(trees.len(), TREES);
    let synced = injector
        .take_syncs()
        .into_iter()
        .filter(|(path, _)| *path == data())
        .count();
    assert_eq!(synced, 1, "one sync of the data folder");
}

/// A tree a batch made in a folder that existed already, empty, is taken
/// back by a batch that fails after it: what the tree put there goes, the
/// folder stays, and a retry makes the tree anew rather than recovering one
/// the failed batch left.
#[test]
fn a_failed_batch_takes_back_a_tree_it_made_in_a_folder_that_existed() {
    let (_, fault) = stack();
    let fs: Arc<dyn Fs> = fault.clone();
    fault
        .create_dir_all(&tree_path(0))
        .expect("an empty tree folder");
    fault.injector().arm(
        FaultRule::new(
            FaultOp::Rename,
            Fault::Error(lsm_tree::io::ErrorKind::Other),
        )
        .on_path(tree_path(1).to_string_lossy()),
    );
    assert!(Config::open_many(configs(&fs)).is_err(), "the batch fails");
    assert!(
        fault.exists(&tree_path(0)).expect("probe"),
        "the folder that existed stays"
    );
    assert_eq!(
        fault
            .read_dir(&tree_path(0))
            .expect("list the folder")
            .into_iter()
            .map(|entry| entry.file_name)
            .collect::<Vec<_>>(),
        Vec::<String>::new(),
        "nothing the tree put there is left"
    );
}

/// Trees made in folders on two backends have each folder synced once, on
/// its own backend.
#[test]
fn a_batch_across_backends_syncs_each_folder_on_its_own() {
    let one = Arc::new(FaultFs::new(MemFs::new()));
    let other = Arc::new(FaultFs::new(MemFs::new()));
    for fs in [&one, &other] {
        fs.create_dir_all(&data()).expect("the data folder");
        let _ = fs.injector().take_syncs();
    }
    let config = |path: &Path, fs: &Arc<FaultFs<MemFs>>| {
        Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(fs.clone())
    };
    let trees = Config::open_many([
        config(&tree_path(0), &one),
        config(&tree_path(1), &other),
        config(&tree_path(2), &one),
    ])
    .expect("the batch");
    assert_eq!(trees.len(), 3);
    for (fs, expected) in [(&one, 1), (&other, 1)] {
        let synced = fs
            .injector()
            .take_syncs()
            .into_iter()
            .filter(|(path, _)| *path == data())
            .count();
        assert_eq!(synced, expected);
    }
}

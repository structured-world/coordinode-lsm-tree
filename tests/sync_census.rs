// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The syncs each tree operation issues. A sync is a device round trip (a
//! full cache flush under `F_FULLFSYNC`), so an operation pays for each one,
//! and one that makes nothing new durable is waste: these tests pin what
//! every operation syncs, by file and kind.

use lsm_tree::config::LevelRoute;
use lsm_tree::fs::{FaultFs, FaultInjector, MemFs, SyncKind};
use lsm_tree::{AbstractTree, AnyTree, Config, KvSeparationOptions, SequenceNumberCounter};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// What a sync made durable, named by its place in the tree.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Synced {
    TreeDir,
    ParentDir,
    TablesDir,
    BlobsDir,
    RouteDir,
    RouteTablesDir,
    Table,
    Blob,
    EditLog,
    Snapshot,
    Other(String),
}

struct Census {
    base: PathBuf,
    route: Option<PathBuf>,
    injector: Arc<FaultInjector>,
}

impl Census {
    /// A census of the syncs `injector` sees for a tree at `base`, routed to
    /// `route` when given. The paths are made absolute as `Config` makes
    /// them, so a sync is matched by the path the tree issues it on (on
    /// Windows `/census` is `<drive>:\census`).
    fn new(base: &Path, route: Option<&Path>, injector: Arc<FaultInjector>) -> Self {
        let absolute = |path: &Path| std::path::absolute(path).expect("absolute path");
        Self {
            base: absolute(base),
            route: route.map(absolute),
            injector,
        }
    }

    fn name(&self, path: &Path, kind: SyncKind) -> (Synced, SyncKind) {
        let file_name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let synced = if kind == SyncKind::Directory {
            if path == self.base {
                Synced::TreeDir
            } else if Some(path) == self.base.parent() {
                Synced::ParentDir
            } else if path == self.base.join("tables") {
                Synced::TablesDir
            } else if path == self.base.join("blobs") {
                Synced::BlobsDir
            } else if Some(path) == self.route.as_deref() {
                Synced::RouteDir
            } else if self
                .route
                .as_ref()
                .is_some_and(|r| path == r.join("tables"))
            {
                Synced::RouteTablesDir
            } else {
                Synced::Other(path.display().to_string())
            }
        } else if path.parent().is_some_and(|p| p.ends_with("tables")) {
            Synced::Table
        } else if path.parent().is_some_and(|p| p.ends_with("blobs")) {
            Synced::Blob
        } else if file_name.starts_with("edits-") {
            Synced::EditLog
        } else if file_name
            .strip_prefix('v')
            .map(|rest| rest.strip_suffix(".tmp").unwrap_or(rest))
            .is_some_and(|id| !id.is_empty() && id.chars().all(|c| c.is_ascii_digit()))
        {
            // A snapshot is synced under its temporary name, before the rename
            // that installs it.
            Synced::Snapshot
        } else {
            Synced::Other(path.display().to_string())
        };
        (synced, kind)
    }

    /// The syncs since the last call, sorted.
    fn take(&self) -> Vec<(Synced, SyncKind)> {
        let mut syncs: Vec<_> = self
            .injector
            .take_syncs()
            .iter()
            .map(|(path, kind)| self.name(path, *kind))
            .collect();
        syncs.sort();
        syncs
    }
}

fn sorted(mut syncs: Vec<(Synced, SyncKind)>) -> Vec<(Synced, SyncKind)> {
    syncs.sort();
    syncs
}

/// A tree at `base` over `fs`, KV-separated when `blob`, its every level
/// routed to `route` when given.
fn open(base: &Path, fs: &Arc<FaultFs<MemFs>>, blob: bool, route: Option<&Path>) -> AnyTree {
    // The folder the tree is made in exists, as it does for a caller: the
    // census counts what the tree makes, not the folders above it.
    {
        use lsm_tree::fs::Fs;
        let absolute = std::path::absolute(base).expect("absolute path");
        if let Some(parent) = absolute.parent() {
            fs.create_dir_all(parent).expect("the tree's parent");
        }
    }
    let config = Config::new(
        base,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(fs.clone());
    let config = match route {
        Some(route) => config.level_routes(vec![LevelRoute {
            levels: 0..7,
            path: route.to_path_buf(),
            fs: fs.clone(),
        }]),
        None => config,
    };
    let config = if blob {
        config.with_kv_separation(Some(
            KvSeparationOptions::default().separation_threshold(64),
        ))
    } else {
        config
    };
    config.open().expect("open")
}

fn fill(tree: &AnyTree, round: u64) {
    for i in 0..1_000u64 {
        tree.insert(
            format!("key{i:08}"),
            vec![u8::try_from(round).expect("a round fits a byte"); 200],
            round * 10_000 + i,
        );
    }
}

/// What creating a tree syncs: the snapshot, the tree folder once (its
/// entries, the tables folder and the snapshot's empty log among them, are
/// all made before), and once the folder above, for the tree folder's own
/// name.
fn creation() -> Vec<(Synced, SyncKind)> {
    sorted(vec![
        (Synced::Snapshot, SyncKind::FileData),
        (Synced::TreeDir, SyncKind::Directory),
        (Synced::ParentDir, SyncKind::Directory),
    ])
}

/// What a flush syncs once its tables folder's pool is warm: its table, the
/// edit naming it, and for a KV-separated tree its blob file and the blob
/// folder for that file's new name. `first` adds the tables folder, synced
/// for the batch of pooled files the first flush makes.
fn flush_syncs(blob: bool, first: bool) -> Vec<(Synced, SyncKind)> {
    let mut flush = vec![
        (Synced::Table, SyncKind::FileData),
        (Synced::EditLog, SyncKind::FileData),
    ];
    if first {
        flush.push((Synced::TablesDir, SyncKind::Directory));
    }
    if blob {
        flush.push((Synced::Blob, SyncKind::FileData));
        flush.push((Synced::BlobsDir, SyncKind::Directory));
    }
    sorted(flush)
}

/// Creating, flushing, compacting, dropping and reopening a tree, standard
/// and KV-separated, each syncs only what it makes durable: a new entry's
/// folder once, a written file once, and a reopen the snapshot it installs.
/// A table is written into a file whose name a batch made durable, so once
/// the batch is made a flush and a compaction sync their tables and the edit
/// naming them, and no folder for them.
#[test]
fn each_tree_operation_syncs_only_what_it_makes_durable() {
    for blob in [false, true] {
        let base = PathBuf::from("/census").join(if blob { "blob" } else { "standard" });
        let fs = Arc::new(FaultFs::new(MemFs::new()));
        let census = Census::new(&base, None, fs.injector());

        let tree = open(&base, &fs, blob, None);
        assert_eq!(census.take(), creation(), "creating (blob: {blob})");

        fill(&tree, 1);
        tree.flush_active_memtable(0).expect("flush");
        assert_eq!(
            census.take(),
            flush_syncs(blob, true),
            "the first flush (blob: {blob})"
        );

        fill(&tree, 2);
        tree.flush_active_memtable(0).expect("flush");
        assert_eq!(
            census.take(),
            flush_syncs(blob, false),
            "a flush into a pooled file (blob: {blob})"
        );

        for round in 3..=4 {
            fill(&tree, round);
            tree.flush_active_memtable(0).expect("flush");
        }
        census.take();
        tree.major_compact(64 * 1024 * 1024, u64::MAX)
            .expect("compact");
        assert_eq!(
            census.take(),
            sorted(vec![
                (Synced::Table, SyncKind::FileData),
                (Synced::EditLog, SyncKind::FileData),
            ]),
            "a compaction into one pooled file (blob: {blob})"
        );

        tree.drop_range::<&[u8], _>(..).expect("drop");
        assert_eq!(
            census.take(),
            vec![(Synced::EditLog, SyncKind::FileData)],
            "dropping every key (blob: {blob})"
        );

        // A reopen installs what it recovered as a fresh snapshot, so what it
        // serves is durable whatever the syncs before it reported.
        drop(tree);
        let tree = open(&base, &fs, blob, None);
        assert_eq!(
            census.take(),
            sorted(vec![
                (Synced::Snapshot, SyncKind::FileData),
                (Synced::TreeDir, SyncKind::Directory),
            ]),
            "reopening (blob: {blob})"
        );

        // The reopen made the log it installed durable by name, so the first
        // flush after it does not sync the tree folder again. It takes a file
        // the open found pooled, whose name it cannot know was ever synced,
        // so it syncs the tables folder once for all of them.
        fill(&tree, 5);
        tree.flush_active_memtable(0).expect("flush");
        assert_eq!(
            census.take(),
            flush_syncs(blob, true),
            "the first flush after reopening (blob: {blob})"
        );
        fill(&tree, 6);
        tree.flush_active_memtable(0).expect("flush");
        assert_eq!(
            census.take(),
            flush_syncs(blob, false),
            "the next one (blob: {blob})"
        );
        drop(tree);
    }
}

/// Over a run of flushes the pool syncs the tables folder once per batch: 33
/// flushes under the default batch of 16 sync it three times, at the flushes
/// that make a batch, where 33 flushes without a pool sync it 33 times. Every
/// flush syncs its table and its edit either way.
#[test]
fn a_run_of_flushes_syncs_the_tables_folder_once_per_batch() {
    const FLUSHES: u64 = 33;
    /// The batch a tree makes when its configuration does not say.
    const DEFAULT_BATCH: usize = 16;
    for pool in [DEFAULT_BATCH, 0] {
        let base = PathBuf::from("/census/amortized").join(pool.to_string());
        let fs = Arc::new(FaultFs::new(MemFs::new()));
        let census = Census::new(&base, None, fs.injector());
        let config = Config::new(
            &base,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(fs.clone());
        // The default arm sets nothing, so it runs the default batch.
        let config = if pool == 0 {
            config.table_file_pool(0)
        } else {
            config
        };
        let tree = config.open().expect("open");
        census.take();

        let mut folder_syncs = Vec::new();
        for round in 1..=FLUSHES {
            fill(&tree, round);
            tree.flush_active_memtable(0).expect("flush");
            let syncs = census.take();
            let count = |synced: &Synced| syncs.iter().filter(|(s, _)| s == synced).count();
            assert_eq!(count(&Synced::Table), 1, "flush {round} (pool {pool})");
            assert_eq!(count(&Synced::EditLog), 1, "flush {round} (pool {pool})");
            if count(&Synced::TablesDir) > 0 {
                folder_syncs.push(round);
            }
        }
        let expected: Vec<u64> = if pool == 0 {
            (1..=FLUSHES).collect()
        } else {
            (1..=FLUSHES).step_by(pool).collect()
        };
        assert_eq!(folder_syncs, expected, "pool {pool}");
    }
}

/// Without a pool every table file is made as its table is written, and the
/// install naming it syncs its folder.
#[test]
fn without_a_pool_each_install_syncs_the_tables_folder() {
    let base = PathBuf::from("/census/unpooled");
    let fs = Arc::new(FaultFs::new(MemFs::new()));
    let census = Census::new(&base, None, fs.injector());
    let tree = Config::new(
        &base,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(fs.clone())
    .table_file_pool(0)
    .open()
    .expect("open");
    census.take();

    for round in 1..=2 {
        fill(&tree, round);
        tree.flush_active_memtable(0).expect("flush");
        assert_eq!(
            census.take(),
            sorted(vec![
                (Synced::Table, SyncKind::FileData),
                (Synced::TablesDir, SyncKind::Directory),
                (Synced::EditLog, SyncKind::FileData),
            ]),
            "flush {round}"
        );
    }
}

/// Without a pool a KV-separated tree's flush and an ingestion of either kind
/// make their table files as they write them, so the install that names them
/// syncs the tables folder too.
#[test]
fn without_a_pool_every_write_kind_syncs_the_tables_folder() {
    for blob in [false, true] {
        let base = PathBuf::from("/census").join(if blob {
            "unpooled-blob"
        } else {
            "unpooled-standard"
        });
        let fs = Arc::new(FaultFs::new(MemFs::new()));
        let census = Census::new(&base, None, fs.injector());
        let config = Config::new(
            &base,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(fs.clone())
        .table_file_pool(0);
        let config = if blob {
            config.with_kv_separation(Some(
                KvSeparationOptions::default().separation_threshold(64),
            ))
        } else {
            config
        };
        let tree = config.open().expect("open");
        census.take();

        fill(&tree, 1);
        tree.flush_active_memtable(0).expect("flush");
        assert!(
            census
                .take()
                .contains(&(Synced::TablesDir, SyncKind::Directory)),
            "a flush (blob: {blob})"
        );

        let mut ingestion = tree.ingestion().expect("ingestion");
        for i in 0..100u32 {
            ingestion
                .write(format!("ingested{i:04}"), vec![7u8; 200])
                .expect("write");
        }
        ingestion.finish().expect("finish");
        assert!(
            census
                .take()
                .contains(&(Synced::TablesDir, SyncKind::Directory)),
            "an ingestion (blob: {blob})"
        );
    }
}

/// Under barrier syncs a pooled table on another device than the manifest
/// still has its folder synced in full: that is the sync that makes the
/// table's data durable on its device before the edit on the other one names
/// it.
#[test]
fn a_pooled_table_on_another_device_flushes_that_device() {
    use lsm_tree::fs::{Fs, SyncMode};
    let base = std::path::absolute("/census/barrier/tree").expect("absolute path");
    let route = std::path::absolute("/elsewhere/hot").expect("absolute path");
    let tree_fs = Arc::new(FaultFs::new(MemFs::new()));
    let route_fs = Arc::new(FaultFs::new(MemFs::new()));
    tree_fs
        .create_dir_all(base.parent().expect("a parent"))
        .expect("the tree's parent");
    route_fs
        .create_dir_all(route.parent().expect("a parent"))
        .expect("the route's parent");
    let tree = Config::new(
        &base,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(tree_fs.clone())
    .sync_mode(SyncMode::Barrier)
    .level_routes(vec![LevelRoute {
        levels: 0..7,
        path: route.clone(),
        fs: route_fs.clone(),
    }])
    .open()
    .expect("open");
    fill(&tree, 1);
    tree.flush_active_memtable(0).expect("flush");
    let before = route_fs
        .injector()
        .directory_sync_modes_for("elsewhere")
        .len();

    fill(&tree, 2);
    tree.flush_active_memtable(0).expect("flush");
    let modes = route_fs.injector().directory_sync_modes_for("elsewhere");
    assert_eq!(
        modes.get(before..),
        Some(&[SyncMode::Full][..]),
        "the route's tables folder, once, in full"
    );
}

/// A flush that rotates the manifest installs its fresh snapshot with two
/// syncs: the snapshot's bytes, then the tree folder for the names of the
/// renamed snapshot and its empty log.
#[test]
fn a_rotating_install_syncs_the_snapshot_and_its_folder_once() {
    let base = PathBuf::from("/census/rotating");
    let fs = Arc::new(FaultFs::new(MemFs::new()));
    let census = Census::new(&base, None, fs.injector());
    let tree = Config::new(
        &base,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(fs.clone())
    .manifest_log_rotate_bytes(0)
    .open()
    .expect("open");
    census.take();

    fill(&tree, 1);
    tree.flush_active_memtable(0).expect("flush");
    assert_eq!(
        census.take(),
        sorted(vec![
            (Synced::Table, SyncKind::FileData),
            (Synced::TablesDir, SyncKind::Directory),
            (Synced::Snapshot, SyncKind::FileData),
            (Synced::TreeDir, SyncKind::Directory),
        ]),
        "a flush whose install rotates"
    );
}

/// A tree whose tables live in a routed folder syncs that folder's new
/// entries too: the route's tables folder in the route, and the route in the
/// folder above it.
#[test]
fn creating_a_routed_tree_syncs_the_route_once() {
    let base = PathBuf::from("/census/routed/tree");
    let route = PathBuf::from("/census/routed/hot");
    let fs = Arc::new(FaultFs::new(MemFs::new()));
    let census = Census::new(&base, Some(&route), fs.injector());

    let tree = open(&base, &fs, false, Some(&route));
    // The folder above the route is the one above the tree here: one sync of
    // it makes both entries durable.
    let mut expected = creation();
    expected.push((Synced::RouteDir, SyncKind::Directory));
    assert_eq!(census.take(), sorted(expected), "creating");

    fill(&tree, 1);
    tree.flush_active_memtable(0).expect("flush");
    assert_eq!(
        census.take(),
        sorted(vec![
            (Synced::Table, SyncKind::FileData),
            (Synced::RouteTablesDir, SyncKind::Directory),
            (Synced::EditLog, SyncKind::FileData),
        ]),
        "the first flush"
    );
}

/// A route inside the tree folder holds a new entry of its own, its tables
/// folder: the tree folder's sync makes only the route's name durable, so
/// creating the tree syncs the route too.
#[test]
fn creating_a_tree_with_a_route_inside_it_syncs_the_route() {
    let base = PathBuf::from("/census/nested/tree");
    let route = base.join("hot");
    let fs = Arc::new(FaultFs::new(MemFs::new()));
    let census = Census::new(&base, Some(&route), fs.injector());

    let _tree = open(&base, &fs, false, Some(&route));
    let mut expected = creation();
    expected.push((Synced::RouteDir, SyncKind::Directory));
    assert_eq!(census.take(), sorted(expected), "creating");
}

/// The folders above a new tree are synced before its snapshot is written:
/// a sync that fails then fails a creation that left no tree behind, rather
/// than one a retry opens without ever syncing them.
#[test]
fn creating_a_tree_syncs_the_folders_above_it_before_its_snapshot() {
    let base = PathBuf::from("/census/ordered/tree");
    let route = PathBuf::from("/census/ordered/hot");
    let fs = Arc::new(FaultFs::new(MemFs::new()));
    let census = Census::new(&base, Some(&route), fs.injector());

    let _tree = open(&base, &fs, false, Some(&route));
    let syncs: Vec<_> = fs
        .injector()
        .take_syncs()
        .iter()
        .map(|(path, kind)| census.name(path, *kind).0)
        .collect();
    let at = |synced: &Synced| {
        syncs
            .iter()
            .position(|s| s == synced)
            .unwrap_or_else(|| panic!("{synced:?} synced: {syncs:?}"))
    };
    let snapshot = at(&Synced::Snapshot);
    for above in [Synced::ParentDir, Synced::RouteDir] {
        assert!(
            at(&above) < snapshot,
            "{above:?} before the snapshot: {syncs:?}"
        );
    }
}

/// A tree made where several folders above it do not exist yet syncs each
/// folder that gained an entry: the open makes the whole chain to lock the
/// tree folder, and every name in it is new, not only the tree folder's.
#[test]
fn creating_a_tree_under_missing_folders_syncs_each_of_them() {
    use lsm_tree::fs::Fs;
    let root = std::path::absolute("/census/deep").expect("absolute path");
    let base = root.join("a").join("b").join("tree");
    let fs = Arc::new(FaultFs::new(MemFs::new()));
    fs.create_dir_all(&root).expect("the folder that exists");

    let _tree = Config::new(
        &base,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(fs.clone())
    .open()
    .expect("open");
    let synced: Vec<PathBuf> = fs
        .injector()
        .take_syncs()
        .into_iter()
        .filter(|(_, kind)| *kind == SyncKind::Directory)
        .map(|(path, _)| path)
        .collect();
    for holder in [root.clone(), root.join("a"), root.join("a").join("b")] {
        assert!(
            synced.contains(&holder),
            "{} gained an entry and is synced: {synced:?}",
            holder.display()
        );
    }
}

/// A creation that failed on a folder sync leaves its folders behind, made but
/// not known durable; the retry finds them existing and syncs each of them
/// all the same, up the chain, since it cannot tell they were made by a
/// creation that never finished.
#[test]
fn a_retried_creation_syncs_the_folders_a_failed_one_left() {
    use lsm_tree::fs::{Fault, FaultOp, FaultRule, Fs};
    let root = std::path::absolute("/census/retry").expect("absolute path");
    let base = root.join("a").join("b").join("tree");
    let fs = Arc::new(FaultFs::new(MemFs::new()));
    fs.create_dir_all(&root).expect("the folder that exists");
    let config = || {
        Config::new(
            &base,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(fs.clone())
    };
    fs.injector().arm(
        FaultRule::new(
            FaultOp::SyncDirectory,
            Fault::Error(lsm_tree::io::ErrorKind::Other),
        )
        .on_path(root.to_string_lossy())
        .once(),
    );
    assert!(
        config().open().is_err(),
        "self-check: the creation fails on a folder sync"
    );
    assert!(
        !fs.exists(&root.join("a")).expect("probe"),
        "the failed creation removes the folders it made"
    );
    let _ = fs.injector().take_syncs();

    let _tree = config().open().expect("the retry");
    let synced: Vec<PathBuf> = fs
        .injector()
        .take_syncs()
        .into_iter()
        .filter(|(_, kind)| *kind == SyncKind::Directory)
        .map(|(path, _)| path)
        .collect();
    for holder in [root.clone(), root.join("a"), root.join("a").join("b")] {
        assert!(
            synced.contains(&holder),
            "{} is synced by the retry: {synced:?}",
            holder.display()
        );
    }
}

/// A failed creation that cannot remove the folders it made syncs their
/// holders instead: a retry finds them as folders that exist and syncs none
/// of them, so their names must be durable before the failure returns.
#[test]
fn a_failed_creation_that_keeps_its_folders_syncs_them() {
    use lsm_tree::fs::{Fault, FaultOp, FaultRule, Fs};
    let root = std::path::absolute("/census/kept").expect("absolute path");
    let base = root.join("a").join("b").join("tree");
    let fs = Arc::new(FaultFs::new(MemFs::new()));
    fs.create_dir_all(&root).expect("the folder that exists");
    fs.injector().arm(
        FaultRule::new(
            FaultOp::SyncDirectory,
            Fault::Error(lsm_tree::io::ErrorKind::Other),
        )
        .on_path(root.to_string_lossy())
        .once(),
    );
    fs.injector().arm(FaultRule::new(
        FaultOp::RemoveDirAll,
        Fault::Error(lsm_tree::io::ErrorKind::PermissionDenied),
    ));
    let _ = fs.injector().take_syncs();

    assert!(
        Config::new(
            &base,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(fs.clone())
        .open()
        .is_err(),
        "self-check: the creation fails on a folder sync"
    );
    assert!(
        fs.exists(&root.join("a")).expect("probe"),
        "self-check: the folders stay"
    );
    let synced: Vec<PathBuf> = fs
        .injector()
        .take_syncs()
        .into_iter()
        .filter(|(_, kind)| *kind == SyncKind::Directory)
        .map(|(path, _)| path)
        .collect();
    assert_eq!(
        synced,
        vec![
            // The rule matches every path under the root: the first sync,
            // of the nearest holder, is the one that fails.
            root.join("a").join("b"),
            // Then every holder, the failed one included, is made durable.
            root.join("a").join("b"),
            root.join("a"),
            root.clone(),
        ],
        "the failed sync and the ones that make every kept name durable"
    );
}

/// A creation in a tree folder that exists already gives no folder above it
/// a new entry, so it syncs none of them: one it may not read (a folder
/// others may only pass through) would fail the creation.
#[test]
fn a_creation_in_a_folder_that_exists_syncs_nothing_above_it() {
    use lsm_tree::fs::Fs;
    let base = std::path::absolute("/census/made/tree").expect("absolute path");
    let fs = Arc::new(FaultFs::new(MemFs::new()));
    fs.create_dir_all(&base)
        .expect("the tree folder, made beforehand");
    let _tree = Config::new(
        &base,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(fs.clone())
    .open()
    .expect("open");

    let above: Vec<PathBuf> = fs
        .injector()
        .take_syncs()
        .into_iter()
        .filter(|(path, kind)| *kind == SyncKind::Directory && !path.starts_with(&base))
        .map(|(path, _)| path)
        .collect();
    assert!(above.is_empty(), "synced above the tree: {above:?}");
}

/// Under barrier syncs a deep route on another device than the tree's
/// flushes that device once, with the last of its folder syncs; the ones
/// before it are ordered ahead of it on the same device.
#[test]
fn a_new_route_on_another_device_flushes_it_once() {
    use lsm_tree::fs::{Fs, SyncMode};
    let base = std::path::absolute("/census/flush/tree").expect("absolute path");
    let route = std::path::absolute("/elsewhere/a/b/c/hot").expect("absolute path");
    let tree_fs = Arc::new(FaultFs::new(MemFs::new()));
    let route_fs = Arc::new(FaultFs::new(MemFs::new()));
    tree_fs
        .create_dir_all(base.parent().expect("a parent"))
        .expect("the tree's parent");
    // Absolute as the route is: on Windows `/elsewhere` is `<drive>:\elsewhere`,
    // and a root made under another spelling would leave the route's chain
    // running up to the drive, past the folders this test watches.
    route_fs
        .create_dir_all(&std::path::absolute("/elsewhere").expect("absolute path"))
        .expect("the route's root");
    let _tree = Config::new(
        &base,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(tree_fs.clone())
    .sync_mode(SyncMode::Barrier)
    .level_routes(vec![LevelRoute {
        levels: 0..7,
        path: route,
        fs: route_fs.clone(),
    }])
    .open()
    .expect("open");

    let modes = route_fs.injector().directory_sync_modes_for("elsewhere");
    assert!(
        modes.len() >= 4,
        "every new folder's holder is synced: {modes:?}"
    );
    assert_eq!(
        modes.iter().filter(|mode| **mode == SyncMode::Full).count(),
        1,
        "one flush of the route's device: {modes:?}"
    );
    assert_eq!(
        modes.last(),
        Some(&SyncMode::Full),
        "the last one: {modes:?}"
    );
}

/// An open of a KV-separated tree that makes its missing `blobs/` folder
/// makes that folder's name durable: an edit the open itself installs (a
/// dictionary it registers) spends the first edit's folder sync, so no later
/// flush would.
#[test]
fn an_open_that_makes_the_blob_folder_syncs_its_name() {
    use lsm_tree::fs::Fs;
    let base = PathBuf::from("/census/blobs-again/tree");
    let fs = Arc::new(FaultFs::new(MemFs::new()));
    let census = Census::new(&base, None, fs.injector());
    drop(open(&base, &fs, true, None));
    let blobs = std::path::absolute(&base)
        .expect("absolute path")
        .join("blobs");
    fs.remove_dir_all(&blobs)
        .expect("lose the empty blob folder");
    let _ = census.take();

    let _tree = open(&base, &fs, true, None);
    assert!(
        census
            .take()
            .contains(&(Synced::TreeDir, SyncKind::Directory)),
        "the tree folder is synced for the blob folder's name"
    );
}

/// A route whose path lies inside the tree folder but on a backend of its
/// own holds its names on that backend: creating the tree syncs, there, every
/// folder that gained one, the tree folder included, since the snapshot's
/// sync on the tree's backend covers none of them.
#[test]
fn a_route_inside_the_tree_on_another_backend_syncs_its_folders_there() {
    use lsm_tree::fs::Fs;
    let base = std::path::absolute("/census/apart/tree").expect("absolute path");
    let tree_fs = Arc::new(FaultFs::new(MemFs::new()));
    let route_fs = Arc::new(FaultFs::new(MemFs::new()));
    tree_fs
        .create_dir_all(base.parent().expect("a parent"))
        .expect("the tree's parent");
    let _tree = Config::new(
        &base,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(tree_fs.clone())
    .level_routes(vec![LevelRoute {
        levels: 0..7,
        path: base.join("hot"),
        fs: route_fs.clone(),
    }])
    .open()
    .expect("open");

    let synced: Vec<PathBuf> = route_fs
        .injector()
        .take_syncs()
        .into_iter()
        .filter(|(_, kind)| *kind == SyncKind::Directory)
        .map(|(path, _)| path)
        .collect();
    for holder in [base.join("hot"), base.clone()] {
        assert!(
            synced.contains(&holder),
            "{} synced on the route's backend: {synced:?}",
            holder.display()
        );
    }
}

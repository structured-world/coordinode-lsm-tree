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
    CurrentPointer,
    Other(String),
}

struct Census {
    base: PathBuf,
    route: Option<PathBuf>,
    injector: Arc<FaultInjector>,
}

impl Census {
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
        } else if file_name.starts_with('v') && file_name[1..].chars().all(|c| c.is_ascii_digit()) {
            Synced::Snapshot
        } else if file_name.starts_with(".tmp_") {
            Synced::CurrentPointer
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
            vec![round as u8; 200],
            round * 10_000 + i,
        );
    }
}

/// What creating a tree syncs: the snapshot, the pointer to it, the tree
/// folder once for each (its entries, the tables folder among them, are all
/// made before), and once the folder above, for the tree folder's own name.
fn creation() -> Vec<(Synced, SyncKind)> {
    sorted(vec![
        (Synced::Snapshot, SyncKind::FileData),
        (Synced::TreeDir, SyncKind::Directory),
        (Synced::CurrentPointer, SyncKind::File),
        (Synced::TreeDir, SyncKind::Directory),
        (Synced::ParentDir, SyncKind::Directory),
    ])
}

/// Creating, flushing, compacting, dropping and reopening a tree, standard
/// and KV-separated, each syncs only what it makes durable: a new entry's
/// folder once, a written file once.
#[test]
fn each_tree_operation_syncs_only_what_it_makes_durable() {
    for blob in [false, true] {
        let base = PathBuf::from("/census").join(if blob { "blob" } else { "standard" });
        let fs = Arc::new(FaultFs::new(MemFs::new()));
        let census = Census {
            base: base.clone(),
            route: None,
            injector: fs.injector(),
        };

        let tree = open(&base, &fs, blob, None);
        assert_eq!(census.take(), creation(), "creating (blob: {blob})");

        fill(&tree, 1);
        tree.flush_active_memtable(0).expect("flush");
        let mut flush = vec![
            (Synced::Table, SyncKind::FileData),
            (Synced::TablesDir, SyncKind::Directory),
            (Synced::EditLog, SyncKind::FileData),
        ];
        if blob {
            flush.push((Synced::Blob, SyncKind::FileData));
            flush.push((Synced::BlobsDir, SyncKind::Directory));
        }
        let flush = sorted(flush);
        assert_eq!(census.take(), flush, "the first flush (blob: {blob})");

        for round in 2..=4 {
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
                (Synced::TablesDir, SyncKind::Directory),
                (Synced::EditLog, SyncKind::FileData),
            ]),
            "a compaction into one table (blob: {blob})"
        );

        tree.drop_range::<&[u8], _>(..).expect("drop");
        assert_eq!(
            census.take(),
            vec![(Synced::EditLog, SyncKind::FileData)],
            "dropping every key (blob: {blob})"
        );

        drop(tree);
        let tree = open(&base, &fs, blob, None);
        assert_eq!(census.take(), Vec::new(), "reopening (blob: {blob})");
        drop(tree);
    }
}

/// A tree whose tables live in a routed folder syncs that folder's new
/// entries too: the route's tables folder in the route, and the route in the
/// folder above it.
#[test]
fn creating_a_routed_tree_syncs_the_route_once() {
    let base = PathBuf::from("/census/routed/tree");
    let route = PathBuf::from("/census/routed/hot");
    let fs = Arc::new(FaultFs::new(MemFs::new()));
    let census = Census {
        base: base.clone(),
        route: Some(route.clone()),
        injector: fs.injector(),
    };

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

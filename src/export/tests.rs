use super::*;
use crate::{AbstractTree, AnyTree, Config, KvSeparationOptions, SequenceNumberCounter};
use std::collections::BTreeMap;
use test_log::test;

fn open(folder: &std::path::Path, kv: Option<KvSeparationOptions>) -> crate::Result<AnyTree> {
    Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(kv)
    .open()
}

/// The manifest layout of `tree` as the open tree holds it.
fn layout(tree: &AnyTree) -> Vec<Vec<Vec<TableRecord>>> {
    tree.current_version()
        .iter_levels()
        .map(|level| {
            level
                .iter()
                .map(|run| {
                    run.iter()
                        .map(|t| TableRecord {
                            id: t.id(),
                            checksum: t.checksum().into_u128(),
                            global_seqno: t.global_seqno(),
                        })
                        .collect()
                })
                .collect()
        })
        .collect()
}

/// Every file under `dir` with its bytes, to tell whether a read changed
/// anything on disk.
fn snapshot(dir: &std::path::Path) -> std::io::Result<BTreeMap<std::path::PathBuf, Vec<u8>>> {
    let mut files = BTreeMap::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        for entry in std::fs::read_dir(&next)? {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                let bytes = std::fs::read(&path)?;
                files.insert(path, bytes);
            }
        }
    }
    Ok(files)
}

/// The exported state places every table where the open tree places it, with
/// the tree's version id and type, after flushes that left several L0 runs
/// and a compaction that moved tables down.
#[test]
fn read_manifest_places_every_table_where_the_tree_does() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = open(folder.path(), None)?;
    let seqno = SequenceNumberCounter::default();
    for round in 0..3u32 {
        for i in 0..50u32 {
            tree.insert(format!("k{i:03}-{round}"), "v", seqno.next());
        }
        tree.flush_active_memtable(0)?;
    }
    tree.major_compact(u64::MAX, 0)?;
    for i in 0..20u32 {
        tree.insert(format!("z{i:03}"), "v", seqno.next());
    }
    tree.flush_active_memtable(0)?;

    let expected = layout(&tree);
    let version_id = tree.current_version().id();
    drop(tree);

    let state = read_manifest(folder.path(), &crate::fs::StdFs, None)?;
    assert_eq!(state.tree_type, TreeType::Standard);
    assert_eq!(state.version_id, version_id);
    assert_eq!(state.levels, expected);
    assert!(
        state.levels.iter().flatten().flatten().count() > 1,
        "the fixture has tables in more than one place"
    );
    assert!(state.blob_files.is_empty());
    Ok(())
}

/// A blob tree's blob files and their garbage statistics come back by id,
/// matching the open tree's version.
#[test]
fn read_manifest_carries_blob_files_and_their_gc_stats() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = open(
        folder.path(),
        Some(KvSeparationOptions::default().separation_threshold(16)),
    )?;
    let seqno = SequenceNumberCounter::default();
    for round in 0..2u32 {
        for i in 0..40u32 {
            tree.insert(
                format!("k{i:03}"),
                format!("{round}").repeat(64),
                seqno.next(),
            );
        }
        tree.flush_active_memtable(0)?;
    }
    tree.major_compact(u64::MAX, seqno.get())?;

    let version = tree.current_version();
    let mut expected_blobs: Vec<BlobFileRecord> = version
        .blob_files
        .iter()
        .map(|b| BlobFileRecord {
            id: b.id(),
            checksum: b.checksum().into_u128(),
        })
        .collect();
    expected_blobs.sort_unstable_by_key(|b| b.id);
    let mut expected_stats: Vec<BlobGcStats> = version
        .gc_stats()
        .iter()
        .map(|(id, e)| BlobGcStats {
            id: *id,
            stale_items: e.len as u64,
            stale_bytes: e.bytes,
            stale_on_disk_bytes: e.on_disk_bytes,
        })
        .collect();
    expected_stats.sort_unstable_by_key(|s| s.id);
    drop(version);
    drop(tree);

    let state = read_manifest(folder.path(), &crate::fs::StdFs, None)?;
    assert_eq!(state.tree_type, TreeType::Blob);
    assert_eq!(state.blob_files, expected_blobs);
    assert_eq!(state.blob_gc_stats, expected_stats);
    assert!(
        !state.blob_gc_stats.is_empty(),
        "the overwrite round left garbage in the first blob file"
    );
    Ok(())
}

/// Reading the manifest changes no byte of the tree's directory, edit log
/// included: the export is for a store the converter has not decided to touch.
#[test]
fn read_manifest_leaves_the_directory_byte_identical() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = open(folder.path(), None)?;
    let seqno = SequenceNumberCounter::default();
    for round in 0..3u32 {
        tree.insert(format!("k{round}"), "v", seqno.next());
        tree.flush_active_memtable(0)?;
    }
    drop(tree);

    let before = snapshot(folder.path())?;
    read_manifest(folder.path(), &crate::fs::StdFs, None)?;
    assert_eq!(snapshot(folder.path())?, before);
    Ok(())
}

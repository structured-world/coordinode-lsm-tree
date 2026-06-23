use super::*;
use crate::comparator::default_comparator;
use crate::fs::{Fs, FsOpenOptions, MemFs};
use test_log::test;

fn new_memtable(id: u64) -> Memtable {
    Memtable::new(id, default_comparator())
}

fn test_super_versions(versions: Vec<SuperVersion>) -> SuperVersions {
    #[cfg(feature = "std")]
    #[expect(
        clippy::expect_used,
        reason = "test helper: every caller passes a non-empty version list"
    )]
    let latest = Arc::new(ArcSwap::from_pointee(
        versions
            .last()
            .cloned()
            .expect("test helper requires at least one version"),
    ));
    SuperVersions {
        versions: versions.into(),
        comparator_name: "default".into(),
        sync_mode: SyncMode::Normal,
        snapshot_id: 0,
        log_rotate_bytes: 1024 * 1024,
        #[cfg(feature = "std")]
        latest,
    }
}

/// Seed version files (`v1`, `v2`, ...) into `fs` at `dir` for each
/// `SuperVersion` in the list. This makes GC tests exercise the real
/// `Fs::remove_file` path instead of only hitting `NotFound`.
fn seed_version_files(dir: &Path, versions: &SuperVersions, fs: &dyn Fs) -> crate::Result<()> {
    fs.create_dir_all(dir)?;
    for sv in &versions.versions {
        let path = dir.join(format!("v{}", sv.version.id()));
        fs.open(
            &path,
            &FsOpenOptions::new().write(true).create(true).truncate(true),
        )?;
    }
    Ok(())
}

#[test]
fn super_version_gc_above_watermark() -> crate::Result<()> {
    let fs = MemFs::new();
    let dir = Path::new("/gc/above");
    let mut history = test_super_versions(vec![
        SuperVersion {
            active_memtable: Arc::new(new_memtable(0)),
            sealed_memtables: Arc::default(),
            version: Version::new(1, crate::TreeType::Standard),
            seqno: 0,
        },
        SuperVersion {
            active_memtable: Arc::new(new_memtable(0)),
            sealed_memtables: Arc::default(),
            version: Version::new(2, crate::TreeType::Standard),
            seqno: 1,
        },
        SuperVersion {
            active_memtable: Arc::new(new_memtable(0)),
            sealed_memtables: Arc::default(),
            version: Version::new(3, crate::TreeType::Standard),
            seqno: 2,
        },
    ]);
    seed_version_files(dir, &history, &fs)?;

    // gc_watermark=0 → early return, no GC
    history.maintenance(dir, 0, &fs)?;

    assert_eq!(history.free_list_len(), 2);
    // All version files still present (no GC ran)
    assert!(fs.exists(&dir.join("v1"))?);
    assert!(fs.exists(&dir.join("v2"))?);
    assert!(fs.exists(&dir.join("v3"))?);

    Ok(())
}

#[test]
fn super_version_gc_preserves_current_snapshot_file() -> crate::Result<()> {
    // The CURRENT snapshot file must survive GC even when its in-memory
    // version is evicted from the history — `CURRENT` still points at it and
    // the edit log layers on top. Set snapshot_id to a seeded v{id} that GC
    // will evict, and assert its file stays while a non-snapshot evictee is
    // removed.
    let fs = MemFs::new();
    let dir = Path::new("/gc/snapshot");
    let mut history = test_super_versions(vec![
        SuperVersion {
            active_memtable: Arc::new(new_memtable(0)),
            sealed_memtables: Arc::default(),
            version: Version::new(1, crate::TreeType::Standard),
            seqno: 0,
        },
        SuperVersion {
            active_memtable: Arc::new(new_memtable(0)),
            sealed_memtables: Arc::default(),
            version: Version::new(2, crate::TreeType::Standard),
            seqno: 1,
        },
        SuperVersion {
            active_memtable: Arc::new(new_memtable(0)),
            sealed_memtables: Arc::default(),
            version: Version::new(3, crate::TreeType::Standard),
            seqno: 2,
        },
    ]);
    // CURRENT points at v1 (the snapshot the edit log is layered on).
    history.snapshot_id = 1;
    seed_version_files(dir, &history, &fs)?;

    // Watermark 3 evicts v1 (seqno 0) and v2 (seqno 1) from the history.
    history.maintenance(dir, 3, &fs)?;

    assert!(
        fs.exists(&dir.join("v1"))?,
        "the CURRENT snapshot file must NOT be GC'd even when its version is evicted"
    );
    assert!(
        !fs.exists(&dir.join("v2"))?,
        "a non-snapshot evicted version's file is still removed"
    );
    Ok(())
}

#[test]
fn super_version_gc_below_watermark_simple() -> crate::Result<()> {
    let fs = MemFs::new();
    let dir = Path::new("/gc/simple");
    let mut history = test_super_versions(vec![
        SuperVersion {
            active_memtable: Arc::new(new_memtable(0)),
            sealed_memtables: Arc::default(),
            version: Version::new(1, crate::TreeType::Standard),
            seqno: 0,
        },
        SuperVersion {
            active_memtable: Arc::new(new_memtable(0)),
            sealed_memtables: Arc::default(),
            version: Version::new(2, crate::TreeType::Standard),
            seqno: 1,
        },
        SuperVersion {
            active_memtable: Arc::new(new_memtable(0)),
            sealed_memtables: Arc::default(),
            version: Version::new(3, crate::TreeType::Standard),
            seqno: 2,
        },
    ]);
    seed_version_files(dir, &history, &fs)?;

    history.maintenance(dir, 3, &fs)?;

    assert_eq!(history.len(), 1);
    // v1 and v2 deleted by GC, v3 kept
    assert!(!fs.exists(&dir.join("v1"))?);
    assert!(!fs.exists(&dir.join("v2"))?);
    assert!(fs.exists(&dir.join("v3"))?);

    Ok(())
}

#[test]
fn super_version_gc_below_watermark_simple_2() -> crate::Result<()> {
    let fs = MemFs::new();
    let dir = Path::new("/gc/simple2");
    let mut history = test_super_versions(vec![
        SuperVersion {
            active_memtable: Arc::new(new_memtable(0)),
            sealed_memtables: Arc::default(),
            version: Version::new(1, crate::TreeType::Standard),
            seqno: 0,
        },
        SuperVersion {
            active_memtable: Arc::new(new_memtable(0)),
            sealed_memtables: Arc::default(),
            version: Version::new(2, crate::TreeType::Standard),
            seqno: 1,
        },
        SuperVersion {
            active_memtable: Arc::new(new_memtable(0)),
            sealed_memtables: Arc::default(),
            version: Version::new(3, crate::TreeType::Standard),
            seqno: 2,
        },
        SuperVersion {
            active_memtable: Arc::new(new_memtable(0)),
            sealed_memtables: Arc::default(),
            version: Version::new(4, crate::TreeType::Standard),
            seqno: 8,
        },
    ]);
    seed_version_files(dir, &history, &fs)?;

    history.maintenance(dir, 3, &fs)?;

    assert_eq!(history.len(), 2);
    // v1 and v2 deleted, v3 and v4 kept
    assert!(!fs.exists(&dir.join("v1"))?);
    assert!(!fs.exists(&dir.join("v2"))?);
    assert!(fs.exists(&dir.join("v3"))?);
    assert!(fs.exists(&dir.join("v4"))?);

    Ok(())
}

#[test]
fn super_version_gc_below_watermark_keep() -> crate::Result<()> {
    let fs = MemFs::new();
    let dir = Path::new("/gc/keep");
    let mut history = test_super_versions(vec![
        SuperVersion {
            active_memtable: Arc::new(new_memtable(0)),
            sealed_memtables: Arc::default(),
            version: Version::new(1, crate::TreeType::Standard),
            seqno: 0,
        },
        SuperVersion {
            active_memtable: Arc::new(new_memtable(0)),
            sealed_memtables: Arc::default(),
            version: Version::new(2, crate::TreeType::Standard),
            seqno: 8,
        },
    ]);
    seed_version_files(dir, &history, &fs)?;

    history.maintenance(dir, 3, &fs)?;

    assert_eq!(history.len(), 2);
    // Both kept — no version below watermark has a successor also below watermark
    assert!(fs.exists(&dir.join("v1"))?);
    assert!(fs.exists(&dir.join("v2"))?);

    Ok(())
}

#[test]
fn super_version_gc_below_watermark_shadowed() -> crate::Result<()> {
    let fs = MemFs::new();
    let dir = Path::new("/gc/shadowed");
    let mut history = test_super_versions(vec![
        SuperVersion {
            active_memtable: Arc::new(new_memtable(0)),
            sealed_memtables: Arc::default(),
            version: Version::new(1, crate::TreeType::Standard),
            seqno: 0,
        },
        SuperVersion {
            active_memtable: Arc::new(new_memtable(0)),
            sealed_memtables: Arc::default(),
            version: Version::new(2, crate::TreeType::Standard),
            seqno: 2,
        },
    ]);
    seed_version_files(dir, &history, &fs)?;

    history.maintenance(dir, 3, &fs)?;

    assert_eq!(history.len(), 1);
    // v1 deleted, v2 kept
    assert!(!fs.exists(&dir.join("v1"))?);
    assert!(fs.exists(&dir.join("v2"))?);

    Ok(())
}

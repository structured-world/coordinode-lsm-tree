use super::*;
use crate::TreeType;

fn empty_version(id: u64) -> Version {
    Version::new(id, TreeType::Standard)
}

/// A snapshot stores a level's run count in one byte. A level wider than that
/// must be refused, not written with a wrapped count that no reopen can walk.
#[test]
fn encode_into_refuses_a_level_with_more_than_255_runs() -> crate::Result<()> {
    use crate::{AbstractTree, Config, SequenceNumberCounter, fs::StdFs};

    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    for i in 0..256u64 {
        let key = format!("k{i:05}");
        // Rewriting `zzz` on every flush makes every table overlap every
        // other, so each one stays its own L0 run.
        tree.insert(key.as_bytes(), key.as_bytes(), 2 * i + 1);
        tree.insert(b"zzz", key.as_bytes(), 2 * i + 2);
        tree.flush_active_memtable(2 * i + 2)?;
    }
    let version = tree.current_version();
    assert_eq!(version.l0().run_count(), 256);
    assert!(!version.fits_snapshot());

    let out = tempfile::tempdir()?;
    let mut writer = crate::manifest_blocks::writer::ManifestArchiveWriter::create(
        &out.path().join("v0"),
        &StdFs,
        Arc::new(crate::runtime_config::RuntimeConfig::default()),
        None,
        crate::fs::SyncMode::Normal,
    )?;
    let Err(crate::Error::Io(err)) = version.encode_into(&mut writer, "default") else {
        panic!("a level of 256 runs must be refused");
    };
    assert_eq!(err.kind(), crate::io::ErrorKind::InvalidInput);
    Ok(())
}

#[test]
fn set_retention_floor_below_the_current_one_is_a_no_op() {
    let mut v = empty_version(1).with_retention_floor(40);
    v.set_retention_floor(10);
    assert_eq!(v.retention_floor(), 40, "the floor never goes down");
    v.set_retention_floor(40);
    assert_eq!(v.retention_floor(), 40, "equal is not above");
}

#[test]
fn set_retention_floor_on_a_sole_owner_writes_through() {
    let mut v = empty_version(1);
    let before = Arc::as_ptr(&v.inner);

    v.set_retention_floor(40);

    assert_eq!(v.retention_floor(), 40);
    assert!(
        core::ptr::eq(before, Arc::as_ptr(&v.inner)),
        "a sole owner keeps its allocation instead of rebuilding it",
    );
}

#[test]
fn set_retention_floor_on_a_shared_handle_leaves_the_other_owner_alone() {
    let mut v = empty_version(1);
    // A second handle on the same allocation, which is what an install whose
    // mutator returned the prior version untouched holds.
    let shared = v.clone();

    v.set_retention_floor(40);

    assert_eq!(
        v.retention_floor(),
        40,
        "the raised handle carries the floor"
    );
    assert_eq!(
        shared.retention_floor(),
        0,
        "the other owner must not see a floor it never recorded",
    );
    assert!(
        !core::ptr::eq(Arc::as_ptr(&v.inner), Arc::as_ptr(&shared.inner)),
        "a shared handle rebuilds rather than mutating in place",
    );
}
